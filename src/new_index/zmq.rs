//! Subscriber for bitcoind's ZMQ `hashblock` notifications.
//!
//! The notification is only ever used as a wake-up hint: `Waiter::wait` discards
//! the hash and simply lets the main loop run a tip check one cycle early. The
//! subscriber therefore treats everything the publisher sends as untrusted input
//! and bounds what it can cost us - there is no transport authentication here, so
//! anyone able to reach or impersonate the endpoint can publish to us.

use std::thread;
use std::time::{Duration, Instant};

use bitcoin::{hashes::Hash, BlockHash};
use crossbeam_channel::{Sender, TrySendError};

use crate::errors::*;
use crate::metrics::{CounterVec, MetricOpts, Metrics};
use crate::util::spawn_thread;

const TOPIC_HASHBLOCK: &[u8] = b"hashblock";
const BLOCK_HASH_BYTES: usize = 32;

/// bitcoind publishes `hashblock` as three frames - topic, 32-byte hash, 4-byte
/// sequence number - roughly 45 bytes in total. Anything materially larger is not
/// something we have a use for, so it is dropped instead of being copied out.
///
/// Enforced here rather than with `ZMQ_MAXMSGSIZE`, deliberately. libzmq treats an
/// oversized message as a *protocol* error: it tears down the session and does not
/// reconnect afterwards. A single hostile message would therefore disable block
/// notifications for the lifetime of the process, silently - no error is returned
/// to `recv`, so nothing logs and nothing retries. Checking the frame ourselves
/// costs one comparison and leaves the connection usable.
const MAX_FRAME_BYTES: usize = 1024;

/// Upper bound on frames buffered from one multipart message. A size limit bounds
/// each frame but not how many a publisher may send, so cap that separately -
/// otherwise unbounded small frames add up to the same thing.
const MAX_FRAMES: usize = 8;

/// Bound on libzmq's own receive queue. SUB sockets discard messages past the
/// high-water mark, which is the behaviour we want given we coalesce anyway.
const RCVHWM: i32 = 16;

/// Return from `recv` this often even when idle, so the thread can notice that
/// the receiving end has gone away and exit instead of blocking forever.
const RCVTIMEO_MS: i32 = 1000;

/// Minimum spacing between forwarded notifications. The main loop polls every 5s
/// regardless, so coalescing at this granularity costs nothing in normal
/// operation while bounding how much daemon RPC work a flood of notifications can
/// force us into.
const MIN_NOTIFY_INTERVAL: Duration = Duration::from_millis(250);

const MIN_ERROR_BACKOFF: Duration = Duration::from_millis(100);
const MAX_ERROR_BACKOFF: Duration = Duration::from_secs(5);

/// Minimum-interval gate over a stream of events.
struct Throttle {
    min_interval: Duration,
    last: Option<Instant>,
}

impl Throttle {
    fn new(min_interval: Duration) -> Self {
        Throttle {
            min_interval,
            last: None,
        }
    }

    /// Whether an event at `now` should be let through. Anything arriving less
    /// than `min_interval` after the last admitted event is rejected.
    fn allow(&mut self, now: Instant) -> bool {
        match self.last {
            Some(last) if now.saturating_duration_since(last) < self.min_interval => false,
            _ => {
                self.last = Some(now);
                true
            }
        }
    }
}

/// Extract the block hash from a well-formed `hashblock` notification.
///
/// Returns `None` for a topic we did not subscribe to, a body that is not exactly
/// a hash, or a message missing frames. bitcoind publishes the hash in display
/// byte order, so it is reversed to get the internal order `BlockHash` expects.
fn parse_hashblock(frames: &[Vec<u8>]) -> Option<BlockHash> {
    let topic = frames.first()?;
    let body = frames.get(1)?;

    if topic.as_slice() != TOPIC_HASHBLOCK || body.len() != BLOCK_HASH_BYTES {
        return None;
    }

    let mut reversed = body.clone();
    reversed.reverse();
    BlockHash::from_slice(&reversed).ok()
}

/// Whether a frame is worth copying out of libzmq, given how many we already hold.
///
/// Rejects a frame that is too large or that is past the count we are willing to
/// buffer. Both bounds matter: the size limit alone leaves a publisher free to send
/// unlimited small parts, and the count limit alone leaves each part unbounded.
fn frame_admissible(frame_len: usize, already_buffered: usize) -> bool {
    frame_len <= MAX_FRAME_BYTES && already_buffered < MAX_FRAMES
}

/// Receive one complete multipart message, buffering at most [`MAX_FRAMES`] frames
/// of at most [`MAX_FRAME_BYTES`] each.
///
/// Inadmissible frames are drained and dropped where they land rather than copied,
/// so neither an oversized part nor an unbounded number of parts can grow our
/// memory. The whole message is still read to completion - leaving parts queued
/// would desynchronise the next read. Returns `None` when anything was rejected,
/// which discards the message rather than acting on a partial one.
fn recv_message(subscriber: &zmq::Socket) -> zmq::Result<Option<Vec<Vec<u8>>>> {
    let mut frames = Vec::new();
    let mut rejected = false;

    loop {
        let frame = subscriber.recv_msg(0)?;
        if frame_admissible(frame.len(), frames.len()) {
            frames.push(frame.to_vec());
        } else {
            rejected = true;
        }

        // libzmq delivers a multipart message atomically, so the remaining parts
        // are already queued and these calls do not block.
        if !subscriber.get_rcvmore()? {
            break;
        }
    }

    Ok(if rejected { None } else { Some(frames) })
}

/// Connect a SUB socket to `url` and forward `hashblock` notifications.
///
/// Takes the sender by reference deliberately. If setup fails the caller's sender
/// must stay alive: dropping it would disconnect the channel, and `Waiter::wait`
/// treats a disconnected channel as a fatal error, so a ZMQ misconfiguration
/// would stop the server instead of falling back to polling.
pub fn start(url: &str, block_hash_notify: &Sender<BlockHash>, metrics: &Metrics) -> Result<()> {
    log::debug!("starting ZMQ subscriber: url='{url}'");

    let ctx = zmq::Context::new();
    let subscriber = ctx
        .socket(zmq::SUB)
        .chain_err(|| format!("failed creating ZMQ subscriber for url='{url}'"))?;

    subscriber
        .set_rcvhwm(RCVHWM)
        .chain_err(|| "failed setting ZMQ rcvhwm")?;
    subscriber
        .set_rcvtimeo(RCVTIMEO_MS)
        .chain_err(|| "failed setting ZMQ rcvtimeo")?;

    subscriber
        .connect(url)
        .chain_err(|| format!("failed connecting ZMQ subscriber to url='{url}'"))?;
    subscriber
        .set_subscribe(TOPIC_HASHBLOCK)
        .chain_err(|| "failed subscribing to ZMQ hashblock")?;

    let notifications = metrics.counter_vec(
        MetricOpts::new(
            "electrs_zmq_notifications_total",
            "ZMQ block notifications received, by disposition",
        ),
        &["result"],
    );

    let url = url.to_owned();
    let block_hash_notify = block_hash_notify.clone();
    spawn_thread("zmq", move || {
        subscriber_loop(&url, subscriber, block_hash_notify, notifications)
    });

    Ok(())
}

fn subscriber_loop(
    url: &str,
    subscriber: zmq::Socket,
    block_hash_notify: Sender<BlockHash>,
    notifications: CounterVec,
) {
    let mut throttle = Throttle::new(MIN_NOTIFY_INTERVAL);
    let mut backoff = MIN_ERROR_BACKOFF;

    loop {
        match recv_message(&subscriber) {
            Ok(received) => {
                backoff = MIN_ERROR_BACKOFF;

                // Over the frame cap. Already drained, so nothing was buffered.
                let Some(frames) = received else {
                    notifications.with_label_values(&["rejected"]).inc();
                    continue;
                };
                let Some(block_hash) = parse_hashblock(&frames) else {
                    notifications.with_label_values(&["rejected"]).inc();
                    continue;
                };
                if !throttle.allow(Instant::now()) {
                    notifications.with_label_values(&["throttled"]).inc();
                    continue;
                }

                match block_hash_notify.try_send(block_hash) {
                    Ok(()) => {
                        notifications.with_label_values(&["forwarded"]).inc();
                        log::debug!("new block from ZMQ: block_hash='{block_hash}'");
                    }
                    // A wake-up is already pending, so this one would add nothing.
                    Err(TrySendError::Full(_)) => {
                        notifications.with_label_values(&["coalesced"]).inc();
                    }
                    Err(TrySendError::Disconnected(_)) => {
                        log::debug!("ZMQ notification receiver is gone, stopping subscriber");
                        return;
                    }
                }
            }
            // The receive timeout expired with nothing waiting.
            Err(zmq::Error::EAGAIN) => continue,
            Err(e) => {
                log::warn!(
                    "ZMQ receive failed, backing off: url='{url}' err='{e}' backoff='{backoff:?}'"
                );
                thread::sleep(backoff);
                backoff = (backoff * 2).min(MAX_ERROR_BACKOFF);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frames(topic: &[u8], body: &[u8]) -> Vec<Vec<u8>> {
        vec![topic.to_vec(), body.to_vec(), 0u32.to_le_bytes().to_vec()]
    }

    #[test]
    fn parses_a_well_formed_hashblock() {
        let body: Vec<u8> = (0..BLOCK_HASH_BYTES as u8).collect();
        let parsed = parse_hashblock(&frames(TOPIC_HASHBLOCK, &body)).unwrap();

        // The published bytes are in display order, so they should come back out
        // unchanged. Skipping the reversal would render this the other way round.
        assert_eq!(
            parsed.to_string(),
            "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
        );
    }

    #[test]
    fn rejects_an_unsubscribed_topic() {
        let body = [0u8; BLOCK_HASH_BYTES];
        assert!(parse_hashblock(&frames(b"rawtx", &body)).is_none());
        assert!(parse_hashblock(&frames(b"hashtx", &body)).is_none());
        assert!(parse_hashblock(&frames(b"", &body)).is_none());
    }

    #[test]
    fn rejects_a_body_that_is_not_a_hash() {
        assert!(parse_hashblock(&frames(TOPIC_HASHBLOCK, &[0u8; 31])).is_none());
        assert!(parse_hashblock(&frames(TOPIC_HASHBLOCK, &[0u8; 33])).is_none());
        assert!(parse_hashblock(&frames(TOPIC_HASHBLOCK, &[])).is_none());
    }

    #[test]
    fn rejects_a_message_missing_frames() {
        assert!(parse_hashblock(&[TOPIC_HASHBLOCK.to_vec()]).is_none());
        assert!(parse_hashblock(&[]).is_none());
    }

    #[test]
    fn admits_a_frame_of_the_size_bitcoind_actually_sends() {
        // topic, 32-byte hash, 4-byte sequence.
        assert!(frame_admissible(TOPIC_HASHBLOCK.len(), 0));
        assert!(frame_admissible(BLOCK_HASH_BYTES, 1));
        assert!(frame_admissible(4, 2));
    }

    #[test]
    fn rejects_a_frame_over_the_size_limit() {
        assert!(frame_admissible(MAX_FRAME_BYTES, 0));
        assert!(!frame_admissible(MAX_FRAME_BYTES + 1, 0));
        assert!(!frame_admissible(1024 * 1024, 0));
    }

    #[test]
    fn rejects_a_frame_past_the_count_limit() {
        assert!(frame_admissible(32, MAX_FRAMES - 1));
        assert!(!frame_admissible(32, MAX_FRAMES));
        assert!(!frame_admissible(32, MAX_FRAMES + 1));
    }

    /// Setup failure has to be reported, not panicked, and it must leave the
    /// caller's sender alive. If `start` consumed the sender, this failure would
    /// drop it, disconnect the channel, and turn a misconfigured endpoint into a
    /// fatal error on the next `Waiter::wait` - the opposite of falling back to
    /// polling.
    #[test]
    fn a_failed_start_reports_an_error_and_leaves_the_sender_usable() {
        let (tx, rx) = crossbeam_channel::bounded(1);
        let metrics = Metrics::new("127.0.0.1:0".parse().unwrap());

        assert!(start("nosuchtransport://endpoint", &tx, &metrics).is_err());

        let hash = BlockHash::from_slice(&[0u8; BLOCK_HASH_BYTES]).unwrap();
        assert!(tx.try_send(hash).is_ok());
        assert_eq!(rx.try_recv().unwrap(), hash);
    }

    #[test]
    fn throttle_admits_the_first_event() {
        let mut throttle = Throttle::new(MIN_NOTIFY_INTERVAL);
        assert!(throttle.allow(Instant::now()));
    }

    #[test]
    fn throttle_rejects_inside_the_window_and_admits_on_the_boundary() {
        let mut throttle = Throttle::new(Duration::from_millis(250));
        let t0 = Instant::now();

        assert!(throttle.allow(t0));
        assert!(!throttle.allow(t0 + Duration::from_millis(1)));
        assert!(!throttle.allow(t0 + Duration::from_millis(249)));
        assert!(throttle.allow(t0 + Duration::from_millis(250)));
        assert!(!throttle.allow(t0 + Duration::from_millis(251)));
    }

    #[test]
    fn throttle_bounds_a_sustained_flood() {
        let mut throttle = Throttle::new(Duration::from_millis(250));
        let t0 = Instant::now();

        // 10k notifications arriving over one second: admit at 0, 250, 500, 750.
        let admitted = (0..10_000u64)
            .filter(|i| throttle.allow(t0 + Duration::from_micros(i * 100)))
            .count();

        assert_eq!(admitted, 4);
    }
}
