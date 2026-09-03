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
/// something we have a use for, so let libzmq discard it before it reaches us.
const MAX_FRAME_BYTES: i64 = 1024;

/// Upper bound on frames buffered from one multipart message. `maxmsgsize` caps
/// the size of each frame but not how many frames a publisher may send, so cap
/// that separately.
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

/// Receive one complete multipart message, buffering at most [`MAX_FRAMES`].
///
/// Frames past the cap are drained and discarded rather than accumulated, so a
/// publisher sending an unbounded number of parts cannot grow our memory. Returns
/// `None` when the message exceeded the cap and was therefore dropped.
fn recv_message(subscriber: &zmq::Socket) -> zmq::Result<Option<Vec<Vec<u8>>>> {
    let mut frames = Vec::new();
    let mut over_cap = false;

    loop {
        let frame = subscriber.recv_msg(0)?;
        if frames.len() < MAX_FRAMES {
            frames.push(frame.to_vec());
        } else {
            over_cap = true;
        }

        // libzmq delivers a multipart message atomically, so the remaining parts
        // are already queued and these calls do not block.
        if !subscriber.get_rcvmore()? {
            break;
        }
    }

    Ok(if over_cap { None } else { Some(frames) })
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
        .set_maxmsgsize(MAX_FRAME_BYTES)
        .chain_err(|| "failed setting ZMQ maxmsgsize")?;
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
