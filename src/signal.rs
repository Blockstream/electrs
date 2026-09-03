use bitcoin::BlockHash;
use crossbeam_channel::{self as channel, after, select};
use std::thread;
use std::time::{Duration, Instant};

use signal_hook::consts::{SIGINT, SIGTERM, SIGUSR1};

use crate::errors::*;

#[derive(Clone)] // so multiple threads could wait on signals
pub struct Waiter {
    receiver: channel::Receiver<i32>,
    zmq_receiver: channel::Receiver<BlockHash>,
}

fn notify(signals: &[i32]) -> channel::Receiver<i32> {
    let (s, r) = channel::bounded(1);
    let mut signals =
        signal_hook::iterator::Signals::new(signals).expect("failed to register signal hook");
    thread::spawn(move || {
        for signal in signals.forever() {
            s.send(signal)
                .unwrap_or_else(|_| panic!("failed to send signal {}", signal));
        }
    });
    r
}

impl Waiter {
    pub fn start(block_hash_receive: channel::Receiver<BlockHash>) -> Waiter {
        Waiter {
            receiver: notify(&[
                SIGINT, SIGTERM,
                SIGUSR1, // allow external triggering (e.g. via bitcoind `blocknotify`)
            ]),
            zmq_receiver: block_hash_receive,
        }
    }

    pub fn wait(&self, duration: Duration, accept_block_notification: bool) -> Result<()> {
        // Iterative rather than recursive. A caller that is not accepting block
        // notifications keeps waiting out the rest of its budget after each one,
        // and recursing there meant one stack frame per notification - a flood of
        // them could exhaust the stack before the deadline ever expired.
        let mut remaining = duration;

        loop {
            let start = Instant::now();

            // `false` means we were woken by a notification rather than by the
            // deadline, so the loop goes round again unless we accept those.
            let deadline_expired = select! {
                recv(self.receiver) -> msg => {
                    match msg {
                        Ok(sig) if sig == SIGUSR1 => {
                            trace!("notified via SIGUSR1");
                            false
                        }
                        Ok(sig) => bail!(ErrorKind::Interrupt(sig)),
                        Err(_) => bail!("signal hook channel disconnected"),
                    }
                },
                recv(self.zmq_receiver) -> msg => {
                    match msg {
                        Ok(_) => false,
                        Err(_) => bail!("signal hook channel disconnected"),
                    }
                },
                recv(after(remaining)) -> _ => true,
            };

            if deadline_expired || accept_block_notification {
                return Ok(());
            }

            remaining = remaining.saturating_sub(start.elapsed());
        }
    }
}
