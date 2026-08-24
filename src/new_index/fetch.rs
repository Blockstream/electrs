use std::sync::mpsc::Receiver;
use std::thread;

use electrs_macros::trace;
use itertools::Itertools;

use crate::chain::{Block, BlockHash, Txid};
use crate::daemon::Daemon;
use crate::errors::*;
use crate::util::{spawn_thread, HeaderEntry, SyncChannel};

#[cfg(feature = "liquid")]
use crate::elements::ebcompact::{SizeMethod, TxidCompat};

#[derive(Clone)]
pub struct BlockEntry {
    pub block: Block,
    pub entry: HeaderEntry,
    pub size: u32,
    /// Pre-computed txids, must always correspond 1:1 with block.txdata
    pub txids: Vec<Txid>,
}

pub struct Fetcher<T> {
    receiver: Receiver<T>,
    thread: thread::JoinHandle<()>,
}

impl<T> Fetcher<T> {
    fn from(receiver: Receiver<T>, thread: thread::JoinHandle<()>) -> Self {
        Fetcher { receiver, thread }
    }

    pub fn map<F>(self, mut func: F)
    where
        F: FnMut(T) -> (),
    {
        for item in self.receiver {
            func(item);
        }
        self.thread.join().expect("fetcher thread panicked")
    }
}

#[trace]
pub fn start_fetcher(
    daemon: &Daemon,
    new_headers: Vec<HeaderEntry>,
    batch_size: usize,
    chain_tip_height: usize,
) -> Result<Fetcher<Vec<BlockEntry>>> {
    if let Some(tip) = new_headers.last() {
        debug!("{:?} ({} left to index)", tip, new_headers.len());
    };
    let daemon = daemon.reconnect()?;
    let chan = SyncChannel::new(1);
    let sender = chan.sender();
    Ok(Fetcher::from(
        chan.into_receiver(),
        spawn_thread("bitcoind_fetcher", move || {
            let mut fetcher_count = 0;
            let total_blocks_fetched = new_headers.len();
            for entries in &new_headers.into_iter().chunks(batch_size) {
                let entries: Vec<_> = entries.collect();
                if fetcher_count % 50 == 0 && total_blocks_fetched >= 50 {
                    let batch_height = entries.last().map(|e| e.height()).unwrap_or(0);
                    info!(
                        "fetching blocks {}/{} ({:.1}%)",
                        batch_height,
                        chain_tip_height,
                        batch_height as f32 / chain_tip_height.max(1) as f32 * 100.0
                    );
                }
                fetcher_count += 1;

                let blockhashes: Vec<BlockHash> = entries.iter().map(|e| *e.hash()).collect();
                let blocks = daemon
                    .getblocks(&blockhashes)
                    .expect("failed to get blocks from bitcoind");
                let block_entries: Vec<BlockEntry> = blocks
                    .into_iter()
                    .zip_eq(entries)
                    .map(|(block, entry)| {
                        let txids = block.txdata.iter().map(|tx| tx.compute_txid()).collect();
                        BlockEntry {
                            entry,
                            size: block.total_size() as u32,
                            txids,
                            block,
                        }
                    })
                    .collect();
                log::debug!("last fetch {:?}", block_entries.last().map(|b| &b.entry));
                sender
                    .send(block_entries)
                    .expect("failed to send fetched blocks");
            }
        }),
    ))
}
