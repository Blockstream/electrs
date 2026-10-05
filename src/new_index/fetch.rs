use rayon::prelude::*;

#[cfg(feature = "liquid")]
use crate::elements::ebcompact::*;
#[cfg(not(feature = "liquid"))]
use bitcoin::consensus::encode::{deserialize, Decodable};
#[cfg(feature = "liquid")]
use elements::encode::{deserialize, Decodable};

use std::collections::HashMap;
use std::fs;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::mpsc::Receiver;
use std::thread;

use electrs_macros::trace;

use crate::chain::{Block, BlockHash, Txid};
use crate::daemon::Daemon;
use crate::errors::*;
use crate::util::{spawn_thread, HeaderEntry, SyncChannel};

#[derive(Clone, Copy, Debug)]
pub enum FetchFrom {
    Bitcoind,
    BlkFiles,
}

#[trace]
pub fn start_fetcher(
    from: FetchFrom,
    daemon: &Daemon,
    new_headers: Vec<HeaderEntry>,
    batch_size: usize,
    chain_tip_height: usize,
) -> Result<Fetcher<Vec<BlockEntry>>> {
    match from {
        FetchFrom::Bitcoind => bitcoind_fetcher(daemon, new_headers, batch_size, chain_tip_height),
        FetchFrom::BlkFiles => blkfiles_fetcher(daemon, new_headers),
    }
}

#[derive(Clone)]
pub struct BlockEntry {
    pub block: Block,
    pub entry: HeaderEntry,
    pub size: u32,
    /// Pre-computed txids, must always correspond 1:1 with block.txdata
    pub txids: Vec<Txid>,
}

type SizedBlock = (Block, u32);

pub struct Fetcher<T> {
    receiver: Receiver<T>,
    thread: thread::JoinHandle<Result<()>>,
}

impl<T> Fetcher<T> {
    fn from(receiver: Receiver<T>, thread: thread::JoinHandle<Result<()>>) -> Self {
        Fetcher { receiver, thread }
    }

    pub fn map<F>(self, mut func: F) -> Result<()>
    where
        F: FnMut(T) -> Result<()>,
    {
        let mut consumer_error = None;
        for item in self.receiver {
            if let Err(e) = func(item) {
                consumer_error = Some(e);
                break;
            }
        }
        let producer_result = match self.thread.join() {
            Ok(result) => result,
            Err(_) => bail!("fetcher thread panicked"),
        };
        match consumer_error {
            Some(e) => Err(e),
            None => producer_result,
        }
    }
}

#[trace]
fn bitcoind_fetcher(
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
            for entries in new_headers.chunks(batch_size) {
                if fetcher_count % 50 == 0 && total_blocks_fetched >= 50 {
                    let batch_height = entries.last().map(|e| e.height()).unwrap_or(0);
                    info!("fetching blocks {}/{} ({:.1}%)",
                        batch_height,
                        chain_tip_height,
                        batch_height as f32 / chain_tip_height.max(1) as f32 * 100.0
                    );
                }
                fetcher_count += 1;

                let blockhashes: Vec<BlockHash> = entries.iter().map(|e| *e.hash()).collect();
                let blocks = daemon.getblocks(&blockhashes).chain_err(|| {
                    format!(
                        "failed to fetch block batch first_hash='{:?}' batch_size='{}'",
                        blockhashes.first(),
                        blockhashes.len()
                    )
                })?;
                if blocks.len() != entries.len() {
                    bail!(
                        "bitcoind_fetcher block/entry count mismatch expected='{}' got='{}'",
                        entries.len(),
                        blocks.len()
                    );
                }
                let block_entries: Vec<BlockEntry> = blocks
                    .into_iter()
                    .zip(entries)
                    .map(|(block, entry)| {
                        let txids = block.txdata.iter().map(|tx| tx.compute_txid()).collect();
                        BlockEntry {
                            entry: entry.clone(), // TODO: remove this clone()
                            size: block.total_size() as u32,
                            txids,
                            block,
                        }
                    })
                    .collect();
                sender
                    .send(block_entries)
                    .chain_err(|| "bitcoind_fetcher receiver dropped")?;
                log::debug!("last fetch {:?}", entries.last());
            }
            Ok(())
        }),
    ))
}

#[trace]
fn blkfiles_fetcher(
    daemon: &Daemon,
    new_headers: Vec<HeaderEntry>,
) -> Result<Fetcher<Vec<BlockEntry>>> {
    let magic = daemon.magic();
    let blk_files = daemon.list_blk_files()?;
    let xor_key = daemon.read_blk_file_xor_key()?;

    // Buffer of 2 lets the parser produce one batch ahead of the consumer,
    // overlapping block-entry construction with the indexer.
    let chan = SyncChannel::new(2);
    let sender = chan.sender();

    let mut entry_map: HashMap<BlockHash, HeaderEntry> =
        new_headers.into_iter().map(|h| (*h.hash(), h)).collect();

    let parser = blkfiles_parser(blkfiles_reader(blk_files, xor_key), magic);
    Ok(Fetcher::from(
        chan.into_receiver(),
        spawn_thread("blkfiles_fetcher", move || {
            parser.map(|sizedblocks| {
                let block_count = sizedblocks.len();
                let mut index = 0;
                let block_entries: Vec<BlockEntry> = sizedblocks
                    .into_iter()
                    .filter_map(|(block, size)| {
                        index += 1;
                        debug!("fetch block {:}/{:} {:.2}%",
                            index,
                            block_count,
                            (index/block_count) as f32/100.0
                        );
                        let blockhash = block.block_hash();
                        entry_map
                            .remove(&blockhash)
                            .map(|entry| {
                                let txids = block.txdata.iter().map(|tx| tx.compute_txid()).collect();
                                BlockEntry { block, entry, size, txids }
                            })
                            .or_else(|| {
                                trace!("skipping block {}", blockhash);
                                None
                            })
                    })
                    .collect();
                trace!("fetched {} blocks", block_entries.len());
                sender.send(block_entries).chain_err(|| {
                    "failed to send block entries from blk*.dat files"
                })?;
                Ok(())
            })?;
            if !entry_map.is_empty() {
                bail!(
                    "failed to index {} blocks from blk*.dat files",
                    entry_map.len()
                )
            }
            Ok(())
        }),
    ))
}

#[trace]
fn blkfiles_reader(blk_files: Vec<PathBuf>, xor_key: Option<[u8; 8]>) -> Fetcher<Vec<u8>> {
    // Buffer of 2 lets the reader read ahead by one blk file while the parser
    // is working, overlapping sequential disk I/O with CPU deserialization.
    let chan = SyncChannel::new(2);
    let sender = chan.sender();

    Fetcher::from(
        chan.into_receiver(),
        spawn_thread("blkfiles_reader", move || {
            let blk_files_len = blk_files.len();
            for (count, path) in blk_files.iter().enumerate() {
                info!("block file reading {:}/{:} {:.2}%",
                    count,
                    blk_files_len,
                    count / blk_files_len
                );

                trace!("reading {:?}", path);
                let mut blob = fs::read(&path)
                    .chain_err(|| format!("failed to read {:?}", path))?;
                if let Some(xor_key) = xor_key {
                    blkfile_apply_xor_key(xor_key, &mut blob);
                }
                sender
                    .send(blob)
                    .chain_err(|| format!("failed to send {:?} contents", path))?;
            }
            Ok(())
        }),
    )
}

/// By default, bitcoind v28.0+ applies an 8-byte "xor key" over each "blk*.dat"
/// file. We have xor again to undo this transformation.
fn blkfile_apply_xor_key(xor_key: [u8; 8], blob: &mut [u8]) {
    for (i, blob_i) in blob.iter_mut().enumerate() {
        *blob_i ^= xor_key[i & 0x7];
    }
}

#[trace]
fn blkfiles_parser(blobs: Fetcher<Vec<u8>>, magic: u32) -> Fetcher<Vec<SizedBlock>> {
    // Buffer of 2 lets the parser stay one batch ahead of the fetcher stage.
    let chan = SyncChannel::new(2);
    let sender = chan.sender();

    Fetcher::from(
        chan.into_receiver(),
        spawn_thread("blkfiles_parser", move || {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(0) // CPU-bound
                .thread_name(|i| format!("parse-blocks-{}", i))
                .build()
                .unwrap();
            blobs.map(|blob| {
                trace!("parsing {} bytes", blob.len());
                let blocks = parse_blocks(&pool, blob, magic)?;
                sender
                    .send(blocks)
                    .chain_err(|| "blkfiles_parser receiver dropped")?;
                Ok(())
            })?;
            Ok(())
        }),
    )
}

#[trace]
fn parse_blocks(pool: &rayon::ThreadPool, blob: Vec<u8>, magic: u32) -> Result<Vec<SizedBlock>> {
    let mut cursor = Cursor::new(&blob);
    let mut slices = vec![];
    let max_pos = blob.len() as u64;

    while cursor.position() < max_pos {
        let offset = cursor.position();
        match u32::consensus_decode(&mut cursor) {
            Ok(value) => {
                if magic != value {
                    cursor.set_position(offset + 1);
                    continue;
                }
            }
            Err(_) => break, // EOF
        };
        let block_size = u32::consensus_decode(&mut cursor).chain_err(|| "no block size")?;
        let start = cursor.position();
        let end = start + block_size as u64;

        // The magic+size truncation handling below covers a truncated header prefix,
        // but not a truncated block body. Treat that as EOF.
        if end > max_pos {
            break;
        }

        // If Core's WriteBlockToDisk ftell fails, only the magic bytes and size will be written
        // and the block body won't be written to the blk*.dat file.
        // Since the first 4 bytes should contain the block's version, we can skip such blocks
        // by peeking the cursor (and skipping previous `magic` and `block_size`).
        match u32::consensus_decode(&mut cursor) {
            Ok(value) => {
                if magic == value {
                    cursor.set_position(start);
                    continue;
                }
            }
            Err(_) => break, // EOF
        }
        slices.push((&blob[start as usize..end as usize], block_size));
        cursor.set_position(end as u64);
    }

    pool.install(|| {
        slices
            .into_par_iter()
            .map(|(slice, size)| {
                let block = deserialize(slice)
                    .chain_err(|| "failed to deserialize block from blk file")?;
                Ok((block, size))
            })
            .collect::<Result<Vec<SizedBlock>>>()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fetcher_surfaces_producer_error() {
        let chan = SyncChannel::<u8>::new(1);
        let fetcher = Fetcher::from(
            chan.into_receiver(),
            spawn_thread("failing_fetcher", move || bail!("producer failed")),
        );

        let error = fetcher.map(|_| Ok(())).unwrap_err();
        assert!(format!("{:?}", error).contains("producer failed"));
    }

    #[test]
    fn fetcher_delivers_partial_output_before_producer_error() {
        let chan = SyncChannel::new(1);
        let sender = chan.sender();
        let fetcher = Fetcher::from(
            chan.into_receiver(),
            spawn_thread("partially_failing_fetcher", move || {
                sender.send(1).chain_err(|| "receiver dropped")?;
                bail!("producer failed after partial output")
            }),
        );
        let mut items = vec![];

        let error = fetcher
            .map(|item| {
                items.push(item);
                Ok(())
            })
            .unwrap_err();

        assert_eq!(items, vec![1]);
        assert!(format!("{:?}", error).contains("producer failed after partial output"));
    }

    #[test]
    fn fetcher_surfaces_consumer_error() {
        let chan = SyncChannel::new(1);
        let sender = chan.sender();
        let fetcher = Fetcher::from(
            chan.into_receiver(),
            spawn_thread("successful_fetcher", move || {
                sender.send(1).chain_err(|| "receiver dropped")?;
                Ok(())
            }),
        );

        let error = fetcher
            .map(|_| bail!("consumer failed"))
            .unwrap_err();
        assert!(format!("{:?}", error).contains("consumer failed"));
    }
}
