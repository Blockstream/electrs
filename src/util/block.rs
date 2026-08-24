use crate::chain::{BlockHash, BlockHeader};
use crate::errors::*;
use crate::new_index::BlockEntry;

use itertools::Itertools;
use std::collections::HashMap;
use std::fmt;
use std::slice;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime as DateTime;

use electrs_macros::trace;

const MTP_SPAN: usize = 11;

lazy_static! {
    pub static ref DEFAULT_BLOCKHASH: BlockHash =
        "0000000000000000000000000000000000000000000000000000000000000000"
            .parse()
            .unwrap();
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct BlockId {
    pub height: usize,
    pub hash: BlockHash,
    pub time: u32,
}

impl From<&HeaderEntry> for BlockId {
    fn from(header: &HeaderEntry) -> Self {
        BlockId {
            height: header.height(),
            hash: *header.hash(),
            time: header.header().time,
        }
    }
}

#[derive(Eq, PartialEq, Clone)]
pub struct HeaderEntry {
    height: usize,
    hash: BlockHash,
    header: BlockHeader,
}

impl HeaderEntry {
    pub fn new(height: usize, hash: BlockHash, header: BlockHeader) -> Self {
        Self {
            height,
            hash,
            header,
        }
    }
    pub fn hash(&self) -> &BlockHash {
        &self.hash
    }

    pub fn header(&self) -> &BlockHeader {
        &self.header
    }

    pub fn height(&self) -> usize {
        self.height
    }
}

impl fmt::Debug for HeaderEntry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let last_block_time = DateTime::from_unix_timestamp(self.header().time as i64).unwrap();
        write!(
            f,
            "hash={} height={} @ {}",
            self.hash(),
            self.height(),
            last_block_time.format(&Rfc3339).unwrap(),
        )
    }
}

pub struct HeaderList {
    headers: Vec<HeaderEntry>,
    heights: HashMap<BlockHash, usize>,
    tip: BlockHash,
}

impl HeaderList {
    pub fn empty() -> HeaderList {
        HeaderList {
            headers: vec![],
            heights: HashMap::new(),
            tip: *DEFAULT_BLOCKHASH,
        }
    }

    #[trace]
    pub fn new(
        mut headers_map: HashMap<BlockHash, BlockHeader>,
        tip_hash: BlockHash,
    ) -> HeaderList {
        trace!(
            "processing {} headers, tip at {:?}",
            headers_map.len(),
            tip_hash
        );

        let mut blockhash = tip_hash;
        let mut headers_chain: Vec<BlockHeader> = vec![];

        while blockhash != *DEFAULT_BLOCKHASH {
            let header = headers_map.remove(&blockhash).unwrap_or_else(|| {
                panic!(
                    "missing expected blockhash in headers map: {:?}, pointed from: {:?}",
                    blockhash,
                    headers_chain.last().map(|h| h.block_hash())
                )
            });
            blockhash = header.prev_blockhash;
            headers_chain.push(header);
        }
        headers_chain.reverse();

        trace!(
            "{} chained headers ({} orphan blocks left)",
            headers_chain.len(),
            headers_map.len()
        );

        let mut headers = HeaderList::empty();
        headers.append(headers.preprocess(headers_chain, &tip_hash).0);
        headers
    }

    /// Pre-process the given `BlockHeader`s to verify they connect to the chain and to
    /// transform them into `HeaderEntry`s with heights and hashes - but without saving them.
    /// If the headers trigger a reorg, the `reorged_since` height is returned too.
    /// Actually applying the headers requires to first pop() the reorged blocks (if any),
    /// then append() the new ones.
    #[trace]
    pub fn preprocess(
        &self,
        new_headers: Vec<BlockHeader>,
        new_tip: &BlockHash,
    ) -> (Vec<HeaderEntry>, Option<usize>) {
        // header[i] -> header[i-1] (i.e. header.last() is the tip)
        let (new_height, header_entries) = if !new_headers.is_empty() {
            let hashed_headers = new_headers
                .into_iter()
                .map(|h| (h.block_hash(), h))
                .collect::<Vec<_>>();
            for ((curr_blockhash, _), (_, next_header)) in hashed_headers.iter().tuple_windows() {
                assert_eq!(*curr_blockhash, next_header.prev_blockhash);
            }
            assert_eq!(hashed_headers.last().unwrap().0, *new_tip);

            let prev_blockhash = &hashed_headers.first().unwrap().1.prev_blockhash;
            let new_height = if *prev_blockhash == *DEFAULT_BLOCKHASH {
                0
            } else {
                self.header_by_blockhash(prev_blockhash)
                    .expect("headers do not connect")
                    .height()
                    + 1
            };
            let header_entries = (new_height..)
                .zip(hashed_headers)
                .map(|(height, (hash, header))| HeaderEntry {
                    height,
                    hash,
                    header,
                })
                .collect();
            (new_height, header_entries)
        } else {
            // No new headers, but the new tip could potentially shorten the chain (or be a no-op if it matches the existing tip)
            // This should not normally happen, but might due to manual `invalidateblock`
            let new_height = self
                .header_by_blockhash(new_tip)
                .expect("new tip not in chain")
                .height()
                + 1;
            (new_height, vec![])
        };
        let reorged_since = (new_height < self.len()).then_some(new_height);
        (header_entries, reorged_since)
    }

    /// Pop off reorged blocks since (including) the given height and return them
    /// alongside the new chain tip (the common ancestor)
    #[trace]
    pub fn pop(&mut self, since_height: usize) -> (Vec<HeaderEntry>, BlockHash) {
        let reorged_headers = self.headers.split_off(since_height);

        for header in &reorged_headers {
            self.heights.remove(header.hash());
        }
        self.tip = self
            .headers
            .last()
            .map(|h| *h.hash())
            .unwrap_or_else(|| *DEFAULT_BLOCKHASH);

        (reorged_headers, self.tip)
    }

    /// Append new headers. Expected to always extend the tip (stale blocks must be removed first)
    #[trace]
    pub fn append(&mut self, new_headers: Vec<HeaderEntry>) {
        // new_headers[i] -> new_headers[i - 1] (i.e. new_headers.last() is the tip)
        for (curr_header, next_header) in new_headers.iter().tuple_windows() {
            assert_eq!(curr_header.height() + 1, next_header.height());
            assert_eq!(*curr_header.hash(), next_header.header().prev_blockhash);
        }
        let new_height = match new_headers.first() {
            Some(entry) => {
                let height = entry.height();
                let expected_prev_blockhash = if height > 0 {
                    *self.headers[height - 1].hash()
                } else {
                    *DEFAULT_BLOCKHASH
                };
                assert_eq!(entry.header().prev_blockhash, expected_prev_blockhash);
                height
            }
            None => return,
        };
        debug!(
            "applying {} new headers from height {}",
            new_headers.len(),
            new_height
        );
        assert_eq!(new_height, self.headers.len());
        for new_header in new_headers {
            let height = new_header.height();
            assert_eq!(height, self.headers.len());
            self.tip = *new_header.hash();
            self.headers.push(new_header);
            self.heights.insert(self.tip, height);
        }
    }

    #[trace]
    pub fn header_by_blockhash(&self, blockhash: &BlockHash) -> Option<&HeaderEntry> {
        let height = self.heights.get(blockhash)?;
        let header = self.headers.get(*height)?;
        assert_eq!(header.hash(), blockhash);
        Some(header)
    }

    #[trace]
    pub fn header_by_height(&self, height: usize) -> Option<&HeaderEntry> {
        self.headers.get(height).map(|entry| {
            assert_eq!(entry.height(), height);
            entry
        })
    }

    pub fn equals(&self, other: &HeaderList) -> bool {
        self.headers.last() == other.headers.last()
    }

    pub fn tip(&self) -> &BlockHash {
        assert_eq!(
            self.tip,
            self.headers
                .last()
                .map(|h| *h.hash())
                .unwrap_or(*DEFAULT_BLOCKHASH)
        );
        &self.tip
    }

    pub fn len(&self) -> usize {
        self.headers.len()
    }

    /// Get the chain tip height. Panics if called on an empty HeaderList.
    pub fn best_height(&self) -> usize {
        self.len()
            .checked_sub(1)
            .expect("best_height() on empty HeaderList")
    }

    pub fn is_empty(&self) -> bool {
        self.headers.is_empty()
    }

    pub fn iter(&self) -> slice::Iter<'_, HeaderEntry> {
        self.headers.iter()
    }

    /// Get the Median Time Past
    pub fn get_mtp(&self, height: usize) -> u32 {
        // Use the timestamp as the mtp of the genesis block.
        // Matches bitcoind's behaviour: bitcoin-cli getblock `bitcoin-cli getblockhash 0` | jq '.time == .mediantime'
        if height == 0 {
            self.headers.get(0).unwrap().header.time
        } else if height > self.best_height() {
            0
        } else {
            let mut timestamps = (height.saturating_sub(MTP_SPAN - 1)..=height)
                .map(|p_height| self.headers.get(p_height).unwrap().header.time)
                .collect::<Vec<_>>();
            timestamps.sort_unstable();
            timestamps[timestamps.len() / 2]
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct BlockStatus {
    pub in_best_chain: bool,
    pub height: Option<usize>,
    pub next_best: Option<BlockHash>,
}

impl BlockStatus {
    pub fn confirmed(height: usize, next_best: Option<BlockHash>) -> BlockStatus {
        BlockStatus {
            in_best_chain: true,
            height: Some(height),
            next_best,
        }
    }

    pub fn orphaned() -> BlockStatus {
        BlockStatus {
            in_best_chain: false,
            height: None,
            next_best: None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BlockMeta {
    #[serde(alias = "nTx")]
    pub tx_count: u32,
    pub size: u32,
    pub weight: u32,
}

pub struct BlockHeaderMeta {
    pub header_entry: HeaderEntry,
    pub meta: BlockMeta,
    pub mtp: u32,
}

impl From<&BlockEntry> for BlockMeta {
    fn from(b: &BlockEntry) -> BlockMeta {
        let weight = b.block.weight();
        #[cfg(not(feature = "liquid"))] // rust-bitcoin has a wrapper Weight type
        let weight = weight.to_wu();

        BlockMeta {
            tx_count: b.block.txdata.len() as u32,
            // To retain DB compatibility, block weights are converted from the u64
            // representation used as of rust-bitcoin v0.30 back to a u32. This is OK
            // because u32::MAX is far above MAX_BLOCK_WEIGHT.
            weight: weight as u32,
            size: b.size,
        }
    }
}

impl BlockMeta {
    pub fn parse_getblock(val: ::serde_json::Value) -> Result<BlockMeta> {
        Ok(BlockMeta {
            tx_count: val
                .get("nTx")
                .chain_err(|| "missing nTx")?
                .as_f64()
                .chain_err(|| "nTx not a number")? as u32,
            size: val
                .get("size")
                .chain_err(|| "missing size")?
                .as_f64()
                .chain_err(|| "size not a number")? as u32,
            weight: val
                .get("weight")
                .chain_err(|| "missing weight")?
                .as_f64()
                .chain_err(|| "weight not a number")? as u32,
        })
    }
}
