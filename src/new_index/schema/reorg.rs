use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::sync::Arc;

use crate::chain::{Block, BlockHash, OutPoint, Transaction, TxOut, Txid};
use crate::daemon::Daemon;
use crate::errors::*;
use crate::new_index::db::{DBFlush, DBRow};
use crate::new_index::fetch::{start_fetcher, BlockEntry, FetchFrom};
use crate::util::{bincode, full_hash, has_prevout, HeaderEntry};

use super::{
    deserialize, get_previous_txos, index_blocks, load_blockhashes, serialize, BlockRow,
    GetAmountVal, Indexer, Store, TxConfRow, TxEdgeRow, TxHistoryKey, TxRow,
};

#[cfg(feature = "liquid")]
use crate::elements::{asset, ebcompact::SizeMethod, ebcompact::TxidCompat};

// Logged with the error chain when cleanup fails (error first, then warn on retries);
// operators alert on it.
pub const STALE_CLEANUP_FAILED: &str =
    "stale block history cleanup failed, indexing cannot advance";

const SCAN_PROGRESS_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

// history_db key: "rewrite C/S/i rows of indexed active blocks from this height" (u32 LE).
// Written before cleanup deletes anything and cleared with the rewrite, so a crash between
// the two cannot lose a shared row once the stale block's done-marker is gone.
const PENDING_REWRITE_KEY: &[u8] = b"R";
const UNDO_PREFIX: &[u8] = b"u";
const REWRITE_BATCH_BYTES: usize = 4 * 1024 * 1024;

pub(super) fn undo_marker_key(hash: &BlockHash) -> Vec<u8> {
    [UNDO_PREFIX, &hash[..]].concat()
}

// A spending input whose prevout was missing from txstore when its block was undone.
// Only its spending script-history row is still outstanding; other unshared history
// rows of the block were already deleted.
struct PendingSpend {
    block: BlockHash,
    height: u32,
    txid: Txid,
    vin: u32,
}

// Undo state carried across batches. Memory scales with unresolved inputs, not with the
// blocks or outputs seen: whole blocks are never retained.
#[derive(Default)]
struct DeferredUndo {
    // Unresolved inputs by the outpoint they spend.
    pending: HashMap<OutPoint, Vec<PendingSpend>>,
    // Remaining inputs and the durable undo marker, distinct from a completed index.
    blocks: HashMap<BlockHash, (usize, DBRow)>,
}

impl DeferredUndo {
    fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

// H/I keys shared with active blocks must survive deletion. C/S/i have only one
// value per key and are restored separately, starting at the actual fork boundary.
pub(super) struct ActiveChainView<'a> {
    store: Arc<Store>,
    daemon: Option<&'a Daemon>,
    saved_prefix_len: usize,
    new_blocks: HashMap<usize, BlockHash>,
    transactions: HashMap<BlockHash, HashSet<Txid>>,
}

impl<'a> ActiveChainView<'a> {
    pub(super) fn new(
        store: Arc<Store>,
        headers: &[HeaderEntry],
        reorged_since: Option<usize>,
        daemon: Option<&'a Daemon>,
    ) -> Self {
        let saved_prefix_len = reorged_since.unwrap_or_else(|| store.headers().len());
        let new_blocks = headers.iter().map(|h| (h.height(), *h.hash())).collect();
        Self {
            store,
            daemon,
            saved_prefix_len,
            new_blocks,
            transactions: HashMap::new(),
        }
    }

    // The indexed active block at `height`, if any.
    fn active_block_at(&self, height: usize) -> Option<BlockHash> {
        self.new_blocks
            .get(&height)
            .copied()
            .filter(|hash| {
                self.store
                    .indexed_blockhashes
                    .read()
                    .unwrap()
                    .contains(hash)
            })
            .or_else(|| {
                if height < self.saved_prefix_len {
                    self.store
                        .headers()
                        .header_by_height(height)
                        .map(|h| *h.hash())
                } else {
                    None
                }
            })
    }

    fn contains_transaction(&mut self, height: usize, txid: Txid) -> Result<bool> {
        let hash = match self.active_block_at(height) {
            Some(hash) => hash,
            None => return Ok(false),
        };
        if !self.transactions.contains_key(&hash) {
            let txids: Vec<Txid> = match self
                .store
                .txstore_db
                .try_get(&BlockRow::txids_key(full_hash(&hash[..])))?
            {
                Some(raw) => bincode::deserialize_little(&raw)
                    .chain_err(|| "invalid active block transaction list during cleanup")?,
                None => {
                    let daemon = self
                        .daemon
                        .chain_err(|| "missing active block transaction list during cleanup")?;
                    let block = daemon.getblock_raw(&hash, 1)?;
                    serde_json::from_value(block["tx"].clone())
                        .chain_err(|| "invalid active block transaction list from daemon")?
                }
            };
            self.transactions.insert(hash, txids.into_iter().collect());
        }
        Ok(self.transactions[&hash].contains(&txid))
    }

    // Only H and I keys include the height, so only they can be shared with an active
    // block by key alone. C, S and i rows are rebuilt by `finish_rewrite`.
    fn owns_history_row(&mut self, row: &DBRow) -> Result<bool> {
        match row.key.first() {
            Some(b'H' | b'I') => {
                let key: TxHistoryKey = bincode::deserialize_big(&row.key)
                    .chain_err(|| "invalid history key during cleanup")?;
                self.contains_transaction(key.confirmed_height as usize, key.txinfo.get_txid())
            }
            _ => Ok(false),
        }
    }

    fn top_height(&self) -> usize {
        self.new_blocks
            .keys()
            .copied()
            .max()
            .unwrap_or(0)
            .max(self.saved_prefix_len.saturating_sub(1))
    }

    fn matches_header(&self, height: usize, hash: &BlockHash) -> bool {
        self.new_blocks.get(&height) == Some(hash)
            || (height < self.saved_prefix_len
                && self
                    .store
                    .headers()
                    .header_by_height(height)
                    .is_some_and(|h| h.hash() == hash))
    }

    fn build_single_value_rows(
        &self,
        height: usize,
        hash: &BlockHash,
        iconfig: &super::IndexerConfig,
    ) -> Result<Vec<DBRow>> {
        let mut rows = Vec::new();
        let block = self.load_block(hash)?;
        let height = height as u32;
        for tx in &block.txdata {
            let txid = tx.compute_txid();
            let txid_hash = full_hash(&txid[..]);
            rows.push(TxConfRow::new(txid_hash, height).into_row());
            for (vin, input) in tx.input.iter().enumerate() {
                if !has_prevout(input) {
                    continue;
                }
                let funding = &input.previous_output;
                let edge = TxEdgeRow::new(
                    full_hash(&funding.txid[..]),
                    funding.vout,
                    txid_hash,
                    vin as u32,
                    height,
                );
                rows.push(edge.into_row());
            }
            #[cfg(feature = "liquid")]
            {
                let mut asset_rows = Vec::new();
                asset::index_confirmed_tx_assets(
                    tx,
                    height,
                    iconfig.network,
                    iconfig.parent_network,
                    &mut asset_rows,
                );
                rows.extend(asset_rows.into_iter().filter(|row| row.key[0] == b'i'));
            }
        }
        #[cfg(not(feature = "liquid"))]
        let _ = iconfig;
        Ok(rows)
    }

    fn load_block(&self, hash: &BlockHash) -> Result<Block> {
        let header = self
            .store
            .txstore_db
            .try_get(&[b"B", &hash[..]].concat())?
            .map(|raw| deserialize(&raw).chain_err(|| "invalid active block header"))
            .transpose()?;
        if let Some(header) = header {
            if let Some((_, txdata)) = stored_transactions(&self.store, hash)? {
                return Ok(Block { header, txdata });
            }
        }
        self.fetch_block(hash)
    }

    // Light mode keeps no transactions in txstore.
    fn fetch_block(&self, hash: &BlockHash) -> Result<Block> {
        let daemon = self
            .daemon
            .chain_err(|| "missing active block transactions during cleanup")?;
        let mut blocks = daemon.getblocks(&[*hash])?;
        blocks
            .pop()
            .chain_err(|| "daemon returned no block during cleanup")
    }
}

impl Store {
    pub(super) fn uncommitted_blocks(&self) -> HashSet<BlockHash> {
        let headers = self.headers();
        self.indexed_blockhashes
            .read()
            .unwrap()
            .iter()
            .filter(|hash| headers.header_by_blockhash(hash).is_none())
            .copied()
            .chain(load_blockhashes(&self.history_db, UNDO_PREFIX))
            .collect()
    }

    fn pop_reorged_headers(&self, since: usize) -> Result<Vec<HeaderEntry>> {
        let (removed, ancestor) = {
            let mut headers = self.indexed_headers.write().unwrap();
            ensure!(since <= headers.len(), "invalid reorg boundary {}", since);
            let removed = headers.pop(since);
            (removed, *headers.tip())
        };
        // The durable tip must stay below every block whose history can be deleted.
        // Sync outside the lock so readers are not stalled on disk I/O.
        self.txstore_db.put_sync(b"t", &serialize(&ancestor));
        Ok(removed)
    }
}

impl Indexer {
    // Track only blocks beyond the gap; a successful run keeps the set empty. After a
    // crash before the first tip commit, `Indexer::open` seeds it with every indexed
    // block until the next commit.
    pub(super) fn remember_uncommitted_headers(&mut self, headers: &[HeaderEntry]) {
        let indexed = self.store.indexed_blockhashes.read().unwrap();
        self.uncommitted_blocks.extend(
            headers
                .iter()
                .filter(|header| indexed.contains(header.hash()))
                .map(|header| *header.hash()),
        );
    }

    pub(super) fn undo_reorged_blocks(
        &mut self,
        daemon: &Daemon,
        reorged_since: usize,
        chain_tip_height: usize,
        active_chain_view: &mut ActiveChainView<'_>,
    ) -> Result<()> {
        // Mark the rewrite pending before moving the saved tip, so a crash in between
        // still leaves a marker that recovery (and the rollback check) can see.
        self.begin_rewrite(reorged_since)?;
        // Hide old heights so queries cannot attach their history to replacement blocks.
        let mut reorged_headers = self.store.pop_reorged_headers(reorged_since)?;

        info!(
            "processing reorg of depth {} since height {}",
            reorged_headers.len(),
            reorged_since,
        );

        reorged_headers.reverse();
        // Retain popped blocks so failed cleanup can be retried.
        self.uncommitted_blocks
            .extend(reorged_headers.iter().map(|h| *h.hash()));

        self.undo_blocks(daemon, reorged_headers, chain_tip_height, active_chain_view)
    }

    // Blocks above an import gap are missing from saved headers, so they are found
    // through their done-markers rather than a header-chain diff.
    pub(super) fn undo_uncommitted_blocks(
        &mut self,
        daemon: &Daemon,
        stale: Vec<BlockHash>,
        tip_height: usize,
        active_chain_view: &mut ActiveChainView<'_>,
    ) -> Result<()> {
        let mut headers = Vec::with_capacity(stale.len());
        for hash in stale {
            let height = self.uncommitted_height(daemon, &hash)?;
            let raw = self.store.txstore_db.try_get(&[b"B", &hash[..]].concat())?;
            // History can reach disk before txstore during WAL-disabled import.
            let header = match raw {
                Some(raw) => deserialize(&raw).chain_err(|| "invalid uncommitted block header")?,
                None => daemon.getblockheader(&hash)?,
            };
            headers.push(HeaderEntry::new(height, hash, header));
        }
        headers.sort_unstable_by_key(|header| std::cmp::Reverse(header.height()));
        let mut forks = HashMap::new();
        let mut boundary = None;
        for header in &headers {
            let height = self.fork_height(daemon, header, active_chain_view, &mut forks)?;
            boundary = Some(boundary.map_or(height, |previous: usize| previous.min(height)));
        }
        if let Some(height) = boundary {
            self.begin_rewrite(height)?;
        }
        self.undo_blocks(daemon, headers, tip_height, active_chain_view)
    }

    fn fork_height(
        &self,
        daemon: &Daemon,
        header: &HeaderEntry,
        active: &ActiveChainView<'_>,
        forks: &mut HashMap<BlockHash, usize>,
    ) -> Result<usize> {
        let mut height = header.height();
        let mut hash = *header.hash();
        #[allow(clippy::clone_on_copy)]
        let mut current = header.header().clone();
        let mut visited = Vec::new();
        let boundary = loop {
            if active.matches_header(height, &hash) {
                break height + 1;
            }
            if let Some(boundary) = forks.get(&hash) {
                break *boundary;
            }
            ensure!(height > 0, "stale block does not share the active genesis");
            visited.push(hash);
            hash = current.prev_blockhash;
            height -= 1;
            if active.matches_header(height, &hash) {
                break height + 1;
            }
            if let Some(boundary) = forks.get(&hash) {
                break *boundary;
            }
            current = match self.store.txstore_db.try_get(&[b"B", &hash[..]].concat())? {
                Some(raw) => deserialize(&raw).chain_err(|| "invalid stale ancestor header")?,
                None => daemon.getblockheader(&hash)?,
            };
        };
        forks.extend(visited.into_iter().map(|hash| (hash, boundary)));
        Ok(boundary)
    }

    pub(super) fn stale_uncommitted(&self, new_headers: &[HeaderEntry]) -> Vec<BlockHash> {
        if self.uncommitted_blocks.is_empty() {
            return vec![];
        }
        let active: HashSet<_> = new_headers.iter().map(|h| h.hash()).collect();
        self.uncommitted_blocks
            .iter()
            .filter(|hash| !active.contains(hash))
            .copied()
            .collect()
    }

    // Rebuild stale blocks from txstore where possible: it keeps reorged blocks by
    // design, so cleanup does not depend on bitcoind still serving them.
    fn undo_blocks(
        &mut self,
        daemon: &Daemon,
        headers: Vec<HeaderEntry>,
        tip_height: usize,
        active_chain_view: &mut ActiveChainView<'_>,
    ) -> Result<()> {
        // Load stored blocks one batch at a time; a long gap can leave thousands of them.
        let batch_size = self.iconfig.block_batch_size.max(1);
        let mut deferred = DeferredUndo::default();
        let mut batch = Vec::with_capacity(batch_size);
        let mut remote = Vec::new();
        for header in headers {
            match self.stored_block(&header)? {
                Some(block) => batch.push(block),
                None => remote.push(header),
            }
            if batch.len() == batch_size {
                self.undo_index_or_defer(&batch, &mut deferred, active_chain_view)?;
                batch.clear();
            }
        }
        if !batch.is_empty() {
            self.undo_index_or_defer(&batch, &mut deferred, active_chain_view)?;
        }
        if !remote.is_empty() {
            start_fetcher(
                FetchFrom::Bitcoind,
                daemon,
                remote,
                self.iconfig.block_batch_size,
                tip_height,
            )?
            .map(|blocks| self.undo_index_or_defer(&blocks, &mut deferred, active_chain_view))?;
        }
        self.finish_deferred_undo(deferred, active_chain_view)
    }

    fn stored_block(&self, header: &HeaderEntry) -> Result<Option<BlockEntry>> {
        let (txids, txdata) = match stored_transactions(&self.store, header.hash())? {
            Some(stored) => stored,
            None => return Ok(None),
        };
        // Elements headers are not Copy.
        #[allow(clippy::clone_on_copy)]
        let block = Block {
            header: header.header().clone(),
            txdata,
        };
        Ok(Some(BlockEntry {
            size: block.total_size() as u32,
            entry: header.clone(),
            txids,
            block,
        }))
    }

    fn uncommitted_height(&self, daemon: &Daemon, hash: &BlockHash) -> Result<usize> {
        let marker = match self.store.history_db.try_get(&undo_marker_key(hash))? {
            Some(marker) => marker,
            None => self
                .store
                .history_db
                .try_get(&BlockRow::new_done(full_hash(&hash[..])).into_row().key)?
                .chain_err(|| format!("missing cleanup marker for block {}", hash))?,
        };
        match done_marker_height(&marker)
            .chain_err(|| format!("invalid done-marker for indexed block {}", hash))?
        {
            Some(height) => Ok(height),
            // Legacy markers lack heights; Core keeps heights for orphaned headers.
            None => daemon.getblockheight(hash),
        }
    }

    // Same sequence as a production cleanup, for tests that supply their own blocks.
    // Keep txstore data so orphaned transactions remain available. Deleting a key does
    // not restore an earlier value, so shared rows are kept or rewritten (see
    // `ActiveChainView`).
    #[cfg(test)]
    fn undo_index(
        &mut self,
        blocks: &[BlockEntry],
        active_chain_view: &mut ActiveChainView<'_>,
    ) -> Result<()> {
        if let Some(lowest) = blocks.iter().map(|b| b.entry.height()).min() {
            self.begin_rewrite(lowest)?;
        }
        let mut deferred = DeferredUndo::default();
        self.undo_index_or_defer(blocks, &mut deferred, active_chain_view)?;
        self.finish_deferred_undo(deferred, active_chain_view)?;
        self.finish_rewrite(active_chain_view)
    }

    // History writes can survive a crash that loses txstore prevouts. Those are usually
    // outputs of other stale blocks in this undo set, which undo (running top-down) sees
    // after the spender. So each batch deletes every row it can, records inputs whose
    // prevout is still missing, and resolves earlier records from its own outputs.
    fn undo_index_or_defer(
        &mut self,
        blocks: &[BlockEntry],
        deferred: &mut DeferredUndo,
        active_chain_view: &mut ActiveChainView<'_>,
    ) -> Result<()> {
        let mut rows = self.resolve_from_outputs(blocks, deferred);
        let (prevouts, missing) = {
            let _timer = self.start_timer("index_lookup");
            self.lookup_undo_prevouts(blocks)?
        };
        let mut indexed = {
            let _timer = self.start_timer("index_process");
            index_blocks(blocks, &prevouts, &self.iconfig)
        };
        let mut pending_markers = Vec::new();
        if !missing.is_empty() {
            let pending_keys = self.record_pending(blocks, &missing, deferred);
            indexed.retain(|row| !pending_keys.contains(&row.key));
        }
        for block in blocks {
            let hash = block.entry.hash();
            let marker = DBRow {
                key: undo_marker_key(hash),
                value: (block.entry.height() as u32).to_le_bytes().to_vec(),
            };
            if let Some((_, held)) = deferred.blocks.get_mut(hash) {
                *held = DBRow {
                    key: marker.key.clone(),
                    value: marker.value.clone(),
                };
                pending_markers.push(marker);
            } else {
                rows.push(marker);
            }
        }
        rows.append(&mut indexed);
        let mut blocks_released = Vec::new();
        rows.extend(self.release_resolved(deferred, &mut blocks_released));
        self.delete_stale_rows(rows, pending_markers, active_chain_view)?;
        {
            let mut indexed = self.store.indexed_blockhashes.write().unwrap();
            for block in blocks {
                indexed.remove(block.entry.hash());
            }
        }
        self.forget_indexed(
            blocks
                .iter()
                .map(|b| *b.entry.hash())
                .filter(|hash| !deferred.blocks.contains_key(hash))
                .chain(blocks_released),
        );
        Ok(())
    }

    // Prevouts from txstore, plus the outpoints txstore no longer has. Missing ones map to
    // a placeholder: it only feeds spending script-history rows, which are then dropped.
    // Only a missing row counts as missing; I/O and decoding errors propagate.
    fn lookup_undo_prevouts(
        &self,
        blocks: &[BlockEntry],
    ) -> Result<(HashMap<OutPoint, TxOut>, HashSet<OutPoint>)> {
        let outpoints = get_previous_txos(blocks);
        let keys: Vec<_> = outpoints.iter().map(super::TxOutRow::key).collect();
        let mut prevouts = HashMap::new();
        let mut missing = HashSet::new();
        for (raw, outpoint) in self
            .store
            .txstore_db
            .multi_get(keys)
            .into_iter()
            .zip(outpoints)
        {
            match raw.chain_err(|| "failed looking up prevouts for history undo")? {
                Some(raw) => {
                    let output =
                        deserialize(&raw).chain_err(|| "invalid prevout during history undo")?;
                    prevouts.insert(outpoint, output);
                }
                None => {
                    prevouts.insert(outpoint, placeholder_txout());
                    missing.insert(outpoint);
                }
            }
        }
        if !missing.is_empty() {
            for block in blocks {
                for (tx, txid) in block.block.txdata.iter().zip(&block.txids) {
                    for (vout, output) in tx.output.iter().enumerate() {
                        let outpoint = OutPoint {
                            txid: *txid,
                            vout: vout as u32,
                        };
                        if missing.remove(&outpoint) {
                            prevouts.insert(outpoint, output.clone());
                        }
                    }
                }
            }
        }
        if let Some(outpoint) = missing.iter().next() {
            warn!(
                "{} prevouts missing from txstore during history undo, e.g. {}",
                missing.len(),
                outpoint
            );
        }
        Ok((prevouts, missing))
    }

    // Records inputs spending `missing` outpoints and returns the keys of their placeholder
    // spending rows, which must not be deleted as if they were real.
    fn record_pending(
        &self,
        blocks: &[BlockEntry],
        missing: &HashSet<OutPoint>,
        deferred: &mut DeferredUndo,
    ) -> HashSet<Vec<u8>> {
        let mut placeholder_keys = HashSet::new();
        let placeholder = placeholder_txout();
        for block in blocks {
            let height = block.entry.height() as u32;
            let hash = *block.entry.hash();
            for (tx, txid) in block.block.txdata.iter().zip(&block.txids) {
                for (vin, input) in tx.input.iter().enumerate() {
                    if !has_prevout(input) || !missing.contains(&input.previous_output) {
                        continue;
                    }
                    let spend = spending_history_row(
                        &placeholder,
                        height,
                        *txid,
                        vin as u32,
                        &input.previous_output,
                    );
                    placeholder_keys.insert(spend.key);
                    deferred
                        .pending
                        .entry(input.previous_output)
                        .or_default()
                        .push(PendingSpend {
                            block: hash,
                            height,
                            txid: *txid,
                            vin: vin as u32,
                        });
                    let empty = DBRow {
                        key: vec![],
                        value: vec![],
                    };
                    deferred.blocks.entry(hash).or_insert((0, empty)).0 += 1;
                }
            }
        }
        placeholder_keys
    }

    // Spending rows for earlier pending inputs whose prevout is an output of these blocks.
    fn resolve_from_outputs(
        &self,
        blocks: &[BlockEntry],
        deferred: &mut DeferredUndo,
    ) -> Vec<DBRow> {
        let mut rows = Vec::new();
        if deferred.is_empty() {
            return rows;
        }
        for block in blocks {
            for (tx, txid) in block.block.txdata.iter().zip(&block.txids) {
                for (vout, output) in tx.output.iter().enumerate() {
                    let outpoint = OutPoint {
                        txid: *txid,
                        vout: vout as u32,
                    };
                    for spend in deferred.pending.remove(&outpoint).unwrap_or_default() {
                        rows.push(spending_history_row(
                            output,
                            spend.height,
                            spend.txid,
                            spend.vin,
                            &outpoint,
                        ));
                        deferred.blocks.get_mut(&spend.block).unwrap().0 -= 1;
                    }
                }
            }
        }
        rows
    }

    // Undo markers of blocks whose inputs are now all resolved.
    fn release_resolved(
        &self,
        deferred: &mut DeferredUndo,
        released: &mut Vec<BlockHash>,
    ) -> Vec<DBRow> {
        let done: Vec<BlockHash> = deferred
            .blocks
            .iter()
            .filter(|(_, (remaining, marker))| *remaining == 0 && !marker.key.is_empty())
            .map(|(hash, _)| *hash)
            .collect();
        done.into_iter()
            .map(|hash| {
                released.push(hash);
                deferred.blocks.remove(&hash).unwrap().1
            })
            .collect()
    }

    fn finish_deferred_undo(
        &mut self,
        deferred: DeferredUndo,
        active_chain_view: &mut ActiveChainView<'_>,
    ) -> Result<()> {
        if deferred.is_empty() {
            return Ok(());
        }
        self.undo_by_history_scan(deferred, active_chain_view)
    }

    // Last resort: a spending row cannot be rebuilt without its prevout script, so find
    // the remaining ones by scanning all script history. Cost is proportional to the
    // database, not the damage.
    fn undo_by_history_scan(
        &mut self,
        deferred: DeferredUndo,
        active_chain_view: &mut ActiveChainView<'_>,
    ) -> Result<()> {
        let spends: HashSet<(u32, Txid, u32)> = deferred
            .pending
            .values()
            .flatten()
            .map(|spend| (spend.height, spend.txid, spend.vin))
            .collect();
        let started = std::time::Instant::now();
        warn!(
            "scanning script history to undo {} spends without txstore prevouts",
            spends.len()
        );
        let mut rows_to_remove = Vec::new();
        {
            let _timer = self.start_timer("undo_history_scan");
            let mut scanned: u64 = 0;
            let mut last_progress = std::time::Instant::now();
            for row in self.store.history_db.iter_scan(b"H") {
                let key: TxHistoryKey = bincode::deserialize_big(&row.key)
                    .chain_err(|| "invalid script history during crash recovery")?;
                if let super::TxHistoryInfo::Spending(ref info) = key.txinfo {
                    let txid = key.txinfo.get_txid();
                    if spends.contains(&(key.confirmed_height, txid, info.vin)) {
                        rows_to_remove.push(row);
                    }
                }
                scanned += 1;
                if last_progress.elapsed() >= SCAN_PROGRESS_INTERVAL {
                    last_progress = std::time::Instant::now();
                    info!(
                        "script history scan for crash recovery: {} rows scanned, {:.0?} elapsed",
                        scanned,
                        started.elapsed()
                    );
                }
            }
        }
        let blocks: Vec<BlockHash> = deferred.blocks.keys().copied().collect();
        rows_to_remove.extend(deferred.blocks.into_values().map(|(_, marker)| marker));
        self.delete_stale_rows(rows_to_remove, vec![], active_chain_view)?;
        self.forget_indexed(blocks.into_iter());
        info!(
            "script history scan for crash recovery complete in {:.1?}",
            started.elapsed()
        );
        Ok(())
    }

    pub(super) fn begin_rewrite(&self, from_height: usize) -> Result<()> {
        let height = match pending_rewrite(&self.store)? {
            Some(pending) => pending.min(from_height),
            None => from_height,
        };
        self.store
            .history_db
            .put_sync(PENDING_REWRITE_KEY, &(height as u32).to_le_bytes());
        Ok(())
    }

    // Keep R until all batches are durable; retries rebuild the suffix idempotently.
    pub(super) fn finish_rewrite(&self, active_chain_view: &ActiveChainView<'_>) -> Result<()> {
        let from_height = match pending_rewrite(&self.store)? {
            Some(height) => height,
            None => return Ok(()),
        };
        let mut batch = rocksdb::WriteBatch::default();
        let mut blocks = 0;
        for height in from_height..=active_chain_view.top_height() {
            if let Some(hash) = active_chain_view.active_block_at(height) {
                for row in
                    active_chain_view.build_single_value_rows(height, &hash, &self.iconfig)?
                {
                    batch.put(row.key, row.value);
                    if batch.size_in_bytes() >= REWRITE_BATCH_BYTES {
                        self.store
                            .history_db
                            .write_batch(std::mem::take(&mut batch), DBFlush::Enable);
                    }
                }
                blocks += 1;
                if blocks % self.iconfig.block_batch_size.max(1) == 0 {
                    self.store
                        .history_db
                        .write_batch(std::mem::take(&mut batch), DBFlush::Enable);
                }
            }
        }
        batch.delete(PENDING_REWRITE_KEY);
        self.store.history_db.write_batch(batch, DBFlush::Enable);
        Ok(())
    }

    fn delete_stale_rows(
        &self,
        rows: Vec<DBRow>,
        pending_markers: Vec<DBRow>,
        active_chain_view: &mut ActiveChainView<'_>,
    ) -> Result<()> {
        let mut batch = rocksdb::WriteBatch::default();
        let mut cache_keys = HashSet::new();
        for row in rows {
            if active_chain_view.owns_history_row(&row)? {
                continue;
            }
            match row.key.first() {
                Some(b'H') => {
                    cache_keys.insert(super::StatsCacheRow::key(&row.key[1..33]));
                    cache_keys.insert(super::UtxoCacheRow::key(&row.key[1..33]));
                }
                Some(b'I') => {
                    cache_keys.insert([b"z", &row.key[1..33]].concat());
                }
                _ => {}
            }
            batch.delete(row.key);
        }
        for marker in pending_markers {
            batch.put(marker.key, marker.value);
        }
        // Queries hold the read lock through cache writes. Invalidate durably before
        // deleting history, so neither concurrent queries nor crashes retain old totals.
        let _cache_guard = self.store.history_cache_lock.write().unwrap();
        if !cache_keys.is_empty() {
            let mut cache_batch = rocksdb::WriteBatch::default();
            for key in cache_keys {
                cache_batch.delete(key);
            }
            self.store
                .cache_db
                .write_batch(cache_batch, DBFlush::Enable);
        }
        self.store.history_db.write_batch(batch, DBFlush::Enable);
        Ok(())
    }

    fn forget_indexed(&mut self, blockhashes: impl Iterator<Item = BlockHash>) {
        let mut indexed_blockhashes = self.store.indexed_blockhashes.write().unwrap();
        for hash in blockhashes {
            indexed_blockhashes.remove(&hash);
            self.uncommitted_blocks.remove(&hash);
        }
    }
}

// Stands in for a prevout txstore lost; only placeholder spending rows ever see it.
fn placeholder_txout() -> TxOut {
    #[cfg(not(feature = "liquid"))]
    return TxOut {
        value: bitcoin::Amount::ZERO,
        script_pubkey: Default::default(),
    };
    #[cfg(feature = "liquid")]
    return TxOut::default();
}

fn spending_history_row(
    prevout: &TxOut,
    height: u32,
    txid: Txid,
    vin: u32,
    spent: &OutPoint,
) -> DBRow {
    super::TxHistoryRow::new(
        &prevout.script_pubkey,
        height,
        super::TxHistoryInfo::Spending(super::SpendingInfo {
            txid: full_hash(&txid[..]),
            vin,
            prev_txid: full_hash(&spent.txid[..]),
            prev_vout: spent.vout,
            value: prevout.value.amount_value(),
        }),
    )
    .into_row()
}

fn stored_transactions(
    store: &Store,
    hash: &BlockHash,
) -> Result<Option<(Vec<Txid>, Vec<Transaction>)>> {
    let txids: Vec<Txid> = match store
        .txstore_db
        .try_get(&BlockRow::txids_key(full_hash(&hash[..])))?
    {
        Some(raw) => bincode::deserialize_little(&raw)
            .chain_err(|| "invalid stored block transaction list")?,
        None => return Ok(None),
    };
    let mut txdata = Vec::with_capacity(txids.len());
    for raw in store
        .txstore_db
        .multi_get(txids.iter().map(|txid| TxRow::key(&txid[..])))
    {
        match raw? {
            Some(raw) => txdata.push(deserialize(&raw).chain_err(|| "invalid stored transaction")?),
            None => return Ok(None),
        }
    }
    Ok(Some((txids, txdata)))
}

pub(super) fn pending_rewrite(store: &Store) -> Result<Option<usize>> {
    match store.history_db.try_get(PENDING_REWRITE_KEY)? {
        Some(raw) => {
            let height: [u8; 4] = raw
                .as_slice()
                .try_into()
                .map_err(|_| format!("invalid pending rewrite marker length {}", raw.len()))?;
            Ok(Some(u32::from_le_bytes(height) as usize))
        }
        None => Ok(None),
    }
}

fn done_marker_height(marker: &[u8]) -> Result<Option<usize>> {
    if marker.is_empty() {
        return Ok(None);
    }
    let height: [u8; 4] = marker
        .try_into()
        .map_err(|_| format!("unexpected done-marker length {}", marker.len()))?;
    Ok(Some(u32::from_le_bytes(height) as usize))
}

#[cfg(test)]
mod tests {
    use super::super::{tests::test_header, IndexerConfig};
    use super::*;
    use crate::chain::{Network, OutPoint, Script, Transaction, TxOut};
    use crate::metrics::{HistogramOpts, MetricOpts, Metrics};
    use crate::new_index::db::DBFlush;
    use bitcoin::hashes::Hash;
    use std::sync::Arc;

    #[cfg(feature = "liquid")]
    use super::super::{FundingInfo, TxHistoryInfo, TxHistoryRow};
    #[cfg(feature = "liquid")]
    use elements::{confidential, AssetId};

    fn test_config() -> IndexerConfig {
        IndexerConfig {
            light_mode: false,
            address_search: false,
            index_unspendables: false,
            #[cfg(not(feature = "liquid"))]
            network: Network::Regtest,
            #[cfg(feature = "liquid")]
            network: Network::LiquidRegtest,
            block_batch_size: 1,
            #[cfg(feature = "liquid")]
            parent_network: bitcoin::Network::Regtest,
        }
    }

    fn test_transaction(prevout: OutPoint, script: u8) -> Transaction {
        #[cfg(not(feature = "liquid"))]
        {
            use bitcoin::{
                absolute::LockTime, transaction::Version, Amount, Sequence, TxIn, Witness,
            };
            Transaction {
                version: Version::TWO,
                lock_time: LockTime::ZERO,
                input: vec![TxIn {
                    previous_output: prevout,
                    script_sig: Default::default(),
                    sequence: Sequence::MAX,
                    witness: Witness::new(),
                }],
                output: vec![TxOut {
                    value: Amount::from_sat(50),
                    script_pubkey: Script::from_bytes(vec![script]),
                }],
            }
        }
        #[cfg(feature = "liquid")]
        {
            use elements::{AssetIssuance, LockTime, TxIn, TxOutWitness};
            Transaction {
                version: 2,
                lock_time: LockTime::ZERO,
                input: vec![TxIn {
                    previous_output: prevout,
                    asset_issuance: if prevout.is_null() {
                        AssetIssuance::null()
                    } else {
                        AssetIssuance {
                            amount: confidential::Value::Explicit(100),
                            ..AssetIssuance::null()
                        }
                    },
                    ..TxIn::default()
                }],
                output: vec![TxOut {
                    asset: confidential::Asset::Explicit(AssetId::LIQUID_BTC),
                    value: confidential::Value::Explicit(50),
                    nonce: confidential::Nonce::Null,
                    script_pubkey: Script::from(vec![script]),
                    witness: TxOutWitness::default(),
                }],
            }
        }
    }

    fn test_block(
        height: usize,
        prev: BlockHash,
        tag: u32,
        txdata: Vec<Transaction>,
    ) -> BlockEntry {
        let mut header = test_header(prev);
        header.time = tag;
        let block = crate::chain::Block { header, txdata };
        BlockEntry {
            entry: HeaderEntry::new(height, block.block_hash(), block.header.clone()),
            txids: block
                .txdata
                .iter()
                .map(|tx| {
                    #[cfg(not(feature = "liquid"))]
                    {
                        tx.compute_txid()
                    }
                    #[cfg(feature = "liquid")]
                    {
                        tx.txid()
                    }
                })
                .collect(),
            block,
            size: 0,
        }
    }

    fn history_rows(store: &Store) -> HashMap<Vec<u8>, Vec<u8>> {
        store
            .history_db
            .iter_scan(b"")
            .map(|row| (row.key, row.value))
            .collect()
    }

    fn write_test_history(
        indexer: &Indexer,
        blocks: &[BlockEntry],
        prevouts: &HashMap<OutPoint, TxOut>,
    ) {
        indexer.add(blocks);
        indexer.store.history_db.write_rows(
            index_blocks(blocks, prevouts, &indexer.iconfig),
            DBFlush::Enable,
        );
        indexer
            .store
            .indexed_blockhashes
            .write()
            .unwrap()
            .extend(blocks.iter().map(|b| *b.entry.hash()));
    }

    fn test_branches(
        common: &BlockEntry,
        active_height: usize,
    ) -> (BlockEntry, Vec<BlockEntry>, HashMap<OutPoint, TxOut>) {
        let prevouts: Vec<_> = (1..=3)
            .map(|i| OutPoint {
                txid: Txid::from_byte_array([i; 32]),
                vout: 0,
            })
            .collect();
        let shared = test_transaction(prevouts[0], 0x51);
        let previous_outputs = prevouts
            .iter()
            .map(|p| (*p, shared.output[0].clone()))
            .collect();
        let stale = test_block(
            1,
            *common.entry.hash(),
            1,
            vec![
                shared.clone(),
                test_transaction(prevouts[1], 0x52),
                test_transaction(prevouts[2], 0x54),
            ],
        );
        let mut active = Vec::new();
        let mut prev = *common.entry.hash();
        for height in 1..=active_height {
            let txs = if height == active_height {
                vec![shared.clone(), test_transaction(prevouts[1], 0x53)]
            } else {
                vec![]
            };
            let entry = test_block(height, prev, height as u32 + 10, txs);
            prev = *entry.entry.hash();
            active.push(entry);
        }
        (stale, active, previous_outputs)
    }

    #[test]
    fn legacy_cleanup_preserves_active_rows() {
        for (active_height, missing_prevouts, beyond_gap) in [
            (1, false, false),
            (1, true, false),
            (2, false, false),
            (2, true, false),
            (2, true, true),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let store = Arc::new(Store::open_test(dir.path()));
            let mut indexer = test_indexer(&store, test_config());
            let common = test_block(0, BlockHash::all_zeros(), 0, vec![]);
            indexer.add(&[common.clone()]);
            indexer.index(&[common.clone()]);
            store
                .advance_synced_tip(vec![common.entry.clone()])
                .unwrap();

            let (stale, active, previous_outputs) = test_branches(&common, active_height);
            indexer.add(&[stale.clone()]);
            indexer.add(&active);
            let active_rows = index_blocks(&active, &previous_outputs, &indexer.iconfig);
            let mut expected = history_rows(&store);
            expected.extend(
                active_rows
                    .iter()
                    .map(|row| (row.key.clone(), row.value.clone())),
            );
            let stale_rows = index_blocks(&[stale.clone()], &previous_outputs, &indexer.iconfig);
            store.history_db.write_rows(stale_rows, DBFlush::Enable);
            store.history_db.write_rows(active_rows, DBFlush::Enable);
            store.history_db.put_sync(
                &BlockRow::new_done(full_hash(&stale.entry.hash()[..]))
                    .into_row()
                    .key,
                &[],
            );
            store.indexed_blockhashes.write().unwrap().extend(
                std::iter::once(*stale.entry.hash()).chain(active.iter().map(|b| *b.entry.hash())),
            );
            let headers: Vec<_> = active.iter().map(|b| b.entry.clone()).collect();
            if !beyond_gap {
                store.advance_synced_tip(headers.clone()).unwrap();
            }
            if !missing_prevouts {
                let rows = previous_outputs
                    .iter()
                    .map(|(outpoint, output)| {
                        super::super::TxOutRow::new(
                            &full_hash(&outpoint.txid[..]),
                            outpoint.vout as usize,
                            output,
                        )
                        .into_row()
                    })
                    .collect();
                store.txstore_db.write_rows(rows, DBFlush::Enable);
            }
            let mut active_chain_view =
                ActiveChainView::new(Arc::clone(&store), &headers, None, None);
            indexer
                .undo_index(&[stale.clone()], &mut active_chain_view)
                .unwrap();
            assert_eq!(history_rows(&store), expected);
            assert!(!store
                .indexed_blockhashes
                .read()
                .unwrap()
                .contains(stale.entry.hash()));
        }
    }

    // The #140-damaged shape: the active branch was indexed first, then a stale block
    // re-wrote the shared transaction's C and S rows at its own height.
    #[test]
    fn shared_rows_last_written_by_stale_block_are_rewritten() {
        for beyond_gap in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let store = Arc::new(Store::open_test(dir.path()));
            let mut indexer = test_indexer(&store, test_config());
            let common = test_block(0, BlockHash::all_zeros(), 0, vec![]);
            indexer.add(&[common.clone()]);
            indexer.index(&[common.clone()]);
            store
                .advance_synced_tip(vec![common.entry.clone()])
                .unwrap();

            let (stale, active, previous_outputs) = test_branches(&common, 2);
            indexer.add(&[stale.clone()]);
            indexer.add(&active);
            let active_rows = index_blocks(&active, &previous_outputs, &indexer.iconfig);
            let mut expected = history_rows(&store);
            expected.extend(
                active_rows
                    .iter()
                    .map(|row| (row.key.clone(), row.value.clone())),
            );
            // Active first, stale last: the stale block owns the shared keys' values.
            store.history_db.write_rows(active_rows, DBFlush::Enable);
            store.history_db.write_rows(
                index_blocks(&[stale.clone()], &previous_outputs, &indexer.iconfig),
                DBFlush::Enable,
            );
            store.indexed_blockhashes.write().unwrap().extend(
                std::iter::once(*stale.entry.hash()).chain(active.iter().map(|b| *b.entry.hash())),
            );
            let shared = stale.txids[0];
            let recorded: u32 = bincode::deserialize_little(
                &store.history_db.get(&TxConfRow::key(&shared)).unwrap(),
            )
            .unwrap();
            assert_eq!(recorded, 1, "test setup: stale block must own C row");

            let headers: Vec<_> = active.iter().map(|b| b.entry.clone()).collect();
            if !beyond_gap {
                store.advance_synced_tip(headers.clone()).unwrap();
            }
            // Ownership above the saved tip is what matters, so commit nothing for the gap
            // case and keep the active blocks indexed but uncommitted.
            let reorged_since = if beyond_gap { None } else { Some(1) };
            let mut view = ActiveChainView::new(Arc::clone(&store), &headers, reorged_since, None);
            indexer.undo_index(&[stale.clone()], &mut view).unwrap();

            assert_eq!(history_rows(&store), expected);
        }
    }

    #[test]
    fn cleanup_errors_leave_the_batch_available_for_retry() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open_test(dir.path()));
        let mut indexer = test_indexer(&store, test_config());
        let common = test_block(0, BlockHash::all_zeros(), 0, vec![]);
        let tx = test_transaction(
            OutPoint {
                txid: Txid::from_byte_array([1; 32]),
                vout: 0,
            },
            0x51,
        );
        let stale = test_block(1, *common.entry.hash(), 1, vec![tx.clone()]);
        let active = test_block(1, *common.entry.hash(), 2, vec![tx.clone()]);
        let prevouts = [(tx.input[0].previous_output, tx.output[0].clone())]
            .iter()
            .cloned()
            .collect();
        write_test_history(
            &indexer,
            &[common.clone(), stale.clone(), active.clone()],
            &prevouts,
        );
        store
            .advance_synced_tip(vec![common.entry, active.entry.clone()])
            .unwrap();
        let key = BlockRow::txids_key(full_hash(&active.entry.hash()[..]));
        let txids = store.txstore_db.get(&key).unwrap();
        store.txstore_db.put_sync(&key, b"invalid txids");
        let mut expected = history_rows(&store);
        assert!(indexer
            .undo_index(&[stale.clone()], &mut test_active_chain_view(&store))
            .is_err());
        // Nothing is deleted, but the rewrite stays pending so a retry restores shared rows.
        expected.insert(PENDING_REWRITE_KEY.to_vec(), 1u32.to_le_bytes().to_vec());
        assert_eq!(history_rows(&store), expected);
        assert!(store
            .indexed_blockhashes
            .read()
            .unwrap()
            .contains(stale.entry.hash()));
        store.txstore_db.put_sync(&key, &txids);
        indexer
            .undo_index(&[stale.clone()], &mut test_active_chain_view(&store))
            .unwrap();
        assert_eq!(pending_rewrite(&store).unwrap(), None);
        assert!(store
            .history_db
            .get(&TxConfRow::key(&active.txids[0]))
            .is_some());
        assert!(!store
            .indexed_blockhashes
            .read()
            .unwrap()
            .contains(stale.entry.hash()));
    }

    #[test]
    fn restart_uses_common_ancestor_while_higher_undo_is_deferred() {
        let dir = tempfile::tempdir().unwrap();
        let hashes = {
            let store = Arc::new(Store::open_test(dir.path()));
            let mut indexer = test_indexer(&store, test_config());
            let common = test_block(0, BlockHash::all_zeros(), 0, vec![]);
            let lower_tx = test_transaction(OutPoint::null(), 0x52);
            let lower = test_block(1, *common.entry.hash(), 1, vec![lower_tx]);
            let tx = test_transaction(
                OutPoint {
                    txid: Txid::from_byte_array([1; 32]),
                    vout: 0,
                },
                0x51,
            );
            let higher = test_block(2, *lower.entry.hash(), 2, vec![tx.clone()]);
            let blocks = [common.clone(), lower.clone(), higher.clone()];
            let prevouts = [(tx.input[0].previous_output, tx.output[0].clone())]
                .iter()
                .cloned()
                .collect();
            write_test_history(&indexer, &blocks, &prevouts);
            store
                .advance_synced_tip(blocks.iter().map(|b| b.entry.clone()).collect())
                .unwrap();
            assert!(store.pop_reorged_headers(4).is_err());
            assert_eq!(store.headers().len(), 3);
            let popped = store.pop_reorged_headers(1).unwrap();
            indexer
                .uncommitted_blocks
                .extend(popped.iter().map(|h| *h.hash()));
            let mut deferred = DeferredUndo::default();
            let mut active_chain_view = test_active_chain_view(&store);
            indexer
                .undo_index_or_defer(&[higher.clone()], &mut deferred, &mut active_chain_view)
                .unwrap();
            // Only the missing prevout's spending row is outstanding; the undo marker remains.
            let pending: Vec<_> = deferred.blocks.keys().copied().collect();
            assert_eq!(pending, vec![*higher.entry.hash()]);
            assert!(store
                .history_db
                .get(&undo_marker_key(higher.entry.hash()))
                .is_some());
            indexer
                .undo_index_or_defer(&[lower.clone()], &mut deferred, &mut active_chain_view)
                .unwrap();
            assert!(store
                .history_db
                .get(&TxConfRow::key(&lower.txids[0]))
                .is_none());
            assert!(store
                .history_db
                .get(
                    &BlockRow::new_done(full_hash(&lower.entry.hash()[..]))
                        .into_row()
                        .key
                )
                .is_none());
            [
                *common.entry.hash(),
                *lower.entry.hash(),
                *higher.entry.hash(),
            ]
        };
        let store = Arc::new(Store::open_test(dir.path()));
        assert_eq!(*store.headers().tip(), hashes[0]);
        assert_eq!(store.headers().len(), 1);
        let pending = store.uncommitted_blocks();
        assert!(!pending.contains(&hashes[1]));
        assert!(pending.contains(&hashes[2]));
        let tx = test_transaction(
            OutPoint {
                txid: Txid::from_byte_array([1; 32]),
                vout: 0,
            },
            0x51,
        );
        let higher = test_block(2, hashes[1], 2, vec![tx]);
        let mut indexer = test_indexer(&store, test_config());
        indexer
            .undo_index(&[higher.clone()], &mut test_active_chain_view(&store))
            .unwrap();
        assert_eq!(*store.headers().tip(), hashes[0]);
        assert!(store.uncommitted_blocks().is_empty());
        assert!(store
            .history_db
            .get(&TxConfRow::key(&higher.txids[0]))
            .is_none());
    }

    #[cfg(feature = "liquid")]
    #[test]
    fn liquid_crash_cleanup_removes_asset_rows_without_txstore() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open_test(dir.path()));
        let prevout = OutPoint {
            txid: Txid::from_byte_array([1; 32]),
            vout: 0,
        };
        let tx = test_transaction(prevout, 0x51);
        let output = tx.output[0].clone();
        let entry = test_block(3, BlockHash::all_zeros(), 0, vec![tx]);
        let hash = *entry.entry.hash();
        let iconfig = test_config();
        let prevouts = [(prevout, output)].iter().cloned().collect();
        store.history_db.write_rows(
            index_blocks(&[entry.clone()], &prevouts, &iconfig),
            DBFlush::Enable,
        );
        store.indexed_blockhashes.write().unwrap().insert(hash);
        for prefix in [b'H', b'S', b'C', b'I', b'i', b'D'] {
            assert!(
                store.history_db.iter_scan(&[prefix]).next().is_some(),
                "missing test row {}",
                prefix
            );
        }
        // Cleanup must leave other transactions at the same height intact.
        let retained = [1, 3]
            .iter()
            .map(|height| {
                TxHistoryRow::new(
                    &Script::from(vec![0x51]),
                    *height,
                    TxHistoryInfo::Funding(FundingInfo {
                        txid: [2; 32],
                        vout: 0,
                        value: confidential::Value::Explicit(10),
                    }),
                )
                .into_row()
            })
            .collect::<Vec<_>>();
        let retained_keys: HashSet<_> = retained.iter().map(|row| row.key.clone()).collect();
        store.history_db.write_rows(retained, DBFlush::Enable);
        let asset_cache_keys: HashSet<_> = store
            .history_db
            .iter_scan(b"I")
            .map(|row| [b"z", &row.key[1..33]].concat())
            .collect();
        for key in &asset_cache_keys {
            store.cache_db.put_sync(key, b"stale cached totals");
        }
        let unaffected = [b"z", &[0xff; 32][..]].concat();
        assert!(!asset_cache_keys.contains(&unaffected));
        store.cache_db.put_sync(&unaffected, b"unaffected");
        let mut indexer = test_indexer(&store, iconfig);
        indexer
            .undo_index(&[entry], &mut test_active_chain_view(&store))
            .unwrap();
        for key in asset_cache_keys {
            assert!(store.cache_db.get(&key).is_none());
        }
        assert_eq!(
            store.cache_db.get(&unaffected).as_deref(),
            Some(&b"unaffected"[..])
        );
        assert!(indexer.uncommitted_blocks.is_empty());
        assert!(store.indexed_blockhashes.read().unwrap().is_empty());
        assert_eq!(
            store
                .history_db
                .iter_scan(b"")
                .map(|row| row.key)
                .collect::<HashSet<_>>(),
            retained_keys
        );
    }

    fn test_active_chain_view(store: &Arc<Store>) -> ActiveChainView<'static> {
        ActiveChainView::new(Arc::clone(store), &[], None, None)
    }

    fn test_indexer(store: &Arc<Store>, iconfig: IndexerConfig) -> Indexer {
        let metrics = Metrics::new("127.0.0.1:0".parse().unwrap());
        Indexer {
            uncommitted_blocks: store.uncommitted_blocks(),
            store: Arc::clone(store),
            flush: DBFlush::Enable,
            from: FetchFrom::Bitcoind,
            iconfig,
            duration: metrics
                .histogram_vec(HistogramOpts::new("index_duration", "test"), &["step"]),
            tip_metric: metrics.gauge(MetricOpts::new("tip_height", "test")),
            stale_cleanup_failing: metrics.gauge(MetricOpts::new("stale_cleanup_failing", "test")),
            cleanup_retry: Default::default(),
            sync_height: metrics.gauge(MetricOpts::new("sync_height", "test")),
            sync_progress: metrics.float_gauge(MetricOpts::new("sync_progress", "test")),
        }
    }

    #[cfg(not(feature = "liquid"))]
    #[test]
    fn deferred_undo_scans_history_once_for_all_batches() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open_test(dir.path()));
        let iconfig = test_config();
        let mut prevouts = HashMap::new();
        let entries: Vec<_> = (0..2u8)
            .map(|i| {
                let prevout = OutPoint {
                    txid: Txid::from_byte_array([i + 1; 32]),
                    vout: 0,
                };
                let spent = TxOut {
                    value: bitcoin::Amount::from_sat(50),
                    script_pubkey: Script::from_bytes(vec![0x51 + i]),
                };
                prevouts.insert(prevout, spent);
                let tx = test_transaction(prevout, 0x60);
                test_block(5 + i as usize, BlockHash::all_zeros(), i as u32, vec![tx])
            })
            .collect();
        store
            .history_db
            .write_rows(index_blocks(&entries, &prevouts, &iconfig), DBFlush::Enable);
        store
            .indexed_blockhashes
            .write()
            .unwrap()
            .extend(entries.iter().map(|e| *e.entry.hash()));
        let mut indexer = test_indexer(&store, iconfig);
        assert_eq!(indexer.uncommitted_blocks.len(), 2);

        let mut deferred = DeferredUndo::default();
        for entry in &entries {
            indexer
                .undo_index_or_defer(
                    std::slice::from_ref(entry),
                    &mut deferred,
                    &mut test_active_chain_view(&store),
                )
                .unwrap();
        }
        // Undo markers survive until the remaining spending rows have been deleted.
        assert!(store.indexed_blockhashes.read().unwrap().is_empty());
        assert_eq!(store.history_db.iter_scan(UNDO_PREFIX).count(), 2);
        indexer
            .finish_deferred_undo(deferred, &mut test_active_chain_view(&store))
            .unwrap();

        let scans = indexer
            .duration
            .with_label_values(&["undo_history_scan"])
            .get_sample_count();
        assert_eq!(scans, 1);
        assert!(indexer.uncommitted_blocks.is_empty());
        assert!(store.indexed_blockhashes.read().unwrap().is_empty());
        assert!(store.history_db.iter_scan(b"").next().is_none());
    }

    // The common crash shape: a stale block spends an output of a lower stale block
    // whose txstore rows were lost. Undo runs top-down, so the spender is seen first.
    #[test]
    fn prevouts_from_lower_stale_blocks_avoid_the_history_scan() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open_test(dir.path()));
        let iconfig = test_config();
        let funding = test_block(
            5,
            BlockHash::all_zeros(),
            1,
            vec![test_transaction(OutPoint::null(), 0x51)],
        );
        let funded = OutPoint {
            txid: funding.txids[0],
            vout: 0,
        };
        let spending = test_block(
            6,
            *funding.entry.hash(),
            2,
            vec![test_transaction(funded, 0x52)],
        );
        let prevouts = [(funded, funding.block.txdata[0].output[0].clone())]
            .iter()
            .cloned()
            .collect();
        let blocks = [funding.clone(), spending.clone()];
        store
            .history_db
            .write_rows(index_blocks(&blocks, &prevouts, &iconfig), DBFlush::Enable);
        store
            .indexed_blockhashes
            .write()
            .unwrap()
            .extend(blocks.iter().map(|b| *b.entry.hash()));
        let mut indexer = test_indexer(&store, iconfig);

        let mut deferred = DeferredUndo::default();
        let mut view = test_active_chain_view(&store);
        for block in [&spending, &funding] {
            indexer
                .undo_index_or_defer(std::slice::from_ref(block), &mut deferred, &mut view)
                .unwrap();
        }
        indexer.finish_deferred_undo(deferred, &mut view).unwrap();

        let scans = indexer
            .duration
            .with_label_values(&["undo_history_scan"])
            .get_sample_count();
        assert_eq!(scans, 0);
        assert!(store.indexed_blockhashes.read().unwrap().is_empty());
        assert!(store.history_db.iter_scan(b"").next().is_none());
    }

    // Deferral keeps one record per unresolved input, not blocks or their outputs, and
    // releases a block's undo marker once its last input resolves.
    #[test]
    fn deferred_undo_retains_only_unresolved_inputs() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open_test(dir.path()));
        let iconfig = test_config();
        // Funder has many outputs; spender spends one of them. Neither is in txstore.
        let mut funding_tx = test_transaction(OutPoint::null(), 0x51);
        for i in 0..50u8 {
            let mut extra = funding_tx.output[0].clone();
            extra.script_pubkey = Script::from(vec![0x60, i]);
            funding_tx.output.push(extra);
        }
        let funding = test_block(5, BlockHash::all_zeros(), 1, vec![funding_tx]);
        let funded = OutPoint {
            txid: funding.txids[0],
            vout: 3,
        };
        let spending = test_block(
            6,
            *funding.entry.hash(),
            2,
            vec![test_transaction(funded, 0x52)],
        );
        let prevouts = [(funded, funding.block.txdata[0].output[3].clone())]
            .iter()
            .cloned()
            .collect();
        let blocks = [funding.clone(), spending.clone()];
        store
            .history_db
            .write_rows(index_blocks(&blocks, &prevouts, &iconfig), DBFlush::Enable);
        store
            .indexed_blockhashes
            .write()
            .unwrap()
            .extend(blocks.iter().map(|b| *b.entry.hash()));
        let mut indexer = test_indexer(&store, iconfig);
        let mut deferred = DeferredUndo::default();
        let mut view = test_active_chain_view(&store);
        let spender_done = undo_marker_key(spending.entry.hash());

        indexer
            .undo_index_or_defer(std::slice::from_ref(&spending), &mut deferred, &mut view)
            .unwrap();
        assert_eq!(deferred.pending.len(), 1);
        assert_eq!(deferred.pending.values().flatten().count(), 1);
        assert_eq!(deferred.blocks.len(), 1);
        // Held back so a crash now still leaves the spender marked for retry.
        assert!(store.history_db.get(&spender_done).is_some());

        indexer
            .undo_index_or_defer(std::slice::from_ref(&funding), &mut deferred, &mut view)
            .unwrap();
        assert!(deferred.pending.is_empty());
        assert!(deferred.blocks.is_empty());
        assert!(store.history_db.get(&spender_done).is_none());
        assert!(store.history_db.iter_scan(b"").next().is_none());
        assert!(store.indexed_blockhashes.read().unwrap().is_empty());
    }

    #[test]
    fn prevout_read_errors_propagate_instead_of_deferring() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open_test(dir.path()));
        let prevout = OutPoint {
            txid: Txid::from_byte_array([1; 32]),
            vout: 0,
        };
        let block = test_block(
            5,
            BlockHash::all_zeros(),
            1,
            vec![test_transaction(prevout, 0x51)],
        );
        // A present but undecodable row is not a missing prevout.
        store.txstore_db.put_sync(
            &super::super::TxOutRow::key(&prevout),
            b"not a serialized txout",
        );
        let indexer = test_indexer(&store, test_config());
        let result = indexer.lookup_undo_prevouts(&[block]);
        assert!(result.is_err());
    }

    #[test]
    fn interrupted_rewrite_keeps_marker_until_all_batches_are_durable() {
        let dir = tempfile::tempdir().unwrap();
        let common = test_block(0, BlockHash::all_zeros(), 0, vec![]);
        let first = test_block(
            1,
            *common.entry.hash(),
            1,
            vec![test_transaction(OutPoint::null(), 0x51)],
        );
        let second = test_block(
            2,
            *first.entry.hash(),
            2,
            vec![test_transaction(OutPoint::null(), 0x52)],
        );
        {
            let store = Arc::new(Store::open_test(dir.path()));
            let indexer = test_indexer(&store, test_config());
            let blocks = [common, first.clone(), second.clone()];
            write_test_history(&indexer, &blocks, &HashMap::new());
            store
                .advance_synced_tip(blocks.iter().map(|b| b.entry.clone()).collect())
                .unwrap();
            store.history_db.delete_rows(
                [first.txids[0], second.txids[0]]
                    .iter()
                    .map(|txid| DBRow {
                        key: TxConfRow::key(txid),
                        value: vec![],
                    })
                    .collect(),
                DBFlush::Enable,
            );
            store.txstore_db.delete_rows(
                vec![DBRow {
                    key: TxRow::key(&second.txids[0][..]),
                    value: vec![],
                }],
                DBFlush::Enable,
            );
            indexer.begin_rewrite(1).unwrap();
            assert!(indexer
                .finish_rewrite(&test_active_chain_view(&store))
                .is_err());
            assert!(store
                .history_db
                .get(&TxConfRow::key(&first.txids[0]))
                .is_some());
            assert!(store
                .history_db
                .get(&TxConfRow::key(&second.txids[0]))
                .is_none());
        }
        let store = Arc::new(Store::open_test(dir.path()));
        assert_eq!(pending_rewrite(&store).unwrap(), Some(1));
        store.txstore_db.put_sync(
            &TxRow::key(&second.txids[0][..]),
            &serialize(&second.block.txdata[0]),
        );
        let indexer = test_indexer(&store, test_config());
        indexer
            .finish_rewrite(&test_active_chain_view(&store))
            .unwrap();
        assert_eq!(pending_rewrite(&store).unwrap(), None);
        assert!(store
            .history_db
            .get(&TxConfRow::key(&second.txids[0]))
            .is_some());
    }

    #[test]
    fn prevouts_in_the_same_batch_avoid_the_history_scan() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open_test(dir.path()));
        let funding = test_block(
            5,
            BlockHash::all_zeros(),
            1,
            vec![test_transaction(OutPoint::null(), 0x51)],
        );
        let outpoint = OutPoint {
            txid: funding.txids[0],
            vout: 0,
        };
        let spending = test_block(
            6,
            *funding.entry.hash(),
            2,
            vec![test_transaction(outpoint, 0x52)],
        );
        let prevouts =
            std::iter::once((outpoint, funding.block.txdata[0].output[0].clone())).collect();
        let blocks = [spending, funding];
        store.history_db.write_rows(
            index_blocks(&blocks, &prevouts, &test_config()),
            DBFlush::Enable,
        );
        store
            .indexed_blockhashes
            .write()
            .unwrap()
            .extend(blocks.iter().map(|b| *b.entry.hash()));
        let mut indexer = test_indexer(&store, test_config());
        indexer
            .undo_index(&blocks, &mut test_active_chain_view(&store))
            .unwrap();
        assert_eq!(
            indexer
                .duration
                .with_label_values(&["undo_history_scan"])
                .get_sample_count(),
            0
        );
        assert!(store.history_db.iter_scan(b"").next().is_none());
    }

    #[test]
    fn malformed_done_marker_is_an_error_not_a_panic() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open_test(dir.path());
        let hash = test_header(BlockHash::all_zeros()).block_hash();
        store.history_db.write_rows(
            vec![DBRow {
                key: BlockRow::new_done(full_hash(&hash[..])).into_row().key,
                value: vec![1, 2, 3],
            }],
            DBFlush::Enable,
        );
        store.indexed_blockhashes.write().unwrap().insert(hash);
        assert_eq!(store.uncommitted_blocks(), std::iter::once(hash).collect());
        assert!(done_marker_height(&[1, 2, 3]).is_err());
        assert_eq!(done_marker_height(&[]).unwrap(), None);
        assert_eq!(done_marker_height(&7u32.to_le_bytes()).unwrap(), Some(7));
    }
}
