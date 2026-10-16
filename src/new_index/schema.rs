mod reorg;

pub use self::reorg::STALE_CLEANUP_FAILED;

use bitcoin::hashes::{sha256, sha256d::Hash as Sha256dHash, Hash};
use bitcoin::hex::FromHex;
#[cfg(not(feature = "liquid"))]
use bitcoin::merkle_tree::MerkleBlock;

use error_chain::ChainedError;
use itertools::Itertools;
use rayon::prelude::*;

#[cfg(not(feature = "liquid"))]
use bitcoin::consensus::encode::{deserialize, serialize};
#[cfg(feature = "liquid")]
use elements::{
    confidential,
    encode::{deserialize, serialize},
    AssetId,
};

use std::collections::{BTreeSet, HashMap, HashSet};
use std::convert::TryInto;
use std::sync::{Arc, RwLock, RwLockReadGuard};

use crate::config::Config;
use crate::daemon::Daemon;
use crate::errors::*;
use crate::metrics::{Gauge, HistogramOpts, HistogramTimer, HistogramVec, MetricOpts, Metrics};
use crate::util::{
    bincode, electrum_merkle::CheckpointMerkleCache, full_hash, has_prevout, is_spendable,
    BlockHeaderMeta, BlockId, BlockMeta, BlockStatus, Bytes, HeaderEntry, HeaderList, ScriptToAddr,
};
use crate::{
    chain::{BlockHash, BlockHeader, Network, OutPoint, Script, Transaction, TxOut, Txid, Value},
    new_index::db_metrics::RocksDbMetrics,
};

use crate::new_index::db::{DBFlush, DBRow, ReverseScanIterator, ScanIterator, DB};
use crate::new_index::fetch::{start_fetcher, BlockEntry, FetchFrom};

#[cfg(feature = "liquid")]
use crate::elements::{asset, ebcompact::TxidCompat, peg};

#[cfg(feature = "liquid")]
use elements::encode::VarInt;

#[cfg(not(feature = "liquid"))]
use bitcoin::VarInt;

const MIN_HISTORY_ITEMS_TO_CACHE: usize = 100;

fn completed_prefix_len<T>(items: &[T], mut is_complete: impl FnMut(&T) -> bool) -> usize {
    items.iter().take_while(|item| is_complete(item)).count()
}

pub struct Store {
    // TODO: should be column families
    txstore_db: DB,
    history_db: DB,
    cache_db: DB,
    pub(crate) history_cache_lock: RwLock<()>,
    added_blockhashes: RwLock<HashSet<BlockHash>>,
    indexed_blockhashes: RwLock<HashSet<BlockHash>>,
    indexed_headers: RwLock<HeaderList>,
}

impl Store {
    pub fn open(config: &Config, metrics: &Metrics, verify_compat: bool) -> Self {
        let path = config.db_path.join("newindex");

        // Create a single shared LRU cache for all three DBs. The total size is
        // --db-block-cache-mb (not multiplied by 3). RocksDB's LRU cache is
        // thread-safe, so all DBs share one eviction pool. This lets the
        // txstore (which holds the bulk of the data) claim as much cache as it
        // needs without being artificially capped at 1/3 of the total.
        let cache_size_bytes = config.db_block_cache_mb * 1024 * 1024;
        let shared_cache = rocksdb::Cache::new_lru_cache(cache_size_bytes);
        debug!(
            "shared LRU block cache: db_block_cache_mb='{}'",
            config.db_block_cache_mb
        );

        let txstore_db = DB::open(&path.join("txstore"), config, verify_compat, &shared_cache);
        let added_blockhashes = load_blockhashes(&txstore_db, &BlockRow::done_filter());
        info!("{} blocks were added", added_blockhashes.len());

        let history_db = DB::open(&path.join("history"), config, verify_compat, &shared_cache);
        let indexed_blockhashes = load_blockhashes(&history_db, &BlockRow::done_filter());
        info!("{} blocks were indexed", indexed_blockhashes.len());

        let cache_db = DB::open(&path.join("cache"), config, verify_compat, &shared_cache);

        let db_metrics = Arc::new(RocksDbMetrics::new(&metrics));
        txstore_db.start_stats_exporter(Arc::clone(&db_metrics), "txstore_db");
        history_db.start_stats_exporter(Arc::clone(&db_metrics), "history_db");
        cache_db.start_stats_exporter(Arc::clone(&db_metrics), "cache_db");

        let headers = Self::load_headers(&txstore_db, &indexed_blockhashes);

        Store {
            txstore_db,
            history_db,
            cache_db,
            history_cache_lock: RwLock::new(()),
            added_blockhashes: RwLock::new(added_blockhashes),
            indexed_blockhashes: RwLock::new(indexed_blockhashes),
            indexed_headers: RwLock::new(headers),
        }
    }

    fn load_headers(txstore_db: &DB, indexed_blockhashes: &HashSet<BlockHash>) -> HeaderList {
        if let Some(tip_hash) = txstore_db.get(b"t") {
            let mut tip_hash = deserialize(&tip_hash).expect("invalid chain tip in `t`");
            let headers_map = load_blockheaders(&txstore_db);

            // Databases written by older versions could save a tip whose history a
            // crash during reorg cleanup had already removed. Current versions move
            // the saved tip to the common ancestor before deleting anything.
            while !indexed_blockhashes.contains(&tip_hash) {
                tip_hash = headers_map
                    .get(&tip_hash)
                    .expect("invalid header chain")
                    .prev_blockhash;
            }
            info!(
                "{} headers were loaded, tip at {:?}",
                headers_map.len(),
                tip_hash
            );
            HeaderList::new(headers_map, tip_hash)
        } else {
            HeaderList::empty()
        }
    }

    pub fn txstore_db(&self) -> &DB {
        &self.txstore_db
    }

    pub fn history_db(&self) -> &DB {
        &self.history_db
    }

    pub fn cache_db(&self) -> &DB {
        &self.cache_db
    }

    pub fn headers(&self) -> RwLockReadGuard<'_, HeaderList> {
        self.indexed_headers.read().unwrap()
    }

    pub fn done_initial_sync(&self) -> bool {
        self.txstore_db.get(b"t").is_some()
    }

    // Headers that need any work: either not yet added to txstore or not yet indexed to history.
    fn headers_to_process(&self, new_headers: &[HeaderEntry]) -> Vec<HeaderEntry> {
        let added = self.added_blockhashes.read().unwrap();
        let indexed = self.indexed_blockhashes.read().unwrap();
        new_headers
            .iter()
            .filter(|e| !added.contains(e.hash()) || !indexed.contains(e.hash()))
            .cloned()
            .collect()
    }

    // The contiguous prefix of `new_headers` that was both added and indexed. Blocks
    // after the first gap stay outstanding, for the next index update to re-request.
    fn completed_header_prefix(&self, new_headers: &[HeaderEntry]) -> Vec<HeaderEntry> {
        let added = self.added_blockhashes.read().unwrap();
        let indexed = self.indexed_blockhashes.read().unwrap();
        let len = completed_prefix_len(new_headers, |e| {
            added.contains(e.hash()) && indexed.contains(e.hash())
        });
        new_headers[..len].to_vec()
    }

    // Append the completed prefix to the in-memory HeaderList and persist the resulting
    // tip under `t`. Errors on an empty index rather than returning the all-zeroes hash
    // that `HeaderList::tip()` yields there, which reads as a real tip to the caller.
    fn advance_synced_tip(&self, completed_headers: Vec<HeaderEntry>) -> Result<BlockHash> {
        let mut headers = self.indexed_headers.write().unwrap();
        headers.append(completed_headers);
        if headers.is_empty() {
            bail!("no blocks were indexed, the index is still empty");
        }
        let synced_tip = *headers.tip();
        debug!("updating synced tip to synced_tip='{}'", synced_tip);
        self.txstore_db.put_sync(b"t", &serialize(&synced_tip));
        Ok(synced_tip)
    }

    #[cfg(test)]
    fn open_test(path: &std::path::Path) -> Self {
        let txstore_db = DB::open_test(&path.join("txstore"));
        let history_db = DB::open_test(&path.join("history"));
        let added_blockhashes = load_blockhashes(&txstore_db, &BlockRow::done_filter());
        let indexed_blockhashes = load_blockhashes(&history_db, &BlockRow::done_filter());
        let headers = Self::load_headers(&txstore_db, &indexed_blockhashes);
        Store {
            txstore_db,
            history_db,
            cache_db: DB::open_test(&path.join("cache")),
            history_cache_lock: RwLock::new(()),
            added_blockhashes: RwLock::new(added_blockhashes),
            indexed_blockhashes: RwLock::new(indexed_blockhashes),
            indexed_headers: RwLock::new(headers),
        }
    }
}

type UtxoMap = HashMap<OutPoint, (BlockId, Value)>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UtxoScanStop {
    ScanLimit,
    CheckpointLimit,
}

#[derive(Debug)]
pub struct Utxo {
    pub txid: Txid,
    pub vout: u32,
    pub confirmed: Option<BlockId>,
    pub value: Value,

    #[cfg(feature = "liquid")]
    pub asset: confidential::Asset,
    #[cfg(feature = "liquid")]
    pub nonce: confidential::Nonce,
    #[cfg(feature = "liquid")]
    pub witness: elements::TxOutWitness,
}

impl From<&Utxo> for OutPoint {
    fn from(utxo: &Utxo) -> Self {
        OutPoint {
            txid: utxo.txid,
            vout: utxo.vout,
        }
    }
}

#[derive(Debug)]
pub struct SpendingInput {
    pub txid: Txid,
    pub vin: u32,
    pub confirmed: Option<BlockId>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ScriptStats {
    pub tx_count: usize,
    pub funded_txo_count: usize,
    pub spent_txo_count: usize,
    #[cfg(not(feature = "liquid"))]
    pub funded_txo_sum: u64,
    #[cfg(not(feature = "liquid"))]
    pub spent_txo_sum: u64,
}

impl ScriptStats {
    pub fn default() -> Self {
        ScriptStats {
            tx_count: 0,
            funded_txo_count: 0,
            spent_txo_count: 0,
            #[cfg(not(feature = "liquid"))]
            funded_txo_sum: 0,
            #[cfg(not(feature = "liquid"))]
            spent_txo_sum: 0,
        }
    }
}

const CLEANUP_RETRY_INITIAL: std::time::Duration = std::time::Duration::from_secs(5);
const CLEANUP_RETRY_MAX: std::time::Duration = std::time::Duration::from_secs(600);

// Capped exponential backoff between failed stale-cleanup attempts.
#[derive(Default)]
struct CleanupRetry {
    delay: Option<std::time::Duration>,
    next_attempt: Option<std::time::Instant>,
    failed_at_tip: Option<BlockHash>,
}

impl CleanupRetry {
    fn due(&self, daemon_tip: &BlockHash) -> bool {
        self.failed_at_tip.as_ref() != Some(daemon_tip)
            || self
                .next_attempt
                .is_none_or(|at| std::time::Instant::now() >= at)
    }

    fn failed(&mut self, daemon_tip: BlockHash) -> std::time::Duration {
        let delay = self.delay.map_or(CLEANUP_RETRY_INITIAL, |delay| {
            (delay * 2).min(CLEANUP_RETRY_MAX)
        });
        self.delay = Some(delay);
        self.next_attempt = Some(std::time::Instant::now() + delay);
        self.failed_at_tip = Some(daemon_tip);
        delay
    }
}

pub struct Indexer {
    store: Arc<Store>,
    flush: DBFlush,
    from: FetchFrom,
    uncommitted_blocks: HashSet<BlockHash>,
    iconfig: IndexerConfig,
    duration: HistogramVec,
    tip_metric: Gauge,
    stale_cleanup_failing: Gauge,
    cleanup_retry: CleanupRetry,
    sync_height: Gauge,
    sync_progress: prometheus::Gauge,
}

struct IndexerConfig {
    light_mode: bool,
    address_search: bool,
    index_unspendables: bool,
    network: Network,
    block_batch_size: usize,
    #[cfg(feature = "liquid")]
    parent_network: crate::chain::BNetwork,
}

impl From<&Config> for IndexerConfig {
    fn from(config: &Config) -> Self {
        IndexerConfig {
            light_mode: config.light_mode,
            address_search: config.address_search,
            index_unspendables: config.index_unspendables,
            network: config.network_type,
            block_batch_size: config.initial_sync_batch_size,
            #[cfg(feature = "liquid")]
            parent_network: config.parent_network,
        }
    }
}

pub struct ChainQuery {
    store: Arc<Store>, // TODO: should be used as read-only
    daemon: Arc<Daemon>,
    light_mode: bool,
    duration: HistogramVec,
    network: Network,
    history_scan_limit: usize,
    utxos_checkpoint_limit: usize,
    checkpoint_merkle_cache: CheckpointMerkleCache,
}

// TODO: &[Block] should be an iterator / a queue.
impl Indexer {
    pub fn open(store: Arc<Store>, from: FetchFrom, config: &Config, metrics: &Metrics) -> Self {
        Indexer {
            uncommitted_blocks: store.uncommitted_blocks(),
            store,
            flush: DBFlush::Disable,
            from,
            iconfig: IndexerConfig::from(config),
            duration: metrics.histogram_vec(
                HistogramOpts::new("index_duration", "Index update duration (in seconds)"),
                &["step"],
            ),
            tip_metric: metrics.gauge(MetricOpts::new("tip_height", "Current chain tip height")),
            stale_cleanup_failing: metrics.gauge(MetricOpts::new(
                "stale_cleanup_failing",
                "1 while stale block history cleanup is failing and indexing cannot advance",
            )),
            cleanup_retry: CleanupRetry::default(),
            sync_height: metrics.gauge(MetricOpts::new(
                "initial_sync_height",
                "Height of the last block batch completed during initial sync",
            )),
            sync_progress: metrics.float_gauge(MetricOpts::new(
                "initial_sync_progress_pct",
                "Initial sync progress as a percentage of the best known chain height",
            )),
        }
    }

    fn start_timer(&self, name: &str) -> HistogramTimer {
        self.duration.with_label_values(&[name]).start_timer()
    }

    /// Whether stale cleanup failed during the last update and needs retrying.
    pub fn stale_cleanup_failing(&self) -> bool {
        self.stale_cleanup_failing.get() != 0
    }

    /// Make the next update retry a failing cleanup without waiting out the backoff.
    #[doc(hidden)]
    pub fn retry_cleanup_now(&mut self) {
        self.cleanup_retry.next_attempt = None;
    }

    fn start_auto_compactions(&self, db: &DB) {
        let key = b"F".to_vec();
        if db.get(&key).is_none() {
            info!(
                "full-compaction sentinel 'F' not found in {:?} — running one-time full compaction",
                db
            );
            db.full_compaction();
            info!(
                "full compaction finished for {:?} — tightening triggers to steady-state values",
                db
            );
            db.apply_steady_state_triggers();
            db.put_sync(&key, b"");
            assert!(db.get(&key).is_some());
            info!("full-compaction sentinel 'F' set in {:?}", db);
        } else {
            trace!(
                "full-compaction sentinel 'F' found in {:?} — skipping full compaction",
                db
            );
        }
        db.enable_auto_compaction();
    }

    fn get_new_headers(
        &self,
        daemon: &Daemon,
        tip: &BlockHash,
    ) -> Result<(Vec<HeaderEntry>, Option<usize>)> {
        let indexed_headers = self.store.indexed_headers.read().unwrap();
        let raw_new_headers = daemon.get_new_headers(&indexed_headers, tip)?;
        let (new_headers, reorged_since) = indexed_headers.preprocess(raw_new_headers, tip);

        if let Some(tip) = new_headers.last() {
            info!("{:?} ({} left to index)", tip, new_headers.len());
        }
        Ok((new_headers, reorged_since))
    }

    pub fn update(&mut self, daemon: &Daemon) -> Result<BlockHash> {
        let daemon = daemon.reconnect()?;
        let tip = daemon.getbestblockhash()?;

        let (new_headers, reorged_since) = self.get_new_headers(&daemon, &tip)?;
        let chain_tip_height = new_headers.last().map(|h| h.height()).unwrap_or(0);

        ensure!(
            reorged_since != Some(0),
            "daemon chain does not share a genesis block with the index (wrong network or database?)"
        );
        // A persistent failure would otherwise redo the fork walk, daemon calls and R write
        // every main-loop cycle. A new daemon tip retries at once: that is when recovery is
        // most likely.
        if self.stale_cleanup_failing() && !self.cleanup_retry.due(&tip) {
            return Ok(*self.store.headers().tip());
        }
        // Stale history must be removed before replacement blocks reuse its heights, so a
        // failure stops this update. Retry next cycle rather than exiting: a restart
        // cannot clear a block bitcoind no longer serves.
        if let Err(err) = self.clean_up_stale_blocks(&daemon, &new_headers, reorged_since) {
            if let ErrorKind::Interrupt(_) = err.kind() {
                return Err(err);
            }
            let delay = self.cleanup_retry.failed(tip);
            // Indexing stays at the common ancestor, so the mempool cannot sync against the
            // daemon tip either: new unconfirmed transactions are not indexed until this clears.
            if self.stale_cleanup_failing.get() == 0 {
                error!(
                    "{} (new unconfirmed transactions are not indexed until it succeeds), retrying in {:?}: {}",
                    STALE_CLEANUP_FAILED,
                    delay,
                    err.display_chain()
                );
            } else {
                warn!(
                    "{}, retrying in {:?}: {}",
                    STALE_CLEANUP_FAILED,
                    delay,
                    err.display_chain()
                );
            }
            self.stale_cleanup_failing.set(1);
            let headers = self.store.headers();
            // Empty after a crash before the first tip commit; best_height() would panic.
            if let Some(height) = headers.len().checked_sub(1) {
                self.tip_metric.set(height as i64);
            }
            return Ok(*headers.tip());
        }
        self.stale_cleanup_failing.set(0);
        self.cleanup_retry = CleanupRetry::default();

        let fetch_result = self.fetch_and_index(&daemon, &new_headers, chain_tip_height);

        if let Err(ref e) = fetch_result {
            warn!(
                "block fetch incomplete, advancing only completed prefix and retrying on next index update: {:?}",
                e
            );
        }

        let completed_headers = self.store.completed_header_prefix(&new_headers);
        self.remember_uncommitted_headers(&new_headers[completed_headers.len()..]);
        if fetch_result.is_ok() && completed_headers.len() != new_headers.len() {
            bail!(
                "block fetch completed without indexing all headers: completed={} requested={}",
                completed_headers.len(),
                new_headers.len()
            );
        }

        self.finish_sync_writes();

        // Publish headers only after their history writes are durable.
        let synced_tip = self.commit_completed_headers(completed_headers)?;

        if fetch_result.is_ok() && synced_tip != tip {
            bail!(
                "synced tip does not match the daemon tip after a complete fetch daemon_tip='{}' synced_tip='{}'",
                tip,
                synced_tip
            );
        }

        self.tip_metric
            .set(self.store.headers().best_height() as i64);

        Ok(synced_tip)
    }

    fn clean_up_stale_blocks(
        &mut self,
        daemon: &Daemon,
        new_headers: &[HeaderEntry],
        reorged_since: Option<usize>,
    ) -> Result<()> {
        let tip_height = new_headers.last().map_or(0, |h| h.height());
        // After a crash before the first tip commit, every indexed block is uncommitted,
        // so this diff covers the whole chain once. It still has to run: the chain may
        // have reorged while electrs was down.
        let stale = self.stale_uncommitted(new_headers);
        let rewrite_pending = reorg::pending_rewrite(&self.store)?.is_some();
        if reorged_since.is_none() && stale.is_empty() && !rewrite_pending {
            return Ok(());
        }
        let mut active_chain_view = reorg::ActiveChainView::new(
            Arc::clone(&self.store),
            new_headers,
            reorged_since,
            Some(daemon),
        );
        // Move the durable tip to the common ancestor first: both cleanups can delete
        // C/S rows shared with blocks above it.
        if let Some(height) = reorged_since {
            self.undo_reorged_blocks(daemon, height, tip_height, &mut active_chain_view)?;
        }
        if !stale.is_empty() {
            self.undo_uncommitted_blocks(daemon, stale, tip_height, &mut active_chain_view)?;
        }
        // Also completes a rewrite left pending by a crash in an earlier cleanup.
        self.finish_rewrite(&active_chain_view)
    }

    fn commit_completed_headers(&mut self, completed: Vec<HeaderEntry>) -> Result<BlockHash> {
        let tip = self.store.advance_synced_tip(completed)?;
        let headers = self.store.headers();
        self.uncommitted_blocks
            .retain(|hash| headers.header_by_blockhash(hash).is_none());
        Ok(tip)
    }

    // Address-search cache writes also disable the WAL during initial sync.
    fn finish_sync_writes(&mut self) {
        let databases = [
            ("txstore_db", &self.store.txstore_db),
            ("history_db", &self.store.history_db),
            ("cache_db", &self.store.cache_db),
        ];
        for (_, db) in databases {
            self.start_auto_compactions(db);
        }
        if let DBFlush::Disable = self.flush {
            for (name, db) in databases {
                let started = std::time::Instant::now();
                info!("flushing {} to disk", name);
                db.flush();
                info!("flushing {} complete in {:.1?}", name, started.elapsed());
            }
            self.flush = DBFlush::Enable;
        }
    }

    fn fetch_and_index(
        &mut self,
        daemon: &Daemon,
        headers: &[HeaderEntry],
        tip_height: usize,
    ) -> Result<()> {
        let result = self.fetch_index_batches(self.from, daemon, headers, tip_height);
        if let FetchFrom::BlkFiles = self.from {
            self.from = FetchFrom::Bitcoind;
        }
        result
    }

    fn fetch_index_batches(
        &self,
        from: FetchFrom,
        daemon: &Daemon,
        headers: &[HeaderEntry],
        tip_height: usize,
    ) -> Result<()> {
        let to_process = self.store.headers_to_process(headers);
        debug!(
            "processing {} blocks (add + index) using {:?}",
            to_process.len(),
            from
        );
        let total = to_process.len();
        let mut count = 0;
        start_fetcher(
            from,
            daemon,
            to_process,
            self.iconfig.block_batch_size,
            tip_height,
        )?
        .map(|blocks| {
            if count % 25 == 0 && total > 20 {
                let height = blocks.last().map_or(0, |b| b.entry.height());
                info!(
                    "processing blocks {}/{} ({:.1}%)",
                    height,
                    tip_height,
                    height as f32 / tip_height.max(1) as f32 * 100.0
                );
            }
            count += 1;
            self.process_block_batch(&blocks, tip_height);
            Ok(())
        })
    }

    // Keep add and index adjacent: the output rows are still in RocksDB's write buffer
    // when history indexing looks up prevouts. Done-markers make retries idempotent.
    fn process_block_batch(&self, blocks: &[BlockEntry], tip_height: usize) {
        let to_add: Vec<_> = {
            let added = self.store.added_blockhashes.read().unwrap();
            blocks
                .iter()
                .filter(|b| !added.contains(b.entry.hash()))
                .cloned()
                .collect()
        };
        let to_index: Vec<_> = {
            let indexed = self.store.indexed_blockhashes.read().unwrap();
            blocks
                .iter()
                .filter(|b| !indexed.contains(b.entry.hash()))
                .cloned()
                .collect()
        };
        if !to_add.is_empty() || !to_index.is_empty() {
            let _timer = self.start_timer("batch_total");
            if !to_add.is_empty() {
                self.add(&to_add);
            }
            if !to_index.is_empty() {
                self.index(&to_index);
            }
        }
        if let Some(last) = blocks.last() {
            let height = last.entry.height();
            self.sync_height.set(height as i64);
            if tip_height > 0 {
                self.sync_progress
                    .set(height as f64 / tip_height as f64 * 100.0);
            }
        }
    }

    fn add(&self, blocks: &[BlockEntry]) {
        // TODO: skip orphaned blocks?
        let rows = {
            let _timer = self.start_timer("add_process");
            add_blocks(blocks, &self.iconfig)
        };
        {
            let _timer = self.start_timer("add_write");
            self.store.txstore_db.write_rows(rows, self.flush);
        }

        self.store
            .added_blockhashes
            .write()
            .unwrap()
            .extend(blocks.iter().map(|b| b.entry.hash()));
    }

    fn index(&self, blocks: &[BlockEntry]) {
        let mut batch = rocksdb::WriteBatch::default();
        let mut rows = self._index(blocks);
        rows.par_sort_unstable_by(|a, b| a.key.cmp(&b.key));
        for row in rows {
            batch.put(row.key, row.value);
        }
        for block in blocks {
            batch.delete(reorg::undo_marker_key(block.entry.hash()));
        }
        self.store.history_db.write_batch(batch, self.flush);

        let mut indexed_blockhashes = self.store.indexed_blockhashes.write().unwrap();
        indexed_blockhashes.extend(blocks.iter().map(|b| b.entry.hash()));
    }

    fn _index(&self, blocks: &[BlockEntry]) -> Vec<DBRow> {
        let previous_txos_map = {
            let _timer = self.start_timer("index_lookup");
            lookup_txos(&self.store.txstore_db, get_previous_txos(blocks)).unwrap()
        };
        let rows = {
            let _timer = self.start_timer("index_process");
            let added_blockhashes = self.store.added_blockhashes.read().unwrap();
            for b in blocks {
                let blockhash = b.entry.hash();
                // TODO: replace by lookup into txstore_db?
                if !added_blockhashes.contains(blockhash) {
                    panic!("cannot index block {} (missing from store)", blockhash);
                }
            }
            index_blocks(blocks, &previous_txos_map, &self.iconfig)
        };
        rows
    }

    pub fn fetch_from(&mut self, from: FetchFrom) {
        self.from = from;
    }
}

impl ChainQuery {
    pub fn new(store: Arc<Store>, daemon: Arc<Daemon>, config: &Config, metrics: &Metrics) -> Self {
        ChainQuery {
            store,
            daemon,
            light_mode: config.light_mode,
            network: config.network_type,
            history_scan_limit: config.history_scan_limit,
            utxos_checkpoint_limit: config.utxos_checkpoint_limit,
            duration: metrics.histogram_vec(
                HistogramOpts::new("query_duration", "Index query duration (in seconds)"),
                &["name"],
            ),
            checkpoint_merkle_cache: CheckpointMerkleCache::new(
                config
                    .electrum_checkpoint_merkle_cache_mb
                    .saturating_mul(1024 * 1024),
            ),
        }
    }

    pub fn network(&self) -> Network {
        self.network
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    pub fn checkpoint_merkle_cache(&self) -> &CheckpointMerkleCache {
        &self.checkpoint_merkle_cache
    }

    fn start_timer(&self, name: &str) -> HistogramTimer {
        self.duration.with_label_values(&[name]).start_timer()
    }

    pub fn get_block_txids(&self, hash: &BlockHash) -> Option<Vec<Txid>> {
        let _timer = self.start_timer("get_block_txids");
        if self.light_mode {
            // TODO fetch block as binary from REST API instead of as hex
            let mut blockinfo = self.daemon.getblock_raw(hash, 1).ok()?;
            Some(serde_json::from_value(blockinfo["tx"].take()).unwrap())
        } else {
            self.store
                .txstore_db
                .get(&BlockRow::txids_key(full_hash(&hash[..])))
                .map(|val| bincode::deserialize_little(&val).expect("failed to parse block txids"))
        }
    }

    pub fn try_get_block_txids(&self, hash: &BlockHash) -> Result<Option<Vec<Txid>>> {
        let _timer = self.start_timer("get_block_txids");
        if self.light_mode {
            let mut blockinfo = match self.daemon.getblock_raw(hash, 1) {
                Ok(blockinfo) => blockinfo,
                Err(_) => return Ok(None),
            };
            Ok(Some(
                serde_json::from_value(blockinfo["tx"].take())
                    .chain_err(|| "failed to parse block txids")?,
            ))
        } else {
            self.store
                .txstore_db
                .try_get(&BlockRow::txids_key(full_hash(&hash[..])))?
                .map(|val| {
                    bincode::deserialize_little(&val).chain_err(|| "failed to parse block txids")
                })
                .transpose()
        }
    }

    pub fn get_block_txs(
        &self,
        hash: &BlockHash,
        start_index: usize,
        limit: usize,
    ) -> Result<Vec<Transaction>> {
        let txids = self
            .try_get_block_txids(hash)?
            .chain_err(|| "block not found")?;
        ensure!(
            start_index < txids.len(),
            ErrorKind::InvalidParams("start index out of range".to_string())
        );

        let txids_with_blockhash = txids
            .into_iter()
            .skip(start_index)
            .take(limit)
            .map(|txid| (txid, *hash))
            .collect::<Vec<_>>();

        self.lookup_txns(&txids_with_blockhash)

        // XXX use getblock in lightmode? a single RPC call, but would fetch all txs to get one page
        // self.daemon.getblock(hash)?.txdata.into_iter().skip(start_index).take(limit).collect()
    }

    pub fn get_block_meta(&self, hash: &BlockHash) -> Option<BlockMeta> {
        let _timer = self.start_timer("get_block_meta");

        if self.light_mode {
            let blockinfo = self.daemon.getblock_raw(hash, 1).ok()?;
            Some(serde_json::from_value(blockinfo).unwrap())
        } else {
            self.store
                .txstore_db
                .get(&BlockRow::meta_key(full_hash(&hash[..])))
                .map(|val| bincode::deserialize_little(&val).expect("failed to parse BlockMeta"))
        }
    }

    pub fn get_block_raw(&self, hash: &BlockHash) -> Option<Vec<u8>> {
        let _timer = self.start_timer("get_block_raw");

        if self.light_mode {
            let blockval = self.daemon.getblock_raw(hash, 0).ok()?;
            let blockhex = blockval.as_str().expect("valid block from bitcoind");
            Some(Vec::from_hex(blockhex).expect("valid block from bitcoind"))
        } else {
            let entry = self.header_by_hash(hash)?;
            let meta = self.get_block_meta(hash)?;
            let txids = self.get_block_txids(hash)?;
            let txids_with_blockhash: Vec<_> =
                txids.into_iter().map(|txid| (txid, *hash)).collect();
            let raw_txs = self.lookup_raw_txns(&txids_with_blockhash).ok()?; // TODO avoid hiding all errors as None, return a Result

            // Reconstruct the raw block using the header and txids,
            // as <raw header><tx count varint><raw txs>
            let mut raw = Vec::with_capacity(meta.size as usize);

            raw.append(&mut serialize(entry.header()));
            raw.append(&mut serialize(&VarInt(raw_txs.len() as u64)));

            for mut raw_tx in raw_txs {
                raw.append(&mut raw_tx);
            }

            Some(raw)
        }
    }

    pub fn get_block_header(&self, hash: &BlockHash) -> Option<BlockHeader> {
        let _timer = self.start_timer("get_block_header");
        Some(self.header_by_hash(hash)?.header().clone())
    }

    pub fn get_mtp(&self, height: usize) -> u32 {
        let _timer = self.start_timer("get_block_mtp");
        self.store.indexed_headers.read().unwrap().get_mtp(height)
    }

    pub fn get_block_with_meta(&self, hash: &BlockHash) -> Option<BlockHeaderMeta> {
        let _timer = self.start_timer("get_block_with_meta");
        let header_entry = self.header_by_hash(hash)?;
        Some(BlockHeaderMeta {
            meta: self.get_block_meta(hash)?,
            mtp: self.get_mtp(header_entry.height()),
            header_entry,
        })
    }

    pub fn history_iter_scan(
        &self,
        code: u8,
        hash: &[u8],
        start_height: usize,
    ) -> ScanIterator<'_> {
        self.store.history_db.iter_scan_from(
            &TxHistoryRow::filter(code, &hash[..]),
            &TxHistoryRow::prefix_height(code, &hash[..], start_height as u32),
        )
    }

    fn history_iter_scan_reverse(&self, code: u8, hash: &[u8]) -> ReverseScanIterator<'_> {
        self.store.history_db.iter_scan_reverse(
            &TxHistoryRow::filter(code, &hash[..]),
            &TxHistoryRow::prefix_end(code, &hash[..]),
        )
    }

    fn history_iter_scan_reverse_from(
        &self,
        code: u8,
        hash: &[u8],
        height: usize,
    ) -> ReverseScanIterator<'_> {
        self.store.history_db.iter_scan_reverse(
            &TxHistoryRow::filter(code, &hash[..]),
            &TxHistoryRow::prefix_height(code, &hash[..], height as u32 + 1),
        )
    }

    pub fn history(
        &self,
        scripthash: &[u8],
        last_seen_txid: Option<&Txid>,
        limit: usize,
    ) -> Result<Vec<(Transaction, BlockId)>> {
        // scripthash lookup
        self._history(b'H', scripthash, last_seen_txid, limit)
    }

    fn _history(
        &self,
        code: u8,
        hash: &[u8],
        last_seen_txid: Option<&Txid>,
        limit: usize,
    ) -> Result<Vec<(Transaction, BlockId)>> {
        let _timer_scan = self.start_timer("history");
        let headers = self.store.indexed_headers.read().unwrap();

        let seek_height = match last_seen_txid {
            Some(txid) => match self.tx_confirming_block_with_headers(txid, &headers) {
                Some(blockid) => Some(blockid.height),
                None => return Ok(vec![]),
            },
            None => None,
        };

        let raw_iter = match seek_height {
            Some(height) => self.history_iter_scan_reverse_from(code, hash, height),
            None => self.history_iter_scan_reverse(code, hash),
        };

        let mut scanned = 0usize;
        let mut hit_scan_limit = false;
        let scan_limit = self.history_scan_limit;
        let history_iter = raw_iter
            .take_while(|_| {
                scanned += 1;
                if scanned > scan_limit {
                    hit_scan_limit = true;
                    false
                } else {
                    true
                }
            })
            .map(TxHistoryRow::from_row)
            .map(|row| (row.get_txid(), row.key.confirmed_height as usize))
            // XXX: unique() requires keeping an in-memory list of all txids, can we avoid that?
            .unique()
            // skip up to and including last_seen_txid, stopping once the scan passes its height
            .scan(last_seen_txid.is_none(), |cursor_seen, (txid, height)| {
                if *cursor_seen {
                    Some(Some((txid, height)))
                } else if last_seen_txid == Some(&txid) {
                    *cursor_seen = true;
                    Some(None)
                } else if seek_height.map_or(false, |seek_height| height < seek_height) {
                    None
                } else {
                    Some(None)
                }
            })
            .flatten()
            // skip over entries that point to non-existing heights (may happen while new/reorged blocks are being processed)
            .filter_map(|(txid, height)| Some((txid, headers.header_by_height(height)?)))
            .take(limit);

        let mut txids_with_blockhash = Vec::with_capacity(limit);
        let mut blockids = Vec::with_capacity(limit);
        for (txid, header) in history_iter {
            txids_with_blockhash.push((txid, *header.hash()));
            blockids.push(BlockId::from(header));
        }
        drop(headers);

        if hit_scan_limit && txids_with_blockhash.len() < limit {
            bail!(ErrorKind::TooBigHistory);
        }

        Ok(self
            .lookup_txns(&txids_with_blockhash)?
            .into_iter()
            .zip(blockids)
            .map(|(tx, blockid)| (tx, blockid))
            .collect())
    }

    pub fn history_txids(&self, scripthash: &[u8], limit: usize) -> Vec<(Txid, BlockId)> {
        // scripthash lookup
        self._history_txids(b'H', scripthash, limit)
    }

    fn _history_txids(&self, code: u8, hash: &[u8], limit: usize) -> Vec<(Txid, BlockId)> {
        let _timer = self.start_timer("history_txids");
        let headers = self.store.indexed_headers.read().unwrap();
        self.history_iter_scan(code, hash, 0)
            .map(TxHistoryRow::from_row)
            .map(|row| (row.get_txid(), row.key.confirmed_height as usize))
            .unique()
            // skip over entries that point to non-existing heights (may happen while new/reorged blocks are being processed)
            .filter_map(|(txid, height)| Some((txid, headers.header_by_height(height)?.into())))
            .take(limit)
            .collect()
    }

    // TODO: avoid duplication with stats/stats_delta?
    pub fn utxo(&self, scripthash: &[u8], limit: usize) -> Result<Vec<Utxo>> {
        let _cache_guard = self.store.history_cache_lock.read().unwrap();
        let _timer = self.start_timer("utxo");

        // get the last known utxo set and the blockhash it was updated for.
        // invalidates the cache if the block was orphaned.
        let cached_bytes = self.store.cache_db.get(&UtxoCacheRow::key(scripthash));
        if let Some(ref bytes) = cached_bytes {
            if UtxoCacheRow::utxo_count(bytes) > self.utxos_checkpoint_limit {
                bail!(ErrorKind::TooBigHistory)
            }
        }
        let cache: Option<(UtxoMap, usize)> = cached_bytes
            .map(|c| bincode::deserialize_little(&c).unwrap())
            .and_then(|(utxos_cache, blockhash)| {
                self.height_by_hash(&blockhash)
                    .map(|height| (utxos_cache, height))
            })
            .map(|(utxos_cache, height)| (from_utxo_cache(utxos_cache, self), height));
        let had_cache = cache.is_some();

        // update utxo set with new transactions since
        let (newutxos, lastblock, processed_items, stop) = cache.map_or_else(
            || self.utxo_delta(scripthash, HashMap::new(), 0),
            |(oldutxos, blockheight)| self.utxo_delta(scripthash, oldutxos, blockheight + 1),
        );

        if stop == Some(UtxoScanStop::CheckpointLimit) {
            bail!(ErrorKind::TooBigHistory)
        }
        let hit_scan_limit = stop == Some(UtxoScanStop::ScanLimit);

        // save updated utxo set to cache, even if we bail out below
        if let Some(lastblock) = lastblock {
            if had_cache || hit_scan_limit || processed_items > MIN_HISTORY_ITEMS_TO_CACHE {
                self.store.cache_db.write_rows(
                    vec![UtxoCacheRow::new(scripthash, &newutxos, &lastblock).into_row()],
                    DBFlush::Enable,
                );
            }
        }

        if hit_scan_limit {
            bail!(ErrorKind::TooBigHistory)
        }

        if newutxos.len() > limit {
            bail!(ErrorKind::TooManyUtxos)
        }

        // format as Utxo objects
        Ok(newutxos
            .into_iter()
            .map(|(outpoint, (blockid, value))| {
                // in elements/liquid chains, we have to lookup the txo in order to get its
                // associated asset. the asset information could be kept in the db history rows
                // alongside the value to avoid this.
                #[cfg(feature = "liquid")]
                let txo = self.lookup_txo(&outpoint).expect("missing utxo");

                Utxo {
                    txid: outpoint.txid,
                    vout: outpoint.vout,
                    value,
                    confirmed: Some(blockid),

                    #[cfg(feature = "liquid")]
                    asset: txo.asset,
                    #[cfg(feature = "liquid")]
                    nonce: txo.nonce,
                    #[cfg(feature = "liquid")]
                    witness: txo.witness,
                }
            })
            .collect())
    }

    fn utxo_delta(
        &self,
        scripthash: &[u8],
        init_utxos: UtxoMap,
        start_height: usize,
    ) -> (UtxoMap, Option<BlockHash>, usize, Option<UtxoScanStop>) {
        let _timer = self.start_timer("utxo_delta");
        let headers = self.store.indexed_headers.read().unwrap();
        let history_iter = self
            .history_iter_scan(b'H', scripthash, start_height)
            .map(TxHistoryRow::from_row)
            // skip over entries that point to non-existing heights (may happen while new/reorged blocks are being processed)
            .filter_map(|history| {
                let header = headers.header_by_height(history.key.confirmed_height as usize)?;
                Some((history, BlockId::from(header)))
            });

        let mut utxos = init_utxos;
        let mut processed_items = 0;
        let mut lastblock = None;
        let mut safe_lastblock = None;
        let mut stop = None;

        for (history, blockid) in history_iter {
            if lastblock.is_some_and(|hash| hash != blockid.hash) {
                safe_lastblock = lastblock;
                if processed_items >= self.history_scan_limit {
                    stop = Some(UtxoScanStop::ScanLimit);
                    break;
                }
            }
            processed_items += 1;
            lastblock = Some(blockid.hash);

            match history.key.txinfo {
                TxHistoryInfo::Funding(ref info) => {
                    utxos.insert(history.get_funded_outpoint(), (blockid, info.value))
                }
                TxHistoryInfo::Spending(_) => utxos.remove(&history.get_funded_outpoint()),
                #[cfg(feature = "liquid")]
                TxHistoryInfo::Issuing(_)
                | TxHistoryInfo::Burning(_)
                | TxHistoryInfo::Pegin(_)
                | TxHistoryInfo::Pegout(_) => unreachable!(),
            };

            if utxos.len() > self.utxos_checkpoint_limit {
                return (
                    utxos,
                    None,
                    processed_items,
                    Some(UtxoScanStop::CheckpointLimit),
                );
            }
        }

        if stop.is_none() {
            safe_lastblock = lastblock;
        }

        (utxos, safe_lastblock, processed_items, stop)
    }

    pub fn stats(&self, scripthash: &[u8]) -> Result<ScriptStats> {
        let _cache_guard = self.store.history_cache_lock.read().unwrap();
        let _timer = self.start_timer("stats");

        // get the last known stats and the blockhash they are updated for.
        // invalidates the cache if the block was orphaned.
        let cache: Option<(ScriptStats, usize)> = self
            .store
            .cache_db
            .get(&StatsCacheRow::key(scripthash))
            .map(|c| bincode::deserialize_little(&c).unwrap())
            .and_then(|(stats, blockhash)| {
                self.height_by_hash(&blockhash)
                    .map(|height| (stats, height))
            });

        // update stats with new transactions since
        let (newstats, lastblock, hit_scan_limit) = cache.map_or_else(
            || self.stats_delta(scripthash, ScriptStats::default(), 0),
            |(oldstats, blockheight)| self.stats_delta(scripthash, oldstats, blockheight + 1),
        );

        // save updated stats to cache, even if we bail out below
        if let Some(lastblock) = lastblock {
            if hit_scan_limit
                || newstats.funded_txo_count + newstats.spent_txo_count > MIN_HISTORY_ITEMS_TO_CACHE
            {
                self.store.cache_db.write_rows(
                    vec![StatsCacheRow::new(scripthash, &newstats, &lastblock).into_row()],
                    DBFlush::Enable,
                );
            }
        }

        if hit_scan_limit {
            bail!(ErrorKind::TooBigHistory)
        }

        Ok(newstats)
    }

    fn stats_delta(
        &self,
        scripthash: &[u8],
        init_stats: ScriptStats,
        start_height: usize,
    ) -> (ScriptStats, Option<BlockHash>, bool) {
        let _timer = self.start_timer("stats_delta"); // TODO: measure also the number of txns processed.
        let headers = self.store.indexed_headers.read().unwrap();
        let history_iter = self
            .history_iter_scan(b'H', scripthash, start_height)
            .map(TxHistoryRow::from_row)
            // skip over entries that point to non-existing heights (may happen while new/reorged blocks are being processed)
            .filter_map(|history| {
                let header = headers.header_by_height(history.key.confirmed_height as usize)?;
                Some((history, BlockId::from(header)))
            });

        let mut stats = init_stats;
        let mut seen_txids = HashSet::new();
        let mut lastblock = None;
        let mut safe_lastblock = None;
        let mut processed_items = 0;
        let mut hit_scan_limit = false;

        for (history, blockid) in history_iter {
            if lastblock != Some(blockid.hash) {
                safe_lastblock = lastblock;
                seen_txids.clear();
                if processed_items >= self.history_scan_limit {
                    hit_scan_limit = true;
                    break;
                }
            }
            processed_items += 1;

            if seen_txids.insert(history.get_txid()) {
                stats.tx_count += 1;
            }

            match history.key.txinfo {
                #[cfg(not(feature = "liquid"))]
                TxHistoryInfo::Funding(ref info) => {
                    stats.funded_txo_count += 1;
                    stats.funded_txo_sum += info.value;
                }

                #[cfg(not(feature = "liquid"))]
                TxHistoryInfo::Spending(ref info) => {
                    stats.spent_txo_count += 1;
                    stats.spent_txo_sum += info.value;
                }

                #[cfg(feature = "liquid")]
                TxHistoryInfo::Funding(_) => {
                    stats.funded_txo_count += 1;
                }

                #[cfg(feature = "liquid")]
                TxHistoryInfo::Spending(_) => {
                    stats.spent_txo_count += 1;
                }

                #[cfg(feature = "liquid")]
                TxHistoryInfo::Issuing(_)
                | TxHistoryInfo::Burning(_)
                | TxHistoryInfo::Pegin(_)
                | TxHistoryInfo::Pegout(_) => unreachable!(),
            }

            lastblock = Some(blockid.hash);
        }

        if !hit_scan_limit {
            safe_lastblock = lastblock;
        }

        (stats, safe_lastblock, hit_scan_limit)
    }

    pub fn address_search(&self, prefix: &str, limit: usize) -> Vec<String> {
        let _timer_scan = self.start_timer("address_search");
        self.store
            .txstore_db
            .iter_scan(&addr_search_filter(prefix))
            .take(limit)
            .map(|row| std::str::from_utf8(&row.key[1..]).unwrap().to_string())
            .collect()
    }

    fn header_by_hash(&self, hash: &BlockHash) -> Option<HeaderEntry> {
        self.store
            .indexed_headers
            .read()
            .unwrap()
            .header_by_blockhash(hash)
            .cloned()
    }

    // Get the height of a blockhash, only if its part of the best chain
    pub fn height_by_hash(&self, hash: &BlockHash) -> Option<usize> {
        self.store
            .indexed_headers
            .read()
            .unwrap()
            .header_by_blockhash(hash)
            .map(|header| header.height())
    }

    pub fn header_by_height(&self, height: usize) -> Option<HeaderEntry> {
        self.store
            .indexed_headers
            .read()
            .unwrap()
            .header_by_height(height)
            .cloned()
    }

    pub fn hash_by_height(&self, height: usize) -> Option<BlockHash> {
        self.store
            .indexed_headers
            .read()
            .unwrap()
            .header_by_height(height)
            .map(|entry| *entry.hash())
    }

    pub fn blockid_by_height(&self, height: usize) -> Option<BlockId> {
        self.store
            .indexed_headers
            .read()
            .unwrap()
            .header_by_height(height)
            .map(BlockId::from)
    }

    // returns None for orphaned blocks
    pub fn blockid_by_hash(&self, hash: &BlockHash) -> Option<BlockId> {
        self.store
            .indexed_headers
            .read()
            .unwrap()
            .header_by_blockhash(hash)
            .map(BlockId::from)
    }

    /// Get the chain tip height. Panics if called on an empty HeaderList.
    pub fn best_height(&self) -> usize {
        self.store.indexed_headers.read().unwrap().best_height()
    }

    pub fn best_hash(&self) -> BlockHash {
        *self.store.indexed_headers.read().unwrap().tip()
    }

    pub fn best_header(&self) -> HeaderEntry {
        let headers = self.store.indexed_headers.read().unwrap();
        headers
            .header_by_blockhash(headers.tip())
            .expect("missing chain tip")
            .clone()
    }

    pub fn lookup_txns(&self, txids: &[(Txid, BlockHash)]) -> Result<Vec<Transaction>> {
        let _timer = self.start_timer("lookup_txns");
        self.lookup_raw_txns(txids)?
            .into_iter()
            .map(|rawtx| deserialize(&rawtx).chain_err(|| "failed to parse Transaction"))
            .collect()
    }

    pub fn lookup_txn(
        &self,
        txid: &Txid,
        blockhash: Option<&BlockHash>,
    ) -> Result<Option<Transaction>> {
        let _timer = self.start_timer("lookup_txn");
        self.try_lookup_raw_txn(txid, blockhash)?
            .map(|rawtx| deserialize(&rawtx).chain_err(|| "failed to parse Transaction"))
            .transpose()
    }

    pub fn lookup_raw_txns(&self, txids: &[(Txid, BlockHash)]) -> Result<Vec<Bytes>> {
        let _timer = self.start_timer("lookup_raw_txns");
        if self.light_mode {
            txids
                .par_iter()
                .map(|(txid, blockhash)| {
                    self.lookup_raw_txn(txid, Some(blockhash))
                        .chain_err(|| "missing tx")
                })
                .collect()
        } else {
            let keys = txids.iter().map(|(txid, _)| TxRow::key(&txid[..]));
            self.store
                .txstore_db
                .multi_get(keys)
                .into_iter()
                .map(|val| val?.chain_err(|| "missing tx"))
                .collect()
        }
    }

    pub fn lookup_raw_txn(&self, txid: &Txid, blockhash: Option<&BlockHash>) -> Option<Bytes> {
        let _timer = self.start_timer("lookup_raw_txn");

        if self.light_mode {
            let queried_blockhash =
                blockhash.map_or_else(|| self.tx_confirming_block(txid).map(|b| b.hash), |_| None);
            let blockhash = blockhash.or_else(|| queried_blockhash.as_ref())?;
            // TODO fetch transaction as binary from REST API instead of as hex
            let txval = self
                .daemon
                .gettransaction_raw(txid, blockhash, false)
                .ok()?;
            let txhex = txval.as_str().expect("valid tx from bitcoind");
            Some(Bytes::from_hex(txhex).expect("valid tx from bitcoind"))
        } else {
            self.store.txstore_db.get(&TxRow::key(&txid[..]))
        }
    }

    fn try_lookup_raw_txn(
        &self,
        txid: &Txid,
        blockhash: Option<&BlockHash>,
    ) -> Result<Option<Bytes>> {
        if self.light_mode {
            let queried_blockhash = if blockhash.is_none() {
                self.try_tx_confirming_block(txid)?
                    .map(|blockid| blockid.hash)
            } else {
                None
            };
            let blockhash = match blockhash.or(queried_blockhash.as_ref()) {
                Some(blockhash) => blockhash,
                None => return Ok(None),
            };
            let txval = match self.daemon.gettransaction_raw(txid, blockhash, false) {
                Ok(txval) => txval,
                Err(_) => return Ok(None),
            };
            let txhex = txval
                .as_str()
                .chain_err(|| "invalid transaction RPC response")?;
            Ok(Some(
                Bytes::from_hex(txhex).chain_err(|| "invalid transaction hex")?,
            ))
        } else {
            Ok(self.store.txstore_db.try_get(&TxRow::key(&txid[..]))?)
        }
    }

    pub fn lookup_txo(&self, outpoint: &OutPoint) -> Option<TxOut> {
        let _timer = self.start_timer("lookup_txo");
        lookup_txo(&self.store.txstore_db, outpoint)
    }

    pub fn lookup_txos(&self, outpoints: BTreeSet<OutPoint>) -> Result<HashMap<OutPoint, TxOut>> {
        let _timer = self.start_timer("lookup_txos");
        lookup_txos(&self.store.txstore_db, outpoints)
    }

    pub fn lookup_spend(&self, outpoint: &OutPoint) -> Option<SpendingInput> {
        let _timer = self.start_timer("lookup_spend");
        let edge = TxEdgeValue::from_bytes(&self.store.history_db.get(&TxEdgeRow::key(outpoint))?);
        let headers = self.store.indexed_headers.read().unwrap();
        // skip over entries that point to non-existing heights (may happen while new/reorged blocks are being processed)
        let header = headers.header_by_height(edge.spending_height as usize)?;
        Some(SpendingInput {
            txid: deserialize(&edge.spending_txid).expect("failed to parse Txid"),
            vin: edge.spending_vin as u32,
            confirmed: Some(header.into()),
        })
    }

    pub fn lookup_spends(&self, outpoints: BTreeSet<OutPoint>) -> HashMap<OutPoint, SpendingInput> {
        let _timer = self.start_timer("lookup_spends");
        let headers = self.store.indexed_headers.read().unwrap();
        self.store
            .history_db
            .multi_get(outpoints.iter().map(TxEdgeRow::key))
            .into_iter()
            .zip(outpoints)
            .filter_map(|(edge_val, outpoint)| {
                let edge = TxEdgeValue::from_bytes(&edge_val.unwrap()?);
                // skip over entries that point to non-existing heights (may happen while new/reorged blocks are being processed)
                let header = headers.header_by_height(edge.spending_height as usize)?;
                Some((
                    outpoint,
                    SpendingInput {
                        txid: deserialize(&edge.spending_txid).expect("failed to parse Txid"),
                        vin: edge.spending_vin,
                        confirmed: Some(header.into()),
                    },
                ))
            })
            .collect()
    }

    pub fn tx_confirming_block(&self, txid: &Txid) -> Option<BlockId> {
        let _timer = self.start_timer("tx_confirming_block");
        let headers = self.store.indexed_headers.read().unwrap();
        self.tx_confirming_block_with_headers(txid, &headers)
    }

    fn tx_confirming_block_with_headers(
        &self,
        txid: &Txid,
        headers: &HeaderList,
    ) -> Option<BlockId> {
        let row_value = self.store.history_db.get(&TxConfRow::key(txid))?;
        let height = TxConfRow::height_from_val(&row_value);
        // skip over entries that point to non-existing heights (may happen while new/reorged blocks are being processed)
        Some(headers.header_by_height(height as usize)?.into())
    }

    pub fn try_tx_confirming_block(&self, txid: &Txid) -> Result<Option<BlockId>> {
        let _timer = self.start_timer("tx_confirming_block");
        let row_value = match self.store.history_db.try_get(&TxConfRow::key(txid))? {
            Some(row_value) => row_value,
            None => return Ok(None),
        };
        ensure!(row_value.len() == 4, "invalid transaction confirmation row");
        let height = TxConfRow::height_from_val(&row_value);
        let headers = self.store.indexed_headers.read().unwrap();
        Ok(headers.header_by_height(height as usize).map(Into::into))
    }

    pub fn lookup_confirmations(&self, txids: BTreeSet<Txid>) -> HashMap<Txid, u32> {
        let _timer = self.start_timer("lookup_confirmations");
        let headers = self.store.indexed_headers.read().unwrap();
        lookup_confirmations(&self.store.history_db, headers.best_height() as u32, txids)
    }

    pub fn get_block_status(&self, hash: &BlockHash) -> BlockStatus {
        // TODO differentiate orphaned and non-existing blocks? telling them apart requires
        // an additional db read.

        let headers = self.store.indexed_headers.read().unwrap();

        // header_by_blockhash only returns blocks that are part of the best chain,
        // or None for orphaned blocks.
        headers
            .header_by_blockhash(hash)
            .map_or_else(BlockStatus::orphaned, |header| {
                BlockStatus::confirmed(
                    header.height(),
                    headers
                        .header_by_height(header.height() + 1)
                        .map(|h| *h.hash()),
                )
            })
    }

    #[cfg(not(feature = "liquid"))]
    pub fn get_merkleblock_proof(&self, txid: &Txid) -> Option<MerkleBlock> {
        let _timer = self.start_timer("get_merkleblock_proof");
        let blockid = self.tx_confirming_block(txid)?;
        let headerentry = self.header_by_hash(&blockid.hash)?;
        let block_txids = self.get_block_txids(&blockid.hash)?;

        Some(MerkleBlock::from_header_txids_with_predicate(
            headerentry.header(),
            &block_txids,
            |t| t == txid,
        ))
    }

    #[cfg(feature = "liquid")]
    pub fn asset_history(
        &self,
        asset_id: &AssetId,
        last_seen_txid: Option<&Txid>,
        limit: usize,
    ) -> Result<Vec<(Transaction, BlockId)>> {
        self._history(b'I', &asset_id.into_inner()[..], last_seen_txid, limit)
    }

    #[cfg(feature = "liquid")]
    pub fn asset_history_txids(&self, asset_id: &AssetId, limit: usize) -> Vec<(Txid, BlockId)> {
        self._history_txids(b'I', &asset_id.into_inner()[..], limit)
    }
}

fn load_blockhashes(db: &DB, prefix: &[u8]) -> HashSet<BlockHash> {
    db.iter_scan(prefix)
        .map(BlockRow::from_row)
        .map(|r| deserialize(&r.key.hash).expect("failed to parse BlockHash"))
        .collect()
}

fn load_blockheaders(db: &DB) -> HashMap<BlockHash, BlockHeader> {
    db.iter_scan(&BlockRow::header_filter())
        .map(BlockRow::from_row)
        .map(|r| {
            let key: BlockHash = deserialize(&r.key.hash).expect("failed to parse BlockHash");
            let value: BlockHeader = deserialize(&r.value).expect("failed to parse BlockHeader");
            (key, value)
        })
        .collect()
}

fn add_blocks(block_entries: &[BlockEntry], iconfig: &IndexerConfig) -> Vec<DBRow> {
    // persist individual transactions:
    //      T{txid} → {rawtx}
    //      O{txid}{index} → {txout}
    // persist block headers', block txids' and metadata rows:
    //      B{blockhash} → {header}
    //      X{blockhash} → {txid1}...{txidN}
    //      M{blockhash} → {tx_count}{size}{weight}
    block_entries
        .par_iter() // serialization is CPU-intensive
        .map(|b| {
            assert_eq!(b.txids.len(), b.block.txdata.len());
            let mut rows = vec![];
            let blockhash = full_hash(&b.entry.hash()[..]);
            for (tx, txid) in b.block.txdata.iter().zip(b.txids.iter()) {
                add_transaction(*txid, tx, &mut rows, iconfig);
            }

            if !iconfig.light_mode {
                rows.push(BlockRow::new_txids(blockhash, &b.txids).into_row());
                rows.push(BlockRow::new_meta(blockhash, &BlockMeta::from(b)).into_row());
            }

            rows.push(BlockRow::new_header(&b).into_row());
            rows.push(BlockRow::new_done(blockhash).into_row()); // mark block as "added"
            rows
        })
        .flatten()
        .collect()
}

fn add_transaction(txid: Txid, tx: &Transaction, rows: &mut Vec<DBRow>, iconfig: &IndexerConfig) {
    if !iconfig.light_mode {
        rows.push(TxRow::new(txid, tx).into_row());
    }

    let txid = full_hash(&txid[..]);
    for (txo_index, txo) in tx.output.iter().enumerate() {
        if is_spendable(txo) {
            rows.push(TxOutRow::new(&txid, txo_index, txo).into_row());
        }

        if iconfig.address_search {
            if let Some(row) = addr_search_row(&txo.script_pubkey, iconfig.network) {
                rows.push(row);
            }
        }
    }
}

fn get_previous_txos(block_entries: &[BlockEntry]) -> BTreeSet<OutPoint> {
    block_entries
        .iter()
        .flat_map(|b| b.block.txdata.iter())
        .flat_map(|tx| {
            tx.input
                .iter()
                .filter(|txin| has_prevout(txin))
                .map(|txin| txin.previous_output)
        })
        .collect()
}

fn lookup_txos(txstore_db: &DB, outpoints: BTreeSet<OutPoint>) -> Result<HashMap<OutPoint, TxOut>> {
    let keys = outpoints.iter().map(TxOutRow::key).collect::<Vec<_>>();
    txstore_db
        .multi_get(keys)
        .into_iter()
        .zip(outpoints)
        .map(|(res, outpoint)| {
            let txo = res?.ok_or_else(|| ErrorKind::MissingTxo(outpoint.to_string()))?;
            let txo = deserialize(&txo).chain_err(|| "failed to parse TxOut")?;
            Ok((outpoint, txo))
        })
        .collect()
}

fn lookup_txo(txstore_db: &DB, outpoint: &OutPoint) -> Option<TxOut> {
    txstore_db
        .get(&TxOutRow::key(&outpoint))
        .map(|val| deserialize(&val).expect("failed to parse TxOut"))
}

pub fn lookup_confirmations(
    history_db: &DB,
    tip_height: u32,
    txids: BTreeSet<Txid>,
) -> HashMap<Txid, u32> {
    history_db
        .multi_get(txids.iter().map(TxConfRow::key))
        .into_iter()
        .zip(txids)
        .filter_map(|(res, txid)| {
            let confirmation_height = u32::from_le_bytes(res.unwrap()?.try_into().unwrap());
            // skip over entries that point to non-existing heights (may happen while new/reorged blocks are being processed)
            (confirmation_height <= tip_height).then_some((txid, confirmation_height))
        })
        .collect()
}

fn index_blocks(
    block_entries: &[BlockEntry],
    previous_txos_map: &HashMap<OutPoint, TxOut>,
    iconfig: &IndexerConfig,
) -> Vec<DBRow> {
    block_entries
        .par_iter() // serialization is CPU-intensive
        .map(|b| {
            assert_eq!(b.txids.len(), b.block.txdata.len());
            let mut rows = vec![];
            let height = b.entry.height() as u32;
            for (tx, txid) in b.block.txdata.iter().zip(b.txids.iter()) {
                let txid_hash = full_hash(&txid[..]);
                index_transaction(tx, txid_hash, height, previous_txos_map, &mut rows, iconfig);
            }
            // The height lets cleanup rebuild a stale block's history keys without the daemon.
            let mut done = BlockRow::new_done(full_hash(&b.entry.hash()[..]));
            done.value = height.to_le_bytes().to_vec();
            rows.push(done.into_row());
            rows
        })
        .flatten()
        .collect()
}

// TODO: return an iterator?
fn index_transaction(
    tx: &Transaction,
    txid: FullHash,
    confirmed_height: u32,
    previous_txos_map: &HashMap<OutPoint, TxOut>,
    rows: &mut Vec<DBRow>,
    iconfig: &IndexerConfig,
) {
    // persist tx confirmation row:
    //      C{txid} → "{block_height}"
    rows.push(TxConfRow::new(txid, confirmed_height).into_row());

    // persist history index:
    //      H{funding-scripthash}{funding-height}F{funding-txid:vout} → ""
    //      H{funding-scripthash}{spending-height}S{spending-txid:vin}{funding-txid:vout} → ""
    // persist "edges" for fast is-this-TXO-spent check
    //      S{funding-txid:vout}{spending-txid:vin} → ""
    for (txo_index, txo) in tx.output.iter().enumerate() {
        if is_spendable(txo) || iconfig.index_unspendables {
            let history = TxHistoryRow::new(
                &txo.script_pubkey,
                confirmed_height,
                TxHistoryInfo::Funding(FundingInfo {
                    txid,
                    vout: txo_index as u32,
                    value: txo.value.amount_value(),
                }),
            );
            rows.push(history.into_row());
        }
    }
    for (txi_index, txi) in tx.input.iter().enumerate() {
        if !has_prevout(txi) {
            continue;
        }
        let prev_txo = previous_txos_map
            .get(&txi.previous_output)
            .unwrap_or_else(|| panic!("missing previous txo {}", txi.previous_output));

        let history = TxHistoryRow::new(
            &prev_txo.script_pubkey,
            confirmed_height,
            TxHistoryInfo::Spending(SpendingInfo {
                txid,
                vin: txi_index as u32,
                prev_txid: full_hash(&txi.previous_output.txid[..]),
                prev_vout: txi.previous_output.vout,
                value: prev_txo.value.amount_value(),
            }),
        );
        rows.push(history.into_row());

        let edge = TxEdgeRow::new(
            full_hash(&txi.previous_output.txid[..]),
            txi.previous_output.vout,
            txid,
            txi_index as u32,
            confirmed_height,
        );
        rows.push(edge.into_row());
    }

    // Index issued assets & native asset pegins/pegouts/burns
    #[cfg(feature = "liquid")]
    asset::index_confirmed_tx_assets(
        tx,
        confirmed_height,
        iconfig.network,
        iconfig.parent_network,
        rows,
    );
}

fn addr_search_row(spk: &Script, network: Network) -> Option<DBRow> {
    spk.to_address_str(network).map(|address| DBRow {
        key: [b"a", address.as_bytes()].concat(),
        value: vec![],
    })
}

fn addr_search_filter(prefix: &str) -> Bytes {
    [b"a", prefix.as_bytes()].concat()
}

// TODO: replace by a separate opaque type (similar to Sha256dHash, but without the "double")
pub type FullHash = [u8; 32]; // serialized SHA256 result

pub fn compute_script_hash(script: &Script) -> FullHash {
    sha256::Hash::hash(script.as_bytes()).to_byte_array()
}

pub fn parse_hash(hash: &FullHash) -> Sha256dHash {
    deserialize(hash).expect("failed to parse Sha256dHash")
}

#[derive(Serialize, Deserialize)]
struct TxRowKey {
    code: u8,
    txid: FullHash,
}

struct TxRow {
    key: TxRowKey,
    value: Bytes, // raw transaction
}

impl TxRow {
    fn new(txid: Txid, txn: &Transaction) -> TxRow {
        let txid = full_hash(&txid[..]);
        TxRow {
            key: TxRowKey { code: b'T', txid },
            value: serialize(txn),
        }
    }

    fn key(prefix: &[u8]) -> Bytes {
        [b"T", prefix].concat()
    }

    fn into_row(self) -> DBRow {
        let TxRow { key, value } = self;
        DBRow {
            key: bincode::serialize_little(&key).unwrap(),
            value,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct TxConfKey {
    code: u8,
    txid: FullHash,
}

pub struct TxConfRow {
    key: TxConfKey,
    value: u32, // the confirmation height
}

impl TxConfRow {
    pub fn new(txid: FullHash, height: u32) -> TxConfRow {
        TxConfRow {
            key: TxConfKey { code: b'C', txid },
            value: height,
        }
    }

    pub fn key(txid: &Txid) -> Bytes {
        bincode::serialize_little(&TxConfKey {
            code: b'C',
            txid: full_hash(&txid[..]),
        })
        .unwrap()
    }

    pub fn into_row(self) -> DBRow {
        DBRow {
            key: bincode::serialize_little(&self.key).unwrap(),
            value: self.value.to_le_bytes().to_vec(),
        }
    }

    fn height_from_val(val: &[u8]) -> u32 {
        u32::from_le_bytes(val.try_into().expect("invalid TxConf value"))
    }
}

#[derive(Serialize, Deserialize)]
struct TxOutKey {
    code: u8,
    txid: FullHash,
    vout: u32,
}

struct TxOutRow {
    key: TxOutKey,
    value: Bytes, // serialized output
}

impl TxOutRow {
    fn new(txid: &FullHash, vout: usize, txout: &TxOut) -> TxOutRow {
        TxOutRow {
            key: TxOutKey {
                code: b'O',
                txid: *txid,
                vout: vout as u32,
            },
            value: serialize(txout),
        }
    }
    fn key(outpoint: &OutPoint) -> Bytes {
        bincode::serialize_little(&TxOutKey {
            code: b'O',
            txid: full_hash(&outpoint.txid[..]),
            vout: outpoint.vout,
        })
        .unwrap()
    }

    fn into_row(self) -> DBRow {
        DBRow {
            key: bincode::serialize_little(&self.key).unwrap(),
            value: self.value,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct BlockKey {
    code: u8,
    hash: FullHash,
}

struct BlockRow {
    key: BlockKey,
    value: Bytes, // serialized output
}

impl BlockRow {
    fn new_header(block_entry: &BlockEntry) -> BlockRow {
        BlockRow {
            key: BlockKey {
                code: b'B',
                hash: full_hash(&block_entry.entry.hash()[..]),
            },
            value: serialize(&block_entry.block.header),
        }
    }

    fn new_txids(hash: FullHash, txids: &[Txid]) -> BlockRow {
        BlockRow {
            key: BlockKey { code: b'X', hash },
            value: bincode::serialize_little(txids).unwrap(),
        }
    }

    fn new_meta(hash: FullHash, meta: &BlockMeta) -> BlockRow {
        BlockRow {
            key: BlockKey { code: b'M', hash },
            value: bincode::serialize_little(meta).unwrap(),
        }
    }

    fn new_done(hash: FullHash) -> BlockRow {
        BlockRow {
            key: BlockKey { code: b'D', hash },
            value: vec![],
        }
    }

    fn header_filter() -> Bytes {
        b"B".to_vec()
    }

    fn txids_key(hash: FullHash) -> Bytes {
        [b"X", &hash[..]].concat()
    }

    fn meta_key(hash: FullHash) -> Bytes {
        [b"M", &hash[..]].concat()
    }

    fn done_filter() -> Bytes {
        b"D".to_vec()
    }

    fn into_row(self) -> DBRow {
        DBRow {
            key: bincode::serialize_little(&self.key).unwrap(),
            value: self.value,
        }
    }

    fn from_row(row: DBRow) -> Self {
        BlockRow {
            key: bincode::deserialize_little(&row.key).unwrap(),
            value: row.value,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FundingInfo {
    pub txid: FullHash,
    pub vout: u32,
    pub value: Value,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SpendingInfo {
    pub txid: FullHash, // spending transaction
    pub vin: u32,
    pub prev_txid: FullHash, // funding transaction
    pub prev_vout: u32,
    pub value: Value,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum TxHistoryInfo {
    Funding(FundingInfo),
    Spending(SpendingInfo),

    #[cfg(feature = "liquid")]
    Issuing(asset::IssuingInfo),
    #[cfg(feature = "liquid")]
    Burning(asset::BurningInfo),
    #[cfg(feature = "liquid")]
    Pegin(peg::PeginInfo),
    #[cfg(feature = "liquid")]
    Pegout(peg::PegoutInfo),
}

impl TxHistoryInfo {
    pub fn get_txid(&self) -> Txid {
        match self {
            TxHistoryInfo::Funding(FundingInfo { txid, .. })
            | TxHistoryInfo::Spending(SpendingInfo { txid, .. }) => deserialize(txid),

            #[cfg(feature = "liquid")]
            TxHistoryInfo::Issuing(asset::IssuingInfo { txid, .. })
            | TxHistoryInfo::Burning(asset::BurningInfo { txid, .. })
            | TxHistoryInfo::Pegin(peg::PeginInfo { txid, .. })
            | TxHistoryInfo::Pegout(peg::PegoutInfo { txid, .. }) => deserialize(txid),
        }
        .expect("cannot parse Txid")
    }
}

#[derive(Serialize, Deserialize)]
pub struct TxHistoryKey {
    pub code: u8,              // H for script history or I for asset history (elements only)
    pub hash: FullHash, // either a scripthash (always on bitcoin) or an asset id (elements only)
    pub confirmed_height: u32, // MUST be serialized as big-endian (for correct scans).
    pub txinfo: TxHistoryInfo,
}

pub struct TxHistoryRow {
    pub key: TxHistoryKey,
}

impl TxHistoryRow {
    fn new(script: &Script, confirmed_height: u32, txinfo: TxHistoryInfo) -> Self {
        let key = TxHistoryKey {
            code: b'H',
            hash: compute_script_hash(&script),
            confirmed_height,
            txinfo,
        };
        TxHistoryRow { key }
    }

    fn filter(code: u8, hash_prefix: &[u8]) -> Bytes {
        [&[code], hash_prefix].concat()
    }

    fn prefix_end(code: u8, hash: &[u8]) -> Bytes {
        bincode::serialize_big(&(code, full_hash(&hash[..]), std::u32::MAX)).unwrap()
    }

    fn prefix_height(code: u8, hash: &[u8], height: u32) -> Bytes {
        bincode::serialize_big(&(code, full_hash(&hash[..]), height)).unwrap()
    }

    pub fn into_row(self) -> DBRow {
        DBRow {
            key: bincode::serialize_big(&self.key).unwrap(),
            value: vec![],
        }
    }

    pub fn from_row(row: DBRow) -> Self {
        let key = bincode::deserialize_big(&row.key).expect("failed to deserialize TxHistoryKey");
        TxHistoryRow { key }
    }

    pub fn get_txid(&self) -> Txid {
        self.key.txinfo.get_txid()
    }
    fn get_funded_outpoint(&self) -> OutPoint {
        self.key.txinfo.get_funded_outpoint()
    }
}

impl TxHistoryInfo {
    // for funding rows, returns the funded output.
    // for spending rows, returns the spent previous output.
    pub fn get_funded_outpoint(&self) -> OutPoint {
        match self {
            TxHistoryInfo::Funding(ref info) => OutPoint {
                txid: deserialize(&info.txid).unwrap(),
                vout: info.vout,
            },
            TxHistoryInfo::Spending(ref info) => OutPoint {
                txid: deserialize(&info.prev_txid).unwrap(),
                vout: info.prev_vout,
            },
            #[cfg(feature = "liquid")]
            TxHistoryInfo::Issuing(_)
            | TxHistoryInfo::Burning(_)
            | TxHistoryInfo::Pegin(_)
            | TxHistoryInfo::Pegout(_) => unreachable!(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct TxEdgeKey {
    code: u8,
    funding_txid: FullHash,
    funding_vout: u32,
}

#[derive(Serialize, Deserialize)]
pub struct TxEdgeValue {
    spending_txid: FullHash,
    spending_vin: u32,
    spending_height: u32,
}

pub struct TxEdgeRow {
    key: TxEdgeKey,
    value: TxEdgeValue,
}

impl TxEdgeRow {
    pub fn new(
        funding_txid: FullHash,
        funding_vout: u32,
        spending_txid: FullHash,
        spending_vin: u32,
        spending_height: u32,
    ) -> Self {
        let key = TxEdgeKey {
            code: b'S',
            funding_txid,
            funding_vout,
        };
        let value = TxEdgeValue {
            spending_txid,
            spending_vin,
            spending_height,
        };
        TxEdgeRow { key, value }
    }

    fn key(outpoint: &OutPoint) -> Bytes {
        bincode::serialize_little(&(b'S', full_hash(&outpoint.txid[..]), outpoint.vout)).unwrap()
    }

    pub fn into_row(self) -> DBRow {
        DBRow {
            key: bincode::serialize_little(&self.key).unwrap(),
            value: bincode::serialize_little(&self.value).unwrap(),
        }
    }
}

impl TxEdgeValue {
    fn from_bytes(bytes: &[u8]) -> Self {
        bincode::deserialize_little(bytes).expect("invalid TxEdgeValue")
    }
}

#[derive(Serialize, Deserialize)]
struct ScriptCacheKey {
    code: u8,
    scripthash: FullHash,
}

struct StatsCacheRow {
    key: ScriptCacheKey,
    value: Bytes,
}

impl StatsCacheRow {
    fn new(scripthash: &[u8], stats: &ScriptStats, blockhash: &BlockHash) -> Self {
        StatsCacheRow {
            key: ScriptCacheKey {
                code: b'A',
                scripthash: full_hash(scripthash),
            },
            value: bincode::serialize_little(&(stats, blockhash)).unwrap(),
        }
    }

    pub fn key(scripthash: &[u8]) -> Bytes {
        [b"A", scripthash].concat()
    }

    fn into_row(self) -> DBRow {
        DBRow {
            key: bincode::serialize_little(&self.key).unwrap(),
            value: self.value,
        }
    }
}

type CachedUtxoMap = HashMap<(Txid, u32), (u32, Value)>; // (txid,vout) => (block_height,output_value)

struct UtxoCacheRow {
    key: ScriptCacheKey,
    value: Bytes,
}

impl UtxoCacheRow {
    fn new(scripthash: &[u8], utxos: &UtxoMap, blockhash: &BlockHash) -> Self {
        let utxos_cache = make_utxo_cache(utxos);

        UtxoCacheRow {
            key: ScriptCacheKey {
                code: b'U',
                scripthash: full_hash(scripthash),
            },
            value: bincode::serialize_little(&(utxos_cache, blockhash)).unwrap(),
        }
    }

    pub fn key(scripthash: &[u8]) -> Bytes {
        [b"U", scripthash].concat()
    }

    /// Number of utxos in a serialized row, read from the map's length prefix without
    /// deserializing the entries.
    fn utxo_count(value: &[u8]) -> usize {
        value
            .get(..8)
            .and_then(|prefix| bincode::deserialize_little::<u64>(prefix).ok())
            .map_or(0, |count| {
                std::convert::TryFrom::try_from(count).unwrap_or(usize::MAX)
            })
    }

    fn into_row(self) -> DBRow {
        DBRow {
            key: bincode::serialize_little(&self.key).unwrap(),
            value: self.value,
        }
    }
}

// keep utxo cache with just the block height (the hash/timestamp are read later from the headers to reconstruct BlockId)
// and use a (txid,vout) tuple instead of OutPoints (they don't play nicely with bincode serialization)
fn make_utxo_cache(utxos: &UtxoMap) -> CachedUtxoMap {
    utxos
        .iter()
        .map(|(outpoint, (blockid, value))| {
            (
                (outpoint.txid, outpoint.vout),
                (blockid.height as u32, *value),
            )
        })
        .collect()
}

fn from_utxo_cache(utxos_cache: CachedUtxoMap, chain: &ChainQuery) -> UtxoMap {
    utxos_cache
        .into_iter()
        .map(|((txid, vout), (height, value))| {
            let outpoint = OutPoint { txid, vout };
            let blockid = chain
                .blockid_by_height(height as usize)
                .expect("missing blockheader for valid utxo cache entry");
            (outpoint, (blockid, value))
        })
        .collect()
}

// Get the amount value as gets stored in the DB and mempool tracker.
// For bitcoin it is the Amount's inner u64, for elements it is the confidential::Value itself.
pub trait GetAmountVal {
    #[cfg(not(feature = "liquid"))]
    fn amount_value(self) -> u64;
    #[cfg(feature = "liquid")]
    fn amount_value(self) -> confidential::Value;
}

#[cfg(not(feature = "liquid"))]
impl GetAmountVal for bitcoin::Amount {
    fn amount_value(self) -> u64 {
        self.to_sat()
    }
}
#[cfg(feature = "liquid")]
impl GetAmountVal for confidential::Value {
    fn amount_value(self) -> confidential::Value {
        self
    }
}

// This is needed to bench private functions
#[cfg(feature = "bench")]
pub mod bench {
    use crate::new_index::schema::IndexerConfig;
    use crate::new_index::BlockEntry;
    use crate::new_index::DBRow;
    use crate::util::HeaderEntry;
    use bitcoin::Block;

    pub struct Data {
        block_entry: BlockEntry,
        iconfig: IndexerConfig,
    }

    impl Data {
        pub fn new(block: Block) -> Data {
            let iconfig = IndexerConfig {
                light_mode: false,
                address_search: false,
                index_unspendables: false,
                network: crate::chain::Network::Regtest,
                block_batch_size: 250,
            };
            let height = 702861;
            let hash = block.block_hash();
            let header = block.header.clone();
            let txids = block.txdata.iter().map(|tx| tx.compute_txid()).collect();
            let block_entry = BlockEntry {
                txids,
                block,
                entry: HeaderEntry::new(height, hash, header),
                size: 0u32, // wrong but not needed for benching
            };

            Data {
                block_entry,
                iconfig,
            }
        }
    }

    pub fn add_blocks(data: &Data) -> Vec<DBRow> {
        super::add_blocks(&[data.block_entry.clone()], &data.iconfig)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_common as common;

    #[cfg(not(feature = "liquid"))]
    #[test]
    fn partial_mempool_sync_reports_failure_and_keeps_resolvable_txs() -> common::Result<()> {
        use crate::new_index::{Mempool, MempoolSyncStatus};
        use bitcoin::consensus::encode::serialize_hex;
        use serde_json::{json, Value};

        let mut tester = common::TestRunner::new()?;
        let address = tester.newaddress()?;
        let funding_txid = tester.send_multi(&address, bitcoin::Amount::ONE_BTC, 2)?;
        tester.mine()?;

        let spend = |vout: u32| -> common::Result<Txid> {
            let tx = Transaction {
                version: bitcoin::transaction::Version::TWO,
                lock_time: bitcoin::absolute::LockTime::ZERO,
                input: vec![bitcoin::TxIn {
                    previous_output: OutPoint::new(funding_txid, vout),
                    ..Default::default()
                }],
                output: vec![TxOut {
                    value: bitcoin::Amount::from_sat(99_990_000),
                    script_pubkey: tester.newaddress()?.script_pubkey(),
                }],
            };
            let signed: Value = tester
                .node_client()
                .call("signrawtransactionwithwallet", &[json!(serialize_hex(&tx))])?;
            Ok(tester
                .node_client()
                .call("sendrawtransaction", &[signed["hex"].clone()])?)
        };
        let unresolvable_txid = spend(0)?;
        let resolvable_txid = spend(1)?;

        tester.query().chain().store.txstore_db.delete_rows(
            vec![DBRow {
                key: TxOutRow::key(&OutPoint::new(funding_txid, 0)),
                value: vec![],
            }],
            DBFlush::Enable,
        );

        let tip = tester.get_best_block_hash()?;
        assert_eq!(
            Mempool::update(tester.mempool(), tester.daemon(), &tip)?,
            MempoolSyncStatus::FailedToIndex
        );
        let mempool = tester.mempool().read().unwrap();
        assert!(mempool.lookup_txn(&resolvable_txid).is_some());
        assert!(mempool.lookup_txn(&unresolvable_txid).is_none());
        Ok(())
    }

    #[test]
    fn corrupt_single_key_rows_return_errors() -> common::Result<()> {
        let mut tester = common::TestRunner::new()?;
        let address = tester.newaddress()?;
        let txid = tester.send(&address, "1 BTC".parse().unwrap())?;
        let blockhash = tester.mine()?;
        let chain = tester.query().chain();

        chain
            .store
            .txstore_db
            .put(&BlockRow::txids_key(full_hash(&blockhash[..])), &[]);
        assert!(chain.get_block_txs(&blockhash, 0, 25).is_err());

        chain.store.history_db.put(&TxConfRow::key(&txid), &[]);
        assert!(chain.try_tx_confirming_block(&txid).is_err());
        Ok(())
    }

    #[cfg(not(feature = "liquid"))]
    #[test]
    fn utxo_cache_row_count_reads_the_serialized_length() {
        let blockid = BlockId {
            height: 7,
            hash: BlockHash::all_zeros(),
            time: 0,
        };
        let utxos: UtxoMap = (0..3u32)
            .map(|vout| (OutPoint::new(Txid::all_zeros(), vout), (blockid, 1_000)))
            .collect();
        let row = UtxoCacheRow::new(&[0u8; 32], &utxos, &BlockHash::all_zeros());
        assert_eq!(UtxoCacheRow::utxo_count(&row.value), 3);
        assert_eq!(UtxoCacheRow::utxo_count(&[]), 0);
    }

    #[test]
    fn test_completed_prefix_stops_at_first_gap() {
        let items = [true, true, false, true];
        assert_eq!(completed_prefix_len(&items, |complete| *complete), 2);
    }

    #[cfg(not(feature = "liquid"))]
    pub(super) fn test_header(prev_blockhash: BlockHash) -> BlockHeader {
        BlockHeader {
            version: bitcoin::block::Version::ONE,
            prev_blockhash,
            merkle_root: crate::chain::TxMerkleNode::all_zeros(),
            time: 0,
            bits: bitcoin::CompactTarget::from_consensus(0x207f_ffff),
            nonce: 0,
        }
    }

    #[cfg(feature = "liquid")]
    pub(super) fn test_header(prev_blockhash: BlockHash) -> BlockHeader {
        BlockHeader {
            version: 1,
            prev_blockhash,
            merkle_root: crate::chain::TxMerkleNode::all_zeros(),
            time: 0,
            height: 0,
            ext: elements::BlockExtData::Proof {
                challenge: Script::new(),
                solution: Script::new(),
            },
        }
    }

    /// A chain of `count` linked headers starting at height 0.
    fn test_chain(count: usize) -> Vec<HeaderEntry> {
        let mut prev = *crate::util::DEFAULT_BLOCKHASH;
        (0..count)
            .map(|height| {
                let header = test_header(prev);
                prev = header.block_hash();
                HeaderEntry::new(height, prev, header)
            })
            .collect()
    }

    /// Mark a block as both added to txstore and indexed to history.
    fn mark_complete(store: &Store, entry: &HeaderEntry) {
        store
            .added_blockhashes
            .write()
            .unwrap()
            .insert(*entry.hash());
        store
            .indexed_blockhashes
            .write()
            .unwrap()
            .insert(*entry.hash());
    }

    fn persisted_tip(store: &Store) -> Option<BlockHash> {
        store
            .txstore_db
            .get(b"t")
            .map(|raw| deserialize(&raw).unwrap())
    }

    #[test]
    fn test_synced_tip_stops_at_the_first_unindexed_block() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open_test(dir.path());
        let headers = test_chain(4);

        // Heights 0, 1 and 3 completed; height 2 was lost to a failed fetch. The tip
        // must stop at height 1, not run on to the completed block above the gap.
        mark_complete(&store, &headers[0]);
        mark_complete(&store, &headers[1]);
        mark_complete(&store, &headers[3]);

        let completed = store.completed_header_prefix(&headers);
        assert_eq!(completed.len(), 2);

        let synced_tip = store.advance_synced_tip(completed).unwrap();
        assert_eq!(synced_tip, *headers[1].hash());
        assert_eq!(persisted_tip(&store), Some(*headers[1].hash()));
        assert_eq!(store.headers().len(), 2);

        // Only the block that actually failed is outstanding. Height 3 is already in the
        // db and merely needs the gap below it filled before it can be appended.
        let outstanding = store.headers_to_process(&headers);
        assert_eq!(outstanding.len(), 1);
        assert_eq!(outstanding[0].height(), 2);

        // Once the refetch of height 2 lands, the next update carries the tip past the gap.
        mark_complete(&store, &headers[2]);
        let completed = store.completed_header_prefix(&headers[2..]);
        assert_eq!(completed.len(), 2);

        let synced_tip = store.advance_synced_tip(completed).unwrap();
        assert_eq!(synced_tip, *headers[3].hash());
        assert_eq!(persisted_tip(&store), Some(*headers[3].hash()));
    }

    #[test]
    fn test_completed_prefix_requires_both_added_and_indexed() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open_test(dir.path());
        let headers = test_chain(2);

        // Added to txstore but never indexed to history.
        store
            .added_blockhashes
            .write()
            .unwrap()
            .insert(*headers[0].hash());

        assert!(store.completed_header_prefix(&headers).is_empty());
    }

    #[test]
    fn test_advance_synced_tip_errs_when_nothing_was_indexed() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open_test(dir.path());

        // Every block of the first batch failed to fetch, so the prefix is empty and
        // there is no tip to report. `HeaderList::tip()` would hand back the all-zeroes
        // default hash here, which reads as a successful sync to the caller.
        assert!(store.advance_synced_tip(vec![]).is_err());
        assert_eq!(persisted_tip(&store), None);
    }

    #[test]
    fn test_advance_synced_tip_keeps_the_existing_tip_when_nothing_completed() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open_test(dir.path());
        let headers = test_chain(3);

        for entry in &headers[..2] {
            mark_complete(&store, entry);
        }
        store
            .advance_synced_tip(store.completed_header_prefix(&headers))
            .unwrap();

        // A later update whose very first block fails to fetch must hold the tip where
        // it is, not error out on an index that already has blocks in it.
        let synced_tip = store.advance_synced_tip(vec![]).unwrap();
        assert_eq!(synced_tip, *headers[1].hash());
        assert_eq!(persisted_tip(&store), Some(*headers[1].hash()));
    }

    #[test]
    fn test_compute_script_hash_p2pkh() {
        // P2PKH scriptPubKey for address 1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa
        // OP_DUP OP_HASH160 <20-byte-hash> OP_EQUALVERIFY OP_CHECKSIG
        let script: Script = Vec::from_hex("76a91462e907b15cbf27d5425399ebf6f0fb50ebb88f1888ac")
            .unwrap()
            .into();
        let expected = "6191c3b590bfcfa0475e877c302da1e323497acf3b42c08d8fa28e364edf018b"
            .parse::<sha256::Hash>()
            .unwrap()
            .to_byte_array();
        assert_eq!(compute_script_hash(&script), expected);
    }

    #[test]
    fn test_sha256_empty_input() {
        // NIST SHA-256 test vector: SHA-256("")
        let expected: sha256::Hash =
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                .parse()
                .unwrap();
        assert_eq!(sha256::Hash::hash(b""), expected);
    }

    #[test]
    fn test_sha256_abc() {
        // NIST SHA-256 test vector: SHA-256("abc")
        let expected: sha256::Hash =
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
                .parse()
                .unwrap();
        assert_eq!(sha256::Hash::hash(b"abc"), expected);
    }
}
