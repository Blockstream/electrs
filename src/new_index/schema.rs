use bitcoin::hashes::sha256d::Hash as Sha256dHash;
use bitcoin::hex::FromHex;
#[cfg(not(feature = "liquid"))]
use bitcoin::merkle_tree::MerkleBlock;

use crypto::digest::Digest;
use crypto::sha2::Sha256;
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

use crate::{chain::{
    BlockHash, BlockHeader, Network, OutPoint, Script, Transaction, TxOut, Txid, Value,
}, new_index::db_metrics::RocksDbMetrics};
use crate::config::Config;
use crate::daemon::Daemon;
use crate::errors::*;
use crate::metrics::{Gauge, HistogramOpts, HistogramTimer, HistogramVec, MetricOpts, Metrics};
use crate::util::{
    bincode, full_hash, has_prevout, is_spendable, BlockHeaderMeta, BlockId, BlockMeta,
    BlockStatus, Bytes, HeaderEntry, HeaderList, ScriptToAddr,
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

pub struct Store {
    // TODO: should be column families
    txstore_db: DB,
    history_db: DB,
    cache_db: DB,
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
        debug!("shared LRU block cache: db_block_cache_mb='{}'", config.db_block_cache_mb);

        let txstore_db = DB::open(&path.join("txstore"), config, verify_compat, &shared_cache);
        let added_blockhashes = load_blockhashes(&txstore_db, &BlockRow::done_filter());
        debug!("{} blocks were added", added_blockhashes.len());

        let history_db = DB::open(&path.join("history"), config, verify_compat, &shared_cache);
        let indexed_blockhashes = load_blockhashes(&history_db, &BlockRow::done_filter());
        debug!("{} blocks were indexed", indexed_blockhashes.len());

        let cache_db = DB::open(&path.join("cache"), config, verify_compat, &shared_cache);

        let db_metrics = Arc::new(RocksDbMetrics::new(&metrics));
        txstore_db.start_stats_exporter(Arc::clone(&db_metrics), "txstore_db");
        history_db.start_stats_exporter(Arc::clone(&db_metrics), "history_db");
        cache_db.start_stats_exporter(Arc::clone(&db_metrics), "cache_db");

        let headers = if let Some(tip_hash) = txstore_db.get(b"t") {
            let mut tip_hash = deserialize(&tip_hash).expect("invalid chain tip in `t`");
            let headers_map = load_blockheaders(&txstore_db);

            // Move the tip back until we reach a block that is indexed in the history db.
            // It is possible for the tip recorded under the db "t" key to be un-indexed if electrs
            // shuts down during reorg handling. Normally this wouldn't matter because the non-indexed
            // block would be stale, but it could matter if the chain later re-orged back to
            // include the previously stale block because more blocks were built on top of it.
            // Without this, the stale-then-not-stale block(s) would not get re-indexed correctly.
            while !indexed_blockhashes.contains(&tip_hash) {
                tip_hash = headers_map
                    .get(&tip_hash)
                    .expect("invalid header chain")
                    .prev_blockhash;
            }
            debug!(
                "{} headers were loaded, tip at {:?}",
                headers_map.len(),
                tip_hash
            );
            HeaderList::new(headers_map, tip_hash)
        } else {
            HeaderList::empty()
        };

        Store {
            txstore_db,
            history_db,
            cache_db,
            added_blockhashes: RwLock::new(added_blockhashes),
            indexed_blockhashes: RwLock::new(indexed_blockhashes),
            indexed_headers: RwLock::new(headers),
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
}

type UtxoMap = HashMap<OutPoint, (BlockId, Value)>;

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

pub struct Indexer {
    store: Arc<Store>,
    flush: DBFlush,
    from: FetchFrom,
    iconfig: IndexerConfig,
    duration: HistogramVec,
    tip_metric: Gauge,
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
}

// TODO: &[Block] should be an iterator / a queue.
impl Indexer {
    pub fn open(store: Arc<Store>, from: FetchFrom, config: &Config, metrics: &Metrics) -> Self {
        Indexer {
            store,
            flush: DBFlush::Disable,
            from,
            iconfig: IndexerConfig::from(config),
            duration: metrics.histogram_vec(
                HistogramOpts::new("index_duration", "Index update duration (in seconds)"),
                &["step"],
            ),
            tip_metric: metrics.gauge(MetricOpts::new("tip_height", "Current chain tip height")),
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

    // Headers that need any work: either not yet added to txstore or not yet indexed to history.
    fn headers_to_process(&self, new_headers: &[HeaderEntry]) -> Vec<HeaderEntry> {
        let added = self.store.added_blockhashes.read().unwrap();
        let indexed = self.store.indexed_blockhashes.read().unwrap();
        new_headers
            .iter()
            .filter(|e| !added.contains(e.hash()) || !indexed.contains(e.hash()))
            .cloned()
            .collect()
    }

    fn start_auto_compactions(&self, db: &DB) {
        let key = b"F".to_vec();
        if db.get(&key).is_none() {
            db.full_compaction();
            db.put_sync(&key, b"");
            assert!(db.get(&key).is_some());
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

        // Handle reorgs by undoing the reorged (stale) blocks first
        if let Some(reorged_since) = reorged_since {
            // Remove reorged headers from the in-memory HeaderList.
            // This will also immediately invalidate all the history db entries originating from those blocks
            // (even before the rows are deleted below), since they reference block heights that will no longer exist.
            // This ensures consistency - it is not possible for blocks to be available (e.g. in GET /blocks/tip or /block/:hash)
            // without the corresponding history entries for these blocks (e.g. in GET /address/:address/txs), or vice-versa.
            let mut reorged_headers = self
                .store
                .indexed_headers
                .write()
                .unwrap()
                .pop(reorged_since);
            // The chain tip will temporarily drop to the common ancestor (at height reorged_since-1),
            // until the new headers are `append()`ed (below).

            info!(
                "processing reorg of depth {} since height {}",
                reorged_headers.len(),
                reorged_since,
            );

            // Reorged blocks are undone in chunks of 100, processed in serial, each as an atomic batch.
            // Reverse them so that chunks closest to the chain tip are processed first,
            // which is necessary to properly recover from crashes during reorg handling.
            // Also see the comment under `Store::open()`.
            reorged_headers.reverse();

            // Fetch the reorged blocks, then undo their history index db rows.
            // The txstore db rows are kept for reorged blocks/transactions.
            start_fetcher(self.from, &daemon, reorged_headers, self.iconfig.block_batch_size, chain_tip_height)?
                .map(|blocks| self.undo_index(&blocks));
        }

        // Single-pass: add to txstore and index to history in the same per-batch loop.
        //
        // In the old two-pass approach, txstore_db was fully compacted between the add
        // and index passes, which pushed all O rows into large SST files deep in the LSM
        // tree. With only a small block cache, lookup_txos() during indexing then caused
        // nearly every key to require a disk read.
        //
        // By interleaving add() and index() in the same batch, the O rows written by
        // add() are still in the write buffer (or nearby L0 SST files) when index()
        // calls lookup_txos() — dramatically increasing cache hit rate.
        //
        // Crash safety: added_blockhashes / indexed_blockhashes are persisted via the
        // "D" done-marker rows. On restart, headers_to_process() re-derives which
        // blocks still need work, so partially-processed batches are re-processed safely.
        let to_process = self.headers_to_process(&new_headers);
        debug!(
            "processing {} blocks (add + index) using {:?}",
            to_process.len(),
            self.from
        );

        let mut fetcher_count = 0;
        let to_process_total = to_process.len();

        start_fetcher(self.from, &daemon, to_process, self.iconfig.block_batch_size, chain_tip_height)?.map(|blocks| {
            if fetcher_count % 25 == 0 && to_process_total > 20 {
                let batch_height = blocks.last().map(|b| b.entry.height()).unwrap_or(0);
                info!(
                    "processing blocks {}/{} ({:.1}%)",
                    batch_height,
                    chain_tip_height,
                    batch_height as f32 / chain_tip_height.max(1) as f32 * 100.0
                );
            }
            fetcher_count += 1;

            // Add blocks not yet in txstore (idempotent: crash recovery skips already-added blocks)
            let to_add: Vec<_> = {
                let added = self.store.added_blockhashes.read().unwrap();
                blocks
                    .iter()
                    .filter(|b| !added.contains(b.entry.hash()))
                    .cloned()
                    .collect()
            };

            // Index blocks not yet in history (O rows for to_add are now in the write buffer)
            let to_index: Vec<_> = {
                let indexed = self.store.indexed_blockhashes.read().unwrap();
                blocks
                    .iter()
                    .filter(|b| !indexed.contains(b.entry.hash()))
                    .cloned()
                    .collect()
            };

            if !to_add.is_empty() || !to_index.is_empty() {
                let _batch_timer = self.start_timer("batch_total");
                if !to_add.is_empty() {
                    self.add(&to_add);
                }
                if !to_index.is_empty() {
                    self.index(&to_index);
                }
            }
            if let Some(last) = blocks.last() {
                let h = last.entry.height();
                self.sync_height.set(h as i64);
                if chain_tip_height > 0 {
                    self.sync_progress.set(h as f64 / chain_tip_height as f64 * 100.0);
                }
            }
        });

        // Compact after all add+index work is done, not between passes.
        self.start_auto_compactions(&self.store.txstore_db);
        self.start_auto_compactions(&self.store.history_db);
        self.start_auto_compactions(&self.store.cache_db);

        if let DBFlush::Disable = self.flush {
            let t = std::time::Instant::now();
            debug!("flushing txstore_db to disk");
            self.store.txstore_db.flush();
            debug!("flushing txstore_db complete in {:.1?}", t.elapsed());

            let t = std::time::Instant::now();
            debug!("flushing history_db to disk");
            self.store.history_db.flush();
            debug!("flushing history_db complete in {:.1?}", t.elapsed());

            // cache_db receives WAL-disabled writes when --address-search is enabled,
            // so it needs the same explicit flush to ensure durability.
            let t = std::time::Instant::now();
            debug!("flushing cache_db to disk");
            self.store.cache_db.flush();
            debug!("flushing cache_db complete in {:.1?}", t.elapsed());

            self.flush = DBFlush::Enable;
        }

        // Update the synced tip after all db writes are flushed
        debug!("updating synced tip to {:?}", tip);
        self.store.txstore_db.put_sync(b"t", &serialize(&tip));

        // Finally, append the new headers to the in-memory HeaderList.
        // This will make both the headers and the history entries visible in the public APIs, consistently with each-other.
        let mut headers = self.store.indexed_headers.write().unwrap();
        headers.append(new_headers);
        assert_eq!(tip, *headers.tip());

        if let FetchFrom::BlkFiles = self.from {
            self.from = FetchFrom::Bitcoind;
        }

        self.tip_metric.set(headers.best_height() as i64);

        Ok(tip)
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
        self.store
            .history_db
            .write_rows(self._index(blocks), self.flush);

        let mut indexed_blockhashes = self.store.indexed_blockhashes.write().unwrap();
        indexed_blockhashes.extend(blocks.iter().map(|b| b.entry.hash()));
    }

    // Undo the history db entries previously written for the given blocks (that were reorged).
    // This includes the TxHistory, TxEdge, TxConf and BlockDone rows ('H', 'S', 'C' and 'D'),
    // as well as the Elements history rows ('I' and 'i').
    //
    // This does *not* remove any txstore db entries, which are intentionally kept
    // even for reorged blocks.
    fn undo_index(&self, blocks: &[BlockEntry]) {
        self.store
            .history_db
            .delete_rows(self._index(blocks), self.flush);
        // Note this doesn't actually "undo" the rows - the keys are simply deleted, and won't get
        // reverted back to their prior value (if there was one). It is expected that the history db
        // keys created by blocks are always unique and impossible to already exist from a prior block.
        // This is true for all history keys (which always include the height or txid), but for example
        // not true for the address prefix search index (in the txstore).

        let mut indexed_blockhashes = self.store.indexed_blockhashes.write().unwrap();
        for block in blocks {
            indexed_blockhashes.remove(block.entry.hash());
        }
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
            duration: metrics.histogram_vec(
                HistogramOpts::new("query_duration", "Index query duration (in seconds)"),
                &["name"],
            ),
        }
    }

    pub fn network(&self) -> Network {
        self.network
    }

    pub fn store(&self) -> &Store {
        &self.store
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

    pub fn get_block_txs(
        &self,
        hash: &BlockHash,
        start_index: usize,
        limit: usize,
    ) -> Result<Vec<Transaction>> {
        let txids = self.get_block_txids(hash).chain_err(|| "block not found")?;
        ensure!(start_index < txids.len(), "start index out of range");

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

    pub fn history(
        &self,
        scripthash: &[u8],
        last_seen_txid: Option<&Txid>,
        limit: usize,
    ) -> Vec<(Transaction, BlockId)> {
        // scripthash lookup
        self._history(b'H', scripthash, last_seen_txid, limit)
    }

    fn _history(
        &self,
        code: u8,
        hash: &[u8],
        last_seen_txid: Option<&Txid>,
        limit: usize,
    ) -> Vec<(Transaction, BlockId)> {
        let _timer_scan = self.start_timer("history");
        let headers = self.store.indexed_headers.read().unwrap();
        let history_iter = self
            .history_iter_scan_reverse(code, hash)
            .map(TxHistoryRow::from_row)
            .map(|row| (row.get_txid(), row.key.confirmed_height as usize))
            // XXX: unique() requires keeping an in-memory list of all txids, can we avoid that?
            .unique()
            // TODO seek directly to last seen tx without reading earlier rows
            .skip_while(|(txid, _)| {
                // skip until we reach the last_seen_txid
                last_seen_txid.map_or(false, |last_seen_txid| last_seen_txid != txid)
            })
            .skip(match last_seen_txid {
                Some(_) => 1, // skip the last_seen_txid itself
                None => 0,
            })
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

        self.lookup_txns(&txids_with_blockhash)
            .expect("failed looking up txs in history index")
            .into_iter()
            .zip(blockids)
            .map(|(tx, blockid)| (tx, blockid))
            .collect()
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
        let _timer = self.start_timer("utxo");

        // get the last known utxo set and the blockhash it was updated for.
        // invalidates the cache if the block was orphaned.
        let cache: Option<(UtxoMap, usize)> = self
            .store
            .cache_db
            .get(&UtxoCacheRow::key(scripthash))
            .map(|c| bincode::deserialize_little(&c).unwrap())
            .and_then(|(utxos_cache, blockhash)| {
                self.height_by_hash(&blockhash)
                    .map(|height| (utxos_cache, height))
            })
            .map(|(utxos_cache, height)| (from_utxo_cache(utxos_cache, self), height));
        let had_cache = cache.is_some();

        // update utxo set with new transactions since
        let (newutxos, lastblock, processed_items) = cache.map_or_else(
            || self.utxo_delta(scripthash, HashMap::new(), 0, limit),
            |(oldutxos, blockheight)| self.utxo_delta(scripthash, oldutxos, blockheight + 1, limit),
        )?;

        // save updated utxo set to cache
        if let Some(lastblock) = lastblock {
            if had_cache || processed_items > MIN_HISTORY_ITEMS_TO_CACHE {
                self.store.cache_db.write_rows(
                    vec![UtxoCacheRow::new(scripthash, &newutxos, &lastblock).into_row()],
                    DBFlush::Enable,
                );
            }
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
        limit: usize,
    ) -> Result<(UtxoMap, Option<BlockHash>, usize)> {
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

        for (history, blockid) in history_iter {
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

            // abort if the utxo set size excedees the limit at any point in time
            if utxos.len() > limit {
                bail!(ErrorKind::TooPopular)
            }
        }

        Ok((utxos, lastblock, processed_items))
    }

    pub fn stats(&self, scripthash: &[u8]) -> ScriptStats {
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
        let (newstats, lastblock) = cache.map_or_else(
            || self.stats_delta(scripthash, ScriptStats::default(), 0),
            |(oldstats, blockheight)| self.stats_delta(scripthash, oldstats, blockheight + 1),
        );

        // save updated stats to cache
        if let Some(lastblock) = lastblock {
            if newstats.funded_txo_count + newstats.spent_txo_count > MIN_HISTORY_ITEMS_TO_CACHE {
                self.store.cache_db.write_rows(
                    vec![StatsCacheRow::new(scripthash, &newstats, &lastblock).into_row()],
                    DBFlush::Enable,
                );
            }
        }

        newstats
    }

    fn stats_delta(
        &self,
        scripthash: &[u8],
        init_stats: ScriptStats,
        start_height: usize,
    ) -> (ScriptStats, Option<BlockHash>) {
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

        for (history, blockid) in history_iter {
            if lastblock != Some(blockid.hash) {
                seen_txids.clear();
            }

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

        (stats, lastblock)
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
        Ok(self
            .lookup_raw_txns(txids)?
            .into_iter()
            .map(|rawtx| deserialize(&rawtx).expect("failed to parse Transaction"))
            .collect())
    }

    pub fn lookup_txn(&self, txid: &Txid, blockhash: Option<&BlockHash>) -> Option<Transaction> {
        let _timer = self.start_timer("lookup_txn");
        let rawtx = self.lookup_raw_txn(txid, blockhash)?;
        Some(deserialize(&rawtx).expect("failed to parse Transaction"))
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
                .map(|val| val.unwrap().chain_err(|| "missing tx"))
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
        let row_value = self.store.history_db.get(&TxConfRow::key(txid))?;
        let height = TxConfRow::height_from_val(&row_value);
        let headers = self.store.indexed_headers.read().unwrap();
        // skip over entries that point to non-existing heights (may happen while new/reorged blocks are being processed)
        Some(headers.header_by_height(height as usize)?.into())
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
    ) -> Vec<(Transaction, BlockId)> {
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
            let txo = res
                .unwrap()
                .ok_or_else(|| format!("missing txo {}", outpoint))?;
            Ok((outpoint, deserialize(&txo).expect("failed to parse TxOut")))
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
            rows.push(BlockRow::new_done(full_hash(&b.entry.hash()[..])).into_row()); // mark block as "indexed"
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
    let mut hash = FullHash::default();
    let mut sha2 = Sha256::new();
    sha2.input(script.as_bytes());
    sha2.result(&mut hash);
    hash
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
        bincode::serialize_little(&(b'S', full_hash(&outpoint.txid[..]), outpoint.vout))
            .unwrap()
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
