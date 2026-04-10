use prometheus::GaugeVec;
use rayon::prelude::*;
use rocksdb;

use std::convert::TryInto;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::config::Config;
use crate::new_index::db_metrics::RocksDbMetrics;
use crate::util::{bincode, spawn_thread, Bytes};

static DB_VERSION: u32 = 3;

const ROCKSDB_NUM_LEVELS: u32 = 7;

#[derive(Debug, Eq, PartialEq)]
pub struct DBRow {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

pub struct ScanIterator<'a> {
    prefix: Vec<u8>,
    iter: rocksdb::DBIterator<'a>,
    done: bool,
}

impl<'a> Iterator for ScanIterator<'a> {
    type Item = DBRow;

    fn next(&mut self) -> Option<DBRow> {
        if self.done {
            return None;
        }
        let (key, value) = self.iter.next()?.expect("valid iterator");
        if !key.starts_with(&self.prefix) {
            self.done = true;
            return None;
        }
        Some(DBRow {
            key: key.into_vec(),
            value: value.into_vec(),
        })
    }
}

pub struct ReverseScanIterator<'a> {
    prefix: Vec<u8>,
    iter: rocksdb::DBRawIterator<'a>,
    done: bool,
}

impl<'a> Iterator for ReverseScanIterator<'a> {
    type Item = DBRow;

    fn next(&mut self) -> Option<DBRow> {
        if self.done || !self.iter.valid() {
            return None;
        }

        let key = self.iter.key().unwrap();
        if !key.starts_with(&self.prefix) {
            self.done = true;
            return None;
        }

        let row = DBRow {
            key: key.into(),
            value: self.iter.value().unwrap().into(),
        };

        self.iter.prev();

        Some(row)
    }
}

#[derive(Debug)]
pub struct DB {
    db: Arc<rocksdb::DB>,
}

#[derive(Copy, Clone, Debug)]
pub enum DBFlush {
    Disable,
    Enable,
}

impl DB {
    pub fn open(path: &Path, config: &Config, verify_compat: bool) -> DB {
        Self::open_with_cache(path, config, verify_compat, None)
    }

    pub fn open_with_cache(path: &Path, config: &Config, verify_compat: bool, shared_cache: Option<&rocksdb::Cache>) -> DB {
        debug!("opening DB at {:?}", path);
        let mut db_opts = rocksdb::Options::default();
        db_opts.create_if_missing(true);
        db_opts.set_max_open_files(100_000); // TODO: make sure to `ulimit -n` this process correctly
        db_opts.set_compaction_style(rocksdb::DBCompactionStyle::Level);
        db_opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        db_opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);
        db_opts.set_target_file_size_base(1_073_741_824);
        // Bulk-load compaction: allow L0 files to accumulate to a bounded limit
        // before compacting. This reduces write amplification compared to the
        // default trigger of 4, while keeping the file count — and therefore
        // bloom-filter memory and lookup cost — bounded.
        //
        // With bloom filters at 10 bits/key and a 512 MB write buffer, each L0
        // file has ~7.8 M keys, so its filter block is ~9.75 MB. At 64 files
        // that is ~625 MB of pinned filter blocks — well within an 8 GB cache.
        // Each lookup checks 64 bloom filters (fast, in-memory) and reads from
        // only ~0.64 files on average (1 % false-positive rate × 64 files).
        //
        // Set slowdown/stop triggers well above the compaction trigger so writes
        // are never stalled while background compaction catches up.
        // Disable the pending-compaction-bytes stall so the large backlog that
        // builds up during the bulk load does not block writes.
        const L0_BULK_TRIGGER: i32 = 64;
        db_opts.set_level_zero_file_num_compaction_trigger(L0_BULK_TRIGGER);
        db_opts.set_level_zero_slowdown_writes_trigger(L0_BULK_TRIGGER * 4);
        db_opts.set_level_zero_stop_writes_trigger(L0_BULK_TRIGGER * 8);
        db_opts.set_hard_pending_compaction_bytes_limit(0);
        db_opts.set_soft_pending_compaction_bytes_limit(0);


        let parallelism: i32 = config.db_parallelism.try_into()
            .expect("db_parallelism value too large for i32");

        // Configure parallelism (background jobs and thread pools)
        db_opts.increase_parallelism(parallelism);

        // Configure write buffer size (not set by increase_parallelism)
        db_opts.set_write_buffer_size(config.db_write_buffer_size_mb * 1024 * 1024);

        // 4 MiB readahead for compaction I/O. Larger than the previous 1 MiB to better
        // amortise syscall overhead when reading the many L0 files accumulated during
        // initial sync.
        db_opts.set_compaction_readahead_size(4 << 20);

        // Background-sync SST files to the OS incrementally as they are written,
        // rather than doing a large fsync on close. Smooths out I/O latency spikes.
        db_opts.set_bytes_per_sync(1 << 20);

        // Parallelize sub-ranges within a single compaction job (including the one-time
        // full_compaction at the end of initial sync). Without this, compact_range() is
        // single-threaded regardless of increase_parallelism(). Setting it equal to the
        // parallelism level keeps all background threads busy during the final compaction.
        db_opts.set_max_subcompactions(parallelism as u32);

        // Configure block cache and table options
        let mut block_opts = rocksdb::BlockBasedOptions::default();
        let owned_cache: rocksdb::Cache;
        let cache = match shared_cache {
            Some(c) => c,
            None => {
                let cache_size_bytes = config.db_block_cache_mb * 1024 * 1024;
                owned_cache = rocksdb::Cache::new_lru_cache(cache_size_bytes);
                &owned_cache
            }
        };
        block_opts.set_block_cache(cache);
        // When --cache-index-filter-blocks is passed, store index and filter blocks
        // inside the block cache so their memory is bounded by --db-block-cache-mb.
        // Without this (the default), RocksDB keeps them on the heap where they may
        // never be evicted — possibly better for read performance compared to needing
        // to go to disk, but uses ~18 MB per SST file.
        if config.db_cache_index_filter_blocks {
            block_opts.set_cache_index_and_filter_blocks(true);
            // Pin L0 index and filter blocks in the cache so they are never evicted.
            // Without this, data block churn evicts L0 index/filter blocks, causing
            // repeated disk reads for every SST lookup.
            block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
        }
        // Bloom filters allow multi_get() to skip SST files that don't contain a key
        // without touching the index or data blocks. Without this, every point lookup
        // must binary-search the index of every L0 file whose key range overlaps the
        // query (all of them for random txids) — extremely expensive with 1000+ L0
        // files accumulated during initial sync. At 10 bits/key the false-positive
        // rate is ~1%, so only ~10 out of 1000 L0 files need actual I/O per key.
        // Combined with the prefix extractor below, these become prefix Bloom filters
        // keyed on `code || hash` (33 bytes), which also allow prefix range scans
        // (e.g. history lookups) to skip L0 files entirely. The filter blocks are
        // cached and pinned alongside the index blocks via the settings above.
        block_opts.set_bloom_filter(10.0, false);

        // All electrs keys share the structure `code (1 byte) || hash (32 bytes) || ...`.
        // A 33-byte fixed prefix extractor enables prefix Bloom filters: range scans
        // like iter_scan("H" + scripthash) can skip SST files whose Bloom filter
        // doesn't match the prefix, rather than checking every L0 file.
        //
        // INVARIANT: All iter_scan* and raw_iterator methods must use total_order_seek
        // when the seek key may be shorter than 33 bytes. Without it, RocksDB silently
        // skips SST files that contain matching keys. See the conditional in iter_scan().
        db_opts.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(33));

        db_opts.set_block_based_table_factory(&block_opts);

        let db = DB {
            db: Arc::new(rocksdb::DB::open(&db_opts, path).expect("failed to open RocksDB"))
        };
        if verify_compat {
            db.verify_compatibility(config);
        }
        db
    }

    pub fn full_compaction(&self) {
        info!("starting full compaction on {:?}", self.db);
        let start = std::time::Instant::now();
        let mut opts = rocksdb::CompactOptions::default();
        opts.set_bottommost_level_compaction(rocksdb::BottommostLevelCompaction::Force);
        self.db.compact_range_opt(None::<&[u8]>, None::<&[u8]>, &opts);
        let elapsed = start.elapsed();
        info!("finished full compaction on {:?} in elapsed='{:.1?}'", self.db, elapsed);
    }

    pub fn enable_auto_compaction(&self) {
       // Reset L0 triggers and pending-compaction stall thresholds to RocksDB
       // defaults, so that steady-state operation compacts promptly and avoids
       // unbounded compaction backlogs that cause read latency spikes.
       // RocksDB defaults (stable since v5.x through v10.4.2). Hardcoded because
       // set_options() doesn't return previous values and the Rust bindings lack getters.

        let soft_limit = (64u64 << 30).to_string(); // 64 GiB
        let hard_limit = (256u64 << 30).to_string(); // 256 GiB

        let opts = [
            ("disable_auto_compactions", "false"),
            ("level0_file_num_compaction_trigger", "4"),
            ("level0_slowdown_writes_trigger", "20"),
            ("level0_stop_writes_trigger", "36"),
            ("soft_pending_compaction_bytes_limit", &soft_limit),
            ("hard_pending_compaction_bytes_limit", &hard_limit),
        ];
        self.db.set_options(&opts).unwrap();
    }

    pub fn raw_iterator(&self) -> rocksdb::DBRawIterator<'_> {
        let mut opts = rocksdb::ReadOptions::default();
        opts.set_total_order_seek(true);
        self.db.raw_iterator_opt(opts)
    }

    pub fn iter_scan(&self, prefix: &[u8]) -> ScanIterator<'_> {
        let iter = if prefix.len() >= 33 {
            self.db.prefix_iterator(prefix)
        } else {
            // Short prefixes (e.g. b"B", b"D") are below the 33-byte prefix extractor
            // length. prefix_iterator would silently skip SST files. Use total_order_seek
            // to fall back to a full scan; ScanIterator enforces the prefix boundary.
            let mut opts = rocksdb::ReadOptions::default();
            opts.set_total_order_seek(true);
            self.db.iterator_opt(
                rocksdb::IteratorMode::From(prefix, rocksdb::Direction::Forward),
                opts,
            )
        };
        ScanIterator {
            prefix: prefix.to_vec(),
            iter,
            done: false,
        }
    }

    pub fn iter_scan_from(&self, prefix: &[u8], start_at: &[u8]) -> ScanIterator<'_> {
        // start_at is always >= prefix length. When >= 33 bytes, the default seek
        // uses the prefix extractor for bloom filtering automatically. When < 33
        // bytes, fall back to total_order_seek to avoid silent misses.
        let iter = if start_at.len() >= 33 {
            self.db.iterator(rocksdb::IteratorMode::From(
                start_at,
                rocksdb::Direction::Forward,
            ))
        } else {
            let mut opts = rocksdb::ReadOptions::default();
            opts.set_total_order_seek(true);
            self.db.iterator_opt(
                rocksdb::IteratorMode::From(start_at, rocksdb::Direction::Forward),
                opts,
            )
        };
        ScanIterator {
            prefix: prefix.to_vec(),
            iter,
            done: false,
        }
    }

    pub fn iter_scan_reverse(&self, prefix: &[u8], prefix_max: &[u8]) -> ReverseScanIterator<'_> {
        // total_order_seek is required for correctness: prefix mode (the default when a
        // prefix extractor is set) is only guaranteed correct for forward iteration.
        // SeekForPrev + Prev() in prefix mode can silently miss entries.
        let mut iter = self.raw_iterator();
        iter.seek_for_prev(prefix_max);

        ReverseScanIterator {
            prefix: prefix.to_vec(),
            iter,
            done: false,
        }
    }

    pub fn write_rows(&self, mut rows: Vec<DBRow>, flush: DBFlush) {
        log::trace!(
            "writing {} rows to {:?}, flush={:?}",
            rows.len(),
            self.db,
            flush
        );
        rows.par_sort_unstable_by(|a, b| a.key.cmp(&b.key));
        let mut batch = rocksdb::WriteBatch::default();
        for row in rows {
            batch.put(&row.key, &row.value);
        }
        self.write_batch(batch, flush)
    }

    pub fn delete_rows(&self, mut rows: Vec<DBRow>, flush: DBFlush) {
        log::trace!("deleting {} rows from {:?}", rows.len(), self.db,);
        rows.par_sort_unstable_by(|a, b| a.key.cmp(&b.key));
        let mut batch = rocksdb::WriteBatch::default();
        for row in rows {
            batch.delete(&row.key);
        }
        self.write_batch(batch, flush)
    }

    pub fn write_batch(&self, batch: rocksdb::WriteBatch, flush: DBFlush) {
        let do_flush = match flush {
            DBFlush::Enable => true,
            DBFlush::Disable => false,
        };
        let mut opts = rocksdb::WriteOptions::new();
        opts.set_sync(do_flush);
        opts.disable_wal(!do_flush);
        self.db.write_opt(batch, &opts).unwrap();
    }

    pub fn flush(&self) {
        self.db.flush().unwrap();
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        self.db.put(key, value).unwrap();
    }

    pub fn put_sync(&self, key: &[u8], value: &[u8]) {
        let mut opts = rocksdb::WriteOptions::new();
        opts.set_sync(true);
        self.db.put_opt(key, value, &opts).unwrap();
    }

    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.db.get(key).unwrap().map(|v| v.to_vec())
    }

    pub fn multi_get<K, I>(&self, keys: I) -> Vec<Result<Option<Vec<u8>>, rocksdb::Error>>
    where
        K: AsRef<[u8]>,
        I: IntoIterator<Item = K>,
    {
        self.db.multi_get(keys)
    }

    /// Remove database entries in the range [from, to)
    pub fn delete_range<K: AsRef<[u8]>>(&self, from: K, to: K, flush: DBFlush) {
        let mut batch = rocksdb::WriteBatch::default();
        batch.delete_range(from, to);
        self.write_batch(batch, flush);
    }

    fn verify_compatibility(&self, config: &Config) {
        let compatibility_bytes = bincode::serialize_little(&(DB_VERSION, config.light_mode)).unwrap();

        match self.get(b"V") {
            None => self.put(b"V", &compatibility_bytes),
            Some(x) if x != compatibility_bytes => {
                panic!("Incompatible database found. Please reindex or migrate.")
            }
            Some(_) => (),
        }
    }

    #[cfg(test)]
    fn open_test(path: &Path) -> DB {
        let mut db_opts = rocksdb::Options::default();
        db_opts.create_if_missing(true);
        db_opts.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(33));

        let mut block_opts = rocksdb::BlockBasedOptions::default();
        block_opts.set_bloom_filter(10.0, false);
        db_opts.set_block_based_table_factory(&block_opts);

        DB {
            db: Arc::new(rocksdb::DB::open(&db_opts, path).expect("failed to open test RocksDB")),
        }
    }

    pub fn start_stats_exporter(&self, db_metrics: Arc<RocksDbMetrics>, db_name: &str) {
        let db_arc = Arc::clone(&self.db);
        let db_arc2 = Arc::clone(&self.db);
        let label = db_name.to_string();
        let label2 = label.clone();

        let update_gauge = move |gauge: &GaugeVec, property: &str| {
            if let Ok(Some(value)) = db_arc.property_value(property) {
                if let Ok(v) = value.parse::<f64>() {
                    gauge.with_label_values(&[&label]).set(v);
                }
            }
        };

        spawn_thread("db_stats_exporter", move || loop {
            update_gauge(&db_metrics.num_immutable_mem_table, "rocksdb.num-immutable-mem-table");
            update_gauge(&db_metrics.mem_table_flush_pending, "rocksdb.mem-table-flush-pending");
            update_gauge(&db_metrics.compaction_pending, "rocksdb.compaction-pending");
            update_gauge(&db_metrics.background_errors, "rocksdb.background-errors");
            update_gauge(&db_metrics.cur_size_active_mem_table, "rocksdb.cur-size-active-mem-table");
            update_gauge(&db_metrics.cur_size_all_mem_tables, "rocksdb.cur-size-all-mem-tables");
            update_gauge(&db_metrics.size_all_mem_tables, "rocksdb.size-all-mem-tables");
            update_gauge(&db_metrics.num_entries_active_mem_table, "rocksdb.num-entries-active-mem-table");
            update_gauge(&db_metrics.num_entries_imm_mem_tables, "rocksdb.num-entries-imm-mem-tables");
            update_gauge(&db_metrics.num_deletes_active_mem_table, "rocksdb.num-deletes-active-mem-table");
            update_gauge(&db_metrics.num_deletes_imm_mem_tables, "rocksdb.num-deletes-imm-mem-tables");
            update_gauge(&db_metrics.estimate_num_keys, "rocksdb.estimate-num-keys");
            update_gauge(&db_metrics.estimate_table_readers_mem, "rocksdb.estimate-table-readers-mem");
            update_gauge(&db_metrics.is_file_deletions_enabled, "rocksdb.is-file-deletions-enabled");
            update_gauge(&db_metrics.num_snapshots, "rocksdb.num-snapshots");
            update_gauge(&db_metrics.oldest_snapshot_time, "rocksdb.oldest-snapshot-time");
            update_gauge(&db_metrics.num_live_versions, "rocksdb.num-live-versions");
            update_gauge(&db_metrics.current_super_version_number, "rocksdb.current-super-version-number");
            update_gauge(&db_metrics.estimate_live_data_size, "rocksdb.estimate-live-data-size");
            update_gauge(&db_metrics.min_log_number_to_keep, "rocksdb.min-log-number-to-keep");
            update_gauge(&db_metrics.min_obsolete_sst_number_to_keep, "rocksdb.min-obsolete-sst-number-to-keep");
            update_gauge(&db_metrics.total_sst_files_size, "rocksdb.total-sst-files-size");
            update_gauge(&db_metrics.live_sst_files_size, "rocksdb.live-sst-files-size");
            update_gauge(&db_metrics.base_level, "rocksdb.base-level");
            update_gauge(&db_metrics.estimate_pending_compaction_bytes, "rocksdb.estimate-pending-compaction-bytes");
            update_gauge(&db_metrics.num_running_compactions, "rocksdb.num-running-compactions");
            update_gauge(&db_metrics.num_running_flushes, "rocksdb.num-running-flushes");
            update_gauge(&db_metrics.actual_delayed_write_rate, "rocksdb.actual-delayed-write-rate");
            update_gauge(&db_metrics.is_write_stopped, "rocksdb.is-write-stopped");
            update_gauge(&db_metrics.estimate_oldest_key_time, "rocksdb.estimate-oldest-key-time");
            update_gauge(&db_metrics.block_cache_capacity, "rocksdb.block-cache-capacity");
            update_gauge(&db_metrics.block_cache_usage, "rocksdb.block-cache-usage");
            update_gauge(&db_metrics.block_cache_pinned_usage, "rocksdb.block-cache-pinned-usage");
            for level in 0..ROCKSDB_NUM_LEVELS {
                let prop = format!("rocksdb.num-files-at-level{}", level);
                if let Ok(Some(value)) = db_arc2.property_value(&prop) {
                    if let Ok(v) = value.parse::<f64>() {
                        let level_str = level.to_string();
                        db_metrics.num_files_at_level.with_label_values(&[&label2, &level_str]).set(v);
                    }
                }
            }
            thread::sleep(Duration::from_secs(5));
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a key mimicking electrs row format: 1-byte code + 32-byte hash + optional suffix.
    fn make_key(code: u8, hash_byte: u8, suffix: &[u8]) -> Vec<u8> {
        let mut key = vec![code];
        key.extend_from_slice(&[hash_byte; 32]);
        key.extend_from_slice(suffix);
        key
    }

    fn write_test_rows(db: &DB) {
        let rows = vec![
            // B rows (block headers) — scanned with 1-byte prefix b"B"
            DBRow { key: make_key(b'B', 0x01, &[]), value: b"header1".to_vec() },
            DBRow { key: make_key(b'B', 0x02, &[]), value: b"header2".to_vec() },
            // D rows (done markers) — scanned with 1-byte prefix b"D"
            DBRow { key: make_key(b'D', 0x01, &[]), value: vec![] },
            DBRow { key: make_key(b'D', 0x02, &[]), value: vec![] },
            // H rows (history) — scanned with 33-byte prefix b"H" + scripthash
            DBRow { key: make_key(b'H', 0xAA, &[0, 0, 0, 1]), value: vec![] },
            DBRow { key: make_key(b'H', 0xAA, &[0, 0, 0, 2]), value: vec![] },
            DBRow { key: make_key(b'H', 0xBB, &[0, 0, 0, 1]), value: vec![] },
            // O rows (txouts) — looked up by exact key, but scannable by 33-byte prefix
            DBRow { key: make_key(b'O', 0xCC, &[0, 1]), value: b"txout1".to_vec() },
            DBRow { key: make_key(b'O', 0xCC, &[0, 2]), value: b"txout2".to_vec() },
        ];
        db.write_rows(rows, DBFlush::Enable);
    }

    #[test]
    fn test_iter_scan_short_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_test(dir.path());
        write_test_rows(&db);

        // 1-byte prefix scan — must find all B rows
        let b_rows: Vec<DBRow> = db.iter_scan(b"B").collect();
        assert_eq!(b_rows.len(), 2, "expected 2 B rows, got {}", b_rows.len());
        assert_eq!(b_rows[0].value, b"header1");
        assert_eq!(b_rows[1].value, b"header2");

        // 1-byte prefix scan — must find all D rows
        let d_rows: Vec<DBRow> = db.iter_scan(b"D").collect();
        assert_eq!(d_rows.len(), 2, "expected 2 D rows, got {}", d_rows.len());
    }

    #[test]
    fn test_iter_scan_full_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_test(dir.path());
        write_test_rows(&db);

        // 33-byte prefix scan — must find only H rows for hash 0xAA
        let prefix = make_key(b'H', 0xAA, &[]);
        let h_rows: Vec<DBRow> = db.iter_scan(&prefix).collect();
        assert_eq!(h_rows.len(), 2, "expected 2 H/0xAA rows, got {}", h_rows.len());

        // 33-byte prefix scan — must find only H rows for hash 0xBB
        let prefix = make_key(b'H', 0xBB, &[]);
        let h_rows: Vec<DBRow> = db.iter_scan(&prefix).collect();
        assert_eq!(h_rows.len(), 1, "expected 1 H/0xBB row, got {}", h_rows.len());

        // 33-byte prefix scan — O rows for hash 0xCC
        let prefix = make_key(b'O', 0xCC, &[]);
        let o_rows: Vec<DBRow> = db.iter_scan(&prefix).collect();
        assert_eq!(o_rows.len(), 2, "expected 2 O/0xCC rows, got {}", o_rows.len());
    }

    #[test]
    fn test_iter_scan_from_full_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_test(dir.path());
        write_test_rows(&db);

        // Scan H/0xAA starting from height 2
        let prefix = make_key(b'H', 0xAA, &[]);
        let start = make_key(b'H', 0xAA, &[0, 0, 0, 2]);
        let rows: Vec<DBRow> = db.iter_scan_from(&prefix, &start).collect();
        assert_eq!(rows.len(), 1, "expected 1 H/0xAA row from height 2, got {}", rows.len());
    }

    #[test]
    fn test_iter_scan_reverse_full_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_test(dir.path());
        write_test_rows(&db);

        // Reverse scan H/0xAA from max
        let prefix = make_key(b'H', 0xAA, &[]);
        let prefix_max = make_key(b'H', 0xAA, &[0xFF, 0xFF, 0xFF, 0xFF]);
        let rows: Vec<DBRow> = db.iter_scan_reverse(&prefix, &prefix_max).collect();
        assert_eq!(rows.len(), 2, "expected 2 H/0xAA rows in reverse, got {}", rows.len());
        // Should be in reverse order
        assert!(rows[0].key > rows[1].key, "reverse scan should return descending keys");
    }

    #[test]
    fn test_iter_scan_no_cross_prefix_leakage() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_test(dir.path());
        write_test_rows(&db);

        // Scanning for a non-existent prefix returns nothing
        let prefix = make_key(b'H', 0xFF, &[]);
        let rows: Vec<DBRow> = db.iter_scan(&prefix).collect();
        assert_eq!(rows.len(), 0, "expected 0 rows for non-existent prefix");

        // Scanning b"B" must not return D, H, or O rows
        let b_rows: Vec<DBRow> = db.iter_scan(b"B").collect();
        for row in &b_rows {
            assert_eq!(row.key[0], b'B', "B scan returned non-B row");
        }
    }

    #[test]
    fn test_raw_iterator_sees_all_rows() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_test(dir.path());
        write_test_rows(&db);

        let mut iter = db.raw_iterator();
        iter.seek_to_first();
        let mut count = 0;
        while iter.valid() {
            count += 1;
            iter.next();
        }
        assert_eq!(count, 9, "expected 9 total rows, got {}", count);
    }
}
