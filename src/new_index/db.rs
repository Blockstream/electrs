use prometheus::GaugeVec;
use rocksdb;

use std::convert::TryInto;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::config::Config;
use crate::new_index::db_metrics::RocksDbMetrics;
use crate::util::{bincode, spawn_thread, Bytes};

static DB_VERSION: u32 = 2;

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
        debug!("opening DB at {:?}", path);
        let mut db_opts = rocksdb::Options::default();
        db_opts.create_if_missing(true);
        db_opts.set_max_open_files(100_000); // TODO: make sure to `ulimit -n` this process correctly
        db_opts.set_compaction_style(rocksdb::DBCompactionStyle::Level);
        db_opts.set_compression_type(rocksdb::DBCompressionType::Snappy);
        db_opts.set_target_file_size_base(1_073_741_824);
        db_opts.set_disable_auto_compactions(!config.initial_sync_compaction); // for initial bulk load

        let parallelism: i32 = config
            .db_parallelism
            .try_into()
            .expect("db_parallelism value too large for i32");

        // Configure parallelism (background jobs and thread pools)
        db_opts.increase_parallelism(parallelism);

        // Configure write buffer size (not set by increase_parallelism)
        db_opts.set_write_buffer_size(config.db_write_buffer_size_mb * 1024 * 1024);

        // db_opts.set_advise_random_on_open(???);
        db_opts.set_compaction_readahead_size(1 << 20);

        // Configure block cache
        let mut block_opts = rocksdb::BlockBasedOptions::default();
        let cache_size_bytes = config.db_block_cache_mb * 1024 * 1024;
        block_opts.set_block_cache(&rocksdb::Cache::new_lru_cache(cache_size_bytes));
        db_opts.set_block_based_table_factory(&block_opts);

        let db = DB {
            db: Arc::new(rocksdb::DB::open(&db_opts, path).expect("failed to open RocksDB")),
        };
        if verify_compat {
            db.verify_compatibility(config);
        }
        db
    }

    pub fn full_compaction(&self) {
        // TODO: make sure this doesn't fail silently
        info!("starting full compaction on {:?}", self.db);
        self.db.compact_range(None::<&[u8]>, None::<&[u8]>);
        info!("finished full compaction on {:?}", self.db);
    }

    pub fn enable_auto_compaction(&self) {
        let opts = [("disable_auto_compactions", "false")];
        self.db.set_options(&opts).unwrap();
    }

    pub fn raw_iterator(&self) -> rocksdb::DBRawIterator {
        self.db.raw_iterator()
    }

    pub fn iter_scan(&self, prefix: &[u8]) -> ScanIterator {
        ScanIterator {
            prefix: prefix.to_vec(),
            iter: self.db.prefix_iterator(prefix),
            done: false,
        }
    }

    pub fn iter_scan_from(&self, prefix: &[u8], start_at: &[u8]) -> ScanIterator {
        let iter = self.db.iterator(rocksdb::IteratorMode::From(
            start_at,
            rocksdb::Direction::Forward,
        ));
        ScanIterator {
            prefix: prefix.to_vec(),
            iter,
            done: false,
        }
    }

    pub fn iter_scan_reverse(&self, prefix: &[u8], prefix_max: &[u8]) -> ReverseScanIterator {
        let mut iter = self.db.raw_iterator();
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
        rows.sort_unstable_by(|a, b| a.key.cmp(&b.key));
        let mut batch = rocksdb::WriteBatch::default();
        for row in rows {
            batch.put(&row.key, &row.value);
        }
        self.write_batch(batch, flush)
    }

    pub fn delete_rows(&self, mut rows: Vec<DBRow>, flush: DBFlush) {
        log::trace!("deleting {} rows from {:?}", rows.len(), self.db,);
        rows.sort_unstable_by(|a, b| a.key.cmp(&b.key));
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
        let compatibility_bytes =
            bincode::serialize_little(&(DB_VERSION, config.light_mode)).unwrap();

        match self.get(b"V") {
            None => self.put(b"V", &compatibility_bytes),
            Some(x) if x != compatibility_bytes => {
                panic!("Incompatible database found. Please reindex or migrate.")
            }
            Some(_) => (),
        }
    }

    pub fn start_stats_exporter(&self, db_metrics: Arc<RocksDbMetrics>, db_name: &str) {
        let db_arc = Arc::clone(&self.db);
        let label = db_name.to_string();

        let update_gauge = move |gauge: &GaugeVec, property: &str| {
            if let Ok(Some(value)) = db_arc.property_value(property) {
                if let Ok(v) = value.parse::<f64>() {
                    gauge.with_label_values(&[&label]).set(v);
                }
            }
        };

        spawn_thread("db_stats_exporter", move || loop {
            update_gauge(
                &db_metrics.num_immutable_mem_table,
                "rocksdb.num-immutable-mem-table",
            );
            update_gauge(
                &db_metrics.mem_table_flush_pending,
                "rocksdb.mem-table-flush-pending",
            );
            update_gauge(&db_metrics.compaction_pending, "rocksdb.compaction-pending");
            update_gauge(&db_metrics.background_errors, "rocksdb.background-errors");
            update_gauge(
                &db_metrics.cur_size_active_mem_table,
                "rocksdb.cur-size-active-mem-table",
            );
            update_gauge(
                &db_metrics.cur_size_all_mem_tables,
                "rocksdb.cur-size-all-mem-tables",
            );
            update_gauge(
                &db_metrics.size_all_mem_tables,
                "rocksdb.size-all-mem-tables",
            );
            update_gauge(
                &db_metrics.num_entries_active_mem_table,
                "rocksdb.num-entries-active-mem-table",
            );
            update_gauge(
                &db_metrics.num_entries_imm_mem_tables,
                "rocksdb.num-entries-imm-mem-tables",
            );
            update_gauge(
                &db_metrics.num_deletes_active_mem_table,
                "rocksdb.num-deletes-active-mem-table",
            );
            update_gauge(
                &db_metrics.num_deletes_imm_mem_tables,
                "rocksdb.num-deletes-imm-mem-tables",
            );
            update_gauge(&db_metrics.estimate_num_keys, "rocksdb.estimate-num-keys");
            update_gauge(
                &db_metrics.estimate_table_readers_mem,
                "rocksdb.estimate-table-readers-mem",
            );
            update_gauge(
                &db_metrics.is_file_deletions_enabled,
                "rocksdb.is-file-deletions-enabled",
            );
            update_gauge(&db_metrics.num_snapshots, "rocksdb.num-snapshots");
            update_gauge(
                &db_metrics.oldest_snapshot_time,
                "rocksdb.oldest-snapshot-time",
            );
            update_gauge(&db_metrics.num_live_versions, "rocksdb.num-live-versions");
            update_gauge(
                &db_metrics.current_super_version_number,
                "rocksdb.current-super-version-number",
            );
            update_gauge(
                &db_metrics.estimate_live_data_size,
                "rocksdb.estimate-live-data-size",
            );
            update_gauge(
                &db_metrics.min_log_number_to_keep,
                "rocksdb.min-log-number-to-keep",
            );
            update_gauge(
                &db_metrics.min_obsolete_sst_number_to_keep,
                "rocksdb.min-obsolete-sst-number-to-keep",
            );
            update_gauge(
                &db_metrics.total_sst_files_size,
                "rocksdb.total-sst-files-size",
            );
            update_gauge(
                &db_metrics.live_sst_files_size,
                "rocksdb.live-sst-files-size",
            );
            update_gauge(&db_metrics.base_level, "rocksdb.base-level");
            update_gauge(
                &db_metrics.estimate_pending_compaction_bytes,
                "rocksdb.estimate-pending-compaction-bytes",
            );
            update_gauge(
                &db_metrics.num_running_compactions,
                "rocksdb.num-running-compactions",
            );
            update_gauge(
                &db_metrics.num_running_flushes,
                "rocksdb.num-running-flushes",
            );
            update_gauge(
                &db_metrics.actual_delayed_write_rate,
                "rocksdb.actual-delayed-write-rate",
            );
            update_gauge(&db_metrics.is_write_stopped, "rocksdb.is-write-stopped");
            update_gauge(
                &db_metrics.estimate_oldest_key_time,
                "rocksdb.estimate-oldest-key-time",
            );
            update_gauge(
                &db_metrics.block_cache_capacity,
                "rocksdb.block-cache-capacity",
            );
            update_gauge(&db_metrics.block_cache_usage, "rocksdb.block-cache-usage");
            update_gauge(
                &db_metrics.block_cache_pinned_usage,
                "rocksdb.block-cache-pinned-usage",
            );
            thread::sleep(Duration::from_secs(5));
        });
    }
}
