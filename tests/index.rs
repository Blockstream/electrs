#![cfg(not(feature = "liquid"))]

pub mod common;

use bitcoin::consensus::encode::{serialize, serialize_hex};
use bitcoin::hashes::{sha256, Hash};
use bitcoin::{Block, BlockHash, ScriptBuf};
use electrs::config::{Config, SensitiveAuth};
use electrs::metrics::Metrics;
use electrs::new_index::{ChainQuery, FetchFrom, Indexer, Store};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::sync::{Arc, Mutex};

struct Chain {
    active: Vec<Block>,
    blocks: HashMap<BlockHash, (usize, Block)>,
    unavailable: HashSet<BlockHash>,
    fetched: Vec<BlockHash>,
}

impl Chain {
    fn set_active(&mut self, blocks: Vec<Block>) {
        self.blocks.extend(
            blocks
                .iter()
                .enumerate()
                .map(|(h, b)| (b.block_hash(), (h, b.clone()))),
        );
        self.active = blocks;
    }

    fn reply(&mut self, method: &str, params: &Value) -> Result<Value, Value> {
        match method {
            "getblockchaininfo" => Ok(json!({"chain": "regtest", "blocks": self.active.len() - 1,
                "headers": self.active.len() - 1, "bestblockhash": self.active.last().unwrap().block_hash(),
                "pruned": false, "verificationprogress": 1.0})),
            "getbestblockhash" => Ok(json!(self.active.last().unwrap().block_hash())),
            "getblockhash" => Ok(json!(
                self.active[params[0].as_u64().unwrap() as usize].block_hash()
            )),
            "getblockheader" | "getblock" => {
                let hash: BlockHash = serde_json::from_value(params[0].clone()).unwrap();
                let (height, block) = &self.blocks[&hash];
                if method == "getblockheader" {
                    return Ok(if params[1] == false {
                        json!(serialize_hex(&block.header))
                    } else {
                        json!({"height": height})
                    });
                }
                self.fetched.push(hash);
                if self.unavailable.contains(&hash) {
                    return Err(json!({"code": -5, "message": "block unavailable"}));
                }
                // Verbosity 1 is what light mode uses to list a block's txids.
                Ok(match params[1].as_u64() {
                    Some(1) => {
                        let txids: Vec<_> =
                            block.txdata.iter().map(|tx| tx.compute_txid()).collect();
                        json!({"hash": hash, "height": height, "tx": txids})
                    }
                    _ => json!(serialize_hex(block)),
                })
            }
            "getrawtransaction" => {
                let txid: bitcoin::Txid = serde_json::from_value(params[0].clone()).unwrap();
                let blockhash: BlockHash = serde_json::from_value(params[2].clone()).unwrap();
                let (_, block) = &self.blocks[&blockhash];
                let tx = block
                    .txdata
                    .iter()
                    .find(|tx| tx.compute_txid() == txid)
                    .unwrap();
                Ok(json!(serialize_hex(tx)))
            }
            _ => panic!("unexpected RPC: {}", method),
        }
    }
}

fn blocks() -> Vec<Block> {
    let genesis = bitcoin::blockdata::constants::genesis_block(bitcoin::Network::Regtest);
    let mut previous = BlockHash::all_zeros();
    (0..4)
        .map(|height| {
            let mut block = genesis.clone();
            block.header.prev_blockhash = previous;
            block.header.time += height;
            block.txdata[0].lock_time = bitcoin::absolute::LockTime::from_consensus(height);
            block.txdata[0].output[0].script_pubkey =
                ScriptBuf::from_bytes(vec![if height == 3 { 0x51 } else { 0x52 }]);
            block.header.merkle_root = block.compute_merkle_root().unwrap();
            previous = block.block_hash();
            block
        })
        .collect()
}

struct Fixture {
    _dir: tempfile::TempDir,
    config: Config,
    chain: Arc<Mutex<Chain>>,
    rpc: common::MockRpc,
}

// Surface mock handler panics (e.g. an unexpected RPC) that the client only saw as an
// RPC error, unless the test is already failing for its own reason.
impl Drop for Fixture {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            self.rpc.assert_no_panics();
        }
    }
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let chain = Arc::new(Mutex::new(Chain {
            active: vec![],
            blocks: HashMap::new(),
            unavailable: HashSet::new(),
            fetched: vec![],
        }));
        chain.lock().unwrap().set_active(blocks());
        let state = Arc::clone(&chain);
        let rpc =
            common::MockRpc::new(move |method, params| state.lock().unwrap().reply(method, params));
        let mut config =
            common::test_config(dir.path().join("db"), dir.path().to_owned(), rpc.addr());
        config.cookie = Some(SensitiveAuth::new("test:test".to_owned()));
        config.db_write_buffer_size_mb = 8;
        config.initial_sync_batch_size = 1;
        std::fs::create_dir_all(&config.blocks_dir).unwrap();
        Self {
            _dir: dir,
            config,
            chain,
            rpc,
        }
    }

    fn open(
        &self,
        from: FetchFrom,
    ) -> (Arc<Store>, Indexer, Arc<electrs::daemon::Daemon>, Metrics) {
        let metrics = Metrics::new("127.0.0.1:0".parse().unwrap());
        let daemon = common::test_daemon(&self.config, &metrics);
        let store = Arc::new(Store::open(&self.config, &metrics, false));
        let indexer = Indexer::open(Arc::clone(&store), from, &self.config, &metrics);
        (store, indexer, daemon, metrics)
    }

    fn write_file(&self, heights: &[usize]) {
        let mut data = Vec::new();
        for &height in heights {
            let raw = serialize(&self.chain.lock().unwrap().active[height]);
            data.extend(self.config.network_type.magic().to_le_bytes());
            data.extend((raw.len() as u32).to_le_bytes());
            data.extend(raw);
        }
        std::fs::write(self.config.blocks_dir.join("blk00000.dat"), data).unwrap();
    }

    fn prepare_gap(&self) {
        let heights: Vec<_> = (0..self.chain.lock().unwrap().active.len())
            .filter(|&height| height != 2)
            .collect();
        self.write_file(&heights);
        let gap = self.chain.lock().unwrap().active[2].block_hash();
        self.chain.lock().unwrap().unavailable.insert(gap);
    }

    fn make_gap(&self, indexer: &mut Indexer, store: &Store, daemon: &electrs::daemon::Daemon) {
        self.prepare_gap();
        assert_eq!(
            indexer.update(daemon).unwrap(),
            self.chain.lock().unwrap().active[1].block_hash()
        );
        assert_eq!(store.headers().len(), 2);
        assert!(store
            .history_db()
            .get(
                &[
                    b"D".as_slice(),
                    self.chain.lock().unwrap().active[3].block_hash().as_ref()
                ]
                .concat()
            )
            .is_some());
    }

    fn switch_branch(&self) {
        self.switch_branch_from(2);
    }

    fn switch_branch_from(&self, since: usize) {
        let mut chain = self.chain.lock().unwrap();
        let mut replacement = chain.active.clone();
        for height in since..replacement.len() {
            replacement[height].txdata[0].output[0].script_pubkey =
                ScriptBuf::from_bytes(vec![0x53]);
            replacement[height].header.prev_blockhash = replacement[height - 1].block_hash();
            replacement[height].header.merkle_root =
                replacement[height].compute_merkle_root().unwrap();
        }
        chain.set_active(replacement);
    }

    // Cleanup rebuilds stale blocks from txstore, so a failure needs the block gone from
    // both txstore (its txid list) and bitcoind.
    fn make_unrecoverable(&self, store: &Store, hash: BlockHash) {
        store.txstore_db().delete_rows(
            vec![electrs::new_index::DBRow {
                key: [b"X".as_slice(), hash.as_ref()].concat(),
                value: vec![],
            }],
            electrs::new_index::db::DBFlush::Enable,
        );
        self.chain.lock().unwrap().unavailable.insert(hash);
    }

    fn run_damage_process(&self, damage: Option<(BlockHash, bitcoin::Txid)>) {
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "light_mode_rewrites_shared_rows_from_fetched_active_blocks",
            ])
            .env("DAMAGE_DIR", self.config.daemon_dir.as_os_str())
            .env("DAMAGE_RPC", self.rpc.addr().to_string());
        if let Some((stale, shared)) = damage {
            command
                .env("DAMAGE_STALE", stale.to_string())
                .env("DAMAGE_SHARED", shared.to_string());
        }
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "damage process failed: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn extend_chain(&self) {
        let mut chain = self.chain.lock().unwrap();
        let mut active = chain.active.clone();
        let mut extra = active.last().unwrap().clone();
        extra.header.prev_blockhash = active.last().unwrap().block_hash();
        extra.header.time += 1;
        extra.txdata[0].lock_time =
            bitcoin::absolute::LockTime::from_consensus(active.len() as u32);
        extra.header.merkle_root = extra.compute_merkle_root().unwrap();
        active.push(extra);
        chain.set_active(active);
    }

    fn restart_after_import(&self, legacy: bool) {
        self.prepare_gap();
        self.run_import_process(legacy, false);
    }

    fn run_import_process(&self, legacy: bool, cleanup: bool) {
        let hash = self.chain.lock().unwrap().active[3].block_hash();
        self.run_import_process_for(hash, legacy, cleanup, false);
    }

    fn run_import_process_with_loss(&self, legacy: bool, cleanup: bool, lose_txstore: bool) {
        let hash = self.chain.lock().unwrap().active[3].block_hash();
        self.run_import_process_for(hash, legacy, cleanup, lose_txstore);
    }

    // `hash` names the block the child process damages; for cleanup runs this is the
    // stale block, which is no longer in `active` after a branch switch.
    fn run_import_process_for(
        &self,
        hash: BlockHash,
        legacy: bool,
        cleanup: bool,
        lose_txstore: bool,
    ) {
        let hash = hash.to_string();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "history_above_gap_is_removed_before_accepting_another_branch",
            ])
            .env("GAP_IMPORT_DIR", self.config.daemon_dir.as_os_str())
            .env("GAP_IMPORT_RPC", self.rpc.addr().to_string())
            .env("GAP_IMPORT_LEGACY", legacy.to_string())
            .env("GAP_IMPORT_CLEANUP", cleanup.to_string())
            .env("GAP_IMPORT_HASH", hash)
            .env("GAP_IMPORT_LOSE_TXSTORE", lose_txstore.to_string())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "import process failed: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn history_above_gap_is_removed_before_accepting_another_branch() {
    if import_process_if_requested() {
        return;
    }
    for (restart, legacy) in [(false, false), (true, false), (true, true)] {
        let fixture = Fixture::new();
        if restart {
            fixture.restart_after_import(legacy);
        }
        let (store, mut indexer, daemon, metrics) = fixture.open(if restart {
            FetchFrom::Bitcoind
        } else {
            FetchFrom::BlkFiles
        });
        if !restart {
            fixture.make_gap(&mut indexer, &store, &daemon);
        }
        let stale = fixture.chain.lock().unwrap().active[3].clone();
        fixture.switch_branch();
        let tip = indexer.update(&daemon).unwrap();
        assert_eq!(tip, fixture.chain.lock().unwrap().active[3].block_hash());
        let query = ChainQuery::new(Arc::clone(&store), daemon, &fixture.config, &metrics);
        let hash =
            sha256::Hash::hash(stale.txdata[0].output[0].script_pubkey.as_bytes()).to_byte_array();
        assert!(query.history(&hash, None, 10).unwrap().is_empty());
        assert!(query.utxo(&hash, 10).unwrap().is_empty());
        assert_eq!(query.stats(&hash).unwrap().funded_txo_sum, 0);
        assert!(query
            .tx_confirming_block(&stale.txdata[0].compute_txid())
            .is_none());
        assert!(store
            .history_db()
            .get(&[b"D".as_slice(), stale.block_hash().as_ref()].concat())
            .is_none());
        assert!(query.get_block_txids(&stale.block_hash()).is_some());
    }
}

#[test]
fn filling_gap_on_same_branch_reuses_completed_blocks() {
    let fixture = Fixture::new();
    let (store, mut indexer, daemon, metrics) = fixture.open(FetchFrom::BlkFiles);
    fixture.make_gap(&mut indexer, &store, &daemon);
    fixture.chain.lock().unwrap().unavailable.clear();
    fixture.chain.lock().unwrap().fetched.clear();
    let tip = indexer.update(&daemon).unwrap();
    let chain = fixture.chain.lock().unwrap();
    assert_eq!(tip, chain.active[3].block_hash());
    assert_eq!(chain.fetched, vec![chain.active[2].block_hash()]);
    let query = ChainQuery::new(store, daemon, &fixture.config, &metrics);
    assert!(query
        .tx_confirming_block(&chain.active[3].txdata[0].compute_txid())
        .is_some());
}

#[test]
fn failed_reorg_saves_common_ancestor_before_retrying_same_indexer() {
    for gap in [false, true] {
        let fixture = Fixture::new();
        if gap {
            fixture.extend_chain();
        }
        let (store, mut indexer, daemon, metrics) = fixture.open(if gap {
            FetchFrom::BlkFiles
        } else {
            FetchFrom::Bitcoind
        });
        if gap {
            fixture.make_gap(&mut indexer, &store, &daemon);
        } else {
            indexer.update(&daemon).unwrap();
        }
        let old = fixture.chain.lock().unwrap().active.clone();
        let since = if gap { 1 } else { 2 };
        fixture.switch_branch_from(since);
        fixture.make_unrecoverable(&store, old[3].block_hash());

        // A failed cleanup keeps serving the consistent ancestor instead of exiting.
        let tip = indexer.update(&daemon).unwrap();
        assert_eq!(tip, old[since - 1].block_hash());
        assert!(indexer.stale_cleanup_failing());
        assert_eq!(store.headers().len(), since);
        let saved_tip: BlockHash =
            bitcoin::consensus::deserialize(&store.txstore_db().get(b"t").unwrap()).unwrap();
        assert_eq!(saved_tip, old[since - 1].block_hash());
        if gap {
            assert!(store
                .history_db()
                .get(&[b"D".as_slice(), old[4].block_hash().as_ref()].concat())
                .is_none());
            assert!(store
                .history_db()
                .get(&[b"D".as_slice(), old[3].block_hash().as_ref()].concat())
                .is_some());
        }
        let query = ChainQuery::new(
            Arc::clone(&store),
            Arc::clone(&daemon),
            &fixture.config,
            &metrics,
        );
        for block in &old[since..] {
            assert!(query
                .tx_confirming_block(&block.txdata[0].compute_txid())
                .is_none());
        }
        fixture.chain.lock().unwrap().unavailable.clear();
        indexer.retry_cleanup_now();
        let tip = indexer.update(&daemon).unwrap();
        assert!(!indexer.stale_cleanup_failing());
        let active = fixture.chain.lock().unwrap().active.clone();
        assert_eq!(tip, active.last().unwrap().block_hash());
        let stale_script =
            sha256::Hash::hash(old[3].txdata[0].output[0].script_pubkey.as_bytes()).to_byte_array();
        assert!(query.history(&stale_script, None, 10).unwrap().is_empty());
        assert!(query.utxo(&stale_script, 10).unwrap().is_empty());
        assert_eq!(query.stats(&stale_script).unwrap().funded_txo_sum, 0);
        for block in &active[since..] {
            assert_eq!(
                query
                    .tx_confirming_block(&block.txdata[0].compute_txid())
                    .unwrap()
                    .hash,
                block.block_hash()
            );
        }
    }
}

#[test]
fn cleanup_failure_can_be_retried_after_restart() {
    let fixture = Fixture::new();
    fixture.extend_chain();
    fixture.restart_after_import(false);
    let old = fixture.chain.lock().unwrap().active.clone();
    fixture.switch_branch();
    fixture
        .chain
        .lock()
        .unwrap()
        .unavailable
        .insert(old[3].block_hash());
    fixture.run_import_process_for(old[3].block_hash(), false, true, false);
    let (store, mut indexer, daemon, _) = fixture.open(FetchFrom::Bitcoind);
    assert!(store
        .history_db()
        .get(&[b"D".as_slice(), old[4].block_hash().as_ref()].concat())
        .is_none());
    assert!(store
        .history_db()
        .get(&[b"D".as_slice(), old[3].block_hash().as_ref()].concat())
        .is_some());
    fixture.chain.lock().unwrap().unavailable.clear();
    assert_eq!(
        indexer.update(&daemon).unwrap(),
        fixture.chain.lock().unwrap().active[4].block_hash()
    );
    assert!(!indexer.stale_cleanup_failing());
    // old[4] still has txstore rows, so it is rebuilt locally rather than fetched.
    let chain = fixture.chain.lock().unwrap();
    assert!(!chain.fetched.contains(&old[4].block_hash()));
    assert!(store
        .history_db()
        .get(&[b"D".as_slice(), old[3].block_hash().as_ref()].concat())
        .is_none());
}

fn import_process_if_requested() -> bool {
    // Import in a separate process so restart coverage closes and reopens the actual DBs.
    // (The metrics exporter retains DB handles until process exit.)
    if let Some(root) = std::env::var_os("GAP_IMPORT_DIR") {
        let root = std::path::PathBuf::from(root);
        let mut config = common::test_config(
            root.join("db"),
            root,
            std::env::var("GAP_IMPORT_RPC").unwrap().parse().unwrap(),
        );
        config.cookie = Some(SensitiveAuth::new("test:test".to_owned()));
        config.initial_sync_batch_size = 1;
        let metrics = Metrics::new("127.0.0.1:0".parse().unwrap());
        let daemon = common::test_daemon(&config, &metrics);
        let store = Arc::new(Store::open(&config, &metrics, false));
        let cleanup = std::env::var("GAP_IMPORT_CLEANUP").unwrap() == "true";
        let mut indexer = Indexer::open(
            Arc::clone(&store),
            if cleanup {
                FetchFrom::Bitcoind
            } else {
                FetchFrom::BlkFiles
            },
            &config,
            &metrics,
        );
        if cleanup {
            let hash: BlockHash = std::env::var("GAP_IMPORT_HASH").unwrap().parse().unwrap();
            store.txstore_db().delete_rows(
                vec![electrs::new_index::DBRow {
                    key: [b"X".as_slice(), hash.as_ref()].concat(),
                    value: vec![],
                }],
                electrs::new_index::db::DBFlush::Enable,
            );
            indexer.update(&daemon).unwrap();
            assert!(indexer.stale_cleanup_failing());
            // Model a crash after a completed deletion batch has reached disk.
            store.history_db().flush();
        } else {
            indexer.update(&daemon).unwrap();
        }
        assert_eq!(store.headers().len(), 2);
        if std::env::var("GAP_IMPORT_LEGACY").unwrap() == "true" {
            let hash: BlockHash = std::env::var("GAP_IMPORT_HASH").unwrap().parse().unwrap();
            store
                .history_db()
                .put_sync(&[b"D".as_slice(), hash.as_ref()].concat(), &[]);
        }
        if std::env::var("GAP_IMPORT_LOSE_TXSTORE").unwrap() == "true" {
            // Model independently flushed stores: history survived, while one
            // uncommitted block's entire txstore batch was lost.
            let hash: BlockHash = std::env::var("GAP_IMPORT_HASH").unwrap().parse().unwrap();
            let block = daemon.getblock(&hash).unwrap();
            let txids: HashSet<_> = block
                .txdata
                .iter()
                .map(|tx| tx.compute_txid().to_byte_array())
                .collect();
            let rows = store
                .txstore_db()
                .iter_scan(b"")
                .filter(|row| match row.key.first() {
                    Some(b'B' | b'D' | b'X' | b'M') => row.key[1..] == hash[..],
                    Some(b'T' | b'O') => {
                        txids.contains(&<[u8; 32]>::try_from(&row.key[1..33]).unwrap())
                    }
                    _ => false,
                })
                .collect();
            store
                .txstore_db()
                .delete_rows(rows, electrs::new_index::db::DBFlush::Enable);
        }
        return true;
    }
    false
}

#[test]
fn recovery_undoes_history_after_losing_txstore_header_marker_and_prevout() {
    for legacy in [false, true] {
        let fixture = Fixture::new();
        let (funding, spending) = {
            let mut chain = fixture.chain.lock().unwrap();
            let mut active = chain.active.clone();
            let funding = active[3].txdata[0].compute_txid();
            let mut next = active[3].clone();
            next.header.prev_blockhash = active[3].block_hash();
            next.header.time += 1;
            next.txdata[0].lock_time = bitcoin::absolute::LockTime::from_consensus(4);
            let mut spend = next.txdata[0].clone();
            spend.input[0].previous_output = bitcoin::OutPoint {
                txid: funding,
                vout: 0,
            };
            spend.output[0].script_pubkey = ScriptBuf::from_bytes(vec![0x54]);
            let spending = spend.compute_txid();
            next.txdata.push(spend);
            next.header.merkle_root = next.compute_merkle_root().unwrap();
            active.push(next);
            chain.set_active(active);
            (funding, spending)
        };
        fixture.prepare_gap();
        fixture.run_import_process_with_loss(legacy, false, true);
        let (store, mut indexer, daemon, metrics) = fixture.open(FetchFrom::Bitcoind);
        let old = fixture.chain.lock().unwrap().active.clone();
        assert!(store
            .txstore_db()
            .get(&[b"B".as_slice(), old[3].block_hash().as_ref()].concat())
            .is_none());
        assert!(store
            .txstore_db()
            .get(&[b"D".as_slice(), old[3].block_hash().as_ref()].concat())
            .is_none());
        assert!(store
            .history_db()
            .get(&[b"D".as_slice(), old[3].block_hash().as_ref()].concat())
            .is_some());
        fixture.switch_branch();
        {
            let mut chain = fixture.chain.lock().unwrap();
            let mut active = chain.active.clone();
            active[4].txdata.truncate(1);
            active[4].header.merkle_root = active[4].compute_merkle_root().unwrap();
            chain.set_active(active);
        }
        assert_eq!(
            indexer.update(&daemon).unwrap(),
            fixture.chain.lock().unwrap().active[4].block_hash()
        );
        let query = ChainQuery::new(Arc::clone(&store), daemon, &fixture.config, &metrics);
        for script in [0x51, 0x54] {
            let hash = sha256::Hash::hash(&[script]).to_byte_array();
            assert!(query.history(&hash, None, 10).unwrap().is_empty());
            assert!(query.utxo(&hash, 10).unwrap().is_empty());
            assert_eq!(query.stats(&hash).unwrap().tx_count, 0);
        }
        assert!(query.tx_confirming_block(&funding).is_none());
        assert!(query.tx_confirming_block(&spending).is_none());
        assert!(query
            .lookup_spend(&bitcoin::OutPoint {
                txid: funding,
                vout: 0
            })
            .is_none());
        for block in &old[3..] {
            assert!(store
                .history_db()
                .get(&[b"D".as_slice(), block.block_hash().as_ref()].concat())
                .is_none());
        }
    }
}

// Light mode stores no txids or transactions in txstore, so cleanup must fetch stale
// blocks over RPC and queries must still see a consistent result afterwards.
#[test]
fn light_mode_reorg_removes_stale_history() {
    let mut fixture = Fixture::new();
    fixture.config.light_mode = true;
    let (store, mut indexer, daemon, metrics) = fixture.open(FetchFrom::Bitcoind);
    let old = fixture.chain.lock().unwrap().active.clone();
    assert_eq!(indexer.update(&daemon).unwrap(), old[3].block_hash());

    fixture.switch_branch_from(2);
    fixture.chain.lock().unwrap().fetched.clear();
    let active = fixture.chain.lock().unwrap().active.clone();
    assert_eq!(indexer.update(&daemon).unwrap(), active[3].block_hash());
    assert!(!indexer.stale_cleanup_failing());
    // Without X/T rows the stale blocks cannot be rebuilt locally.
    let fetched = fixture.chain.lock().unwrap().fetched.clone();
    assert!(fetched.contains(&old[2].block_hash()));
    assert!(fetched.contains(&old[3].block_hash()));

    let query = ChainQuery::new(Arc::clone(&store), daemon, &fixture.config, &metrics);
    for height in 2..=3 {
        assert!(query
            .tx_confirming_block(&old[height].txdata[0].compute_txid())
            .is_none());
        assert_eq!(
            query
                .tx_confirming_block(&active[height].txdata[0].compute_txid())
                .map(|b| b.hash),
            Some(active[height].block_hash())
        );
    }
    let stale_script =
        sha256::Hash::hash(old[3].txdata[0].output[0].script_pubkey.as_bytes()).to_byte_array();
    assert!(query.history(&stale_script, None, 10).unwrap().is_empty());
}

// A crash after a cleanup batch deleted shared rows but before they were rewritten leaves
// the pending-rewrite marker. The next update must finish the rewrite even when no stale
// blocks remain, and then clear the marker.
#[test]
fn pending_rewrite_survives_a_crash_and_completes_without_stale_blocks() {
    let fixture = Fixture::new();
    let (store, mut indexer, daemon, metrics) = fixture.open(FetchFrom::Bitcoind);
    let active = fixture.chain.lock().unwrap().active.clone();
    assert_eq!(indexer.update(&daemon).unwrap(), active[3].block_hash());

    let txid = active[2].txdata[0].compute_txid();
    let conf_key = [b"C".as_slice(), txid.as_ref()].concat();
    let conf = store.history_db().get(&conf_key).unwrap();
    store.history_db().delete_rows(
        vec![electrs::new_index::DBRow {
            key: conf_key.clone(),
            value: vec![],
        }],
        electrs::new_index::db::DBFlush::Enable,
    );
    store.history_db().put_sync(b"R", &2u32.to_le_bytes());

    // Nothing new and nothing stale: only the pending marker drives this cleanup.
    assert_eq!(indexer.update(&daemon).unwrap(), active[3].block_hash());
    assert_eq!(store.history_db().get(&conf_key), Some(conf));
    assert!(store.history_db().get(b"R").is_none());
    let query = ChainQuery::new(Arc::clone(&store), daemon, &fixture.config, &metrics);
    assert_eq!(
        query.tx_confirming_block(&txid).map(|b| b.hash),
        Some(active[2].block_hash())
    );
}

// Light mode keeps no transactions in txstore, so rewriting rows of an indexed active block
// must fetch that block over RPC. That only happens in the #140-damaged state: a stale
// block that owns a shared row and was never undone. Child processes sync each branch and
// apply the damage, so the parent finds it after a real restart, as production would.
#[test]
fn light_mode_rewrites_shared_rows_from_fetched_active_blocks() {
    if damage_process_if_requested() {
        return;
    }
    let mut fixture = Fixture::new();
    fixture.config.light_mode = true;
    let old = fixture.chain.lock().unwrap().active.clone();
    fixture.run_damage_process(None);
    fixture.switch_branch_from(3);
    let active = fixture.chain.lock().unwrap().active.clone();
    let shared = active[3].txdata[0].compute_txid();
    fixture.run_damage_process(Some((old[3].block_hash(), shared)));

    let (store, mut indexer, daemon, metrics) = fixture.open(FetchFrom::Bitcoind);
    fixture.chain.lock().unwrap().fetched.clear();
    assert_eq!(indexer.update(&daemon).unwrap(), active[3].block_hash());
    assert!(!indexer.stale_cleanup_failing());
    let fetched = fixture.chain.lock().unwrap().fetched.clone();
    assert!(
        fetched.contains(&old[3].block_hash()),
        "stale block fetched"
    );
    assert!(
        fetched.contains(&active[3].block_hash()),
        "active block fetched to rewrite its rows"
    );
    assert!(store
        .history_db()
        .get(&[b"D".as_slice(), old[3].block_hash().as_ref()].concat())
        .is_none());
    let query = ChainQuery::new(Arc::clone(&store), daemon, &fixture.config, &metrics);
    assert_eq!(
        query.tx_confirming_block(&shared).map(|b| b.hash),
        Some(active[3].block_hash())
    );
}

// Syncs the current mock chain in light mode. With `damage`, then restores the stale
// block's done-marker and points the shared transaction's confirmation row at it, as
// pre-fix code could leave them.
fn damage_process_if_requested() -> bool {
    let root = match std::env::var_os("DAMAGE_DIR") {
        Some(root) => std::path::PathBuf::from(root),
        None => return false,
    };
    let rpc = std::env::var("DAMAGE_RPC").unwrap().parse().unwrap();
    let mut config = common::test_config(root.join("db"), root, rpc);
    config.cookie = Some(SensitiveAuth::new("test:test".to_owned()));
    config.light_mode = true;
    let metrics = Metrics::new("127.0.0.1:0".parse().unwrap());
    let daemon = common::test_daemon(&config, &metrics);
    let store = Arc::new(Store::open(&config, &metrics, false));
    let mut indexer = Indexer::open(Arc::clone(&store), FetchFrom::Bitcoind, &config, &metrics);
    let old_history: Vec<_> = store
        .history_db()
        .iter_scan(
            &[
                b"H".as_slice(),
                &sha256::Hash::hash(&[0x51]).to_byte_array(),
            ]
            .concat(),
        )
        .collect();
    indexer.update(&daemon).unwrap();
    if std::env::var_os("DAMAGE_CACHE").is_some() {
        store
            .history_db()
            .write_rows(old_history, electrs::new_index::db::DBFlush::Enable);
    }
    if let Ok(stale) = std::env::var("DAMAGE_STALE") {
        let stale: BlockHash = stale.parse().unwrap();
        let shared: bitcoin::Txid = std::env::var("DAMAGE_SHARED").unwrap().parse().unwrap();
        store.history_db().put_sync(
            &[b"D".as_slice(), stale.as_ref()].concat(),
            &3u32.to_le_bytes(),
        );
        store.history_db().put_sync(
            &[b"C".as_slice(), shared.as_ref()].concat(),
            &99u32.to_le_bytes(),
        );
    }
    true
}

// A failed stale cleanup on a quiet chain returns the saved tip, which already equals the
// daemon tip, so the main loop retries on `stale_cleanup_failing()` rather than a tip
// change. Both the failure signal and recovery without a new block are checked here.
#[test]
fn failed_cleanup_on_a_quiet_chain_recovers_when_retried() {
    let fixture = Fixture::new();
    let (store, mut indexer, daemon, _) = fixture.open(FetchFrom::Bitcoind);
    let active = fixture.chain.lock().unwrap().active.clone();
    assert_eq!(indexer.update(&daemon).unwrap(), active[3].block_hash());

    // A pending rewrite whose active block can be loaded from neither txstore nor bitcoind.
    store.history_db().put_sync(b"R", &3u32.to_le_bytes());
    fixture.make_unrecoverable(&store, active[3].block_hash());

    let tip = indexer.update(&daemon).unwrap();
    assert_eq!(
        tip,
        active[3].block_hash(),
        "quiet chain: saved tip is the daemon tip"
    );
    assert!(indexer.stale_cleanup_failing());
    assert!(store.history_db().get(b"R").is_some());

    // Bitcoind recovers; the same tip, retried, completes the rewrite.
    fixture.chain.lock().unwrap().unavailable.clear();
    indexer.retry_cleanup_now();
    assert_eq!(indexer.update(&daemon).unwrap(), active[3].block_hash());
    assert!(!indexer.stale_cleanup_failing());
    assert!(store.history_db().get(b"R").is_none());
}

#[test]
fn reorg_back_after_partial_undo_restores_history() {
    for restart in [false, true] {
        let fixture = Fixture::new();
        fixture.extend_chain();
        let old = {
            let mut chain = fixture.chain.lock().unwrap();
            let mut active = chain.active.clone();
            let mut spend = active[4].txdata[0].clone();
            spend.input[0].previous_output = bitcoin::OutPoint {
                txid: active[3].txdata[0].compute_txid(),
                vout: 0,
            };
            spend.output[0].script_pubkey = ScriptBuf::from_bytes(vec![0x54]);
            active[4].txdata.push(spend);
            active[4].header.merkle_root = active[4].compute_merkle_root().unwrap();
            chain.set_active(active.clone());
            active
        };
        fixture.prepare_gap();
        fixture.run_import_process_for(old[3].block_hash(), false, false, true);
        fixture.switch_branch();
        fixture
            .chain
            .lock()
            .unwrap()
            .unavailable
            .insert(old[3].block_hash());
        if restart {
            fixture.run_import_process_for(old[3].block_hash(), false, true, false);
        }
        let (store, mut indexer, daemon, metrics) = fixture.open(FetchFrom::Bitcoind);
        assert_eq!(store.headers().len(), 2);
        let script = sha256::Hash::hash(&[0x54]).to_byte_array();
        let query = ChainQuery::new(
            Arc::clone(&store),
            Arc::clone(&daemon),
            &fixture.config,
            &metrics,
        );
        if !restart {
            indexer.update(&daemon).unwrap();
            assert!(indexer.stale_cleanup_failing());
        }
        assert!(store
            .history_db()
            .get(&[b"D".as_slice(), old[4].block_hash().as_ref()].concat())
            .is_none());
        assert!(store
            .history_db()
            .get(&[b"u".as_slice(), old[4].block_hash().as_ref()].concat())
            .is_some());
        {
            let mut chain = fixture.chain.lock().unwrap();
            chain.unavailable.clear();
            chain.set_active(old.clone());
        }
        assert_eq!(indexer.update(&daemon).unwrap(), old[4].block_hash());
        assert!(!indexer.stale_cleanup_failing());
        assert_eq!(
            query.history(&script, None, 10).unwrap().len(),
            1,
            "reactivated block must recover its funding history"
        );
        assert!(store.history_db().iter_scan(b"u").next().is_none());
    }
}

#[test]
fn shared_transaction_reconfirmed_below_lowest_stale_height() {
    let mut fixture = Fixture::new();
    fixture.config.light_mode = true;
    let (old, shared_tx) = {
        let mut chain = fixture.chain.lock().unwrap();
        let mut active = chain.active.clone();
        let mut spend = active[3].txdata[0].clone();
        spend.input[0].previous_output = bitcoin::OutPoint {
            txid: active[1].txdata[0].compute_txid(),
            vout: 0,
        };
        spend.output[0].script_pubkey = ScriptBuf::from_bytes(vec![0x54]);
        active[3].txdata.push(spend.clone());
        active[3].header.merkle_root = active[3].compute_merkle_root().unwrap();
        chain.set_active(active.clone());
        (active, spend)
    };
    fixture.run_damage_process(None);
    fixture.switch_branch();
    {
        let mut chain = fixture.chain.lock().unwrap();
        let mut active = chain.active.clone();
        active[3].txdata.truncate(1);
        active[2].txdata.push(shared_tx.clone());
        active[2].header.merkle_root = active[2].compute_merkle_root().unwrap();
        active[3].header.prev_blockhash = active[2].block_hash();
        active[3].header.merkle_root = active[3].compute_merkle_root().unwrap();
        chain.set_active(active);
    }
    fixture.run_damage_process(Some((old[3].block_hash(), shared_tx.compute_txid())));
    let (store, mut indexer, daemon, metrics) = fixture.open(FetchFrom::Bitcoind);
    let active = fixture.chain.lock().unwrap().active.clone();
    assert_eq!(indexer.update(&daemon).unwrap(), active[3].block_hash());
    assert!(!indexer.stale_cleanup_failing());
    let query = ChainQuery::new(store, daemon, &fixture.config, &metrics);
    assert_eq!(
        query
            .tx_confirming_block(&shared_tx.compute_txid())
            .map(|b| b.hash),
        Some(active[2].block_hash())
    );
}

#[test]
fn same_block_prevout_does_not_need_full_history_scan() {
    let fixture = Fixture::new();
    let (old, funding) = {
        let mut chain = fixture.chain.lock().unwrap();
        let mut active = chain.active.clone();
        let mut fund = active[3].txdata[0].clone();
        fund.input[0].previous_output = bitcoin::OutPoint {
            txid: active[1].txdata[0].compute_txid(),
            vout: 0,
        };
        fund.output[0].script_pubkey = ScriptBuf::from_bytes(vec![0x54]);
        let mut spend = fund.clone();
        spend.input[0].previous_output = bitcoin::OutPoint {
            txid: fund.compute_txid(),
            vout: 0,
        };
        spend.output[0].script_pubkey = ScriptBuf::from_bytes(vec![0x55]);
        active[3].txdata.extend([fund.clone(), spend]);
        active[3].header.merkle_root = active[3].compute_merkle_root().unwrap();
        chain.set_active(active.clone());
        (active, fund.compute_txid())
    };
    let (store, mut indexer, daemon, metrics) = fixture.open(FetchFrom::Bitcoind);
    indexer.update(&daemon).unwrap();
    let prevout_key = [b"O".as_slice(), funding.as_ref(), &0u32.to_le_bytes()].concat();
    assert!(store.txstore_db().get(&prevout_key).is_some());
    store.txstore_db().delete_rows(
        vec![electrs::new_index::DBRow {
            key: prevout_key,
            value: vec![],
        }],
        electrs::new_index::db::DBFlush::Enable,
    );
    fixture.switch_branch();
    {
        let mut chain = fixture.chain.lock().unwrap();
        let mut active = chain.active.clone();
        active[3].txdata.truncate(1);
        active[3].header.merkle_root = active[3].compute_merkle_root().unwrap();
        chain.set_active(active);
    }
    assert_ne!(indexer.update(&daemon).unwrap(), old[3].block_hash());
    assert!(!indexer.stale_cleanup_failing());
    let scans: u64 = metrics
        .gather()
        .iter()
        .flat_map(|family| family.get_metric())
        .filter(|metric| {
            metric
                .get_label()
                .iter()
                .any(|label| label.value() == "undo_history_scan")
        })
        .map(|metric| metric.get_histogram().get_sample_count())
        .sum();
    assert_eq!(
        scans, 0,
        "all missing prevouts were in the same fetched block"
    );
}

#[test]
fn cleanup_invalidates_caches_built_from_legacy_stale_rows() {
    let mut fixture = Fixture::new();
    fixture.config.light_mode = true;
    let old = {
        let mut chain = fixture.chain.lock().unwrap();
        let mut active = chain.active.clone();
        let output = active[3].txdata[0].output[0].clone();
        active[3].txdata[0].output = vec![output; 101];
        active[3].header.merkle_root = active[3].compute_merkle_root().unwrap();
        chain.set_active(active.clone());
        active
    };
    fixture.run_damage_process(None);
    fixture.switch_branch();
    {
        let mut chain = fixture.chain.lock().unwrap();
        let mut active = chain.active.clone();
        active[3].txdata[0].output.truncate(1);
        active[3].header.merkle_root = active[3].compute_merkle_root().unwrap();
        chain.set_active(active);
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "light_mode_rewrites_shared_rows_from_fetched_active_blocks",
        ])
        .env("DAMAGE_DIR", fixture.config.daemon_dir.as_os_str())
        .env("DAMAGE_RPC", fixture.rpc.addr().to_string())
        .env("DAMAGE_STALE", old[3].block_hash().to_string())
        .env("DAMAGE_SHARED", old[3].txdata[0].compute_txid().to_string())
        .env("DAMAGE_CACHE", "true")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let (store, mut indexer, daemon, metrics) = fixture.open(FetchFrom::Bitcoind);
    let query = Arc::new(ChainQuery::new(
        Arc::clone(&store),
        Arc::clone(&daemon),
        &fixture.config,
        &metrics,
    ));
    let script = sha256::Hash::hash(&[0x51]).to_byte_array();
    assert_eq!(query.stats(&script).unwrap().funded_txo_count, 101);
    assert_eq!(query.utxo(&script, 200).unwrap().len(), 101);
    assert!(store.cache_db().iter_scan(b"A").next().is_some());
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let ready = Arc::new(std::sync::Barrier::new(2));
    let reader = {
        let query = Arc::clone(&query);
        let stop = Arc::clone(&stop);
        let ready = Arc::clone(&ready);
        std::thread::spawn(move || {
            ready.wait();
            while !stop.load(std::sync::atomic::Ordering::Acquire) {
                query.stats(&script).unwrap();
                query.utxo(&script, 200).unwrap();
            }
        })
    };
    ready.wait();
    let result = indexer.update(&daemon);
    stop.store(true, std::sync::atomic::Ordering::Release);
    reader.join().unwrap();
    result.unwrap();
    assert!(!indexer.stale_cleanup_failing());
    assert!(store
        .history_db()
        .iter_scan(&[b"H".as_slice(), &script].concat())
        .next()
        .is_none());
    assert_eq!(
        (
            query.stats(&script).unwrap().funded_txo_count,
            query.utxo(&script, 200).unwrap().len()
        ),
        (0, 0)
    );
}

// Persistent cleanup failure backs off instead of redoing the work every cycle, but a new
// daemon tip retries at once.
#[test]
fn failing_cleanup_backs_off_until_the_daemon_tip_changes() {
    let fixture = Fixture::new();
    let (store, mut indexer, daemon, _) = fixture.open(FetchFrom::Bitcoind);
    let active = fixture.chain.lock().unwrap().active.clone();
    assert_eq!(indexer.update(&daemon).unwrap(), active[3].block_hash());
    store.history_db().put_sync(b"R", &3u32.to_le_bytes());
    fixture.make_unrecoverable(&store, active[3].block_hash());

    fixture.chain.lock().unwrap().fetched.clear();
    indexer.update(&daemon).unwrap();
    assert!(indexer.stale_cleanup_failing());
    let first = fixture.chain.lock().unwrap().fetched.len();
    assert!(first > 0, "the first failure attempted cleanup");

    // Within the backoff on the same tip: no cleanup work, still reported as failing.
    for _ in 0..5 {
        indexer.update(&daemon).unwrap();
    }
    assert!(indexer.stale_cleanup_failing());
    assert_eq!(fixture.chain.lock().unwrap().fetched.len(), first);

    // A new daemon tip retries immediately and, with bitcoind recovered, succeeds.
    fixture.extend_chain();
    fixture.chain.lock().unwrap().unavailable.clear();
    let tip = fixture.chain.lock().unwrap().active.last().unwrap().block_hash();
    assert_eq!(indexer.update(&daemon).unwrap(), tip);
    assert!(!indexer.stale_cleanup_failing());
    assert!(store.history_db().get(b"R").is_none());
}
