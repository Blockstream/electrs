use std::net;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Barrier, Mutex, RwLock};
use std::thread;
use std::time::Duration;

use electrs::chain::Network;
use electrs::config::SensitiveAuth;
use electrs::metrics::Metrics;
use electrs::new_index::{ChainQuery, Mempool, Query, Store};
use serde_json::json;

pub mod common;

use common::Result;

const CONCURRENT_CALLERS: usize = 10;

fn fetch_metrics(addr: net::SocketAddr) -> String {
    ureq::get(&format!("http://{}/metrics", addr))
        .call()
        .expect("failed to scrape metrics")
        .into_body()
        .read_to_string()
        .expect("failed to read metrics body")
}

fn metric_value(body: &str, metric: &str, label_match: &str) -> u64 {
    let prefix = format!("{}{{{}}} ", metric, label_match);
    body.lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .and_then(|rest| rest.trim().parse::<f64>().ok())
        .map(|value| value as u64)
        .unwrap_or(0)
}

fn daemon_rpc_count(metrics_addr: net::SocketAddr, method: &str) -> u64 {
    metric_value(
        &fetch_metrics(metrics_addr),
        "daemon_rpc_count",
        &format!(r#"method="{}""#, method),
    )
}

#[test]
fn test_estimate_fee_refresh_is_single_flight() -> Result<()> {
    let rpc_count = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&rpc_count);
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let rpc = common::MockRpc::new(move |method, params| match method {
        "getblockchaininfo" => Ok(json!({
            "chain": "main", "blocks": 0, "headers": 0,
            "bestblockhash": "00".repeat(32), "pruned": false,
            "verificationprogress": 1.0, "initialblockdownload": false
        })),
        "estimatesmartfee" => {
            calls.fetch_add(1, Ordering::SeqCst);
            if params[0] == 1 {
                started_tx.send(()).unwrap();
                release_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(5))
                    .expect("fee refresh was not released");
            }
            Ok(json!({"feerate": 0.00002}))
        }
        _ => panic!("unexpected RPC: {}", method),
    });
    let dir = tempfile::tempdir()?;
    let mut config = common::test_config(dir.path().join("db"), dir.path().to_owned(), rpc.addr());
    config.cookie = Some(SensitiveAuth::new("test:test".to_owned()));
    config.db_write_buffer_size_mb = 8;
    #[cfg(not(feature = "liquid"))]
    {
        config.network_type = Network::Bitcoin;
    }
    #[cfg(feature = "liquid")]
    {
        config.network_type = Network::Liquid;
    }
    let config = Arc::new(config);
    let metrics = Metrics::new("127.0.0.1:0".parse().unwrap());
    let daemon = common::test_daemon(&config, &metrics);
    let store = Arc::new(Store::open(&config, &metrics, false));
    let chain = Arc::new(ChainQuery::new(
        store,
        Arc::clone(&daemon),
        &config,
        &metrics,
    ));
    let mempool = Arc::new(RwLock::new(Mempool::new(
        Arc::clone(&chain),
        &metrics,
        Arc::clone(&config),
    )));
    let query = Arc::new(Query::new(
        chain,
        mempool,
        daemon,
        config,
        #[cfg(feature = "liquid")]
        None,
    ));

    let before = rpc_count.load(Ordering::SeqCst);
    let refreshing = {
        let query = Arc::clone(&query);
        thread::spawn(move || query.estimate_fee_map())
    };
    started_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("fee refresh did not start");

    let (completed_tx, completed_rx) = mpsc::channel();
    let barrier = Arc::new(Barrier::new(CONCURRENT_CALLERS + 1));
    let handles: Vec<_> = (0..CONCURRENT_CALLERS)
        .map(|i| {
            let query = Arc::clone(&query);
            let barrier = Arc::clone(&barrier);
            let completed_tx = completed_tx.clone();
            thread::spawn(move || {
                barrier.wait();
                let estimate = if i % 2 == 0 {
                    let estimates = query.estimate_fee_map();
                    assert_eq!(estimates.len(), 28);
                    assert!(estimates.values().all(|fee| *fee == 2.0));
                    estimates.get(&6).copied()
                } else {
                    query.estimate_fee(6)
                };
                completed_tx.send(()).unwrap();
                estimate
            })
        })
        .collect();
    barrier.wait();
    let early_result = completed_rx.recv_timeout(Duration::from_millis(100));
    release_tx.send(()).unwrap();
    assert_eq!(early_result, Err(mpsc::RecvTimeoutError::Timeout));

    let estimates = refreshing.join().expect("initial fee refresh panicked");
    assert_eq!(estimates.len(), 28);
    assert!(estimates.values().all(|fee| *fee == 2.0));
    for handle in handles {
        assert_eq!(handle.join().expect("fee estimate panicked"), Some(2.0));
    }

    assert_eq!(query.estimate_fee_map(), estimates);
    assert_eq!(query.estimate_fee(6), Some(2.0));

    let after = rpc_count.load(Ordering::SeqCst);

    assert_eq!(
        after - before,
        28,
        "expected exactly one fee-estimate refresh (28 RPCs) from {} concurrent callers, got {}",
        CONCURRENT_CALLERS,
        after - before
    );
    rpc.assert_no_panics();
    Ok(())
}

#[test]
fn test_get_relayfee_refresh_is_single_flight() -> Result<()> {
    let tester = common::TestRunner::new()?;
    let query = tester.query();
    let metrics_addr = tester.metrics_addr();

    let before = daemon_rpc_count(metrics_addr, "getnetworkinfo");

    let barrier = Arc::new(Barrier::new(CONCURRENT_CALLERS));
    let handles: Vec<_> = (0..CONCURRENT_CALLERS)
        .map(|_| {
            let query = query.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                query.get_relayfee()
            })
        })
        .collect();
    for handle in handles {
        handle.join().expect("get_relayfee panicked")?;
    }

    let after = daemon_rpc_count(metrics_addr, "getnetworkinfo");

    assert_eq!(
        after - before,
        1,
        "expected exactly one getnetworkinfo call from {} concurrent get_relayfee() callers, got {}",
        CONCURRENT_CALLERS,
        after - before
    );
    Ok(())
}

// A real elementsd reorg that removes an asset issuance. Cleanup must remove the
// Liquid-only rows (the `i` asset row and `I` history) along with the confirmation.
#[cfg(feature = "liquid")]
#[test]
fn liquid_reorg_removes_asset_issuance() -> Result<()> {
    use elementsd::bitcoincore_rpc::RpcApi;
    use serde_json::{json, Value};
    use std::str::FromStr;

    let mut tester = common::TestRunner::new()?;
    let issuance = tester
        .node_client()
        .call::<Value>("issueasset", &[1.5.into(), 0.into(), false.into()])?;
    let asset_id = elements::AssetId::from_str(issuance["asset"].as_str().unwrap()).unwrap();
    let issuance_txid = elements::Txid::from_str(issuance["txid"].as_str().unwrap()).unwrap();
    let issued_in = tester.mine()?;
    assert!(tester.query().lookup_asset(&asset_id)?.is_some());
    let store = tester.query().chain().store();
    assert!(store
        .history_db()
        .get(&[b"i".as_slice(), &asset_id.into_inner()[..]].concat())
        .is_some());
    assert_eq!(
        tester
            .query()
            .chain()
            .tx_confirming_block(&issuance_txid)
            .map(|b| b.hash),
        Some(issued_in)
    );

    // Replace the issuance block with a longer branch that never confirms the issuance.
    tester
        .node_client()
        .call::<Value>("invalidateblock", &[json!(issued_in.to_string())])?;
    // The issuance returns to elementsd's mempool, so mine explicitly empty blocks.
    let address = tester.newaddress()?;
    for _ in 0..2 {
        tester
            .node_client()
            .call::<Value>("generateblock", &[json!(address.to_string()), json!([])])?;
    }
    tester.sync()?;

    // The issuance is back in the mempool, so only its confirmed rows must be gone.
    assert!(tester
        .query()
        .chain()
        .tx_confirming_block(&issuance_txid)
        .is_none());
    let store = tester.query().chain().store();
    assert!(store
        .history_db()
        .get(&[b"i".as_slice(), &asset_id.into_inner()[..]].concat())
        .is_none());
    assert!(store
        .history_db()
        .iter_scan(&[b"I".as_slice(), &asset_id.into_inner()[..]].concat())
        .next()
        .is_none());
    Ok(())
}
