use std::net;
use std::sync::{Arc, Barrier};
use std::thread;

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
    let tester = common::TestRunner::new()?;
    let query = tester.query();
    let metrics_addr = tester.metrics_addr();

    let before = daemon_rpc_count(metrics_addr, "estimatesmartfee");

    let barrier = Arc::new(Barrier::new(CONCURRENT_CALLERS));
    let handles: Vec<_> = (0..CONCURRENT_CALLERS)
        .map(|_| {
            let query = query.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                query.estimate_fee_map()
            })
        })
        .collect();
    for handle in handles {
        handle.join().expect("estimate_fee_map panicked");
    }

    let after = daemon_rpc_count(metrics_addr, "estimatesmartfee");

    assert_eq!(
        after - before,
        28,
        "expected exactly one fee-estimate refresh (28 RPCs) from {} concurrent callers, got {}",
        CONCURRENT_CALLERS,
        after - before
    );
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
