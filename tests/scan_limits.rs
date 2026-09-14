use serde_json::Value;
use std::net;

use electrs::chain::Txid;

pub mod common;

use common::Result;

fn get_json(rest_addr: net::SocketAddr, path: &str) -> Result<Value> {
    Ok(ureq::get(&format!("http://{}{}", rest_addr, path))
        .call()?
        .into_body()
        .read_json()?)
}

// like get_json, but doesn't treat a non-2xx response as an error
fn get_allow_error(
    rest_addr: net::SocketAddr,
    path: &str,
) -> std::result::Result<ureq::http::Response<ureq::Body>, ureq::Error> {
    ureq::get(&format!("http://{}{}", rest_addr, path))
        .config()
        .http_status_as_error(false)
        .build()
        .call()
}

// utxos_limit must apply to the final UTXO set, not any point during the replay.
#[cfg(not(feature = "liquid"))]
#[test]
fn test_utxo_limit_checked_against_final_state() -> Result<()> {
    let (rest_handle, rest_addr, mut tester) = common::init_rest_tester().unwrap();

    let addr1 = tester.newaddress()?;
    let amount = bitcoin::Amount::from_sat(5_000);

    // 101 outputs in one tx -- one more than utxos_limit (100, tests/common.rs)
    let fund_txid = tester.send_multi(&addr1, amount, 101)?;
    tester.mine()?;

    let resp = get_allow_error(rest_addr, &format!("/address/{}/utxo", addr1))?;
    assert_eq!(resp.status(), 400);
    assert_eq!(
        resp.into_body().read_to_string()?,
        "Too many unspent outputs"
    );

    let stats = get_json(rest_addr, &format!("/address/{}", addr1))?;
    assert_eq!(stats["chain_stats"]["funded_txo_count"].as_u64(), Some(101));

    tester.consolidate(
        fund_txid,
        101,
        amount,
        bitcoin::Amount::from_sat(30_000),
        &addr1,
    )?;
    tester.mine()?;

    let res = get_json(rest_addr, &format!("/address/{}/utxo", addr1))?;
    let utxos = res.as_array().expect("array of utxos");
    assert_eq!(utxos.len(), 1);

    rest_handle.stop();
    Ok(())
}

// history_scan_limit must bound the scan, and resume across repeated requests.
#[test]
fn test_history_scan_limit_bounds_and_resumes() -> Result<()> {
    // above MIN_HISTORY_ITEMS_TO_CACHE (100), so the first capped scan actually gets cached
    let (rest_handle, rest_addr, mut tester) =
        common::init_rest_tester_with(|c| c.history_scan_limit = 110).unwrap();

    let addr1 = tester.newaddress()?;
    let amount = bitcoin::Amount::from_sat(1_000);

    // 130 confirmed sends, one output per block -- more than history_scan_limit
    for _ in 0..130 {
        tester.send(&addr1, amount)?;
        tester.mine()?;
    }

    // first request: only the first ~110 rows fit under the cap
    let resp = get_allow_error(rest_addr, &format!("/address/{}", addr1))?;
    assert_eq!(resp.status(), 400);
    assert_eq!(
        resp.into_body().read_to_string()?,
        "Scripthash history too large to scan"
    );

    // a handful of retries should be enough to resume from the checkpoint and catch up
    let mut stats = None;
    for _ in 0..5 {
        match get_json(rest_addr, &format!("/address/{}", addr1)) {
            Ok(res) => {
                stats = Some(res);
                break;
            }
            Err(_) => continue,
        }
    }
    let stats = stats.expect("scan should have converged within a few requests");
    assert_eq!(stats["chain_stats"]["funded_txo_count"].as_u64(), Some(130));

    rest_handle.stop();
    Ok(())
}

// an unmatched last_seen_txid cursor must return an empty page, not a full scan.
#[test]
fn test_history_cursor_unmatched_returns_empty() -> Result<()> {
    let (rest_handle, rest_addr, mut tester) = common::init_rest_tester().unwrap();

    // fund addr2 before addr1 has any UTXOs, so the wallet can't pick addr1 as an input
    let addr2 = tester.newaddress()?;
    let foreign_txid = tester.send(&addr2, bitcoin::Amount::from_sat(100_000))?;
    tester.mine()?;

    let addr1 = tester.newaddress()?;
    let mut addr1_txids: Vec<Txid> = vec![];
    for _ in 0..3 {
        addr1_txids.push(tester.send(&addr1, bitcoin::Amount::from_sat(100_000))?);
        tester.mine()?;
    }

    // sanity check: a real cursor works and excludes the cursor tx itself
    let cursor = addr1_txids[1];
    let res = get_json(
        rest_addr,
        &format!("/address/{}/txs/chain/{}", addr1, cursor),
    )?;
    let txs = res.as_array().expect("array of txs");
    assert!(txs
        .iter()
        .all(|tx| tx["txid"].as_str() != Some(cursor.to_string().as_str())));

    // a confirmed txid that belongs to a different scripthash's history
    let res = get_json(
        rest_addr,
        &format!("/address/{}/txs/chain/{}", addr1, foreign_txid),
    )?;
    assert_eq!(res.as_array().expect("array of txs").len(), 0);

    // a syntactically valid txid that was never confirmed at all
    let never_confirmed = "22".repeat(32);
    let res = get_json(
        rest_addr,
        &format!("/address/{}/txs/chain/{}", addr1, never_confirmed),
    )?;
    assert_eq!(res.as_array().expect("array of txs").len(), 0);

    rest_handle.stop();
    Ok(())
}
