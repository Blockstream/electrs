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

#[cfg(not(feature = "liquid"))]
#[test]
fn test_utxo_limit_checked_against_final_state() -> Result<()> {
    let (rest_handle, rest_addr, mut tester) = common::init_rest_tester().unwrap();

    let addr1 = tester.newaddress()?;
    let amount = bitcoin::Amount::from_sat(5_000);

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

#[cfg(not(feature = "liquid"))]
#[test]
fn test_utxo_limit_stays_until_consolidation_confirms() -> Result<()> {
    let (rest_handle, rest_addr, mut tester) = common::init_rest_tester().unwrap();

    let addr1 = tester.newaddress()?;
    let amount = bitcoin::Amount::from_sat(5_000);

    let fund_txid = tester.send_multi(&addr1, amount, 101)?;
    tester.mine()?;

    let resp = get_allow_error(rest_addr, &format!("/address/{}/utxo", addr1))?;
    assert_eq!(resp.status(), 400);

    tester.consolidate(
        fund_txid,
        101,
        amount,
        bitcoin::Amount::from_sat(30_000),
        &addr1,
    )?;

    let resp = get_allow_error(rest_addr, &format!("/address/{}/utxo", addr1))?;
    assert_eq!(resp.status(), 400);

    // Once mined, the limit is evaluated against the new confirmed state and clears.
    tester.mine()?;
    let res = get_json(rest_addr, &format!("/address/{}/utxo", addr1))?;
    let utxos = res.as_array().expect("array of utxos");
    assert_eq!(utxos.len(), 1);

    rest_handle.stop();
    Ok(())
}

#[cfg(not(feature = "liquid"))]
#[test]
fn test_history_scan_limit_finishes_oversized_height() -> Result<()> {
    let (rest_handle, rest_addr, mut tester) =
        common::init_rest_tester_with(|c| c.history_scan_limit = 5).unwrap();

    let addr1 = tester.newaddress()?;
    let amount = bitcoin::Amount::from_sat(5_000);

    tester.send_multi(&addr1, amount, 150)?;
    tester.mine()?;

    for _ in 0..3 {
        tester.send(&addr1, amount)?;
        tester.mine()?;
    }

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
    let stats = stats.expect("scan should get past the oversized height and converge");
    assert_eq!(stats["chain_stats"]["funded_txo_count"].as_u64(), Some(153));

    rest_handle.stop();
    Ok(())
}

#[test]
fn test_history_scan_limit_bounds_and_resumes() -> Result<()> {
    let (rest_handle, rest_addr, mut tester) =
        common::init_rest_tester_with(|c| c.history_scan_limit = 110).unwrap();

    let addr1 = tester.newaddress()?;
    let amount = bitcoin::Amount::from_sat(1_000);

    for _ in 0..130 {
        tester.send(&addr1, amount)?;
        tester.mine()?;
    }

    let resp = get_allow_error(rest_addr, &format!("/address/{}", addr1))?;
    assert_eq!(resp.status(), 400);
    assert_eq!(
        resp.into_body().read_to_string()?,
        "Scripthash history too large to scan"
    );

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

#[test]
fn test_history_scan_limit_checkpoints_below_cache_threshold() -> Result<()> {
    let (rest_handle, rest_addr, mut tester) =
        common::init_rest_tester_with(|c| c.history_scan_limit = 5).unwrap();

    let addr1 = tester.newaddress()?;
    let amount = bitcoin::Amount::from_sat(1_000);

    for _ in 0..6 {
        tester.send(&addr1, amount)?;
        tester.mine()?;
    }

    let resp = get_allow_error(rest_addr, &format!("/address/{}", addr1))?;
    assert_eq!(resp.status(), 400);

    let mut stats = None;
    for _ in 0..3 {
        match get_json(rest_addr, &format!("/address/{}", addr1)) {
            Ok(res) => {
                stats = Some(res);
                break;
            }
            Err(_) => continue,
        }
    }
    let stats =
        stats.expect("checkpoint below the cache threshold should still be saved and progress");
    assert_eq!(stats["chain_stats"]["funded_txo_count"].as_u64(), Some(6));

    rest_handle.stop();
    Ok(())
}

#[cfg(not(feature = "liquid"))]
#[test]
fn test_utxo_scan_limit_checkpoints_below_cache_threshold() -> Result<()> {
    let (rest_handle, rest_addr, mut tester) =
        common::init_rest_tester_with(|c| c.history_scan_limit = 5).unwrap();

    let addr1 = tester.newaddress()?;
    let amount = bitcoin::Amount::from_sat(1_000);

    for _ in 0..6 {
        tester.send_multi(&addr1, amount, 1)?;
        tester.mine()?;
    }

    let resp = get_allow_error(rest_addr, &format!("/address/{}/utxo", addr1))?;
    assert_eq!(resp.status(), 400);

    let mut utxos = None;
    for _ in 0..3 {
        match get_json(rest_addr, &format!("/address/{}/utxo", addr1)) {
            Ok(res) => {
                utxos = Some(res);
                break;
            }
            Err(_) => continue,
        }
    }
    let utxos =
        utxos.expect("checkpoint below the cache threshold should still be saved and progress");
    assert_eq!(utxos.as_array().expect("array of utxos").len(), 6);

    rest_handle.stop();
    Ok(())
}

#[cfg(not(feature = "liquid"))]
#[test]
fn test_history_scan_limit_errors_on_short_page() -> Result<()> {
    let (rest_handle, rest_addr, mut tester) =
        common::init_rest_tester_with(|c| c.history_scan_limit = 5).unwrap();

    let addr1 = tester.newaddress()?;
    let amount = bitcoin::Amount::from_sat(1_000);

    tester.send(&addr1, amount)?;
    tester.mine()?;

    tester.send_multi(&addr1, amount, 20)?;
    tester.mine()?;

    let resp = get_allow_error(rest_addr, &format!("/address/{}/txs/chain", addr1))?;
    assert_eq!(resp.status(), 400);
    assert_eq!(
        resp.into_body().read_to_string()?,
        "Scripthash history too large to scan"
    );

    rest_handle.stop();
    Ok(())
}

#[test]
fn test_history_cursor_unmatched_returns_empty() -> Result<()> {
    let (rest_handle, rest_addr, mut tester) = common::init_rest_tester().unwrap();

    let addr2 = tester.newaddress()?;
    let foreign_txid = tester.send(&addr2, bitcoin::Amount::from_sat(100_000))?;
    tester.mine()?;

    let addr1 = tester.newaddress()?;
    let mut addr1_txids: Vec<Txid> = vec![];
    for _ in 0..3 {
        addr1_txids.push(tester.send(&addr1, bitcoin::Amount::from_sat(100_000))?);
        tester.mine()?;
    }

    let cursor = addr1_txids[1];
    let res = get_json(
        rest_addr,
        &format!("/address/{}/txs/chain/{}", addr1, cursor),
    )?;
    let txs = res.as_array().expect("array of txs");
    assert!(txs
        .iter()
        .all(|tx| tx["txid"].as_str() != Some(cursor.to_string().as_str())));

    let res = get_json(
        rest_addr,
        &format!("/address/{}/txs/chain/{}", addr1, foreign_txid),
    )?;
    assert_eq!(res.as_array().expect("array of txs").len(), 0);

    let never_confirmed = "22".repeat(32);
    let res = get_json(
        rest_addr,
        &format!("/address/{}/txs/chain/{}", addr1, never_confirmed),
    )?;
    assert_eq!(res.as_array().expect("array of txs").len(), 0);

    rest_handle.stop();
    Ok(())
}
