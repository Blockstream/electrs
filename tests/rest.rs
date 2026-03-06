use bitcoin::hashes::{sha256, Hash};
use bitcoin::hex::FromHex;
use serde_json::Value;
use std::collections::HashSet;
use std::net;

#[cfg(feature = "liquid")]
use elementsd::bitcoincore_rpc::RpcApi;
#[cfg(not(feature = "liquid"))]
use {bitcoin::Amount, serde_json::from_value};

use electrs::chain::Txid;

pub mod common;

use common::Result;

fn get(
    rest_addr: net::SocketAddr,
    path: &str,
) -> std::result::Result<ureq::http::Response<ureq::Body>, ureq::Error> {
    ureq::get(&format!("http://{}{}", rest_addr, path)).call()
}

fn get_json(rest_addr: net::SocketAddr, path: &str) -> Result<Value> {
    Ok(get(rest_addr, path)?.into_body().read_json()?)
}

fn get_plain(rest_addr: net::SocketAddr, path: &str) -> Result<String> {
    Ok(get(rest_addr, path)?.into_body().read_to_string()?)
}

#[test]
fn test_rest_tx() -> Result<()> {
    let (rest_handle, rest_addr, mut tester) = common::init_rest_tester().unwrap();

    // Send transaction and confirm it
    let addr1 = tester.newaddress()?;
    let txid1_confirmed = tester.send(&addr1, "1.19123 BTC".parse().unwrap())?;
    tester.mine()?;
    let mine_height = tester.get_block_count()?;

    // Send transaction and leave it unconfirmed
    let txid2_mempool = tester.send(&addr1, "0.7113 BTC".parse().unwrap())?;

    // Test GET /tx/:txid
    let res = get_json(rest_addr, &format!("/tx/{}", txid1_confirmed))?;
    log::debug!("tx: {:#?}", res);

    // Verify TransactionValue fields with actual values
    assert_eq!(
        res["txid"].as_str(),
        Some(txid1_confirmed.to_string().as_str())
    );
    assert_eq!(res["version"].as_u64(), Some(2));
    assert!(res["locktime"].as_u64().is_some());
    assert!(res["size"].as_u64().unwrap() > 0);
    assert!(res["weight"].as_u64().unwrap() > 0);
    assert!(res["fee"].as_u64().unwrap() > 0);
    #[cfg(feature = "liquid")]
    {
        assert_eq!(res["discount_vsize"].as_u64().unwrap(), 228);
        assert_eq!(res["discount_weight"].as_u64().unwrap(), 912);
    }

    // Verify status on the TransactionValue itself
    assert_eq!(res["status"]["confirmed"].as_bool(), Some(true));
    assert_eq!(res["status"]["block_height"].as_u64(), Some(mine_height));
    assert!(res["status"]["block_hash"].is_string());
    assert!(res["status"]["block_time"].as_u64().unwrap() > 0);

    // Verify vout fields and find our target output
    let outs = res["vout"].as_array().expect("array of outs");
    assert!(outs.iter().any(|vout| {
        vout["scriptpubkey_address"].as_str() == Some(&addr1.to_string())
            && vout["value"].as_u64() == Some(119123000)
    }));
    for vout in outs {
        assert!(vout["scriptpubkey"].is_string());
        assert!(vout["scriptpubkey_asm"].is_string());
        assert!(vout["scriptpubkey_type"].is_string());
    }
    // Verify our target output's scriptpubkey_type (Bitcoin uses segwit address types)
    #[cfg(not(feature = "liquid"))]
    {
        let target_vout = outs
            .iter()
            .find(|v| v["scriptpubkey_address"].as_str() == Some(&addr1.to_string()))
            .unwrap();
        let spk_type = target_vout["scriptpubkey_type"].as_str().unwrap();
        assert!(
            spk_type == "v0_p2wpkh" || spk_type == "v1_p2tr",
            "unexpected scriptpubkey_type: {}",
            spk_type
        );
    }

    // Verify vin fields (non-coinbase input)
    let vin0 = &res["vin"][0];
    assert!(vin0["txid"].is_string());
    assert!(vin0["vout"].is_u64());
    assert_eq!(vin0["is_coinbase"].as_bool(), Some(false));
    assert!(vin0["sequence"].as_u64().is_some());
    assert!(vin0["scriptsig"].is_string());
    assert!(vin0["scriptsig_asm"].is_string());
    // prevout should be present for non-coinbase inputs
    assert!(vin0["prevout"].is_object());
    assert!(vin0["prevout"]["scriptpubkey"].is_string());
    assert!(vin0["prevout"]["scriptpubkey_type"].is_string());
    #[cfg(not(feature = "liquid"))]
    assert!(vin0["prevout"]["value"].as_u64().unwrap() > 0);

    // Verify coinbase tx input
    let block_hash = res["status"]["block_hash"].as_str().unwrap();
    let block_txs = get_json(rest_addr, &format!("/block/{}/txs", block_hash))?;
    let coinbase_tx = &block_txs.as_array().unwrap()[0];
    let cb_vin = &coinbase_tx["vin"][0];
    assert_eq!(cb_vin["is_coinbase"].as_bool(), Some(true));
    assert!(cb_vin["scriptsig"].is_string());
    assert!(cb_vin["scriptsig_asm"].is_string());
    assert!(cb_vin["prevout"].is_null());

    // Test GET /tx/:txid/status (confirmed)
    let res = get_json(rest_addr, &format!("/tx/{}/status", txid1_confirmed))?;
    assert_eq!(res["confirmed"].as_bool(), Some(true));
    assert_eq!(res["block_height"].as_u64(), Some(mine_height));
    assert!(res["block_hash"].is_string());
    assert!(res["block_time"].as_u64().unwrap() > 0);

    // Test GET /tx/:txid/status (unconfirmed)
    let res = get_json(rest_addr, &format!("/tx/{}/status", txid2_mempool))?;
    assert_eq!(res["confirmed"].as_bool(), Some(false));
    assert_eq!(res["block_height"].as_u64(), None);
    assert!(res["block_hash"].is_null());
    assert!(res["block_time"].is_null());

    rest_handle.stop();
    Ok(())
}

#[test]
fn test_rest_address() -> Result<()> {
    let (rest_handle, rest_addr, mut tester) = common::init_rest_tester().unwrap();

    let addr1 = tester.newaddress()?;
    let txid1_confirmed = tester.send(&addr1, "1.19123 BTC".parse().unwrap())?;
    tester.mine()?;

    let txid2_mempool = tester.send(&addr1, "0.7113 BTC".parse().unwrap())?;

    // Test GET /address/:address
    let res = get_json(rest_addr, &format!("/address/{}", addr1))?;
    assert_eq!(res["address"].as_str(), Some(addr1.to_string().as_str()));

    // chain_stats: 1 confirmed funding tx, nothing spent
    assert_eq!(res["chain_stats"]["tx_count"].as_u64(), Some(1));
    assert_eq!(res["chain_stats"]["funded_txo_count"].as_u64(), Some(1));
    assert_eq!(res["chain_stats"]["spent_txo_count"].as_u64(), Some(0));
    #[cfg(not(feature = "liquid"))]
    {
        assert_eq!(
            res["chain_stats"]["funded_txo_sum"].as_u64(),
            Some(119123000)
        );
        assert_eq!(res["chain_stats"]["spent_txo_sum"].as_u64(), Some(0));
    }

    // mempool_stats: 1 unconfirmed funding tx; the wallet may also spend
    // addr1's confirmed UTXO as an input, so spent_txo_count can be 0 or 1
    assert!(res["mempool_stats"]["tx_count"].as_u64().unwrap() >= 1);
    assert_eq!(res["mempool_stats"]["funded_txo_count"].as_u64(), Some(1));
    assert!(res["mempool_stats"]["spent_txo_count"].is_u64());
    #[cfg(not(feature = "liquid"))]
    {
        assert_eq!(
            res["mempool_stats"]["funded_txo_sum"].as_u64(),
            Some(71130000)
        );
        assert!(res["mempool_stats"]["spent_txo_sum"].is_u64());
    }

    // Test GET /address/:address/txs
    let res = get_json(rest_addr, &format!("/address/{}/txs", addr1))?;
    let txs = res.as_array().expect("array of transactions");
    let mut txids = txs
        .iter()
        .map(|tx| tx["txid"].as_str().unwrap().parse().unwrap())
        .collect::<HashSet<Txid>>();
    assert!(txids.remove(&txid1_confirmed));
    assert!(txids.remove(&txid2_mempool));
    assert!(txids.is_empty());

    // Test GET /address-prefix/:prefix
    let addr1_prefix = &addr1.to_string()[0..8];
    let res = get_json(rest_addr, &format!("/address-prefix/{}", addr1_prefix))?;
    let found = res.as_array().expect("array of matching addresses");
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].as_str(), Some(addr1.to_string().as_str()));

    rest_handle.stop();
    Ok(())
}

#[test]
fn test_rest_blocks() -> Result<()> {
    let (rest_handle, rest_addr, mut tester) = common::init_rest_tester().unwrap();

    // Test GET /blocks/tip/hash
    let bestblockhash = tester.get_best_block_hash()?;
    let res = get_plain(rest_addr, "/blocks/tip/hash")?;
    assert_eq!(res, bestblockhash.to_string());

    let bestblockhash = tester.mine()?;
    let res = get_plain(rest_addr, "/blocks/tip/hash")?;
    assert_eq!(res, bestblockhash.to_string());

    // Test GET /blocks/tip/height
    let bestblockheight = tester.get_block_count()?;
    let res = get_plain(rest_addr, "/blocks/tip/height")?;
    assert_eq!(
        res.parse::<u64>().expect("tip block height as an int"),
        bestblockheight
    );

    // Test GET /block-height/:height
    let res = get_plain(rest_addr, &format!("/block-height/{}", bestblockheight))?;
    assert_eq!(res, bestblockhash.to_string());

    // Test GET /blocks
    let res = get_json(rest_addr, "/blocks")?;
    let last_blocks = res.as_array().unwrap();
    assert_eq!(last_blocks.len(), 10); // limited to 10 per page
    assert_eq!(
        last_blocks[0]["id"].as_str(),
        Some(bestblockhash.to_string().as_str())
    );

    // Verify first block (tip) has correct height
    assert_eq!(
        last_blocks[0]["height"].as_u64(),
        Some(bestblockheight)
    );

    // Verify block list entries have all BlockValue fields with value checks
    for block in last_blocks {
        assert!(block["id"].is_string());
        assert!(block["height"].is_u64());
        assert!(block["version"].is_u64());
        assert!(block["timestamp"].as_u64().unwrap() > 0);
        assert!(block["tx_count"].as_u64().unwrap() >= 1); // coinbase at minimum
        assert!(block["size"].as_u64().unwrap() > 0);
        assert!(block["weight"].as_u64().unwrap() > 0);
        assert!(block["merkle_root"].is_string());
        assert!(block["mediantime"].as_u64().unwrap() > 0);
        #[cfg(not(feature = "liquid"))]
        {
            assert!(block["nonce"].is_u64());
            assert!(block["bits"].is_u64());
            assert!(block["difficulty"].is_f64());
        }
    }

    // Verify previousblockhash links blocks together correctly
    for i in 0..last_blocks.len() - 1 {
        assert_eq!(
            last_blocks[i]["previousblockhash"].as_str(),
            last_blocks[i + 1]["id"].as_str()
        );
    }

    let bestblockhash = tester.mine()?;
    let res = get_json(rest_addr, "/blocks")?;
    let last_blocks = res.as_array().unwrap();
    assert_eq!(
        last_blocks[0]["id"].as_str(),
        Some(bestblockhash.to_string().as_str())
    );

    rest_handle.stop();
    Ok(())
}

#[test]
fn test_rest_block() -> Result<()> {
    let (rest_handle, rest_addr, mut tester) = common::init_rest_tester().unwrap();

    let addr1 = tester.newaddress()?;

    // Test GET /block/:hash
    let txid = tester.send(&addr1, "0.98765432 BTC".parse().unwrap())?;
    let blockhash = tester.mine()?;

    let res = get_json(rest_addr, &format!("/block/{}", blockhash))?;
    assert_eq!(res["id"].as_str(), Some(blockhash.to_string().as_str()));
    assert_eq!(res["height"].as_u64(), Some(tester.get_block_count()?));
    assert_eq!(res["tx_count"].as_u64(), Some(2));

    // Cross-reference BlockValue fields against bitcoind's getblockheader
    let node_header: Value = tester
        .node_client()
        .call("getblockheader", &[blockhash.to_string().into()])?;
    assert_eq!(res["version"].as_u64(), node_header["version"].as_u64());
    assert_eq!(res["timestamp"].as_u64(), node_header["time"].as_u64());
    assert_eq!(
        res["merkle_root"].as_str(),
        node_header["merkleroot"].as_str()
    );
    assert_eq!(
        res["previousblockhash"].as_str(),
        node_header["previousblockhash"].as_str()
    );
    assert_eq!(res["mediantime"].as_u64(), node_header["mediantime"].as_u64());
    assert!(res["size"].as_u64().unwrap() > 0);
    assert!(res["weight"].as_u64().unwrap() > 0);
    #[cfg(not(feature = "liquid"))]
    {
        assert_eq!(res["nonce"].as_u64(), node_header["nonce"].as_u64());
        // bits is serialized differently (compact target int vs hex string), just check presence
        assert!(res["bits"].is_u64());
        assert!(res["difficulty"].is_f64());
    }

    // Test GET /block/:hash/raw
    let rest_rawblock = get(rest_addr, &format!("/block/{}/raw", blockhash))?
        .into_body()
        .read_to_vec()?;
    let node_hexblock = // uses low-level call() to support Elements
        tester.node_client().call::<String>("getblock", &[blockhash.to_string().into(), 0.into()])?;
    assert_eq!(rest_rawblock, Vec::from_hex(&node_hexblock).unwrap());

    // Test GET /block/:hash/txid/:index
    let res = get_plain(rest_addr, &format!("/block/{}/txid/1", blockhash))?;
    assert_eq!(res, txid.to_string());

    rest_handle.stop();
    Ok(())
}

#[test]
fn test_rest_block_txs() -> Result<()> {
    let (rest_handle, rest_addr, mut tester) = common::init_rest_tester().unwrap();

    let addr1 = tester.newaddress()?;
    let txid = tester.send(&addr1, "0.98765432 BTC".parse().unwrap())?;
    let blockhash = tester.mine()?;

    // Test GET /block/:hash/txs
    let res = get_json(rest_addr, &format!("/block/{}/txs", blockhash))?;
    let block_txs = res.as_array().expect("list of txs");
    assert_eq!(block_txs.len(), 2);
    assert_eq!(block_txs[0]["vin"][0]["is_coinbase"].as_bool(), Some(true));
    assert_eq!(
        block_txs[1]["txid"].as_str(),
        Some(txid.to_string().as_str())
    );

    // Test GET /block/:hash/txs/:index
    let res = get_json(rest_addr, &format!("/block/{}/txs/0", blockhash))?;
    let block_txs = res.as_array().expect("list of txs");
    assert_eq!(block_txs.len(), 2);
    assert_eq!(block_txs[0]["vin"][0]["is_coinbase"].as_bool(), Some(true));
    assert_eq!(
        block_txs[1]["txid"].as_str(),
        Some(txid.to_string().as_str())
    );

    // Test GET /block/:hash/txs/:index
    // Should fail with 404 code when block isn't found
    let invalid_resp = ureq::get(&format!("http://{}/block/{}/txs/0", rest_addr, "0000000000000000000000000000000000000000000000000000000000000000"))
        .config()
        .http_status_as_error(false)
        .build()
        .call()?;
    assert_eq!(invalid_resp.status(), 404);
    assert_eq!(invalid_resp.into_body().read_to_string()?, "Block not found");

    // Test GET /block/:hash/txs/:index
    // Should fail with 400 code when block hash is invalid
    let invalid_resp = ureq::get(&format!("http://{}/block/{}/txs/0", rest_addr, "invalid_hash"))
        .config()
        .http_status_as_error(false)
        .build()
        .call()?;
    assert_eq!(invalid_resp.status(), 400);
    assert_eq!(invalid_resp.into_body().read_to_string()?, "Invalid hex string");

    // Test GET /block/:hash/txs/:index
    // Should fail with 400 code when `(index % 25) != 0`
    let invalid_hash_resp = ureq::get(&format!("http://{}/block/{}/txs/1", rest_addr, blockhash))
        .config()
        .http_status_as_error(false)
        .build()
        .call()?;
    assert_eq!(invalid_hash_resp.status(), 400);
    assert_eq!(invalid_hash_resp.into_body().read_to_string()?, "start index must be a multiple of 25");

    // Test GET /block/:hash/txs/:index
    // Should fail with 400 code when index is out of range
    let invalid_hash_resp = ureq::get(&format!("http://{}/block/{}/txs/25", rest_addr, blockhash))
        .config()
        .http_status_as_error(false)
        .build()
        .call()?;
    assert_eq!(invalid_hash_resp.status(), 400);
    assert_eq!(invalid_hash_resp.into_body().read_to_string()?, "start index out of range");

    rest_handle.stop();
    Ok(())
}

#[test]
fn test_rest_mempool() -> Result<()> {
    let (rest_handle, rest_addr, mut tester) = common::init_rest_tester().unwrap();

    let addr1 = tester.newaddress()?;

    // Test GET /mempool/txids
    let txid = tester.send(&addr1, "3.21 BTC".parse().unwrap())?;
    let res = get_json(rest_addr, "/mempool/txids")?;
    let mempool_txids = res.as_array().expect("list of txids");
    assert_eq!(mempool_txids.len(), 1);
    assert_eq!(mempool_txids[0].as_str(), Some(txid.to_string().as_str()));

    tester.send(&addr1, "0.0001 BTC".parse().unwrap())?;
    let res = get_json(rest_addr, "/mempool/txids")?;
    let mempool_txids = res.as_array().expect("list of txids");
    assert_eq!(mempool_txids.len(), 2);

    // Test GET /mempool
    let mempool_stats = get_json(rest_addr, "/mempool")?;
    assert_eq!(mempool_stats["count"].as_u64(), Some(2));
    assert!(mempool_stats["vsize"].as_u64().unwrap() > 0);
    assert!(mempool_stats["total_fee"].as_u64().unwrap() > 0);
    assert!(mempool_stats["fee_histogram"].is_array());

    tester.send(&addr1, "0.00022 BTC".parse().unwrap())?;
    assert_eq!(get_json(rest_addr, "/mempool")?["count"].as_u64(), Some(3));

    tester.mine()?;
    let mempool_after = get_json(rest_addr, "/mempool")?;
    assert_eq!(mempool_after["count"].as_u64(), Some(0));
    assert_eq!(mempool_after["vsize"].as_u64(), Some(0));
    assert_eq!(mempool_after["total_fee"].as_u64(), Some(0));
    assert_eq!(
        mempool_after["fee_histogram"].as_array().unwrap().len(),
        0
    );

    rest_handle.stop();
    Ok(())
}

#[test]
fn test_rest_broadcast_tx() -> Result<()> {
    let (rest_handle, rest_addr, mut tester) = common::init_rest_tester().unwrap();

    let addr1 = tester.newaddress()?;

    // Test POST /tx
    let txid = tester.send(&addr1, "9.9 BTC".parse().unwrap())?;
    let tx_hex = get_plain(rest_addr, &format!("/tx/{}/hex", txid))?;
    // Re-send the tx created by send(). It'll be accepted again since its still in the mempool.
    let broadcast1_resp = ureq::post(&format!("http://{}/tx", rest_addr)).send(&tx_hex)?;
    assert_eq!(broadcast1_resp.status(), 200);
    assert_eq!(
        broadcast1_resp.into_body().read_to_string()?,
        txid.to_string()
    );
    // Mine the tx then submit it again. Should now fail.
    tester.mine()?;
    let broadcast2_resp = ureq::post(&format!("http://{}/tx", rest_addr))
        .config()
        .http_status_as_error(false)
        .build()
        .send(&tx_hex)?;
    assert_eq!(broadcast2_resp.status(), 400);

    rest_handle.stop();
    Ok(())
}

#[test]
fn test_rest_package_validation() -> Result<()> {
    let (rest_handle, rest_addr, _tester) = common::init_rest_tester().unwrap();

    // Test POST /txs/package - simple validation test
    // Test with invalid JSON first to verify the endpoint exists
    let invalid_package_resp = ureq::post(&format!("http://{}/txs/package", rest_addr))
        .header("Content-Type", "application/json")
        .config()
        .http_status_as_error(false)
        .build()
        .send("invalid json")?;
    // Should be 400 for bad JSON, not 404 for missing endpoint
    assert_eq!(
        invalid_package_resp.status(),
        400,
        "Endpoint should exist and return 400 for invalid JSON"
    );

    // Now test with valid but empty package, should fail
    let empty_package_resp = ureq::post(&format!("http://{}/txs/package", rest_addr))
        .header("Content-Type", "application/json")
        .config()
        .http_status_as_error(false)
        .build()
        .send("[]")?;
    assert_eq!(empty_package_resp.status(), 400);

    rest_handle.stop();
    Ok(())
}

#[test]
fn test_rest_block_status() -> Result<()> {
    let (rest_handle, rest_addr, mut tester) = common::init_rest_tester().unwrap();

    let addr1 = tester.newaddress()?;
    tester.send(&addr1, "0.5 BTC".parse().unwrap())?;
    let blockhash1 = tester.mine()?;
    let blockhash2 = tester.mine()?; // tip

    let block_count = tester.get_block_count()?;

    // Non-tip block should have next_best pointing to next block
    let res = get_json(rest_addr, &format!("/block/{}/status", blockhash1))?;
    assert_eq!(res["in_best_chain"].as_bool(), Some(true));
    assert_eq!(res["height"].as_u64(), Some(block_count - 1));
    assert_eq!(
        res["next_best"].as_str(),
        Some(blockhash2.to_string().as_str())
    );

    // Tip block should have next_best as null
    let res = get_json(rest_addr, &format!("/block/{}/status", blockhash2))?;
    assert_eq!(res["in_best_chain"].as_bool(), Some(true));
    assert_eq!(res["height"].as_u64(), Some(block_count));
    assert!(res["next_best"].is_null());

    rest_handle.stop();
    Ok(())
}

#[test]
fn test_rest_block_txids() -> Result<()> {
    let (rest_handle, rest_addr, mut tester) = common::init_rest_tester().unwrap();

    let addr1 = tester.newaddress()?;
    let txid = tester.send(&addr1, "0.5 BTC".parse().unwrap())?;
    let blockhash = tester.mine()?;

    let res = get_json(rest_addr, &format!("/block/{}/txids", blockhash))?;
    let txids = res.as_array().expect("array of txids");

    // Should match tx_count from /block/:hash
    let block = get_json(rest_addr, &format!("/block/{}", blockhash))?;
    assert_eq!(txids.len(), block["tx_count"].as_u64().unwrap() as usize);

    // First txid should be the coinbase (not our user txid)
    assert_ne!(
        txids[0].as_str(),
        Some(txid.to_string().as_str()),
        "first txid should be coinbase, not user tx"
    );
    // Our txid should be present
    assert!(txids
        .iter()
        .any(|t| t.as_str() == Some(&txid.to_string())));

    rest_handle.stop();
    Ok(())
}

#[test]
fn test_rest_block_header() -> Result<()> {
    let (rest_handle, rest_addr, mut tester) = common::init_rest_tester().unwrap();

    let blockhash = tester.mine()?;
    let header_hex = get_plain(rest_addr, &format!("/block/{}/header", blockhash))?;

    // Verify it's valid hex
    let header_bytes = Vec::from_hex(&header_hex).expect("valid hex");
    assert!(!header_bytes.is_empty());

    // On Bitcoin, verify the header is 80 bytes and its hash matches the block hash
    #[cfg(not(feature = "liquid"))]
    {
        assert_eq!(header_bytes.len(), 80);
        let header: bitcoin::block::Header =
            bitcoin::consensus::deserialize(&header_bytes).expect("valid header");
        assert_eq!(header.block_hash().to_string(), blockhash.to_string());
    }

    rest_handle.stop();
    Ok(())
}

#[test]
fn test_rest_address_mempool_txs() -> Result<()> {
    let (rest_handle, rest_addr, mut tester) = common::init_rest_tester().unwrap();

    let addr1 = tester.newaddress()?;

    // Send tx to address but don't mine
    let txid = tester.send(&addr1, "0.5 BTC".parse().unwrap())?;

    // Verify it appears in mempool txs
    let res = get_json(rest_addr, &format!("/address/{}/txs/mempool", addr1))?;
    let txs = res.as_array().expect("array of txs");
    assert_eq!(txs.len(), 1);
    assert_eq!(txs[0]["txid"].as_str(), Some(txid.to_string().as_str()));
    assert_eq!(txs[0]["status"]["confirmed"].as_bool(), Some(false));
    assert!(txs[0]["fee"].as_u64().unwrap() > 0);

    // Mine and verify mempool list is now empty
    tester.mine()?;
    let res = get_json(rest_addr, &format!("/address/{}/txs/mempool", addr1))?;
    let txs = res.as_array().expect("array of txs");
    assert!(txs.is_empty());

    rest_handle.stop();
    Ok(())
}

#[test]
fn test_rest_address_utxo() -> Result<()> {
    let (rest_handle, rest_addr, mut tester) = common::init_rest_tester().unwrap();

    let addr1 = tester.newaddress()?;

    // Send to address and mine - verify confirmed UTXO
    let sent_txid = tester.send(&addr1, "0.5 BTC".parse().unwrap())?;
    tester.mine()?;
    let mine_height = tester.get_block_count()?;

    let res = get_json(rest_addr, &format!("/address/{}/utxo", addr1))?;
    let utxos = res.as_array().expect("array of utxos");
    assert_eq!(utxos.len(), 1);
    assert_eq!(
        utxos[0]["txid"].as_str(),
        Some(sent_txid.to_string().as_str())
    );
    assert!(utxos[0]["vout"].is_u64());
    assert_eq!(utxos[0]["status"]["confirmed"].as_bool(), Some(true));
    assert_eq!(utxos[0]["status"]["block_height"].as_u64(), Some(mine_height));
    assert!(utxos[0]["status"]["block_hash"].is_string());
    assert!(utxos[0]["status"]["block_time"].as_u64().unwrap() > 0);
    #[cfg(not(feature = "liquid"))]
    assert_eq!(utxos[0]["value"].as_u64(), Some(50000000));

    // Send again without mining - the wallet may spend the existing UTXO as input,
    // so we just verify that UTXOs exist and have correct fields
    tester.send(&addr1, "0.3 BTC".parse().unwrap())?;
    let res = get_json(rest_addr, &format!("/address/{}/utxo", addr1))?;
    let utxos = res.as_array().expect("array of utxos");
    assert!(!utxos.is_empty());
    for utxo in utxos {
        assert!(utxo["txid"].is_string());
        assert!(utxo["vout"].is_u64());
        assert!(utxo["status"].is_object());
        assert!(utxo["status"]["confirmed"].is_boolean());
    }

    rest_handle.stop();
    Ok(())
}

#[test]
fn test_rest_scripthash() -> Result<()> {
    let (rest_handle, rest_addr, mut tester) = common::init_rest_tester().unwrap();

    let addr1 = tester.newaddress()?;
    tester.send(&addr1, "0.5 BTC".parse().unwrap())?;
    tester.mine()?;
    tester.send(&addr1, "0.3 BTC".parse().unwrap())?; // mempool tx

    // Get the scriptpubkey from a tx to addr1
    let addr_txs = get_json(rest_addr, &format!("/address/{}/txs", addr1))?;
    let txs = addr_txs.as_array().unwrap();
    let vout = txs[0]["vout"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["scriptpubkey_address"].as_str() == Some(&addr1.to_string()))
        .expect("vout to our address");
    let scriptpubkey_hex = vout["scriptpubkey"].as_str().unwrap();
    let scriptpubkey_bytes = Vec::from_hex(scriptpubkey_hex).unwrap();

    // Compute scripthash (SHA256 of scriptpubkey bytes)
    let scripthash = sha256::Hash::hash(&scriptpubkey_bytes).to_string();

    // Verify /scripthash/:hash matches /address/:address
    // (the top-level objects differ by "address" vs "scripthash" key, so compare stats)
    let addr_stats = get_json(rest_addr, &format!("/address/{}", addr1))?;
    let sh_stats = get_json(rest_addr, &format!("/scripthash/{}", scripthash))?;
    assert_eq!(addr_stats["chain_stats"], sh_stats["chain_stats"]);
    assert_eq!(addr_stats["mempool_stats"], sh_stats["mempool_stats"]);

    // Verify /scripthash/:hash/txs matches /address/:address/txs
    let addr_txs = get_json(rest_addr, &format!("/address/{}/txs", addr1))?;
    let sh_txs = get_json(rest_addr, &format!("/scripthash/{}/txs", scripthash))?;
    assert_eq!(addr_txs, sh_txs);

    // Verify /scripthash/:hash/txs/chain matches /address/:address/txs/chain
    let addr_chain = get_json(rest_addr, &format!("/address/{}/txs/chain", addr1))?;
    let sh_chain = get_json(rest_addr, &format!("/scripthash/{}/txs/chain", scripthash))?;
    assert_eq!(addr_chain, sh_chain);

    // Verify /scripthash/:hash/txs/mempool matches /address/:address/txs/mempool
    let addr_mempool = get_json(rest_addr, &format!("/address/{}/txs/mempool", addr1))?;
    let sh_mempool = get_json(rest_addr, &format!("/scripthash/{}/txs/mempool", scripthash))?;
    assert_eq!(addr_mempool, sh_mempool);

    // Verify /scripthash/:hash/utxo matches /address/:address/utxo
    let addr_utxo = get_json(rest_addr, &format!("/address/{}/utxo", addr1))?;
    let sh_utxo = get_json(rest_addr, &format!("/scripthash/{}/utxo", scripthash))?;
    assert_eq!(addr_utxo, sh_utxo);

    rest_handle.stop();
    Ok(())
}

#[test]
fn test_rest_tx_outspends() -> Result<()> {
    let (rest_handle, rest_addr, mut tester) = common::init_rest_tester().unwrap();

    let addr1 = tester.newaddress()?;
    let txid = tester.send(&addr1, "0.5 BTC".parse().unwrap())?;
    tester.mine()?;
    let mine_height = tester.get_block_count()?;

    // Check outspends of a freshly mined tx - outputs should be unspent
    let res = get_json(rest_addr, &format!("/tx/{}/outspends", txid))?;
    let outspends = res.as_array().expect("array of outspends");
    assert!(!outspends.is_empty());
    for outspend in outspends {
        assert_eq!(outspend["spent"].as_bool(), Some(false));
        assert!(outspend["txid"].is_null());
        assert!(outspend["vin"].is_null());
        assert!(outspend["status"].is_null());
    }

    // The send tx spent some input. Check that the parent tx shows a spent output.
    let tx_detail = get_json(rest_addr, &format!("/tx/{}", txid))?;
    let spent_txid = tx_detail["vin"][0]["txid"].as_str().unwrap();
    let spent_vout = tx_detail["vin"][0]["vout"].as_u64().unwrap();
    let spent_vin = 0u64; // our tx is the spender, using vin index 0

    let res = get_json(rest_addr, &format!("/tx/{}/outspends", spent_txid))?;
    let outspends = res.as_array().expect("array of outspends");
    let spent_entry = &outspends[spent_vout as usize];
    assert_eq!(spent_entry["spent"].as_bool(), Some(true));
    assert_eq!(
        spent_entry["txid"].as_str(),
        Some(txid.to_string().as_str())
    );
    assert_eq!(spent_entry["vin"].as_u64(), Some(spent_vin));
    assert_eq!(spent_entry["status"]["confirmed"].as_bool(), Some(true));
    assert_eq!(spent_entry["status"]["block_height"].as_u64(), Some(mine_height));
    assert!(spent_entry["status"]["block_hash"].is_string());
    assert!(spent_entry["status"]["block_time"].as_u64().unwrap() > 0);

    rest_handle.stop();
    Ok(())
}

#[test]
fn test_rest_tx_merkle_proof() -> Result<()> {
    let (rest_handle, rest_addr, mut tester) = common::init_rest_tester().unwrap();

    let addr1 = tester.newaddress()?;
    let txid = tester.send(&addr1, "0.5 BTC".parse().unwrap())?;
    tester.mine()?;
    let mine_height = tester.get_block_count()?;

    let res = get_json(rest_addr, &format!("/tx/{}/merkle-proof", txid))?;
    assert_eq!(res["block_height"].as_u64(), Some(mine_height));
    let merkle = res["merkle"].as_array().expect("merkle array");
    assert!(!merkle.is_empty());
    for entry in merkle {
        let hex = entry.as_str().expect("merkle entry is string");
        assert_eq!(hex.len(), 64, "merkle hash should be 64 hex chars");
        assert!(
            hex.chars().all(|c| c.is_ascii_hexdigit()),
            "merkle hash should be valid hex"
        );
    }
    assert!(res["pos"].as_u64().is_some());

    rest_handle.stop();
    Ok(())
}

#[cfg(not(feature = "liquid"))]
#[test]
fn test_rest_tx_merkleblock_proof() -> Result<()> {
    let (rest_handle, rest_addr, mut tester) = common::init_rest_tester().unwrap();

    let addr1 = tester.newaddress()?;
    let txid = tester.send(&addr1, "0.5 BTC".parse().unwrap())?;
    tester.mine()?;

    let hex = get_plain(rest_addr, &format!("/tx/{}/merkleblock-proof", txid))?;
    assert!(!hex.is_empty());
    // Verify it's valid hex
    let bytes = Vec::from_hex(&hex).expect("valid hex");
    assert!(!bytes.is_empty());

    rest_handle.stop();
    Ok(())
}

#[test]
fn test_rest_mempool_recent() -> Result<()> {
    let (rest_handle, rest_addr, mut tester) = common::init_rest_tester().unwrap();

    let addr1 = tester.newaddress()?;
    let txid1 = tester.send(&addr1, "0.5 BTC".parse().unwrap())?;
    let txid2 = tester.send(&addr1, "0.3 BTC".parse().unwrap())?;

    let res = get_json(rest_addr, "/mempool/recent")?;
    let recent = res.as_array().expect("array of recent txs");
    assert!(recent.len() >= 2);

    for entry in recent {
        assert!(entry["txid"].is_string());
        assert!(entry["fee"].as_u64().unwrap() > 0);
        assert!(entry["vsize"].as_u64().unwrap() > 0);
        #[cfg(not(feature = "liquid"))]
        assert!(entry["value"].as_u64().unwrap() > 0);
    }

    // Verify our sent txids are included
    let recent_txids: HashSet<&str> = recent
        .iter()
        .map(|e| e["txid"].as_str().unwrap())
        .collect();
    assert!(recent_txids.contains(txid1.to_string().as_str()));
    assert!(recent_txids.contains(txid2.to_string().as_str()));

    rest_handle.stop();
    Ok(())
}

#[test]
fn test_rest_fee_estimates() -> Result<()> {
    let (rest_handle, rest_addr, _tester) = common::init_rest_tester().unwrap();

    let res = get_json(rest_addr, "/fee-estimates")?;
    // On regtest, may be empty but should be a JSON object
    assert!(res.is_object());

    rest_handle.stop();
    Ok(())
}

#[test]
fn test_rest_broadcast_get() -> Result<()> {
    let (rest_handle, rest_addr, mut tester) = common::init_rest_tester().unwrap();

    let addr1 = tester.newaddress()?;
    let txid = tester.send(&addr1, "0.5 BTC".parse().unwrap())?;
    let tx_hex = get_plain(rest_addr, &format!("/tx/{}/hex", txid))?;

    // Re-send via GET /broadcast?tx=:txhex (legacy endpoint)
    let res = get_plain(rest_addr, &format!("/broadcast?tx={}", tx_hex))?;
    assert_eq!(res, txid.to_string());

    rest_handle.stop();
    Ok(())
}

#[cfg(not(feature = "liquid"))]
#[test]
fn test_rest_reorg() -> Result<()> {
    let (rest_handle, rest_addr, mut tester) = common::init_rest_tester().unwrap();

    let get_conf_height = |txid| -> Result<Option<u64>> {
        Ok(get_json(rest_addr, &format!("/tx/{}/status", txid))?["block_height"].as_u64())
    };
    let get_chain_stats = |addr| -> Result<Value> {
        Ok(get_json(rest_addr, &format!("/address/{}", addr))?["chain_stats"].take())
    };
    let get_chain_txs = |addr| -> Result<Vec<Value>> {
        Ok(from_value(get_json(
            rest_addr,
            &format!("/address/{}/txs/chain", addr),
        )?)?)
    };
    let get_outspend = |outpoint: &bitcoin::OutPoint| -> Result<Value> {
        get_json(
            rest_addr,
            &format!("/tx/{}/outspend/{}", outpoint.txid, outpoint.vout),
        )
    };

    let init_height = tester.get_block_count()?;

    let address = tester.newaddress()?;
    let miner_address = tester.newaddress()?;

    let txid_a = tester.send(&address, Amount::from_sat(100000))?;
    let txid_b = tester.send(&address, Amount::from_sat(200000))?;
    let txid_c = tester.send(&address, Amount::from_sat(500000))?;

    let tx_a = tester.get_raw_transaction(txid_a)?;
    let tx_b = tester.get_raw_transaction(txid_b)?;
    let tx_c = tester.get_raw_transaction(txid_c)?;

    // Confirm tx_a, tx_b and tx_c
    let blockhash_1 = tester.mine()?;

    assert_eq!(
        get_plain(rest_addr, "/blocks/tip/height")?,
        (init_height + 1).to_string()
    );
    assert_eq!(
        get_plain(rest_addr, "/blocks/tip/hash")?,
        blockhash_1.to_string()
    );
    assert_eq!(get_conf_height(&txid_a)?, Some(init_height + 1));
    assert_eq!(get_conf_height(&txid_b)?, Some(init_height + 1));
    assert_eq!(get_conf_height(&txid_c)?, Some(init_height + 1));
    assert_eq!(
        get_chain_stats(&address)?["funded_txo_sum"].as_u64(),
        Some(800000)
    );
    assert_eq!(get_chain_txs(&address)?.len(), 3);

    let c_outspend = get_outspend(&tx_c.input[0].previous_output)?;
    assert_eq!(
        c_outspend["txid"].as_str(),
        Some(txid_c.to_string().as_str())
    );
    assert_eq!(
        c_outspend["status"]["block_height"].as_u64(),
        Some(init_height + 1)
    );

    // Reorg the last block, re-confirm tx_a at the same height
    tester.node_client().invalidate_block(blockhash_1)?;
    tester.node_client().call::<Value>(
        "generateblock",
        &[
            miner_address.to_string().into(),
            [txid_a.to_string()].into(),
        ],
    )?;
    // Re-confirm tx_b at a different height
    tester.node_client().call::<Value>(
        "generateblock",
        &[
            miner_address.to_string().into(),
            [txid_b.to_string()].into(),
        ],
    )?;
    // Don't re-confirm tx_c at all

    let blockhash_2 = tester.get_best_block_hash()?;

    tester.sync()?;

    assert_eq!(
        get_plain(rest_addr, "/blocks/tip/height")?,
        (init_height + 2).to_string()
    );
    assert_eq!(
        get_plain(rest_addr, "/blocks/tip/hash")?,
        blockhash_2.to_string()
    );

    // Test address stats (GET /address/:address)
    assert_eq!(
        get_chain_stats(&address)?["funded_txo_sum"].as_u64(),
        Some(300000)
    );

    // Test address history (GET /address/:address/txs/chain)
    let addr_txs = get_chain_txs(&address)?;
    assert_eq!(addr_txs.len(), 2);
    assert_eq!(
        addr_txs[0]["txid"].as_str(),
        Some(txid_b.to_string().as_str())
    );
    assert_eq!(
        addr_txs[0]["status"]["block_height"].as_u64(),
        Some(init_height + 2)
    );
    assert_eq!(
        addr_txs[1]["txid"].as_str(),
        Some(txid_a.to_string().as_str())
    );
    assert_eq!(
        addr_txs[1]["status"]["block_height"].as_u64(),
        Some(init_height + 1)
    );

    // Test transaction status lookup (GET /tx/:txid/status)
    assert_eq!(get_conf_height(&txid_a)?, Some(init_height + 1));
    assert_eq!(get_conf_height(&txid_b)?, Some(init_height + 2));
    assert_eq!(get_conf_height(&txid_c)?, None);

    // Test spend edge lookup (GET /tx/:txid/outspend/:vout)
    let a_spends = get_outspend(&tx_a.input[0].previous_output)?;
    assert_eq!(a_spends["txid"].as_str(), Some(txid_a.to_string().as_str()));
    assert_eq!(
        a_spends["status"]["block_height"].as_u64(),
        Some(init_height + 1)
    );
    let b_spends = get_outspend(&tx_b.input[0].previous_output)?;
    assert_eq!(b_spends["txid"].as_str(), Some(txid_b.to_string().as_str()));
    assert_eq!(
        b_spends["status"]["block_height"].as_u64(),
        Some(init_height + 2)
    );
    let c_spends = get_outspend(&tx_c.input[0].previous_output)?;
    assert_eq!(c_spends["status"]["confirmed"].as_bool(), Some(false));

    // Test a deeper reorg, all the way back to exclude tx_b
    tester.node_client().generate_to_address(15, &address)?;
    tester.sync()?;
    tester.node_client().invalidate_block(blockhash_2)?;

    for _ in 0..20 {
        // Mine some empty blocks, intentionally without tx_b
        tester.node_client().call::<Value>(
            "generateblock",
            &[miner_address.to_string().into(), Vec::<Value>::new().into()],
        )?;
    }
    tester.sync()?;

    assert_eq!(
        get_plain(rest_addr, "/blocks/tip/height")?,
        (init_height + 21).to_string()
    );
    assert_eq!(
        get_plain(rest_addr, "/blocks/tip/hash")?,
        tester.get_best_block_hash()?.to_string()
    );

    assert_eq!(
        get_chain_stats(&address)?["funded_txo_sum"].as_u64(),
        Some(100000)
    );

    let addr_txs = get_chain_txs(&address)?;
    assert_eq!(addr_txs.len(), 1);
    assert_eq!(
        addr_txs[0]["txid"].as_str(),
        Some(txid_a.to_string().as_str())
    );
    assert_eq!(
        addr_txs[0]["status"]["block_height"].as_u64(),
        Some(init_height + 1)
    );

    assert_eq!(get_conf_height(&txid_a)?, Some(init_height + 1));
    assert_eq!(get_conf_height(&txid_b)?, None);
    assert_eq!(get_conf_height(&txid_c)?, None);

    let a_spends = get_outspend(&tx_a.input[0].previous_output)?;
    assert_eq!(
        a_spends["status"]["block_height"].as_u64(),
        Some(init_height + 1)
    );
    let b_spends = get_outspend(&tx_b.input[0].previous_output)?;
    assert_eq!(b_spends["spent"].as_bool(), Some(false));
    let c_spends = get_outspend(&tx_b.input[0].previous_output)?;
    assert_eq!(c_spends["spent"].as_bool(), Some(false));

    // Invalidate the tip with no replacement, shortening the chain by one block
    tester
        .node_client()
        .invalidate_block(tester.get_best_block_hash()?)?;
    tester.sync()?;
    assert_eq!(
        get_plain(rest_addr, "/blocks/tip/height")?,
        (init_height + 20).to_string()
    );

    // Reorg everything back to genesis
    tester
        .node_client()
        .invalidate_block(tester.get_block_hash(1)?)?;
    tester.sync()?;

    assert_eq!(get_plain(rest_addr, "/blocks/tip/height")?, 0.to_string());
    assert_eq!(
        get_chain_stats(&address)?["funded_txo_sum"].as_u64(),
        Some(0)
    );
    assert_eq!(get_chain_txs(&address)?.len(), 0);
    assert_eq!(get_conf_height(&txid_a)?, None);
    assert_eq!(get_conf_height(&txid_b)?, None);
    assert_eq!(get_conf_height(&txid_c)?, None);
    let a_spends = get_outspend(&tx_a.input[0].previous_output)?;
    assert_eq!(a_spends["spent"].as_bool(), Some(false));

    rest_handle.stop();
    Ok(())
}

// bitcoin 28.0 only tests - submitpackage
#[cfg(not(feature = "liquid"))]
#[test]
fn test_rest_submit_package() -> Result<()> {
    let (rest_handle, rest_addr, tester) = common::init_rest_tester().unwrap();

    // Test with a real transaction package - create parent-child transactions
    // submitpackage requires between 2 and 25 transactions with proper dependencies
    let package_addr1 = tester.newaddress()?;
    let package_addr2 = tester.newaddress()?;

    // Create parent transaction
    let tx1_result = tester.node_client().call::<Value>(
        "createrawtransaction",
        &[
            serde_json::json!([]),
            serde_json::json!({package_addr1.to_string(): 0.5}),
        ],
    )?;
    let tx1_unsigned_hex = tx1_result.as_str().expect("raw tx hex").to_string();

    let tx1_fund_result = tester
        .node_client()
        .call::<Value>("fundrawtransaction", &[serde_json::json!(tx1_unsigned_hex)])?;
    let tx1_funded_hex = tx1_fund_result["hex"]
        .as_str()
        .expect("funded tx hex")
        .to_string();

    let tx1_sign_result = tester.node_client().call::<Value>(
        "signrawtransactionwithwallet",
        &[serde_json::json!(tx1_funded_hex)],
    )?;
    let tx1_signed_hex = tx1_sign_result["hex"]
        .as_str()
        .expect("signed tx hex")
        .to_string();

    // Decode parent transaction to get its txid and find the output to spend
    let tx1_decoded = tester
        .node_client()
        .call::<Value>("decoderawtransaction", &[serde_json::json!(tx1_signed_hex)])?;
    let tx1_txid = tx1_decoded["txid"].as_str().expect("parent txid");

    // Find the output going to package_addr1 (the one we want to spend)
    let tx1_vouts = tx1_decoded["vout"].as_array().expect("parent vouts");
    let mut spend_vout_index = None;
    let mut spend_vout_value = 0u64;

    for (i, vout) in tx1_vouts.iter().enumerate() {
        if let Some(script_pub_key) = vout.get("scriptPubKey") {
            if let Some(address) = script_pub_key.get("address") {
                if address.as_str() == Some(&package_addr1.to_string()) {
                    spend_vout_index = Some(i);
                    // Convert from BTC to satoshis
                    spend_vout_value =
                        (vout["value"].as_f64().expect("vout value") * 100_000_000.0) as u64;
                    break;
                }
            }
        }
    }

    let spend_vout_index = spend_vout_index.expect("Could not find output to spend");

    // Create child transaction that spends from parent
    // Leave some satoshis for fee (e.g., 1000 sats)
    let child_output_value = spend_vout_value - 1000;
    let child_output_btc = child_output_value as f64 / 100_000_000.0;

    let tx2_result = tester.node_client().call::<Value>(
        "createrawtransaction",
        &[
            serde_json::json!([{
                "txid": tx1_txid,
                "vout": spend_vout_index
            }]),
            serde_json::json!({package_addr2.to_string(): child_output_btc}),
        ],
    )?;
    let tx2_unsigned_hex = tx2_result.as_str().expect("raw tx hex").to_string();

    // Sign the child transaction
    // We need to provide the parent transaction's output details for signing
    let tx2_sign_result = tester.node_client().call::<Value>(
        "signrawtransactionwithwallet",
        &[
            serde_json::json!(tx2_unsigned_hex),
            serde_json::json!([{
                "txid": tx1_txid,
                "vout": spend_vout_index,
                "scriptPubKey": tx1_vouts[spend_vout_index]["scriptPubKey"]["hex"].as_str().unwrap(),
                "amount": spend_vout_value as f64 / 100_000_000.0
            }])
        ],
    )?;
    let tx2_signed_hex = tx2_sign_result["hex"]
        .as_str()
        .expect("signed tx hex")
        .to_string();

    // Debug: try calling submitpackage directly to see the result
    log::debug!("Trying submitpackage directly with parent-child transactions...");
    let direct_result = tester.node_client().call::<Value>(
        "submitpackage",
        &[serde_json::json!([
            tx1_signed_hex.clone(),
            tx2_signed_hex.clone()
        ])],
    );
    match direct_result {
        Ok(result) => {
            log::debug!("Direct submitpackage succeeded: {:#?}", result);
        }
        Err(e) => {
            log::debug!("Direct submitpackage failed: {:?}", e);
        }
    }

    // Now submit this transaction package via the package endpoint
    let package_resp = ureq::post(&format!("http://{}/txs/package", rest_addr))
        .send_json([tx1_signed_hex, tx2_signed_hex])?;

    assert_eq!(package_resp.status(), 200);
    let package_result = package_resp.into_body().read_json::<Value>()?;

    // Verify the response structure
    assert!(package_result["tx-results"].is_object());
    assert!(package_result["package_msg"].is_string());

    let tx_results = package_result["tx-results"].as_object().unwrap();
    assert_eq!(tx_results.len(), 2);

    // The transactions should be processed (whether accepted or rejected)
    assert!(!tx_results.is_empty());

    rest_handle.stop();
    Ok(())
}

// Elements-only tests

#[cfg(feature = "liquid")]
#[test]
fn test_rest_liquid_confidential_tx() -> Result<()> {
    let (rest_handle, rest_addr, mut tester) = common::init_rest_tester().unwrap();

    let (c_addr, uc_addr) = tester.ct_newaddress()?;
    let txid = tester.send(&c_addr, "3.5 BTC".parse().unwrap())?;
    tester.mine()?;
    let mine_height = tester.get_block_count()?;

    let tx = get_json(rest_addr, &format!("/tx/{}", txid))?;
    log::debug!("blinded tx = {:#?}", tx);
    assert_eq!(tx["status"]["confirmed"].as_bool(), Some(true));
    assert_eq!(tx["status"]["block_height"].as_u64(), Some(mine_height));
    assert!(tx["status"]["block_hash"].is_string());
    let outs = tx["vout"].as_array().expect("array of outs");
    let vout = outs
        .iter()
        .find(|vout| vout["scriptpubkey_address"].as_str() == Some(&uc_addr.to_string()))
        .expect("our output");
    assert!(vout["value"].is_null());
    assert!(vout["valuecommitment"].is_string());
    assert!(vout["assetcommitment"].is_string());
    assert!(vout["scriptpubkey_type"].is_string());

    rest_handle.stop();
    Ok(())
}

#[cfg(feature = "liquid")]
#[test]
fn test_rest_liquid_blinded_issuance() -> Result<()> {
    let (rest_handle, rest_addr, mut tester) = common::init_rest_tester().unwrap();

    use bitcoin::hashes::{sha256, Hash};
    let contract_hash = sha256::Hash::hash(&[0x11, 0x22, 0x33, 0x44]).to_string();
    let contract_hash = contract_hash.as_str();
    let issuance = tester.node_client().call::<Value>(
        "issueasset",
        &[1.5.into(), 0.into(), true.into(), contract_hash.into()],
    )?;
    tester.mine()?;

    let assetid = issuance["asset"].as_str().expect("asset id");
    let issuance_txid = issuance["txid"].as_str().expect("issuance txid");

    // Test GET /asset/:assetid
    let asset = get_json(rest_addr, &format!("/asset/{}", assetid))?;
    let stats = &asset["chain_stats"];
    assert_eq!(asset["asset_id"].as_str(), Some(assetid));
    assert_eq!(asset["issuance_txin"]["txid"].as_str(), Some(issuance_txid));
    assert_eq!(asset["contract_hash"].as_str(), Some(contract_hash));
    assert_eq!(asset["status"]["confirmed"].as_bool(), Some(true));
    assert_eq!(stats["issuance_count"].as_u64(), Some(1));
    assert_eq!(stats["has_blinded_issuances"].as_bool(), Some(true));
    assert_eq!(stats["issued_amount"].as_u64(), Some(0));

    // Test GET /tx/:txid for issuance tx
    let issuance_tx = get_json(rest_addr, &format!("/tx/{}", issuance_txid))?;
    let issuance_in_index = asset["issuance_txin"]["vin"].as_u64().unwrap();
    let issuance_in = &issuance_tx["vin"][issuance_in_index as usize];
    let issuance_data = &issuance_in["issuance"];
    assert_eq!(issuance_data["asset_id"].as_str(), Some(assetid));
    assert_eq!(issuance_data["is_reissuance"].as_bool(), Some(false));
    assert_eq!(issuance_data["contract_hash"].as_str(), Some(contract_hash));
    assert!(issuance_data["asset_entropy"].is_string());
    assert!(issuance_data["assetamount"].is_null());
    assert!(issuance_data["assetamountcommitment"].is_string());

    // Verify asset stats
    // TODO properly validate asset stats
    assert_eq!(stats["tx_count"].as_u64(), Some(1));

    rest_handle.stop();
    Ok(())
}

#[cfg(feature = "liquid")]
#[test]
fn test_rest_liquid_unblinded_issuance() -> Result<()> {
    let (rest_handle, rest_addr, mut tester) = common::init_rest_tester().unwrap();

    let issuance = tester
        .node_client()
        .call::<Value>("issueasset", &[1.5.into(), 0.into(), false.into()])?;
    tester.mine()?;
    let assetid = issuance["asset"].as_str().expect("asset id");
    let issuance_txid = issuance["txid"].as_str().expect("issuance txid");

    // Test GET /asset/:assetid
    let asset = get_json(rest_addr, &format!("/asset/{}", assetid))?;
    let stats = &asset["chain_stats"];
    assert_eq!(stats["has_blinded_issuances"].as_bool(), Some(false));
    assert_eq!(stats["issued_amount"].as_u64(), Some(150000000));
    assert_eq!(stats["issuance_count"].as_u64(), Some(1));
    assert_eq!(stats["tx_count"].as_u64(), Some(1));

    // Test GET /tx/:txid for issuance tx
    let issuance_tx = get_json(rest_addr, &format!("/tx/{}", issuance_txid))?;
    let issuance_in_index = asset["issuance_txin"]["vin"].as_u64().unwrap();
    let issuance_in = &issuance_tx["vin"][issuance_in_index as usize];
    let issuance_data = &issuance_in["issuance"];
    assert_eq!(issuance_data["assetamount"].as_u64(), Some(150000000));
    assert!(issuance_data["assetamountcommitment"].is_null());

    rest_handle.stop();
    Ok(())
}

#[cfg(feature = "liquid")]
#[test]
fn test_rest_liquid_asset_transfer() -> Result<()> {
    let (rest_handle, rest_addr, mut tester) = common::init_rest_tester().unwrap();

    let issuance = tester
        .node_client()
        .call::<Value>("issueasset", &[1.5.into(), 0.into(), false.into()])?;
    let assetid = issuance["asset"].as_str().expect("asset id");
    tester.mine()?;

    let (c_addr, uc_addr) = tester.ct_newaddress()?;

    // With blinding off
    let txid = tester.send_asset(
        &uc_addr,
        "0.3 BTC".parse().unwrap(), // not actually BTC, but this is what Amount expects
        assetid.parse().unwrap(),
    )?;
    let tx = get_json(rest_addr, &format!("/tx/{}", txid))?;
    let outs = tx["vout"].as_array().expect("array of outs");
    let vout = outs
        .iter()
        .find(|vout| vout["scriptpubkey_address"].as_str() == Some(&uc_addr.to_string()))
        .expect("our output");
    assert_eq!(vout["asset"].as_str(), Some(assetid));
    assert_eq!(vout["value"].as_u64(), Some(30000000));
    assert!(vout["scriptpubkey_type"].is_string());
    assert_eq!(
        vout["scriptpubkey_address"].as_str(),
        Some(uc_addr.to_string().as_str())
    );

    // With blinding on
    let txid = tester.send_asset(
        &c_addr,
        "0.3 BTC".parse().unwrap(),
        assetid.parse().unwrap(),
    )?;
    let tx = get_json(rest_addr, &format!("/tx/{}", txid))?;
    let outs = tx["vout"].as_array().expect("array of outs");
    let vout = outs
        .iter()
        .find(|vout| vout["scriptpubkey_address"].as_str() == Some(&uc_addr.to_string()))
        .expect("our output");
    assert!(vout["asset"].is_null());
    assert!(vout["value"].is_null());
    assert!(vout["assetcommitment"].is_string());
    assert!(vout["valuecommitment"].is_string());

    rest_handle.stop();
    Ok(())
}

#[cfg(feature = "liquid")]
#[test]
fn test_rest_liquid_block() -> Result<()> {
    let (rest_handle, rest_addr, _tester) = common::init_rest_tester().unwrap();

    // Test GET /block/:hash
    let block1_hash = get_plain(rest_addr, "/block-height/1")?;
    let block1 = get_json(rest_addr, &format!("/block/{}", block1_hash))?;

    // No PoW-related stuff
    assert!(block1["bits"].is_null());
    assert!(block1["nonce"].is_null());
    assert!(block1["difficulty"].is_null());

    // TODO properly validate dynafed parameters in first and second blocks
    // Dynamic Federations (dynafed) fields
    // Block #1 should have the Full dynafed params
    // See https://docs.rs/elements/latest/elements/dynafed/enum.Params.html
    assert!(block1["ext"]["current"]["signblockscript"].is_string());
    assert!(block1["ext"]["current"]["fedpegscript"].is_string());
    assert!(block1["ext"]["current"]["fedpeg_program"].is_string());
    assert!(block1["ext"]["current"]["signblock_witness_limit"].is_u64());
    assert!(block1["ext"]["proposed"].is_object());
    // TODO

    assert!(block1["ext"]["signblock_witness"].is_array());

    // Block #2 should have the Compact params
    let block2_hash = get_plain(rest_addr, "/block-height/2")?;
    let block2 = get_json(rest_addr, &format!("/block/{}", block2_hash))?;
    assert!(block2["ext"]["current"]["signblockscript"].is_string());
    assert!(block2["ext"]["current"]["signblock_witness_limit"].is_u64());
    // With the `elided_root` in place of `fedpegscript`/`fedpeg_program`/`extension_space``
    assert!(block2["ext"]["current"]["elided_root"].is_string());
    assert!(block2["ext"]["current"]["fedpegscript"].is_null());
    assert!(block2["ext"]["current"]["fedpeg_program"].is_null());
    assert!(block2["ext"]["current"]["extension_space"].is_null());

    rest_handle.stop();
    Ok(())
}
