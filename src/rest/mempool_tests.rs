//! Deterministic races through the private preparation functions used by REST.
use super::*;
use crate::test_common as common;
use crate::test_common::Result;

#[test]
#[cfg(not(feature = "liquid"))]
fn prevout_lookup_survives_ancestor_eviction() -> Result<()> {
    let mut tester = common::TestRunner::new()?;

    // Parent: a wallet spend, left unconfirmed.
    let addr_parent = tester.newaddress()?;
    let parent_txid = tester.send(&addr_parent, "1.0 BTC".parse().unwrap())?;

    let parent_tx = tester.get_raw_transaction(parent_txid)?;
    let parent_spk = addr_parent.script_pubkey();
    let (parent_vout, parent_txout) = parent_tx
        .output
        .iter()
        .enumerate()
        .find(|(_, o)| o.script_pubkey == parent_spk)
        .expect("funding output not found in parent tx");

    // Child: explicitly spends the parent's own unconfirmed output, so the
    // ancestor relationship (and thus the prevout dependency) is guaranteed.
    let addr_child = tester.newaddress()?;
    let child_value = parent_txout.value - bitcoin::Amount::from_sat(1000);
    let raw_child: String = tester.node_client().call(
        "createrawtransaction",
        &[
            json!([{ "txid": parent_txid.to_string(), "vout": parent_vout }]),
            json!({ addr_child.to_string(): child_value.to_btc() }),
        ],
    )?;
    let signed: serde_json::Value = tester
        .node_client()
        .call("signrawtransactionwithwallet", &[json!(raw_child)])?;
    assert_eq!(signed["complete"].as_bool(), Some(true));
    let child_txid_str: String = tester
        .node_client()
        .call("sendrawtransaction", &[signed["hex"].clone()])?;
    let child_txid = child_txid_str.parse().unwrap();

    tester.sync()?;

    let query = Arc::clone(tester.query());
    let config = Arc::clone(tester.config());
    let snapshot = MempoolTxs::capture(&query, |mempool| {
        assert!(tester.mempool().try_write().is_err());
        vec![mempool.lookup_txn(&child_txid).unwrap()]
    });
    let child_tx = snapshot.txs[0].0.clone();
    let parent_outpoint = OutPoint::new(parent_txid, parent_vout as u32);
    assert!(snapshot.prevouts.contains_key(&parent_outpoint));
    assert!(tester.mempool().try_write().is_ok());

    // EVICT: RBF-replace the parent on the node by directly double-spending
    // its inputs (not via `bumpfee`, which refuses once the wallet sees a
    // real descendant -- exactly the descendant we just created). This also
    // invalidates and evicts the child, since its input no longer exists.
    // Then apply that eviction to electrs's local mempool view -- exactly
    // what the background sync loop does on its next tick, just called
    // directly and synchronously here instead of racing its timer.
    let gettx: serde_json::Value = tester
        .node_client()
        .call("gettransaction", &[json!(parent_txid.to_string())])?;
    let parent_fee = bitcoin::Amount::from_btc(gettx["fee"].as_f64().unwrap().abs()).unwrap();
    let total_output: bitcoin::Amount = parent_tx.output.iter().map(|o| o.value).sum();
    let total_input = total_output + parent_fee;
    let higher_fee = bitcoin::Amount::from_sat(50_000); // well above the parent+child's combined fee
    let replacement_value = total_input - higher_fee;

    let addr_conflict = tester.newaddress()?;
    let raw_conflict: String = tester.node_client().call(
        "createrawtransaction",
        &[
            json!(parent_tx
                .input
                .iter()
                .map(|txin| json!({
                    "txid": txin.previous_output.txid.to_string(),
                    "vout": txin.previous_output.vout,
                }))
                .collect::<Vec<_>>()),
            json!({ addr_conflict.to_string(): replacement_value.to_btc() }),
        ],
    )?;
    let signed_conflict: serde_json::Value = tester
        .node_client()
        .call("signrawtransactionwithwallet", &[json!(raw_conflict)])?;
    assert_eq!(signed_conflict["complete"].as_bool(), Some(true));
    let _replacement_txid: String = tester
        .node_client()
        .call("sendrawtransaction", &[signed_conflict["hex"].clone()])?;

    let tip = tester.get_best_block_hash()?;
    assert_eq!(
        Mempool::update(&tester.mempool(), &tester.daemon(), &tip)?,
        crate::new_index::MempoolSyncStatus::Synced
    );

    // Exercise the actual REST preparation path after the ancestor is gone.
    // Keep a writer guard during preparation: no mempool re-read is allowed.
    let writer = tester.mempool().try_write().unwrap();
    let prepared = snapshot.prepare(&query, &config).unwrap();
    assert_eq!(prepared[0].txid, child_txid);
    assert_eq!(prepared[0].fee, 1000);
    let body = serde_json::to_value(&prepared[0])?;
    assert_eq!(
        body["vin"][0]["prevout"]["value"],
        parent_txout.value.to_sat()
    );

    // Losing the captured prevout must reach REST's 404 mapping, not abort.
    let stale = MempoolTxs {
        txs: vec![(child_tx, None)],
        prevouts: HashMap::new(),
    };
    let error = stale.prepare(&query, &config).err().unwrap();
    assert_eq!(error.0, StatusCode::NOT_FOUND);
    drop(writer);
    Ok(())
}

#[test]
fn history_keeps_transaction_confirmed_between_reads() -> Result<()> {
    for confirm_before_chain_read in [false, true] {
        let mut tester = common::TestRunner::new()?;
        let address = tester.newaddress()?;
        let txid = tester.send(&address, "1 BTC".parse().unwrap())?;
        let script_hash = compute_script_hash(&address.script_pubkey());
        let query = Arc::clone(tester.query());
        let config = Arc::clone(tester.config());
        let txs = prepare_history(
            &query,
            &config,
            |mempool| mempool.history(&script_hash, MAX_MEMPOOL_TXS),
            || {
                assert!(tester.mempool().try_write().is_ok());
                if confirm_before_chain_read {
                    tester.mine().unwrap();
                }
                let txs = query
                    .chain()
                    .history(&script_hash, None, CHAIN_TXS_PER_PAGE);
                if !confirm_before_chain_read {
                    // A chain-first implementation loses this transaction when
                    // it subsequently reads the now-empty mempool.
                    tester.mine().unwrap();
                }
                txs
            },
        )
        .unwrap();
        let matches: Vec<_> = txs.iter().filter(|tx| tx.txid == txid).collect();
        assert_eq!(matches.len(), 1);
        assert_eq!(
            matches[0].status.as_ref().unwrap().confirmed,
            confirm_before_chain_read
        );
    }
    Ok(())
}

#[test]
fn selected_mempool_transaction_uses_current_confirmation_status() -> Result<()> {
    let mut tester = common::TestRunner::new()?;
    let address = tester.newaddress()?;
    let txid = tester.send(&address, "1 BTC".parse().unwrap())?;
    let query = Arc::clone(tester.query());
    let config = Arc::clone(tester.config());
    let snapshot = MempoolTxs::capture(&query, |mempool| vec![mempool.lookup_txn(&txid).unwrap()]);
    let blockhash = tester.mine()?;
    let writer = tester.mempool().try_write().unwrap();
    let (tx, ttl) = prepare_captured_tx(snapshot, &txid, &query, &config).unwrap();
    let status = tx.status.unwrap();
    assert!(status.confirmed);
    assert_eq!(status.block_hash, Some(blockhash));
    assert_eq!(ttl, ttl_by_depth(status.block_height, &query));
    drop(writer);

    // The same path must also find a transaction already removed from mempool.
    let snapshot = MempoolTxs::capture(&query, |mempool| {
        mempool.lookup_txn(&txid).into_iter().collect()
    });
    assert!(snapshot.txs.is_empty());
    let (tx, _) = prepare_captured_tx(snapshot, &txid, &query, &config).unwrap();
    assert_eq!(tx.status.unwrap().block_hash, Some(blockhash));
    Ok(())
}

#[test]
fn confirmed_transaction_does_not_wait_for_mempool_writer() -> Result<()> {
    use std::sync::mpsc;
    use std::time::Duration;

    let mut tester = common::TestRunner::new()?;
    let address = tester.newaddress()?;
    let txid = tester.send(&address, "1 BTC".parse().unwrap())?;
    tester.mine()?;

    let query = Arc::clone(tester.query());
    let config = Arc::clone(tester.config());
    let writer = tester.mempool().try_write().unwrap();
    let (sender, receiver) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let result = prepare_tx(&txid, &query, &config).map(|(tx, _)| tx.status.unwrap().confirmed);
        sender.send(result).unwrap();
    });

    let completed = receiver.recv_timeout(Duration::from_secs(5));
    drop(writer);
    worker.join().unwrap();
    assert!(completed.unwrap().unwrap());
    Ok(())
}

#[test]
fn missing_confirmed_parent_is_unavailable() -> Result<()> {
    let mut tester = common::TestRunner::new()?;
    let address = tester.newaddress()?;
    let parent = tester.send(&address, "1 BTC".parse().unwrap())?;
    tester.mine()?;

    let missing = errors::Error::from(errors::ErrorKind::MissingTxo(
        OutPoint::new(parent, 0).to_string(),
    ));
    let status = HttpError::mempool_prevout(missing, tester.query(), &BTreeSet::new()).0;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    Ok(())
}

#[test]
#[cfg(feature = "liquid")]
fn asset_history_keeps_transaction_confirmed_between_reads() -> Result<()> {
    use elementsd::bitcoincore_rpc::RpcApi;
    for confirm_before_chain_read in [false, true] {
        let mut tester = common::TestRunner::new()?;
        let issued: serde_json::Value = tester
            .node_client()
            .call("issueasset", &[json!(1), json!(0)])?;
        let txid: Txid = issued["txid"].as_str().unwrap().parse().unwrap();
        let asset: AssetId = issued["asset"].as_str().unwrap().parse().unwrap();
        tester.sync()?;
        let query = Arc::clone(tester.query());
        let config = Arc::clone(tester.config());
        let txs = prepare_history(
            &query,
            &config,
            |mempool| mempool.asset_history(&asset, MAX_MEMPOOL_TXS),
            || {
                assert!(tester.mempool().try_write().is_ok());
                if confirm_before_chain_read {
                    tester.mine().unwrap();
                }
                let txs = query
                    .chain()
                    .asset_history(&asset, None, CHAIN_TXS_PER_PAGE);
                if !confirm_before_chain_read {
                    tester.mine().unwrap();
                }
                txs
            },
        )
        .unwrap();
        let matches: Vec<_> = txs.iter().filter(|tx| tx.txid == txid).collect();
        assert_eq!(matches.len(), 1);
        assert_eq!(
            matches[0].status.as_ref().unwrap().confirmed,
            confirm_before_chain_read
        );
    }
    Ok(())
}
