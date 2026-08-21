//! Deterministic reproduction of the mempool-eviction prevout-lookup abort
//! this branch fixes (see src/new_index/query.rs's `Query::lookup_txos`).
//!
//! The REST handlers for e.g. GET /tx/:txid used to look up a transaction
//! (once, in one mempool snapshot) and then separately resolve its prevouts
//! via `Query::lookup_txos()` (a second, later mempool snapshot). If the
//! referenced mempool ancestor was evicted in between -- e.g. by an RBF
//! replacement -- the second lookup could come back empty and hit an
//! `.expect("failed loading txos")`.
//!
//! Rather than racing real wall-clock timing against electrs's background
//! mempool-sync thread (which only runs on a fixed poll interval and makes
//! the window practically unhittable black-box), this test drives the same
//! two steps directly and deterministically: select the transaction, then
//! explicitly evict its ancestor by replacing it on the node and calling
//! `Mempool::update()` inline (this is exactly what the background sync
//! loop does, just invoked synchronously instead of on a timer), then
//! resolve prevouts. No timing luck involved.

use std::collections::BTreeSet;

use serde_json::json;

use electrs::chain::OutPoint;
use electrs::errors::ErrorKind;
use electrs::new_index::Mempool;

pub mod common;

use common::Result;

#[test]
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

    let query = tester.query();

    // SELECT: mirrors the GET /tx/:txid handler's `query.lookup_txn()` --
    // one mempool snapshot, cloning out the child tx.
    let child_tx = query
        .lookup_txn(&child_txid)?
        .expect("child tx should be indexed before eviction");
    let outpoints: BTreeSet<OutPoint> = child_tx
        .input
        .iter()
        .map(|txin| txin.previous_output)
        .collect();
    assert!(outpoints.contains(&OutPoint::new(parent_txid, parent_vout as u32)));

    // Sanity check: resolving prevouts works fine right now, before any
    // eviction -- proves the graceful error below is caused by the
    // eviction, not by some unrelated test-setup mistake.
    let prevouts_before = query.lookup_txos(outpoints.clone())?;
    assert!(prevouts_before.contains_key(&OutPoint::new(parent_txid, parent_vout as u32)));

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
    assert!(Mempool::update(&tester.mempool(), &tester.daemon(), &tip)?);

    // LOOKUP: mirrors prepare_txs()'s `query.lookup_txos()` on the *same*
    // outpoints selected before the eviction -- the parent's output is now
    // gone from both the local mempool view (evicted) and the confirmed
    // chain (it was never mined). Pre-fix this panicked; post-fix it must
    // come back as a graceful `MissingTxo` error instead.
    let result = query.lookup_txos(outpoints);

    match result {
        Ok(txos) => panic!(
            "expected lookup_txos() to report the evicted prevout as missing, \
             but it returned successfully with {} txos -- the race wasn't \
             reproduced (check the eviction actually happened)",
            txos.len()
        ),
        Err(ref e) => match e.kind() {
            ErrorKind::MissingTxo(outpoint) => {
                assert_eq!(
                    outpoint,
                    &OutPoint::new(parent_txid, parent_vout as u32).to_string()
                );
            }
            other => panic!("expected ErrorKind::MissingTxo, got: {:?}", other),
        },
    }

    Ok(())
}
