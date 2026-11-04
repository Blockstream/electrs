pub mod common;
use std::io::{Read, Write};
use std::net::TcpStream;

use common::Result;

#[cfg(not(feature = "liquid"))]
use serde_json::json;

#[cfg(not(feature = "liquid"))]
use bdk_electrum::{electrum_client, BdkElectrumClient};
#[cfg(not(feature = "liquid"))]
use bdk_wallet::{KeychainKind, SignOptions, Wallet};
#[cfg(not(feature = "liquid"))]
use bitcoin::{bip32::Xpriv, Amount, Network, Txid};

#[cfg(not(feature = "liquid"))]
use electrs::chain::Address;
#[cfg(not(feature = "liquid"))]
use electrs::electrum::RPC as ElectrumRPC;

/// An in-process BDK wallet synced through the electrs Electrum server under test.
#[cfg(not(feature = "liquid"))]
struct WalletTester {
    _electrum_server: ElectrumRPC,
    client: BdkElectrumClient<electrum_client::Client>,
    wallet: Wallet,
    xprv: Xpriv,
    tester: common::TestRunner,
}

#[cfg(not(feature = "liquid"))]
impl WalletTester {
    fn new() -> Result<Self> {
        let (electrum_server, electrum_addr, tester) = common::init_electrum_tester()?;

        // Deterministic test keys, regtest only.
        let xprv = Xpriv::new_master(Network::Regtest, &[42u8; 32]).unwrap();
        let wallet = Wallet::create(
            format!("wpkh({}/84'/1'/0'/0/*)", xprv),
            format!("wpkh({}/84'/1'/0'/1/*)", xprv),
        )
        .network(Network::Regtest)
        .create_wallet_no_persist()
        .expect("valid descriptors");

        let client = electrum_client::Client::new(&format!("tcp://{}", electrum_addr))
            .expect("connect to electrs");
        let client = BdkElectrumClient::new(client);

        Ok(WalletTester {
            _electrum_server: electrum_server,
            client,
            wallet,
            xprv,
            tester,
        })
    }

    /// Scan the wallet's scripts through electrs and apply the result.
    fn sync_wallet(&mut self) {
        let request = self.wallet.start_full_scan().build();
        let update = self
            .client
            .full_scan(request, 10, 5, true)
            .expect("full scan through electrs");
        self.wallet.apply_update(update).expect("apply update");
    }

    /// Assert the wallet's confirmed and unconfirmed balances, in sats.
    fn assert_balance(&self, confirmed: u64, unconfirmed: u64) {
        let balance = self.wallet.balance();
        log::info!("balance: {}", balance);
        assert_eq!(balance.confirmed.to_sat(), confirmed, "confirmed balance");
        assert_eq!(
            (balance.trusted_pending + balance.untrusted_pending).to_sat(),
            unconfirmed,
            "unconfirmed balance"
        );
        assert_eq!(balance.immature.to_sat(), 0);
    }

    fn newaddress(&mut self) -> Address {
        self.wallet
            .reveal_next_address(KeychainKind::External)
            .address
    }

    /// Height of the block that confirmed `txid` according to the synced wallet.
    fn confirmation_height(&self, txid: Txid) -> Option<u32> {
        let tx = self.wallet.get_tx(txid).expect("tx known to the wallet");
        tx.chain_position.confirmation_height_upper_bound()
    }
}

/// Test balance tracking with confirmed and unconfirmed transactions
#[cfg(not(feature = "liquid"))]
#[test]
fn test_electrum_balance() -> Result<()> {
    let mut wt = WalletTester::new()?;

    let addr1 = wt.newaddress();
    let addr2 = wt.newaddress();

    wt.sync_wallet();
    wt.assert_balance(0, 0);

    wt.tester.send(&addr1, "0.1 BTC".parse().unwrap())?;
    wt.sync_wallet();
    wt.assert_balance(0, 10_000_000);

    wt.tester.mine()?;
    wt.sync_wallet();
    wt.assert_balance(10_000_000, 0);

    wt.tester.send(&addr2, "0.2 BTC".parse().unwrap())?;
    wt.sync_wallet();
    wt.assert_balance(10_000_000, 20_000_000);

    wt.tester.mine()?;
    wt.sync_wallet();
    wt.assert_balance(30_000_000, 0);

    Ok(())
}

/// Test transaction history and confirmation heights
#[cfg(not(feature = "liquid"))]
#[test]
fn test_electrum_history() -> Result<()> {
    let mut wt = WalletTester::new()?;

    let addr1 = wt.newaddress();
    let addr2 = wt.newaddress();

    let txid1 = wt.tester.send(&addr1, "0.1 BTC".parse().unwrap())?;
    wt.tester.mine()?;
    let txid2 = wt.tester.send(&addr2, "0.2 BTC".parse().unwrap())?;
    wt.tester.mine()?;
    wt.sync_wallet();

    assert_eq!(wt.wallet.transactions().count(), 2);

    assert_eq!(wt.confirmation_height(txid1), Some(102));
    let tx1 = wt.wallet.get_tx(txid1).unwrap().tx_node.tx;
    assert_eq!(
        wt.wallet.sent_and_received(&tx1).1,
        Amount::from_sat(10_000_000)
    );

    assert_eq!(wt.confirmation_height(txid2), Some(103));
    let tx2 = wt.wallet.get_tx(txid2).unwrap().tx_node.tx;
    assert_eq!(
        wt.wallet.sent_and_received(&tx2).1,
        Amount::from_sat(20_000_000)
    );

    Ok(())
}

/// Test signing an outgoing payment and broadcasting it through electrs
#[cfg(not(feature = "liquid"))]
#[test]
fn test_electrum_payment() -> Result<()> {
    let mut wt = WalletTester::new()?;

    let addr1 = wt.newaddress();
    wt.tester.send(&addr1, "0.3 BTC".parse().unwrap())?;
    wt.tester.mine()?;
    wt.sync_wallet();
    wt.assert_balance(30_000_000, 0);

    let destination = wt.tester.node_client().new_address()?;
    let mut builder = wt.wallet.build_tx();
    builder
        .add_recipient(destination.script_pubkey(), Amount::from_sat(16_000_000))
        .fee_absolute(Amount::from_sat(100_000));
    let mut psbt = builder.finish().expect("build payment");
    psbt.sign(&wt.xprv, wt.wallet.secp_ctx())
        .expect("sign payment");
    assert!(wt
        .wallet
        .finalize_psbt(&mut psbt, SignOptions::default())
        .expect("finalize payment"));
    let tx = psbt.extract_tx().expect("extract signed tx");

    let txid = wt.client.transaction_broadcast(&tx).expect("broadcast");
    assert_eq!(txid, tx.compute_txid());
    assert!(wt
        .tester
        .node_client()
        .get_raw_mempool()?
        .0
        .contains(&txid.to_string()));

    // The change output (0.3 - 0.16 - 0.001 fee) is unconfirmed until mined.
    wt.tester.sync()?;
    wt.sync_wallet();
    assert_eq!(
        wt.wallet.calculate_fee(&tx).unwrap(),
        Amount::from_sat(100_000)
    );
    wt.assert_balance(0, 13_900_000);

    wt.tester.mine()?;
    wt.sync_wallet();
    wt.assert_balance(13_900_000, 0);
    assert!(wt.confirmation_height(txid).is_some());

    Ok(())
}

/// BDK syncs by polling, so cover the server's subscription push path over a raw socket:
/// `blockchain.headers.subscribe` and `blockchain.scripthash.subscribe` notifications.
#[cfg(not(feature = "liquid"))]
#[test]
fn test_electrum_subscription_notifications() -> Result<()> {
    use bitcoin::hashes::{sha256, Hash};
    use bitcoin::hex::DisplayHex;

    let (electrum_server, electrum_addr, mut tester) = common::init_electrum_tester()?;
    let mut stream = TcpStream::connect(electrum_addr).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(10)))
        .unwrap();
    let parse = |s: &str| -> serde_json::Value { serde_json::from_str(s).unwrap() };

    let reply = parse(&write_and_read(
        &mut stream,
        "{\"jsonrpc\": \"2.0\", \"method\": \"blockchain.headers.subscribe\", \"params\": [], \"id\": 1}",
    ));
    let tip_height = reply["result"]["height"].as_u64().unwrap();
    assert_eq!(tip_height, tester.get_block_count()?);

    let addr = tester.newaddress()?;
    let mut hash = sha256::Hash::hash(addr.script_pubkey().as_bytes()).to_byte_array();
    hash.reverse();
    let scripthash = hash.to_lower_hex_string();

    let reply = parse(&write_and_read(
        &mut stream,
        &format!(
            "{{\"jsonrpc\": \"2.0\", \"method\": \"blockchain.scripthash.subscribe\", \"params\": [\"{}\"], \"id\": 2}}",
            scripthash
        ),
    ));
    assert!(reply["result"].is_null(), "unused script has no status");

    // A mempool tx changes the script's status.
    tester.send(&addr, "0.1 BTC".parse().unwrap())?;
    electrum_server.notify();
    let note = parse(&read_line(&mut stream));
    assert_eq!(note["method"], "blockchain.scripthash.subscribe");
    assert_eq!(note["params"][0], scripthash.as_str());
    let mempool_status = note["params"][1].as_str().unwrap().to_string();

    // Mining it pushes a new header and a new (confirmed) status.
    tester.mine()?;
    electrum_server.notify();
    let mut notes: Vec<serde_json::Value> =
        (0..2).map(|_| parse(&read_line(&mut stream))).collect();
    notes.sort_by_key(|n| n["method"].as_str().unwrap().to_string());
    assert_eq!(notes[0]["method"], "blockchain.headers.subscribe");
    assert_eq!(
        notes[0]["params"][0]["height"].as_u64(),
        Some(tip_height + 1)
    );
    assert_eq!(notes[1]["method"], "blockchain.scripthash.subscribe");
    assert_eq!(notes[1]["params"][0], scripthash.as_str());
    assert_ne!(notes[1]["params"][1].as_str().unwrap(), mempool_status);

    Ok(())
}

/// Test the Electrum RPC server using a raw TCP socket
#[cfg_attr(not(feature = "liquid"), test)]
#[cfg_attr(feature = "liquid", allow(dead_code))]
fn test_electrum_raw() {
    let (_electrum_server, electrum_addr, mut _tester) = common::init_electrum_tester().unwrap();

    let mut stream = TcpStream::connect(electrum_addr).unwrap();
    let write = "{\"jsonrpc\": \"2.0\", \"method\": \"server.version\", \"id\": 0}";

    let s = write_and_read(&mut stream, write);
    let expected = "{\"id\":0,\"jsonrpc\":\"2.0\",\"result\":[\"electrs-esplora 0.4.1\",\"1.4\"]}";
    assert_eq!(s, expected);

    let write = "[{\"jsonrpc\": \"2.0\", \"method\": \"server.version\", \"id\": 0}]";
    let s = write_and_read(&mut stream, write);
    let expected =
        "[{\"id\":0,\"jsonrpc\":\"2.0\",\"result\":[\"electrs-esplora 0.4.1\",\"1.4\"]}]";
    assert_eq!(s, expected);
}

#[test]
fn test_electrum_server_features_without_public_hosts() {
    // Electrum >= 4.7.0 calls server.features on connect, so it must answer
    // even when peer discovery is off (no --electrum-public-hosts). See #229.
    let (_electrum_server, electrum_addr, tester) = common::init_electrum_tester().unwrap();

    let mut stream = TcpStream::connect(electrum_addr).unwrap();
    let s = write_and_read(
        &mut stream,
        "{\"jsonrpc\": \"2.0\", \"method\": \"server.features\", \"params\": [], \"id\": 1}",
    );
    let reply: serde_json::Value = serde_json::from_str(&s).unwrap();
    assert!(
        reply.get("error").is_none(),
        "server.features errored: {}",
        s
    );

    let features = &reply["result"];
    assert_eq!(features["hosts"], serde_json::json!({}));
    assert_eq!(features["server_version"], "electrs-esplora 0.4.1");
    assert_eq!(features["hash_function"], "sha256");

    // The genesis hash must be the actual genesis block hash indexed at height 0,
    // not the hardcoded genesis_hash() (which is an all-zero placeholder on
    // Liquid testnet/regtest). The Electrum protocol serializes hashes reversed.
    use bitcoin::hashes::Hash;
    use bitcoin::hex::DisplayHex;
    let mut genesis_bytes = tester
        .get_block_hash(0)
        .unwrap()
        .to_raw_hash()
        .to_byte_array();
    genesis_bytes.reverse();
    let expected = genesis_bytes.to_lower_hex_string();
    #[cfg(feature = "liquid")]
    assert_ne!(
        expected,
        "0".repeat(64),
        "Elements genesis must differ from the hardcoded placeholder"
    );
    assert_eq!(
        features["genesis_hash"].as_str(),
        Some(expected.as_str()),
        "genesis_hash must be the indexed height-0 block hash: {}",
        s
    );
}

#[cfg_attr(not(feature = "liquid"), test)]
#[cfg_attr(feature = "liquid", allow(dead_code))]
fn test_electrum_jsonrpc_errors() {
    let (_electrum_server, electrum_addr, mut _tester) = common::init_electrum_tester().unwrap();

    let mut stream = TcpStream::connect(electrum_addr).unwrap();

    // unknown method: -32601 error reply instead of dropping the connection
    let s = write_and_read(
        &mut stream,
        "{\"jsonrpc\": \"2.0\", \"method\": \"foo.bar\", \"params\": [], \"id\": 1}",
    );
    let expected = "{\"error\":{\"code\":-32601,\"message\":\"unknown method foo.bar\"},\"id\":1,\"jsonrpc\":\"2.0\"}";
    assert_eq!(s, expected);

    // missing param: -32602 invalid params
    let s = write_and_read(
        &mut stream,
        "{\"jsonrpc\": \"2.0\", \"method\": \"blockchain.block.header\", \"params\": [], \"id\": 2}",
    );
    let expected =
        "{\"error\":{\"code\":-32602,\"message\":\"missing height\"},\"id\":2,\"jsonrpc\":\"2.0\"}";
    assert_eq!(s, expected);

    // valid JSON but not a request object: -32600 invalid request, id echoed back
    let s = write_and_read(&mut stream, "{\"jsonrpc\": \"2.0\", \"id\": 3}");
    let expected =
        "{\"error\":{\"code\":-32600,\"message\":\"invalid request\"},\"id\":3,\"jsonrpc\":\"2.0\"}";
    assert_eq!(s, expected);

    // unparseable JSON: -32700 parse error with null id
    let s = write_and_read(&mut stream, "{not json");
    let expected =
        "{\"error\":{\"code\":-32700,\"message\":\"parse error\"},\"id\":null,\"jsonrpc\":\"2.0\"}";
    assert_eq!(s, expected);

    // a batch with an unknown method still answers the other entries
    let s = write_and_read(
        &mut stream,
        "[{\"jsonrpc\": \"2.0\", \"method\": \"server.ping\", \"id\": 4}, {\"jsonrpc\": \"2.0\", \"method\": \"foo.bar\", \"id\": 5}]",
    );
    let expected = "[{\"id\":4,\"jsonrpc\":\"2.0\",\"result\":null},{\"error\":{\"code\":-32601,\"message\":\"unknown method foo.bar\"},\"id\":5,\"jsonrpc\":\"2.0\"}]";
    assert_eq!(s, expected);

    // the connection survived all of the above
    let s = write_and_read(
        &mut stream,
        "{\"jsonrpc\": \"2.0\", \"method\": \"server.ping\", \"id\": 6}",
    );
    let expected = "{\"id\":6,\"jsonrpc\":\"2.0\",\"result\":null}";
    assert_eq!(s, expected);
}

/// Test blockchain.scripthash.get_mempool returns only the unconfirmed history of a scripthash
#[cfg_attr(not(feature = "liquid"), test)]
#[cfg_attr(feature = "liquid", allow(dead_code))]
fn test_electrum_scripthash_get_mempool() -> Result<()> {
    use bitcoin::hashes::{sha256, Hash};
    use bitcoin::hex::DisplayHex;

    let (_electrum_server, electrum_addr, mut tester) = common::init_electrum_tester()?;

    let addr = tester.newaddress()?;

    // Electrum protocol script hash: single SHA256 of the scriptPubKey, in reversed byte order
    let mut hash = sha256::Hash::hash(addr.script_pubkey().as_bytes()).to_byte_array();
    hash.reverse();
    let scripthash = hash.to_lower_hex_string();

    let request = |id: u32| {
        format!(
            "{{\"jsonrpc\": \"2.0\", \"method\": \"blockchain.scripthash.get_mempool\", \"params\": [\"{}\"], \"id\": {}}}",
            scripthash, id
        )
    };

    let mut stream = TcpStream::connect(electrum_addr).unwrap();

    // no mempool history yet
    let s = write_and_read(&mut stream, &request(1));
    assert_eq!(s, "{\"id\":1,\"jsonrpc\":\"2.0\",\"result\":[]}");

    // an unconfirmed tx funding the address shows up (tester.send syncs the mempool index)
    let txid = tester.send(&addr, "0.1 BTC".parse().unwrap())?;

    let s = write_and_read(&mut stream, &request(2));
    let v: serde_json::Value = serde_json::from_str(&s).unwrap();
    let entries = v["result"].as_array().expect("result array");
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0]["tx_hash"].as_str(),
        Some(txid.to_string().as_str())
    );
    // height 0: all inputs confirmed (no unconfirmed parents)
    assert_eq!(entries[0]["height"].as_i64(), Some(0));
    assert!(entries[0]["fee"].as_u64().is_some());

    // once confirmed, it no longer appears in the mempool result
    tester.mine()?;
    let s = write_and_read(&mut stream, &request(3));
    assert_eq!(s, "{\"id\":3,\"jsonrpc\":\"2.0\",\"result\":[]}");

    Ok(())
}

/// Test blockchain.transaction.broadcast_package submits a package via bitcoind's submitpackage
#[cfg(not(feature = "liquid"))]
#[test]
fn test_electrum_broadcast_package() -> Result<()> {
    use bitcoin::consensus::encode::serialize_hex;

    let (_electrum_server, electrum_addr, mut tester) = common::init_electrum_tester()?;

    let addr = tester.newaddress()?;
    // create a tx; it stays in the mempool, so re-submitting it as a 1-tx package succeeds
    let txid = tester.send(&addr, "0.1 BTC".parse().unwrap())?;
    let tx_hex = serialize_hex(&tester.get_raw_transaction(txid)?);

    let mut stream = TcpStream::connect(electrum_addr).unwrap();

    // non-verbose: returns { "success": true }
    let s = write_and_read(
        &mut stream,
        &format!(
            "{{\"jsonrpc\": \"2.0\", \"method\": \"blockchain.transaction.broadcast_package\", \"params\": [[\"{}\"]], \"id\": 1}}",
            tx_hex
        ),
    );
    let v: serde_json::Value = serde_json::from_str(&s).unwrap();
    assert_eq!(
        v["result"]["success"].as_bool(),
        Some(true),
        "unexpected response: {}",
        s
    );

    // verbose: returns the full submitpackage result, including package_msg
    let s = write_and_read(
        &mut stream,
        &format!(
            "{{\"jsonrpc\": \"2.0\", \"method\": \"blockchain.transaction.broadcast_package\", \"params\": [[\"{}\"], true], \"id\": 2}}",
            tx_hex
        ),
    );
    let v: serde_json::Value = serde_json::from_str(&s).unwrap();
    assert_eq!(
        v["result"]["package_msg"].as_str(),
        Some("success"),
        "unexpected verbose response: {}",
        s
    );

    Ok(())
}

/// Regression test: a package broadcast must add accepted txs to electrs' local mempool, so a
/// subscribed/queried scripthash sees them immediately (not only after the next background sync).
#[cfg(not(feature = "liquid"))]
#[test]
fn test_electrum_broadcast_package_updates_mempool() -> Result<()> {
    use bitcoin::consensus::encode::serialize_hex;
    use bitcoin::hashes::{sha256, Hash};
    use bitcoin::hex::DisplayHex;

    let (_electrum_server, electrum_addr, tester) = common::init_electrum_tester()?;

    let addr = tester.newaddress()?;
    let mut hash = sha256::Hash::hash(addr.script_pubkey().as_bytes()).to_byte_array();
    hash.reverse();
    let scripthash = hash.to_lower_hex_string();

    // Create a tx via the node directly, WITHOUT tester.send() -- so electrs does not sync it into
    // its local mempool. The tx is in bitcoind's mempool but unknown to electrs.
    let amount = bitcoin::Amount::from_btc(0.1).unwrap();
    let txid: bitcoin::Txid = tester
        .node_client()
        .call(
            "sendtoaddress",
            &[addr.to_string().into(), json!(amount.to_btc())],
        )
        .unwrap();
    let tx_hex = serialize_hex(&tester.get_raw_transaction(txid)?);

    let mut stream = TcpStream::connect(electrum_addr).unwrap();

    let history = |stream: &mut TcpStream, id: u32| -> Vec<String> {
        let s = write_and_read(
            stream,
            &format!(
                "{{\"jsonrpc\": \"2.0\", \"method\": \"blockchain.scripthash.get_history\", \"params\": [\"{}\"], \"id\": {}}}",
                scripthash, id
            ),
        );
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        v["result"]
            .as_array()
            .expect("result array")
            .iter()
            .map(|e| e["tx_hash"].as_str().unwrap().to_string())
            .collect()
    };

    // electrs has not synced the tx yet, so its history is empty
    assert!(
        history(&mut stream, 1).is_empty(),
        "tx should not be in electrs' mempool before broadcast_package"
    );

    // broadcast the (already-in-bitcoind-mempool) tx as a 1-tx package
    let s = write_and_read(
        &mut stream,
        &format!(
            "{{\"jsonrpc\": \"2.0\", \"method\": \"blockchain.transaction.broadcast_package\", \"params\": [[\"{}\"]], \"id\": 2}}",
            tx_hex
        ),
    );
    let v: serde_json::Value = serde_json::from_str(&s).unwrap();
    assert_eq!(
        v["result"]["success"].as_bool(),
        Some(true),
        "response: {}",
        s
    );

    // the accepted tx must now be visible in the scripthash history immediately
    assert_eq!(
        history(&mut stream, 3),
        vec![txid.to_string()],
        "broadcast_package did not add the accepted tx to electrs' local mempool"
    );

    Ok(())
}

#[cfg(not(feature = "liquid"))]
mod checkpoint {
    use bitcoin::hashes::{sha256d, Hash};

    pub fn merklize(left: sha256d::Hash, right: sha256d::Hash) -> sha256d::Hash {
        let mut data = [0u8; 64];
        data[..32].copy_from_slice(&left[..]);
        data[32..].copy_from_slice(&right[..]);
        sha256d::Hash::hash(&data)
    }

    pub fn merkle_root(mut hashes: Vec<sha256d::Hash>) -> sha256d::Hash {
        assert!(!hashes.is_empty());
        while hashes.len() > 1 {
            if hashes.len() % 2 != 0 {
                hashes.push(*hashes.last().unwrap());
            }
            hashes = hashes
                .chunks(2)
                .map(|pair| merklize(pair[0], pair[1]))
                .collect();
        }
        hashes[0]
    }

    pub fn root_from_branch(
        leaf: sha256d::Hash,
        branch: &[sha256d::Hash],
        mut index: usize,
    ) -> sha256d::Hash {
        let mut current = leaf;
        for sibling in branch {
            current = if index % 2 == 0 {
                merklize(current, *sibling)
            } else {
                merklize(*sibling, current)
            };
            index /= 2;
        }
        current
    }
}

#[cfg(not(feature = "liquid"))]
#[test]
fn test_electrum_checkpoint_proofs() -> Result<()> {
    use bitcoin::hashes::{sha256d, Hash};
    use bitcoin::hex::FromHex;
    use std::str::FromStr;

    let (_electrum_server, electrum_addr, tester) = common::init_electrum_tester()?;
    let tip = tester.get_block_count()? as usize;

    let leaves: Vec<sha256d::Hash> = (0..=tip)
        .map(|h| Ok(tester.get_block_hash(h as u64)?.to_raw_hash()))
        .collect::<Result<_>>()?;
    let expected_root = checkpoint::merkle_root(leaves.clone());

    let mut stream = TcpStream::connect(electrum_addr).unwrap();
    let parse = |s: &str| -> serde_json::Value { serde_json::from_str(s).unwrap() };

    let height = 5usize;
    let v = parse(&write_and_read(
        &mut stream,
        &format!(
            "{{\"jsonrpc\": \"2.0\", \"method\": \"blockchain.block.header\", \"params\": [{}, {}], \"id\": 1}}",
            height, tip
        ),
    ));
    let result = &v["result"];
    let header_bytes = Vec::<u8>::from_hex(result["header"].as_str().expect("header hex")).unwrap();
    assert_eq!(
        sha256d::Hash::hash(&header_bytes),
        leaves[height],
        "returned header must hash to the block hash at the requested height"
    );
    let root = sha256d::Hash::from_str(result["root"].as_str().expect("root")).unwrap();
    assert_eq!(
        root, expected_root,
        "proof root must match the independent tree"
    );
    let branch: Vec<sha256d::Hash> = result["branch"]
        .as_array()
        .expect("branch array")
        .iter()
        .map(|h| sha256d::Hash::from_str(h.as_str().unwrap()).unwrap())
        .collect();
    assert_eq!(
        checkpoint::root_from_branch(leaves[height], &branch, height),
        expected_root,
        "branch must connect the leaf to the root"
    );

    let v2 = parse(&write_and_read(
        &mut stream,
        &format!(
            "{{\"jsonrpc\": \"2.0\", \"method\": \"blockchain.block.header\", \"params\": [{}, {}], \"id\": 2}}",
            height, tip
        ),
    ));
    assert_eq!(v["result"], v2["result"], "cached proof must be identical");

    let v = parse(&write_and_read(
        &mut stream,
        &format!(
            "{{\"jsonrpc\": \"2.0\", \"method\": \"blockchain.block.header\", \"params\": [{}, 0], \"id\": 3}}",
            height
        ),
    ));
    assert!(v["result"].is_string(), "cp_height=0 must return bare hex");

    let v = parse(&write_and_read(
        &mut stream,
        &format!(
            "{{\"jsonrpc\": \"2.0\", \"method\": \"blockchain.block.header\", \"params\": [{}, {}], \"id\": 4}}",
            height, height - 1
        ),
    ));
    assert!(!v["error"].is_null(), "cp_height < height must be an error");

    let v = parse(&write_and_read(
        &mut stream,
        &format!(
            "{{\"jsonrpc\": \"2.0\", \"method\": \"blockchain.block.header\", \"params\": [{}, {}], \"id\": 5}}",
            height, tip + 1
        ),
    ));
    assert!(
        !v["error"].is_null(),
        "cp_height above the tip must be an error"
    );

    let (start, count) = (0usize, 10usize);
    let v = parse(&write_and_read(
        &mut stream,
        &format!(
            "{{\"jsonrpc\": \"2.0\", \"method\": \"blockchain.block.headers\", \"params\": [{}, {}, {}], \"id\": 6}}",
            start, count, tip
        ),
    ));
    let result = &v["result"];
    assert_eq!(result["count"].as_u64(), Some(count as u64));
    let hex = result["hex"].as_str().expect("headers hex");
    assert_eq!(hex.len(), count * 80 * 2);
    let root = sha256d::Hash::from_str(result["root"].as_str().expect("root")).unwrap();
    assert_eq!(root, expected_root);
    let branch: Vec<sha256d::Hash> = result["branch"]
        .as_array()
        .expect("branch array")
        .iter()
        .map(|h| sha256d::Hash::from_str(h.as_str().unwrap()).unwrap())
        .collect();
    let proof_height = start + count - 1;
    assert_eq!(
        checkpoint::root_from_branch(leaves[proof_height], &branch, proof_height),
        expected_root,
        "headers branch must connect the last requested header to the root"
    );

    let v = parse(&write_and_read(
        &mut stream,
        &format!(
            "{{\"jsonrpc\": \"2.0\", \"method\": \"blockchain.block.headers\", \"params\": [{}, {}, {}], \"id\": 7}}",
            tip - 1, 5, tip
        ),
    ));
    assert!(
        !v["error"].is_null(),
        "a headers range extending past cp_height must be an error"
    );

    Ok(())
}

#[cfg(not(feature = "liquid"))]
#[test]
fn test_electrum_checkpoint_proof_reorg_consistency() -> Result<()> {
    use bitcoin::hashes::{sha256d, Hash};
    use bitcoin::hex::FromHex;
    use std::str::FromStr;

    let (_electrum_server, electrum_addr, mut tester) = common::init_electrum_tester()?;
    let tip = tester.get_block_count()? as usize;
    let height = 3usize;

    let mut stream = TcpStream::connect(electrum_addr).unwrap();
    let parse = |s: &str| -> serde_json::Value { serde_json::from_str(s).unwrap() };
    let request = |stream: &mut TcpStream, id: u32| {
        write_and_read(
            stream,
            &format!(
                "{{\"jsonrpc\": \"2.0\", \"method\": \"blockchain.block.header\", \"params\": [{}, {}], \"id\": {}}}",
                height, tip, id
            ),
        )
    };

    let v = parse(&request(&mut stream, 1));
    let old_root = sha256d::Hash::from_str(v["result"]["root"].as_str().expect("root")).unwrap();

    let old_tip_hash = tester.get_block_hash(tip as u64)?;
    let _: serde_json::Value = tester
        .node_client()
        .call("invalidateblock", &[json!(old_tip_hash.to_string())])?;
    tester.mine()?;
    tester.mine()?;
    assert_ne!(
        tester.get_block_hash(tip as u64)?,
        old_tip_hash,
        "the reorg must have replaced the block at the old tip height"
    );

    let v = parse(&request(&mut stream, 2));
    let result = &v["result"];
    let new_root = sha256d::Hash::from_str(result["root"].as_str().expect("root")).unwrap();
    assert_ne!(
        new_root, old_root,
        "a reorg past the checkpoint must invalidate the cached tree"
    );

    let leaves: Vec<sha256d::Hash> = (0..=tip)
        .map(|h| Ok(tester.get_block_hash(h as u64)?.to_raw_hash()))
        .collect::<Result<_>>()?;
    assert_eq!(
        new_root,
        checkpoint::merkle_root(leaves.clone()),
        "post-reorg root must match the new chain"
    );
    let branch: Vec<sha256d::Hash> = result["branch"]
        .as_array()
        .expect("branch array")
        .iter()
        .map(|h| sha256d::Hash::from_str(h.as_str().unwrap()).unwrap())
        .collect();
    assert_eq!(
        checkpoint::root_from_branch(leaves[height], &branch, height),
        new_root,
        "post-reorg branch must connect the leaf to the new root"
    );
    let header_bytes = Vec::<u8>::from_hex(result["header"].as_str().expect("header hex")).unwrap();
    assert_eq!(
        sha256d::Hash::hash(&header_bytes),
        leaves[height],
        "post-reorg header must belong to the same snapshot as the proof"
    );

    Ok(())
}

fn write_and_read(stream: &mut TcpStream, write: &str) -> String {
    stream.write_all(write.as_bytes()).unwrap();
    stream.write(b"\n").unwrap();
    stream.flush().unwrap();
    read_line(stream)
}

fn read_line(stream: &mut TcpStream) -> String {
    let mut result = vec![];
    loop {
        let mut buf = [0u8];
        stream.read_exact(&mut buf).unwrap();

        if buf[0] == b'\n' {
            break;
        } else {
            result.push(buf[0]);
        }
    }
    std::str::from_utf8(&result).unwrap().to_string()
}
