pub mod common;
use std::io::{Read, Write};
use std::net::TcpStream;

use common::Result;

use electrumd::jsonrpc::serde_json::json;
use electrumd::ElectrumD;

use electrs::chain::Address;
use electrs::electrum::RPC as ElectrumRPC;

#[cfg(not(feature = "liquid"))]
use bitcoin::address;
#[cfg(feature = "liquid")]
use elementsd::bitcoincore_rpc::RpcApi;

struct WalletTester {
    electrum_server: ElectrumRPC,
    electrum_wallet: ElectrumD,
    tester: common::TestRunner,
}

impl WalletTester {
    fn new() -> Result<Self> {
        let (electrum_server, electrum_addr, tester) = common::init_electrum_tester().unwrap();

        let mut electrum_wallet_conf = electrumd::Conf::default();
        let server_arg = format!("{}:t", electrum_addr);
        electrum_wallet_conf.args = if std::env::var_os("RUST_LOG").is_some() {
            vec!["-v", "--server", &server_arg]
        } else {
            vec!["--server", &server_arg]
        };
        electrum_wallet_conf.view_stdout = true;
        let electrum_wallet =
            ElectrumD::with_conf(electrumd::exe_path()?, &electrum_wallet_conf)?;

        log::info!(
            "Electrum wallet version: {:?}",
            electrum_wallet.call("version", &json!([]))?
        );

        Ok(WalletTester {
            electrum_server,
            electrum_wallet,
            tester,
        })
    }

    fn notify_wallet(&self) {
        self.electrum_server.notify();
        std::thread::sleep(std::time::Duration::from_millis(200));
        self.electrum_wallet.call("wait_for_sync", &json!([])).unwrap();
    }

    fn assert_balance(&self, confirmed: f64, unconfirmed: f64) {
        let balance = self.electrum_wallet.call("getbalance", &json!([])).unwrap();
        log::info!("balance: {}", balance);

        assert_eq!(
            balance["confirmed"].as_str(),
            Some(confirmed.to_string().as_str())
        );
        if unconfirmed != 0.0 {
            assert_eq!(
                balance["unconfirmed"].as_str(),
                Some(unconfirmed.to_string().as_str())
            );
        } else {
            assert!(balance["unconfirmed"].is_null())
        }
    }

    fn newaddress(&self) -> Address {
        #[cfg(not(feature = "liquid"))]
        type ParseAddrType = Address<address::NetworkUnchecked>;
        #[cfg(feature = "liquid")]
        type ParseAddrType = Address;

        let addr = self
            .electrum_wallet
            .call("createnewaddress", &json!([]))
            .unwrap()
            .as_str()
            .expect("missing address")
            .parse::<ParseAddrType>()
            .expect("invalid address");

        #[cfg(not(feature = "liquid"))]
        let addr = addr.assume_checked();

        addr
    }
}

/// Test balance tracking with confirmed and unconfirmed transactions
#[cfg_attr(not(feature = "liquid"), test)]
#[cfg_attr(feature = "liquid", allow(dead_code))]
fn test_electrum_balance() -> Result<()> {
    let mut wt = WalletTester::new()?;

    let addr1 = wt.newaddress();
    let addr2 = wt.newaddress();

    wt.assert_balance(0.0, 0.0);

    wt.tester.send(&addr1, "0.1 BTC".parse().unwrap())?;
    wt.notify_wallet();
    wt.assert_balance(0.0, 0.1);

    wt.tester.mine()?;
    wt.notify_wallet();
    wt.assert_balance(0.1, 0.0);

    wt.tester.send(&addr2, "0.2 BTC".parse().unwrap())?;
    wt.notify_wallet();
    wt.assert_balance(0.1, 0.2);

    wt.tester.mine()?;
    wt.notify_wallet();
    wt.assert_balance(0.3, 0.0);

    Ok(())
}

/// Test transaction history via onchain_history
#[cfg_attr(not(feature = "liquid"), test)]
#[cfg_attr(feature = "liquid", allow(dead_code))]
fn test_electrum_history() -> Result<()> {
    let mut wt = WalletTester::new()?;

    let addr1 = wt.newaddress();
    let addr2 = wt.newaddress();

    let txid1 = wt.tester.send(&addr1, "0.1 BTC".parse().unwrap())?;
    wt.tester.mine()?;
    let txid2 = wt.tester.send(&addr2, "0.2 BTC".parse().unwrap())?;
    wt.tester.mine()?;
    wt.notify_wallet();

    let history = wt.electrum_wallet.call("onchain_history", &json!([]))?;
    log::debug!("history = {:#?}", history);
    assert_eq!(
        history[0]["txid"].as_str(),
        Some(txid1.to_string().as_str())
    );
    assert_eq!(history[0]["height"].as_u64(), Some(102));
    assert_eq!(history[0]["bc_value"].as_str(), Some("0.1"));

    assert_eq!(
        history[1]["txid"].as_str(),
        Some(txid2.to_string().as_str())
    );
    assert_eq!(history[1]["height"].as_u64(), Some(103));
    assert_eq!(history[1]["bc_value"].as_str(), Some("0.2"));

    Ok(())
}

/// Test sending an outgoing payment
#[cfg_attr(not(feature = "liquid"), test)]
#[cfg_attr(feature = "liquid", allow(dead_code))]
fn test_electrum_payment() -> Result<()> {
    let mut wt = WalletTester::new()?;

    let addr1 = wt.newaddress();
    wt.tester.send(&addr1, "0.3 BTC".parse().unwrap())?;
    wt.tester.mine()?;
    wt.notify_wallet();
    wt.assert_balance(0.3, 0.0);

    wt.electrum_wallet.call(
        "broadcast",
        &json!([wt.electrum_wallet.call(
            "payto",
            &json!({
                "destination": wt.tester.node_client().get_new_address(None, None)?,
                "amount": 0.16,
                "fee": 0.001,
            }),
        )?]),
    )?;
    wt.notify_wallet();
    wt.assert_balance(0.139, 0.0);

    wt.tester.mine()?;
    wt.notify_wallet();
    wt.assert_balance(0.139, 0.0);

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
    let v: electrumd::jsonrpc::serde_json::Value =
        electrumd::jsonrpc::serde_json::from_str(&s).unwrap();
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
    let v: electrumd::jsonrpc::serde_json::Value =
        electrumd::jsonrpc::serde_json::from_str(&s).unwrap();
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
    let v: electrumd::jsonrpc::serde_json::Value =
        electrumd::jsonrpc::serde_json::from_str(&s).unwrap();
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
        let v: electrumd::jsonrpc::serde_json::Value =
            electrumd::jsonrpc::serde_json::from_str(&s).unwrap();
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
    let v: electrumd::jsonrpc::serde_json::Value =
        electrumd::jsonrpc::serde_json::from_str(&s).unwrap();
    assert_eq!(v["result"]["success"].as_bool(), Some(true), "response: {}", s);

    // the accepted tx must now be visible in the scripthash history immediately
    assert_eq!(
        history(&mut stream, 3),
        vec![txid.to_string()],
        "broadcast_package did not add the accepted tx to electrs' local mempool"
    );

    Ok(())
}

fn write_and_read(stream: &mut TcpStream, write: &str) -> String {
    stream.write_all(write.as_bytes()).unwrap();
    stream.write(b"\n").unwrap();
    stream.flush().unwrap();
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
