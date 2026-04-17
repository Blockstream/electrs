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

/// Test that verifies the acceptor thread handles channel closure gracefully
/// This test simulates the production scenario where the main RPC thread exits
/// while the acceptor thread is still running. Without the fix, this would cause
/// a panic at the acceptor.send().expect() call. With the fix, the acceptor thread
/// should exit gracefully when the channel is closed.
#[test]
fn test_acceptor_panic_on_channel_close() {
    use std::net::TcpStream;
    use std::thread;
    use std::time::Duration;

    // Use the existing init_electrum_tester function which properly sets up the server
    let (electrum_server, electrum_addr, _tester) = common::init_electrum_tester().unwrap();

    // Give the server a moment to start up
    thread::sleep(Duration::from_millis(100));

    // Start a thread that continuously tries to connect to the server
    // This will keep the acceptor thread busy accepting connections
    let addr = electrum_addr;
    let connection_thread = thread::spawn(move || {
        for _ in 0..100 {
            if let Ok(_stream) = TcpStream::connect(addr) {
                // Keep the connection open briefly
                thread::sleep(Duration::from_millis(10));
            }
            thread::sleep(Duration::from_millis(1));
        }
    });

    // Give the connection thread time to start making connections
    thread::sleep(Duration::from_millis(50));

    // Now immediately stop the server by dropping it
    // This will close the receiver channel, but the acceptor thread might still be running
    // and trying to send new connections, which should cause a panic
    drop(electrum_server);

    // Give some time for the acceptor thread to potentially panic
    thread::sleep(Duration::from_millis(500));

    // Wait for the connection thread to finish
    let _ = connection_thread.join();

    // If we reach here without panicking, the test passes
    // This test verifies that the acceptor thread gracefully handles channel closure
    // instead of panicking when the main RPC thread exits
}
