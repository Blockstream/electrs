//! Needs bitcoind 32.1+, skipped unless BITCOIND_EXE points at one:
//!
//!     BITCOIND_EXE=/path/to/bitcoind cargo test --test rpc_max_connections
#![cfg(not(feature = "liquid"))]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::Command;
use std::time::{Duration, Instant};

use base64::prelude::{Engine, BASE64_STANDARD};
use corepc_node::{self as noded, Node};
use electrs::errors::ErrorKind;
use electrs::metrics::Metrics;

pub mod common;

/// Below the D+2 connections a Daemon holds.
const RPC_MAX_CONNECTIONS: &str = "-rpcmaxconnections=4";
const MIN_VERSION: (u32, u32) = (32, 1);

fn bitcoind_version(exe: &str) -> Option<(u32, u32)> {
    // -version writes settings.json into the datadir.
    let datadir = tempfile::tempdir().unwrap();
    let out = Command::new(exe)
        .arg(format!("-datadir={}", datadir.path().display()))
        .arg("-version")
        .output()
        .ok()?;
    let text = String::from_utf8_lossy(&out.stdout);
    let version = text.split_whitespace().find_map(|w| {
        w.strip_prefix('v')
            .filter(|v| v.starts_with(|c: char| c.is_ascii_digit()))
    })?;
    let mut parts = version.split('.');
    Some((parts.next()?.parse().ok()?, parts.next()?.parse().ok()?))
}

fn start_capped_node() -> Option<Node> {
    let exe = match noded::exe_path() {
        Ok(exe) => exe,
        Err(e) => {
            eprintln!("skipping: no bitcoind executable found error='{}'", e);
            return None;
        }
    };
    match bitcoind_version(&exe) {
        Some(version) if version >= MIN_VERSION => {}
        version => {
            eprintln!(
                "skipping: needs bitcoind >= {:?} via BITCOIND_EXE, found exe='{}' version='{:?}'",
                MIN_VERSION, exe, version
            );
            return None;
        }
    }
    let mut conf = noded::Conf::default();
    conf.args.push(RPC_MAX_CONNECTIONS);
    // Keep idle held slots from being dropped mid-test.
    conf.args.push("-rpcservertimeout=600");
    let node = Node::with_conf(&exe, &conf).unwrap();
    // Daemon::new waits for regtest to leave initialblockdownload.
    let address = node.client.new_address().unwrap();
    node.client.generate_to_address(1, &address).unwrap();
    Some(node)
}

fn basic_auth(node: &Node) -> String {
    BASE64_STANDARD.encode(std::fs::read(&node.params.cookie_file).unwrap())
}

/// Returns None once the cap is full and the request is not answered.
fn take_slot(addr: SocketAddr, auth: &str, timeout: Duration) -> Option<TcpStream> {
    let mut socket = TcpStream::connect(addr).unwrap();
    socket.set_read_timeout(Some(timeout)).unwrap();
    let body = r#"{"method":"uptime","params":[],"id":1}"#;
    write!(
        socket,
        "POST / HTTP/1.1\r\nAuthorization: Basic {}\r\nContent-Length: {}\r\n\r\n{}",
        auth,
        body.len(),
        body
    )
    .unwrap();
    let mut reader = BufReader::new(socket.try_clone().unwrap());
    let mut length = 0;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).ok()? == 0 {
            return None;
        }
        if line.trim().is_empty() {
            break;
        }
        if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
            length = v.trim().parse().unwrap();
        }
    }
    let mut response = vec![0; length];
    reader.read_exact(&mut response).ok()?;
    Some(socket)
}

fn fill_remaining_slots(addr: SocketAddr, auth: &str) -> Vec<TcpStream> {
    let mut held = vec![];
    while let Some(socket) = take_slot(addr, auth, Duration::from_secs(2)) {
        held.push(socket);
        assert!(held.len() < 64, "the node does not seem to enforce the cap");
    }
    held
}

fn start_metrics() -> (Metrics, SocketAddr) {
    let addr = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let metrics = Metrics::new(addr);
    metrics.start();
    (metrics, addr)
}

fn handshake_count(metrics_addr: SocketAddr, result: &str) -> u64 {
    let body = ureq::get(&format!("http://{}/metrics", metrics_addr))
        .call()
        .expect("failed to scrape metrics")
        .into_body()
        .read_to_string()
        .unwrap();
    let prefix = format!("daemon_rpc_conn_handshake{{result=\"{}\"}} ", result);
    body.lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .map_or(0, |v| v.parse::<f64>().unwrap() as u64)
}

#[test]
fn client_rpc_fails_visibly_when_bitcoind_is_at_rpcmaxconnections() {
    let node = match start_capped_node() {
        Some(node) => node,
        None => return,
    };
    let rpc_addr: SocketAddr = node.params.rpc_socket.into();
    let mut daemon_dir = node.params.cookie_file.clone();
    daemon_dir.pop();
    let db = tempfile::tempdir().unwrap();
    let config = common::test_config(db.path().to_path_buf(), daemon_dir, rpc_addr);
    let (metrics, metrics_addr) = start_metrics();
    let daemon = common::test_daemon(&config, &metrics);

    let held = fill_remaining_slots(rpc_addr, &basic_auth(&node));
    let timeouts_before = handshake_count(metrics_addr, "timeout");

    let started = Instant::now();
    let err = daemon.broadcast_raw("00").unwrap_err();
    let elapsed = started.elapsed();

    match err.kind() {
        ErrorKind::DaemonUnavailable(msg) => assert!(
            msg.contains("-rpcmaxconnections"),
            "the error should point at the connection cap, got msg='{}'",
            msg
        ),
        other => panic!("expected DaemonUnavailable, got {:?}", other),
    }
    // The 10s handshake, not the 30s client RPC timeout.
    assert!(
        elapsed < Duration::from_secs(20),
        "broadcast should fail at the handshake deadline, took {:?}",
        elapsed
    );
    assert_eq!(
        handshake_count(metrics_addr, "timeout"),
        timeouts_before + 1
    );

    // With a free slot the junk hex reaches bitcoind and is rejected there.
    drop(held);
    let err = daemon.broadcast_raw("00").unwrap_err();
    assert!(
        matches!(err.kind(), ErrorKind::RpcError(..)),
        "expected bitcoind to reject the junk hex, got {:?}",
        err
    );
}

#[test]
fn fee_estimates_pin_the_block_policy_estimator_on_32_1() {
    // 31.x rejects the options argument, so Ok means the version gate matched.
    let node = match start_capped_node() {
        Some(node) => node,
        None => return,
    };
    let rpc_addr: SocketAddr = node.params.rpc_socket.into();
    let mut daemon_dir = node.params.cookie_file.clone();
    daemon_dir.pop();
    let db = tempfile::tempdir().unwrap();
    let config = common::test_config(db.path().to_path_buf(), daemon_dir, rpc_addr);
    let (metrics, _) = start_metrics();
    let daemon = common::test_daemon(&config, &metrics);

    let estimates = daemon.estimatesmartfee_batch(&[2, 6]).unwrap();
    assert!(
        estimates.is_empty(),
        "unexpected estimates: {:?}",
        estimates
    );
}
