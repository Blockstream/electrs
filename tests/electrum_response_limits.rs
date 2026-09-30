#[allow(dead_code)]
mod common;

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

struct Client(BufReader<TcpStream>);

impl Client {
    fn connect(addr: SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(15)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(15)))
            .unwrap();
        Self(BufReader::new(stream))
    }

    fn send(&mut self, request: &Value) {
        serde_json::to_writer(self.0.get_mut(), request).unwrap();
        self.0.get_mut().write_all(b"\n").unwrap();
    }

    fn receive(&mut self) -> (Value, usize) {
        let mut line = String::new();
        assert!(
            self.0.read_line(&mut line).unwrap() > 0,
            "connection closed"
        );
        assert!(line.ends_with('\n'));
        (serde_json::from_str(&line).unwrap(), line.len())
    }

    fn call(&mut self, request: Value) -> (Value, usize) {
        self.send(&request);
        self.receive()
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.0.get_ref().shutdown(Shutdown::Both);
    }
}

#[test]
fn unaligned_budget_admits_replies_through_the_exact_line_limit() {
    let cap = 100_000;
    let banner = "a".repeat(90_000);
    let (_server, addr, _tester) = common::init_electrum_tester_with_config(|config| {
        config.electrum_rpc_max_response_num_bytes = cap;
        config.electrum_rpc_global_response_budget_bytes = cap;
        config.electrum_banner = banner.clone();
    })
    .unwrap();
    let mut client = Client::connect(addr);
    let (reply, _) = client.call(json!({"id":1,"method":"server.banner"}));
    assert_eq!(reply["result"], banner);

    let base = serde_json::to_vec(&json!({"id":"","jsonrpc":"2.0","result":banner}))
        .unwrap()
        .len()
        + 1;
    let id = "b".repeat(cap - base);
    let (reply, size) = client.call(json!({"id":id,"method":"server.banner"}));
    assert_eq!(size, cap);
    assert_eq!(reply["id"], id);
    assert_eq!(reply["result"], banner);

    let id = format!("{}c", id);
    let (reply, size) = client.call(json!({"id":id,"method":"server.banner"}));
    assert!(size <= cap);
    assert_eq!(reply["id"], id);
    assert_eq!(reply["error"]["code"], 1);

    client.0.get_mut().write_all(b"{\n").unwrap();
    let (reply, size) = client.receive();
    assert!(size <= cap);
    assert_eq!(reply["id"], Value::Null);
    assert_eq!(reply["error"]["code"], -32700);
    let (reply, _) = client.call(json!({"id":2,"method":false}));
    assert_eq!(reply["id"], 2);
    assert_eq!(reply["error"]["code"], -32600);
    let (reply, _) = client.call(json!({"id":3,"method":"server.ping"}));
    assert_eq!(reply["result"], Value::Null);
    assert!(reply.get("error").is_none());
}

#[test]
fn response_line_limits_preserve_ids_and_skip_later_commands() {
    let cap = 100_000;
    let (_server, addr, _tester) = common::init_electrum_tester_with_config(|config| {
        config.electrum_rpc_max_response_num_bytes = cap;
        config.electrum_rpc_global_response_budget_bytes = usize::MAX;
        config.electrum_banner = "a".repeat(80_000);
    })
    .unwrap();
    let mut client = Client::connect(addr);
    let (single, len) = client.call(json!({"id":1,"method":"server.banner"}));
    assert_eq!(single["result"].as_str().unwrap().len(), 80_000);
    assert!(len <= cap);

    let hash = "00".repeat(32);
    let (batch, len) = client.call(json!([
        {"id":1,"method":"server.banner"},
        {"id":2,"method":"server.banner"},
        {"id":3,"method":"blockchain.scripthash.subscribe","params":[hash]}
    ]));
    assert!(len <= cap);
    assert_eq!(batch[0]["result"], single["result"]);
    assert_eq!(batch[1]["id"], 2);
    assert_eq!(batch[1]["error"]["code"], 1);
    assert_eq!(batch[2]["id"], 3);
    assert_eq!(batch[2]["error"]["code"], 1);
    let (unsubscribed, _) =
        client.call(json!({"id":4,"method":"blockchain.scripthash.unsubscribe","params":[hash]}));
    assert_eq!(unsubscribed["result"], false);
    let (empty, len) = client.call(json!([]));
    assert_eq!(empty, json!([]));
    assert_eq!(len, 3);

    let id = "b".repeat(30_000);
    let (overflow, len) = client.call(json!({"id":id,"method":"server.banner"}));
    assert_eq!(overflow["id"], id);
    assert_eq!(overflow["error"]["code"], 1);
    assert!(len <= cap);
}

#[test]
fn response_budget_recovers_when_stalled_batch_writers_time_out() {
    let cap = 16 * 1024 * 1024;
    let banner_bytes = 4 * 1024 * 1024 - 4096;
    let (_server, addr, tester) = common::init_electrum_tester_with_config(|config| {
        config.electrum_rpc_max_response_num_bytes = cap;
        config.electrum_rpc_global_response_budget_bytes = cap;
        config.electrum_rpc_write_timeout = Some(Duration::from_secs(5));
        config.electrum_rpc_conn_max_age = None;
        config.electrum_banner = "a".repeat(banner_bytes);
    })
    .unwrap();

    let mut holders = Vec::new();
    for _ in 0..2 {
        // The 8 MiB reply fills the default TCP buffers.
        let mut client = Client::connect(addr);
        client.send(&json!([
            {"id":1,"method":"server.banner"},
            {"id":2,"method":"server.banner"}
        ]));
        let mut first = [0];
        client.0.read_exact(&mut first).unwrap();
        assert_eq!(first, [b'[']);
        holders.push(client);
    }

    let mut healthy = Client::connect(addr);
    let request = json!({"id":"x".repeat(20 * 1024),"method":"server.ping"});
    let (rejected, _) = healthy.call(request.clone());
    assert_eq!(rejected["error"]["code"], 2);

    // Keep both clients open and unread. Recovery must come from the server's
    // write deadline, with connection-age expiry explicitly disabled.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (reply, _) = healthy.call(request.clone());
        if reply.get("result").is_some() {
            assert_eq!(reply["result"], Value::Null);
            break;
        }
        assert_eq!(reply["error"]["code"], 2);
        assert!(
            Instant::now() < deadline,
            "stalled writers did not release the budget"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let (reply, _) = healthy.call(json!({"id":3,"method":"server.banner"}));
    assert_eq!(reply["result"].as_str().unwrap().len(), banner_bytes);
    assert!(
        tester.counter_value("electrum_client_write_timeouts_total") >= 1.0,
        "budget recovered without a stalled writer timing out"
    );
}

#[test]
fn normal_close_delivers_final_response_before_eof() {
    let banner = "a".repeat(256 * 1024);
    let (_server, addr, _tester) = common::init_electrum_tester_with_config(|config| {
        config.electrum_banner = banner.clone();
        config.electrum_rpc_write_timeout = Some(Duration::from_secs(5));
    })
    .unwrap();
    let mut client = Client::connect(addr);
    client.send(&json!({"id":1,"method":"server.banner"}));
    // EOF on the request side makes the server close after its final reply.
    client.0.get_ref().shutdown(Shutdown::Write).unwrap();
    let (reply, _) = client.receive();
    assert_eq!(reply["result"], banner);
    assert_eq!(client.0.read(&mut [0]).unwrap(), 0);
}

#[test]
fn response_budget_streams_large_ids_and_recovers_after_blocked_writers() {
    let cap = 16 * 1024 * 1024;
    let banner_bytes = 8 * 1024 * 1024 - 8192;
    let (_server, addr, _tester) = common::init_electrum_tester_with_config(|config| {
        config.electrum_rpc_max_response_num_bytes = cap;
        config.electrum_rpc_global_response_budget_bytes = cap;
        config.electrum_banner = "a".repeat(banner_bytes);
    })
    .unwrap();

    let mut holders = Vec::new();
    for id in 0..2 {
        let mut client = Client::connect(addr);
        socket2::SockRef::from(client.0.get_ref())
            .set_recv_buffer_size(4096)
            .unwrap();
        client.send(&json!({"id":id,"method":"server.banner"}));
        // Receiving the start proves assembly finished and its reservation is
        // held by write_all. The tiny receive window cannot absorb 8 MiB.
        let mut first = [0];
        client.0.read_exact(&mut first).unwrap();
        assert_eq!(first, [b'{']);
        holders.push(client);
    }

    let mut client = Client::connect(addr);
    let id = "x".repeat(20 * 1024);
    let hash = "00".repeat(32);
    let (error, len) =
        client.call(json!({"id":id,"method":"blockchain.scripthash.subscribe","params":[hash]}));
    assert_eq!(error["id"], id);
    assert_eq!(error["error"]["code"], 2);
    assert!(len <= cap);
    let (unsubscribed, _) =
        client.call(json!({"id":4,"method":"blockchain.scripthash.unsubscribe","params":[hash]}));
    assert_eq!(unsubscribed["result"], false);

    let (batch, len) = client.call(json!([
        {"id":id,"method":"server.ping"},
        {"id":5,"method":"blockchain.scripthash.subscribe","params":[hash]}
    ]));
    assert_eq!(batch[0]["id"], id);
    assert_eq!(batch[0]["error"]["code"], 2);
    assert_eq!(batch[1]["id"], 5);
    assert_eq!(batch[1]["error"]["code"], 2);
    assert!(len <= cap);
    let (unsubscribed, _) =
        client.call(json!({"id":6,"method":"blockchain.scripthash.unsubscribe","params":[hash]}));
    assert_eq!(unsubscribed["result"], false);

    // Free one holder, leaving 8 MiB available. A banner fits, and the
    // reserved error for the next element must fit without another grant.
    drop(holders.pop());
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (reply, _) = client.call(json!({"id":id,"method":"server.ping"}));
        assert_eq!(reply["id"], id);
        if reply.get("result").is_some() {
            assert_eq!(reply["result"], Value::Null);
            break;
        }
        assert!(
            Instant::now() < deadline,
            "response budget was not released"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let second_id = "y".repeat(14_000);
    let (batch, len) = client.call(json!([
        {"id":7,"method":"server.banner"},
        {"id":second_id,"method":"server.banner"},
        {"id":8,"method":"blockchain.scripthash.subscribe","params":[hash]}
    ]));
    assert_eq!(batch[0]["result"].as_str().unwrap().len(), banner_bytes);
    assert_eq!(batch[1]["id"], second_id);
    assert_eq!(batch[1]["error"]["code"], 2);
    assert_eq!(batch[2]["id"], 8);
    assert_eq!(batch[2]["error"]["code"], 2);
    assert!(len <= cap);
    let (unsubscribed, _) =
        client.call(json!({"id":9,"method":"blockchain.scripthash.unsubscribe","params":[hash]}));
    assert_eq!(unsubscribed["result"], false);
    drop(holders);
}
