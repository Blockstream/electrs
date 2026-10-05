use std::net;
use std::sync::{Arc, Barrier};
use std::thread;

pub mod common;

use common::Result;

const CONCURRENT_CALLERS: usize = 10;

fn fetch_metrics(addr: net::SocketAddr) -> String {
    ureq::get(&format!("http://{}/metrics", addr))
        .call()
        .expect("failed to scrape metrics")
        .into_body()
        .read_to_string()
        .expect("failed to read metrics body")
}

fn metric_value(body: &str, metric: &str, label_match: &str) -> u64 {
    let prefix = format!("{}{{{}}} ", metric, label_match);
    body.lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .and_then(|rest| rest.trim().parse::<f64>().ok())
        .map(|value| value as u64)
        .unwrap_or(0)
}

fn daemon_rpc_count(metrics_addr: net::SocketAddr, method: &str) -> u64 {
    metric_value(
        &fetch_metrics(metrics_addr),
        "daemon_rpc_count",
        &format!(r#"method="{}""#, method),
    )
}

#[test]
fn test_estimate_fee_refresh_is_single_flight() -> Result<()> {
    let tester = common::TestRunner::new()?;
    let query = tester.query();
    let metrics_addr = tester.metrics_addr();

    let before = daemon_rpc_count(metrics_addr, "estimatesmartfee");

    let barrier = Arc::new(Barrier::new(CONCURRENT_CALLERS));
    let handles: Vec<_> = (0..CONCURRENT_CALLERS)
        .map(|_| {
            let query = query.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                query.estimate_fee_map()
            })
        })
        .collect();
    for handle in handles {
        handle.join().expect("estimate_fee_map panicked");
    }

    let after = daemon_rpc_count(metrics_addr, "estimatesmartfee");

    assert_eq!(
        after - before,
        28,
        "expected exactly one fee-estimate refresh (28 RPCs) from {} concurrent callers, got {}",
        CONCURRENT_CALLERS,
        after - before
    );
    Ok(())
}

#[test]
fn test_get_relayfee_refresh_is_single_flight() -> Result<()> {
    let tester = common::TestRunner::new()?;
    let query = tester.query();
    let metrics_addr = tester.metrics_addr();

    let before = daemon_rpc_count(metrics_addr, "getnetworkinfo");

    let barrier = Arc::new(Barrier::new(CONCURRENT_CALLERS));
    let handles: Vec<_> = (0..CONCURRENT_CALLERS)
        .map(|_| {
            let query = query.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                query.get_relayfee()
            })
        })
        .collect();
    for handle in handles {
        handle.join().expect("get_relayfee panicked")?;
    }

    let after = daemon_rpc_count(metrics_addr, "getnetworkinfo");

    assert_eq!(
        after - before,
        1,
        "expected exactly one getnetworkinfo call from {} concurrent get_relayfee() callers, got {}",
        CONCURRENT_CALLERS,
        after - before
    );
    Ok(())
}
