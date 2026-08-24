use std::cell::OnceCell;
use std::collections::{HashMap, HashSet};
use std::env;
use std::io::{BufRead, BufReader, Lines, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use base64::prelude::{Engine, BASE64_STANDARD};
#[cfg(feature = "liquid")]
use bitcoin::hex::FromHex;
use error_chain::ChainedError;
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};
use serde_json::{from_str, from_value, Value};

#[cfg(not(feature = "liquid"))]
use bitcoin::consensus::{
    encode::{deserialize_hex, serialize_hex, VarInt},
    Decodable,
};
#[cfg(feature = "liquid")]
use elements::encode::{deserialize, serialize_hex, Decodable};
use rayon::iter::IntoParallelRefIterator;

use electrs_macros::trace;

use crate::chain::{Block, BlockHash, BlockHeader, Network, Transaction, Txid};
use crate::metrics::{CounterVec, HistogramOpts, HistogramVec, MetricOpts, Metrics};
use crate::signal::Waiter;
use crate::util::{HeaderList, DEFAULT_BLOCKHASH};

use crate::errors::*;

lazy_static! {
    static ref DAEMON_CONNECTION_TIMEOUT: Duration = Duration::from_secs(
        env::var("DAEMON_CONNECTION_TIMEOUT").map_or(10, |s| s.parse().unwrap())
    );
    static ref DAEMON_READ_TIMEOUT: Duration = Duration::from_secs(
        env::var("DAEMON_READ_TIMEOUT").map_or(10 * 60, |s| s.parse().unwrap())
    );
    static ref DAEMON_WRITE_TIMEOUT: Duration = Duration::from_secs(
        env::var("DAEMON_WRITE_TIMEOUT").map_or(10 * 60, |s| s.parse().unwrap())
    );
    // Minimum delay between *failed* proactive max-age recycle attempts, so that a sustained
    // inability to open new connections doesn't make every request pay a connect timeout.
    static ref DAEMON_CONN_RECYCLE_COOLDOWN: Duration = Duration::from_secs(
        env::var("DAEMON_CONN_RECYCLE_COOLDOWN").map_or(30, |s| s.parse().unwrap())
    );
    // Maximum number of daemon RPCs made on behalf of API clients (transaction broadcast
    // and package submission) that may be in flight at once. These endpoints are reachable
    // anonymously, so this is what bounds how many threads a slow or wedged daemon can
    // park. Kept well below bitcoind's own rpcthreads/rpcworkqueue so client traffic
    // cannot starve the indexer of daemon capacity.
    static ref DAEMON_PROXY_MAX_CONCURRENCY: usize =
        env::var("DAEMON_PROXY_MAX_CONCURRENCY").map_or(8, |s| s.parse().unwrap());
    // Read/write timeout for client-proxied RPCs. Deliberately far shorter than
    // DAEMON_READ_TIMEOUT: an API client must get an answer (or a 504) in bounded time,
    // whereas the indexer can afford to wait out a long-running daemon call.
    static ref DAEMON_PROXY_RPC_TIMEOUT: Duration = Duration::from_secs(
        env::var("DAEMON_PROXY_RPC_TIMEOUT").map_or(30, |s| s.parse().unwrap())
    );
    // How long a client-proxied RPC waits for a free slot before giving up with
    // `DaemonBusy`, rather than queueing behind an unbounded backlog.
    static ref DAEMON_PROXY_QUEUE_TIMEOUT: Duration = Duration::from_secs(
        env::var("DAEMON_PROXY_QUEUE_TIMEOUT").map_or(5, |s| s.parse().unwrap())
    );
}

const MAX_ATTEMPTS: u32 = 5;
const RETRY_WAIT_DURATION: Duration = Duration::from_secs(1);
const BLOCK_TEMPLATE_RPC_TIMEOUT: Duration = Duration::from_secs(30);

#[trace]
fn parse_hash<T>(value: &Value) -> Result<T>
where
    T: FromStr,
    T::Err: 'static + std::error::Error + Send,
{
    Ok(T::from_str(
        value
            .as_str()
            .chain_err(|| format!("non-string value: {}", value))?,
    )
    .chain_err(|| format!("non-hex value: {}", value))?)
}

#[trace]
fn header_from_value(value: Value) -> Result<BlockHeader> {
    let header_hex = value
        .as_str()
        .chain_err(|| format!("non-string header: {}", value))?;
    deserialize_value(header_hex)
}

fn tx_from_value(value: Value) -> Result<Transaction> {
    let tx_hex = value.as_str().chain_err(|| "non-string tx")?;
    deserialize_value(tx_hex)
}

#[cfg(not(feature = "liquid"))]
fn deserialize_value<T: bitcoin::consensus::Decodable>(hex: &str) -> Result<T> {
    Ok(deserialize_hex(hex)
        .chain_err(|| format!("failed to deserialize {}", std::any::type_name::<T>()))?)
}

#[cfg(feature = "liquid")]
fn deserialize_value<T: elements::encode::Decodable>(hex: &str) -> Result<T> {
    let bytes = Vec::from_hex(hex).chain_err(|| "invalid hex")?;
    Ok(deserialize(&bytes)
        .chain_err(|| format!("failed to deserialize {}", std::any::type_name::<T>()))?)
}

/// Parse JSONRPC error code, if exists.
fn parse_error_code(err: &Value) -> Option<i64> {
    err.as_object()?.get("code")?.as_i64()
}

fn parse_jsonrpc_reply(mut reply: Value, method: &str, expected_id: u64) -> Result<Value> {
    if let Some(reply_obj) = reply.as_object_mut() {
        if let Some(err) = reply_obj.get_mut("error") {
            if !err.is_null() {
                if let Some(code) = parse_error_code(&err) {
                    let msg = err["message"]
                        .as_str()
                        .map_or_else(|| err.to_string(), |s| s.to_string());
                    bail!(ErrorKind::RpcError(code, msg, method.to_string()))
                }
            }
        }
        let id = reply_obj
            .get("id")
            .chain_err(|| format!("no id in reply: {:?}", reply_obj))?
            .clone();
        if id != expected_id {
            bail!(
                "wrong {} response id {}, expected {}",
                method,
                id,
                expected_id
            );
        }
        if let Some(result) = reply_obj.get_mut("result") {
            return Ok(result.take());
        }
        bail!("no result in reply: {:?}", reply_obj);
    }
    bail!("non-object reply: {:?}", reply);
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BlockchainInfo {
    pub chain: String,
    pub blocks: u32,
    pub headers: u32,
    pub bestblockhash: String,
    pub pruned: bool,
    pub verificationprogress: f32,
    pub initialblockdownload: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug)]
struct NetworkInfo {
    version: u64,
    subversion: String,
    relayfee: f64, // in BTC/kB
}

#[derive(Serialize, Deserialize, Debug)]
struct MempoolFeesSubmitPackage {
    base: f64,
    #[serde(rename = "effective-feerate")]
    effective_feerate: Option<f64>,
    #[serde(rename = "effective-includes")]
    effective_includes: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SubmitPackageResult {
    package_msg: String,
    #[serde(rename = "tx-results")]
    tx_results: HashMap<String, TxResult>,
    #[serde(rename = "replaced-transactions")]
    replaced_transactions: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TxResult {
    txid: String,
    #[serde(rename = "other-wtxid")]
    other_wtxid: Option<String>,
    vsize: Option<u32>,
    fees: Option<MempoolFeesSubmitPackage>,
    error: Option<String>,
}

impl SubmitPackageResult {
    /// Txids of the transactions accepted into the daemon's mempool by this package submission
    /// (those without a per-transaction error).
    pub fn accepted_txids(&self) -> Vec<Txid> {
        self.tx_results
            .values()
            .filter(|tx| tx.error.is_none())
            .filter_map(|tx| Txid::from_str(&tx.txid).ok())
            .collect()
    }

    /// Build the response for the Electrum `blockchain.transaction.broadcast_package` method.
    ///
    /// When `verbose` is true, the full `submitpackage` result is returned. Otherwise a compact
    /// `{ success, errors? }` object is returned, where `success` is whether the package was
    /// accepted and `errors` lists any per-transaction errors.
    ///
    /// Ported from romanz/electrs (https://github.com/romanz/electrs).
    pub fn into_electrum_response(self, verbose: bool) -> Value {
        if verbose {
            return json!(self);
        }
        let success = self.package_msg == "success";
        let errors: Vec<Value> = self
            .tx_results
            .values()
            .filter_map(|tx| {
                tx.error
                    .as_ref()
                    .map(|error| json!({ "error": error, "txid": tx.txid }))
            })
            .collect();
        if errors.is_empty() {
            json!({ "success": success })
        } else {
            json!({ "success": success, "errors": errors })
        }
    }
}

pub trait CookieGetter: Send + Sync {
    fn get(&self) -> Result<Vec<u8>>;
}

#[derive(Clone)]
struct ConnectionConfig {
    addr: SocketAddr,
    fallback: Option<SocketAddr>,
    cookie_getter: Arc<dyn CookieGetter>,
    signal: Waiter,
    max_age: Option<Duration>,
}

impl ConnectionConfig {
    fn connect(&self) -> Result<Connection> {
        Connection::new(
            self.addr,
            self.fallback,
            Arc::clone(&self.cookie_getter),
            self.signal.clone(),
            self.max_age,
        )
    }

    fn connect_once(&self, io_timeout: Duration) -> Result<Connection> {
        let (conn, active_addr) = tcp_connect_once(self.addr, self.fallback)?;
        conn.set_read_timeout(Some(io_timeout))
            .chain_err(|| "failed to configure one-shot daemon read timeout")?;
        conn.set_write_timeout(Some(io_timeout))
            .chain_err(|| "failed to configure one-shot daemon write timeout")?;
        Connection::from_stream(
            conn,
            active_addr,
            self.addr,
            self.fallback,
            Arc::clone(&self.cookie_getter),
            self.signal.clone(),
            None, // a one-shot connection never needs proactive recycling
        )
    }
}

struct Connection {
    tx: TcpStream,
    rx: Lines<BufReader<TcpStream>>,
    cookie_getter: Arc<dyn CookieGetter>,
    addr: SocketAddr,
    fallback: Option<SocketAddr>,
    // The address this connection is actually established to: either `addr` (primary)
    // or `fallback`. Used for accurate operational logging.
    active_addr: SocketAddr,
    signal: Waiter,
    // When the TCP connection was (re)established, used together with `max_age` to
    // proactively recycle long-lived connections (see `should_recycle`).
    established: Instant,
    // Maximum age of a connection before it is proactively recycled, or None for unlimited.
    max_age: Option<Duration>,
    // When the last *failed* proactive recycle attempt happened, used to rate-limit retries
    // (see `DAEMON_CONN_RECYCLE_COOLDOWN`). None until a recycle attempt fails.
    last_recycle_attempt: Option<Instant>,
}

fn configure_stream(conn: &TcpStream) {
    // can only fail if DAEMON_TIMEOUT is 0
    conn.set_read_timeout(Some(*DAEMON_READ_TIMEOUT)).unwrap();
    conn.set_write_timeout(Some(*DAEMON_WRITE_TIMEOUT)).unwrap();
}

/// Attempt a single connection to the primary address, falling back to the fallback
/// address once. Returns an error if neither is reachable. Does not retry or back off,
/// so callers that must stay available (e.g. proactive max-age recycling) can give up
/// and keep using their existing connection.
#[trace]
fn tcp_connect_once(
    primary: SocketAddr,
    fallback: Option<SocketAddr>,
) -> Result<(TcpStream, SocketAddr)> {
    let primary_err = match TcpStream::connect_timeout(&primary, *DAEMON_CONNECTION_TIMEOUT) {
        Ok(conn) => {
            configure_stream(&conn);
            return Ok((conn, primary));
        }
        Err(err) => err,
    };
    // Return a single descriptive error and let the caller decide how to log it, rather than
    // warning per-attempt here (which would double-log on the best-effort recycle path).
    match fallback {
        Some(fallback_addr) => {
            debug!(
                "primary daemon at {} unreachable ({}), trying fallback {}",
                primary, primary_err, fallback_addr
            );
            match TcpStream::connect_timeout(&fallback_addr, *DAEMON_CONNECTION_TIMEOUT) {
                Ok(conn) => {
                    info!("connected to fallback daemon at {}", fallback_addr);
                    configure_stream(&conn);
                    Ok((conn, fallback_addr))
                }
                Err(fallback_err) => bail!(ErrorKind::Connection(format!(
                    "failed to connect to primary daemon at {} ({}) and fallback at {} ({})",
                    primary, primary_err, fallback_addr, fallback_err
                ))),
            }
        }
        None => bail!(ErrorKind::Connection(format!(
            "failed to connect to daemon at {}: {}",
            primary, primary_err
        ))),
    }
}

/// Connect to the daemon, retrying indefinitely (with backoff) until a connection
/// succeeds. Used for startup and for reconnecting after a real send/recv failure,
/// where there is no usable connection to fall back to.
#[trace]
fn tcp_connect(
    primary: SocketAddr,
    fallback: Option<SocketAddr>,
    signal: &Waiter,
) -> Result<(TcpStream, SocketAddr)> {
    loop {
        match tcp_connect_once(primary, fallback) {
            Ok(res) => return Ok(res),
            Err(err) => {
                warn!(
                    "{}; backoff 3 seconds before next attempt",
                    err.display_chain()
                );
                signal.wait(Duration::from_secs(3), false)?;
                continue;
            }
        }
    }
}

/// Decide whether an expired connection is due for a (re)attempt at proactive recycling.
/// Returns false when no max age is configured, when the connection is younger than the max
/// age, or when a previous recycle attempt failed less than `cooldown` ago (to avoid paying a
/// connect timeout on every request during a sustained connect failure). Pure for testability.
fn recycle_due(
    age: Duration,
    max_age: Option<Duration>,
    since_last_attempt: Option<Duration>,
    cooldown: Duration,
) -> bool {
    match max_age {
        None => false,
        Some(max_age) => {
            age >= max_age && since_last_attempt.map_or(true, |since| since >= cooldown)
        }
    }
}

impl Connection {
    /// Return the active address and connection timestamp, used to identify this connection.
    fn current(&self) -> (SocketAddr, Instant) {
        (self.active_addr, self.established)
    }

    #[trace]
    fn new(
        addr: SocketAddr,
        fallback: Option<SocketAddr>,
        cookie_getter: Arc<dyn CookieGetter>,
        signal: Waiter,
        max_age: Option<Duration>,
    ) -> Result<Connection> {
        let (conn, active_addr) = tcp_connect(addr, fallback, &signal)?;
        Connection::from_stream(conn, active_addr, addr, fallback, cookie_getter, signal, max_age)
    }

    /// Build a `Connection` wrapper around an already-established TCP stream.
    fn from_stream(
        conn: TcpStream,
        active_addr: SocketAddr,
        addr: SocketAddr,
        fallback: Option<SocketAddr>,
        cookie_getter: Arc<dyn CookieGetter>,
        signal: Waiter,
        max_age: Option<Duration>,
    ) -> Result<Connection> {
        debug!("connected to bitcoind at {}", active_addr);
        let reader = BufReader::new(
            conn.try_clone()
                .chain_err(|| format!("failed to clone {:?}", conn))?,
        );
        Ok(Connection {
            tx: conn,
            rx: reader.lines(),
            cookie_getter,
            addr,
            fallback,
            active_addr,
            signal,
            established: Instant::now(),
            max_age,
            last_recycle_attempt: None,
        })
    }

    #[trace]
    fn reconnect(&self) -> Result<Connection> {
        Connection::new(
            self.addr,
            self.fallback,
            self.cookie_getter.clone(),
            self.signal.clone(),
            self.max_age,
        )
    }

    /// Attempt a single reconnect for proactive max-age recycling. Unlike `reconnect`,
    /// this makes one bounded attempt (primary then fallback) and returns an error
    /// instead of looping, so the caller can keep using the existing healthy connection
    /// if no fresh socket is available.
    #[trace]
    fn try_reconnect_once(&self) -> Result<Connection> {
        let (conn, active_addr) = tcp_connect_once(self.addr, self.fallback)?;
        Connection::from_stream(
            conn,
            active_addr,
            self.addr,
            self.fallback,
            self.cookie_getter.clone(),
            self.signal.clone(),
            self.max_age,
        )
    }

    /// Whether this connection is due to be proactively recycled now: it has exceeded its
    /// configured `max_age` and no recent recycle attempt has failed within the cooldown.
    /// Always false when no max age is configured (unlimited).
    fn should_recycle(&self) -> bool {
        recycle_due(
            self.established.elapsed(),
            self.max_age,
            self.last_recycle_attempt.map(|at| at.elapsed()),
            *DAEMON_CONN_RECYCLE_COOLDOWN,
        )
    }

    #[trace]
    fn send(&mut self, request: &str) -> Result<()> {
        let cookie = &self.cookie_getter.get()?;
        let msg = format!(
            "POST / HTTP/1.1\nAuthorization: Basic {}\nContent-Length: {}\n\n{}",
            BASE64_STANDARD.encode(cookie),
            request.len(),
            request,
        );
        self.tx.write_all(msg.as_bytes()).chain_err(|| {
            ErrorKind::Connection("disconnected from daemon while sending".to_owned())
        })
    }

    #[trace]
    fn recv(&mut self) -> Result<String> {
        // TODO: use proper HTTP parser.
        let mut in_header = true;
        let mut contents: Option<String> = None;
        let iter = self.rx.by_ref();
        let status = iter
            .next()
            .chain_err(|| {
                ErrorKind::Connection("disconnected from daemon while receiving".to_owned())
            })?
            .chain_err(|| ErrorKind::Connection("failed to read status".to_owned()))?;
        let mut headers = HashMap::new();
        for line in iter {
            let line = line.chain_err(|| ErrorKind::Connection("failed to read".to_owned()))?;
            if line.is_empty() {
                in_header = false; // next line should contain the actual response.
            } else if in_header {
                let parts: Vec<&str> = line.splitn(2, ": ").collect();
                if parts.len() == 2 {
                    headers.insert(parts[0].to_lowercase(), parts[1].to_owned());
                } else {
                    warn!("invalid header: {:?}", line);
                }
            } else {
                contents = Some(line);
                break;
            }
        }

        let contents =
            contents.chain_err(|| ErrorKind::Connection("no reply from daemon".to_owned()))?;
        let contents_length: &str = headers
            .get("content-length")
            .chain_err(|| format!("Content-Length is missing: {:?}", headers))?;
        let contents_length: usize = contents_length
            .parse()
            .chain_err(|| format!("invalid Content-Length: {:?}", contents_length))?;

        let expected_length = contents_length - 1; // trailing EOL is skipped
        if expected_length != contents.len() {
            bail!(ErrorKind::Connection(format!(
                "expected {} bytes, got {}",
                expected_length,
                contents.len()
            )));
        }

        Ok(if status == "HTTP/1.1 200 OK" {
            contents
        } else if status == "HTTP/1.1 500 Internal Server Error" {
            debug!("RPC HTTP 500 error: {}", contents);
            contents // the contents should have a JSONRPC error field
        } else {
            bail!(
                "request failed {:?}: {:?} = {:?}",
                status,
                headers,
                contents
            );
        })
    }
}

/// A counting semaphore for blocking (non-async) callers, used to cap how many daemon RPCs
/// may be in flight on behalf of API clients at any one time.
///
/// Client-triggered RPCs are anonymous and unmetered, so without a cap a caller can open as
/// many concurrent daemon calls as it can open sockets. Each of those calls occupies a
/// thread for as long as the daemon takes to answer, which is what turns a slow daemon into
/// a full API outage. Bounding them means a slow daemon degrades the endpoints that need it
/// and leaves every other endpoint untouched.
struct BlockingSemaphore {
    /// Number of permits still available.
    available: Mutex<usize>,
    released: Condvar,
    capacity: usize,
}

impl BlockingSemaphore {
    fn new(capacity: usize) -> Self {
        // A zero capacity would deadlock every caller, so treat it as "one at a time".
        let capacity = capacity.max(1);
        BlockingSemaphore {
            available: Mutex::new(capacity),
            released: Condvar::new(),
            capacity,
        }
    }

    /// Take a permit, waiting at most `wait_timeout` for one to be released. Returns `None`
    /// if none became available in time, so the caller can fail fast instead of queueing
    /// behind an unbounded backlog of requests to an unresponsive daemon.
    fn acquire(&self, wait_timeout: Duration) -> Option<SemaphorePermit<'_>> {
        let deadline = Instant::now() + wait_timeout;
        let mut available = self.available.lock().unwrap();
        loop {
            if *available > 0 {
                *available -= 1;
                return Some(SemaphorePermit { semaphore: self });
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return None;
            }
            available = self.released.wait_timeout(available, remaining).unwrap().0;
        }
    }

    fn capacity(&self) -> usize {
        self.capacity
    }
}

/// Returns its permit to the [`BlockingSemaphore`] on drop.
struct SemaphorePermit<'a> {
    semaphore: &'a BlockingSemaphore,
}

impl Drop for SemaphorePermit<'_> {
    fn drop(&mut self) {
        *self.semaphore.available.lock().unwrap() += 1;
        self.semaphore.released.notify_one();
    }
}

struct Counter {
    value: Mutex<u64>,
}

impl Counter {
    fn new() -> Self {
        Counter {
            value: Mutex::new(0),
        }
    }

    fn next(&self) -> u64 {
        let mut value = self.value.lock().unwrap();
        *value += 1;
        *value
    }
}

struct RestAgent {
    agent: ureq::Agent,
    pool_created: Instant,
    max_age: Option<Duration>,
}

impl RestAgent {
    fn new(daemon_parallelism: usize, max_age: Option<Duration>) -> Self {
        RestAgent {
            agent: ureq::Agent::new_with_config(
                ureq::Agent::config_builder()
                    .max_idle_connections(daemon_parallelism * 2)
                    .max_idle_connections_per_host(daemon_parallelism * 2)
                    .timeout_connect(Some(*DAEMON_CONNECTION_TIMEOUT))
                    .timeout_send_request(Some(*DAEMON_WRITE_TIMEOUT))
                    .timeout_recv_response(Some(*DAEMON_READ_TIMEOUT))
                    .timeout_recv_body(Some(*DAEMON_READ_TIMEOUT))
                    .timeout_global(Some(
                        *DAEMON_CONNECTION_TIMEOUT + *DAEMON_WRITE_TIMEOUT + *DAEMON_READ_TIMEOUT,
                    ))
                    .build(),
            ),
            pool_created: Instant::now(),
            max_age,
        }
    }

    /// Return an `Agent`, recycling the HTTP connection pool if expired.
    fn agent(&mut self) -> ureq::Agent {
        if self
            .max_age
            .map_or(false, |max_age| self.pool_created.elapsed() >= max_age)
        {
            self.recycle();
            debug!("recycled REST connection pool");
        }
        self.agent.clone() // cheap Arc clone internally
    }

    /// Reset the HTTP connection pool, so subsequent requests will use new connections.
    /// In-flight requests will continue using the existing pool.
    fn recycle(&mut self) {
        self.agent = ureq::Agent::new_with_config(self.agent.config().clone());
        self.pool_created = Instant::now();
    }
}

pub struct Daemon {
    daemon_dir: PathBuf,
    network: Network,
    connection_config: ConnectionConfig,
    conn: Mutex<Connection>,
    message_id: Counter, // for monotonic JSONRPC 'id'
    signal: Waiter,
    request_pool: Arc<rayon::ThreadPool>,
    rest_agent: Arc<Mutex<RestAgent>>, // connection-pooling HTTP agent for REST endpoints

    // Caps concurrent RPCs issued on behalf of API clients (see `request_proxied`).
    // Shared across reconnects so the cap stays global to the process.
    proxy_limit: Arc<BlockingSemaphore>,

    // monitoring
    latency: HistogramVec,
    size: HistogramVec,
    conn_recycle: CounterVec,
    proxy_rpc: CounterVec,
}

impl Daemon {
    pub fn new(
        daemon_dir: &PathBuf,
        daemon_rpc_addr: SocketAddr,
        daemon_rpc_fallback_addr: Option<SocketAddr>,
        daemon_parallelism: usize,
        cookie_getter: Arc<dyn CookieGetter>,
        network: Network,
        signal: Waiter,
        metrics: &Metrics,
        conn_max_age: Option<Duration>,
    ) -> Result<Daemon> {
        let connection_config = ConnectionConfig {
            addr: daemon_rpc_addr,
            fallback: daemon_rpc_fallback_addr,
            cookie_getter,
            signal: signal.clone(),
            max_age: conn_max_age,
        };
        let conn = connection_config.connect()?;
        let daemon = Daemon {
            daemon_dir: daemon_dir.clone(),
            network,
            connection_config,
            conn: Mutex::new(conn),
            message_id: Counter::new(),
            signal: signal.clone(),
            request_pool: Arc::new(
                rayon::ThreadPoolBuilder::new()
                    .num_threads(daemon_parallelism)
                    .thread_name(|i| format!("daemon-requests-{}", i))
                    .build()
                    .unwrap(),
            ),
            rest_agent: Arc::new(Mutex::new(RestAgent::new(daemon_parallelism, conn_max_age))),
            proxy_limit: Arc::new(BlockingSemaphore::new(*DAEMON_PROXY_MAX_CONCURRENCY)),
            latency: metrics.histogram_vec(
                HistogramOpts::new("daemon_rpc", "Bitcoind RPC latency (in seconds)"),
                &["method"],
            ),
            size: metrics.histogram_vec(
                HistogramOpts::new("daemon_bytes", "Bitcoind RPC size (in bytes)"),
                &["method", "dir"],
            ),
            proxy_rpc: metrics.counter_vec(
                MetricOpts::new(
                    "daemon_rpc_proxied",
                    "Daemon RPCs made on behalf of API clients (by result)",
                ),
                &["result"],
            ),
            conn_recycle: metrics.counter_vec(
                MetricOpts::new(
                    "daemon_rpc_conn_recycled",
                    "Proactive daemon RPC connection recycle attempts (by result)",
                ),
                &["result"],
            ),
        };
        let network_info = daemon.getnetworkinfo()?;
        info!("{:?}", network_info);
        if network_info.version < 16_00_00 {
            bail!(
                "{} is not supported - please use bitcoind 0.16+",
                network_info.subversion,
            )
        }
        let blockchain_info = daemon.getblockchaininfo()?;
        info!("{:?}", blockchain_info);
        if blockchain_info.pruned {
            bail!("pruned node is not supported (use '-prune=0' bitcoind flag)".to_owned())
        }
        daemon
            .rest_get("chaininfo.json", |_, _| Ok(false))
            .chain_err(|| "daemon REST API unavailable (enabling '-rest' is required)")?;
        loop {
            let info = daemon.getblockchaininfo()?;

            if !info.initialblockdownload.unwrap_or(false) && info.blocks == info.headers {
                break;
            }

            warn!(
                "waiting for bitcoind sync to finish: {}/{} blocks, verification progress: {:.3}%",
                info.blocks,
                info.headers,
                info.verificationprogress * 100.0
            );
            signal.wait(Duration::from_secs(5), false)?;
        }
        Ok(daemon)
    }

    #[trace]
    pub fn reconnect(&self) -> Result<Daemon> {
        Ok(Daemon {
            daemon_dir: self.daemon_dir.clone(),
            network: self.network,
            connection_config: self.connection_config.clone(),
            conn: Mutex::new(self.conn.lock().unwrap().reconnect()?),
            message_id: Counter::new(),
            signal: self.signal.clone(),
            request_pool: self.request_pool.clone(),
            rest_agent: self.rest_agent.clone(),
            proxy_limit: Arc::clone(&self.proxy_limit),
            latency: self.latency.clone(),
            size: self.size.clone(),
            conn_recycle: self.conn_recycle.clone(),
            proxy_rpc: self.proxy_rpc.clone(),
        })
    }

    pub fn with_request_pool<T, F>(&self, f: F) -> T
    where
        T: Send,
        F: FnOnce() -> T + Send,
    {
        self.request_pool.install(f)
    }

    #[cfg(not(feature = "liquid"))]
    pub fn supports_spenttxouts(&self) -> bool {
        match self.get_spent_txouts(&crate::chain::genesis_hash(self.network)) {
            Ok(_) => true,
            Err(err) => {
                debug!("REST spenttxouts check failed: {}", err.display_chain());
                false
            }
        }
    }

    #[cfg(feature = "liquid")]
    pub fn supports_spenttxouts(&self) -> bool {
        false
    }

    #[trace]
    fn call_jsonrpc_on_connection(
        &self,
        method: &str,
        request: &Value,
        conn: &mut Connection,
    ) -> Result<Value> {
        // Proactively recycle connections older than the configured max age. Re-establishing
        // the TCP connection lets a fronting load balancer (e.g. a Kubernetes ClusterSetIP)
        // re-select a backend, so a long-lived connection does not stay pinned to a stale
        // endpoint after node rotations. No-op when no max age is configured (the default).
        if conn.should_recycle() {
            match conn.try_reconnect_once() {
                Ok(new_conn) => {
                    debug!(
                        "recycled expired daemon RPC connection to {} after {:?}",
                        conn.active_addr,
                        conn.established.elapsed()
                    );
                    *conn = new_conn;
                    self.conn_recycle.with_label_values(&["ok"]).inc();
                }
                Err(err) => {
                    // Recycling is best-effort: if no fresh socket is available (e.g. a
                    // transient load-balancer hiccup), keep using the existing healthy
                    // connection rather than blocking requests while it is still usable.
                    // Record the failed attempt so we don't retry (and pay a connect timeout)
                    // on every subsequent request; the next attempt waits out the cooldown.
                    conn.last_recycle_attempt = Some(Instant::now());
                    self.conn_recycle.with_label_values(&["failed"]).inc();
                    warn!(
                        "failed recycling expired daemon RPC connection, keeping existing connection: {}",
                        err.display_chain()
                    );
                }
            }
        }
        let timer = self.latency.with_label_values(&[method]).start_timer();
        let request = request.to_string();
        conn.send(&request)?;
        self.size
            .with_label_values(&[method, "send"])
            .observe(request.len() as f64);
        let response = conn.recv()?;
        let result: Value = from_str(&response).chain_err(|| "invalid JSON")?;
        timer.observe_duration();
        self.size
            .with_label_values(&[method, "recv"])
            .observe(response.len() as f64);
        Ok(result)
    }

    #[trace]
    fn call_jsonrpc(&self, method: &str, request: &Value) -> Result<Value> {
        let mut conn = self.conn.lock().unwrap();
        self.call_jsonrpc_on_connection(method, request, &mut conn)
    }

    #[trace(method = %method)]
    fn handle_request(&self, method: &str, params: &Value) -> Result<Value> {
        let id = self.message_id.next();
        let req = json!({"method": method, "params": params, "id": id});
        let reply = self.call_jsonrpc(method, &req)?;
        parse_jsonrpc_reply(reply, method, id)
    }

    fn retry_request(&self, method: &str, params: &Value) -> Result<Value> {
        loop {
            match self.handle_request(method, &params) {
                Err(e @ Error(ErrorKind::Connection(_), _)) => {
                    warn!("reconnecting to bitcoind: {}", e.display_chain());
                    self.signal.wait(Duration::from_secs(3), false)?;
                    let mut conn = self.conn.lock().unwrap();
                    *conn = conn.reconnect()?;
                    continue;
                }
                Err(e @ Error(ErrorKind::RpcError(-28, _, _), _)) => {
                    warn!("bitcoind is warming up: {}", e.display_chain());
                    self.signal.wait(Duration::from_secs(3), false)?;
                    let mut conn = self.conn.lock().unwrap();
                    *conn = conn.reconnect()?;
                    continue;
                }
                result => return result,
            }
        }
    }

    #[trace]
    fn request(&self, method: &str, params: Value) -> Result<Value> {
        self.retry_request(method, &params)
    }

    /// Perform one RPC on a fresh connection isolated from singleton RPC users.
    /// Connection and warmup failures are returned to the caller without retrying.
    #[trace]
    fn request_once(&self, method: &str, params: Value, io_timeout: Duration) -> Result<Value> {
        let id = self.message_id.next();
        let req = json!({"method": method, "params": params, "id": id});
        let mut conn = self.connection_config.connect_once(io_timeout)?;
        let reply = self.call_jsonrpc_on_connection(method, &req, &mut conn)?;
        parse_jsonrpc_reply(reply, method, id)
    }

    /// Perform one RPC on behalf of an API client, bounded in both concurrency and time.
    ///
    /// The REST and Electrum endpoints that reach the daemon are anonymous and unmetered,
    /// so they must not use `request`: that path serializes on the process-wide
    /// `Mutex<Connection>` (letting one slow client request stall the indexer and every
    /// other client) and retries transport failures forever (letting one client request
    /// occupy a thread indefinitely). Instead each call gets its own short-lived
    /// connection with a short I/O timeout, and `proxy_limit` caps how many may be in
    /// flight at once. Failures are reported to the client rather than retried.
    #[trace(method = %method)]
    fn request_proxied(&self, method: &str, params: Value) -> Result<Value> {
        let _permit = self
            .proxy_limit
            .acquire(*DAEMON_PROXY_QUEUE_TIMEOUT)
            .ok_or_else(|| {
                self.proxy_rpc.with_label_values(&["busy"]).inc();
                Error::from(ErrorKind::DaemonBusy(format!(
                    "all {} client RPC slots are in use, gave up waiting after {:?}",
                    self.proxy_limit.capacity(),
                    *DAEMON_PROXY_QUEUE_TIMEOUT
                )))
            })?;

        match self.request_once(method, params, *DAEMON_PROXY_RPC_TIMEOUT) {
            Ok(result) => {
                self.proxy_rpc.with_label_values(&["ok"]).inc();
                Ok(result)
            }
            Err(err) => {
                // Report transport-level failures (which include hitting
                // DAEMON_PROXY_RPC_TIMEOUT) distinctly from daemon-level rejections, so
                // callers can answer "the daemon didn't respond" with a gateway error
                // instead of blaming the client's request.
                let unavailable = match err.kind() {
                    ErrorKind::Connection(msg) => Some(format!("{} failed: {}", method, msg)),
                    _ => None,
                };
                match unavailable {
                    Some(msg) => {
                        // The concise message is what the client sees, so log the full
                        // chain (which carries the underlying io error) before dropping it.
                        warn!("client daemon RPC failed: {}", err.display_chain());
                        self.proxy_rpc.with_label_values(&["unavailable"]).inc();
                        Err(ErrorKind::DaemonUnavailable(msg).into())
                    }
                    None => {
                        self.proxy_rpc.with_label_values(&["error"]).inc();
                        Err(err)
                    }
                }
            }
        }
    }

    #[trace]
    fn retry_reconnect(&self) -> Daemon {
        // XXX add a max reconnection attempts limit?
        loop {
            match self.reconnect() {
                Ok(daemon) => break daemon,
                Err(e) => {
                    warn!("failed connecting to RPC daemon: {}", e.display_chain());
                }
            }
        }
    }

    // Send requests in parallel over multiple RPC connections as individual JSON-RPC requests (with no JSON-RPC batching),
    // buffering the replies into a vector. If any of the requests fail, processing is terminated and an Err is returned.
    #[trace]
    fn requests(&self, method: &str, params_list: Vec<Value>) -> Result<Vec<Value>> {
        self.request_pool
            .install(|| self.requests_iter(method, params_list).collect())
    }

    // Send requests in parallel over multiple RPC connections, iterating over the results without buffering them.
    // Errors are included in the iterator and do not terminate other pending requests.
    //
    // IMPORTANT: The returned parallel iterator must be collected inside self.request_pool.install()
    // to ensure it runs on the daemon's own thread pool, not the global rayon pool. This is necessary
    // because the per-thread DAEMON_INSTANCE thread-locals would otherwise be shared across different
    // daemon instances in the same process (e.g. during parallel tests).
    #[trace]
    fn requests_iter<'a>(
        &'a self,
        method: &'a str,
        params_list: Vec<Value>,
    ) -> impl ParallelIterator<Item = Result<Value>> + IndexedParallelIterator + 'a {
        params_list.into_par_iter().map(move |params| {
            // Store a local per-thread Daemon, each with its own TCP connection. These will
            // get initialized as necessary for the request_pool thread managed by rayon.
            thread_local!(static DAEMON_INSTANCE: OnceCell<Daemon> = OnceCell::new());

            DAEMON_INSTANCE.with(|daemon| {
                daemon
                    .get_or_init(|| self.retry_reconnect())
                    .retry_request(&method, &params)
            })
        })
    }

    // bitcoind JSONRPC API:

    #[trace]
    pub fn getblockchaininfo(&self) -> Result<BlockchainInfo> {
        let info: Value = self.request("getblockchaininfo", json!([]))?;
        Ok(from_value(info).chain_err(|| "invalid blockchain info")?)
    }

    #[trace]
    fn getnetworkinfo(&self) -> Result<NetworkInfo> {
        let info: Value = self.request("getnetworkinfo", json!([]))?;
        Ok(from_value(info).chain_err(|| "invalid network info")?)
    }

    #[trace]
    pub fn getbestblockhash(&self) -> Result<BlockHash> {
        parse_hash(&self.request("getbestblockhash", json!([]))?)
    }

    #[trace]
    pub fn getblockheader(&self, blockhash: &BlockHash) -> Result<BlockHeader> {
        header_from_value(self.request("getblockheader", json!([blockhash, /*verbose=*/ false]))?)
    }

    #[trace]
    pub fn getblockheaders(&self, heights: &[usize]) -> Result<Vec<BlockHeader>> {
        let heights: Vec<Value> = heights.iter().map(|height| json!([height])).collect();
        let params_list: Vec<Value> = self
            .requests("getblockhash", heights)?
            .into_iter()
            .map(|hash| json!([hash, /*verbose=*/ false]))
            .collect();
        let mut result = vec![];
        for h in self.requests("getblockheader", params_list)? {
            result.push(header_from_value(h)?);
        }
        Ok(result)
    }

    /// Send a REST GET, retrying on transport failures and retryable error responses.
    /// `should_retry` classifies non-200 responses; returning an `Err` aborts the request.
    fn rest_get<F>(
        &self,
        path: &str,
        mut should_retry: F,
    ) -> Result<ureq::http::Response<ureq::Body>>
    where
        F: FnMut(hyper::StatusCode, &str) -> Result<bool>,
    {
        let mut attempts = MAX_ATTEMPTS;
        loop {
            attempts -= 1;

            // Reuse the Connection's active address for REST requests,
            // keeping it as the source of truth for server selection and failover.
            let (active_addr, conn_established) = self.conn.lock().unwrap().current();
            let agent = self.rest_agent.lock().unwrap().agent();
            let url = format!("http://{}/rest/{}", active_addr, path);
            let request = agent.get(&url).config().http_status_as_error(false).build();

            match request.call() {
                Ok(response) if response.status() == hyper::StatusCode::OK => return Ok(response),
                Ok(mut response) => {
                    let status = response.status();
                    let body = response.body_mut().read_to_string().map_err(|err| {
                        ErrorKind::Connection(format!(
                            "REST request to {} failed reading status {} response: {}",
                            path, status, err
                        ))
                    })?;
                    // Retry non-200 responses only when `should_retry` explicitly marks them as transient,
                    // otherwise consider them fatal. Transport failures are retried in the match arm below.
                    if !should_retry(status, &body)? {
                        bail!(ErrorKind::Connection(format!(
                            "REST request to {} failed with status {}: {}",
                            path, status, body
                        )))
                    }
                    if attempts == 0 {
                        bail!(ErrorKind::Connection(format!(
                            "REST request to {} failed with status {} after {} attempts: {}",
                            path, status, MAX_ATTEMPTS, body
                        )))
                    }
                    warn!(
                        "REST request to {} failed with status {}: {}, trying {} more times",
                        path, status, body, attempts
                    );
                }
                Err(err) => {
                    if attempts == 0 {
                        bail!(ErrorKind::Connection(format!(
                            "REST request to {} failed after {} attempts: {:?}",
                            path, MAX_ATTEMPTS, err
                        )))
                    }
                    {
                        let mut conn = self.conn.lock().unwrap();
                        // Trigger the Connection's reconnect selection mechanism on transport failures,
                        // unless another parallel REST request already replaced this connection.
                        if conn.established == conn_established {
                            *conn = conn.reconnect()?;
                            self.rest_agent.lock().unwrap().recycle();
                        }
                    }
                    warn!(
                        "REST request to {} failed: {:?}, trying {} more time",
                        path, err, attempts
                    );
                }
            }

            self.signal.wait(RETRY_WAIT_DURATION, false)?;
        }
    }

    #[trace]
    pub fn getblock(&self, blockhash: &BlockHash) -> Result<Block> {
        let mut response = self.rest_get(&format!("block/{}.bin", blockhash), |status, body| {
            // A node can return the header before it finishes indexing the block.
            Ok(status == hyper::StatusCode::NOT_FOUND
                && body.contains("not available (not fully downloaded)"))
        })?;
        let mut reader = BufReader::new(response.body_mut().as_reader());
        let block = Block::consensus_decode(&mut reader).chain_err(|| "failed to parse block")?;
        ensure!(
            reader
                .fill_buf()
                .chain_err(|| "failed to end stream")?
                .is_empty(),
            "block response has trailing data"
        );
        ensure!(
            block.block_hash() == *blockhash,
            "REST block hash mismatch: expected {}, got {}",
            blockhash,
            block.block_hash()
        );
        Ok(block)
    }

    #[trace]
    pub fn getblock_raw(&self, blockhash: &BlockHash, verbose: u32) -> Result<Value> {
        self.request("getblock", json!([blockhash, verbose]))
    }

    #[trace]
    pub fn getblocks(&self, blockhashes: &[BlockHash]) -> Result<Vec<Block>> {
        self.with_request_pool(|| {
            blockhashes
                .par_iter()
                .map(|hash| self.getblock(hash))
                .collect()
        })
    }

    /// Fetch the given transactions in parallel over multiple threads and RPC connections,
    /// ignoring any missing ones and returning whatever is available.
    #[trace]
    pub fn gettransactions_available(&self, txids: &[&Txid]) -> Result<HashMap<Txid, Transaction>> {
        const RPC_INVALID_ADDRESS_OR_KEY: i64 = -5;

        let params_list: Vec<Value> = txids
            .iter()
            .map(|txhash| json!([txhash, /*verbose=*/ false]))
            .collect();

        self.request_pool.install(|| {
            self.requests_iter("getrawtransaction", params_list)
                .zip(txids)
                .filter_map(|(res, txid)| match res {
                    Ok(val) => Some(tx_from_value(val).map(|tx| (**txid, tx))),
                    // Ignore 'tx not found' errors
                    Err(Error(ErrorKind::RpcError(code, _, _), _))
                        if code == RPC_INVALID_ADDRESS_OR_KEY =>
                    {
                        None
                    }
                    // Terminate iteration if any other errors are encountered
                    Err(e) => Some(Err(e)),
                })
                .collect()
        })
    }

    #[trace]
    pub fn gettransaction_raw(
        &self,
        txid: &Txid,
        blockhash: &BlockHash,
        verbose: bool,
    ) -> Result<Value> {
        self.request("getrawtransaction", json!([txid, verbose, blockhash]))
    }

    #[trace]
    pub fn getmempooltx(&self, txhash: &Txid) -> Result<Transaction> {
        let value = self.request("getrawtransaction", json!([txhash, /*verbose=*/ false]))?;
        tx_from_value(value)
    }

    #[trace]
    pub fn getmempooltxids(&self) -> Result<HashSet<Txid>> {
        let res = self.request("getrawmempool", json!([/*verbose=*/ false]))?;
        Ok(serde_json::from_value(res).chain_err(|| "invalid getrawmempool reply")?)
    }

    #[cfg(not(feature = "liquid"))]
    #[trace]
    pub fn getblocktemplate(&self, rules: &[&str]) -> Result<Value> {
        self.request_once(
            "getblocktemplate",
            json!([{ "rules": rules }]),
            BLOCK_TEMPLATE_RPC_TIMEOUT,
        )
    }

    #[cfg(feature = "liquid")]
    #[trace]
    pub fn getnewblockhex(&self) -> Result<String> {
        let value = self.request_once("getnewblockhex", json!([]), BLOCK_TEMPLATE_RPC_TIMEOUT)?;
        value
            .as_str()
            .map(str::to_owned)
            .chain_err(|| "non-string getnewblockhex response")
    }

    #[trace]
    pub fn broadcast(&self, tx: &Transaction) -> Result<Txid> {
        self.broadcast_raw(&serialize_hex(tx))
    }

    /// Broadcast a raw transaction on behalf of an API client.
    ///
    /// Uses the bounded client RPC path (`request_proxied`) rather than the shared
    /// singleton connection: this is reachable anonymously over both the REST and Electrum
    /// interfaces, so it must not be able to stall the indexer or other clients.
    #[trace]
    pub fn broadcast_raw(&self, txhex: &str) -> Result<Txid> {
        let txid = self.request_proxied("sendrawtransaction", json!([txhex]))?;
        Ok(
            Txid::from_str(txid.as_str().chain_err(|| "non-string txid")?)
                .chain_err(|| "failed to parse txid")?,
        )
    }

    pub fn submit_package(
        &self,
        txhex: Vec<String>,
        maxfeerate: Option<f64>,
        maxburnamount: Option<f64>,
    ) -> Result<SubmitPackageResult> {
        let params = match (maxfeerate, maxburnamount) {
            (Some(rate), Some(burn)) => {
                json!([txhex, format!("{:.8}", rate), format!("{:.8}", burn)])
            }
            (Some(rate), None) => json!([txhex, format!("{:.8}", rate)]),
            (None, Some(burn)) => json!([txhex, null, format!("{:.8}", burn)]),
            (None, None) => json!([txhex]),
        };
        // Anonymously reachable, so bounded like broadcast_raw() above.
        let result = self.request_proxied("submitpackage", params)?;
        serde_json::from_value::<SubmitPackageResult>(result)
            .chain_err(|| "invalid submitpackage reply")
    }

    // Get estimated feerates for the provided confirmation targets using a batch RPC request
    // Missing estimates are logged but do not cause a failure, whatever is available is returned
    #[allow(clippy::float_cmp)]
    #[trace]
    pub fn estimatesmartfee_batch(&self, conf_targets: &[u16]) -> Result<HashMap<u16, f64>> {
        let params_list: Vec<Value> = conf_targets
            .iter()
            .map(|t| json!([t, "ECONOMICAL"]))
            .collect();

        Ok(self
            .requests("estimatesmartfee", params_list)?
            .iter()
            .zip(conf_targets)
            .filter_map(|(reply, target)| {
                if !reply["errors"].is_null() {
                    warn!(
                        "failed estimating fee for target {}: {:?}",
                        target, reply["errors"]
                    );
                    return None;
                }

                let feerate = reply["feerate"]
                    .as_f64()
                    .unwrap_or_else(|| panic!("invalid estimatesmartfee response: {:?}", reply));

                if feerate == -1f64 {
                    warn!("not enough data to estimate fee for target {}", target);
                    return None;
                }

                // from BTC/kB to sat/b
                Some((*target, feerate * 100_000f64))
            })
            .collect())
    }

    #[trace]
    fn get_all_headers(&self, tip: &BlockHash) -> Result<Vec<BlockHeader>> {
        let info: Value = self.request("getblockheader", json!([tip]))?;
        let tip_height = info
            .get("height")
            .expect("missing height")
            .as_u64()
            .expect("non-numeric height") as usize;
        let all_heights: Vec<usize> = (0..=tip_height).collect();
        let chunk_size = 100_000;
        let mut result = vec![];
        for heights in all_heights.chunks(chunk_size) {
            let mut headers = self.getblockheaders(&heights)?;
            assert!(headers.len() == heights.len());

            result.append(&mut headers);

            debug!(
                "downloaded {}/{} block headers ({:.0}%)",
                result.len(),
                tip_height + 1,
                result.len() as f32 / (tip_height + 1) as f32 * 100.0
            );
        }

        let mut blockhash = *DEFAULT_BLOCKHASH;
        for header in &result {
            assert_eq!(header.prev_blockhash, blockhash);
            blockhash = header.block_hash();
        }
        assert_eq!(blockhash, *tip);
        Ok(result)
    }

    // Returns a list of BlockHeaders in ascending height (i.e. the tip is last).
    #[trace]
    pub fn get_new_headers(
        &self,
        indexed_headers: &HeaderList,
        bestblockhash: &BlockHash,
    ) -> Result<Vec<BlockHeader>> {
        // Iterate back over headers until known blockash is found:
        if indexed_headers.is_empty() {
            info!("downloading all block headers up to {}", bestblockhash);
            return self.get_all_headers(bestblockhash);
        }
        debug!(
            "downloading new block headers ({} already indexed) from {}",
            indexed_headers.len(),
            bestblockhash,
        );
        let mut new_headers = vec![];
        let mut blockhash = *bestblockhash;
        while blockhash != *DEFAULT_BLOCKHASH {
            if indexed_headers.header_by_blockhash(&blockhash).is_some() {
                break;
            }
            let header = self
                .getblockheader(&blockhash)
                .chain_err(|| format!("failed to get {} header", blockhash))?;
            blockhash = header.prev_blockhash;
            new_headers.push(header);
        }
        trace!("downloaded {} block headers", new_headers.len());
        new_headers.reverse(); // so the tip is the last vector entry
        Ok(new_headers)
    }

    #[trace]
    pub fn get_relayfee(&self) -> Result<f64> {
        let relayfee = self.getnetworkinfo()?.relayfee;

        // from BTC/kB to sat/b
        Ok(relayfee * 100_000f64)
    }

    /// Fetch spent transaction outputs for the given block via Bitcoin Core's
    /// REST /rest/spenttxouts/<hash>.bin endpoint.
    ///
    /// Returns one Vec<TxOut> per non-coinbase transaction, with TxOuts ordered
    /// by input index.
    ///
    /// Requires Bitcoin Core 30+ started with -rest=1.
    #[cfg(not(feature = "liquid"))]
    pub fn get_spent_txouts(&self, blockhash: &BlockHash) -> Result<Vec<Vec<bitcoin::TxOut>>> {
        let mut response =
            self.rest_get(&format!("spenttxouts/{}.bin", blockhash), |status, body| {
                bail!(ErrorKind::Connection(format!(
                    "REST spenttxouts failed for {} with status {}: {}",
                    blockhash, status, body
                )))
            })?;
        let mut reader = BufReader::new(response.body_mut().as_reader());
        parse_spent_txouts(&mut reader)
    }
}

#[cfg(not(feature = "liquid"))]
fn parse_spent_txouts<R: bitcoin::io::BufRead + ?Sized>(
    reader: &mut R,
) -> Result<Vec<Vec<bitcoin::TxOut>>> {
    // The binary spenttxouts format is: the CompactSize tx count, then for each tx the CompactSize TxOut count followed by the TxOuts.
    // Entry 0 is an empty coinbase placeholder, which we drop from the final returned structure.
    let tx_count = VarInt::consensus_decode(reader)
        .chain_err(|| "failed to parse spenttxouts tx count")?
        .0 as usize;

    ensure!(tx_count > 0, "spenttxouts response is empty");

    let coinbase_input_count = VarInt::consensus_decode(reader)
        .chain_err(|| "failed to parse spenttxouts coinbase placeholder")?
        .0;
    ensure!(
        coinbase_input_count == 0,
        "spenttxouts coinbase placeholder is not empty"
    );

    let mut spenttxouts = Vec::with_capacity(tx_count - 1);
    for _ in 1..tx_count {
        let input_count = VarInt::consensus_decode(reader)
            .chain_err(|| "failed to parse spenttxouts input count")?
            .0 as usize;
        let mut prevouts = Vec::with_capacity(input_count);
        for _ in 0..input_count {
            prevouts.push(
                bitcoin::TxOut::consensus_decode(reader)
                    .chain_err(|| "failed to parse spenttxout")?,
            );
        }
        spenttxouts.push(prevouts);
    }

    ensure!(
        reader
            .fill_buf()
            .chain_err(|| "failed to end stream")?
            .is_empty(),
        "spenttxouts response has trailing data"
    );

    Ok(spenttxouts)
}

#[cfg(test)]
mod tests {
    use super::{
        parse_jsonrpc_reply, recycle_due, BlockingSemaphore, ConnectionConfig, CookieGetter,
        RestAgent,
    };
    use crate::errors::{Error, ErrorKind, Result};
    use crate::signal::Waiter;
    use serde_json::json;
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};

    const COOLDOWN: Duration = Duration::from_secs(30);
    const MAX_AGE: Option<Duration> = Some(Duration::from_secs(60));

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    struct StaticCookie;

    impl CookieGetter for StaticCookie {
        fn get(&self) -> Result<Vec<u8>> {
            Ok(b"user:password".to_vec())
        }
    }

    #[test]
    fn one_shot_connection_uses_endpoint_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let config = ConnectionConfig {
            addr: listener.local_addr().unwrap(),
            fallback: None,
            cookie_getter: Arc::new(StaticCookie),
            signal: Waiter::start(crossbeam_channel::never()),
            max_age: None,
        };

        let connection = config.connect_once(secs(2)).unwrap();
        assert_eq!(connection.tx.read_timeout().unwrap(), Some(secs(2)));
        assert_eq!(connection.tx.write_timeout().unwrap(), Some(secs(2)));
    }

    #[test]
    fn no_max_age_never_recycles() {
        // Unlimited (the default): never recycle, regardless of age.
        assert!(!recycle_due(secs(10_000), None, None, COOLDOWN));
    }

    #[test]
    fn younger_than_max_age_does_not_recycle() {
        assert!(!recycle_due(secs(5), MAX_AGE, None, COOLDOWN));
    }

    #[test]
    fn expired_with_no_prior_attempt_recycles() {
        assert!(recycle_due(secs(61), MAX_AGE, None, COOLDOWN));
    }

    #[test]
    fn expired_within_cooldown_waits() {
        // A recent failed attempt should suppress retries until the cooldown elapses,
        // even though the connection is well past its max age.
        assert!(!recycle_due(secs(600), MAX_AGE, Some(secs(5)), COOLDOWN));
    }

    #[test]
    fn expired_after_cooldown_retries() {
        assert!(recycle_due(secs(600), MAX_AGE, Some(secs(31)), COOLDOWN));
    }

    #[test]
    fn one_shot_connection_recv_gives_up_at_the_endpoint_timeout() {
        // A daemon that accepts the connection and then never answers is exactly the
        // failure mode behind XF-05: without a short client-facing timeout the caller
        // blocks for DAEMON_READ_TIMEOUT (10 minutes by default). Hold the accepted socket
        // open for the duration so the read blocks rather than seeing EOF.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let blackhole = thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            thread::sleep(secs(5));
            drop(socket);
        });

        let config = ConnectionConfig {
            addr,
            fallback: None,
            cookie_getter: Arc::new(StaticCookie),
            signal: Waiter::start(crossbeam_channel::never()),
            max_age: None,
        };

        let mut connection = config.connect_once(secs(1)).unwrap();
        connection
            .send(&json!({"method": "getblockcount"}).to_string())
            .unwrap();

        let started = Instant::now();
        let err = connection.recv().unwrap_err();
        let elapsed = started.elapsed();

        assert!(
            matches!(err.kind(), ErrorKind::Connection(_)),
            "expected a connection error, got {:?}",
            err
        );
        assert!(
            elapsed >= secs(1) && elapsed < secs(4),
            "recv should give up at the endpoint timeout, took {:?}",
            elapsed
        );

        blackhole.join().unwrap();
    }

    #[test]
    fn semaphore_hands_out_capacity_permits() {
        let semaphore = BlockingSemaphore::new(2);
        let first = semaphore.acquire(secs(0));
        let second = semaphore.acquire(secs(0));
        assert!(first.is_some());
        assert!(second.is_some());
        // At capacity: the third caller is refused rather than queued indefinitely.
        assert!(semaphore.acquire(secs(0)).is_none());
    }

    #[test]
    fn semaphore_permit_is_returned_on_drop() {
        let semaphore = BlockingSemaphore::new(1);
        {
            let _permit = semaphore.acquire(secs(0)).unwrap();
            assert!(semaphore.acquire(secs(0)).is_none());
        }
        assert!(semaphore.acquire(secs(0)).is_some());
    }

    #[test]
    fn semaphore_zero_capacity_is_treated_as_one() {
        // A zero cap would wedge every client request, so it is clamped rather than honored.
        let semaphore = BlockingSemaphore::new(0);
        assert_eq!(semaphore.capacity(), 1);
        assert!(semaphore.acquire(secs(0)).is_some());
    }

    #[test]
    fn semaphore_gives_up_after_wait_timeout() {
        let semaphore = BlockingSemaphore::new(1);
        let _permit = semaphore.acquire(secs(0)).unwrap();

        // This is what stops client requests from piling up behind a wedged daemon: the
        // caller waits a bounded time and is then refused.
        let started = Instant::now();
        assert!(semaphore.acquire(Duration::from_millis(150)).is_none());
        assert!(started.elapsed() >= Duration::from_millis(150));
    }

    #[test]
    fn semaphore_wakes_a_waiter_when_a_permit_is_released() {
        let semaphore = Arc::new(BlockingSemaphore::new(1));
        let permit = semaphore.acquire(secs(0)).unwrap();

        let waiter = {
            let semaphore = Arc::clone(&semaphore);
            thread::spawn(move || semaphore.acquire(secs(5)).is_some())
        };

        thread::sleep(Duration::from_millis(50));
        drop(permit);
        assert!(waiter.join().unwrap());
    }

    #[test]
    fn rest_agent_recycles_after_max_age() {
        let mut rest_agent = RestAgent::new(1, MAX_AGE);
        let pool_created = rest_agent.pool_created;
        let _ = rest_agent.agent();
        assert_eq!(rest_agent.pool_created, pool_created);

        rest_agent.pool_created = Instant::now() - (MAX_AGE.unwrap() + secs(1));
        let expired_pool_created = rest_agent.pool_created;
        let _ = rest_agent.agent();
        assert!(rest_agent.pool_created > expired_pool_created);

        let pool_created = rest_agent.pool_created;
        let _ = rest_agent.agent();
        assert_eq!(rest_agent.pool_created, pool_created);

        let mut unlimited_agent = RestAgent::new(1, None);
        unlimited_agent.pool_created = Instant::now() - secs(100_000);
        let unlimited_created = unlimited_agent.pool_created;
        let _ = unlimited_agent.agent();
        assert_eq!(unlimited_agent.pool_created, unlimited_created);
    }

    #[test]
    fn warmup_error_parses_as_rpc_error() {
        let reply = json!({
            "result": null,
            "error": { "code": -28, "message": "warming up" },
            "id": 1
        });
        match parse_jsonrpc_reply(reply, "getblocktemplate", 1) {
            Err(Error(ErrorKind::RpcError(-28, message, method), _)) => {
                assert_eq!(message, "warming up");
                assert_eq!(method, "getblocktemplate");
            }
            other => panic!("unexpected getblocktemplate warmup result: {:?}", other),
        }
    }
}
