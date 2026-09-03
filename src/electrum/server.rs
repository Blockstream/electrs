use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::io::{BufRead, BufReader, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::thread;
use std::time::{Duration, Instant};

use bitcoin::hashes::{sha256, sha256d::Hash as Sha256dHash, Hash, HashEngine};
use bitcoin::hex::DisplayHex;
use error_chain::ChainedError;
use rand::Rng;
use serde_json::{from_str, Value};

use electrs_macros::trace;

#[cfg(not(feature = "liquid"))]
use bitcoin::consensus::encode::serialize_hex;
#[cfg(feature = "liquid")]
use elements::encode::serialize_hex;
use crate::chain::Txid;
use crate::config::{Config, RpcLogging};
use crate::electrum::{get_electrum_height, ProtocolVersion};
use crate::errors::*;
use crate::metrics::{Gauge, HistogramOpts, HistogramVec, MetricOpts, Metrics};
use crate::new_index::{Query, Utxo};
#[cfg(not(feature = "liquid"))]
use crate::util::electrum_merkle::get_tx_witness_merkle_proof;
use crate::util::electrum_merkle::{get_header_merkle_proof, get_id_from_pos, get_tx_merkle_proof};
use crate::util::{create_socket, spawn_thread, BlockId, BoolThen, Channel, FullHash, HeaderEntry};

const ELECTRS_VERSION: &str = env!("CARGO_PKG_VERSION");
const PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion::new(1, 4);
const MAX_HEADERS: usize = 2016;
const MAX_ARRAY_BATCH: usize = 20;

#[cfg(feature = "electrum-discovery")]
use crate::electrum::{DiscoveryManager, ServerFeatures};

fn invalid_params(msg: impl Into<String>) -> Error {
    ErrorKind::InvalidParams(msg.into()).into()
}

// TODO: Sha256dHash should be a generic hash-container (since script hash is single SHA256)
fn hash_from_value(val: Option<&Value>) -> Result<Sha256dHash> {
    let script_hash = val.ok_or_else(|| invalid_params("missing hash"))?;
    let script_hash = script_hash
        .as_str()
        .ok_or_else(|| invalid_params("non-string hash"))?;
    let script_hash = script_hash
        .parse()
        .map_err(|_| invalid_params("non-hex hash"))?;
    Ok(script_hash)
}

fn usize_from_value(val: Option<&Value>, name: &str) -> Result<usize> {
    let val = val.ok_or_else(|| invalid_params(format!("missing {}", name)))?;
    let val = val
        .as_u64()
        .ok_or_else(|| invalid_params(format!("non-integer {}", name)))?;
    Ok(val as usize)
}

fn usize_from_value_or(val: Option<&Value>, name: &str, default: usize) -> Result<usize> {
    if val.is_none() {
        return Ok(default);
    }
    usize_from_value(val, name)
}

fn bool_from_value(val: Option<&Value>, name: &str) -> Result<bool> {
    let val = val.ok_or_else(|| invalid_params(format!("missing {}", name)))?;
    let val = val
        .as_bool()
        .ok_or_else(|| invalid_params(format!("not a bool {}", name)))?;
    Ok(val)
}

fn bool_from_value_or(val: Option<&Value>, name: &str, default: bool) -> Result<bool> {
    if val.is_none() {
        return Ok(default);
    }
    bool_from_value(val, name)
}

// JSON-RPC 2.0 error codes (https://www.jsonrpc.org/specification#error_object),
// plus the application-level codes used by ElectrumX and romanz/electrs.
#[repr(i16)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum JsonRpcV2Error {
    ParseError = -32700,
    InvalidRequest = -32600,
    MethodNotFound = -32601,
    InvalidParams = -32602,
    InternalError = -32603,
    BadRequest = 1,
    DaemonError = 2,
}

impl JsonRpcV2Error {
    #[inline]
    fn into_i16(self) -> i16 {
        self as i16
    }
}

fn jsonrpc_code(e: &Error) -> JsonRpcV2Error {
    match e.kind() {
        ErrorKind::InvalidParams(_) => JsonRpcV2Error::InvalidParams,
        ErrorKind::TooPopular
        | ErrorKind::TooManyUtxos
        | ErrorKind::TooManySubscriptions(_) => JsonRpcV2Error::BadRequest,
        // The daemon could not be reached (or we refused to queue for it) for a request
        // made on the client's behalf. This is a downstream failure, not a client error.
        ErrorKind::RpcError(..) | ErrorKind::DaemonBusy(_) | ErrorKind::DaemonUnavailable(_) => {
            JsonRpcV2Error::DaemonError
        }
        _ => JsonRpcV2Error::InternalError,
    }
}

#[inline]
fn json_rpc_error(
    input: impl core::fmt::Display,
    id: Option<&Value>,
    code: JsonRpcV2Error,
) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id.unwrap_or(&Value::Null),
        "error": {
            "code": code.into_i16(),
            "message": format!("{}", input),
        },
    })
}

// TODO: implement caching and delta updates
#[trace]
fn get_status_hash(txs: Vec<(Txid, Option<BlockId>)>, query: &Query) -> Option<FullHash> {
    if txs.is_empty() {
        None
    } else {
        let mut engine = sha256::Hash::engine();
        for (txid, blockid) in txs {
            let is_mempool = blockid.is_none();
            let has_unconfirmed_parents = is_mempool
                .and_then(|| Some(query.has_unconfirmed_parents(&txid)))
                .unwrap_or(false);
            let height = get_electrum_height(blockid, has_unconfirmed_parents);
            let part = format!("{}:{}:", txid, height);
            engine.input(part.as_bytes());
        }
        Some(sha256::Hash::from_engine(engine).to_byte_array())
    }
}

fn subscription_allowed(
    status_hashes: &HashMap<Sha256dHash, Value>,
    script_hash: &Sha256dHash,
    limit: usize,
) -> bool {
    limit == 0 || status_hashes.len() < limit || status_hashes.contains_key(script_hash)
}

macro_rules! conditionally_log_rpc_event {
    ($self:ident, $event:expr) => {
        if $self.rpc_logging.enabled {
            $self.log_rpc_event($event);
        }
    };
}

struct Connection {
    query: Arc<Query>,
    last_header_entry: Option<HeaderEntry>,
    status_hashes: HashMap<Sha256dHash, Value>, // ScriptHash -> StatusHash
    // Shared with the connection reaper (via a Weak), which enforces the
    // maximum connection age by shutting the socket down at the deadline.
    stream: Arc<TcpStream>,
    addr: SocketAddr,
    sender: SyncSender<Message>,
    stats: Arc<Stats>,
    txs_limit: usize,
    subscription_limit: usize,
    #[cfg(feature = "electrum-discovery")]
    discovery: Option<Arc<DiscoveryManager>>,
    rpc_logging: RpcLogging,
    salt: String,
}

fn hash_ip_with_salt(salt: &str, ip: &str) -> String {
    let mut engine = sha256::Hash::engine();
    engine.input(salt.as_bytes());
    engine.input(ip.as_bytes());
    format!("{:x}", sha256::Hash::from_engine(engine))
}

impl Connection {
    pub fn new(
        query: Arc<Query>,
        stream: Arc<TcpStream>,
        addr: SocketAddr,
        sender: SyncSender<Message>,
        stats: Arc<Stats>,
        txs_limit: usize,
        subscription_limit: usize,
        #[cfg(feature = "electrum-discovery")] discovery: Option<Arc<DiscoveryManager>>,
        rpc_logging: RpcLogging,
        salt: String,
    ) -> Connection {
        Connection {
            query,
            last_header_entry: None, // disable header subscription for now
            status_hashes: HashMap::new(),
            stream,
            addr,
            sender,
            stats,
            txs_limit,
            subscription_limit,
            #[cfg(feature = "electrum-discovery")]
            discovery,
            rpc_logging,
            salt,
        }
    }

    fn blockchain_headers_subscribe(&mut self) -> Result<Value> {
        let entry = self.query.chain().best_header();
        let hex_header = serialize_hex(entry.header());
        let result = json!({"hex": hex_header, "height": entry.height()});
        self.last_header_entry = Some(entry);
        Ok(result)
    }

    fn server_version(&self) -> Result<Value> {
        Ok(json!([
            format!("electrs-esplora {}", ELECTRS_VERSION),
            PROTOCOL_VERSION
        ]))
    }

    fn server_banner(&self) -> Result<Value> {
        Ok(json!(self.query.config().electrum_banner.clone()))
    }

    #[cfg(feature = "electrum-discovery")]
    fn server_features(&self) -> Result<Value> {
        let discovery = self
            .discovery
            .as_ref()
            .chain_err(|| "discovery is disabled")?;
        Ok(json!(discovery.our_features()))
    }

    fn server_donation_address(&self) -> Result<Value> {
        Ok(Value::Null)
    }

    fn server_peers_subscribe(&self) -> Result<Value> {
        #[cfg(feature = "electrum-discovery")]
        let servers = self
            .discovery
            .as_ref()
            .map_or_else(|| json!([]), |d| json!(d.get_servers()));

        #[cfg(not(feature = "electrum-discovery"))]
        let servers = json!([]);

        Ok(servers)
    }

    #[cfg(feature = "electrum-discovery")]
    fn server_add_peer(&self, params: &[Value]) -> Result<Value> {
        let discovery = self
            .discovery
            .as_ref()
            .chain_err(|| "discovery is disabled")?;

        let features = params
            .get(0)
            .ok_or_else(|| invalid_params("missing features param"))?
            .clone();
        let features =
            serde_json::from_value(features).map_err(|_| invalid_params("invalid features"))?;

        discovery.add_server_request(self.addr.ip(), features)?;
        Ok(json!(true))
    }

    fn mempool_get_fee_histogram(&self) -> Result<Value> {
        Ok(json!(&self.query.mempool().backlog_stats().fee_histogram))
    }

    fn blockchain_block_header(&self, params: &[Value]) -> Result<Value> {
        let height = usize_from_value(params.get(0), "height")?;
        let cp_height = usize_from_value_or(params.get(1), "cp_height", 0)?;

        let raw_header_hex: String = self
            .query
            .chain()
            .header_by_height(height)
            .map(|entry| serialize_hex(entry.header()))
            .chain_err(|| "missing header")?;

        if cp_height == 0 {
            return Ok(json!(raw_header_hex));
        }
        let (branch, root) = get_header_merkle_proof(self.query.chain(), height, cp_height)?;

        Ok(json!({
            "header": raw_header_hex,
            "root": root,
            "branch": branch
        }))
    }

    fn blockchain_block_headers(&self, params: &[Value]) -> Result<Value> {
        let start_height = usize_from_value(params.get(0), "start_height")?;
        let count = MAX_HEADERS.min(usize_from_value(params.get(1), "count")?);
        let cp_height = usize_from_value_or(params.get(2), "cp_height", 0)?;
        let heights: Vec<usize> = (start_height..(start_height + count)).collect();
        let headers: Vec<String> = heights
            .into_iter()
            .filter_map(|height| {
                self.query
                    .chain()
                    .header_by_height(height)
                    .map(|entry| serialize_hex(entry.header()))
            })
            .collect();

        if count == 0 || cp_height == 0 {
            return Ok(json!({
                "count": headers.len(),
                "hex": headers.join(""),
                "max": MAX_HEADERS,
            }));
        }

        let (branch, root) =
            get_header_merkle_proof(self.query.chain(), start_height + (count - 1), cp_height)?;

        Ok(json!({
            "count": headers.len(),
            "hex": headers.join(""),
            "max": MAX_HEADERS,
            "root": root,
            "branch" : branch,
        }))
    }

    #[trace]
    fn blockchain_estimatefee(&self, params: &[Value]) -> Result<Value> {
        let conf_target = usize_from_value(params.get(0), "blocks_count")?;
        let fee_rate = self
            .query
            .estimate_fee(conf_target as u16)
            .chain_err(|| format!("cannot estimate fee for {} blocks", conf_target))?;
        // convert from sat/b to BTC/kB, as expected by Electrum clients
        Ok(json!(fee_rate / 100_000f64))
    }

    fn blockchain_relayfee(&self) -> Result<Value> {
        let relayfee = self.query.get_relayfee()?;
        // convert from sat/b to BTC/kB, as expected by Electrum clients
        Ok(json!(relayfee / 100_000f64))
    }

    fn blockchain_scripthash_subscribe(&mut self, params: &[Value]) -> Result<Value> {
        let script_hash = hash_from_value(params.get(0))?;

        ensure!(
            subscription_allowed(&self.status_hashes, &script_hash, self.subscription_limit),
            ErrorKind::TooManySubscriptions(self.subscription_limit)
        );

        let history_txids = get_history(&self.query, &script_hash[..], self.txs_limit)?;
        let status_hash = get_status_hash(history_txids, &self.query)
            .map_or(Value::Null, |h| json!(h.to_lower_hex_string()));

        if let None = self.status_hashes.insert(script_hash, status_hash.clone()) {
            self.stats.subscriptions.inc();
        }
        Ok(status_hash)
    }

    fn blockchain_scripthash_unsubscribe(&mut self, params: &[Value]) -> Result<Value> {
        let script_hash = hash_from_value(params.get(0))?;

        match self.status_hashes.remove(&script_hash) {
            None => Ok(json!(false)),
            Some(_) => {
                self.stats.subscriptions.dec();
                Ok(json!(true))
            }
        }
    }

    #[cfg(not(feature = "liquid"))]
    fn blockchain_scripthash_get_balance(&self, params: &[Value]) -> Result<Value> {
        let script_hash = hash_from_value(params.get(0))?;
        let (chain_stats, mempool_stats) = self.query.stats(&script_hash[..]);

        Ok(json!({
            "confirmed": chain_stats.funded_txo_sum - chain_stats.spent_txo_sum,
            "unconfirmed": mempool_stats.funded_txo_sum as i64 - mempool_stats.spent_txo_sum as i64,
        }))
    }

    fn blockchain_scripthash_get_history(&self, params: &[Value]) -> Result<Value> {
        let script_hash = hash_from_value(params.get(0))?;
        let history_txids = get_history(&self.query, &script_hash[..], self.txs_limit)?;

        Ok(json!(history_txids
            .into_iter()
            .map(|(txid, blockid)| {
                let is_mempool = blockid.is_none();
                let fee = is_mempool.and_then(|| self.query.get_mempool_tx_fee(&txid));
                let has_unconfirmed_parents = is_mempool
                    .and_then(|| Some(self.query.has_unconfirmed_parents(&txid)))
                    .unwrap_or(false);
                let height = get_electrum_height(blockid, has_unconfirmed_parents);
                GetHistoryResult { txid, height, fee }
            })
            .collect::<Vec<_>>()))
    }

    fn blockchain_scripthash_get_mempool(&self, params: &[Value]) -> Result<Value> {
        let script_hash = hash_from_value(params.get(0))?;
        // ask for one extra more than the limit and fail if it exists, to avoid silently truncating
        let mempool_txids = self
            .query
            .mempool()
            .history_txids(&script_hash[..], self.txs_limit + 1);
        ensure!(mempool_txids.len() <= self.txs_limit, ErrorKind::TooPopular);

        Ok(json!(mempool_txids
            .into_iter()
            .map(|txid| {
                let fee = self.query.get_mempool_tx_fee(&txid);
                let has_unconfirmed_parents = self.query.has_unconfirmed_parents(&txid);
                // per the Electrum protocol: 0 if all inputs are confirmed, -1 otherwise
                let height = if has_unconfirmed_parents { -1 } else { 0 };
                GetHistoryResult { txid, height, fee }
            })
            .collect::<Vec<_>>()))
    }

    fn blockchain_scripthash_listunspent(&self, params: &[Value]) -> Result<Value> {
        let script_hash = hash_from_value(params.get(0))?;
        let utxos = self.query.utxo(&script_hash[..])?;

        let to_json = |utxo: Utxo| {
            let json = json!({
                "height": utxo.confirmed.map_or(0, |b| b.height),
                "tx_pos": utxo.vout,
                "tx_hash": utxo.txid,
                "value": utxo.value,
            });

            #[cfg(feature = "liquid")]
            let json = {
                let mut json = json;
                json["asset"] = json!(utxo.asset);
                json["nonce"] = json!(utxo.nonce);
                json
            };

            json
        };

        Ok(json!(Value::Array(
            utxos.into_iter().map(to_json).collect()
        )))
    }

    fn blockchain_transaction_broadcast(&self, params: &[Value]) -> Result<Value> {
        let tx = params.get(0).ok_or_else(|| invalid_params("missing tx"))?;
        let tx = tx
            .as_str()
            .ok_or_else(|| invalid_params("non-string tx"))?
            .to_string();
        let txid = self.query.broadcast_raw(&tx)?;
        if let Err(e) = self.sender.try_send(Message::PeriodicUpdate) {
            warn!("failed to issue PeriodicUpdate after broadcast: {}", e);
        }
        Ok(json!(txid))
    }

    // Ported from romanz/electrs (https://github.com/romanz/electrs).
    fn blockchain_transaction_broadcast_package(&self, params: &[Value]) -> Result<Value> {
        let txhexes: Vec<String> = params
            .get(0)
            .ok_or_else(|| invalid_params("missing transactions"))
            .and_then(|txs| {
                serde_json::from_value(txs.clone())
                    .map_err(|_| invalid_params("non-array transactions"))
            })?;
        let verbose = bool_from_value_or(params.get(1), "verbose", false)?;

        let result = self.query.submit_package(txhexes, None, None)?;
        if let Err(e) = self.sender.try_send(Message::PeriodicUpdate) {
            warn!(
                "failed to issue PeriodicUpdate after broadcast_package: {}",
                e
            );
        }
        Ok(result.into_electrum_response(verbose))
    }

    fn blockchain_transaction_get(&self, params: &[Value]) -> Result<Value> {
        let tx_hash = Txid::from(hash_from_value(params.get(0))?);
        let verbose = match params.get(1) {
            Some(value) => value
                .as_bool()
                .ok_or_else(|| invalid_params("non-bool verbose value"))?,
            None => false,
        };

        // FIXME: implement verbose support
        if verbose {
            bail!("verbose transactions are currently unsupported");
        }

        let rawtx = self
            .query
            .lookup_raw_txn(&tx_hash)
            .chain_err(|| "missing transaction")?;
        Ok(json!(rawtx.to_lower_hex_string()))
    }

    #[trace]
    fn blockchain_transaction_get_merkle(&self, params: &[Value]) -> Result<Value> {
        let txid = Txid::from(hash_from_value(params.get(0))?);
        let height = usize_from_value(params.get(1), "height")?;
        let blockid = self
            .query
            .chain()
            .tx_confirming_block(&txid)
            .ok_or_else(|| "tx not found or is unconfirmed")?;
        if blockid.height != height {
            return Err(invalid_params("invalid confirmation height provided"));
        }
        let (merkle, pos) = get_tx_merkle_proof(self.query.chain(), &txid, &blockid.hash)
            .chain_err(|| "cannot create merkle proof")?;
        Ok(json!({
            "block_height": blockid.height,
            "merkle": merkle,
            "pos": pos
        }))
    }

    /// Witness-tree twin of get_merkle: proves the tx against the block's
    /// BIP-141 witness merkle tree (wtxids, zeroed coinbase leaf), binding the
    /// full serialization including witness data.
    #[cfg(not(feature = "liquid"))]
    #[trace]
    fn blockchain_transaction_get_witness_merkle(&self, params: &[Value]) -> Result<Value> {
        let txid = Txid::from(hash_from_value(params.get(0))?);
        let height = usize_from_value(params.get(1), "height")?;
        let blockid = self
            .query
            .chain()
            .tx_confirming_block(&txid)
            .ok_or_else(|| "tx not found or is unconfirmed")?;
        if blockid.height != height {
            return Err(invalid_params("invalid confirmation height provided"));
        }
        let (merkle, pos, witness_root) =
            get_tx_witness_merkle_proof(self.query.chain(), &txid, &blockid.hash)
                .chain_err(|| "cannot create witness merkle proof")?;
        Ok(json!({
            "block_height": blockid.height,
            "merkle": merkle,
            "pos": pos,
            "witness_root": witness_root,
        }))
    }

    fn blockchain_transaction_id_from_pos(&self, params: &[Value]) -> Result<Value> {
        let height = usize_from_value(params.get(0), "height")?;
        let tx_pos = usize_from_value(params.get(1), "tx_pos")?;
        let want_merkle = bool_from_value_or(params.get(2), "merkle", false)?;

        let (txid, merkle) = get_id_from_pos(self.query.chain(), height, tx_pos, want_merkle)?;

        if !want_merkle {
            return Ok(json!(txid));
        }

        Ok(json!({
            "tx_hash": txid,
            "merkle" : merkle
        }))
    }

    #[trace(method = %method)]
    fn handle_command(&mut self, method: &str, params: &[Value], id: &Value) -> Result<Value> {
        let timer = self
            .stats
            .latency
            .with_label_values(&[method])
            .start_timer();

        let result = match method {
            "blockchain.block.header" => self.blockchain_block_header(&params),
            "blockchain.block.headers" => self.blockchain_block_headers(&params),
            "blockchain.estimatefee" => self.blockchain_estimatefee(&params),
            "blockchain.headers.subscribe" => self.blockchain_headers_subscribe(),
            "blockchain.relayfee" => self.blockchain_relayfee(),
            #[cfg(not(feature = "liquid"))]
            "blockchain.scripthash.get_balance" => self.blockchain_scripthash_get_balance(&params),
            "blockchain.scripthash.get_history" => self.blockchain_scripthash_get_history(&params),
            "blockchain.scripthash.get_mempool" => self.blockchain_scripthash_get_mempool(&params),
            "blockchain.scripthash.listunspent" => self.blockchain_scripthash_listunspent(&params),
            "blockchain.scripthash.subscribe" => self.blockchain_scripthash_subscribe(&params),
            "blockchain.scripthash.unsubscribe" => self.blockchain_scripthash_unsubscribe(&params),
            "blockchain.transaction.broadcast" => self.blockchain_transaction_broadcast(&params),
            "blockchain.transaction.broadcast_package" => {
                self.blockchain_transaction_broadcast_package(&params)
            }
            "blockchain.transaction.get" => self.blockchain_transaction_get(&params),
            "blockchain.transaction.get_merkle" => self.blockchain_transaction_get_merkle(&params),
            #[cfg(not(feature = "liquid"))]
            "blockchain.transaction.get_witness_merkle" => {
                self.blockchain_transaction_get_witness_merkle(&params)
            }
            "blockchain.transaction.id_from_pos" => {
                self.blockchain_transaction_id_from_pos(&params)
            }
            "mempool.get_fee_histogram" => self.mempool_get_fee_histogram(),
            "server.banner" => self.server_banner(),
            "server.donation_address" => self.server_donation_address(),
            "server.peers.subscribe" => self.server_peers_subscribe(),
            "server.ping" => Ok(Value::Null),
            "server.version" => self.server_version(),

            #[cfg(feature = "electrum-discovery")]
            "server.features" => self.server_features(),
            #[cfg(feature = "electrum-discovery")]
            "server.add_peer" => self.server_add_peer(&params),

            &_ => {
                warn!("rpc #{} unknown method {} {:?}", id, method, params);
                return Ok(json_rpc_error(
                    format!("unknown method {}", method),
                    Some(id),
                    JsonRpcV2Error::MethodNotFound,
                ));
            }
        };
        timer.observe_duration();
        Ok(match result {
            Ok(result) => json!({"jsonrpc": "2.0", "id": id, "result": result}),
            Err(e) => {
                warn!(
                    "rpc #{} {} {:?} failed: {}",
                    id,
                    method,
                    params,
                    e.display_chain()
                );
                json_rpc_error(&e, Some(id), jsonrpc_code(&e))
            }
        })
    }

    #[trace]
    fn update_subscriptions(&mut self) -> Result<Vec<Value>> {
        let timer = self
            .stats
            .latency
            .with_label_values(&["periodic_update"])
            .start_timer();
        let mut result = vec![];
        if let Some(ref mut last_entry) = self.last_header_entry {
            let entry = self.query.chain().best_header();
            if *last_entry != entry {
                *last_entry = entry;
                let hex_header = serialize_hex(last_entry.header());
                let header = json!({"hex": hex_header, "height": last_entry.height()});
                result.push(json!({
                    "jsonrpc": "2.0",
                    "method": "blockchain.headers.subscribe",
                    "params": [header]}));
            }
        }
        for (script_hash, status_hash) in self.status_hashes.iter_mut() {
            let history_txids = get_history(&self.query, &script_hash[..], self.txs_limit)?;
            let new_status_hash = get_status_hash(history_txids, &self.query)
                .map_or(Value::Null, |h| json!(h.to_lower_hex_string()));
            if new_status_hash == *status_hash {
                continue;
            }
            result.push(json!({
                "jsonrpc": "2.0",
                "method": "blockchain.scripthash.subscribe",
                "params": [script_hash, new_status_hash]}));
            *status_hash = new_status_hash;
        }
        timer.observe_duration();
        Ok(result)
    }

    fn log_rpc_event(&self, mut log: Value) {
        let real_ip = self.addr.ip().to_string();
        let ip_to_log = if self.rpc_logging.anonymize_ip {
            hash_ip_with_salt(&self.salt, &real_ip)
        } else {
            real_ip
        };

        log.as_object_mut().unwrap().insert(
            "source".into(),
            json!({
                "ip": ip_to_log,
                "port": self.addr.port(),
            }),
        );
        println!("{}", log);
    }

    fn send_values(&mut self, values: &[Value]) -> Result<()> {
        for value in values {
            let line = value.to_string() + "\n";
            (&*self.stream)
                .write_all(line.as_bytes())
                .chain_err(|| format!("failed to send response ({} bytes)", line.len()))?;
        }
        Ok(())
    }

    #[trace]
    fn handle_replies(&mut self, receiver: Receiver<Message>) -> Result<()> {
        let empty_params = json!([]);
        loop {
            let msg = receiver.recv().chain_err(|| "channel closed")?;
            trace!("RPC {:?}", msg);
            match msg {
                Message::Request(line) => {
                    let reply = match from_str::<Value>(&line) {
                        Ok(Value::Array(arr)) => {
                            if arr.len() > MAX_ARRAY_BATCH {
                                bail!(
                                    "Too many elements in batch requests {} max:{}",
                                    arr.len(),
                                    MAX_ARRAY_BATCH
                                );
                            }
                            let mut result = Vec::with_capacity(arr.len());
                            for el in arr {
                                result.push(self.handle_value(el, &empty_params));
                            }
                            Value::Array(result)
                        }
                        Ok(cmd) => self.handle_value(cmd, &empty_params),
                        Err(err) => {
                            warn!("[{}] invalid JSON request: {}", self.addr, err);
                            json_rpc_error("parse error", None, JsonRpcV2Error::ParseError)
                        }
                    };
                    self.send_values(&[reply])?
                }
                Message::PeriodicUpdate => {
                    let values = self
                        .update_subscriptions()
                        .chain_err(|| "failed to update subscriptions")?;
                    self.send_values(&values)?
                }
                Message::Done => return Ok(()),
            }
        }
    }

    fn handle_value(&mut self, cmd: Value, empty_params: &Value) -> Value {
        let start_time = Instant::now();
        match (
            cmd.get("method"),
            cmd.get("params").unwrap_or_else(|| empty_params),
            cmd.get("id"),
        ) {
            (Some(&Value::String(ref method)), &Value::Array(ref params), Some(ref id)) => {
                let reply = self.handle_command(method, params, id).unwrap_or_else(|e| {
                    json_rpc_error(
                        format!("{} failed: {}", method, e),
                        Some(id),
                        JsonRpcV2Error::InternalError,
                    )
                });

                conditionally_log_rpc_event!(
                    self,
                    json!({
                        "event": "rpc_response",
                        "method": method,
                        "params": if self.rpc_logging.hide_params {
                                Value::Null
                            } else {
                                json!(params)
                            },
                        "request_size": serde_json::to_vec(&cmd).map(|v| v.len()).unwrap_or(0),
                        "response_size": reply.to_string().as_bytes().len(),
                        "duration_micros": start_time.elapsed().as_micros(),
                        "id": id,
                    })
                );

                reply
            }
            _ => {
                warn!("[{}] invalid request: {}", self.addr, cmd);
                json_rpc_error("invalid request", cmd.get("id"), JsonRpcV2Error::InvalidRequest)
            }
        }
    }

    #[trace]
    fn parse_requests(mut reader: BufReader<TcpStream>, tx: &SyncSender<Message>) -> Result<()> {
        loop {
            let mut line = Vec::<u8>::new();
            reader
                .read_until(b'\n', &mut line)
                .chain_err(|| "failed to read a request")?;
            if line.is_empty() {
                return Ok(());
            } else {
                if line.starts_with(&[22, 3, 1]) {
                    // (very) naive SSL handshake detection
                    bail!("invalid request - maybe SSL-encrypted data?: {:?}", line)
                }
                match String::from_utf8(line) {
                    Ok(req) => tx
                        .send(Message::Request(req))
                        .chain_err(|| "channel closed")?,
                    Err(err) => {
                        bail!("invalid UTF8: {}", err)
                    }
                }
            }
        }
    }

    fn reader_thread(reader: BufReader<TcpStream>, tx: SyncSender<Message>) -> Result<()> {
        let result = Connection::parse_requests(reader, &tx);
        if let Err(e) = tx.send(Message::Done) {
            // The writer already tore the channel down (e.g. after a write
            // error or connection expiry) — expected during teardown races.
            debug!("failed closing channel: {}", e);
        }
        result
    }

    pub fn run(mut self, receiver: Receiver<Message>) {
        self.stats.clients.inc();
        conditionally_log_rpc_event!(self, json!({ "event": "connection_established" }));

        let reader = BufReader::new(self.stream.try_clone().expect("failed to clone TcpStream"));
        let sender = self.sender.clone();
        let child = spawn_thread("reader", || Connection::reader_thread(reader, sender));
        if let Err(e) = self.handle_replies(receiver) {
            if is_disconnect(&e) {
                // client went away mid-exchange (broken pipe / reset) — not actionable
                debug!("[{}] connection closed by client: {}", self.addr, e);
            } else {
                error!(
                    "[{}] connection handling failed: {}",
                    self.addr,
                    e.display_chain().to_string()
                );
            }
        }
        self.stats.clients.dec();
        self.stats
            .subscriptions
            .sub(self.status_hashes.len() as i64);

        debug!("[{}] shutting down connection", self.addr);
        conditionally_log_rpc_event!(self, json!({ "event": "connection_closed" }));

        let _ = self.stream.shutdown(Shutdown::Both);
        if let Err(err) = child.join().expect("receiver panicked") {
            if is_disconnect(&err) || is_channel_closed(&err) {
                // Reader failures rooted in a disconnect or in the reply
                // channel tearing down are expected when the socket was shut
                // down under it (client reset or expiry).
                debug!("[{}] receiver closed: {}", self.addr, err);
            } else {
                error!("[{}] receiver failed: {}", self.addr, err);
            }
        }
    }
}

fn connection_lifetime(max_age: Option<Duration>) -> Option<Duration> {
    max_age.and_then(|max_age| {
        let max_secs = max_age.as_secs();
        if max_secs == 0 {
            return None;
        }
        let min_secs = max_secs / 2 + max_secs % 2;
        Some(Duration::from_secs(
            rand::rng().random_range(min_secs..=max_secs),
        ))
    })
}

/// The absolute deadline for a new connection, jittered between 50% and 100% of
/// `max_age`. `None` means the connection never expires — either the max age is
/// disabled, or it is so large that the deadline is not representable.
fn connection_deadline(max_age: Option<Duration>) -> Option<Instant> {
    connection_lifetime(max_age).and_then(|lifetime| Instant::now().checked_add(lifetime))
}

/// A connection registered with the reaper: shut `stream` down at `expires_at`.
struct ConnectionExpiry {
    expires_at: Instant,
    addr: SocketAddr,
    // Weak so the reaper never keeps the socket (and its fd) alive after the
    // connection ends on its own before the deadline.
    stream: Weak<TcpStream>,
}

enum ReaperMessage {
    Register(ConnectionExpiry),
    Shutdown,
}

// Ordered by soonest deadline first, so a BinaryHeap acts as a min-heap.
impl Ord for ConnectionExpiry {
    fn cmp(&self, other: &Self) -> Ordering {
        other.expires_at.cmp(&self.expires_at)
    }
}

impl PartialOrd for ConnectionExpiry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ConnectionExpiry {
    fn eq(&self, other: &Self) -> bool {
        self.expires_at == other.expires_at
    }
}

impl Eq for ConnectionExpiry {}

/// Compact the expiry queue once it reaches this many entries.
const REAPER_COMPACT_MIN: usize = 1024;

/// Pending connection expiries, ordered by soonest deadline. Entries for
/// connections that ended before their deadline are dropped by an amortized
/// compaction pass on registration, so the queue stays proportional to the
/// number of live connections even under high connection churn combined with
/// a large max age.
struct ExpiryQueue {
    expiries: BinaryHeap<ConnectionExpiry>,
    compact_at: usize,
}

impl ExpiryQueue {
    fn new() -> ExpiryQueue {
        ExpiryQueue {
            expiries: BinaryHeap::new(),
            compact_at: REAPER_COMPACT_MIN,
        }
    }

    fn register(&mut self, expiry: ConnectionExpiry) {
        self.expiries.push(expiry);
        if self.expiries.len() >= self.compact_at {
            self.expiries.retain(|e| e.stream.strong_count() > 0);
            self.compact_at = REAPER_COMPACT_MIN.max(self.expiries.len() * 2);
        }
    }

    /// Shut down every connection whose deadline has passed.
    fn reap_due(&mut self, now: Instant) {
        while self.expiries.peek().map_or(false, |e| e.expires_at <= now) {
            let expiry = self.expiries.pop().unwrap();
            // A connection that already ended no longer upgrades.
            if let Some(stream) = expiry.stream.upgrade() {
                debug!(
                    "[{}] maximum connection age reached, closing connection",
                    expiry.addr
                );
                let _ = stream.shutdown(Shutdown::Both);
            }
        }
    }

    fn next_deadline(&self) -> Option<Instant> {
        self.expiries.peek().map(|e| e.expires_at)
    }
}

/// Enforces the maximum connection age for all Electrum RPC connections by
/// shutting each socket down at its absolute deadline. The shutdown unblocks
/// both peer threads even when the writer is stuck in a blocking write to a
/// client that stopped reading, which an in-band expiry check between messages
/// could never catch. Runs until a `Shutdown` message arrives or the
/// registration channel is closed.
fn reap_expired_connections(registrations: Receiver<ReaperMessage>) {
    let mut queue = ExpiryQueue::new();
    loop {
        queue.reap_due(Instant::now());
        let next = match queue.next_deadline() {
            Some(deadline) => {
                registrations.recv_timeout(deadline.saturating_duration_since(Instant::now()))
            }
            None => registrations.recv().map_err(RecvTimeoutError::from),
        };
        match next {
            Ok(ReaperMessage::Register(registration)) => queue.register(registration),
            Ok(ReaperMessage::Shutdown) | Err(RecvTimeoutError::Disconnected) => return,
            Err(RecvTimeoutError::Timeout) => {}
        }
    }
}

/// True if the error chain is rooted in a client disconnect (broken pipe /
/// connection reset / aborted), which is expected and shouldn't be logged as ERROR.
fn is_disconnect(err: &Error) -> bool {
    use std::io::ErrorKind::*;
    let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(e) = cause {
        if let Some(io_err) = e.downcast_ref::<std::io::Error>() {
            if matches!(
                io_err.kind(),
                BrokenPipe | ConnectionReset | ConnectionAborted | UnexpectedEof
            ) {
                return true;
            }
        }
        cause = e.source();
    }
    false
}

/// True if the error chain is rooted in the reply channel closing, meaning the
/// writer half tore down first (e.g. on connection expiry or a write error)
/// while the reader still had a request in flight — expected during teardown.
fn is_channel_closed(err: &Error) -> bool {
    let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(e) = cause {
        if e.downcast_ref::<mpsc::SendError<Message>>().is_some() {
            return true;
        }
        cause = e.source();
    }
    false
}

#[trace]
fn get_history(
    query: &Query,
    scripthash: &[u8],
    txs_limit: usize,
) -> Result<Vec<(Txid, Option<BlockId>)>> {
    // to avoid silently trunacting history entries, ask for one extra more than the limit and fail if it exists
    let history_txids = query.history_txids(scripthash, txs_limit + 1);
    ensure!(history_txids.len() <= txs_limit, ErrorKind::TooPopular);
    Ok(history_txids)
}

#[derive(Serialize, Debug)]
struct GetHistoryResult {
    #[serde(rename = "tx_hash")]
    txid: Txid,
    height: isize,
    #[serde(skip_serializing_if = "Option::is_none")]
    fee: Option<u64>,
}

#[derive(Debug)]
pub enum Message {
    Request(String),
    PeriodicUpdate,
    Done,
}

pub enum Notification {
    Periodic,
    Exit,
}

pub struct RPC {
    notification: Sender<Notification>,
    server: Option<thread::JoinHandle<()>>, // so we can join the server while dropping this ojbect
}

struct Stats {
    latency: HistogramVec,
    clients: Gauge,
    subscriptions: Gauge,
}

impl RPC {
    fn start_notifier(
        notification: Channel<Notification>,
        senders: Arc<Mutex<Vec<SyncSender<Message>>>>,
        acceptor: Sender<Option<(Arc<TcpStream>, SocketAddr)>>,
    ) {
        spawn_thread("notification", move || {
            for msg in notification.receiver().iter() {
                let mut senders = senders.lock().unwrap();
                match msg {
                    Notification::Periodic => {
                        senders.retain(|sender| {
                            if let Err(TrySendError::Disconnected(_)) =
                                sender.try_send(Message::PeriodicUpdate)
                            {
                                false // drop disconnected clients
                            } else {
                                true
                            }
                        })
                    }
                    Notification::Exit => {
                        if acceptor.send(None).is_err() {
                            warn!("acceptor already shut down before Exit notification");
                        }
                    }
                }
            }
        });
    }

    fn start_acceptor(
        addr: SocketAddr,
        conn_max_age: Option<Duration>,
        reaper: Option<mpsc::Sender<ReaperMessage>>,
    ) -> Channel<Option<(Arc<TcpStream>, SocketAddr)>> {
        let chan = Channel::unbounded();
        let acceptor = chan.sender();
        spawn_thread("acceptor", move || {
            let socket = create_socket(&addr);
            socket.listen(511).expect("setting backlog failed");
            socket
                .set_nonblocking(false)
                .expect("cannot set nonblocking to false");
            let listener = TcpListener::from(socket);

            info!("Electrum RPC server running on {}", addr);
            loop {
                let (stream, addr) = listener.accept().expect("accept failed");
                stream
                    .set_nonblocking(false)
                    .expect("failed to set connection as blocking");
                stream.set_nodelay(true).expect("failed to set TCP_NODELAY");
                // Register with the reaper before enqueueing, so the deadline
                // is anchored to the accept and enforced even while the socket
                // waits behind other new connections during an accept burst.
                let stream = Arc::new(stream);
                if let Some(reaper) = &reaper {
                    if let Some(expires_at) = connection_deadline(conn_max_age) {
                        let _ = reaper.send(ReaperMessage::Register(ConnectionExpiry {
                            expires_at,
                            addr,
                            stream: Arc::downgrade(&stream),
                        }));
                    }
                }
                if acceptor.send(Some((stream, addr))).is_err() {
                    break; // receiver dropped, server is shutting down
                }
            }
        });
        chan
    }

    pub fn start(
        config: Arc<Config>,
        query: Arc<Query>,
        metrics: &Metrics,
        salt_rwlock: Arc<RwLock<String>>
    ) -> RPC {
        let stats = Arc::new(Stats {
            latency: metrics.histogram_vec(
                HistogramOpts::new("electrum_rpc", "Electrum RPC latency (seconds)"),
                &["method"],
            ),
            clients: metrics.gauge(MetricOpts::new("electrum_clients", "# of Electrum clients")),
            subscriptions: metrics.gauge(MetricOpts::new(
                "electrum_subscriptions",
                "# of Electrum subscriptions",
            )),
        });
        stats.clients.set(0);
        stats.subscriptions.set(0);

        let notification = Channel::unbounded();

        // Discovery is enabled when electrum-public-hosts is set
        #[cfg(feature = "electrum-discovery")]
        let discovery = config.electrum_public_hosts.clone().map(|hosts| {
            use crate::chain::genesis_hash;
            let features = ServerFeatures {
                hosts,
                server_version: format!("electrs-esplora {}", ELECTRS_VERSION),
                genesis_hash: genesis_hash(config.network_type),
                protocol_min: PROTOCOL_VERSION,
                protocol_max: PROTOCOL_VERSION,
                hash_function: "sha256".into(),
                pruning: None,
            };
            let discovery = Arc::new(DiscoveryManager::new(
                config.network_type,
                features,
                PROTOCOL_VERSION,
                config.electrum_announce,
                config.tor_proxy,
            ));
            DiscoveryManager::spawn_jobs_thread(Arc::clone(&discovery));
            discovery
        });

        let rpc_addr = config.electrum_rpc_addr;
        let txs_limit = config.electrum_txs_limit;
        let subscription_limit = config.electrum_subscription_limit;
        let conn_max_age = config.electrum_rpc_conn_max_age;

        RPC {
            notification: notification.sender(),
            server: Some(spawn_thread("rpc", move || {
                let senders = Arc::new(Mutex::new(Vec::<SyncSender<Message>>::new()));

                // The reaper enforces the maximum connection age. It is
                // stopped with an explicit Shutdown message below, since the
                // acceptor's sender clone can outlive this thread (the
                // acceptor stays blocked in accept() during shutdown).
                let reaper = conn_max_age.map(|_| {
                    let (sender, receiver) = mpsc::channel();
                    let handle = spawn_thread("reaper", move || reap_expired_connections(receiver));
                    (sender, handle)
                });

                let acceptor = RPC::start_acceptor(
                    rpc_addr,
                    conn_max_age,
                    reaper.as_ref().map(|(sender, _)| sender.clone()),
                );
                RPC::start_notifier(notification, senders.clone(), acceptor.sender());

                let mut threads = HashMap::new();
                let (garbage_sender, garbage_receiver) = crossbeam_channel::unbounded();

                while let Some((stream, addr)) = acceptor.receiver().recv().unwrap() {
                    // explicitly scope the shadowed variables for the new thread
                    let query = Arc::clone(&query);
                    let stats = Arc::clone(&stats);
                    let garbage_sender = garbage_sender.clone();
                    let rpc_logging = config.rpc_logging.clone();
                    #[cfg(feature = "electrum-discovery")]
                    let discovery = discovery.clone();

                    let (sender, receiver) = mpsc::sync_channel(10);
                    senders.lock().unwrap().push(sender.clone());

                    let salt = salt_rwlock.read().unwrap().clone();

                    let spawned = spawn_thread("peer", move || {
                        debug!("[{}] connected peer", addr);
                        let conn = Connection::new(
                            query,
                            stream,
                            addr,
                            sender,
                            stats,
                            txs_limit,
                            subscription_limit,
                            #[cfg(feature = "electrum-discovery")]
                            discovery,
                            rpc_logging,
                            salt,
                        );
                        conn.run(receiver);
                        debug!("[{}] disconnected peer", addr);
                        let _ = garbage_sender.send(std::thread::current().id());
                    });

                    trace!("[{}] spawned {:?}", addr, spawned.thread().id());
                    threads.insert(spawned.thread().id(), spawned);
                    while let Ok(id) = garbage_receiver.try_recv() {
                        if let Some(thread) = threads.remove(&id) {
                            trace!("[{}] joining {:?}", addr, id);
                            if let Err(error) = thread.join() {
                                error!("failed to join {:?}: {:?}", id, error);
                            }
                        }
                    }
                }

                trace!("closing {} RPC connections", senders.lock().unwrap().len());
                for sender in senders.lock().unwrap().iter() {
                    let _ = sender.send(Message::Done);
                }

                for (id, thread) in threads {
                    trace!("joining {:?}", id);
                    if let Err(error) = thread.join() {
                        error!("failed to join {:?}: {:?}", id, error);
                    }
                }

                trace!("RPC connections are closed");

                if let Some((sender, handle)) = reaper {
                    let _ = sender.send(ReaperMessage::Shutdown);
                    handle.join().expect("reaper panicked");
                    trace!("reaper stopped");
                }
            })),
        }
    }

    pub fn notify(&self) {
        self.notification.send(Notification::Periodic).unwrap();
    }
}

impl Drop for RPC {
    fn drop(&mut self) {
        trace!("stop accepting new RPCs");
        self.notification.send(Notification::Exit).unwrap();
        if let Some(handle) = self.server.take() {
            handle.join().unwrap();
        }
        trace!("RPC server is stopped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_ip_with_salt() {
        // SHA-256("test_salt" || "127.0.0.1")
        let result = hash_ip_with_salt("test_salt", "127.0.0.1");
        assert_eq!(
            result,
            "d474826bbd126d38bdfb1e61bf727a2d9a306ea1645071faf2638cc3891a2b30"
        );
    }

    fn tracking(count: usize) -> HashMap<Sha256dHash, Value> {
        (0..count)
            .map(|i| (scripthash(i as u64), Value::Null))
            .collect()
    }

    fn scripthash(seed: u64) -> Sha256dHash {
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&seed.to_le_bytes());
        Sha256dHash::from_byte_array(bytes)
    }

    #[test]
    fn subscription_limit_of_zero_is_unlimited() {
        let tracked = tracking(1_000);
        assert!(subscription_allowed(&tracked, &scripthash(200), 0));
    }

    #[test]
    fn subscription_allowed_below_the_limit() {
        let tracked = tracking(3);
        assert!(subscription_allowed(&tracked, &scripthash(200), 4));
    }

    #[test]
    fn new_subscription_refused_at_the_limit() {
        let tracked = tracking(4);
        assert!(!subscription_allowed(&tracked, &scripthash(200), 4));
        assert!(!subscription_allowed(&tracked, &scripthash(200), 2));
    }

    #[test]
    fn resubscribing_to_a_tracked_scripthash_is_allowed_at_the_limit() {
        let tracked = tracking(4);
        assert!(subscription_allowed(&tracked, &scripthash(0), 4));
        assert!(subscription_allowed(&tracked, &scripthash(3), 4));
    }

    #[test]
    fn too_many_subscriptions_is_a_bad_request() {
        let error = ErrorKind::TooManySubscriptions(4).into();
        assert!(jsonrpc_code(&error) == JsonRpcV2Error::BadRequest);
        assert_eq!(jsonrpc_code(&error).into_i16(), 1);
    }

    #[test]
    fn connection_lifetime_is_disabled_without_max_age() {
        assert_eq!(connection_lifetime(None), None);
        assert_eq!(connection_lifetime(Some(Duration::ZERO)), None);
    }

    #[test]
    fn connection_lifetime_is_jittered_up_to_max_age() {
        let max_age = Duration::from_secs(3_600);
        for _ in 0..100 {
            let lifetime = connection_lifetime(Some(max_age)).unwrap();
            assert!(lifetime >= Duration::from_secs(1_800));
            assert!(lifetime <= max_age);
        }
    }

    #[test]
    fn connection_expiry_does_not_overflow() {
        // A max age too large to be representable as a deadline must mean
        // "never expires", not a panic.
        assert_eq!(
            connection_deadline(Some(Duration::from_secs(u64::MAX))),
            None
        );
        assert_eq!(connection_deadline(None), None);
        assert!(connection_deadline(Some(Duration::from_secs(3_600))).is_some());
    }

    /// Spawn a reaper and register `stream` to expire at `expires_at`.
    fn spawn_reaper(stream: &Arc<TcpStream>, expires_at: Instant) -> mpsc::Sender<ReaperMessage> {
        let (registrations, receiver) = mpsc::channel();
        thread::spawn(move || reap_expired_connections(receiver));
        registrations
            .send(ReaperMessage::Register(ConnectionExpiry {
                expires_at,
                addr: stream.peer_addr().unwrap(),
                stream: Arc::downgrade(stream),
            }))
            .unwrap();
        registrations
    }

    #[test]
    fn reaper_stops_on_shutdown_message() {
        let (registrations, receiver) = mpsc::channel();
        let reaper = thread::spawn(move || reap_expired_connections(receiver));
        // A pending far-future registration must not delay the shutdown.
        registrations
            .send(ReaperMessage::Register(ConnectionExpiry {
                expires_at: Instant::now() + Duration::from_secs(3_600),
                addr: "127.0.0.1:1".parse().unwrap(),
                stream: std::sync::Weak::new(),
            }))
            .unwrap();
        registrations.send(ReaperMessage::Shutdown).unwrap();
        reaper.join().unwrap();
    }

    #[test]
    fn reaper_closes_connection_of_client_that_stops_reading() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        // A client that never reads its socket: writes towards it fill the
        // kernel buffers and then block indefinitely.
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let stream = Arc::new(listener.accept().unwrap().0);

        let started = Instant::now();
        let expires_at = started + Duration::from_millis(200);
        let _registrations = spawn_reaper(&stream, expires_at);

        let (done_sender, done_receiver) = mpsc::channel();
        let writer = Arc::clone(&stream);
        thread::spawn(move || {
            let chunk = [0u8; 1 << 20];
            while (&*writer).write_all(&chunk).is_ok() {}
            let _ = done_sender.send(());
        });

        // The blocked write must be forced to fail at the deadline, not linger
        // for as long as the client keeps the socket open.
        done_receiver
            .recv_timeout(Duration::from_secs(30))
            .expect("writer stayed blocked past the connection deadline");
        assert!(started.elapsed() >= Duration::from_millis(200));
        drop(client);
    }

    #[test]
    fn reaper_queue_drops_entries_of_finished_connections() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let _client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (live_stream, addr) = listener.accept().unwrap();
        let live_stream = Arc::new(live_stream);

        // A Weak whose connection already ended (strong count is zero).
        let dead = {
            let stream = Arc::new(TcpStream::connect(listener.local_addr().unwrap()).unwrap());
            Arc::downgrade(&stream)
        };

        // Far-future deadlines: without compaction, the entries below would
        // all sit in the queue until the deadline no matter that their
        // connections are long gone.
        let expires_at = Instant::now() + Duration::from_secs(3_600);
        let mut queue = ExpiryQueue::new();
        for _ in 0..(REAPER_COMPACT_MIN - 1) {
            queue.register(ConnectionExpiry {
                expires_at,
                addr,
                stream: dead.clone(),
            });
        }
        // This registration reaches the compaction threshold, so the pass runs
        // with both the dead entries and this live one in the queue.
        queue.register(ConnectionExpiry {
            expires_at,
            addr,
            stream: Arc::downgrade(&live_stream),
        });

        // Compaction dropped the dead entries and kept the live one.
        assert_eq!(queue.expiries.len(), 1);
        assert!(queue.expiries.peek().unwrap().stream.upgrade().is_some());
    }

    #[test]
    fn reply_channel_teardown_is_not_logged_as_error() {
        let (sender, receiver) = mpsc::sync_channel(1);
        drop(receiver);
        let err: Error = sender
            .send(Message::Done)
            .chain_err(|| "channel closed")
            .unwrap_err();
        assert!(is_channel_closed(&err));
        assert!(!is_channel_closed(&"unrelated".into()));
    }

    #[test]
    fn reaper_does_not_keep_finished_connections_alive() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let _client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let stream = Arc::new(listener.accept().unwrap().0);

        let weak = Arc::downgrade(&stream);
        let _registrations = spawn_reaper(&stream, Instant::now() + Duration::from_secs(3_600));

        // The reaper holds only a Weak: once the connection is done with the
        // stream, the socket must close immediately instead of staying open
        // (leaking the fd) until the deadline.
        drop(stream);
        assert!(weak.upgrade().is_none());
    }
}
