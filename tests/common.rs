use std::str::FromStr;
use std::sync::{Arc, Once, RwLock};
use std::{env, net};

use log::LevelFilter;
use stderrlog::StdErrLog;
use tempfile::TempDir;

use serde_json::json;
#[cfg(feature = "liquid")]
use serde_json::Value;

#[cfg(not(feature = "liquid"))]
use corepc_node::{self as noded, client::client_sync as nclient, Client, Node as NodeD};
#[cfg(feature = "liquid")]
use elementsd::{self as noded, bitcoincore_rpc as nclient, ElementsD as NodeD};

#[cfg(feature = "liquid")]
use nclient::{Client, RpcApi};

use electrs::{
    chain::{Address, BlockHash, Network, Txid},
    config::{Config, RpcLogging},
    daemon::Daemon,
    electrum::RPC as ElectrumRPC,
    metrics::Metrics,
    new_index::{ChainQuery, FetchFrom, Indexer, Mempool, Query, Store},
    rest,
    signal::Waiter,
};

pub struct TestRunner {
    config: Arc<Config>,
    /// bitcoind::BitcoinD or an elementsd::ElementsD in liquid mode
    node: NodeD,
    _electrsdb: TempDir, // rm'd when dropped
    indexer: Indexer,
    query: Arc<Query>,
    daemon: Arc<Daemon>,
    mempool: Arc<RwLock<Mempool>>,
    metrics: Metrics,
    salt_rwlock: Arc<RwLock<String>>,
}

impl TestRunner {
    pub fn new() -> Result<TestRunner> {
        let log = init_log();

        // Setup the bitcoind/elementsd config
        let mut node_conf = noded::Conf::default();
        {
            #[cfg(not(feature = "liquid"))]
            let node_conf = &mut node_conf;
            #[cfg(feature = "liquid")]
            let node_conf = &mut node_conf.0;

            #[cfg(feature = "liquid")]
            node_conf.args.push("-anyonecanspendaremine=1");

            node_conf.view_stdout = std::env::var_os("RUST_LOG").is_some();
        }

        // Setup node
        let node = NodeD::with_conf(noded::exe_path().unwrap(), &node_conf).unwrap();

        #[cfg(not(feature = "liquid"))]
        let (node_client, params) = (&node.client, &node.params);
        #[cfg(feature = "liquid")]
        let (node_client, params) = (node.client(), &node.params());

        log::info!("node params: {:?}", params);

        generate(node_client, 101).chain_err(|| "failed initializing blocks")?;

        // Needed to claim the initialfreecoins as our own
        // See https://github.com/ElementsProject/elements/issues/956
        #[cfg(feature = "liquid")]
        node_client.call::<Value>("rescanblockchain", &[])?;

        #[cfg(not(feature = "liquid"))]
        let network_type = Network::Regtest;
        #[cfg(feature = "liquid")]
        let network_type = Network::LiquidRegtest;

        let mut daemon_subdir = params.cookie_file.clone();
        // drop `.cookie` filename, leaving just the network subdirectory
        daemon_subdir.pop();

        let electrsdb = tempfile::tempdir().unwrap();

        let config = Arc::new(Config {
            log,
            network_type,
            db_path: electrsdb.path().to_path_buf(),
            daemon_dir: daemon_subdir.clone(),
            daemon_parallelism: 3,
            blocks_dir: daemon_subdir.join("blocks"),
            daemon_rpc_addr: params.rpc_socket.into(),
            cookie: None,
            electrum_rpc_addr: rand_available_addr(),
            http_addr: rand_available_addr(),
            http_socket_file: None, // XXX test with socket file or tcp?
            monitoring_addr: rand_available_addr(),
            jsonrpc_import: false,
            light_mode: false,
            address_search: true,
            index_unspendables: false,
            cors: None,
            precache_scripts: None,
            utxos_limit: 100,
            electrum_txs_limit: 100,
            electrum_banner: "".into(),
            rpc_logging: RpcLogging::default(),
            zmq_addr: None,

            #[cfg(feature = "liquid")]
            asset_db_path: None, // XXX
            #[cfg(feature = "liquid")]
            parent_network: bitcoin::Network::Regtest,
            db_block_cache_mb: 8,
            db_parallelism: 2,
            db_write_buffer_size_mb: 256,
            initial_sync_batch_size: 250,
            db_cache_index_filter_blocks: false,
            //#[cfg(feature = "electrum-discovery")]
            //electrum_public_hosts: Option<crate::electrum::ServerHosts>,
            //#[cfg(feature = "electrum-discovery")]
            //electrum_announce: bool,
            //#[cfg(feature = "electrum-discovery")]
            //tor_proxy: Option<std::net::SocketAddr>,
        });

        let signal = Waiter::start(crossbeam_channel::never());
        let metrics = Metrics::new(rand_available_addr());
        metrics.start();

        let daemon = Arc::new(Daemon::new(
            &config.daemon_dir,
            &config.blocks_dir,
            config.daemon_rpc_addr,
            config.daemon_parallelism,
            config.cookie_getter(),
            config.network_type,
            signal.clone(),
            &metrics,
        )?);

        let store = Arc::new(Store::open(&config, &metrics, true));

        let fetch_from = if !env::var("JSONRPC_IMPORT").is_ok() && !cfg!(feature = "liquid") {
            // run the initial indexing from the blk files then switch to using the jsonrpc,
            // similarly to how electrs is typically used.
            FetchFrom::BlkFiles
        } else {
            // when JSONRPC_IMPORT is set, use the jsonrpc for the initial indexing too.
            // this runs faster on small regtest chains and can be useful for quicker local development iteration.
            // this is also used on liquid regtest, which currently fails to parse the BlkFiles due to the magic bytes
            FetchFrom::Bitcoind
        };

        let mut indexer = Indexer::open(Arc::clone(&store), fetch_from, &config, &metrics);
        let tip = indexer.update(&daemon)?;
        indexer.fetch_from(FetchFrom::Bitcoind);

        let chain = Arc::new(ChainQuery::new(
            Arc::clone(&store),
            Arc::clone(&daemon),
            &config,
            &metrics,
        ));

        let mempool = Arc::new(RwLock::new(Mempool::new(
            Arc::clone(&chain),
            &metrics,
            Arc::clone(&config),
        )));
        assert!(Mempool::update(&mempool, &daemon, &tip)?);

        let query = Arc::new(Query::new(
            Arc::clone(&chain),
            Arc::clone(&mempool),
            Arc::clone(&daemon),
            Arc::clone(&config),
            #[cfg(feature = "liquid")]
            None, // TODO
        ));

        let salt_rwlock = Arc::new(RwLock::new(String::from("foobar")));

        Ok(TestRunner {
            config,
            node,
            _electrsdb: electrsdb,
            indexer,
            query,
            daemon,
            mempool,
            metrics,
            salt_rwlock,
        })
    }

    pub fn node_client(&self) -> &Client {
        #[cfg(not(feature = "liquid"))]
        return &self.node.client;
        #[cfg(feature = "liquid")]
        return &self.node.client();
    }

    pub fn sync(&mut self) -> Result<()> {
        let tip = self.indexer.update(&self.daemon)?;
        assert!(Mempool::update(&self.mempool, &self.daemon, &tip)?);
        // force an update for the mempool stats, which are normally cached
        self.mempool.write().unwrap().update_backlog_stats();
        Ok(())
    }

    pub fn mine(&mut self) -> Result<BlockHash> {
        let mut generated = generate(self.node_client(), 1)?;
        self.sync()?;
        Ok(generated.remove(0))
    }

    pub fn send(&mut self, addr: &Address, amount: bitcoin::Amount) -> Result<Txid> {
        // Must use raw call() because send_to_address() expects a bitcoin::Address and not an elements::Address
        let txid = self.node_client().call(
            "sendtoaddress",
            &[addr.to_string().into(), json!(amount.to_btc())],
        )?;
        self.sync()?;
        Ok(txid)
    }

    #[cfg(feature = "liquid")]
    pub fn send_asset(
        &mut self,
        addr: &Address,
        amount: bitcoin::Amount,
        assetid: elements::AssetId,
    ) -> Result<Txid> {
        let txid = self.node_client().call(
            "sendtoaddress",
            &[
                addr.to_string().into(),
                json!(amount.to_btc()),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                json!(assetid),
            ],
        )?;
        self.sync()?;
        Ok(txid)
    }

    /// Generate and return a new address.
    /// Returns the unconfidential address in Liquid mode, to make it interchangeable with Bitcoin addresses in tests.
    pub fn newaddress(&self) -> Result<Address> {
        #[cfg(not(feature = "liquid"))]
        return Ok(raw_new_address(self.node_client())?);

        #[cfg(feature = "liquid")]
        return Ok(self.ct_newaddress()?.1);
    }
    /// Generate a new address, returning both the confidential and non-confidential versions
    #[cfg(feature = "liquid")]
    pub fn ct_newaddress(&self) -> Result<(Address, Address)> {
        let client = self.node_client();
        let c_addr = raw_new_address(client)?;
        let mut info = client.call::<Value>("getaddressinfo", &[c_addr.to_string().into()])?;
        let uc_addr = serde_json::from_value(info["unconfidential"].take())?;
        Ok((c_addr, uc_addr))
    }

    // Utility functions to iron out some differences between `elementsd` which
    // internally uses `bitcoincore-rpc` and `corepc-node` which uses `corerpc-client`

    pub fn get_best_block_hash(&self) -> Result<BlockHash> {
        let bestblockhash = self.node_client().get_best_block_hash()?;
        #[cfg(not(feature = "liquid"))] // from corepc_types::GetBestBlockHash to bitcoin::BlockHash
        let bestblockhash = bestblockhash.0.parse().unwrap();
        #[cfg(feature = "liquid")] // from bitcoin::BlockHash to elements::BlockHash
        let bestblockhash = BlockHash::from_raw_hash(bestblockhash.to_raw_hash());
        Ok(bestblockhash)
    }

    pub fn get_block_count(&self) -> Result<u64> {
        let blockcount = self.node_client().get_block_count()?;
        #[cfg(not(feature = "liquid"))]
        let blockcount = blockcount.0;
        Ok(blockcount)
    }

    pub fn get_block_hash(&self, height: u64) -> Result<BlockHash> {
        let blockhash = self.node_client().get_block_hash(height)?;
        #[cfg(not(feature = "liquid"))] // from corepc_types::GetBlockHash to bitcoin::BlockHash
        let blockhash = blockhash.block_hash().unwrap();
        #[cfg(feature = "liquid")] // from bitcoin::BlockHash to elements::BlockHash
        let blockhash = BlockHash::from_raw_hash(blockhash.to_raw_hash());
        Ok(blockhash)
    }

    // currently not used in liquid mode

    #[cfg(not(feature = "liquid"))]
    pub fn get_raw_transaction(&self, txid: Txid) -> Result<bitcoin::Transaction> {
        Ok(self
            .node_client()
            .get_raw_transaction(txid)?
            .transaction()
            .unwrap())
    }
}

pub fn init_rest_tester() -> Result<(rest::Handle, net::SocketAddr, TestRunner)> {
    let tester = TestRunner::new()?;
    let addr = tester.config.http_addr;
    let rest_server = rest::start(Arc::clone(&tester.config), Arc::clone(&tester.query));
    wait_for_tcp(addr, "REST");
    Ok((rest_server, addr, tester))
}
pub fn init_electrum_tester() -> Result<(ElectrumRPC, net::SocketAddr, TestRunner)> {
    let tester = TestRunner::new()?;
    let addr = tester.config.electrum_rpc_addr;
    let electrum_server = ElectrumRPC::start(
        Arc::clone(&tester.config),
        Arc::clone(&tester.query),
        &tester.metrics,
        Arc::clone(&tester.salt_rwlock),
    );
    wait_for_tcp(addr, "Electrum");
    Ok((electrum_server, addr, tester))
}

fn wait_for_tcp(addr: net::SocketAddr, name: &str) {
    for _ in 0..50 {
        if net::TcpStream::connect(addr).is_ok() {
            log::info!("{} server running on {}", name, addr);
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    panic!("{} server failed to start on {}", name, addr);
}

#[cfg(not(feature = "liquid"))]
fn raw_new_address(client: &Client) -> nclient::Result<Address<bitcoin::address::NetworkChecked>> {
    Ok(client
        .get_new_address(None, None)?
        .address()
        .unwrap()
        .assume_checked())
}

// Returns the confidential address
#[cfg(feature = "liquid")]
fn raw_new_address(client: &Client) -> nclient::Result<Address> {
    // Must use raw call() because get_new_address() returns a bitcoin::Address and not an elements::Address
    Ok(client.call::<Address>("getnewaddress", &[])?)
}

fn generate(client: &Client, num_blocks: u32) -> nclient::Result<Vec<BlockHash>> {
    let addr = raw_new_address(client)?;
    client.call(
        "generatetoaddress",
        &[num_blocks.into(), addr.to_string().into()],
    )
}

fn init_log() -> StdErrLog {
    static ONCE: Once = Once::new();
    let mut log = stderrlog::new();
    match std::env::var("RUST_LOG") {
        Ok(e) => log.verbosity(LevelFilter::from_str(&e).unwrap_or(LevelFilter::Off)),
        Err(_) => log.verbosity(0),
    };

    // log.timestamp(stderrlog::Timestamp::Millisecond        );
    ONCE.call_once(|| log.init().expect("logging initialization failed"));
    log
}

fn rand_available_addr() -> net::SocketAddr {
    use std::collections::HashSet;
    use std::sync::Mutex;

    lazy_static::lazy_static! {
        static ref USED_PORTS: Mutex<HashSet<u16>> = Mutex::new(HashSet::new());
    }

    loop {
        let mut used = USED_PORTS.lock().unwrap();
        let socket = net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = socket.local_addr().unwrap();
        if used.insert(addr.port()) {
            return addr;
        }
    }
}

error_chain::error_chain! {
    types {
        Error, ErrorKind, ResultExt, Result;
    }

    errors {
        Electrs(e: electrs::errors::Error) {
            description("Electrs error")
            display("Electrs error: {:?}", e)
        }

        BitcoindRpc(e: nclient::Error) {
            description("Bitcoind RPC error")
            display("Bitcoind RPC error: {:?}", e)
        }

        ElectrumD(e: electrumd::Error) {
            description("Electrum wallet RPC error")
            display("Electrum wallet RPC error: {:?}", e)
        }

        Io(e: std::io::Error) {
            description("IO error")
            display("IO error: {:?}", e)
        }
        Ureq(e: ureq::Error) {
            description("ureq error")
            display("ureq error: {:?}", e)
        }
        Json(e: serde_json::Error) {
            description("JSON error")
            display("JSON error: {:?}", e)
        }
    }
}

impl From<electrs::errors::Error> for Error {
    fn from(e: electrs::errors::Error) -> Self {
        Error::from(ErrorKind::Electrs(e))
    }
}
impl From<nclient::Error> for Error {
    fn from(e: nclient::Error) -> Self {
        Error::from(ErrorKind::BitcoindRpc(e))
    }
}
impl From<electrumd::Error> for Error {
    fn from(e: electrumd::Error) -> Self {
        Error::from(ErrorKind::ElectrumD(e))
    }
}
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::from(ErrorKind::Io(e))
    }
}
impl From<ureq::Error> for Error {
    fn from(e: ureq::Error) -> Self {
        Error::from(ErrorKind::Ureq(e))
    }
}
impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::from(ErrorKind::Json(e))
    }
}
