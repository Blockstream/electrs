use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, RwLock, RwLockReadGuard};
use std::time::{Duration, Instant};

use crate::chain::{Network, OutPoint, Transaction, TxOut, Txid};
use crate::config::Config;
use crate::daemon::{Daemon, SubmitPackageResult};
use crate::errors::*;
use crate::new_index::block_template::BlockTemplateCache;
use crate::new_index::{ChainQuery, Mempool, ScriptStats, SpendingInput, Utxo};
use crate::util::{is_spendable, BlockId, Bytes, TransactionStatus};

use electrs_macros::trace;
use hyper::body::Bytes as BodyBytes;

#[cfg(feature = "liquid")]
use crate::{
    chain::AssetId,
    elements::{
        ebcompact::TxidCompat, lookup_asset, AssetMeta, AssetSearchFilters, AssetSorting,
        LiquidAsset, RegistryClient, RegistryError,
    },
};

const FEE_ESTIMATES_TTL: u64 = 60; // seconds

const CONF_TARGETS: [u16; 28] = [
    1u16, 2u16, 3u16, 4u16, 5u16, 6u16, 7u16, 8u16, 9u16, 10u16, 11u16, 12u16, 13u16, 14u16, 15u16,
    16u16, 17u16, 18u16, 19u16, 20u16, 21u16, 22u16, 23u16, 24u16, 25u16, 144u16, 504u16, 1008u16,
];

pub struct Query {
    chain: Arc<ChainQuery>, // TODO: should be used as read-only
    mempool: Arc<RwLock<Mempool>>,
    daemon: Arc<Daemon>,
    config: Arc<Config>,
    cached_estimates: RwLock<(HashMap<u16, f64>, Option<Instant>)>,
    cached_relayfee: RwLock<Option<f64>>,
    cached_block_template: BlockTemplateCache,
    #[cfg(feature = "liquid")]
    asset_registry: Option<Arc<RegistryClient>>,
}

#[cfg(feature = "liquid")]
#[derive(Debug)]
pub enum AssetRegistryStatus {
    NotRequested,
    Available,
    Stale,
    NotFound,
    Unavailable(RegistryError),
}

#[cfg(feature = "liquid")]
pub struct AssetLookup {
    pub asset: Option<LiquidAsset>,
    pub registry_status: AssetRegistryStatus,
}

impl Query {
    #[cfg(not(feature = "liquid"))]
    pub fn new(
        chain: Arc<ChainQuery>,
        mempool: Arc<RwLock<Mempool>>,
        daemon: Arc<Daemon>,
        config: Arc<Config>,
    ) -> Self {
        Query {
            chain,
            mempool,
            daemon,
            config,
            cached_estimates: RwLock::new((HashMap::new(), None)),
            cached_relayfee: RwLock::new(None),
            cached_block_template: BlockTemplateCache::new(),
        }
    }

    pub fn chain(&self) -> &ChainQuery {
        &self.chain
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn network(&self) -> Network {
        self.config.network_type
    }

    pub fn mempool(&self) -> RwLockReadGuard<'_, Mempool> {
        self.mempool.read().unwrap()
    }

    #[trace]
    pub fn broadcast_raw(&self, txhex: &str) -> Result<Txid> {
        let txid = self.daemon.broadcast_raw(txhex)?;
        let _ = self
            .mempool
            .write()
            .unwrap()
            .add_by_txid(&self.daemon, txid);
        Ok(txid)
    }

    #[trace]
    pub fn submit_package(
        &self,
        txhex: Vec<String>,
        maxfeerate: Option<f64>,
        maxburnamount: Option<f64>,
    ) -> Result<SubmitPackageResult> {
        let result = self.daemon.submit_package(txhex, maxfeerate, maxburnamount)?;
        // Add accepted txs to the local mempool so subscription status updates reflect them
        // immediately (they read from the local mempool), mirroring broadcast_raw() above.
        let accepted_txids = result.accepted_txids();
        if !accepted_txids.is_empty() {
            let _ = self
                .mempool
                .write()
                .unwrap()
                .add_by_txids(&self.daemon, &accepted_txids);
        }
        Ok(result)
    }

    #[trace]
    pub async fn getblocktemplate(&self) -> Result<BodyBytes> {
        self.cached_block_template
            .get(
                Arc::clone(&self.chain),
                Arc::clone(&self.daemon),
                self.config.network_type,
            )
            .await
    }

    #[trace]
    pub fn utxo(&self, scripthash: &[u8]) -> Result<Vec<Utxo>> {
        let mut utxos = self.chain.utxo(scripthash, self.config.utxos_limit)?;
        let mempool = self.mempool();
        utxos.retain(|utxo| !mempool.has_spend(&OutPoint::from(utxo)));
        utxos.extend(mempool.utxo(scripthash));
        Ok(utxos)
    }

    #[trace]
    pub fn history_txids(&self, scripthash: &[u8], limit: usize) -> Vec<(Txid, Option<BlockId>)> {
        let confirmed_txids = self.chain.history_txids(scripthash, limit);
        let confirmed_len = confirmed_txids.len();
        let confirmed_txids = confirmed_txids.into_iter().map(|(tx, b)| (tx, Some(b)));

        let mempool_txids = self
            .mempool()
            .history_txids(scripthash, limit - confirmed_len)
            .into_iter()
            .map(|tx| (tx, None));

        confirmed_txids.chain(mempool_txids).collect()
    }

    pub fn stats(&self, scripthash: &[u8]) -> (ScriptStats, ScriptStats) {
        (
            self.chain.stats(scripthash),
            self.mempool().stats(scripthash),
        )
    }

    #[trace]
    pub fn lookup_txn(&self, txid: &Txid) -> Option<Transaction> {
        self.chain
            .lookup_txn(txid, None)
            .or_else(|| self.mempool().lookup_txn(txid))
    }

    #[trace]
    pub fn lookup_raw_txn(&self, txid: &Txid) -> Option<Bytes> {
        self.chain
            .lookup_raw_txn(txid, None)
            .or_else(|| self.mempool().lookup_raw_txn(txid))
    }

    #[trace]
    pub fn lookup_txos(&self, outpoints: BTreeSet<OutPoint>) -> HashMap<OutPoint, TxOut> {
        // the mempool lookup_txos() internally looks up confirmed txos as well
        self.mempool()
            .lookup_txos(outpoints)
            .expect("failed loading txos")
    }

    #[trace]
    pub fn lookup_spend(&self, outpoint: &OutPoint) -> Option<SpendingInput> {
        self.chain
            .lookup_spend(outpoint)
            .or_else(|| self.mempool().lookup_spend(outpoint))
    }

    #[trace]
    pub fn lookup_tx_spends(&self, tx: &Transaction) -> Vec<Option<SpendingInput>> {
        let txid = tx.compute_txid();
        let outpoints = tx
            .output
            .iter()
            .enumerate()
            .filter(|(_, txout)| is_spendable(txout))
            .map(|(vout, _)| OutPoint::new(txid, vout as u32))
            .collect::<BTreeSet<_>>();

        // First fetch all confirmed spends using a MultiGet operation,
        // then fall back to the mempool for any outpoints not spent on-chain
        let mut chain_spends = self.chain.lookup_spends(outpoints);
        let mempool = self.mempool();
        tx.output
            .iter()
            .enumerate()
            .map(|(vout, txout)| {
                if is_spendable(txout) {
                    let outpoint = OutPoint::new(txid, vout as u32);
                    chain_spends
                        .remove(&outpoint)
                        .or_else(|| mempool.lookup_spend(&outpoint))
                } else {
                    None
                }
            })
            .collect()
    }

    #[trace]
    pub fn get_tx_status(&self, txid: &Txid) -> TransactionStatus {
        TransactionStatus::from(self.chain.tx_confirming_block(txid))
    }

    #[trace]
    pub fn get_mempool_tx_fee(&self, txid: &Txid) -> Option<u64> {
        self.mempool().get_tx_fee(txid)
    }

    #[trace]
    pub fn has_unconfirmed_parents(&self, txid: &Txid) -> bool {
        self.mempool().has_unconfirmed_parents(txid)
    }

    #[trace]
    pub fn estimate_fee(&self, conf_target: u16) -> Option<f64> {
        if self.config.network_type.is_regtest() {
            return self.get_relayfee().ok();
        }
        if let (ref cache, Some(cache_time)) = *self.cached_estimates.read().unwrap() {
            if cache_time.elapsed() < Duration::from_secs(FEE_ESTIMATES_TTL) {
                return cache.get(&conf_target).copied();
            }
        }

        self.update_fee_estimates();
        self.cached_estimates
            .read()
            .unwrap()
            .0
            .get(&conf_target)
            .copied()
    }

    #[trace]
    pub fn estimate_fee_map(&self) -> HashMap<u16, f64> {
        if let (ref cache, Some(cache_time)) = *self.cached_estimates.read().unwrap() {
            if cache_time.elapsed() < Duration::from_secs(FEE_ESTIMATES_TTL) {
                return cache.clone();
            }
        }

        self.update_fee_estimates();
        self.cached_estimates.read().unwrap().0.clone()
    }

    #[trace]
    fn update_fee_estimates(&self) {
        match self.daemon.estimatesmartfee_batch(&CONF_TARGETS) {
            Ok(estimates) => {
                *self.cached_estimates.write().unwrap() = (estimates, Some(Instant::now()));
            }
            Err(err) => {
                warn!("failed estimating feerates: {:?}", err);
            }
        }
    }

    #[trace]
    pub fn get_relayfee(&self) -> Result<f64> {
        if let Some(cached) = *self.cached_relayfee.read().unwrap() {
            return Ok(cached);
        }

        let relayfee = self.daemon.get_relayfee()?;
        self.cached_relayfee.write().unwrap().replace(relayfee);
        Ok(relayfee)
    }

    #[cfg(feature = "liquid")]
    pub fn new(
        chain: Arc<ChainQuery>,
        mempool: Arc<RwLock<Mempool>>,
        daemon: Arc<Daemon>,
        config: Arc<Config>,
        asset_registry: Option<Arc<RegistryClient>>,
    ) -> Self {
        Query {
            chain,
            mempool,
            daemon,
            config,
            asset_registry,
            cached_estimates: RwLock::new((HashMap::new(), None)),
            cached_relayfee: RwLock::new(None),
            cached_block_template: BlockTemplateCache::new(),
        }
    }

    #[cfg(feature = "liquid")]
    #[trace]
    pub fn lookup_asset_local(&self, asset_id: &AssetId) -> Result<Option<LiquidAsset>> {
        lookup_asset(self, asset_id, None)
    }

    #[cfg(feature = "liquid")]
    #[trace]
    pub async fn lookup_asset(self: Arc<Self>, asset_id: &AssetId) -> Result<AssetLookup> {
        let query = Arc::clone(&self);
        let asset_id_owned = *asset_id;
        let mut asset =
            match tokio::task::spawn_blocking(move || query.lookup_asset_local(&asset_id_owned))
                .await
                .map_err(|err| Error::from(format!("asset lookup task failed: {}", err)))??
            {
                Some(asset) => asset,
                None => {
                    return Ok(AssetLookup {
                        asset: None,
                        registry_status: AssetRegistryStatus::NotRequested,
                    })
                }
            };

        if !matches!(asset, LiquidAsset::Issued(_)) {
            return Ok(AssetLookup {
                asset: Some(asset),
                registry_status: AssetRegistryStatus::NotRequested,
            });
        }

        let registry = match &self.asset_registry {
            Some(registry) => registry,
            None => {
                return Ok(AssetLookup {
                    asset: Some(asset),
                    registry_status: AssetRegistryStatus::NotRequested,
                })
            }
        };

        let registry_status = match registry.get_asset_with_status(asset_id).await {
            Ok(lookup) => match lookup.asset {
                Some(registry_asset) => {
                    let metadata = AssetMeta::from_registry_asset(registry_asset);
                    if let LiquidAsset::Issued(issued) = &mut asset {
                        issued.meta = Some(metadata);
                    }
                    if lookup.stale {
                        AssetRegistryStatus::Stale
                    } else {
                        AssetRegistryStatus::Available
                    }
                }
                None if lookup.stale => AssetRegistryStatus::Stale,
                None => AssetRegistryStatus::NotFound,
            },
            Err(error) => AssetRegistryStatus::Unavailable(error),
        };

        Ok(AssetLookup {
            asset: Some(asset),
            registry_status,
        })
    }

    #[cfg(feature = "liquid")]
    #[trace]
    pub async fn list_registry_assets(
        self: Arc<Self>,
        start_index: usize,
        limit: usize,
        sorting: AssetSorting,
        filters: AssetSearchFilters,
    ) -> std::result::Result<(usize, Vec<LiquidAsset>), RegistryError> {
        let registry = match &self.asset_registry {
            None => return Ok((0, vec![])),
            Some(registry) => Arc::clone(registry),
        };

        let page = registry
            .list_assets(start_index, limit, sorting, &filters)
            .await?;
        let total_count = page.total_count;
        let items = page.items;
        let query = Arc::clone(&self);
        let (missing, results) = tokio::task::spawn_blocking(
            move || -> std::result::Result<(Vec<AssetId>, Vec<LiquidAsset>), RegistryError> {
                let mut results = Vec::with_capacity(items.len());
                let mut missing = Vec::new();
                for registry_asset in items {
                    let asset_id = registry_asset.asset_id;
                    let metadata = AssetMeta::from_registry_asset(Arc::new(registry_asset));
                    match lookup_asset(&query, &asset_id, Some(metadata))
                        .map_err(|error| RegistryError::LocalLookup(error.to_string()))?
                    {
                        Some(asset) => results.push(asset),
                        None => missing.push(asset_id),
                    }
                }
                Ok((missing, results))
            },
        )
        .await
        .map_err(|err| {
            RegistryError::LocalLookup(format!("asset registry lookup task failed: {}", err))
        })??;

        if !missing.is_empty() {
            return Err(RegistryError::LocalLookup(format!(
                "registered assets not yet available in the local index count='{}' asset_ids='{}'",
                missing.len(),
                missing
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            )));
        }
        Ok((total_count, results))
    }
}
