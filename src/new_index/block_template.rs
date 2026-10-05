use std::convert::TryFrom;
use std::sync::Arc;
use std::time::{Duration, Instant};

use error_chain::ChainedError;
use hyper::body::Bytes as BodyBytes;
use log::warn;
#[cfg(any(not(feature = "liquid"), test))]
use serde_json::Value;
use tokio::sync::{watch, Mutex};

use crate::chain::{BlockHash, Network};
use crate::daemon::Daemon;
use crate::errors::*;
use crate::new_index::ChainQuery;

pub const GETBLOCKTEMPLATE_TTL: u64 = 15; // seconds
const GETBLOCKTEMPLATE_FAILURE_BACKOFF: u64 = 1; // seconds

pub struct BlockTemplateCache {
    entry: Arc<Mutex<BlockTemplateCacheEntry>>,
}

enum BlockTemplateCacheEntry {
    Empty,
    Fetching(watch::Receiver<Option<Arc<FetchOutcome>>>),
    Ready(CachedBlockTemplate),
    Failed(CachedBlockTemplateFailure),
}

struct CachedBlockTemplate {
    template_tip: BlockHash,
    template_height: usize,
    observed_tip: BlockHash,
    fetched_at: Instant,
    body: BodyBytes,
}

struct CachedBlockTemplateFailure {
    failed_at: Instant,
    error: CachedFetchError,
}

enum FetchOutcome {
    Success(BodyBytes),
    Failure(CachedFetchError),
}

enum ReceivedFetchOutcome {
    Completed(Result<BodyBytes>),
    WorkerStopped,
}

#[derive(Clone)]
enum CachedFetchError {
    Connection(String),
    Rpc(i64, String, String),
    Other(String),
}

struct FetchedBlockTemplate {
    tip: BlockHash,
    template_height: usize,
    body: BodyBytes,
}

#[derive(Copy, Clone)]
struct ChainTipState {
    hash: BlockHash,
    height: usize,
}

enum TemplateTipRelation {
    Current,
    AheadOfIndex,
    Stale,
}

struct ValidatedBlockTemplate {
    fetched: FetchedBlockTemplate,
    observed_tip: BlockHash,
}

type TipProbe = Arc<dyn Fn() -> ChainTipState + Send + Sync>;

impl CachedBlockTemplate {
    fn is_valid(&self, tip: ChainTipState, ttl: Duration) -> bool {
        if self.fetched_at.elapsed() >= ttl {
            return false;
        }

        let parent_height = self
            .template_height
            .checked_sub(1)
            .expect("validated block-template height");
        if self.template_tip == tip.hash {
            return parent_height == tip.height;
        }

        // The daemon can be ahead of electrs while the index catches up. Keep that template only
        // while the locally observed tip is unchanged; any indexed advance forces a fresh probe.
        parent_height > tip.height && self.observed_tip == tip.hash
    }
}

impl CachedFetchError {
    fn from_error(error: &Error) -> Self {
        match error.kind() {
            ErrorKind::Connection(message) => Self::Connection(message.clone()),
            ErrorKind::RpcError(code, message, method) => {
                Self::Rpc(*code, message.clone(), method.clone())
            }
            _ => Self::Other(error.to_string()),
        }
    }

    fn to_error(&self) -> Error {
        match self {
            Self::Connection(message) => ErrorKind::Connection(message.clone()).into(),
            Self::Rpc(code, message, method) => {
                ErrorKind::RpcError(*code, message.clone(), method.clone()).into()
            }
            Self::Other(message) => message.clone().into(),
        }
    }
}

impl FetchOutcome {
    fn to_result(&self) -> Result<BodyBytes> {
        match self {
            Self::Success(body) => Ok(body.clone()),
            Self::Failure(error) => Err(error.to_error()),
        }
    }
}

fn classify_template_tip(
    fetched: &FetchedBlockTemplate,
    tip: ChainTipState,
) -> Result<TemplateTipRelation> {
    let parent_height = fetched
        .template_height
        .checked_sub(1)
        .chain_err(|| "block-template height must be greater than zero")?;

    if fetched.tip == tip.hash {
        if parent_height != tip.height {
            bail!(
                "block-template parent hash matches indexed tip but height {} does not match {}",
                parent_height,
                tip.height
            )
        }
        return Ok(TemplateTipRelation::Current);
    }

    if parent_height > tip.height {
        Ok(TemplateTipRelation::AheadOfIndex)
    } else {
        Ok(TemplateTipRelation::Stale)
    }
}

fn fetch_valid_template<F>(fetch: &mut F, tip_probe: &TipProbe) -> Result<ValidatedBlockTemplate>
where
    F: FnMut() -> Result<FetchedBlockTemplate>,
{
    for attempt in 0..=1 {
        let fetched = fetch()?;
        let observed_tip = tip_probe();
        match classify_template_tip(&fetched, observed_tip)? {
            TemplateTipRelation::Current | TemplateTipRelation::AheadOfIndex => {
                return Ok(ValidatedBlockTemplate {
                    fetched,
                    observed_tip: observed_tip.hash,
                })
            }
            TemplateTipRelation::Stale if attempt == 0 => continue,
            TemplateTipRelation::Stale => {
                bail!("daemon returned a stale or competing block template twice")
            }
        }
    }
    unreachable!("bounded block-template fetch loop")
}

async fn receive_fetch_outcome(
    mut receiver: watch::Receiver<Option<Arc<FetchOutcome>>>,
) -> ReceivedFetchOutcome {
    if receiver.borrow().is_none() && receiver.changed().await.is_err() {
        return ReceivedFetchOutcome::WorkerStopped;
    }
    match receiver.borrow().as_ref().cloned() {
        Some(outcome) => ReceivedFetchOutcome::Completed(outcome.to_result()),
        None => ReceivedFetchOutcome::WorkerStopped,
    }
}

impl BlockTemplateCache {
    pub fn new() -> Self {
        Self {
            entry: Arc::new(Mutex::new(BlockTemplateCacheEntry::Empty)),
        }
    }

    pub async fn get(
        &self,
        chain: Arc<ChainQuery>,
        daemon: Arc<Daemon>,
        network: Network,
    ) -> Result<BodyBytes> {
        let tip_probe: TipProbe = Arc::new(move || {
            let current = chain.best_header();
            ChainTipState {
                hash: *current.hash(),
                height: current.height(),
            }
        });
        self.get_or_fetch_with_tip_probe(
            Duration::from_secs(GETBLOCKTEMPLATE_TTL),
            Duration::from_secs(GETBLOCKTEMPLATE_FAILURE_BACKOFF),
            tip_probe,
            move || fetch_block_template(&daemon, network),
        )
        .await
    }

    async fn get_or_fetch_with_tip_probe<F>(
        &self,
        ttl: Duration,
        failure_backoff: Duration,
        tip_probe: TipProbe,
        fetch: F,
    ) -> Result<BodyBytes>
    where
        F: FnMut() -> Result<FetchedBlockTemplate> + Send + 'static,
    {
        let receiver = {
            let mut entry = self.entry.lock().await;
            if let BlockTemplateCacheEntry::Ready(cached) = &*entry {
                if cached.is_valid(tip_probe(), ttl) {
                    return Ok(cached.body.clone());
                }
            }
            if let BlockTemplateCacheEntry::Failed(failure) = &*entry {
                if failure.failed_at.elapsed() < failure_backoff {
                    return Err(failure.error.to_error());
                }
            }
            if let BlockTemplateCacheEntry::Fetching(receiver) = &*entry {
                receiver.clone()
            } else {
                let (outcome_sender, outcome_receiver) = watch::channel(None);
                *entry = BlockTemplateCacheEntry::Fetching(outcome_receiver.clone());

                let cache_entry = Arc::clone(&self.entry);
                let tip_probe = Arc::clone(&tip_probe);
                // The cache owns the worker so disconnecting the request that observed the miss
                // does not cancel the shared fetch and cause a second daemon call.
                tokio::spawn(async move {
                    let result = match tokio::task::spawn_blocking(move || {
                        let mut fetch = fetch;
                        fetch_valid_template(&mut fetch, &tip_probe)
                    })
                    .await
                    {
                        Ok(result) => result,
                        Err(error) => Err(format!("block-template worker failed: {error}").into()),
                    };

                    let (completed, outcome) = match result {
                        Ok(validated) => {
                            let body = validated.fetched.body.clone();
                            (
                                BlockTemplateCacheEntry::Ready(CachedBlockTemplate {
                                    template_tip: validated.fetched.tip,
                                    template_height: validated.fetched.template_height,
                                    observed_tip: validated.observed_tip,
                                    fetched_at: Instant::now(),
                                    body: validated.fetched.body,
                                }),
                                FetchOutcome::Success(body),
                            )
                        }
                        Err(error) => {
                            warn!("block-template fetch failed: {}", error.display_chain());
                            let cached_error = CachedFetchError::from_error(&error);
                            (
                                BlockTemplateCacheEntry::Failed(CachedBlockTemplateFailure {
                                    failed_at: Instant::now(),
                                    error: cached_error.clone(),
                                }),
                                FetchOutcome::Failure(cached_error),
                            )
                        }
                    };

                    *cache_entry.lock().await = completed;
                    outcome_sender.send_replace(Some(Arc::new(outcome)));
                });

                outcome_receiver
            }
        };

        match receive_fetch_outcome(receiver.clone()).await {
            ReceivedFetchOutcome::Completed(result) => result,
            ReceivedFetchOutcome::WorkerStopped => {
                let mut entry = self.entry.lock().await;
                if matches!(
                    &*entry,
                    BlockTemplateCacheEntry::Fetching(current)
                        if current.same_channel(&receiver)
                ) {
                    warn!("block-template worker stopped without a result; resetting cache");
                    *entry = BlockTemplateCacheEntry::Empty;
                }
                bail!("block-template worker stopped without a result")
            }
        }
    }

    #[cfg(test)]
    async fn get_or_fetch<F>(
        &self,
        current_tip: ChainTipState,
        ttl: Duration,
        fetch: F,
    ) -> Result<BodyBytes>
    where
        F: FnMut() -> Result<FetchedBlockTemplate> + Send + 'static,
    {
        self.get_or_fetch_with_failure_backoff(current_tip, ttl, Duration::ZERO, fetch)
            .await
    }

    #[cfg(test)]
    async fn get_or_fetch_with_failure_backoff<F>(
        &self,
        current_tip: ChainTipState,
        ttl: Duration,
        failure_backoff: Duration,
        fetch: F,
    ) -> Result<BodyBytes>
    where
        F: FnMut() -> Result<FetchedBlockTemplate> + Send + 'static,
    {
        let tip_probe: TipProbe = Arc::new(move || current_tip);
        self.get_or_fetch_with_tip_probe(ttl, failure_backoff, tip_probe, fetch)
            .await
    }
}

#[cfg(any(not(feature = "liquid"), test))]
fn json_template_from_value(value: Value) -> Result<FetchedBlockTemplate> {
    let previousblockhash = value
        .get("previousblockhash")
        .and_then(Value::as_str)
        .chain_err(|| "getblocktemplate response missing previousblockhash")?;
    let tip = previousblockhash
        .parse::<BlockHash>()
        .chain_err(|| "invalid getblocktemplate previousblockhash")?;
    let height = value
        .get("height")
        .and_then(Value::as_u64)
        .chain_err(|| "getblocktemplate response missing height")?;
    let height = usize::try_from(height).chain_err(|| "getblocktemplate height is too large")?;
    if height == 0 {
        bail!("getblocktemplate height must be greater than zero")
    }
    let body = BodyBytes::from(
        serde_json::to_string(&value).chain_err(|| "failed to serialize getblocktemplate")?,
    );
    Ok(FetchedBlockTemplate {
        tip,
        template_height: height,
        body,
    })
}

#[cfg(not(feature = "liquid"))]
fn fetch_block_template(daemon: &Daemon, network: Network) -> Result<FetchedBlockTemplate> {
    json_template_from_value(daemon.getblocktemplate(block_template_rules(network))?)
}

#[cfg(not(feature = "liquid"))]
fn block_template_rules(network: Network) -> &'static [&'static str] {
    match network {
        Network::Signet => &["segwit", "signet"],
        _ => &["segwit"],
    }
}

#[cfg(feature = "liquid")]
fn fetch_block_template(daemon: &Daemon, network: Network) -> Result<FetchedBlockTemplate> {
    elements_template_from_hex(daemon.getnewblockhex()?, network)
}

#[cfg(feature = "liquid")]
use std::collections::{BTreeMap, HashMap};

#[cfg(feature = "liquid")]
use bitcoin::hex::{DisplayHex, FromHex};

#[cfg(feature = "liquid")]
use crate::chain::{AssetId, Block, Transaction, Txid};

#[cfg(feature = "liquid")]
#[derive(Debug, Deserialize, Serialize)]
struct BlockTemplateResponse {
    capabilities: Vec<String>,
    version: u32,
    rules: Vec<String>,
    vbavailable: BTreeMap<String, u32>,
    vbrequired: u32,
    previousblockhash: String,
    transactions: Vec<BlockTemplateTransaction>,
    coinbaseaux: BTreeMap<String, String>,
    coinbasevalue: u64,
    target: String,
    mutable: Vec<String>,
    noncerange: String,
    curtime: u32,
    bits: String,
    height: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    default_witness_commitment: Option<String>,
}

#[cfg(feature = "liquid")]
#[derive(Debug, Deserialize, Serialize)]
struct BlockTemplateTransaction {
    data: String,
    txid: String,
    hash: String,
    depends: Vec<usize>,
    fee: u64,
    weight: usize,
}

#[cfg(feature = "liquid")]
fn elements_template_from_hex(raw_hex: String, network: Network) -> Result<FetchedBlockTemplate> {
    let bytes = Vec::from_hex(&raw_hex).chain_err(|| "invalid getnewblockhex block hex")?;
    let block: Block = elements::encode::deserialize(&bytes)
        .chain_err(|| "failed to deserialize getnewblockhex Elements block")?;
    let height = usize::try_from(block.header.height)
        .chain_err(|| "getnewblockhex block height is too large")?;
    if height == 0 {
        bail!("getnewblockhex block height must be greater than zero")
    }
    let response = BlockTemplateResponse::from_block(&block, *network.native_asset())?;
    let body = BodyBytes::from(
        serde_json::to_string(&response)
            .chain_err(|| "failed to serialize Elements block template")?,
    );
    Ok(FetchedBlockTemplate {
        tip: block.header.prev_blockhash,
        template_height: height,
        body,
    })
}

#[cfg(feature = "liquid")]
impl BlockTemplateResponse {
    fn from_block(block: &Block, policy_asset: AssetId) -> Result<Self> {
        let coinbase = block
            .txdata
            .first()
            .chain_err(|| "getnewblockhex block has no transactions")?;
        if !coinbase.is_coinbase() {
            bail!("getnewblockhex transaction zero is not coinbase")
        }
        if block.txdata.iter().skip(1).any(Transaction::is_coinbase) {
            bail!("getnewblockhex block contains multiple coinbase transactions")
        }

        let mut indexes = HashMap::<Txid, usize>::new();
        let mut transactions = Vec::with_capacity(block.txdata.len().saturating_sub(1));
        for (index, tx) in block.txdata.iter().enumerate() {
            let txid = tx.txid();
            if index == 0 {
                indexes.insert(txid, index);
                continue;
            }
            let mut depends: Vec<_> = tx
                .input
                .iter()
                .filter_map(|input| indexes.get(&input.previous_output.txid).copied())
                .filter(|dependency| *dependency != 0)
                .collect();
            depends.sort_unstable();
            depends.dedup();
            transactions.push(BlockTemplateTransaction {
                data: elements::encode::serialize_hex(tx),
                txid: txid.to_string(),
                hash: tx.wtxid().to_string(),
                depends,
                fee: transaction_fee(tx, policy_asset)?,
                weight: tx.weight(),
            });
            indexes.insert(txid, index);
        }

        Ok(Self {
            capabilities: vec![],
            version: block.header.version,
            rules: vec![],
            vbavailable: BTreeMap::new(),
            vbrequired: 0,
            previousblockhash: block.header.prev_blockhash.to_string(),
            transactions,
            coinbaseaux: BTreeMap::new(),
            coinbasevalue: coinbase_value(coinbase, policy_asset)?,
            target: "0".repeat(64),
            mutable: vec![],
            noncerange: "00000000ffffffff".to_string(),
            curtime: block.header.time,
            bits: "00000000".to_string(),
            height: block.header.height,
            default_witness_commitment: witness_commitment(coinbase),
        })
    }
}

#[cfg(feature = "liquid")]
fn transaction_fee(transaction: &Transaction, policy_asset: AssetId) -> Result<u64> {
    transaction.output.iter().try_fold(0u64, |total, output| {
        if !output.is_fee() || output.asset.explicit() != Some(policy_asset) {
            return Ok(total);
        }
        let value = output
            .value
            .explicit()
            .chain_err(|| "non-explicit policy-asset transaction fee")?;
        total
            .checked_add(value)
            .chain_err(|| "policy-asset transaction fee overflow")
    })
}

#[cfg(feature = "liquid")]
fn coinbase_value(coinbase: &Transaction, policy_asset: AssetId) -> Result<u64> {
    coinbase.output.iter().try_fold(0u64, |total, output| {
        if output.asset.explicit() != Some(policy_asset) {
            return Ok(total);
        }
        let value = output
            .value
            .explicit()
            .chain_err(|| "non-explicit policy-asset coinbase value")?;
        total
            .checked_add(value)
            .chain_err(|| "policy-asset coinbase value overflow")
    })
}

#[cfg(feature = "liquid")]
fn witness_commitment(coinbase: &Transaction) -> Option<String> {
    const PREFIX: &[u8] = &[0x6a, 0x24, 0xaa, 0x21, 0xa9, 0xed];
    coinbase.output.iter().rev().find_map(|output| {
        let script = output.script_pubkey.as_bytes();
        (script.len() >= 38 && script.starts_with(PREFIX)).then(|| script.to_lower_hex_string())
    })
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "liquid")]
    use std::convert::TryInto;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex as StdMutex};

    use super::*;
    use serde_json::json;
    use tokio::sync::Barrier;

    fn hash(byte: u8) -> BlockHash {
        format!("{:02x}", byte)
            .repeat(32)
            .parse()
            .expect("valid block hash")
    }

    fn chain_tip(byte: u8, height: usize) -> ChainTipState {
        ChainTipState {
            hash: hash(byte),
            height,
        }
    }

    fn fetched(tip: BlockHash, height: usize, body: &'static str) -> FetchedBlockTemplate {
        FetchedBlockTemplate {
            tip,
            template_height: height,
            body: BodyBytes::from_static(body.as_bytes()),
        }
    }

    #[test]
    fn json_template_preserves_unknown_fields() {
        let value = json!({
            "previousblockhash": hash(1).to_string(),
            "height": 2,
            "future-field": { "kept": true }
        });
        let fetched = json_template_from_value(value.clone()).unwrap();
        assert_eq!(fetched.template_height, 2);
        assert_eq!(
            serde_json::from_slice::<Value>(&fetched.body).unwrap(),
            value
        );

        assert!(json_template_from_value(json!({
            "previousblockhash": hash(1).to_string()
        }))
        .is_err());
        assert!(json_template_from_value(json!({
            "height": 2
        }))
        .is_err());
        assert!(json_template_from_value(json!({
            "previousblockhash": "not-a-block-hash",
            "height": 2
        }))
        .is_err());
        assert!(json_template_from_value(json!({
            "previousblockhash": hash(1).to_string(),
            "height": 0
        }))
        .is_err());
    }

    #[test]
    fn classifies_template_hash_and_height_together() {
        let tip = chain_tip(20, 100);

        assert!(matches!(
            classify_template_tip(&fetched(tip.hash, 101, "current"), tip).unwrap(),
            TemplateTipRelation::Current
        ));
        assert!(matches!(
            classify_template_tip(&fetched(hash(21), 102, "ahead"), tip).unwrap(),
            TemplateTipRelation::AheadOfIndex
        ));
        assert!(matches!(
            classify_template_tip(&fetched(hash(22), 101, "competing"), tip).unwrap(),
            TemplateTipRelation::Stale
        ));
        assert!(matches!(
            classify_template_tip(&fetched(hash(23), 100, "older"), tip).unwrap(),
            TemplateTipRelation::Stale
        ));
        assert!(classify_template_tip(&fetched(tip.hash, 102, "inconsistent"), tip).is_err());
        assert!(classify_template_tip(&fetched(tip.hash, 0, "zero"), tip).is_err());
    }

    #[tokio::test]
    async fn cache_hits_until_expiry_or_tip_change() {
        let cache = BlockTemplateCache::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let tip = chain_tip(2, 100);

        let body = cache
            .get_or_fetch(tip, Duration::from_secs(60), {
                let calls = Arc::clone(&calls);
                move || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(fetched(tip.hash, 101, "first"))
                }
            })
            .await
            .unwrap();
        assert_eq!(&body[..], b"first");

        let body = cache
            .get_or_fetch(tip, Duration::from_secs(60), {
                let calls = Arc::clone(&calls);
                move || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(fetched(tip.hash, 101, "unexpected"))
                }
            })
            .await
            .unwrap();
        assert_eq!(&body[..], b"first");
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        cache
            .get_or_fetch(tip, Duration::ZERO, {
                let calls = Arc::clone(&calls);
                move || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(fetched(tip.hash, 101, "expired"))
                }
            })
            .await
            .unwrap();
        cache
            .get_or_fetch(chain_tip(3, 101), Duration::from_secs(60), {
                let calls = Arc::clone(&calls);
                move || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(fetched(hash(3), 102, "new-tip"))
                }
            })
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn failed_fetch_is_negatively_cached_until_backoff_expires() {
        let cache = BlockTemplateCache::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let tip = chain_tip(4, 100);

        let result = cache
            .get_or_fetch_with_failure_backoff(
                tip,
                Duration::from_secs(60),
                Duration::from_secs(60),
                {
                    let calls = Arc::clone(&calls);
                    move || {
                        calls.fetch_add(1, Ordering::SeqCst);
                        bail!("fetch failed")
                    }
                },
            )
            .await;
        assert_eq!(result.unwrap_err().to_string(), "fetch failed");

        let result = cache
            .get_or_fetch_with_failure_backoff(
                tip,
                Duration::from_secs(60),
                Duration::from_secs(60),
                {
                    let calls = Arc::clone(&calls);
                    move || {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(fetched(tip.hash, 101, "unexpected"))
                    }
                },
            )
            .await;
        assert_eq!(result.unwrap_err().to_string(), "fetch failed");
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let body = cache
            .get_or_fetch(tip, Duration::from_secs(60), {
                let calls = Arc::clone(&calls);
                move || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(fetched(tip.hash, 101, "recovered"))
                }
            })
            .await
            .unwrap();
        assert_eq!(&body[..], b"recovered");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn dead_worker_state_is_cleared_for_next_request() {
        let cache = BlockTemplateCache::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let tip = chain_tip(24, 100);
        let (sender, receiver) = watch::channel(None);
        *cache.entry.lock().await = BlockTemplateCacheEntry::Fetching(receiver);
        drop(sender);

        let error = cache
            .get_or_fetch(tip, Duration::from_secs(60), {
                let calls = Arc::clone(&calls);
                move || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(fetched(tip.hash, 101, "unexpected"))
                }
            })
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "block-template worker stopped without a result"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        let body = cache
            .get_or_fetch(tip, Duration::from_secs(60), {
                let calls = Arc::clone(&calls);
                move || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(fetched(tip.hash, 101, "recovered"))
                }
            })
            .await
            .unwrap();

        assert_eq!(&body[..], b"recovered");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(matches!(
            &*cache.entry.lock().await,
            BlockTemplateCacheEntry::Ready(_)
        ));
    }

    #[tokio::test]
    async fn daemon_ahead_template_is_cached_until_index_catches_up() {
        let cache = BlockTemplateCache::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let observed_tip = chain_tip(5, 100);
        let template_tip = hash(6);

        cache
            .get_or_fetch(observed_tip, Duration::from_secs(60), {
                let calls = Arc::clone(&calls);
                move || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(fetched(template_tip, 102, "new-tip-template"))
                }
            })
            .await
            .unwrap();
        let body = cache
            .get_or_fetch(observed_tip, Duration::from_secs(60), {
                let calls = Arc::clone(&calls);
                move || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(fetched(template_tip, 102, "unexpected"))
                }
            })
            .await
            .unwrap();

        assert_eq!(&body[..], b"new-tip-template");
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let body = cache
            .get_or_fetch(
                ChainTipState {
                    hash: template_tip,
                    height: 101,
                },
                Duration::from_secs(60),
                {
                    let calls = Arc::clone(&calls);
                    move || {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(fetched(template_tip, 102, "unexpected"))
                    }
                },
            )
            .await
            .unwrap();
        assert_eq!(&body[..], b"new-tip-template");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn stale_template_is_refetched_once() {
        let cache = BlockTemplateCache::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let tip = chain_tip(10, 100);

        let body = cache
            .get_or_fetch(tip, Duration::from_secs(60), {
                let calls = Arc::clone(&calls);
                move || match calls.fetch_add(1, Ordering::SeqCst) {
                    0 => Ok(fetched(hash(11), 101, "stale")),
                    _ => Ok(fetched(tip.hash, 101, "fresh")),
                }
            })
            .await
            .unwrap();
        assert_eq!(&body[..], b"fresh");
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        let body = cache
            .get_or_fetch(tip, Duration::from_secs(60), {
                let calls = Arc::clone(&calls);
                move || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(fetched(tip.hash, 101, "unexpected"))
                }
            })
            .await
            .unwrap();
        assert_eq!(&body[..], b"fresh");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn two_stale_templates_fail_and_are_not_cached() {
        let cache = BlockTemplateCache::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let tip = chain_tip(12, 100);

        let error = cache
            .get_or_fetch(tip, Duration::from_secs(60), {
                let calls = Arc::clone(&calls);
                move || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(fetched(hash(13), 101, "stale"))
                }
            })
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "daemon returned a stale or competing block template twice"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        let body = cache
            .get_or_fetch(tip, Duration::from_secs(60), {
                let calls = Arc::clone(&calls);
                move || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(fetched(tip.hash, 101, "recovered"))
                }
            })
            .await
            .unwrap();
        assert_eq!(&body[..], b"recovered");
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn inconsistent_template_height_fails_without_retry() {
        let cache = BlockTemplateCache::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let tip = chain_tip(14, 100);

        let error = cache
            .get_or_fetch(tip, Duration::from_secs(60), {
                let calls = Arc::clone(&calls);
                move || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(fetched(tip.hash, 102, "inconsistent"))
                }
            })
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("parent hash matches indexed tip but height"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_miss_is_single_flight() {
        let cache = Arc::new(BlockTemplateCache::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let ready = Arc::new(Barrier::new(5));
        let tip = chain_tip(7, 100);
        let mut tasks = vec![];
        for _ in 0..4 {
            let cache = Arc::clone(&cache);
            let calls = Arc::clone(&calls);
            let ready = Arc::clone(&ready);
            tasks.push(tokio::spawn(async move {
                ready.wait().await;
                cache
                    .get_or_fetch(tip, Duration::from_secs(60), move || {
                        calls.fetch_add(1, Ordering::SeqCst);
                        std::thread::sleep(Duration::from_millis(25));
                        Ok(fetched(tip.hash, 101, "shared"))
                    })
                    .await
                    .unwrap()
            }));
        }
        ready.wait().await;
        for task in tasks {
            assert_eq!(&task.await.unwrap()[..], b"shared");
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_failed_miss_is_single_flight() {
        let cache = Arc::new(BlockTemplateCache::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let ready = Arc::new(Barrier::new(5));
        let tip = chain_tip(8, 100);
        let mut tasks = vec![];
        for _ in 0..4 {
            let cache = Arc::clone(&cache);
            let calls = Arc::clone(&calls);
            let ready = Arc::clone(&ready);
            tasks.push(tokio::spawn(async move {
                ready.wait().await;
                cache
                    .get_or_fetch(tip, Duration::from_secs(60), move || {
                        calls.fetch_add(1, Ordering::SeqCst);
                        std::thread::sleep(Duration::from_millis(25));
                        bail!("shared failure")
                    })
                    .await
                    .unwrap_err()
                    .to_string()
            }));
        }
        ready.wait().await;
        for task in tasks {
            assert_eq!(task.await.unwrap(), "shared failure");
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let body = cache
            .get_or_fetch(tip, Duration::from_secs(60), {
                let calls = Arc::clone(&calls);
                move || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(fetched(tip.hash, 101, "recovered"))
                }
            })
            .await
            .unwrap();
        assert_eq!(&body[..], b"recovered");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_request_does_not_cancel_shared_fetch() {
        let cache = Arc::new(BlockTemplateCache::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let tip = chain_tip(9, 100);
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let started_tx = Arc::new(StdMutex::new(Some(started_tx)));

        let leader = {
            let cache = Arc::clone(&cache);
            let calls = Arc::clone(&calls);
            let started_tx = Arc::clone(&started_tx);
            tokio::spawn(async move {
                cache
                    .get_or_fetch(tip, Duration::from_secs(60), move || {
                        calls.fetch_add(1, Ordering::SeqCst);
                        if let Some(started_tx) = started_tx.lock().unwrap().take() {
                            let _ = started_tx.send(());
                        }
                        std::thread::sleep(Duration::from_millis(25));
                        Ok(fetched(tip.hash, 101, "shared"))
                    })
                    .await
            })
        };
        started_rx.await.unwrap();
        leader.abort();

        let body = cache
            .get_or_fetch(tip, Duration::from_secs(60), {
                let calls = Arc::clone(&calls);
                move || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(fetched(tip.hash, 101, "unexpected"))
                }
            })
            .await
            .unwrap();
        assert_eq!(&body[..], b"shared");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[cfg(feature = "liquid")]
    fn dynafed_fixture() -> (String, Block) {
        // A witness-bearing dynafed block from rust-elements' block_decoder test,
        // used here as representative getnewblockhex output.
        let raw = include_str!("../../tests/fixtures/liquid_dynafed_getnewblockhex.hex")
            .trim()
            .to_string();
        let bytes = Vec::from_hex(&raw).unwrap();
        let block = elements::encode::deserialize(&bytes).unwrap();
        (raw, block)
    }

    #[cfg(feature = "liquid")]
    #[test]
    fn projects_dynafed_getnewblockhex_fixture() {
        let (raw, block) = dynafed_fixture();
        let fetched = elements_template_from_hex(raw.clone(), Network::LiquidRegtest).unwrap();
        let response: Value = serde_json::from_slice(&fetched.body).unwrap();
        let raw_bytes = Vec::from_hex(&raw).unwrap();

        assert_eq!(
            u32::from_le_bytes(raw_bytes[..4].try_into().unwrap()),
            0xa000_0000
        );
        assert_eq!(block.header.version, 0x2000_0000);
        assert_eq!(response["version"].as_u64(), Some(0x2000_0000));
        assert_eq!(response["height"].as_u64(), Some(7));
        assert_eq!(response["curtime"].as_u64(), Some(block.header.time.into()));
        assert_eq!(
            response["previousblockhash"].as_str(),
            Some(block.header.prev_blockhash.to_string().as_str())
        );
        assert_eq!(response["transactions"].as_array().unwrap().len(), 0);
        assert_eq!(response["coinbasevalue"].as_u64(), Some(0));
        assert_eq!(response["capabilities"], json!([]));
        assert_eq!(response["rules"], json!([]));
        assert_eq!(response["vbavailable"], json!({}));
        assert_eq!(response["vbrequired"].as_u64(), Some(0));
        assert_eq!(response["coinbaseaux"], json!({}));
        assert_eq!(response["mutable"], json!([]));
        assert_eq!(response["bits"].as_str(), Some("00000000"));
        assert_eq!(response["target"], json!("0".repeat(64)));
        assert_eq!(response["noncerange"].as_str(), Some("00000000ffffffff"));
        assert_eq!(
            response["default_witness_commitment"].as_str(),
            Some("6a24aa21a9ed94f15ed3a62165e4a0b99699cc28b48e19cb5bc1b1f47155db62d63f1e047d45")
        );
        for omitted in [
            "longpollid",
            "mintime",
            "sigoplimit",
            "sizelimit",
            "weightlimit",
        ] {
            assert!(response.get(omitted).is_none(), "unexpected {}", omitted);
        }
    }

    #[cfg(feature = "liquid")]
    #[test]
    fn projects_fees_dependencies_and_witness_transactions() {
        use elements::{LockTime, OutPoint, TxIn, TxInWitness, TxOut};

        let (_, mut block) = dynafed_fixture();
        let policy_asset = *Network::LiquidRegtest.native_asset();
        let external_txid: Txid = "11".repeat(32).parse().unwrap();
        let parent = Transaction {
            version: 2,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: external_txid,
                    vout: 0,
                },
                ..TxIn::default()
            }],
            output: vec![
                TxOut::new_fee(2, policy_asset),
                TxOut::new_fee(3, policy_asset),
            ],
        };
        let other_parent = Transaction {
            version: 2,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: "22".repeat(32).parse().unwrap(),
                    vout: 0,
                },
                ..TxIn::default()
            }],
            output: vec![TxOut::new_fee(6, policy_asset)],
        };
        let child = Transaction {
            version: 2,
            lock_time: LockTime::ZERO,
            input: vec![
                TxIn {
                    previous_output: OutPoint {
                        txid: other_parent.txid(),
                        vout: 0,
                    },
                    ..TxIn::default()
                },
                TxIn {
                    previous_output: OutPoint {
                        txid: parent.txid(),
                        vout: 0,
                    },
                    ..TxIn::default()
                },
                TxIn {
                    previous_output: OutPoint {
                        txid: parent.txid(),
                        vout: 1,
                    },
                    witness: TxInWitness {
                        script_witness: vec![vec![1, 2, 3]],
                        ..TxInWitness::default()
                    },
                    ..TxIn::default()
                },
            ],
            output: vec![TxOut::new_fee(7, policy_asset)],
        };
        block.txdata.push(parent.clone());
        block.txdata.push(other_parent);
        block.txdata.push(child.clone());

        let response = BlockTemplateResponse::from_block(&block, policy_asset).unwrap();
        assert_eq!(response.transactions.len(), 3);
        assert_eq!(response.transactions[0].depends, Vec::<usize>::new());
        assert_eq!(response.transactions[1].depends, Vec::<usize>::new());
        assert_eq!(response.transactions[2].depends, vec![1, 2]);
        assert_eq!(response.transactions[0].fee, 5);
        assert_eq!(response.transactions[1].fee, 6);
        assert_eq!(response.transactions[2].fee, 7);
        assert_eq!(response.transactions[0].txid, parent.txid().to_string());
        assert_eq!(response.transactions[2].hash, child.wtxid().to_string());
        assert_ne!(response.transactions[2].hash, response.transactions[2].txid);
        assert_eq!(response.transactions[2].weight, child.weight());
        assert_eq!(
            response.transactions[2].data,
            elements::encode::serialize_hex(&child)
        );
        let response = serde_json::to_value(&response).unwrap();
        assert!(response["transactions"]
            .as_array()
            .unwrap()
            .iter()
            .all(|tx| tx.get("sigops").is_none()));
    }

    #[cfg(feature = "liquid")]
    #[test]
    fn validates_coinbase_and_malformed_source() {
        use elements::{confidential, TxOut};

        let (_, mut block) = dynafed_fixture();
        let coinbase = block.txdata.first_mut().unwrap();
        let fixture_asset = coinbase.output[0].asset.explicit().unwrap();
        coinbase.output[0].value = confidential::Value::Explicit(42);
        assert_eq!(
            BlockTemplateResponse::from_block(&block, fixture_asset)
                .unwrap()
                .coinbasevalue,
            42
        );

        let mut empty = block.clone();
        empty.txdata.clear();
        assert!(BlockTemplateResponse::from_block(&empty, fixture_asset).is_err());

        let mut duplicate = block.clone();
        duplicate.txdata.push(duplicate.txdata[0].clone());
        assert!(BlockTemplateResponse::from_block(&duplicate, fixture_asset).is_err());

        let mut overflowing_fee_tx = block.txdata[0].clone();
        overflowing_fee_tx.output = vec![
            TxOut::new_fee(u64::MAX, fixture_asset),
            TxOut::new_fee(1, fixture_asset),
        ];
        assert!(transaction_fee(&overflowing_fee_tx, fixture_asset).is_err());

        assert!(elements_template_from_hex("not hex".to_string(), Network::LiquidRegtest).is_err());
        assert!(elements_template_from_hex("00".to_string(), Network::LiquidRegtest).is_err());
    }

    #[cfg(feature = "liquid")]
    #[test]
    fn bitcoin_and_elements_bodies_share_the_response_contract() {
        let (raw, _) = dynafed_fixture();
        let elements = elements_template_from_hex(raw, Network::LiquidRegtest).unwrap();
        let elements_value: Value = serde_json::from_slice(&elements.body).unwrap();
        let _: BlockTemplateResponse = serde_json::from_value(elements_value.clone()).unwrap();

        let mut bitcoin_value = elements_value;
        bitcoin_value["previousblockhash"] = json!(hash(9).to_string());
        bitcoin_value["bitcoin-only-field"] = json!("preserved");
        let bitcoin = json_template_from_value(bitcoin_value.clone()).unwrap();
        let parsed: Value = serde_json::from_slice(&bitcoin.body).unwrap();
        let _: BlockTemplateResponse = serde_json::from_value(parsed.clone()).unwrap();
        assert_eq!(parsed, bitcoin_value);
    }
}
