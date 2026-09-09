use std::collections::HashMap;
use std::env;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use elements::AssetId;
use reqwest::{Client, StatusCode};
use serde::de::DeserializeOwned;
use serde_json::{Map as JsonMap, Value as JsonValue};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tokio::sync::{oneshot, Mutex, OwnedSemaphorePermit, Semaphore};
use url::{Host, Url};

use crate::errors::*;

const DEFAULT_REGISTRY_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_REGISTRY_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const REGISTRY_CONNECT_TIMEOUT_ENV: &str = "ELECTRS_ASSET_REGISTRY_CONNECT_TIMEOUT_MS";
const REGISTRY_REQUEST_TIMEOUT_ENV: &str = "ELECTRS_ASSET_REGISTRY_REQUEST_TIMEOUT_MS";
const REGISTRY_MAX_PAGE_SIZE: usize = 500;
const REGISTRY_MAX_PAGE: usize = 1_000_000;
// Registry metadata changes infrequently. Keep the default aligned with the old
// background registry refresh cadence instead of polling upstream once per second.
const REGISTRY_ASSET_CACHE_TTL: Duration = Duration::from_secs(15);
const REGISTRY_ERROR_CACHE_TTL: Duration = Duration::from_secs(1);
const REGISTRY_ASSET_CACHE_MAX_ENTRIES: usize = 1024;
const REGISTRY_MAX_CONCURRENT_REQUESTS: usize = 16;
const REGISTRY_MAX_ASSET_RESPONSE_SIZE: usize = 1024 * 1024;
const REGISTRY_MAX_LIST_RESPONSE_SIZE: usize = 16 * 1024 * 1024;
const REGISTRY_FETCH_COMPLETION_GRACE: Duration = Duration::from_secs(1);

#[derive(Clone, Debug)]
pub enum RegistryError {
    InvalidBaseUrl(String),
    InvalidRequest(String),
    Timeout(String),
    Transport(String),
    HttpStatus(u16),
    InvalidResponse(String),
    Overloaded(String),
    LocalLookup(String),
}

impl fmt::Display for RegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBaseUrl(message)
            | Self::InvalidRequest(message)
            | Self::Timeout(message)
            | Self::Transport(message)
            | Self::InvalidResponse(message)
            | Self::Overloaded(message)
            | Self::LocalLookup(message) => f.write_str(message),
            Self::HttpStatus(status) => {
                write!(f, "asset registry returned HTTP status {}", status)
            }
        }
    }
}

impl std::error::Error for RegistryError {}

type RegistryAssetValue = Option<Arc<RegistryAsset>>;
type RegistryFetchResult = std::result::Result<RegistryAssetValue, RegistryError>;
type RegistryAssetResult = std::result::Result<RegistryAssetLookup, RegistryError>;

#[derive(Clone)]
pub(crate) struct RegistryAssetLookup {
    pub asset: RegistryAssetValue,
    pub stale: bool,
}

enum AssetCacheEntry {
    Ready {
        fetched_at: Instant,
        result: RegistryAssetResult,
    },
    Fetching {
        started_at: Instant,
        // A successful expired value can be served while one owned task refreshes it.
        // The outer Option distinguishes "no stale value" from a stale NotFound.
        stale: Option<RegistryAssetValue>,
        waiters: Vec<oneshot::Sender<RegistryAssetResult>>,
    },
}

#[derive(Clone)]
pub struct RegistryClient {
    base_url: Url,
    http: Client,
    asset_cache: Arc<Mutex<HashMap<AssetId, AssetCacheEntry>>>,
    concurrency: Arc<Semaphore>,
    asset_cache_ttl: Duration,
    error_cache_ttl: Duration,
    asset_cache_max_entries: usize,
    permit_acquire_timeout: Duration,
    fetch_wait_timeout: Duration,
    fetch_poison_timeout: Duration,
}

impl RegistryClient {
    pub fn new(base_url: Url) -> std::result::Result<Self, RegistryError> {
        Self::with_timeouts(
            base_url,
            registry_timeout_from_env(
                REGISTRY_CONNECT_TIMEOUT_ENV,
                DEFAULT_REGISTRY_CONNECT_TIMEOUT,
            )?,
            registry_timeout_from_env(
                REGISTRY_REQUEST_TIMEOUT_ENV,
                DEFAULT_REGISTRY_REQUEST_TIMEOUT,
            )?,
        )
    }

    pub fn with_cache_ttl(
        base_url: Url,
        asset_cache_ttl: Duration,
    ) -> std::result::Result<Self, RegistryError> {
        Self::with_options(
            base_url,
            DEFAULT_REGISTRY_CONNECT_TIMEOUT,
            DEFAULT_REGISTRY_REQUEST_TIMEOUT,
            asset_cache_ttl,
            REGISTRY_ASSET_CACHE_MAX_ENTRIES,
            REGISTRY_MAX_CONCURRENT_REQUESTS,
            DEFAULT_REGISTRY_REQUEST_TIMEOUT,
        )
    }

    fn with_timeouts(
        base_url: Url,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> std::result::Result<Self, RegistryError> {
        Self::with_options(
            base_url,
            connect_timeout,
            request_timeout,
            REGISTRY_ASSET_CACHE_TTL,
            REGISTRY_ASSET_CACHE_MAX_ENTRIES,
            REGISTRY_MAX_CONCURRENT_REQUESTS,
            request_timeout,
        )
    }

    fn with_options(
        mut base_url: Url,
        connect_timeout: Duration,
        request_timeout: Duration,
        asset_cache_ttl: Duration,
        asset_cache_max_entries: usize,
        max_concurrent_requests: usize,
        permit_acquire_timeout: Duration,
    ) -> std::result::Result<Self, RegistryError> {
        if !base_url.username().is_empty() || base_url.password().is_some() {
            return Err(RegistryError::InvalidBaseUrl(
                "asset registry URL must not contain a username or password".to_string(),
            ));
        }
        if !matches!(base_url.scheme(), "http" | "https") {
            return Err(RegistryError::InvalidBaseUrl(format!(
                "asset registry URL must use http or https (got scheme '{}')",
                base_url.scheme()
            )));
        }

        if !base_url.path().ends_with('/') {
            let path = format!("{}/", base_url.path());
            base_url.set_path(&path);
        }
        base_url.set_query(None);
        base_url.set_fragment(None);

        let http = Client::builder()
            .connect_timeout(connect_timeout)
            .timeout(request_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .user_agent(concat!("electrs/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|error| RegistryError::Transport(error.to_string()))?;

        let fetch_wait_timeout = permit_acquire_timeout
            .saturating_add(request_timeout)
            .saturating_add(REGISTRY_FETCH_COMPLETION_GRACE);
        Ok(Self {
            base_url,
            http,
            asset_cache: Arc::new(Mutex::new(HashMap::new())),
            concurrency: Arc::new(Semaphore::new(max_concurrent_requests.max(1))),
            asset_cache_ttl,
            error_cache_ttl: asset_cache_ttl.min(REGISTRY_ERROR_CACHE_TTL),
            asset_cache_max_entries: asset_cache_max_entries.max(1),
            permit_acquire_timeout,
            fetch_wait_timeout,
            fetch_poison_timeout: fetch_wait_timeout
                .saturating_add(REGISTRY_FETCH_COMPLETION_GRACE),
        })
    }

    pub async fn get_asset(
        &self,
        asset_id: &AssetId,
    ) -> std::result::Result<Option<Arc<RegistryAsset>>, RegistryError> {
        self.get_asset_with_status(asset_id)
            .await
            .map(|lookup| lookup.asset)
    }

    pub(crate) async fn get_asset_with_status(&self, asset_id: &AssetId) -> RegistryAssetResult {
        let mut cache = self.asset_cache.lock().await;
        let now = Instant::now();

        if let Some(AssetCacheEntry::Ready { fetched_at, result }) = cache.get(asset_id) {
            let ttl = match result {
                Ok(lookup) if !lookup.stale => self.asset_cache_ttl,
                Ok(_) | Err(_) => self.error_cache_ttl,
            };
            if now.duration_since(*fetched_at) < ttl {
                return result.clone();
            }
        }

        let join_receiver = match cache.get_mut(asset_id) {
            Some(AssetCacheEntry::Fetching {
                started_at,
                stale,
                waiters,
            }) if now.duration_since(*started_at) < self.fetch_poison_timeout => {
                if let Some(asset) = stale {
                    return Ok(RegistryAssetLookup {
                        asset: asset.clone(),
                        stale: true,
                    });
                }
                let (sender, receiver) = oneshot::channel();
                waiters.push(sender);
                Some(receiver)
            }
            _ => None,
        };

        let receiver = if let Some(receiver) = join_receiver {
            drop(cache);
            receiver
        } else {
            let removed = cache.remove(asset_id);
            let (stale, mut waiters) = match removed {
                Some(AssetCacheEntry::Ready {
                    result: Ok(lookup), ..
                }) => (Some(lookup.asset), vec![]),
                Some(AssetCacheEntry::Fetching { stale, waiters, .. }) => (stale, waiters),
                _ => (None, vec![]),
            };
            prune_asset_cache(
                &mut cache,
                now,
                self.error_cache_ttl,
                self.fetch_poison_timeout,
                self.asset_cache_max_entries,
            );

            if cache.len() >= self.asset_cache_max_entries {
                return Err(RegistryError::Overloaded(
                    "asset registry cache is at capacity".to_string(),
                ));
            }

            let (sender, receiver) = oneshot::channel();
            if stale.is_none() {
                waiters.push(sender);
            }
            cache.insert(
                *asset_id,
                AssetCacheEntry::Fetching {
                    started_at: now,
                    stale: stale.clone(),
                    waiters,
                },
            );
            drop(cache);

            // The owned task covers both admission and I/O. Once Fetching is visible,
            // cancelling the request that created it cannot strand the cache entry.
            let client = self.clone();
            let asset_id = *asset_id;
            tokio::spawn(async move {
                let fetch_client = client.clone();
                let worker = tokio::spawn(async move {
                    match fetch_client.acquire_permit().await {
                        Ok(_permit) => fetch_client.fetch_asset(&asset_id).await,
                        Err(error) => Err(error),
                    }
                });
                let result = match worker.await {
                    Ok(result) => result,
                    Err(error) => Err(RegistryError::Transport(format!(
                        "asset registry lookup worker failed: {}",
                        error
                    ))),
                };
                if let Err(error) = &result {
                    warn!(
                        "asset registry lookup failed for asset_id='{}' error='{}'",
                        asset_id, error
                    );
                }
                client.complete_asset_fetch(asset_id, now, result).await;
            });

            if let Some(asset) = stale {
                return Ok(RegistryAssetLookup { asset, stale: true });
            }
            receiver
        };

        match tokio::time::timeout(self.fetch_wait_timeout, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(RegistryError::Transport(
                "asset registry lookup worker stopped unexpectedly".to_string(),
            )),
            Err(_) => Err(RegistryError::Timeout(
                "timed out waiting for asset registry lookup".to_string(),
            )),
        }
    }

    async fn fetch_asset(&self, asset_id: &AssetId) -> RegistryFetchResult {
        let url = self
            .base_url
            .join(&format!("v2/assets/{}", asset_id))
            .map_err(|error| RegistryError::InvalidBaseUrl(error.to_string()))?;
        let response = self.http.get(url).send().await.map_err(request_error)?;

        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(RegistryError::HttpStatus(response.status().as_u16()));
        }

        let mut asset: RegistryAsset =
            decode_json_response(response, REGISTRY_MAX_ASSET_RESPONSE_SIZE).await?;
        if asset.asset_id != *asset_id {
            return Err(RegistryError::InvalidResponse(format!(
                "asset registry returned {} for requested asset {}",
                asset.asset_id, asset_id
            )));
        }
        self.make_icon_absolute(&mut asset)?;
        Ok(Some(Arc::new(asset)))
    }

    async fn complete_asset_fetch(
        &self,
        asset_id: AssetId,
        started_at: Instant,
        result: RegistryFetchResult,
    ) {
        let mut cache = self.asset_cache.lock().await;
        if !matches!(
            cache.get(&asset_id),
            Some(AssetCacheEntry::Fetching {
                started_at: current_started_at,
                ..
            }) if *current_started_at == started_at
        ) {
            return;
        }
        let (stale, waiters) = match cache.remove(&asset_id) {
            Some(AssetCacheEntry::Fetching { stale, waiters, .. }) => (stale, waiters),
            _ => (None, vec![]),
        };
        let cache_result = !matches!(&result, Err(RegistryError::Overloaded(_))) || stale.is_some();
        let result = match (result, stale) {
            (Ok(asset), _) => Ok(RegistryAssetLookup {
                asset,
                stale: false,
            }),
            (Err(_), Some(asset)) => Ok(RegistryAssetLookup { asset, stale: true }),
            (Err(error), None) => Err(error),
        };
        if cache_result {
            cache.insert(
                asset_id,
                AssetCacheEntry::Ready {
                    fetched_at: Instant::now(),
                    result: result.clone(),
                },
            );
        }
        drop(cache);

        for waiter in waiters {
            let _ = waiter.send(result.clone());
        }
    }

    fn make_icon_absolute(
        &self,
        asset: &mut RegistryAsset,
    ) -> std::result::Result<(), RegistryError> {
        if let Some(icon) = &mut asset.icon {
            let mut href = self
                .base_url
                .join(&icon.href)
                .map_err(|error| RegistryError::InvalidResponse(error.to_string()))?;
            if !matches!(href.scheme(), "http" | "https") || href.origin() != self.base_url.origin()
            {
                return Err(RegistryError::InvalidResponse(format!(
                    "asset registry returned an invalid icon URL: {}",
                    icon.href
                )));
            }
            // origin() excludes userinfo (RFC 6454), so the check above does not reject
            // an otherwise same-origin absolute href containing user:pass@.
            let _ = href.set_username("");
            let _ = href.set_password(None);
            icon.href = href.to_string();
        }
        Ok(())
    }

    async fn acquire_permit(&self) -> std::result::Result<OwnedSemaphorePermit, RegistryError> {
        match tokio::time::timeout(
            self.permit_acquire_timeout,
            self.concurrency.clone().acquire_owned(),
        )
        .await
        {
            Ok(Ok(permit)) => Ok(permit),
            Ok(Err(_)) => Err(RegistryError::Transport(
                "asset registry concurrency semaphore closed".to_string(),
            )),
            Err(_) => Err(RegistryError::Overloaded(
                "too many concurrent asset registry requests".to_string(),
            )),
        }
    }

    pub async fn list_assets(
        &self,
        start_index: usize,
        limit: usize,
        sorting: AssetSorting,
        filters: &AssetSearchFilters,
    ) -> std::result::Result<RegistryAssetList, RegistryError> {
        if limit > REGISTRY_MAX_PAGE_SIZE {
            return Err(RegistryError::InvalidRequest(format!(
                "asset registry page size cannot exceed {}",
                REGISTRY_MAX_PAGE_SIZE
            )));
        }

        // The v2 API has no zero-sized page, but electrs historically accepts limit=0 and
        // still returns the total count.
        let page_size = limit.max(1);
        let page = if limit == 0 {
            1
        } else {
            (start_index / page_size).checked_add(1).ok_or_else(|| {
                RegistryError::InvalidRequest("asset registry page overflow".to_string())
            })?
        };
        if page > REGISTRY_MAX_PAGE {
            return Err(RegistryError::InvalidRequest(format!(
                "asset registry page cannot exceed {}",
                REGISTRY_MAX_PAGE
            )));
        }

        let offset = if limit == 0 {
            0
        } else {
            start_index % page_size
        };
        let first = self.fetch_page(page, page_size, sorting, filters).await?;
        let total_count = first.total_count.ok_or_else(|| {
            RegistryError::InvalidResponse(
                "asset registry response is missing total_count".to_string(),
            )
        })?;

        if limit == 0 || start_index >= total_count {
            return Ok(RegistryAssetList {
                total_count,
                items: vec![],
            });
        }

        let mut items = first.items;
        if items.len() == page_size
            && offset.saturating_add(limit) > items.len()
            && page < REGISTRY_MAX_PAGE
            && start_index.saturating_add(page_size - offset) < total_count
        {
            let second = self
                .fetch_page(page + 1, page_size, sorting, filters)
                .await?;
            items.extend(second.items);
        }

        let mut seen = std::collections::HashSet::new();
        items.retain(|asset| seen.insert(asset.asset_id));

        Ok(RegistryAssetList {
            total_count,
            items: items.into_iter().skip(offset).take(limit).collect(),
        })
    }

    async fn fetch_page(
        &self,
        page: usize,
        page_size: usize,
        sorting: AssetSorting,
        filters: &AssetSearchFilters,
    ) -> std::result::Result<RegistryListResponse, RegistryError> {
        let _permit = self.acquire_permit().await?;
        let url = self
            .base_url
            .join("v2/assets")
            .map_err(|error| RegistryError::InvalidBaseUrl(error.to_string()))?;
        let mut query = vec![
            ("page", page.to_string()),
            ("page_size", page_size.to_string()),
            ("sort", sorting.as_str().to_string()),
        ];
        filters.append_query(&mut query);
        let response = self
            .http
            .get(url)
            .query(&query)
            .send()
            .await
            .map_err(request_error)?;

        if !response.status().is_success() {
            return Err(RegistryError::HttpStatus(response.status().as_u16()));
        }

        let mut page_response: RegistryListResponse =
            decode_json_response(response, REGISTRY_MAX_LIST_RESPONSE_SIZE).await?;
        if page_response.page != page || page_response.page_size != page_size {
            return Err(RegistryError::InvalidResponse(format!(
                "asset registry returned page {}/{} for requested page {}/{}",
                page_response.page, page_response.page_size, page, page_size
            )));
        }
        if page_response.items.len() > page_size {
            return Err(RegistryError::InvalidResponse(format!(
                "asset registry returned {} items for page size {}",
                page_response.items.len(),
                page_size
            )));
        }
        for asset in &mut page_response.items {
            self.make_icon_absolute(asset)?;
        }
        Ok(page_response)
    }
}

fn registry_timeout_from_env(
    variable: &str,
    default: Duration,
) -> std::result::Result<Duration, RegistryError> {
    match env::var(variable) {
        Ok(value) => parse_registry_timeout(variable, &value),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(env::VarError::NotUnicode(_)) => Err(RegistryError::InvalidRequest(format!(
            "{} must be valid Unicode",
            variable
        ))),
    }
}

fn parse_registry_timeout(
    variable: &str,
    value: &str,
) -> std::result::Result<Duration, RegistryError> {
    let millis = value.parse::<u64>().map_err(|_| {
        RegistryError::InvalidRequest(format!("{} must be a positive integer", variable))
    })?;
    if millis == 0 {
        return Err(RegistryError::InvalidRequest(format!(
            "{} must be greater than zero",
            variable
        )));
    }
    Ok(Duration::from_millis(millis))
}

fn prune_asset_cache(
    cache: &mut HashMap<AssetId, AssetCacheEntry>,
    now: Instant,
    error_ttl: Duration,
    fetch_poison_timeout: Duration,
    max_entries: usize,
) {
    cache.retain(|_, entry| match entry {
        // Expired successful values are still useful for stale-while-revalidate.
        // Keep Ready entries until capacity pressure selects one for eviction.
        AssetCacheEntry::Ready { fetched_at, result } => {
            result.is_ok() || now.duration_since(*fetched_at) < error_ttl
        }
        AssetCacheEntry::Fetching {
            started_at,
            stale,
            waiters,
        } => {
            if now.duration_since(*started_at) < fetch_poison_timeout {
                return true;
            }
            let result = match stale.take() {
                Some(asset) => Ok(RegistryAssetLookup { asset, stale: true }),
                None => Err(RegistryError::Timeout(
                    "asset registry lookup exceeded its recovery deadline".to_string(),
                )),
            };
            for waiter in waiters.drain(..) {
                let _ = waiter.send(result.clone());
            }
            if result.is_err() {
                return false;
            }
            *entry = AssetCacheEntry::Ready {
                fetched_at: now,
                result,
            };
            true
        }
    });

    while cache.len() >= max_entries {
        let oldest = cache
            .iter()
            .filter_map(|(asset_id, entry)| match entry {
                AssetCacheEntry::Ready { fetched_at, .. } => Some((*asset_id, *fetched_at)),
                AssetCacheEntry::Fetching { .. } => None,
            })
            .min_by_key(|(_, fetched_at)| *fetched_at)
            .map(|(asset_id, _)| asset_id);
        match oldest {
            Some(asset_id) => {
                cache.remove(&asset_id);
            }
            None => break,
        }
    }
}

async fn decode_json_response<T: DeserializeOwned>(
    mut response: reqwest::Response,
    max_size: usize,
) -> std::result::Result<T, RegistryError> {
    if let Some(length) = response.content_length() {
        if length > max_size as u64 {
            return Err(RegistryError::InvalidResponse(format!(
                "asset registry response exceeds {} bytes",
                max_size
            )));
        }
    }

    let mut body =
        Vec::with_capacity(response.content_length().unwrap_or(0).min(max_size as u64) as usize);
    while let Some(chunk) = response.chunk().await.map_err(response_error)? {
        let new_len = body.len().checked_add(chunk.len()).ok_or_else(|| {
            RegistryError::InvalidResponse("asset registry response size overflow".to_string())
        })?;
        if new_len > max_size {
            return Err(RegistryError::InvalidResponse(format!(
                "asset registry response exceeds {} bytes",
                max_size
            )));
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).map_err(|error| RegistryError::InvalidResponse(error.to_string()))
}

fn request_error(error: reqwest::Error) -> RegistryError {
    if error.is_timeout() {
        RegistryError::Timeout(error.to_string())
    } else {
        RegistryError::Transport(error.to_string())
    }
}

fn response_error(error: reqwest::Error) -> RegistryError {
    if error.is_timeout() {
        RegistryError::Timeout(error.to_string())
    } else {
        RegistryError::InvalidResponse(error.to_string())
    }
}

#[derive(Deserialize)]
struct RegistryContractFields {
    entity: JsonValue,
    name: String,
    precision: u8,
    #[serde(default)]
    ticker: Option<String>,
    version: u64,
    #[serde(default)]
    initial_issuer_pubkey: Option<String>,
    #[serde(default)]
    issuer_pubkey: Option<String>,
}

#[derive(Clone, Debug)]
pub struct RegistryContract {
    pub entity: JsonValue,
    pub name: String,
    pub precision: u8,
    pub ticker: Option<String>,
    pub version: u64,
    pub initial_issuer_pubkey: Option<String>,
    pub issuer_pubkey: Option<String>,
    // Preserve the authoritative contract representation exactly. Reconstructing it
    // from typed fields can change missing/null optionals and discard future fields.
    raw: Arc<JsonValue>,
}

impl<'de> serde::Deserialize<'de> for RegistryContract {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = <JsonValue as serde::Deserialize>::deserialize(deserializer)?;
        if !value.is_object() {
            return Err(serde::de::Error::custom(
                "registry contract must be an object",
            ));
        }
        let fields: RegistryContractFields =
            serde_json::from_value(value.clone()).map_err(serde::de::Error::custom)?;

        Ok(Self {
            entity: fields.entity,
            name: fields.name,
            precision: fields.precision,
            ticker: fields.ticker,
            version: fields.version,
            initial_issuer_pubkey: fields.initial_issuer_pubkey,
            issuer_pubkey: fields.issuer_pubkey,
            raw: Arc::new(value),
        })
    }
}

impl serde::Serialize for RegistryContract {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serde::Serialize::serialize(self.raw.as_ref(), serializer)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RegistryIcon {
    pub href: String,
    #[serde(flatten)]
    pub extra: JsonMap<String, JsonValue>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RegistryAsset {
    pub asset_id: AssetId,
    pub contract: RegistryContract,
    pub initial_issuer_pubkey: String,
    pub initial_issuer_pubkey_source: String,
    pub current_issuer_pubkey: String,
    #[serde(default)]
    pub issuer_pubkey_history: Vec<JsonValue>,
    pub mutable: JsonValue,
    #[serde(default)]
    pub admin: Option<JsonValue>,
    #[serde(default)]
    pub icon: Option<RegistryIcon>,
    pub status: String,
    pub created_at: String,
    pub updated_at: String,
    #[serde(flatten)]
    pub extra: JsonMap<String, JsonValue>,
}

#[derive(Serialize, Clone, Debug)]
pub struct AssetMeta {
    #[serde(
        serialize_with = "serialize_arc",
        skip_serializing_if = "arc_json_is_null"
    )]
    pub contract: Arc<JsonValue>,
    #[serde(skip_serializing_if = "JsonValue::is_null")]
    pub entity: JsonValue,
    pub precision: u8,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ticker: Option<String>,
    #[serde(serialize_with = "serialize_arc")]
    pub registry: Arc<RegistryAsset>,
}

fn serialize_arc<T, S>(value: &Arc<T>, serializer: S) -> std::result::Result<S::Ok, S::Error>
where
    T: serde::Serialize,
    S: serde::Serializer,
{
    T::serialize(value.as_ref(), serializer)
}

fn arc_json_is_null(value: &Arc<JsonValue>) -> bool {
    value.is_null()
}

impl AssetMeta {
    pub fn from_registry_asset(registry: Arc<RegistryAsset>) -> Self {
        Self {
            contract: Arc::clone(&registry.contract.raw),
            entity: registry.contract.entity.clone(),
            precision: registry.contract.precision,
            name: registry.contract.name.clone(),
            ticker: registry.contract.ticker.clone(),
            registry,
        }
    }
}

#[derive(Debug)]
pub struct RegistryAssetList {
    pub total_count: usize,
    pub items: Vec<RegistryAsset>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AssetSearchFilters {
    asset_id: Option<String>,
    domain: Option<String>,
    ticker: Option<String>,
    name: Option<String>,
    asset_type: Option<String>,
    category_tags: Vec<String>,
    trading_venue: Option<String>,
    created_after: Option<String>,
    updated_after: Option<String>,
}

impl AssetSearchFilters {
    pub fn from_query_pairs(query: &[(String, String)]) -> Result<Self> {
        let get_last = |name: &str| {
            query
                .iter()
                .rev()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value.clone())
        };
        let filters = Self {
            asset_id: get_last("asset_id"),
            domain: get_last("domain"),
            ticker: get_last("ticker"),
            name: get_last("name"),
            asset_type: get_last("asset_type"),
            category_tags: query
                .iter()
                .filter(|(key, _)| key == "category_tag")
                .map(|(_, value)| value.clone())
                .collect(),
            trading_venue: get_last("trading_venue"),
            created_after: get_last("created_after"),
            updated_after: get_last("updated_after"),
        };
        filters.validate()?;
        Ok(filters)
    }

    fn append_query<'a>(&'a self, query: &mut Vec<(&'a str, String)>) {
        append_optional_query(query, "asset_id", &self.asset_id);
        append_optional_query(query, "domain", &self.domain);
        append_optional_query(query, "ticker", &self.ticker);
        append_optional_query(query, "name", &self.name);
        append_optional_query(query, "asset_type", &self.asset_type);
        for category_tag in &self.category_tags {
            query.push(("category_tag", category_tag.clone()));
        }
        append_optional_query(query, "trading_venue", &self.trading_venue);
        append_optional_query(query, "created_after", &self.created_after);
        append_optional_query(query, "updated_after", &self.updated_after);
    }

    fn validate(&self) -> Result<()> {
        if let Some(value) = &self.asset_id {
            ensure!(
                (1..=64).contains(&value.len())
                    && value.bytes().all(|byte| byte.is_ascii_hexdigit()),
                "invalid asset_id: expected 1 to 64 hexadecimal characters"
            );
        }
        if let Some(value) = &self.domain {
            ensure!(
                is_valid_registry_domain(value),
                "invalid domain: expected a valid domain name"
            );
        }
        validate_optional_registry_text("ticker", self.ticker.as_deref(), 24)?;
        validate_optional_registry_text("name", self.name.as_deref(), 255)?;
        validate_optional_registry_enum(
            "asset_type",
            self.asset_type.as_deref(),
            &["AMP_asset", "stablecoin", "security_token", "other"],
        )?;
        for value in &self.category_tags {
            validate_registry_enum(
                "category_tag",
                value,
                &["stablecoin", "bond", "fixed-income", "tokenized"],
            )?;
        }
        validate_optional_registry_enum(
            "trading_venue",
            self.trading_venue.as_deref(),
            &["sideswap", "bitfinex"],
        )?;
        validate_optional_registry_timestamp("created_after", self.created_after.as_deref())?;
        validate_optional_registry_timestamp("updated_after", self.updated_after.as_deref())?;
        Ok(())
    }
}

fn append_optional_query<'a>(
    query: &mut Vec<(&'a str, String)>,
    name: &'a str,
    value: &Option<String>,
) {
    if let Some(value) = value {
        query.push((name, value.clone()));
    }
}

fn validate_optional_registry_text(name: &str, value: Option<&str>, max_len: usize) -> Result<()> {
    if let Some(value) = value {
        ensure!(
            value.chars().count() <= max_len && !value.contains('\0'),
            "invalid {}: expected at most {} characters without NUL",
            name,
            max_len
        );
    }
    Ok(())
}

fn validate_optional_registry_enum(
    name: &str,
    value: Option<&str>,
    allowed: &[&str],
) -> Result<()> {
    if let Some(value) = value {
        validate_registry_enum(name, value, allowed)?;
    }
    Ok(())
}

fn validate_registry_enum(name: &str, value: &str, allowed: &[&str]) -> Result<()> {
    ensure!(
        allowed
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(value)),
        "invalid {}: expected one of {}",
        name,
        allowed.join(", ")
    );
    Ok(())
}

fn is_valid_registry_domain(value: &str) -> bool {
    if !(3..=255).contains(&value.len()) || !value.is_ascii() {
        return false;
    }
    let value = value.strip_suffix('.').unwrap_or(value);
    // `Host::parse` covers the structural checks (non-empty ASCII labels within [1, 63],
    // no leading/trailing hyphens, no invalid characters); we still need to reject IP
    // literals and single-label hostnames, and require an alphabetic TLD.
    let domain = match Host::parse(value) {
        Ok(Host::Domain(domain)) => domain,
        _ => return false,
    };
    let labels: Vec<&str> = domain.split('.').collect();
    labels.len() >= 2
        && labels
            .last()
            .and_then(|label| label.bytes().next())
            .map(|byte| byte.is_ascii_alphabetic())
            .unwrap_or(false)
}

fn validate_optional_registry_timestamp(name: &str, value: Option<&str>) -> Result<()> {
    if let Some(value) = value {
        OffsetDateTime::parse(value, &Rfc3339)
            .map_err(|_| format!("invalid {}: expected an RFC 3339 date-time", name))?;
    }
    Ok(())
}

#[derive(Deserialize, Debug)]
struct RegistryListResponse {
    items: Vec<RegistryAsset>,
    page: usize,
    page_size: usize,
    total_count: Option<usize>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AssetSorting {
    AssetIdAsc,
    AssetIdDesc,
    NameAsc,
    NameDesc,
    DomainAsc,
    DomainDesc,
    TickerAsc,
    TickerDesc,
    CreatedAtAsc,
    CreatedAtDesc,
    UpdatedAtAsc,
    UpdatedAtDesc,
}

impl AssetSorting {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AssetIdAsc => "asset_id_asc",
            Self::AssetIdDesc => "asset_id_desc",
            Self::NameAsc => "name_asc",
            Self::NameDesc => "name_desc",
            Self::DomainAsc => "domain_asc",
            Self::DomainDesc => "domain_desc",
            Self::TickerAsc => "ticker_asc",
            Self::TickerDesc => "ticker_desc",
            Self::CreatedAtAsc => "created_at_asc",
            Self::CreatedAtDesc => "created_at_desc",
            Self::UpdatedAtAsc => "updated_at_asc",
            Self::UpdatedAtDesc => "updated_at_desc",
        }
    }

    pub fn from_query_params(query: &HashMap<String, String>) -> Result<Self> {
        if let Some(sort) = query.get("sort") {
            ensure!(
                !query.contains_key("sort_field") && !query.contains_key("sort_dir"),
                "cannot combine sort with sort_field or sort_dir"
            );
            return match sort.as_str() {
                "asset_id_asc" => Ok(Self::AssetIdAsc),
                "asset_id_desc" => Ok(Self::AssetIdDesc),
                "name_asc" => Ok(Self::NameAsc),
                "name_desc" => Ok(Self::NameDesc),
                "domain_asc" => Ok(Self::DomainAsc),
                "domain_desc" => Ok(Self::DomainDesc),
                "ticker_asc" => Ok(Self::TickerAsc),
                "ticker_desc" => Ok(Self::TickerDesc),
                "created_at_asc" => Ok(Self::CreatedAtAsc),
                "created_at_desc" => Ok(Self::CreatedAtDesc),
                "updated_at_asc" => Ok(Self::UpdatedAtAsc),
                "updated_at_desc" => Ok(Self::UpdatedAtDesc),
                _ => bail!("invalid asset registry sort"),
            };
        }

        let field = query
            .get("sort_field")
            .map(String::as_str)
            .unwrap_or("ticker");
        let direction = query.get("sort_dir").map(String::as_str).unwrap_or("asc");
        match (field, direction) {
            ("name", "asc") => Ok(Self::NameAsc),
            ("name", "desc") => Ok(Self::NameDesc),
            ("domain", "asc") => Ok(Self::DomainAsc),
            ("domain", "desc") => Ok(Self::DomainDesc),
            ("ticker", "asc") => Ok(Self::TickerAsc),
            ("ticker", "desc") => Ok(Self::TickerDesc),
            ("name" | "domain" | "ticker", _) => bail!("invalid sort direction"),
            _ => bail!("invalid sort field"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::str::FromStr;
    use std::sync::mpsc;
    use std::thread;

    const ASSET_ID_A: &str = "0000000000000000000000000000000000000000000000000000000000000001";
    const ASSET_ID_B: &str = "0000000000000000000000000000000000000000000000000000000000000002";
    const ASSET_ID_C: &str = "0000000000000000000000000000000000000000000000000000000000000003";

    fn search_filters(pairs: &[(&str, &str)]) -> AssetSearchFilters {
        AssetSearchFilters::from_query_pairs(
            &pairs
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect::<Vec<_>>(),
        )
        .unwrap()
    }

    fn asset_response(asset_id: &str, name: &str, ticker: Option<&str>) -> JsonValue {
        json!({
            "asset_id": asset_id,
            "contract": {
                "entity": {"domain": "example.com"},
                "name": name,
                "precision": 8,
                "ticker": ticker,
                "version": 1,
                "custom_contract_field": "preserved"
            },
            "initial_issuer_pubkey": format!("02{}", "11".repeat(32)),
            "initial_issuer_pubkey_source": "contract",
            "current_issuer_pubkey": format!("02{}", "11".repeat(32)),
            "issuer_pubkey_history": [],
            "mutable": {"category_tags": ["stablecoin"], "custom": {"website": "https://example.com"}},
            "admin": {"featured": true},
            "icon": {"href": format!("/v2/assets/{}/icon/{}.png", asset_id, "22".repeat(32))},
            "status": "active",
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-02T00:00:00Z",
            "future_field": {"preserved": true}
        })
    }

    fn mock_server(
        responses: Vec<(u16, JsonValue)>,
    ) -> (Url, mpsc::Receiver<String>, thread::JoinHandle<()>) {
        mock_server_with_delays(
            responses
                .into_iter()
                .map(|(status, body)| (status, body, Duration::ZERO))
                .collect(),
        )
    }

    fn mock_server_with_delays(
        responses: Vec<(u16, JsonValue, Duration)>,
    ) -> (Url, mpsc::Receiver<String>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (request_tx, request_rx) = mpsc::channel();
        let thread = thread::spawn(move || {
            for (status, body, delay) in responses {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(1)))
                    .unwrap();
                let mut request = vec![0u8; 8192];
                let len = stream.read(&mut request).unwrap();
                request.truncate(len);
                request_tx.send(String::from_utf8(request).unwrap()).ok();
                thread::sleep(delay);

                let body = serde_json::to_string(&body).unwrap();
                let reason = match status {
                    200 => "OK",
                    404 => "Not Found",
                    503 => "Service Unavailable",
                    _ => "Error",
                };
                let response = format!(
                    "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    status,
                    reason,
                    body.len(),
                    body
                );
                stream.write_all(response.as_bytes()).unwrap();
            }
        });
        (
            Url::parse(&format!("http://{}/api", addr)).unwrap(),
            request_rx,
            thread,
        )
    }

    fn mock_unbounded_body(body: Vec<u8>) -> (Url, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let thread = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = vec![0u8; 8192];
            let _ = stream.read(&mut request);
            let header =
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n";
            let _ = stream.write_all(header.as_bytes());
            let _ = stream.write_all(&body);
        });
        (Url::parse(&format!("http://{}/api", addr)).unwrap(), thread)
    }

    fn mock_redirect() -> (Url, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let thread = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = vec![0u8; 8192];
            let _ = stream.read(&mut request);
            let response = format!(
                "HTTP/1.1 302 Found\r\nLocation: /api/v2/assets/{}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                ASSET_ID_A
            );
            stream.write_all(response.as_bytes()).unwrap();
        });
        (Url::parse(&format!("http://{}/api", addr)).unwrap(), thread)
    }

    #[tokio::test]
    async fn get_asset_projects_legacy_fields_and_preserves_v2_data() {
        let body = asset_response(ASSET_ID_A, "Asset A", None);
        let original_contract = body["contract"].clone();
        let (url, requests, server) = mock_server(vec![(200, body)]);
        let expected_icon = url
            .join(&format!(
                "/v2/assets/{}/icon/{}.png",
                ASSET_ID_A,
                "22".repeat(32)
            ))
            .unwrap()
            .to_string();
        let client = RegistryClient::new(url).unwrap();
        let id = AssetId::from_str(ASSET_ID_A).unwrap();

        let asset = client.get_asset(&id).await.unwrap().unwrap();
        let metadata = AssetMeta::from_registry_asset(asset);

        assert_eq!(metadata.name, "Asset A");
        assert_eq!(metadata.ticker, None);
        assert_eq!(metadata.contract.as_ref(), &original_contract);
        assert_eq!(
            serde_json::to_value(&metadata.registry.contract).unwrap(),
            original_contract
        );
        assert_eq!(metadata.contract["custom_contract_field"], "preserved");
        assert_eq!(metadata.registry.mutable["category_tags"][0], "stablecoin");
        assert_eq!(metadata.registry.extra["future_field"]["preserved"], true);
        assert_eq!(metadata.registry.icon.as_ref().unwrap().href, expected_icon);
        assert!(requests
            .recv()
            .unwrap()
            .starts_with(&format!("GET /api/v2/assets/{} HTTP/1.1", ASSET_ID_A)));
        server.join().unwrap();
    }

    #[tokio::test]
    async fn get_asset_maps_not_found_and_rejects_mismatched_id() {
        let mismatched = asset_response(ASSET_ID_A, "Asset A", Some("AAA"));
        let (url, _, server) = mock_server(vec![(404, json!({})), (200, mismatched)]);
        let client = RegistryClient::new(url).unwrap();
        let id_a = AssetId::from_str(ASSET_ID_A).unwrap();
        let id_b = AssetId::from_str(ASSET_ID_B).unwrap();

        assert!(client.get_asset(&id_a).await.unwrap().is_none());
        assert!(matches!(
            client.get_asset(&id_b).await,
            Err(RegistryError::InvalidResponse(_))
        ));
        server.join().unwrap();
    }

    #[tokio::test]
    async fn asset_cache_hits_and_coalesces_concurrent_misses() {
        let body = asset_response(ASSET_ID_A, "Asset A", Some("AAA"));
        let (url, requests, server) =
            mock_server_with_delays(vec![(200, body, Duration::from_millis(40))]);
        let client = RegistryClient::new(url).unwrap();
        let id = AssetId::from_str(ASSET_ID_A).unwrap();

        let (first, second) = tokio::join!(client.get_asset(&id), client.get_asset(&id));
        assert!(first.unwrap().is_some());
        assert!(second.unwrap().is_some());
        assert!(client.get_asset(&id).await.unwrap().is_some());
        assert!(requests.recv().is_ok());
        server.join().unwrap();
    }

    #[tokio::test]
    async fn expired_success_is_served_while_one_refresh_runs() {
        let first = asset_response(ASSET_ID_A, "Asset A", Some("AAA"));
        let updated = asset_response(ASSET_ID_A, "Updated Asset A", Some("AAA"));
        let (url, requests, server) = mock_server_with_delays(vec![
            (200, first, Duration::ZERO),
            (200, updated, Duration::from_millis(50)),
        ]);
        let client = RegistryClient::with_options(
            url,
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
            10,
            2,
            Duration::from_secs(1),
        )
        .unwrap();
        let id = AssetId::from_str(ASSET_ID_A).unwrap();

        assert_eq!(
            client.get_asset(&id).await.unwrap().unwrap().contract.name,
            "Asset A"
        );
        assert!(requests.recv().is_ok());
        match client.asset_cache.lock().await.get_mut(&id) {
            Some(AssetCacheEntry::Ready { fetched_at, .. }) => {
                *fetched_at = Instant::now() - Duration::from_secs(2)
            }
            _ => panic!("successful fetch was not cached"),
        }

        let stale = client.get_asset_with_status(&id).await.unwrap();
        assert!(stale.stale);
        assert_eq!(stale.asset.unwrap().contract.name, "Asset A");
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match requests.try_recv() {
                    Ok(_) => break,
                    Err(mpsc::TryRecvError::Empty) => tokio::task::yield_now().await,
                    Err(error) => panic!("mock registry stopped early: {}", error),
                }
            }
        })
        .await
        .expect("refresh request did not start");

        let fresh = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let result = client.get_asset_with_status(&id).await.unwrap();
                if !result.stale {
                    break result;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("refresh did not complete");
        assert!(!fresh.stale);
        assert_eq!(fresh.asset.unwrap().contract.name, "Updated Asset A");
        server.join().unwrap();
    }

    #[tokio::test]
    async fn failed_refresh_keeps_the_last_success_and_backs_off() {
        let first = asset_response(ASSET_ID_A, "Asset A", Some("AAA"));
        let updated = asset_response(ASSET_ID_A, "Updated Asset A", Some("AAA"));
        let (url, requests, server) = mock_server(vec![
            (200, first),
            (503, json!({"detail": "unavailable"})),
            (200, updated),
        ]);
        let client = RegistryClient::with_options(
            url,
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(15),
            10,
            2,
            Duration::from_secs(1),
        )
        .unwrap();
        let id = AssetId::from_str(ASSET_ID_A).unwrap();

        assert!(client.get_asset(&id).await.unwrap().is_some());
        assert!(requests.recv().is_ok());
        match client.asset_cache.lock().await.get_mut(&id) {
            Some(AssetCacheEntry::Ready { fetched_at, .. }) => {
                *fetched_at = Instant::now() - Duration::from_secs(16)
            }
            _ => panic!("successful fetch was not cached"),
        }

        let stale = client.get_asset_with_status(&id).await.unwrap();
        assert!(stale.stale);
        assert_eq!(stale.asset.unwrap().contract.name, "Asset A");
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match requests.try_recv() {
                    Ok(_) => break,
                    Err(mpsc::TryRecvError::Empty) => tokio::task::yield_now().await,
                    Err(error) => panic!("mock registry stopped early: {}", error),
                }
            }
        })
        .await
        .expect("refresh request did not start");

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if matches!(
                    client.asset_cache.lock().await.get(&id),
                    Some(AssetCacheEntry::Ready {
                        result: Ok(RegistryAssetLookup { stale: true, .. }),
                        ..
                    })
                ) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("failed refresh did not restore the stale value");

        let stale = client.get_asset_with_status(&id).await.unwrap();
        assert!(stale.stale);
        assert_eq!(stale.asset.unwrap().contract.name, "Asset A");
        assert!(matches!(
            requests.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));

        match client.asset_cache.lock().await.get_mut(&id) {
            Some(AssetCacheEntry::Ready { fetched_at, .. }) => {
                *fetched_at = Instant::now() - client.error_cache_ttl - Duration::from_millis(1)
            }
            _ => panic!("failed refresh did not leave a cache entry"),
        }
        assert!(client.get_asset_with_status(&id).await.unwrap().stale);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match requests.try_recv() {
                    Ok(_) => break,
                    Err(mpsc::TryRecvError::Empty) => tokio::task::yield_now().await,
                    Err(error) => panic!("mock registry stopped early: {}", error),
                }
            }
        })
        .await
        .expect("refresh retry request did not start");
        let fresh = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let result = client.get_asset_with_status(&id).await.unwrap();
                if !result.stale {
                    break result;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("refresh retry did not complete");
        assert_eq!(fresh.asset.unwrap().contract.name, "Updated Asset A");
        server.join().unwrap();
    }

    #[tokio::test]
    async fn pruning_expired_fetch_notifies_waiters_before_eviction() {
        let client = RegistryClient::new(Url::parse("http://127.0.0.1:1/").unwrap()).unwrap();
        let id = AssetId::from_str(ASSET_ID_A).unwrap();
        let now = Instant::now();
        let mut cache = client.asset_cache.lock().await;
        let (sender, receiver) = oneshot::channel();
        cache.insert(
            id,
            AssetCacheEntry::Fetching {
                started_at: now - Duration::from_secs(60),
                stale: None,
                waiters: vec![sender],
            },
        );

        prune_asset_cache(
            &mut cache,
            now,
            client.error_cache_ttl,
            client.fetch_poison_timeout,
            client.asset_cache_max_entries,
        );

        assert!(!cache.contains_key(&id));
        assert!(matches!(
            receiver.await.unwrap(),
            Err(RegistryError::Timeout(_))
        ));
    }

    #[tokio::test]
    async fn pruning_expired_refresh_preserves_metadata_and_notifies_waiters() {
        let client = RegistryClient::new(Url::parse("http://127.0.0.1:1/").unwrap()).unwrap();
        let id = AssetId::from_str(ASSET_ID_A).unwrap();
        let asset = Arc::new(
            serde_json::from_value::<RegistryAsset>(asset_response(ASSET_ID_A, "Asset A", None))
                .unwrap(),
        );
        let now = Instant::now();
        let (sender, receiver) = oneshot::channel();
        let mut cache = client.asset_cache.lock().await;
        cache.insert(
            id,
            AssetCacheEntry::Fetching {
                started_at: now - client.fetch_poison_timeout,
                stale: Some(Some(Arc::clone(&asset))),
                waiters: vec![sender],
            },
        );

        prune_asset_cache(
            &mut cache,
            now,
            client.error_cache_ttl,
            client.fetch_poison_timeout,
            client.asset_cache_max_entries,
        );
        assert!(matches!(cache.get(&id), Some(AssetCacheEntry::Ready { .. })));
        drop(cache);

        let parked = receiver.await.unwrap().unwrap();
        assert!(parked.stale);
        assert!(Arc::ptr_eq(parked.asset.as_ref().unwrap(), &asset));
        let cached = client.get_asset_with_status(&id).await.unwrap();
        assert!(cached.stale);
        assert!(Arc::ptr_eq(cached.asset.as_ref().unwrap(), &asset));
    }

    #[tokio::test]
    async fn expired_ready_entry_is_kept_until_capacity_requires_eviction() {
        let client = RegistryClient::new(Url::parse("http://127.0.0.1:1/").unwrap()).unwrap();
        let id_a = AssetId::from_str(ASSET_ID_A).unwrap();
        let id_b = AssetId::from_str(ASSET_ID_B).unwrap();
        let now = Instant::now();
        let mut cache = client.asset_cache.lock().await;
        cache.insert(
            id_a,
            AssetCacheEntry::Ready {
                fetched_at: now - client.asset_cache_ttl - Duration::from_secs(1),
                result: Ok(RegistryAssetLookup {
                    asset: None,
                    stale: false,
                }),
            },
        );

        prune_asset_cache(
            &mut cache,
            now,
            client.error_cache_ttl,
            client.fetch_poison_timeout,
            2,
        );
        assert!(cache.contains_key(&id_a));

        cache.insert(
            id_b,
            AssetCacheEntry::Ready {
                fetched_at: now,
                result: Ok(RegistryAssetLookup {
                    asset: None,
                    stale: false,
                }),
            },
        );
        prune_asset_cache(
            &mut cache,
            now,
            client.error_cache_ttl,
            client.fetch_poison_timeout,
            2,
        );
        assert!(!cache.contains_key(&id_a));
        assert!(cache.contains_key(&id_b));
    }

    #[tokio::test]
    async fn poisoned_fetching_entry_does_not_block_new_callers() {
        let body = asset_response(ASSET_ID_A, "Asset A", Some("AAA"));
        let (url, requests, server) = mock_server(vec![(200, body)]);
        let client = RegistryClient::new(url).unwrap();
        let id = AssetId::from_str(ASSET_ID_A).unwrap();
        client.asset_cache.lock().await.insert(
            id,
            AssetCacheEntry::Fetching {
                started_at: Instant::now() - Duration::from_secs(60),
                stale: None,
                waiters: vec![],
            },
        );

        assert!(client.get_asset(&id).await.unwrap().is_some());
        assert!(requests.recv().is_ok());
        server.join().unwrap();
    }

    #[tokio::test]
    async fn poisoned_fetching_entry_transfers_existing_waiters() {
        let body = asset_response(ASSET_ID_A, "Asset A", Some("AAA"));
        let (url, requests, server) = mock_server(vec![(200, body)]);
        let client = RegistryClient::new(url).unwrap();
        let id = AssetId::from_str(ASSET_ID_A).unwrap();
        let (sender, parked) = oneshot::channel();
        client.asset_cache.lock().await.insert(
            id,
            AssetCacheEntry::Fetching {
                started_at: Instant::now() - client.fetch_poison_timeout - Duration::from_secs(1),
                stale: None,
                waiters: vec![sender],
            },
        );

        let (replacement, parked) = tokio::join!(
            client.get_asset(&id),
            tokio::time::timeout(Duration::from_secs(1), parked)
        );

        assert!(replacement.unwrap().is_some());
        assert!(parked.unwrap().unwrap().unwrap().asset.is_some());
        assert!(requests.recv().is_ok());
        server.join().unwrap();
    }

    #[tokio::test]
    async fn stale_fetch_completion_does_not_overwrite_replacement() {
        let client = RegistryClient::new(Url::parse("http://127.0.0.1:1/").unwrap()).unwrap();
        let id = AssetId::from_str(ASSET_ID_A).unwrap();
        let stale_started_at = Instant::now() - Duration::from_secs(60);
        let replacement_started_at = Instant::now();
        client.asset_cache.lock().await.insert(
            id,
            AssetCacheEntry::Fetching {
                started_at: replacement_started_at,
                stale: None,
                waiters: vec![],
            },
        );

        client
            .complete_asset_fetch(id, stale_started_at, Ok(None))
            .await;

        assert!(matches!(
            client.asset_cache.lock().await.get(&id),
            Some(AssetCacheEntry::Fetching { started_at, .. })
                if *started_at == replacement_started_at
        ));
    }

    #[tokio::test]
    async fn cancelling_the_leader_does_not_poison_a_fetch_waiting_for_admission() {
        let body = asset_response(ASSET_ID_A, "Asset A", Some("AAA"));
        let (url, requests, server) = mock_server(vec![(200, body)]);
        let client = RegistryClient::with_options(
            url,
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
            10,
            1,
            Duration::from_secs(1),
        )
        .unwrap();
        let id = AssetId::from_str(ASSET_ID_A).unwrap();
        let permit = client.concurrency.clone().acquire_owned().await.unwrap();
        let leader_client = client.clone();
        let leader = tokio::spawn(async move { leader_client.get_asset(&id).await });

        loop {
            if matches!(
                client.asset_cache.lock().await.get(&id),
                Some(AssetCacheEntry::Fetching { .. })
            ) {
                break;
            }
            tokio::task::yield_now().await;
        }
        leader.abort();
        drop(permit);

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match requests.try_recv() {
                    Ok(_) => break,
                    Err(mpsc::TryRecvError::Empty) => tokio::task::yield_now().await,
                    Err(error) => panic!("mock registry stopped early: {}", error),
                }
            }
        })
        .await
        .expect("owned fetch did not continue after leader cancellation");
        assert!(client.get_asset(&id).await.unwrap().is_some());
        server.join().unwrap();
    }

    #[tokio::test]
    async fn fetching_entries_cannot_exceed_the_cache_capacity() {
        let client = RegistryClient::with_options(
            Url::parse("http://127.0.0.1:1/").unwrap(),
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
            1,
            1,
            Duration::from_secs(1),
        )
        .unwrap();
        let id_a = AssetId::from_str(ASSET_ID_A).unwrap();
        let id_b = AssetId::from_str(ASSET_ID_B).unwrap();
        client.asset_cache.lock().await.insert(
            id_a,
            AssetCacheEntry::Fetching {
                started_at: Instant::now(),
                stale: None,
                waiters: vec![],
            },
        );

        assert!(matches!(
            client.get_asset(&id_b).await,
            Err(RegistryError::Overloaded(_))
        ));
        assert_eq!(client.asset_cache.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn receiver_timeout_returns_registry_error_timeout() {
        let mut client = RegistryClient::with_options(
            Url::parse("http://127.0.0.1:1/").unwrap(),
            Duration::from_millis(10),
            Duration::from_millis(10),
            Duration::from_secs(1),
            10,
            1,
            Duration::from_millis(10),
        )
        .unwrap();
        client.fetch_wait_timeout = Duration::from_millis(20);
        let id = AssetId::from_str(ASSET_ID_A).unwrap();
        let (sender, _receiver) = oneshot::channel();
        client.asset_cache.lock().await.insert(
            id,
            AssetCacheEntry::Fetching {
                started_at: Instant::now(),
                stale: None,
                waiters: vec![sender],
            },
        );

        assert!(matches!(
            client.get_asset(&id).await,
            Err(RegistryError::Timeout(_))
        ));
    }

    #[tokio::test]
    async fn error_result_is_cached_for_ttl() {
        let success = asset_response(ASSET_ID_A, "Asset A", Some("AAA"));
        let (url, requests, server) = mock_server(vec![
            (503, json!({"detail": "unavailable"})),
            (200, success),
        ]);
        let client = RegistryClient::with_options(
            url,
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_millis(20),
            10,
            2,
            Duration::from_secs(1),
        )
        .unwrap();
        let id = AssetId::from_str(ASSET_ID_A).unwrap();

        assert!(matches!(
            client.get_asset(&id).await,
            Err(RegistryError::HttpStatus(503))
        ));
        assert!(requests.recv().is_ok());
        assert!(matches!(
            client.get_asset(&id).await,
            Err(RegistryError::HttpStatus(503))
        ));
        assert!(matches!(
            requests.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));

        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(client.get_asset(&id).await.unwrap().is_some());
        assert!(requests.recv().is_ok());
        server.join().unwrap();
    }

    #[tokio::test]
    async fn transient_errors_expire_before_successful_metadata() {
        let success = asset_response(ASSET_ID_A, "Asset A", Some("AAA"));
        let (url, requests, server) = mock_server(vec![
            (503, json!({"detail": "unavailable"})),
            (200, success),
        ]);
        let client = RegistryClient::with_options(
            url,
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(15),
            10,
            2,
            Duration::from_secs(1),
        )
        .unwrap();
        let id = AssetId::from_str(ASSET_ID_A).unwrap();

        assert!(matches!(
            client.get_asset(&id).await,
            Err(RegistryError::HttpStatus(503))
        ));
        assert!(requests.recv().is_ok());
        assert!(client.error_cache_ttl < client.asset_cache_ttl);
        match client.asset_cache.lock().await.get_mut(&id) {
            Some(AssetCacheEntry::Ready { fetched_at, .. }) => {
                *fetched_at = Instant::now() - client.error_cache_ttl - Duration::from_millis(1)
            }
            _ => panic!("failed fetch was not cached"),
        }

        assert!(client.get_asset(&id).await.unwrap().is_some());
        assert!(requests.recv().is_ok());
        server.join().unwrap();
    }

    #[test]
    fn fetch_timeouts_include_completion_headroom() {
        let request_timeout = Duration::from_secs(5);
        let permit_timeout = Duration::from_secs(5);
        let client = RegistryClient::with_options(
            Url::parse("http://127.0.0.1:1/").unwrap(),
            Duration::from_secs(2),
            request_timeout,
            Duration::from_secs(15),
            10,
            2,
            permit_timeout,
        )
        .unwrap();

        assert!(client.fetch_wait_timeout > permit_timeout + request_timeout);
        assert!(client.fetch_poison_timeout > client.fetch_wait_timeout);
    }

    #[tokio::test]
    async fn cache_hit_shares_arc_instance() {
        let body = asset_response(ASSET_ID_A, "Asset A", Some("AAA"));
        let (url, requests, server) = mock_server(vec![(200, body)]);
        let client = RegistryClient::new(url).unwrap();
        let id = AssetId::from_str(ASSET_ID_A).unwrap();

        let first = client.get_asset(&id).await.unwrap().unwrap();
        let second = client.get_asset(&id).await.unwrap().unwrap();

        assert!(Arc::ptr_eq(&first, &second));
        assert!(requests.recv().is_ok());
        server.join().unwrap();
    }

    #[tokio::test]
    async fn client_queues_and_times_out_excess_concurrent_requests() {
        let body = asset_response(ASSET_ID_A, "Asset A", Some("AAA"));
        let (url, requests, server) =
            mock_server_with_delays(vec![(200, body, Duration::from_millis(400))]);
        let client = RegistryClient::with_options(
            url,
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
            10,
            1,
            Duration::from_millis(100),
        )
        .unwrap();
        let id_a = AssetId::from_str(ASSET_ID_A).unwrap();
        let id_b = AssetId::from_str(ASSET_ID_B).unwrap();
        let first_client = client.clone();
        let first = tokio::spawn(async move { first_client.get_asset(&id_a).await });

        loop {
            match requests.try_recv() {
                Ok(_) => break,
                Err(mpsc::TryRecvError::Empty) => {
                    tokio::time::sleep(Duration::from_millis(1)).await
                }
                Err(error) => panic!("mock registry stopped early: {}", error),
            }
        }
        assert!(matches!(
            client.get_asset(&id_b).await,
            Err(RegistryError::Overloaded(_))
        ));
        assert!(first.await.unwrap().unwrap().is_some());
        server.join().unwrap();
    }

    #[tokio::test]
    async fn queued_caller_gets_permit_when_first_completes() {
        let first_body = asset_response(ASSET_ID_A, "Asset A", Some("AAA"));
        let second_body = asset_response(ASSET_ID_B, "Asset B", Some("BBB"));
        let (url, requests, server) = mock_server_with_delays(vec![
            (200, first_body, Duration::from_millis(50)),
            (200, second_body, Duration::ZERO),
        ]);
        let client = RegistryClient::with_options(
            url,
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
            10,
            1,
            Duration::from_secs(5),
        )
        .unwrap();
        let id_a = AssetId::from_str(ASSET_ID_A).unwrap();
        let id_b = AssetId::from_str(ASSET_ID_B).unwrap();
        let first_client = client.clone();
        let first = tokio::spawn(async move { first_client.get_asset(&id_a).await });

        loop {
            match requests.try_recv() {
                Ok(_) => break,
                Err(mpsc::TryRecvError::Empty) => {
                    tokio::time::sleep(Duration::from_millis(1)).await
                }
                Err(error) => panic!("mock registry stopped early: {}", error),
            }
        }
        let second = client.get_asset(&id_b).await;

        assert!(first.await.unwrap().unwrap().is_some());
        assert!(second.unwrap().is_some());
        assert!(requests.recv().is_ok());
        server.join().unwrap();
    }

    #[tokio::test]
    async fn acquire_failure_cleans_up_pending_fetch_entry() {
        let client = RegistryClient::with_options(
            Url::parse("http://127.0.0.1:1/").unwrap(),
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
            10,
            1,
            Duration::from_millis(10),
        )
        .unwrap();
        let id = AssetId::from_str(ASSET_ID_A).unwrap();
        let permit = client.concurrency.clone().acquire_owned().await.unwrap();

        assert!(matches!(
            client.get_asset(&id).await,
            Err(RegistryError::Overloaded(_))
        ));
        assert!(!client.asset_cache.lock().await.contains_key(&id));
        drop(permit);
    }

    #[tokio::test]
    async fn client_limits_streamed_response_bodies() {
        let (url, server) = mock_unbounded_body(vec![b' '; REGISTRY_MAX_ASSET_RESPONSE_SIZE + 1]);
        let client = RegistryClient::new(url).unwrap();
        let id = AssetId::from_str(ASSET_ID_A).unwrap();

        assert!(matches!(
            client.get_asset(&id).await,
            Err(RegistryError::InvalidResponse(message))
                if message.contains("exceeds")
        ));
        server.join().unwrap();
    }

    #[tokio::test]
    async fn client_rejects_redirects() {
        let (url, server) = mock_redirect();
        let client = RegistryClient::new(url).unwrap();
        let id = AssetId::from_str(ASSET_ID_A).unwrap();

        assert!(matches!(
            client.get_asset(&id).await,
            Err(RegistryError::HttpStatus(302))
        ));
        server.join().unwrap();
    }

    #[test]
    fn client_rejects_credentialed_base_urls() {
        assert!(matches!(
            RegistryClient::new(
                Url::parse("https://user:pass@registry.example/api").unwrap()
            ),
            Err(RegistryError::InvalidBaseUrl(message))
                if message.contains("must not contain a username or password")
        ));
    }

    #[test]
    fn icon_urls_never_include_userinfo() {
        let client =
            RegistryClient::new(Url::parse("https://registry.example/api").unwrap()).unwrap();
        let mut asset: RegistryAsset =
            serde_json::from_value(asset_response(ASSET_ID_A, "Asset A", Some("AAA"))).unwrap();
        asset.icon.as_mut().unwrap().href =
            "https://user:pass@registry.example/icon.png".to_string();

        client.make_icon_absolute(&mut asset).unwrap();

        assert_eq!(
            asset.icon.as_ref().unwrap().href,
            "https://registry.example/icon.png"
        );
    }

    #[test]
    fn registry_timeout_environment_values_are_milliseconds() {
        assert_eq!(
            parse_registry_timeout(REGISTRY_CONNECT_TIMEOUT_ENV, "2500").unwrap(),
            Duration::from_millis(2500)
        );
        assert!(parse_registry_timeout(REGISTRY_CONNECT_TIMEOUT_ENV, "0").is_err());
        assert!(parse_registry_timeout(REGISTRY_REQUEST_TIMEOUT_ENV, "invalid").is_err());
    }

    #[tokio::test]
    async fn list_assets_translates_unaligned_offsets_across_pages() {
        let first = json!({
            "items": [
                asset_response(ASSET_ID_A, "Asset A", Some("AAA")),
                asset_response(ASSET_ID_B, "Asset B", Some("BBB"))
            ],
            "page": 2,
            "page_size": 2,
            "total_count": 5,
            "total_pages": 3
        });
        let second = json!({
            "items": [asset_response(ASSET_ID_C, "Asset C", Some("CCC"))],
            "page": 3,
            "page_size": 2,
            "total_count": 5,
            "total_pages": 3
        });
        let (url, requests, server) = mock_server(vec![(200, first), (200, second)]);
        let client = RegistryClient::new(url).unwrap();

        let result = client
            .list_assets(
                3,
                2,
                AssetSorting::UpdatedAtAsc,
                &search_filters(&[
                    ("asset_id", "aB12"),
                    ("domain", "Example.com"),
                    ("ticker", "EXM"),
                    ("name", "Example"),
                    ("asset_type", "AMP_asset"),
                    ("category_tag", "stablecoin"),
                    ("category_tag", "bond"),
                    ("trading_venue", "sideswap"),
                    ("created_after", "2026-01-01T00:00:00Z"),
                    ("updated_after", "2026-02-01T12:30:00-05:00"),
                ]),
            )
            .await
            .unwrap();

        assert_eq!(result.total_count, 5);
        assert_eq!(result.items.len(), 2);
        let first_request = requests.recv().unwrap();
        let second_request = requests.recv().unwrap();
        assert!(first_request.contains("page=2"));
        assert!(first_request.contains("page_size=2"));
        assert!(first_request.contains("sort=updated_at_asc"));
        assert!(first_request.contains("asset_id=aB12"));
        assert!(first_request.contains("domain=Example.com"));
        assert!(first_request.contains("ticker=EXM"));
        assert!(first_request.contains("name=Example"));
        assert!(first_request.contains("asset_type=AMP_asset"));
        assert!(first_request.contains("category_tag=stablecoin"));
        assert!(first_request.contains("category_tag=bond"));
        assert!(first_request.contains("trading_venue=sideswap"));
        assert!(first_request.contains("created_after=2026-01-01T00%3A00%3A00Z"));
        assert!(first_request.contains("updated_after=2026-02-01T12%3A30%3A00-05%3A00"));
        assert!(second_request.contains("page=3"));
        assert!(second_request.contains("sort=updated_at_asc"));
        assert!(second_request.contains("name=Example"));
        assert!(second_request.contains("category_tag=stablecoin"));
        assert!(second_request.contains("category_tag=bond"));
        assert!(second_request.contains("created_after=2026-01-01T00%3A00%3A00Z"));
        assert!(second_request.contains("updated_after=2026-02-01T12%3A30%3A00-05%3A00"));
        server.join().unwrap();
    }

    #[tokio::test]
    async fn list_assets_tolerates_total_changes_between_pages() {
        let first = json!({
            "items": [
                asset_response(ASSET_ID_A, "Asset A", Some("AAA")),
                asset_response(ASSET_ID_B, "Asset B", Some("BBB"))
            ],
            "page": 2,
            "page_size": 2,
            "total_count": 5
        });
        let second = json!({
            "items": [
                asset_response(ASSET_ID_B, "Asset B", Some("BBB")),
                asset_response(ASSET_ID_C, "Asset C", Some("CCC"))
            ],
            "page": 3,
            "page_size": 2,
            "total_count": 6
        });
        let (url, _, server) = mock_server(vec![(200, first), (200, second)]);
        let client = RegistryClient::new(url).unwrap();

        let result = client
            .list_assets(
                3,
                2,
                AssetSorting::UpdatedAtAsc,
                &AssetSearchFilters::default(),
            )
            .await
            .unwrap();
        assert_eq!(result.total_count, 5);
        assert_eq!(result.items.len(), 2);
        assert_eq!(
            result.items[0].asset_id,
            AssetId::from_str(ASSET_ID_B).unwrap()
        );
        assert_eq!(
            result.items[1].asset_id,
            AssetId::from_str(ASSET_ID_C).unwrap()
        );
        server.join().unwrap();
    }

    #[tokio::test]
    async fn list_assets_tolerates_a_short_nonfinal_page() {
        let first = json!({
            "items": [
                asset_response(ASSET_ID_A, "Asset A", Some("AAA")),
                asset_response(ASSET_ID_B, "Asset B", Some("BBB")),
                asset_response(ASSET_ID_C, "Asset C", Some("CCC"))
            ],
            "page": 2,
            "page_size": 25,
            "total_count": 100,
            "total_pages": 4
        });
        let (url, requests, server) = mock_server(vec![(200, first)]);
        let client = RegistryClient::new(url).unwrap();

        let result = client
            .list_assets(
                25,
                25,
                AssetSorting::TickerAsc,
                &AssetSearchFilters::default(),
            )
            .await
            .unwrap();
        assert_eq!(result.total_count, 100);
        assert_eq!(result.items.len(), 3);
        let request = requests.recv().unwrap();
        assert!(request.contains("page=2"));
        assert!(request.contains("page_size=25"));
        server.join().unwrap();
        assert!(matches!(
            requests.try_recv(),
            Err(mpsc::TryRecvError::Disconnected)
        ));
    }

    #[tokio::test]
    async fn list_assets_requires_total_count() {
        let body = json!({
            "items": [],
            "page": 1,
            "page_size": 25,
            "total_count": null,
            "total_pages": null
        });
        let (url, _, server) = mock_server(vec![(200, body)]);
        let client = RegistryClient::new(url).unwrap();

        assert!(matches!(
            client
                .list_assets(
                    0,
                    25,
                    AssetSorting::TickerAsc,
                    &AssetSearchFilters::default()
                )
                .await,
            Err(RegistryError::InvalidResponse(_))
        ));
        server.join().unwrap();
    }

    #[tokio::test]
    async fn zero_limit_returns_only_the_total_count() {
        let body = json!({
            "items": [asset_response(ASSET_ID_A, "Asset A", Some("AAA"))],
            "page": 1,
            "page_size": 1,
            "total_count": 5,
            "total_pages": 5
        });
        let (url, requests, server) = mock_server(vec![(200, body)]);
        let client = RegistryClient::new(url).unwrap();

        let result = client
            .list_assets(
                usize::MAX,
                0,
                AssetSorting::TickerAsc,
                &AssetSearchFilters::default(),
            )
            .await
            .unwrap();
        assert_eq!(result.total_count, 5);
        assert!(result.items.is_empty());
        let request = requests.recv().unwrap();
        assert!(request.contains("page=1"));
        assert!(request.contains("page_size=1"));
        server.join().unwrap();
    }

    #[tokio::test]
    async fn pagination_rejects_overflow_without_an_http_request() {
        let client = RegistryClient::new(Url::parse("http://127.0.0.1:1/").unwrap()).unwrap();
        assert!(matches!(
            client
                .list_assets(
                    usize::MAX,
                    1,
                    AssetSorting::TickerAsc,
                    &AssetSearchFilters::default(),
                )
                .await,
            Err(RegistryError::InvalidRequest(_))
        ));
    }

    #[tokio::test]
    async fn client_distinguishes_http_status_and_timeout() {
        let (url, _, server) = mock_server(vec![(503, json!({"detail": "unavailable"}))]);
        let client = RegistryClient::new(url).unwrap();
        let id = AssetId::from_str(ASSET_ID_A).unwrap();
        assert!(matches!(
            client.get_asset(&id).await,
            Err(RegistryError::HttpStatus(503))
        ));
        server.join().unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (_stream, _) = listener.accept().unwrap();
            thread::sleep(Duration::from_millis(100));
        });
        let client = RegistryClient::with_timeouts(
            Url::parse(&format!("http://{}/", addr)).unwrap(),
            Duration::from_millis(20),
            Duration::from_millis(20),
        )
        .unwrap();
        assert!(matches!(
            client.get_asset(&id).await,
            Err(RegistryError::Timeout(_))
        ));
        server.join().unwrap();
    }

    #[test]
    fn sorting_supports_legacy_and_native_parameters() {
        let mut query = HashMap::new();
        query.insert("sort_field".to_string(), "domain".to_string());
        query.insert("sort_dir".to_string(), "desc".to_string());
        assert_eq!(
            AssetSorting::from_query_params(&query).unwrap(),
            AssetSorting::DomainDesc
        );

        let mut query = HashMap::new();
        query.insert("sort".to_string(), "updated_at_desc".to_string());
        assert_eq!(
            AssetSorting::from_query_params(&query).unwrap(),
            AssetSorting::UpdatedAtDesc
        );
        query.insert("sort_dir".to_string(), "asc".to_string());
        assert!(AssetSorting::from_query_params(&query).is_err());

        let mut query = HashMap::new();
        query.insert("sort".to_string(), "created_at_asc".to_string());
        assert_eq!(
            AssetSorting::from_query_params(&query).unwrap(),
            AssetSorting::CreatedAtAsc
        );
        query.insert("sort".to_string(), "updated_at_asc".to_string());
        assert_eq!(
            AssetSorting::from_query_params(&query).unwrap(),
            AssetSorting::UpdatedAtAsc
        );
        query.insert("sort".to_string(), "asset_id_desc".to_string());
        assert_eq!(
            AssetSorting::from_query_params(&query).unwrap(),
            AssetSorting::AssetIdDesc
        );
    }

    #[test]
    fn search_filters_validate_the_openapi_constraints() {
        let filters = search_filters(&[
            ("asset_id", "aB12"),
            ("domain", "Sub.Example.com."),
            ("ticker", "EXM"),
            ("name", "Example Asset"),
            ("asset_type", "amp_ASSET"),
            ("category_tag", "StableCoin"),
            ("category_tag", "fixed-income"),
            ("trading_venue", "SideSwap"),
            ("created_after", "2026-01-01T00:00:00Z"),
            ("updated_after", "2026-02-01T12:30:00-05:00"),
        ]);
        assert_eq!(filters.category_tags.len(), 2);

        let invalid = [
            ("asset_id", "not-hex"),
            ("domain", "invalid"),
            ("ticker", "1234567890123456789012345"),
            ("name", "contains\0nul"),
            ("asset_type", "invalid"),
            ("category_tag", "invalid"),
            ("trading_venue", "invalid"),
            ("created_after", "2026-01-01"),
            ("updated_after", "2026-01-01T00:00:00"),
        ];
        for (name, value) in invalid {
            let query = vec![(name.to_string(), value.to_string())];
            assert!(
                AssetSearchFilters::from_query_pairs(&query).is_err(),
                "{} should reject {:?}",
                name,
                value
            );
        }
    }
}
