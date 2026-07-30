use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};
use std::{cmp, fs, path, thread};

use serde_json::Value as JsonValue;

use elements::AssetId;

use crate::errors::*;

// length of asset id prefix to use for sub-directory partitioning
// (in number of hex characters, not bytes)

const DIR_PARTITION_LEN: usize = 2;
pub struct AssetRegistry {
    directory: path::PathBuf,
    assets_cache: HashMap<AssetId, (SystemTime, AssetMeta)>,
}

pub type AssetEntry<'a> = (&'a AssetId, &'a AssetMeta);

impl AssetRegistry {
    pub fn new(directory: path::PathBuf) -> Self {
        Self {
            directory,
            assets_cache: Default::default(),
        }
    }

    pub fn get(&self, asset_id: &AssetId) -> Option<&AssetMeta> {
        self.assets_cache
            .get(asset_id)
            .map(|(_, metadata)| metadata)
    }

    pub fn list(
        &self,
        start_index: usize,
        limit: usize,
        sorting: AssetSorting,
    ) -> (usize, Vec<AssetEntry<'_>>) {
        let mut assets: Vec<AssetEntry> = self
            .assets_cache
            .iter()
            .map(|(asset_id, (_, metadata))| (asset_id, metadata))
            .collect();
        assets.sort_by(sorting.as_comparator());
        (
            assets.len(),
            assets.into_iter().skip(start_index).take(limit).collect(),
        )
    }

    pub fn fs_sync(&mut self) -> Result<()> {
        let mut assets_cache = HashMap::new();

        for entry in fs::read_dir(&self.directory).chain_err(|| "failed reading asset dir")? {
            let entry = entry.chain_err(|| "invalid fh")?;
            let filetype = entry.file_type().chain_err(|| "failed getting file type")?;
            if !filetype.is_dir() || entry.file_name().len() != DIR_PARTITION_LEN {
                continue;
            }

            for file_entry in
                fs::read_dir(entry.path()).chain_err(|| "failed reading asset subdir")?
            {
                let file_entry = file_entry.chain_err(|| "invalid fh")?;
                let path = file_entry.path();
                if path.extension().and_then(|e| e.to_str()) != Some("json") {
                    continue;
                }

                let asset_id = AssetId::from_str(
                    path.file_stem()
                        .unwrap() // cannot fail if extension() succeeded
                        .to_str()
                        .chain_err(|| "invalid filename")?,
                )
                .chain_err(|| "invalid filename")?;

                let modified = file_entry
                    .metadata()
                    .chain_err(|| "failed reading metadata")?
                    .modified()
                    .chain_err(|| "metadata modified failed")?;

                if let Some((last_update, metadata)) = self.assets_cache.get(&asset_id) {
                    if *last_update == modified {
                        assets_cache.insert(asset_id, (modified, metadata.clone()));
                        continue;
                    }
                }

                let metadata: AssetMeta = serde_json::from_str(
                    &fs::read_to_string(path).chain_err(|| "failed reading file")?,
                )
                .chain_err(|| "failed parsing file")?;

                assets_cache.insert(asset_id, (modified, metadata));
            }
        }

        self.assets_cache = assets_cache;
        Ok(())
    }

    pub fn spawn_sync(asset_db: Arc<RwLock<AssetRegistry>>) -> thread::JoinHandle<()> {
        thread::spawn(move || loop {
            if let Err(e) = asset_db.write().unwrap().fs_sync() {
                error!("registry fs_sync failed: {:?}", e);
            }

            thread::sleep(Duration::from_secs(15));
            // TODO handle shutdowm
        })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AssetMeta {
    #[serde(skip_serializing_if = "JsonValue::is_null")]
    pub contract: JsonValue,
    #[serde(skip_serializing_if = "JsonValue::is_null")]
    pub entity: JsonValue,
    pub precision: u8,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ticker: Option<String>,
}

impl AssetMeta {
    fn domain(&self) -> Option<&str> {
        self.entity["domain"].as_str()
    }
}

pub struct AssetSorting(AssetSortField, AssetSortDir);

pub enum AssetSortField {
    Name,
    Domain,
    Ticker,
}
pub enum AssetSortDir {
    Descending,
    Ascending,
}

impl AssetSorting {
    fn as_comparator(self) -> Box<dyn Fn(&AssetEntry, &AssetEntry) -> cmp::Ordering> {
        let sort_fn: Box<dyn Fn(&AssetEntry, &AssetEntry) -> cmp::Ordering> = match self.0 {
            AssetSortField::Name => {
                // Order by name first, use asset id as a tie breaker. the other sorting fields
                // don't require this because they're guaranteed to be unique.
                Box::new(|a, b| lc_cmp(&a.1.name, &b.1.name).then_with(|| a.0.cmp(b.0)))
            }
            AssetSortField::Domain => Box::new(|a, b| a.1.domain().cmp(&b.1.domain())),
            AssetSortField::Ticker => Box::new(|a, b| lc_cmp_opt(&a.1.ticker, &b.1.ticker)),
        };

        match self.1 {
            AssetSortDir::Ascending => sort_fn,
            AssetSortDir::Descending => Box::new(move |a, b| sort_fn(a, b).reverse()),
        }
    }

    pub fn from_query_params(query: &HashMap<String, String>) -> Result<Self> {
        let field = match query.get("sort_field").map(String::as_str) {
            None => AssetSortField::Ticker,
            Some("name") => AssetSortField::Name,
            Some("domain") => AssetSortField::Domain,
            Some("ticker") => AssetSortField::Ticker,
            _ => bail!("invalid sort field"),
        };

        let dir = match query.get("sort_dir").map(String::as_str) {
            None => AssetSortDir::Ascending,
            Some("asc") => AssetSortDir::Ascending,
            Some("desc") => AssetSortDir::Descending,
            _ => bail!("invalid sort direction"),
        };

        Ok(Self(field, dir))
    }
}

fn lc_cmp(a: &str, b: &str) -> cmp::Ordering {
    a.to_lowercase().cmp(&b.to_lowercase())
}
fn lc_cmp_opt(a: &Option<String>, b: &Option<String>) -> cmp::Ordering {
    a.as_ref()
        .map(|a| a.to_lowercase())
        .cmp(&b.as_ref().map(|b| b.to_lowercase()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    const ASSET_ID_A: &str = "0000000000000000000000000000000000000000000000000000000000000001";
    const ASSET_ID_B: &str = "0000000000000000000000000000000000000000000000000000000000000002";

    fn asset_id(id: &str) -> AssetId {
        AssetId::from_str(id).unwrap()
    }

    fn asset_file(dir: &TempDir, id: &str) -> path::PathBuf {
        dir.path()
            .join(&id[..DIR_PARTITION_LEN])
            .join(format!("{}.json", id))
    }

    fn write_asset(dir: &TempDir, id: &str, name: &str, ticker: &str) {
        let partition_dir = dir.path().join(&id[..DIR_PARTITION_LEN]);
        fs::create_dir_all(&partition_dir).unwrap();
        fs::write(
            partition_dir.join(format!("{}.json", id)),
            format!(
                r#"{{"contract":null,"entity":{{"domain":"example.com"}},"precision":8,"name":"{}","ticker":"{}"}}"#,
                name, ticker
            ),
        )
        .unwrap();
    }

    #[test]
    fn fs_sync_loads_assets_from_disk() {
        let dir = tempfile::tempdir().unwrap();
        write_asset(&dir, ASSET_ID_A, "Asset A", "AAA");

        let mut registry = AssetRegistry::new(dir.path().to_path_buf());
        registry.fs_sync().unwrap();

        let metadata = registry.get(&asset_id(ASSET_ID_A)).unwrap();
        assert_eq!(metadata.name, "Asset A");
        assert_eq!(metadata.ticker.as_deref(), Some("AAA"));
    }

    #[test]
    fn fs_sync_removes_assets_deleted_from_disk() {
        let dir = tempfile::tempdir().unwrap();
        write_asset(&dir, ASSET_ID_A, "Asset A", "AAA");
        write_asset(&dir, ASSET_ID_B, "Asset B", "BBB");

        let mut registry = AssetRegistry::new(dir.path().to_path_buf());
        registry.fs_sync().unwrap();
        assert_eq!(registry.assets_cache.len(), 2);

        fs::remove_file(asset_file(&dir, ASSET_ID_A)).unwrap();
        registry.fs_sync().unwrap();

        assert!(registry.get(&asset_id(ASSET_ID_A)).is_none());
        assert!(registry.get(&asset_id(ASSET_ID_B)).is_some());
        assert_eq!(registry.assets_cache.len(), 1);
    }

    #[test]
    fn fs_sync_reuses_cached_metadata_when_file_is_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        write_asset(&dir, ASSET_ID_A, "Asset A", "AAA");

        let mut registry = AssetRegistry::new(dir.path().to_path_buf());
        registry.fs_sync().unwrap();

        let id = asset_id(ASSET_ID_A);
        let modified = fs::metadata(asset_file(&dir, ASSET_ID_A))
            .unwrap()
            .modified()
            .unwrap();
        registry.assets_cache.insert(
            id,
            (
                modified,
                AssetMeta {
                    contract: JsonValue::Null,
                    entity: json!({"domain": "cached.example.com"}),
                    precision: 0,
                    name: "Cached Asset".to_string(),
                    ticker: Some("CACHE".to_string()),
                },
            ),
        );

        registry.fs_sync().unwrap();

        let metadata = registry.get(&id).unwrap();
        assert_eq!(metadata.name, "Cached Asset");
        assert_eq!(metadata.ticker.as_deref(), Some("CACHE"));
    }

    #[test]
    fn fs_sync_refreshes_metadata_when_file_is_modified() {
        let dir = tempfile::tempdir().unwrap();
        write_asset(&dir, ASSET_ID_A, "Asset A", "AAA");

        let mut registry = AssetRegistry::new(dir.path().to_path_buf());
        registry.fs_sync().unwrap();

        let id = asset_id(ASSET_ID_A);
        registry.assets_cache.get_mut(&id).unwrap().0 = SystemTime::UNIX_EPOCH;
        write_asset(&dir, ASSET_ID_A, "Updated Asset", "UPD");

        registry.fs_sync().unwrap();

        let metadata = registry.get(&id).unwrap();
        assert_eq!(metadata.name, "Updated Asset");
        assert_eq!(metadata.ticker.as_deref(), Some("UPD"));
    }
}
