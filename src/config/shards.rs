use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use std::{collections::HashMap, path::Path, sync::Arc};
use thiserror::Error;
use tokio::fs;
use tracing::error;

// -----------------------------------------------------------------------------
// ----- Singleton -------------------------------------------------------------

static SHARDS: OnceCell<ShardsConfig> = OnceCell::new();

// -----------------------------------------------------------------------------
// ----- ShardsConfig ----------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ShardsConfig {
    inner: Arc<RwLock<ShardsMap>>,
}

// -----------------------------------------------------------------------------
// ----- ShardsConfig: Static --------------------------------------------------

impl ShardsConfig {
    pub async fn init(path: &Path) {
        let cfg = Self::from_file_async(path)
            .await
            .unwrap_or_else(|e| panic!("failed to load shards config from {:?}: {e}", path));

        SHARDS
            .set(cfg)
            .unwrap_or_else(|_| panic!("ShardsConfig::init called twice"));
    }

    pub async fn reload(path: &Path) {
        let new_cfg = match Self::from_file_async(path).await {
            Ok(cfg) => cfg,
            Err(e) => {
                error!(
                    "reload failed; keeping previous shards config. path={:?} error={}",
                    path, e
                );
                return;
            }
        };

        let new_map = new_cfg.inner.read().clone();
        let current = Self::handle();

        let mut guard = current.inner.write();
        *guard = new_map;
    }

    pub fn handle() -> &'static ShardsConfig {
        SHARDS.get().expect("Shards not initialized")
    }

    pub fn snapshot() -> Vec<ShardRecord> {
        let handle = Self::handle();
        let guard = handle.inner.read();
        guard.by_name.values().cloned().collect()
    }

    pub fn get_shard(name: &str) -> Option<ShardRecord> {
        let handle = Self::handle();
        let guard = handle.inner.read();
        guard.by_name.get(name).cloned()
    }
}

// -----------------------------------------------------------------------------
// ----- ShardsConfig: Private -------------------------------------------------

impl ShardsConfig {
    async fn from_file_async(path: &Path) -> Result<ShardsConfig, ShardsError> {
        let raw = fs::read_to_string(path).await.map_err(|e| ShardsError::Io {
            path: path.to_path_buf(),
            source: e,
        })?;
        Self::parse(&raw)
    }

    fn parse(raw: &str) -> Result<ShardsConfig, ShardsError> {
        let mut doc: ShardsFile = toml::from_str(raw).map_err(|e| ShardsError::Toml { source: e })?;

        let mut by_name = HashMap::with_capacity(doc.shards.len());

        for shard in doc.shards.drain(..) {
            let record = ShardRecord {
                shard_name: shard.name.clone(),
                host: shard.host,
                port: shard.port,
                user: shard.user,
                password: SecretString::new(shard.password.into_boxed_str()),
            };

            if by_name.insert(record.shard_name.clone(), record).is_some() {
                return Err(ShardsError::DuplicateShard {
                    name: shard.name,
                });
            }
        }

        Ok(ShardsConfig {
            inner: Arc::new(RwLock::new(ShardsMap { by_name })),
        })
    }
}

// -----------------------------------------------------------------------------
// ----- Internal: map ---------------------------------------------------------

#[derive(Debug, Clone, Default)]
struct ShardsMap {
    by_name: HashMap<String, ShardRecord>,
}

// -----------------------------------------------------------------------------
// ----- Internal: On-disk format ----------------------------------------------

#[derive(Debug, Clone, Deserialize)]
struct ShardsFile {
    #[serde(default)]
    shards: Vec<ShardFileEntry>,
}

#[derive(Debug, Clone, Deserialize)]
struct ShardFileEntry {
    name: String,
    host: String,
    port: u16,
    user: String,
    password: String,
}

// -----------------------------------------------------------------------------
// ----- Internal: In-memory record --------------------------------------------

#[derive(Debug, Clone)]
pub struct ShardRecord {
    pub shard_name: String,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: SecretString,
}

impl ShardRecord {
    pub fn password_exposed(&self) -> &str {
        self.password.expose_secret()
    }
}

// -----------------------------------------------------------------------------
// ----- Errors ----------------------------------------------------------------

#[derive(Debug, Error)]
pub enum ShardsError {
    #[error("duplicate [[shards]] entry for shard '{name}'")]
    DuplicateShard { name: String },

    #[error("read error for {path:?}: {source}")]
    Io {
        path: std::path::PathBuf,
        source: std::io::Error,
    },

    #[error("toml parse error: {source}")]
    Toml { source: toml::de::Error },
}
