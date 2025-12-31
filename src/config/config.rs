use parking_lot::RwLock;
use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, OnceLock},
};

use super::{shards::ShardsConfig, types::LogLevel, users::UsersConfig};

// -----------------------------------------------------------------------------
// ----- Global Singleton ------------------------------------------------------

static CONFIG_FILE_PATH: OnceLock<PathBuf> = OnceLock::new();
static CONFIG: OnceLock<Arc<RwLock<Config>>> = OnceLock::new();

// -----------------------------------------------------------------------------
// ----- Config ----------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct Config {
    pub listen_addr: SocketAddr,
    pub log_level: LogLevel,
    pub parser_cache_capacity: usize,
    pub users: &'static UsersConfig,
    pub shards: &'static ShardsConfig,
}

// -----------------------------------------------------------------------------
// ----- Config: Static --------------------------------------------------------

impl Config {
    /// Async because UsersConfig::init() is async (non-blocking IO).
    pub async fn init(
        listen_addr: SocketAddr,
        log_level: LogLevel,
        parser_cache_capacity: usize,
        config_path: PathBuf,
    ) {
        CONFIG_FILE_PATH
            .set(config_path)
            .unwrap_or_else(|_| panic!("Config::init called twice"));

        let path = config_path_handle();
        UsersConfig::init(path).await;
        ShardsConfig::init(path).await;

        Self::load(listen_addr, log_level, parser_cache_capacity).await;
    }

    /// Pure in-memory reload. Call this after you've reloaded sub-configs.
    pub async fn reload() {
        let current = Self::snapshot();
        Self::load(
            current.listen_addr,
            current.log_level,
            current.parser_cache_capacity,
        )
        .await;
    }

    pub fn snapshot() -> Config {
        Self::handle().read().clone()
    }
}

// -----------------------------------------------------------------------------
// ----- Config: Private -------------------------------------------------------

impl Config {
    async fn load(listen_addr: SocketAddr, log_level: LogLevel, parser_cache_capacity: usize) {
        let users = UsersConfig::handle();
        let shards = ShardsConfig::handle();

        let path = config_path_handle();
        UsersConfig::reload(path).await;
        ShardsConfig::reload(path).await;

        let next = Config {
            listen_addr,
            log_level,
            parser_cache_capacity,
            users,
            shards,
        };

        if let Some(handle) = CONFIG.get() {
            *handle.write() = next;
        } else {
            let _ = CONFIG.set(Arc::new(RwLock::new(next)));
        }
    }

    fn handle() -> Arc<RwLock<Config>> {
        CONFIG
            .get()
            .expect("Config not initialized; call Config::init().await first")
            .clone()
    }
}

// -----------------------------------------------------------------------------
// ----- Private Helpers -------------------------------------------------------

fn config_path_handle() -> &'static PathBuf {
    CONFIG_FILE_PATH
        .get()
        .expect("config path not initialized; call Config::init() first")
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
