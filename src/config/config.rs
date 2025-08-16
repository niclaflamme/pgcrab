use parking_lot::RwLock;
use std::{
    net::SocketAddr,
    sync::{Arc, OnceLock},
};

use super::{cli::CliConfig, types::LogLevel, users::UsersConfig};

// -----------------------------------------------------------------------------
// ----- Global Singleton ------------------------------------------------------

static ROOT_CONFIG: OnceLock<Arc<RwLock<Config>>> = OnceLock::new();

// -----------------------------------------------------------------------------
// ----- Config ----------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct Config {
    pub listen_addr: SocketAddr,
    pub log_level: LogLevel,
    pub users: &'static UsersConfig,
}

// -----------------------------------------------------------------------------
// ----- Config: Static --------------------------------------------------------

impl Config {
    /// Async because UsersConfig::init() is async (non-blocking IO).
    pub async fn init() {
        CliConfig::init();
        UsersConfig::init().await;

        Self::load().await;
    }

    /// Pure in-memory reload. Call this after you've reloaded sub-configs.
    pub async fn reload() {
        Self::load().await;
    }

    pub fn snapshot() -> Config {
        Self::handle().read().clone()
    }
}

// -----------------------------------------------------------------------------
// ----- Config: Private -------------------------------------------------------

impl Config {
    async fn load() {
        let cli = CliConfig::snapshot();
        let users = UsersConfig::handle();

        UsersConfig::reload().await;

        let next = Config {
            listen_addr: cli.listen_addr,
            log_level: cli.log_level,
            users,
        };

        if let Some(handle) = ROOT_CONFIG.get() {
            *handle.write() = next;
        } else {
            let _ = ROOT_CONFIG.set(Arc::new(RwLock::new(next)));
        }
    }

    fn handle() -> Arc<RwLock<Config>> {
        ROOT_CONFIG
            .get()
            .expect("Config not initialized; call Config::init().await first")
            .clone()
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
