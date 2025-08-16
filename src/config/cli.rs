use clap::Parser;
use parking_lot::RwLock;
use std::{
    fs,
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
};

use super::types::LogLevel;

// -----------------------------------------------------------------------------
// ----- Global Singleton ------------------------------------------------------

static CLI_CONFIG: OnceLock<Arc<RwLock<CliConfig>>> = OnceLock::new();

// -----------------------------------------------------------------------------
// ----- CliConfig -------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct CliConfig {
    pub listen_addr: SocketAddr,
    pub config_file_location: PathBuf,
    pub users_file_location: PathBuf,
    pub log_level: LogLevel,
}

impl CliConfig {
    pub fn init() {
        CLI_CONFIG.get_or_init(|| {
            let cfg = Self::from_args();
            cfg.validate();
            Arc::new(RwLock::new(cfg))
        });
    }

    pub fn snapshot() -> CliConfig {
        handle().read().clone()
    }
}

// -----------------------------------------------------------------------------
// ----- CliConfig: Private ----------------------------------------------------

impl CliConfig {
    fn from_args() -> Self {
        let args = Args::try_parse().unwrap_or_else(|e| panic!("Invalid CLI/ENV: {e}"));

        Self {
            listen_addr: SocketAddr::from((args.host, args.port)),
            config_file_location: args.config_file,
            users_file_location: args.users_file,
            log_level: args.log_level,
        }
    }

    fn validate(&self) {
        must_exist_file(&self.config_file_location, "--config / pgcrab.toml");
        must_exist_file(&self.users_file_location, "--users / users.toml");
    }
}

// -----------------------------------------------------------------------------
// ----- Args ------------------------------------------------------------------

#[derive(Parser, Debug)]
#[command(name = "pgcrab", version, about = "Postgres pooler")]
struct Args {
    // IPv4 or IPv6 literal (e.g., 0.0.0.0, 127.0.0.1, ::, ::1). Required via CLI or ENV.
    #[arg(long = "host", short = 'H', env = "PGCRAB_HOST")]
    host: IpAddr,

    // Required via CLI or ENV.
    #[arg(long = "port", short = 'p', env = "PGCRAB_PORT")]
    port: u16,

    // Not required via CLI or ENV (defaults to info).
    #[arg(long = "log", default_value = "info")]
    log_level: LogLevel,

    // Must exist; no defaults.
    #[arg(long = "config", env = "PGCRAB_CONFIG_FILE")]
    config_file: PathBuf,

    // Must exist; no defaults.
    #[arg(long = "users", env = "PGCRAB_USERS_FILE")]
    users_file: PathBuf,
}

// -----------------------------------------------------------------------------
// ----- Private Utils ---------------------------------------------------------

fn handle() -> Arc<RwLock<CliConfig>> {
    CLI_CONFIG
        .get()
        .expect("config not initialized; call config::init().await first")
        .clone()
}

fn must_exist_file(path: &Path, hint: &str) {
    let md = fs::metadata(path).unwrap_or_else(|_| {
        panic!("required file missing: {} (from {hint})", path.display());
    });

    if !md.is_file() {
        panic!("path is not a file: {} (from {hint})", path.display());
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
