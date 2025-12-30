use clap::Parser;
use std::{
    fs,
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
};
use tokio::net::{TcpListener, TcpSocket};
use tokio::signal;
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, fmt};

use pgcrab::{Config, FrontendConnection, config::types::LogLevel};

// -----------------------------------------------------------------------------
// ----- Constants -------------------------------------------------------------

const APP_NAME: &str = "🦀 PgCrab";

// -----------------------------------------------------------------------------
// ----- Main ------------------------------------------------------------------

#[tokio::main]
async fn main() -> std::io::Result<()> {
    setup().await;
    run_forever().await
}

// -----------------------------------------------------------------------------
// ----- Setup -----------------------------------------------------------------

async fn setup() {
    let args = Args::try_parse().unwrap_or_else(|e| panic!("Invalid CLI/ENV: {e}"));
    must_exist_file(&args.config_file, "--config / pgcrab.toml");

    let listen_addr = SocketAddr::from((args.host, args.port));
    Config::init(listen_addr, args.log_level, args.config_file).await;

    init_tracing();
}

fn init_tracing() {
    let config = Config::snapshot();
    let filter = EnvFilter::try_new(config.log_level.as_str()).unwrap();
    let _ = fmt().with_env_filter(filter).with_target(false).try_init();
}

// -----------------------------------------------------------------------------
// ----- Run -------------------------------------------------------------------

async fn run_forever() -> std::io::Result<()> {
    let config = Config::snapshot();

    let socket = if config.listen_addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };

    socket.bind(config.listen_addr)?;

    let listener: TcpListener = socket.listen(1024)?;

    info!("{} :: Listening on {}", APP_NAME, config.listen_addr);

    loop {
        tokio::select! {
            _ = signal::ctrl_c() => {
                info!("{} :: Shutting down", APP_NAME);
                break;
            }

            accept_res = listener.accept() => {
                let (stream, peer) = match accept_res {
                    Ok(v) => v,
                    Err(e) => { error!("accept error: {e}"); continue; }
                };

                // You can still set nodelay on the Tokio stream.
                let _ = stream.set_nodelay(true);

                tokio::spawn(async move {
                    let conn = FrontendConnection::new(stream);

                    if let Err(e) = conn.serve().await {
                        error!("client {peer} error: {e}");
                    }
                });
            }
        }
    }

    Ok(())
}

// -----------------------------------------------------------------------------
// ----- CLI -------------------------------------------------------------------

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
