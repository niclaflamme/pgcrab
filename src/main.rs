use tokio::net::{TcpListener, TcpSocket};
use tokio::signal;
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, fmt};

use pgcrab::{Config, FrontendConnection};

// -----------------------------------------------------------------------------
// ----- Constants -------------------------------------------------------------

const APP_NAME: &str = "🦀 pgcrab";

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
    // This has to be the first thing we do, because it initializes the config
    Config::init().await;

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
    // Config might reload, but the fields used by run_forever are set at startup
    let config = Config::snapshot();

    let socket = if config.listen_addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };

    socket.bind(config.listen_addr)?;

    let listener: TcpListener = socket.listen(1024)?;

    info!("{} listening on {}", APP_NAME, config.listen_addr);

    loop {
        tokio::select! {
            _ = signal::ctrl_c() => {
                info!("{} shutting down", APP_NAME);
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

                    if let Err(e) = conn.run().await {
                        error!("client {peer} error: {e}");
                    }
                });
            }
        }
    }

    Ok(())
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
