use std::env;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::OnceLock;

use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tracing::error;

// -----------------------------------------------------------------------------
// ----- Constants -------------------------------------------------------------

static TLS_ACCEPTOR: OnceLock<Option<TlsAcceptor>> = OnceLock::new();

// -----------------------------------------------------------------------------
// ----- TLS: Exported ---------------------------------------------------------

pub fn acceptor() -> Option<TlsAcceptor> {
    TLS_ACCEPTOR
        .get_or_init(|| match load_from_env() {
            Ok(acceptor) => acceptor,
            Err(err) => {
                error!("tls disabled: {err}");
                None
            }
        })
        .clone()
}

// -----------------------------------------------------------------------------
// ----- TLS: Private helpers --------------------------------------------------

fn load_from_env() -> Result<Option<TlsAcceptor>, String> {
    let cert_path = env::var("PGCRAB_TLS_CERT").ok();
    let key_path = env::var("PGCRAB_TLS_KEY").ok();

    if cert_path.is_none() && key_path.is_none() {
        return Ok(None);
    }

    let cert_path = cert_path.ok_or("PGCRAB_TLS_CERT is required when enabling TLS")?;
    let key_path = key_path.ok_or("PGCRAB_TLS_KEY is required when enabling TLS")?;

    let certs = load_certs(Path::new(&cert_path))?;
    let key = load_key(Path::new(&key_path))?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| format!("invalid tls key/cert pair: {e}"))?;

    Ok(Some(TlsAcceptor::from(std::sync::Arc::new(config))))
}

fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, String> {
    let file =
        File::open(path).map_err(|e| format!("failed to open tls cert {}: {e}", path.display()))?;
    let mut reader = BufReader::new(file);
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("failed to read tls cert {}: {e}", path.display()))?;

    if certs.is_empty() {
        return Err(format!("no certificates found in {}", path.display()));
    }

    Ok(certs)
}

fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>, String> {
    let file =
        File::open(path).map_err(|e| format!("failed to open tls key {}: {e}", path.display()))?;
    let mut reader = BufReader::new(file);
    let key = rustls_pemfile::private_key(&mut reader)
        .map_err(|e| format!("failed to read tls key {}: {e}", path.display()))?
        .ok_or_else(|| format!("no private key found in {}", path.display()))?;

    Ok(key)
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
