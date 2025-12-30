use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use std::{collections::HashMap, path::Path, sync::Arc, time::Duration};
use thiserror::Error;
use tokio::fs;
use tracing::error;

// -----------------------------------------------------------------------------
// ----- Singleton -------------------------------------------------------------

static USERS: OnceCell<UsersConfig> = OnceCell::new();

// -----------------------------------------------------------------------------
// ----- UsersConfig -----------------------------------------------------------

#[derive(Debug, Clone)]
pub struct UsersConfig {
    inner: Arc<RwLock<UsersMap>>,
}

// -----------------------------------------------------------------------------
// ----- UsersConfig: Static ---------------------------------------------------

impl UsersConfig {
    /// Init: panic on any error. Do not continue with a bad state.
    pub async fn init(path: &Path) {
        let cfg = Self::from_file_async(path)
            .await
            .unwrap_or_else(|e| panic!("failed to load users config from {:?}: {e}", path));

        USERS
            .set(cfg)
            .unwrap_or_else(|_| panic!("UsersConfig::init called twice"));
    }

    /// Reload: on error, DO NOT swap; keep current map and log.
    pub async fn reload(path: &Path) {
        let new_cfg = match Self::from_file_async(path).await {
            Ok(cfg) => cfg,
            Err(e) => {
                error!(
                    "reload failed; keeping previous users config. path={:?} error={}",
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

    pub fn handle() -> &'static UsersConfig {
        USERS.get().expect("Users not initialized")
    }

    pub fn snapshot() -> Vec<UserRecord> {
        let handle = Self::handle();
        let guard = handle.inner.read();
        guard.by_key.values().cloned().collect()
    }
}

// -----------------------------------------------------------------------------
// ----- UsersConfig: Public ---------------------------------------------------

impl UsersConfig {
    pub fn authenticate(
        &self,
        client_username: &str,
        client_password: &str,
    ) -> Result<UserRecord, UsersError> {
        let key = UserKey::new(client_username);

        let guard = self.inner.read();
        let user = guard
            .by_key
            .get(&key)
            .ok_or_else(|| UsersError::UnknownUser {
                username: client_username.to_string(),
            })?;

        if user.client_password.expose_secret() != client_password {
            return Err(UsersError::BadPassword);
        }

        Ok(user.clone())
    }
}

// -----------------------------------------------------------------------------
// ----- UsersConfig: Private --------------------------------------------------

impl UsersConfig {
    async fn from_file_async(path: &Path) -> Result<UsersConfig, UsersError> {
        let raw = fs::read_to_string(path).await.map_err(|e| UsersError::Io {
            path: path.to_path_buf(),
            source: e,
        })?;
        Self::parse(&raw)
    }

    fn parse(raw: &str) -> Result<UsersConfig, UsersError> {
        let mut doc: UsersFile = toml::from_str(raw).map_err(|e| UsersError::Toml { source: e })?;

        if doc.users.is_empty() {
            return Err(UsersError::EmptyConfig);
        }

        let mut by_key = HashMap::with_capacity(doc.users.len());
        for mut user in doc.users.drain(..) {
            normalize_defaults(&mut user);
            validate(&user)?;

            let server_username = user
                .server_username
                .clone()
                .unwrap_or_else(|| user.username.clone());
            let server_password = user
                .server_password
                .clone()
                .unwrap_or_else(|| user.password.clone());

            let record = UserRecord {
                client_username: user.username.clone(),

                client_password: SecretString::new(user.password.into_boxed_str()),
                server_username,
                server_password: SecretString::new(server_password.into_boxed_str()),

                pool_size: user.pool_size,
                pooler_mode: user.pooler_mode,
                statement_timeout: user.statement_timeout,
                admin: user.admin,
            };

            let key = UserKey::new(&record.client_username);
            if by_key.insert(key, record).is_some() {
                return Err(UsersError::DuplicateUser {
                    username: user.username,
                });
            }
        }

        Ok(UsersConfig {
            inner: Arc::new(RwLock::new(UsersMap { by_key })),
        })
    }
}

// -----------------------------------------------------------------------------
// ----- Internal: map/key -----------------------------------------------------

#[derive(Debug, Clone, Default)]
struct UsersMap {
    by_key: HashMap<UserKey, UserRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct UserKey {
    client_username: String,
}

impl UserKey {
    fn new(client_username: &str) -> Self {
        Self {
            client_username: client_username.to_string(),
        }
    }
}

// -----------------------------------------------------------------------------
// ----- Internal: PoolerMode --------------------------------------------------

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PoolerMode {
    Transaction,
    Session,
}

// -----------------------------------------------------------------------------
// ----- Internal: On-disk format ----------------------------------------------

#[derive(Debug, Clone, Deserialize)]
struct UsersFile {
    #[serde(default)]
    users: Vec<UsersFileEntry>,
}

#[derive(Debug, Clone, Deserialize)]
struct UsersFileEntry {
    #[serde(alias = "name")]
    username: String,

    password: String,

    #[serde(default)]
    pool_size: Option<u32>,

    #[serde(default)]
    pooler_mode: Option<PoolerMode>,

    #[serde(default, alias = "server_user")]
    server_username: Option<String>,

    #[serde(default)]
    server_password: Option<String>,

    #[serde(default, deserialize_with = "de_ms")]
    statement_timeout: Option<Duration>,

    #[serde(default)]
    admin: bool,
}

// -----------------------------------------------------------------------------
// ----- Internal: In-memory record --------------------------------------------

#[derive(Debug, Clone)]
pub struct UserRecord {
    pub client_username: String,

    pub client_password: SecretString,
    pub server_username: String,
    pub server_password: SecretString,

    pub pool_size: Option<u32>,
    pub pooler_mode: Option<PoolerMode>,
    pub statement_timeout: Option<Duration>,
    pub admin: bool,
}

// -----------------------------------------------------------------------------
// ----- Internal: Helpers -----------------------------------------------------

fn normalize_defaults(_u: &mut UsersFileEntry) {}

fn validate(u: &UsersFileEntry) -> Result<(), UsersError> {
    if u.username.trim().is_empty() {
        return Err(UsersError::InvalidField("username".into()));
    }
    if u.password.is_empty() {
        return Err(UsersError::InvalidField("password".into()));
    }
    Ok(())
}

fn de_ms<'de, D>(d: D) -> Result<Option<Duration>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{Error, Unexpected, Visitor};
    use std::fmt;

    struct OptVisitor;
    struct MsVisitor;

    impl<'de> Visitor<'de> for OptVisitor {
        type Value = Option<Duration>;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("integer milliseconds (e.g., 30000)")
        }
        fn visit_none<E: Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }
        fn visit_unit<E: Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }
        fn visit_some<D2>(self, d2: D2) -> Result<Self::Value, D2::Error>
        where
            D2: serde::Deserializer<'de>,
        {
            d2.deserialize_any(MsVisitor)
        }
    }

    impl<'de> Visitor<'de> for MsVisitor {
        type Value = Option<Duration>;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("integer milliseconds (e.g., 30000)")
        }

        fn visit_u64<E: Error>(self, v: u64) -> Result<Self::Value, E> {
            Ok(Some(Duration::from_millis(v)))
        }

        fn visit_i64<E: Error>(self, v: i64) -> Result<Self::Value, E> {
            if v < 0 {
                return Err(E::invalid_value(Unexpected::Signed(v), &self));
            }
            Ok(Some(Duration::from_millis(v as u64)))
        }

        fn visit_str<E: Error>(self, v: &str) -> Result<Self::Value, E> {
            Err(E::invalid_value(Unexpected::Str(v), &self))
        }
    }

    d.deserialize_option(OptVisitor)
}

// -----------------------------------------------------------------------------
// ----- Errors ----------------------------------------------------------------

#[derive(Debug, Error)]
pub enum UsersError {
    #[error("users config already initialized")]
    AlreadyInitialized,

    #[error("users config is empty")]
    EmptyConfig,

    #[error("duplicate [[users]] entry for user '{username}'")]
    DuplicateUser { username: String },

    #[error("unknown user '{username}'")]
    UnknownUser { username: String },

    #[error("invalid or missing field '{0}'")]
    InvalidField(String),

    #[error("bad password")]
    BadPassword,

    #[error("read error for {path:?}: {source}")]
    Io {
        path: std::path::PathBuf,
        source: std::io::Error,
    },

    #[error("toml parse error: {source}")]
    Toml { source: toml::de::Error },
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn write_tmp(contents: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(contents.as_bytes()).unwrap();
        file
    }

    #[tokio::test]
    async fn parse_and_authenticate_new_field_names() {
        let toml = r#"
            [[users]]
            username = "alice"
            password = "hunter2"
            pool_size = 64
            pooler_mode = "transaction"
            statement_timeout = 30_000

            [[users]]
            username = "bob"
            password = "opensesame"
            server_username = "pgapp"
            server_password = "server-secret"
            pooler_mode = "session"
            statement_timeout = 10_000
            admin = true
        "#;

        let tmp = write_tmp(toml);
        let users = UsersConfig::from_file_async(tmp.path()).await.unwrap();

        let rec = users.authenticate("alice", "hunter2").unwrap();
        assert_eq!(rec.server_username, "alice");
        assert_eq!(rec.pool_size, Some(64));
        assert_eq!(rec.pooler_mode, Some(PoolerMode::Transaction));
        assert_eq!(rec.statement_timeout, Some(Duration::from_millis(30_000)));
        assert!(!rec.admin);

        let rec = users.authenticate("bob", "opensesame").unwrap();
        assert_eq!(rec.server_username, "pgapp");
        assert_eq!(rec.server_password.expose_secret(), "server-secret");
        assert_eq!(rec.pooler_mode, Some(PoolerMode::Session));
        assert_eq!(rec.statement_timeout, Some(Duration::from_millis(10_000)));
        assert!(rec.admin);
    }

    #[tokio::test]
    async fn backward_compat_aliases_still_work() {
        let toml = r#"
            [[users]]
            name = "legacy"
            password = "password"
            server_user = "legacy_backend"
        "#;

        let tmp = write_tmp(toml);
        let users = UsersConfig::from_file_async(tmp.path()).await.unwrap();

        let rec = users.authenticate("legacy", "password").unwrap();
        assert_eq!(rec.server_username, "legacy_backend");
    }

    #[tokio::test]
    async fn bad_password_and_unknown_user() {
        let toml = r#"
            [[users]]
            username = "alice"
            password = "password"
        "#;

        let tmp = write_tmp(toml);
        let users = UsersConfig::from_file_async(tmp.path()).await.unwrap();

        let err = users.authenticate("alice", "nope").unwrap_err();
        assert!(matches!(err, UsersError::BadPassword));

        let err = users.authenticate("steeve", "nope").unwrap_err();
        match err {
            UsersError::UnknownUser { username } => assert_eq!(username, "steeve"),
            _ => panic!("expected UnknownUser"),
        }
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
