use serde::Deserialize;
use std::{env, fs, path::PathBuf};
use tokio::sync::OnceCell;
use tokio_postgres::NoTls;

static SHARDS_OK: OnceCell<()> = OnceCell::const_new();

// Call this at the top of integration tests to fail fast if any shard is down.
pub async fn ensure_shards_accessible() {
    SHARDS_OK
        .get_or_init(|| async {
            if let Err(err) = check_shards().await {
                panic!("{err}");
            }
        })
        .await;
}

pub fn load_config() -> Result<ConfigFile, String> {
    let config_path = config_path()?;
    let raw = fs::read_to_string(&config_path)
        .map_err(|e| format!("failed to read {}: {e}", config_path.display()))?;

    toml::from_str(&raw).map_err(|e| format!("invalid {}: {e}", config_path.display()))
}

async fn check_shards() -> Result<(), String> {
    let cfg = load_config()?;

    if cfg.shards.is_empty() {
        return Err("pgcrab.toml has no [[shards]] entries".to_string());
    }

    for shard in cfg.shards {
        let conn_str = format!(
            "host={} port={} user={} password={} dbname={}",
            shard.host, shard.port, shard.user, shard.password, shard.name
        );

        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
            .await
            .map_err(|e| format!("shard {} connect failed: {e}", shard.name))?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("shard connection error: {e}");
            }
        });

        let row = client
            .query_one("select 1", &[])
            .await
            .map_err(|e| format!("shard {} query failed: {e}", shard.name))?;

        let value: i32 = row
            .try_get(0)
            .map_err(|e| format!("shard {} bad result: {e}", shard.name))?;

        if value != 1 {
            return Err(format!(
                "shard {} unexpected result: got {value}, want 1",
                shard.name
            ));
        }
    }

    Ok(())
}

fn config_path() -> Result<PathBuf, String> {
    if let Ok(path) = env::var("PGCRAB_CONFIG_FILE") {
        return Ok(PathBuf::from(path));
    }

    Ok(PathBuf::from("pgcrab.toml"))
}

#[derive(Debug, Deserialize)]
pub struct ConfigFile {
    #[serde(default)]
    pub shards: Vec<ShardEntry>,
    #[serde(default)]
    pub users: Vec<UserEntry>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ShardEntry {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct UserEntry {
    #[serde(alias = "name")]
    pub username: String,
    pub password: String,
}
