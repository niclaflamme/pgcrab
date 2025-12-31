use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use rand::seq::IteratorRandom;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tracing::{info, warn};

use crate::backend::BackendConnection;
use crate::config::shards::ShardRecord;

// -----------------------------------------------------------------------------
// ----- GatewayPools ----------------------------------------------------------

#[derive(Debug)]
pub struct GatewayPools {
    pools: HashMap<String, Arc<ShardPool>>,
}

#[derive(Debug, Clone)]
pub struct PoolStats {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub min: u32,
    pub max: u32,
    pub idle: usize,
    pub in_use: usize,
    pub available: usize,
}

impl GatewayPools {
    pub fn new(shards: Vec<ShardRecord>) -> Self {
        let mut pools = HashMap::with_capacity(shards.len());
        for shard in shards {
            let name = shard.shard_name.clone();
            pools.insert(name, Arc::new(ShardPool::new(shard)));
        }

        Self { pools }
    }

    pub fn get(&self, shard_name: &str) -> Option<Arc<ShardPool>> {
        self.pools.get(shard_name).cloned()
    }

    pub fn random_pool(&self) -> Option<Arc<ShardPool>> {
        let mut rng = rand::rng();
        self.pools.values().choose(&mut rng).cloned()
    }

    pub async fn snapshot(&self) -> Vec<PoolStats> {
        let mut stats = Vec::with_capacity(self.pools.len());
        for pool in self.pools.values() {
            stats.push(pool.stats().await);
        }
        stats.sort_by(|a, b| a.name.cmp(&b.name));
        stats
    }

    pub async fn warm_all(&self) {
        for pool in self.pools.values() {
            pool.warm_min().await;
        }
    }
}

// -----------------------------------------------------------------------------
// ----- ShardPool -------------------------------------------------------------

#[derive(Debug)]
pub struct ShardPool {
    shard: ShardRecord,
    idle: Mutex<VecDeque<IdleConnection>>,
    max: Arc<Semaphore>,
    min: u32,
    max_connections: u32,
}

impl ShardPool {
    fn new(shard: ShardRecord) -> Self {
        let min = shard.min_connections.max(1);
        let max = shard.max_connections.max(1);
        Self {
            shard,
            idle: Mutex::new(VecDeque::new()),
            max: Arc::new(Semaphore::new(max as usize)),
            min,
            max_connections: max,
        }
    }

    pub fn name(&self) -> &str {
        &self.shard.shard_name
    }

    pub async fn stats(&self) -> PoolStats {
        let idle = self.idle.lock().await.len();
        let available = self.max.available_permits();
        let max = self.max_connections as usize;
        let in_use = max.saturating_sub(available).saturating_sub(idle);

        PoolStats {
            name: self.shard.shard_name.clone(),
            host: self.shard.host.clone(),
            port: self.shard.port,
            min: self.min,
            max: self.max_connections,
            idle,
            in_use,
            available,
        }
    }

    pub async fn warm_min(&self) {
        let current = { self.idle.lock().await.len() as u32 };
        if current >= self.min {
            return;
        }

        let target = self.min - current;
        info!(
            "warming shard {}: creating {target} backend connections",
            self.shard.shard_name
        );

        for _ in 0..target {
            if let Err(err) = self.open_new_connection().await {
                warn!(
                    "failed to warm shard {} connection: {err}",
                    self.shard.shard_name
                );
            }
        }
    }

    pub async fn acquire(self: &Arc<Self>) -> Result<PooledConnection, String> {
        if let Some(idle) = self.idle.lock().await.pop_front() {
            return Ok(PooledConnection::new(self.clone(), idle.conn, idle.permit));
        }

        let permit = self
            .max
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| "backend pool closed".to_string())?;

        let conn = BackendConnection::connect(&self.shard.host, self.shard.port)
            .await
            .map_err(|e| format!("failed to connect to backend: {e}"))?;
        let mut conn = conn;
        conn.startup(
            &self.shard.user,
            &self.shard.shard_name,
            self.shard.password_exposed(),
        )
        .await
        .map_err(|e| format!("backend startup failed: {e}"))?;

        Ok(PooledConnection::new(self.clone(), conn, permit))
    }

    async fn open_new_connection(&self) -> Result<(), String> {
        let permit = self
            .max
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| "backend pool closed".to_string())?;

        let conn = BackendConnection::connect(&self.shard.host, self.shard.port)
            .await
            .map_err(|e| format!("failed to connect to backend: {e}"))?;
        let mut conn = conn;
        conn.startup(
            &self.shard.user,
            &self.shard.shard_name,
            self.shard.password_exposed(),
        )
        .await
        .map_err(|e| format!("backend startup failed: {e}"))?;

        self.push_idle(conn, permit).await;
        Ok(())
    }

    async fn push_idle(&self, mut conn: BackendConnection, permit: OwnedSemaphorePermit) {
        if let Err(err) = conn.reset_session().await {
            warn!(
                "dropping backend connection after reset failure on shard {}: {err}",
                self.shard.shard_name
            );
            return;
        }
        let mut idle = self.idle.lock().await;
        idle.push_back(IdleConnection { conn, permit });
    }
}

// -----------------------------------------------------------------------------
// ----- PooledConnection ------------------------------------------------------

#[derive(Debug)]
pub struct PooledConnection {
    pool: Arc<ShardPool>,
    conn: Option<BackendConnection>,
    permit: Option<OwnedSemaphorePermit>,
}

impl PooledConnection {
    fn new(pool: Arc<ShardPool>, conn: BackendConnection, permit: OwnedSemaphorePermit) -> Self {
        Self {
            pool,
            conn: Some(conn),
            permit: Some(permit),
        }
    }

    pub fn connection(&mut self) -> &mut BackendConnection {
        self.conn
            .as_mut()
            .expect("pooled connection missing backend connection")
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        let Some(conn) = self.conn.take() else {
            return;
        };
        let Some(permit) = self.permit.take() else {
            return;
        };

        let pool = self.pool.clone();
        tokio::spawn(async move {
            pool.push_idle(conn, permit).await;
        });
    }
}

#[derive(Debug)]
struct IdleConnection {
    conn: BackendConnection,
    permit: OwnedSemaphorePermit,
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
