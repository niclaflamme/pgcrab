use std::sync::Arc;

use crate::backend::BackendConnection;
use crate::gateway::{PooledConnection, ShardPool};

#[derive(Debug)]
pub struct GatewaySession {
    backend: PooledConnection,
}

impl GatewaySession {
    pub async fn from_pool(pool: &Arc<ShardPool>) -> Result<Self, String> {
        let backend = pool.acquire().await?;
        let mut session = Self { backend };
        let _ = session.backend.connection().peer_addr();
        Ok(session)
    }

    pub fn backend(&mut self) -> &mut BackendConnection {
        self.backend.connection()
    }
}
