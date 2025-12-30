use crate::backend::BackendConnection;
use crate::config::shards::ShardRecord;

#[derive(Debug)]
pub struct GatewaySession {
    backend: BackendConnection,
}

impl GatewaySession {
    pub async fn connect_to_shard(shard: &ShardRecord) -> Result<Self, String> {
        let backend = BackendConnection::connect(&shard.host, shard.port)
            .await
            .map_err(|e| format!("failed to connect to backend: {}", e))?;

        Ok(Self { backend })
    }

    #[allow(dead_code)]
    pub fn backend_mut(&mut self) -> &mut BackendConnection {
        &mut self.backend
    }
}
