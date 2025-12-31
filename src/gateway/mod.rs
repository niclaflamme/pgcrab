pub mod pool;
pub mod session;

pub use pool::{GatewayPools, PoolStats, PooledConnection, ShardPool};
pub use session::GatewaySession;

// Gateway orchestration module; keep protocol-specific code in frontend/backend.
