pub mod session;
pub mod pool;

pub use session::GatewaySession;
pub use pool::{GatewayPools, PoolStats, PooledConnection, ShardPool};

// Gateway orchestration module; keep protocol-specific code in frontend/backend.
