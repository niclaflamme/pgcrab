pub mod session;
pub mod pool;

pub use session::GatewaySession;
pub use pool::{GatewayPools, PooledConnection, ShardPool};

// Gateway orchestration module; keep protocol-specific code in frontend/backend.
