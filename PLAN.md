# Plan: prewarm shard connections on boot

## Goals
- Maintain a per-shard pool with `min_connections` warmed on startup.
- Enforce `max_connections` and reuse idle backend connections.
- Keep frontend auth flow unchanged (backend connections still deferred until first query).

## Data structures (where they live)
- `backend`:
  - Keep `BackendConnection` as the raw socket wrapper.
  - Optional helper: `BackendConnector` with `connect(shard)` for single responsibility.
- `gateway`:
  - `ShardPool`: holds shard config + idle queue + max permits.
    - Fields: `shard: ShardRecord`, `idle: VecDeque<BackendConnection>`, `max: Semaphore`, `min: u32`.
    - `acquire() -> PooledConnection` (uses permit + pulls/creates connection).
    - `release(conn)` returns to idle queue (non-async path via channel).
  - `GatewayPools`: `HashMap<String, Arc<ShardPool>>` keyed by shard name.
    - `warm_all()` spawns tasks to reach `min_connections` per shard.
- `main`:
  - Build a shared `GatewayPools` after `Config::init`.
  - Call `warm_all()` before accepting client connections.
  - Pass `Arc<GatewayPools>` into `FrontendConnection::new`.

## Implementation steps
1. Add pool structs in `src/gateway/pool.rs` and export from `src/gateway/mod.rs`.
2. Implement `ShardPool::warm_min()` that opens connections until `idle.len() >= min`.
3. Modify `GatewaySession::connect_to_shard` to accept a pool (or `GatewayPools`) and `acquire()`.
4. Update `FrontendConnection::new` signature to accept `Arc<GatewayPools>`.
5. In `main`, create `GatewayPools` from `Config::snapshot().shards` and call `warm_all()`.
6. Add basic logging around pool warmup and connection failures.

## Validation
- Unit: test `ShardPool` min/max enforcement (mock connector or loopback test).
- Integration: existing auth flow should remain green.
- Manual: verify startup creates backend connections even without client queries.
