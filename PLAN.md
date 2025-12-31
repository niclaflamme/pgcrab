# PLAN: Prepared statement virtualization + cycle pinning

## Context (current code)
- `GatewaySession` is acquired per frontend sequence and released on `ReadyForQuery` in `src/frontend/connection.rs`.
- `prepare_sequence` in `src/frontend/handlers/ready.rs` inspects Parse/Bind/Close and injects Parse before Bind when needed.
- `FrontendContext` tracks `prepared_statements` and `pending_parses` by client statement name in `src/frontend/context.rs`.
- `BackendConnection` tracks prepared names only and clears them on `reset_session()` in `src/backend/backend_connection.rs`.

## Problems to fix
- Statement names are client-owned, so backend hopping or multiplexing can still hit "already exists" / "does not exist".
- No portal virtualization; Describe/Execute/Close are not rewritten, and portal lifetime is not modeled.
- Backend release on the first `ReadyForQuery` breaks pipelined extended protocol cycles.
- Prepared statement identity is not keyed by (sql + param types), and invalidation only happens on pool reset.

## Goals
- Prepared statements work across backend hops without session pinning.
- Extended protocol cycles are atomic and stay on one backend until `Sync` completes.
- Statement and portal names are virtual at the proxy and backend-specific on the wire.
- Invalidation is explicit and safe when backend state is reset or cleared.

## Non-goals
- Preserve backend session state across hops beyond prepared statements and portals.
- Support portals that survive beyond a `Sync` boundary.

## Plan

### Phase 1: Cycle pinning and pipelining safety
- Add a `pending_syncs` counter (or equivalent) in `src/frontend/context.rs`.
- Increment on forwarded `Sync` and simple `Query`; decrement on backend `ReadyForQuery`.
- Release `gateway_session` only when `pending_syncs == 0`.
- Decide policy for pipelining:
  - If supported, keep counting cycles and only release when all are done.
  - If not supported, gate `FrontendBuffers::pull_next_sequence` to one cycle at a time.

### Phase 2: Virtual statement + portal state (frontend)
- Replace `prepared_statements` with `virtual_statements: HashMap<ClientName, VirtualStatement>`.
- Add `virtual_portals: HashMap<ClientPortal, PortalBinding>` cleared at `Sync`.
- `VirtualStatement` should store `sql`, `param_type_oids`, `signature`, `generation`, `closed`.
- Extend `PendingParse` to include `signature`, `backend_stmt_name`, and whether to forward ParseComplete.

### Phase 3: Backend prepared cache + naming
- Extend `BackendConnection` with a backend-side cache:
  - `prepared_by_signature: HashMap<Signature, BackendStmtName>`
  - `signature_by_name: HashMap<BackendStmtName, Signature>`
  - `epoch`, `next_stmt_id`, `next_portal_id`
- Name backend statements `ps_<epoch>_<counter>` and portals `pt_<counter>`.
- Clear caches and bump epoch on reset and on invalidating SQL.

### Phase 4: Frame rewrite and injection path
- Add a small rewrite helper (new module under `src/wire/` or `src/frontend/`) to rebuild:
  - Parse, Bind, Describe, Execute, Close with rewritten names.
- Replace `prepare_sequence` logic in `src/frontend/handlers/ready.rs` with a router that:
  - Parse: update virtual statement, allocate backend name, forward Parse with backend name.
  - Bind: ensure prepared (inject Parse if needed), allocate backend portal, rewrite names.
  - Describe/Execute/Close: rewrite to backend names, enforce portal lookup.
  - Sync: clear `virtual_portals` for the current cycle.
- Conflict policy:
  - Default strict error if a client reuses a statement name with a different signature.
  - Optional "replace" mode behind config if needed.

### Phase 5: Backend response handling and invalidation
- Update `src/frontend/connection.rs` to map ParseComplete to `pending_parses` entries and update backend caches.
- Suppress ParseComplete for injected prepares; forward for client-visible Parse.
- On ErrorResponse, clear pending parse state for that backend to avoid stale mappings.
- Detect `DISCARD ALL`, `DEALLOCATE ALL`, `RESET ALL` in simple Query and clear backend caches.

### Phase 6: Observability + tests
- Add counters/logs for injected prepares, dedupe hits, conflicts, missing portals.
- Unit tests for frame rewrites and name substitution.
- Integration tests for:
  - Prepared statements across backend hops.
  - Same statement name with different SQL.
  - Pipelined extended cycles and correct backend release.
  - Close/portal cleanup.

## Acceptance criteria
- No "already exists" or "does not exist" errors when hopping backends.
- Extended protocol cycles stay on one backend until `Sync` completes.
- Portal messages (Describe/Execute/Close) work with rewritten names.
- Backend invalidation and reset clear prepared caches correctly.

## Open questions
- Do we want to keep pipelining enabled or gate to one cycle at a time?
- Should the default conflict policy be strict or allow redefinition behind a flag?
- Do we keep `DISCARD ALL` on pool return or optimize once virtualization is in place?
