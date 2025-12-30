# Plan: forward queries via pooled backend connections

## Goals
- Acquire a backend connection on first frontend query.
- Forward frontend messages to the backend and stream backend responses back.
- Release the backend connection when the backend emits `ReadyForQuery`.
- Keep protocol framing correct (no partial or merged frames).

## Data structures (where they live)
- `frontend`:
  - `FrontendContext` keeps `gateway_session: Option<GatewaySession>`.
  - Add a small backend response tracker to parse message frames (tag + length).
- `gateway`:
  - `GatewaySession` wraps `PooledConnection` and provides `backend()` access.
  - `GatewayPools` stays the source of pooled connections.
- `backend`:
  - Extend `BackendConnection` with buffer accessors: `buffer()`, `consume(n)`.

## Implementation steps
1. Add backend frame parsing helpers.
   - Implement a `peek_backend(bytes) -> Option<(tag, len)>` utility.
   - Minimal: parse `tag` + `i32 length` (length includes itself).
2. Extend `BackendConnection` to expose its read buffer.
   - Provide `buffer()` for read-only slice and `consume(n)` to advance.
3. Add backend read branch to `FrontendConnection::serve`.
   - When `gateway_session` is present, `select!` on backend read.
   - For each complete backend frame, forward bytes to the frontend outbox.
   - Detect `ReadyForQuery` (tag `Z`), then release the session.
4. Forward frontend Ready-stage sequences to backend.
   - In `handle_ready`, if session is missing, acquire a random pool.
   - Write the frontend sequence bytes directly to `backend()`.
5. Release the backend connection on ReadyForQuery.
   - Drop `GatewaySession` (returning `PooledConnection` to the pool).
   - Reset any backend tracker state.
6. Error handling and safety.
   - On backend read/write errors, send an `ErrorResponse` to the frontend and
     clear `gateway_session`.
   - Ensure no writes happen after a session is released.

## Validation
- Add a unit test for `peek_backend` frame parsing.
- Exercise a simple query in integration tests and ensure responses are forwarded.
- Verify that backend connections return to the pool after `ReadyForQuery`.
