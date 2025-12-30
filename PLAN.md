# Refactor plan: FrontendConnection readability

## Goals
- Make the Startup -> Authenticating -> Ready flow obvious at a glance.
- Separate IO/buffering, protocol parsing, auth/backends, and response building.
- Reduce the size and cognitive load of `FrontendConnection` without changing behavior.

## Current pain points (from `src/frontend/connection.rs`)
- One file handles IO, state machine, parsing, auth, backend connect, and response encoding.
- Stage-specific handling mixes parsing, state updates, and response assembly.
- Response builders are embedded at the bottom of the connection implementation.
- Debug prints in the Ready handler obscure intent.

## Proposed refactor steps
1. Document the state machine in-code.
   - Add a top-level doc comment for `FrontendConnection` explaining stages and key transitions.
   - Add a short comment in the main loop about the read -> track -> process -> flush flow.
2. Extract backend response builders.
   - Move `be_*` helpers into `src/frontend/responses.rs` (or `src/wire_protocol/backend.rs`).
   - Keep the same public signatures and return types.
3. Split stage handlers into focused modules.
   - Create `src/frontend/handlers/startup.rs`, `authenticating.rs`, `ready.rs`.
   - Each module exposes a `handle_*` function that receives a mutable context.
4. Introduce a small context struct for shared state.
   - Example fields: `stage`, `username`, `database`, `backend_identity`, `gateway_session`.
   - Keep IO buffers and tracking separate from stage logic.
5. Isolate IO buffering and sequencing.
   - Move inbox/outbox + `SequenceTracker` logic into a helper struct (e.g., `FrontendBuffers`).
   - Expose small methods: `read_into_inbox`, `drain_sequences`, `queue_response`, `flush`.
6. Clean up error handling and logging.
   - Replace `println!` with structured logging or remove noisy debug output.
   - Normalize protocol violation responses in one place.

## Non-goals
- No behavior changes to wire protocol handling.
- No changes to public exports in `src/lib.rs` or `src/frontend/mod.rs`.

## Validation
- Manual smoke test: `psql` connection and auth flow.
- If tests exist, run the existing suite unchanged.
