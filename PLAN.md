# Design doc: Virtual prepared statements in a Postgres proxy (Option 2)

## Problem

Some clients use the extended query protocol and rely on server-side prepared statement state:

- Client sends `Parse(name="X", sql="...")`
- Later sends `Bind(statement="X", values=[...])`
- Expects `Bind` to hit the same backend connection that already saw `Parse`

Your proxy breaks that assumption when it load-balances / pools across backend connections. Result:

- `prepared statement "X" already exists`
  You forwarded a `Parse` for `X` onto a backend connection that already has `X`.
- `prepared statement "X" does not exist`
  You forwarded a `Bind` for `X` onto a backend connection that never saw the `Parse`.

**Option 2** means: don’t pin sessions. Instead, **virtualize** prepared statements and portals at the proxy layer.

---

## Goal

Make prepared statements “work” for clients even when you do **transaction pooling** (or otherwise hop backend connections), by making statement names **proxy-managed** and backend-specific.

**Client-visible invariant:** client can reuse `name="X"` and it behaves consistently.

---

## Non-goals (explicit constraints)

- You will not try to preserve server-side _session_ semantics across backend hops. The only thing you virtualize is **extended-protocol state** (statements + portals).
- You will not support long-lived portals that span `Sync` across different backends. (You can later, but it’s a trap.)
- You will not attempt to replicate backend caches. You’ll just **prepare on demand** on whatever backend you route to.

---

## Protocol reality you must model

### Two stateful objects (both per backend connection)

1. **Prepared statement**

- Created by `Parse(statement_name, sql, param_type_oids)`
- Referenced by `Bind(statement_name, …)`
- Destroyed by `Close(Statement, statement_name)` or `DEALLOCATE ALL` / `DISCARD ALL` (or connection reset)

2. **Portal**

- Created by `Bind(portal_name, statement_name, …)`
- Referenced by `Execute(portal_name)`
- Destroyed by `Close(Portal, portal_name)` or `Sync` (effectively, in most client patterns)

### Message groups

Most clients treat everything between a “start” and `Sync` as a tight unit. Your proxy should too.

---

## Core idea

### 1) Client statement names are **virtual**

Client says: `Parse(name="X")`
Proxy treats `"X"` as a handle that maps to a query definition (SQL + param types).

### 2) Backend statement names are **proxy-assigned**

Proxy picks a backend-specific name like: `ps_<backendId>_<counter>`
Client never sees it.

### 3) On each backend hop, you re-prepare as needed

If client binds to `X` on backend B7 and B7 hasn’t prepared that statement yet, proxy sends a `Parse` first (with a backend-specific name), then sends the `Bind`.

That prevents both “already exists” and “does not exist”.

---

## High-level architecture

- **Frontend session**: one client connection to the proxy
- **Backend pool**: many server connections to Postgres
- **Extended-protocol router**:
  - tracks virtual statements per frontend session
  - tracks prepared statements per backend connection
  - rewrites names in-flight
  - injects `Parse` when required

---

## Data model (what you must store)

### A) Per frontend session (client connection)

```text
FrontendSessionState:
  virtual_statements: Map<ClientStatementName, VirtualStatement>
  virtual_portals:    Map<ClientPortalName, PortalBinding>   (cleared at Sync)
  in_flight_backend:  BackendConnId | null                   (pin only within a cycle)
```

```text
VirtualStatement:
  generation: integer                 // bump when redefined
  sql_text: string
  param_type_oids: int[]              // from Parse
  signature: bytes                    // hash(sql_text + param_type_oids)
  closed: boolean
```

```text
PortalBinding:
  backend_conn_id: BackendConnId
  backend_portal_name: string
```

### B) Per backend connection (server connection)

```text
BackendConnState:
  prepared_by_signature: Map<Signature, BackendStatementName>
  signature_by_name:     Map<BackendStatementName, Signature>
  epoch: integer         // bump on reset / DISCARD ALL / DEALLOCATE ALL
  prepare_locks: Map<Signature, Promise<void>>   // single-flight
```

**Why both maps?**

- `prepared_by_signature` lets you dedupe prepares
- `signature_by_name` lets you debug and handle “already exists” safely

---

## Naming / rewriting strategy

### Backend statement names

Must be:

- unique per backend connection
- never collide with client-generated names
- short-ish (protocol overhead matters)

Example:

- `ps_<epoch>_<counter>` (counter per backend conn)
- or `ps_<first8(sig)>_<counter>` (makes debugging easier)

### Backend portal names

Treat similarly:

- `pt_<counter>` per backend conn

You will rewrite:

- `Parse` statement name
- `Bind` statement name + portal name
- `Describe` (statement/portal)
- `Execute` portal name
- `Close` statement/portal name

---

## Routing policy (the key correctness move)

### “Cycle pinning”

Even with transaction pooling, you should pin **only within an extended-protocol cycle**:

- From first extended message (`Parse` / `Bind` / `Describe` / `Execute`) until `Sync`
- All messages in that cycle go to the same backend connection
- After `Sync`, release backend to pool

This isn’t “session pinning”. It’s “don’t scramble a single pipeline”.

---

## Signature rules (how you decide “same statement”)

Compute:

`signature = hash( sql_text || 0x00 || param_type_oids_as_bytes )`

Include param types because Postgres prepared statement identity includes parameter typing at parse/analyze time.

If a client sends no types, use empty list. (It’s still part of identity.)

---

## Message handling (exact behavior)

### 1) On `Parse(client_stmt_name, sql, param_types)`

Steps:

1. Ensure you have a backend for the current cycle:
   - if `in_flight_backend` is null: acquire one from pool and set it

2. Update virtual statement definition:
   - If no entry for `client_stmt_name`: create `VirtualStatement(generation=1, signature=...)`
   - If exists and signature matches:
     - Treat as idempotent. You can **suppress** forwarding to backend.

   - If exists and signature differs:
     - Bump generation, replace definition (or error; see “conflicts” below)

3. Ensure statement is prepared on this backend:
   - Look up `BackendConnState.prepared_by_signature[signature]`
   - If present: done
   - Else: **prepare on backend** (single-flight)

4. Forward to backend?
   - You may choose to **not** forward the client’s Parse at all (recommended).
   - Instead:
     - prepare with proxy-assigned backend name
     - return `ParseComplete` to client

   - This keeps backend state clean and avoids name collisions entirely.

**Recommended:** proxy becomes the only entity creating backend statement names.

---

### 2) On `Describe(Statement, client_stmt_name)`

Steps:

1. Ensure backend for cycle
2. Resolve virtual statement; if missing/closed → error to client
3. Ensure prepared on backend (inject Parse if needed)
4. Rewrite to backend statement name and forward Describe
5. Forward backend’s `ParameterDescription` and `RowDescription` back to client unchanged

---

### 3) On `Bind(client_portal_name, client_stmt_name, …)`

Steps:

1. Ensure backend for cycle
2. Resolve virtual statement; if missing/closed → error
3. Ensure prepared on backend (inject Parse if needed)
4. Allocate backend portal name (per backend conn counter)
5. Store portal mapping:
   - `virtual_portals[client_portal_name] = { backend_conn_id, backend_portal_name }`

6. Rewrite:
   - statement name → backend statement name
   - portal name → backend portal name

7. Forward Bind

---

### 4) On `Execute(client_portal_name, …)`

Steps:

1. Look up portal mapping:
   - if missing → error (or recover only if you can re-run Bind, but you usually can’t)

2. Ensure cycle backend matches portal backend:
   - If not, that’s a proxy bug. In practice, cycle pinning prevents this.

3. Rewrite portal name to backend portal name
4. Forward Execute

---

### 5) On `Close(Statement, client_stmt_name)`

What “close” should mean under virtualization:

- Client expects: “I can’t bind this statement name anymore unless I Parse again.”

So you do:

1. Mark `VirtualStatement.closed = true` and delete virtual mapping
2. Optionally forward `Close(Statement, backend_name)` **only to the current cycle backend** if prepared there
   - Don’t attempt to close on all backends (expensive and often impossible)
   - Backend caches will naturally die on reset/eviction anyway

This keeps client semantics correct without needing global backend cleanup.

---

### 6) On `Close(Portal, client_portal_name)`

1. Rewrite to backend portal name and forward
2. Delete portal mapping

---

### 7) On `Sync`

1. Clear `virtual_portals` (portals do not survive hop)
2. Release `in_flight_backend` back to pool
3. Set `in_flight_backend = null`
4. Forward `ReadyForQuery` handling as usual

---

## Preparing on demand (single-flight)

When you need to prepare `signature` on backend B:

1. Check `prepared_by_signature`
2. If absent, acquire a per-(backend, signature) lock:
   - if a prepare is already in progress, await it

3. Send to backend:
   - `Parse(backend_stmt_name, sql_text, param_type_oids)`
   - (optional) `Describe` only if the client asked later; don’t pre-describe by default
   - `Sync` if needed for backend flow control (depends on your backend driver)

4. Record:
   - `prepared_by_signature[signature] = backend_stmt_name`
   - `signature_by_name[backend_stmt_name] = signature`

**Important:** your backend connection must serialize protocol messages anyway. So single-flight is mostly about preventing duplicate work when frontend pipelines hard.

---

## Conflict policy (client reuses a name)

### Case A: Same name, same signature

Treat as idempotent. Suppress duplicate prepare.

### Case B: Same name, different signature

Two choices:

**Strict (safe):**

- Return error to client: “statement name reused without close”
- This matches server behavior (server would error on duplicate Parse name)

**Pragmatic (usually fine):**

- Bump generation and replace virtual definition
- Future `Bind(name)` uses the latest definition
- Risk: if client still has portals bound to the old definition under the same name, it can get confusing
- In practice most client libs don’t do that; they close first

I’d implement **strict by default**, pragmatic behind a flag, then see what real clients do.

---

## Backend invalidation rules

Your backend prepared cache can become wrong. You need explicit invalidation.

### Invalidate everything on a backend connection when:

- backend connection resets / reconnects
- you forward `DISCARD ALL` to that backend
- you forward `DEALLOCATE ALL` to that backend
- you forward `RESET ALL` (optional; less direct)
- you detect protocol-level fatal error that implies session reset

Implementation:

- `BackendConnState.epoch++`
- clear `prepared_by_signature` and `signature_by_name`

### Detecting `DISCARD ALL` / `DEALLOCATE ALL`

These come via the simple query protocol (`Query` message) as SQL text. If you support that mode, you need to parse just enough to recognize these commands (case-insensitive, ignore whitespace/comments).

---

## Recovery strategy (when things still go wrong)

### If backend replies `prepared statement "...already exists"`

This should not happen if you never forward client statement names and you dedupe by signature. If it does happen:

- treat it as an internal bug
- recover by allocating a new backend statement name and re-Parse once
- update `prepared_by_signature` to the new name

### If backend replies `prepared statement "...does not exist"`

Likely causes:

- backend invalidation you didn’t notice
- lost backend state due to reconnect
- bug in routing

Recovery:

- if you have the signature and SQL: re-prepare and retry exactly once
- if it still fails: surface error

### If backend replies portal does not exist

Don’t retry. A portal depends on Bind parameters. You usually cannot reconstruct it unless you buffered the whole Bind.

---

## Observability (so you can prove it works)

### Metrics

- `proxy_prepare_injected_total`
- `proxy_prepare_dedup_hits_total`
- `proxy_prepare_conflicts_total`
- `proxy_bind_missing_virtual_statement_total`
- `proxy_execute_missing_portal_total`
- `proxy_backend_invalidation_total`
- `proxy_cycle_backend_acquire_total`
- `proxy_cycle_backend_release_total`

### Logs (sampled)

For each extended cycle:

- frontend session id
- chosen backend id
- list of virtual statement names referenced
- for each: signature + whether prepared was injected or hit

You want to answer: “why did we inject a prepare here?”

---

## Implementation plan

### Phase 1: Make cycles atomic

- Add `in_flight_backend` pinning until `Sync`
- Ensure you never route `Bind/Execute` of the same cycle to different backends

### Phase 2: Add virtualization tables

- `virtual_statements` per frontend
- `prepared_by_signature` per backend
- portal mapping per cycle

### Phase 3: Rewrite and inject

- Stop forwarding client statement names to backend
- Inject `Parse` with proxy names on demand
- Rewrite `Bind/Describe/Close/Execute` accordingly

### Phase 4: Invalidation + retry-once

- backend reset handling
- SQL detection for wipe commands
- retry once on missing prepared statement

### Phase 5: Hardening

- strict conflict policy by default
- single-flight prepares
- backpressure / pipeline correctness

---

## Acceptance criteria

- Clients can:
  - `Parse` once, then `Bind/Execute` many times, even if your proxy uses transaction pooling

- Under concurrent load:
  - no “already exists”
  - no “does not exist”

- Your proxy shows:
  - high dedupe rate for repeated statements
  - injected prepares only on first use per backend

---

If you tell me your pooling model (pure transaction pooling vs mixed) and whether you support pipelining (client sends multiple extended messages without waiting), I can tighten the design into a concrete state machine with exact buffering rules (the tricky part is pipelined `Parse/Bind/Describe` ordering).
