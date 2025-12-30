# Plan: Prepared Statements in PgCrab (current state)

## Status (implemented)
- Cache named `Parse` frames per client session.
- Track per-backend prepared names.
- Inject cached `Parse` before `Bind` when the backend hasn’t seen it.
- Suppress injected `ParseComplete` to keep client responses aligned.
- Track statement `Close` and drop cached/prepared entries.

## Next steps (priority)
1. Add backend reset on pool release.
   - Send a reset query (prefer `DISCARD ALL`) before placing a connection back in the idle pool.
   - Drain backend responses until `ReadyForQuery`.
2. Add integration tests for prepared statements across pooled backends.
   - Prepare named statement; execute across multiple transactions; ensure no extra `ParseComplete` reaches the client.
   - Ensure a new client cannot use a previously prepared statement after pool reuse.
3. Extend protocol handling for edge cases.
   - Decide whether to cache/replay `Describe`.
   - Decide how to update the cache on `DEALLOCATE`/`DISCARD` via simple `Query`.
