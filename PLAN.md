# Plan: Analytics Module (parse cache metrics)

## Goal
Track parse cache activity with atomic counters so we can measure effectiveness.

## Scope
- Add an `analytics` module with two `AtomicU128` counters:
  - `parse_cache_hit`
  - `parse_cache_miss`
- Increment the counters on parse-cache hit/miss in `src/parser/mod.rs`.
- Expose a small API for incrementing and snapshotting counts.

## Implementation steps
1. Create `src/analytics/mod.rs`.
   - Define `static` counters using `AtomicU128` with relaxed ordering.
   - Provide `inc_parse_cache_hit()`, `inc_parse_cache_miss()`, and `snapshot()`.
2. Wire counters into the parse cache.
   - In `ParserCache::get` (or the cache-hit branch), call `inc_parse_cache_hit()`.
   - In the cache-miss path (before parsing), call `inc_parse_cache_miss()`.
3. Add a lightweight test (optional).
   - Verify increments and snapshotting in a unit test, with a reset helper guarded under `cfg(test)`.

## Open questions
- Do we need a reset API outside tests?
- Should we include per-statement or per-shard counters later?
