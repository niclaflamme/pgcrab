# Plan: SQL parser (early wins first)

## Early wins (now)
- Intercept frontend `Query`/`Parse` messages.
- Parse SQL text into an AST with a library parser.
- Print the AST in debug logs, then relay the query unchanged.
- Treat parse errors as non-fatal: log and keep forwarding.

## Minimal data structures
- `parser::parse(query: &str) -> Result<ParsedQuery, ParseError>`
- `ParsedQuery`:
  - `statement_type` (Select/Insert/Update/Delete/Other)
  - `tables` (qualified names when available)

## Implementation steps
1. Pick a parser crate and add it as a dependency (start with `pg_query` or `sqlparser`).
2. Create `src/parser/mod.rs` with a small wrapper:
   - Parse SQL text to AST.
   - Accept only the first statement.
3. Wire parsing into `frontend` ready handling:
   - On `Query` frames, parse and log the AST, then forward as-is.
   - On `Parse` frames, parse and log the AST, then forward as-is.
4. Non-fatal errors:
   - If parsing fails, log `ParseError` and keep forwarding.
5. Tests:
   - Unit tests for parsing basic `SELECT/INSERT/UPDATE/DELETE`.
   - Integration test to ensure parsing does not break query forwarding.

## Future TODO (after early wins)
- Extract statement type and referenced tables from the AST.
- Add a prepared-statement metadata cache keyed by name.
- Parse `Bind` to map parameters to parsed statements.
- Normalize or fingerprint queries for caching/metrics.
- Use parsed output for routing (shards, read/write hints).
