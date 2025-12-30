# PgCrab

A Postgres connection pooler with sharding in mind. Today it focuses on being a
simple, fast pooler; query-aware routing comes later.

```
   _  _  ____   ____                 _     
  | || ||  _ \ / ___|_ __ __ _  ___ | |__  
  | || || |_) | |  _| '__/ _` |/ __|| '_ \ 
  | || ||  __/| |_| | | | (_| | (__ | |_) |
  |_||_||_|    \____|_|  \__,_|\___||_.__/ 
```

## Status
- Early-stage but functional pooler with a working frontend/backend proxy.
- Sharding is not wired yet; backend selection is currently random per query.

## Features
- Backend connection pooling with min/max sizing and warm-up.
- Transparent query forwarding (simple and extended protocol sequences).
- Transaction-style pooling: backend returned to pool on `ReadyForQuery`.
- Cleartext client auth against `[[users]]` in config.
- Query parser scaffolding (AST parsing module) ready for routing work.

## How it works
- Client connects to PgCrab and authenticates.
- On first query, PgCrab acquires a backend connection from a shard pool.
- Frontend frames are forwarded to the backend, backend frames stream back.
- When backend sends `ReadyForQuery`, the connection is released to the pool.

## Configuration
The config file defines backend shards and client users. Each shard entry also
defines the backend credentials used by PgCrab.

`pgcrab.toml` example:

```toml
[[shards]]
host = "127.0.0.1"
port = 5432
name = "pgcrab_shard_1"
user = "mr_krabs"
password = "i_love_money"
min_connections = 5
max_connections = 20

[[users]]
username = "pgcrab"
password = "pgcrab"
```

Notes:
- PgCrab currently uses the shard `name` as the backend database name.
- Backend auth only supports cleartext for now.

## Run
```bash
cargo run -- --host 127.0.0.1 --port 6432 --config pgcrab.toml
```

Environment-based configuration is also supported:

```bash
PGCRAB_HOST=127.0.0.1 \
PGCRAB_PORT=6432 \
PGCRAB_CONFIG_FILE=pgcrab.toml \
cargo run
```

## Connect
```bash
psql "host=127.0.0.1 port=6432 user=pgcrab password=pgcrab dbname=pgcrab_shard_1"
```

## Tests
Integration tests expect live Postgres instances for each shard in
`pgcrab.toml`.

```bash
cargo test
```

## Limitations (current)
- No shard routing yet (backend selection is random).
- Prepared statements do not persist across pooled sessions.
- Backend auth supports cleartext only.
- Query parsing is not wired into the frontend yet.

## Roadmap
See `PLAN.md` for the near-term work.

## License
TBD
