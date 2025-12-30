#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# This script provisions local Postgres shard databases for PgCrab integration
# tests. We keep it explicit and repeatable so contributors can bootstrap a
# known-good local setup without manual psql steps.
# -----------------------------------------------------------------------------

CONFIG_PATH="${PGCRAB_CONFIG_FILE:-./pgcrab.toml}"

ADMIN_USER="${PGUSER:-postgres}"
ADMIN_PASSWORD="${PGPASSWORD:-postgres}"

HOST=""
PORT=""
declare -a SHARD_USERS=()
declare -a SHARD_PASSWORDS=()
declare -a DATABASES=()
declare -a DB_USERS=()

trim() {
  local s="$1"
  s="${s#"${s%%[![:space:]]*}"}"
  s="${s%"${s##*[![:space:]]}"}"
  printf '%s' "$s"
}

read_shards() {
  local in_shard=0
  local name="" host="" port="" user="" password=""

  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%%#*}"
    line="$(trim "$line")"
    [[ -z "$line" ]] && continue

    if [[ "$line" == "[[shards]]" ]]; then
      if [[ $in_shard -eq 1 ]]; then
        DATABASES+=("$name")
        DB_USERS+=("$user")
        SHARD_USERS+=("$user")
        SHARD_PASSWORDS+=("$password")
        if [[ -z "$HOST" ]]; then
          HOST="$host"
          PORT="$port"
        elif [[ "$HOST" != "$host" || "$PORT" != "$port" ]]; then
          echo "setup-db requires all shards to share the same host and port" >&2
          exit 1
        fi
      fi

      in_shard=1
      name="" host="" port="" user="" password=""
      continue
    fi

    if [[ $in_shard -eq 1 ]]; then
      key="${line%%=*}"
      val="${line#*=}"
      key="$(trim "$key")"
      val="$(trim "$val")"
      val="${val%\"}"
      val="${val#\"}"

      case "$key" in
        name) name="$val" ;;
        host) host="$val" ;;
        port) port="$val" ;;
        user) user="$val" ;;
        password) password="$val" ;;
      esac
    fi
  done < "$CONFIG_PATH"

  if [[ $in_shard -eq 1 ]]; then
    DATABASES+=("$name")
    DB_USERS+=("$user")
    SHARD_USERS+=("$user")
    SHARD_PASSWORDS+=("$password")
    if [[ -z "$HOST" ]]; then
      HOST="$host"
      PORT="$port"
    elif [[ "$HOST" != "$host" || "$PORT" != "$port" ]]; then
      echo "setup-db requires all shards to share the same host and port" >&2
      exit 1
    fi
  fi
}

read_shards

if [[ ${#DATABASES[@]} -eq 0 ]]; then
  echo "pgcrab.toml has no [[shards]] entries" >&2
  exit 1
fi

if [[ -z "${PGPASSWORD:-}" ]]; then
  export PGPASSWORD="$ADMIN_PASSWORD"
fi

if ! psql -h "$HOST" -p "$PORT" -U "$ADMIN_USER" -d postgres -c "select 1;" >/dev/null 2>&1; then
  echo "Postgres is required at ${HOST}:${PORT} with ${ADMIN_USER}:${ADMIN_PASSWORD} for setup-db."
  echo "Start Postgres and ensure credentials are correct, then rerun."
  echo "psql error:"
  psql -h "$HOST" -p "$PORT" -U "$ADMIN_USER" -d postgres -c "select 1;"
  exit 1
fi

for i in "${!SHARD_USERS[@]}"; do
  user="${SHARD_USERS[$i]}"
  password="${SHARD_PASSWORDS[$i]}"
  echo "Ensuring role ${user}"
  psql -h "$HOST" -p "$PORT" -U "$ADMIN_USER" -d postgres -v ON_ERROR_STOP=1 -q <<SQL
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${user}') THEN
    CREATE ROLE ${user} WITH LOGIN PASSWORD '${password}';
  ELSE
    ALTER ROLE ${user} WITH LOGIN PASSWORD '${password}';
  END IF;
END
\$\$;
SQL
done

for i in "${!DATABASES[@]}"; do
  db="${DATABASES[$i]}"
  db_user="${DB_USERS[$i]}"
  exists="$(psql -h "$HOST" -p "$PORT" -U "$ADMIN_USER" -d postgres -tAc "select 1 from pg_database where datname='${db}'")"
  if [[ "$exists" != "1" ]]; then
    echo "Creating database ${db} (owner ${db_user})"
    psql -h "$HOST" -p "$PORT" -U "$ADMIN_USER" -d postgres -v ON_ERROR_STOP=1 -q \
      -c "create database ${db} owner ${db_user};"
  else
    echo "Database ${db} already exists"
  fi
done
