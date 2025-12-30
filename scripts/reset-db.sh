#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Reset shard databases defined in pgcrab.toml.
# This is a cleanup helper for local dev environments.
# -----------------------------------------------------------------------------

CONFIG_PATH="${PGCRAB_CONFIG_FILE:-./pgcrab.toml}"

ADMIN_USER="${PGUSER:-postgres}"
ADMIN_PASSWORD="${PGPASSWORD:-postgres}"

HOST=""
PORT=""
declare -a DATABASES=()

trim() {
  local s="$1"
  s="${s#"${s%%[![:space:]]*}"}"
  s="${s%"${s##*[![:space:]]}"}"
  printf '%s' "$s"
}

read_shards() {
  local in_shard=0
  local name="" host="" port=""

  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%%#*}"
    line="$(trim "$line")"
    [[ -z "$line" ]] && continue

    if [[ "$line" == "[[shards]]" ]]; then
      if [[ $in_shard -eq 1 ]]; then
        DATABASES+=("$name")
        if [[ -z "$HOST" ]]; then
          HOST="$host"
          PORT="$port"
        elif [[ "$HOST" != "$host" || "$PORT" != "$port" ]]; then
          echo "reset-db requires all shards to share the same host and port" >&2
          exit 1
        fi
      fi

      in_shard=1
      name="" host="" port=""
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
      esac
    fi
  done < "$CONFIG_PATH"

  if [[ $in_shard -eq 1 ]]; then
    DATABASES+=("$name")
    if [[ -z "$HOST" ]]; then
      HOST="$host"
      PORT="$port"
    elif [[ "$HOST" != "$host" || "$PORT" != "$port" ]]; then
      echo "reset-db requires all shards to share the same host and port" >&2
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
  echo "Postgres is required at ${HOST}:${PORT} with ${ADMIN_USER}:${ADMIN_PASSWORD} for reset-db."
  echo "Start Postgres and ensure credentials are correct, then rerun."
  exit 1
fi

for db in "${DATABASES[@]}"; do
  psql -h "$HOST" -p "$PORT" -U "$ADMIN_USER" -d postgres -q -v ON_ERROR_STOP=1 \
    -c "select pg_terminate_backend(pid) from pg_stat_activity where datname = '${db}';" \
    >/dev/null
  psql -h "$HOST" -p "$PORT" -U "$ADMIN_USER" -d postgres -q -v ON_ERROR_STOP=1 \
    -c "drop database if exists ${db};" \
    >/dev/null
done

make -s setup-db
