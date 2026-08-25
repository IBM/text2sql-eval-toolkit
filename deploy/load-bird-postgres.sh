#!/usr/bin/env bash
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
# Load the BIRD Mini-Dev PostgreSQL database (plan item 3.11).
#
# Unlike the SQLite and MySQL benchmarks, the Postgres variant is a *single*
# database: the upstream dump merges all eleven BIRD databases into one `public`
# schema (75 tables). That matches how the toolkit executes against it --
# postgres_run_execution_async() sets `search_path` once and never switches on
# `db_id` -- so there is nothing to rename here, only to load.
#
# Usage:
#   BIRD_DUMP=/path/to/MINIDEV_postgresql/BIRD_dev.sql \
#   PGHOST=127.0.0.1 PGUSER=postgres BIRD_DB=bird \
#     deploy/load-bird-postgres.sh
#
# Drops and recreates the target database, so a failed load can be re-run.
set -euo pipefail

# No default; see load-beaver.sh.
DUMP="${BIRD_DUMP:?set BIRD_DUMP to the BIRD_dev.sql from MINIDEV_postgresql}"
DB="${BIRD_DB:-bird}"
PGHOST="${PGHOST:-127.0.0.1}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-$(whoami)}"
export PGHOST PGPORT PGUSER

log() { printf '[bird-pg] %s\n' "$*"; }
fail() { printf '[bird-pg] ERROR: %s\n' "$*" >&2; exit 1; }

command -v psql >/dev/null 2>&1 || fail "psql is not on PATH."
[ -f "$DUMP" ] || fail "dump not found: $DUMP"

psql -d postgres -c "SELECT 1" >/dev/null 2>&1 \
  || fail "cannot connect to PostgreSQL at ${PGHOST}:${PGPORT} as ${PGUSER}."

size=$(du -m "$DUMP" | cut -f1)
log "loading ${DUMP} (${size} MB) into database '${DB}'"

# The dump carries `OWNER TO <role>` for whoever produced it, and psql aborts on
# an unknown role. Create any such role as NOLOGIN rather than rewriting the
# dump: editing 1 GB of SQL to strip ownership risks mangling a data line, and a
# login-less role grants nobody anything.
owners=$(grep -oE 'OWNER TO [A-Za-z0-9_]+' "$DUMP" | awk '{print $3}' | sort -u || true)
for owner in $owners; do
  if ! psql -d postgres -At -c "SELECT 1 FROM pg_roles WHERE rolname='${owner}'" | grep -q 1; then
    log "creating absent dump owner role '${owner}' (NOLOGIN)"
    psql -d postgres -v ON_ERROR_STOP=1 -q -c "CREATE ROLE \"${owner}\" NOLOGIN;"
  fi
done

psql -d postgres -v ON_ERROR_STOP=1 -q <<SQL
DROP DATABASE IF EXISTS "${DB}";
CREATE DATABASE "${DB}";
SQL

# ON_ERROR_STOP so a partial load fails loudly rather than leaving a database
# that looks fine until a query hits a missing table.
psql -d "$DB" -v ON_ERROR_STOP=1 -q -f "$DUMP" \
  || fail "loading the dump failed; the database is left in place for inspection."

tables=$(psql -d "$DB" -At -c \
  "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public';")
log "loaded ${tables} tables into ${DB}.public"

if [ "$tables" -lt 70 ]; then
  fail "expected ~75 tables in public; found ${tables}. The load looks incomplete."
fi

# Optional read-only role, matching what the deployment uses. The execute
# endpoint runs arbitrary caller-supplied SQL, so a write should be refused by
# the database rather than by application logic.
if [ -n "${POSTGRES_READONLY_PASSWORD:-}" ]; then
  log "creating read-only role"
  psql -d "$DB" -v ON_ERROR_STOP=1 -q <<SQL
DO \$\$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'readonly') THEN
    CREATE ROLE readonly LOGIN PASSWORD '${POSTGRES_READONLY_PASSWORD}';
  END IF;
END \$\$;
GRANT CONNECT ON DATABASE "${DB}" TO readonly;
GRANT USAGE ON SCHEMA public TO readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO readonly;
ALTER ROLE readonly SET default_transaction_read_only = on;
ALTER ROLE readonly SET statement_timeout = '90s';
REVOKE CREATE ON SCHEMA public FROM readonly;
SQL
  log "read-only role ready"
else
  log "POSTGRES_READONLY_PASSWORD unset; skipping the read-only role."
  log "Set it before exposing this database to the dashboard."
fi

cat <<NOTE
[bird-pg] done.

[bird-pg] Point the toolkit at it with:
[bird-pg]   export POSTGRES_CONNECTION_STRING="postgresql://USER:PASS@${PGHOST}:${PGPORT}/${DB}"
[bird-pg] All eleven BIRD databases live in this one schema, so unlike the
[bird-pg] SQLite and MySQL benchmarks there is no per-record database switching.
NOTE
