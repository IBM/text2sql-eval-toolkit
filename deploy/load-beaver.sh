#!/usr/bin/env bash
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
# Load the Beaver MySQL databases (plan item 3.12).
#
# The upstream dumps are named after the source systems (nova, neutron, dw) but
# the benchmark addresses two of them by a prefixed name (csail_stata_nova,
# csail_stata_neutron), because `db_id` is substituted into the connection
# string at execution time. Loading the dumps unchanged therefore produces
# databases the benchmark cannot find, so the CREATE DATABASE / USE statements
# are rewritten to the names the questions actually reference.
#
# Coverage, from the dumps currently published upstream:
#
#   dw                    121 questions   dw.sql
#   csail_stata_nova       43 questions   nova.sql
#   csail_stata_neutron    30 questions   neutron.sql
#   ---------------------------------------------------------------
#   keystone                8 questions   NO DUMP PUBLISHED
#   csail_stata_glance      5 questions   NO DUMP PUBLISHED
#   csail_stata_cinder      2 questions   NO DUMP PUBLISHED
#
# So 194 of 209 questions (93%) are executable. The remaining 15 will fail with
# an unknown-database error until upstream publishes those three dumps; that is
# a data gap, not a configuration mistake.
#
# Usage:
#   BEAVER_DUMP_DIR=/path/to/beaver_db \
#   MYSQL_HOST=127.0.0.1 MYSQL_USER=root MYSQL_PASSWORD=... \
#     deploy/load-beaver.sh
#
# Idempotent in the sense that it drops and recreates each target database, so a
# failed load can simply be re-run.
set -euo pipefail

# No default: the dumps are downloaded separately and there is no location
# every operator shares. Failing here beats failing later against the wrong path.
DUMP_DIR="${BEAVER_DUMP_DIR:?set BEAVER_DUMP_DIR to the directory holding the Beaver .sql dumps}"
MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_USER="${MYSQL_USER:-root}"

# dump file : target database name the benchmark uses
MAPPINGS=(
  "dw.sql:dw"
  "neutron.sql:csail_stata_neutron"
  "nova.sql:csail_stata_nova"
)

log() { printf '[beaver] %s\n' "$*"; }
fail() { printf '[beaver] ERROR: %s\n' "$*" >&2; exit 1; }

mysql_cmd() {
  mysql --host="$MYSQL_HOST" --port="$MYSQL_PORT" --user="$MYSQL_USER" \
        ${MYSQL_PASSWORD:+--password="$MYSQL_PASSWORD"} "$@"
}

command -v mysql >/dev/null 2>&1 || fail "the mysql client is not on PATH."
[ -d "$DUMP_DIR" ] || fail "dump directory not found: $DUMP_DIR"

mysql_cmd -e "SELECT 1" >/dev/null 2>&1 \
  || fail "cannot connect to MySQL at ${MYSQL_HOST}:${MYSQL_PORT} as ${MYSQL_USER}."

for mapping in "${MAPPINGS[@]}"; do
  dump="${mapping%%:*}"
  target="${mapping##*:}"
  path="${DUMP_DIR}/${dump}"

  if [ ! -f "$path" ]; then
    log "SKIP ${target}: ${dump} not present in ${DUMP_DIR}"
    continue
  fi

  size=$(du -m "$path" | cut -f1)
  log "loading ${dump} (${size} MB) into database '${target}'"

  mysql_cmd -e "DROP DATABASE IF EXISTS \`${target}\`;
                CREATE DATABASE \`${target}\`
                  CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;"

  # Drop the dump's own CREATE DATABASE / USE lines so the data lands in the
  # target name instead. Anchored to line start and matched against the exact
  # source names, so an INSERT payload cannot be mistaken for a statement. The
  # table-count check below is what actually proves nothing was mangled.
  sed -E '/^CREATE DATABASE +`?(dw|nova|neutron)`?/d; /^USE +`?(dw|nova|neutron)`?;/d' "$path" \
    | mysql_cmd --database="$target" \
    || fail "loading ${dump} into ${target} failed."

  count=$(mysql_cmd -N -B -e \
    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='${target}';")
  log "  ${target}: ${count} tables"
done

log "verifying against the benchmark schema"
for mapping in "${MAPPINGS[@]}"; do
  target="${mapping##*:}"
  count=$(mysql_cmd -N -B -e \
    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='${target}';" \
    2>/dev/null || echo 0)
  printf '[beaver]   %-24s %s tables\n' "$target" "$count"
done

cat <<'NOTE'
[beaver] done.

[beaver] Three databases referenced by the benchmark have no published dump:
[beaver]   keystone (8 questions), csail_stata_glance (5), csail_stata_cinder (2)
[beaver] Those 15 questions will fail with an unknown-database error until the
[beaver] dumps exist. The other 194 are ready.

[beaver] Point the toolkit at the server with, for example:
[beaver]   export MYSQL_CONNECTION_STRING="mysql://readonly:PASS@127.0.0.1:3306/dw"
[beaver] db_id is substituted into that connection string per record, so the
[beaver] database in the URL is only a default.
NOTE
