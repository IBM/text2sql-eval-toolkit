#!/bin/bash
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
# Runs once, on first initialisation of the MySQL volume.
#
# Beaver uses six databases and selects between them by swapping the database
# in the connection string, so the grant has to span them. It is scoped by
# prefix rather than *.* so the server's own schemas stay unreachable.
set -euo pipefail

if [ -z "${MYSQL_READONLY_PASSWORD:-}" ]; then
  echo "[init] MYSQL_READONLY_PASSWORD is unset; skipping read-only user." >&2
  exit 0
fi

PREFIX="${BEAVER_DB_PREFIX:-beaver}"

mysql --protocol=socket -uroot -p"${MYSQL_ROOT_PASSWORD}" <<-SQL
	CREATE USER IF NOT EXISTS 'readonly'@'%'
	    IDENTIFIED BY '${MYSQL_READONLY_PASSWORD}';

	-- SELECT only, and only on the benchmark databases. Never *.*, which would
	-- include mysql, performance_schema and sys.
	GRANT SELECT ON \`${PREFIX}%\`.* TO 'readonly'@'%';

	FLUSH PRIVILEGES;
SQL

echo "[init] read-only MySQL user created for ${PREFIX}* databases."
echo "[init] NOTE: run deploy/load-beaver.sh once the Beaver dump is available;"
echo "[init] databases created afterwards are covered by the prefix grant."
