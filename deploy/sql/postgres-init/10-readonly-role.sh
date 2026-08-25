#!/bin/bash
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
# Runs once, on first initialisation of the Postgres volume.
#
# The app connects as this role and nothing else. The execute endpoint runs
# arbitrary caller-supplied SQL, so a write must be refused by the database
# itself rather than by application logic -- application-layer checks are the
# wrong place for that guarantee.
set -euo pipefail

if [ -z "${POSTGRES_READONLY_PASSWORD:-}" ]; then
  echo "[init] POSTGRES_READONLY_PASSWORD is unset; skipping read-only role." >&2
  echo "[init] The app would then need superuser credentials. Set it and" >&2
  echo "[init] recreate the volume before exposing this deployment." >&2
  exit 0
fi

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-SQL
	CREATE ROLE readonly LOGIN PASSWORD '${POSTGRES_READONLY_PASSWORD}';

	GRANT CONNECT ON DATABASE ${POSTGRES_DB} TO readonly;
	GRANT USAGE ON SCHEMA public TO readonly;
	GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly;
	GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO readonly;

	-- Tables created later by the data load must be readable too, without
	-- anyone having to remember to re-grant.
	ALTER DEFAULT PRIVILEGES IN SCHEMA public
	    GRANT SELECT ON TABLES TO readonly;

	-- Belt and braces: even a granted write is refused, and a runaway query is
	-- bounded server-side rather than only by the API's timeout parameter.
	ALTER ROLE readonly SET default_transaction_read_only = on;
	ALTER ROLE readonly SET statement_timeout = '90s';
	ALTER ROLE readonly SET idle_in_transaction_session_timeout = '60s';

	-- No schema creation, no table creation.
	REVOKE CREATE ON SCHEMA public FROM readonly;
SQL

echo "[init] read-only Postgres role created."
