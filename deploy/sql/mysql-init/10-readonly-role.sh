#!/bin/bash
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
# Runs once, on first initialisation of the MySQL volume.
#
# Beaver spreads its questions across six databases and selects between them by
# swapping the database in the connection string, so the grant has to name all
# of them.
#
# They are named individually rather than by prefix. An earlier version granted
# on `beaver%`.*, which matched nothing: load-beaver.sh creates `dw`,
# `csail_stata_neutron` and `csail_stata_nova`, none of which start with
# "beaver". The read-only user could not read a single table.
#
# MySQL grants privileges by name, so granting on a database that does not exist
# yet is fine -- which is what lets this run at first init, before any data has
# been loaded.

set -euo pipefail

if [ -z "${MYSQL_READONLY_PASSWORD:-}" ]; then
  # Refuse rather than skip. Skipping leaves a server whose only account is
  # root, so the app would have to connect as superuser -- and the whole point
  # of this file is that a write is refused by the database rather than by
  # application logic. Initialisation happens once, so a message on stderr would
  # be gone by the time anyone looked.
  echo "[init] MYSQL_READONLY_PASSWORD is unset. Set it in deploy/.env and" >&2
  echo "[init] recreate this volume; refusing to initialise with root only." >&2
  exit 1
fi

# The six databases the Beaver benchmark refers to. Three have published dumps
# today; the other three are granted anyway so that loading them later needs no
# privilege change.
BEAVER_DATABASES="${BEAVER_DATABASES:-dw csail_stata_neutron csail_stata_nova keystone csail_stata_glance csail_stata_cinder}"

{
  echo "CREATE USER IF NOT EXISTS 'readonly'@'%' IDENTIFIED BY '${MYSQL_READONLY_PASSWORD}';"
  for db in $BEAVER_DATABASES; do
    # SELECT only, and named individually. Never *.*, which would include
    # mysql, performance_schema and sys.
    echo "GRANT SELECT ON \`${db}\`.* TO 'readonly'@'%';"
  done
  echo "FLUSH PRIVILEGES;"
} | mysql --protocol=socket -uroot -p"${MYSQL_ROOT_PASSWORD}"

echo "[init] read-only MySQL user granted SELECT on: ${BEAVER_DATABASES}"
echo "[init] NOTE: run deploy/load-beaver.sh once the Beaver dump is available."
echo "[init] These grants name each database individually -- there is no prefix"
echo "[init] grant -- so a database outside the list above needs its own GRANT,"
echo "[init] added here before the volume is initialised or by hand afterwards."
