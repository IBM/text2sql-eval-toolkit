#!/bin/bash
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
# Runs once, on first initialisation of the MySQL volume.
#
# Creates a SELECT-only user and grants it the databases named in
# MYSQL_READONLY_DATABASES. A benchmark may spread its questions across several
# databases and select between them by swapping the database in the connection
# string, so the grant has to name each of them.
#
# Names are listed individually rather than matched by prefix: a prefix grant
# once matched nothing, because the databases a benchmark loads need not share
# one, and the read-only user could not read a single table. The list is not
# kept in this repository -- which databases a gated benchmark uses is part of
# what its licence keeps unpublished -- so it comes from the environment.
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

DATABASES="$(echo "${MYSQL_READONLY_DATABASES:-}" | tr ',' ' ')"
if [ -z "${DATABASES// /}" ]; then
  # Same reasoning: a read-only user that can read nothing looks like a working
  # setup until the first query, long after this message is gone.
  echo "[init] MYSQL_READONLY_DATABASES is unset. List the databases the" >&2
  echo "[init] read-only user may read in deploy/.env and recreate this volume." >&2
  exit 1
fi

# Both values are interpolated into SQL below, so both are made safe first
# rather than trusted. A password is a string literal: a single quote would end
# it and a backslash escapes whatever follows, so each is doubled. A database
# name is an identifier, and there is no escaping to do -- a name outside the
# characters MySQL allows unquoted is refused, because a backtick in one would
# end the quoted identifier and the rest would be parsed as SQL.
escaped_password=$(
  printf '%s' "${MYSQL_READONLY_PASSWORD}" | sed -e 's/\\/\\\\/g' -e "s/'/''/g"
)

for db in $DATABASES; do
  case "$db" in
    *[!A-Za-z0-9_$]*)
      echo "[init] MYSQL_READONLY_DATABASES names a database that is not a" >&2
      echo "[init] plain identifier: '${db}'. Letters, digits, underscore and" >&2
      echo "[init] \$ only; refusing rather than emitting SQL it would break." >&2
      exit 1
      ;;
  esac
done

{
  echo "CREATE USER IF NOT EXISTS 'readonly'@'%' IDENTIFIED BY '${escaped_password}';"
  for db in $DATABASES; do
    # SELECT only, and named individually. Never *.*, which would include
    # mysql, performance_schema and sys.
    echo "GRANT SELECT ON \`${db}\`.* TO 'readonly'@'%';"
  done
  echo "FLUSH PRIVILEGES;"
} | mysql --protocol=socket -uroot -p"${MYSQL_ROOT_PASSWORD}"

echo "[init] read-only MySQL user granted SELECT on: ${DATABASES}"
echo "[init] These grants name each database individually -- there is no prefix"
echo "[init] grant -- so a database outside the list needs its own GRANT, added"
echo "[init] to MYSQL_READONLY_DATABASES before the volume is initialised or by"
echo "[init] hand afterwards."
