#!/usr/bin/env bash
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
# One-time data provisioning for the public dashboard (plan item 3.7).
#
#   fetch the pinned Hugging Face snapshot -> build the query indices -> mark done
#
# Idempotent: a completed run leaves a marker and subsequent runs exit early.
# Run it BEFORE starting the app. Index building is memory-hungry -- peak is
# driven by the largest single record, and Beaver contains one of 108 MB whose
# parsed form costs several hundred more -- so doing it while the app and both
# databases are serving is how a 4 GB box runs out of memory.
#
# Usage (inside the app image, or any environment with the toolkit installed):
#   TEXT2SQL_DATA_ROOT=/data deploy/provision.sh
set -euo pipefail

DATA_ROOT="${TEXT2SQL_DATA_ROOT:-/data}"
REVISION="${TEXT2SQL_RESULTS_REVISION:-}"
MARKER="${DATA_ROOT}/.provisioned"

log() { printf '[provision] %s\n' "$*"; }
fail() { printf '[provision] ERROR: %s\n' "$*" >&2; exit 1; }

# Seed default benchmark logos. Deliberately before the marker check: an
# already-provisioned data root that predates this still has blank tiles, and
# re-running provisioning is the obvious thing to reach for. Only fills gaps --
# an uploaded logo of the same name is left alone.
DEFAULT_LOGOS="/opt/text2sql/default-logos"
if [ -d "$DEFAULT_LOGOS" ]; then
  mkdir -p "${DATA_ROOT}/benchmarks/logos"
  seeded=0
  for logo in "$DEFAULT_LOGOS"/*; do
    [ -e "$logo" ] || continue
    target="${DATA_ROOT}/benchmarks/logos/$(basename "$logo")"
    if [ ! -e "$target" ]; then
      cp "$logo" "$target"
      seeded=$((seeded + 1))
    fi
  done
  log "seeded ${seeded} default logo(s) into ${DATA_ROOT}/benchmarks/logos"

# Seed benchmark definitions and schemas, for the same reason and with the same
# rule: never overwrite. The registry is placed in the data root so that the
# `data` and `schema` paths inside it resolve there too -- relative to the
# registry file's own directory -- rather than into the installed package, which
# is read-only and replaced on every rebuild.
DEFAULT_DATA="/opt/text2sql/default-data"
if [ -d "${DEFAULT_DATA}" ]; then
  mkdir -p "${DATA_ROOT}/benchmarks"
  if [ ! -f "${DATA_ROOT}/benchmarks.json" ] && [ -f "${DEFAULT_DATA}/benchmarks.json" ]; then
    cp "${DEFAULT_DATA}/benchmarks.json" "${DATA_ROOT}/benchmarks.json"
    log "seeded the benchmark registry into ${DATA_ROOT}"
  fi
  seeded_data=0
  for f in "${DEFAULT_DATA}"/benchmarks/*.json; do
    [ -e "$f" ] || continue
    target="${DATA_ROOT}/benchmarks/$(basename "$f")"
    if [ ! -f "$target" ]; then
      cp "$f" "$target"
      seeded_data=$((seeded_data + 1))
    fi
  done
  log "seeded ${seeded_data} benchmark definition file(s)"
fi
fi

if [ -f "$MARKER" ]; then
  log "already provisioned:"
  sed 's/^/[provision]   /' "$MARKER"
  log "delete ${MARKER} to force a re-run."
  exit 0
fi

command -v text2sql-eval-toolkit >/dev/null 2>&1 \
  || fail "text2sql-eval-toolkit is not on PATH."

if [ -z "$REVISION" ]; then
  # DEFAULT_REVISION is derived from the installed toolkit version and silently
  # falls back to `main` when that tag is absent. A floating `main` means the
  # public dataset can change under shared links, which defeats citable URLs.
  fail "TEXT2SQL_RESULTS_REVISION is unset. Pin the snapshot explicitly."
fi

mkdir -p "$DATA_ROOT"

log "fetching results snapshot ${REVISION} into ${DATA_ROOT} (~4 GB)"
# A partial fetch must fail loudly here rather than yielding a site that renders
# empty benchmarks.
text2sql-eval-toolkit results fetch \
  --revision "$REVISION" \
  --data-root "$DATA_ROOT" \
  || fail "results fetch failed; not marking provisioned."

log "building query indices"
text2sql-eval-toolkit index build --data-root "$DATA_ROOT" \
  || fail "index build failed; not marking provisioned."

log "verifying every benchmark has a current index"
text2sql-eval-toolkit index status --data-root "$DATA_ROOT" | tee /tmp/index-status.txt
if grep -q 'stale' /tmp/index-status.txt; then
  fail "some indices are still stale after building."
fi

{
  echo "revision=${REVISION}"
  echo "provisioned_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "toolkit_version=$(python -c 'import text2sql_eval_toolkit as t; print(t.__version__)' 2>/dev/null || echo unknown)"
} > "$MARKER"

log "done. Marker written to ${MARKER}"
log "the revision recorded above is what the UI should show as its data stamp."
