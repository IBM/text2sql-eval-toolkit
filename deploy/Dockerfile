# syntax=docker/dockerfile:1
#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
# Two stages: Node builds the dashboard, Python runs it.
#
# The frontend is built from source here rather than copied from the committed
# dashboard/dist. That directory is checked in so `pip install` users get a UI
# without Node, but trusting it in the image would let the deployed site drift
# silently from dashboard/src.

# --- Stage 1: build the dashboard -------------------------------------------
FROM node:22-bookworm-slim AS frontend

WORKDIR /build
# Copy manifests first so dependency installation is cached independently of
# source edits.
COPY dashboard/package.json dashboard/package-lock.json ./
RUN npm ci

COPY dashboard/ ./
RUN npm run build


# --- Stage 2: runtime --------------------------------------------------------
# Pin the exact interpreter, not `3.13` or `3`: a base-image refresh must not
# change the Python under a running deployment.
FROM python:3.13.15-slim-bookworm AS runtime

# Pinning by digest is stronger still. Resolve with:
#   docker buildx imagetools inspect python:3.13.15-slim-bookworm
# and replace the FROM line before deploying.

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    TEXT2SQL_DATA_ROOT=/data

# libpq for psycopg2, and curl for the healthcheck.
RUN apt-get update \
    && apt-get install --no-install-recommends -y libpq5 curl \
    && rm -rf /var/lib/apt/lists/*

# Unprivileged, and owning only what it needs to write.
RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin app

WORKDIR /app

# Dependencies before source, so a code change does not reinstall the world.
COPY pyproject.toml README.md ./
COPY src/ ./src/
RUN pip install --no-cache-dir ".[dashboard]"

# The build output from stage 1 -- never the committed dist.
COPY --from=frontend /build/dist ./dashboard/dist

# Provisioning runs inside this image, so the script has to be in it. Installed
# on PATH as a command rather than left at deploy/provision.sh: the entrypoint
# has to be overridden to run it either way, and a bare path invites
# `docker compose run app deploy/provision.sh`, which appends the path to the
# dashboard's argv instead of executing it.
COPY deploy/provision.sh /usr/local/bin/text2sql-provision
RUN chmod +x /usr/local/bin/text2sql-provision

RUN mkdir -p /data && chown -R app:app /data /app
USER app

EXPOSE 8000

# The app is behind Caddy, so this checks the app itself rather than the edge.
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8000/api/me || exit 1

# --no-watch-dashboard: never invoke npm at runtime. --mode comes from the
# environment so the image itself carries no capability decision.
ENTRYPOINT ["text2sql-eval-dashboard", "--host", "0.0.0.0", "--port", "8000", \
            "--no-watch-dashboard"]
