#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

from __future__ import annotations

import sqlite3


def apply_pending_migrations(conn: sqlite3.Connection) -> None:
    """Apply incremental schema migrations for existing databases."""
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'"
    ).fetchone()
    if row is None:
        return

    if not _has_migration(conn, 3):
        _migrate_v3_pipeline_jobs(conn)
        conn.execute(
            """
            INSERT OR IGNORE INTO schema_migrations (version, description)
            VALUES (3, 'Pipeline job types: inference, execution, eval, llm_judge')
            """
        )
        conn.commit()


def _has_migration(conn: sqlite3.Connection, version: int) -> bool:
    row = conn.execute(
        "SELECT 1 FROM schema_migrations WHERE version = ?",
        (version,),
    ).fetchone()
    return row is not None


def _migrate_v3_pipeline_jobs(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        DROP TABLE IF EXISTS jobs;
        CREATE TABLE jobs (
            id            TEXT PRIMARY KEY,
            job_type      TEXT NOT NULL
                CHECK (job_type IN ('inference', 'execution', 'eval', 'llm_judge')),
            status        TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled')),
            benchmark_id  TEXT REFERENCES benchmarks(benchmark_id),
            result_set_id INTEGER REFERENCES result_sets(id),
            progress      REAL DEFAULT 0,
            message       TEXT,
            error         TEXT,
            params        TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(params)),
            started_at    TEXT,
            completed_at  TEXT,
            created_at    TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status, created_at);
        """
    )
