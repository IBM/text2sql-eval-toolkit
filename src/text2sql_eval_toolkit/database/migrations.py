#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

_MIGRATION_V4_DESCRIPTION = (
    "Decouple pipelines from result_sets — pipeline_id is now globally unique"
)


def apply_pending_migrations(conn: sqlite3.Connection) -> None:
    """Apply incremental schema migrations for existing databases."""
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'"
    ).fetchone()
    if row is None:
        return

    changed = False

    if not _has_migration(conn, 3):
        _migrate_v3_pipeline_jobs(conn)
        conn.execute(
            """
            INSERT OR IGNORE INTO schema_migrations (version, description)
            VALUES (3, 'Pipeline job types: inference, execution, eval, llm_judge')
            """
        )
        changed = True

    if not _has_migration(conn, 4):
        if _pipelines_has_result_set_id(conn):
            _migrate_v4_decouple_pipelines(conn)
        conn.execute(
            """
            INSERT OR IGNORE INTO schema_migrations (version, description)
            VALUES (4, ?)
            """,
            (_MIGRATION_V4_DESCRIPTION,),
        )
        changed = True

    if changed:
        conn.commit()


def _has_migration(conn: sqlite3.Connection, version: int) -> bool:
    row = conn.execute(
        "SELECT 1 FROM schema_migrations WHERE version = ?",
        (version,),
    ).fetchone()
    return row is not None


def _pipelines_has_result_set_id(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'pipelines'"
    ).fetchone()
    if row is None:
        return False
    columns = conn.execute("PRAGMA table_info(pipelines)").fetchall()
    return any(col[1] == "result_set_id" for col in columns)


def _migration_sql_path(filename: str) -> Path:
    return Path(__file__).resolve().parents[3] / "scripts" / "migration" / filename


def _load_migration_sql(filename: str) -> str:
    path = _migration_sql_path(filename)
    if not path.is_file():
        raise FileNotFoundError(f"Migration SQL not found: {path}")
    sql = path.read_text(encoding="utf-8")
    # Version row is recorded in apply_pending_migrations().
    return re.sub(
        r"INSERT INTO schema_migrations\s*\([^;]+\);",
        "",
        sql,
        flags=re.DOTALL | re.IGNORECASE,
    )


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


def _migrate_v4_decouple_pipelines(conn: sqlite3.Connection) -> None:
    conn.executescript(_load_migration_sql("004_decouple_pipelines_from_result_sets.sql"))
