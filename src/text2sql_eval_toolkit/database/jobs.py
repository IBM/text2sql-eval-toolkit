#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

JOB_TYPES = frozenset({"inference", "execution", "eval", "llm_judge"})


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def resolve_eval_job_type(*, use_llm_judge: bool) -> str:
    return "llm_judge" if use_llm_judge else "eval"


def create_pending_job(
    conn: sqlite3.Connection,
    job_type: str,
    benchmark_id: str,
    *,
    job_id: str | None = None,
    params: dict[str, Any] | None = None,
) -> str:
    """Insert a pending job row (for async endpoints that return a job id immediately)."""
    if job_type not in JOB_TYPES:
        raise ValueError(f"Unsupported job_type {job_type!r}; expected one of {sorted(JOB_TYPES)}")
    job_id = job_id or str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO jobs (id, job_type, status, benchmark_id, params)
        VALUES (?, ?, 'pending', ?, ?)
        """,
        (
            job_id,
            job_type,
            benchmark_id,
            json.dumps(params or {}, ensure_ascii=False),
        ),
    )
    conn.commit()
    return job_id


@contextmanager
def track_job(
    conn: sqlite3.Connection,
    job_type: str,
    benchmark_id: str,
    *,
    job_id: str | None = None,
    params: dict[str, Any] | None = None,
    result_set_id: int | None = None,
) -> Iterator[str]:
    """Create or resume a jobs row, mark it running, then complete or fail on exit."""
    if job_type not in JOB_TYPES:
        raise ValueError(f"Unsupported job_type {job_type!r}; expected one of {sorted(JOB_TYPES)}")

    job_id = job_id or str(uuid.uuid4())
    existing = get_job(conn, job_id)
    if existing is None:
        conn.execute(
            """
            INSERT INTO jobs (
                id, job_type, status, benchmark_id, result_set_id, params, started_at
            ) VALUES (?, ?, 'running', ?, ?, ?, ?)
            """,
            (
                job_id,
                job_type,
                benchmark_id,
                result_set_id,
                json.dumps(params or {}, ensure_ascii=False),
                _utc_now(),
            ),
        )
    else:
        conn.execute(
            """
            UPDATE jobs
            SET status = 'running', started_at = COALESCE(started_at, ?), params = ?
            WHERE id = ?
            """,
            (
                _utc_now(),
                json.dumps(params or existing.get("params") or {}, ensure_ascii=False),
                job_id,
            ),
        )
    conn.commit()
    try:
        yield job_id
        conn.execute(
            """
            UPDATE jobs
            SET status = 'completed', progress = 1.0, completed_at = ?
            WHERE id = ?
            """,
            (_utc_now(), job_id),
        )
        conn.commit()
    except Exception as exc:
        conn.execute(
            """
            UPDATE jobs
            SET status = 'failed', error = ?, completed_at = ?
            WHERE id = ?
            """,
            (repr(exc), _utc_now(), job_id),
        )
        conn.commit()
        raise


def update_job_progress(
    conn: sqlite3.Connection,
    job_id: str,
    *,
    progress: float | None = None,
    message: str | None = None,
) -> None:
    updates: list[str] = []
    values: list[Any] = []
    if progress is not None:
        updates.append("progress = ?")
        values.append(progress)
    if message is not None:
        updates.append("message = ?")
        values.append(message)
    if not updates:
        return
    values.append(job_id)
    conn.execute(
        f"UPDATE jobs SET {', '.join(updates)} WHERE id = ?",
        values,
    )
    conn.commit()


def get_job(conn: sqlite3.Connection, job_id: str) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    if row is None:
        return None
    return _row_to_job(row)


def list_jobs(
    conn: sqlite3.Connection,
    *,
    benchmark_id: str | None = None,
    job_type: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if benchmark_id is not None:
        clauses.append("benchmark_id = ?")
        params.append(benchmark_id)
    if job_type is not None:
        clauses.append("job_type = ?")
        params.append(job_type)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(limit)
    rows = conn.execute(
        f"""
        SELECT * FROM jobs
        {where}
        ORDER BY created_at DESC
        LIMIT ?
        """,
        params,
    ).fetchall()
    return [_row_to_job(row) for row in rows]


def _row_to_job(row: sqlite3.Row) -> dict[str, Any]:
    params_raw = row["params"] or "{}"
    try:
        params = json.loads(params_raw)
    except json.JSONDecodeError:
        params = {}
    return {
        "job_id": row["id"],
        "job_type": row["job_type"],
        "benchmark_id": row["benchmark_id"],
        "result_set_id": row["result_set_id"],
        "status": row["status"],
        "progress": row["progress"],
        "message": row["message"],
        "error": row["error"],
        "params": params,
        "started_at": row["started_at"],
        "completed_at": row["completed_at"],
        "created_at": row["created_at"],
    }
