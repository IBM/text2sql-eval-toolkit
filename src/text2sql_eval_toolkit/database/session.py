#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

from __future__ import annotations

import sqlite3
import threading
import time
from contextlib import contextmanager
from functools import wraps
from typing import Callable, Iterator, TypeVar

from text2sql_eval_toolkit.database.connection import (
    apply_schema,
    connect,
    resolve_database_path,
    resolve_schema_path,
)
from text2sql_eval_toolkit.database.migrations import apply_pending_migrations

T = TypeVar("T")

_local = threading.local()
_schema_initialized = False
_schema_lock = threading.Lock()

DEFAULT_BUSY_TIMEOUT_MS = 60_000
MAX_DB_RETRIES = 8
RETRY_BACKOFF_SEC = 0.05


def _configure_connection(conn: sqlite3.Connection) -> None:
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute(f"PRAGMA busy_timeout = {DEFAULT_BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA synchronous = NORMAL")


def ensure_schema() -> None:
    global _schema_initialized
    if _schema_initialized:
        return
    with _schema_lock:
        if _schema_initialized:
            return
        db_path = resolve_database_path()
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(db_path)
        try:
            _configure_connection(conn)
            row = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'"
            ).fetchone()
            if row is None:
                apply_schema(conn, resolve_schema_path())
            apply_pending_migrations(conn)
            conn.commit()
        finally:
            conn.close()
        _schema_initialized = True


def get_connection() -> sqlite3.Connection:
    """Return a thread-local SQLite connection (safe for concurrent workers)."""
    ensure_schema()
    conn = getattr(_local, "connection", None)
    if conn is None:
        conn = connect(resolve_database_path())
        _configure_connection(conn)
        _local.connection = conn
    return conn


@contextmanager
def transaction(*, immediate: bool = False) -> Iterator[sqlite3.Connection]:
    """Context manager that commits on success and rolls back on error."""
    conn = get_connection()
    begin = "BEGIN IMMEDIATE" if immediate else "BEGIN"
    conn.execute(begin)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def retry_on_locked(func: Callable[..., T]) -> Callable[..., T]:
    @wraps(func)
    def wrapper(*args, **kwargs) -> T:
        delay = RETRY_BACKOFF_SEC
        for attempt in range(MAX_DB_RETRIES):
            try:
                return func(*args, **kwargs)
            except sqlite3.OperationalError as exc:
                message = str(exc).lower()
                if "locked" not in message and "busy" not in message:
                    raise
                if attempt == MAX_DB_RETRIES - 1:
                    raise
                time.sleep(delay)
                delay = min(delay * 2, 2.0)
        raise RuntimeError("unreachable")

    return wrapper


def close_thread_connection() -> None:
    conn = getattr(_local, "connection", None)
    if conn is not None:
        conn.close()
        _local.connection = None
