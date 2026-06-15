#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Union
from urllib.parse import unquote, urlparse

DEFAULT_DATABASE_PATH = Path("data/text2sql_eval.db")
DEFAULT_SCHEMA_PATH = Path("data/database-schema/schema.sql")


def resolve_database_path(
    database_url: str | None = None,
    *,
    repo_root: Path | None = None,
) -> Path:
    """
    Resolve a SQLite database file path from ``TEXT2SQL_DATABASE_URL`` or a default.

    Supports ``sqlite:///relative/path.db`` and ``sqlite:////absolute/path.db``.
    """
    url = database_url or os.getenv("TEXT2SQL_DATABASE_URL")
    if not url:
        root = repo_root or _find_repo_root()
        return (root / DEFAULT_DATABASE_PATH).resolve()

    parsed = urlparse(url)
    if parsed.scheme not in {"sqlite", "sqlite3"}:
        raise ValueError(
            f"Unsupported database URL scheme {parsed.scheme!r}; "
            "only sqlite:/// paths are supported by the migration script."
        )

    raw_path = unquote(parsed.path or "")
    if raw_path.startswith("//"):
        return Path(raw_path[1:]).resolve()
    if raw_path.startswith("/") and len(raw_path) > 2 and raw_path[2] == ":":
        # sqlite:///C:/path on Windows-style URLs
        return Path(raw_path[1:]).resolve()
    if raw_path.startswith("/"):
        return Path(raw_path).resolve()

    root = repo_root or _find_repo_root()
    return (root / raw_path.lstrip("/")).resolve()


def resolve_schema_path(repo_root: Path | None = None) -> Path:
    root = repo_root or _find_repo_root()
    return (root / DEFAULT_SCHEMA_PATH).resolve()


def _find_repo_root() -> Path:
    cwd = Path.cwd().resolve()
    for candidate in [cwd, *cwd.parents]:
        if (candidate / "pyproject.toml").is_file() and (candidate / "data").is_dir():
            return candidate
    return cwd


def connect(
    database_path: Union[str, Path, None] = None,
    *,
    database_url: str | None = None,
) -> sqlite3.Connection:
    path = Path(database_path) if database_path is not None else resolve_database_path(database_url)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 60000")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


def apply_schema(conn: sqlite3.Connection, schema_path: Path) -> None:
    if not schema_path.is_file():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
    sql = schema_path.read_text(encoding="utf-8")
    conn.executescript(sql)
