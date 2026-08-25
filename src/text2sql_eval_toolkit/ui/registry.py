#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Reading and writing ``benchmarks.json``.

The registry is the one file the dashboard mutates that is also shipped inside
the package, so writes go through ``write_json_atomic`` and ids and configs are
normalised before they reach disk. ``CLAUDE.md`` records the rule that matters
around it: the checkout copy is canonical and the packaged copy is generated
from it.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Dict

from fastapi import HTTPException

from text2sql_eval_toolkit.logging import get_logger
from text2sql_eval_toolkit.ui.paths import get_data_root
from text2sql_eval_toolkit.utils import get_benchmarks_file_path

logger = get_logger(__name__)


ALLOWED_DB_TYPES = {"sqlite", "postgres", "mysql", "db2", "presto"}
ALLOWED_LOGO_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp", ".gif", ".svg"}

# The only directory /api/static serves. Restricting to it keeps the rest of
# the data root -- results, indices, and the judge spend store -- unreadable,
# since that route is a GET and therefore runs at the public tier.
STATIC_ASSET_SUBDIR = Path("benchmarks") / "logos"
MAX_LOGO_UPLOAD_BYTES = 5 * 1024 * 1024  # 5 MB


def get_benchmark_registry_path() -> Path:
    """
    Resolve the benchmark registry path used for dashboard CRUD.
    """
    path = get_benchmarks_file_path(is_test=False)
    if path.exists():
        return path

    fallback = (get_data_root() / "benchmarks.json").resolve()
    if fallback.parent.exists():
        return fallback
    return path


def load_benchmark_registry(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to read benchmark registry: {e}"
        ) from e
    if not isinstance(data, dict):
        raise HTTPException(status_code=500, detail="Invalid benchmark registry format")
    return data


def write_json_atomic(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.tmp")
    try:
        with temp_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
            f.write("\n")
        os.replace(temp_path, path)
    except Exception as e:
        try:
            if temp_path.exists():
                temp_path.unlink()
        except Exception:
            pass
        raise HTTPException(
            status_code=500, detail=f"Failed to write benchmark registry: {e}"
        ) from e


def normalize_benchmark_id(raw: str) -> str:
    benchmark_id = (raw or "").strip()
    if not benchmark_id:
        raise HTTPException(status_code=400, detail="benchmark_id is required")
    if not re.fullmatch(r"[A-Za-z0-9_-]+", benchmark_id):
        raise HTTPException(
            status_code=400,
            detail="benchmark_id must only contain letters, numbers, underscore, and dash",
        )
    return benchmark_id


def normalize_benchmark_config(benchmark_id: str, payload: Any) -> Dict[str, Any]:
    name = (payload.name or "").strip() or benchmark_id
    description = (payload.description or "").strip()
    data = (payload.data or "").strip()
    schema = (payload.schema_path or "").strip()
    predictions = (payload.predictions or "").strip()
    db_engine = payload.db_engine or {}

    if not data:
        raise HTTPException(status_code=400, detail="data is required")
    if not schema:
        raise HTTPException(status_code=400, detail="schema is required")
    if not predictions:
        raise HTTPException(status_code=400, detail="predictions is required")
    if not isinstance(db_engine, dict):
        raise HTTPException(status_code=400, detail="db_engine must be an object")

    db_type = str(db_engine.get("db_type") or "").strip().lower()
    if db_type not in ALLOWED_DB_TYPES:
        raise HTTPException(
            status_code=400,
            detail=(
                f"db_engine.db_type must be one of "
                f"{', '.join(sorted(ALLOWED_DB_TYPES))}"
            ),
        )

    normalized_engine = dict(db_engine)
    normalized_engine["db_type"] = db_type
    if db_type == "sqlite":
        db_folder = str(normalized_engine.get("db_folder") or "").strip()
        if not db_folder:
            raise HTTPException(
                status_code=400, detail="db_engine.db_folder is required for sqlite"
            )
        normalized_engine["db_folder"] = db_folder
    elif db_type in {"postgres", "mysql", "db2", "presto"}:
        env_var = str(normalized_engine.get("connection_string_env_var") or "").strip()
        if not env_var:
            raise HTTPException(
                status_code=400,
                detail=(
                    "db_engine.connection_string_env_var is required " f"for {db_type}"
                ),
            )
        normalized_engine["connection_string_env_var"] = env_var

    config: Dict[str, Any] = {
        "name": name,
        "description": description,
        "data": data,
        "schema": schema,
        "predictions": predictions,
        "db_engine": normalized_engine,
    }
    # Backward compatibility: accept legacy logo_url but store only logo filename.
    raw_logo = (getattr(payload, "logo", None) or "").strip()
    raw_logo_url = (getattr(payload, "logo_url", None) or "").strip()
    logo = ""
    if raw_logo:
        logo = Path(raw_logo).name
    elif raw_logo_url:
        logo = Path(raw_logo_url.split("?", 1)[0]).name
    if logo:
        config["logo"] = logo
    return config
