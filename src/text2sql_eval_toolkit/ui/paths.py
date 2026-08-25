#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Where the dashboard reads its data from, and what it says when the data is not
there.

The 404 messages live here rather than at their call sites because they are
tier-dependent: a local operator gets the exact path and the command that fixes
it, and a shared deployment gets neither, since a visitor can act on none of it
and it discloses the server's filesystem layout. Two call sites writing that
rule independently is how one of them ends up leaking.
"""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from text2sql_eval_toolkit.logging import get_logger
from text2sql_eval_toolkit.ui.capabilities import Tier
from text2sql_eval_toolkit.ui.runtime import get_mode

logger = get_logger(__name__)


def get_data_root() -> Path:
    """
    Resolve the data root directory.

    Priority:
    1. TEXT2SQL_DATA_ROOT env var
    2. ./data relative to current working directory
    """
    env_root = os.getenv("TEXT2SQL_DATA_ROOT")
    if env_root:
        return Path(env_root).expanduser().resolve()
    return Path.cwd() / "data"


def get_results_dir() -> Path:
    """
    Directory that contains *-predictions_eval_summary.json and *-predictions_eval.json.
    """
    return get_data_root() / "results"


def _eval_not_found_detail(benchmark_id: str) -> str:
    """
    404 detail for a missing predictions_eval.json.

    Local operators want the exact path and the command that fixes it. A public
    visitor can act on none of that, and it discloses the server's filesystem
    layout and data root, so the shared deployment says only that the benchmark
    is unavailable.
    """
    if get_mode() is not Tier.FULL:
        return (
            f"No evaluation results are available for '{benchmark_id}' on this "
            "server."
        )
    rel = f"data/results/{benchmark_id}-predictions_eval.json"
    return (
        f"Evaluation results file not found: {rel}. "
        "Download pre-computed results with: "
        "`text2sql-eval-toolkit results fetch` "
        "(or `text2sql-eval-toolkit results fetch "
        f"--benchmarks {benchmark_id}` for this benchmark only). "
        "Alternatively, generate the file locally by running the evaluation pipeline "
        "(e.g. `uv run python scripts/evaluation/run_evaluation.py`), "
        "or set TEXT2SQL_DATA_ROOT to a directory that already contains this file."
    )


def _summary_not_found_detail(benchmark_id: str) -> str:
    """404 detail for a missing summary file. See _eval_not_found_detail."""
    if get_mode() is not Tier.FULL:
        return f"No summary is available for '{benchmark_id}' on this server."
    rel = f"data/results/{benchmark_id}-predictions_eval_summary.json"
    return (
        f"Summary file not found: {rel}. "
        "Download pre-computed results with: "
        "`text2sql-eval-toolkit results fetch`, "
        "or generate locally by running the evaluation pipeline."
    )


def load_json(path: Path) -> Any:
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


# Record counts keyed by (path, size, mtime_ns).  The landing page asks for every
# benchmark's count on each request, and these files are megabytes each; the
# fingerprint means an edited file is recounted while an unchanged one is not.
_RECORD_COUNT_CACHE: Dict[Tuple[str, int, int], int] = {}
_RECORD_COUNT_LOCK = threading.Lock()


def _count_records_uncached(data_path: Any) -> int:
    # importlib.resources Traversable paths expose open()
    if hasattr(data_path, "open") and not isinstance(data_path, Path):
        with data_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
            return len(data) if isinstance(data, list) else 0

    p = Path(str(data_path))
    if not p.exists():
        return 0
    with p.open("r", encoding="utf-8") as f:
        data = json.load(f)
        return len(data) if isinstance(data, list) else 0


def count_records(data_path: Any) -> int:
    """
    Count benchmark records from either a pathlib path or an
    importlib.resources traversable path-like object.

    Cached on the file's size and mtime so repeated listing requests do not
    re-parse every benchmark data file.
    """
    if data_path is None:
        return 0

    key: Optional[Tuple[str, int, int]] = None
    try:
        p = Path(str(data_path))
        st = p.stat()
        key = (str(p), st.st_size, st.st_mtime_ns)
    except (OSError, TypeError, ValueError):
        # Traversable resources may not expose stat(); fall through uncached.
        key = None

    if key is not None:
        with _RECORD_COUNT_LOCK:
            hit = _RECORD_COUNT_CACHE.get(key)
        if hit is not None:
            return hit

    count = _count_records_uncached(data_path)

    if key is not None:
        with _RECORD_COUNT_LOCK:
            _RECORD_COUNT_CACHE[key] = count
    return count
