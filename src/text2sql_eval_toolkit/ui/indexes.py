#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Cached query indices over the evaluation artifacts.

Handles are cached and reused; a source file that changed on disk invalidates
its index, so a re-run is picked up without a restart. Building is expensive and
every GET is public tier, so builds are serialised per benchmark and refused
outright outside local mode -- provisioning owns that (deploy/provision.sh).
"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import Dict, Optional

from fastapi import HTTPException

from text2sql_eval_toolkit.indexing import is_stale as is_index_stale
from text2sql_eval_toolkit.indexing.store import EvalIndex
from text2sql_eval_toolkit.logging import get_logger
from text2sql_eval_toolkit.ui.capabilities import Tier
from text2sql_eval_toolkit.ui.paths import _eval_not_found_detail, get_results_dir
from text2sql_eval_toolkit.ui.runtime import get_mode

logger = get_logger(__name__)


# Open index handles, keyed by benchmark.  These hold a read-only SQLite
# connection and a path, not parsed records, so this map stays small no matter how
# large the artifacts are.
EVAL_INDEX_CACHE: Dict[str, EvalIndex] = {}
EVAL_INDEX_LOCK = threading.Lock()
# Per-benchmark build locks, so a burst of requests for an unbuilt index results
# in one build rather than one per request.
_INDEX_BUILD_LOCKS: Dict[str, threading.Lock] = {}


def get_index(benchmark_id: str) -> EvalIndex:
    """
    Return the query index for a benchmark, building it if missing or stale.

    Replaces parsing the whole evaluation artifact per request.  Handles are
    cached and reused; a source file that changed on disk invalidates its index,
    so a re-run is picked up without restarting the server.
    """
    eval_path = get_results_dir() / f"{benchmark_id}-predictions_eval.json"
    if not eval_path.exists():
        raise HTTPException(
            status_code=404, detail=_eval_not_found_detail(benchmark_id)
        )

    with EVAL_INDEX_LOCK:
        cached = EVAL_INDEX_CACHE.get(benchmark_id)
        if cached is not None:
            if not is_index_stale(eval_path):
                return cached
            cached.close()
            EVAL_INDEX_CACHE.pop(benchmark_id, None)

    # Building is expensive -- peak memory is driven by the largest single
    # record -- and every GET is public tier, so an unbuilt index would let
    # anonymous traffic trigger concurrent builds. Serialise per benchmark, and
    # on a shared deployment refuse to build at all: provisioning is responsible
    # for that (deploy/provision.sh).
    with _index_build_lock(benchmark_id):
        cached = EVAL_INDEX_CACHE.get(benchmark_id)
        if cached is not None and not is_index_stale(eval_path):
            return cached
        if get_mode() is not Tier.FULL and is_index_stale(eval_path):
            raise HTTPException(
                status_code=503,
                detail=(
                    "This benchmark is not ready to serve yet. Its search index "
                    "is still being prepared."
                ),
            )
        return _open_index(benchmark_id)


def _open_index(benchmark_id: str) -> EvalIndex:
    try:
        index = EvalIndex.for_benchmark(benchmark_id, get_results_dir())
    except FileNotFoundError:
        raise HTTPException(
            status_code=404, detail=_eval_not_found_detail(benchmark_id)
        ) from None
    except Exception as e:
        logger.exception("Failed to open evaluation index")
        # The exception text embeds the absolute index path, which would
        # disclose the data root -- the same leak the 404 messages were made
        # tier-dependent to avoid.
        detail = (
            f"Failed to open evaluation index: {e}"
            if get_mode() is Tier.FULL
            else "This benchmark is temporarily unavailable."
        )
        raise HTTPException(status_code=500, detail=detail) from e

    with EVAL_INDEX_LOCK:
        EVAL_INDEX_CACHE[benchmark_id] = index
    return index


@contextmanager
def _index_build_lock(benchmark_id: str):
    """One build at a time per benchmark, so concurrent requests wait rather
    than each starting their own."""
    with EVAL_INDEX_LOCK:
        lock = _INDEX_BUILD_LOCKS.setdefault(benchmark_id, threading.Lock())
    with lock:
        yield


def invalidate_index_cache(benchmark_id: Optional[str] = None) -> None:
    """Drop cached index handles, e.g. after an evaluation run rewrites results."""
    with EVAL_INDEX_LOCK:
        keys = [benchmark_id] if benchmark_id else list(EVAL_INDEX_CACHE)
        for key in keys:
            handle = EVAL_INDEX_CACHE.pop(key, None)
            if handle is not None:
                handle.close()
