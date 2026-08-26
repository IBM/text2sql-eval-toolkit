#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Fetching a results snapshot from the Hugging Face Hub.

Off unless ``--enable-fetch`` was passed, because the endpoint downloads
gigabytes into the data root and a shared deployment has provisioning do that
once, ahead of serving, rather than on a request.
"""

import threading
import uuid
from typing import Any, Dict

from fastapi import APIRouter, HTTPException

import text2sql_eval_toolkit.env_loader  # noqa: F401 — load .env (WATSONX_*, etc.) before eval/inference

from text2sql_eval_toolkit.ui.models import (
    FetchJobStatus,
    ResultsFetchRequest,
)

from text2sql_eval_toolkit.ui.paths import get_data_root
from text2sql_eval_toolkit.logging import get_logger
from text2sql_eval_toolkit.ui import runtime
from text2sql_eval_toolkit.ui.capabilities import Tier
from text2sql_eval_toolkit.ui.jobs import FETCH_JOBS, FETCH_JOBS_LOCK

logger = get_logger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Results Hub endpoints (enabled only when --enable-fetch is passed)
# ---------------------------------------------------------------------------


@router.get("/api/results/status")
def get_results_status() -> Dict[str, Any]:
    """
    Report whether the fetch endpoint is enabled and whether local results exist.

    The React UI calls this on mount to decide whether to show the
    "Fetch results" banner.
    """
    data_root = get_data_root()
    results_dir = data_root / "results"
    # An evaluation artifact, not merely "something is in the directory". The
    # derived .index/ directory, logs/ and bak/ all live here, so any of them
    # left behind after the artifacts were removed would report results the
    # dashboard cannot actually serve, and suppress the banner offering to fetch.
    has_results = results_dir.is_dir() and any(
        results_dir.glob("*-predictions_eval.json")
    )
    status: Dict[str, Any] = {
        "fetch_enabled": runtime.fetch_endpoint_enabled(),
        "has_results": has_results,
    }
    # The absolute path is operator guidance -- it tells you where to put the
    # files -- and it is also filesystem layout, which a public visitor can do
    # nothing with. Shared modes redact it, as they already do for 404 detail.
    if runtime.get_mode() is Tier.FULL:
        status["results_path"] = str(results_dir)
    return status


@router.post("/api/results/fetch", response_model=FetchJobStatus)
def start_results_fetch(
    req: ResultsFetchRequest = ResultsFetchRequest(),
) -> FetchJobStatus:
    """
    Kick off a background download of results from the Hugging Face Hub.

    Only available when the dashboard is started with ``--enable-fetch``.
    Returns 404 otherwise (so that the default prod setup is unaffected).
    """
    if not runtime.fetch_endpoint_enabled():
        raise HTTPException(status_code=404, detail="Not Found")

    job_id = str(uuid.uuid4())
    job = FetchJobStatus(job_id=job_id, state="queued")
    with FETCH_JOBS_LOCK:
        FETCH_JOBS[job_id] = job

    def worker() -> None:
        job.state = "running"
        try:
            from text2sql_eval_toolkit.results import fetch_results

            fetch_results(
                benchmarks=req.benchmarks,
                pipelines=req.pipelines,
                models=req.models,
                revision=req.revision,
                data_root=get_data_root(),
                force=req.force,
                show_progress=False,
            )
            job.state = "completed"
        except Exception as exc:
            logger.exception("Results fetch job failed")
            job.state = "failed"
            job.error = str(exc)

    threading.Thread(target=worker, daemon=True).start()
    return job


@router.get("/api/results/fetch/{job_id}", response_model=FetchJobStatus)
def get_results_fetch_status(job_id: str) -> FetchJobStatus:
    """Poll the status of a fetch job started by POST /api/results/fetch."""
    if not runtime.fetch_endpoint_enabled():
        raise HTTPException(status_code=404, detail="Not Found")
    with FETCH_JOBS_LOCK:
        job = FETCH_JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Fetch job not found")
    return job
