#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Whole-benchmark re-evaluation, and polling the background jobs it starts.

``full`` tier: it spends LLM budget and rewrites the shared artifacts every
visitor reads.
"""

import threading
import uuid

from fastapi import APIRouter, HTTPException

import text2sql_eval_toolkit.env_loader  # noqa: F401 — load .env (WATSONX_*, etc.) before eval/inference

from text2sql_eval_toolkit.evaluation.evaluation_tools import (
    run_evaluation,
)
from text2sql_eval_toolkit.ui.models import (
    EvaluateRequest,
    JobStatus,
)

from text2sql_eval_toolkit.ui.indexes import invalidate_index_cache
from text2sql_eval_toolkit.logging import get_logger
from text2sql_eval_toolkit.ui.jobs import JOBS, JOBS_LOCK, update_job

logger = get_logger(__name__)

router = APIRouter()


@router.post("/api/benchmarks/{benchmark_id}/evaluate", response_model=JobStatus)
def evaluate_benchmark(benchmark_id: str, req: EvaluateRequest):
    """
    Trigger an evaluation run for a benchmark.
    The evaluation runs in a background thread; this endpoint returns a job id.
    """
    job_id = str(uuid.uuid4())
    job = JobStatus(
        job_id=job_id,
        benchmark_id=benchmark_id,
        status="queued",
        error=None,
    )
    update_job(job)

    def worker():
        job.status = "running"
        update_job(job)
        try:
            run_evaluation(
                benchmark_id,
                use_llm=req.use_llm,
                llm_judge_config_path=req.llm_judge_config_path,
                force_rerun_llm_judge=req.force_rerun_llm_judge or req.force_rerun,
                force_rerun=req.force_rerun,
            )
            # The run rewrote the artifact; drop the cached index handle so the
            # next request rebuilds against the new bytes.
            invalidate_index_cache(benchmark_id)
            job.status = "completed"
            job.error = None
        except Exception as e:  # pragma: no cover - defensive
            logger.exception("Evaluation job failed")
            job.status = "failed"
            job.error = repr(e)
        finally:
            update_job(job)

    threading.Thread(target=worker, daemon=True).start()
    return job


@router.get("/api/jobs/{job_id}", response_model=JobStatus)
def get_job_status(job_id: str) -> JobStatus:
    with JOBS_LOCK:
        job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job
