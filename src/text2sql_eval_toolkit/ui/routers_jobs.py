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

# Runtime state (deployment ceiling, judge allowlist, request identity) and the
# middleware stack live in their own modules.  Their names are re-exported here
# because ``server.set_mode`` / ``server.reset_rate_limits`` are the surface the
# CLI and the tests already use -- and re-exporting is only safe because they are
# accessors over module state rather than the state itself.
from text2sql_eval_toolkit.ui.indexes import (  # noqa: F401
    EVAL_INDEX_CACHE,
    get_index,
    invalidate_index_cache,
)
from text2sql_eval_toolkit.ui.paths import (  # noqa: F401
    _eval_not_found_detail,
    _summary_not_found_detail,
    count_records,
    get_data_root,
    get_results_dir,
    load_json,
)
from text2sql_eval_toolkit.ui.registry import (  # noqa: F401
    ALLOWED_DB_TYPES,
    ALLOWED_LOGO_EXTENSIONS,
    MAX_LOGO_UPLOAD_BYTES,
    STATIC_ASSET_SUBDIR,
    get_benchmark_registry_path,
    load_benchmark_registry,
    normalize_benchmark_config,
    normalize_benchmark_id,
    write_json_atomic,
)
from text2sql_eval_toolkit.ui.middleware import reset_rate_limits  # noqa: F401
from text2sql_eval_toolkit.ui.runtime import (  # noqa: F401
    _cookie_secure,
    current_user_email,
    get_judge_allowlist,
    get_mode,
    set_judge_allowlist,
    set_mode,
)
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
