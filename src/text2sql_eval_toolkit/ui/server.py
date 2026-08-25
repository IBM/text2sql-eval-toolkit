import argparse
import base64
import os
import subprocess
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query, Request
import uvicorn

import text2sql_eval_toolkit.env_loader  # noqa: F401 — load .env (WATSONX_*, etc.) before eval/inference

from text2sql_eval_toolkit.utils import get_benchmarks_info
from text2sql_eval_toolkit.evaluation.evaluation_tools import (
    run_evaluation,
)
from text2sql_eval_toolkit import __version__
from text2sql_eval_toolkit.ui import auth
from text2sql_eval_toolkit.ui.aliases import alias_map
from text2sql_eval_toolkit.ui.models import (
    BenchmarkCategorySummaryResponse,
    BenchmarkConfigInput,
    BenchmarkConfigResponse,
    BenchmarkDetailResponse,
    BenchmarkLogoUploadRequest,
    BenchmarkSummary,
    BenchmarksResponse,
    DeploymentInfo,
    ErrorRecordSummary,
    EvaluateRequest,
    FetchJobStatus,
    JobStatus,
    PaginatedErrorResponse,
    PipelineAliasesResponse,
    PipelineMetrics,
    ResultsFetchRequest,
    SessionInfo,
)
from text2sql_eval_toolkit.ui.judge_budget import (
    judge_disabled,
)
from text2sql_eval_toolkit.ui.capabilities import (
    Tier,
    parse_allowlist,
    resolve_tier,
)

# Runtime state (deployment ceiling, judge allowlist, request identity) and the
# middleware stack live in their own modules.  Their names are re-exported here
# because ``server.set_mode`` / ``server.reset_rate_limits`` are the surface the
# CLI and the tests already use -- and re-exporting is only safe because they are
# accessors over module state rather than the state itself.
from text2sql_eval_toolkit.ui import middleware as _middleware
from text2sql_eval_toolkit.ui import (
    routers_auth,
    routers_compare,
    routers_execution,
    routers_judge,
    routers_judge_configs,
    static_files,
)
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

logger = get_logger(__name__)

app = FastAPI(title="Text2SQL Evaluation Dashboard API")

# When True the /api/results/fetch endpoints are active.  Set by main() via
# the --enable-fetch CLI flag.  Off by default so production deployments are
# safe without any configuration.
_ENABLE_FETCH_ENDPOINT: bool = False

_middleware.install(app)

# Routes are grouped by what they can do rather than by URL shape: the execution
# and judge routers are the two that carry capability beyond reading artifacts,
# and keeping each in one file is what makes that reviewable.
for _router in (
    routers_auth.router,
    routers_judge.router,
    routers_execution.router,
    routers_compare.router,
    routers_judge_configs.router,
    static_files.router,
):
    app.include_router(_router)

# Names main() and the tests reach for, kept on `server` so moving a definition
# is not also an API change for callers.
SPAStaticFiles = static_files.SPAStaticFiles
mount_static = static_files.mount_static
_resolve_dashboard_source_dir = static_files._resolve_dashboard_source_dir
_ensure_dashboard_dist = static_files._ensure_dashboard_dist
_spawn_dashboard_watch = static_files._spawn_dashboard_watch
_terminate_dashboard_watch = static_files._terminate_dashboard_watch
get_judge_store = routers_judge.get_judge_store
reset_judge_store = routers_judge.reset_judge_store
_judge_config_dir = routers_judge._judge_config_dir
get_oauth = routers_auth.get_oauth
_judge_usage_model = routers_judge._judge_usage_model


def configure_cors(mode: Tier) -> None:
    """Narrow CORS for shared deployments. See ``ui.middleware``."""
    _middleware.configure_cors(app, mode)


class CreateBenchmarkRequest(BenchmarkConfigInput):
    benchmark_id: str


class UpdateBenchmarkRequest(BenchmarkConfigInput):
    pass


JOBS: Dict[str, JobStatus] = {}
JOBS_LOCK = threading.Lock()


FETCH_JOBS: Dict[str, FetchJobStatus] = {}
FETCH_JOBS_LOCK = threading.Lock()


def _update_job(job: JobStatus) -> None:
    with JOBS_LOCK:
        JOBS[job.job_id] = job


@app.get("/api/me", response_model=SessionInfo)
def get_session_info(request: Request) -> SessionInfo:
    """
    The caller's effective capability.

    The UI uses this to hide actions that would 403, so a read-only visitor is
    not offered buttons that cannot work.
    """
    email = current_user_email(request)
    tier = resolve_tier(get_mode(), email, get_judge_allowlist())
    judge_usage = None
    if tier >= Tier.JUDGE and not judge_disabled():
        try:
            judge_usage = _judge_usage_model(get_judge_store().usage())
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Could not read judge usage: %s", exc)

    return SessionInfo(
        tier=tier.name.lower(),
        mode=get_mode().name.lower(),
        email=email,
        signed_in=bool(email),
        # Reported false when the kill switch is on, so the UI stops offering an
        # action that would 503.
        can_run_judge=tier >= Tier.JUDGE and not judge_disabled(),
        can_mutate=tier >= Tier.FULL,
        judge_usage=judge_usage,
    )


def _read_provisioning_marker() -> Dict[str, str]:
    """Parse the marker deploy/provision.sh leaves behind, if present."""
    marker = get_data_root() / ".provisioned"
    values: Dict[str, str] = {}
    try:
        for line in marker.read_text(encoding="utf-8").splitlines():
            if "=" in line:
                key, _, value = line.partition("=")
                values[key.strip()] = value.strip()
    except OSError:
        pass
    return values


@app.get("/api/deployment", response_model=DeploymentInfo)
def get_deployment_info() -> DeploymentInfo:
    """
    Describe the deployment itself, as opposed to the caller.

    Separate from /api/me because it is the same for everyone and changes only
    on redeploy, and because the UI needs it before anyone signs in.
    """
    marker = _read_provisioning_marker()
    return DeploymentInfo(
        mode=get_mode().name.lower(),
        toolkit_version=__version__,
        data_revision=marker.get("revision"),
        data_provisioned_at=marker.get("provisioned_at"),
        sign_in_available=auth.is_configured(),
        judge_available=get_mode() >= Tier.JUDGE and not judge_disabled(),
    )


@app.get("/api/benchmarks", response_model=BenchmarksResponse)
def list_benchmarks() -> BenchmarksResponse:
    """
    List benchmarks with basic metadata and counts.
    """
    benchmarks_info = get_benchmarks_info(is_test=False)
    items: List[BenchmarkSummary] = []
    results_dir = get_results_dir()

    for benchmark_id, info in benchmarks_info.items():
        name = info.get("name", benchmark_id)
        description = info.get("description", "")
        db_type = info.get("db_engine", {}).get("db_type", "N/A")
        logo = info.get("logo")
        if not logo:
            # Backward compatibility for previously saved absolute/static URL values.
            legacy_logo_url = info.get("logo_url")
            if isinstance(legacy_logo_url, str) and legacy_logo_url.strip():
                logo = Path(legacy_logo_url.split("?", 1)[0]).name
        num_records = 0
        num_pipelines = 0

        # Count records from benchmark data file
        data_path = info.get("benchmark_json_path")
        try:
            # Prefer repository data root if configured (data/benchmarks/*.json),
            # then fall back to benchmark_json_path from package metadata.
            rel_data_path = info.get("data")
            if isinstance(rel_data_path, str):
                num_records = count_records(get_data_root() / rel_data_path)
            if num_records == 0:
                num_records = count_records(data_path)
        except Exception as e:  # pragma: no cover - defensive
            logger.warning(f"Could not count records for {benchmark_id}: {e}")

        # Count pipelines from summary JSON if present
        summary_path = results_dir / f"{benchmark_id}-predictions_eval_summary.json"
        if summary_path.exists():
            try:
                summary = load_json(summary_path)
                num_pipelines = len(
                    [k for k in summary.keys() if k != "llm_judge_config"]
                )
            except Exception as e:  # pragma: no cover - defensive
                logger.warning(f"Could not read summary for {benchmark_id}: {e}")

        eval_results_bytes: Optional[int] = None
        eval_path = results_dir / f"{benchmark_id}-predictions_eval.json"
        if eval_path.is_file():
            try:
                eval_results_bytes = eval_path.stat().st_size
            except OSError as e:  # pragma: no cover - defensive
                logger.warning(
                    "Could not stat eval results for %s: %s", benchmark_id, e
                )

        items.append(
            BenchmarkSummary(
                benchmark_id=benchmark_id,
                name=name,
                description=description,
                db_type=db_type,
                num_records=num_records,
                num_pipelines=num_pipelines,
                logo=logo,
                eval_results_bytes=eval_results_bytes,
            )
        )

    return BenchmarksResponse(items=items)


@app.post("/api/benchmarks", response_model=BenchmarkConfigResponse)
def create_benchmark(req: CreateBenchmarkRequest) -> BenchmarkConfigResponse:
    benchmark_id = normalize_benchmark_id(req.benchmark_id)
    registry_path = get_benchmark_registry_path()
    registry = load_benchmark_registry(registry_path)
    if benchmark_id in registry:
        raise HTTPException(status_code=409, detail="Benchmark already exists")

    config = normalize_benchmark_config(benchmark_id, req)
    registry[benchmark_id] = config
    write_json_atomic(registry_path, registry)
    return BenchmarkConfigResponse(benchmark_id=benchmark_id, config=config)


@app.get(
    "/api/benchmarks/{benchmark_id}/config", response_model=BenchmarkConfigResponse
)
def get_benchmark_config(benchmark_id: str) -> BenchmarkConfigResponse:
    normalized_id = normalize_benchmark_id(benchmark_id)
    registry_path = get_benchmark_registry_path()
    registry = load_benchmark_registry(registry_path)
    config = registry.get(normalized_id)
    if not isinstance(config, dict):
        raise HTTPException(status_code=404, detail="Benchmark not found")
    return BenchmarkConfigResponse(benchmark_id=normalized_id, config=config)


@app.put("/api/benchmarks/{benchmark_id}", response_model=BenchmarkConfigResponse)
def update_benchmark(
    benchmark_id: str, req: UpdateBenchmarkRequest
) -> BenchmarkConfigResponse:
    normalized_id = normalize_benchmark_id(benchmark_id)
    registry_path = get_benchmark_registry_path()
    registry = load_benchmark_registry(registry_path)
    if normalized_id not in registry:
        raise HTTPException(status_code=404, detail="Benchmark not found")

    config = normalize_benchmark_config(normalized_id, req)
    registry[normalized_id] = config
    write_json_atomic(registry_path, registry)
    return BenchmarkConfigResponse(benchmark_id=normalized_id, config=config)


@app.post("/api/benchmarks/logo-upload", response_model=Dict[str, str])
def upload_benchmark_logo(req: BenchmarkLogoUploadRequest):
    benchmark_id = normalize_benchmark_id(req.benchmark_id)
    filename = (req.filename or "").strip()
    mime_type = (req.mime_type or "").strip().lower()
    ext = Path(filename).suffix.lower() if filename else ""
    mime_to_ext = {
        "image/png": ".png",
        "image/jpeg": ".jpg",
        "image/webp": ".webp",
        "image/gif": ".gif",
        "image/svg+xml": ".svg",
    }
    if ext not in ALLOWED_LOGO_EXTENSIONS and mime_type in mime_to_ext:
        ext = mime_to_ext[mime_type]
    if ext not in ALLOWED_LOGO_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=(
                "Unsupported image type. Allowed: png, jpg, jpeg, webp, gif, svg "
                f"(received filename='{filename or '<empty>'}', mime_type='{mime_type or '<empty>'}')"
            ),
        )
    ext = ".jpg" if ext == ".jpeg" else ext

    try:
        raw = base64.b64decode(req.content_base64, validate=True)
    except Exception as e:
        raise HTTPException(status_code=400, detail="Invalid base64 payload") from e

    if not raw:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")
    if len(raw) > MAX_LOGO_UPLOAD_BYTES:
        raise HTTPException(
            status_code=400,
            detail=f"File exceeds max size of {MAX_LOGO_UPLOAD_BYTES} bytes",
        )

    stored_name = f"{benchmark_id}{ext}"
    relative_path = Path("benchmarks") / "logos" / stored_name
    abs_path = get_data_root() / relative_path
    abs_path.parent.mkdir(parents=True, exist_ok=True)

    # Keep only one logo per benchmark regardless of extension.
    for existing_ext in ALLOWED_LOGO_EXTENSIONS:
        existing = abs_path.parent / f"{benchmark_id}{existing_ext}"
        if existing.resolve() == abs_path.resolve():
            continue
        if existing.exists():
            try:
                existing.unlink()
            except Exception:
                logger.warning(f"Could not remove stale benchmark logo: {existing}")

    # If same target path already exists, remove it first so replacement is deterministic.
    if abs_path.exists():
        try:
            abs_path.unlink()
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Failed to replace existing logo: {e}"
            ) from e

    try:
        with abs_path.open("wb") as f:
            f.write(raw)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to store image: {e}"
        ) from e

    normalized_relative = relative_path.as_posix()
    version_token = str(int(time.time() * 1000))
    return {
        "logo": stored_name,
        "logo_url": f"/api/static/{normalized_relative}?v={version_token}",
        "path": normalized_relative,
    }


@app.get(
    "/api/benchmarks/{benchmark_id}/summary", response_model=BenchmarkDetailResponse
)
def get_benchmark_summary(benchmark_id: str) -> BenchmarkDetailResponse:
    """
    Return pipeline-level summary metrics for a benchmark
    similar to data/results/*-predictions_eval_summary.json.
    """
    summary_path = get_results_dir() / f"{benchmark_id}-predictions_eval_summary.json"
    if not summary_path.exists():
        raise HTTPException(
            status_code=404, detail=_summary_not_found_detail(benchmark_id)
        )

    raw = load_json(summary_path)
    llm_cfg = raw.pop("llm_judge_config", None)
    default_sort_metric = "subset_non_empty_execution_accuracy"
    if llm_cfg and isinstance(llm_cfg, dict):
        default_sort_metric = llm_cfg.get("default_sort_metric", default_sort_metric)

    pipelines: List[PipelineMetrics] = []
    for name, metrics in raw.items():
        pipelines.append(PipelineMetrics(name=name, metrics=metrics))

    return BenchmarkDetailResponse(
        benchmark_id=benchmark_id,
        default_sort_metric=default_sort_metric,
        pipelines=pipelines,
    )


@app.get(
    "/api/benchmarks/{benchmark_id}/pipeline-aliases",
    response_model=PipelineAliasesResponse,
)
def get_pipeline_aliases(benchmark_id: str) -> PipelineAliasesResponse:
    """
    Short aliases for this benchmark's pipelines, in both directions.

    A pipeline id is long enough that two of them in one URL make an address
    that chat clients truncate, so the dashboard accepts a short alias wherever
    it accepts an id.  The mapping is derived (see ``ui.aliases``), so this
    endpoint is a lookup table rather than a registry -- nothing is stored, and
    two servers reading the same artifacts return the same answer.

    Read from the summary file, which is small and always written alongside the
    evaluation artifact, so resolving a link never triggers an index build.
    """
    summary_path = get_results_dir() / f"{benchmark_id}-predictions_eval_summary.json"
    if summary_path.exists():
        raw = load_json(summary_path)
        pipeline_ids = [k for k in raw.keys() if k != "llm_judge_config"]
    else:
        # No summary: fall back to the artifact itself rather than 404, so a
        # benchmark that was evaluated but never summarised still has links.
        pipeline_ids = get_index(benchmark_id).pipeline_ids()

    aliases = alias_map(pipeline_ids)
    return PipelineAliasesResponse(
        benchmark_id=benchmark_id,
        aliases=aliases,
        by_pipeline={pipeline: alias for alias, pipeline in aliases.items()},
    )


def _collect_category_summary(
    records: List[Dict[str, Any]],
) -> Dict[str, Dict[str, Dict[str, float]]]:
    """
    Aggregate numeric evaluation metrics overall and by category.
    """
    from collections import defaultdict

    category_metrics: Dict[str, Dict[str, Dict[str, List[float]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(list))
    )
    overall_metrics: Dict[str, Dict[str, List[float]]] = defaultdict(
        lambda: defaultdict(list)
    )

    for rec in records:
        categories = rec.get("meta", {}).get("categories", [])
        predictions = rec.get("predictions", {})

        for pipeline, pred_info in predictions.items():
            eval_metrics = pred_info.get("evaluation", {})
            for metric_name, metric_value in eval_metrics.items():
                if isinstance(metric_value, (int, float)):
                    overall_metrics[pipeline][metric_name].append(float(metric_value))
                    for cat in categories:
                        category_metrics[cat][pipeline][metric_name].append(
                            float(metric_value)
                        )

    def to_avg(metrics_dict):
        from statistics import stdev
        import math

        z = 1.96  # 95% confidence level

        out = {}
        for pipeline, metric_map in metrics_dict.items():
            out[pipeline] = {}
            for metric, values in metric_map.items():
                if values:
                    n = len(values)
                    avg = sum(values) / n
                    sd = stdev(values) if n > 1 else 0.0

                    # If metric values are binary, use Wilson interval (better than normal approx).
                    is_binary = all(v in (0.0, 1.0) for v in values)
                    if is_binary and n > 0:
                        p = avg
                        denom = 1.0 + (z * z) / n
                        center = (p + (z * z) / (2.0 * n)) / denom
                        margin = (
                            z
                            * math.sqrt((p * (1.0 - p) / n) + ((z * z) / (4.0 * n * n)))
                            / denom
                        )
                        ci95_low = max(0.0, center - margin)
                        ci95_high = min(1.0, center + margin)
                    else:
                        # Generic normal-approx CI around sample mean.
                        stderr = (sd / math.sqrt(n)) if n > 0 else 0.0
                        margin = z * stderr
                        ci95_low = avg - margin
                        ci95_high = avg + margin

                    out[pipeline][metric] = {
                        "average": avg,
                        "stddev": sd,
                        "n": n,
                        "ci95_low": ci95_low,
                        "ci95_high": ci95_high,
                    }
        return out

    return {
        "overall": to_avg(overall_metrics),
        "categories": {k: to_avg(v) for k, v in category_metrics.items()},
    }


@app.get(
    "/api/benchmarks/{benchmark_id}/summary/by-category",
    response_model=BenchmarkCategorySummaryResponse,
)
def get_benchmark_summary_by_category(
    benchmark_id: str,
) -> BenchmarkCategorySummaryResponse:
    """
    Return summary metrics overall and broken down by categories.
    """
    summary_path = get_results_dir() / f"{benchmark_id}-predictions_eval_summary.json"
    eval_path = get_results_dir() / f"{benchmark_id}-predictions_eval.json"

    if not summary_path.exists():
        raise HTTPException(
            status_code=404, detail=_summary_not_found_detail(benchmark_id)
        )

    summary_raw = load_json(summary_path)
    llm_cfg = summary_raw.pop("llm_judge_config", None)
    default_sort_metric = "subset_non_empty_execution_accuracy"
    if llm_cfg and isinstance(llm_cfg, dict):
        default_sort_metric = llm_cfg.get("default_sort_metric", default_sort_metric)

    if not eval_path.exists():
        # Full eval file is large and may not be present in a fresh checkout.
        # Fall back to summary-only data: overall metrics available, no category breakdown.
        logger.warning(
            "Full evaluation results not found for %s (%s); "
            "returning summary-only data without category breakdown.",
            benchmark_id,
            eval_path,
        )
        overall_from_summary = [
            PipelineMetrics(name=name, metrics=metrics)
            for name, metrics in summary_raw.items()
            if isinstance(metrics, dict)
        ]
        return BenchmarkCategorySummaryResponse(
            benchmark_id=benchmark_id,
            default_sort_metric=default_sort_metric,
            overall=overall_from_summary,
            categories={},
            has_full_results=False,
        )

    # Whole-corpus aggregation the index does not model.  Streamed one record at
    # a time so memory stays bounded even on multi-hundred-megabyte artifacts.
    agg = _collect_category_summary(get_index(benchmark_id).iter_records())

    overall = [
        PipelineMetrics(name=name, metrics=metrics)
        for name, metrics in agg["overall"].items()
    ]
    categories: Dict[str, List[PipelineMetrics]] = {}
    for category, category_metrics in agg["categories"].items():
        categories[category] = [
            PipelineMetrics(name=name, metrics=metrics)
            for name, metrics in category_metrics.items()
        ]

    return BenchmarkCategorySummaryResponse(
        benchmark_id=benchmark_id,
        default_sort_metric=default_sort_metric,
        overall=overall,
        categories=categories,
        has_full_results=True,
    )


@app.get("/api/benchmarks/{benchmark_id}/errors", response_model=PaginatedErrorResponse)
def list_errors(
    benchmark_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=500),
    q: Optional[str] = Query(None, description="Search in question text or id"),
    pipeline: Optional[str] = Query(
        None, description="Primary pipeline id for filtering"
    ),
    metric: str = Query(
        "execution_accuracy", description="Metric key for single-pipeline filter"
    ),
    value: Optional[float] = Query(
        None, description="Expected value for metric (e.g. 0 or 1)"
    ),
    op: str = Query(
        "eq",
        description="Comparison operator for metric filter: eq, ne, lt, gt, le, ge",
    ),
    pipeline2: Optional[str] = Query(
        None,
        description="Optional second pipeline id for cross-pipeline comparison",
    ),
    metric2: Optional[str] = Query(
        None,
        description="Optional second metric key (defaults to metric if omitted)",
    ),
    disagree: bool = Query(
        False,
        description="If true and pipeline & pipeline2 set, filter where metric values differ",
    ),
    failed_only: bool = Query(
        False,
        description="If true, include only records where selected pipeline has execution_accuracy == 0",
    ),
):
    """
    Paginated list of records for error analysis with simple single- and
    cross-pipeline filters.

    Filtering, counting, and pagination run in the index rather than over a
    freshly parsed copy of the artifact, so page latency no longer scales with
    file size or page number.
    """
    if failed_only and not pipeline:
        raise HTTPException(
            status_code=400,
            detail="pipeline is required when failed_only=true",
        )

    index = get_index(benchmark_id)
    summaries, total = index.list_records(
        page=page,
        page_size=page_size,
        q=q,
        pipeline=pipeline,
        metric=metric,
        value=value,
        op=op,
        pipeline2=pipeline2,
        metric2=metric2,
        disagree=disagree,
        failed_only=failed_only,
    )

    items = [
        ErrorRecordSummary(
            record_id=summary.record_id,
            question=summary.question,
            # Only evaluations are exposed; raw dataframes can be large.
            predictions=summary.predictions,
        )
        for summary in summaries
    ]

    return PaginatedErrorResponse(
        items=items,
        total=total,
        page=page,
        page_size=page_size,
    )


@app.get(
    "/api/benchmarks/{benchmark_id}/errors/{record_id}",
    response_model=Dict[str, Any],
)
def get_error_detail(benchmark_id: str, record_id: str):
    """
    Return full record for a given benchmark and record id for detailed error analysis.
    """
    record = get_index(benchmark_id).read_record(record_id)
    if record is None:
        raise HTTPException(status_code=404, detail="Record not found")
    return record


@app.get(
    "/api/benchmarks/{benchmark_id}/errors/{record_id}/detail",
    response_model=Dict[str, Any],
)
def get_error_detail_for_pipeline(
    benchmark_id: str,
    record_id: str,
    pipeline: str = Query(..., description="Pipeline id to inspect"),
):
    """
    Return a normalized, UI-friendly detail payload for one record and one pipeline.
    """
    rec = get_index(benchmark_id).read_record(record_id)
    if rec is None:
        raise HTTPException(status_code=404, detail="Record not found")

    preds = rec.get("predictions", {})
    if pipeline not in preds:
        raise HTTPException(
            status_code=404, detail=f"Pipeline '{pipeline}' not found in record"
        )
    pred = preds[pipeline]
    eval_metrics = pred.get("evaluation", {})

    gt_sql = rec.get("sql", [])
    if isinstance(gt_sql, str):
        gt_sql = [gt_sql]

    gt_df = rec.get("gt_df", [])
    if not isinstance(gt_df, list):
        gt_df = [gt_df]

    return {
        "record_id": record_id,
        "pipeline": pipeline,
        "question": rec.get("question")
        or rec.get("utterance")
        or rec.get("page_content")
        or "",
        "db_id": rec.get("db_id"),
        "ground_truth_sql": gt_sql,
        "predicted_sql": pred.get("predicted_sql"),
        "evaluation_metrics": eval_metrics,
        "ground_truth_results": gt_df,
        "predicted_result": pred.get("predicted_df"),
        "prompt": pred.get("prompt"),
        "token_usage": pred.get("token_usage"),
        "inference_time_ms": pred.get("inference_time_ms"),
        "execution_time_ms": pred.get("execution_time_ms"),
        "llm_judge_score": eval_metrics.get("llm_score"),
        "llm_judge_explanation": eval_metrics.get("llm_explanation"),
        "sql_execution_error": pred.get("sql_execution_error"),
        "inference_error": pred.get("inference_error"),
    }


@app.post("/api/benchmarks/{benchmark_id}/evaluate", response_model=JobStatus)
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
    _update_job(job)

    def worker():
        job.status = "running"
        _update_job(job)
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
            _update_job(job)

    threading.Thread(target=worker, daemon=True).start()
    return job


@app.get("/api/jobs/{job_id}", response_model=JobStatus)
def get_job_status(job_id: str) -> JobStatus:
    with JOBS_LOCK:
        job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


# ---------------------------------------------------------------------------
# Results Hub endpoints (enabled only when --enable-fetch is passed)
# ---------------------------------------------------------------------------


@app.get("/api/results/status")
def get_results_status() -> Dict[str, Any]:
    """
    Report whether the fetch endpoint is enabled and whether local results exist.

    The React UI calls this on mount to decide whether to show the
    "Fetch results" banner.
    """
    data_root = get_data_root()
    results_dir = data_root / "results"
    has_results = results_dir.is_dir() and any(results_dir.iterdir())
    return {
        "fetch_enabled": _ENABLE_FETCH_ENDPOINT,
        "has_results": has_results,
        "results_path": str(results_dir),
    }


@app.post("/api/results/fetch", response_model=FetchJobStatus)
def start_results_fetch(
    req: ResultsFetchRequest = ResultsFetchRequest(),
) -> FetchJobStatus:
    """
    Kick off a background download of results from the Hugging Face Hub.

    Only available when the dashboard is started with ``--enable-fetch``.
    Returns 404 otherwise (so that the default prod setup is unaffected).
    """
    if not _ENABLE_FETCH_ENDPOINT:
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


@app.get("/api/results/fetch/{job_id}", response_model=FetchJobStatus)
def get_results_fetch_status(job_id: str) -> FetchJobStatus:
    """Poll the status of a fetch job started by POST /api/results/fetch."""
    if not _ENABLE_FETCH_ENDPOINT:
        raise HTTPException(status_code=404, detail="Not Found")
    with FETCH_JOBS_LOCK:
        job = FETCH_JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Fetch job not found")
    return job


def main(argv: Optional[List[str]] = None) -> None:
    """
    Console entrypoint that starts the API (and static UI if built),
    intended to be wired as `text2sql-eval-dashboard`.
    """
    dashboard_dir = _resolve_dashboard_source_dir()
    default_watch = dashboard_dir is not None

    parser = argparse.ArgumentParser(
        description="Run the Text2SQL Evaluation Dashboard"
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument(
        "--open-browser",
        action="store_true",
        help="Open the default browser to the dashboard URL after startup",
    )
    parser.add_argument(
        "--watch-dashboard",
        action=argparse.BooleanOptionalAction,
        default=default_watch,
        help=(
            "Watch dashboard sources and rebuild dashboard/dist via `vite build --watch` (requires npm). "
            "Defaults to on when a dashboard/ tree with package.json is found next to the repo or cwd; "
            "use --no-watch-dashboard to serve existing dist only."
        ),
    )
    parser.add_argument(
        "--mode",
        default=os.getenv("TEXT2SQL_DASHBOARD_MODE", "full"),
        choices=[t.name.lower() for t in Tier],
        help=(
            "Capability ceiling for this deployment. 'full' (default) is the "
            "local operator tool and is refused on a non-loopback interface "
            "without --allow-remote-full. 'public' serves pre-computed results "
            "read-only. 'judge' additionally lets allowlisted signed-in users "
            "run LLM-as-judge."
        ),
    )
    parser.add_argument(
        "--allow-remote-full",
        action="store_true",
        help=(
            "Permit --mode full on a non-loopback interface. This exposes SQL "
            "execution and registry writes to anyone who can reach the port; "
            "do not use it to serve the public dashboard."
        ),
    )
    parser.add_argument(
        "--enable-fetch",
        action="store_true",
        default=False,
        help=(
            "Enable the /api/results/fetch endpoint and the in-dashboard "
            "'Fetch results' button.  Off by default; intended for developer "
            "or controlled environments only.  Production deployments should "
            "use `text2sql-eval-toolkit results fetch` from the CLI instead."
        ),
    )
    args = parser.parse_args(argv)

    mode = Tier.parse(args.mode)
    # The dangerous configuration should take deliberate effort, not a default.
    loopback = args.host in {"127.0.0.1", "localhost", "::1"}
    if mode is Tier.FULL and not loopback and not args.allow_remote_full:
        parser.error(
            f"--mode full refuses to bind {args.host}: it exposes SQL execution "
            "against configured database credentials, evaluation runs, and "
            "registry writes to anyone who can reach the port. Use --mode public "
            "for a shared deployment, or --allow-remote-full if you are certain."
        )
    set_mode(mode)
    set_judge_allowlist(parse_allowlist(os.getenv("TEXT2SQL_JUDGE_ALLOWLIST")))
    configure_cors(mode)

    if auth.is_configured():
        from starlette.middleware.sessions import SessionMiddleware

        # Lax rather than Strict: the OAuth callback is a cross-site redirect
        # back to us, and Strict would withhold the cookie and break the state
        # check. https_only is off for a loopback dev run and must be on behind
        # TLS, which is what the deployment terminates at.
        app.add_middleware(
            SessionMiddleware,
            secret_key=auth.session_secret(),
            session_cookie="t2s_session",
            max_age=auth.SESSION_MAX_AGE_SECONDS,
            same_site="lax",
            # Driven by the deployment mode, not the bind address: behind a
            # TLS-terminating proxy the app binds an internal address, which
            # would have silently dropped Secure on exactly the deployment that
            # needs it. Override only for a local HTTP experiment.
            https_only=_cookie_secure(mode),
        )
        logger.info("Google sign-in enabled")
    elif mode is not Tier.FULL:
        logger.warning(
            "Mode is '%s' but Google sign-in is not configured, so nobody can "
            "reach the judge tier. Set GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET.",
            mode.name.lower(),
        )
    logger.info(
        "Capability mode: %s (judge allowlist: %d entr%s)",
        mode.name.lower(),
        len(get_judge_allowlist()),
        "y" if len(get_judge_allowlist()) == 1 else "ies",
    )

    global _ENABLE_FETCH_ENDPOINT
    if args.enable_fetch:
        _ENABLE_FETCH_ENDPOINT = True
        logger.info(
            "Results fetch endpoint enabled.  " "POST /api/results/fetch is active."
        )

    # Check whether results are present; hint if not.
    data_root = get_data_root()
    results_dir = data_root / "results"
    if not results_dir.is_dir() or not any(results_dir.iterdir()):
        logger.info(
            "No results found at %s.  Run: text2sql-eval-toolkit results fetch",
            results_dir,
        )

    watch_proc: Optional[subprocess.Popen] = None
    try:
        if args.watch_dashboard:
            if dashboard_dir is None:
                logger.warning(
                    "--watch-dashboard is enabled but no dashboard/package.json was found; "
                    "skipping watch. Use --no-watch-dashboard to silence this."
                )
            else:
                _ensure_dashboard_dist(dashboard_dir)
                watch_proc = _spawn_dashboard_watch(dashboard_dir)

        mount_static(app)

        if args.open_browser:
            import webbrowser

            url = f"http://{args.host}:{args.port}"
            # Open slightly after startup; this is best-effort.
            threading.Timer(1.5, lambda: webbrowser.open(url)).start()

        uvicorn.run(app, host=args.host, port=args.port)
    finally:
        _terminate_dashboard_watch(watch_proc)


if __name__ == "__main__":
    main()
