import argparse
import asyncio
import base64
import json
import math
import os
from copy import deepcopy
import re
import shutil
import subprocess
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field
import uvicorn

import text2sql_eval_toolkit.env_loader  # noqa: F401 — load .env (WATSONX_*, etc.) before eval/inference

from text2sql_eval_toolkit.utils import get_benchmarks_info
from text2sql_eval_toolkit.utils import (
    get_benchmark_info,
    load_benchmark_records,
    load_eval_summary,
    load_predictions_data,
    save_benchmark_records as save_gold_records,
    get_writable_data_root,
)
from text2sql_eval_toolkit.database.store import get_store
from text2sql_eval_toolkit.database.session import ensure_schema, get_connection
from text2sql_eval_toolkit.database import jobs as db_jobs
from text2sql_eval_toolkit.database.json_importer import FEATURE_FIELDS
from text2sql_eval_toolkit.execution.execution_tools import (
    _parse_presto_sqlalchemy_url,
    _normalize_sql_for_db2,
    normalize_mysql_connection_string,
    quote_mixed_case_columns,
    quote_mysql_identifiers,
    run_sql_and_get_dataframe_async,
    run_sql_and_get_dataframe_mysql_async,
    run_sqlite_query_with_timeout,
)
from text2sql_eval_toolkit.evaluation.evaluation_tools import (
    evaluate_prediction,
    run_evaluation,
)
from text2sql_eval_toolkit.evaluation.llm_as_judge import load_llm_judge_config
from text2sql_eval_toolkit.evaluation.metric_definitions import (
    get_metric_definitions_payload,
)
from text2sql_eval_toolkit.logging import get_logger
from text2sql_eval_toolkit.profiling.sql_ast import (
    PARSE_MODES,
    benchmark_db_type_to_dialect,
    parse_sql_to_tree,
)
from text2sql_eval_toolkit.utils import get_gt_sqls, get_question


logger = get_logger(__name__)

app = FastAPI(title="Text2SQL Evaluation Dashboard API")

# When True the /api/results/fetch endpoints are active.  Set by main() via
# the --enable-fetch CLI flag.  Off by default so production deployments are
# safe without any configuration.
_ENABLE_FETCH_ENDPOINT: bool = False

# Allow local dev frontends by default
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost", "http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_data_root() -> Path:
    """Resolve the writable data directory (same logic as pipelines and the DB store)."""
    return get_writable_data_root()


def get_results_dir() -> Path:
    """
    Directory that contains *-predictions_eval_summary.json and *-predictions_eval.json.
    """
    return get_data_root() / "results"


def _eval_not_found_detail(benchmark_id: str) -> str:
    return (
        f"No evaluation results found in the database for benchmark '{benchmark_id}'. "
        "Run the evaluation pipeline (e.g. `python scripts/evaluation/run_evaluation.py "
        f"{benchmark_id}`) or import existing JSON with "
        "`python scripts/migration/import_json_to_db.py --benchmark-id "
        f"{benchmark_id}`."
    )


def _summary_not_found_detail(benchmark_id: str) -> str:
    return (
        f"No evaluation summary found in the database for benchmark '{benchmark_id}'. "
        "Run evaluation or import existing JSON summaries via the migration script."
    )


ALLOWED_DB_TYPES = {"sqlite", "postgres", "mysql", "db2", "presto"}
ALLOWED_LOGO_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp", ".gif", ".svg"}
MAX_LOGO_UPLOAD_BYTES = 5 * 1024 * 1024  # 5 MB


def load_benchmark_registry(path: Path | None = None) -> Dict[str, Any]:
    ensure_schema()
    return get_store(data_root=get_data_root()).load_registry(production_only=False)


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
                    "db_engine.connection_string_env_var is required "
                    f"for {db_type}"
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


class BenchmarkSummary(BaseModel):
    benchmark_id: str
    name: str
    description: str
    db_type: str
    num_records: int
    num_pipelines: int
    logo: Optional[str] = None
    eval_results_bytes: Optional[int] = None


class BenchmarksResponse(BaseModel):
    items: List[BenchmarkSummary]


class BenchmarkConfigInput(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    description: str
    data: str
    schema_path: str = Field(alias="schema")
    predictions: str
    db_engine: Dict[str, Any]
    logo: Optional[str] = None
    logo_url: Optional[str] = None


class CreateBenchmarkRequest(BenchmarkConfigInput):
    benchmark_id: str


class UpdateBenchmarkRequest(BenchmarkConfigInput):
    pass


class BenchmarkConfigResponse(BaseModel):
    benchmark_id: str
    config: Dict[str, Any]


class BenchmarkLogoUploadRequest(BaseModel):
    benchmark_id: str
    filename: Optional[str] = None
    mime_type: Optional[str] = None
    content_base64: str


class PipelineMetrics(BaseModel):
    name: str
    metrics: Dict[str, Any]


class BenchmarkDetailResponse(BaseModel):
    benchmark_id: str
    default_sort_metric: str
    pipelines: List[PipelineMetrics]


class BenchmarkCategorySummaryResponse(BaseModel):
    benchmark_id: str
    default_sort_metric: str
    overall: List[PipelineMetrics]
    categories: Dict[str, List[PipelineMetrics]]
    has_full_results: bool = True


class ErrorRecordSummary(BaseModel):
    record_id: str
    question: str
    predictions: Dict[str, Dict[str, Any]]


class PaginatedErrorResponse(BaseModel):
    items: List[ErrorRecordSummary]
    total: int
    page: int
    page_size: int


class CompareRow(BaseModel):
    pipeline: str
    metric: str
    left: Optional[float]
    right: Optional[float]
    diff: Optional[float]


class CompareResponse(BaseModel):
    benchmark_id: str
    left_id: str
    right_id: str
    rows: List[CompareRow]


class BinaryMetricConfusionCounts(BaseModel):
    a0b0: int
    a0b1: int
    a1b0: int
    a1b1: int


class BinaryMetricConfusionRates(BaseModel):
    a0b0: float
    a0b1: float
    a1b0: float
    a1b1: float


class BinaryMetricConfusionByPipelineRow(BaseModel):
    pipeline: str
    counts: BinaryMetricConfusionCounts
    n_valid: int
    rates: BinaryMetricConfusionRates
    agreement_rate: float
    disagreement_rate: float


class BinaryMetricConfusionByPipelineResponse(BaseModel):
    benchmark_id: str
    metric_a: str
    metric_b: str
    per_pipeline: List[BinaryMetricConfusionByPipelineRow]


class CrossPipelineBinaryMetricConfusionCounts(BaseModel):
    left0right0: int
    left0right1: int
    left1right0: int
    left1right1: int


class CrossPipelineBinaryMetricConfusionRates(BaseModel):
    left0right0: float
    left0right1: float
    left1right0: float
    left1right1: float


class CrossPipelineBinaryMetricConfusionResponse(BaseModel):
    benchmark_id: str
    left_id: str
    right_id: str
    metric_left: str
    metric_right: str
    n_valid: int
    counts: CrossPipelineBinaryMetricConfusionCounts
    rates: CrossPipelineBinaryMetricConfusionRates
    agreement_rate: float
    disagreement_rate: float


class FeatureMetricCorrelation(BaseModel):
    feature: str
    metric: str
    n: int
    pearson_r: Optional[float] = None
    pearson_p: Optional[float] = None
    spearman_rho: Optional[float] = None
    spearman_p: Optional[float] = None


class CategoryMetricAssociation(BaseModel):
    category: str
    metric: str
    n_with: int
    n_without: int
    mean_with: Optional[float] = None
    mean_without: Optional[float] = None
    delta: Optional[float] = None
    point_biserial_r: Optional[float] = None
    point_biserial_p: Optional[float] = None


class FeatureMetricBin(BaseModel):
    x_value: float
    x_label: str
    average: Optional[float] = None
    n: int


class FeatureMetricSeries(BaseModel):
    feature: str
    metric: str
    bins: List[FeatureMetricBin]
    scatter: List[Dict[str, float]] = Field(default_factory=list)


class CategoryMetricMean(BaseModel):
    category: str
    metric: str
    average: Optional[float] = None
    n: int
    ci95_low: Optional[float] = None
    ci95_high: Optional[float] = None


class ProfileMetricCorrelationsResponse(BaseModel):
    benchmark_id: str
    pipeline: str
    metric: str
    metrics: List[str]
    n_records: int
    n_with_features: int
    n_with_categories: int
    feature_correlations: List[FeatureMetricCorrelation]
    category_associations: List[CategoryMetricAssociation]
    category_means: List[CategoryMetricMean]
    feature_series: List[FeatureMetricSeries]
    available_pipelines: List[str]
    available_features: List[str]
    available_categories: List[str]


class LLMJudgeConfigInfo(BaseModel):
    name: str
    path: str


class LLMJudgeConfigListResponse(BaseModel):
    items: List[LLMJudgeConfigInfo]


class BenchmarkLlmJudgeConfig(BaseModel):
    id: int
    name: str
    model_id: Optional[str] = None


class BenchmarkLlmJudgeConfigListResponse(BaseModel):
    items: List[BenchmarkLlmJudgeConfig]
    default_id: Optional[int] = None


class EvaluateRequest(BaseModel):
    use_llm: bool = False
    llm_judge_config_path: Optional[str] = None
    force_rerun_llm_judge: bool = False
    force_rerun: bool = False


class ExecuteSqlRequest(BaseModel):
    sql: str
    record_id: Optional[str] = None
    db_id: Optional[str] = None
    timeout_s: Optional[int] = None


class ExecuteSqlResponse(BaseModel):
    benchmark_id: str
    db_type: str
    sql: str
    db_id: Optional[str] = None
    execution_time_ms: float
    row_count: int
    column_count: int
    result: Dict[str, Any]


class AddGroundTruthSqlRequest(BaseModel):
    record_id: str
    sql: str


class AddGroundTruthSqlResponse(BaseModel):
    benchmark_id: str
    record_id: str
    added: bool
    message: str
    ground_truth_count: int


class RecordIdItem(BaseModel):
    record_id: str
    question: str


class RecordIdsResponse(BaseModel):
    benchmark_id: str
    items: List[RecordIdItem]


class PipelinePlaygroundInfo(BaseModel):
    name: str
    predicted_sql: Optional[str] = None
    has_prompt: bool = False
    has_agent_trace: bool = False
    evaluation: Optional[Dict[str, Any]] = None
    prediction_error: Optional[str] = None
    prediction_row_count: Optional[int] = None
    prediction_column_count: Optional[int] = None
    predicted_df: Optional[str] = None


class PlaygroundInitResponse(BaseModel):
    benchmark_id: str
    record_id: str
    question: str
    db_id: Optional[str] = None
    ground_truth_sqls: List[str]
    pipelines: List[PipelinePlaygroundInfo] = Field(default_factory=list)
    ground_truth_row_counts: List[int] = Field(default_factory=list)
    ground_truth_dfs: List[str] = Field(default_factory=list)


class PlaygroundEvaluateRequest(BaseModel):
    record_id: str
    ground_truth_sqls: List[str]
    predicted_sql: str
    timeout_s: Optional[int] = 90
    use_llm: bool = False
    llm_judge_config_path: Optional[str] = None
    force_rerun_llm_judge: bool = False
    merge_pipeline: Optional[str] = None


class PlaygroundEvaluateResponse(BaseModel):
    benchmark_id: str
    record_id: str
    evaluation: Dict[str, Any]
    ground_truth_row_counts: List[int] = Field(default_factory=list)
    ground_truth_dfs: List[str] = Field(
        default_factory=list,
        description="Pandas orient=split JSON per ground-truth SQL (same order as ground_truth_sqls)",
    )
    predicted_df: Optional[str] = Field(
        default=None,
        description="Pandas orient=split JSON for predicted SQL result (empty schema if execution failed)",
    )
    prediction_error: Optional[str] = None
    prediction_row_count: Optional[int] = None
    prediction_column_count: Optional[int] = None


class SqlParseRequest(BaseModel):
    sql: str
    dialect: Optional[str] = Field(
        default=None,
        description="sqlglot dialect (e.g. sqlite, postgres). Defaults to postgres.",
    )
    parse_mode: Optional[str] = Field(
        default="sqlglot",
        description="sqlglot (raw parse) or sqlglot_optimized (sqlglot.optimizer.optimize).",
    )


class SqlParseResponse(BaseModel):
    ok: bool
    error: Optional[str] = None
    dialect: Optional[str] = None
    parse_mode: Optional[str] = None
    tree: Optional[Dict[str, Any]] = None
    visual_tree: Optional[Dict[str, Any]] = None
    formatted_sql: Optional[str] = None
    analysis: Optional[Dict[str, Any]] = None
    analysis_error: Optional[str] = None


class EvaluationMetricDefinitionsResponse(BaseModel):
    groups: List[str]
    metrics: List[Dict[str, Any]]


class JobStatus(BaseModel):
    job_id: str
    job_type: str
    benchmark_id: str
    status: str
    progress: float = 0.0
    message: Optional[str] = None
    error: Optional[str] = None
    params: Dict[str, Any] = Field(default_factory=dict)
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    created_at: Optional[str] = None


def _job_status_from_row(row: Dict[str, Any]) -> JobStatus:
    return JobStatus(
        job_id=row["job_id"],
        job_type=row["job_type"],
        benchmark_id=row["benchmark_id"],
        status=row["status"],
        progress=row.get("progress") or 0.0,
        message=row.get("message"),
        error=row.get("error"),
        params=row.get("params") or {},
        started_at=row.get("started_at"),
        completed_at=row.get("completed_at"),
        created_at=row.get("created_at"),
    )


class FetchJobStatus(BaseModel):
    job_id: str
    state: str  # queued | running | completed | failed
    bytes_downloaded: int = 0
    total_bytes: int = 0
    error: Optional[str] = None


FETCH_JOBS: Dict[str, FetchJobStatus] = {}
FETCH_JOBS_LOCK = threading.Lock()

# Cache slim evaluation records (metrics + metadata only — no DF/prompt/trace payloads).
# Value: (cache_version, records). Capped with LRU so switching benchmarks releases memory.
EVAL_RECORDS_CACHE: Dict[str, Tuple[str, List[Dict[str, Any]]]] = {}
EVAL_RECORDS_LOCK = threading.Lock()
EVAL_RECORDS_CACHE_MAX = 6


def _records_have_eval_predictions(records: List[Dict[str, Any]]) -> bool:
    for record in records:
        predictions = record.get("predictions")
        if not isinstance(predictions, dict):
            continue
        for block in predictions.values():
            if isinstance(block, dict) and block.get("evaluation"):
                return True
    return False


def _cache_put(cache_key: str, cache_version: str, data: List[Dict[str, Any]]) -> None:
    """Insert into the slim-records LRU cache (caller must hold EVAL_RECORDS_LOCK)."""
    EVAL_RECORDS_CACHE.pop(cache_key, None)
    EVAL_RECORDS_CACHE[cache_key] = (cache_version, data)
    while len(EVAL_RECORDS_CACHE) > EVAL_RECORDS_CACHE_MAX:
        EVAL_RECORDS_CACHE.pop(next(iter(EVAL_RECORDS_CACHE)))


def load_eval_records(
    benchmark_id: str,
    llm_judge_config_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Load slim evaluation records for dashboard aggregation / filtering.

    Omits prompts, agent traces, and dataframe payloads so large benchmarks
    (e.g. Beaver) stay fast and do not pin ~1GB in the process cache.
    """
    ensure_schema()
    store = get_store(data_root=get_data_root())
    cache_key = f"{benchmark_id}:{llm_judge_config_id or 'default'}:slim"
    try:
        cache_version = store.get_cache_version(benchmark_id)
    except ValueError:
        raise HTTPException(status_code=404, detail=_eval_not_found_detail(benchmark_id))

    with EVAL_RECORDS_LOCK:
        cached = EVAL_RECORDS_CACHE.get(cache_key)
        if cached is not None and cached[0] == cache_version:
            # Refresh LRU order.
            EVAL_RECORDS_CACHE.pop(cache_key)
            EVAL_RECORDS_CACHE[cache_key] = cached
            return cached[1]

    try:
        data = load_predictions_data(
            benchmark_id,
            include_eval=True,
            llm_judge_config_id=llm_judge_config_id,
            include_payloads=False,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=_eval_not_found_detail(benchmark_id)) from exc
    if not data:
        raise HTTPException(status_code=404, detail=_eval_not_found_detail(benchmark_id))
    if not _records_have_eval_predictions(data):
        raise HTTPException(status_code=404, detail=_eval_not_found_detail(benchmark_id))
    if not isinstance(data, list):
        raise HTTPException(status_code=500, detail="Invalid evaluation record format")

    with EVAL_RECORDS_LOCK:
        _cache_put(cache_key, cache_version, data)
        return data


def load_eval_record(
    benchmark_id: str,
    record_id: str,
    llm_judge_config_id: Optional[int] = None,
) -> Dict[str, Any]:
    """Load a single record with full payloads (DFs, prompts, traces). Not cached."""
    ensure_schema()
    try:
        data = load_predictions_data(
            benchmark_id,
            include_eval=True,
            llm_judge_config_id=llm_judge_config_id,
            include_payloads=True,
            record_ids=[record_id],
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=_eval_not_found_detail(benchmark_id)) from exc
    for rec in data or []:
        rid = str(rec.get("id") or rec.get("question_id") or "")
        if rid == record_id:
            return rec
    raise HTTPException(status_code=404, detail="Record not found")


def clear_eval_records_cache(benchmark_id: Optional[str] = None) -> int:
    """Drop cached slim records. If benchmark_id is set, only that benchmark."""
    with EVAL_RECORDS_LOCK:
        if benchmark_id is None:
            n = len(EVAL_RECORDS_CACHE)
            EVAL_RECORDS_CACHE.clear()
            return n
        prefix = f"{benchmark_id}:"
        keys = [k for k in EVAL_RECORDS_CACHE if k.startswith(prefix)]
        for k in keys:
            EVAL_RECORDS_CACHE.pop(k, None)
        return len(keys)


class EvalCacheClearResponse(BaseModel):
    benchmark_id: str
    cleared: int


@app.delete(
    "/api/benchmarks/{benchmark_id}/cache",
    response_model=EvalCacheClearResponse,
)
def unload_benchmark_eval_cache(benchmark_id: str) -> EvalCacheClearResponse:
    """Drop in-memory eval records for a benchmark (e.g. after deselecting in the UI)."""
    cleared = clear_eval_records_cache(benchmark_id)
    return EvalCacheClearResponse(benchmark_id=benchmark_id, cleared=cleared)


def get_pipeline_metric_value(
    record: Dict[str, Any], pipeline_id: str, metric_key: str
) -> Optional[float]:
    preds = record.get("predictions", {})
    if not isinstance(preds, dict) or pipeline_id not in preds:
        return None
    eval_block = preds[pipeline_id].get("evaluation", {})
    if not isinstance(eval_block, dict):
        return None
    val = eval_block.get(metric_key)
    if isinstance(val, (int, float)):
        return float(val)
    return None


def to_binary_metric(value: Optional[float]) -> Optional[int]:
    """Metrics in this UI are binary; treat exactly 1 as positive, else 0."""
    if value is None:
        return None
    return 1 if float(value) == 1.0 else 0


def _student_t_sf_two_tail(t_abs: float, df: float) -> Optional[float]:
    """Two-tailed p-value for |t| under Student-t with ``df`` degrees of freedom."""
    if df <= 0 or not math.isfinite(t_abs):
        return None
    try:
        from scipy import stats as scipy_stats

        return float(2.0 * scipy_stats.t.sf(t_abs, df))
    except Exception:
        # Normal approximation is adequate for dashboard display when n is moderate+.
        # For small df this is slightly anti-conservative.
        z = t_abs * (1.0 - 1.0 / (4.0 * df)) / math.sqrt(1.0 + (t_abs * t_abs) / (2.0 * df))
        # erfc for two-tailed normal survival
        p = math.erfc(z / math.sqrt(2.0))
        return float(min(1.0, max(0.0, p)))


def _pearson_corr(xs: List[float], ys: List[float]) -> Tuple[Optional[float], Optional[float]]:
    n = len(xs)
    if n < 3:
        return None, None
    import numpy as np

    x = np.asarray(xs, dtype=float)
    y = np.asarray(ys, dtype=float)
    if np.std(x) == 0 or np.std(y) == 0:
        return None, None
    r = float(np.corrcoef(x, y)[0, 1])
    if not math.isfinite(r):
        return None, None
    r = max(-1.0, min(1.0, r))
    if abs(r) >= 1.0 - 1e-15:
        return r, 0.0
    t_abs = abs(r) * math.sqrt((n - 2) / (1.0 - r * r))
    return r, _student_t_sf_two_tail(t_abs, float(n - 2))


def _rankdata_average(values: List[float]) -> List[float]:
    """Average ranks for ties (1-based), matching typical Spearman implementations."""
    indexed = sorted(enumerate(values), key=lambda item: item[1])
    ranks = [0.0] * len(values)
    i = 0
    while i < len(indexed):
        j = i
        while j + 1 < len(indexed) and indexed[j + 1][1] == indexed[i][1]:
            j += 1
        avg_rank = 0.5 * ((i + 1) + (j + 1))
        for k in range(i, j + 1):
            ranks[indexed[k][0]] = avg_rank
        i = j + 1
    return ranks


def _spearman_corr(xs: List[float], ys: List[float]) -> Tuple[Optional[float], Optional[float]]:
    return _pearson_corr(_rankdata_average(xs), _rankdata_average(ys))


def _safe_corr_pair(
    xs: List[float], ys: List[float]
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """Return (pearson_r, pearson_p, spearman_rho, spearman_p) or Nones if underpowered."""
    if len(xs) < 3 or len(ys) < 3 or len(xs) != len(ys):
        return None, None, None, None
    if len(set(xs)) < 2 or len(set(ys)) < 2:
        return None, None, None, None
    try:
        from scipy import stats as scipy_stats

        pearson_r, pearson_p = scipy_stats.pearsonr(xs, ys)
        spearman_rho, spearman_p = scipy_stats.spearmanr(xs, ys)
        return (
            float(pearson_r),
            float(pearson_p),
            float(spearman_rho),
            float(spearman_p),
        )
    except Exception:
        pearson_r, pearson_p = _pearson_corr(xs, ys)
        spearman_rho, spearman_p = _spearman_corr(xs, ys)
        return pearson_r, pearson_p, spearman_rho, spearman_p


def _mean_and_ci95(values: List[float]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    if not values:
        return None, None, None
    n = len(values)
    mean = sum(values) / n
    if n < 2:
        return mean, None, None
    try:
        from statistics import stdev

        sd = stdev(values)
        se = sd / math.sqrt(n)
        # Approximate normal 95% CI (adequate for dashboard display).
        return mean, mean - 1.96 * se, mean + 1.96 * se
    except Exception:
        return mean, None, None


def _parse_metric_list(raw: Optional[str], primary: str) -> List[str]:
    metrics: List[str] = []
    seen = set()
    for part in (raw or "").split(","):
        name = part.strip()
        if name and name not in seen:
            seen.add(name)
            metrics.append(name)
    if primary not in seen:
        metrics.insert(0, primary)
    elif metrics and metrics[0] != primary:
        metrics = [primary] + [m for m in metrics if m != primary]
    return metrics or [primary]


@app.get("/api/benchmarks", response_model=BenchmarksResponse)
def list_benchmarks() -> BenchmarksResponse:
    """
    List benchmarks with basic metadata and counts.
    """
    benchmarks_info = get_benchmarks_info(is_test=False)
    items: List[BenchmarkSummary] = []
    store = get_store(data_root=get_data_root())

    for benchmark_id, info in benchmarks_info.items():
        name = info.get("name", benchmark_id)
        description = info.get("description", "")
        db_type = info.get("db_engine", {}).get("db_type", "N/A")
        logo = info.get("logo")
        if not logo:
            legacy_logo_url = info.get("logo_url")
            if isinstance(legacy_logo_url, str) and legacy_logo_url.strip():
                logo = Path(legacy_logo_url.split("?", 1)[0]).name
        num_records = info.get("num_records") or 0
        if num_records == 0:
            try:
                num_records = len(load_benchmark_records(benchmark_id))
            except Exception as e:  # pragma: no cover - defensive
                logger.warning(f"Could not count records for {benchmark_id}: {e}")

        num_pipelines = 0
        try:
            summary = store.load_summary(benchmark_id)
            num_pipelines = len(
                [k for k in summary.keys() if k != "llm_judge_config"]
            )
        except FileNotFoundError:
            pass
        except Exception as e:  # pragma: no cover - defensive
            logger.warning(f"Could not read summary for {benchmark_id}: {e}")

        eval_results_bytes: Optional[int] = None
        try:
            eval_results_bytes = store.estimate_eval_payload_bytes(benchmark_id)
        except Exception as e:  # pragma: no cover - defensive
            logger.warning(f"Could not estimate payload size for {benchmark_id}: {e}")

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
    registry = load_benchmark_registry()
    if benchmark_id in registry:
        raise HTTPException(status_code=409, detail="Benchmark already exists")

    config = normalize_benchmark_config(benchmark_id, req)
    registry[benchmark_id] = config
    get_store(data_root=get_data_root()).save_registry_entry(
        benchmark_id, config, is_test=False
    )
    return BenchmarkConfigResponse(benchmark_id=benchmark_id, config=config)


@app.get("/api/benchmarks/{benchmark_id}/config", response_model=BenchmarkConfigResponse)
def get_benchmark_config(benchmark_id: str) -> BenchmarkConfigResponse:
    normalized_id = normalize_benchmark_id(benchmark_id)
    registry = load_benchmark_registry()
    config = registry.get(normalized_id)
    if not isinstance(config, dict):
        raise HTTPException(status_code=404, detail="Benchmark not found")
    return BenchmarkConfigResponse(benchmark_id=normalized_id, config=config)


@app.put("/api/benchmarks/{benchmark_id}", response_model=BenchmarkConfigResponse)
def update_benchmark(
    benchmark_id: str, req: UpdateBenchmarkRequest
) -> BenchmarkConfigResponse:
    normalized_id = normalize_benchmark_id(benchmark_id)
    registry = load_benchmark_registry()
    if normalized_id not in registry:
        raise HTTPException(status_code=404, detail="Benchmark not found")

    config = normalize_benchmark_config(normalized_id, req)
    get_store(data_root=get_data_root()).save_registry_entry(
        normalized_id, config, is_test=bool(config.get("is_test_subset"))
    )
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
        raise HTTPException(status_code=500, detail=f"Failed to store image: {e}") from e

    normalized_relative = relative_path.as_posix()
    version_token = str(int(time.time() * 1000))
    return {
        "logo": stored_name,
        "logo_url": f"/api/static/{normalized_relative}?v={version_token}",
        "path": normalized_relative,
    }


@app.get("/api/benchmarks/{benchmark_id}/summary", response_model=BenchmarkDetailResponse)
def get_benchmark_summary(
    benchmark_id: str,
    llm_judge_config_id: Optional[int] = Query(
        None, description="LLM judge config id for llm_score metrics"
    ),
) -> BenchmarkDetailResponse:
    """Return pipeline-level summary metrics for a benchmark from SQLite."""
    try:
        raw = load_eval_summary(
            benchmark_id, llm_judge_config_id=llm_judge_config_id
        )
    except FileNotFoundError as exc:
        raise HTTPException(
            status_code=404, detail=_summary_not_found_detail(benchmark_id)
        ) from exc

    llm_cfg = raw.pop("llm_judge_config", None)
    raw.pop("llm_judge_config_id", None)
    default_sort_metric = "subset_non_empty_execution_accuracy"
    if llm_cfg and isinstance(llm_cfg, dict):
        default_sort_metric = (
            llm_cfg.get("default_sort_metric", default_sort_metric)
        )

    pipelines: List[PipelineMetrics] = []
    for name, metrics in raw.items():
        pipelines.append(PipelineMetrics(name=name, metrics=metrics))

    return BenchmarkDetailResponse(
        benchmark_id=benchmark_id,
        default_sort_metric=default_sort_metric,
        pipelines=pipelines,
    )


def _collect_category_summary(records: List[Dict[str, Any]]) -> Dict[str, Dict[str, Dict[str, float]]]:
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
                            * math.sqrt(
                                (p * (1.0 - p) / n) + ((z * z) / (4.0 * n * n))
                            )
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

    return {"overall": to_avg(overall_metrics), "categories": {k: to_avg(v) for k, v in category_metrics.items()}}


@app.get(
    "/api/benchmarks/{benchmark_id}/summary/by-category",
    response_model=BenchmarkCategorySummaryResponse,
)
def get_benchmark_summary_by_category(
    benchmark_id: str,
    llm_judge_config_id: Optional[int] = Query(
        None, description="LLM judge config id for llm_score metrics"
    ),
) -> BenchmarkCategorySummaryResponse:
    """Return summary metrics overall and broken down by categories."""
    try:
        summary_raw = load_eval_summary(
            benchmark_id, llm_judge_config_id=llm_judge_config_id
        )
    except FileNotFoundError:
        summary_raw = {}

    llm_cfg = summary_raw.pop("llm_judge_config", None)
    summary_raw.pop("llm_judge_config_id", None)
    default_sort_metric = "subset_non_empty_execution_accuracy"
    if llm_cfg and isinstance(llm_cfg, dict):
        default_sort_metric = llm_cfg.get("default_sort_metric", default_sort_metric)

    try:
        records = load_eval_records(benchmark_id, llm_judge_config_id)
    except HTTPException:
        if not summary_raw:
            raise HTTPException(
                status_code=404, detail=_summary_not_found_detail(benchmark_id)
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
    agg = _collect_category_summary(records)

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


@app.get(
    "/api/benchmarks/{benchmark_id}/errors", response_model=PaginatedErrorResponse
)
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
    agree: bool = Query(
        False,
        description="If true and pipeline & pipeline2 set, filter where metric values match",
    ),
    category: Optional[str] = Query(
        None,
        description="Filter to records tagged with this SQL profile category (meta.categories)",
    ),
    failed_only: bool = Query(
        False,
        description="If true, include only records where selected pipeline has execution_accuracy == 0",
    ),
    llm_judge_config_id: Optional[int] = Query(
        None, description="LLM judge config id for llm_score metrics"
    ),
):
    """
    Paginated list of records for error analysis with simple single- and cross-pipeline filters.
    """
    data = load_eval_records(benchmark_id, llm_judge_config_id)

    def match_search(rec: Dict[str, Any]) -> bool:
        if not q:
            return True
        q_lower = q.lower()
        rid = str(rec.get("id") or rec.get("question_id") or "")
        question = (
            rec.get("page_content")
            or rec.get("question")
            or rec.get("utterance", "")
        )
        return q_lower in rid.lower() or q_lower in str(question).lower()

    def get_metric(rec: Dict[str, Any], pl: str, m: str) -> Optional[float]:
        preds = rec.get("predictions", {})
        if pl not in preds:
            return None
        eval_block = preds[pl].get("evaluation", {})
        val = eval_block.get(m)
        if isinstance(val, (int, float)):
            return float(val)
        return None

    def apply_op(lhs: Optional[float], rhs: float, operator: str) -> bool:
        if lhs is None:
            return False
        if operator == "eq":
            return lhs == rhs
        if operator == "ne":
            return lhs != rhs
        if operator == "lt":
            return lhs < rhs
        if operator == "gt":
            return lhs > rhs
        if operator == "le":
            return lhs <= rhs
        if operator == "ge":
            return lhs >= rhs
        return False

    filtered: List[Dict[str, Any]] = []
    for rec in data:
        if not match_search(rec):
            continue

        # Single-pipeline metric filter
        if pipeline and value is not None:
            mv = get_metric(rec, pipeline, metric)
            if not apply_op(mv, value, op):
                continue

        # Common "pipeline failed" view for drill-down screens
        if failed_only:
            if not pipeline:
                raise HTTPException(
                    status_code=400,
                    detail="pipeline is required when failed_only=true",
                )
            exec_acc = get_metric(rec, pipeline, "execution_accuracy")
            if exec_acc != 0:
                continue

        if category:
            cats = rec.get("meta", {}).get("categories", [])
            if category not in cats:
                continue

        # Cross-pipeline or same-pipeline metric comparison (agree / disagree / both present)
        if pipeline and metric2:
            pl2 = pipeline2 or pipeline
            m2 = metric2
            v1 = get_metric(rec, pipeline, metric)
            v2 = get_metric(rec, pl2, m2)
            if v1 is None or v2 is None:
                continue
            if disagree and v1 == v2:
                continue
            if agree and v1 != v2:
                continue

        filtered.append(rec)

    total = len(filtered)
    start = (page - 1) * page_size
    end = start + page_size
    page_items = filtered[start:end]

    items: List[ErrorRecordSummary] = []
    for rec in page_items:
        rid = str(rec.get("id") or rec.get("question_id") or "")
        question = (
            rec.get("page_content")
            or rec.get("question")
            or rec.get("utterance", "")
        )
        preds = rec.get("predictions", {})

        # Only expose evaluations; raw DFs can be large
        evals: Dict[str, Dict[str, Any]] = {}
        for pl, info in preds.items():
            evals[pl] = info.get("evaluation", {})

        items.append(
            ErrorRecordSummary(
                record_id=rid,
                question=str(question),
                predictions=evals,
            )
        )

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
def get_error_detail(
    benchmark_id: str,
    record_id: str,
    llm_judge_config_id: Optional[int] = Query(
        None, description="LLM judge config id for llm_score metrics"
    ),
):
    """
    Return full record for a given benchmark and record id for detailed error analysis.
    """
    return load_eval_record(benchmark_id, record_id, llm_judge_config_id)


@app.get(
    "/api/benchmarks/{benchmark_id}/errors/{record_id}/detail",
    response_model=Dict[str, Any],
)
def get_error_detail_for_pipeline(
    benchmark_id: str,
    record_id: str,
    pipeline: str = Query(..., description="Pipeline id to inspect"),
    llm_judge_config_id: Optional[int] = Query(
        None, description="LLM judge config id for llm_score metrics"
    ),
):
    """
    Return a normalized, UI-friendly detail payload for one record and one pipeline.
    """
    rec = load_eval_record(benchmark_id, record_id, llm_judge_config_id)
    rid = str(rec.get("id") or rec.get("question_id") or "")

    preds = rec.get("predictions", {})
    if pipeline not in preds:
        raise HTTPException(status_code=404, detail=f"Pipeline '{pipeline}' not found in record")
    pred = preds[pipeline]
    eval_metrics = pred.get("evaluation", {})

    gt_sql = rec.get("sql", [])
    if isinstance(gt_sql, str):
        gt_sql = [gt_sql]

    gt_df = rec.get("gt_df", [])
    if not isinstance(gt_df, list):
        gt_df = [gt_df]

    return {
        "record_id": rid,
        "pipeline": pipeline,
        "question": rec.get("question") or rec.get("utterance") or rec.get("page_content") or "",
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


def _resolve_record_db_id(
    benchmark_id: str, record_id: Optional[str], explicit_db_id: Optional[str]
) -> Optional[str]:
    if explicit_db_id:
        return explicit_db_id
    if not record_id:
        return None
    gold = _find_gold_record(benchmark_id, record_id)
    if gold is not None:
        return gold.get("db_id")
    return None


def _resolve_sqlite_db_path(db_folder: str, db_id: str) -> Path:
    db_filename = f"{db_id}.sqlite"
    folder_path = Path(db_folder)

    # Support both absolute db_folder and relative layouts.
    if folder_path.is_absolute():
        candidate = folder_path / db_id / db_filename
        if candidate.exists():
            return candidate

    candidates = [
        get_data_root() / folder_path / db_id / db_filename,
        get_data_root() / db_id / db_filename,
        get_writable_data_root() / folder_path / db_id / db_filename,
        Path.cwd() / "data" / folder_path / db_id / db_filename,
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate

    tried = ", ".join(str(p) for p in candidates)
    raise ValueError(f"SQLite DB does not exist. Tried: {tried}")


def _normalize_sql_for_dedupe(sql: str) -> str:
    return " ".join((sql or "").strip().rstrip(";").split()).lower()


def _get_ground_truth_sql_key(record: Dict[str, Any]) -> str:
    for key in ("sql", "SQL", "target", "query"):
        value = record.get(key)
        if isinstance(value, dict):
            continue
        if value is not None:
            return key
    return "sql"


def _load_gold_benchmark_data_list(benchmark_id: str) -> List[Dict[str, Any]]:
    try:
        return load_benchmark_records(benchmark_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


def _find_gold_record(
    benchmark_id: str, record_id: str
) -> Optional[Dict[str, Any]]:
    data = _load_gold_benchmark_data_list(benchmark_id)
    for rec in data:
        rid = str(rec.get("id") or rec.get("question_id") or "")
        if rid == record_id:
            return rec
    return None


def _find_eval_record_optional(
    benchmark_id: str, record_id: str
) -> Optional[Dict[str, Any]]:
    try:
        return load_eval_record(benchmark_id, record_id)
    except HTTPException:
        return None


def _split_df_shape_from_json(s: Any) -> Optional[Tuple[int, int]]:
    """Return (nrows, ncols) from pandas orient='split' JSON string."""
    if not isinstance(s, str) or not s.strip():
        return None
    try:
        o = json.loads(s)
        if not isinstance(o, dict):
            return None
        data = o.get("data")
        cols = o.get("columns")
        nrows = len(data) if isinstance(data, list) else 0
        ncols = len(cols) if isinstance(cols, list) else 0
        return (nrows, ncols)
    except Exception:
        return None


def _gt_row_counts_from_eval_record(eval_rec: Dict[str, Any]) -> List[int]:
    """Row counts from serialized gt_df(s) on the eval record when present."""
    out: List[int] = []
    gt_df = eval_rec.get("gt_df")
    if isinstance(gt_df, str):
        shape = _split_df_shape_from_json(gt_df)
        if shape is not None:
            out.append(shape[0])
    elif isinstance(gt_df, list):
        for item in gt_df:
            if isinstance(item, str):
                shape = _split_df_shape_from_json(item)
                out.append(shape[0] if shape is not None else 0)
    return out


def _gt_dfs_from_eval_record(eval_rec: Dict[str, Any]) -> List[str]:
    """Serialized gt_df JSON strings (orient=split) from the eval record when present."""
    out: List[str] = []
    gt_df = eval_rec.get("gt_df")
    if isinstance(gt_df, str):
        out.append(gt_df)
    elif isinstance(gt_df, list):
        for item in gt_df:
            if isinstance(item, str):
                out.append(item)
    return out


async def _playground_execute_sql_guarded(
    benchmark_id: str,
    sql: str,
    db_id: Optional[str],
    timeout_s: int,
    error_label: str,
) -> Any:
    """Run SQL on the benchmark DB; map failures to HTTP errors like execute_sql_for_record."""
    try:
        df, _db_type = await _execute_sql_for_benchmark(
            benchmark_id=benchmark_id,
            sql=sql,
            db_id=db_id,
            timeout_s=timeout_s,
        )
        return df
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=f"{error_label}: {e}",
        ) from e
    except asyncio.TimeoutError:
        raise HTTPException(
            status_code=408,
            detail=f"{error_label}: SQL execution timed out after {timeout_s}s",
        )
    except Exception as e:
        logger.exception("Playground SQL execution failed")
        raise HTTPException(
            status_code=500,
            detail=f"{error_label}: SQL execution failed: {e}",
        ) from e


async def _execute_sql_for_benchmark(
    benchmark_id: str, sql: str, db_id: Optional[str], timeout_s: int
) -> tuple[Any, str]:
    benchmark_info = get_benchmark_info(benchmark_id)
    db_engine = benchmark_info.get("db_engine", {})
    db_type = db_engine.get("db_type")
    if not db_type:
        raise ValueError("Missing db_type in benchmark config")

    if db_type == "sqlite":
        db_folder = db_engine.get("db_folder")
        if not db_folder:
            raise ValueError("Missing sqlite db_folder in benchmark config")
        if not db_id:
            raise ValueError(
                "db_id is required for sqlite benchmarks. Provide record_id or db_id."
            )
        db_path = _resolve_sqlite_db_path(db_folder, db_id)
        df = await run_sqlite_query_with_timeout(db_path, sql, timeout_s)
        return df, db_type

    if db_type == "postgres":
        import asyncpg

        schema_name = db_engine.get("schema_name")
        if not schema_name:
            raise ValueError("Missing postgres schema_name in benchmark config")
        connection_string = os.getenv(db_engine.get("connection_string_env_var", ""))
        if not connection_string:
            raise ValueError("Missing postgres connection string environment variable")

        fixed_sql = quote_mixed_case_columns(sql)
        pool = await asyncpg.create_pool(
            dsn=connection_string,
            min_size=1,
            max_size=1,
            server_settings={"search_path": schema_name},
        )
        try:
            df = await run_sql_and_get_dataframe_async(pool, schema_name, fixed_sql, timeout_s)
        finally:
            await pool.close()
        return df, db_type

    if db_type == "mysql":
        connection_string = os.getenv(db_engine.get("connection_string_env_var", ""))
        if not connection_string:
            raise ValueError("Missing MySQL connection string environment variable")
        normalized_conn_str, connect_args = normalize_mysql_connection_string(
            connection_string
        )
        fixed_sql = quote_mysql_identifiers(sql)
        df = await run_sql_and_get_dataframe_mysql_async(
            normalized_conn_str, connect_args, db_id, fixed_sql, timeout=timeout_s
        )
        return df, db_type

    if db_type == "db2":
        import pandas as pd
        from text2sql_eval_toolkit.execution.execution_tools import _require_ibm_db

        schema_name = db_engine.get("schema_name")
        connection_string = os.getenv(db_engine.get("connection_string_env_var", ""))
        if not connection_string:
            raise ValueError("Missing DB2 connection string environment variable")

        fixed_sql = _normalize_sql_for_db2(sql)

        def _run_db2_query() -> Any:
            ibm_db = _require_ibm_db()
            conn = ibm_db.connect(connection_string, "", "")
            try:
                if schema_name:
                    ibm_db.exec_immediate(conn, f"SET CURRENT SCHEMA {schema_name}")
                stmt = ibm_db.prepare(conn, fixed_sql)
                try:
                    ibm_db.set_option(
                        stmt, {ibm_db.SQL_ATTR_QUERY_TIMEOUT: timeout_s}, 0
                    )
                except Exception:
                    pass
                ok = ibm_db.execute(stmt)
                rows: List[Any] = []
                cols: List[str] = []
                if ok and ibm_db.num_fields(stmt) > 0:
                    ncols = ibm_db.num_fields(stmt)
                    cols = [ibm_db.field_name(stmt, i) for i in range(ncols)]
                    tup = ibm_db.fetch_tuple(stmt)
                    while tup:
                        rows.append(tup)
                        tup = ibm_db.fetch_tuple(stmt)
                ibm_db.free_stmt(stmt)
                return pd.DataFrame(rows, columns=cols)
            finally:
                ibm_db.close(conn)

        df = await asyncio.wait_for(
            asyncio.to_thread(_run_db2_query), timeout=timeout_s + 5
        )
        return df, db_type

    if db_type == "presto":
        import pandas as pd
        import prestodb

        connection_string = os.getenv(db_engine.get("connection_string_env_var", ""))
        if not connection_string:
            raise ValueError("Missing Presto connection string environment variable")
        connect_kwargs = _parse_presto_sqlalchemy_url(connection_string)
        fixed_sql = quote_mixed_case_columns(sql)

        def _run_presto_query() -> Any:
            conn = prestodb.dbapi.connect(**connect_kwargs)
            try:
                cur = conn.cursor()
                cur.execute(fixed_sql)
                rows = cur.fetchall() or []
                cols = [d[0] for d in (cur.description or [])]
                cur.close()
                return pd.DataFrame(rows, columns=cols)
            finally:
                conn.close()

        df = await asyncio.wait_for(asyncio.to_thread(_run_presto_query), timeout=timeout_s)
        return df, db_type

    raise ValueError(f"Unsupported db_type '{db_type}'")


@app.post(
    "/api/benchmarks/{benchmark_id}/execute",
    response_model=ExecuteSqlResponse,
)
async def execute_sql_for_record(
    benchmark_id: str, req: ExecuteSqlRequest
) -> ExecuteSqlResponse:
    sql = (req.sql or "").strip()
    if not sql:
        raise HTTPException(status_code=400, detail="sql is required")

    timeout_s = req.timeout_s or 90
    if timeout_s < 1 or timeout_s > 600:
        raise HTTPException(status_code=400, detail="timeout_s must be between 1 and 600")

    db_id = _resolve_record_db_id(benchmark_id, req.record_id, req.db_id)
    started = time.perf_counter()
    try:
        df, db_type = await _execute_sql_for_benchmark(
            benchmark_id=benchmark_id,
            sql=sql,
            db_id=db_id,
            timeout_s=timeout_s,
        )
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except asyncio.TimeoutError as e:
        raise HTTPException(
            status_code=408,
            detail=f"SQL execution timed out after {timeout_s}s",
        ) from e
    except Exception as e:
        logger.exception("SQL execution failed")
        raise HTTPException(status_code=500, detail=f"SQL execution failed: {e}") from e

    elapsed_ms = (time.perf_counter() - started) * 1000.0
    result_payload = json.loads(df.to_json(orient="split"))
    return ExecuteSqlResponse(
        benchmark_id=benchmark_id,
        db_type=db_type,
        sql=sql,
        db_id=db_id,
        execution_time_ms=round(elapsed_ms, 2),
        row_count=int(df.shape[0]),
        column_count=int(df.shape[1]),
        result=result_payload,
    )


@app.post(
    "/api/benchmarks/{benchmark_id}/ground-truth-sql",
    response_model=AddGroundTruthSqlResponse,
)
def add_ground_truth_sql(
    benchmark_id: str, req: AddGroundTruthSqlRequest
) -> AddGroundTruthSqlResponse:
    record_id = (req.record_id or "").strip()
    sql = (req.sql or "").strip()
    if not record_id:
        raise HTTPException(status_code=400, detail="record_id is required")
    if not sql:
        raise HTTPException(status_code=400, detail="sql is required")

    try:
        data = load_benchmark_records(benchmark_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    target_record: Optional[Dict[str, Any]] = None
    for rec in data:
        rid = str(rec.get("id") or rec.get("question_id") or "")
        if rid == record_id:
            target_record = rec
            break

    if target_record is None:
        raise HTTPException(status_code=404, detail="Record not found in benchmark data")

    sql_key = _get_ground_truth_sql_key(target_record)
    current_value = target_record.get(sql_key)
    if current_value is None:
        sql_list: List[str] = []
    elif isinstance(current_value, list):
        sql_list = [str(v) for v in current_value if isinstance(v, str)]
    elif isinstance(current_value, str):
        sql_list = [current_value]
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot extend non-string SQL field '{sql_key}'",
        )

    normalized_existing = {_normalize_sql_for_dedupe(s) for s in sql_list}
    normalized_new = _normalize_sql_for_dedupe(sql)
    added = normalized_new not in normalized_existing
    if added:
        sql_list.append(sql)
        target_record[sql_key] = sql_list
        try:
            save_gold_records(benchmark_id, data)
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Failed to save benchmark data: {e}"
            ) from e

    return AddGroundTruthSqlResponse(
        benchmark_id=benchmark_id,
        record_id=record_id,
        added=added,
        message=(
            "Query added to ground truth SQLs"
            if added
            else "Query already exists in ground truth SQLs"
        ),
        ground_truth_count=len(sql_list),
    )


@app.get(
    "/api/evaluation-metric-definitions",
    response_model=EvaluationMetricDefinitionsResponse,
)
def evaluation_metric_definitions() -> EvaluationMetricDefinitionsResponse:
    payload = get_metric_definitions_payload()
    return EvaluationMetricDefinitionsResponse(
        groups=payload["groups"],
        metrics=payload["metrics"],
    )


@app.get(
    "/api/benchmarks/{benchmark_id}/record-ids",
    response_model=RecordIdsResponse,
)
def list_benchmark_record_ids(benchmark_id: str) -> RecordIdsResponse:
    data = _load_gold_benchmark_data_list(benchmark_id)
    items: List[RecordIdItem] = []
    for rec in data:
        rid = str(rec.get("id") or rec.get("question_id") or "")
        if not rid.strip():
            continue
        q = (
            rec.get("page_content")
            or rec.get("question")
            or rec.get("utterance", "")
        )
        items.append(RecordIdItem(record_id=rid, question=str(q)))
    return RecordIdsResponse(benchmark_id=benchmark_id, items=items)


@app.get(
    "/api/benchmarks/{benchmark_id}/playground/{record_id}",
    response_model=PlaygroundInitResponse,
)
def get_playground_init(benchmark_id: str, record_id: str) -> PlaygroundInitResponse:
    gold = _find_gold_record(benchmark_id, record_id)
    if gold is None:
        raise HTTPException(status_code=404, detail="Record not found in benchmark data")

    rec_copy = deepcopy(gold)
    try:
        gt_sqls = get_gt_sqls(rec_copy)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    question = get_question(rec_copy)
    rid = str(gold.get("id") or gold.get("question_id") or "")

    pipelines: List[PipelinePlaygroundInfo] = []
    gt_row_counts: List[int] = []
    gt_dfs_list: List[str] = []
    eval_rec = _find_eval_record_optional(benchmark_id, rid)
    if eval_rec:
        gt_row_counts = _gt_row_counts_from_eval_record(eval_rec)
        gt_dfs_list = _gt_dfs_from_eval_record(eval_rec)
        preds = eval_rec.get("predictions", {})
        if isinstance(preds, dict):
            for name, pred in sorted(preds.items()):
                if not isinstance(pred, dict):
                    continue
                ev_raw = pred.get("evaluation")
                ev_dict = ev_raw if isinstance(ev_raw, dict) else None
                pred_err: Optional[str] = None
                for k in ("sql_execution_error", "inference_error"):
                    v = pred.get(k)
                    if isinstance(v, str) and v.strip():
                        pred_err = v
                        break
                pr: Optional[int] = None
                pc: Optional[int] = None
                pd_raw = pred.get("predicted_df")
                predicted_df_str = pd_raw if isinstance(pd_raw, str) else None
                if isinstance(pd_raw, str):
                    shape = _split_df_shape_from_json(pd_raw)
                    if shape is not None:
                        pr, pc = shape[0], shape[1]
                pipelines.append(
                    PipelinePlaygroundInfo(
                        name=name,
                        predicted_sql=pred.get("predicted_sql"),
                        has_prompt=bool(pred.get("prompt")),
                        has_agent_trace=bool(pred.get("agent_trace")),
                        evaluation=ev_dict,
                        prediction_error=pred_err,
                        prediction_row_count=pr,
                        prediction_column_count=pc,
                        predicted_df=predicted_df_str,
                    )
                )

    return PlaygroundInitResponse(
        benchmark_id=benchmark_id,
        record_id=rid,
        question=str(question),
        db_id=gold.get("db_id"),
        ground_truth_sqls=list(gt_sqls),
        pipelines=pipelines,
        ground_truth_row_counts=gt_row_counts,
        ground_truth_dfs=gt_dfs_list,
    )


@app.post(
    "/api/benchmarks/{benchmark_id}/playground/evaluate",
    response_model=PlaygroundEvaluateResponse,
)
async def playground_evaluate(
    benchmark_id: str, req: PlaygroundEvaluateRequest
) -> PlaygroundEvaluateResponse:
    gold = _find_gold_record(benchmark_id, req.record_id)
    if gold is None:
        raise HTTPException(status_code=404, detail="Record not found in benchmark data")

    sqls = [s.strip() for s in req.ground_truth_sqls if s and str(s).strip()]
    if not sqls:
        raise HTTPException(
            status_code=400,
            detail="ground_truth_sqls must contain at least one non-empty SQL",
        )

    pred_sql = (req.predicted_sql or "").strip()
    if not pred_sql:
        raise HTTPException(status_code=400, detail="predicted_sql is required")

    timeout_s = req.timeout_s or 90
    if timeout_s < 1 or timeout_s > 600:
        raise HTTPException(status_code=400, detail="timeout_s must be between 1 and 600")

    db_id = gold.get("db_id")
    record = deepcopy(gold)
    record["sql"] = sqls

    gt_dfs_json: List[str] = []
    gt_row_counts: List[int] = []
    for i, sql in enumerate(sqls):
        label = f"Ground truth SQL #{i + 1}"
        df = await _playground_execute_sql_guarded(
            benchmark_id, sql, db_id, timeout_s, label
        )
        gt_dfs_json.append(df.to_json(orient="split"))
        gt_row_counts.append(int(df.shape[0]))

    record["gt_df"] = gt_dfs_json

    pred_err: Optional[str] = None
    pred_rows: Optional[int] = None
    pred_cols: Optional[int] = None
    pred_df_json: str
    try:
        pdf = await _playground_execute_sql_guarded(
            benchmark_id, pred_sql, db_id, timeout_s, "Predicted SQL"
        )
        pred_df_json = pdf.to_json(orient="split")
        pred_rows = int(pdf.shape[0])
        pred_cols = int(pdf.shape[1])
    except HTTPException as e:
        detail = e.detail
        pred_err = detail if isinstance(detail, str) else str(detail)
        pred_df_json = '{"columns":[],"index":[],"data":[]}'

    prediction: Dict[str, Any] = {
        "predicted_sql": pred_sql,
        "predicted_df": pred_df_json,
    }
    if pred_err is not None:
        prediction["sql_execution_error"] = pred_err

    merge_pl = (req.merge_pipeline or "").strip()
    if merge_pl:
        eval_rec = _find_eval_record_optional(benchmark_id, req.record_id)
        if eval_rec:
            preds = eval_rec.get("predictions", {})
            src = preds.get(merge_pl) if isinstance(preds, dict) else None
            if isinstance(src, dict):
                for k in (
                    "prompt",
                    "agent_trace",
                    "agent_reasoning",
                    "token_usage",
                    "inference_time_ms",
                    "execution_time_ms",
                    "logic_df",
                ):
                    if k in src and src[k] is not None:
                        prediction[k] = src[k]

    llm_cfg = None
    if req.use_llm:
        try:
            llm_cfg = load_llm_judge_config(req.llm_judge_config_path)
        except FileNotFoundError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

    def _run_eval() -> Dict[str, Any]:
        return evaluate_prediction(
            record,
            prediction,
            llm_judge_config=llm_cfg,
            force_rerun_llm_judge=req.force_rerun_llm_judge,
        )

    evaluation = await asyncio.to_thread(_run_eval)

    return PlaygroundEvaluateResponse(
        benchmark_id=benchmark_id,
        record_id=req.record_id,
        evaluation=evaluation,
        ground_truth_row_counts=gt_row_counts,
        ground_truth_dfs=gt_dfs_json,
        predicted_df=pred_df_json,
        prediction_error=pred_err,
        prediction_row_count=pred_rows,
        prediction_column_count=pred_cols,
    )


@app.post("/api/sql/parse", response_model=SqlParseResponse)
def parse_sql_ast(req: SqlParseRequest) -> SqlParseResponse:
    """Parse SQL into a sqlglot AST tree for the eval playground."""
    dialect = benchmark_db_type_to_dialect(req.dialect or "postgres")
    mode = (req.parse_mode or "sqlglot").strip().lower()
    if mode not in PARSE_MODES:
        raise HTTPException(
            status_code=400,
            detail=f"parse_mode must be one of: {', '.join(PARSE_MODES)}",
        )
    payload = parse_sql_to_tree(req.sql, dialect=dialect, mode=mode)
    if not payload.get("ok"):
        return SqlParseResponse(
            ok=False,
            error=payload.get("error"),
            dialect=dialect,
            parse_mode=mode,
        )
    return SqlParseResponse(
        ok=True,
        dialect=payload.get("dialect"),
        parse_mode=payload.get("parse_mode"),
        tree=payload.get("tree"),
        visual_tree=payload.get("visual_tree"),
        formatted_sql=payload.get("formatted_sql"),
        analysis=payload.get("analysis"),
        analysis_error=payload.get("analysis_error"),
    )


@app.get("/api/compare", response_model=CompareResponse)
def compare_summaries(
    benchmark_id: str,
    left_id: str = Query(..., description="Left result id (usually same as benchmark)"),
    right_id: str = Query(
        ..., description="Right result id (usually same as benchmark)"
    ),
):
    """
    Compare two summary JSON files for the same benchmark.

    For now, we assume filenames follow the pattern
    {id}-predictions_eval_summary.json under the results dir.
    """
    try:
        left_raw = load_eval_summary(left_id)
        right_raw = load_eval_summary(right_id)
    except FileNotFoundError as exc:
        raise HTTPException(
            status_code=404,
            detail="Summary not found in database for one or both benchmarks.",
        ) from exc

    left_raw.pop("llm_judge_config", None)
    right_raw.pop("llm_judge_config", None)

    rows: List[CompareRow] = []
    pipelines = sorted(set(left_raw.keys()) | set(right_raw.keys()))
    numeric_keys = set()
    for src in (left_raw, right_raw):
        for pl, metrics in src.items():
            for k, v in metrics.items():
                if isinstance(v, dict) and "average" in v:
                    numeric_keys.add(k)

    for pl in pipelines:
        l_metrics = left_raw.get(pl, {})
        r_metrics = right_raw.get(pl, {})
        for metric in sorted(numeric_keys):
            l_val = l_metrics.get(metric, {}).get("average")
            r_val = r_metrics.get(metric, {}).get("average")
            diff = None
            if isinstance(l_val, (int, float)) and isinstance(r_val, (int, float)):
                diff = r_val - l_val
            rows.append(
                CompareRow(
                    pipeline=pl,
                    metric=metric,
                    left=l_val,
                    right=r_val,
                    diff=diff,
                )
            )

    return CompareResponse(
        benchmark_id=benchmark_id, left_id=left_id, right_id=right_id, rows=rows
    )


@app.get(
    "/api/benchmarks/{benchmark_id}/insights/binary-metric-confusion-by-pipeline",
    response_model=BinaryMetricConfusionByPipelineResponse,
)
def binary_metric_confusion_by_pipeline(
    benchmark_id: str,
    metric_a: str = Query(..., description="Metric key for dimension A"),
    metric_b: str = Query(..., description="Metric key for dimension B"),
    llm_judge_config_id: Optional[int] = Query(
        None, description="LLM judge config id for llm_score metrics"
    ),
):
    """
    For each pipeline, compute binary confusion counts for (A, B).
    Metrics are treated as binary (1 means success, anything else is 0).
    Only records where both metric values exist are counted.
    """
    data = load_eval_records(benchmark_id, llm_judge_config_id)

    per_pipeline_counts: Dict[str, Dict[str, int]] = {}
    per_pipeline_n: Dict[str, int] = {}

    def ensure(p: str) -> None:
        if p not in per_pipeline_counts:
            per_pipeline_counts[p] = {"a0b0": 0, "a0b1": 0, "a1b0": 0, "a1b1": 0}
            per_pipeline_n[p] = 0

    for rec in data:
        preds = rec.get("predictions", {})
        if not isinstance(preds, dict):
            continue

        for pipeline_id in preds.keys():
            a_val = get_pipeline_metric_value(rec, pipeline_id, metric_a)
            b_val = get_pipeline_metric_value(rec, pipeline_id, metric_b)
            if a_val is None or b_val is None:
                continue

            ensure(pipeline_id)
            a_bin = to_binary_metric(a_val)
            b_bin = to_binary_metric(b_val)
            if a_bin is None or b_bin is None:
                continue

            key = f"a{a_bin}b{b_bin}"
            per_pipeline_counts[pipeline_id][key] += 1
            per_pipeline_n[pipeline_id] += 1

    per_pipeline_rows: List[BinaryMetricConfusionByPipelineRow] = []
    for pipeline_id in sorted(per_pipeline_counts.keys()):
        counts = per_pipeline_counts[pipeline_id]
        n_valid = per_pipeline_n.get(pipeline_id, 0)

        if n_valid <= 0:
            rates = {"a0b0": 0.0, "a0b1": 0.0, "a1b0": 0.0, "a1b1": 0.0}
            agreement_rate = 0.0
        else:
            rates = {
                "a0b0": counts["a0b0"] / n_valid,
                "a0b1": counts["a0b1"] / n_valid,
                "a1b0": counts["a1b0"] / n_valid,
                "a1b1": counts["a1b1"] / n_valid,
            }
            agreement_rate = (counts["a0b0"] + counts["a1b1"]) / n_valid

        per_pipeline_rows.append(
            BinaryMetricConfusionByPipelineRow(
                pipeline=pipeline_id,
                counts=BinaryMetricConfusionCounts(**counts),
                n_valid=n_valid,
                rates=BinaryMetricConfusionRates(**rates),
                agreement_rate=agreement_rate,
                disagreement_rate=1.0 - agreement_rate if n_valid > 0 else 0.0,
            )
        )

    return BinaryMetricConfusionByPipelineResponse(
        benchmark_id=benchmark_id,
        metric_a=metric_a,
        metric_b=metric_b,
        per_pipeline=per_pipeline_rows,
    )


@app.get(
    "/api/benchmarks/{benchmark_id}/insights/cross-pipeline-binary-metric-confusion",
    response_model=CrossPipelineBinaryMetricConfusionResponse,
)
def cross_pipeline_binary_metric_confusion(
    benchmark_id: str,
    pipeline_left: str = Query(..., description="Left pipeline id"),
    pipeline_right: str = Query(..., description="Right pipeline id"),
    metric_left: str = Query("execution_accuracy", description="Metric key for left"),
    metric_right: Optional[str] = Query(
        None,
        description="Metric key for right (defaults to metric_left)",
    ),
    llm_judge_config_id: Optional[int] = Query(
        None, description="LLM judge config id for llm_score metrics"
    ),
):
    """
    Compute binary confusion counts across two pipelines for a (possibly)
    different metric. Metrics are treated as binary (1 means success).
    Only records where both metric values exist are counted.
    """
    metric_right_key = metric_right or metric_left
    data = load_eval_records(benchmark_id, llm_judge_config_id)

    counts = {
        "left0right0": 0,
        "left0right1": 0,
        "left1right0": 0,
        "left1right1": 0,
    }
    n_valid = 0

    for rec in data:
        l_val = get_pipeline_metric_value(rec, pipeline_left, metric_left)
        r_val = get_pipeline_metric_value(rec, pipeline_right, metric_right_key)
        if l_val is None or r_val is None:
            continue
        l_bin = to_binary_metric(l_val)
        r_bin = to_binary_metric(r_val)
        if l_bin is None or r_bin is None:
            continue

        if l_bin == 0 and r_bin == 0:
            counts["left0right0"] += 1
        elif l_bin == 0 and r_bin == 1:
            counts["left0right1"] += 1
        elif l_bin == 1 and r_bin == 0:
            counts["left1right0"] += 1
        else:
            counts["left1right1"] += 1

        n_valid += 1

    if n_valid <= 0:
        rates = {
            "left0right0": 0.0,
            "left0right1": 0.0,
            "left1right0": 0.0,
            "left1right1": 0.0,
        }
        agreement_rate = 0.0
    else:
        rates = {
            "left0right0": counts["left0right0"] / n_valid,
            "left0right1": counts["left0right1"] / n_valid,
            "left1right0": counts["left1right0"] / n_valid,
            "left1right1": counts["left1right1"] / n_valid,
        }
        agreement_rate = (counts["left0right0"] + counts["left1right1"]) / n_valid

    return CrossPipelineBinaryMetricConfusionResponse(
        benchmark_id=benchmark_id,
        left_id=pipeline_left,
        right_id=pipeline_right,
        metric_left=metric_left,
        metric_right=metric_right_key,
        n_valid=n_valid,
        counts=CrossPipelineBinaryMetricConfusionCounts(**counts),
        rates=CrossPipelineBinaryMetricConfusionRates(**rates),
        agreement_rate=agreement_rate,
        disagreement_rate=1.0 - agreement_rate if n_valid > 0 else 0.0,
    )


@app.get(
    "/api/benchmarks/{benchmark_id}/insights/profile-metric-correlations",
    response_model=ProfileMetricCorrelationsResponse,
)
def profile_metric_correlations(
    benchmark_id: str,
    pipeline: Optional[str] = Query(
        None,
        description="Pipeline id; defaults to the pipeline with most evaluated records",
    ),
    metric: str = Query(
        "subset_non_empty_execution_accuracy",
        description="Primary metric for charts and category associations",
    ),
    metrics: Optional[str] = Query(
        None,
        description=(
            "Comma-separated metric keys for the correlation matrix "
            "(primary metric is always included)"
        ),
    ),
    scatter_feature: Optional[str] = Query(
        None,
        description="Feature used for scatter sample points (default: first available)",
    ),
    scatter_limit: int = Query(
        400,
        ge=0,
        le=2000,
        description="Max scatter points returned for the selected feature",
    ),
    llm_judge_config_id: Optional[int] = Query(
        None, description="LLM judge config id for llm_score metrics"
    ),
):
    """
    Correlate SQL/question profiling signals with evaluation metrics for one pipeline.

    - Numeric ``meta.features`` → Pearson / Spearman vs each metric
    - Binary profile ``meta.categories`` → point-biserial (Pearson of indicator) and
      mean(metric | tag present) − mean(metric | tag absent)
    - Feature bins and category means for dashboard charts
    """
    data = load_eval_records(benchmark_id, llm_judge_config_id)
    metric_keys = _parse_metric_list(metrics, metric)

    pipeline_counts: Dict[str, int] = {}
    for rec in data:
        preds = rec.get("predictions", {})
        if not isinstance(preds, dict):
            continue
        for pipeline_id, block in preds.items():
            if isinstance(block, dict) and isinstance(block.get("evaluation"), dict):
                pipeline_counts[pipeline_id] = pipeline_counts.get(pipeline_id, 0) + 1

    available_pipelines = sorted(
        pipeline_counts.keys(), key=lambda p: (-pipeline_counts[p], p)
    )
    if not available_pipelines:
        raise HTTPException(
            status_code=404,
            detail=f"No evaluated pipelines found for benchmark '{benchmark_id}'.",
        )

    selected_pipeline = pipeline if pipeline in pipeline_counts else available_pipelines[0]

    # Lightweight per-record rows used for correlation / charts.
    rows: List[Dict[str, Any]] = []
    all_features_seen: set = set()
    all_categories_seen: set = set()

    for rec in data:
        metric_vals: Dict[str, float] = {}
        for mk in metric_keys:
            val = get_pipeline_metric_value(rec, selected_pipeline, mk)
            if val is not None:
                metric_vals[mk] = val
        if not metric_vals:
            continue

        meta = rec.get("meta") if isinstance(rec.get("meta"), dict) else {}
        features_raw = meta.get("features") if isinstance(meta.get("features"), dict) else {}
        categories_raw = meta.get("categories") if isinstance(meta.get("categories"), list) else []
        categories = sorted(
            {str(c) for c in categories_raw if c is not None and str(c).strip()}
        )
        features: Dict[str, float] = {}
        for fname in FEATURE_FIELDS:
            raw = features_raw.get(fname)
            if isinstance(raw, (int, float)):
                features[fname] = float(raw)
                all_features_seen.add(fname)
        if categories:
            all_categories_seen.update(categories)

        rows.append(
            {
                "metrics": metric_vals,
                "features": features,
                "categories": set(categories),
            }
        )

    n_records = len(rows)
    n_with_features = sum(1 for r in rows if r["features"])
    n_with_categories = sum(1 for r in rows if r["categories"])
    category_list = sorted(all_categories_seen)

    scatter_feature_resolved = scatter_feature
    if scatter_feature_resolved not in all_features_seen:
        scatter_feature_resolved = None
        for preferred in (
            "query_join_count",
            "query_table_count",
            "query_nested_count",
            "query_column_count",
        ):
            if preferred in all_features_seen:
                scatter_feature_resolved = preferred
                break
        if scatter_feature_resolved is None and all_features_seen:
            scatter_feature_resolved = sorted(all_features_seen)[0]

    feature_metric_pairs: Dict[Tuple[str, str], Tuple[List[float], List[float]]] = {}
    feature_bin_values: Dict[Tuple[str, str, float], List[float]] = {}
    category_means_values: Dict[Tuple[str, str], List[float]] = {}
    category_metric_values: Dict[Tuple[str, str], Tuple[List[float], List[float]]] = {
        (cat, mk): ([], []) for cat in category_list for mk in metric_keys
    }
    scatter_points: List[Dict[str, float]] = []

    for row in rows:
        metric_vals = row["metrics"]
        features = row["features"]
        category_set = row["categories"]

        for fname, fval in features.items():
            for mk, mval in metric_vals.items():
                xs, ys = feature_metric_pairs.setdefault((fname, mk), ([], []))
                xs.append(fval)
                ys.append(mval)
                feature_bin_values.setdefault((fname, mk, fval), []).append(mval)

        for mk, mval in metric_vals.items():
            for cat in category_set:
                category_means_values.setdefault((cat, mk), []).append(mval)
            for cat in category_list:
                with_vals, without_vals = category_metric_values[(cat, mk)]
                if cat in category_set:
                    with_vals.append(mval)
                else:
                    without_vals.append(mval)

        if (
            scatter_feature_resolved
            and scatter_feature_resolved in features
            and metric in metric_vals
            and len(scatter_points) < scatter_limit
        ):
            scatter_points.append(
                {
                    "x": features[scatter_feature_resolved],
                    "y": metric_vals[metric],
                }
            )

    feature_correlations: List[FeatureMetricCorrelation] = []
    for (fname, mk), (xs, ys) in sorted(feature_metric_pairs.items()):
        pearson_r, pearson_p, spearman_rho, spearman_p = _safe_corr_pair(xs, ys)
        feature_correlations.append(
            FeatureMetricCorrelation(
                feature=fname,
                metric=mk,
                n=len(xs),
                pearson_r=pearson_r,
                pearson_p=pearson_p,
                spearman_rho=spearman_rho,
                spearman_p=spearman_p,
            )
        )

    category_associations: List[CategoryMetricAssociation] = []
    for (cat, mk), (with_vals, without_vals) in sorted(category_metric_values.items()):
        mean_with = (sum(with_vals) / len(with_vals)) if with_vals else None
        mean_without = (sum(without_vals) / len(without_vals)) if without_vals else None
        delta = (
            mean_with - mean_without
            if mean_with is not None and mean_without is not None
            else None
        )
        indicators: List[float] = [1.0] * len(with_vals) + [0.0] * len(without_vals)
        metric_series: List[float] = list(with_vals) + list(without_vals)
        r, p, _, _ = _safe_corr_pair(indicators, metric_series)
        category_associations.append(
            CategoryMetricAssociation(
                category=cat,
                metric=mk,
                n_with=len(with_vals),
                n_without=len(without_vals),
                mean_with=mean_with,
                mean_without=mean_without,
                delta=delta,
                point_biserial_r=r,
                point_biserial_p=p,
            )
        )

    category_means: List[CategoryMetricMean] = []
    for (cat, mk), vals in sorted(category_means_values.items()):
        avg, lo, hi = _mean_and_ci95(vals)
        category_means.append(
            CategoryMetricMean(
                category=cat,
                metric=mk,
                average=avg,
                n=len(vals),
                ci95_low=lo,
                ci95_high=hi,
            )
        )

    feature_series: List[FeatureMetricSeries] = []
    for fname in sorted(all_features_seen):
        bins_map: Dict[float, List[float]] = {}
        for (f, mk, xval), vals in feature_bin_values.items():
            if f == fname and mk == metric:
                bins_map[xval] = vals
        bins = [
            FeatureMetricBin(
                x_value=xval,
                x_label=str(int(xval) if float(xval).is_integer() else xval),
                average=(sum(vals) / len(vals)) if vals else None,
                n=len(vals),
            )
            for xval, vals in sorted(bins_map.items())
        ]
        feature_series.append(
            FeatureMetricSeries(
                feature=fname,
                metric=metric,
                bins=bins,
                scatter=scatter_points if fname == scatter_feature_resolved else [],
            )
        )

    return ProfileMetricCorrelationsResponse(
        benchmark_id=benchmark_id,
        pipeline=selected_pipeline,
        metric=metric,
        metrics=metric_keys,
        n_records=n_records,
        n_with_features=n_with_features,
        n_with_categories=n_with_categories,
        feature_correlations=feature_correlations,
        category_associations=category_associations,
        category_means=category_means,
        feature_series=feature_series,
        available_pipelines=available_pipelines,
        available_features=sorted(all_features_seen),
        available_categories=sorted(all_categories_seen),
    )


@app.get(
    "/api/benchmarks/{benchmark_id}/llm-judge-configs",
    response_model=BenchmarkLlmJudgeConfigListResponse,
)
def list_benchmark_llm_judge_configs(
    benchmark_id: str,
) -> BenchmarkLlmJudgeConfigListResponse:
    """List LLM judge configurations that have stored results for this benchmark."""
    store = get_store(data_root=get_data_root())
    try:
        items_raw = store.list_llm_judge_configs_for_benchmark(benchmark_id)
    except Exception as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    items = [
        BenchmarkLlmJudgeConfig(
            id=item["id"],
            name=item["name"],
            model_id=item.get("model_id"),
        )
        for item in items_raw
    ]
    default_id = items[-1].id if items else None
    return BenchmarkLlmJudgeConfigListResponse(items=items, default_id=default_id)


@app.get("/api/llm-judge/configs", response_model=LLMJudgeConfigListResponse)
def list_llm_judge_configs() -> LLMJudgeConfigListResponse:
    """
    List available LLM-judge YAML config files.
    """
    from text2sql_eval_toolkit.evaluation import llm_as_judge

    base_dir = Path(llm_as_judge.__file__).parent / "llm_judge_config"
    items: List[LLMJudgeConfigInfo] = []
    if base_dir.exists():
        for path in sorted(base_dir.glob("*.yaml")):
            items.append(
                LLMJudgeConfigInfo(name=path.stem, path=str(path.resolve()))
            )
    return LLMJudgeConfigListResponse(items=items)


@app.get("/api/llm-judge/configs/{name}", response_model=Dict[str, Any])
def get_llm_judge_config(name: str):
    """
    Return the parsed YAML config by name (stem).
    """
    from text2sql_eval_toolkit.evaluation import llm_as_judge

    base_dir = Path(llm_as_judge.__file__).parent / "llm_judge_config"
    path = base_dir / f"{name}.yaml"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Config not found")

    import yaml

    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


@app.put("/api/llm-judge/configs/{name}", response_model=Dict[str, Any])
def update_llm_judge_config(name: str, body: Dict[str, Any] = Body(...)):
    """
    Overwrite a YAML config file with the provided structure.
    Performs minimal validation (must have model.id and prompt_template).
    """
    model_cfg = body.get("model") or {}
    if "id" not in model_cfg:
        raise HTTPException(status_code=400, detail="model.id is required")
    if "prompt_template" not in body:
        raise HTTPException(status_code=400, detail="prompt_template is required")

    from text2sql_eval_toolkit.evaluation import llm_as_judge

    base_dir = Path(llm_as_judge.__file__).parent / "llm_judge_config"
    base_dir.mkdir(parents=True, exist_ok=True)
    path = base_dir / f"{name}.yaml"

    import yaml

    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(body, f, sort_keys=False, allow_unicode=True)

    return body


@app.post("/api/benchmarks/{benchmark_id}/evaluate", response_model=JobStatus)
def evaluate_benchmark(benchmark_id: str, req: EvaluateRequest):
    """
    Trigger an evaluation run for a benchmark.
    The evaluation runs in a background thread; this endpoint returns a job id.
    """
    ensure_schema()
    job_type = db_jobs.resolve_eval_job_type(use_llm_judge=req.use_llm)
    job_params = {
        "use_llm_judge": req.use_llm,
        "llm_judge_config_path": req.llm_judge_config_path,
        "force_rerun": req.force_rerun,
        "force_rerun_llm_judge": req.force_rerun_llm_judge,
    }
    conn = get_connection()
    job_id = db_jobs.create_pending_job(
        conn, job_type, benchmark_id, params=job_params
    )
    job_row = get_store(data_root=get_data_root()).get_job(job_id)
    assert job_row is not None

    def worker():
        try:
            run_evaluation(
                benchmark_id,
                use_llm=req.use_llm,
                llm_judge_config_path=req.llm_judge_config_path,
                force_rerun_llm_judge=req.force_rerun_llm_judge or req.force_rerun,
                force_rerun=req.force_rerun,
                job_id=job_id,
            )
        except Exception:
            logger.exception("Evaluation job failed")

    threading.Thread(target=worker, daemon=True).start()
    return _job_status_from_row(job_row)


@app.get("/api/jobs/{job_id}", response_model=JobStatus)
def get_job_status(job_id: str) -> JobStatus:
    ensure_schema()
    job_row = get_store(data_root=get_data_root()).get_job(job_id)
    if not job_row:
        raise HTTPException(status_code=404, detail="Job not found")
    return _job_status_from_row(job_row)


@app.get("/api/benchmarks/{benchmark_id}/jobs", response_model=List[JobStatus])
def list_benchmark_jobs(
    benchmark_id: str,
    job_type: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
) -> List[JobStatus]:
    ensure_schema()
    rows = get_store(data_root=get_data_root()).list_jobs(
        benchmark_id=benchmark_id,
        job_type=job_type,
        limit=limit,
    )
    return [_job_status_from_row(row) for row in rows]


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


class ResultsFetchRequest(BaseModel):
    benchmarks: Optional[List[str]] = None
    pipelines: Optional[List[str]] = None
    models: Optional[List[str]] = None
    revision: Optional[str] = None
    force: bool = False


@app.post("/api/results/fetch", response_model=FetchJobStatus)
def start_results_fetch(req: ResultsFetchRequest = ResultsFetchRequest()) -> FetchJobStatus:
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


@app.get("/api/static/{file_path:path}")
def serve_dashboard_asset(file_path: str):
    data_root = get_data_root().resolve()
    candidate = (data_root / file_path).resolve()
    if data_root != candidate and data_root not in candidate.parents:
        raise HTTPException(status_code=403, detail="Forbidden path")
    if not candidate.exists() or not candidate.is_file():
        raise HTTPException(status_code=404, detail="Asset not found")
    return FileResponse(
        str(candidate),
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


def _resolve_dashboard_source_dir() -> Optional[Path]:
    """
    Location of the Vite project (directory containing package.json), if present.
    Prefer cwd (repo checkout) then package-relative path for editable installs.
    """
    candidates = [
        Path.cwd() / "dashboard",
        Path(__file__).resolve().parents[3] / "dashboard",
    ]
    for p in candidates:
        pkg = p / "package.json"
        if pkg.is_file():
            return p.resolve()
    return None


def _ensure_dashboard_dist(dashboard_dir: Path) -> None:
    """Ensure dist/index.html exists so StaticFiles can mount before the first watch rebuild."""
    dist_index = dashboard_dir / "dist" / "index.html"
    if dist_index.is_file():
        return
    npm = shutil.which("npm")
    if not npm:
        logger.warning(
            "npm not found on PATH; cannot build dashboard. Install Node.js/npm or run "
            "`cd dashboard && npm install && npm run build`."
        )
        return
    if not (dashboard_dir / "node_modules").is_dir():
        logger.info("dashboard/node_modules missing; running `npm install` in %s", dashboard_dir)
        install = subprocess.run(
            [npm, "install"],
            cwd=str(dashboard_dir),
        )
        if install.returncode != 0:
            logger.warning(
                "npm install failed (exit %s). Run `cd dashboard && npm install && npm run build` manually.",
                install.returncode,
            )
            return
    logger.info("No dashboard dist found; running one-time `npm run build` in %s", dashboard_dir)
    r = subprocess.run(
        [npm, "run", "build"],
        cwd=str(dashboard_dir),
    )
    if r.returncode != 0:
        logger.warning(
            "Dashboard build failed (exit %s). The UI may not load until you build successfully.",
            r.returncode,
        )


def _spawn_dashboard_watch(dashboard_dir: Path) -> Optional[subprocess.Popen]:
    """Run `vite build --watch` so dashboard/dist updates when sources change."""
    npm = shutil.which("npm")
    if not npm:
        logger.warning(
            "npm not found on PATH; skipping dashboard watch. Run `cd dashboard && npm run build` after edits."
        )
        return None
    if not (dashboard_dir / "node_modules").is_dir():
        logger.warning(
            "dashboard/node_modules missing; skipping dashboard watch. Run `cd dashboard && npm install`."
        )
        return None
    try:
        proc = subprocess.Popen(
            [npm, "run", "watch-build"],
            cwd=str(dashboard_dir),
        )
        logger.info(
            "Dashboard watch started (%s): Vite will rebuild dashboard/dist when sources change",
            dashboard_dir,
        )
        return proc
    except OSError as exc:
        logger.warning("Could not start dashboard watch: %s", exc)
        return None


def _terminate_dashboard_watch(proc: Optional[subprocess.Popen], *, timeout: float = 12.0) -> None:
    if proc is None or proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()


def mount_static(app: FastAPI) -> None:
    """
    Mount built frontend assets if available.

    We expect a Vite build under `dashboard/dist` at the project root.
    When installed as a package, these assets can be bundled as package data
    and looked up via importlib.resources instead; for now we focus on
    local development usage.
    """
    candidate_dirs = [
        Path.cwd() / "dashboard" / "dist",
        Path(__file__).resolve().parents[3] / "dashboard" / "dist",
    ]
    for static_dir in candidate_dirs:
        if static_dir.exists():
            app.mount(
                "/",
                StaticFiles(directory=str(static_dir), html=True),
                name="dashboard",
            )
            logger.info(f"Mounted dashboard static files from {static_dir}")
            return
    logger.info("No built dashboard assets found to mount")


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

    global _ENABLE_FETCH_ENDPOINT
    if args.enable_fetch:
        _ENABLE_FETCH_ENDPOINT = True
        logger.info(
            "Results fetch endpoint enabled.  "
            "POST /api/results/fetch is active."
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

