import argparse
import asyncio
import base64
import json
import os
import re
import shutil
import subprocess
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field
import uvicorn

from text2sql_eval_toolkit.utils import get_benchmarks_info
from text2sql_eval_toolkit.utils import (
    get_benchmark_info,
    get_benchmarks_file_path,
    BENCHMARKS_FILE,
)
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
from text2sql_eval_toolkit.evaluation.evaluation_tools import run_evaluation
from text2sql_eval_toolkit.logging import get_logger


logger = get_logger(__name__)

app = FastAPI(title="Text2SQL Evaluation Dashboard API")

# Allow local dev frontends by default
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost", "http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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


def load_json(path: Path) -> Any:
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    import json

    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def count_records(data_path: Any) -> int:
    """
    Count benchmark records from either a pathlib path or an
    importlib.resources traversable path-like object.
    """
    import json

    if data_path is None:
        return 0

    # importlib.resources Traversable paths expose open()
    if hasattr(data_path, "open"):
        with data_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
            return len(data) if isinstance(data, list) else 0

    # Fallback to regular filesystem path
    p = Path(str(data_path))
    if not p.exists():
        return 0
    with p.open("r", encoding="utf-8") as f:
        data = json.load(f)
        return len(data) if isinstance(data, list) else 0


ALLOWED_DB_TYPES = {"sqlite", "postgres", "mysql", "db2", "presto"}
ALLOWED_LOGO_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp", ".gif", ".svg"}
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


class LLMJudgeConfigInfo(BaseModel):
    name: str
    path: str


class LLMJudgeConfigListResponse(BaseModel):
    items: List[LLMJudgeConfigInfo]


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


class JobStatus(BaseModel):
    job_id: str
    benchmark_id: str
    status: str
    error: Optional[str] = None


JOBS: Dict[str, JobStatus] = {}
JOBS_LOCK = threading.Lock()

# Cache loaded evaluation records to avoid repeatedly parsing large JSON artifacts.
EVAL_RECORDS_CACHE: Dict[str, List[Dict[str, Any]]] = {}
EVAL_RECORDS_LOCK = threading.Lock()


def _update_job(job: JobStatus) -> None:
    with JOBS_LOCK:
        JOBS[job.job_id] = job


def load_eval_records(benchmark_id: str) -> List[Dict[str, Any]]:
    """
    Load {benchmark_id}-predictions_eval.json as a list of records.
    Uses an in-memory cache for performance.
    """
    with EVAL_RECORDS_LOCK:
        cached = EVAL_RECORDS_CACHE.get(benchmark_id)
        if cached is not None:
            return cached

    eval_path = get_results_dir() / f"{benchmark_id}-predictions_eval.json"
    if not eval_path.exists():
        raise HTTPException(status_code=404, detail="Full evaluation results not found")

    data = load_json(eval_path)
    if not isinstance(data, list):
        raise HTTPException(status_code=500, detail="Invalid evaluation JSON format")

    with EVAL_RECORDS_LOCK:
        EVAL_RECORDS_CACHE[benchmark_id] = data
        return data


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

        items.append(
            BenchmarkSummary(
                benchmark_id=benchmark_id,
                name=name,
                description=description,
                db_type=db_type,
                num_records=num_records,
                num_pipelines=num_pipelines,
                logo=logo,
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


@app.get("/api/benchmarks/{benchmark_id}/config", response_model=BenchmarkConfigResponse)
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
        raise HTTPException(status_code=500, detail=f"Failed to store image: {e}") from e

    normalized_relative = relative_path.as_posix()
    version_token = str(int(time.time() * 1000))
    return {
        "logo": stored_name,
        "logo_url": f"/api/static/{normalized_relative}?v={version_token}",
        "path": normalized_relative,
    }


@app.get("/api/benchmarks/{benchmark_id}/summary", response_model=BenchmarkDetailResponse)
def get_benchmark_summary(benchmark_id: str) -> BenchmarkDetailResponse:
    """
    Return pipeline-level summary metrics for a benchmark
    similar to data/results/*-predictions_eval_summary.json.
    """
    summary_path = get_results_dir() / f"{benchmark_id}-predictions_eval_summary.json"
    if not summary_path.exists():
        raise HTTPException(status_code=404, detail="Summary not found")

    raw = load_json(summary_path)
    llm_cfg = raw.pop("llm_judge_config", None)
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
def get_benchmark_summary_by_category(benchmark_id: str) -> BenchmarkCategorySummaryResponse:
    """
    Return summary metrics overall and broken down by categories.
    """
    summary_path = get_results_dir() / f"{benchmark_id}-predictions_eval_summary.json"
    eval_path = get_results_dir() / f"{benchmark_id}-predictions_eval.json"

    if not summary_path.exists():
        raise HTTPException(status_code=404, detail="Summary not found")
    if not eval_path.exists():
        raise HTTPException(
            status_code=404,
            detail="Full evaluation results not found for category breakdown",
        )

    summary_raw = load_json(summary_path)
    llm_cfg = summary_raw.pop("llm_judge_config", None)
    default_sort_metric = "subset_non_empty_execution_accuracy"
    if llm_cfg and isinstance(llm_cfg, dict):
        default_sort_metric = llm_cfg.get("default_sort_metric", default_sort_metric)

    records = load_json(eval_path)
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
    failed_only: bool = Query(
        False,
        description="If true, include only records where selected pipeline has execution_accuracy == 0",
    ),
):
    """
    Paginated list of records for error analysis with simple single- and cross-pipeline filters.
    """
    eval_path = get_results_dir() / f"{benchmark_id}-predictions_eval.json"
    if not eval_path.exists():
        raise HTTPException(status_code=404, detail="Full evaluation results not found")

    data = load_json(eval_path)

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

        # Cross-pipeline disagreement filter
        if pipeline and pipeline2 and disagree:
            m2 = metric2 or metric
            v1 = get_metric(rec, pipeline, metric)
            v2 = get_metric(rec, pipeline2, m2)
            if v1 is None or v2 is None or v1 == v2:
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
def get_error_detail(benchmark_id: str, record_id: str):
    """
    Return full record for a given benchmark and record id for detailed error analysis.
    """
    eval_path = get_results_dir() / f"{benchmark_id}-predictions_eval.json"
    if not eval_path.exists():
        raise HTTPException(status_code=404, detail="Full evaluation results not found")

    data = load_json(eval_path)
    for rec in data:
        rid = str(rec.get("id") or rec.get("question_id") or "")
        if rid == record_id:
            return rec

    raise HTTPException(status_code=404, detail="Record not found")


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
    eval_path = get_results_dir() / f"{benchmark_id}-predictions_eval.json"
    if not eval_path.exists():
        raise HTTPException(status_code=404, detail="Full evaluation results not found")

    data = load_json(eval_path)
    for rec in data:
        rid = str(rec.get("id") or rec.get("question_id") or "")
        if rid != record_id:
            continue

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

    raise HTTPException(status_code=404, detail="Record not found")


def _resolve_record_db_id(
    benchmark_id: str, record_id: Optional[str], explicit_db_id: Optional[str]
) -> Optional[str]:
    if explicit_db_id:
        return explicit_db_id
    if not record_id:
        return None
    records = load_eval_records(benchmark_id)
    for rec in records:
        rid = str(rec.get("id") or rec.get("question_id") or "")
        if rid == record_id:
            return rec.get("db_id")
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
        Path(BENCHMARKS_FILE).parent / folder_path / db_id / db_filename,
        Path.cwd() / "data" / folder_path / db_id / db_filename,
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate

    tried = ", ".join(str(p) for p in candidates)
    raise ValueError(f"SQLite DB does not exist. Tried: {tried}")


def _resolve_benchmark_data_path(benchmark_id: str) -> Path:
    benchmark_info = get_benchmark_info(benchmark_id)
    rel_data = benchmark_info.get("data")
    explicit_path = benchmark_info.get("benchmark_json_path")

    candidates: List[Path] = []
    if isinstance(rel_data, str):
        rel_path = Path(rel_data)
        candidates.append(get_data_root() / rel_path)
        candidates.append(Path.cwd() / "data" / rel_path)
    if explicit_path:
        candidates.append(Path(str(explicit_path)))

    for candidate in candidates:
        if candidate.exists():
            return candidate

    tried = ", ".join(str(p) for p in candidates)
    raise ValueError(f"Benchmark data file not found. Tried: {tried}")


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
        data_path = _resolve_benchmark_data_path(benchmark_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    try:
        with data_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to read benchmark data file: {e}"
        ) from e

    if not isinstance(data, list):
        raise HTTPException(status_code=500, detail="Invalid benchmark data format")

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
            with data_path.open("w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                f.write("\n")
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Failed to write benchmark data file: {e}"
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
    results_dir = get_results_dir()
    left_path = results_dir / f"{left_id}-predictions_eval_summary.json"
    right_path = results_dir / f"{right_id}-predictions_eval_summary.json"
    if not left_path.exists() or not right_path.exists():
        raise HTTPException(status_code=404, detail="One or both summary files not found")

    left_raw = load_json(left_path)
    right_raw = load_json(right_path)
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
):
    """
    For each pipeline, compute binary confusion counts for (A, B).
    Metrics are treated as binary (1 means success, anything else is 0).
    Only records where both metric values exist are counted.
    """
    data = load_eval_records(benchmark_id)

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
):
    """
    Compute binary confusion counts across two pipelines for a (possibly)
    different metric. Metrics are treated as binary (1 means success).
    Only records where both metric values exist are counted.
    """
    metric_right_key = metric_right or metric_left
    data = load_eval_records(benchmark_id)

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
        logger.warning(
            "dashboard/node_modules missing; run `cd dashboard && npm install && npm run build`."
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
    args = parser.parse_args(argv)

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

