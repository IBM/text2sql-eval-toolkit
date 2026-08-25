import argparse
import asyncio
import base64
import json
import os
from contextlib import contextmanager
from copy import deepcopy
import re
import shutil
import subprocess
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fastapi import Body, FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.routing import get_route_path
from starlette.exceptions import HTTPException as StarletteHTTPException
from pydantic import BaseModel, ConfigDict, Field
import uvicorn

import text2sql_eval_toolkit.env_loader  # noqa: F401 — load .env (WATSONX_*, etc.) before eval/inference

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
from text2sql_eval_toolkit.evaluation.evaluation_tools import (
    evaluate_prediction,
    run_evaluation,
)
from text2sql_eval_toolkit.evaluation.llm_as_judge import (
    evaluate_sql_prediction_with_llm,
    load_llm_judge_config,
)
from text2sql_eval_toolkit.evaluation.metric_definitions import (
    get_metric_definitions_payload,
)
from text2sql_eval_toolkit.indexing import is_stale as is_index_stale
from text2sql_eval_toolkit import __version__
from text2sql_eval_toolkit.ui import auth
from text2sql_eval_toolkit.ui.judge_budget import (
    BudgetExceeded,
    JudgeStore,
    judge_disabled,
    verdict_cache_key,
)
from text2sql_eval_toolkit.ui.capabilities import (
    Tier,
    parse_allowlist,
    required_tier,
    resolve_tier,
)
from text2sql_eval_toolkit.indexing.store import EvalIndex
from text2sql_eval_toolkit.logging import get_logger
from text2sql_eval_toolkit.utils import get_gt_sqls, get_question

logger = get_logger(__name__)

app = FastAPI(title="Text2SQL Evaluation Dashboard API")

# When True the /api/results/fetch endpoints are active.  Set by main() via
# the --enable-fetch CLI flag.  Off by default so production deployments are
# safe without any configuration.
_ENABLE_FETCH_ENDPOINT: bool = False


def _cookie_secure(mode: Tier) -> bool:
    """Whether the session cookie carries `Secure`. Secure unless explicitly
    opted out for a local HTTP run."""
    raw = os.getenv("TEXT2SQL_COOKIE_SECURE")
    if raw is not None:
        return raw.strip().lower() in {"1", "true", "yes"}
    return mode is not Tier.FULL


def _mode_from_env() -> Tier:
    """
    Deployment ceiling from the environment.

    Resolved at import, not only inside main(), because the ASGI app is also
    served directly (``uvicorn text2sql_eval_toolkit.ui.server:app``). Reading it
    only in main() meant such a deployment silently ran at FULL -- every
    endpoint, including SQL execution, open to anonymous callers -- while the
    operator believed TEXT2SQL_DASHBOARD_MODE had taken effect.
    """
    raw = os.getenv("TEXT2SQL_DASHBOARD_MODE")
    if not raw:
        return Tier.FULL
    try:
        return Tier.parse(raw)
    except ValueError:
        logger.warning(
            "TEXT2SQL_DASHBOARD_MODE=%r is not a valid mode; refusing to guess "
            "and falling back to the most restrictive setting.",
            raw,
        )
        return Tier.PUBLIC


# Deployment ceiling. FULL is the local-operator default so a plain
# `text2sql-eval-dashboard` keeps every capability it has today; a public
# deployment lowers it and no sign-in can raise it back.
_MODE: Tier = _mode_from_env()

# Emails allowed to reach the judge tier, from TEXT2SQL_JUDGE_ALLOWLIST.
_JUDGE_ALLOWLIST: set = parse_allowlist(os.getenv("TEXT2SQL_JUDGE_ALLOWLIST"))


def get_mode() -> Tier:
    return _MODE


def set_mode(mode: Tier) -> None:
    global _MODE
    _MODE = mode


def get_judge_allowlist() -> set:
    return _JUDGE_ALLOWLIST


def set_judge_allowlist(allowlist: set) -> None:
    global _JUDGE_ALLOWLIST
    _JUDGE_ALLOWLIST = allowlist


def current_user_email(request: Request) -> Optional[str]:
    """
    Identity for the request.

    Sign-in lands in a later step; until then only the local operator exists,
    and FULL mode does not consult identity at all.
    """
    # request.session raises unless SessionMiddleware is installed, and it is
    # not in local mode, so check the scope rather than the property.
    session = request.scope.get("session")
    if isinstance(session, dict):
        email = session.get("email")
        if isinstance(email, str):
            return email
    return None


def _route_template(request: Request) -> Optional[str]:
    """
    The matched route's path template (``/api/benchmarks/{benchmark_id}/execute``).

    Returns None when nothing matches, which leaves the caller with the concrete
    path -- and therefore the fail-closed FULL default for mutating methods.
    """
    from starlette.routing import Match

    for route in app.router.routes:
        try:
            match, _ = route.matches(request.scope)
        except Exception:  # pragma: no cover - defensive
            continue
        if match is Match.FULL:
            return getattr(route, "path", None)
    return None


@app.middleware("http")
async def enforce_capability_tier(request: Request, call_next):
    """
    Single choke point for authorization.

    Applied as middleware rather than per-handler so a route added later is
    covered whether or not its author remembered. Undeclared mutating routes
    require FULL, so the failure mode is a locked door rather than an open one.
    """
    # Starlette routes on get_route_path(), which strips scope["root_path"].
    # Gating on the raw scope path instead meant that under a non-empty
    # root_path -- an app mounted at a sub-path, or uvicorn --root-path -- this
    # check returned early while the router still dispatched the handler,
    # skipping authorization entirely.
    path = get_route_path(request.scope)
    if not path.startswith("/api/"):
        return await call_next(request)

    # HTTP middleware runs before routing, so scope["route"] is not set yet;
    # resolve the template ourselves. Matching the template rather than the
    # concrete path means ids in the URL cannot be used to dodge a rule.
    template = _route_template(request) or path
    needed = required_tier(request.method, template)
    granted = resolve_tier(
        get_mode(), current_user_email(request), get_judge_allowlist()
    )

    if granted < needed:
        return JSONResponse(
            status_code=403,
            content={
                "detail": (
                    f"This endpoint requires the '{needed.name.lower()}' capability; "
                    f"this session has '{granted.name.lower()}'. "
                    "The public dashboard is read-only."
                )
            },
        )
    return await call_next(request)


# Allow local dev frontends by default
# The Vite dev server runs on a different port, so local development needs CORS
# with credentials. A shared deployment serves the UI from the same origin and
# needs neither -- and `allow_credentials` with a permissive origin list stops
# being theoretical once session cookies exist, so the allowance is withdrawn
# outside full mode by `configure_cors()` below.
_DEV_ORIGINS = [
    "http://localhost",
    "http://localhost:5173",
    "http://127.0.0.1:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_DEV_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def configure_cors(mode: "Tier") -> None:
    """
    Narrow CORS for shared deployments.

    Middleware cannot be removed once the app has started, so the instance is
    reconfigured in place: outside full mode the origin list is emptied and
    credentialed cross-origin requests are refused.
    """
    for entry in app.user_middleware:
        if entry.cls is not CORSMiddleware:
            continue
        kwargs = getattr(entry, "kwargs", None)
        if not isinstance(kwargs, dict):
            continue
        if mode is Tier.FULL:
            kwargs["allow_origins"] = list(_DEV_ORIGINS)
            kwargs["allow_credentials"] = True
        else:
            kwargs["allow_origins"] = []
            kwargs["allow_credentials"] = False
    # Clear rather than rebuild: Starlette refuses add_middleware() once the
    # stack exists, and eagerly rebuilding here made a later
    # add_middleware(SessionMiddleware) raise -- which crashed startup for every
    # deployment that configured Google sign-in. None lets it rebuild on demand
    # and keeps the ordering decision with whoever adds middleware last.
    app.middleware_stack = None


# In-process token buckets, keyed by client address. Adequate for a
# single-container deployment; a multi-replica setup would need shared state,
# and the reverse proxy should carry a limit of its own regardless.
_RATE_BUCKETS: Dict[str, Tuple[float, float]] = {}
_RATE_LOCK = threading.Lock()

#: Requests per second sustained, and the burst allowance above it.
RATE_LIMIT_RPS = float(os.getenv("TEXT2SQL_RATE_LIMIT_RPS", "20"))
RATE_LIMIT_BURST = float(os.getenv("TEXT2SQL_RATE_LIMIT_BURST", "60"))

#: Sign-in is cheaper to abuse than to serve, so it gets its own tighter bucket.
AUTH_RATE_LIMIT_RPS = float(os.getenv("TEXT2SQL_AUTH_RATE_LIMIT_RPS", "1"))
AUTH_RATE_LIMIT_BURST = float(os.getenv("TEXT2SQL_AUTH_RATE_LIMIT_BURST", "10"))


#: Peer addresses whose X-Forwarded-For header is believed. Empty by default:
#: a proxy *appends* to that header, so its leftmost value is whatever the
#: client sent. Honouring it unconditionally let anyone rotate the header to
#: get a fresh bucket per request, defeating the limit entirely and growing the
#: bucket map without bound.
TRUSTED_PROXIES = {
    addr.strip()
    for addr in os.getenv("TEXT2SQL_TRUSTED_PROXIES", "").split(",")
    if addr.strip()
}

#: Ceiling on tracked buckets, so the key space cannot grow without limit.
MAX_RATE_BUCKETS = 10_000


def _client_key(request: Request) -> str:
    client = request.client
    peer = client.host if client else "unknown"

    if peer in TRUSTED_PROXIES:
        forwarded = request.headers.get("x-forwarded-for")
        if forwarded:
            # Rightmost hop, which the trusted proxy appended itself; the
            # leftmost is client-supplied and therefore forgeable.
            return forwarded.split(",")[-1].strip() or peer
    return peer


def _take_token(key: str, rps: float, burst: float) -> bool:
    """Classic token bucket. Returns False when the caller is over budget."""
    if rps <= 0:
        return True
    now = time.monotonic()
    with _RATE_LOCK:
        if key not in _RATE_BUCKETS and len(_RATE_BUCKETS) >= MAX_RATE_BUCKETS:
            # First drop buckets that have refilled completely: they are
            # indistinguishable from a fresh one, so forgetting them costs
            # nothing.
            for stale, (t, ts) in list(_RATE_BUCKETS.items()):
                if min(burst, t + (now - ts) * rps) >= burst:
                    del _RATE_BUCKETS[stale]
            # If that was not enough, evict least-recently-seen until under the
            # cap. Sweeping alone is only a soft bound, and the key space is
            # attacker-influenced, so a hard ceiling is what is needed.
            if len(_RATE_BUCKETS) >= MAX_RATE_BUCKETS:
                for stale, _ in sorted(_RATE_BUCKETS.items(), key=lambda kv: kv[1][1])[
                    : len(_RATE_BUCKETS) - MAX_RATE_BUCKETS + 1
                ]:
                    del _RATE_BUCKETS[stale]
        tokens, last = _RATE_BUCKETS.get(key, (burst, now))
        tokens = min(burst, tokens + (now - last) * rps)
        if tokens < 1.0:
            _RATE_BUCKETS[key] = (tokens, now)
            return False
        _RATE_BUCKETS[key] = (tokens - 1.0, now)
        return True


def reset_rate_limits() -> None:
    with _RATE_LOCK:
        _RATE_BUCKETS.clear()


@app.middleware("http")
async def rate_limit(request: Request, call_next):
    """
    Coarse per-client rate limiting.

    Local mode is exempt: it is one operator on loopback, and throttling an
    interactive tool would be a regression for no benefit.
    """
    # Same root_path correction as the tier gate above: throttling must not
    # silently switch off on a sub-path deployment.
    path = get_route_path(request.scope)
    if get_mode() is Tier.FULL or not path.startswith("/api/"):
        return await call_next(request)

    is_auth = path.startswith("/api/auth/")
    rps = AUTH_RATE_LIMIT_RPS if is_auth else RATE_LIMIT_RPS
    burst = AUTH_RATE_LIMIT_BURST if is_auth else RATE_LIMIT_BURST
    key = f"{'auth' if is_auth else 'api'}:{_client_key(request)}"

    if not _take_token(key, rps, burst):
        return JSONResponse(
            status_code=429,
            content={"detail": "Too many requests. Please slow down."},
            headers={"Retry-After": "1"},
        )
    return await call_next(request)


@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    """
    Baseline response hardening.

    The dashboard is self-contained -- its own bundle, its own API, no third
    party scripts or frames -- so a restrictive policy costs nothing here and
    removes a class of injection. Carbon injects styles at runtime, hence
    'unsafe-inline' for style-src only.
    """
    response = await call_next(request)
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("Referrer-Policy", "same-origin")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault(
        "Content-Security-Policy",
        "default-src 'self'; "
        "img-src 'self' data:; "
        "style-src 'self' 'unsafe-inline'; "
        "script-src 'self'; "
        "connect-src 'self'; "
        "frame-ancestors 'none'; "
        "base-uri 'self'; "
        "form-action 'self'",
    )
    return response


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
    import json

    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


# Record counts keyed by (path, size, mtime_ns).  The landing page asks for every
# benchmark's count on each request, and these files are megabytes each; the
# fingerprint means an edited file is recounted while an unchanged one is not.
_RECORD_COUNT_CACHE: Dict[Tuple[str, int, int], int] = {}
_RECORD_COUNT_LOCK = threading.Lock()


def _count_records_uncached(data_path: Any) -> int:
    import json

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


class EvaluationMetricDefinitionsResponse(BaseModel):
    groups: List[str]
    metrics: List[Dict[str, Any]]


class JobStatus(BaseModel):
    job_id: str
    benchmark_id: str
    status: str
    error: Optional[str] = None


JOBS: Dict[str, JobStatus] = {}
JOBS_LOCK = threading.Lock()


class FetchJobStatus(BaseModel):
    job_id: str
    state: str  # queued | running | completed | failed
    bytes_downloaded: int = 0
    total_bytes: int = 0
    error: Optional[str] = None


FETCH_JOBS: Dict[str, FetchJobStatus] = {}
FETCH_JOBS_LOCK = threading.Lock()

# Open index handles, keyed by benchmark.  These hold a read-only SQLite
# connection and a path, not parsed records, so this map stays small no matter how
# large the artifacts are.
EVAL_INDEX_CACHE: Dict[str, EvalIndex] = {}
EVAL_INDEX_LOCK = threading.Lock()
# Per-benchmark build locks, so a burst of requests for an unbuilt index results
# in one build rather than one per request.
_INDEX_BUILD_LOCKS: Dict[str, threading.Lock] = {}


def _update_job(job: JobStatus) -> None:
    with JOBS_LOCK:
        JOBS[job.job_id] = job


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


# ---------------------------------------------------------------------------
# Authentication (Google OIDC)
# ---------------------------------------------------------------------------

_OAUTH: Any = None


def get_oauth() -> Any:
    """Lazily built so importing the module never requires OAuth credentials."""
    global _OAUTH
    if _OAUTH is None:
        _OAUTH = auth.build_oauth_client()
    return _OAUTH


@app.get("/api/auth/login")
async def auth_login(request: Request, next: str = Query("/")):
    """Start the Google sign-in redirect."""
    if not auth.is_configured():
        raise HTTPException(
            status_code=503,
            detail=(
                "Google sign-in is not configured on this server "
                "(GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET are unset)."
            ),
        )
    request.session["post_login_redirect"] = auth.safe_redirect_target(next)
    redirect_uri = str(request.url_for("auth_callback"))
    return await get_oauth().google.authorize_redirect(request, redirect_uri)


@app.get("/api/auth/callback", name="auth_callback")
async def auth_callback(request: Request):
    """
    Complete sign-in.

    Authlib verifies the ID token signature, issuer, audience, and nonce, and
    checks the PKCE and state values against the session. What is left for us is
    the claim that actually matters: the address must be *verified*.
    """
    if not auth.is_configured():
        raise HTTPException(status_code=503, detail="Google sign-in is not configured.")

    try:
        token = await get_oauth().google.authorize_access_token(request)
    except Exception as e:
        logger.warning("Sign-in failed during token exchange: %s", e)
        raise HTTPException(status_code=401, detail="Sign-in failed.") from None

    email = auth.extract_verified_email(token.get("userinfo") or {})
    if not email:
        raise HTTPException(
            status_code=403,
            detail=("Sign-in requires a Google account with a verified email address."),
        )

    request.session["email"] = email
    tier = resolve_tier(get_mode(), email, get_judge_allowlist())
    logger.info(
        "Sign-in for identity %s granted tier %s",
        auth.hash_identity(email),
        tier.name.lower(),
    )

    target = request.session.pop("post_login_redirect", "/")
    return RedirectResponse(url=target, status_code=303)


@app.post("/api/auth/logout")
def auth_logout(request: Request):
    """Clear the session. The cookie is the whole of the state."""
    session = request.scope.get("session")
    if isinstance(session, dict):
        session.clear()
    return {"signed_out": True}


# ---------------------------------------------------------------------------
# On-demand LLM-as-judge (judge tier)
# ---------------------------------------------------------------------------


def _judge_config_dir() -> Path:
    from text2sql_eval_toolkit.evaluation import llm_as_judge

    return Path(llm_as_judge.__file__).parent / "llm_judge_config"


def _resolve_judge_config_path(name: str) -> Path:
    """
    Path of a judge config, refusing anything that escapes the config directory.

    ``name`` arrives from a URL segment or request body. FastAPI will not match
    a raw ``/`` into a single path parameter, but percent-encoded separators and
    ``..`` segments are decoded before they reach here, so containment is
    asserted on the resolved path rather than assumed from the routing.
    """
    # Constrain the shape first: a plain stem cannot traverse, cannot name a
    # dotfile, and cannot be empty. Containment below stays as a second line of
    # defence rather than the only one.
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", name or ""):
        raise FileNotFoundError(name)
    base = _judge_config_dir().resolve()
    candidate = (base / f"{name}.yaml").resolve()
    if candidate.parent != base:
        raise FileNotFoundError(name)
    return candidate


def _load_judge_config_by_name(name: str) -> Dict[str, Any]:
    """
    Load a judge config by stem, refusing anything that escapes the config dir.

    The name arrives from a request body, so a traversal attempt must not be
    able to read or select an arbitrary YAML file.
    """
    candidate = _resolve_judge_config_path(name)
    if not candidate.is_file():
        raise FileNotFoundError(name)
    return load_llm_judge_config(str(candidate))


_JUDGE_STORE: Optional[JudgeStore] = None
_JUDGE_STORE_LOCK = threading.Lock()
# One judge call at a time. The ceiling is per month, but a burst of concurrent
# requests could each pass the check before any of them records spend, so the
# semaphore is what makes the check meaningful under load.
_JUDGE_SEMAPHORE = asyncio.Semaphore(1)


def get_judge_store() -> JudgeStore:
    global _JUDGE_STORE
    with _JUDGE_STORE_LOCK:
        if _JUDGE_STORE is None:
            _JUDGE_STORE = JudgeStore(get_data_root() / "judge" / "usage.sqlite")
        return _JUDGE_STORE


def reset_judge_store() -> None:
    """Drop the cached handle; used by tests and after a data-root change."""
    global _JUDGE_STORE
    with _JUDGE_STORE_LOCK:
        _JUDGE_STORE = None


class JudgeRequest(BaseModel):
    record_id: str
    pipeline: str
    config_name: Optional[str] = None


class JudgeUsage(BaseModel):
    month: str
    spent_usd: float
    budget_usd: float
    remaining_usd: float
    calls: int
    warning: bool


class JudgeResponse(BaseModel):
    benchmark_id: str
    record_id: str
    pipeline: str
    verdict: str
    score: Optional[float]
    explanation: Optional[str]
    model: str
    config_name: str
    cached: bool
    # Distinguishes this from the llm_score baked into the published snapshot.
    source: str = "on-demand"
    usage: Optional[JudgeUsage] = None


def _judge_usage_model(usage: Any) -> JudgeUsage:
    return JudgeUsage(
        month=usage.month,
        # Six places, not two: a single judge call costs a fraction of a cent,
        # and rounding that to 0.00 would make the meter look stuck. Enforcement
        # always uses the unrounded value from the store.
        spent_usd=round(usage.spent_usd, 6),
        budget_usd=usage.budget_usd,
        remaining_usd=round(usage.remaining_usd, 6),
        calls=usage.calls,
        warning=usage.warning,
    )


@app.post("/api/benchmarks/{benchmark_id}/judge", response_model=JudgeResponse)
async def judge_record(benchmark_id: str, req: JudgeRequest, request: Request):
    """
    Run LLM-as-judge for one (record, pipeline) pair.

    Deliberately narrow. The existing /evaluate endpoint re-evaluates an entire
    benchmark and writes back to the shared artifacts, which on a shared
    deployment would mean one user's re-run silently changing what every other
    visitor sees. Verdicts here are stored separately and attributed to the
    caller; the published numbers stay reproducible against the pinned snapshot.

    Needs no database: the judge reads question, SQL, and dataframes from the
    artifacts, so this works on a deployment that holds no DB credentials.
    """
    if judge_disabled():
        raise HTTPException(
            status_code=503,
            detail="LLM-as-judge is currently disabled on this server.",
        )

    store = get_judge_store()
    email = current_user_email(request) or "local-operator"
    user_hash = auth.hash_identity(email)

    config_name = req.config_name or "llm_judge_default_config"
    try:
        judge_config = _load_judge_config_by_name(config_name)
    except FileNotFoundError:
        raise HTTPException(
            status_code=404, detail=f"LLM judge config '{config_name}' not found"
        ) from None
    model = str((judge_config.get("model") or {}).get("id", "unknown"))

    cache_key = verdict_cache_key(
        benchmark_id, req.record_id, req.pipeline, config_name, model
    )
    cached = store.get_verdict(cache_key)
    if cached:
        return JudgeResponse(
            benchmark_id=benchmark_id,
            record_id=req.record_id,
            pipeline=req.pipeline,
            verdict=cached["verdict"],
            score=cached["score"],
            explanation=cached["explanation"],
            model=cached["model"],
            config_name=config_name,
            cached=True,
            usage=_judge_usage_model(store.usage()),
        )

    try:
        store.check_budget()
    except BudgetExceeded as exc:
        raise HTTPException(status_code=429, detail=str(exc)) from None

    # Index access can build an index; keep it off the event loop.
    index = await asyncio.to_thread(get_index, benchmark_id)
    record = await asyncio.to_thread(index.read_record, req.record_id)
    if record is None:
        raise HTTPException(status_code=404, detail="Record not found")

    predictions = record.get("predictions", {})
    if req.pipeline not in predictions:
        raise HTTPException(
            status_code=404, detail=f"Pipeline '{req.pipeline}' not found in record"
        )
    prediction = predictions[req.pipeline]

    gt_sqls = record.get("sql") or []
    if isinstance(gt_sqls, str):
        gt_sqls = [gt_sqls]
    gt_dfs = record.get("gt_df") or []
    if not isinstance(gt_dfs, list):
        gt_dfs = [gt_dfs]

    async with _JUDGE_SEMAPHORE:
        # Re-check inside the semaphore: a queued request may have exhausted the
        # budget while this one was waiting.
        try:
            store.check_budget()
        except BudgetExceeded as exc:
            raise HTTPException(status_code=429, detail=str(exc)) from None
        try:
            result = await asyncio.to_thread(
                evaluate_sql_prediction_with_llm,
                record.get("question")
                or record.get("utterance")
                or record.get("page_content")
                or "",
                gt_sqls[0] if gt_sqls else "",
                gt_dfs[0] if gt_dfs else "",
                prediction.get("predicted_sql") or "",
                prediction.get("predicted_df") or "",
                prediction.get("prompt") or "",
                judge_config,
            )
        except ValueError as exc:
            raise HTTPException(status_code=502, detail=f"LLM judge failed: {exc}")
        except Exception as exc:
            logger.exception("LLM judge call failed")
            raise HTTPException(
                status_code=502, detail=f"LLM judge failed: {exc}"
            ) from exc

    token_usage = result.get("token_usage") or {}
    store.record_spend(
        user_hash,
        model,
        int(token_usage.get("prompt_tokens") or 0),
        int(token_usage.get("completion_tokens") or 0),
    )
    if not token_usage:
        # Spend is metered from reported tokens; without them the ceiling would
        # quietly stop counting, so make the gap visible.
        logger.warning(
            "LLM judge returned no token usage for model %s; that call was not "
            "metered against the budget.",
            model,
        )

    store.put_verdict(
        cache_key,
        benchmark_id=benchmark_id,
        record_id=req.record_id,
        pipeline_id=req.pipeline,
        config_name=config_name,
        model=model,
        verdict=str(result.get("verdict", "N/A")),
        score=result.get("score"),
        explanation=result.get("explanation"),
        user_hash=user_hash,
    )

    usage = store.usage()
    if usage.warning:
        logger.warning(
            "LLM judge spend is at %.0f%% of the $%.2f monthly budget.",
            usage.fraction_used * 100,
            usage.budget_usd,
        )

    return JudgeResponse(
        benchmark_id=benchmark_id,
        record_id=req.record_id,
        pipeline=req.pipeline,
        verdict=str(result.get("verdict", "N/A")),
        score=result.get("score"),
        explanation=result.get("explanation"),
        model=model,
        config_name=config_name,
        cached=False,
        usage=_judge_usage_model(usage),
    )


class SessionInfo(BaseModel):
    """What the frontend needs to decide which actions to offer."""

    tier: str
    mode: str
    email: Optional[str] = None
    signed_in: bool = False
    can_run_judge: bool = False
    can_mutate: bool = False
    # Remaining budget, so a user sees the ceiling approaching rather than
    # meeting it as an opaque error.
    judge_usage: Optional["JudgeUsage"] = None


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


class DeploymentInfo(BaseModel):
    """What this server is serving, and how current it is."""

    mode: str
    toolkit_version: str
    # Which Hugging Face snapshot the results came from. A link shared today is
    # only interpretable months later if the reader can see which data it shows.
    data_revision: Optional[str] = None
    data_provisioned_at: Optional[str] = None
    # Pre-computed results, not a live evaluation. Worth stating outright.
    results_are_precomputed: bool = True
    sign_in_available: bool = False
    judge_available: bool = False


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


def _resolve_record_db_id(
    benchmark_id: str, record_id: Optional[str], explicit_db_id: Optional[str]
) -> Optional[str]:
    if explicit_db_id:
        return explicit_db_id
    if not record_id:
        return None
    return get_index(benchmark_id).record_db_id(record_id)


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
        # Editable install / repo checkout: .../src/text2sql_eval_toolkit/ui/server.py → repo root is parents[3]
        _here = Path(__file__).resolve()
        candidates.append(_here.parents[3] / "data" / rel_path)
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


def _load_gold_benchmark_data_list(benchmark_id: str) -> List[Dict[str, Any]]:
    try:
        path = _resolve_benchmark_data_path(benchmark_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    data = load_json(path)
    if not isinstance(data, list):
        raise HTTPException(status_code=500, detail="Invalid benchmark data format")
    return data


def _find_gold_record(benchmark_id: str, record_id: str) -> Optional[Dict[str, Any]]:
    data = _load_gold_benchmark_data_list(benchmark_id)
    for rec in data:
        rid = str(rec.get("id") or rec.get("question_id") or "")
        if rid == record_id:
            return rec
    return None


def _find_eval_record_optional(
    benchmark_id: str, record_id: str
) -> Optional[Dict[str, Any]]:
    eval_path = get_results_dir() / f"{benchmark_id}-predictions_eval.json"
    if not eval_path.exists():
        return None
    try:
        return get_index(benchmark_id).read_record(record_id)
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
            df = await run_sql_and_get_dataframe_async(
                pool, schema_name, fixed_sql, timeout_s
            )
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

        df = await asyncio.wait_for(
            asyncio.to_thread(_run_presto_query), timeout=timeout_s
        )
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

    # Warm the index off the event loop. The sync helpers below reach
    # get_index(), which builds the index when it is missing or stale -- seconds
    # on a large artifact -- and doing that inline in an async handler would
    # stall every other in-flight request. After this the helpers hit the cache.
    await asyncio.to_thread(get_index, benchmark_id)

    timeout_s = req.timeout_s or 90
    if timeout_s < 1 or timeout_s > 600:
        raise HTTPException(
            status_code=400, detail="timeout_s must be between 1 and 600"
        )

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
        raise HTTPException(
            status_code=404, detail="Record not found in benchmark data"
        )

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
        q = rec.get("page_content") or rec.get("question") or rec.get("utterance", "")
        items.append(RecordIdItem(record_id=rid, question=str(q)))
    return RecordIdsResponse(benchmark_id=benchmark_id, items=items)


@app.get(
    "/api/benchmarks/{benchmark_id}/playground/{record_id}",
    response_model=PlaygroundInitResponse,
)
def get_playground_init(benchmark_id: str, record_id: str) -> PlaygroundInitResponse:
    gold = _find_gold_record(benchmark_id, record_id)
    if gold is None:
        raise HTTPException(
            status_code=404, detail="Record not found in benchmark data"
        )

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
    # See execute_sql_for_record: keep a possible index build off the event loop.
    await asyncio.to_thread(get_index, benchmark_id)

    gold = _find_gold_record(benchmark_id, req.record_id)
    if gold is None:
        raise HTTPException(
            status_code=404, detail="Record not found in benchmark data"
        )

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
        raise HTTPException(
            status_code=400, detail="timeout_s must be between 1 and 600"
        )

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
    missing = [
        f"data/results/{left_id}-predictions_eval_summary.json"
        for _ in [None]
        if not left_path.exists()
    ] + [
        f"data/results/{right_id}-predictions_eval_summary.json"
        for _ in [None]
        if not right_path.exists()
    ]
    if missing:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Summary file(s) not found: {', '.join(missing)}. "
                "Download pre-computed results with: "
                "`text2sql-eval-toolkit results fetch`, "
                "or generate locally by running the evaluation pipeline."
            ),
        )

    left_raw = load_json(left_path)
    right_raw = load_json(right_path)
    left_raw.pop("llm_judge_config", None)
    right_raw.pop("llm_judge_config", None)

    rows: List[CompareRow] = []
    pipelines = sorted(set(left_raw.keys()) | set(right_raw.keys()))
    numeric_keys = set()
    for src in (left_raw, right_raw):
        for _pl, metrics in src.items():
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
    per_pipeline_counts = get_index(benchmark_id).binary_confusion_by_pipeline(
        metric_a, metric_b
    )
    per_pipeline_n: Dict[str, int] = {
        pipeline_id: sum(counts.values())
        for pipeline_id, counts in per_pipeline_counts.items()
    }

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
    counts = get_index(benchmark_id).cross_pipeline_binary_confusion(
        pipeline_left, metric_left, pipeline_right, metric_right_key
    )
    n_valid = sum(counts.values())

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
            items.append(LLMJudgeConfigInfo(name=path.stem, path=str(path.resolve())))
    return LLMJudgeConfigListResponse(items=items)


@app.get("/api/llm-judge/configs/{name}", response_model=Dict[str, Any])
def get_llm_judge_config(name: str):
    """
    Return the parsed YAML config by name (stem).
    """

    try:
        path = _resolve_judge_config_path(name)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Config not found") from None
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

    try:
        path = _resolve_judge_config_path(name)
    except FileNotFoundError:
        raise HTTPException(status_code=400, detail="Invalid config name") from None
    path.parent.mkdir(parents=True, exist_ok=True)

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


class ResultsFetchRequest(BaseModel):
    benchmarks: Optional[List[str]] = None
    pipelines: Optional[List[str]] = None
    models: Optional[List[str]] = None
    revision: Optional[str] = None
    force: bool = False


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


@app.get("/api/static/{file_path:path}")
def serve_dashboard_asset(file_path: str, request: Request):
    """
    Serve a benchmark logo.

    Deliberately scoped to one directory and to image types. This used to serve
    anything beneath the data root, which on a shared deployment would have
    exposed internal files that happen to live there -- notably
    ``judge/usage.sqlite`` -- to any anonymous visitor, because this is a GET
    and so runs at the public tier.

    Responses are validated with an ETag from the file's size and mtime, so an
    unchanged logo costs a 304 rather than a full body.
    """
    # The URL carries the full relative path ("benchmarks/logos/beaver.png"), so
    # resolve against the data root and then require the result to sit inside
    # the logos directory.
    data_root = get_data_root().resolve()
    allowed_root = (data_root / STATIC_ASSET_SUBDIR).resolve()
    candidate = (data_root / file_path).resolve()

    # Containment, then type: a logo directory should only yield images.
    if allowed_root not in candidate.parents:
        raise HTTPException(status_code=403, detail="Forbidden path")
    if candidate.suffix.lower() not in ALLOWED_LOGO_EXTENSIONS:
        raise HTTPException(status_code=403, detail="Forbidden asset type")
    if not candidate.is_file():
        raise HTTPException(status_code=404, detail="Asset not found")

    stat = candidate.stat()
    etag = f'W/"{stat.st_size:x}-{stat.st_mtime_ns:x}"'
    headers = {"ETag": etag, "Cache-Control": "no-cache, must-revalidate"}

    if request.headers.get("if-none-match") == etag:
        return Response(status_code=304, headers=headers)

    return FileResponse(str(candidate), headers=headers)


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
        logger.info(
            "dashboard/node_modules missing; running `npm install` in %s", dashboard_dir
        )
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
    logger.info(
        "No dashboard dist found; running one-time `npm run build` in %s", dashboard_dir
    )
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


def _terminate_dashboard_watch(
    proc: Optional[subprocess.Popen], *, timeout: float = 12.0
) -> None:
    if proc is None or proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()


class SPAStaticFiles(StaticFiles):
    """
    Static files with a single-page-app fallback.

    The dashboard uses real paths (``/b/{benchmark}/errors``) so links can be
    shared, but those paths exist only in the client router -- there is no such
    file on disk.  Without a fallback, opening a shared link or refreshing any
    view returns 404.  Unknown non-API paths therefore serve ``index.html`` and
    let the client resolve them.

    ``/api/*`` is deliberately excluded: an unknown API path must stay a real
    404 rather than silently returning an HTML page, which would turn a typo
    into a confusing parse error in the caller.
    """

    async def get_response(self, path: str, scope: Any) -> Any:
        try:
            return await super().get_response(path, scope)
        except StarletteHTTPException as exc:
            if exc.status_code != 404:
                raise
            request_path = scope.get("path", "")
            if request_path.startswith("/api/"):
                raise
            # Anything that looks like a missing asset should stay a 404 rather
            # than returning HTML with a JS or CSS content type.
            if Path(path).suffix:
                raise
            return await super().get_response("index.html", scope)


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
                SPAStaticFiles(directory=str(static_dir), html=True),
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
