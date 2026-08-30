#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
On-demand LLM-as-judge for a single record.

This is the one mutating capability a signed-in, allowlisted visitor gets on the
public deployment, which is why it is scoped as tightly as it is: one record at
a time, results returned to the caller rather than written into the shared
artifacts, metered against a monthly budget that survives restarts, and needing
no database -- so it is safe on a host that holds no database credentials at all.
"""

import asyncio
import re
import threading
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import (
    APIRouter,
    HTTPException,
    Request,
)

import text2sql_eval_toolkit.env_loader  # noqa: F401 — load .env (WATSONX_*, etc.) before eval/inference

from text2sql_eval_toolkit.evaluation.llm_as_judge import (
    evaluate_sql_prediction_with_llm,
    load_llm_judge_config,
)
from text2sql_eval_toolkit.ui import auth, runtime
from text2sql_eval_toolkit.ui.models import (
    JudgeRequest,
    JudgeResponse,
    JudgeUsage,
)
from text2sql_eval_toolkit.ui.judge_budget import (
    BudgetExceeded,
    JudgeStore,
    judge_disabled,
    verdict_cache_key,
)

from text2sql_eval_toolkit.ui.indexes import get_index
from text2sql_eval_toolkit.ui.paths import get_data_root
from text2sql_eval_toolkit.ui.runtime import current_user_email
from text2sql_eval_toolkit.logging import get_logger

logger = get_logger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# On-demand LLM-as-judge (judge tier)
# ---------------------------------------------------------------------------


def _judge_config_dir() -> Path:
    """
    The packaged judge configs, shipped read-only inside the installed package.
    """
    from text2sql_eval_toolkit.evaluation import llm_as_judge

    return Path(llm_as_judge.__file__).parent / "llm_judge_config"


def _user_judge_config_dir() -> Path:
    """
    Where configs written through the dashboard are kept.

    Not the package directory. Writing there needs the installed tree to be
    writable by the server process, which it generally is not -- in a container
    the package is root-owned and the app runs unprivileged, and even where the
    permissions happen to allow it, a pip upgrade discards the edit. The data
    root is the writable, persistent location the deployment already mounts.
    """
    return get_data_root() / "llm_judge_config"


def _validate_config_name(name: str) -> str:
    """
    Accept a config name, or raise ``FileNotFoundError``.

    A plain stem cannot traverse, cannot name a dotfile, and cannot be empty.
    Callers still assert containment on the resolved path, so this stays the
    first line of defence rather than the only one.
    """
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", name or ""):
        raise FileNotFoundError(name)
    return name


def _contained(base: Path, name: str) -> Path:
    """
    ``base/name.yaml``, asserted to still be directly inside *base*.
    """
    base = base.resolve()
    candidate = (base / f"{name}.yaml").resolve()
    if candidate.parent != base:
        raise FileNotFoundError(name)
    return candidate


def _judge_config_write_path(name: str) -> Path:
    """
    Where a write of *name* lands: always the user directory, never the package.
    """
    return _contained(_user_judge_config_dir(), _validate_config_name(name))


def _resolve_judge_config_path(name: str) -> Path:
    """
    Path a judge config is read from, refusing anything that escapes its
    directory.

    A user copy shadows the packaged config of the same name, so editing a
    shipped config through the dashboard takes effect without the packaged file
    ever being touched -- and deleting the copy restores the original. A name
    with no user copy resolves to the packaged path, whether or not that file
    exists; the caller reports the miss.

    ``name`` arrives from a URL segment or request body. FastAPI will not match
    a raw ``/`` into a single path parameter, but percent-encoded separators and
    ``..`` segments are decoded before they reach here, so containment is
    asserted on the resolved path rather than assumed from the routing.
    """
    _validate_config_name(name)
    override = _contained(_user_judge_config_dir(), name)
    if override.is_file():
        return override
    return _contained(_judge_config_dir(), name)


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


def _user_api_key(email: Optional[str], model: str) -> Optional[str]:
    """
    The signed-in caller's stored key for this model's provider, if any.

    Returns ``None`` for an anonymous caller, when no key store is configured, or
    when nothing is stored -- in which case the request falls back to the
    server-held credential exactly as before.
    """
    store = runtime.get_user_key_store()
    if store is None or not email:
        return None
    provider = model.split(":", 1)[0] if ":" in model else ""
    if not provider:
        return None
    try:
        return store.reveal_for_request(email, provider)
    except Exception:  # pragma: no cover - a key problem must not fail the run
        logger.warning("Could not read a stored key; using the server credential")
        return None


@router.post("/api/benchmarks/{benchmark_id}/judge", response_model=JudgeResponse)
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
        benchmark_id, req.record_id, req.pipeline, config_name, model, judge_config
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
                # The caller's own key when they have stored one, so the request
                # bills their provider account rather than the server's. Tier
                # decided whether they may run a judge at all; this only decides
                # who pays.
                api_key=_user_api_key(email, model),
            )
        except ValueError as exc:
            raise HTTPException(
                status_code=502, detail=f"LLM judge failed: {exc}"
            ) from exc
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
