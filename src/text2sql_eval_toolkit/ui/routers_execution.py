#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Running SQL: the execute endpoint and the evaluation playground.

Everything here is ``full`` tier and never runs on the public host -- these are
the routes that take caller-supplied SQL and run it against whatever database
credentials the server holds.  Grouping them in one module makes that boundary
visible: if a route in this file were ever reachable below ``full``, the
deployment would be executing arbitrary SQL for anonymous callers.

The tier table in ``ui.capabilities`` is what actually enforces that; this is
where to look to see what it is protecting.
"""

import asyncio
import json
import os
from copy import deepcopy
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, HTTPException

import text2sql_eval_toolkit.env_loader  # noqa: F401 — load .env (WATSONX_*, etc.) before eval/inference

from text2sql_eval_toolkit.utils import (
    get_benchmark_info,
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
)
from text2sql_eval_toolkit.evaluation.llm_as_judge import (
    load_llm_judge_config,
)
from text2sql_eval_toolkit.evaluation.metric_definitions import (
    get_metric_definitions_payload,
)
from text2sql_eval_toolkit.ui.models import (
    AddGroundTruthSqlRequest,
    AddGroundTruthSqlResponse,
    EvaluationMetricDefinitionsResponse,
    ExecuteSqlRequest,
    ExecuteSqlResponse,
    PipelinePlaygroundInfo,
    PlaygroundEvaluateRequest,
    PlaygroundEvaluateResponse,
    PlaygroundInitResponse,
    RecordIdItem,
    RecordIdsResponse,
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
from text2sql_eval_toolkit.utils import get_gt_sqls, get_question

logger = get_logger(__name__)

router = APIRouter()


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
        ) from None
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


@router.post(
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


@router.post(
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


@router.get(
    "/api/evaluation-metric-definitions",
    response_model=EvaluationMetricDefinitionsResponse,
)
def evaluation_metric_definitions() -> EvaluationMetricDefinitionsResponse:
    payload = get_metric_definitions_payload()
    return EvaluationMetricDefinitionsResponse(
        groups=payload["groups"],
        metrics=payload["metrics"],
    )


@router.get(
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


@router.get(
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


@router.post(
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
