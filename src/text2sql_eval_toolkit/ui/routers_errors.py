#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
The error-analysis listing and per-record detail.

The busiest read path in the dashboard, and the one that used to re-parse a
multi-hundred-megabyte artifact per request.  Everything here goes through the
query index: the listing filters and paginates in SQL, and a detail is a
byte-range read of the one record asked for.
"""

from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query

import text2sql_eval_toolkit.env_loader  # noqa: F401 — load .env (WATSONX_*, etc.) before eval/inference

from text2sql_eval_toolkit.ui.models import (
    ErrorRecordSummary,
    PaginatedErrorResponse,
)

from text2sql_eval_toolkit.ui.indexes import get_index
from text2sql_eval_toolkit.logging import get_logger
from text2sql_eval_toolkit.ui.dataframes import MAX_PREVIEW_ROWS, truncate_dataframe

logger = get_logger(__name__)

router = APIRouter()


@router.get(
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


@router.get(
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


@router.get(
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

    # Trimmed for display, with the true sizes alongside. One Beaver record
    # holds an 86,502-row ground truth and a 55,817-row prediction; sending both
    # built 854,563 DOM nodes to fill a 240-pixel scroll box.
    gt_previews = []
    gt_totals = []
    gt_truncated = False
    for frame in gt_df:
        preview, total, was_cut = truncate_dataframe(frame)
        gt_previews.append(preview)
        gt_totals.append(total)
        gt_truncated = gt_truncated or was_cut

    pred_preview, pred_total, pred_truncated = truncate_dataframe(
        pred.get("predicted_df")
    )

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
        "ground_truth_results": gt_previews,
        "ground_truth_result_row_counts": gt_totals,
        "ground_truth_results_truncated": gt_truncated,
        "predicted_result": pred_preview,
        "predicted_result_row_count": pred_total,
        "predicted_result_truncated": pred_truncated,
        "preview_row_limit": MAX_PREVIEW_ROWS,
        "prompt": pred.get("prompt"),
        "token_usage": pred.get("token_usage"),
        "inference_time_ms": pred.get("inference_time_ms"),
        "execution_time_ms": pred.get("execution_time_ms"),
        "llm_judge_score": eval_metrics.get("llm_score"),
        "llm_judge_explanation": eval_metrics.get("llm_explanation"),
        "sql_execution_error": pred.get("sql_execution_error"),
        "inference_error": pred.get("inference_error"),
    }
