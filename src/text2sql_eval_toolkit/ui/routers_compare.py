#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Cross-pipeline comparison: side-by-side summaries and the binary-metric
confusion matrices behind the insights view.

All read-only and all served from the query index rather than by parsing
artifacts, which is what makes them viable on a public host.
"""

from typing import Dict, List, Optional

from fastapi import (
    APIRouter,
    HTTPException,
    Query,
)

import text2sql_eval_toolkit.env_loader  # noqa: F401 — load .env (WATSONX_*, etc.) before eval/inference

from text2sql_eval_toolkit.ui.models import (
    BinaryMetricConfusionByPipelineResponse,
    BinaryMetricConfusionByPipelineRow,
    BinaryMetricConfusionCounts,
    BinaryMetricConfusionRates,
    CompareResponse,
    CompareRow,
    CrossPipelineBinaryMetricConfusionCounts,
    CrossPipelineBinaryMetricConfusionRates,
    CrossPipelineBinaryMetricConfusionResponse,
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

logger = get_logger(__name__)

router = APIRouter()


@router.get("/api/compare", response_model=CompareResponse)
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


@router.get(
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


@router.get(
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
