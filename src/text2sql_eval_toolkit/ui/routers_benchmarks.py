#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Benchmark listing, configuration, and summaries.

Mixed tiers, which is the thing to keep in mind when editing: the reads are what
a public visitor browses, while creating or updating a benchmark rewrites the
registry and uploading a logo writes into the data root.  ``ROUTE_TIERS`` is
what separates them.
"""

import base64
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, HTTPException

import text2sql_eval_toolkit.env_loader  # noqa: F401 — load .env (WATSONX_*, etc.) before eval/inference

from text2sql_eval_toolkit.utils import get_benchmarks_info
from text2sql_eval_toolkit.evaluation.evaluation_tools import split_summary
from text2sql_eval_toolkit.ui.aliases import alias_map
from text2sql_eval_toolkit.ui.models import (
    BenchmarkCategorySummaryResponse,
    BenchmarkConfigResponse,
    BenchmarkDetailResponse,
    BenchmarkLogoUploadRequest,
    BenchmarkSummary,
    BenchmarksResponse,
    PipelineAliasesResponse,
    PipelineMetrics,
)

from text2sql_eval_toolkit.ui.indexes import get_index
from text2sql_eval_toolkit.ui.paths import (
    _summary_not_found_detail,
    count_records,
    get_data_root,
    get_results_dir,
    load_json,
)
from text2sql_eval_toolkit.ui.registry import (
    ALLOWED_LOGO_EXTENSIONS,
    MAX_LOGO_UPLOAD_BYTES,
    get_benchmark_registry_path,
    load_benchmark_registry,
    normalize_benchmark_config,
    normalize_benchmark_id,
    write_json_atomic,
)
from text2sql_eval_toolkit.logging import get_logger
from text2sql_eval_toolkit.ui.models import (
    CreateBenchmarkRequest,
    UpdateBenchmarkRequest,
)

logger = get_logger(__name__)

router = APIRouter()


@router.get("/api/benchmarks", response_model=BenchmarksResponse)
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

        # How many records this benchmark has.
        #
        # The index first, because it is the count of what a visitor can
        # actually browse and because it is the only source a deployment has:
        # the published snapshot ships `results/` and nothing else, so a public
        # host has no `benchmarks/*.json` to count and every benchmark showed
        # "0 records" on the landing page.
        try:
            num_records = get_index(benchmark_id).record_count()
        except Exception:
            # No index, or one this deployment may not build. Fall back to the
            # benchmark data file, which is what a local checkout has.
            data_path = info.get("benchmark_json_path")
            try:
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
                num_pipelines = len(split_summary(summary)[0])
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


@router.post("/api/benchmarks", response_model=BenchmarkConfigResponse)
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


@router.get(
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


@router.put("/api/benchmarks/{benchmark_id}", response_model=BenchmarkConfigResponse)
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


@router.post("/api/benchmarks/logo-upload", response_model=Dict[str, str])
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


@router.get(
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

    raw, llm_cfg = split_summary(load_json(summary_path))
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


@router.get(
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
        pipeline_ids = list(split_summary(load_json(summary_path))[0])
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


def _collect_metric_values(
    records: List[Dict[str, Any]],
) -> Tuple[
    Dict[str, Dict[str, List[float]]], Dict[str, Dict[str, Dict[str, List[float]]]]
]:
    """
    Gather every numeric metric value from parsed records, overall and per
    category.

    The dashboard does not call this -- it reads the same values out of the
    index, which is orders of magnitude cheaper. It is kept as the reference
    implementation the differential test compares against, because "the fast
    path returns what the slow path returned" is the only claim worth making
    about a rewrite like that.
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

    return overall_metrics, category_metrics


def _summarize_metric_values(
    overall_metrics: Dict[str, Dict[str, List[float]]],
    category_metrics: Dict[str, Dict[str, Dict[str, List[float]]]],
) -> Dict[str, Dict[str, Dict[str, float]]]:
    """
    Average, spread and 95% interval per pipeline and metric.

    Split from the gathering above so both the index path and the reference
    implementation run *this* code, rather than two copies that could drift.
    """

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


def _collect_category_summary(
    records: List[Dict[str, Any]],
) -> Dict[str, Dict[str, Dict[str, float]]]:
    """Reference implementation over parsed records. See ``_collect_metric_values``."""
    return _summarize_metric_values(*_collect_metric_values(records))


@router.get(
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

    summary_raw, llm_cfg = split_summary(load_json(summary_path))
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

    # Read from the index rather than by parsing the artifact. This used to
    # stream every record -- bounded in memory, but not in time: it parsed 880 MB
    # of Beaver JSON to collect a few tens of thousands of floats, and the page
    # took 14 seconds to load. The index already holds those floats, and now
    # holds the record categories too.
    agg = _summarize_metric_values(*get_index(benchmark_id).metric_values_by_category())

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
