#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
"""
Re-run evaluation metrics on existing predictions without inference or SQL execution.

Refreshes execution accuracy, SQL equivalence (sqlglot, sqlparse, exact match), and
related fields from stored ``predicted_sql`` / ``predicted_df`` values. Does not
call the model or re-execute SQL on the database.

By default LLM-as-judge is skipped. Use ``--preserve-llm-judge`` to recompute other
metrics while keeping cached ``llm_score`` / ``llm_explanation`` from the output file.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from pathlib import Path
from typing import Optional, Tuple

from text2sql_eval_toolkit import env_loader  # noqa: F401 — load .env when present
from text2sql_eval_toolkit.evaluation.evaluation_tools import async_evaluate_predictions
from text2sql_eval_toolkit.utils import (
    add_summary_csv_suffix,
    add_summary_json_suffix,
    get_benchmark_info,
    get_default_eval_filename,
)


def _resolve_paths(
    input_file: Path,
    output_file: Optional[Path],
    summary_file: Optional[Path],
    csv_summary_file: Optional[Path],
    *,
    in_place: bool,
) -> Tuple[Path, Path, str, str]:
    """Resolve input/output/summary paths for metrics-only re-evaluation."""
    input_path = input_file.resolve()
    if not input_path.is_file():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    stem = input_path.stem
    is_eval_input = stem.endswith("_eval") or stem.endswith("-predictions_eval")

    if output_file is not None:
        out_path = output_file.resolve()
    elif in_place or is_eval_input:
        out_path = input_path
    else:
        out_path = Path(get_default_eval_filename(str(input_path))).resolve()

    summary_path = (
        summary_file.resolve()
        if summary_file
        else Path(add_summary_json_suffix(str(out_path))).resolve()
    )
    csv_path = (
        csv_summary_file.resolve()
        if csv_summary_file
        else Path(add_summary_csv_suffix(str(out_path))).resolve()
    )
    return out_path, summary_path, str(csv_path), str(input_path)


def _paths_from_benchmark(benchmark_id: str, is_test: bool) -> Tuple[Path, Path, Path, Path]:
    info = get_benchmark_info(benchmark_id, is_test=is_test)
    predictions = Path(info["predictions_path"]).resolve()
    eval_out = Path(info["eval_results_path"]).resolve()
    summary = Path(info["eval_summary_path"]).resolve()
    csv_summary = Path(add_summary_csv_suffix(str(eval_out))).resolve()
    return predictions, eval_out, summary, csv_summary


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Re-run evaluation metrics on existing predictions (no inference, no SQL execution). "
            "Updates sqlglot_equivalence, execution accuracy, and related metrics from stored outputs."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # By benchmark id (reads *-predictions.json, writes *-predictions_eval.json)
  python scripts/evaluation/rerun_metrics.py bird_mini_dev_sqlite

  # By file path
  python scripts/evaluation/rerun_metrics.py \\
    --input-file data/results/my_benchmark-predictions.json

  # Refresh metrics in-place on an existing eval file
  python scripts/evaluation/rerun_metrics.py \\
    --input-file data/results/my_benchmark-predictions_eval.json --in-place

  # Recompute metrics but keep cached LLM judge scores
  python scripts/evaluation/rerun_metrics.py bird_mini_dev_sqlite --preserve-llm-judge
""",
    )
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument(
        "benchmark_id",
        nargs="?",
        default=None,
        help="Benchmark id from data/benchmarks.json (uses configured predictions path)",
    )
    target.add_argument(
        "--input-file",
        type=Path,
        help="Path to *-predictions.json or *-predictions_eval.json",
    )

    parser.add_argument(
        "--output-file",
        type=Path,
        help="Evaluated JSON output (default: <input>_eval.json, or in-place for *_eval.json)",
    )
    parser.add_argument("--summary-file", type=Path, help="Summary JSON path (optional)")
    parser.add_argument("--csv-summary-file", type=Path, help="Summary CSV path (optional)")
    parser.add_argument(
        "--in-place",
        action="store_true",
        help="Write metrics back to the input file (use with *_eval.json)",
    )
    parser.add_argument(
        "--test-benchmark",
        action="store_true",
        help="Resolve benchmark_id from data/test-benchmarks.json",
    )
    parser.add_argument(
        "--preserve-llm-judge",
        action="store_true",
        help=(
            "Re-run non-LLM metrics but reuse cached llm_score/llm_explanation from the "
            "output eval file (implies --use-llm-judge)."
        ),
    )
    parser.add_argument(
        "--use-llm-judge",
        "--use_llm_judge",
        action="store_true",
        help="Include LLM-as-judge (calls the LLM unless cached and not forced)",
    )
    parser.add_argument(
        "--force-rerun-llm-judge",
        "--force_rerun_llm_judge",
        action="store_true",
        help="Re-call LLM-as-judge even when cached scores exist",
    )
    parser.add_argument(
        "--llm-judge-config-path",
        "--llm_judge_config_path",
        help="YAML config for LLM-as-judge (optional; uses toolkit default if omitted)",
    )
    parser.add_argument(
        "--max-concurrency",
        type=int,
        default=16,
        metavar="N",
        help="Concurrent evaluation tasks (default: 16)",
    )

    args = parser.parse_args(argv)

    use_llm = args.use_llm_judge or args.preserve_llm_judge
    force_rerun_llm = args.force_rerun_llm_judge
    if args.preserve_llm_judge and force_rerun_llm:
        parser.error("--preserve-llm-judge and --force-rerun-llm-judge are mutually exclusive")

    llm_judge_config = None
    if use_llm:
        from text2sql_eval_toolkit.evaluation.llm_as_judge import load_llm_judge_config

        llm_judge_config = load_llm_judge_config(args.llm_judge_config_path)

    try:
        if args.input_file is not None:
            input_path = args.input_file
            output_path, summary_path, csv_path, input_str = _resolve_paths(
                input_path,
                args.output_file,
                args.summary_file,
                args.csv_summary_file,
                in_place=args.in_place,
            )
            label = input_str
        else:
            predictions, output_path, summary_path, csv_path = _paths_from_benchmark(
                args.benchmark_id,
                is_test=args.test_benchmark,
            )
            input_str = str(predictions)
            label = args.benchmark_id
            if args.output_file is not None:
                output_path = args.output_file.resolve()
            if args.in_place:
                output_path = predictions

        print("Re-running metrics only (no inference, no SQL execution)")
        print(f"  Target:     {label}")
        print(f"  Input:      {input_str}")
        print(f"  Output:     {output_path}")
        print(f"  Summary:    {summary_path}")
        print(f"  CSV:        {csv_path}")
        print(f"  LLM judge:  {'on' if use_llm else 'off'}", end="")
        if use_llm:
            if force_rerun_llm:
                print(" (force re-run)")
            elif args.preserve_llm_judge:
                print(" (preserve cached scores)")
            else:
                print(" (cached when available)")
        else:
            print()

        start = time.time()
        asyncio.run(
            async_evaluate_predictions(
                input_file=input_str,
                output_file=str(output_path),
                summary_file=str(summary_path),
                csv_summary_file=str(csv_path),
                llm_judge_config=llm_judge_config,
                max_concurrency=max(1, args.max_concurrency),
                force_rerun_llm_judge=force_rerun_llm,
                force_rerun=False,
            )
        )
        elapsed = time.time() - start
        print(f"Done in {elapsed:.1f}s")
        return 0
    except FileNotFoundError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
