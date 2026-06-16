#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
"""
Re-run evaluation metrics on existing predictions without inference or SQL execution.

Reads and writes results via SQLite for the given benchmark id.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from typing import Optional

from text2sql_eval_toolkit import env_loader  # noqa: F401 — load .env when present
from text2sql_eval_toolkit.evaluation.evaluation_tools import async_evaluate_predictions
from text2sql_eval_toolkit.evaluation.llm_as_judge import load_llm_judge_config


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Re-run evaluation metrics on existing predictions (no inference, no SQL execution). "
            "Results are stored in SQLite."
        ),
    )
    parser.add_argument(
        "benchmark_id",
        help="Benchmark id from data/benchmarks.json",
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
            "Re-run non-LLM metrics but reuse cached llm_score/llm_explanation "
            "(implies --use-llm-judge)."
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
    parser.add_argument(
        "--csv-summary-path",
        help="Optional path to write evaluation summary CSV",
    )

    args = parser.parse_args(argv)

    use_llm = args.use_llm_judge or args.preserve_llm_judge
    force_rerun_llm = args.force_rerun_llm_judge
    if args.preserve_llm_judge and force_rerun_llm:
        parser.error("--preserve-llm-judge and --force-rerun-llm-judge are mutually exclusive")

    llm_judge_config = None
    if use_llm:
        llm_judge_config = load_llm_judge_config(args.llm_judge_config_path)

    benchmark_id = args.benchmark_id
    if args.test_benchmark:
        from text2sql_eval_toolkit.utils import get_benchmark_info

        get_benchmark_info(benchmark_id, is_test=True)

    print("Re-running metrics only (no inference, no SQL execution)")
    print(f"  Benchmark:  {benchmark_id}")
    print(f"  Storage:    SQLite (text2sql_eval.db)")
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
    try:
        asyncio.run(
            async_evaluate_predictions(
                benchmark_id=benchmark_id,
                llm_judge_config=llm_judge_config,
                max_concurrency=max(1, args.max_concurrency),
                force_rerun_llm_judge=force_rerun_llm,
                force_rerun=False,
                csv_summary_path=args.csv_summary_path,
            )
        )
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    elapsed = time.time() - start
    print(f"Done in {elapsed:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
