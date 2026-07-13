#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
"""
Run LLM-as-judge only on existing predictions.

Skips inference, SQL execution, and deterministic evaluation metrics
(execution accuracy, sqlglot equivalence, etc.). Results are stored in SQLite.
"""

from __future__ import annotations

import argparse
import sys
import time
from typing import Optional

from text2sql_eval_toolkit import env_loader  # noqa: F401 — load .env when present
from text2sql_eval_toolkit.evaluation.evaluation_tools import run_llm_judge


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run LLM-as-judge only on existing predictions. "
            "No inference, SQL execution, or deterministic metrics."
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
        "--llm-judge-config-path",
        "--llm_judge_config_path",
        help="YAML config for LLM-as-judge (default: llm_judge_default_config.yaml)",
    )
    parser.add_argument(
        "--force-rerun-llm-judge",
        "--force_rerun_llm_judge",
        action="store_true",
        help="Re-call LLM-as-judge even when cached scores exist",
    )
    parser.add_argument(
        "--max-concurrency",
        type=int,
        default=16,
        metavar="N",
        help="Concurrent LLM judge tasks (default: 16)",
    )
    parser.add_argument(
        "--csv-summary-path",
        help="Optional path to write evaluation summary CSV (requires prior full eval)",
    )

    args = parser.parse_args(argv)

    benchmark_id = args.benchmark_id
    if args.test_benchmark:
        from text2sql_eval_toolkit.utils import get_benchmark_info

        get_benchmark_info(benchmark_id, is_test=True)

    print("Running LLM judge only (no inference, execution, or deterministic metrics)")
    print(f"  Benchmark:  {benchmark_id}")
    print(f"  Storage:    SQLite (text2sql_eval.db)")
    print(
        f"  Config:     {args.llm_judge_config_path or 'default'}"
    )
    print(
        f"  Cache:      {'force re-run' if args.force_rerun_llm_judge else 'reuse when available'}"
    )

    start = time.time()
    try:
        run_llm_judge(
            benchmark_id,
            llm_judge_config_path=args.llm_judge_config_path,
            force_rerun_llm_judge=args.force_rerun_llm_judge,
            max_concurrency=max(1, args.max_concurrency),
            csv_summary_path=args.csv_summary_path,
        )
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    elapsed = time.time() - start
    print(f"Done in {elapsed:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
