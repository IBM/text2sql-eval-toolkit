#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

import argparse
from text2sql_eval_toolkit.evaluation.evaluation_tools import run_evaluation
from text2sql_eval_toolkit import env_loader  # Load .env file automatically


def main():
    parser = argparse.ArgumentParser(
        description="Evaluate SQL predictions stored in SQLite for a benchmark."
    )
    parser.add_argument("benchmark_id", help="Benchmark ID")
    parser.add_argument(
        "--csv_summary_path",
        help="Optional path to write evaluation summary CSV",
    )
    parser.add_argument(
        "--use_llm_judge",
        "--use_llm",
        action="store_true",
        help="Enable LLM-as-judge evaluation metrics (optional)",
    )
    parser.add_argument(
        "--llm_judge_config_path",
        help="Path to config yaml file for LLM as judge (optional)",
    )
    parser.add_argument(
        "--force-rerun",
        action="store_true",
        help="Recompute all evaluation metrics",
    )
    parser.add_argument(
        "--force-rerun-llm-judge",
        action="store_true",
        help="Recompute LLM judge scores even when cached",
    )
    args = parser.parse_args()

    run_evaluation(
        args.benchmark_id,
        use_llm=args.use_llm_judge,
        llm_judge_config_path=args.llm_judge_config_path,
        force_rerun=args.force_rerun,
        force_rerun_llm_judge=args.force_rerun_llm_judge,
        csv_summary_path=args.csv_summary_path,
    )


if __name__ == "__main__":
    main()
