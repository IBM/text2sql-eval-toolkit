#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

import argparse
from text2sql_eval_toolkit.evaluation.evaluation_tools import evaluate_predictions
from text2sql_eval_toolkit import env_loader  # Load .env file automatically


def main():
    parser = argparse.ArgumentParser(description="Evaluate SQL predictions.")
    parser.add_argument("input_file", help="Input JSON file path")
    parser.add_argument("--output_file", help="Evaluated JSON file path (optional)")
    parser.add_argument(
        "--summary_file", help="Evaluation summary JSON file path (optional)"
    )
    parser.add_argument(
        "--csv_summary_file", help="Evaluation summary CSV output file (optional)"
    )
    parser.add_argument(
        "--use_llm_judge",
        "--use_llm",  # Backward compatibility alias
        action="store_true",
        help="Enable LLM-as-judge evaluation metrics (optional)",
    )
    parser.add_argument(
        "--llm_judge_config_path",
        help="Path to config yaml file for LLM as judge (optional)",
    )
    args = parser.parse_args()

    evaluate_predictions(
        input_file=args.input_file,
        output_file=args.output_file,
        summary_file=args.summary_file,
        csv_summary_file=args.csv_summary_file,
        use_llm=args.use_llm_judge,
        llm_judge_config_path=args.llm_judge_config_path,
    )


if __name__ == "__main__":
    main()
