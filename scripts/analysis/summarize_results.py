#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

import argparse
import json
from pathlib import Path

from text2sql_eval_toolkit.analysis.report_tools import (
    print_summary_results_by_category,
    DEFAULT_PRINT_METRICS,
    DEFAULT_METRIC,
)


def main():
    parser = argparse.ArgumentParser(
        description="Print summary of Text-to-SQL evaluation results to terminal."
    )
    parser.add_argument(
        "input_file", help="Path to the JSON file containing evaluation records"
    )
    parser.add_argument(
        "--metrics",
        nargs="+",
        default=DEFAULT_PRINT_METRICS,
        help=f"List of metric keys to display (default: {DEFAULT_PRINT_METRICS})",
    )
    parser.add_argument(
        "--sort_by",
        default=DEFAULT_METRIC,
        help=f"Metric to sort pipelines by (default: {DEFAULT_METRIC})",
    )

    args = parser.parse_args()

    input_path = Path(args.input_file)
    if not input_path.exists():
        print(f"❌ File not found: {input_path}")
        return

    with open(input_path, "r", encoding="utf-8") as f:
        records = json.load(f)

    print_summary_results_by_category(
        records,
        sort_by=args.sort_by,
        metrics_to_print=args.metrics,
    )


if __name__ == "__main__":
    main()
