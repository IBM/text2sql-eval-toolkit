#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

import argparse

from text2sql_eval_toolkit.analysis.report_tools import (
    print_summary_results_by_category,
    DEFAULT_PRINT_METRICS,
    DEFAULT_METRIC,
)
from text2sql_eval_toolkit.utils import load_predictions_data


def main():
    parser = argparse.ArgumentParser(
        description="Print summary of Text-to-SQL evaluation results to terminal."
    )
    parser.add_argument(
        "benchmark_id",
        help="Benchmark id (loads evaluation records from SQLite)",
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
    records = load_predictions_data(args.benchmark_id, include_eval=True)

    print_summary_results_by_category(
        records,
        sort_by=args.sort_by,
        metrics_to_print=args.metrics,
    )


if __name__ == "__main__":
    main()
