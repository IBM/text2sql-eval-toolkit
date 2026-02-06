#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

import argparse
from text2sql_eval_toolkit.profiling.profiling_tools import (
    profile_pred_or_eval_json_file,
)
from text2sql_eval_toolkit.logging import get_logger


logger = get_logger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Profile SQL queries in a JSON file.")
    parser.add_argument("json_file", help="Path to the JSON file to process.")
    parser.add_argument(
        "--dialect", default="postgres", help="SQL dialect to use (default: postgres)"
    )

    args = parser.parse_args()

    try:
        profile_pred_or_eval_json_file(args.json_file, args.dialect)
    except Exception as e:
        logger.error(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
