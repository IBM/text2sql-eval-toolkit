#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

import argparse
from text2sql_eval_toolkit.analysis.report_tools import (
    collect_results,
    create_dashboard,
)
from pathlib import Path

# CONFIGURATION
OUTPUT_FILE = "data/results/README.md"
TEST_OUTPUT_FILE = "data/benchmarks/test_benchmarks/results/README.md"


def main():
    parser = argparse.ArgumentParser(
        description='Generate summary report for benchmarks'
    )
    parser.add_argument(
        '--test',
        action='store_true',
        help='Generate report for test benchmarks instead of production benchmarks'
    )
    args = parser.parse_args()
    
    # Select output file based on mode
    output_file = TEST_OUTPUT_FILE if args.test else OUTPUT_FILE
    output_folder = Path(output_file).parent
    
    # Collect results and generate dashboard
    results, benchmarks_info = collect_results(output_folder, is_test=args.test)
    markdown_content = create_dashboard(output_file, results, benchmarks_info)
    
    # Write to file
    with open(output_file, "w") as f:
        f.write(markdown_content)

    mode = "test" if args.test else "production"
    print(f"✅ {mode.capitalize()} dashboard written to: {output_file}")


if __name__ == "__main__":
    main()
