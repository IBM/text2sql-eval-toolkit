#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
This script takes in the output predictions of a benchmark datasets and runs all the SQL statements in it that do not have a dataframe. The script augments the predictions file with predicted dataframes.

Usage:
    python run_execution.py <benchmark_id>

Arguments:
    benchmark_id (str): Identifier for the benchmark dataset to evaluate.

Workflow:
    - Reads data/benchmarks.json file to get the benchmark_info for the given `benchmark_id`.
    - Reads the `db_engine` from benchmark_info.
    - From the `db_engine`, gets `db_type`. If it's `postgres`, it gets the `connection_string_env_var` value, and gets the environment variable and reads the connection string from it. If it's other `db_type` raises NotImplementedError for now.
    - Uses the connection string to connect to the (postgres) database.
    - Reads the `predictions` field from benchmark_info, which is a path to the predictions file.
    - Loads the predictions file and loads it as a JSON object.
    - For each object in the predictions, go over all the predictions in the `predictions` field, which looks like this:
        ```
        "predictions": {
            "meta-llama/llama-3-3-70b-instruct": {
                "predicted_sql": "SELECT \n    ProductLine, \n    ProductType, \n    SUM(Revenue) AS TotalRevenue\nFROM \n    gosales_and_forecast\nWHERE \n    ProductLine = 'Personal Accessories' \n    AND ProductType = 'Eyewear'\nGROUP BY \n    ProductLine, \n    ProductType",
                "model_parameters": {
                "decoding_method": "greedy",
                "max_new_tokens": 256,
                "stop_sequences": [
                    "```"
                ]
                }
            },
            "ibm/granite-3-3-8b-instruct": {
                "predicted_sql": "SELECT\n    CalendarYear,\n    CalendarMonth,\n    Region,\n    Country,\n    Province,\n    City,\n    ProductLine,\n    ProductType,\n    BaseProductId,\n    ProductId,\n    ProductName,\n    ProductColor,\n    ProductBrand,\n    ProductSize,\n    Revenue,\n    PlannedRevenue\nFROM\n    gosales_and_forecast;",
                "model_parameters": {
                "decoding_method": "greedy",
                "max_new_tokens": 256,
                "stop_sequences": [
                    "```"
                ]
                }
            }
        }
        ```
    - For each prediction, it checks if the `predicted_sql` is not empty and it does not have a `predicted_df` field. If so, it runs the SQL query in the database and gets the result as a dataframe, stores it in the `predicted_df` field, and saves the updated predictions back to the predictions file.
"""

import argparse
import time

from text2sql_eval_toolkit import env_loader  # Load .env file automatically
from text2sql_eval_toolkit.execution.execution_tools import run_execution


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run SQL execution for a benchmark dataset."
    )
    parser.add_argument("benchmark_id", help="Benchmark ID")
    args = parser.parse_args()

    start_time = time.time()
    run_execution(args.benchmark_id)
    end_time = time.time()
    print(f"Total running time for execution: {end_time - start_time:.2f} seconds")
    print(f"Execution completed for benchmark '{args.benchmark_id}'.")

# Example usage:
# python run_execution.py bird_mini_dev_sqlite
# This will read the predictions file for the 'bird_mini_dev_sqlite' benchmark, run the SQL queries that do not have a predicted dataframe, and augment the predictions with the resulting dataframes
