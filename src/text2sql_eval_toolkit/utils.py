#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

import asyncio
import json
import os
import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from typing import Any, Dict
from pathlib import Path
from text2sql_eval_toolkit.logging import get_logger


BENCHMARKS_FILE = (
    Path(__file__).resolve().parent.parent.parent / "data" / "benchmarks.json"
)
TEST_BENCHMARKS_FILE = (
    Path(__file__).resolve().parent.parent.parent / "data" / "test-benchmarks.json"
)
logger = get_logger(__name__)


def get_available_benchmarks(include_test: bool = True):
    """
    Get list of available benchmark IDs.
    
    Args:
        include_test: If True, include test benchmarks from test-benchmarks.json
    
    Returns:
        List of benchmark IDs
    """
    benchmarks = []
    
    # Load production benchmarks
    if BENCHMARKS_FILE.exists():
        with open(BENCHMARKS_FILE, "r") as f:
            data = json.load(f)
        benchmarks.extend(list(data.keys()))
    
    # Load test benchmarks if requested
    if include_test and TEST_BENCHMARKS_FILE.exists():
        with open(TEST_BENCHMARKS_FILE, "r") as f:
            data = json.load(f)
        benchmarks.extend(list(data.keys()))
    
    return benchmarks


def get_benchmarks_info(is_test: bool = False) -> Dict[str, Any]:
    """
    Retrieves all the benchmarks' information.

    Args:
        is_test: If True, load test benchmarks from test-benchmarks.json

    Returns:
        Dict[str, Any]: Dictionary containing info and paths to benchmark files.
    """
    benchmarks_file = TEST_BENCHMARKS_FILE if is_test else BENCHMARKS_FILE
    benchmarks_info = {}
    try:
        with open(benchmarks_file, "r") as meta_file:
            benchmarks_meta = json.load(meta_file)
    except Exception as e:
        logger.error(f"Error loading the benchmarks JSON file: {benchmarks_file}.")
        raise e
    for benchmark_id, benchmark_info in benchmarks_meta.items():
        root = BENCHMARKS_FILE.parent
        if benchmark_id not in benchmarks_meta:
            raise ValueError(
                f"Benchmark ID '{benchmark_id}' not found in benchmarks.json."
            )
        benchmark_info = benchmarks_meta[benchmark_id]
        benchmark_info["benchmark_json_path"] = resolve_path(
            root, benchmark_info["data"]
        )
        benchmark_info["schema_json_path"] = resolve_path(
            root, benchmark_info["schema"]
        )
        benchmark_info["predictions_path"] = resolve_path(
            root, benchmark_info["predictions"]
        )
        benchmark_info["eval_results_path"] = Path(
            benchmark_info["predictions_path"].with_name(
                benchmark_info["predictions_path"].stem + "_eval.json"
            )
        ).resolve()
        benchmark_info["eval_summary_path"] = Path(
            benchmark_info["predictions_path"].with_name(
                benchmark_info["predictions_path"].stem + "_eval_summary.json"
            )
        ).resolve()
        benchmarks_info[benchmark_id] = benchmark_info
    return benchmarks_info


def resolve_path(root, path_str):
    """
    Resolves a given path string relative to a root directory.
    If the provided path string is absolute, returns it as a Path object.
    Otherwise, returns the path relative to the specified root directory.
    Args:
        root (Path): The root directory to resolve relative paths against.
        path_str (str): The path string to resolve.
    Returns:
        Path: The resolved absolute or relative path as a Path object.
    """
    p = Path(path_str)
    if p.is_absolute():
        return p
    return root / p


def get_benchmark_info(benchmark_id: str, is_test: bool = False) -> Dict[str, Any]:
    """
    Retrieves the benchmark files for a given benchmark ID.
    Automatically detects if benchmark is in test-benchmarks.json if not found in benchmarks.json.

    Args:
        benchmark_id (str): Identifier for the benchmark dataset.
        is_test (bool): If True, load from test-benchmarks.json. If False, tries production first, then test.

    Returns:
        Dict[str, Any]: Dictionary containing info and paths to benchmark files.
    """
    # If is_test is True, only look in test benchmarks
    if is_test:
        benchmarks_file = TEST_BENCHMARKS_FILE
        with open(benchmarks_file, "r") as meta_file:
            benchmarks_meta = json.load(meta_file)
        if benchmark_id not in benchmarks_meta:
            raise ValueError(f"Benchmark ID '{benchmark_id}' not found in test-benchmarks.json.")
    else:
        # Try production benchmarks first
        benchmarks_file = BENCHMARKS_FILE
        with open(benchmarks_file, "r") as meta_file:
            benchmarks_meta = json.load(meta_file)
        
        # If not found in production, try test benchmarks
        if benchmark_id not in benchmarks_meta and TEST_BENCHMARKS_FILE.exists():
            benchmarks_file = TEST_BENCHMARKS_FILE
            with open(benchmarks_file, "r") as meta_file:
                benchmarks_meta = json.load(meta_file)
            if benchmark_id not in benchmarks_meta:
                raise ValueError(f"Benchmark ID '{benchmark_id}' not found in benchmarks.json or test-benchmarks.json.")
        elif benchmark_id not in benchmarks_meta:
            raise ValueError(f"Benchmark ID '{benchmark_id}' not found in benchmarks.json.")
    
    root = benchmarks_file.parent
    benchmark_info = benchmarks_meta[benchmark_id]
    benchmark_info["benchmark_json_path"] = resolve_path(root, benchmark_info["data"])
    benchmark_info["schema_json_path"] = resolve_path(root, benchmark_info["schema"])
    benchmark_info["predictions_path"] = resolve_path(
        root, benchmark_info["predictions"]
    )
    return benchmark_info


def run_with_timeout(func, timeout=90, retries=2, wait=3, *args, **kwargs):
    """
    Runs a function with a timeout and retries.

    Parameters:
        func (callable): The function to run.
        timeout (int): Timeout in seconds for each attempt.
        retries (int): Number of retries after the first attempt.
        wait (int): Seconds to wait between retries.
        *args, **kwargs: Arguments to pass to the function.

    Returns:
        The result of the function if successful.

    Raises:
        TimeoutError: If all attempts time out.
    """
    for attempt in range(retries + 1):
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(func, *args, **kwargs)
            try:
                return future.result(timeout=timeout)
            except TimeoutError:
                logger.info(f"⚠️ Attempt {attempt + 1} timed out.")
                if attempt < retries:
                    time.sleep(wait)
                else:
                    raise TimeoutError(
                        f"❗️ Function timed out after {retries + 1} attempts."
                    )


async def run_with_timeout_async(task, base_timeout=90, retries=2, wait=3):
    for attempt in range(retries + 1):
        timeout = base_timeout * (attempt + 1)
        try:
            return await asyncio.wait_for(task(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.info(f"⚠️ Attempt {attempt + 1} timed out after {timeout} seconds.")
            if attempt < retries:
                logger.info(f"⏳ Retrying in {wait} seconds...")
                await asyncio.sleep(wait)
            else:
                raise asyncio.TimeoutError(
                    f"❗️ Function timed out after {retries + 1} attempts."
                )


def parse_dataframe(json_str):
    """Reconstruct a DataFrame from a JSON-encoded dictionary."""
    try:
        df_dict = json.loads(json_str)
        return pd.DataFrame(
            data=df_dict["data"], columns=df_dict["columns"], index=df_dict["index"]
        )
    except Exception as e:
        raise ValueError(
            f"Failed to parse DataFrame JSON. Error: {e}. JSON string: {json_str}"
        )


def truncate_dataframe(
    df: pd.DataFrame, head: int = 10, tail: int = 10
) -> pd.DataFrame:
    """
    Truncate a DataFrame to show only the first `head` rows and last `tail` rows.
    If the DataFrame has more rows than head + tail, insert a '...' row in between
    with empty strings so print(df) looks clean.
    """
    if len(df) <= head + tail:
        return df

    top = df.head(head)
    bottom = df.tail(tail)

    # Row of empty strings with index labeled "..."
    ellipsis_row = pd.DataFrame([[""] * df.shape[1]], columns=df.columns, index=["..."])

    return pd.concat([top, ellipsis_row, bottom])


def get_question_id(record):
    """Gets Question ID from benchmark or prediction data record"""
    id_keys = ["id", "question_id", "qid", "_id"]
    for key in id_keys:
        question_id = record.get(key)
        if question_id is not None:
            record["id"] = question_id  # Ensure 'id' is always set
            return question_id
    raise ValueError(f"Record has no ID field among {id_keys}: {record}")


def get_utterance(record):
    """Gets the question (utterance) from the benchmark or prediction data record"""
    utterance_keys = ["utterance", "page_content", "question"]
    for key in utterance_keys:
        utterance = record.get(key)
        if utterance:
            record["utterance"] = utterance  # Ensure 'utterance' is always set
            return utterance
    raise ValueError(f"Record has no utterance field among {utterance_keys}: {record}")


def get_gt_sqls(record):
    gt_sql_keys = ["sql", "SQL", "target", "query"]
    for key in gt_sql_keys:
        gt_sqls = record.get(key)
        if gt_sqls:
            # Skip if it's a dict (structured SQL representation, not a string)
            if isinstance(gt_sqls, dict):
                continue
            if not isinstance(gt_sqls, list):
                gt_sqls = [gt_sqls]
            record["sql"] = gt_sqls
            return gt_sqls
    if "metadata" in record and "sql" in record["metadata"]:
        return [record["metadata"]["sql"]]
    raise ValueError(f"Record has no ground truth SQL: {record}")


def get_question(record):
    return (
        record["page_content"]
        if "page_content" in record
        else (record["question"] if "question" in record else record["utterance"])
    )


def get_default_eval_filename(predictions_file):
    base_name, ext = os.path.splitext(predictions_file)
    return f"{base_name}_eval{ext}"


def add_summary_json_suffix(path: str) -> str:
    return os.path.splitext(path)[0] + "_summary.json"


def add_summary_csv_suffix(path: str) -> str:
    return os.path.splitext(path)[0] + "_summary.csv"
