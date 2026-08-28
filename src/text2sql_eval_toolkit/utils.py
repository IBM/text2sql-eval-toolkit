#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

import asyncio
import importlib.resources as resources
import json
import os
import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from typing import Any, Dict
from pathlib import Path
from text2sql_eval_toolkit.logging import get_logger

BENCHMARKS_FILE = resources.files("text2sql_eval_toolkit.data").joinpath(
    "benchmarks.json"
)
TEST_BENCHMARKS_FILE = resources.files("text2sql_eval_toolkit.data").joinpath(
    "test-benchmarks.json"
)
logger = get_logger(__name__)

# Override with absolute path to the repo's `data/` directory when the package
# is installed and CWD-based detection is wrong.
_WRITABLE_DATA_ROOT_ENV = "TEXT2SQL_EVAL_TOOLKIT_DATA_ROOT"


def get_writable_data_root() -> Path:
    """
    Directory containing user-writable benchmark outputs (predictions, eval JSON).

    Resolution order:
    1. ``TEXT2SQL_EVAL_TOOLKIT_DATA_ROOT`` if set (must be the ``data`` folder).
    2. ``<repo>/data`` where ``repo`` is the nearest ancestor of the current
       working directory that contains ``pyproject.toml`` and a ``data`` directory.
    3. ``Path.cwd() / "data"``.
    """
    env = os.environ.get(_WRITABLE_DATA_ROOT_ENV)
    if env:
        return Path(env).expanduser().resolve()
    cwd = Path.cwd().resolve()
    for d in [cwd, *cwd.parents]:
        if (d / "pyproject.toml").is_file() and (d / "data").is_dir():
            return (d / "data").resolve()
    return (cwd / "data").resolve()


def _registry_filename(is_test: bool) -> str:
    return "test-benchmarks.json" if is_test else "benchmarks.json"


def get_benchmarks_file_path(is_test: bool = False) -> Path:
    """
    Resolve benchmark registry path, preferring writable local data roots.

    Priority:
    1) TEXT2SQL_DATA_ROOT/{benchmarks|test-benchmarks}.json
    2) ./data/{benchmarks|test-benchmarks}.json
    3) Packaged data file under text2sql_eval_toolkit.data
    """
    filename = _registry_filename(is_test)
    env_root = os.getenv("TEXT2SQL_DATA_ROOT")
    if env_root:
        env_candidate = Path(env_root).expanduser().resolve() / filename
        if env_candidate.exists():
            return env_candidate

    cwd_candidate = (Path.cwd() / "data" / filename).resolve()
    if cwd_candidate.exists():
        return cwd_candidate

    return Path(str(TEST_BENCHMARKS_FILE if is_test else BENCHMARKS_FILE)).resolve()


def get_available_benchmarks(include_test: bool = True):
    """
    Get list of available benchmark IDs.

    Args:
        include_test (bool): If True, include test benchmarks from
            test-benchmarks.json.

    Returns:
        list[str]: Benchmark IDs.
    """
    benchmarks = []

    # Load production benchmarks
    benchmarks_path = get_benchmarks_file_path(is_test=False)
    if benchmarks_path.exists():
        with open(benchmarks_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        benchmarks.extend(list(data.keys()))

    # Load test benchmarks if requested
    test_benchmarks_path = get_benchmarks_file_path(is_test=True)
    if include_test and test_benchmarks_path.exists():
        with open(test_benchmarks_path, "r", encoding="utf-8") as f:
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
    benchmarks_file = get_benchmarks_file_path(is_test=is_test)
    benchmarks_info = {}
    try:
        with open(benchmarks_file, "r", encoding="utf-8") as meta_file:
            benchmarks_meta = json.load(meta_file)
    except Exception as e:
        logger.error(f"Error loading the benchmarks JSON file: {benchmarks_file}.")
        raise e
    package_data_root = benchmarks_file.parent
    predictions_root = get_writable_data_root()
    for benchmark_id in benchmarks_meta:
        benchmark_info = benchmarks_meta[benchmark_id]
        benchmark_info["benchmark_json_path"] = resolve_path(
            package_data_root, benchmark_info["data"]
        )
        benchmark_info["schema_json_path"] = resolve_path(
            package_data_root, benchmark_info["schema"]
        )
        benchmark_info["predictions_path"] = resolve_path(
            predictions_root, benchmark_info["predictions"]
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
        benchmarks_file = get_benchmarks_file_path(is_test=True)
        with open(benchmarks_file, "r", encoding="utf-8") as meta_file:
            benchmarks_meta = json.load(meta_file)
        if benchmark_id not in benchmarks_meta:
            raise ValueError(
                f"Benchmark ID '{benchmark_id}' not found in test-benchmarks.json."
            )
    else:
        # Try production benchmarks first
        benchmarks_file = get_benchmarks_file_path(is_test=False)
        with open(benchmarks_file, "r", encoding="utf-8") as meta_file:
            benchmarks_meta = json.load(meta_file)

        # If not found in production, try test benchmarks
        test_benchmarks_file = get_benchmarks_file_path(is_test=True)
        if benchmark_id not in benchmarks_meta and test_benchmarks_file.exists():
            benchmarks_file = test_benchmarks_file
            with open(benchmarks_file, "r", encoding="utf-8") as meta_file:
                benchmarks_meta = json.load(meta_file)
            if benchmark_id not in benchmarks_meta:
                raise ValueError(
                    f"Benchmark ID '{benchmark_id}' not found in benchmarks.json or test-benchmarks.json."
                )
        elif benchmark_id not in benchmarks_meta:
            raise ValueError(
                f"Benchmark ID '{benchmark_id}' not found in benchmarks.json."
            )

    package_data_root = benchmarks_file.parent
    predictions_root = get_writable_data_root()
    benchmark_info = benchmarks_meta[benchmark_id]
    benchmark_info["benchmark_json_path"] = resolve_path(
        package_data_root, benchmark_info["data"]
    )
    benchmark_info["schema_json_path"] = resolve_path(
        package_data_root, benchmark_info["schema"]
    )
    benchmark_info["predictions_path"] = resolve_path(
        predictions_root, benchmark_info["predictions"]
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
    return benchmark_info


def run_with_timeout(func, timeout=90, retries=2, wait=3, *args, **kwargs):
    """
    Runs a function with a timeout and retries.

    Parameters:
        func (callable): The function to run.
        timeout (int): Timeout in seconds for each attempt.
        retries (int): Number of retries after the first attempt.
        wait (int): Seconds to wait between retries.
        *args (Any): Positional arguments passed through to *func*.
        **kwargs (Any): Keyword arguments passed through to *func*.

    Returns:
        Any: The result of the function if successful.

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
                    ) from None


async def run_with_timeout_async(task, base_timeout=90, retries=2, wait=3):
    """
    Await a coroutine factory with a timeout, retrying with a longer one.

    The timeout **escalates**: attempt *n* is allowed ``base_timeout * (n + 1)``
    seconds, so the default gives 90s, then 180s, then 270s. This differs from the
    synchronous :func:`run_with_timeout`, which applies the same timeout every
    attempt. Escalation suits work whose first attempt is slow because a resource
    is cold rather than because it is stuck.

    Args:
        task (Callable): Zero-argument callable returning an awaitable. It is called afresh
            on each attempt, so it must be a factory rather than a single
            coroutine object -- an already-awaited coroutine cannot be retried.
        base_timeout (int): Seconds allowed for the first attempt.
        retries (int): Attempts *after* the first.
        wait (int): Seconds to sleep between attempts.

    Returns:
        Any: Whatever the awaited task returns.

    Raises:
        asyncio.TimeoutError: If every attempt times out.

    Example:
        ```python
        >>> async def slow():
        ...     await asyncio.sleep(0.1)
        ...     return "done"
        >>> await run_with_timeout_async(slow, base_timeout=1)
        'done'
        ```
    """
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
                ) from None


def parse_dataframe(json_str):
    """
    Reconstruct a DataFrame from the toolkit's serialised form.

    Result sets are stored throughout the toolkit as pandas ``orient="split"``
    JSON -- an object with ``columns``, ``index`` and ``data`` keys. This is the
    reader for the ``gt_df`` and ``predicted_df`` fields of a prediction record.

    Args:
        json_str (str): JSON text in pandas ``orient="split"`` form.

    Returns:
        pandas.DataFrame: The reconstructed frame, with its original index.

    Raises:
        ValueError: If the text is not valid JSON or lacks the expected keys.
            The offending string is included in the message.

    Example:
        ```python
        >>> parse_dataframe('{"columns": ["n"], "index": [0], "data": [[1]]}')
           n
        0  1
        ```
    """
    try:
        df_dict = json.loads(json_str)
        return pd.DataFrame(
            data=df_dict["data"], columns=df_dict["columns"], index=df_dict["index"]
        )
    except Exception as e:
        raise ValueError(
            f"Failed to parse DataFrame JSON. Error: {e}. JSON string: {json_str}"
        ) from e


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


def normalize_record(record):
    """
    Write the canonical keys onto *record*, in place.

    Benchmarks spell their fields differently -- ``question_id`` or ``qid``,
    ``page_content`` or ``question``, ``SQL`` or ``target``. Predictions are
    stored under the canonical spellings ``id``, ``utterance`` and ``sql``, and
    inference looks records up by ``record["id"]`` when resuming, so the
    normalisation has to happen before a record is written.

    The readers (:func:`get_question_id`, :func:`get_utterance`,
    :func:`get_gt_sqls`) used to do this as a side effect of being called, which
    meant a caller who only wanted to read a value silently modified the record
    they were given. Call this instead, where normalising is what you mean.

    Missing fields are skipped rather than raising: a record with no ground-truth
    SQL is still worth normalising for its id and question.

    Args:
        record (dict): A benchmark question or prediction record. Modified in
            place.

    Returns:
        dict: The same record, for chaining.
    """
    for key, reader in (
        ("id", get_question_id),
        ("utterance", get_utterance),
        ("sql", get_gt_sqls),
    ):
        try:
            record[key] = reader(record)
        except (ValueError, KeyError):
            continue
    return record


def get_question_id(record):
    """
    Read the identifier from a benchmark or prediction record.

    Benchmarks disagree on what the id field is called, so the first of ``id``,
    ``question_id``, ``qid`` and ``_id`` that is present wins.

    Reading is side-effect free. Use :func:`normalize_record` to write the
    canonical keys onto a record before storing it.

    Args:
        record (dict): A benchmark question or prediction record.

    Returns:
        str | int: The identifier, in whatever type the record used -- commonly ``str`` or
        ``int``. Note that ``0`` is a valid id, so test for ``None`` rather than
        for falsiness.

    Raises:
        ValueError: If the record carries none of the recognised keys.
    """
    id_keys = ["id", "question_id", "qid", "_id"]
    for key in id_keys:
        question_id = record.get(key)
        if question_id is not None:
            return question_id
    raise ValueError(f"Record has no ID field among {id_keys}: {record}")


def get_utterance(record):
    """
    Read the natural-language question from a record.

    Tries ``utterance``, then ``page_content``, then ``question``.

    Reading is side-effect free; see :func:`normalize_record` to normalise.

    See Also:
        :func:`get_question`, which reads the same content but tries the keys in
        the opposite order and raises ``KeyError`` rather than ``ValueError``.

    Args:
        record (dict): A benchmark question or prediction record.

    Returns:
        str: The question text.

    Raises:
        ValueError: If the record carries none of the recognised keys.
    """
    utterance_keys = ["utterance", "page_content", "question"]
    for key in utterance_keys:
        utterance = record.get(key)
        if utterance:
            return utterance
    raise ValueError(f"Record has no utterance field among {utterance_keys}: {record}")


def get_gt_sqls(record):
    """
    Read the ground-truth SQL from a record, always as a list.

    A question may have more than one correct SQL formulation, so this always
    returns a list even when the record stores a single string. Tries ``sql``,
    ``SQL``, ``target`` and ``query``, then falls back to
    ``record["metadata"]["sql"]``.

    Values stored as a ``dict`` are skipped rather than returned: some benchmarks
    keep a structured representation of the query under the same key, and that is
    not executable SQL.

    Reading is side-effect free; see :func:`normalize_record` to normalise.

    Args:
        record (dict): A benchmark question record.

    Returns:
        list[str]: One or more ground-truth statements.

    Raises:
        ValueError: If no ground-truth SQL can be found.
    """
    gt_sql_keys = ["sql", "SQL", "target", "query"]
    for key in gt_sql_keys:
        gt_sqls = record.get(key)
        if gt_sqls:
            # Skip if it's a dict (structured SQL representation, not a string)
            if isinstance(gt_sqls, dict):
                continue
            if not isinstance(gt_sqls, list):
                gt_sqls = [gt_sqls]
            return gt_sqls
    if "metadata" in record and "sql" in record["metadata"]:
        return [record["metadata"]["sql"]]
    raise ValueError(f"Record has no ground truth SQL: {record}")


def get_question(record):
    """
    Read the question text, preferring ``page_content``.

    See Also:
        :func:`get_utterance`, which reads the same content but prefers
        ``utterance``, normalises the record as a side effect, and raises
        ``ValueError`` instead of ``KeyError``. Prefer that one in new code; this
        exists for callers that predate it.

    Args:
        record (dict): A benchmark question or prediction record.

    Returns:
        str: The question text.

    Raises:
        KeyError: If the record has none of ``page_content``, ``question`` or
            ``utterance``.
    """
    return (
        record["page_content"]
        if "page_content" in record
        else (record["question"] if "question" in record else record["utterance"])
    )


def get_default_eval_filename(predictions_file):
    """
    Derive the evaluation artifact path from a predictions path.

    Inserts ``_eval`` before the extension, which is the naming convention the
    whole pipeline relies on: ``{benchmark}-predictions.json`` becomes
    ``{benchmark}-predictions_eval.json``.

    Args:
        predictions_file (str | Path): Path to a predictions file.

    Returns:
        str: The evaluation filename.

    Example:
        ```python
        >>> get_default_eval_filename("data/results/spider_dev-predictions.json")
        'data/results/spider_dev-predictions_eval.json'
        ```
    """
    base_name, ext = os.path.splitext(predictions_file)
    return f"{base_name}_eval{ext}"


def add_summary_json_suffix(path: str) -> str:
    """
    Derive the JSON summary path from an evaluation artifact path.

    Replaces the extension with ``_summary.json``.

    Args:
        path: Path to an evaluation artifact.

    Returns:
        str: The summary path.

    Example:
        ```python
        >>> add_summary_json_suffix("spider_dev-predictions_eval.json")
        'spider_dev-predictions_eval_summary.json'
        ```
    """
    return os.path.splitext(path)[0] + "_summary.json"


def add_summary_csv_suffix(path: str) -> str:
    """
    Derive the CSV summary path from an evaluation artifact path.

    Replaces the extension with ``_summary.csv``. The CSV carries the same
    per-pipeline rows as the JSON summary, for spreadsheet use.

    Args:
        path: Path to an evaluation artifact.

    Returns:
        str: The summary path.

    Example:
        ```python
        >>> add_summary_csv_suffix("spider_dev-predictions_eval.json")
        'spider_dev-predictions_eval_summary.csv'
        ```
    """
    return os.path.splitext(path)[0] + "_summary.csv"
