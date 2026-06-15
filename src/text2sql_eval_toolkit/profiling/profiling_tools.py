#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

import json
import os
import re
import shutil
from sqlglot import parse_one, exp
from tqdm import tqdm
from pathlib import Path
from typing import Dict, List, Optional, Set, Union
from text2sql_eval_toolkit.utils import get_gt_sqls, get_question_id
from text2sql_eval_toolkit.logging import get_logger


logger = get_logger(__name__)

_QUESTION_TEXT_KEYS = ("question", "utterance", "page_content")

# Benchmark-agnostic natural-language intent patterns (English).
_QUESTION_PATTERNS: Dict[str, re.Pattern[str]] = {
    "question_counting": re.compile(
        r"\b(how many|number of|count of|total number)\b", re.IGNORECASE
    ),
    "question_superlative": re.compile(
        r"\b(most|least|highest|lowest|largest|smallest|best|worst|"
        r"maximum|minimum|max|min|top|bottom|first|last)\b",
        re.IGNORECASE,
    ),
    "question_comparison": re.compile(
        r"\b(more than|less than|greater|fewer|at least|at most|above|below|between)\b",
        re.IGNORECASE,
    ),
    "question_temporal": re.compile(
        r"\b(19\d{2}|20\d{2}|year|month|day|date|when|before|after|since|until)\b",
        re.IGNORECASE,
    ),
    "question_aggregation_intent": re.compile(
        r"\b(ratio|percentage|percent|average|avg|mean|proportion|rate)\b",
        re.IGNORECASE,
    ),
    "question_listing": re.compile(
        r"\b(list|what are|which|name all|names of|give me)\b", re.IGNORECASE
    ),
    "question_existence": re.compile(
        r"\b(is there|are there|does|do any|exist|have any)\b", re.IGNORECASE
    ),
    "question_negation": re.compile(
        r"\b(not|without|never|no |none)\b", re.IGNORECASE
    ),
    "question_grouping_intent": re.compile(
        r"\b(for each|per |by |group)\b", re.IGNORECASE
    ),
}


def _question_length_tags(word_count: int) -> Set[str]:
    if word_count <= 8:
        return {"question_brief"}
    if word_count <= 15:
        return {"question_moderate"}
    return {"question_verbose"}


def get_question_text(record: Dict) -> Optional[str]:
    """Return natural-language question text from a benchmark or prediction record."""
    for key in _QUESTION_TEXT_KEYS:
        value = record.get(key)
        if value:
            return str(value).strip()
    return None


def analyze_question(question: str) -> List[str]:
    """
    Classify a natural-language question into benchmark-agnostic intent tags.

    Args:
        question: The question text.

    Returns:
        Sorted list of descriptive tags.
    """
    text = question.strip()
    if not text:
        return []

    tags: Set[str] = set(_question_length_tags(len(text.split())))
    for tag, pattern in _QUESTION_PATTERNS.items():
        if pattern.search(text):
            tags.add(tag)

    return sorted(tags)


def analyze_sql_query(sql: str, dialect: str = "postgres") -> Dict:
    """
    Analyze a SQL query and classify it into categories with structural features and descriptive tags.

    Args:
        sql (str): The SQL query string.
        dialect (str): SQL dialect for parsing (default is 'postgres').

    Returns:
        Dict: A dictionary with structural features and a set of descriptive tags.
    """
    parsed = parse_one(sql, dialect=dialect)

    def count(exp_type):
        return len(list(parsed.find_all(exp_type)))

    def count_names(exp_type):
        return len([e.name for e in parsed.find_all(exp_type)])

    features = {
        "query_table_count": count_names(exp.Table),
        "query_column_count": count_names(exp.Column),
        "query_nested_count": count(exp.Select)
        + count(exp.Delete)
        + count(exp.Insert)
        - 1,
        "query_aggregate_count": count(exp.AggFunc),
        "query_sort_count": count(exp.Ordered),
        "query_window_func_count": count(exp.Window),
        "query_join_count": count(exp.Join),
    }

    # Classification logic
    is_basic = (
        features["query_table_count"] == 1
        and features["query_nested_count"] == 0
        and features["query_window_func_count"] == 0
        and features["query_join_count"] == 0
    )

    is_multi_table = (
        features["query_table_count"] > 1
        and features["query_nested_count"] == 0
        and features["query_window_func_count"] == 0
        and features["query_join_count"] >= 1
    )

    is_advanced = features["query_table_count"] == 1 and (
        features["query_nested_count"] > 0 or features["query_window_func_count"] > 0
    )

    # Tag generation
    tags = set()
    if is_basic:
        tags.add("single_source_basic")
    if is_multi_table:
        tags.add("multi_table_simple")
    if is_advanced:
        tags.add("single_source_advanced")

    if features["query_join_count"] > 0:
        tags.add("has_join")
    if features["query_nested_count"] > 0:
        tags.add("has_nested_query")
    if features["query_aggregate_count"] > 0:
        tags.add("has_aggregation")
    if features["query_sort_count"] > 0:
        tags.add("has_sorting")
    if features["query_window_func_count"] > 0:
        tags.add("has_window_function")

    if count(exp.Group) > 0:
        tags.add("has_group_by")
    if count(exp.Having) > 0:
        tags.add("has_having")
    if count(exp.Distinct) > 0:
        tags.add("has_distinct")
    if count(exp.Limit) > 0:
        tags.add("has_limit")
    if (
        count(exp.Union) > 0
        or count(exp.Intersect) > 0
        or count(exp.Except) > 0
    ):
        tags.add("has_set_operation")
    if count(exp.Case) > 0:
        tags.add("has_case_expression")
    if count(exp.CTE) > 0:
        tags.add("has_cte")
    if count(exp.Cast) > 0:
        tags.add("has_cast")
    if count(exp.Like) > 0:
        tags.add("has_like")
    if count(exp.Between) > 0:
        tags.add("has_between")
    if count(exp.In) > 0:
        tags.add("has_in_predicate")
    if any(
        isinstance(node.expression, exp.Null) for node in parsed.find_all(exp.Is)
    ):
        tags.add("has_null")
    if (
        count(exp.Except) > 0
        or any(isinstance(node.this, exp.In) for node in parsed.find_all(exp.Not))
        or any(
            isinstance(node.this, exp.Exists) for node in parsed.find_all(exp.Not)
        )
    ):
        tags.add("has_negation")

    return {"features": features, "categories": sorted(tags)}


def build_benchmark_category_index(
    benchmark_records: List[Dict],
) -> Dict[str, List[str]]:
    """Map question id -> profile categories from a benchmark dataset."""
    index: Dict[str, List[str]] = {}
    for record in benchmark_records:
        try:
            question_id = str(get_question_id(record))
        except ValueError:
            continue
        categories = record.get("meta", {}).get("categories")
        if categories:
            index[question_id] = list(categories)
    return index


def merge_benchmark_categories_into_records(
    eval_records: List[Dict],
    benchmark_json_path: Union[str, Path, None] = None,
    *,
    benchmark_id: str | None = None,
) -> List[Dict]:
    """
    Union eval record ``meta.categories`` with categories from gold benchmark records.

    The dashboard aggregates profiles from evaluation records; gold records are
    updated by ``profile_all_benchmarks.py`` while eval artifacts are often stale.
    """
    if benchmark_id:
        from text2sql_eval_toolkit.utils import load_benchmark_records

        benchmark_records = load_benchmark_records(benchmark_id)
    else:
        path = Path(benchmark_json_path or "")
        if not path.is_file():
            return eval_records

        with open(path, "r", encoding="utf-8") as f:
            benchmark_records = json.load(f)
    if not isinstance(benchmark_records, list):
        return eval_records

    category_index = build_benchmark_category_index(benchmark_records)
    if not category_index:
        return eval_records

    for record in eval_records:
        try:
            question_id = str(get_question_id(record))
        except ValueError:
            continue
        benchmark_categories = category_index.get(question_id)
        if not benchmark_categories:
            continue
        if "meta" not in record:
            record["meta"] = {}
        existing = record["meta"].get("categories", [])
        record["meta"]["categories"] = sorted(
            set(existing) | set(benchmark_categories)
        )

    return eval_records


def analyze_record(record: Dict, dialect: str = "postgres") -> Dict:
    """
    Profile ground-truth SQL and optional natural-language question for a record.

    Returns:
        Dict with ``features`` and merged ``categories`` (SQL + question tags).
    """
    gt_sqls = get_gt_sqls(record)
    if len(gt_sqls) > 1:
        logger.warning("More than one ground-truth SQL; profiling only the first.")

    analysis = analyze_sql_query(gt_sqls[0], dialect)
    question = get_question_text(record)
    if question:
        analysis["categories"] = sorted(
            set(analysis["categories"]) | set(analyze_question(question))
        )
    return analysis


def merge_dictionaries(original_dict, new_dict):
    """
    Merges new_dict into original_dict in-place with specific logic for 'features' and 'categories' keys.

    Args:
        original_dict (dict): The original dictionary (modified in-place)
        new_dict (dict): The new dictionary to merge

    Returns:
        bool: True if any conflicts/overwrites occurred, False otherwise
    """
    overwrite_occurred = False

    for key, value in new_dict.items():
        if key == "features":
            # Initialize features if it doesn't exist
            if key not in original_dict:
                original_dict[key] = {}

            # Merge features dictionaries
            for feature_key, feature_value in value.items():
                if (
                    feature_key in original_dict[key]
                    and original_dict[key][feature_key] != feature_value
                ):
                    overwrite_occurred = True
                original_dict[key][feature_key] = feature_value

        elif key == "categories":
            # Initialize categories if it doesn't exist
            if key not in original_dict:
                original_dict[key] = []

            # Add new categories that aren't already present
            for category in value:
                if category not in original_dict[key]:
                    original_dict[key].append(category)

        else:
            # For other keys, new dictionary takes precedence
            if key in original_dict and original_dict[key] != value:
                overwrite_occurred = True
            original_dict[key] = value

    return overwrite_occurred


def profile_records(
    records: List[Dict], dialect: str = "postgres"
) -> tuple[List[Dict], bool]:
    """Profile in-memory records and return them with an overwrite flag."""
    if not isinstance(records, list):
        raise ValueError("Records must be a list of objects.")

    overwrite_occurred = False
    for record in tqdm(records):
        try:
            analysis_result = analyze_record(record, dialect)
        except Exception as e:
            try:
                sql_query = get_gt_sqls(record)[0]
            except Exception:
                sql_query = get_question_text(record) or "<unknown>"
            logger.error(f"Failed to profile record: {sql_query}. Error: {repr(e)}")
            continue
        if "meta" not in record:
            record["meta"] = analysis_result
        else:
            overwrite_occurred = merge_dictionaries(record["meta"], analysis_result)

    return records, overwrite_occurred


def profile_benchmark_id(
    benchmark_id: str,
    *,
    profile_gold: bool = True,
    profile_predictions: bool = True,
    profile_eval: bool = True,
) -> None:
    """Profile gold and/or result records for a benchmark in SQLite."""
    from text2sql_eval_toolkit.utils import (
        get_benchmark_info,
        load_benchmark_records,
        load_predictions_data,
        save_benchmark_records,
        save_predictions_data,
    )

    benchmark_info = get_benchmark_info(benchmark_id)
    dialect = benchmark_info["db_engine"]["db_type"]
    if dialect == "db2":
        dialect = "postgres"

    if profile_gold:
        gold_records = load_benchmark_records(benchmark_id)
        profile_records(gold_records, dialect)
        save_benchmark_records(benchmark_id, gold_records)
        logger.info(f"Profiling complete for gold records: {benchmark_id}")

    if profile_predictions or profile_eval:
        try:
            pred_records = load_predictions_data(
                benchmark_id, include_eval=profile_eval
            )
        except ValueError:
            pred_records = []
        if pred_records:
            profile_records(pred_records, dialect)
            save_predictions_data(
                benchmark_id,
                pred_records,
                include_eval=profile_eval,
                status="evaluated" if profile_eval else "executed",
            )
            logger.info(f"Profiling complete for result records: {benchmark_id}")


def profile_pred_or_eval_json_file(
    json_file_path: str, dialect: str = "postgres"
) -> None:
    with open(json_file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    backup_file_path = json_file_path + ".bak"
    shutil.copy2(json_file_path, backup_file_path)

    data, overwrite_occurred = profile_records(data, dialect)

    with open(json_file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    if not overwrite_occurred:
        os.remove(backup_file_path)
    else:
        print(f"Backup created at {backup_file_path} due to overwrites.")

    logger.info(f"Profiling complete. Results written in {json_file_path}")
