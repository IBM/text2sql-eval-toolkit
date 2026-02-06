#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

import json
import os
from sqlglot import parse_one, exp
import shutil
from tqdm import tqdm
from typing import Dict
from text2sql_eval_toolkit.utils import get_gt_sqls
from text2sql_eval_toolkit.logging import get_logger


logger = get_logger(__name__)


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

    return {"features": features, "categories": sorted(tags)}


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


def profile_pred_or_eval_json_file(
    json_file_path: str, dialect: str = "postgres"
) -> None:
    # Load the JSON data
    with open(json_file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    backup_file_path = json_file_path + ".bak"
    shutil.copy2(json_file_path, backup_file_path)

    # Ensure the data is a list
    if not isinstance(data, list):
        raise ValueError("JSON file must contain an array of objects.")

    # Track if any overwrites occur
    overwrite_occurred = False
    # Process each record in the input
    for record in tqdm(data):
        gt_sqls = get_gt_sqls(record)
        sql_query = gt_sqls[0]
        if len(gt_sqls) > 1:
            logger.warning(
                f"More than on gt query in record in {json_file_path}. Profiling only the first one."
            )

        try:
            analysis_result = analyze_sql_query(sql_query, dialect)
        except Exception as e:
            logger.error(f"Failed to profile SQL query: {sql_query}. Error: {repr(e)}")
            continue
        # Initialize or update the 'meta' field
        if "meta" not in record:
            record["meta"] = analysis_result
        else:
            overwrite_occurred = merge_dictionaries(record["meta"], analysis_result)

        # Write the updated data back to the original file
        with open(json_file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    if not overwrite_occurred:
        os.remove(backup_file_path)
    else:
        print(f"Backup created at {backup_file_path} due to overwrites.")

    logger.info(f"Profiling complete. Results written in {json_file_path}")
