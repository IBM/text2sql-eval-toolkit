#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Human-readable definitions for per-record evaluation metrics (evaluate_prediction output).
Used by the dashboard API and UI.
"""

from typing import Any, Dict, List, Literal, TypedDict


class MetricDefinition(TypedDict):
    group: str
    name: str
    description: str
    value_type: Literal["binary", "float", "int", "text"]


# Groups are stable labels for the UI (accordion / table sections).
METRIC_DEFINITIONS: List[MetricDefinition] = [
    {
        "group": "Execution match",
        "name": "execution_accuracy",
        "description": "1 if the predicted result table exactly matches the ground-truth result (same rows/columns per toolkit comparison); 0 otherwise.",
        "value_type": "binary",
    },
    {
        "group": "Execution match",
        "name": "non_empty_execution_accuracy",
        "description": "1 if execution_accuracy holds and the result is non-empty; 0 otherwise.",
        "value_type": "binary",
    },
    {
        "group": "Execution match",
        "name": "subset_non_empty_execution_accuracy",
        "description": "1 if the predicted result is a non-empty subset or superset of the ground-truth result (relaxed match); evaluation may stop at the first GT SQL that satisfies this.",
        "value_type": "binary",
    },
    {
        "group": "Execution match",
        "name": "logic_execution_accuracy",
        "description": "Matches subset when no logic_df is present. If logic_df exists on the prediction, compares that executed result to GT; otherwise same as subset match for that GT variant.",
        "value_type": "binary",
    },
    {
        "group": "Execution match",
        "name": "bird_execution_accuracy",
        "description": "1 if the BIRD-style relaxed dataframe comparison passes between GT and predicted results; 0 otherwise.",
        "value_type": "binary",
    },
    {
        "group": "SQL equivalence",
        "name": "sql_exact_match",
        "description": "1 if the predicted SQL string exactly matches the ground-truth SQL string (after normalization used by the toolkit).",
        "value_type": "binary",
    },
    {
        "group": "SQL equivalence",
        "name": "sqlglot_equivalence",
        "description": "1 if SQLGlot considers the predicted and ground-truth queries equivalent (parsed).",
        "value_type": "binary",
    },
    {
        "group": "SQL equivalence",
        "name": "sqlglot_optimized_equivalence",
        "description": "1 if SQLGlot optimized forms of the two queries are equivalent.",
        "value_type": "binary",
    },
    {
        "group": "SQL equivalence",
        "name": "sqlparse_equivalence",
        "description": "1 if sqlparse-based equivalence holds between predicted and ground-truth SQL.",
        "value_type": "binary",
    },
    {
        "group": "SQL equivalence",
        "name": "sql_syntactic_equivalence",
        "description": "1 if any of sqlglot_equivalence, sqlglot_optimized_equivalence, sqlparse_equivalence, or sql_exact_match is 1.",
        "value_type": "binary",
    },
    {
        "group": "Parsing",
        "name": "is_sqlglot_parsable",
        "description": "1 if the predicted SQL parses with SQLGlot.",
        "value_type": "binary",
    },
    {
        "group": "Parsing",
        "name": "is_sqlparse_parsable",
        "description": "1 if the predicted SQL parses with sqlparse.",
        "value_type": "binary",
    },
    {
        "group": "LLM judge",
        "name": "llm_score",
        "description": "Model-based score (often 0, 0.5, or 1) when LLM-as-judge is enabled; may be skipped when execution already subset-matches.",
        "value_type": "float",
    },
    {
        "group": "LLM judge",
        "name": "llm_explanation",
        "description": "Short explanation from the LLM judge, or a note when the judge was not invoked.",
        "value_type": "text",
    },
    {
        "group": "LLM judge",
        "name": "llm_judge_error",
        "description": "Present if the LLM judge call failed; contains error details.",
        "value_type": "text",
    },
    {
        "group": "Timing and tokens",
        "name": "prompt_tokens",
        "description": "Prompt tokens copied from the prediction’s token_usage, if any.",
        "value_type": "int",
    },
    {
        "group": "Timing and tokens",
        "name": "completion_tokens",
        "description": "Completion tokens from token_usage, if any.",
        "value_type": "int",
    },
    {
        "group": "Timing and tokens",
        "name": "total_tokens",
        "description": "Total tokens from token_usage, if any.",
        "value_type": "int",
    },
    {
        "group": "Timing and tokens",
        "name": "inference_time_ms",
        "description": "Inference latency from the prediction, if recorded.",
        "value_type": "float",
    },
    {
        "group": "Timing and tokens",
        "name": "execution_time_ms",
        "description": "SQL execution time from the prediction, if recorded.",
        "value_type": "float",
    },
    {
        "group": "Errors",
        "name": "df_error",
        "description": "1 if the predicted result could not be parsed as a dataframe or was missing; 0 otherwise.",
        "value_type": "binary",
    },
    {
        "group": "Errors",
        "name": "df_error_message",
        "description": "Details when df_error is set (e.g. SQL execution failure message).",
        "value_type": "text",
    },
    {
        "group": "Errors",
        "name": "eval_error",
        "description": "1 if evaluation raised an unexpected exception during metric computation.",
        "value_type": "binary",
    },
    {
        "group": "Errors",
        "name": "eval_error_message",
        "description": "Exception repr when eval_error is set.",
        "value_type": "text",
    },
    {
        "group": "Ground truth (when matched)",
        "name": "gt_sql",
        "description": "Ground-truth SQL that produced the accepted match when subset_non_empty_execution_accuracy is 1.",
        "value_type": "text",
    },
    {
        "group": "Ground truth (when matched)",
        "name": "gt_df",
        "description": "Serialized ground-truth dataframe for the matched GT SQL when subset_non_empty_execution_accuracy is 1.",
        "value_type": "text",
    },
]


def get_metric_definitions_payload() -> Dict[str, Any]:
    """Return API-serializable structure: flat list plus grouped index."""
    groups: List[str] = []
    seen = set()
    for m in METRIC_DEFINITIONS:
        g = m["group"]
        if g not in seen:
            seen.add(g)
            groups.append(g)
    return {
        "groups": groups,
        "metrics": list(METRIC_DEFINITIONS),
    }
