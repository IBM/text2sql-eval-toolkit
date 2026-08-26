#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
``evaluate_prediction`` produces every number the toolkit publishes.

A bug here does not crash anything -- it quietly changes reported accuracy, and
the results are then uploaded, browsed, and cited. It was at 4% coverage, which
is the least-tested code with the widest blast radius in the project.

These tests pin the contract: which metrics appear, what makes each one 1 rather
than 0, that multiple ground truths are honoured, and that failures are reported
rather than silently scored as wrong.
"""

import pandas as pd
import pytest

from text2sql_eval_toolkit.evaluation.evaluation_tools import evaluate_prediction


def df_json(rows, columns):
    """Serialize like the toolkit does: pandas orient='split'."""
    return pd.DataFrame(rows, columns=columns).to_json(orient="split")


CUSTOMERS = df_json([[1, "ada"], [2, "grace"]], ["id", "name"])
CUSTOMERS_REORDERED = df_json([[2, "grace"], [1, "ada"]], ["id", "name"])
NAMES_ONLY = df_json([["ada"], ["grace"]], ["name"])
EMPTY = df_json([], ["id", "name"])
DIFFERENT = df_json([[9, "mallory"]], ["id", "name"])


def record(sql="SELECT id, name FROM customers", gt_df=CUSTOMERS, **extra):
    base = {
        "id": "q1",
        "question": "who are the customers?",
        "sql": sql,
        "gt_df": gt_df,
    }
    base.update(extra)
    return base


def prediction(sql="SELECT id, name FROM customers", df=CUSTOMERS, **extra):
    base = {"predicted_sql": sql, "predicted_df": df}
    base.update(extra)
    return base


# --- the metric surface ---------------------------------------------------


def test_reports_the_documented_metrics():
    result = evaluate_prediction(record(), prediction())
    for metric in (
        "execution_accuracy",
        "non_empty_execution_accuracy",
        "subset_non_empty_execution_accuracy",
        "logic_execution_accuracy",
        "bird_execution_accuracy",
        "is_sqlglot_parsable",
        "is_sqlparse_parsable",
        "sqlglot_equivalence",
        "sqlglot_optimized_equivalence",
        "sqlparse_equivalence",
        "sql_exact_match",
        "sql_syntactic_equivalence",
        "df_error",
        "eval_error",
    ):
        assert metric in result, f"missing metric: {metric}"


def test_every_reported_metric_is_declared_for_the_dashboard():
    """
    The dashboard renders its metric help from metric_definitions; a metric that
    appears in results but not there shows up unexplained.
    """
    from text2sql_eval_toolkit.evaluation.metric_definitions import METRIC_DEFINITIONS

    declared = {m["name"] for m in METRIC_DEFINITIONS}
    produced = set(evaluate_prediction(record(), prediction()))
    undeclared = produced - declared
    assert not undeclared, f"metrics with no definition: {sorted(undeclared)}"


# --- exact matching -------------------------------------------------------


def test_identical_results_score_one():
    result = evaluate_prediction(record(), prediction())
    assert result["execution_accuracy"] == 1
    assert result["non_empty_execution_accuracy"] == 1
    assert result["subset_non_empty_execution_accuracy"] == 1
    assert result["eval_error"] == 0


def test_different_results_score_zero():
    result = evaluate_prediction(record(), prediction(df=DIFFERENT))
    assert result["execution_accuracy"] == 0
    assert result["non_empty_execution_accuracy"] == 0
    assert result["subset_non_empty_execution_accuracy"] == 0


def test_row_order_does_not_decide_execution_accuracy():
    """Two results with the same rows in a different order are the same answer."""
    result = evaluate_prediction(record(), prediction(df=CUSTOMERS_REORDERED))
    assert result["execution_accuracy"] == 1


def test_empty_result_is_not_a_non_empty_match():
    """
    The distinction the toolkit exists to make: an empty result can match an
    empty ground truth exactly while being useless as an answer.
    """
    result = evaluate_prediction(record(gt_df=EMPTY), prediction(df=EMPTY))
    assert result["execution_accuracy"] == 1
    assert result["non_empty_execution_accuracy"] == 0
    assert result["subset_non_empty_execution_accuracy"] == 0


# --- the relaxed subset match --------------------------------------------


def test_subset_match_accepts_a_narrower_column_set():
    """
    README's motivating case: asked for customers, returning names alone is a
    reasonable answer even though the columns differ.
    """
    result = evaluate_prediction(
        record(gt_df=CUSTOMERS),
        prediction(sql="SELECT name FROM customers", df=NAMES_ONLY),
    )
    assert result["subset_non_empty_execution_accuracy"] == 1
    assert result["execution_accuracy"] == 0, "still not an exact match"


# --- multiple ground truths ----------------------------------------------


def test_matching_any_ground_truth_counts():
    """Many questions have more than one correct SQL."""
    result = evaluate_prediction(
        record(
            sql=["SELECT id, name FROM customers", "SELECT name FROM customers"],
            gt_df=[DIFFERENT, NAMES_ONLY],
        ),
        prediction(sql="SELECT name FROM customers", df=NAMES_ONLY),
    )
    assert result["subset_non_empty_execution_accuracy"] == 1


def test_the_matched_ground_truth_is_reported():
    """The dashboard shows which ground truth was accepted."""
    result = evaluate_prediction(
        record(
            sql=["SELECT 1 AS wrong", "SELECT name FROM customers"],
            gt_df=[DIFFERENT, NAMES_ONLY],
        ),
        prediction(sql="SELECT name FROM customers", df=NAMES_ONLY),
    )
    assert result["subset_non_empty_execution_accuracy"] == 1
    assert result.get("gt_sql") == "SELECT name FROM customers"


def test_matching_no_ground_truth_scores_zero():
    result = evaluate_prediction(
        record(sql=["SELECT 1", "SELECT 2"], gt_df=[DIFFERENT, DIFFERENT]),
        prediction(df=CUSTOMERS),
    )
    assert result["subset_non_empty_execution_accuracy"] == 0


# --- SQL-level comparison -------------------------------------------------


def test_identical_sql_is_an_exact_match():
    result = evaluate_prediction(record(), prediction())
    assert result["sql_exact_match"] == 1
    assert result["sql_syntactic_equivalence"] == 1


def test_exact_match_normalises_whitespace_and_case():
    """
    "Exact" is after the toolkit's normalisation, as metric_definitions states.
    Pinning it because the name reads as byte equality.
    """
    result = evaluate_prediction(
        record(sql="SELECT id, name FROM customers"),
        prediction(sql="select   id,\n  name\nfrom customers"),
    )
    assert result["sql_exact_match"] == 1
    assert result["sql_syntactic_equivalence"] == 1


def test_genuinely_different_sql_is_not_an_exact_match():
    result = evaluate_prediction(
        record(sql="SELECT id, name FROM customers"),
        prediction(sql="SELECT name FROM customers", df=NAMES_ONLY),
    )
    assert result["sql_exact_match"] == 0


def test_unparsable_sql_is_reported_as_unparsable():
    result = evaluate_prediction(
        record(), prediction(sql="SELECT FROM WHERE ((", df=EMPTY)
    )
    assert result["is_sqlglot_parsable"] == 0


# --- failures are reported, not silently scored --------------------------


def test_a_missing_predicted_dataframe_is_flagged():
    result = evaluate_prediction(record(), {"predicted_sql": "SELECT 1"})
    assert result["df_error"] == 1
    assert result["execution_accuracy"] == 0


def test_a_malformed_predicted_dataframe_is_flagged_with_a_message():
    result = evaluate_prediction(record(), prediction(df="not json at all"))
    assert result["df_error"] == 1
    assert result.get("df_error_message")


def test_a_sql_execution_error_is_carried_through():
    result = evaluate_prediction(
        record(),
        {"predicted_sql": "SELECT bad", "sql_execution_error": "no such column: bad"},
    )
    assert result["df_error"] == 1
    assert "bad" in str(result.get("df_error_message", ""))


def test_ground_truth_sql_with_no_dataframe_is_an_evaluation_error():
    """
    Such a record used to return `{"df_error": 0}` -- no metrics, no error flag.
    compute_summary then subscripted a missing metric and took out the whole
    benchmark's summary, so the failure surfaced far from its cause.
    """
    result = evaluate_prediction(record(gt_df=[]), prediction())
    assert result["eval_error"] == 1
    assert "ground-truth dataframe" in result["eval_error_message"]


def test_one_unevaluable_record_does_not_abort_the_summary():
    from text2sql_eval_toolkit.evaluation.evaluation_tools import compute_summary

    good = evaluate_prediction(record(), prediction())
    unevaluable = evaluate_prediction(record(gt_df=[]), prediction())

    summary = compute_summary({"pipeline-a": [good, unevaluable]}, None)["pipeline-a"]
    assert summary["num_records"] == 2
    assert (
        summary["num_evaluated"] == 1
    ), "the unevaluable record is excluded, not fatal"


def test_a_record_missing_metrics_entirely_counts_as_zero_not_a_crash():
    """The stated intent is that failures count as 0; it used to raise KeyError."""
    from text2sql_eval_toolkit.evaluation.evaluation_tools import compute_summary

    good = evaluate_prediction(record(), prediction())
    summary = compute_summary({"p": [good, {"df_error": 0}]}, None)["p"]
    assert summary["num_records"] == 2


# --- token and timing passthrough ----------------------------------------


def test_token_usage_and_timings_are_copied_from_the_prediction():
    result = evaluate_prediction(
        record(),
        prediction(
            token_usage={
                "prompt_tokens": 120,
                "completion_tokens": 8,
                "total_tokens": 128,
            },
            inference_time_ms=42.5,
            execution_time_ms=7.25,
        ),
    )
    assert result["prompt_tokens"] == 120
    assert result["completion_tokens"] == 8
    assert result["inference_time_ms"] == 42.5
    assert result["execution_time_ms"] == 7.25


# --- LLM judge wiring -----------------------------------------------------


def test_cached_llm_judge_results_are_reused_without_calling_the_model(monkeypatch):
    """Re-running evaluation must not spend budget re-judging."""
    called = []
    monkeypatch.setattr(
        "text2sql_eval_toolkit.evaluation.evaluation_tools.evaluate_sql_prediction_with_llm",
        lambda *a, **k: called.append(1)
        or {"verdict": "Yes", "score": 1.0, "explanation": ""},
    )
    pred = prediction(
        df=DIFFERENT,
        evaluation={"llm_score": 0.5, "llm_explanation": "cached verdict"},
    )
    result = evaluate_prediction(
        record(), pred, llm_judge_config={"model": {"id": "wxai:x"}}
    )
    assert result["llm_score"] == 0.5
    assert not called, "a cached verdict must not trigger a model call"


def test_no_judge_config_means_no_judge_metrics():
    result = evaluate_prediction(record(), prediction())
    assert "llm_score" not in result or result.get("llm_score") is None


@pytest.mark.parametrize("bad_record", [{}, {"id": "x"}, {"sql": None}])
def test_a_record_without_ground_truth_sql_does_not_crash_the_run(bad_record):
    """
    One malformed record must not abort evaluation of an entire benchmark.
    """
    result = evaluate_prediction(bad_record, prediction())
    assert result["eval_error"] == 1
    assert result.get("eval_error_message")
