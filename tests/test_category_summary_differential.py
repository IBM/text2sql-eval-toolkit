#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
The by-category summary used to parse the whole artifact; it now reads the same
numbers out of the index.

That endpoint feeds the benchmark page's headline table, so a divergence is not
a slow page -- it is a page of confident wrong averages. These tests run both
paths over the same data and require the results to be **equal**, not close:
the index preserves record order precisely so the floating-point sums accumulate
in the same sequence, and anything less than equality would mean that guarantee
had quietly lapsed.
"""

import json
import random

import pytest

from text2sql_eval_toolkit.indexing import build_index
from text2sql_eval_toolkit.indexing.store import EvalIndex
from text2sql_eval_toolkit.ui.routers_benchmarks import (
    _collect_metric_values,
    _summarize_metric_values,
)

PIPELINES = [
    "wxai:openai/gpt-oss-120b-greedy-zero-shot-chatapi",
    "wxai:ibm/granite-4-h-small-agentic-baseline1-3attempts",
    "gemini:gemini-3-flash-preview-greedy-zero-shot-chatapi",
]
METRICS = ["execution_accuracy", "subset_non_empty_execution_accuracy", "llm_score"]
CATEGORIES = [
    "has_join",
    "has_aggregation",
    "has_nested_query",
    "single_source_basic",
    "has_window_function",
]


def _synthetic(n_records=180, seed=11):
    """
    Records with the gaps real data has: a pipeline missing entirely, a metric
    absent, a non-numeric metric, a record with no categories, and a boolean --
    which passes `isinstance(v, (int, float))` in Python and so must be counted.
    """
    rng = random.Random(seed)
    records = []
    for i in range(n_records):
        preds = {}
        for pipeline in PIPELINES:
            if rng.random() < 0.15:
                continue
            evaluation = {}
            for metric in METRICS:
                if rng.random() < 0.1:
                    continue
                evaluation[metric] = rng.choice([0, 1, 0.5, 0.25])
            evaluation["is_sqlglot_parsable"] = rng.choice([True, False])
            evaluation["llm_explanation"] = "not a number"
            preds[pipeline] = {"predicted_sql": "SELECT 1", "evaluation": evaluation}

        if rng.random() < 0.1:
            categories = []  # profiling never ran for this record
        else:
            categories = rng.sample(CATEGORIES, rng.randint(1, 3))

        records.append(
            {
                "id": f"rec-{i:04d}",
                "question": f"Question {i}",
                "db_id": rng.choice(["shop", "hr"]),
                "meta": {"categories": categories, "features": {}},
                "predictions": preds,
            }
        )
    return records


@pytest.fixture(scope="module")
def index_and_records(tmp_path_factory):
    root = tmp_path_factory.mktemp("catsum")
    records = _synthetic()
    artifact = root / "demo-predictions_eval.json"
    artifact.write_text(json.dumps(records), encoding="utf-8")
    with EvalIndex(build_index(artifact), artifact) as index:
        yield index, records


def test_the_index_returns_the_same_values_the_full_parse_collected(
    index_and_records,
):
    index, records = index_and_records
    assert index.metric_values_by_category() == _collect_metric_values(records)


def test_the_summary_is_identical_through_both_paths(index_and_records):
    index, records = index_and_records
    from_index = _summarize_metric_values(*index.metric_values_by_category())
    from_parse = _summarize_metric_values(*_collect_metric_values(records))
    assert from_index == from_parse


def test_every_category_present_in_the_data_is_present_in_the_summary(
    index_and_records,
):
    index, records = index_and_records
    expected = {c for r in records for c in r["meta"]["categories"]}
    summary = _summarize_metric_values(*index.metric_values_by_category())
    assert set(summary["categories"]) == expected


def test_a_non_numeric_metric_is_counted_by_neither_path(index_and_records):
    index, _ = index_and_records
    overall, by_category = index.metric_values_by_category()
    for metrics in overall.values():
        assert "llm_explanation" not in metrics


def test_a_boolean_metric_is_counted_by_both_paths(index_and_records):
    """
    `isinstance(True, (int, float))` is True in Python, so the reference
    implementation counts booleans. The index must too, or an average silently
    changes.
    """
    index, _ = index_and_records
    overall, _cats = index.metric_values_by_category()
    for metrics in overall.values():
        assert "is_sqlglot_parsable" in metrics
        assert set(metrics["is_sqlglot_parsable"]) <= {0.0, 1.0}


def test_a_record_with_no_categories_still_counts_towards_overall(tmp_path):
    records = [
        {
            "id": "r1",
            "question": "q",
            "meta": {"categories": []},
            "predictions": {
                PIPELINES[0]: {"evaluation": {"execution_accuracy": 1}},
            },
        }
    ]
    artifact = tmp_path / "demo-predictions_eval.json"
    artifact.write_text(json.dumps(records), encoding="utf-8")
    with EvalIndex(build_index(artifact), artifact) as index:
        overall, by_category = index.metric_values_by_category()
    assert overall[PIPELINES[0]]["execution_accuracy"] == [1.0]
    assert by_category == {}


def test_a_record_with_no_meta_at_all_does_not_break_the_build(tmp_path):
    """Older artifacts predate profiling and carry no `meta` block."""
    records = [
        {
            "id": "r1",
            "question": "q",
            "predictions": {PIPELINES[0]: {"evaluation": {"execution_accuracy": 0}}},
        }
    ]
    artifact = tmp_path / "demo-predictions_eval.json"
    artifact.write_text(json.dumps(records), encoding="utf-8")
    with EvalIndex(build_index(artifact), artifact) as index:
        overall, by_category = index.metric_values_by_category()
    assert overall[PIPELINES[0]]["execution_accuracy"] == [0.0]
    assert by_category == {}


def test_key_order_is_preserved_not_merely_the_values(index_and_records):
    """
    Dict equality ignores key order, so the first version of this passed while
    the JSON response had its metric keys shuffled -- the index was ordering
    metrics by global first-appearance instead of by each record's own key
    order. Serializing catches what `==` does not.
    """
    import json

    index, records = index_and_records
    from_index = _summarize_metric_values(*index.metric_values_by_category())
    from_parse = _summarize_metric_values(*_collect_metric_values(records))
    assert json.dumps(from_index) == json.dumps(from_parse)
