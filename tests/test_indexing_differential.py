#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Differential tests: the index must return exactly what the endpoints' previous
in-Python filtering returned.

``_reference_filter`` below is a faithful copy of the logic that
``ui/server.py::list_errors`` applied while iterating the parsed artifact. Every
filter combination is run through both paths and the resulting record ids,
ordering, and totals must match. This is the safety net for replacing that code:
a divergence here means the dashboard would show different records than before.
"""

import itertools
import json
import random

import pytest

from text2sql_eval_toolkit.indexing import build_index
from text2sql_eval_toolkit.indexing.store import EvalIndex, default_filters

PIPELINES = [
    "wxai:meta-llama/llama-3-3-70b-instruct-greedy-zero-shot-chatapi",
    "wxai:openai/gpt-oss-120b-agentic-baseline1-3attempts",
    "wxai:ibm/granite-4-h-small-greedy-zero-shot-chatapi",
]
METRICS = ["execution_accuracy", "subset_non_empty_execution_accuracy", "llm_score"]


def _synthetic(n_records: int = 260, seed: int = 7):
    """An artifact with gaps: missing pipelines, missing metrics, text metrics."""
    rng = random.Random(seed)
    records = []
    for i in range(n_records):
        preds = {}
        for pipeline in PIPELINES:
            if rng.random() < 0.15:
                continue  # pipeline absent for this record
            evaluation = {}
            for metric in METRICS:
                if rng.random() < 0.12:
                    continue  # metric absent
                evaluation[metric] = rng.choice([0, 1, 0.5])
            evaluation["llm_explanation"] = rng.choice(["ok", "wrong", None])
            preds[pipeline] = {"predicted_sql": "SELECT 1", "evaluation": evaluation}
        records.append(
            {
                "id": f"rec-{i:04d}",
                "question": rng.choice(
                    ["How many customers?", "List orders", "Total {revenue}", "查询"]
                )
                + f" #{i}",
                "db_id": rng.choice(["shop", "hr"]),
                "predictions": preds,
            }
        )
    return records


def _reference_filter(
    data, *, q, pipeline, metric, value, op, pipeline2, metric2, disagree, failed_only
):
    """Verbatim reimplementation of the previous endpoint filtering."""

    def match_search(rec):
        if not q:
            return True
        q_lower = q.lower()
        rid = str(rec.get("id") or rec.get("question_id") or "")
        question = (
            rec.get("page_content") or rec.get("question") or rec.get("utterance", "")
        )
        return q_lower in rid.lower() or q_lower in str(question).lower()

    def get_metric(rec, pl, m):
        preds = rec.get("predictions", {})
        if pl not in preds:
            return None
        val = preds[pl].get("evaluation", {}).get(m)
        if isinstance(val, (int, float)):
            return float(val)
        return None

    def apply_op(lhs, rhs, operator):
        if lhs is None:
            return False
        return {
            "eq": lhs == rhs,
            "ne": lhs != rhs,
            "lt": lhs < rhs,
            "gt": lhs > rhs,
            "le": lhs <= rhs,
            "ge": lhs >= rhs,
        }.get(operator, False)

    out = []
    for rec in data:
        if not match_search(rec):
            continue
        if pipeline and value is not None:
            if not apply_op(get_metric(rec, pipeline, metric), value, op):
                continue
        if failed_only:
            if get_metric(rec, pipeline, "execution_accuracy") != 0:
                continue
        if pipeline and pipeline2 and disagree:
            v1 = get_metric(rec, pipeline, metric)
            v2 = get_metric(rec, pipeline2, metric2 or metric)
            if v1 is None or v2 is None or v1 == v2:
                continue
        out.append(rec)
    return out


@pytest.fixture(scope="module")
def indexed(tmp_path_factory):
    data = _synthetic()
    path = tmp_path_factory.mktemp("diff") / "synthetic-predictions_eval.json"
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    build_index(path)
    index = EvalIndex.for_benchmark("synthetic", path.parent)
    yield data, index
    index.close()


FILTER_CASES = [
    default_filters(),
    default_filters(q="customers"),
    default_filters(q="REC-0001"),  # case-insensitive id match
    default_filters(q="{revenue}"),  # braces in search text
    default_filters(q="查询"),  # non-ascii
    default_filters(q="no-such-text"),
    *[
        default_filters(pipeline=p, metric=m, value=v, op=o)
        for p, m, v, o in itertools.product(
            PIPELINES[:2],
            METRICS[:2],
            [0, 1, 0.5],
            ["eq", "ne", "lt", "gt", "le", "ge"],
        )
    ],
    default_filters(pipeline=PIPELINES[0], failed_only=True),
    default_filters(pipeline="does-not-exist", value=1),
    default_filters(pipeline=PIPELINES[0], metric="not_a_metric", value=1),
    default_filters(pipeline=PIPELINES[0], pipeline2=PIPELINES[1], disagree=True),
    default_filters(
        pipeline=PIPELINES[0], pipeline2=PIPELINES[1], metric2=METRICS[1], disagree=True
    ),
    default_filters(
        q="orders", pipeline=PIPELINES[0], metric=METRICS[0], value=0, op="eq"
    ),
    default_filters(
        q="list", pipeline=PIPELINES[1], pipeline2=PIPELINES[2], disagree=True
    ),
]


@pytest.mark.parametrize("filters", FILTER_CASES, ids=range(len(FILTER_CASES)))
def test_filtering_matches_the_previous_implementation(indexed, filters):
    data, index = indexed
    expected = _reference_filter(data, **filters)
    expected_ids = [str(r["id"]) for r in expected]

    items, total = index.list_records(page=1, page_size=10_000, **filters)

    assert total == len(expected_ids)
    assert [i.record_id for i in items] == expected_ids


@pytest.mark.parametrize("page_size", [1, 7, 25, 100])
def test_pagination_matches_and_is_stable(indexed, page_size):
    data, index = indexed
    filters = default_filters(pipeline=PIPELINES[0], metric=METRICS[0], value=1)
    expected = [str(r["id"]) for r in _reference_filter(data, **filters)]

    collected = []
    for page in range(1, (len(expected) // page_size) + 2):
        items, total = index.list_records(page=page, page_size=page_size, **filters)
        assert total == len(expected)
        collected += [i.record_id for i in items]
    assert collected == expected


def test_evaluation_blocks_match_the_previous_payload(indexed):
    """The listing exposed every pipeline's evaluation block, verbatim."""
    data, index = indexed
    by_id = {str(r["id"]): r for r in data}
    items, _ = index.list_records(page=1, page_size=50, **default_filters())
    for item in items:
        expected = {
            pipeline: pred.get("evaluation", {})
            for pipeline, pred in by_id[item.record_id]["predictions"].items()
        }
        assert item.predictions == expected


def test_read_record_returns_the_whole_record(indexed):
    data, index = indexed
    for record in (data[0], data[137], data[-1]):
        assert index.read_record(str(record["id"])) == record


def test_read_record_unknown_id_returns_none(indexed):
    _, index = indexed
    assert index.read_record("nope") is None


def test_confusion_matches_a_manual_crosstab(indexed):
    data, index = indexed
    got = index.confusion(PIPELINES[0], METRICS[0], PIPELINES[1], METRICS[0])

    expected = {}
    for rec in data:
        preds = rec.get("predictions", {})
        a = preds.get(PIPELINES[0], {}).get("evaluation", {}).get(METRICS[0])
        b = preds.get(PIPELINES[1], {}).get("evaluation", {}).get(METRICS[0])
        if isinstance(a, (int, float)) and isinstance(b, (int, float)):
            key = (float(a), float(b))
            expected[key] = expected.get(key, 0) + 1
    assert got == expected


def test_pipeline_and_metric_listings(indexed):
    data, index = indexed
    expected_pipelines = set()
    for rec in data:
        expected_pipelines.update(rec.get("predictions", {}))
    assert set(index.pipeline_ids()) == expected_pipelines
    assert "llm_explanation" not in index.metric_names()
