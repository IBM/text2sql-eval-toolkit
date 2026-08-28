#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
"""
Regression tests for defects found while documenting the public API.

Each was real behaviour that the docstrings had to warn about. Writing the
warning is not a fix, so each is pinned here.
"""

import asyncio
import json

import pandas as pd
import pytest

from text2sql_eval_toolkit import (
    compare_result_dfs,
    evaluate_predictions,
    get_gt_sqls,
    get_question_id,
    get_utterance,
    normalize_record,
    split_summary,
    sql_exact_match,
    sqlglot_parsed_queries_equivalent,
)


class TestReadersDoNotMutate:
    """Reading a field used to rewrite the caller's record as a side effect."""

    def test_readers_leave_the_record_untouched(self):
        record = {"question_id": 7, "page_content": "How many?", "SQL": "SELECT 1"}
        before = dict(record)

        get_question_id(record)
        get_utterance(record)
        get_gt_sqls(record)

        assert record == before

    def test_normalize_record_writes_the_canonical_keys(self):
        record = {"qid": 7, "page_content": "How many?", "target": "SELECT 1"}
        normalize_record(record)
        assert record["id"] == 7
        assert record["utterance"] == "How many?"
        assert record["sql"] == ["SELECT 1"]

    def test_normalize_record_tolerates_missing_fields(self):
        # Inference normalises before storing; a record without ground truth is
        # still worth normalising for its id and question.
        record = {"qid": 3, "question": "q"}
        normalize_record(record)
        assert record["id"] == 3 and "sql" not in record

    def test_zero_is_a_valid_id(self):
        record = {"id": 0, "utterance": "u"}
        normalize_record(record)
        assert record["id"] == 0


class TestStringLiteralsAreNotCaseFolded:
    """`WHERE name = 'bob'` matching `'BOB'` is a false positive."""

    def test_literal_case_now_matters(self):
        assert not sql_exact_match(
            "SELECT a FROM t WHERE n = 'bob'", "SELECT a FROM t WHERE n = 'BOB'"
        )

    def test_keyword_and_identifier_case_still_ignored(self):
        assert sql_exact_match("SELECT a FROM t", "select A from T")

    def test_formatting_still_ignored(self):
        assert sql_exact_match("SELECT a FROM t;", "SELECT   a\nFROM t")

    def test_doubled_quotes_stay_inside_the_literal(self):
        assert sql_exact_match(
            "SELECT a FROM t WHERE n = 'it''s'", "select a from t where n = 'it''s'"
        )


class TestOrderByDetection:
    """Row-order significance was decided by a substring search."""

    def test_order_by_inside_a_literal_is_not_an_ordered_query(self):
        ordered = pd.DataFrame({"x": [2, 1]})
        reversed_rows = pd.DataFrame({"x": [1, 2]})
        match, _, _ = compare_result_dfs(
            ordered, reversed_rows, "SELECT x FROM t WHERE label = 'order by'"
        )
        assert match == 1

    def test_a_real_order_by_still_makes_row_order_significant(self):
        ordered = pd.DataFrame({"x": [2, 1]})
        reversed_rows = pd.DataFrame({"x": [1, 2]})
        match, _, _ = compare_result_dfs(
            ordered, reversed_rows, "SELECT x FROM t ORDER BY x"
        )
        assert match == 0


class TestNonSelectStatements:
    """Two byte-identical INSERTs used to compare unequal."""

    def test_identical_inserts_are_equivalent(self):
        assert sqlglot_parsed_queries_equivalent(
            "INSERT INTO t VALUES (1)", "INSERT INTO t VALUES (1)"
        )

    def test_differing_inserts_are_not(self):
        assert not sqlglot_parsed_queries_equivalent(
            "INSERT INTO t VALUES (1)", "INSERT INTO t VALUES (2)"
        )

    def test_different_statement_kinds_are_never_equivalent(self):
        assert not sqlglot_parsed_queries_equivalent(
            "SELECT a FROM t", "INSERT INTO t VALUES (1)"
        )

    def test_selects_still_compared(self):
        assert sqlglot_parsed_queries_equivalent("SELECT a FROM t", "SELECT a FROM t")
        assert not sqlglot_parsed_queries_equivalent(
            "SELECT a FROM t", "SELECT b FROM t"
        )


class TestSyncEntryPointInsideAnEventLoop:
    """`asyncio.run` refuses to run inside a loop, which notebooks and servers have."""

    def test_evaluate_predictions_works_inside_a_running_loop(self, tmp_path):
        predictions = tmp_path / "b-predictions.json"
        predictions.write_text(json.dumps([]), encoding="utf-8")

        async def call_from_a_loop():
            return evaluate_predictions(str(predictions))

        data, summary_df = asyncio.run(call_from_a_loop())
        assert data == []

    def test_it_still_works_without_a_loop(self, tmp_path):
        predictions = tmp_path / "b-predictions.json"
        predictions.write_text(json.dumps([]), encoding="utf-8")
        data, _ = evaluate_predictions(str(predictions))
        assert data == []

    def test_errors_surface_on_the_calling_thread(self, tmp_path):
        missing = tmp_path / "does-not-exist.json"

        async def call_from_a_loop():
            return evaluate_predictions(str(missing))

        # The worker thread must re-raise here, not swallow into a None result.
        with pytest.raises((FileNotFoundError, OSError)):
            asyncio.run(call_from_a_loop())


class TestSummaryShape:
    """The judge config sits among the pipeline ids and must not be mistaken for one."""

    def test_split_summary_separates_the_two(self):
        summary = {
            "pipeA": {"num_records": 1},
            "pipeB": {"num_records": 2},
            "llm_judge_config": {"model": {"id": "wxai:x"}},
        }
        pipelines, judge = split_summary(summary)
        assert sorted(pipelines) == ["pipeA", "pipeB"]
        assert judge["model"]["id"] == "wxai:x"

    def test_absent_judge_config_is_none(self):
        pipelines, judge = split_summary({"pipeA": {}})
        assert sorted(pipelines) == ["pipeA"] and judge is None

    def test_does_not_mutate_the_input(self):
        # Callers used to `.pop()` the key, which edited a summary the caller
        # may still need.
        summary = {"pipeA": {}, "llm_judge_config": {}}
        split_summary(summary)
        assert "llm_judge_config" in summary

    def test_tolerates_junk(self):
        assert split_summary(None) == ({}, None)
