#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
``error_analysis`` writes the per-pipeline failure reports that ship beside
``data/results/README.md`` and that the analysis notebooks render inline. It was
at 9%.

The risk here is not a crash -- it is a report that looks fine and says
something untrue. A failure count that includes records the pipeline never
attempted, or a truncation that silently drops the rows a reader is looking for,
both read as authoritative.
"""

import json

import pandas as pd
import pytest

from text2sql_eval_toolkit.analysis.error_analysis import (
    chat_prompt_to_html,
    export_failed_examples_to_markdown,
    format_failed_example,
    get_failed_records,
    get_pipeline_ids,
    head_tail_with_ellipsis,
    safe_code_block,
    safe_snippet,
)

PIPE = "modelA-greedy-zero-shot-chatapi"
OTHER = "modelB-agentic-baseline1-3attempts"


def df_json(rows, columns=("n",)):
    """A dataframe serialized the way the toolkit stores them."""
    return json.dumps(
        {"columns": list(columns), "index": list(range(len(rows))), "data": rows}
    )


def record(rid, *, accuracy=0, pipelines=(PIPE,), **extra):
    predictions = {
        p: {
            "predicted_sql": f"SELECT {rid}",
            "predicted_df": df_json([[1]]),
            "evaluation": {"execution_accuracy": accuracy},
        }
        for p in pipelines
    }
    out = {
        "id": rid,
        "question": f"Question {rid}?",
        "sql": [f"SELECT gold_{rid}"],
        "gt_df": [df_json([[1]])],
        "predictions": predictions,
    }
    out.update(extra)
    return out


# --- discovering pipelines ------------------------------------------------


def test_pipelines_come_from_the_first_record():
    assert get_pipeline_ids([record("r1", pipelines=(PIPE, OTHER))]) == [PIPE, OTHER]


@pytest.mark.parametrize("records", [[], [{"id": "r1"}]])
def test_no_predictions_means_no_pipelines(records):
    """The caller renders "no predictions found" from this rather than crashing."""
    assert get_pipeline_ids(records) is None


# --- which records count as failures --------------------------------------


def test_only_records_scoring_zero_are_failures():
    records = [record("r1", accuracy=0), record("r2", accuracy=1)]
    failed = get_failed_records(records, PIPE)
    assert [r["id"] for r in failed] == ["r1"]


def test_the_metric_is_selectable():
    records = [record("r1", accuracy=1)]
    records[0]["predictions"][PIPE]["evaluation"]["llm_score"] = 0
    assert get_failed_records(records, PIPE, metric="llm_score")
    assert not get_failed_records(records, PIPE, metric="execution_accuracy")


def test_a_metric_the_pipeline_never_reported_is_not_a_failure():
    """Absent is not zero: a metric that was never computed says nothing."""
    assert not get_failed_records([record("r1")], PIPE, metric="never_computed")


def test_a_record_the_pipeline_never_attempted_is_reported_as_a_record():
    """
    A record with no prediction for this pipeline used to be appended as the
    bare string "No predictions for {pipeline}". Two things followed: the
    failure count included it, and the report rendered "Error reading
    prediction in record: ..." with the whole record dict inlined, because the
    formatter was handed a string where it expected a mapping.

    It is still counted -- a pipeline that produced nothing for a question did
    fail on it -- but it stays a record, so the report can say which one.
    """
    records = [record("r1", pipelines=(OTHER,))]
    failed = get_failed_records(records, PIPE)
    assert len(failed) == 1
    assert isinstance(failed[0], dict), "a missing prediction must not become a string"
    assert failed[0]["id"] == "r1"


# --- truncation helpers ---------------------------------------------------


def test_short_text_is_returned_whole():
    assert safe_snippet("short") == "short"


def test_long_text_keeps_both_ends():
    text = "A" * 100 + "B" * 100
    out = safe_snippet(text, head=10, tail=10)
    assert out.startswith("A" * 10)
    assert out.endswith("B" * 10)
    assert "…" in out, "the reader needs to see that something was removed"


def test_a_code_block_escapes_its_content():
    """
    These reports are rendered as HTML. Model output is arbitrary text, so
    anything unescaped in it becomes markup in someone's browser.
    """
    out = safe_code_block("<script>alert(1)</script>")
    assert "<script>" not in out
    assert "&lt;script&gt;" in out


def test_a_code_block_says_when_it_truncated():
    out = safe_code_block("x" * 50, max_length=10)
    assert "truncated" in out


def test_a_small_dataframe_is_not_truncated():
    df = pd.DataFrame({"n": range(5)})
    assert len(head_tail_with_ellipsis(df, k=20)) == 5


def test_a_large_dataframe_keeps_the_head_and_the_tail():
    df = pd.DataFrame({"n": range(100)})
    out = head_tail_with_ellipsis(df, k=3)
    values = out["n"].tolist()
    assert values[:3] == [0, 1, 2]
    assert values[-3:] == [97, 98, 99]
    assert any("truncated" in str(v) for v in values)


def test_truncating_does_not_mutate_the_original():
    df = pd.DataFrame({"n": range(5)})
    head_tail_with_ellipsis(df, k=2)
    assert df["n"].tolist() == [0, 1, 2, 3, 4]


def test_a_chat_prompt_escapes_message_content():
    html = chat_prompt_to_html([{"role": "user", "content": "<img onerror=x>"}])
    assert "<img" not in html
    assert "&lt;img" in html


def test_a_chat_prompt_labels_each_turn():
    html = chat_prompt_to_html(
        [{"role": "system", "content": "a"}, {"role": "user", "content": "b"}]
    )
    assert "System Message 1" in html
    assert "User Message 2" in html


# --- formatting one example -----------------------------------------------


def test_a_failed_example_shows_both_sides():
    out = format_failed_example(record("r1"), PIPE, 1, 1)
    assert "SELECT gold_r1" in out, "the ground truth SQL"
    assert "SELECT r1" in out, "the predicted SQL"
    assert "Question r1?" in out


def test_an_inference_failure_is_reported_as_one():
    """
    No SQL was generated, so there is nothing to diff. Rendering it as a wrong
    answer would send the reader looking for a query that does not exist.
    """
    rec = record("r1")
    rec["predictions"][PIPE] = {
        "inference_error": "context length exceeded",
        "prompt": "the prompt",
    }
    out = format_failed_example(rec, PIPE, 1, 1)
    assert "Inference Failed" in out
    assert "context length exceeded" in out


def test_a_prediction_that_cannot_be_read_degrades_to_a_note():
    """One malformed record must not abort a whole report."""
    out = format_failed_example({"id": "r1"}, PIPE, 1, 1)
    assert "Error reading prediction" in out


def test_an_unparseable_dataframe_is_reported_in_place():
    rec = record("r1")
    rec["gt_df"] = ["not json at all"]
    out = format_failed_example(rec, PIPE, 1, 1)
    assert "Error loading GT DF" in out
    assert "SELECT gold_r1" in out, "the rest of the example still renders"


# --- the whole report -----------------------------------------------------


def test_report_covers_every_pipeline(tmp_path):
    out = tmp_path / "errors.md"
    export_failed_examples_to_markdown(
        [record("r1", accuracy=0, pipelines=(PIPE, OTHER))], out
    )
    text = out.read_text()
    assert PIPE in text and OTHER in text


def test_report_says_so_when_a_pipeline_has_no_failures(tmp_path):
    out = tmp_path / "errors.md"
    export_failed_examples_to_markdown([record("r1", accuracy=1)], out)
    assert "No failed predictions found" in out.read_text()


def test_report_states_how_many_were_shown_of_how_many(tmp_path):
    """
    Capping the examples is fine; capping them silently is not -- a reader
    would take 5 shown as 5 total.
    """
    out = tmp_path / "errors.md"
    records = [record(f"r{i}", accuracy=0) for i in range(10)]
    export_failed_examples_to_markdown(records, out, max_examples=3)
    text = out.read_text()
    assert "3 failed predictions shown (out of 10)" in text


def test_report_creates_missing_directories(tmp_path):
    out = tmp_path / "nested" / "deeper" / "errors.md"
    export_failed_examples_to_markdown([record("r1")], out)
    assert out.exists()


def test_report_with_no_predictions_writes_nothing_rather_than_a_broken_file(tmp_path):
    out = tmp_path / "errors.md"
    export_failed_examples_to_markdown([{"id": "r1"}], out)
    assert not out.exists()


def test_a_missing_prediction_renders_as_an_example_naming_the_question(tmp_path):
    """
    The whole point of keeping the record: the report can say which question
    went unanswered, instead of "Error reading prediction in record: {…}".
    """
    out = tmp_path / "errors.md"
    export_failed_examples_to_markdown(
        [record("r1", pipelines=(PIPE,)), record("r2", pipelines=(OTHER,))], out
    )
    text = out.read_text()
    assert "No Prediction" in text
    assert "r2" in text
    assert "Error reading prediction" not in text


def test_an_unreadable_record_is_named_not_inlined():
    """
    A record carries serialized dataframes. Interpolating it into the note put
    hundreds of kilobytes of them into a published file and the same into the
    log line beside it.
    """
    payload = "x" * 5000
    out = format_failed_example(["not", "a", "record", payload], PIPE, 7, 9)
    assert "example_7" in out
    assert payload not in out
    assert len(out) < 200
