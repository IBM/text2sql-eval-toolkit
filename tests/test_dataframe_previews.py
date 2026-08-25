#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Result dataframes are trimmed before they reach a browser.

The stakes are not performance on one side and correctness on the other -- they
are both on the same side. A table that shows 200 of 86,502 rows without saying
so misrepresents what the query returned, so every test here is as much about
the reported count as about the trim.
"""

import json

import pytest

from text2sql_eval_toolkit.ui.dataframes import MAX_PREVIEW_ROWS, truncate_dataframe

PIPE = "modelA-greedy-zero-shot-chatapi"


def frame(n_rows, columns=("a", "b")):
    return json.dumps(
        {
            "columns": list(columns),
            "index": list(range(n_rows)),
            "data": [[i, f"v{i}"] for i in range(n_rows)],
        }
    )


# --- the helper -----------------------------------------------------------


def test_a_small_frame_is_returned_untouched():
    raw = frame(5)
    out, total, truncated = truncate_dataframe(raw)
    assert out == raw
    assert total == 5
    assert truncated is False


def test_a_large_frame_is_cut_and_says_how_large_it_was():
    out, total, truncated = truncate_dataframe(frame(5000), max_rows=10)
    assert total == 5000, "the caller needs the real size, not the trimmed one"
    assert truncated is True
    assert len(json.loads(out)["data"]) == 10


def test_the_index_is_cut_with_the_data():
    """A frame whose index and data disagree in length is not a valid frame."""
    parsed = json.loads(truncate_dataframe(frame(5000), max_rows=10)[0])
    assert len(parsed["index"]) == len(parsed["data"])


def test_columns_survive_the_cut():
    parsed = json.loads(truncate_dataframe(frame(500, ("x", "y")), max_rows=3)[0])
    assert parsed["columns"] == ["x", "y"]


def test_a_frame_at_exactly_the_limit_is_not_marked_truncated():
    _out, total, truncated = truncate_dataframe(frame(10), max_rows=10)
    assert (total, truncated) == (10, False)


def test_the_shape_it_arrived_in_is_the_shape_it_leaves_in():
    """A JSON string stays a string, a dict stays a dict; callers parse neither."""
    as_dict = json.loads(frame(500))
    out, _total, _cut = truncate_dataframe(as_dict, max_rows=5)
    assert isinstance(out, dict)
    assert isinstance(truncate_dataframe(frame(500), max_rows=5)[0], str)


@pytest.mark.parametrize(
    "raw", [None, "", "not json", "[]", '{"no":"data"}', 42, {"data": "not a list"}]
)
def test_anything_unrecognised_passes_through(raw):
    """
    An unfamiliar shape is not a reason to show the reader nothing. These all
    come back unchanged rather than emptied.
    """
    out, total, truncated = truncate_dataframe(raw)
    assert out == raw
    assert truncated is False
    if raw is None:
        assert total is None


def test_the_default_limit_is_bigger_than_any_panel_shows():
    # If this ever drops below what fits on screen, scrolling breaks.
    assert MAX_PREVIEW_ROWS >= 100


# --- through the endpoint -------------------------------------------------

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from text2sql_eval_toolkit.ui import server  # noqa: E402

BIG_ROWS = MAX_PREVIEW_ROWS * 3


@pytest.fixture
def client(tmp_path, monkeypatch):
    results = tmp_path / "results"
    results.mkdir()
    records = [
        {
            "id": "big",
            "question": "everything",
            "sql": ["SELECT *"],
            "gt_df": [frame(BIG_ROWS)],
            "predictions": {
                PIPE: {
                    "predicted_sql": "SELECT *",
                    "predicted_df": frame(BIG_ROWS),
                    "evaluation": {"execution_accuracy": 1},
                }
            },
        },
        {
            "id": "small",
            "question": "a few",
            "sql": ["SELECT 1"],
            "gt_df": [frame(3)],
            "predictions": {
                PIPE: {
                    "predicted_sql": "SELECT 1",
                    "predicted_df": frame(3),
                    "evaluation": {"execution_accuracy": 1},
                }
            },
        },
    ]
    (results / "demo-predictions_eval.json").write_text(
        json.dumps(records), encoding="utf-8"
    )
    monkeypatch.setenv("TEXT2SQL_DATA_ROOT", str(tmp_path))
    server.invalidate_index_cache()
    try:
        yield TestClient(server.app)
    finally:
        server.invalidate_index_cache()


def detail(client, record_id):
    resp = client.get(
        f"/api/benchmarks/demo/errors/{record_id}/detail", params={"pipeline": PIPE}
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


def test_detail_previews_a_large_result(client):
    body = detail(client, "big")
    assert len(json.loads(body["ground_truth_results"][0])["data"]) == MAX_PREVIEW_ROWS
    assert len(json.loads(body["predicted_result"])["data"]) == MAX_PREVIEW_ROWS


def test_detail_reports_the_real_row_counts(client):
    body = detail(client, "big")
    assert body["ground_truth_result_row_counts"] == [BIG_ROWS]
    assert body["predicted_result_row_count"] == BIG_ROWS
    assert body["ground_truth_results_truncated"] is True
    assert body["predicted_result_truncated"] is True
    assert body["preview_row_limit"] == MAX_PREVIEW_ROWS


def test_a_small_result_is_not_flagged_as_previewed(client):
    """The notice must not appear where nothing was withheld."""
    body = detail(client, "small")
    assert body["ground_truth_results_truncated"] is False
    assert body["predicted_result_truncated"] is False
    assert body["predicted_result_row_count"] == 3


def test_the_whole_record_is_still_available(client):
    """
    Trimming is a display decision for one endpoint. The record endpoint --
    what "View raw JSON" calls -- must still hand over everything, or the
    preview would be hiding data with no way to reach it.
    """
    resp = client.get("/api/benchmarks/demo/errors/big")
    assert resp.status_code == 200
    record = resp.json()
    assert len(json.loads(record["gt_df"][0])["data"]) == BIG_ROWS
    assert (
        len(json.loads(record["predictions"][PIPE]["predicted_df"])["data"]) == BIG_ROWS
    )


def test_the_response_is_small_enough_to_be_worth_it(client):
    """The whole point: a bounded payload regardless of result size."""
    resp = client.get(
        "/api/benchmarks/demo/errors/big/detail", params={"pipeline": PIPE}
    )
    assert len(resp.content) < 100_000
