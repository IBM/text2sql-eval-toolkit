#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
An index that disagrees with its source produces plausible but wrong analysis,
which is worse than a slow dashboard.  These tests pin the index to a full parse
of the same file and cover the staleness rules that trigger a rebuild.
"""

import json
import sqlite3

import pytest

from text2sql_eval_toolkit.indexing import build_index, index_path_for, is_stale

RECORDS = [
    {
        "id": "q1",
        "question": "How many customers?",
        "db_id": "shop",
        "predictions": {
            "modelA-greedy-zero-shot-chatapi": {
                "predicted_sql": "SELECT 1",
                "evaluation": {
                    "execution_accuracy": 1,
                    "llm_score": 0.5,
                    "llm_explanation": "looks right",
                    "eval_error": 0,
                },
            },
            "modelB-agentic-baseline1-3attempts": {
                "evaluation": {"execution_accuracy": 0, "df_error": 1},
            },
        },
    },
    {
        "id": "q2",
        "utterance": 'List orders with a {brace} and "quote"',
        "db_id": "shop",
        "predictions": {
            "modelA-greedy-zero-shot-chatapi": {
                "evaluation": {"execution_accuracy": 0, "is_flag": True},
            }
        },
    },
    {
        "question_id": "q3",
        "page_content": "Fallback id and question keys",
        "predictions": {},
    },
]


@pytest.fixture
def artifact(tmp_path):
    path = tmp_path / "demo-predictions_eval.json"
    path.write_text(json.dumps(RECORDS, ensure_ascii=False), encoding="utf-8")
    return path


def _conn(index_path):
    conn = sqlite3.connect(index_path)
    conn.row_factory = sqlite3.Row
    return conn


def test_index_matches_a_full_parse(artifact):
    conn = _conn(build_index(artifact))
    assert conn.execute("SELECT COUNT(*) FROM records").fetchone()[0] == len(RECORDS)

    expected_preds = sum(len(r.get("predictions", {})) for r in RECORDS)
    assert (
        conn.execute("SELECT COUNT(*) FROM predictions").fetchone()[0] == expected_preds
    )

    for rec in RECORDS:
        rid = str(rec.get("id") or rec.get("question_id"))
        row = conn.execute(
            "SELECT * FROM records WHERE record_id = ?", (rid,)
        ).fetchone()
        assert row is not None, rid
        assert row["db_id"] == rec.get("db_id")


def test_byte_ranges_resolve_to_the_original_record(artifact):
    conn = _conn(build_index(artifact))
    raw = artifact.read_bytes()
    for rec in RECORDS:
        rid = str(rec.get("id") or rec.get("question_id"))
        row = conn.execute(
            "SELECT byte_start, byte_end FROM records WHERE record_id = ?", (rid,)
        ).fetchone()
        assert json.loads(raw[row["byte_start"] : row["byte_end"]]) == rec


def test_file_order_is_preserved_for_stable_pagination(artifact):
    conn = _conn(build_index(artifact))
    ordered = [
        r[0] for r in conn.execute("SELECT record_id FROM records ORDER BY ordinal")
    ]
    assert ordered == ["q1", "q2", "q3"]


def test_question_falls_back_across_key_names(artifact):
    conn = _conn(build_index(artifact))
    got = dict(conn.execute("SELECT record_id, question FROM records").fetchall())
    assert got["q1"] == "How many customers?"
    assert got["q2"].startswith("List orders")  # from `utterance`
    assert got["q3"] == "Fallback id and question keys"  # from `page_content`


def test_numeric_metrics_are_indexed_and_text_is_not(artifact):
    conn = _conn(build_index(artifact))
    metrics = {r[0] for r in conn.execute("SELECT metric FROM metric_names").fetchall()}
    assert {"execution_accuracy", "llm_score", "eval_error", "df_error"} <= metrics
    # Text metrics are not filterable and must not become numeric rows.
    assert "llm_explanation" not in metrics
    # bool passes isinstance(v, (int, float)) in Python, and the endpoints rely
    # on that, so a boolean metric is indexed as 1.0 rather than dropped.
    assert "is_flag" in metrics
    value = conn.execute(
        "SELECT value FROM metrics m JOIN metric_names USING (metric_ref)"
        " WHERE metric = 'is_flag'"
    ).fetchone()[0]
    assert value == 1.0


def test_evaluation_blocks_are_stored_verbatim(artifact):
    conn = _conn(build_index(artifact))
    row = conn.execute(
        "SELECT evaluation_json FROM predictions p"
        " JOIN pipelines pl USING (pipeline_ref)"
        " JOIN records r USING (ordinal)"
        " WHERE r.record_id = 'q1' AND pl.pipeline_id = 'modelA-greedy-zero-shot-chatapi'"
    ).fetchone()
    assert (
        json.loads(row[0])
        == RECORDS[0]["predictions"]["modelA-greedy-zero-shot-chatapi"]["evaluation"]
    )


def test_index_is_written_beside_the_artifact(artifact):
    path = build_index(artifact)
    assert path == index_path_for(artifact)
    assert path.parent.name == ".index"


def test_rebuild_is_skipped_when_current(artifact):
    first = build_index(artifact)
    mtime = first.stat().st_mtime_ns
    assert build_index(artifact).stat().st_mtime_ns == mtime  # no work done
    assert not is_stale(artifact)


def test_source_change_marks_index_stale(artifact):
    build_index(artifact)
    assert not is_stale(artifact)
    changed = RECORDS + [{"id": "q4", "predictions": {}}]
    artifact.write_text(json.dumps(changed), encoding="utf-8")
    assert is_stale(artifact), "an edited artifact must invalidate its index"
    conn = _conn(build_index(artifact))
    assert conn.execute("SELECT COUNT(*) FROM records").fetchone()[0] == 4


def test_missing_index_is_stale(artifact):
    path = build_index(artifact)
    path.unlink()
    assert is_stale(artifact)


def test_corrupt_index_is_stale_rather_than_fatal(artifact):
    path = build_index(artifact)
    path.write_bytes(b"this is not a database")
    assert is_stale(artifact)
    build_index(artifact)  # must recover by rebuilding
    assert not is_stale(artifact)


def test_schema_version_change_forces_rebuild(artifact):
    path = build_index(artifact)
    with sqlite3.connect(path) as conn:
        conn.execute("UPDATE meta SET value = '999' WHERE key = 'schema_version'")
    assert is_stale(artifact)


def test_records_without_an_id_are_skipped_not_fatal(tmp_path):
    path = tmp_path / "odd-predictions_eval.json"
    path.write_text(json.dumps([{"no_id": 1}, {"id": "ok", "predictions": {}}]))
    conn = _conn(build_index(path))
    assert [r[0] for r in conn.execute("SELECT record_id FROM records")] == ["ok"]


def test_missing_artifact_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        build_index(tmp_path / "nope-predictions_eval.json")


def test_no_partial_index_is_left_behind_on_failure(tmp_path):
    path = tmp_path / "broken-predictions_eval.json"
    path.write_text('[{"id": "a"')  # truncated
    with pytest.raises(ValueError):
        build_index(path)
    assert not index_path_for(path).exists()
    # ...and no half-written temporary file is left to fill the volume.
    leftovers = list(index_path_for(path).parent.glob("*.building"))
    assert leftovers == [], leftovers
