#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Every SQLite connection a request opens must be closed by that request.

``with sqlite3.connect(...) as conn:`` reads as if it does that and does not: a
connection's context manager commits or rolls back and leaves the connection
open. Nor does dropping the last reference close it, because a connection refers
to itself through its statement cache -- it holds its file descriptor until the
cyclic collector happens to run.

The public deployment ran into Docker's default limit of 1024 open files that
way. ``is_stale`` runs on every cached index lookup, so each landing-page load
left one descriptor per benchmark; ``/api/me`` left one more on the judge ledger
for a signed-in caller. Once the limit was reached the app could not open the
results it serves, and listed every benchmark with no pipelines.

These tests watch every connection opened and require each to be closed on
return, rather than counting descriptors, which is neither portable nor
deterministic under a collector.
"""

import json
import sqlite3

import pytest

from text2sql_eval_toolkit.indexing import build_index, is_stale
from text2sql_eval_toolkit.ui.judge_budget import JudgeStore


@pytest.fixture
def opened(monkeypatch):
    """Every connection sqlite3.connect returns while the test runs."""
    connections: list[sqlite3.Connection] = []
    real_connect = sqlite3.connect

    def spy(*args, **kwargs):
        conn = real_connect(*args, **kwargs)
        connections.append(conn)
        return conn

    monkeypatch.setattr(sqlite3, "connect", spy)
    return connections


def _assert_all_closed(connections):
    assert connections, "nothing was opened, so nothing was checked"
    for conn in connections:
        with pytest.raises(sqlite3.ProgrammingError):
            conn.execute("SELECT 1")


@pytest.fixture
def artifact(tmp_path):
    path = tmp_path / "demo-predictions_eval.json"
    path.write_text(
        json.dumps([{"id": "q1", "question": "q", "predictions": {}}]),
        encoding="utf-8",
    )
    return path


def test_checking_a_current_index_closes_its_connection(artifact, opened):
    build_index(artifact)
    opened.clear()

    assert not is_stale(artifact)
    _assert_all_closed(opened)


def test_checking_a_corrupt_index_closes_its_connection(artifact, opened):
    build_index(artifact).write_bytes(b"this is not a database")
    opened.clear()

    assert is_stale(artifact)
    _assert_all_closed(opened)


def test_every_judge_ledger_call_closes_its_connection(tmp_path, opened):
    store = JudgeStore(tmp_path / "judge" / "usage.sqlite")
    calls = [
        lambda: store.usage(),
        lambda: store.check_budget(),
        lambda: store.set_user_cap("u1", 5.0),
        lambda: store.user_cap("u1"),
        lambda: store.user_spent("u1"),
        lambda: store.reserve("u1", 0.01),
        lambda: store.release("u1", 0.01),
        lambda: store.record_spend("u1", "wxai:some/model", 100, 20),
        lambda: store.put_verdict(
            "k1",
            benchmark_id="b",
            record_id="r",
            pipeline_id="p",
            config_name="c",
            model="m",
            verdict="correct",
            score=1.0,
            explanation="e",
            user_hash="u1",
        ),
        lambda: store.get_verdict("k1"),
        lambda: store.delete_verdict("k1"),
    ]
    for call in calls:
        call()

    _assert_all_closed(opened)


def test_the_judge_ledger_still_commits_what_it_writes(tmp_path):
    """
    Closing is only half of what the ledger's connection does on exit; the other
    half is committing. `set_user_cap` has no commit of its own and relied on the
    context manager for it, so a close that skipped the commit would lose caps
    silently.
    """
    path = tmp_path / "usage.sqlite"
    store = JudgeStore(path)
    store.set_user_cap("u1", 5.0)
    store.record_spend("u1", "wxai:some/model", 100, 20)

    reopened = JudgeStore(path)
    assert reopened.user_cap("u1") == 5.0
    assert reopened.usage().calls == 1
