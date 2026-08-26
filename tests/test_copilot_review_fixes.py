#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
"""
Regression tests for defects found in review of the dashboard branch.

Each of these passed review and CI while broken, so each gets a test that fails
if the fix is reverted.
"""

import importlib
import json
import sqlite3
from pathlib import Path

import pytest

from text2sql_eval_toolkit.indexing.store import _casefold, _escape_like
from text2sql_eval_toolkit.ui.capabilities import Tier
from text2sql_eval_toolkit.ui.judge_budget import verdict_cache_key


def test_import_time_mode_is_public_without_env(monkeypatch):
    """
    Serving the ASGI app directly skips main(), and with it the guard that
    refuses `full` on a non-loopback interface. If the import-time default were
    FULL, `uvicorn ...:app --host 0.0.0.0` would expose SQL execution to
    anonymous callers with nothing left to stop it.
    """
    monkeypatch.delenv("TEXT2SQL_DASHBOARD_MODE", raising=False)
    runtime = importlib.import_module("text2sql_eval_toolkit.ui.runtime")
    importlib.reload(runtime)
    assert runtime.get_mode() is Tier.PUBLIC


def test_cli_default_mode_is_still_full():
    """The local operator tool must not be downgraded by the fix above."""
    from text2sql_eval_toolkit.ui import server

    parser = server.build_parser() if hasattr(server, "build_parser") else None
    if parser is None:  # parser is built inline in main()
        src = Path(server.__file__).read_text()
        assert 'default=os.getenv("TEXT2SQL_DASHBOARD_MODE", "full")' in src
    else:
        assert parser.get_default("mode") == "full"


@pytest.mark.parametrize(
    "term,expected",
    [("%", "\\%"), ("_", "\\_"), ("a_b", "a\\_b"), ("\\", "\\\\")],
)
def test_like_metacharacters_are_escaped(term, expected):
    """`%` and `_` are LIKE wildcards; a search for them must be literal."""
    assert _escape_like(term) == expected


def test_casefold_handles_non_ascii():
    """SQLite's LOWER() is ASCII-only, so folding has to happen in Python."""
    assert _casefold("Élève") == "élève"
    assert _casefold("ABC") == "abc"
    assert _casefold(None) is None


def test_search_treats_wildcards_literally(tmp_path):
    """End-to-end through the actual query builder, not just the helper."""
    from text2sql_eval_toolkit.indexing.store import EvalIndex

    db = tmp_path / "idx.sqlite"
    con = sqlite3.connect(db)
    con.execute(
        "CREATE TABLE records (ordinal INTEGER, record_id TEXT, question TEXT,"
        " db_id TEXT, start INTEGER, length INTEGER)"
    )
    rows = [
        (0, "r0", "How many customers?"),
        (1, "r1", "Discount 50% off"),
        (2, "r2", "Élève count"),
        (3, "r3", "a_b naming"),
    ]
    con.executemany(
        "INSERT INTO records VALUES (?,?,?,'db',0,0)",
        rows,
    )
    con.commit()
    con.close()

    src = tmp_path / "src.json"
    src.write_text(json.dumps({"records": []}))
    idx = EvalIndex(db, src)
    try:

        def ids(term):
            where, params = idx._filter_sql(
                q=term,
                pipeline=None,
                metric=None,
                value=None,
                op="eq",
                pipeline2=None,
                metric2=None,
                disagree=False,
                failed_only=False,
            )
            cur = idx._conn.execute(
                f"SELECT record_id FROM records r WHERE {where}", params
            )
            return {r[0] for r in cur}

        # A bare wildcard matched every record before this was escaped.
        assert ids("%") == {"r1"}
        assert ids("_") == {"r3"}
        # Non-ASCII case folding: SQLite's LOWER() left "É" alone.
        assert ids("élève") == {"r2"}
        assert ids("customers") == {"r0"}
    finally:
        idx.close()


def test_verdict_cache_key_changes_with_config_contents():
    """
    Judge prompts are editable from the UI and keep their filename. Keying on
    the name alone served the verdict produced by the previous prompt.
    """
    args = ("b", "r", "p", "cfg", "model")
    a = verdict_cache_key(*args, {"prompt": "old"})
    b = verdict_cache_key(*args, {"prompt": "new"})
    assert a != b
    # Key ordering in the YAML is not a semantic change.
    assert verdict_cache_key(*args, {"x": 1, "y": 2}) == verdict_cache_key(
        *args, {"y": 2, "x": 1}
    )


def test_results_status_hides_absolute_path_outside_full(monkeypatch):
    """Filesystem layout is withheld in shared modes, as it is for 404 detail."""
    from text2sql_eval_toolkit.ui import routers_results, runtime

    monkeypatch.setattr(runtime, "get_mode", lambda: Tier.PUBLIC)
    assert "results_path" not in routers_results.get_results_status()

    monkeypatch.setattr(runtime, "get_mode", lambda: Tier.FULL)
    assert "results_path" in routers_results.get_results_status()


def test_has_results_ignores_derived_directories(tmp_path, monkeypatch):
    """
    `.index/`, `logs/` and `bak/` all live under results/. Counting any entry
    reported results that could not be served and hid the fetch banner.
    """
    from text2sql_eval_toolkit.ui import routers_results

    results = tmp_path / "results"
    (results / ".index").mkdir(parents=True)
    (results / ".index" / "x.sqlite").write_text("")
    monkeypatch.setattr(routers_results, "get_data_root", lambda: tmp_path)
    assert routers_results.get_results_status()["has_results"] is False

    (results / "b-predictions_eval.json").write_text("{}")
    assert routers_results.get_results_status()["has_results"] is True


def test_stale_index_handle_is_not_closed_under_other_threads():
    """
    get_index() returns a bare handle and the caller queries it with no lock, so
    invalidating the cache must drop the reference rather than close connections
    another request may be using.
    """
    src = Path("src/text2sql_eval_toolkit/ui/indexes.py").read_text()
    invalidation = src[src.index("def get_index") : src.index("_index_build_lock(")]
    assert "cached.close()" not in invalidation
