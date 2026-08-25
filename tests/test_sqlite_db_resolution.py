#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Where SQLite benchmark databases are looked up.

``db_folder`` in the registry is registry-relative, so it has to resolve against
the registry that was actually loaded. It previously resolved against the copy
packaged inside the installed wheel, which meant databases placed exactly where
``data/benchmarks/dbs/README.md`` tells users to put them were never found --
the documented setup could not work.
"""

import sqlite3

import pytest

from text2sql_eval_toolkit.execution.execution_tools import resolve_sqlite_db_path


def _make_db(root, folder, db_id):
    path = root / folder / db_id / f"{db_id}.sqlite"
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE t (a INTEGER)")
    conn.commit()
    conn.close()
    return path


def test_resolves_against_the_configured_data_root(tmp_path, monkeypatch):
    """TEXT2SQL_DATA_ROOT wins, which is what a deployment sets."""
    registry = tmp_path / "benchmarks.json"
    registry.write_text("{}", encoding="utf-8")
    expected = _make_db(
        tmp_path, "benchmarks/dbs/bird/dev_databases", "california_schools"
    )

    monkeypatch.setenv("TEXT2SQL_DATA_ROOT", str(tmp_path))
    resolved = resolve_sqlite_db_path(
        "benchmarks/dbs/bird/dev_databases",
        "california_schools",
        "california_schools.sqlite",
    )
    assert resolved == expected
    assert resolved.exists()


def test_does_not_look_only_inside_the_installed_package(tmp_path, monkeypatch):
    """
    The regression itself: a database in the documented location must be found
    even though the packaged registry sits somewhere else entirely.
    """
    from text2sql_eval_toolkit.utils import BENCHMARKS_FILE

    registry = tmp_path / "benchmarks.json"
    registry.write_text("{}", encoding="utf-8")
    _make_db(tmp_path, "benchmarks/dbs/bird/dev_databases", "financial")

    monkeypatch.setenv("TEXT2SQL_DATA_ROOT", str(tmp_path))
    resolved = resolve_sqlite_db_path(
        "benchmarks/dbs/bird/dev_databases", "financial", "financial.sqlite"
    )
    packaged_root = str(BENCHMARKS_FILE)
    assert not str(resolved).startswith(packaged_root.rsplit("/", 1)[0] + "/benchmarks")
    assert resolved.exists()


def test_absolute_db_folder_is_used_verbatim(tmp_path):
    absolute = tmp_path / "elsewhere"
    expected = _make_db(tmp_path, "elsewhere", "superhero")
    resolved = resolve_sqlite_db_path(str(absolute), "superhero", "superhero.sqlite")
    assert resolved == expected


def test_missing_database_reports_the_documented_location(tmp_path, monkeypatch):
    """
    The error a user sees should name the place they were told to use, not an
    obscure path inside site-packages.
    """
    registry = tmp_path / "benchmarks.json"
    registry.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("TEXT2SQL_DATA_ROOT", str(tmp_path))

    resolved = resolve_sqlite_db_path(
        "benchmarks/dbs/bird/dev_databases", "absent", "absent.sqlite"
    )
    assert not resolved.exists()
    assert str(resolved).startswith(str(tmp_path))


@pytest.mark.parametrize("db_id", ["california_schools", "card_games", "toxicology"])
def test_each_benchmark_database_gets_its_own_path(tmp_path, monkeypatch, db_id):
    registry = tmp_path / "benchmarks.json"
    registry.write_text("{}", encoding="utf-8")
    _make_db(tmp_path, "benchmarks/dbs/bird/dev_databases", db_id)
    monkeypatch.setenv("TEXT2SQL_DATA_ROOT", str(tmp_path))

    resolved = resolve_sqlite_db_path(
        "benchmarks/dbs/bird/dev_databases", db_id, f"{db_id}.sqlite"
    )
    assert resolved.name == f"{db_id}.sqlite"
    assert resolved.parent.name == db_id
    assert resolved.exists()
