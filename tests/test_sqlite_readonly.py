#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Benchmark SQLite databases are opened read-only.

They are immutable reference data, and the dashboard's execute endpoint runs
arbitrary caller-supplied SQL, so a write has to be refused by SQLite itself
rather than by application logic upstream of it.
"""

import sqlite3

import pytest

from text2sql_eval_toolkit.execution.execution_tools import run_sqlite_query


@pytest.fixture
def db(tmp_path):
    path = tmp_path / "bench.db"
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE customers (id INTEGER, name TEXT)")
    conn.executemany("INSERT INTO customers VALUES (?, ?)", [(1, "ada"), (2, "grace")])
    conn.commit()
    conn.close()
    return path


def test_reads_work(db):
    result = run_sqlite_query(str(db), "SELECT COUNT(*) AS n FROM customers")
    assert '"data":[[2]]' in result.replace(" ", "")


def test_unicode_and_replacement_still_handled(db, tmp_path):
    conn = sqlite3.connect(db)
    conn.execute("INSERT INTO customers VALUES (3, ?)", ("日本語",))
    conn.commit()
    conn.close()
    # pandas escapes non-ASCII in JSON output, so decode before asserting
    # rather than matching the raw string.
    import json

    payload = json.loads(
        run_sqlite_query(str(db), "SELECT name FROM customers WHERE id = 3")
    )
    assert payload["data"] == [["日本語"]]


@pytest.mark.parametrize(
    "statement",
    [
        "INSERT INTO customers VALUES (99, 'mallory')",
        "UPDATE customers SET name = 'x'",
        "DELETE FROM customers",
        "DROP TABLE customers",
        "CREATE TABLE injected (x INTEGER)",
        "ALTER TABLE customers ADD COLUMN extra TEXT",
        "CREATE INDEX idx ON customers (id)",
        "PRAGMA journal_mode = WAL",
        "VACUUM",
    ],
)
def test_writes_are_refused_by_sqlite(db, statement):
    # sqlite3.OperationalError specifically: a TypeError from a bug in our own
    # code would otherwise read as "the database refused it". The message varies
    # by mechanism -- VACUUM is stopped by the attach limit it needs internally
    # rather than by the read-only flag -- so match either refusal.
    with pytest.raises(
        sqlite3.OperationalError,
        match="readonly|not authorized|attached databases",
    ):
        run_sqlite_query(str(db), statement)


def test_database_is_unchanged_after_attempted_writes(db):
    for statement in (
        "INSERT INTO customers VALUES (99, 'mallory')",
        "DELETE FROM customers",
        "DROP TABLE customers",
    ):
        with pytest.raises(sqlite3.OperationalError):
            run_sqlite_query(str(db), statement)

    # The point of the guarantee: the file on disk is untouched.
    conn = sqlite3.connect(db)
    rows = conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
    conn.close()
    assert rows == 2


def test_attach_cannot_reach_another_file(db, tmp_path):
    """
    Not exploitable through this function today -- each call opens a fresh
    connection and sqlite3 runs one statement per execute -- but refusing it
    outright keeps that from depending on a driver detail.
    """
    other = tmp_path / "other.db"
    sqlite3.connect(other).close()
    with pytest.raises(sqlite3.OperationalError):
        run_sqlite_query(str(db), f"ATTACH DATABASE '{other}' AS o")


def test_paths_with_spaces_and_unicode_open_correctly(tmp_path):
    """The read-only handle needs a URI, so the path has to be quoted."""
    directory = tmp_path / "some dir with spaces" / "日本語"
    directory.mkdir(parents=True)
    path = directory / "bench db.sqlite"
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE t (a INTEGER)")
    conn.execute("INSERT INTO t VALUES (7)")
    conn.commit()
    conn.close()

    assert '"data":[[7]]' in run_sqlite_query(str(path), "SELECT a FROM t").replace(
        " ", ""
    )


def test_missing_database_raises_rather_than_creating_one(tmp_path):
    """
    Read-write mode would silently create an empty database and report zero
    rows, which looks like a benchmark with no data rather than a missing file.
    """
    missing = tmp_path / "absent.db"
    with pytest.raises(sqlite3.OperationalError, match="unable to open"):
        run_sqlite_query(str(missing), "SELECT 1")
    assert not missing.exists()


def test_a_statement_returning_no_rows_yields_an_empty_frame(db):
    """
    cursor.description is None for statements with no result set. The function
    only ever saw SELECTs, so that path raised TypeError rather than returning
    something a caller could handle.
    """
    import json

    payload = json.loads(run_sqlite_query(str(db), "SELECT 1 WHERE 0"))
    assert payload["data"] == []
