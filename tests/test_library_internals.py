#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
"""
Tests for library code that needed no endpoint to exercise.

The modules that produce the toolkit's numbers were the least covered in the
package -- inference and execution sat near 9%, against 80-95% for the dashboard
that merely displays their output. The parts that parse model output and build
SQL are pure functions, so the low coverage was habit rather than necessity.

Model output is arbitrary text. These parsers decide what counts as the SQL a
model produced, so a regression here silently changes every score downstream.
"""

import sqlite3

import pytest

from text2sql_eval_toolkit.execution.execution_tools import (
    normalize_mysql_connection_string,
    quote_mixed_case_columns,
    quote_mysql_identifiers,
    resolve_sqlite_db_path,
    run_sqlite_query,
)
from text2sql_eval_toolkit.inference.inference_tools import (
    extract_sql_from_reasoning,
    postprocess_sql,
)


class TestPostprocessSql:
    """Turning a model's reply into a statement worth executing."""

    def test_plain_sql_passes_through(self):
        assert postprocess_sql("SELECT a FROM t") == "SELECT a FROM t"

    def test_fenced_sql_block_is_unwrapped(self):
        assert postprocess_sql("```sql\nSELECT a FROM t\n```") == "SELECT a FROM t"

    def test_fence_label_is_case_insensitive(self):
        assert postprocess_sql("```SQL\nSELECT a FROM t\n```") == "SELECT a FROM t"

    def test_unlabelled_fence_is_unwrapped(self):
        assert postprocess_sql("```\nSELECT a FROM t\n```") == "SELECT a FROM t"

    def test_unterminated_fence_still_yields_sql(self):
        # Models truncated mid-reply are common; the closing fence is often lost.
        assert postprocess_sql("```sql\nSELECT a FROM t") == "SELECT a FROM t"

    def test_trailing_semicolon_and_whitespace_removed(self):
        assert postprocess_sql("SELECT a FROM t;  \n") == "SELECT a FROM t"

    def test_leading_sql_word_removed(self):
        assert postprocess_sql("sql\nSELECT a FROM t") == "SELECT a FROM t"

    def test_prose_around_a_fenced_block_is_dropped(self):
        reply = "Here is the query:\n```sql\nSELECT a FROM t\n```\nHope that helps."
        assert postprocess_sql(reply) == "SELECT a FROM t"

    def test_empty_input_gives_empty_output(self):
        assert postprocess_sql("") == ""

    def test_multiline_sql_keeps_its_newlines(self):
        out = postprocess_sql("```sql\nSELECT a\nFROM t\nWHERE b = 1\n```")
        assert out == "SELECT a\nFROM t\nWHERE b = 1"


class TestExtractSqlFromReasoning:
    """Some models return only reasoning, with the query buried in it."""

    def test_empty_input_yields_empty_string(self):
        assert extract_sql_from_reasoning("") == ""

    def test_fenced_block_inside_reasoning(self):
        text = "Let me think about the joins.\n```sql\nSELECT a FROM t\n```"
        assert extract_sql_from_reasoning(text) == "SELECT a FROM t"

    def test_unterminated_fenced_block_is_recovered(self):
        text = "First I consider the schema.\n```sql\nSELECT a FROM t"
        assert extract_sql_from_reasoning(text) == "SELECT a FROM t"

    def test_reasoning_without_any_sql(self):
        assert extract_sql_from_reasoning("I am not sure how to answer this.") == ""

    def test_a_bare_select_is_found(self):
        out = extract_sql_from_reasoning("The answer is SELECT a FROM t")
        assert "SELECT" in out.upper()

    def test_result_never_keeps_a_trailing_semicolon(self):
        out = extract_sql_from_reasoning("```sql\nSELECT a FROM t;\n```")
        assert not out.endswith(";")


class TestIdentifierQuoting:
    """Dialect fix-ups applied before a statement is executed."""

    def test_mysql_quoting_returns_a_string(self):
        assert isinstance(quote_mysql_identifiers("SELECT a FROM t"), str)

    def test_mysql_quoting_preserves_a_simple_query(self):
        assert "SELECT" in quote_mysql_identifiers("SELECT a FROM t").upper()

    def test_mixed_case_columns_are_quoted_for_case_sensitive_engines(self):
        out = quote_mixed_case_columns("SELECT MyColumn FROM t")
        assert isinstance(out, str) and "MyColumn" in out

    def test_quoting_is_idempotent(self):
        once = quote_mixed_case_columns("SELECT a FROM t")
        assert quote_mixed_case_columns(once) == once


class TestSqliteExecution:
    """SQLite is the default benchmark engine and needs no running service."""

    @pytest.fixture
    def db(self, tmp_path):
        path = tmp_path / "test.sqlite"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE customers (id INTEGER, name TEXT)")
        conn.executemany(
            "INSERT INTO customers VALUES (?, ?)", [(1, "alice"), (2, "bob")]
        )
        conn.commit()
        conn.close()
        return str(path)

    def test_a_query_returns_serialised_rows(self, db):
        out = run_sqlite_query(db, "SELECT name FROM customers ORDER BY id")
        assert "alice" in out and "bob" in out

    def test_result_is_the_stored_dataframe_format(self, db):
        from text2sql_eval_toolkit import parse_dataframe

        df = parse_dataframe(run_sqlite_query(db, "SELECT id, name FROM customers"))
        assert list(df.columns) == ["id", "name"]
        assert len(df) == 2

    def test_an_empty_result_is_still_a_frame(self, db):
        from text2sql_eval_toolkit import parse_dataframe

        df = parse_dataframe(
            run_sqlite_query(db, "SELECT * FROM customers WHERE id = 99")
        )
        assert len(df) == 0

    def test_invalid_sql_surfaces_the_database_error(self, db):
        with pytest.raises(sqlite3.OperationalError):
            run_sqlite_query(db, "SELECT * FROM no_such_table")


class TestSqliteDbPathResolution:
    def test_a_database_is_found_by_id(self, tmp_path):
        folder = tmp_path / "dbs"
        (folder / "shop").mkdir(parents=True)
        target = folder / "shop" / "shop.sqlite"
        target.write_bytes(b"")
        resolved = resolve_sqlite_db_path(str(folder), "shop", "shop.sqlite")
        assert resolved == target

    def test_a_missing_database_returns_a_path_rather_than_raising(self, tmp_path):
        """
        Resolution is path arithmetic, not a existence check: it falls back
        through the candidate roots and returns a path even when nothing is
        there. The caller finds out when it tries to open it.
        """
        resolved = resolve_sqlite_db_path(str(tmp_path), "absent", "absent.sqlite")
        assert resolved.name == "absent.sqlite"
        assert not resolved.exists()

    def test_an_absolute_folder_is_used_directly(self, tmp_path):
        resolved = resolve_sqlite_db_path(
            str(tmp_path.resolve()), "shop", "shop.sqlite"
        )
        assert resolved == tmp_path.resolve() / "shop" / "shop.sqlite"


class TestMysqlConnectionStrings:
    """
    Execution is asyncio, so the URL must name an async driver.

    A connection string written as ``mysql+pymysql://`` -- the form most MySQL
    documentation shows, and a perfectly good sync driver -- used to pass
    straight through into an async engine and fail with "The asyncio extension
    requires an async driver to be used", which names the symptom and not the
    fix.
    """

    @pytest.mark.parametrize(
        "given",
        [
            "mysql://u:p@h:3306/",
            "mysql+pymysql://u:p@h:3306/",
            "mysql+aiomysql://u:p@h:3306/",
            "mysql+mysqldb://u:p@h:3306/",
            "mysql+mysqlconnector://u:p@h:3306/",
        ],
    )
    def test_every_form_yields_an_async_driver(self, given):
        url, _ = normalize_mysql_connection_string(given, db_id="dw")
        assert url.startswith("mysql+aiomysql://")

    @pytest.mark.parametrize(
        "given",
        [
            "mysql://u:p@h:3306/",
            "mysql+pymysql://u:p@h:3306/",
            "mysql+aiomysql://u:p@h:3306/",
            "mysql+aiomysql://u:p@h:3306/some_other_db",
        ],
    )
    def test_the_record_database_is_substituted(self, given):
        """
        Beaver queries six databases through one connection string, so the
        db_id decides which. Substitution used to happen only for the bare
        `mysql://` form, so a URL already naming aiomysql queried whatever
        database the string ended with -- silently the wrong one.
        """
        url, _ = normalize_mysql_connection_string(given, db_id="dw")
        assert url.endswith("/dw")

    def test_credentials_and_host_are_preserved(self):
        url, _ = normalize_mysql_connection_string(
            "mysql+pymysql://readonly:secret@mysql:3306/", db_id="csail_stata_nova"
        )
        assert "readonly:secret@mysql:3306" in url
        assert url.endswith("/csail_stata_nova")

    def test_no_db_id_leaves_the_path_alone(self):
        url, _ = normalize_mysql_connection_string("mysql://u:p@h:3306/base")
        assert url == "mysql+aiomysql://u:p@h:3306/base"
