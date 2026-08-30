#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

import functools
import glob
import hashlib
import json
import os
import re
import time
from abc import ABC, abstractmethod
from collections import Counter
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Optional, Tuple

import numpy as np
import pandas as pd
import requests
from requests.exceptions import ConnectionError, ReadTimeout

from text2sql_eval_toolkit.logging import get_logger

try:
    import sqlite3
except ImportError:
    sqlite3 = None


logger = get_logger(__name__)
SQLDatabase = dict[str, Any]

# Keep env var names for compatibility with existing setups.
CACHE_LOCATION = os.getenv("UNITXT_CACHE_LOCATION")
MAX_CACHE_SIZE = os.getenv("MAX_CACHE_SIZE", 10 * 1024**3)
_cache_instance = None


class DatabaseConnector(ABC):
    """Abstract base class for database connectors."""

    def __init__(self, db_config: SQLDatabase):
        self.db_config = db_config
        self.databases_folder = os.path.join(
            os.environ.get("UNITXT_CACHE_LOCATION", "cache/text2sql"), "databases"
        )
        os.makedirs(self.databases_folder, exist_ok=True)

    @abstractmethod
    def get_table_schema(self) -> str:
        pass

    @abstractmethod
    def execute_query(self, query: str) -> Any:
        pass


@lru_cache(maxsize=128)
def execute_query_local(db_path: str, query: str) -> Any:
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchall(), None
    except sqlite3.Error as e:
        logger.info(f"Error executing SQL: {e}")
        return None, f"Error executing SQL: {e}"
    finally:
        if conn:
            conn.close()


class LocalSQLiteConnector(DatabaseConnector):
    """Database connector for SQLite databases."""

    def __init__(self, db_config: SQLDatabase):
        super().__init__(db_config)
        db_id = self.db_config.get("db_id")
        if not db_id:
            raise ValueError("db_id is required for SQLiteConnector.")
        self.db_path = self.get_db_file_path(db_id)
        self.conn: sqlite3.Connection = sqlite3.connect(self.db_path)
        self.cursor: sqlite3.Cursor = self.conn.cursor()

    def download_database(self, db_id):
        done_file_path = os.path.join(self.databases_folder, "download_done")
        if "bird/" in db_id:
            if not os.path.exists(done_file_path):
                try:
                    from huggingface_hub import snapshot_download
                except ImportError as exc:
                    raise ImportError(
                        "huggingface_hub is required to download local BIRD databases. "
                        "Install it with `pip install huggingface_hub`."
                    ) from exc
                snapshot_download(
                    repo_id="premai-io/birdbench",
                    repo_type="dataset",
                    local_dir=self.databases_folder,
                    force_download=False,
                    allow_patterns="*validation*",
                )
                open(done_file_path, "w", encoding="utf-8").close()
        else:
            raise NotImplementedError(
                f"current local db: {db_id} is not supported, only bird"
            )

    def get_db_file_path(self, db_id):
        self.download_database(db_id)
        db_id = db_id.split("/")[-1]

        db_file_pattern = os.path.join(self.databases_folder, "**", db_id + ".sqlite")
        db_file_paths = glob.glob(db_file_pattern, recursive=True)

        if not db_file_paths:
            raise FileNotFoundError(f"Database file {db_id} not found.")
        if len(db_file_paths) > 1:
            raise FileExistsError(f"More than one files matched for {db_id}")
        return db_file_paths[0]

    def get_table_schema(self) -> str:
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables: list[tuple[str]] = self.cursor.fetchall()
        schemas: dict[str, str] = {}

        for table in tables:
            if isinstance(table, tuple):
                table = table[0]
            if table == "sqlite_sequence":
                continue
            sql_query: str = (
                f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}';"
            )
            self.cursor.execute(sql_query)
            schema_prompt: str = self.cursor.fetchone()[0]
            schemas[table] = schema_prompt

        return "\n\n".join(list(schemas.values()))

    def execute_query(self, query: str) -> Any:
        return execute_query_local(self.db_path, query)


class InMemoryDatabaseConnector(DatabaseConnector):
    """Database connector for mocking databases with in-memory data structures."""

    def __init__(self, db_config: SQLDatabase):
        super().__init__(db_config)
        self.tables = db_config.get("data", None)
        if not self.tables:
            raise ValueError("data is required for InMemoryDatabaseConnector.")

    def get_table_schema(
        self,
        select_tables: Optional[list[str]] = None,
    ) -> str:
        schemas = {}
        for table_name, table_data in self.tables.items():
            if select_tables and table_name.lower() not in select_tables:
                continue
            columns = ", ".join([f"`{col}` TEXT" for col in table_data["columns"]])
            schema = f"CREATE TABLE `{table_name}` ({columns});"
            schemas[table_name] = schema
        return "\n\n".join(list(schemas.values()))

    def execute_query(self, query: str) -> Any:
        conn = sqlite3.connect(":memory:")
        cursor = conn.cursor()
        logger.debug("Running SQL query over in-memory DB")

        for table_name, table_data in self.tables.items():
            columns = table_data["columns"]
            rows = table_data["rows"]
            cursor.execute(f"CREATE TABLE {table_name} ({', '.join(columns)})")
            placeholders = ", ".join(["?"] * len(columns))
            cursor.executemany(
                f"INSERT INTO {table_name} VALUES ({placeholders})",
                rows,
            )

        try:
            cursor.execute(query)
            return cursor.fetchall(), None
        except sqlite3.Error as e:
            logger.info(f"Error executing SQL: {e}")
            return None, f"Error executing SQL: {e}"
        finally:
            conn.close()


def get_cache():
    global _cache_instance
    if _cache_instance is None:
        _cache_instance = Cache()
    return _cache_instance


def generate_cache_key(*args, **kwargs):
    try:
        serialized = json.dumps(
            {"args": args, "kwargs": kwargs},
            sort_keys=True,
            default=str,
        )
    except TypeError:
        serialized = repr((args, kwargs))
    return hashlib.md5(serialized.encode()).hexdigest()


class Cache:
    """A class that provides disk-based caching functionality."""

    def __init__(self):
        if CACHE_LOCATION:
            try:
                import diskcache

                os.makedirs(CACHE_LOCATION, exist_ok=True)
                self.cache = diskcache.Cache(CACHE_LOCATION, size_limit=MAX_CACHE_SIZE)
                logger.info(f"Caching enabled at {CACHE_LOCATION}")
            except ImportError as e:
                raise ImportError(
                    "UNITXT_CACHE_LOCATION is set, but diskcache is not installed.\n"
                    "Please install diskcache `pip install diskcache` "
                    "or unset UNITXT_CACHE_LOCATION."
                ) from e
        else:
            self.cache = None

    def get_or_set(self, key, compute_fn, no_cache=False, refresh=False):
        if not self.cache or no_cache:
            logger.info(f"Bypassing cache for key: {key}")
            return compute_fn()

        if refresh and key in self.cache:
            logger.info(f"Refreshing cache for key: {key}")
            del self.cache[key]

        if key in self.cache:
            logger.info(f"Cache hit for key: {key}")
            return self.cache[key]

        logger.info(f"Cache miss for key: {key}. Computing value...")
        result = compute_fn()

        if result and not (
            isinstance(result, tuple) and len(result) == 2 and result[0] is None
        ):
            self.cache[key] = result
            logger.info(f"Stored result in cache for key: {key}")
        else:
            logger.info(f"None result. Bypassing caching for key: {key}")

        return result

    async def async_get_or_set(self, key, compute_fn, no_cache=False, refresh=False):
        if not self.cache or no_cache:
            logger.info(f"Bypassing cache for key: {key}")
            return await compute_fn()

        if refresh and key in self.cache:
            logger.info(f"Refreshing cache for key: {key}")
            del self.cache[key]

        if key in self.cache:
            logger.info(f"Cache hit for key: {key}")
            return self.cache[key]

        logger.info(f"Cache miss for key: {key}. Computing value asynchronously...")
        result = await compute_fn()
        self.cache[key] = result
        logger.info(f"Stored result in cache for key: {key}")
        return result

    def memoize(self, key_func=generate_cache_key, no_cache=False, refresh=False):
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                if not self.cache or no_cache:
                    logger.info(f"Bypassing cache for function: {func.__name__}")
                    return func(*args, **kwargs)

                key = key_func(func.__name__, *args, **kwargs)

                if refresh and key in self.cache:
                    logger.info(
                        f"Refreshing cache for function: {func.__name__}, key: {key}"
                    )
                    del self.cache[key]

                if key in self.cache:
                    logger.info(f"Cache hit for function: {func.__name__}, key: {key}")
                    return self.cache[key]

                logger.info(
                    f"Cache miss for function: {func.__name__}, key: {key}. Computing value..."
                )
                result = func(*args, **kwargs)
                self.cache[key] = result
                logger.info(
                    f"Stored result in cache for function: {func.__name__}, key: {key}"
                )
                return result

            return wrapper

        return decorator

    def async_memoize(self, key_func=generate_cache_key, no_cache=False, refresh=False):
        def decorator(func):
            @functools.wraps(func)
            async def wrapper(*args, **kwargs):
                if no_cache:
                    logger.info(f"Bypassing cache for async function: {func.__name__}")
                    return await func(*args, **kwargs)

                key = key_func(func.__name__, *args, **kwargs)

                if refresh and key in self.cache:
                    logger.info(
                        f"Refreshing cache for async function: {func.__name__}, key: {key}"
                    )
                    del self.cache[key]

                if key in self.cache:
                    logger.info(
                        f"Cache hit for async function: {func.__name__}, key: {key}"
                    )
                    return self.cache[key]

                logger.info(
                    f"Cache miss for async function: {func.__name__}, key: {key}. Computing value..."
                )
                result = await func(*args, **kwargs)
                self.cache[key] = result
                logger.info(
                    f"Stored result in cache for async function: {func.__name__}, key: {key}"
                )
                return result

            return wrapper

        return decorator


@lru_cache(maxsize=128)
def execute_query_remote(
    api_url: str,
    database_id: str,
    api_key: str,
    query: str,
    retryable_exceptions: tuple = (ConnectionError, ReadTimeout),
    max_retries: int = 3,
    retry_delay: int = 5,
    timeout: int = 30,
) -> tuple[Optional[dict], str]:
    headers = {
        "Content-Type": "application/json",
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    retries = 0
    while retries <= max_retries:
        try:
            response = requests.post(
                f"{api_url}/sql",
                headers=headers,
                json={"sql": query, "dataSourceId": database_id},
                verify=False,
                timeout=timeout,
            )
            response.raise_for_status()
            return response.json(), None

        except retryable_exceptions as e:
            retries += 1
            logger.warning(
                f"Attempt {retries} failed with error: {e}. Retrying in {retry_delay} seconds."
            )
            if retries <= max_retries:
                time.sleep(retry_delay)
            else:
                logger.error(f"Max retries ({max_retries}) exceeded for query: {query}")
                return (
                    None,
                    f"Max retries ({max_retries}) exceeded for query: {query} - Error: {e!s}",
                )

        except requests.exceptions.HTTPError as e:
            if e.response.status_code >= 500:
                retries += 1
                logger.warning(
                    f"Server error, attempt {retries} failed with error: {e}. Retrying in {retry_delay} seconds."
                )
                if retries <= max_retries:
                    time.sleep(retry_delay)
                else:
                    logger.error(
                        f"Max retries ({max_retries}) exceeded for query: {query}"
                    )
                    return (
                        None,
                        f"Max retries ({max_retries}) exceeded for query: {query} - Error: {e!s}",
                    )
            else:
                logger.error(f"HTTP Error on attempt {retries}: {e}")
                return (
                    None,
                    f"HTTP Error on attempt {retries}: {e}",
                )

        except Exception as e:
            logger.error(f"Unexpected error on attempt {retries}: {e}")
            return (None, f"Unexpected error on attempt {retries}: {e}")

    return None, "Unknown Error in SQL execution"


class RemoteDatabaseConnector(DatabaseConnector):
    """Database connector for remote databases accessed via HTTP."""

    def __init__(self, db_config: SQLDatabase):
        super().__init__(db_config)
        assert db_config[
            "db_id"
        ], "db_id must be in db_config for RemoteDatabaseConnector"
        self.api_url, self.database_id = (
            db_config["db_id"].split(",")[0],
            db_config["db_id"].split("db_id=")[-1].split(",")[0],
        )

        if not self.api_url or not self.database_id:
            raise ValueError(
                "Both 'api_url' and 'database_id' are required for RemoteDatabaseConnector."
            )

        self.api_key = os.getenv("SQL_API_KEY", None)
        if not self.api_key:
            raise ValueError(
                "The environment variable 'SQL_API_KEY' must be set to use the RemoteDatabaseConnector."
            )

        self.headers = {
            "Content-Type": "application/json",
            "accept": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }
        self.timeout = 30

    def get_table_schema(self) -> str:
        cur_api_url = f"{self.api_url}/datasources/{self.database_id}"
        response = requests.get(
            cur_api_url,
            headers=self.headers,
            verify=False,
            timeout=self.timeout,
        )
        if response.status_code == 200:
            schema = response.json()["schema"]
        else:
            raise OSError(f"Could not fetch schema from {cur_api_url}")

        schema_text = ""
        for table in schema["tables"]:
            schema_text += f"Table: {table['name'] if 'name' in table else table['table_name']} has columns: {[col['name'] if 'name' in col else col['column_name'] for col in table['columns']]}\n"

        return schema_text

    def execute_query(self, query: str) -> Any:
        cache = get_cache()
        cache_key = generate_cache_key(
            "sql_request",
            self.api_url,
            self.database_id,
            query,
        )
        return cache.get_or_set(
            cache_key,
            lambda: execute_query_remote(
                api_url=self.api_url,
                database_id=self.database_id,
                api_key=self.api_key,
                query=query,
                timeout=self.timeout,
            ),
        )


def get_db_connector(db_type: str):
    if db_type == "local":
        connector = LocalSQLiteConnector
    elif db_type == "in_memory":
        connector = InMemoryDatabaseConnector
    elif db_type == "remote":
        connector = RemoteDatabaseConnector
    else:
        raise ValueError(f"Unsupported database type: {db_type}")
    return connector


@dataclass
class SQLNonExecutionMetricResult:
    sqlglot_validity: int
    sqlparse_validity: int
    sqlglot_equivalence: int
    sqlglot_optimized_equivalence: int
    sqlparse_equivalence: int
    sql_exact_match: int
    sql_syntactic_equivalence: int


def _upper_outside_string_literals(sql: str) -> str:
    """
    Upper-case *sql* while leaving quoted string literals alone.

    Case-folding the whole statement makes ``WHERE name = 'bob'`` equal to
    ``WHERE name = 'BOB'``, which on a case-sensitive column is a false match.
    Doubled quotes inside a literal (``'it''s'``) stay part of that literal.
    """
    out, i, n = [], 0, len(sql)
    while i < n:
        ch = sql[i]
        if ch in ("'", '"'):
            quote, j = ch, i + 1
            while j < n:
                if sql[j] == quote:
                    if j + 1 < n and sql[j + 1] == quote:
                        j += 2
                        continue
                    break
                j += 1
            out.append(sql[i : j + 1])
            i = j + 1
        else:
            out.append(ch.upper())
            i += 1
    return "".join(out)


def _strip_string_literals(sql: str) -> str:
    """Blank the contents of quoted literals, so keyword searches cannot match inside one."""
    out, i, n = [], 0, len(sql)
    while i < n:
        ch = sql[i]
        if ch in ("'", '"'):
            quote, j = ch, i + 1
            while j < n:
                if sql[j] == quote:
                    if j + 1 < n and sql[j + 1] == quote:
                        j += 2
                        continue
                    break
                j += 1
            out.append(quote * 2)
            i = j + 1
        else:
            out.append(ch)
            i += 1
    return "".join(out)


def is_sqlglot_parsable(sql: str, db_type="sqlite") -> bool:
    """
    Report whether sqlglot can parse *sql* in the given dialect.

    Used to separate "the model produced something that is not SQL" from "the
    model produced SQL that is wrong", which are different failure modes in error
    analysis.

    ``db_type="db2"`` is read as ``postgres``: sqlglot has no Db2 dialect, and
    Postgres is the closest of those it supports. Db2-specific syntax may
    therefore parse when it should not, or fail when it is valid.

    Args:
        sql (str): The statement to check.
        db_type (str): sqlglot dialect name. ``"db2"`` is mapped to ``"postgres"``.

    Returns:
        bool: ``False`` for empty or whitespace-only input, and for anything
        sqlglot rejects. Never raises -- parse failures are the answer, not an
        error.
    """
    from sqlglot import parse

    if not sql.strip():
        return False
    if db_type == "db2":
        db_type = "postgres"
    try:
        parse(sql, read=db_type)
        return True
    except Exception as e:
        logger.debug(f"SQL query could not parse: {e}")
        return False


def is_sqlparse_parsable(sql: str) -> bool:
    """
    Report whether sqlparse can tokenise *sql* without producing error tokens.

    A **weaker** check than [`is_sqlglot_parsable`][text2sql_eval_toolkit.is_sqlglot_parsable]. sqlparse is a
    non-validating tokeniser: it accepts a great deal that no database would run,
    so a ``True`` here means considerably less than a ``True`` there. Useful
    mainly as a first filter, or where a dialect is unknown.

    Args:
        sql: The statement to check.

    Returns:
        bool: ``False`` for empty or whitespace-only input, and if any token is
        an error token. Never raises.
    """
    from sqlparse import parse
    from sqlparse.tokens import Error

    if not sql.strip():
        return False
    try:
        statements = parse(sql)
        for statement in statements:
            for token in statement.tokens:
                if token.ttype == Error:
                    return False
        return True
    except Exception as e:
        logger.debug(f"SQL query could not parse: {e}")
        return False


def sqlglot_optimized_equivalence(
    expected: str, generated: str, dialect: str = ""
) -> int:
    """
    Compare two statements after sqlglot's optimizer has normalised both.

    The strongest of the syntactic comparisons here: the optimizer canonicalises
    aliases, predicate order and other rewrites, so queries that differ in
    spelling but not in meaning can still match. It remains a *syntactic* check --
    two queries that always return the same rows may still compare unequal.

    Warning:
        This returns ``int`` (``0`` or ``1``), unlike
        [`sqlglot_parsed_queries_equivalent`][text2sql_eval_toolkit.sqlglot_parsed_queries_equivalent], [`sqlparse_queries_equivalent`][text2sql_eval_toolkit.sqlparse_queries_equivalent]
        and [`sql_exact_match`][text2sql_eval_toolkit.sql_exact_match], which return ``bool``. The inconsistency is
        preserved for backwards compatibility. ``if result:`` behaves the same
        either way; ``result is True`` does not.

    Args:
        expected: Ground-truth statement.
        generated: Predicted statement.
        dialect: sqlglot dialect name. Empty means sqlglot's default.

    Returns:
        int: ``1`` if the optimised trees are equal, otherwise ``0``. A parse or
        optimizer failure on either side also yields ``0``, so a ``0`` means
        "not shown to be equivalent" rather than "shown to differ".
    """
    from sqlglot import parse_one
    from sqlglot.optimizer import optimize

    try:
        return int(
            optimize(parse_one(expected, read=dialect))
            == optimize(parse_one(generated, read=dialect))
        )
    except Exception as e:
        logger.debug(f"Error parsing SQL for comparison: {e}")
        return 0


def extract_select_columns(statement):
    from sqlparse.sql import Identifier, IdentifierList
    from sqlparse.tokens import DML, Keyword

    columns = []
    select_seen = False
    for token in statement.tokens:
        if token.ttype is DML and token.value.upper() == "SELECT":
            select_seen = True
            continue
        if select_seen:
            if token.ttype is Keyword and token.value.upper() in (
                "FROM",
                "WHERE",
                "GROUP",
                "HAVING",
                "ORDER",
                "LIMIT",
            ):
                break
            if isinstance(token, IdentifierList):
                for identifier in token.get_identifiers():
                    columns.append(strip_alias(identifier.value))
            elif isinstance(token, Identifier):
                columns.append(strip_alias(token.value))
            else:
                val = token.value.strip()
                if val:
                    columns.append(strip_alias(val))
    return frozenset(columns)


def strip_alias(col: str) -> str:
    col = col.strip()
    upper = col.upper()
    if " AS " in upper:
        return col[: upper.index(" AS ")].strip()
    parts_alias = col.split()
    if len(parts_alias) > 1:
        return " ".join(parts_alias[:-1])
    return col


def collect_clause(statement, clause_keyword):
    from sqlparse.tokens import Keyword

    found = False
    collected = []
    for token in statement.tokens:
        tvalue = token.value.upper()
        if token.ttype is Keyword:
            if tvalue.startswith(clause_keyword):
                found = True
                continue
            if found and tvalue in (
                "FROM",
                "WHERE",
                "GROUP",
                "HAVING",
                "ORDER",
                "LIMIT",
            ):
                break
        if found:
            collected.append(token.value)
    return " ".join(collected).strip()


def extract_select_info(sql: str):
    from sqlparse import parse
    from sqlparse.tokens import DML

    statements = parse(sql)
    if len(statements) != 1:
        return None
    stmt = statements[0]
    if not any(t.ttype is DML and t.value.upper() == "SELECT" for t in stmt.tokens):
        return None
    parts = {
        "columns": None,
        "from": "",
        "where": "",
        "group": "",
        "having": "",
        "order": "",
    }
    columns = extract_select_columns(stmt)
    if not columns:
        columns = frozenset()
    parts["columns"] = columns
    parts["from"] = collect_clause(stmt, "FROM")
    parts["where"] = collect_clause(stmt, "WHERE")
    parts["group"] = collect_clause(stmt, "GROUP")
    parts["having"] = collect_clause(stmt, "HAVING")
    parts["order"] = collect_clause(stmt, "ORDER")
    return parts


def sqlparse_queries_equivalent(sql1: str, sql2: str) -> bool:
    """
    Compare two SELECT statements clause by clause, ignoring whitespace and case.

    Extracts the select list and the ``FROM``, ``WHERE``, ``GROUP``, ``HAVING``
    and ``ORDER`` clauses from each, then compares them as strings with
    whitespace removed and case folded. Because the comparison is textual, a
    reordered ``WHERE`` or a different alias makes two equivalent queries
    disagree; [`sqlglot_optimized_equivalence`][text2sql_eval_toolkit.sqlglot_optimized_equivalence] is more tolerant.

    Args:
        sql1: First statement.
        sql2: Second statement.

    Returns:
        bool: ``True`` only if every extracted clause matches. ``False`` if
        either statement cannot be parsed as a SELECT. Never raises.
    """
    try:
        info1 = extract_select_info(sql1)
        info2 = extract_select_info(sql2)
        if not info1 or not info2:
            return False
        if info1["columns"] != info2["columns"]:
            return False
        for k in ["from", "where", "group", "having", "order"]:
            if (
                info1[k].replace(" ", "").replace("\n", "").upper()
                != info2[k].replace(" ", "").replace("\n", "").upper()
            ):
                return False
        return True
    except Exception as e:
        logger.debug(f"Error parsing SQL query for comparison: {e}")
        return False


def sqlglot_parsed_queries_equivalent(sql1: str, sql2: str, dialect: str = "") -> bool:
    """
    Compare two statements by sqlglot AST equality, without optimising.

    Stricter than [`sqlglot_optimized_equivalence`][text2sql_eval_toolkit.sqlglot_optimized_equivalence], which normalises both
    trees first: here, a difference in alias or predicate order is a difference.

    Any statement kind is compared, not only ``SELECT``. Two statements of
    different kinds are never equivalent.

    Args:
        sql1: First statement.
        sql2: Second statement.
        dialect: sqlglot dialect name. Empty means sqlglot's default.

    Returns:
        bool: ``True`` if both parse as SELECT and their trees are equal.
        ``False`` on any parse failure. Never raises.
    """
    from sqlglot import parse_one

    try:
        ast1 = parse_one(sql1, read=dialect)
        ast2 = parse_one(sql2, read=dialect)
    except Exception:
        return False
    if ast1 is None or ast2 is None:
        return False
    # Statements of different kinds are never equivalent, but two of the same
    # kind are compared on their trees whatever that kind is. This used to
    # require both sides to be SELECT, which meant two byte-identical INSERTs
    # compared unequal.
    if type(ast1) is not type(ast2):
        return False

    return ast1 == ast2


def sql_exact_match(sql1: str, sql2: str) -> bool:
    """
    Compare two statements as normalised text.

    Normalisation strips surrounding whitespace and any trailing semicolon,
    collapses internal whitespace runs to single spaces, and upper-cases. No
    parsing is involved, so this is the weakest and cheapest of the comparisons
    here -- two queries that differ only in formatting match, and two that differ
    in any other way do not.

    Warning:
        Upper-casing applies to the whole statement, **string literals
        included**, so ``WHERE name = 'bob'`` matches ``WHERE NAME = 'BOB'``.
        On a case-sensitive column that is a false match.

    Args:
        sql1: First statement.
        sql2: Second statement.

    Returns:
        bool: ``True`` if the normalised forms are identical.
    """

    def normalize_sql(s: str) -> str:
        s = s.strip().rstrip(";")
        s = re.sub(r"\s+", " ", s)
        return _upper_outside_string_literals(s)

    return normalize_sql(sql1) == normalize_sql(sql2)


@dataclass
class SQLExecutionResult:
    execution_accuracy: int
    non_empty_execution_accuracy: int
    subset_non_empty_execution_accuracy: int
    execution_accuracy_bird: int
    non_empty_gold_df: int
    gold_sql_runtime: float
    predicted_sql_runtime: float
    pred_to_gold_runtime_ratio: float
    gold_error: int
    predicted_error: int
    gold_df_json: str
    predicted_df_json: str
    error_message: str


def compare_dfs_ignore_colnames_ordered_rows(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
) -> bool:
    if df1.shape != df2.shape:
        return False
    df1_sorted_rows = np.array([np.sort(row) for row in df1.values.astype(str)])
    df2_sorted_rows = np.array([np.sort(row) for row in df2.values.astype(str)])
    return np.array_equal(df1_sorted_rows, df2_sorted_rows)


def compare_dfs_ignore_colnames_unordered_rows(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
) -> bool:
    if df1.shape != df2.shape:
        return False
    df1_sorted = np.sort(np.sort(df1.values.astype(str), axis=1), axis=0)
    df2_sorted = np.sort(np.sort(df2.values.astype(str), axis=1), axis=0)
    return np.array_equal(df1_sorted, df2_sorted)


def _canonical_scalar(value: Any) -> str:
    """Normalize scalar values for robust subset comparison.

    This specifically resolves mixed numeric representations that can appear when
    row-wise numpy coercion upcasts integers to floats (e.g., 4 -> 4.0).
    """
    if value is None:
        return "None"
    # Normalize numpy/pandas scalar wrappers.
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            pass

    if isinstance(value, bool):
        return str(value)
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if np.isnan(value):
            return "nan"
        if np.isinf(value):
            return "inf" if value > 0 else "-inf"
        if float(value).is_integer():
            return str(int(value))
        return format(value, ".15g")

    text = str(value).strip()
    # Best-effort canonicalization for numeric strings.
    try:
        as_float = float(text)
        if np.isnan(as_float):
            return "nan"
        if np.isinf(as_float):
            return "inf" if as_float > 0 else "-inf"
        if as_float.is_integer():
            return str(int(as_float))
        return format(as_float, ".15g")
    except Exception:
        return text


def compare_dfs_ignore_colnames_subset(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    ignore_row_order: bool = True,
) -> bool:
    """Checks if the smaller of the two DataFrames is likely a subset of the other."""

    def row_to_multiset(row):
        return Counter(_canonical_scalar(x) for x in row)

    def rows_to_multisets(df):
        return [row_to_multiset(row) for row in df.values]

    def sort_df(df):
        # Built rather than assigned into a copy. `_canonical_scalar` can return
        # a type the original column cannot hold -- a string where the column is
        # int64 -- and writing that back through `.iloc` asks pandas to coerce.
        # It warned about that for several releases and now raises, so an
        # installation with a newer pandas failed every execution-match metric
        # with "Invalid value ... for dtype 'int64'".
        return pd.DataFrame(
            {
                i: df.iloc[:, i].map(_canonical_scalar).sort_values(ignore_index=True)
                for i in range(df.shape[1])
            }
        )

    if df1.empty or df2.empty or len(df1) != len(df2):
        return False

    df1.columns = range(df1.shape[1])
    df2.columns = range(df2.shape[1])
    subset_df, superset_df = (df1, df2) if df1.shape[1] <= df2.shape[1] else (df2, df1)

    if ignore_row_order:
        subset_df = sort_df(subset_df)
        superset_df = sort_df(superset_df)

    subset_rows = rows_to_multisets(subset_df)
    superset_rows = rows_to_multisets(superset_df)

    for r1, r2 in zip(subset_rows, superset_rows, strict=True):
        if not all(r1[k] <= r2.get(k, 0) for k in r1):
            return False
    return True


def compare_dfs_bird_eval_logic(df1: pd.DataFrame, df2: pd.DataFrame):
    """
    Compare two result sets using BIRD's published evaluation logic.

    Reproduced so that scores from this toolkit can be lined up against numbers
    reported by BIRD. Each frame becomes a set of row tuples with every value
    cast to ``str``, and the sets are compared.

    Three consequences of that, all inherited from BIRD rather than chosen here:

    - **Row order is ignored**, since sets are unordered.
    - **Duplicate rows collapse.** A query returning the same row three times
      matches one returning it once.
    - **Values are compared as text.** ``1`` and ``"1"`` match; ``1`` and ``1.0``
      do not, because they render as ``"1"`` and ``"1.0"``.

    Column *names* are ignored, but column *order* within a row is significant.

    Args:
        df1: First result set.
        df2: Second result set.

    Returns:
        int: ``1`` if the row sets are equal, else ``0``.
    """
    df1_set = {tuple(row) for row in df1.values.astype(str)}
    df2_set = {tuple(row) for row in df2.values.astype(str)}
    return int(df1_set == df2_set)


def compare_result_dfs(
    gold_df: pd.DataFrame,
    pred_df: pd.DataFrame,
    gold_sql: str,
) -> Tuple[int, int, int]:
    """
    Compare a predicted result set against the ground truth three ways.

    This is where the toolkit's three headline execution metrics come from, and
    it exists because a strict comparison misjudges legitimate answers: column
    names are ignored throughout, so a query asked for "the customers" is not
    marked wrong for labelling the column differently.

    Row order is significant only when the ground-truth SQL orders its output.
    That is decided by looking for ``ORDER BY`` in *gold_sql*.

    Warning:
        That check is a case-insensitive substring test, not a parse. A gold
        query containing the text ``ORDER BY`` inside a string literal or an
        identifier is treated as ordered, and comparison becomes stricter than
        intended.

    Args:
        gold_df: Result of the ground-truth SQL.
        pred_df: Result of the predicted SQL.
        gold_sql: The ground-truth statement, read only to decide whether row
            order matters.

    Returns:
        tuple[int, int, int]: ``(match, non_empty_match, subset_match)``, each
        ``0`` or ``1``, feeding ``execution_accuracy``,
        ``non_empty_execution_accuracy`` and
        ``subset_non_empty_execution_accuracy`` respectively.

        ``match`` is full equality ignoring column names. The other two are
        ``0`` whenever either frame is empty -- which is the point of them: a
        query returning nothing trivially "matches" another returning nothing,
        and that flatters a model that has learned to return nothing.
        ``subset_match`` additionally allows the prediction to carry extra
        columns, so answering with id *and* name where only name was required
        still counts.
    """
    subset_match = 0
    non_empty_match = 0
    # Literals are blanked first: a gold query selecting the text 'order by'
    # is not an ordered query, and treating it as one silently applies a
    # stricter comparison than the benchmark intends.
    if "ORDER BY" in _strip_string_literals(gold_sql).upper():
        match = int(compare_dfs_ignore_colnames_ordered_rows(pred_df, gold_df))
        if not gold_df.empty and not pred_df.empty:
            non_empty_match = match
            if compare_dfs_ignore_colnames_subset(
                gold_df,
                pred_df,
                ignore_row_order=False,
            ):
                subset_match = 1
    else:
        match = int(compare_dfs_ignore_colnames_unordered_rows(pred_df, gold_df))
        if not gold_df.empty and not pred_df.empty:
            non_empty_match = match
            if compare_dfs_ignore_colnames_subset(
                gold_df,
                pred_df,
                ignore_row_order=True,
            ):
                subset_match = 1
    return match, non_empty_match, subset_match


def run_query(
    sql: str, connector, sql_timeout: float
) -> Tuple[Optional[pd.DataFrame], float, str]:
    from func_timeout import func_timeout
    from func_timeout.exceptions import FunctionTimedOut

    if not sql.strip():
        return None, 0.0, "No SQL query found in the prediction."

    try:
        start = time.perf_counter()
        result, error = func_timeout(sql_timeout, connector.execute_query, args=(sql,))
        duration = time.perf_counter() - start
        if isinstance(result, dict) and "results" in result:
            result = result["results"]
        if error:
            return None, duration, error
        return pd.DataFrame(result), duration, ""
    except FunctionTimedOut as e:
        return None, 0.0, f"Timeout: {e}"
    except Exception as e:
        return None, 0.0, f"Error: {e}"


def get_sql_execution_results(
    predicted_sql: str,
    gold_sql: str,
    connector,
    sql_timeout: float,
) -> SQLExecutionResult:
    gold_df, gold_runtime, gold_error_msg = run_query(gold_sql, connector, sql_timeout)
    gold_error = int(bool(gold_error_msg))

    if gold_error:
        return SQLExecutionResult(
            execution_accuracy=0,
            non_empty_execution_accuracy=0,
            subset_non_empty_execution_accuracy=0,
            execution_accuracy_bird=0,
            non_empty_gold_df=0,
            gold_sql_runtime=gold_runtime,
            predicted_sql_runtime=0,
            pred_to_gold_runtime_ratio=0,
            gold_error=gold_error,
            predicted_error=0,
            gold_df_json="",
            predicted_df_json="",
            error_message=gold_error_msg,
        )

    non_empty_gold_df = int(not gold_df.empty)
    if predicted_sql.strip().lower() == gold_sql.strip().lower():
        return SQLExecutionResult(
            execution_accuracy=1,
            non_empty_execution_accuracy=non_empty_gold_df,
            subset_non_empty_execution_accuracy=non_empty_gold_df,
            execution_accuracy_bird=1,
            non_empty_gold_df=non_empty_gold_df,
            gold_sql_runtime=gold_runtime,
            predicted_sql_runtime=0,
            pred_to_gold_runtime_ratio=0,
            gold_error=0,
            predicted_error=0,
            gold_df_json=gold_df.to_json(),
            predicted_df_json=gold_df.to_json(),
            error_message="",
        )

    try:
        if sqlglot_optimized_equivalence(gold_sql, predicted_sql):
            return SQLExecutionResult(
                execution_accuracy=1,
                non_empty_execution_accuracy=non_empty_gold_df,
                subset_non_empty_execution_accuracy=non_empty_gold_df,
                execution_accuracy_bird=1,
                non_empty_gold_df=non_empty_gold_df,
                gold_sql_runtime=gold_runtime,
                predicted_sql_runtime=0,
                pred_to_gold_runtime_ratio=0,
                gold_error=0,
                predicted_error=0,
                gold_df_json=gold_df.to_json(),
                predicted_df_json=gold_df.to_json(),
                error_message="",
            )
    except Exception as e:
        logger.info(f"Could not check SQL equivalence: {e}")

    pred_df, pred_runtime, pred_error_msg = run_query(
        predicted_sql, connector, sql_timeout
    )
    pred_error = 1 if pred_error_msg else 0

    if pred_df is None:
        return SQLExecutionResult(
            execution_accuracy=0,
            non_empty_execution_accuracy=0,
            subset_non_empty_execution_accuracy=0,
            execution_accuracy_bird=0,
            non_empty_gold_df=non_empty_gold_df,
            gold_sql_runtime=gold_runtime,
            predicted_sql_runtime=pred_runtime,
            pred_to_gold_runtime_ratio=(
                (pred_runtime / gold_runtime) if gold_runtime > 0 else 0
            ),
            gold_error=0,
            predicted_error=pred_error,
            gold_df_json=gold_df.to_json(),
            predicted_df_json="",
            error_message=pred_error_msg,
        )

    match, non_empty_match, subset_match = compare_result_dfs(
        gold_df, pred_df, gold_sql
    )
    bird_match = compare_dfs_bird_eval_logic(gold_df, pred_df)

    return SQLExecutionResult(
        execution_accuracy=match,
        non_empty_execution_accuracy=non_empty_match,
        subset_non_empty_execution_accuracy=subset_match,
        execution_accuracy_bird=bird_match,
        non_empty_gold_df=non_empty_gold_df,
        gold_sql_runtime=gold_runtime,
        predicted_sql_runtime=pred_runtime,
        pred_to_gold_runtime_ratio=(
            (pred_runtime / gold_runtime) if gold_runtime > 0 else 0
        ),
        gold_error=0,
        predicted_error=0,
        gold_df_json=gold_df.to_json(),
        predicted_df_json=pred_df.to_json(),
        error_message=pred_error_msg,
    )


def replace_select_clause(
    source_query: str,
    target_query: str,
    dialect: str = "postgres",
) -> str:
    from sqlglot import exp, parse_one

    if not dialect:
        dialect = "postgres"

    source_ast = parse_one(source_query, read=dialect)
    target_ast = parse_one(target_query, read=dialect)

    if not isinstance(source_ast, exp.Select) or not isinstance(target_ast, exp.Select):
        raise ValueError("Both queries must be valid SELECT statements.")

    target_ast.set("expressions", source_ast.expressions)
    return target_ast.sql(dialect=dialect)


def extract_sql_from_text(text: str) -> str:
    fenced_block_pattern = re.compile(r"```sql\s+(.*?)```", re.IGNORECASE | re.DOTALL)
    match = fenced_block_pattern.search(text)
    if match:
        return match.group(1).strip()

    sql_keywords = r"(?:SELECT|INSERT|UPDATE|DELETE|WITH)\s+"
    sql_start = r"(?:^|\n|:\s*)"
    sql_pattern = re.compile(
        rf"{sql_start}({sql_keywords}.*?;)",
        re.IGNORECASE | re.DOTALL,
    )
    match = sql_pattern.search(text)
    if match:
        return match.group(1).strip()

    fallback_pattern = re.compile(
        rf"{sql_start}({sql_keywords}.*)",
        re.IGNORECASE | re.DOTALL,
    )
    fallback_match = fallback_pattern.search(text)
    if fallback_match:
        return fallback_match.group(1).strip()

    return ""


ALL_DIALECTS = [
    "Athena",
    "BigQuery",
    "ClickHouse",
    "Databricks",
    "Doris",
    "Drill",
    "Druid",
    "DuckDB",
    "Hive",
    "Materialize",
    "MySQL",
    "Oracle",
    "Postgres",
    "Presto",
    "PRQL",
    "Redshift",
    "RisingWave",
    "Snowflake",
    "Spark",
    "Spark2",
    "SQLite",
    "StarRocks",
    "Tableau",
    "Teradata",
    "Trino",
    "TSQL",
]
