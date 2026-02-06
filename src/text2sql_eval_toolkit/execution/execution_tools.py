#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
This module provides functions to execute SQL queries against a database and augment prediction data with actual query results.

Functions for postgres DBs:
1. `quote_mixed_case_columns(sql: str) -> str`:
   - This function takes a SQL query string and ensures that any column names not in lowercase are enclosed in double quotes.
   - This is necessary for PostgreSQL compatibility.

2. `run_sql_and_get_dataframe(connection_string: str, schema_name: str, sql: str) -> pd.DataFrame`:
   - Executes the provided SQL query on the database using the given connection string and schema.
   - Returns the query results as a pandas DataFrame.

3. `run_execution(benchmark_id: str)`:
   - This function processes predictions for a specific benchmark, running each SQL query and augmenting the predictions with the actual dataframes.
   - It retrieves the database engine information for the given benchmark_id, constructs the connection string, and loads prediction data from a JSON file.
   - For each prediction, it attempts to execute the predicted SQL, catches exceptions, and stores either the resulting dataframe or an error message.
   - Finally, it writes the updated predictions back to the JSON file.

Requirements:
- `os`
- `json`
- `pathlib`
- `pandas`
- `sqlalchemy`
- `sqlglot`
- `text2sql_eval_toolkit.utils` (assumed to provide `get_benchmark_info`)

Assumptions:
- The environment variable for the PostgreSQL connection string is set.
- The predictions file is in JSON format and contains a structure with model predictions, each having a 'predicted_sql' key.
- The database schema_name is correctly specified in the benchmark information.
- Supported database type is currently only PostgreSQL.

Assisted by watsonx Code Assistant
"""

import asyncio
import asyncpg
import os
import importlib
import json
import pandas as pd
import re
import sqlite3
import time
from func_timeout import func_timeout, FunctionTimedOut
from io import StringIO
from pathlib import Path
from sqlglot import parse_one, exp
from tqdm.asyncio import tqdm_asyncio
from text2sql_eval_toolkit.utils import (
    get_benchmark_info,
    get_gt_sqls,
    parse_dataframe,
    BENCHMARKS_FILE,
)
from text2sql_eval_toolkit.execution.replace_select_tool import (
    replace_select_for_logic_ex,
)
from text2sql_eval_toolkit.logging import get_logger
from urllib.parse import urlparse, parse_qs, unquote


_ibm_db_mod = None
_sqlalchemy_mod = None
_aiomysql_mod = None

logger = get_logger(__name__)


def _require_mysql_deps():
    """Import MySQL dependencies only when needed; raise a helpful error if missing."""
    global _sqlalchemy_mod, _aiomysql_mod

    if _sqlalchemy_mod is None:
        try:
            _sqlalchemy_mod = importlib.import_module("sqlalchemy.ext.asyncio")
        except ModuleNotFoundError as e:
            raise RuntimeError(
                "MySQL support is optional and not installed. "
                "Install extras with: pip install sqlalchemy[asyncio] asyncio-mysql (or aiomysql)"
            ) from e

    # Also import the text function
    try:
        from sqlalchemy import text

        return _sqlalchemy_mod, text
    except ImportError as e:
        raise RuntimeError(
            "MySQL support is optional and not installed. "
            "Install extras with: pip install sqlalchemy[asyncio] asyncio-mysql (or aiomysql)"
        ) from e


def normalize_mysql_connection_string(
    connection_string: str, db_id: str = None
) -> tuple[str, dict]:
    """
    Normalize MySQL connection string and return connect_args.
    Based on your working normalize_connection_string function.
    """
    import re
    from urllib.parse import urlparse, urlunparse

    connect_args = {}

    # Handle mysql:// with SSL parameters like your example
    if connection_string.startswith("mysql://"):
        # For async, we'll try aiomysql first, then fall back to asyncio-mysql
        # But follow your SSL handling pattern
        connection_string = connection_string.replace(
            "mysql://", "mysql+aiomysql://", 1
        )

        # Parse the URL to modify the database part only if db_id is provided
        if db_id:
            parsed = urlparse(connection_string)
            # Replace the database part (path) with the db_id
            # Remove leading slash and any existing database name
            new_path = f"/{db_id}"
            # Rebuild the URL with the new database name
            connection_string = urlunparse(
                (
                    parsed.scheme,
                    parsed.netloc,
                    new_path,
                    parsed.params,
                    parsed.query,
                    parsed.fragment,
                )
            )

        # Handle SSL mode parameter conversion - remove problematic SSL params
        if "sslMode=" in connection_string:
            # Remove the sslMode parameter entirely and let driver handle SSL automatically
            connection_string = re.sub(r"[&?]sslMode=[^&]*", "", connection_string)

        # Determine if this is an IBM Cloud or other SSL-required connection
        ssl_required = any(
            domain in connection_string
            for domain in ["databases.appdomain.cloud", "ssl=true", "sslMode="]
        )

        if ssl_required:
            # For aiomysql, let's try a simpler SSL approach
            # Just enable SSL without complex verification for IBM Cloud
            import ssl

            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            connect_args = {"ssl": ssl_context}

    return connection_string, connect_args


def quote_mysql_identifiers(sql: str) -> str:
    """
    Quote MySQL identifiers using backticks instead of double quotes.
    MySQL uses backticks (`) for identifier quoting, not double quotes.
    """
    try:
        tree = parse_one(sql)
    except Exception as e:
        logger.debug(f"Failed to parse SQL: {e}")
        return sql

    def quote_identifier_if_needed(identifier):
        if isinstance(identifier, exp.Identifier):
            name = identifier.name
            # Check if identifier needs quoting (contains special chars, is reserved word, etc.)
            if not identifier.args.get("quoted") and (
                name != name.lower()
                or any(char in name for char in [" ", "-", "."])
                or name.upper()
                in [
                    "ORDER",
                    "GROUP",
                    "SELECT",
                    "FROM",
                    "WHERE",
                    "JOIN",
                ]  # Add more reserved words as needed
            ):
                return exp.Identifier(this=name, quoted=True)
            return identifier
        elif isinstance(identifier, str):
            if identifier != identifier.lower() or any(
                char in identifier for char in [" ", "-", "."]
            ):
                return exp.Identifier(this=identifier, quoted=True)
            return exp.Identifier(this=identifier)
        return identifier

    for node in tree.walk():
        if isinstance(node, exp.Column):
            node.set("this", quote_identifier_if_needed(node.this))
            if node.table:
                node.set("table", quote_identifier_if_needed(node.table))
        elif isinstance(node, exp.Alias):
            alias = node.args.get("alias")
            if isinstance(alias, exp.Identifier):
                node.set("alias", quote_identifier_if_needed(alias))

    # Use MySQL dialect to generate SQL with backticks
    return tree.sql(dialect="mysql")


async def run_sql_and_get_dataframe_mysql_async(
    normalized_conn_str: str, connect_args: dict, db_id: str, sql: str, timeout: int = 90
) -> pd.DataFrame:
    """
    Execute SQL query on MySQL using SQLAlchemy async engine with timeout.
    
    Args:
        normalized_conn_str: MySQL connection string
        connect_args: Connection arguments
        db_id: Database ID to connect to
        sql: SQL query to execute
        timeout: Query timeout in seconds (default: 90)
        
    Returns:
        pd.DataFrame: Query results
        
    Raises:
        asyncio.TimeoutError: If query execution exceeds timeout
    """
    sqlalchemy_async, text = _require_mysql_deps()

    # If db_id is provided, create a new connection string for that specific database
    if db_id:
        final_conn_str, final_connect_args = normalize_mysql_connection_string(
            normalized_conn_str.replace("mysql+aiomysql://", "mysql://"), db_id
        )
    else:
        final_conn_str, final_connect_args = normalized_conn_str, connect_args

    engine = sqlalchemy_async.create_async_engine(
        final_conn_str,
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True,
        pool_recycle=3600,
        echo=False,
        connect_args=final_connect_args,
    )

    # Log query execution start
    sql_preview = sql[:100] + "..." if len(sql) > 100 else sql
    logger.debug(f"Executing MySQL query on db_id={db_id}: {sql_preview}")
    start_time = time.perf_counter()

    try:
        # Wrap execution with timeout
        async with asyncio.timeout(timeout):
            async with engine.begin() as conn:
                result = await conn.execute(text(sql))
                if result.returns_rows:
                    rows = result.fetchall()
                    columns = list(result.keys())
                    data = [dict(zip(columns, row)) for row in rows]
                else:
                    columns = []
                    data = []
                
                elapsed = time.perf_counter() - start_time
                logger.debug(f"Query completed in {elapsed:.2f}s, returned {len(data)} rows")
                
                return await asyncio.to_thread(pd.DataFrame, data, columns=columns)
    except asyncio.TimeoutError:
        elapsed = time.perf_counter() - start_time
        logger.error(f"Query timed out after {elapsed:.2f}s (limit: {timeout}s)")
        raise
    finally:
        await engine.dispose()


async def mysql_run_execution_async(
    connection_string: str,
    predictions_path: Path | str,
    max_concurrent_tasks: int = 16,
    per_query_timeout_s: int = 90,
    force_rerun: bool = False
):
    """
    Execute SQL queries against MySQL database asynchronously.
    Each prediction record can specify its own db_id for database selection.

    Args:
        connection_string: MySQL connection string (e.g., "mysql://user:pass@host:port/default_db")
        predictions_path: Path to predictions JSON file
        max_concurrent_tasks: Maximum number of concurrent database connections
        per_query_timeout_s: Timeout for each SQL query in seconds (default: 90)
        force_rerun: Force re-execution even if results exist
    """
    sqlalchemy_async, text = _require_mysql_deps()

    with open(predictions_path, "r") as pf:
        predictions_data = json.load(pf)

    logger.debug(f"Original MySQL connection string: {connection_string}")

    # Test the base connection first
    try:
        normalized_conn_str, connect_args = normalize_mysql_connection_string(
            connection_string
        )
        logger.debug(f"Normalized MySQL connection string: {normalized_conn_str}")
        logger.debug(f"MySQL connect_args: {connect_args}")

        # Create a test engine to verify base connection
        test_engine = sqlalchemy_async.create_async_engine(
            normalized_conn_str,
            pool_size=1,
            max_overflow=0,
            pool_pre_ping=True,
            pool_recycle=3600,
            echo=False,
            connect_args=connect_args,
        )

        # Test the connection
        logger.debug("Testing MySQL base connection...")
        async with test_engine.begin() as conn:
            result = await conn.execute(text("SELECT 1"))
            test_result = result.fetchone()
            logger.debug(f"MySQL base connection test successful: {test_result}")

        await test_engine.dispose()

    except Exception as e:
        logger.error(f"Error connecting to MySQL database: {e}")
        raise RuntimeError(f"Failed to connect to MySQL database: {e}") from e

    semaphore = asyncio.Semaphore(max_concurrent_tasks)

    # Counter and lock for tracking queries
    query_count = 0
    query_lock = asyncio.Lock()

    async def run_sql_with_count(db_id: str, sql: str):
        nonlocal query_count
        df = await run_sql_and_get_dataframe_mysql_async(
            normalized_conn_str, connect_args, db_id, sql, per_query_timeout_s
        )
        async with query_lock:
            query_count += 1
        return df

    async def process_prediction(obj):
        async with semaphore:
            obj = json.loads(json.dumps(obj))  # deepcopy-safe

            if "metadata" in obj and "sql" in obj["metadata"]:
                obj["sql"] = obj["metadata"]["sql"]

            # Get db_id from the prediction record, default to None (use connection string default)
            record_db_id = obj.get("db_id")

            gt_sqls = obj.get("sql")
            if not isinstance(gt_sqls, list):
                gt_sqls = [gt_sqls]

            gt_dfs = []
            for gt_sql in gt_sqls:
                try:
                    gt_df = await run_sql_with_count(record_db_id, gt_sql)
                    gt_dfs.append(gt_df.to_json(orient="split"))
                except asyncio.TimeoutError:
                    logger.error(f"GT SQL timed out after {per_query_timeout_s}s")
                    obj["gt_sql_execution_error"] = (
                        f"GT SQL timed out after {per_query_timeout_s}s"
                    )
                    # Continue to allow prediction execution even if GT times out
                except Exception as e:
                    logger.error(f"Error running ground truth SQL: {e}")
                    logger.error(f"SQL: {gt_sql}")
                    raise e

            obj["gt_df"] = gt_dfs if len(gt_dfs) > 1 else gt_dfs[0]
            obj.pop("gt_sql_execution_error", None)

            async def _run_and_store_df(
                sql_key: str,
                df_key: str,
                error_key: str,
                truncated_flag_key: str,
                execution_time_key: str,
                model_predictions: dict,
                obj: dict,
                run_sql_fn,
            ):
                sql = model_predictions.get(sql_key)
                if not sql or (df_key in model_predictions and not force_rerun):
                    return

                # Use MySQL-specific identifier quoting
                sql = quote_mysql_identifiers(sql)
                model_predictions[sql_key] = sql

                try:
                    # Time the execution
                    execution_start = time.perf_counter()
                    df = await run_sql_fn(record_db_id, sql)
                    execution_end = time.perf_counter()
                    execution_time_ms = (execution_end - execution_start) * 1000

                    # Determine max ground truth row count
                    if isinstance(obj["gt_df"], list):
                        max_gt_rows = max(
                            pd.read_json(gt, orient="split").shape[0]
                            for gt in obj["gt_df"]
                        )
                    else:
                        max_gt_rows = pd.read_json(obj["gt_df"], orient="split").shape[
                            0
                        ]

                    # Truncate if too many rows
                    if df.shape[0] - max_gt_rows >= 10:
                        df = df.head(max_gt_rows + 9)
                        model_predictions[truncated_flag_key] = True

                    model_predictions[df_key] = df.to_json(orient="split")
                    model_predictions[execution_time_key] = round(execution_time_ms, 2)
                    model_predictions.pop(error_key, None)

                except asyncio.TimeoutError:
                    model_predictions[error_key] = (
                        f"Error running SQL: timed out after {per_query_timeout_s}s"
                    )
                    model_predictions[execution_time_key] = None
                    logger.debug(f"SQL timed out after {per_query_timeout_s}s")
                except Exception as e:
                    model_predictions[error_key] = f"Error running SQL: {e}"
                    model_predictions[execution_time_key] = None
                    logger.debug(f"SQL error: {e}")

            for model_name, model_predictions in obj.get("predictions", {}).items():
                await _run_and_store_df(
                    sql_key="predicted_sql",
                    df_key="predicted_df",
                    error_key="sql_execution_error",
                    truncated_flag_key="predicted_df_truncated",
                    execution_time_key="execution_time_ms",
                    model_predictions=model_predictions,
                    obj=obj,
                    run_sql_fn=run_sql_with_count,
                )
                await _run_and_store_df(
                    sql_key="logic_sql",
                    df_key="logic_df",
                    error_key="logic_sql_execution_error",
                    truncated_flag_key="logic_df_truncated",
                    execution_time_key="logic_execution_time_ms",
                    model_predictions=model_predictions,
                    obj=obj,
                    run_sql_fn=run_sql_with_count,
                )

            return obj

    tasks = [process_prediction(obj) for obj in predictions_data]
    updated_predictions_data = await tqdm_asyncio.gather(
        *tasks, desc="Executing MySQL SQL queries"
    )

    with open(predictions_path, "w") as pf:
        json.dump(updated_predictions_data, pf, indent=4)

    return query_count


def _require_ibm_db():
    """Import ibm_db only when needed; raise a helpful error if missing."""
    global _ibm_db_mod
    if _ibm_db_mod is None:
        try:
            _ibm_db_mod = importlib.import_module("ibm_db")
        except ModuleNotFoundError as e:
            raise RuntimeError(
                "DB2 support is optional and not installed. "
                "Install extras with: pip install .[db2]  (or) pip install ibm-db>=3.2.6"
            ) from e
    return _ibm_db_mod


def quote_mixed_case_columns(sql: str) -> str:
    try:
        tree = parse_one(sql)
    except Exception as e:
        logger.debug(f"Failed to parse SQL: {e}")
        return sql

    def quote_identifier_if_needed(identifier):
        if isinstance(identifier, exp.Identifier):
            name = identifier.name
            if not identifier.args.get("quoted") and name != name.lower():
                return exp.Identifier(this=name, quoted=True)
            return identifier
        elif isinstance(identifier, str):
            if identifier != identifier.lower():
                return exp.Identifier(this=identifier, quoted=True)
            return exp.Identifier(this=identifier)
        return identifier

    for node in tree.walk():
        if isinstance(node, exp.Column):
            node.set("this", quote_identifier_if_needed(node.this))
            if node.table:
                node.set("table", quote_identifier_if_needed(node.table))
        elif isinstance(node, exp.Alias):
            alias = node.args.get("alias")
            if isinstance(alias, exp.Identifier):
                node.set("alias", quote_identifier_if_needed(alias))

    return tree.sql(dialect="postgres")


async def run_sql_and_get_dataframe_async(
    pool, schema_name: str, sql: str, timeout: int = 90
) -> pd.DataFrame:
    """
    Execute SQL query on PostgreSQL with timeout.
    
    Args:
        pool: asyncpg connection pool
        schema_name: PostgreSQL schema name
        sql: SQL query to execute
        timeout: Query timeout in seconds (default: 90)
    
    Returns:
        pd.DataFrame: Query results as DataFrame
    
    Raises:
        asyncio.TimeoutError: If query execution exceeds timeout
    """
    async def _execute_query():
        async with pool.acquire() as conn:
            # Set search_path for this connection
            await conn.execute(f"SET search_path TO {schema_name}")
            rows = await conn.fetch(sql)
            if rows:
                columns = rows[0].keys()
            else:
                columns = []
            data = [dict(row) for row in rows]
            # Use `to_thread` because DataFrame creation is not async
            return await asyncio.to_thread(pd.DataFrame, data, columns=columns)
    
    # Wrap execution with timeout
    return await asyncio.wait_for(_execute_query(), timeout=timeout)


async def postgres_run_execution_async(
    connection_string, schema_name, predictions_path, max_concurrent_tasks: int = 16, per_query_timeout_s: int = 90, force_rerun: bool = False
):
    """
    Execute SQL queries on PostgreSQL using asyncpg with timeout.
    
    Args:
        connection_string: PostgreSQL connection string
        schema_name: PostgreSQL schema name
        predictions_path: Path to predictions JSON file
        max_concurrent_tasks: Maximum number of concurrent database connections
        per_query_timeout_s: Timeout for each SQL query in seconds (default: 90)
        force_rerun: Force re-execution even if results exist
    
    Returns:
        int: Total number of queries executed
    """
    with open(predictions_path, "r") as pf:
        predictions_data = json.load(pf)
    
    # Set search_path using server_settings parameter
    pool = await asyncpg.create_pool(
        dsn=connection_string,
        min_size=1,
        max_size=max_concurrent_tasks,
        server_settings={'search_path': schema_name}
    )
    semaphore = asyncio.Semaphore(max_concurrent_tasks)

    # Counter and lock
    query_count = 0
    query_lock = asyncio.Lock()

    async def run_sql_with_count(sql):
        nonlocal query_count
        df = await run_sql_and_get_dataframe_async(pool, schema_name, sql, per_query_timeout_s)
        async with query_lock:
            query_count += 1
        return df

    async def process_prediction(obj):
        async with semaphore:
            obj = json.loads(json.dumps(obj))  # deepcopy-safe

            if "metadata" in obj and "sql" in obj["metadata"]:
                obj["sql"] = obj["metadata"]["sql"]

            gt_sqls = obj.get("sql")
            if not isinstance(gt_sqls, list):
                gt_sqls = [gt_sqls]

            gt_dfs = []
            for gt_sql in gt_sqls:
                try:
                    gt_df = await run_sql_with_count(gt_sql)
                    gt_dfs.append(gt_df.to_json(orient="split"))
                except asyncio.TimeoutError:
                    logger.error(f"GT SQL timed out after {per_query_timeout_s}s")
                    obj["gt_sql_execution_error"] = (
                        f"GT SQL timed out after {per_query_timeout_s}s"
                    )
                except Exception as e:
                    logger.error(f"Error running ground truth SQL: {e}")
                    logger.error(f"SQL: {gt_sql}")
                    obj["gt_sql_execution_error"] = (
                        f"Error running ground truth SQL: {e}"
                    )
                    # Continue processing other queries instead of raising

            if gt_dfs:
                obj["gt_df"] = gt_dfs if len(gt_dfs) > 1 else gt_dfs[0]
                obj.pop("gt_sql_execution_error", None)

            async def _run_and_store_df(
                sql_key: str,
                df_key: str,
                error_key: str,
                truncated_flag_key: str,
                execution_time_key: str,
                model_predictions: dict,
                obj: dict,
                run_sql_fn,
            ):
                sql = model_predictions.get(sql_key)
                if not sql or (df_key in model_predictions and not force_rerun):
                    return

                sql = quote_mixed_case_columns(sql)
                model_predictions[sql_key] = sql

                try:
                    # Time the execution
                    execution_start = time.perf_counter()
                    df = await run_sql_fn(sql)
                    execution_end = time.perf_counter()
                    execution_time_ms = (execution_end - execution_start) * 1000

                    # Determine max ground truth row count
                    if isinstance(obj["gt_df"], list):
                        max_gt_rows = max(
                            pd.read_json(gt, orient="split").shape[0]
                            for gt in obj["gt_df"]
                        )
                    else:
                        max_gt_rows = pd.read_json(obj["gt_df"], orient="split").shape[
                            0
                        ]

                    # Truncate if too many rows
                    if df.shape[0] - max_gt_rows >= 10:
                        df = df.head(max_gt_rows + 9)
                        model_predictions[truncated_flag_key] = True

                    model_predictions[df_key] = df.to_json(orient="split")
                    model_predictions[execution_time_key] = round(execution_time_ms, 2)
                    model_predictions.pop(error_key, None)

                except asyncio.TimeoutError:
                    model_predictions[error_key] = (
                        f"Error running SQL: timed out after {per_query_timeout_s}s"
                    )
                    model_predictions[execution_time_key] = None
                    logger.debug(f"SQL timed out after {per_query_timeout_s}s")
                except Exception as e:
                    model_predictions[error_key] = f"Error running SQL: {e}"
                    model_predictions[execution_time_key] = None
                    logger.debug(f"SQL error: {e}")

            for model_name, model_predictions in obj.get("predictions", {}).items():
                await _run_and_store_df(
                    sql_key="predicted_sql",
                    df_key="predicted_df",
                    error_key="sql_execution_error",
                    truncated_flag_key="predicted_df_truncated",
                    execution_time_key="execution_time_ms",
                    model_predictions=model_predictions,
                    obj=obj,
                    run_sql_fn=run_sql_with_count,
                )
                await _run_and_store_df(
                    sql_key="logic_sql",
                    df_key="logic_df",
                    error_key="logic_sql_execution_error",
                    truncated_flag_key="logic_df_truncated",
                    execution_time_key="logic_execution_time_ms",
                    model_predictions=model_predictions,
                    obj=obj,
                    run_sql_fn=run_sql_with_count,
                )

            return obj

    tasks = [process_prediction(obj) for obj in predictions_data]
    # updated_predictions_data = await asyncio.gather(*tasks)
    updated_predictions_data = await tqdm_asyncio.gather(
        *tasks, desc="Executing Postgres SQL queries"
    )
    await pool.close()

    with open(predictions_path, "w") as pf:
        json.dump(updated_predictions_data, pf, indent=4)

    return query_count


def run_sqlite_query(db_path: str, sql: str) -> str:
    conn = sqlite3.connect(db_path)
    conn.text_factory = lambda b: b.decode(errors='replace')
    conn.row_factory = sqlite3.Row
    cursor = conn.execute(sql)
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    data = [dict(zip(columns, row)) for row in rows]
    conn.close()
    return pd.DataFrame(data, columns=columns).to_json(orient="split")


async def run_sqlite_query_with_timeout(
    db_path: Path, sql: str, timeout: int
) -> pd.DataFrame:
    loop = asyncio.get_running_loop()
    try:
        json_result = await loop.run_in_executor(
            None,
            lambda: func_timeout(timeout, run_sqlite_query, args=(str(db_path), sql)),
        )
        return pd.read_json(StringIO(json_result), orient="split")
    except FunctionTimedOut:
        raise asyncio.TimeoutError(f"Query timed out after {timeout} seconds")
    except Exception as e:
        raise RuntimeError(f"Error running query: {e}")


async def sqlite_run_execution_async(
    db_folder,
    predictions_path,
    max_concurrent_tasks: int = 32,
    sql_execution_timeout: int = 5,
    force_rerun: bool = False,
):
    with open(predictions_path, "r") as pf:
        predictions_data = json.load(pf)

    semaphore = asyncio.Semaphore(max_concurrent_tasks)
    query_count = 0

    async def process_prediction(record):
        nonlocal query_count
        async with semaphore:
            record = json.loads(json.dumps(record))
            if "metadata" in record and "sql" in record["metadata"]:
                record["sql"] = record["metadata"]["sql"]
            db_id = record["db_id"]
            db_filename = db_id + ".sqlite"
            db_path = (
                Path(BENCHMARKS_FILE).parent / Path(db_folder) / db_id / db_filename
            )
            if not db_path.exists():
                raise ValueError(f"DB does not exist: {db_path}")

            gt_sqls = get_gt_sqls(record)

            gt_dfs = []
            for gt_sql in gt_sqls:
                try:
                    gt_df = await run_sqlite_query_with_timeout(
                        db_path, gt_sql, sql_execution_timeout
                    )
                    query_count += 1
                    gt_dfs.append(gt_df.to_json(orient="split"))
                except asyncio.TimeoutError:
                    logger.error(f"Ground truth query timed out: {gt_sql}")
                    record["gt_sql_execution_error"] = "Ground truth query timed out."
                except Exception as e:
                    logger.debug(f"DB path: {db_path}")
                    logger.error(f"Error running ground truth SQL: {e}")
                    logger.debug(f"Ground truth SQL with error: {gt_sql}")
                    record["gt_sql_execution_error"] = (
                        f"Error running ground truth SQL: {e}"
                    )
                    raise e
            if len(gt_dfs) == 1:
                record["gt_df"] = gt_dfs[0]
                record.pop("gt_sql_execution_error", None)
            elif len(gt_dfs) > 1:
                record["gt_df"] = gt_dfs
                record.pop("gt_sql_execution_error", None)

            async def _run_sqlite_and_store(
                sql_key: str,
                df_key: str,
                error_key: str,
                truncated_flag_key: str,
                execution_time_key: str,
                model_predictions: dict,
                record: dict,
                db_path: str,
                sql_execution_timeout: float,
                query_count_ref: list,
            ):
                sql = model_predictions.get(sql_key)
                if not sql or (df_key in model_predictions and not force_rerun):
                    return

                try:
                    # Time the execution
                    execution_start = time.perf_counter()
                    df = await run_sqlite_query_with_timeout(
                        db_path, sql, sql_execution_timeout
                    )
                    execution_end = time.perf_counter()
                    execution_time_ms = (execution_end - execution_start) * 1000
                    
                    query_count_ref[0] += 1
                    logger.debug(f"Finished running SQL query #{query_count_ref[0]}.")

                    max_gt_rows = 10
                    if "gt_df" in record:
                        if isinstance(record["gt_df"], list):
                            max_gt_rows = max(
                                pd.read_json(StringIO(gt), orient="split").shape[0]
                                for gt in record["gt_df"]
                            )
                        else:
                            max_gt_rows = pd.read_json(
                                StringIO(record["gt_df"]), orient="split"
                            ).shape[0]

                        if df.shape[0] - max_gt_rows >= 10:
                            df = df.head(max_gt_rows + 9)
                            model_predictions[truncated_flag_key] = True

                    model_predictions[df_key] = df.to_json(orient="split")
                    model_predictions[execution_time_key] = round(execution_time_ms, 2)
                    model_predictions.pop(error_key, None)

                except asyncio.TimeoutError:
                    logger.info(f"{sql_key} query execution timed out: {sql}")
                    model_predictions[error_key] = (
                        f"{sql_key} query execution timed out: {sql}"
                    )
                    model_predictions[execution_time_key] = None
                except Exception as e:
                    model_predictions[error_key] = f"Error running SQL: {e}"
                    model_predictions[execution_time_key] = None
                    logger.debug(f"{sql_key} error: {e}")

            query_count_ref = [query_count]  # wrap in list to mutate inside helper
            for model_name, model_predictions in record.get("predictions", {}).items():
                await _run_sqlite_and_store(
                    sql_key="predicted_sql",
                    df_key="predicted_df",
                    error_key="sql_execution_error",
                    truncated_flag_key="predicted_df_truncated",
                    execution_time_key="execution_time_ms",
                    model_predictions=model_predictions,
                    record=record,
                    db_path=db_path,
                    sql_execution_timeout=sql_execution_timeout,
                    query_count_ref=query_count_ref,
                )
                await _run_sqlite_and_store(
                    sql_key="logic_sql",
                    df_key="logic_df",
                    error_key="logic_sql_execution_error",
                    truncated_flag_key="logic_df_truncated",
                    execution_time_key="logic_execution_time_ms",
                    model_predictions=model_predictions,
                    record=record,
                    db_path=db_path,
                    sql_execution_timeout=sql_execution_timeout,
                    query_count_ref=query_count_ref,
                )

            return record

    tasks = [process_prediction(obj) for obj in predictions_data]
    # updated_predictions_data = await asyncio.gather(*tasks)
    updated_predictions_data = await tqdm_asyncio.gather(
        *tasks, desc="Executing SQLite SQL queries"
    )

    logger.debug("Writing updated predictions with execution dataframes to file...")
    with open(predictions_path, "w") as pf:
        json.dump(updated_predictions_data, pf, indent=4)
    logger.debug("Finished writing.")

    return query_count



_LIMIT_RE = re.compile(r"(?is)\s+LIMIT\s+(\d+)\s*$")


def _parse_db2_dsn(dsn: str) -> dict:
    parts = [p for p in dsn.strip().strip(";").split(";") if p.strip()]
    kv = {}
    for p in parts:
        if "=" in p:
            k, v = p.split("=", 1)
            kv[k.strip().upper()] = v.strip()
    return kv


def _normalize_sql_for_db2(sql: str) -> str:
    s = (sql or "").rstrip().rstrip(";")
    m = _LIMIT_RE.search(s)
    if m:
        n = m.group(1)
        s = _LIMIT_RE.sub(f" FETCH FIRST {n} ROWS ONLY", s)
    return s


async def db2_run_execution_async(
    connection_string: str,
    schema_name: str | None,
    predictions_path: Path | str,
    max_concurrent_tasks: int = 16,
    per_query_timeout_s: int = 90,
):
    with open(predictions_path, "r") as pf:
        predictions_data = json.load(pf)

    dsn_kv = _parse_db2_dsn(connection_string)
    dsn_schema = dsn_kv.get("CURRENTSCHEMA")
    effective_schema = schema_name or dsn_schema

    semaphore = asyncio.Semaphore(max_concurrent_tasks)
    query_count = 0
    query_lock = asyncio.Lock()

    def _run_sql_and_get_dataframe_db2(sql: str) -> pd.DataFrame:
        ibm_db = _require_ibm_db()  # lazy import
        fixed_sql = _normalize_sql_for_db2(sql)

        conn = ibm_db.connect(connection_string, "", "")
        try:
            try:
                ibm_db.autocommit(conn, ibm_db.SQL_AUTOCOMMIT_ON)
            except Exception:
                pass

            if effective_schema:
                ibm_db.exec_immediate(conn, f"SET CURRENT SCHEMA {effective_schema}")

            stmt = ibm_db.prepare(conn, fixed_sql)
            try:
                ibm_db.set_option(
                    stmt, {ibm_db.SQL_ATTR_QUERY_TIMEOUT: per_query_timeout_s}, 0
                )
            except Exception:
                pass

            ok = ibm_db.execute(stmt)
            rows, cols = [], []
            if ok and ibm_db.num_fields(stmt) > 0:
                ncols = ibm_db.num_fields(stmt)
                cols = [ibm_db.field_name(stmt, i) for i in range(ncols)]
                tup = ibm_db.fetch_tuple(stmt)
                while tup:
                    rows.append(tup)
                    tup = ibm_db.fetch_tuple(stmt)

            ibm_db.free_stmt(stmt)
            return pd.DataFrame(rows, columns=cols)
        finally:
            ibm_db.close(conn)

    async def run_sql_and_get_dataframe_async_db2(sql: str) -> pd.DataFrame:
        return await asyncio.wait_for(
            asyncio.to_thread(_run_sql_and_get_dataframe_db2, sql),
            timeout=per_query_timeout_s + 5,
        )

    async def run_sql_with_count(sql: str) -> pd.DataFrame:
        nonlocal query_count
        df = await run_sql_and_get_dataframe_async_db2(sql)
        async with query_lock:
            query_count += 1
        return df

    async def process_prediction(obj: dict):
        async with semaphore:
            obj = json.loads(json.dumps(obj))  # deepcopy-safe

            if "metadata" in obj and "sql" in obj["metadata"]:
                obj["sql"] = obj["metadata"]["sql"]

            gt_sqls = obj.get("sql")
            if not isinstance(gt_sqls, list):
                gt_sqls = [gt_sqls]

            gt_dfs = []
            for gt_sql in gt_sqls:
                try:
                    gt_df = await run_sql_with_count(gt_sql)
                    gt_dfs.append(gt_df.to_json(orient="split"))
                except asyncio.TimeoutError:
                    logger.error(f"GT SQL timed out after {per_query_timeout_s}s")
                    obj["gt_sql_execution_error"] = (
                        f"GT SQL timed out after {per_query_timeout_s}s"
                    )
                    # raise
                except Exception as e:
                    logger.error(f"Error running ground truth SQL: {e}")
                    logger.error(f"SQL: {gt_sql}")
                    obj["gt_sql_execution_error"] = (
                        f"Error running ground truth SQL: {e}"
                    )
                    # raise
            if len(gt_dfs) > 0:
                obj["gt_df"] = gt_dfs if len(gt_dfs) > 1 else gt_dfs[0]
                obj.pop("gt_sql_execution_error", None)

            async def _run_and_store_df(
                sql_key: str,
                df_key: str,
                error_key: str,
                truncated_flag_key: str,
                execution_time_key: str,
                model_predictions: dict,
                obj: dict,
                run_sql_fn,
            ):
                sql = model_predictions.get(sql_key)
                if not sql or df_key in model_predictions:
                    return

                sql = quote_mixed_case_columns(sql)
                model_predictions[sql_key] = sql

                try:
                    # Time the execution
                    execution_start = time.perf_counter()
                    df = await run_sql_fn(sql)
                    execution_end = time.perf_counter()
                    execution_time_ms = (execution_end - execution_start) * 1000

                    if isinstance(obj["gt_df"], list):
                        max_gt_rows = max(
                            pd.read_json(gt, orient="split").shape[0]
                            for gt in obj["gt_df"]
                        )
                    else:
                        max_gt_rows = pd.read_json(obj["gt_df"], orient="split").shape[
                            0
                        ]

                    if df.shape[0] - max_gt_rows >= 10:
                        df = df.head(max_gt_rows + 9)
                        model_predictions[truncated_flag_key] = True

                    model_predictions[df_key] = df.to_json(orient="split")
                    model_predictions[execution_time_key] = round(execution_time_ms, 2)
                    model_predictions.pop(error_key, None)

                except asyncio.TimeoutError:
                    model_predictions[error_key] = (
                        f"Error running SQL: timed out after {per_query_timeout_s}s"
                    )
                    model_predictions[execution_time_key] = None
                except Exception as e:
                    model_predictions[error_key] = f"Error running SQL: {e}"
                    model_predictions[execution_time_key] = None
                    logger.debug(f"SQL error: {e}")

            for _, model_predictions in obj.get("predictions", {}).items():
                await _run_and_store_df(
                    sql_key="predicted_sql",
                    df_key="predicted_df",
                    error_key="sql_execution_error",
                    truncated_flag_key="predicted_df_truncated",
                    execution_time_key="execution_time_ms",
                    model_predictions=model_predictions,
                    obj=obj,
                    run_sql_fn=run_sql_with_count,
                )
                await _run_and_store_df(
                    sql_key="logic_sql",
                    df_key="logic_df",
                    error_key="logic_sql_execution_error",
                    truncated_flag_key="logic_df_truncated",
                    execution_time_key="logic_execution_time_ms",
                    model_predictions=model_predictions,
                    obj=obj,
                    run_sql_fn=run_sql_with_count,
                )

            return obj

    tasks = [process_prediction(obj) for obj in predictions_data]
    updated_predictions_data = await tqdm_asyncio.gather(
        *tasks, desc="Executing DB2 SQL queries"
    )
    with open(predictions_path, "w") as pf:
        json.dump(updated_predictions_data, pf, indent=4)
    return query_count


def _parse_presto_sqlalchemy_url(conn_str: str) -> dict:
    """
    Parse a SQLAlchemy-style Presto URL into prestodb.dbapi.connect(**kwargs) args.

    Example input:
      presto://user:pass@host:30624/catalog/schema?currentSchema=schema
    """
    u = urlparse(conn_str)
    username = unquote(u.username or "")
    password = unquote(u.password or "")
    host = u.hostname
    port = u.port or 443  # default to 443 for https
    # path like "/catalog/schema"
    path_parts = [p for p in (u.path or "").split("/") if p]
    catalog = path_parts[0] if len(path_parts) >= 1 else None
    schema = path_parts[1] if len(path_parts) >= 2 else None

    qs = parse_qs(u.query or "")
    # Allow overriding schema via ?currentSchema=...
    if "currentSchema" in qs and qs["currentSchema"]:
        schema = qs["currentSchema"][0]

    # Build BasicAuthentication if password is provided
    import prestodb  # local import to keep optional

    auth = prestodb.auth.BasicAuthentication(username, password) if password else None

    # Filter out Nones
    kwargs = {
        "host": host,
        "port": port,
        "user": username,
        "catalog": catalog,
        "schema": schema,
        "http_scheme": "https",
        "auth": auth,
        "source": "text2sql-eval",
    }
    return {k: v for k, v in kwargs.items() if v is not None}


async def presto_run_execution_async(
    connection_string: str,
    predictions_path: Path | str,
    max_concurrent_tasks: int = 16,
    per_query_timeout_s: int = 300,
):
    """
    Execute SQL queries on Presto (IBM Lakehouse) using prestodb.dbapi with BasicAuthentication.
    - connection_string: SQLAlchemy-style Presto URL
    - predictions_path: JSON file with records and model predictions
    - max_concurrent_tasks: concurrency bound
    - per_query_timeout_s: per-query timeout (client-side)
    """
    import prestodb
    import pandas as pd
    import asyncio
    import json

    # Parse once and reuse for all queries
    base_connect_kwargs = _parse_presto_sqlalchemy_url(connection_string)
    logger.debug(
        f"Presto connect kwargs (redacted): "
        f"{ {k: ('***' if k == 'auth' else v) for k, v in base_connect_kwargs.items()} }"
    )

    # Simple connectivity check
    try:

        def _ping():
            conn = prestodb.dbapi.connect(**base_connect_kwargs)
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchall()
            cur.close()
            conn.close()

        await asyncio.wait_for(asyncio.to_thread(_ping), timeout=30)
        logger.debug("Presto connectivity check succeeded.")
    except Exception as e:
        logger.error(f"Failed Presto connectivity check: {e}")
        raise RuntimeError(f"Failed to connect to Presto: {e}") from e

    # Helper: run one SQL and return DataFrame (threaded, with timeout)
    def _run_presto_sql_to_df(sql: str) -> pd.DataFrame:
        conn = prestodb.dbapi.connect(**base_connect_kwargs)
        try:
            cur = conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall() or []
            cols = [d[0] for d in (cur.description or [])]
            cur.close()
            return pd.DataFrame(rows, columns=cols)
        finally:
            conn.close()

    async def run_sql_and_get_dataframe_presto(sql: str) -> pd.DataFrame:
        # client-side timeout guard; Presto may continue server-side even if we time out here
        return await asyncio.wait_for(
            asyncio.to_thread(_run_presto_sql_to_df, sql),
            timeout=per_query_timeout_s,
        )

    # Orchestrate over predictions
    with open(predictions_path, "r") as pf:
        predictions_data = json.load(pf)

    semaphore = asyncio.Semaphore(max_concurrent_tasks)

    # Shared counter + lock
    query_count = 0
    query_lock = asyncio.Lock()

    async def run_sql_with_count(sql: str) -> pd.DataFrame:
        nonlocal query_count
        df = await run_sql_and_get_dataframe_presto(sql)
        async with query_lock:
            query_count += 1
        return df

    async def process_record(obj: dict):
        async with semaphore:
            obj = json.loads(json.dumps(obj))  # deep-copy-safe

            # Normalize where GT SQL lives
            if "metadata" in obj and "sql" in obj["metadata"]:
                obj["sql"] = obj["metadata"]["sql"]

            # Get GT sql(s)
            gt_sqls = get_gt_sqls(obj) if callable(get_gt_sqls) else obj.get("sql")
            if not isinstance(gt_sqls, list):
                gt_sqls = [gt_sqls]

            # Ground truth execution
            gt_dfs = []
            for gt_sql in gt_sqls:
                if not gt_sql:
                    continue
                try:
                    df = await run_sql_with_count(gt_sql)
                    gt_dfs.append(df.to_json(orient="split"))
                except asyncio.TimeoutError:
                    logger.error(f"GT SQL timed out after {per_query_timeout_s}s")
                    obj["gt_sql_execution_error"] = (
                        f"GT SQL timed out after {per_query_timeout_s}s"
                    )
                except Exception as e:
                    logger.error(f"Error running GT SQL: {e}")
                    logger.debug(f"GT SQL was: {gt_sql}")
                    obj["gt_sql_execution_error"] = (
                        f"Error running ground truth SQL: {e}"
                    )
                    # Keep going, or:
                    # raise e

            if gt_dfs:
                obj["gt_df"] = gt_dfs if len(gt_dfs) > 1 else gt_dfs[0]
                obj.pop("gt_sql_execution_error", None)

            # Helper for model SQLs
            async def _run_and_store_df(
                sql_key: str,
                df_key: str,
                error_key: str,
                truncated_flag_key: str,
                execution_time_key: str,
                model_predictions: dict,
                obj_ref: dict,
            ):
                sql = model_predictions.get(sql_key)
                if not sql or df_key in model_predictions:
                    return

                # Quote mixed-case identifiers for Presto (double-quotes)
                sql = quote_mixed_case_columns(sql)
                model_predictions[sql_key] = sql

                try:
                    # Time the execution
                    execution_start = time.perf_counter()
                    df = await run_sql_with_count(sql)
                    execution_end = time.perf_counter()
                    execution_time_ms = (execution_end - execution_start) * 1000

                    # Establish reference size from GT (max of multiple GTs if present)
                    max_gt_rows = 0
                    if "gt_df" in obj_ref and obj_ref["gt_df"]:
                        if isinstance(obj_ref["gt_df"], list):
                            max_gt_rows = max(
                                pd.read_json(gt_json, orient="split").shape[0]
                                for gt_json in obj_ref["gt_df"]
                            )
                        else:
                            max_gt_rows = pd.read_json(
                                obj_ref["gt_df"], orient="split"
                            ).shape[0]

                    # Truncate if too many extra rows
                    if max_gt_rows and (df.shape[0] - max_gt_rows >= 10):
                        df = df.head(max_gt_rows + 9)
                        model_predictions[truncated_flag_key] = True

                    model_predictions[df_key] = df.to_json(orient="split")
                    model_predictions[execution_time_key] = round(execution_time_ms, 2)
                    model_predictions.pop(error_key, None)

                except asyncio.TimeoutError:
                    model_predictions[error_key] = (
                        f"Error running SQL: timed out after {per_query_timeout_s}s"
                    )
                    model_predictions[execution_time_key] = None
                except Exception as e:
                    model_predictions[error_key] = f"Error running SQL: {e}"
                    model_predictions[execution_time_key] = None
                    logger.debug(f"{sql_key} error: {e}")

            # Execute model predictions
            for _, model_predictions in obj.get("predictions", {}).items():
                await _run_and_store_df(
                    sql_key="predicted_sql",
                    df_key="predicted_df",
                    error_key="sql_execution_error",
                    truncated_flag_key="predicted_df_truncated",
                    execution_time_key="execution_time_ms",
                    model_predictions=model_predictions,
                    obj_ref=obj,
                )
                await _run_and_store_df(
                    sql_key="logic_sql",
                    df_key="logic_df",
                    error_key="logic_sql_execution_error",
                    truncated_flag_key="logic_df_truncated",
                    execution_time_key="logic_execution_time_ms",
                    model_predictions=model_predictions,
                    obj_ref=obj,
                )

            return obj

    tasks = [process_record(obj) for obj in predictions_data]
    updated_predictions_data = await tqdm_asyncio.gather(
        *tasks, desc="Executing Presto SQL queries"
    )

    with open(predictions_path, "w") as pf:
        json.dump(updated_predictions_data, pf, indent=4)

    return query_count


# For running from script
def run_execution(benchmark_id: str, num_threads: int = 16, force_rerun: bool = False):
    benchmark_info = get_benchmark_info(benchmark_id)
    predictions_path = Path(benchmark_info["predictions_path"])
    db_engine = benchmark_info["db_engine"]

    replace_select_for_logic_ex(predictions_path, db_engine)

    if db_engine["db_type"] not in [
        "postgres",
        "sqlite",
        "db2",
        "mysql",
        "presto",
    ]:
        raise NotImplementedError(f"Unsupported DB type '{db_engine['db_type']}'.")

    query_count = 0

    if db_engine["db_type"] == "postgres":
        schema_name = db_engine.get("schema_name")
        connection_string = os.getenv(db_engine["connection_string_env_var"])
        if not connection_string:
            raise ValueError("Missing connection string.")
        
        # Optional: get query timeout from config
        query_timeout = db_engine.get("query_timeout", 90)
        
        query_count = asyncio.run(
            postgres_run_execution_async(
                connection_string, schema_name, predictions_path, num_threads, query_timeout, force_rerun
            )
        )

    elif db_engine["db_type"] == "sqlite":
        db_folder = benchmark_info["db_engine"]["db_folder"]
        query_count = asyncio.run(
            sqlite_run_execution_async(db_folder, predictions_path, force_rerun=force_rerun)
        )

    elif db_engine["db_type"] == "db2":
        schema_name = db_engine.get("schema_name")
        connection_string = os.getenv(db_engine["connection_string_env_var"])
        if not connection_string:
            raise ValueError("Missing DB2 connection string.")
        query_count = asyncio.run(
            db2_run_execution_async(
                connection_string, schema_name, predictions_path, num_threads, force_rerun
            )
        )
    elif db_engine["db_type"] == "mysql":
        connection_string = os.getenv(db_engine["connection_string_env_var"])
        if not connection_string:
            raise ValueError("Missing MySQL connection string.")
        
        # Optional: get query timeout from config
        query_timeout = db_engine.get("query_timeout", 90)
        
        query_count = asyncio.run(
            mysql_run_execution_async(
                connection_string, predictions_path, num_threads, query_timeout, force_rerun
            )
        )
    elif db_engine["db_type"] == "presto":
        connection_string = os.getenv(db_engine["connection_string_env_var"])
        if not connection_string:
            raise ValueError("Missing Presto connection string.")

        # Optional: get query timeout from config
        query_timeout = db_engine.get("query_timeout", 300)

        query_count = asyncio.run(
            presto_run_execution_async(
                connection_string, predictions_path, num_threads, query_timeout
            )
        )

    logger.info(f"Total SQL queries executed: {query_count}")
