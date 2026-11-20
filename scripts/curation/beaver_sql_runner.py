#!/usr/bin/env python3
#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Script to execute SQL queries from a JSON file containing records with "sql" fields.
Reuses MySQL execution infrastructure from existing predictions runner.
"""

import argparse
import asyncio
import importlib
import json
import logging
from pathlib import Path

import pandas as pd
from sqlglot import exp, parse_one
from tqdm.asyncio import tqdm as tqdm_asyncio

from text2sql_eval_toolkit.execution.execution_tools import (
    normalize_mysql_connection_string,
    redact_url_password,
)

_sqlalchemy_mod = None
_aiomysql_mod = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _require_mysql_deps():
    """Import MySQL dependencies only when needed; raise a helpful error if missing."""
    global _sqlalchemy_mod, _aiomysql_mod

    if _sqlalchemy_mod is None:
        try:
            _sqlalchemy_mod = importlib.import_module("sqlalchemy.ext.asyncio")
        except ModuleNotFoundError as e:
            raise RuntimeError(
                "MySQL support is optional and not installed. "
                "Install extras with: pip install sqlalchemy[asyncio] aiomysql"
            ) from e

    # Also import the text function
    try:
        from sqlalchemy import text

        return _sqlalchemy_mod, text
    except ImportError as e:
        raise RuntimeError(
            "MySQL support is optional and not installed. "
            "Install extras with: pip install sqlalchemy[asyncio] aiomysql"
        ) from e


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
    normalized_conn_str: str, connect_args: dict, db_id: str, sql: str
) -> pd.DataFrame:
    """Execute SQL query on MySQL using SQLAlchemy async engine."""
    sqlalchemy_async, text = _require_mysql_deps()

    # Only the database changes per record: the TLS context is already in
    # connect_args, and the URL no longer carries the options it came from.
    final_conn_str = normalized_conn_str
    if db_id:
        final_conn_str, _ = normalize_mysql_connection_string(
            normalized_conn_str, db_id
        )

    engine = sqlalchemy_async.create_async_engine(
        final_conn_str,
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True,
        pool_recycle=3600,
        echo=False,
        connect_args=connect_args,
    )

    try:
        async with engine.begin() as conn:
            result = await conn.execute(text(sql))
            if result.returns_rows:
                rows = result.fetchall()
                columns = list(result.keys())
                data = [dict(zip(columns, row, strict=True)) for row in rows]
            else:
                columns = []
                data = []
            return await asyncio.to_thread(pd.DataFrame, data, columns=columns)
    finally:
        await engine.dispose()


async def run_sql_queries(
    connection_string: str,
    json_file_path: Path | str,
    output_file: Path | str = None,
    max_concurrent_tasks: int = 16,
    save_results: bool = False,
):
    """
    Execute SQL queries from JSON file containing records with "sql" fields.

    Args:
        connection_string: MySQL connection string (e.g., "mysql://user:pass@host:port/db")
        json_file_path: Path to JSON file with records containing "sql" fields
        output_file: Optional path to save results (defaults to input_file_results.json)
        max_concurrent_tasks: Maximum number of concurrent database connections
        save_results: Whether to save results to output file
    """
    # Load the JSON data
    with open(json_file_path, "r") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("JSON file must contain an array of records")

    logger.info(f"Loaded {len(data)} records from {json_file_path}")

    # Test the base connection first
    try:
        normalized_conn_str, connect_args = normalize_mysql_connection_string(
            connection_string
        )
        logger.debug(
            "Normalized MySQL connection string: "
            f"{redact_url_password(normalized_conn_str)}"
        )

        # Create a test engine to verify base connection
        sqlalchemy_async, text = _require_mysql_deps()
        test_engine = sqlalchemy_async.create_async_engine(
            normalized_conn_str,
            pool_size=1,
            max_overflow=0,
            pool_pre_ping=True,
            pool_recycle=3600,
            echo=True,
            connect_args=connect_args,
        )

        # Test the connection
        logger.info("Testing MySQL connection...")
        async with test_engine.begin() as conn:
            result = await conn.execute(text("SELECT 1"))
            test_result = result.fetchone()
            logger.info(f"MySQL connection successful: {test_result}")

        await test_engine.dispose()

    except Exception as e:
        logger.error(f"Error connecting to MySQL database: {e}")
        raise RuntimeError(f"Failed to connect to MySQL database: {e}") from e

    semaphore = asyncio.Semaphore(max_concurrent_tasks)

    # Counter and lock for tracking queries
    query_count = 0
    success_count = 0
    error_count = 0
    query_lock = asyncio.Lock()

    async def process_record(record, index):
        nonlocal query_count, success_count, error_count

        async with semaphore:
            # Make a copy of the record
            result_record = record.copy()

            # Get SQL query
            sql_query = record.get("sql")
            if not sql_query:
                logger.warning(f"Record {index}: No 'sql' field found")
                result_record["execution_error"] = "No 'sql' field found"
                async with query_lock:
                    error_count += 1
                return result_record

            # Get db_id if available
            db_id = record.get("db_id")

            try:
                # Quote identifiers for MySQL
                # quoted_sql = quote_mysql_identifiers(sql_query)

                # Execute the query
                df = await run_sql_and_get_dataframe_mysql_async(
                    normalized_conn_str, connect_args, db_id, sql_query
                )

                # Store results
                if save_results:
                    result_record["result_df"] = df.to_json(orient="split")
                    result_record["row_count"] = len(df)
                    result_record["column_count"] = len(df.columns)
                    result_record["columns"] = list(df.columns)

                # Log summary
                logger.info(
                    f"Record {index}: Query executed successfully - {len(df)} rows, {len(df.columns)} columns"
                )

                async with query_lock:
                    query_count += 1
                    success_count += 1

            except Exception as e:
                error_msg = f"Error executing SQL: {e}"
                result_record["execution_error"] = error_msg
                logger.error(f"Record {index}: {error_msg}")
                logger.debug(f"SQL: {sql_query}")

                async with query_lock:
                    query_count += 1
                    error_count += 1

            return result_record

    # Process all records
    logger.info("Starting SQL execution...")
    tasks = [process_record(record, i) for i, record in enumerate(data)]
    results = await tqdm_asyncio.gather(*tasks, desc="Executing SQL queries")

    # Save results if requested
    if save_results:
        if output_file is None:
            input_path = Path(json_file_path)
            output_file = input_path.parent / f"{input_path.stem}_results.json"

        with open(output_file, "w") as f:
            json.dump(results, f, indent=2)

        logger.info(f"Results saved to {output_file}")

    # Print summary
    logger.info("\nExecution Summary:")
    logger.info(f"Total records: {len(data)}")
    logger.info(f"Successful queries: {success_count}")
    logger.info(f"Failed queries: {error_count}")
    logger.info(f"Total queries executed: {query_count}")

    return results


def main():
    parser = argparse.ArgumentParser(description="Execute SQL queries from JSON file")
    parser.add_argument(
        "json_file",
        type=str,
        help="Path to JSON file containing records with 'sql' fields",
    )
    parser.add_argument(
        "--connection-string",
        type=str,
        required=True,
        help="MySQL connection string (e.g., mysql://user:pass@host:port/db)",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output file path (defaults to input_file_results.json)",
    )
    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=16,
        help="Maximum number of concurrent database connections (default: 16)",
    )
    parser.add_argument(
        "--save-results", action="store_true", help="Save query results to output file"
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Validate input file
    json_file_path = Path(args.json_file)
    if not json_file_path.exists():
        logger.error(f"Input file not found: {json_file_path}")
        return 1

    try:
        # Run the SQL queries
        asyncio.run(
            run_sql_queries(
                connection_string=args.connection_string,
                json_file_path=json_file_path,
                output_file=args.output,
                max_concurrent_tasks=args.max_concurrent,
                save_results=args.save_results,
            )
        )
        return 0
    except Exception as e:
        logger.error(f"Script failed: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
