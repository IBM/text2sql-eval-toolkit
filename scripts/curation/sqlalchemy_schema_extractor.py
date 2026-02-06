#!/usr/bin/env python3
#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
SQLAlchemy Schema Extractor

This script connects to any SQL database supported by SQLAlchemy and extracts
schema information for specified databases, outputting a comprehensive schema.json file.

Supports: MySQL, PostgreSQL, SQLite, SQL Server, Oracle, and more.

Usage:
    python sqlalchemy_schema_extractor.py

Requirements:
    pip install sqlalchemy pymysql psycopg2-binary  # Add other drivers as needed
"""

import json
import sys
from typing import Dict, List, Any
import argparse
import re

from sqlalchemy import create_engine, MetaData, Table, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


def normalize_connection_string(connection_string: str) -> str:
    """
    Normalize connection string for SQLAlchemy.
    Handles various formats and adds appropriate drivers.
    """
    # Handle mysql:// with SSL parameters like your example
    if connection_string.startswith("mysql://"):
        # Replace mysql:// with mysql+pymysql:// for better SSL support
        connection_string = connection_string.replace("mysql://", "mysql+pymysql://")

        # Handle SSL mode parameter conversion - remove problematic SSL params for now
        # PyMySQL has issues with certain SSL parameter formats from IBM Cloud
        if "sslMode=" in connection_string:
            # Remove the sslMode parameter entirely and let PyMySQL handle SSL automatically
            connection_string = re.sub(r"[&?]sslMode=[^&]*", "", connection_string)
            # Add ssl_disabled=false to ensure SSL is used
            if "?" in connection_string:
                connection_string += "&ssl_disabled=false"
            else:
                connection_string += "?ssl_disabled=false"

    # Handle postgresql://
    elif connection_string.startswith("postgresql://"):
        connection_string = connection_string.replace(
            "postgresql://", "postgresql+psycopg2://"
        )

    # Handle other database types
    elif connection_string.startswith("sqlite://"):
        pass  # SQLite works as-is

    return connection_string


def get_database_engine(connection_string: str) -> Engine:
    """Create SQLAlchemy engine from connection string."""
    normalized_conn_str = normalize_connection_string(connection_string)

    # Debug: Print connection string details
    print(f"Original connection string: {connection_string}")
    print(f"Normalized connection string: {normalized_conn_str}")

    # Determine if this is an IBM Cloud or other SSL-required connection
    ssl_required = any(
        domain in connection_string
        for domain in ["databases.appdomain.cloud", "ssl=true", "sslMode="]
    )

    try:
        # Create engine with SSL configuration for cloud databases
        connect_args = {}
        if ssl_required:
            connect_args = {
                "ssl": {
                    "check_hostname": False,
                    "verify_mode": "CERT_NONE",  # Less strict for IBM Cloud
                },
                "ssl_disabled": False,
            }

        engine = create_engine(
            normalized_conn_str,
            pool_pre_ping=True,
            pool_recycle=3600,
            echo=False,
            connect_args=connect_args,
        )

        # Debug: Print parsed URL components
        url = engine.url
        print(f"Parsed URL components:")
        print(f"  Database type: {url.drivername}")
        print(f"  Username: {url.username}")
        print(f"  Password: {'*' * len(url.password) if url.password else 'None'}")
        print(f"  Host: {url.host}")
        print(f"  Port: {url.port}")
        print(f"  Database: {url.database}")
        print(f"  Query params: {dict(url.query)}")
        print(f"  SSL required: {ssl_required}")
        print(f"  Connect args: {connect_args}")

        # Test the connection
        print("Testing connection...")
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            print(f"Connection test successful: {result.fetchone()}")

        return engine
    except SQLAlchemyError as err:
        print(f"Error connecting to database: {err}")

        # Try alternative SSL configuration if the first attempt fails
        if ssl_required:
            print("Trying alternative SSL configuration...")
            try:
                alt_connect_args = {
                    "ssl_ca": "",  # Empty CA for IBM Cloud
                    "ssl_disabled": False,
                }

                engine = create_engine(
                    normalized_conn_str,
                    pool_pre_ping=True,
                    pool_recycle=3600,
                    echo=False,
                    connect_args=alt_connect_args,
                )

                with engine.connect() as conn:
                    result = conn.execute(text("SELECT 1"))
                    print(f"Alternative SSL connection successful: {result.fetchone()}")

                return engine
            except SQLAlchemyError as alt_err:
                print(f"Alternative SSL configuration also failed: {alt_err}")

        sys.exit(1)


def get_sample_values(
    engine: Engine,
    database_name: str,
    table_name: str,
    column_name: str,
    limit: int = 5,
) -> List[str]:
    """Get sample values from a column."""
    try:
        # Different databases handle schema/database selection differently
        db_type = engine.dialect.name

        if db_type == "mysql":
            query = text(
                f"SELECT DISTINCT `{column_name}` FROM `{database_name}`.`{table_name}` WHERE `{column_name}` IS NOT NULL LIMIT :limit"
            )
        elif db_type == "postgresql":
            query = text(
                f'SELECT DISTINCT "{column_name}" FROM "{database_name}"."{table_name}" WHERE "{column_name}" IS NOT NULL LIMIT :limit'
            )
        elif db_type == "sqlite":
            query = text(
                f'SELECT DISTINCT "{column_name}" FROM "{table_name}" WHERE "{column_name}" IS NOT NULL LIMIT :limit'
            )
        else:
            # Generic approach
            query = text(
                f'SELECT DISTINCT "{column_name}" FROM "{table_name}" WHERE "{column_name}" IS NOT NULL LIMIT :limit'
            )

        with engine.connect() as conn:
            result = conn.execute(query, {"limit": limit})
            return [str(row[0]) for row in result.fetchall()]
    except Exception:
        return []


def get_foreign_key_info(table: Table) -> Dict[str, List[Dict[str, str]]]:
    """Extract foreign key information from SQLAlchemy Table object."""
    fk_info = {}

    for column in table.columns:
        foreign_keys = []
        for fk in column.foreign_keys:
            foreign_keys.append(
                {"target_table": fk.column.table.name, "target_column": fk.column.name}
            )
        fk_info[column.name] = foreign_keys

    return fk_info


def get_primary_key_info(table: Table) -> List[str]:
    """Get primary key column names."""
    return [col.name for col in table.primary_key.columns]


def switch_database_context(engine: Engine, database_name: str):
    """Switch database context if needed (mainly for MySQL)."""
    db_type = engine.dialect.name

    if db_type == "mysql":
        with engine.connect() as conn:
            conn.execute(text(f"USE `{database_name}`"))


def extract_database_schema(engine: Engine, database_name: str) -> Dict[str, Any]:
    """Extract schema information for a single database."""

    database_schema = {"name": database_name, "tables": {}}

    try:
        # Switch database context if needed
        switch_database_context(engine, database_name)

        # Create inspector for the database
        inspector = inspect(engine)

        # For MySQL, we need to specify the schema (database)
        db_type = engine.dialect.name
        schema_name = database_name if db_type in ["mysql", "postgresql"] else None

        # Get all table names
        table_names = inspector.get_table_names(schema=schema_name)

        for table_name in table_names:
            print(f"  Processing table: {table_name}")

            # Get table metadata
            metadata = MetaData()
            table = Table(
                table_name, metadata, autoload_with=engine, schema=schema_name
            )

            # Get column information
            columns_info = inspector.get_columns(table_name, schema=schema_name)

            # Get foreign key and primary key info
            fk_info = get_foreign_key_info(table)
            pk_columns = get_primary_key_info(table)

            columns = []
            for column_info in columns_info:
                column_name = column_info["name"]

                # Map SQLAlchemy types to standard types
                column_type = str(column_info["type"]).upper()

                # Simplify common type names
                if (
                    "VARCHAR" in column_type
                    or "TEXT" in column_type
                    or "CHAR" in column_type
                ):
                    column_type = "TEXT"
                elif "INT" in column_type:
                    column_type = "INTEGER"
                elif (
                    "FLOAT" in column_type
                    or "DOUBLE" in column_type
                    or "DECIMAL" in column_type
                ):
                    column_type = "NUMERIC"
                elif "BOOL" in column_type:
                    column_type = "BOOLEAN"
                elif "DATE" in column_type or "TIME" in column_type:
                    column_type = "DATETIME"

                column_data = {
                    "name": column_name,
                    "type": column_type,
                    "primary_key": column_name in pk_columns,
                    "foreign_keys": fk_info.get(column_name, []),
                    "description": column_info.get("comment", "") or "",
                    "value_samples": get_sample_values(
                        engine, database_name, table_name, column_name
                    ),
                }

                columns.append(column_data)

            database_schema["tables"][table_name] = {
                "name": table_name,
                "columns": columns,
            }

    except SQLAlchemyError as err:
        print(f"Error processing database {database_name}: {err}")
        raise

    return database_schema


def list_available_databases(engine: Engine) -> List[str]:
    """List all available databases/schemas."""
    db_type = engine.dialect.name

    try:
        with engine.connect() as conn:
            if db_type == "mysql":
                result = conn.execute(text("SHOW DATABASES"))
                return [
                    row[0]
                    for row in result.fetchall()
                    if row[0]
                    not in ["information_schema", "performance_schema", "mysql", "sys"]
                ]
            elif db_type == "postgresql":
                result = conn.execute(
                    text("SELECT datname FROM pg_database WHERE datistemplate = false")
                )
                return [row[0] for row in result.fetchall()]
            elif db_type == "sqlite":
                return ["main"]  # SQLite has one database
            else:
                inspector = inspect(engine)
                return inspector.get_schema_names()
    except SQLAlchemyError:
        return []


def main():
    script_name = sys.argv[0].split("/")[-1]  # Get just the filename

    parser = argparse.ArgumentParser(
        description="Extract SQL database schemas to JSON using SQLAlchemy",
        epilog=f"""
Examples:
  {script_name} 'mysql://user:pass@host:port/db' database1 database2
  {script_name} 'mysql://user:pass@host:port/db' --list-databases
  {script_name} 'postgresql://user:pass@host:port/db' mydb
  {script_name} 'sqlite:///path/to/database.db' main

IBM Cloud MySQL:
  {script_name} 'mysql://user:pass@host.databases.appdomain.cloud:31618/db?sslMode=VERIFY_IDENTITY' dbname

Supported databases: MySQL, PostgreSQL, SQLite, SQL Server, Oracle
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "connection_string",
        help="Database connection string (e.g., mysql://user:pass@host:port/db)",
    )
    parser.add_argument(
        "databases",
        nargs="*",
        help="List of database names to extract (if empty, will list available databases)",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="schema.json",
        help="Output file name (default: schema.json)",
    )
    parser.add_argument(
        "--list-databases",
        action="store_true",
        help="List available databases and exit",
    )

    args = parser.parse_args()

    print(f"Connecting to database server...")
    engine = get_database_engine(args.connection_string)

    # If requested, list databases and exit
    if args.list_databases or not args.databases:
        print("Available databases:")
        databases = list_available_databases(engine)
        for db in databases:
            print(f"  - {db}")

        if not args.databases:
            print("\nPlease specify which databases to extract.")
            sys.exit(0)
        return

    schema_data = {}

    for database_name in args.databases:
        print(f"Extracting schema for database: {database_name}")
        try:
            database_schema = extract_database_schema(engine, database_name)
            schema_data[database_name] = database_schema
        except Exception as err:
            print(f"Error processing database {database_name}: {err}")
            continue

    engine.dispose()

    # Write to JSON file
    print(f"Writing schema to {args.output}...")
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(schema_data, f, indent=2, ensure_ascii=False)

    print(f"Schema extraction complete! Output saved to {args.output}")


if __name__ == "__main__":
    main()
