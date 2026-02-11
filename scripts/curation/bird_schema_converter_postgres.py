import argparse
import os
import json
import logging
import psycopg2

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

logger = logging.getLogger(__name__)


def get_primary_keys(cur, table):
    """
    Gets the primary keys for a given table.
    Args:
        cur: Postgres cursor.
        table (str): Table name.
    Returns:
        set: Set of primary key column names.
    """
    logger.debug(f"Fetching primary keys for table: {table}")
    cur.execute(
        """
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
        WHERE tc.table_name = %s
          AND tc.constraint_type = 'PRIMARY KEY';
        """,
        (table,),
    )
    return {row[0] for row in cur.fetchall()}


def get_foreign_keys_by_column(cur, table):
    """
    Gets the foreign keys for a given table, organized by column.
    Args:
        cur: Postgres cursor.
        table (str): Table name.
    Returns:
        dict: Mapping from column names to list of foreign key targets.
    """
    logger.debug(f"Fetching foreign keys for table: {table}")
    cur.execute(
        """
        SELECT
            kcu.column_name,
            ccu.table_name AS foreign_table,
            ccu.column_name AS foreign_column
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage ccu
            ON ccu.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND kcu.table_name = %s;
        """,
        (table,),
    )

    fk_map = {}
    for col, ft, fc in cur.fetchall():
        fk_map.setdefault(col, []).append(
            {
                "target_table": ft,
                "target_column": fc,
            }
        )

    return fk_map


def get_columns(cur, table):
    """
    Get column metadata for a given table.
    Args:
        cur: Postgres cursor.
        table (str): Table name.
    Returns:
        list: List of column metadata dictionaries.
    """
    logger.debug(f"Processing columns for table: {table}")

    primary_keys = get_primary_keys(cur, table)
    foreign_keys_map = get_foreign_keys_by_column(cur, table)

    cur.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position;
        """,
        (table,),
    )

    columns = []
    for column_name, data_type in cur.fetchall():
        logger.debug(f"Sampling values for column: {table}.{column_name}")
        cur.execute(
            f'SELECT "{column_name}" FROM "{table}" '
            f'WHERE "{column_name}" IS NOT NULL LIMIT 5;'
        )

        samples = [str(row[0]) for row in cur.fetchall()]

        columns.append(
            {
                "name": column_name,
                "type": data_type.upper(),
                "primary_key": column_name in primary_keys,
                "foreign_keys": foreign_keys_map.get(column_name, []),
                "description": "",
                "value_samples": samples,
            }
        )

    return columns


def get_tables(cur):
    """
    Get all table names in the public schema.
    Args:
        cur: Postgres cursor.
    Returns:
        list: List of table names.
    """
    logger.debug("Fetching public tables from Postgres")
    cur.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
    )
    return sorted(t for (t,) in cur.fetchall())


def fill_table_info(cur, table_name):
    """
    Fill in table info for a single table.
    Args:
        cur: Postgres cursor.
        table_name (str): Name of the table.
    Returns:
        dict: Table information with columns and metadata.
    """

    logger.debug(f"Building schema for table: {table_name}")
    return {
        "name": table_name,
        "columns": get_columns(cur, table_name),
        "description": "",
        "table_str": "",
    }


def get_bird_pg_schema_dict(dev_tables_json, pg_conn_str=None):
    """
    Generate a schema dictionary for BIRD Postgres databases.

    Args:
        dev_tables_json (list): List of database schema information from dev_tables.json.
        pg_conn_str (str, optional): Postgres connection string. If None, uses the
            POSTGRES_CONNECTION_STRING environment variable.
    Returns:
        dict: Schema dictionary with table and column metadata.
    """

    logger.info("Connecting to Postgres database")
    conn = psycopg2.connect(pg_conn_str)
    cur = conn.cursor()

    pg_tables = get_tables(cur)
    pg_tables_lower = {t.lower(): t for t in pg_tables}

    schema = {}

    for db_info in dev_tables_json:
        db_id = db_info["db_id"]
        logger.debug(f"Processing database: {db_id}")

        schema[db_id] = {"name": db_id, "tables": {}}

        for table in db_info["table_names_original"]:
            if table not in pg_tables:
                table_lower = table.lower()
                if table_lower in pg_tables_lower:
                    resolved = pg_tables_lower[table_lower]
                    logger.warning(
                        f"Table '{table}' not found, using case-insensitive match '{resolved}'"
                    )
                    table = resolved
                else:
                    logger.warning(f"Table '{table}' not found in Postgres, skipping")
                    continue

            schema[db_id]["tables"][table] = fill_table_info(cur, table)

    conn.close()
    logger.info("Postgres connection closed")
    return schema


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Export Postgres schema with column-level PK/FK metadata. Requires BIRD db Postgres server running."
    )
    parser.add_argument(
        "--dev_tables_filepath",
        help="Path to dev_tables.json",
        type=str,
        default=os.path.join("minidev", "MINIDEV", "dev_tables.json"),
    )
    parser.add_argument(
        "--output_filepath",
        help="Output JSON path",
        type=str,
        default="bird_mini_dev_postgres-schema.json",
    )
    parser.add_argument(
        "--pg_conn_str",
        help="Postgres connection string (overrides POSTGRES_CONNECTION_STRING env var)",
        type=str,
        default=None,
    )
    args = parser.parse_args()
    logger.setLevel("INFO")

    logger.info("Starting schema export")

    with open(args.dev_tables_filepath, "r") as f:
        dev_tables_json = json.load(f)

    if args.pg_conn_str is None:
        pg_conn_str = os.getenv("POSTGRES_CONNECTION_STRING")
        if not pg_conn_str:
            logger.error("POSTGRES_CONNECTION_STRING is not set")
            raise RuntimeError("Missing POSTGRES_CONNECTION_STRING")
        else:
            pg_conn_str = args.pg_conn_str
            logger.info(
                f"Using Postgres connection string from environment variable: {pg_conn_str}"
            )

    schema = get_bird_pg_schema_dict(dev_tables_json, pg_conn_str)

    with open(args.output_filepath, "w") as f:
        json.dump(schema, f, indent=2)

    logger.info(f"Schema written to {args.output_filepath}")
