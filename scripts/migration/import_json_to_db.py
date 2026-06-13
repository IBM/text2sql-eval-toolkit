#!/usr/bin/env python3
#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
"""Import legacy JSON benchmark artifacts into the SQLite database schema."""

from __future__ import annotations

import argparse
import importlib.util
import logging
import os
import sqlite3
import sys
from pathlib import Path


def _load_migration_module(module_name: str, filename: str):
    module_dir = Path(__file__).resolve().parents[2] / "src" / "text2sql_eval_toolkit" / "database"
    module_path = module_dir / filename
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load migration module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


_connection = _load_migration_module("t2s_db_connection", "connection.py")
_json_importer = _load_migration_module("t2s_json_importer", "json_importer.py")

apply_schema = _connection.apply_schema
connect = _connection.connect
resolve_database_path = _connection.resolve_database_path
resolve_schema_path = _connection.resolve_schema_path
JsonToDbImporter = _json_importer.JsonToDbImporter
default_data_root = _json_importer.default_data_root

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Migrate legacy JSON benchmark data (benchmarks.json, benchmark records, "
            "predictions, eval results, and summaries) into text2sql_eval.db."
        )
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=None,
        help="Path to the data directory (default: repo data/ or TEXT2SQL_DATA_ROOT).",
    )
    parser.add_argument(
        "--database",
        type=Path,
        default=None,
        help="SQLite database file path (overrides TEXT2SQL_DATABASE_URL).",
    )
    parser.add_argument(
        "--init",
        action="store_true",
        help="Apply data/database-schema/schema.sql before importing.",
    )
    parser.add_argument(
        "--benchmark-id",
        action="append",
        dest="benchmark_ids",
        default=None,
        help="Import only the given benchmark id (repeatable). Default: all benchmarks.",
    )
    parser.add_argument(
        "--production-only",
        action="store_true",
        help="Skip test benchmarks from test-benchmarks.json.",
    )
    parser.add_argument(
        "--skip-predictions",
        action="store_true",
        help="Import catalog and gold records only.",
    )
    parser.add_argument(
        "--skip-eval",
        action="store_true",
        help="Import predictions without evaluation metrics.",
    )
    parser.add_argument(
        "--skip-summaries",
        action="store_true",
        help="Do not import *-predictions_eval_summary.json files.",
    )
    parser.add_argument(
        "--compute-category-summaries",
        action="store_true",
        help="Recompute per-category eval_summaries from imported evaluations.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Delete existing rows for each benchmark before re-importing.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = build_parser()
    args = parser.parse_args(argv)

    data_root = (args.data_root or default_data_root()).resolve()
    if not data_root.is_dir():
        logger.error("Data root does not exist: %s", data_root)
        return 1

    db_path = args.database or resolve_database_path()
    schema_path = resolve_schema_path()

    os.environ["TEXT2SQL_DATA_ROOT"] = str(data_root)
    os.environ["TEXT2SQL_EVAL_TOOLKIT_DATA_ROOT"] = str(data_root)

    logger.info("Data root: %s", data_root)
    logger.info("Database: %s", db_path)

    needs_schema = args.init or not db_path.exists()
    conn = connect(db_path)
    try:
        if needs_schema and not _database_has_schema(conn):
            logger.info("Applying schema from %s", schema_path)
            apply_schema(conn, schema_path)
            conn.commit()
        elif needs_schema:
            logger.warning(
                "Database already initialized; skipping schema apply. "
                "Use a fresh database file or delete schema_migrations to re-apply."
            )

        importer = JsonToDbImporter(conn=conn, data_root=data_root)
        stats = importer.import_all(
            benchmark_ids=args.benchmark_ids,
            include_test=not args.production_only,
            skip_predictions=args.skip_predictions,
            skip_eval=args.skip_eval,
            skip_summaries=args.skip_summaries,
            compute_category_summaries=args.compute_category_summaries,
            force=args.force,
        )
    finally:
        conn.close()

    logger.info(
        "Import complete: benchmarks=%s records=%s predictions=%s evaluations=%s dataframes=%s summaries=%s",
        stats.benchmarks,
        stats.records,
        stats.predictions,
        stats.evaluations,
        stats.dataframes,
        stats.summaries,
    )
    return 0


def _database_has_schema(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'"
    ).fetchone()
    return row is not None


if __name__ == "__main__":
    sys.exit(main())
