#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Curate the FIBEN benchmark into the text2sql-eval-toolkit SQLite database.

FIBEN (https://github.com/IBM/fiben-benchmark) is a financial natural-language
querying benchmark. The upstream repository ships two artifacts this script
consumes:

* ``FIBEN_Queries.json`` - 300 NL questions, each with a gold PostgreSQL/Db2
  query and a ``queryType`` label (non-nested / type-n / type-a / type-j /
  type-ja).
* ``FIBEN.sql`` - the DDL (``CREATE TABLE`` + ``ALTER TABLE ... ADD CONSTRAINT
  ... FOREIGN KEY``) for the single ``FIBEN`` schema the queries target.

The script converts those into the canonical toolkit artifacts and imports them
into ``data/text2sql_eval.db`` through the shared :class:`JsonToDbImporter`:

1. ``data/benchmarks/fiben.json``        - one record per NL question.
2. ``data/benchmarks/fiben-schema.json`` - ``{db_id: {tables: {...}}}`` map.
3. a ``fiben`` entry in ``data/benchmarks.json`` - the registry config.
4. rows in ``benchmarks`` / ``benchmark_db_config`` /
   ``benchmark_schema_snapshots`` / ``benchmark_records`` / ``record_gt_sql`` /
   ``record_categories``.

Usage:
    python scripts/curation/fiben_curation.py
    python scripts/curation/fiben_curation.py --dry-run      # write JSON, skip DB
    python scripts/curation/fiben_curation.py \\
        --fiben-dir raw_benchmarks/fiben-benchmark --data-root data
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BENCHMARK_ID = "fiben"
DB_ID = "FIBEN"
SCHEMA_NAME = "FIBEN"
DESCRIPTION = (
    "FIBEN financial natural-language querying benchmark "
    "(300 analytical/nested queries over a finance data mart) "
    "https://github.com/IBM/fiben-benchmark"
)

_CREATE_RE = re.compile(
    r"^CREATE\s+TABLE\s+(?P<name>\w+)\s*\((?P<body>.*)\)\s*$",
    re.IGNORECASE | re.DOTALL,
)
_FK_RE = re.compile(
    r"^ALTER\s+TABLE\s+(?P<table>\w+)\s+ADD\s+CONSTRAINT\s+\w+\s+"
    r"FOREIGN\s+KEY\s*\(\s*(?P<col>\w+)\s*\)\s+"
    r"REFERENCES\s+(?P<ftable>\w+)\s*\(\s*(?P<fcol>\w+)\s*\)",
    re.IGNORECASE | re.DOTALL,
)
_CONSTRAINT_PREFIXES = ("PRIMARY KEY", "FOREIGN KEY", "CONSTRAINT", "UNIQUE", "CHECK")


# --------------------------------------------------------------------------- #
# DDL parsing
# --------------------------------------------------------------------------- #
def _split_top_level(body: str) -> list[str]:
    """Split a CREATE TABLE body on commas that sit outside any parentheses."""
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    for char in body:
        if char == "(":
            depth += 1
            current.append(char)
        elif char == ")":
            depth -= 1
            current.append(char)
        elif char == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(char)
    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return parts


def parse_ddl(ddl_text: str) -> dict[str, dict[str, Any]]:
    """Parse FIBEN.sql into ``{table: {columns, pks, fks}}``.

    ``columns`` preserves declaration order; ``fks`` maps a column name to a
    list of ``{target_table, target_column}`` dicts.
    """
    tables: dict[str, dict[str, Any]] = {}

    for raw in ddl_text.split(";"):
        statement = raw.strip()
        if not statement:
            continue

        create = _CREATE_RE.match(statement)
        if create:
            name = create.group("name").upper()
            columns: list[dict[str, str]] = []
            pks: set[str] = set()
            for part in _split_top_level(create.group("body")):
                upper = part.upper()
                if upper.startswith("PRIMARY KEY"):
                    inner = part[part.index("(") + 1 : part.rindex(")")]
                    pks.update(col.strip().strip('"').upper() for col in inner.split(","))
                    continue
                if upper.startswith(_CONSTRAINT_PREFIXES):
                    continue
                tokens = part.split()
                if not tokens:
                    continue
                col_name = tokens[0].strip('"').upper()
                col_type = " ".join(tokens[1:])
                col_type = re.sub(r"\s+NOT\s+NULL\b", "", col_type, flags=re.IGNORECASE)
                col_type = re.sub(r"\s+DEFAULT\s+.*$", "", col_type, flags=re.IGNORECASE)
                columns.append({"name": col_name, "type": col_type.strip().upper()})
            tables[name] = {"columns": columns, "pks": pks, "fks": defaultdict(list)}
            continue

        fk = _FK_RE.match(statement)
        if fk:
            table = fk.group("table").upper()
            if table not in tables:
                logger.warning("FK references unknown table %s; skipping", table)
                continue
            tables[table]["fks"][fk.group("col").upper()].append(
                {
                    "target_table": fk.group("ftable").upper(),
                    "target_column": fk.group("fcol").upper(),
                }
            )
            continue

        logger.debug("Unrecognized DDL statement skipped: %s", statement[:80])

    return tables


def build_schema(tables: dict[str, dict[str, Any]], db_id: str = DB_ID) -> dict[str, Any]:
    """Convert parsed DDL into the toolkit schema-snapshot JSON format."""
    out_tables: dict[str, Any] = {}
    for table_name, info in sorted(tables.items()):
        columns = [
            {
                "name": column["name"],
                "type": column["type"],
                "primary_key": column["name"] in info["pks"],
                "foreign_keys": info["fks"].get(column["name"], []),
                "description": "",
                # No live DB here, so samples are left empty. They can be
                # backfilled later from a running Postgres instance using the
                # same shape as scripts/curation/bird_schema_converter_postgres.py.
                "value_samples": [],
            }
            for column in info["columns"]
        ]
        out_tables[table_name] = {
            "name": table_name,
            "columns": columns,
            "description": "",
            "table_str": "",
        }
    return {db_id: {"name": db_id, "tables": out_tables}}


# --------------------------------------------------------------------------- #
# Query parsing
# --------------------------------------------------------------------------- #
def build_records(queries: list[dict[str, Any]], db_id: str = DB_ID) -> list[dict[str, Any]]:
    """Convert FIBEN_Queries.json entries into canonical benchmark records."""
    records: list[dict[str, Any]] = []
    for index, query in enumerate(queries, start=1):
        sql = (query.get("SQL") or "").strip()
        question = (query.get("question") or "").strip()
        if not sql or not question:
            logger.warning("Skipping FIBEN entry %s with missing SQL/question", index)
            continue

        query_type = (query.get("queryType") or "").strip()
        normalized_type = query_type.lower()
        is_nested = bool(normalized_type) and normalized_type != "non-nested"

        categories = [f"querytype_{normalized_type.replace('-', '_')}"] if normalized_type else []
        categories.append("nested" if is_nested else "non_nested")
        if query.get("isParaphrased"):
            categories.append("paraphrased")

        records.append(
            {
                "id": f"fiben_{index:04d}",
                "db_id": db_id,
                "question": question,
                "sql": [sql],
                "difficulty": "nested" if is_nested else "non-nested",
                "meta": {"categories": categories},
                # Non-core keys land in benchmark_records.extra_metadata.
                "unique_query_id": query.get("uniqueQueryID"),
                "query_type": query_type,
                "is_paraphrased": bool(query.get("isParaphrased")),
            }
        )
    return records


# --------------------------------------------------------------------------- #
# Registry + DB import
# --------------------------------------------------------------------------- #
def update_registry(registry_path: Path) -> None:
    """Add (or refresh) the ``fiben`` entry in ``data/benchmarks.json``."""
    registry: dict[str, Any] = {}
    if registry_path.is_file():
        with open(registry_path, encoding="utf-8") as handle:
            registry = json.load(handle)

    registry[BENCHMARK_ID] = {
        "name": BENCHMARK_ID,
        "description": DESCRIPTION,
        "data": "benchmarks/fiben.json",
        "schema": "benchmarks/fiben-schema.json",
        "predictions": "results/fiben-predictions.json",
        "db_engine": {
            "db_type": "postgres",
            "db_folder": "",
            "schema_name": SCHEMA_NAME,
            "connection_string_env_var": "POSTGRES_CONNECTION_STRING",
        },
    }

    with open(registry_path, "w", encoding="utf-8") as handle:
        json.dump(registry, handle, indent=4, ensure_ascii=False)
        handle.write("\n")


def import_into_db(data_root: Path) -> None:
    """Import the freshly written fiben artifacts into data/text2sql_eval.db."""
    try:
        from text2sql_eval_toolkit.database.connection import connect, resolve_database_path
        from text2sql_eval_toolkit.database.session import ensure_schema
        from text2sql_eval_toolkit.database.json_importer import JsonToDbImporter
    except ModuleNotFoundError:  # pragma: no cover - fallback for un-installed pkg
        sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
        from text2sql_eval_toolkit.database.connection import connect, resolve_database_path
        from text2sql_eval_toolkit.database.session import ensure_schema
        from text2sql_eval_toolkit.database.json_importer import JsonToDbImporter

    ensure_schema()
    db_path = resolve_database_path()
    logger.info("Importing into %s", db_path)

    conn = connect(db_path)
    try:
        importer = JsonToDbImporter(conn=conn, data_root=data_root)
        stats = importer.import_all(
            benchmark_ids=[BENCHMARK_ID],
            skip_predictions=True,
            skip_eval=True,
            skip_summaries=True,
            force=True,
        )
    finally:
        conn.close()
    logger.info(
        "Imported benchmark=%d records=%d into the database",
        stats.benchmarks,
        stats.records,
    )


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #
def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--fiben-dir",
        type=Path,
        default=Path("raw_benchmarks/fiben-benchmark"),
        help="Directory containing FIBEN_Queries.json and FIBEN.sql (default: %(default)s).",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path("data"),
        help="Toolkit data root holding benchmarks.json and benchmarks/ (default: %(default)s).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Write the JSON artifacts and registry entry but skip the DB import.",
    )
    args = parser.parse_args()

    fiben_dir: Path = args.fiben_dir
    data_root: Path = args.data_root
    queries_path = fiben_dir / "FIBEN_Queries.json"
    ddl_path = fiben_dir / "FIBEN.sql"
    for path in (queries_path, ddl_path):
        if not path.is_file():
            parser.error(f"Required FIBEN file not found: {path}")

    with open(queries_path, encoding="utf-8") as handle:
        queries = json.load(handle)
    records = build_records(queries)
    type_counts = Counter(record["query_type"] for record in records)
    logger.info("Parsed %d FIBEN queries: %s", len(records), dict(type_counts))

    tables = parse_ddl(ddl_path.read_text(encoding="utf-8"))
    schema = build_schema(tables)
    fk_total = sum(len(fks) for info in tables.values() for fks in info["fks"].values())
    logger.info("Parsed FIBEN DDL: %d tables, %d foreign keys", len(tables), fk_total)

    benchmarks_dir = data_root / "benchmarks"
    benchmarks_dir.mkdir(parents=True, exist_ok=True)
    data_out = benchmarks_dir / "fiben.json"
    schema_out = benchmarks_dir / "fiben-schema.json"

    with open(data_out, "w", encoding="utf-8") as handle:
        json.dump(records, handle, indent=2, ensure_ascii=False)
        handle.write("\n")
    with open(schema_out, "w", encoding="utf-8") as handle:
        json.dump(schema, handle, indent=2, ensure_ascii=False)
        handle.write("\n")
    logger.info("Wrote %s and %s", data_out, schema_out)

    update_registry(data_root / "benchmarks.json")
    logger.info("Registered '%s' in %s", BENCHMARK_ID, data_root / "benchmarks.json")

    if args.dry_run:
        logger.info("Dry run: skipping database import.")
        return

    import_into_db(data_root.resolve())
    logger.info("Done.")


if __name__ == "__main__":
    main()
