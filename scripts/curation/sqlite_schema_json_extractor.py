#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

import os
import sqlite3
import argparse
import json
from collections import defaultdict


def collect_value_samples(cursor, table_name, column_name):
    try:
        query = f'''
            SELECT DISTINCT "{column_name}"
            FROM "{table_name}"
            WHERE "{column_name}" IS NOT NULL
            LIMIT 5
        '''
        cursor.execute(query)
        return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        print(f"  ⚠ Failed to sample values from {table_name}.{column_name}: {e}")
        return []


def extract_schema_from_db(db_id, db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    tables = {}
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    table_names = [row[0] for row in cursor.fetchall()]

    for table in table_names:
        cursor.execute(f'PRAGMA table_info("{table}")')
        cols = cursor.fetchall()

        primary_keys = set()
        column_defs = []
        for col in cols:
            col_id, col_name, col_type, _, _, pk = col
            if pk != 0:
                primary_keys.add(col_name)
            samples = collect_value_samples(cursor, table, col_name)
            column_defs.append(
                {
                    "name": col_name,
                    "type": (col_type or "TEXT").upper(),
                    "primary_key": pk != 0,
                    "foreign_keys": [],
                    "description": "",
                    "value_samples": samples,
                }
            )

        cursor.execute(f'PRAGMA foreign_key_list("{table}")')
        fks = cursor.fetchall()
        fk_map = defaultdict(list)
        for fk in fks:
            _, _, ref_table, from_col, to_col, *_ = fk
            fk_map[from_col].append(
                {"target_table": ref_table, "target_column": to_col}
            )

        for col in column_defs:
            col["foreign_keys"] = fk_map.get(col["name"], [])

        tables[table] = {
            "name": table,
            "columns": column_defs,
            "description": "",
            "table_str": "",
        }

    conn.close()

    return {db_id: {"name": db_id, "tables": tables}}


def main():
    parser = argparse.ArgumentParser(
        description="Extract schema (with value samples) from SQLite databases in a folder and output as structured JSON."
    )
    parser.add_argument("output_file", help="Path to the output JSON file")
    parser.add_argument(
        "--db-root",
        default="data/benchmarks/dbs/bird/dev_databases",
        help="Root folder containing DBs (default: %(default)s)",
    )

    args = parser.parse_args()

    output = {}

    for db_id in sorted(os.listdir(args.db_root)):
        db_dir = os.path.join(args.db_root, db_id)
        db_file = os.path.join(db_dir, f"{db_id}.sqlite")
        if not os.path.isfile(db_file):
            print(f"⚠ Skipping {db_id}: no DB file at {db_file}")
            continue
        print(f"🔍 Extracting schema from {db_file}")
        try:
            schema = extract_schema_from_db(db_id, db_file)
            output.update(schema)
        except Exception as e:
            print(f"❌ Failed to process {db_id}: {e}")

    with open(args.output_file, "w") as f:
        json.dump(output, f, indent=2)
        print(f"\n✅ Schema with value samples written to {args.output_file}")


if __name__ == "__main__":
    main()
