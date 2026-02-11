#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

import json
import argparse
import os
import sqlite3
import sys
from collections import defaultdict


def get_table_columns(cursor, table_name):
    try:
        cursor.execute(f'PRAGMA table_info("{table_name}")')
        return [row[1] for row in cursor.fetchall()]
    except Exception:
        return []


def collect_value_samples(cursor, table_name, column_name):
    try:
        query = (
            f'SELECT DISTINCT "{column_name}" '
            f'FROM "{table_name}" '
            f'WHERE "{column_name}" IS NOT NULL LIMIT 10'
        )
        cursor.execute(query)
        return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        return []


def transform_schema(schema_obj, db_root=None, verbose=False):
    db_id = schema_obj["db_id"]
    col_types = schema_obj["column_types"]
    tbl_names = schema_obj["table_names"]
    primary_keys = schema_obj["primary_keys"]
    foreign_keys = schema_obj["foreign_keys"]

    pk_set = {
        pk
        for pk_group in primary_keys
        for pk in ([pk_group] if not isinstance(pk_group, list) else pk_group)
    }

    # build column index -> table + type map from input
    input_column_meta = defaultdict(list)
    for idx, (tbl_idx, _) in enumerate(schema_obj["column_names"]):
        if tbl_idx == -1:
            continue
        table_name = tbl_names[tbl_idx]
        input_column_meta[table_name].append((idx, col_types[idx].upper()))

    fk_map = defaultdict(list)
    col_names_raw = schema_obj["column_names"]
    tbl_names_raw = schema_obj["table_names"]
    for src_idx, tgt_idx in foreign_keys:
        src_tbl_idx, _ = col_names_raw[src_idx]
        tgt_tbl_idx, tgt_col_name = col_names_raw[tgt_idx]
        if src_tbl_idx == -1 or tgt_tbl_idx == -1:
            continue
        src_tbl = tbl_names_raw[src_tbl_idx]
        tgt_tbl = tbl_names_raw[tgt_tbl_idx]
        fk_map[src_idx].append({"target_table": tgt_tbl, "target_column": tgt_col_name})

    db_path = os.path.join(db_root, db_id, f"{db_id}.sqlite") if db_root else None
    has_db = db_path and os.path.exists(db_path)
    if db_path and not has_db:
        print(f"⚠  DB not found for {db_id}: {db_path} — skipping")
        return {}

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    tables = {}
    for table_name in tbl_names:
        actual_columns = get_table_columns(cursor, table_name)
        if not actual_columns:
            continue

        columns_out = []
        for col_name in actual_columns:
            idx_type_pairs = input_column_meta.get(table_name, [])
            matching_idx = None
            col_type = "TEXT"
            for idx, typ in idx_type_pairs:
                if (
                    col_name.lower()
                    == schema_obj["column_names"][idx][1].lower().replace(" ", "_")
                    or col_name.lower()
                    == schema_obj["column_names"][idx][1].lower().replace(" ", "")
                    or col_name.lower() == schema_obj["column_names"][idx][1].lower()
                ):
                    matching_idx = idx
                    col_type = typ
                    break

            if matching_idx is None:
                print(f"  ⚠ Skipping unknown column in DB: {table_name}.{col_name}")
                continue

            samples = collect_value_samples(cursor, table_name, col_name)
            if verbose:
                print(f"  ✔ {table_name}.{col_name}: {len(samples)} sample(s)")

            columns_out.append(
                {
                    "name": col_name,
                    "type": col_type,
                    "primary_key": matching_idx in pk_set,
                    "foreign_keys": fk_map.get(matching_idx, []),
                    "description": "",
                    "value_samples": samples,
                }
            )

        tables[table_name] = {
            "name": table_name,
            "columns": columns_out,
            "description": "",
            "table_str": "",
        }

    conn.close()
    return {db_id: {"name": db_id, "tables": tables}}


def main():
    parser = argparse.ArgumentParser(
        description="Convert BIRD-style schema JSON to structured format using actual SQLite DB column names and optional value samples."
    )
    parser.add_argument("input_file", help="Input JSON in BIRD schema format")
    parser.add_argument("output_file", help="Output JSON path")
    parser.add_argument(
        "--db-root",
        default="data/benchmarks/dbs/bird/dev_databases",
        help="Root directory where DB folders live (default: %(default)s)",
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Print sample counts for each column"
    )

    args = parser.parse_args()

    with open(args.input_file, "r") as fp:
        raw = json.load(fp)
    raw = raw if isinstance(raw, list) else [raw]

    final = {}

    for schema in raw:
        print(f"\n🔄 Processing {schema['db_id']}")
        db_struct = transform_schema(schema, db_root=args.db_root, verbose=args.verbose)
        final.update(db_struct)

    with open(args.output_file, "w") as fp:
        json.dump(final, fp, indent=2)
        print(f"\n✅ Output written to {args.output_file}")


if __name__ == "__main__":
    main()
