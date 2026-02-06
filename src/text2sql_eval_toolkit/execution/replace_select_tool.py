#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict
from unitxt.text2sql_utils import replace_select_clause
from text2sql_eval_toolkit.utils import get_gt_sqls
from text2sql_eval_toolkit.logging import get_logger

logger = get_logger(__name__)


def clean_sql(s: str | None) -> str | None:
    """Strip code fences and trailing semicolons to normalize for comparison."""
    if s is None:
        return None
    t = s.strip()
    # remove ```sql ... ``` or ``` ... ``` fences if present
    if t.lower().startswith("```sql"):
        t = t[6:].lstrip("`").strip()  # drop the leading ```sql
    if t.startswith("```") and t.endswith("```"):
        t = t[3:-3].strip()
    # strip trailing semicolons and whitespace
    t = t.rstrip(";\n\r\t ")
    return t


def get_gt_sql(record: Dict[str, Any]) -> str | None:
    """Get the ground truth SQL from the record (first gt if multiple)"""
    gt_sqls = get_gt_sqls(record)
    return gt_sqls[0]


def replace_select_for_logic_ex(
    predictions_path: str | Path, db_engine: Dict[str, Any]
) -> None:
    """
    Iterate over predictions in a JSON file, and for each prediction record's
    'predicted_sql', call the unitxt `replace_select_clause(gt_sql, predicted_sql, dialect)`
    function. If the returned SQL differs, set 'predicted_sql_revised' on that prediction.

    The file is updated in-place; a '.bak' backup is created alongside it.

    Expected db_engine example:
        {"db_type": "postgres" | "sqlite" | "db2" | "mysql" | "presto", ...}
    """
    predictions_path = Path(predictions_path)
    if not predictions_path.exists():
        raise FileNotFoundError(f"No such file: {predictions_path}")

    with predictions_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    dialect = db_engine.get("db_type")
    if dialect not in {"postgres", "sqlite", "db2", "mysql", "presto"}:
        raise NotImplementedError(f"Unsupported DB type '{dialect}'.")
    if dialect == "db2":
        dialect = "postgres"

    modified_count = 0
    total_predictions = 0

    for record in data:
        gt_sql_raw = get_gt_sql(record)
        gt_sql = clean_sql(gt_sql_raw)
        preds = record.get("predictions")

        for _, pred in preds.items():
            original_pred_sql_raw = pred.get("predicted_sql")
            original_pred_sql = clean_sql(original_pred_sql_raw)
            if not original_pred_sql:
                continue

            total_predictions += 1

            try:
                revised_sql_raw = replace_select_clause(
                    gt_sql, original_pred_sql, dialect
                )
            except Exception as e:
                # pred.setdefault("errors", {})
                # pred["errors"]["replace_select_clause"] = f"{type(e).__name__}: {e}"
                logger.error(f"Error replacing select clause: {repr(e)}")
                continue

            revised_sql = clean_sql(revised_sql_raw)
            if revised_sql and revised_sql != original_pred_sql:
                # Only set if actually changed
                pred["logic_sql"] = revised_sql
                modified_count += 1

    # Write back with a backup
    # backup_path = predictions_path.with_suffix(predictions_path.suffix + ".bak")
    # if not backup_path.exists():
    #     predictions_path.replace(backup_path)
    # else:
    #     with (
    #         predictions_path.open("r", encoding="utf-8") as src,
    #         backup_path.open("w", encoding="utf-8") as dst,
    #     ):
    #         dst.write(src.read())

    with predictions_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logger.info(
        f"[replace_select_clause] processed {total_predictions} predictions; "
        f"updated {modified_count} with 'predicted_sql_revised'."
    )
