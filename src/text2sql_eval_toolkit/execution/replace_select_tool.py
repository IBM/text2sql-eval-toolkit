#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from text2sql_eval_toolkit.logging import get_logger
from text2sql_eval_toolkit.metrics.text2sql_utils import replace_select_clause
from text2sql_eval_toolkit.utils import get_gt_sqls

logger = get_logger(__name__)


def clean_sql(s: str | None) -> str | None:
    """Strip code fences and trailing semicolons to normalize for comparison."""
    if s is None:
        return None
    t = s.strip()
    if t.lower().startswith("```sql"):
        t = t[6:].lstrip("`").strip()
    if t.startswith("```") and t.endswith("```"):
        t = t[3:-3].strip()
    return t.rstrip(";\n\r\t ")


def get_gt_sql(record: Dict[str, Any]) -> str | None:
    gt_sqls = get_gt_sqls(record)
    return gt_sqls[0]


def replace_select_for_logic_ex_data(
    data: List[Dict[str, Any]], db_engine: Dict[str, Any]
) -> List[Dict[str, Any]]:
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
        preds = record.get("predictions") or {}
        for _, pred in preds.items():
            original_pred_sql = clean_sql(pred.get("predicted_sql"))
            if not original_pred_sql:
                continue
            total_predictions += 1
            try:
                revised_sql_raw = replace_select_clause(
                    gt_sql, original_pred_sql, dialect
                )
            except Exception as e:
                logger.error(f"Error replacing select clause: {repr(e)}")
                continue
            revised_sql = clean_sql(revised_sql_raw)
            if revised_sql and revised_sql != original_pred_sql:
                pred["logic_sql"] = revised_sql
                modified_count += 1

    logger.info(
        f"[replace_select_clause] processed {total_predictions} predictions; "
        f"updated {modified_count} with logic_sql."
    )
    return data


def replace_select_for_logic_ex(
    predictions_path: str | Path, db_engine: Dict[str, Any]
) -> None:
    predictions_path = Path(predictions_path)
    if not predictions_path.exists():
        raise FileNotFoundError(f"No such file: {predictions_path}")
    with predictions_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    data = replace_select_for_logic_ex_data(data, db_engine)
    with predictions_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
