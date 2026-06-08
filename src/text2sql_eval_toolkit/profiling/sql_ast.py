#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""Serialize sqlglot AST nodes into JSON-friendly trees for the dashboard."""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, Union

from sqlglot import parse_one
from sqlglot.expressions import Expression
from sqlglot.optimizer import optimize

from text2sql_eval_toolkit.profiling.profiling_tools import analyze_sql_query

ParseMode = Literal["sqlglot", "sqlglot_optimized"]
PARSE_MODES: tuple[ParseMode, ...] = ("sqlglot", "sqlglot_optimized")

MAX_TREE_DEPTH = 40
MAX_LIST_ITEMS = 64
MAX_LEAF_SQL_LEN = 120


def benchmark_db_type_to_dialect(db_type: str) -> str:
    """Map benchmark ``db_engine.db_type`` to a sqlglot dialect name."""
    normalized = (db_type or "").strip().lower()
    mapping = {
        "sqlite": "sqlite",
        "postgres": "postgres",
        "mysql": "mysql",
        "presto": "presto",
        "db2": "db2",
    }
    return mapping.get(normalized, "postgres")


def _truncate_str(value: str, limit: int = MAX_LEAF_SQL_LEN) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 1] + "…"


def expression_to_tree(
    node: Any,
    *,
    depth: int = 0,
    max_depth: int = MAX_TREE_DEPTH,
) -> Dict[str, Any]:
    """Convert a sqlglot ``Expression`` (or primitive arg) into a nested dict."""
    if depth >= max_depth:
        return {
            "class": type(node).__name__ if node is not None else "?",
            "truncated": True,
        }

    if not isinstance(node, Expression):
        return {"value": _truncate_str(repr(node))}

    out: Dict[str, Any] = {
        "class": node.__class__.__name__,
        "key": node.key,
    }

    child_groups: List[Dict[str, Any]] = []
    for arg_key, arg_val in node.args.items():
        if arg_val is None:
            continue
        if isinstance(arg_val, list):
            nodes: List[Dict[str, Any]] = []
            for item in arg_val[:MAX_LIST_ITEMS]:
                nodes.append(expression_to_tree(item, depth=depth + 1, max_depth=max_depth))
            if len(arg_val) > MAX_LIST_ITEMS:
                nodes.append(
                    {
                        "truncated": True,
                        "message": f"{len(arg_val) - MAX_LIST_ITEMS} more item(s) omitted",
                    }
                )
            child_groups.append({"name": arg_key, "nodes": nodes})
        elif isinstance(arg_val, Expression):
            child_groups.append(
                {
                    "name": arg_key,
                    "nodes": [
                        expression_to_tree(arg_val, depth=depth + 1, max_depth=max_depth)
                    ],
                }
            )
        else:
            child_groups.append({"name": arg_key, "value": _truncate_str(repr(arg_val))})

    if child_groups:
        out["children"] = child_groups
    else:
        try:
            out["sql"] = _truncate_str(node.sql())
        except Exception:
            pass

    return out


def expression_to_visual_tree(
    node: Any,
    *,
    depth: int = 0,
    max_depth: int = MAX_TREE_DEPTH,
) -> Dict[str, Any]:
    """
    Flatten sqlglot nodes into a UI-friendly tree: label, meta, detail, children[].
    """
    if depth >= max_depth:
        return {
            "label": type(node).__name__ if node is not None else "?",
            "detail": "depth limit",
            "children": [],
        }

    if not isinstance(node, Expression):
        return {
            "label": "value",
            "detail": _truncate_str(repr(node)),
            "children": [],
        }

    children: List[Dict[str, Any]] = []
    for arg_key, arg_val in node.args.items():
        if arg_val is None:
            continue
        if isinstance(arg_val, list):
            for idx, item in enumerate(arg_val[:MAX_LIST_ITEMS]):
                child = expression_to_visual_tree(
                    item, depth=depth + 1, max_depth=max_depth
                )
                child["edge"] = f"{arg_key}[{idx}]"
                children.append(child)
            if len(arg_val) > MAX_LIST_ITEMS:
                children.append(
                    {
                        "label": "…",
                        "detail": f"{len(arg_val) - MAX_LIST_ITEMS} more",
                        "children": [],
                    }
                )
        elif isinstance(arg_val, Expression):
            child = expression_to_visual_tree(
                arg_val, depth=depth + 1, max_depth=max_depth
            )
            child["edge"] = arg_key
            children.append(child)
        else:
            children.append(
                {
                    "label": arg_key,
                    "detail": _truncate_str(repr(arg_val)),
                    "children": [],
                }
            )

    out: Dict[str, Any] = {
        "label": node.__class__.__name__,
        "meta": node.key,
        "children": children,
    }
    if not children:
        try:
            out["detail"] = _truncate_str(node.sql())
        except Exception:
            pass
    return out


def parse_sql_expression(
    sql: str,
    dialect: str,
    mode: ParseMode = "sqlglot",
) -> Expression:
    """Parse SQL and optionally run the sqlglot optimizer (same as equivalence metrics)."""
    parsed = parse_one(sql, dialect=dialect)
    if mode == "sqlglot_optimized":
        return optimize(parsed, dialect=dialect)
    return parsed


def parse_sql_to_tree(
    sql: str,
    dialect: str = "postgres",
    *,
    mode: ParseMode = "sqlglot",
    include_analysis: bool = True,
) -> Dict[str, Any]:
    """
    Parse SQL with sqlglot and return a JSON-serializable AST tree.

    Returns a dict with ``ok``; on success also ``tree``, ``formatted_sql``,
    ``dialect``, and optionally profiling ``analysis``.
    """
    text = (sql or "").strip()
    if not text:
        return {"ok": False, "error": "SQL is empty"}

    if mode not in PARSE_MODES:
        return {
            "ok": False,
            "error": f"parse_mode must be one of: {', '.join(PARSE_MODES)}",
            "dialect": dialect,
        }

    try:
        parsed = parse_sql_expression(text, dialect, mode=mode)
    except Exception as exc:
        return {"ok": False, "error": str(exc), "dialect": dialect, "parse_mode": mode}

    result: Dict[str, Any] = {
        "ok": True,
        "dialect": dialect,
        "parse_mode": mode,
        "tree": expression_to_tree(parsed),
        "visual_tree": expression_to_visual_tree(parsed),
        "formatted_sql": parsed.sql(pretty=True),
    }
    if include_analysis:
        try:
            result["analysis"] = analyze_sql_query(text, dialect=dialect)
        except Exception as exc:
            result["analysis_error"] = str(exc)
    return result
