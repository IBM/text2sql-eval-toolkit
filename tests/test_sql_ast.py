#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

from text2sql_eval_toolkit.profiling.sql_ast import (
    benchmark_db_type_to_dialect,
    expression_to_tree,
    expression_to_visual_tree,
    parse_sql_to_tree,
)
from sqlglot import parse_one


def test_benchmark_db_type_to_dialect():
    assert benchmark_db_type_to_dialect("sqlite") == "sqlite"
    assert benchmark_db_type_to_dialect("POSTGRES") == "postgres"
    assert benchmark_db_type_to_dialect("") == "postgres"


def test_parse_sql_to_tree_success():
    result = parse_sql_to_tree("SELECT a FROM t", dialect="postgres")
    assert result["ok"] is True
    assert result["tree"]["class"] == "Select"
    assert "analysis" in result
    assert "single_source_basic" in result["analysis"]["categories"]


def test_parse_sql_to_tree_empty():
    result = parse_sql_to_tree("   ")
    assert result["ok"] is False
    assert "empty" in result["error"].lower()


def test_parse_sql_to_tree_invalid():
    result = parse_sql_to_tree("SELECT FROM", dialect="postgres")
    assert result["ok"] is False
    assert result["error"]


def test_expression_to_tree_join():
    parsed = parse_one(
        "SELECT a FROM t JOIN u ON t.id = u.id WHERE x > 1",
        dialect="postgres",
    )
    tree = expression_to_tree(parsed)
    assert tree["class"] == "Select"
    assert any(c["name"] == "joins" for c in tree.get("children", []))


def test_parse_sql_optimized_mode():
    sql = "SELECT a FROM t WHERE x > 1"
    raw = parse_sql_to_tree(sql, dialect="postgres", mode="sqlglot")
    opt = parse_sql_to_tree(sql, dialect="postgres", mode="sqlglot_optimized")
    assert raw["ok"] and opt["ok"]
    assert raw["parse_mode"] == "sqlglot"
    assert opt["parse_mode"] == "sqlglot_optimized"
    assert "visual_tree" in opt
    assert opt["visual_tree"]["label"] == "Select"
    assert raw["formatted_sql"] != opt["formatted_sql"]


def test_expression_to_visual_tree_edges():
    parsed = parse_one("SELECT a, b FROM t", dialect="postgres")
    visual = expression_to_visual_tree(parsed)
    assert visual["label"] == "Select"
    assert any(c.get("edge", "").startswith("expressions") for c in visual["children"])
