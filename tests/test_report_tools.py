#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
``report_tools`` writes ``data/results/README.md`` -- the published summary of
every benchmark and pipeline. It was at 0% coverage.

Like the evaluation code, a bug here is silent: it produces a table with the
wrong numbers in it, which is then committed and read as authoritative. These
tests cover the pieces that decide what appears in that table.
"""

import json

import pytest

from text2sql_eval_toolkit.analysis.report_tools import (
    abbreviate,
    collect_results,
    generate_markdown_table,
    generate_toc_section,
    get_benchmark_statistics,
    prettify,
)

PIPE_A = "wxai:openai/gpt-oss-120b-greedy-zero-shot-chatapi"
PIPE_B = "wxai:ibm/granite-4-h-small-greedy-zero-shot-chatapi"


def metrics(subset=0.75, exact=0.80, records=500):
    """
    Matches what compute_summary emits: each metric is {average, stddev}, with
    counts as plain integers alongside.
    """
    return {
        "execution_accuracy": {"average": exact, "stddev": 0.4},
        "non_empty_execution_accuracy": {"average": exact, "stddev": 0.4},
        "subset_non_empty_execution_accuracy": {"average": subset, "stddev": 0.4},
        "num_records": records,
        "num_evaluated": records,
        "num_predictions": records,
    }


# --- header naming --------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("execution_accuracy", "Execution Accuracy"),
        ("llm_score", "Llm Score"),
        ("single", "Single"),
    ],
)
def test_prettify_titlecases_snake_case(raw, expected):
    assert prettify(raw) == expected


def test_short_names_are_used_as_is():
    pretty, abbr = abbreviate("llm_score")
    assert pretty == "Llm Score"
    assert abbr is None, "a short name needs no abbreviation"


def test_long_names_are_abbreviated_with_the_full_name_retained():
    """
    The table would be unreadable with full names, but the reader still needs to
    know what an abbreviation stands for.
    """
    abbr, full = abbreviate("subset_non_empty_execution_accuracy")
    assert abbr == "SNEEA"
    assert full == "Subset Non Empty Execution Accuracy"


def test_abbreviation_is_derived_from_every_word():
    abbr, _ = abbreviate("non_empty_execution_accuracy")
    assert abbr == "NEEA"


# --- per-benchmark statistics --------------------------------------------


def test_statistics_are_read_from_the_benchmark_registry(tmp_path):
    info = {
        "demo": {
            "description": "A demo benchmark",
            "db_engine": {"db_type": "sqlite"},
            "data": "benchmarks/demo.json",
        }
    }
    stats = get_benchmark_statistics("demo", info, {PIPE_A: metrics()})
    assert stats["description"] == "A demo benchmark"
    assert stats["db_type"] == "sqlite"
    assert stats["num_pipelines"] == 1


def test_pipeline_count_reflects_the_metrics_supplied():
    info = {"demo": {"description": "d", "db_engine": {"db_type": "mysql"}}}
    stats = get_benchmark_statistics(
        "demo", info, {PIPE_A: metrics(), PIPE_B: metrics()}
    )
    assert stats["num_pipelines"] == 2
    assert stats["db_type"] == "mysql"


def test_an_unknown_benchmark_degrades_rather_than_raising():
    """A registry entry can disappear; the report should still generate."""
    stats = get_benchmark_statistics("absent", {}, {})
    assert stats["description"] == "N/A"
    assert stats["db_type"] == "N/A"
    assert stats["num_pipelines"] == 0


# --- the results table ----------------------------------------------------

SORT_METRIC = "subset_non_empty_execution_accuracy"


def table_for(pipeline_metrics, tmp_path):
    """Charts are written beside the report, so give it a real directory."""
    eval_path = tmp_path / "demo-predictions_eval.json"
    eval_path.write_text("[]", encoding="utf-8")
    return generate_markdown_table(
        str(tmp_path / "README.md"),
        "demo",
        eval_path,
        "demo-predictions_eval.json",
        pipeline_metrics,
        SORT_METRIC,
    )


def test_table_lists_every_pipeline(tmp_path):
    table = table_for({PIPE_A: metrics(0.75), PIPE_B: metrics(0.60)}, tmp_path)
    assert PIPE_A in table
    assert PIPE_B in table


def test_table_is_ordered_best_first(tmp_path):
    """
    The report is read top-down; a table in arbitrary order misrepresents which
    pipeline won.
    """
    table = table_for({PIPE_A: metrics(0.42), PIPE_B: metrics(0.91)}, tmp_path)
    assert table.index(PIPE_B) < table.index(PIPE_A)


def test_table_renders_metric_values(tmp_path):
    table = table_for({PIPE_A: metrics(0.75)}, tmp_path)
    assert "0.75" in table


def test_table_copes_with_a_pipeline_missing_the_sort_metric(tmp_path):
    """One incomplete pipeline should not omit the rest of the table."""
    table = table_for({PIPE_A: metrics(), PIPE_B: {"num_records": 10}}, tmp_path)
    assert PIPE_A in table
    assert PIPE_B in table


def test_table_for_no_pipelines_is_not_a_crash(tmp_path):
    assert isinstance(table_for({}, tmp_path), str)


# --- table of contents ----------------------------------------------------


def test_toc_links_every_benchmark():
    results = {
        "alpha": ("p", "rel", {PIPE_A: metrics()}),
        "beta": ("p", "rel", {PIPE_A: metrics()}),
    }
    info = {
        "alpha": {"description": "A", "db_engine": {"db_type": "sqlite"}},
        "beta": {"description": "B", "db_engine": {"db_type": "mysql"}},
    }
    toc = generate_toc_section(results, info)
    assert "alpha" in toc and "beta" in toc


def test_toc_reports_the_database_type():
    results = {"alpha": ("p", "rel", {PIPE_A: metrics()})}
    info = {"alpha": {"description": "A", "db_engine": {"db_type": "sqlite"}}}
    assert "sqlite" in generate_toc_section(results, info)


# --- collecting results from disk ----------------------------------------


def test_benchmarks_without_results_are_skipped(tmp_path, monkeypatch):
    """
    A registry lists every benchmark, but a checkout may only have fetched some.
    Those must be left out rather than rendered as zero.
    """
    results_dir = tmp_path / "results"
    results_dir.mkdir()
    (tmp_path / "benchmarks.json").write_text(
        json.dumps(
            {
                "present": {
                    "name": "present",
                    "description": "has results",
                    "data": "benchmarks/present.json",
                    "schema": "benchmarks/present-schema.json",
                    "predictions": "results/present-predictions.json",
                    "db_engine": {"db_type": "sqlite"},
                },
                "absent": {
                    "name": "absent",
                    "description": "no results",
                    "data": "benchmarks/absent.json",
                    "schema": "benchmarks/absent-schema.json",
                    "predictions": "results/absent-predictions.json",
                    "db_engine": {"db_type": "sqlite"},
                },
            }
        ),
        encoding="utf-8",
    )
    (results_dir / "present-predictions_eval.json").write_text("[]", encoding="utf-8")
    (results_dir / "present-predictions_eval_summary.json").write_text(
        json.dumps({PIPE_A: metrics(), "llm_judge_config": {"model": {"id": "x"}}}),
        encoding="utf-8",
    )

    monkeypatch.setenv("TEXT2SQL_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("TEXT2SQL_EVAL_TOOLKIT_DATA_ROOT", str(tmp_path))

    results, _info = collect_results(results_dir)
    assert "present" in results
    assert "absent" not in results


def test_judge_config_is_not_treated_as_a_pipeline(tmp_path, monkeypatch):
    """
    Summary files carry llm_judge_config alongside the pipelines; leaving it in
    would add a phantom row to every table.
    """
    results_dir = tmp_path / "results"
    results_dir.mkdir()
    (tmp_path / "benchmarks.json").write_text(
        json.dumps(
            {
                "demo": {
                    "name": "demo",
                    "description": "d",
                    "data": "benchmarks/demo.json",
                    "schema": "benchmarks/demo-schema.json",
                    "predictions": "results/demo-predictions.json",
                    "db_engine": {"db_type": "sqlite"},
                }
            }
        ),
        encoding="utf-8",
    )
    (results_dir / "demo-predictions_eval.json").write_text("[]", encoding="utf-8")
    (results_dir / "demo-predictions_eval_summary.json").write_text(
        json.dumps({PIPE_A: metrics(), "llm_judge_config": {"model": {"id": "x"}}}),
        encoding="utf-8",
    )

    monkeypatch.setenv("TEXT2SQL_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("TEXT2SQL_EVAL_TOOLKIT_DATA_ROOT", str(tmp_path))

    results, _ = collect_results(results_dir)
    _path, _rel, pipeline_metrics = results["demo"]
    assert "llm_judge_config" not in pipeline_metrics
    assert PIPE_A in pipeline_metrics


def test_a_metric_stored_as_a_bare_number_does_not_break_the_report(tmp_path):
    """
    compute_summary nests each metric as {average, stddev}, but an older or
    hand-edited summary may hold a bare number. That used to raise
    AttributeError and take out the entire report.
    """
    flat = {
        "subset_non_empty_execution_accuracy": 0.61,
        "execution_accuracy": 0.70,
        "num_records": 10,
    }
    table = table_for({PIPE_A: metrics(0.20), PIPE_B: flat}, tmp_path)
    assert PIPE_A in table and PIPE_B in table
    # ...and the bare number is still read, so ordering stays meaningful.
    assert table.index(PIPE_B) < table.index(PIPE_A)


def test_a_metric_of_an_unexpected_type_falls_back_rather_than_raising(tmp_path):
    odd = {"subset_non_empty_execution_accuracy": "not a number", "num_records": 1}
    table = table_for({PIPE_A: metrics(), PIPE_B: odd}, tmp_path)
    assert PIPE_B in table
    assert "N/A" in table
