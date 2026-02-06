#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

import json
import pathlib
from pathlib import Path
import re
import matplotlib.pyplot as plt
from collections import defaultdict
from statistics import mean
from tabulate import tabulate
from text2sql_eval_toolkit.utils import get_benchmarks_info
from text2sql_eval_toolkit.analysis.error_analysis import (
    export_failed_examples_to_markdown,
)
from text2sql_eval_toolkit.logging import get_logger


logger = get_logger(__name__)
DEFAULT_METRIC = "subset_non_empty_execution_accuracy"
DEFAULT_PRINT_METRICS = [
    "subset_non_empty_execution_accuracy",
    "execution_accuracy",
    "llm_score",
    "total_tokens",
    "inference_time_ms",
    "execution_time_ms",
]
COUNT_KEYS = ["num_records", "num_evaluated", "sum_total_tokens", "sum_inference_time_ms", "sum_execution_time_ms"]


def prettify(metric_name):
    """Convert snake_case to Title Case."""
    return " ".join(word.capitalize() for word in metric_name.split("_"))


def abbreviate(metric_name):
    """Return an abbreviation only if prettified name is too long."""
    pretty = prettify(metric_name)
    if len(pretty) > 14:
        abbr = "".join(word[0].upper() for word in metric_name.split("_"))
        return abbr, pretty
    return pretty, None


def print_summary_results_by_category(
    records, sort_by=DEFAULT_METRIC, metrics_to_print=None
):
    """
    Print evaluation summaries (overall and per category), plus comparison across categories for sort_by metric.
    """

    if metrics_to_print is None:
        metrics_to_print = DEFAULT_PRINT_METRICS

    def collect_metrics(records):
        """Aggregate average metrics and record counts per pipeline and per category."""
        category_metrics = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        all_metrics = defaultdict(lambda: defaultdict(list))

        for rec in records:
            categories = rec.get("meta", {}).get("categories", [])
            predictions = rec.get("predictions", {})

            for pipeline, pred_info in predictions.items():
                eval_metrics = pred_info.get("evaluation", {})
                for metric_name, metric_value in eval_metrics.items():
                    if isinstance(metric_value, (int, float)):
                        all_metrics[pipeline][metric_name].append(metric_value)
                        for cat in categories:
                            category_metrics[cat][pipeline][metric_name].append(
                                metric_value
                            )

                all_metrics[pipeline]["num_records"].append(1)
                for cat in categories:
                    category_metrics[cat][pipeline]["num_records"].append(1)

        def avg_metrics(metrics_dict):
            return {
                pipeline: {
                    metric: (sum(values) if metric == "num_records" else mean(values))
                    for metric, values in metric_dict.items()
                }
                for pipeline, metric_dict in metrics_dict.items()
            }

        return avg_metrics(all_metrics), {
            cat: avg_metrics(p) for cat, p in category_metrics.items()
        }

    def print_abbreviation_legend(name_map):
        if not name_map:
            return
        for abbr, full in name_map.items():
            print(f"- {abbr}: {full}")
        print()

    def print_table(metrics_dict, label=None):
        if label:
            print(f"\n=== {label} ===")

        pipelines_sorted = sorted(
            metrics_dict.items(),
            key=lambda x: x[1].get(sort_by, 0) or 0,
            reverse=True,
        )

        headers = ["Pipeline"]
        abbrev_legend = {}
        column_keys = ["num_records"] + list(metrics_to_print)
        for key in column_keys:
            label, full = abbreviate(key)
            headers.append(label)
            if full:
                abbrev_legend[label] = full

        print_abbreviation_legend(abbrev_legend)

        rows = []
        for pipeline, m in pipelines_sorted:
            row = [pipeline]
            row.append(str(int(m.get("num_records", 0))))
            for metric in metrics_to_print:
                val = m.get(metric)
                row.append(f"{val:.3f}" if val is not None else "-")
            rows.append(row)

        print(
            tabulate(
                rows,
                headers=headers,
                tablefmt="github",
                numalign="right",
                stralign="left",
            )
        )

    def print_per_pipeline_tables(sort_metric, all_avg, cat_avg):
        """Print one table per pipeline, showing the metric and num_records across categories."""
        print(f"\n=== Per-Pipeline Comparison of `{sort_metric}` Across Categories ===")

        pipelines = sorted(
            {p for cat_metrics in cat_avg.values() for p in cat_metrics.keys()}
        )
        categories = sorted(cat_avg.keys())

        for pipeline in pipelines:
            print(f"\n--- Pipeline: `{pipeline}` ---")
            metrics = all_avg.get(pipeline, {})
            score = metrics.get(sort_metric)
            count = int(metrics.get("num_records", 0))
            rows = [
                "All Categories",
                str(count) if count else "-",
                f"{score:.3f}" if score is not None else "-",
            ]
            for cat in categories:
                metrics = cat_avg[cat].get(pipeline, {})
                score = metrics.get(sort_metric)
                count = int(metrics.get("num_records", 0))
                rows.append(
                    [
                        cat,
                        str(count) if count else "-",
                        f"{score:.3f}" if score is not None else "-",
                    ]
                )
            print(
                tabulate(
                    rows,
                    headers=["Category", "# Records", prettify(sort_metric)],
                    tablefmt="github",
                    numalign="right",
                    stralign="left",
                )
            )

    # Aggregate
    all_avg, cat_avg = collect_metrics(records)

    # Print tables
    print_table(all_avg, label="Overall (All Categories Combined)")
    for cat, metrics in sorted(cat_avg.items()):
        print_table(metrics, label=f"Category: {cat}")

    # Per-pipeline compact comparison
    print_per_pipeline_tables(sort_by, all_avg, cat_avg)


def get_benchmark_statistics(benchmark_id: str, benchmarks_info: dict, pipeline_metrics: dict) -> dict:
    """
    Extract statistics for a benchmark including:
    - Description from benchmarks.json
    - Database type (sqlite/mysql)
    - Number of records in benchmark data file
    - Number of pipelines with predictions
    
    Args:
        benchmark_id: Benchmark identifier
        benchmarks_info: Full benchmark metadata from get_benchmarks_info()
        pipeline_metrics: Evaluation metrics for all pipelines
        
    Returns:
        dict with keys: description, db_type, num_records, num_pipelines
    """
    stats = {
        "description": "N/A",
        "db_type": "N/A",
        "num_records": 0,
        "num_pipelines": 0,
    }
    
    # Get benchmark metadata
    if benchmark_id in benchmarks_info:
        benchmark_info = benchmarks_info[benchmark_id]
        stats["description"] = benchmark_info.get("description", "N/A")
        
        # Extract db_type from db_engine
        db_engine = benchmark_info.get("db_engine", {})
        stats["db_type"] = db_engine.get("db_type", "N/A")
        
        # Count records from benchmark data file
        try:
            benchmark_data_path = benchmark_info.get("benchmark_json_path")
            if benchmark_data_path and Path(benchmark_data_path).exists():
                with open(benchmark_data_path, "r") as f:
                    data = json.load(f)
                    stats["num_records"] = len(data) if isinstance(data, list) else 0
        except Exception as e:
            logger.warning(f"Could not count records for {benchmark_id}: {e}")
    
    # Count pipelines (exclude llm_judge_config if present)
    if pipeline_metrics:
        stats["num_pipelines"] = len([k for k in pipeline_metrics.keys() if k != "llm_judge_config"])
    
    return stats


def generate_toc_section(results: dict, benchmarks_info: dict, sort_by: str = DEFAULT_METRIC) -> str:
    """
    Generate table of contents with benchmark summaries.
    
    Args:
        results: Dict mapping benchmark_id to (eval_path, eval_relpath, metrics)
        benchmarks_info: Full benchmark metadata
        sort_by: Metric used for sorting (for anchor link generation)
        
    Returns:
        Markdown string for TOC section
    """
    toc_lines = []
    
    # Table header
    toc_lines.append("| Benchmark | Description | DB Type | Records | Pipelines |")
    toc_lines.append("|-----------|-------------|---------|---------|-----------|")
    
    # Generate rows for each benchmark
    for benchmark_id, (eval_results_path, eval_results_relpath, pipeline_metrics) in results.items():
        stats = get_benchmark_statistics(benchmark_id, benchmarks_info, pipeline_metrics)
        
        # Create anchor link (matches GitHub's automatic heading anchor generation)
        # GitHub converts: lowercase, removes special chars, keeps underscores, replaces spaces with hyphens
        # Format: #benchmark-{benchmark_id}
        anchor = f"#benchmark-{benchmark_id}".lower()
        
        # Create table row
        row = [
            f"[{benchmark_id}]({anchor})",
            stats["description"],
            stats["db_type"],
            str(stats["num_records"]),
            str(stats["num_pipelines"]),
        ]
        toc_lines.append("| " + " | ".join(row) + " |")
    
    toc_lines.append("\n---\n")
    
    return "\n".join(toc_lines)


def collect_results(output_folder: Path, is_test: bool = False):
    benchmarks_info = get_benchmarks_info(is_test=is_test)
    results = {}
    for benchmark_id, benchmark_info in benchmarks_info.items():
        # Skip test benchmarks in production mode, but include all in test mode
        if not is_test and "test" in benchmark_id:
            continue
        eval_summary_path = Path(benchmark_info["eval_summary_path"]).resolve()
        eval_results_path = Path(benchmark_info["eval_results_path"]).resolve()
        eval_results_relpath = eval_results_path.relative_to(output_folder.resolve())
        if eval_results_path.exists():
            with open(eval_summary_path, "r") as f:
                data = json.load(f)
                data.pop("llm_judge_config", None)
            results[benchmark_id] = (eval_results_path, eval_results_relpath, data)
    return results, benchmarks_info


def generate_bar_chart(output_file_path, pipeline_metrics, title, chart_filename):
    pipelines = list(pipeline_metrics.keys())
    subset_scores = [
        pipeline_metrics[m]
        .get("subset_non_empty_execution_accuracy", {})
        .get("average", 0)
        for m in pipelines
    ]
    non_empty_scores = [
        pipeline_metrics[m].get("non_empty_execution_accuracy", {}).get("average", 0)
        for m in pipelines
    ]

    has_llm_score = any("llm_score" in pipeline_metrics[p] for p in pipelines)
    if has_llm_score:
        llm_scores = [
            pipeline_metrics[p].get("llm_score", {}).get("average", 0)
            for p in pipelines
        ]

    x = range(len(pipelines))
    width = 0.25 if has_llm_score else 0.35

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(
        [i - width for i in x],
        subset_scores,
        width,
        label="Subset Non-Empty Exec Acc",
    )
    ax.bar(x, non_empty_scores, width, label="Non-Empty Exec Acc")

    if has_llm_score:
        ax.bar([i + width for i in x], llm_scores, width, label="LLM Score")

    ax.set_ylabel("Accuracy")
    ax.set_title(title)
    ax.set_xticks(x)
    ax.set_xticklabels(pipelines, rotation=45, ha="right")
    ax.legend()
    ax.set_ylim(0, 1)

    charts_dir = Path(output_file_path).parent / "charts"
    pathlib.Path(charts_dir).mkdir(exist_ok=True)
    chart_path = charts_dir / chart_filename
    plt.tight_layout()
    plt.savefig(chart_path)
    plt.close()
    return chart_path


def generate_markdown_table(
    output_file_path,
    benchmark,
    eval_results_path,
    eval_results_relpath,
    pipeline_metrics,
    sort_by,
):
    rows = []
    header = [
        "Rank",
        "Model / Pipeline",
        "Execution Acc",
        "Non-Empty Exec Acc",
        "Subset Non-Empty Exec Acc",
        "BIRD Exec Acc",
        "LLM Judge Score",
        "Parsable SQL",
        "SQL Syntactic Match",
        "Eval Err",
        "DF Err",
        "Avg Tokens/Q",
        "Avg Inference (ms)",
        "Avg Execution (ms)",
        "Total Tokens",
        "Total Inference (ms)",
        "Total Execution (ms)",
        "#Records",
        "#Predictions",
        "#Evaluated",
        "#Correct Non-Empty Exec Acc",
        "#Correct Subset Non-Empty Exec Acc",
        "#Correct As Per LLM Judge",
    ]
    metric_keys = {
        "Execution Acc": "execution_accuracy",
        "Non-Empty Exec Acc": "non_empty_execution_accuracy",
        "Subset Non-Empty Exec Acc": "subset_non_empty_execution_accuracy",
        "BIRD Exec Acc": "bird_execution_accuracy",
        "LLM Judge Score": "llm_score",
        "Parsable SQL": "is_sqlparse_parsable",
        "SQL Syntactic Match": "sql_syntactic_equivalence",
        "Eval Err": "eval_error",
        "DF Err": "df_error",
        "Avg Tokens/Q": "total_tokens",
        "Avg Inference (ms)": "inference_time_ms",
        "Avg Execution (ms)": "execution_time_ms",
    }
    count_keys = {
        "Total Tokens": "sum_total_tokens",
        "Total Inference (ms)": "sum_inference_time_ms",
        "Total Execution (ms)": "sum_execution_time_ms",
        "#Records": "num_records",
        "#Predictions": "num_predictions",
        "#Evaluated": "num_evaluated",
        "#Correct Non-Empty Exec Acc": "num_correct_non_empty_execution_accuracy",
        "#Correct Subset Non-Empty Exec Acc": "num_correct_subset_non_empty_execution_accuracy",
        "#Correct As Per LLM Judge": "num_correct_llm",
    }
    if "llm_judge_config" in pipeline_metrics:
        pipeline_metrics.pop("llm_judge_config")
    sorted_pipelines = sorted(
        pipeline_metrics.items(),
        key=lambda x: x[1].get(sort_by, {}).get("average", 0.0),
        reverse=True,
    )

    for rank, (pipeline, metrics) in enumerate(sorted_pipelines, start=1):
        row = [str(rank), pipeline]
        for label, key in metric_keys.items():
            score = metrics.get(key, {}).get("average", None)
            row.append(f"{score:.2f}" if score is not None else "N/A")
        for label, key in count_keys.items():
            val = metrics.get(key, None)
            row.append(str(val) if val is not None else "N/A")
        rows.append(row)

    table_md = f"### Benchmark: {benchmark}\n\n"
    table_md += f"_Results sorted by default on `{sort_by}` (higher is better)_\n\n"
    with eval_results_path.open("r") as eval_results_file:
        records = json.load(eval_results_file)
        summary_md_path = eval_results_path.with_name(
            eval_results_path.stem + "_summary.md"
        )
        summary_md_relpath = summary_md_path.relative_to(
            Path(output_file_path).parent.resolve()
        )
        export_summary_results_by_category_to_markdown(records, summary_md_path)
        errors_path = eval_results_path.with_name(eval_results_path.stem + "_errors.md")
        errors_relpath = errors_path.relative_to(
            Path(output_file_path).parent.resolve()
        )
        export_failed_examples_to_markdown(records, errors_path)
    # table_md += f"📄 [View Evaluation Results JSON]({eval_results_relpath})\n\n"
    # table_md += f"📄 [View In-Depth Summary Results Across Categories]({summary_md_relpath}) - [View Examples of Errors for Error Analysis]({errors_relpath}) - [View Full Results JSON]({eval_results_relpath})\n\n"

    gitignore_path = Path(".gitignore")

    # Default: include full results
    show_full_results = True

    if gitignore_path.exists():
        ignored_files = {
            Path(line.strip()).name
            for line in gitignore_path.read_text().splitlines()
            if line.strip() and not line.strip().startswith("#")
        }
        if Path(eval_results_relpath).name in ignored_files:
            show_full_results = False

    table_md += (
        f"📄 [View In-Depth Summary Results Across Categories]({summary_md_relpath})"
        f" - [View Examples of Errors for Error Analysis]({errors_relpath})"
    )

    if show_full_results:
        table_md += f" - [View Full Results JSON]({eval_results_relpath})"

    table_md += "\n\n"

    table_md += "| " + " | ".join(header) + " |\n"
    table_md += "| " + " | ".join(["---"] * len(header)) + " |\n"
    for row in rows:
        table_md += "| " + " | ".join(row) + " |\n"
    table_md += "\n"

    chart_filename = summary_md_relpath.stem + ".png"
    chart_path = generate_bar_chart(
        output_file_path,
        pipeline_metrics,
        f"Overall Accuracy - {benchmark}",
        chart_filename,
    )
    chart_rel_path = Path(chart_path).relative_to(Path(output_file_path).parent)
    table_md += f"![Chart for {benchmark}]({chart_rel_path})\n\n"
    # table_md += f'<img src="{chart_rel_path}" alt="Chart for {benchmark}" width="50%"/>\n\n'

    return table_md


def create_dashboard(output_file_path: str, results, benchmarks_info, sort_by=DEFAULT_METRIC):
    markdown = "# Text-to-SQL Evaluation Results Dashboard\n\n"
    
    # Add table of contents
    markdown += generate_toc_section(results, benchmarks_info, sort_by)

    for benchmark, (
        eval_results_path,
        eval_results_relpath,
        pipeline_metrics,
    ) in results.items():
        markdown += generate_markdown_table(
            output_file_path,
            benchmark,
            eval_results_path,
            eval_results_relpath,
            pipeline_metrics,
            sort_by,
        )

    return markdown


def export_summary_results_by_category_to_markdown(
    records, category_summary_path, sort_by=DEFAULT_METRIC
):
    """
    Creates a markdown file summarizing evaluation results overall and by category.
    """

    def collect_metrics(records):
        """Aggregate average metrics and record counts per pipeline and per category."""

        category_metrics = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        all_metrics = defaultdict(lambda: defaultdict(list))

        for rec in records:
            categories = rec.get("meta", {}).get("categories", [])
            predictions = rec.get("predictions", {})

            for pipeline, pred_info in predictions.items():
                eval_metrics = pred_info.get("evaluation", {})
                for metric_name, metric_value in eval_metrics.items():
                    if isinstance(metric_value, (int, float)):
                        all_metrics[pipeline][metric_name].append(metric_value)
                        for cat in categories:
                            category_metrics[cat][pipeline][metric_name].append(
                                metric_value
                            )

                # Count one record per example
                all_metrics[pipeline]["num_records"].append(1)
                for cat in categories:
                    category_metrics[cat][pipeline]["num_records"].append(1)

        def avg_metrics(metrics_dict):
            """Return dict[pipeline][metric_name] = average OR count"""
            return {
                pipeline: {
                    metric: (sum(values) if metric == "num_records" else mean(values))
                    for metric, values in metric_dict.items()
                }
                for pipeline, metric_dict in metrics_dict.items()
            }

        return avg_metrics(all_metrics), {
            cat: avg_metrics(p) for cat, p in category_metrics.items()
        }

    def generate_table_md(pipeline_metrics, sort_by):
        pipelines_sorted = sorted(
            pipeline_metrics.items(),
            key=lambda x: x[1].get(sort_by, 0) or 0,
            reverse=True,
        )
        headers = [
            "Rank",
            "Pipeline",
            "Records #",
            "Predictions #",
            "Exec Acc",
            "Non-Empty Exec Acc",
            "Subset Non-Empty Exec Acc",
            "BIRD Exec Acc",
            "Parsable SQL",
            "Syntactic Equivalence Score",
            "LLM Score",
        ]
        rows = []
        for rank, (pipeline, m) in enumerate(pipelines_sorted, start=1):
            rows.append(
                [
                    rank,
                    pipeline,
                    f"{(m.get('num_records') or 0)}",
                    f"{(m.get('num_predictions') or 0)}",
                    f"{(m.get('execution_accuracy') or 0):.2f}",
                    f"{(m.get('non_empty_execution_accuracy') or 0):.2f}",
                    f"{(m.get('subset_non_empty_execution_accuracy') or 0):.2f}",
                    f"{(m.get('bird_execution_accuracy') or 0):.2f}",
                    f"{(m.get('is_sqlparse_parsable') or 0):.2f}",
                    f"{(m.get('sql_syntactic_equivalence') or 0):.2f}",
                    f"{(m.get('llm_score') or 0):.2f}",
                ]
            )
        md = "| " + " | ".join(headers) + " |\n"
        md += "| " + " | ".join(["---"] * len(headers)) + " |\n"
        for row in rows:
            md += "| " + " | ".join(map(str, row)) + " |\n"
        return md

    def make_safe_filename(s: str, replacement="_"):
        return re.sub(r"[^A-Za-z0-9_.-]", replacement, s)

    def convert_avg_to_pipeline_metrics(avg_metrics):
        """
        Convert from:
            {pipeline: {metric_name: avg_value}}
        To:
            {pipeline: {metric_name: {"average": avg_value}}}
        so that generate_bar_chart() works without changes.
        """
        out = {}
        for pipeline, metrics in avg_metrics.items():
            out[pipeline] = {}
            for mname, avg_val in metrics.items():
                out[pipeline][mname] = {
                    "average": avg_val if avg_val is not None else 0
                }
        return out

    # --- Aggregate ---
    all_avg, cat_avg = collect_metrics(records)

    # --- Build markdown ---
    md_lines = ["# Summary Results\n"]

    # Overall
    md_lines.append("## Overall Average Accuracy Results\n")
    overall_pipeline_metrics = convert_avg_to_pipeline_metrics(all_avg)

    eval_summary_filename = Path(category_summary_path).stem
    chart_filename = eval_summary_filename + ".png"
    chart_path = generate_bar_chart(
        category_summary_path,
        overall_pipeline_metrics,
        f"Overall Accuracy - {eval_summary_filename}",
        chart_filename,
    )
    md_lines.append(generate_table_md(all_avg, sort_by))
    md_lines.append(
        f"\n![Overall Chart]({Path(chart_path).relative_to(Path(category_summary_path).parent)})\n"
    )

    # Per category
    for cat, metrics in sorted(cat_avg.items()):
        md_lines.append(f"\n## Category: `{cat}`\n")
        cat_pipeline_metrics = convert_avg_to_pipeline_metrics(metrics)
        category_chart_filename = (
            eval_summary_filename + "-" + make_safe_filename(cat) + ".png"
        )
        chart_path = generate_bar_chart(
            category_summary_path,
            cat_pipeline_metrics,
            f"Accuracy for Category: {cat} - {eval_summary_filename}",
            category_chart_filename,
        )
        md_lines.append(generate_table_md(metrics, sort_by))
        md_lines.append(
            f"\n![Chart for {cat}]({Path(chart_path).relative_to(Path(category_summary_path).parent)})\n"
        )

    # Per-Pipeline Category Comparison
    md_lines.append("\n# Per-Pipeline Comparison Across Categories\n")

    SELECTED_METRICS = [
        "execution_accuracy",
        "non_empty_execution_accuracy",
        "subset_non_empty_execution_accuracy",
        "bird_execution_accuracy",
        "llm_score",
    ]

    def metric_label(name):
        return {
            "execution_accuracy": "Exec Acc",
            "non_empty_execution_accuracy": "Non-Empty Exec Acc",
            "subset_non_empty_execution_accuracy": "Subset Non-Empty Exec Acc",
            "bird_execution_accuracy": "BIRD Exec Acc",
            "llm_score": "LLM Score",
        }.get(name, name)

    all_pipelines = sorted(
        {p for cat_data in cat_avg.values() for p in cat_data.keys()}
    )
    sorted_categories = sorted(cat_avg.keys())

    for pipeline in all_pipelines:
        md_lines.append(f"\n### Pipeline: `{pipeline}`")
        headers = ["Category", "# Records", "# Predictions"] + [
            metric_label(m) for m in SELECTED_METRICS
        ]
        md_lines.append("| " + " | ".join(headers) + " |")
        md_lines.append("|" + "|".join(["---"] * len(headers)) + "|")

        p_metrics = all_avg.get(pipeline, {})
        count = int(p_metrics.get("num_records", 0))
        pred_count = int(p_metrics.get("num_predictions", 0))
        row = ["All Categories", str(count), str(pred_count)]
        for m in SELECTED_METRICS:
            val = p_metrics.get(m)
            row.append(f"{val:.3f}" if val is not None else "-")
        md_lines.append("| " + " | ".join(row) + " |")
        for cat in sorted_categories:
            p_metrics = cat_avg[cat].get(pipeline, {})
            count = int(p_metrics.get("num_records", 0))
            pred_count = int(p_metrics.get("num_predictions", 0))
            row = [cat, str(count), str(pred_count)]
            for m in SELECTED_METRICS:
                val = p_metrics.get(m)
                row.append(f"{val:.3f}" if val is not None else "-")
            md_lines.append("| " + " | ".join(row) + " |")

    with open(category_summary_path, "w", encoding="utf-8") as f:
        f.write("\n".join(md_lines))

    logger.info(f"✅ Results summary markdown saved to {category_summary_path}")
