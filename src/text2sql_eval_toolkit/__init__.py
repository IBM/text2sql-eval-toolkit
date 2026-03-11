#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
"""
Public API for the text2sql-eval-toolkit library.

This package exposes multiple levels of functionality:

- Low-level, record-based evaluation (`evaluate_prediction`)
- File-based evaluation over prediction JSON files (`evaluate_predictions`)
- Benchmark-based orchestration that discovers files from benchmark metadata (`run_evaluation`, `run_execution`)
- Inference pipelines for generating SQL (`LLMSQLGenerationPipeline`, `AgenticSQLGenerationPipeline`)
- Utilities for discovering and inspecting available benchmarks (`get_available_benchmarks`, etc.)
"""

from .evaluation.evaluation_tools import (
    evaluate_prediction,
    async_evaluate_predictions,
    evaluate_predictions,
    compute_summary,
    summary_to_df_csv,
    print_summary,
    run_evaluation,
)
from .evaluation.llm_as_judge import (
    load_llm_judge_config,
    evaluate_sql_prediction_with_llm,
)
from .evaluation import (
    compare_result_dfs,
    compare_dfs_bird_eval_logic,
    is_sqlglot_parsable,
    is_sqlparse_parsable,
    sqlglot_parsed_queries_equivalent,
    sqlglot_optimized_equivalence,
    sqlparse_queries_equivalent,
    sql_exact_match,
)
from .execution.execution_tools import run_execution
from .inference.baseline_llm_pipeline import (
    LLMSQLGenerationPipelineSimple,
    LLMSQLGenerationPipeline,
)
from .inference.agentic_pipeline import AgenticSQLGenerationPipeline
from .utils import (
    get_available_benchmarks,
    get_benchmarks_info,
    get_benchmark_info,
    run_with_timeout,
    run_with_timeout_async,
    parse_dataframe,
    truncate_dataframe,
    get_question_id,
    get_utterance,
    get_gt_sqls,
    get_question,
    get_default_eval_filename,
    add_summary_json_suffix,
    add_summary_csv_suffix,
)

__all__ = [
    # Evaluation APIs
    "evaluate_prediction",
    "async_evaluate_predictions",
    "evaluate_predictions",
    "compute_summary",
    "summary_to_df_csv",
    "print_summary",
    "run_evaluation",
    # LLM-as-judge helpers
    "load_llm_judge_config",
    "evaluate_sql_prediction_with_llm",
    # Low-level SQL equivalence / parsing helpers (from unitxt.text2sql_utils)
    "compare_result_dfs",
    "compare_dfs_bird_eval_logic",
    "is_sqlglot_parsable",
    "is_sqlparse_parsable",
    "sqlglot_parsed_queries_equivalent",
    "sqlglot_optimized_equivalence",
    "sqlparse_queries_equivalent",
    "sql_exact_match",
    # Execution
    "run_execution",
    # Inference pipelines
    "LLMSQLGenerationPipelineSimple",
    "LLMSQLGenerationPipeline",
    "AgenticSQLGenerationPipeline",
    # Benchmark utilities
    "get_available_benchmarks",
    "get_benchmarks_info",
    "get_benchmark_info",
    # Misc utilities (advanced usage)
    "run_with_timeout",
    "run_with_timeout_async",
    "parse_dataframe",
    "truncate_dataframe",
    "get_question_id",
    "get_utterance",
    "get_gt_sqls",
    "get_question",
    "get_default_eval_filename",
    "add_summary_json_suffix",
    "add_summary_csv_suffix",
]

