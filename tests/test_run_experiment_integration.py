#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

import json
import shutil
from pathlib import Path
import pytest
from text2sql_eval_toolkit.inference.baseline_llm_pipeline import (
    LLMSQLGenerationPipeline,
)
from text2sql_eval_toolkit.execution.execution_tools import run_execution
from text2sql_eval_toolkit.evaluation.evaluation_tools import run_evaluation
from text2sql_eval_toolkit.analysis.error_analysis import (
    export_failed_examples_to_markdown,
)


def test_run_experiment_end_to_end():
    benchmark_id = "bird_sqlite_test_benchmark"
    model_name = "wxai:meta-llama/llama-3-3-70b-instruct"
    model_parameters = {
        "decoding_method": "greedy",
        "max_new_tokens": 512,
        # "stop_sequences": ["```"],
    }

    # Base directory relative to this script
    base_dir = (
        Path(__file__).resolve().parent.parent
        / "data"
        / "benchmarks"
        / "test_benchmarks"
        / "results"
    )

    # Clean up the results directory
    if base_dir.exists():
        shutil.rmtree(base_dir)
    base_dir.mkdir(parents=True, exist_ok=True)

    # Define output file paths
    output_file = base_dir / f"{benchmark_id}-predictions.json"
    eval_results_path = base_dir / f"{benchmark_id}-predictions_eval.json"
    eval_results_md_path = base_dir / f"{benchmark_id}-predictions_eval_errors.md"

    # Run pipeline and evaluations
    pipeline = LLMSQLGenerationPipeline()
    pipeline.run_pipeline(
        benchmark_id,
        pipeline_id=model_name + "-greedy-zero-shot",
        model_name=model_name,
        model_parameters=model_parameters,
    )
    run_execution(benchmark_id)
    run_evaluation(benchmark_id, use_llm=True)

    # Assertions and error export
    assert output_file.exists(), f"Expected output file not found: {output_file}"
    assert eval_results_path.exists(), (
        f"Expected output file not found: {eval_results_path}"
    )

    with eval_results_path.open("r", encoding="utf-8") as eval_results_file:
        records = json.load(eval_results_file)
        errors_path = eval_results_path.with_name(eval_results_path.stem + "_errors.md")
        export_failed_examples_to_markdown(records, errors_path)

    assert eval_results_md_path.exists(), (
        f"Expected output file not found: {eval_results_md_path}"
    )
