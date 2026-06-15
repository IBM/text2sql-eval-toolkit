#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

import os
import json
import argparse
import asyncio
import time
from pathlib import Path
from dotenv import load_dotenv
from tqdm.asyncio import tqdm_asyncio

from text2sql_eval_toolkit.utils import (
    get_question,
    get_available_benchmarks,
    load_predictions_data,
    save_predictions_data,
)
from text2sql_eval_toolkit.evaluation.llm_as_judge import (
    load_llm_judge_config,
    evaluate_sql_prediction_with_llm,
)

# Load environment variables from ~/.env
load_dotenv(os.path.expanduser("~/.env"))

# Paths to LLM judge configuration files
llm_judge_config_paths = [
    "src/text2sql_eval_toolkit/evaluation/llm_judge_config/llm_judge_default_config.yaml",
    "src/text2sql_eval_toolkit/evaluation/llm_judge_config/llm_judge_alt_config.yaml",
    "src/text2sql_eval_toolkit/evaluation/llm_judge_config/llm_judge_no_gt_v1.yaml",
    "src/text2sql_eval_toolkit/evaluation/llm_judge_config/llm_judge_no_gt_v2.yaml",
]


async def evaluate_with_llm_judge(
    record, llm_judge_config_path, llm_judge_configs, semaphore
):
    """
    Evaluate a single record's predictions using a specified LLM judge configuration.
    """
    async with semaphore:
        try:
            question = get_question(record)
            ground_truth_sql = record["sql"]
            ground_truth_df = record["gt_df"]
            pipeline_predictions = record.get("predictions", {})

            llm_judge_config_path = Path(llm_judge_config_path)
            llm_judge_config_name = llm_judge_config_path.stem
            llm_judge_config = llm_judge_configs[llm_judge_config_name]

            for pipeline_id, prediction in pipeline_predictions.items():
                if llm_judge_config_name not in prediction.setdefault(
                    "llm_judge_evaluation", {}
                ):
                    predicted_sql = prediction["predicted_sql"]
                    predicted_df = prediction.get("predicted_df", None)
                    prompt = prediction["prompt"]

                    llm_as_judge_response = await asyncio.to_thread(
                        evaluate_sql_prediction_with_llm,
                        question,
                        ground_truth_sql,
                        ground_truth_df,
                        predicted_sql,
                        predicted_df,
                        prompt,
                        llm_judge_config,
                    )

                    prediction["llm_judge_evaluation"][llm_judge_config_name] = (
                        llm_as_judge_response
                    )
                    prediction["evaluation"][llm_judge_config_name + "_score"] = (
                        llm_as_judge_response["score"]
                    )

        except Exception as e:
            print(f"Error evaluating record with config {llm_judge_config_path}: {e}")


async def evaluate_all_predictions_with_llm_judge(benchmark_id):
    """
    Evaluate all predictions for a given benchmark using multiple LLM judge configurations.
    Results are written back to the evaluation file.
    """
    start_time = time.time()

    print(f"\n📄 Benchmark: {benchmark_id} (SQLite storage)")

    pred_eval_data = load_predictions_data(benchmark_id, include_eval=True)

    llm_judge_configs = {}
    for config_path in llm_judge_config_paths:
        config_path_obj = Path(config_path)
        config_name = config_path_obj.stem
        llm_judge_configs[config_name] = load_llm_judge_config(config_path_obj)

    semaphore = asyncio.Semaphore(16)
    tasks = []

    for record in pred_eval_data:
        for llm_judge_config_path in llm_judge_config_paths:
            tasks.append(
                evaluate_with_llm_judge(
                    record, llm_judge_config_path, llm_judge_configs, semaphore
                )
            )

    print(f"\n🚀 Starting LLM judge evaluations with {len(tasks)} tasks...\n")
    await tqdm_asyncio.gather(*tasks, desc="Evaluating")

    save_predictions_data(
        benchmark_id,
        pred_eval_data,
        include_eval=True,
        status="evaluated",
    )

    elapsed = time.time() - start_time
    print(f"\n✅ LLM judge evaluations completed in {elapsed:.2f} seconds.")
    print(f"📁 Results written to: {eval_path.resolve()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Evaluate SQL predictions with LLM judge."
    )
    parser.add_argument(
        "benchmark_id",
        help="Benchmark ID. Available benchmarks: "
        + ", ".join(get_available_benchmarks()),
    )
    args = parser.parse_args()

    asyncio.run(evaluate_all_predictions_with_llm_judge(args.benchmark_id))
