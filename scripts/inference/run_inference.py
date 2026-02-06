#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
This script runs SQL generation inference using multiple LLM models on a specified benchmark.

Usage:
    python run_inference.py <benchmark_id> [--model_names model1 model2 ...] [--decoding_method greedy]
                             [--max_new_tokens 256] [--stop_sequences "```"] [--pipeline_type baseline|agentic]
                             [--max_attempts 3]

Arguments:
    benchmark_id (str): Identifier for the benchmark dataset to evaluate.
    --model_names (list of str): Optional list of model names. Defaults to LLaMA-4.
    --decoding_method (str): Decoding method to use. Default: greedy.
    --max_new_tokens (int): Max number of new tokens to generate. Default: 256.
    --stop_sequences (list of str): Optional stop sequences. Default: ["```"]
    --pipeline_type (str): Type of pipeline to use. Options: 'baseline' or 'agentic'. Default: 'baseline'.
    --max_attempts (int): Maximum number of attempts for agentic pipeline. Default: 3.

Workflow:
    - Accepts optional list of model names and model parameters.
    - Initializes the selected pipeline (baseline or agentic).
    - Runs the pipeline for each model on the provided benchmark.
    - Prints the total running time for all models.

Dependencies:
    - baseline_llm_pipeline.LLMSQLGenerationPipeline
    - agentic_pipeline.AgenticSQLGenerationPipeline
    - argparse
    - time
"""

import argparse
import time

# Load environment variables from .env file
from text2sql_eval_toolkit.env_loader import load_env

load_env()

from text2sql_eval_toolkit.config_args import add_common_arguments
from text2sql_eval_toolkit.inference.baseline_llm_pipeline import (
    LLMSQLGenerationPipeline,
)
from text2sql_eval_toolkit.inference.agentic_pipeline import (
    AgenticSQLGenerationPipeline,
)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser = add_common_arguments(parser)
    parser.add_argument(
        "--pipeline_type",
        type=str,
        default="baseline",
        choices=["baseline", "agentic"],
        help="Type of pipeline to use: 'baseline' or 'agentic'. Default: 'baseline'.",
    )
    parser.add_argument(
        "--max_attempts",
        type=int,
        default=3,
        help="Maximum number of attempts for agentic pipeline. Default: 3.",
    )
    parser.add_argument(
        "--use_baseline_prompt",
        action="store_true",
        help="For agentic pipeline: use baseline-compatible prompt. Creates 'baseline+agentic-fixes' variant. Default: False.",
    )
    parser.add_argument(
        "--agentic_version",
        type=str,
        default="v1",
        choices=["v0", "v1", "v2", "v3", "v4", "v5"],
        help="Agentic pipeline version. v0: agent-aware (baseline0), v1: baseline-compatible (baseline1), v2: smart retries (baseline2), v3: LLM judge (baseline3), v4: truly agentic (baseline4), v5: improved agentic (baseline5). Default: v1.",
    )
    args = parser.parse_args()

    model_parameters = {
        "decoding_method": args.decoding_method,
        "max_new_tokens": args.max_new_tokens,
        "stop_sequences": args.stop_sequences,
    }

    if args.pipeline_type == "agentic":
        pipeline = AgenticSQLGenerationPipeline(
            max_attempts=args.max_attempts,
            use_baseline_prompt=args.use_baseline_prompt,
            version=args.agentic_version,
        )
    else:
        pipeline = LLMSQLGenerationPipeline()

    # Determine which models to use
    if args.pipeline_type == "agentic" and args.agentic_models:
        # Use specified agentic models
        models_to_run = args.agentic_models
        print(f"Using {len(models_to_run)} specified agentic model(s): {', '.join(models_to_run)}")
    else:
        # Use all models from --model_names
        models_to_run = args.model_names
        if args.pipeline_type == "agentic":
            print(f"Using {len(models_to_run)} model(s) for agentic pipeline")

    start_time = time.time()
    for model_name in models_to_run:
        if args.pipeline_type == "agentic":
            pipeline.run_pipeline(
                args.benchmark_id,
                model_name=model_name,
                model_parameters=model_parameters,
                max_attempts=args.max_attempts,
            )
        else:
            pipeline.run_pipeline(
                args.benchmark_id,
                model_name=model_name,
                model_parameters=model_parameters,
            )
    end_time = time.time()
    print(f"Total running time: {end_time - start_time:.2f} seconds")
