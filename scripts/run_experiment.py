#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

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
from text2sql_eval_toolkit.evaluation.evaluation_tools import run_evaluation
from text2sql_eval_toolkit.execution.execution_tools import run_execution


def main():
    parser = argparse.ArgumentParser()
    parser = add_common_arguments(parser)
    parser.add_argument(
        "--use_llm_judge",
        "--use_llm",  # Backward compatibility alias
        action="store_true",
        help="Enable LLM-as-judge evaluation metrics (optional)",
    )
    parser.add_argument(
        "--force_rerun_llm_judge",
        action="store_true",
        help="Force re-run LLM judge evaluation even if cached results exist. Default: False (reuse cached results)",
    )
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
    parser.add_argument(
        "--force_rerun",
        action="store_true",
        help="Force re-run entire experiment from scratch, ignoring existing predictions and evaluations. Default: False",
    )
    parser.add_argument(
        "--skip_inference_error_retries",
        action="store_true",
        help="Skip retrying records that previously failed with inference errors. Default: False (retry errors automatically)",
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

    start_time = time.time()
    
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
    
    for model_name in models_to_run:
        pipeline.run_pipeline(
            args.benchmark_id,
            model_name=model_name,
            model_parameters=model_parameters,
            force_rerun=args.force_rerun,
            skip_inference_error_retries=args.skip_inference_error_retries,
        )
    inference_end_time = time.time()
    print(
        f"Total inference running time: {inference_end_time - start_time:.2f} seconds"
    )

    execution_start_time = time.time()
    run_execution(args.benchmark_id, force_rerun=args.force_rerun)
    execution_end_time = time.time()
    print(f"✅ Execution completed for benchmark '{args.benchmark_id}'.")
    print(
        f"Total running time for execution: {execution_end_time - execution_start_time:.2f} seconds"
    )

    evaluation_start_time = time.time()
    run_evaluation(args.benchmark_id, args.use_llm_judge, force_rerun_llm_judge=args.force_rerun_llm_judge, force_rerun=args.force_rerun)
    evaluation_end_time = time.time()
    print(f"✅ Evaluation completed for benchmark '{args.benchmark_id}'.")
    print(
        f"Total running time for evaluation: {evaluation_end_time - evaluation_start_time:.2f} seconds"
    )

    print(
        f"Total running time for experiment: {evaluation_end_time - start_time:.2f} seconds"
    )


if __name__ == "__main__":
    main()
