#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

import argparse
import time
from pathlib import Path
from text2sql_eval_toolkit import env_loader  # Load .env file automatically
from text2sql_eval_toolkit.utils import get_benchmarks_info
from text2sql_eval_toolkit.inference.baseline_llm_pipeline import (
    LLMSQLGenerationPipeline,
)
from text2sql_eval_toolkit.inference.agentic_pipeline import (
    AgenticSQLGenerationPipeline,
)
from text2sql_eval_toolkit.config_args import DEFAULT_MODEL_NAMES, DEFAULT_AGENTIC_MODELS
from text2sql_eval_toolkit.evaluation.evaluation_tools import run_evaluation
from text2sql_eval_toolkit.execution.execution_tools import run_execution
from text2sql_eval_toolkit.analysis.report_tools import (
    collect_results,
    create_dashboard,
)
from text2sql_eval_toolkit.logging import get_logger


SUMMARY_OUTPUT_FILE = "data/results/README.md"
TEST_SUMMARY_OUTPUT_FILE = "data/benchmarks/test_benchmarks/results/README.md"
logger = get_logger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Run experiments on all benchmarks with inference, execution, and evaluation."
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run on test benchmarks from test-benchmarks.json instead of production benchmarks",
    )
    # Accept multiple benchmark IDs - if none provided, runs on all benchmarks
    parser.add_argument(
        "benchmark_ids",
        nargs="*",
        default=[],
        help="Optional benchmark ID(s). If not provided, runs on all benchmarks. Available benchmarks: "
        + ", ".join(get_benchmarks_info().keys()),
    )
    parser.add_argument(
        "--model_names",
        nargs="+",
        default=None,  # Will use DEFAULT_MODEL_NAMES for baseline, DEFAULT_AGENTIC_MODELS for agentic
        help=f"Optional list of model names for standard baseline. Defaults to: {', '.join(DEFAULT_MODEL_NAMES)}. For agentic baselines, use --agentic_models (defaults to: {', '.join(DEFAULT_AGENTIC_MODELS)})",
    )
    parser.add_argument(
        "--agentic_models",
        nargs="+",
        default=None,  # Will use DEFAULT_AGENTIC_MODELS if not specified
        help=f"Optional list of model names for agentic baselines. Defaults to: {', '.join(DEFAULT_AGENTIC_MODELS)}",
    )
    parser.add_argument(
        "--decoding_method",
        type=str,
        default="greedy",
        help="Decoding method (e.g., greedy, sampling). Default: greedy.",
    )
    parser.add_argument(
        "--max_new_tokens",
        type=int,
        default=256,
        help="Maximum number of new tokens to generate. Default: 256.",
    )
    parser.add_argument(
        "--stop_sequences",
        nargs="+",
        default=[],
        help="Stop sequences. Pass each one as a separate string. Default: []",
    )
    parser.add_argument(
        "--use_llm_judge",
        "--use_llm",  # Backward compatibility alias
        action="store_true",
        help="Enable LLM-as-judge evaluation metrics (optional)",
    )
    parser.add_argument(
        "--force_rerun",
        action="store_true",
        help="Force re-run entire experiment from scratch, ignoring existing predictions and evaluations. Default: False",
    )
    parser.add_argument(
        "--force_rerun_llm_judge",
        action="store_true",
        help="Force re-run LLM judge evaluation even if cached results exist. Default: False (reuse cached results)",
    )
    parser.add_argument(
        "--run_all_baselines",
        action="store_true",
        help="Run all baselines (standard + all 6 agentic variants). If set, ignores --pipeline_type and related args.",
    )
    parser.add_argument(
        "--pipeline_type",
        type=str,
        default="baseline",
        choices=["baseline", "agentic"],
        help="Type of pipeline to run (baseline or agentic). Ignored if --run_all_baselines is set. Default: baseline.",
    )
    parser.add_argument(
        "--max_attempts",
        type=int,
        default=5,
        help="Maximum number of attempts for agentic pipeline. Default: 5.",
    )
    parser.add_argument(
        "--use_baseline_prompt",
        action="store_true",
        help="Use baseline-compatible prompts for agentic pipeline (for agentic-baseline1). Ignored if --run_all_baselines is set.",
    )
    parser.add_argument(
        "--agentic_versions",
        nargs="+",
        default=["v1"],
        choices=["v0", "v1", "v2", "v3", "v4", "v5"],
        help="List of agentic versions to run (e.g., --agentic_versions v0 v1 v2 v3). Runs standard baseline plus specified agentic versions. Ignored if --run_all_baselines is set. Default: ['v1'].",
    )
    args = parser.parse_args()

    model_parameters = {
        "decoding_method": args.decoding_method,
        "max_new_tokens": args.max_new_tokens,
        "stop_sequences": args.stop_sequences,
    }

    # Define pipeline configurations
    if args.run_all_baselines:
        # Run all baselines: standard + all 6 agentic variants
        pipeline_configs = [
            {
                "name": "standard-baseline",
                "type": "baseline",
                "pipeline": LLMSQLGenerationPipeline(),
            },
            {
                "name": "agentic-baseline0",
                "type": "agentic",
                "pipeline": AgenticSQLGenerationPipeline(
                    max_attempts=args.max_attempts,
                    use_baseline_prompt=False,
                    version="v0",
                ),
            },
            {
                "name": "agentic-baseline1",
                "type": "agentic",
                "pipeline": AgenticSQLGenerationPipeline(
                    max_attempts=args.max_attempts,
                    use_baseline_prompt=True,
                    version="v1",
                ),
            },
            {
                "name": "agentic-baseline2",
                "type": "agentic",
                "pipeline": AgenticSQLGenerationPipeline(
                    max_attempts=args.max_attempts,
                    use_baseline_prompt=False,
                    version="v2",
                ),
            },
            {
                "name": "agentic-baseline3",
                "type": "agentic",
                "pipeline": AgenticSQLGenerationPipeline(
                    max_attempts=args.max_attempts,
                    use_baseline_prompt=False,
                    version="v3",
                ),
            },
            {
                "name": "agentic-baseline4",
                "type": "agentic",
                "pipeline": AgenticSQLGenerationPipeline(
                    max_attempts=args.max_attempts,
                    use_baseline_prompt=False,
                    version="v4",
                ),
            },
            {
                "name": "agentic-baseline5",
                "type": "agentic",
                "pipeline": AgenticSQLGenerationPipeline(
                    max_attempts=args.max_attempts,
                    use_baseline_prompt=False,
                    version="v5",
                ),
            },
        ]
        logger.info(
            f"🎯 Running ALL {len(pipeline_configs)} baselines (standard + 6 agentic variants)"
        )
    else:
        # Run pipelines based on args
        pipeline_configs = []
        
        if args.pipeline_type == "baseline":
            # Run standard baseline only
            pipeline_configs.append({
                "name": "standard-baseline",
                "type": "baseline",
                "pipeline": LLMSQLGenerationPipeline(),
            })
        else:
            # pipeline_type == "agentic"
            # Always include standard baseline when running agentic versions
            pipeline_configs.append({
                "name": "standard-baseline",
                "type": "baseline",
                "pipeline": LLMSQLGenerationPipeline(),
            })
            
            # Add specified agentic versions
            for version in args.agentic_versions:
                use_baseline_prompt = (version == "v1")
                pipeline_configs.append({
                    "name": f"agentic-baseline{version[1]}",  # e.g., "agentic-baseline1" for v1
                    "type": "agentic",
                    "pipeline": AgenticSQLGenerationPipeline(
                        max_attempts=args.max_attempts,
                        use_baseline_prompt=use_baseline_prompt,
                        version=version,
                    ),
                })
        
        logger.info(
            f"🎯 Running {len(pipeline_configs)} pipeline(s): {', '.join(c['name'] for c in pipeline_configs)}"
        )

    # Load benchmarks from appropriate file based on --test flag
    benchmarks = get_benchmarks_info(is_test=args.test)
    
    # Set output file based on mode
    summary_output_file = TEST_SUMMARY_OUTPUT_FILE if args.test else SUMMARY_OUTPUT_FILE
    mode_name = "test" if args.test else "production"
    
    logger.info(f"📁 Mode: {mode_name.upper()}")
    logger.info(f"📁 Output: {summary_output_file}")

    # If benchmark_ids are provided, run only those; otherwise run all
    if args.benchmark_ids:
        # Validate all provided benchmark IDs
        invalid_benchmarks = [
            bid for bid in args.benchmark_ids if bid not in benchmarks
        ]
        if invalid_benchmarks:
            logger.error(f"❌ Unknown benchmark(s): {', '.join(invalid_benchmarks)}")
            logger.info(f"Available benchmarks: {', '.join(benchmarks.keys())}")
            return
        benchmark_list = args.benchmark_ids
        if len(benchmark_list) == 1:
            logger.info(f"🎯 Running on single benchmark: {benchmark_list[0]}")
        else:
            logger.info(
                f"🎯 Running on {len(benchmark_list)} selected benchmarks: {', '.join(benchmark_list)}"
            )
    else:
        benchmark_list = list(benchmarks.keys())
        logger.info(f"🎯 Running on ALL {len(benchmark_list)} benchmarks")

    total_benchmarks = len(benchmark_list)

    # Use specified models or default
    model_names = args.model_names if args.model_names else DEFAULT_MODEL_NAMES

    start_time = time.time()
    for bench_idx, benchmark_id in enumerate(benchmark_list, 1):
        try:
            logger.info(f"\n{'=' * 80}")
            logger.info(
                f"📊 Processing benchmark {bench_idx}/{total_benchmarks}: {benchmark_id}"
            )
            logger.info(f"{'=' * 80}\n")

            experiment_start_time = time.time()

            # Run all pipeline configurations
            for pipeline_idx, pipeline_config in enumerate(pipeline_configs, 1):
                logger.info(
                    f"\n🔧 Pipeline {pipeline_idx}/{len(pipeline_configs)}: {pipeline_config['name']}"
                )

                # Determine which models to use for this pipeline
                if pipeline_config['type'] == 'agentic':
                    # Agentic baselines: use agentic models list
                    agentic_models = args.agentic_models if args.agentic_models else DEFAULT_AGENTIC_MODELS
                    pipeline_models = agentic_models
                    logger.info(f"   Using {len(pipeline_models)} model(s) for agentic baseline: {', '.join(pipeline_models)}")
                else:
                    # Standard baseline: use all models
                    pipeline_models = model_names
                    logger.info(f"   Using {len(pipeline_models)} model(s) for standard baseline")

                for model_idx, model_name in enumerate(pipeline_models, 1):
                    logger.info(
                        f"🚀 Running {pipeline_config['name']} inference for model {model_idx}/{len(pipeline_models)}: {model_name}"
                    )
                    pipeline_config["pipeline"].run_pipeline(
                        benchmark_id,
                        model_name=model_name,
                        model_parameters=model_parameters,
                        force_rerun=args.force_rerun,
                    )

            logger.info(f"✅ All inference completed for benchmark '{benchmark_id}'.")
            logger.info(
                f"Total inference running time: {time.time() - start_time:.2f} seconds"
            )

            execution_start_time = time.time()
            run_execution(benchmark_id, force_rerun=args.force_rerun)
            logger.info(f"✅ Execution completed for benchmark '{benchmark_id}'.")
            logger.info(
                f"Total running time for execution: {time.time() - execution_start_time:.2f} seconds"
            )

            evaluation_start_time = time.time()
            run_evaluation(
                benchmark_id,
                args.use_llm_judge,
                force_rerun_llm_judge=args.force_rerun_llm_judge,
                force_rerun=args.force_rerun
            )
            logger.info(f"✅ Evaluation completed for benchmark '{benchmark_id}'.")
            logger.info(
                f"Total running time for evaluation: {time.time() - evaluation_start_time:.2f} seconds"
            )

            logger.info(
                f"Total running time for {benchmark_id} experiment: {time.time() - experiment_start_time:.2f} seconds"
            )
        except Exception as e:
            logger.info(
                f"‼ ERROR: Experiment for benchmark {benchmark_id} failed : {repr(e)}"
            )
            raise e

    logger.info(f"\n{'=' * 80}\n🎉 All benchmarks complete!\n{'=' * 80}")
    logger.info(
        f"Total running time for all benchmarks: {time.time() - start_time:.2f} seconds"
    )

    logger.info(f"\n{'=' * 80}")
    logger.info("📊 Generating comprehensive analysis reports...")
    logger.info(f"{'=' * 80}\n")

    output_folder = Path(summary_output_file).parent
    results, benchmarks_info = collect_results(output_folder, is_test=args.test)

    logger.info("📝 Generating:")
    logger.info("   - Main dashboard (README.md)")
    logger.info("   - Summary reports by category (*_summary.md)")
    logger.info("   - Error analysis reports (*_errors.md)")
    logger.info("   - Performance charts (*.png)")

    markdown_content = create_dashboard(summary_output_file, results, benchmarks_info)

    with open(summary_output_file, "w") as f:
        f.write(markdown_content)

    logger.info(f"\n✅ Dashboard written to: {summary_output_file}")
    logger.info(
        f"✅ All summary and error analysis markdowns generated in: {output_folder}"
    )
    logger.info(f"\n🎯 Experiment Configuration:")
    if args.run_all_baselines:
        logger.info(
            f"   Pipelines: ALL baselines (standard + agentic-baseline0/1/2/3/4/5)"
        )
        logger.info(f"   Max attempts (agentic): {args.max_attempts}")
    else:
        logger.info(f"   Pipeline: {args.pipeline_type}")
        if args.pipeline_type == "agentic":
            logger.info(f"   Versions: {', '.join(args.agentic_versions)}")
            logger.info(f"   Max attempts: {args.max_attempts}")
    
    # Show model configuration
    agentic_models = args.agentic_models if args.agentic_models else DEFAULT_AGENTIC_MODELS
    logger.info(f"   Standard baseline models: {len(model_names)} model(s) - {', '.join(model_names)}")
    logger.info(f"   Agentic baseline models: {len(agentic_models)} model(s) - {', '.join(agentic_models)}")
    logger.info(f"   Benchmarks: {total_benchmarks} benchmark(s)")
    logger.info(f"   LLM Judge: {'enabled' if args.use_llm_judge else 'disabled'}")
    logger.info(f"   Force Rerun: {'enabled' if args.force_rerun else 'disabled'}")
    if args.use_llm_judge:
        logger.info(f"   Force Rerun LLM Judge: {'enabled' if args.force_rerun_llm_judge else 'disabled'}")


if __name__ == "__main__":
    main()
