#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

import argparse
from text2sql_eval_toolkit.utils import get_available_benchmarks

DEFAULT_MODEL_NAMES = [
    "wxai:meta-llama/llama-3-3-70b-instruct",
    "wxai:ibm/granite-4-h-small",
    "wxai:meta-llama/llama-4-maverick-17b-128e-instruct-fp8",
    "wxai:openai/gpt-oss-120b",
    # "vllm:Qwen/Qwen2.5-1.5B-Instruct",
    # "rits:microsoft/Phi-4-reasoning",
    # "anthropic:claude-opus-4-1-20250805"
]

# Default models for agentic baselines (single model to reduce runtime)
DEFAULT_AGENTIC_MODELS = ["wxai:openai/gpt-oss-120b"]


def add_common_arguments(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument(
        "benchmark_id",
        help="Benchmark ID. Available benchmarks: "
        + ", ".join(get_available_benchmarks()),
    )
    parser.add_argument(
        "--model_names",
        nargs="+",
        default=DEFAULT_MODEL_NAMES,
        help=F"Optional list of model names. Defaults to {DEFAULT_MODEL_NAMES}.",
    )
    parser.add_argument(
        "--decoding_method",
        type=str,
        default="greedy",
        help="Decoding method (e.g., greedy, sampling). Default: greedy. "
        "Note: Only supported by WatsonX legacy API; ignored by Chat APIs (WatsonX Chat, Claude, VLLM).",
    )
    parser.add_argument(
        "--max_new_tokens",
        type=int,
        default=256,
        help="Maximum number of new tokens to generate. Default: 256. "
        "Automatically converted to 'max_tokens' for Chat APIs.",
    )
    parser.add_argument(
        "--stop_sequences",
        nargs="+",
        default=[],
        help="Stop sequences. Pass each one as a separate string. Default: []. "
        "Note: Supported by Claude and VLLM, but not by WatsonX Chat API.",
    )
    parser.add_argument(
        "--agentic_models",
        nargs="+",
        default=None,
        help=f"List of models to use for agentic baselines when using --run_all_baselines or --pipeline_type agentic. "
        f"Defaults to: {', '.join(DEFAULT_AGENTIC_MODELS)}. "
        "If not specified, uses all models from --model_names for run_experiment.py, "
        f"or DEFAULT_AGENTIC_MODELS for run_all_benchmarks.py.",
    )
    return parser
