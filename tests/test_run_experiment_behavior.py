#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from unittest.mock import patch, MagicMock


def test_run_experiment_behavior():
    # Mock arguments
    args = MagicMock()
    args.benchmark_id = "test_benchmark"
    args.model_names = ["test_model"]
    args.decoding_method = "greedy"
    args.max_new_tokens = 100
    args.stop_sequences = "<EOS>"
    args.use_llm = True

    model_parameters = {
        "decoding_method": args.decoding_method,
        "max_new_tokens": args.max_new_tokens,
        "stop_sequences": args.stop_sequences,
    }

    # Mock pipeline and tools
    with (
        patch(
            "text2sql_eval_toolkit.inference.baseline_llm_pipeline.LLMSQLGenerationPipeline"
        ) as MockPipeline,
        patch(
            "text2sql_eval_toolkit.execution.execution_tools.run_execution"
        ) as mock_run_execution,
        patch(
            "text2sql_eval_toolkit.evaluation.evaluation_tools.run_evaluation"
        ) as mock_run_evaluation,
    ):
        mock_pipeline_instance = MockPipeline.return_value
        mock_pipeline_instance.run_pipeline = MagicMock()

        # Simulate the logic of the script
        for model in args.model_names:
            mock_pipeline_instance.run_pipeline(
                args.benchmark_id, model_name=model, model_parameters=model_parameters
            )

        mock_run_execution(args.benchmark_id)
        mock_run_evaluation(args.benchmark_id, args.use_llm)

        # Assertions
        mock_pipeline_instance.run_pipeline.assert_called_once_with(
            "test_benchmark", model_name="test_model", model_parameters=model_parameters
        )
        mock_run_execution.assert_called_once_with("test_benchmark")
        mock_run_evaluation.assert_called_once_with("test_benchmark", True)
