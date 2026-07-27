#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

import asyncio
from unittest.mock import patch

import pandas as pd
import pytest

from text2sql_eval_toolkit.database import session
from text2sql_eval_toolkit.evaluation.evaluation_tools import (
    _build_llm_judge_generation_context,
    _compute_llm_judge_only_summary,
    _merge_llm_judge_summary,
    async_run_llm_judge,
    evaluate_llm_judge_for_prediction,
    evaluate_prediction,
)
from text2sql_eval_toolkit.evaluation.llm_as_judge import load_llm_judge_config
from text2sql_eval_toolkit.utils import (
    load_eval_summary,
    load_predictions_data,
    save_eval_summary,
    save_predictions_data,
)
from tests.test_store_transactions import (
    _seed_test_benchmark,
    _write_minimal_test_benchmark,
)


def _df_json(df: pd.DataFrame) -> str:
    return df.to_json(orient="split")


@pytest.fixture
def isolated_db(tmp_path, monkeypatch):
    benchmark_id = "bird_sqlite_test_benchmark"
    _write_minimal_test_benchmark(tmp_path, benchmark_id)
    monkeypatch.setenv("TEXT2SQL_EVAL_TOOLKIT_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("TEXT2SQL_DATA_ROOT", str(tmp_path))
    db_path = tmp_path / "text2sql_eval.db"
    monkeypatch.setenv("TEXT2SQL_DATABASE_URL", f"sqlite:///{db_path}")
    session.close_thread_connection()
    session._schema_initialized = False
    _seed_test_benchmark(tmp_path, benchmark_id)
    yield
    session.close_thread_connection()
    session._schema_initialized = False


@pytest.fixture
def llm_judge_config():
    return {
        "model": {"id": "openai:test-model", "max_new_tokens": 32},
        "prompt_template": "Q={question}",
    }


@pytest.fixture
def sample_record():
    return {
        "question_id": 1,
        "question": "How many rows?",
        "sql": "SELECT COUNT(*) AS c FROM t",
        "gt_df": _df_json(pd.DataFrame({"c": [1]})),
    }


@pytest.fixture
def sample_prediction(sample_record):
    return {
        "predicted_sql": "SELECT COUNT(*) AS c FROM t",
        "predicted_df": _df_json(pd.DataFrame({"c": [1]})),
        "prompt": "Generate SQL for the question.",
    }


def test_load_llm_judge_default_config():
    config = load_llm_judge_config()
    assert "model" in config
    assert config["model"]["id"].startswith("wxai:")


def test_build_llm_judge_generation_context_prefers_prompt(sample_record, sample_prediction):
    context = _build_llm_judge_generation_context(
        sample_record, sample_prediction, sample_record["question"]
    )
    assert context == "Generate SQL for the question."


def test_evaluate_llm_judge_reuses_cached_scores(sample_record, sample_prediction, llm_judge_config):
    sample_prediction["evaluation"] = {
        "llm_score": 1.0,
        "llm_explanation": "Yes, correct.",
    }

    with patch(
        "text2sql_eval_toolkit.evaluation.evaluation_tools.evaluate_sql_prediction_with_llm"
    ) as mock_llm:
        result = evaluate_llm_judge_for_prediction(
            sample_record, sample_prediction, llm_judge_config
        )

    assert result["llm_score"] == 1.0
    assert result["llm_explanation"] == "Yes, correct."
    mock_llm.assert_not_called()


def test_evaluate_llm_judge_calls_llm_when_forced(
    sample_record, sample_prediction, llm_judge_config
):
    sample_prediction["evaluation"] = {
        "llm_score": 1.0,
        "llm_explanation": "Yes, correct.",
    }

    with patch(
        "text2sql_eval_toolkit.evaluation.evaluation_tools.evaluate_sql_prediction_with_llm",
        return_value={"score": 0.0, "explanation": "No, wrong.", "verdict": "No"},
    ) as mock_llm:
        result = evaluate_llm_judge_for_prediction(
            sample_record,
            sample_prediction,
            llm_judge_config,
            force_rerun_llm_judge=True,
        )

    assert result["llm_score"] == 0.0
    assert result["llm_explanation"] == "No, wrong."
    mock_llm.assert_called_once()


def test_evaluate_llm_judge_skips_inference_error(sample_record, llm_judge_config):
    prediction = {"inference_error": "timeout", "predicted_sql": ""}

    with patch(
        "text2sql_eval_toolkit.evaluation.evaluation_tools.evaluate_sql_prediction_with_llm"
    ) as mock_llm:
        result = evaluate_llm_judge_for_prediction(
            sample_record, prediction, llm_judge_config
        )

    assert result["llm_score"] == 0.0
    assert "inference failed" in result["llm_explanation"]
    mock_llm.assert_not_called()


def test_evaluate_llm_judge_ignores_null_inference_error(
    sample_record, sample_prediction, llm_judge_config
):
    """Store-backed predictions include inference_error: null; that is not a failure."""
    prediction = {
        **sample_prediction,
        "inference_error": None,
        "sql_execution_error": None,
    }

    with patch(
        "text2sql_eval_toolkit.evaluation.evaluation_tools.evaluate_sql_prediction_with_llm",
        return_value={"score": 1.0, "explanation": "ok", "verdict": "Yes"},
    ) as mock_llm:
        result = evaluate_llm_judge_for_prediction(
            sample_record, prediction, llm_judge_config
        )

    assert result["llm_score"] == 1.0
    mock_llm.assert_called_once()


def test_evaluate_prediction_ignores_null_inference_error(
    sample_record, sample_prediction
):
    prediction = {
        **sample_prediction,
        "inference_error": None,
        "sql_execution_error": None,
    }
    result = evaluate_prediction(sample_record, prediction, llm_judge_config=None)
    assert result["df_error"] == 0
    assert "df_error_message" not in result
    assert "Inference failed" not in str(result.get("df_error_message", ""))


def test_evaluate_llm_judge_missing_prediction_df(sample_record, llm_judge_config):
    prediction = {"predicted_sql": "SELECT 1"}

    with patch(
        "text2sql_eval_toolkit.evaluation.evaluation_tools.evaluate_sql_prediction_with_llm"
    ) as mock_llm:
        result = evaluate_llm_judge_for_prediction(
            sample_record, prediction, llm_judge_config
        )

    assert result["llm_score"] == 0.0
    assert "missing prediction dataframe" in result["llm_explanation"]
    mock_llm.assert_not_called()


def test_evaluate_prediction_still_runs_llm_judge_via_helper(
    sample_record, sample_prediction, llm_judge_config
):
    with patch(
        "text2sql_eval_toolkit.evaluation.evaluation_tools.evaluate_llm_judge_for_prediction",
        return_value={"llm_score": 1.0, "llm_explanation": "Yes"},
    ) as mock_helper:
        result = evaluate_prediction(
            sample_record, sample_prediction, llm_judge_config=llm_judge_config
        )

    mock_helper.assert_called()
    assert result["llm_score"] == 1.0
    assert result["execution_accuracy"] == 1


def test_compute_llm_judge_only_summary(llm_judge_config):
    metrics_by_model = {
        "pipeline-a": [
            {"llm_score": 1.0},
            {"llm_score": 0.0},
            {"llm_judge_error": "boom"},
        ]
    }

    summary = _compute_llm_judge_only_summary(metrics_by_model, llm_judge_config)

    assert summary["pipeline-a"]["num_records"] == 3
    assert summary["pipeline-a"]["num_correct_llm"] == 1
    assert summary["pipeline-a"]["num_llm_judge_errors"] == 1
    assert summary["llm_judge_config"] == llm_judge_config


def test_merge_llm_judge_summary_preserves_existing_metrics(llm_judge_config):
    existing = {
        "pipeline-a": {
            "sql_exact_match": {"average": 0.8, "stddev": 0.1},
            "num_records": 10,
        }
    }
    llm_summary = {
        "llm_judge_config": llm_judge_config,
        "pipeline-a": {
            "num_records": 10,
            "num_correct_llm": 7,
            "num_llm_judge_errors": 1,
            "llm_score": {"average": 0.7},
        },
    }

    merged = _merge_llm_judge_summary(existing, llm_summary)

    assert merged["pipeline-a"]["sql_exact_match"]["average"] == 0.8
    assert merged["pipeline-a"]["num_correct_llm"] == 7
    assert merged["llm_judge_config"] == llm_judge_config


def test_async_run_llm_judge_preserves_existing_metrics(isolated_db, llm_judge_config):
    benchmark_id = "bird_sqlite_test_benchmark"
    pipeline_id = "test-pipeline"
    records = [
        {
            "question_id": 11,
            "question": "test question",
            "sql": "SELECT 1",
            "gt_df": _df_json(pd.DataFrame({"x": [1]})),
            "predictions": {
                pipeline_id: {
                    "predicted_sql": "SELECT 1",
                    "predicted_df": _df_json(pd.DataFrame({"x": [1]})),
                    "prompt": "prompt text",
                    "evaluation": {
                        "execution_accuracy": 1,
                        "sql_exact_match": 1,
                    },
                }
            },
        }
    ]
    save_predictions_data(
        benchmark_id, records, include_eval=True, status="evaluated"
    )
    save_eval_summary(
        benchmark_id,
        {
            pipeline_id: {
                "sql_exact_match": {"average": 1.0, "stddev": 0.0},
                "num_records": 1,
            }
        },
    )

    with patch(
        "text2sql_eval_toolkit.evaluation.evaluation_tools.evaluate_sql_prediction_with_llm",
        return_value={"score": 1.0, "explanation": "Yes", "verdict": "Yes"},
    ):
        asyncio.run(
            async_run_llm_judge(
                benchmark_id,
                llm_judge_config,
                max_concurrency=1,
            )
        )

    loaded = load_predictions_data(benchmark_id, include_eval=True)
    evaluation = loaded[0]["predictions"][pipeline_id]["evaluation"]
    assert evaluation["execution_accuracy"] == 1
    assert evaluation["sql_exact_match"] == 1
    assert evaluation["llm_score"] == 1.0
    assert evaluation["llm_explanation"] == "Yes"

    summary = load_eval_summary(benchmark_id)
    assert summary[pipeline_id]["sql_exact_match"]["average"] == 1.0
    assert summary[pipeline_id]["llm_score"]["average"] == 1.0


def test_run_llm_judge_script_import():
    import importlib.util
    from pathlib import Path

    script_path = Path(__file__).resolve().parent.parent / "scripts/evaluation/run_llm_judge.py"
    spec = importlib.util.spec_from_file_location("run_llm_judge", script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert callable(module.main)
