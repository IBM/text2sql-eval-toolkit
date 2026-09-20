#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
A stored LLM-judge verdict is reused only under the config that gave it.

The batch judge used to reuse any stored ``llm_score``, whichever config had
produced it. Evaluating Llama-judged results with the gpt-oss-120b config would
have kept every Llama score and recorded the new config in the summary: results
labelled with a judge that did not produce them. A verdict now carries the
digest of the config that gave it, and is reused only under that digest --
unless the caller asks, with ``llm_judge_reuse="any"``, to keep what is stored,
in which case the stored digest travels with it and nothing is relabelled.
"""

import asyncio
import json
import logging

import pandas as pd
import pytest

from text2sql_eval_toolkit.evaluation import evaluation_tools
from text2sql_eval_toolkit.evaluation.evaluation_tools import (
    JUDGE_DIGEST_KEY,
    async_evaluate_predictions,
    compute_summary,
    evaluate_prediction,
)
from text2sql_eval_toolkit.evaluation.llm_as_judge import judge_config_digest


def df_json(rows, columns):
    return pd.DataFrame(rows, columns=columns).to_json(orient="split")


GT = df_json([[1, "ada"]], ["id", "name"])
OTHER = df_json([[9, "mallory"]], ["id", "name"])

NEW = {"model": {"id": "wxai:openai/gpt-oss-120b"}, "prompt_template": "new {question}"}
OLD = {
    "model": {"id": "wxai:meta-llama/llama-3-3-70b-instruct"},
    "prompt_template": "old {question}",
}


def record(**extra):
    return {
        "id": "q1",
        "question": "q",
        "sql": "SELECT id, name FROM t",
        "gt_df": GT,
        **extra,
    }


def mismatch(evaluation=None):
    prediction = {"predicted_sql": "SELECT 9", "predicted_df": OTHER}
    if evaluation is not None:
        prediction["evaluation"] = evaluation
    return prediction


def stored(config=None, score=1.0, explanation="stored verdict"):
    evaluation = {"llm_score": score, "llm_explanation": explanation}
    if config is not None:
        evaluation[JUDGE_DIGEST_KEY] = judge_config_digest(config)
    return evaluation


@pytest.fixture
def judge(monkeypatch):
    """Every call the judge would have made, answered No."""
    calls = []

    def fake(llm_judge_config, **inputs):
        calls.append((llm_judge_config, inputs))
        return {"verdict": "No", "score": 0.0, "explanation": "fresh verdict"}

    monkeypatch.setattr(evaluation_tools, "evaluate_sql_prediction_with_llm", fake)
    return calls


# --- reuse ------------------------------------------------------------------


def test_a_verdict_from_the_same_config_is_reused(judge):
    result = evaluate_prediction(record(), mismatch(stored(NEW)), llm_judge_config=NEW)
    assert result["llm_explanation"] == "stored verdict"
    assert result[JUDGE_DIGEST_KEY] == judge_config_digest(NEW)
    assert not judge


def test_a_verdict_from_another_config_is_judged_again(judge):
    result = evaluate_prediction(record(), mismatch(stored(OLD)), llm_judge_config=NEW)
    assert len(judge) == 1 and judge[0][0] is NEW
    assert (result["llm_score"], result["llm_explanation"]) == (0.0, "fresh verdict")
    assert result[JUDGE_DIGEST_KEY] == judge_config_digest(NEW)


def test_a_verdict_stored_before_digests_existed_is_judged_again(judge):
    """Every score published before 1.6.0 has no digest; none may be relabelled."""
    evaluate_prediction(record(), mismatch(stored()), llm_judge_config=NEW)
    assert len(judge) == 1


def test_reuse_any_keeps_the_stored_verdict_and_its_provenance(judge):
    kept = evaluate_prediction(
        record(), mismatch(stored(OLD)), llm_judge_config=NEW, llm_judge_reuse="any"
    )
    assert kept["llm_explanation"] == "stored verdict"
    assert kept[JUDGE_DIGEST_KEY] == judge_config_digest(OLD)

    undated = evaluate_prediction(
        record(), mismatch(stored()), llm_judge_config=NEW, llm_judge_reuse="any"
    )
    assert undated["llm_explanation"] == "stored verdict"
    assert JUDGE_DIGEST_KEY not in undated
    assert not judge


def test_force_rerun_judges_even_a_matching_verdict(judge):
    evaluate_prediction(
        record(),
        mismatch(stored(NEW)),
        llm_judge_config=NEW,
        force_rerun_llm_judge=True,
    )
    assert len(judge) == 1


def test_a_stored_judge_error_is_retried(judge):
    failed = {**stored(NEW), "llm_judge_error": "ValueError('boom')"}
    evaluate_prediction(record(), mismatch(failed), llm_judge_config=NEW)
    assert len(judge) == 1


def test_a_stored_note_that_the_judge_was_not_asked_is_not_a_verdict(judge):
    """A result that once matched and no longer does is judged, not scored 1."""
    note = {
        "llm_score": 1.0,
        "llm_explanation": "N/A (did not use LLM due to subset match)",
    }
    result = evaluate_prediction(
        record(), mismatch(note), llm_judge_config=NEW, llm_judge_reuse="any"
    )
    assert len(judge) == 1 and result["llm_score"] == 0.0


def test_a_failed_call_keeps_the_stored_verdict_rather_than_erasing_it(monkeypatch):
    """
    A refused call used to leave the prediction with no score at all, and a
    summary counts a missing score as 0 -- so a run the provider rate-limited
    published scores far below the ones the judge had given.
    """

    def refuse(**kwargs):
        raise RuntimeError("Exceeded limit of calls to endpoint")

    monkeypatch.setattr(evaluation_tools, "evaluate_sql_prediction_with_llm", refuse)
    result = evaluate_prediction(
        record(), mismatch(stored(OLD, score=1.0)), llm_judge_config=NEW
    )
    assert result["llm_score"] == 1.0
    assert result["llm_explanation"] == "stored verdict"
    # Kept under the digest of the judge that gave it, not this one.
    assert result[JUDGE_DIGEST_KEY] == judge_config_digest(OLD)
    assert "Exceeded limit" in result["llm_judge_error"]


def test_a_second_failed_call_in_a_row_still_keeps_the_verdict(monkeypatch):
    """
    Retrying is what the run tells the operator to do, so the retry must not be
    the thing that loses the score.

    The first refusal stores the verdict beside an `llm_judge_error`. Treating
    that error as "no verdict here" made the second refusal drop it, and the
    prediction ended with no `llm_score` at all -- which a summary counts as 0,
    the exact collapse the first refusal's handler exists to prevent.
    """

    def refuse(**kwargs):
        raise RuntimeError("Exceeded limit of calls to endpoint")

    monkeypatch.setattr(evaluation_tools, "evaluate_sql_prediction_with_llm", refuse)

    first = evaluate_prediction(
        record(), mismatch(stored(OLD, score=1.0)), llm_judge_config=NEW
    )
    assert first["llm_score"] == 1.0

    # The stored evaluation is now the output of a run that failed.
    second = evaluate_prediction(record(), mismatch(first), llm_judge_config=NEW)
    assert second["llm_score"] == 1.0
    assert second["llm_explanation"] == "stored verdict"
    assert second[JUDGE_DIGEST_KEY] == judge_config_digest(OLD)
    assert "Exceeded limit" in second["llm_judge_error"]


def test_a_verdict_kept_through_a_failure_is_still_retried(judge):
    """Keeping it must not turn into settling for it: the next run asks again."""
    kept = {**stored(OLD), "llm_judge_error": "RuntimeError('refused')"}
    result = evaluate_prediction(record(), mismatch(kept), llm_judge_config=NEW)
    assert len(judge) == 1
    assert result["llm_explanation"] == "fresh verdict"
    assert result[JUDGE_DIGEST_KEY] == judge_config_digest(NEW)
    assert "llm_judge_error" not in result


def test_a_verdict_kept_through_a_failure_is_reported_as_foreign(
    tmp_path, monkeypatch, caplog
):
    """
    The warning that says scores are recorded under a config that did not give
    them has to cover this route into it. It counted only verdicts with no
    error beside them, which is never how one of these is stored -- so under
    the default "matching" mode it never fired, and a run whose judge the
    provider refused published Llama scores labelled gpt-oss without a word.
    """

    def refuse(**kwargs):
        raise RuntimeError("refused")

    monkeypatch.setattr(evaluation_tools, "evaluate_sql_prediction_with_llm", refuse)
    predictions = tmp_path / "demo-predictions.json"
    predictions.write_text(
        json.dumps(
            [{**record(), "predictions": {"p": mismatch(stored(OLD, score=1.0))}}]
        ),
        encoding="utf-8",
    )

    with caplog.at_level(logging.WARNING):
        asyncio.run(
            async_evaluate_predictions(
                str(predictions),
                str(tmp_path / "demo-predictions_eval.json"),
                llm_judge_config=NEW,
            )
        )

    assert "1 LLM judge verdicts were kept from a different judge config" in caplog.text


def test_a_failed_call_with_nothing_stored_records_only_the_error(monkeypatch):
    def refuse(**kwargs):
        raise RuntimeError("no")

    monkeypatch.setattr(evaluation_tools, "evaluate_sql_prediction_with_llm", refuse)
    result = evaluate_prediction(record(), mismatch(), llm_judge_config=NEW)
    assert "llm_score" not in result
    assert result["llm_judge_error"]


def test_an_unknown_reuse_mode_is_refused():
    with pytest.raises(ValueError, match="llm_judge_reuse"):
        evaluate_prediction(
            record(), mismatch(), llm_judge_config=NEW, llm_judge_reuse="sometimes"
        )


# --- what the judge is asked --------------------------------------------------


def test_scores_decided_without_the_judge_carry_no_digest(judge):
    matched = evaluate_prediction(
        record(),
        {"predicted_sql": "SELECT id, name FROM t", "predicted_df": GT},
        llm_judge_config=NEW,
    )
    assert matched["llm_score"] == 1.0 and JUDGE_DIGEST_KEY not in matched

    missing = evaluate_prediction(
        record(),
        {"predicted_sql": "SELECT 9", "predicted_df": None},
        llm_judge_config=NEW,
    )
    assert missing["llm_score"] == 0.0 and JUDGE_DIGEST_KEY not in missing
    assert not judge


def test_the_judge_is_asked_once_about_the_ground_truth_that_decided(judge):
    """It used to be asked once per ground truth, every answer but the last discarded."""
    two = record(
        sql=["SELECT 1", "SELECT 2"],
        gt_df=[GT, df_json([[2, "grace"]], ["id", "name"])],
    )
    evaluate_prediction(two, mismatch(), llm_judge_config=NEW)
    assert len(judge) == 1
    assert judge[0][1]["ground_truth_sql"] == "SELECT 2"


def test_a_matched_ground_truth_is_still_reported_after_the_verdict(judge):
    two = record(sql=["SELECT 1", "SELECT id, name FROM t"], gt_df=[OTHER, GT])
    result = evaluate_prediction(
        two,
        {"predicted_sql": "SELECT id, name FROM t", "predicted_df": GT},
        llm_judge_config=NEW,
    )
    assert result["gt_sql"] == "SELECT id, name FROM t"
    assert list(result)[-2:] == ["gt_sql", "gt_df"]
    assert not judge


def test_the_digest_ignores_key_order_and_changes_with_the_prompt():
    reordered = {"prompt_template": NEW["prompt_template"], "model": dict(NEW["model"])}
    assert judge_config_digest(reordered) == judge_config_digest(NEW)
    assert judge_config_digest(OLD) != judge_config_digest(NEW)


def test_a_kept_verdict_is_counted_by_both_the_average_and_the_tally():
    """
    A verdict kept through a failed call is in the `llm_score` average, so it
    has to be in the count printed beside it too, or a summary contradicts
    itself: 1.00 average, "1 of 2 correct".
    """
    kept = {
        "subset_non_empty_execution_accuracy": 0,
        "llm_score": 1.0,
        "llm_explanation": "stored yes",
        JUDGE_DIGEST_KEY: judge_config_digest(OLD),
        "llm_judge_error": "RuntimeError('refused')",
    }
    fresh = {
        "subset_non_empty_execution_accuracy": 0,
        "llm_score": 1.0,
        "llm_explanation": "yes",
        JUDGE_DIGEST_KEY: judge_config_digest(NEW),
    }
    summary = compute_summary({"p": [kept, fresh]}, NEW)["p"]
    assert summary["llm_score"]["average"] == 1.0
    assert summary["num_correct_llm"] == 2
    assert summary["num_llm_judge_errors"] == 1


def test_a_failure_with_no_verdict_is_counted_by_neither():
    failed = {
        "subset_non_empty_execution_accuracy": 0,
        "llm_judge_error": "RuntimeError('refused')",
    }
    summary = compute_summary({"p": [failed]}, NEW)["p"]
    assert summary["num_correct_llm"] == 0
    assert summary["num_llm_judge_errors"] == 1


def test_a_summary_is_not_confused_by_the_digest(judge):
    evaluation = evaluate_prediction(record(), mismatch(), llm_judge_config=NEW)
    summary = compute_summary({"p": [evaluation]}, NEW)
    assert summary["p"]["llm_score"]["average"] == 0.0
    assert JUDGE_DIGEST_KEY not in summary["p"]


# --- through a file, as a re-run meets it -------------------------------------


def test_re_evaluating_a_file_judges_again_only_when_the_config_changes(
    tmp_path, judge
):
    predictions = tmp_path / "demo-predictions.json"
    predictions.write_text(
        json.dumps([{**record(), "predictions": {"p": mismatch()}}]), encoding="utf-8"
    )
    output = tmp_path / "demo-predictions_eval.json"

    def run(config):
        asyncio.run(
            async_evaluate_predictions(
                str(predictions), str(output), llm_judge_config=config
            )
        )
        return json.loads(output.read_text())[0]["predictions"]["p"]["evaluation"]

    assert run(OLD)[JUDGE_DIGEST_KEY] == judge_config_digest(OLD)
    assert len(judge) == 1
    run(OLD)
    assert len(judge) == 1, "the same config must reuse its stored verdict"
    assert run(NEW)[JUDGE_DIGEST_KEY] == judge_config_digest(NEW)
    assert len(judge) == 2, "a different config must judge again"
