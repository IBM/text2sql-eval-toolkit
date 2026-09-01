#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
On-demand LLM-as-judge: scope, spend metering, and caching.

Two properties matter more than the happy path.

*The canonical artifacts must not change.* A verdict requested by one user must
not alter what every other visitor sees, or the published numbers stop being
reproducible against the pinned snapshot.

*The budget must actually bound spend.* It runs on a personal watsonx key behind
a public site, so a ceiling that resets on restart, or that a burst of
concurrent requests can slip past, is not a ceiling.
"""

import json

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from text2sql_eval_toolkit.ui import routers_judge, server  # noqa: E402
from text2sql_eval_toolkit.ui.capabilities import Tier  # noqa: E402
from text2sql_eval_toolkit.ui.judge_budget import (  # noqa: E402
    JudgeStore,
    Usage,
    current_month,
    estimate_cost_usd,
    verdict_cache_key,
)

PIPE = "modelA-greedy-zero-shot-chatapi"
RECORDS = [
    {
        "id": "r1",
        "question": "How many customers?",
        "sql": ["SELECT COUNT(*) FROM customers"],
        "gt_df": ["{}"],
        "predictions": {
            PIPE: {
                "predicted_sql": "SELECT COUNT(1) FROM customers",
                "predicted_df": "{}",
                "prompt": "…",
                "evaluation": {"execution_accuracy": 0},
            }
        },
    }
]

FAKE_RESULT = {
    "verdict": "Yes",
    "score": 1.0,
    "explanation": "Equivalent count.",
    "token_usage": {
        "prompt_tokens": 1200,
        "completion_tokens": 80,
        "total_tokens": 1280,
    },
}


@pytest.fixture
def client(tmp_path, monkeypatch):
    results = tmp_path / "results"
    results.mkdir()
    artifact = results / "demo-predictions_eval.json"
    artifact.write_text(json.dumps(RECORDS), encoding="utf-8")

    monkeypatch.setenv("TEXT2SQL_DATA_ROOT", str(tmp_path))
    monkeypatch.delenv("TEXT2SQL_JUDGE_DISABLED", raising=False)
    monkeypatch.delenv("TEXT2SQL_JUDGE_MONTHLY_BUDGET_USD", raising=False)
    server.invalidate_index_cache()
    server.reset_judge_store()

    original_mode = server.get_mode()
    server.set_mode(Tier.FULL)  # so the tier gate is not what is under test
    try:
        yield TestClient(server.app), artifact
    finally:
        server.set_mode(original_mode)
        server.invalidate_index_cache()
        server.reset_judge_store()


@pytest.fixture
def fake_llm(monkeypatch):
    """Replace the watsonx call; these tests are about plumbing, not the model."""
    calls = []

    def _fake(*args, **kwargs):
        calls.append(args)
        return dict(FAKE_RESULT)

    monkeypatch.setattr(routers_judge, "evaluate_sql_prediction_with_llm", _fake)
    return calls


# --- scope ----------------------------------------------------------------


def test_judging_does_not_touch_the_canonical_artifact(client, fake_llm):
    api, artifact = client
    before = artifact.read_bytes()

    resp = api.post(
        "/api/benchmarks/demo/judge", json={"record_id": "r1", "pipeline": PIPE}
    )
    assert resp.status_code == 200, resp.text

    assert (
        artifact.read_bytes() == before
    ), "an on-demand verdict must not rewrite the published artifact"


def test_verdict_is_labelled_as_on_demand(client, fake_llm):
    api, _ = client
    body = api.post(
        "/api/benchmarks/demo/judge", json={"record_id": "r1", "pipeline": PIPE}
    ).json()
    assert body["source"] == "on-demand"
    assert body["verdict"] == "Yes"
    assert body["score"] == 1.0
    assert body["cached"] is False


def test_unknown_record_is_404(client, fake_llm):
    api, _ = client
    resp = api.post(
        "/api/benchmarks/demo/judge", json={"record_id": "nope", "pipeline": PIPE}
    )
    assert resp.status_code == 404


def test_unknown_pipeline_is_404(client, fake_llm):
    api, _ = client
    resp = api.post(
        "/api/benchmarks/demo/judge",
        json={"record_id": "r1", "pipeline": "not-a-pipeline"},
    )
    assert resp.status_code == 404


def test_config_name_cannot_escape_the_config_directory(client, fake_llm):
    api, _ = client
    for name in ["../../../etc/passwd", "../llm_as_judge", "..%2f..%2fsecret"]:
        resp = api.post(
            "/api/benchmarks/demo/judge",
            json={"record_id": "r1", "pipeline": PIPE, "config_name": name},
        )
        assert resp.status_code == 404, name


# --- caching --------------------------------------------------------------


def test_second_request_is_served_from_cache_without_calling_the_model(
    client, fake_llm
):
    api, _ = client
    payload = {"record_id": "r1", "pipeline": PIPE}
    first = api.post("/api/benchmarks/demo/judge", json=payload).json()
    second = api.post("/api/benchmarks/demo/judge", json=payload).json()

    assert first["cached"] is False
    assert second["cached"] is True
    assert second["verdict"] == first["verdict"]
    assert len(fake_llm) == 1, "a cached verdict must not cost another call"


def test_an_unreadable_verdict_is_not_cached(client, monkeypatch):
    """
    An N/A means nobody could read the reply, not that the answer is "N/A".
    Caching it made that permanent: the next run answered from the cache
    without calling the model, so the button offered no way to try again.
    """
    calls = []

    def _fake(*args, **kwargs):
        calls.append(args)
        return {
            "verdict": "N/A",
            "score": 0.0,
            "explanation": "the model said something else entirely",
            "token_usage": {"prompt_tokens": 10, "completion_tokens": 5},
        }

    monkeypatch.setattr(routers_judge, "evaluate_sql_prediction_with_llm", _fake)

    api, _ = client
    payload = {"record_id": "r1", "pipeline": PIPE}
    first = api.post("/api/benchmarks/demo/judge", json=payload).json()
    second = api.post("/api/benchmarks/demo/judge", json=payload).json()

    assert first["verdict"] == "N/A"
    assert first["cached"] is False
    assert second["cached"] is False, "an N/A was cached and the retry was lost"
    assert len(calls) == 2, "the second run must reach the model again"

    # And there is nothing for a cached-only lookup to find.
    resp = api.post(
        "/api/benchmarks/demo/judge",
        json={"record_id": "r1", "pipeline": PIPE, "cached_only": True},
    )
    assert resp.status_code == 204


def test_an_unreadable_verdict_is_still_metered(client, monkeypatch):
    """The tokens were spent whether or not the reply could be read."""

    def _fake(*args, **kwargs):
        return {
            "verdict": "N/A",
            "score": 0.0,
            "explanation": "unrecognised",
            "token_usage": {"prompt_tokens": 1200, "completion_tokens": 80},
        }

    monkeypatch.setattr(routers_judge, "evaluate_sql_prediction_with_llm", _fake)

    api, _ = client
    body = api.post(
        "/api/benchmarks/demo/judge", json={"record_id": "r1", "pipeline": PIPE}
    ).json()
    assert body["usage"]["calls"] == 1
    assert body["usage"]["spent_usd"] > 0


def test_cache_key_changes_with_the_inputs_that_determine_a_verdict():
    base = ("bench", "rec", "pipe", "cfg", "model")
    key = verdict_cache_key(*base)
    assert verdict_cache_key("other", "rec", "pipe", "cfg", "model") != key
    assert verdict_cache_key("bench", "rec2", "pipe", "cfg", "model") != key
    assert verdict_cache_key("bench", "rec", "pipe2", "cfg", "model") != key
    # A different judge prompt or model must not reuse an old verdict.
    assert verdict_cache_key("bench", "rec", "pipe", "cfg2", "model") != key
    assert verdict_cache_key("bench", "rec", "pipe", "cfg", "model2") != key
    assert verdict_cache_key(*base) == key


# --- spend metering -------------------------------------------------------


def test_spend_is_metered_from_reported_tokens(client, fake_llm):
    api, _ = client
    body = api.post(
        "/api/benchmarks/demo/judge", json={"record_id": "r1", "pipeline": PIPE}
    ).json()

    expected = estimate_cost_usd(body["model"], 1200, 80)
    assert body["usage"]["spent_usd"] == pytest.approx(expected, rel=1e-6)
    assert body["usage"]["calls"] == 1
    assert body["usage"]["budget_usd"] == 50.0


def test_spend_survives_a_restart(tmp_path):
    """An in-memory counter would reset on restart; the ceiling must not."""
    path = tmp_path / "judge" / "usage.sqlite"
    store = JudgeStore(path)
    store.record_spend("user", "m", 1_000_000, 0)
    spent = store.usage().spent_usd
    assert spent > 0

    reopened = JudgeStore(path)
    assert reopened.usage().spent_usd == pytest.approx(spent)


def test_exhausted_budget_refuses_further_calls(client, fake_llm, monkeypatch):
    api, _ = client
    monkeypatch.setenv("TEXT2SQL_JUDGE_MONTHLY_BUDGET_USD", "0.000001")

    resp = api.post(
        "/api/benchmarks/demo/judge", json={"record_id": "r1", "pipeline": PIPE}
    )
    # First call may pass (nothing spent yet), but it must record spend.
    if resp.status_code == 200:
        resp = api.post(
            "/api/benchmarks/demo/judge",
            json={"record_id": "r1", "pipeline": PIPE, "config_name": None},
        )
    # A cached hit is free, so use a different record identity to force a call.
    server.get_judge_store().record_spend("u", "m", 10_000_000, 10_000_000)
    blocked = api.post(
        "/api/benchmarks/demo/judge",
        json={
            "record_id": "r1",
            "pipeline": PIPE,
            "config_name": "llm_judge_alt_config",
        },
    )
    assert blocked.status_code == 429
    assert "budget" in blocked.json()["detail"].lower()


def test_kill_switch_disables_the_endpoint(client, fake_llm, monkeypatch):
    api, _ = client
    monkeypatch.setenv("TEXT2SQL_JUDGE_DISABLED", "true")
    resp = api.post(
        "/api/benchmarks/demo/judge", json={"record_id": "r1", "pipeline": PIPE}
    )
    assert resp.status_code == 503
    assert "disabled" in resp.json()["detail"].lower()
    assert not fake_llm, "the kill switch must prevent the call, not just the response"


def test_missing_token_usage_is_not_silently_treated_as_free(client, monkeypatch):
    """
    A provider that reports no usage would otherwise let calls run untracked
    while the meter reads zero.
    """
    monkeypatch.setattr(
        routers_judge,
        "evaluate_sql_prediction_with_llm",
        lambda *a, **k: {"verdict": "Yes", "score": 1.0, "explanation": "x"},
    )
    warnings = []
    monkeypatch.setattr(
        routers_judge.logger, "warning", lambda *a, **k: warnings.append(a)
    )

    api, _ = client
    resp = api.post(
        "/api/benchmarks/demo/judge", json={"record_id": "r1", "pipeline": PIPE}
    )
    assert resp.status_code == 200
    assert any("token usage" in str(w[0]).lower() for w in warnings)


# --- usage arithmetic -----------------------------------------------------


@pytest.mark.parametrize(
    "spent,budget,exhausted,warning",
    [
        (0.0, 50.0, False, False),
        (39.0, 50.0, False, False),
        (40.0, 50.0, False, True),
        (49.99, 50.0, False, True),
        (50.0, 50.0, True, False),
        (75.0, 50.0, True, False),
        (1.0, 0.0, True, False),
    ],
)
def test_usage_thresholds(spent, budget, exhausted, warning):
    usage = Usage(month=current_month(), spent_usd=spent, budget_usd=budget, calls=1)
    assert usage.exhausted is exhausted
    assert usage.warning is warning


def test_remaining_never_goes_negative():
    usage = Usage(month="2026-08", spent_usd=80.0, budget_usd=50.0, calls=3)
    assert usage.remaining_usd == 0.0


# --- restoring a shared verdict -------------------------------------------
#
# A playground link carries the judge config it was run with, and the reader's
# browser asks for that verdict on load. That lookup must never start an
# inference: the sender is sharing an answer, not authorising the reader to
# spend against the budget -- or, once per-user keys are in play, against the
# reader's own provider account.


def test_cached_only_returns_nothing_and_calls_no_model(client, fake_llm):
    api, _ = client
    resp = api.post(
        "/api/benchmarks/demo/judge",
        json={"record_id": "r1", "pipeline": PIPE, "cached_only": True},
    )
    assert resp.status_code == 204, resp.text
    assert resp.content == b""
    assert fake_llm == [], "a cached-only lookup started an inference"


def test_cached_only_returns_a_stored_verdict(client, fake_llm):
    api, _ = client
    first = api.post(
        "/api/benchmarks/demo/judge", json={"record_id": "r1", "pipeline": PIPE}
    )
    assert first.status_code == 200
    assert len(fake_llm) == 1

    resp = api.post(
        "/api/benchmarks/demo/judge",
        json={"record_id": "r1", "pipeline": PIPE, "cached_only": True},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["verdict"] == "Yes"
    assert body["cached"] is True
    assert len(fake_llm) == 1, "restoring a shared verdict re-ran the model"


def test_cached_only_does_not_spend_when_the_budget_is_exhausted(
    client, fake_llm, monkeypatch
):
    """An exhausted budget must not turn a shared link into a 429."""
    api, _ = client
    api.post("/api/benchmarks/demo/judge", json={"record_id": "r1", "pipeline": PIPE})

    monkeypatch.setenv("TEXT2SQL_JUDGE_BUDGET_USD", "0")
    routers_judge.reset_judge_store()
    try:
        resp = api.post(
            "/api/benchmarks/demo/judge",
            json={"record_id": "r1", "pipeline": PIPE, "cached_only": True},
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["cached"] is True
    finally:
        routers_judge.reset_judge_store()


def test_cached_only_is_off_by_default(client, fake_llm):
    """Omitting the flag must keep the ordinary Run judge behaviour."""
    api, _ = client
    resp = api.post(
        "/api/benchmarks/demo/judge", json={"record_id": "r1", "pipeline": PIPE}
    )
    assert resp.status_code == 200
    assert len(fake_llm) == 1
