#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Hardening applied for the shared deployment (plan item 3.5).

Each of these guards something that is harmless on loopback and a real problem
once the server is reachable from the internet: a config name that can escape
its directory, credentialed CORS now that session cookies exist, unbounded
request rates, and 404s that disclose the server's filesystem layout.
"""

import json

import pytest

pytest.importorskip("fastapi")
from fastapi.middleware.cors import CORSMiddleware  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from text2sql_eval_toolkit.ui import server  # noqa: E402
from text2sql_eval_toolkit.ui.capabilities import Tier  # noqa: E402


@pytest.fixture
def client(tmp_path, monkeypatch):
    results = tmp_path / "results"
    results.mkdir()
    (results / "demo-predictions_eval.json").write_text(
        json.dumps([{"id": "r1", "question": "q", "predictions": {}}]), encoding="utf-8"
    )
    monkeypatch.setattr(server, "get_data_root", lambda: tmp_path)
    server.invalidate_index_cache()
    server.reset_rate_limits()
    original = server.get_mode()
    try:
        yield TestClient(server.app)
    finally:
        server.set_mode(original)
        server.configure_cors(original)
        server.invalidate_index_cache()
        server.reset_rate_limits()


# --- judge config path containment ---------------------------------------


@pytest.mark.parametrize(
    "name",
    [
        "../../../etc/passwd",
        "..",
        ".",
        ".hidden",
        "",
        "a/../../b",
        "sub/dir",
        "name with spaces",
        "x" * 200,
    ],
)
def test_hostile_config_names_are_refused(name):
    with pytest.raises(FileNotFoundError):
        server._resolve_judge_config_path(name)


@pytest.mark.parametrize(
    "name", ["llm_judge_default_config", "llm_judge_alt_config", "llm_judge_no_gt_v1"]
)
def test_real_config_names_still_resolve(name):
    path = server._resolve_judge_config_path(name)
    assert path.name == f"{name}.yaml"
    assert path.parent == server._judge_config_dir().resolve()


def test_reading_a_traversal_config_over_http_is_404(client):
    resp = client.get("/api/llm-judge/configs/..%2f..%2fsecret")
    assert resp.status_code == 404


def test_writing_a_traversal_config_over_http_is_refused(client):
    server.set_mode(Tier.FULL)  # tier is not what is under test here
    resp = client.put(
        "/api/llm-judge/configs/..%2f..%2fowned",
        json={"model": {"id": "wxai:x"}, "prompt_template": "t"},
    )
    assert resp.status_code in (400, 404), resp.status_code


# --- CORS -----------------------------------------------------------------


def _cors_kwargs():
    for entry in server.app.user_middleware:
        if entry.cls is CORSMiddleware:
            return entry.kwargs
    raise AssertionError("CORS middleware not installed")


def test_local_mode_keeps_credentialed_cors_for_the_vite_dev_server():
    server.configure_cors(Tier.FULL)
    kwargs = _cors_kwargs()
    assert kwargs["allow_credentials"] is True
    assert "http://localhost:5173" in kwargs["allow_origins"]


@pytest.mark.parametrize("mode", [Tier.PUBLIC, Tier.JUDGE])
def test_shared_modes_withdraw_credentialed_cross_origin_access(mode):
    """
    Session cookies make a permissive credentialed CORS policy a real risk
    rather than a theoretical one; a shared deployment is same-origin anyway.
    """
    server.configure_cors(mode)
    kwargs = _cors_kwargs()
    assert kwargs["allow_origins"] == []
    assert kwargs["allow_credentials"] is False
    server.configure_cors(Tier.FULL)


# --- security headers -----------------------------------------------------


def test_responses_carry_baseline_security_headers(client):
    resp = client.get("/api/evaluation-metric-definitions")
    assert resp.headers["X-Content-Type-Options"] == "nosniff"
    assert resp.headers["X-Frame-Options"] == "DENY"
    assert resp.headers["Referrer-Policy"] == "same-origin"

    csp = resp.headers["Content-Security-Policy"]
    assert "default-src 'self'" in csp
    assert "frame-ancestors 'none'" in csp
    # Scripts must not be inline-executable; Carbon needs inline styles only.
    assert "script-src 'self'" in csp
    assert "'unsafe-inline'" not in csp.split("script-src")[1].split(";")[0]


# --- rate limiting --------------------------------------------------------


def test_local_mode_is_not_rate_limited(client):
    """Throttling a single-operator interactive tool would be a regression."""
    server.set_mode(Tier.FULL)
    server.reset_rate_limits()
    codes = {
        client.get("/api/evaluation-metric-definitions").status_code for _ in range(80)
    }
    assert codes == {200}


def test_public_mode_throttles_a_burst(client, monkeypatch):
    server.set_mode(Tier.PUBLIC)
    monkeypatch.setattr(server, "RATE_LIMIT_RPS", 1.0)
    monkeypatch.setattr(server, "RATE_LIMIT_BURST", 5.0)
    server.reset_rate_limits()

    codes = [
        client.get("/api/evaluation-metric-definitions").status_code for _ in range(20)
    ]
    assert 429 in codes, "a burst should eventually be refused"
    assert codes[0] == 200, "the burst allowance should let the first through"


def test_throttled_response_tells_the_client_to_retry(client, monkeypatch):
    server.set_mode(Tier.PUBLIC)
    monkeypatch.setattr(server, "RATE_LIMIT_RPS", 0.001)
    monkeypatch.setattr(server, "RATE_LIMIT_BURST", 1.0)
    server.reset_rate_limits()

    client.get("/api/evaluation-metric-definitions")
    resp = client.get("/api/evaluation-metric-definitions")
    assert resp.status_code == 429
    assert resp.headers.get("Retry-After")
    assert "slow down" in resp.json()["detail"].lower()


def test_auth_endpoints_get_their_own_tighter_bucket(client, monkeypatch):
    """Sign-in is cheaper to abuse than to serve."""
    server.set_mode(Tier.PUBLIC)
    monkeypatch.setattr(server, "AUTH_RATE_LIMIT_RPS", 0.001)
    monkeypatch.setattr(server, "AUTH_RATE_LIMIT_BURST", 2.0)
    monkeypatch.setattr(server, "RATE_LIMIT_RPS", 1000.0)
    monkeypatch.setattr(server, "RATE_LIMIT_BURST", 1000.0)
    server.reset_rate_limits()

    auth_codes = [
        client.get("/api/auth/login", follow_redirects=False).status_code
        for _ in range(6)
    ]
    assert 429 in auth_codes

    # The general bucket is untouched by the auth burst.
    assert client.get("/api/evaluation-metric-definitions").status_code == 200


def test_clients_are_bucketed_separately(client, monkeypatch):
    server.set_mode(Tier.PUBLIC)
    monkeypatch.setattr(server, "RATE_LIMIT_RPS", 0.001)
    monkeypatch.setattr(server, "RATE_LIMIT_BURST", 2.0)
    server.reset_rate_limits()

    a = {"X-Forwarded-For": "203.0.113.10"}
    b = {"X-Forwarded-For": "203.0.113.11"}
    for _ in range(3):
        client.get("/api/evaluation-metric-definitions", headers=a)

    assert (
        client.get("/api/evaluation-metric-definitions", headers=a).status_code == 429
    )
    assert (
        client.get("/api/evaluation-metric-definitions", headers=b).status_code == 200
    ), "one noisy client must not throttle everyone else"


# --- error detail ---------------------------------------------------------


def test_local_errors_are_actionable(client):
    server.set_mode(Tier.FULL)
    detail = server._eval_not_found_detail("spider_dev")
    assert "data/results/" in detail
    assert "results fetch" in detail


@pytest.mark.parametrize("mode", [Tier.PUBLIC, Tier.JUDGE])
def test_shared_errors_disclose_no_filesystem_layout(client, mode):
    server.set_mode(mode)
    for detail in (
        server._eval_not_found_detail("spider_dev"),
        server._summary_not_found_detail("spider_dev"),
    ):
        assert "data/results/" not in detail
        assert "TEXT2SQL_DATA_ROOT" not in detail
        assert "uv run" not in detail
        assert "spider_dev" in detail, "the message should still name the benchmark"


def test_missing_benchmark_over_http_leaks_nothing_in_public_mode(client):
    server.set_mode(Tier.PUBLIC)
    server.reset_rate_limits()
    resp = client.get("/api/benchmarks/not-a-benchmark/errors")
    assert resp.status_code == 404
    assert "data/results/" not in resp.text
