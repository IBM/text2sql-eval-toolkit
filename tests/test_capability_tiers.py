#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Authorization is enforced in one place, so it is tested in one place.

The properties that matter:

* every mutating endpoint is refused below its declared tier;
* a route nobody classified fails closed rather than open;
* a default local launch still behaves exactly as it did before tiers existed,
  because the local dashboard must keep working;
* the deployment mode is a ceiling that no sign-in can raise.
"""

import json

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from fastapi import Request  # noqa: E402

from text2sql_eval_toolkit.ui import middleware, runtime, server  # noqa: E402
from text2sql_eval_toolkit.ui.capabilities import (  # noqa: E402
    iter_routes,
    ROUTE_TIERS,
    SAFE_METHODS,
    Tier,
    parse_allowlist,
    required_tier,
    resolve_tier,
    unclassified_routes,
)

ALLOWED = "oktieh@gmail.com"


@pytest.fixture
def client(tmp_path, monkeypatch):
    results = tmp_path / "results"
    results.mkdir()
    (results / "demo-predictions_eval.json").write_text(
        json.dumps([{"id": "r1", "question": "q", "predictions": {}}]), encoding="utf-8"
    )
    monkeypatch.setenv("TEXT2SQL_DATA_ROOT", str(tmp_path))
    server.invalidate_index_cache()
    original_mode = server.get_mode()
    try:
        yield TestClient(server.app)
    finally:
        server.set_mode(original_mode)
        server.set_judge_allowlist(set())
        server.invalidate_index_cache()


def _mutating_routes():
    out = []
    # iter_routes, not app.routes: an included router is a wrapper with no path
    # of its own, so a flat walk would report every route inside it as missing.
    for route in iter_routes(server.app):
        path = getattr(route, "path", "")
        methods = getattr(route, "methods", None) or set()
        if not path.startswith("/api"):
            continue
        for method in sorted(methods - SAFE_METHODS):
            out.append((method, path))
    return out


def _concrete(path: str) -> str:
    """Substitute placeholders so the request actually routes."""
    return (
        path.replace("{benchmark_id}", "demo")
        .replace("{record_id}", "r1")
        .replace("{name}", "llm_judge_default_config")
        .replace("{job_id}", "abc")
        .replace("{file_path:path}", "logo.png")
    )


# --- the classification table itself -------------------------------------


def test_every_mutating_route_is_explicitly_classified():
    """
    An unclassified route still fails closed, but silently defaulting is how an
    endpoint meant for the judge tier ends up unreachable in production.
    """
    missing = unclassified_routes(server.app)
    assert (
        not missing
    ), "these mutating routes have no entry in ROUTE_TIERS:\n  " + "\n  ".join(
        f"{m} {p}" for m, p in missing
    )


def test_classification_table_has_no_stale_entries():
    live = set(_mutating_routes())
    declared = set(ROUTE_TIERS)
    # A judge endpoint may be declared before it is implemented; anything else
    # stale means the table drifted from the app.
    stale = {e for e in declared - live if not e[1].endswith("/judge")}
    assert not stale, f"ROUTE_TIERS references routes that no longer exist: {stale}"


def test_safe_methods_are_public():
    assert required_tier("GET", "/api/benchmarks") is Tier.PUBLIC
    assert required_tier("HEAD", "/api/benchmarks") is Tier.PUBLIC


def test_unknown_mutating_route_fails_closed():
    assert required_tier("POST", "/api/some/future/endpoint") is Tier.FULL
    assert required_tier("DELETE", "/api/anything") is Tier.FULL


# --- enforcement ----------------------------------------------------------


@pytest.mark.parametrize("method,path", _mutating_routes(), ids=lambda v: str(v))
def test_public_mode_refuses_endpoints_above_its_tier(client, method, path):
    """
    Checked against each route's *declared* tier rather than assuming every
    mutating route is privileged -- signing out, for instance, is deliberately
    available to anyone.
    """
    server.set_mode(Tier.PUBLIC)
    resp = client.request(method, _concrete(path), json={})
    if required_tier(method, path) > Tier.PUBLIC:
        assert resp.status_code == 403, f"{method} {path} returned {resp.status_code}"
        assert "read-only" in resp.json()["detail"]
    else:
        assert resp.status_code != 403, f"{method} {path} was wrongly blocked"


def test_at_least_the_dangerous_endpoints_are_privileged():
    """
    A guard on the guard: if someone relaxes a tier by accident, the
    parametrised test above would happily assert the new, weaker behaviour.
    These specific endpoints must never be reachable from the public tier.
    """
    dangerous = [
        ("POST", "/api/benchmarks/{benchmark_id}/execute"),
        ("POST", "/api/benchmarks/{benchmark_id}/playground/evaluate"),
        ("POST", "/api/benchmarks/{benchmark_id}/evaluate"),
        ("POST", "/api/benchmarks/{benchmark_id}/ground-truth-sql"),
        ("PUT", "/api/llm-judge/configs/{name}"),
        ("POST", "/api/benchmarks"),
        ("PUT", "/api/benchmarks/{benchmark_id}"),
        ("POST", "/api/benchmarks/logo-upload"),
        ("POST", "/api/results/fetch"),
    ]
    for method, path in dangerous:
        assert required_tier(method, path) is Tier.FULL, f"{method} {path}"


def test_public_mode_still_serves_reads(client):
    server.set_mode(Tier.PUBLIC)
    assert client.get("/api/benchmarks").status_code == 200
    assert client.get("/api/evaluation-metric-definitions").status_code == 200
    # Indices are built by provisioning, not by request traffic; with one
    # present, reads work normally.
    from text2sql_eval_toolkit.indexing import build_index

    build_index(server.get_results_dir() / "demo-predictions_eval.json")
    server.invalidate_index_cache()
    assert client.get("/api/benchmarks/demo/errors").status_code == 200


def test_shared_mode_refuses_to_build_an_index_on_demand(client):
    """
    Every GET is public tier, and building peaks on the largest single record,
    so anonymous traffic must not be able to trigger builds. Provisioning owns
    that step; an unprovisioned benchmark says so instead.
    """
    server.set_mode(Tier.PUBLIC)
    server.invalidate_index_cache()
    index_dir = server.get_results_dir() / ".index"
    for stale in index_dir.glob("*.sqlite"):
        stale.unlink()

    resp = client.get("/api/benchmarks/demo/errors")
    assert resp.status_code == 503
    assert "not ready" in resp.json()["detail"].lower()
    # ...and nothing was built as a side effect of asking.
    assert not list(index_dir.glob("*.sqlite")) if index_dir.exists() else True


def test_local_mode_still_builds_on_demand(client):
    """The local tool must keep working against freshly generated results."""
    server.set_mode(Tier.FULL)
    server.invalidate_index_cache()
    index_dir = server.get_results_dir() / ".index"
    if index_dir.exists():
        for stale in index_dir.glob("*.sqlite"):
            stale.unlink()

    assert client.get("/api/benchmarks/demo/errors").status_code == 200


def test_full_mode_reaches_the_handlers(client):
    """The local dashboard must keep every capability it had before tiers."""
    server.set_mode(Tier.FULL)
    for method, path in _mutating_routes():
        resp = client.request(method, _concrete(path), json={})
        assert resp.status_code != 403, f"{method} {path} was blocked in full mode"


def test_mode_is_a_ceiling_that_signing_in_cannot_raise(client, monkeypatch):
    """A public deployment must not become full because someone signed in."""
    server.set_mode(Tier.PUBLIC)
    server.set_judge_allowlist({ALLOWED})
    monkeypatch.setattr(runtime, "current_user_email", lambda request: ALLOWED)

    resp = client.post("/api/benchmarks/demo/execute", json={"sql": "SELECT 1"})
    assert resp.status_code == 403

    body = client.get("/api/me").json()
    assert body["tier"] == "public"
    assert body["can_mutate"] is False


# --- tier resolution ------------------------------------------------------


def test_allowlisted_user_reaches_judge_but_not_full():
    allowlist = {ALLOWED}
    assert resolve_tier(Tier.JUDGE, ALLOWED, allowlist) is Tier.JUDGE
    assert resolve_tier(Tier.JUDGE, ALLOWED, allowlist) < Tier.FULL


def test_signed_in_stranger_is_public():
    assert resolve_tier(Tier.JUDGE, "someone@else.com", {ALLOWED}) is Tier.PUBLIC


def test_anonymous_is_public():
    assert resolve_tier(Tier.JUDGE, None, {ALLOWED}) is Tier.PUBLIC


def test_allowlist_matching_is_case_insensitive():
    assert resolve_tier(Tier.JUDGE, "Oktieh@Gmail.COM", {ALLOWED}) is Tier.JUDGE


def test_empty_allowlist_grants_nobody_judge():
    assert resolve_tier(Tier.JUDGE, ALLOWED, set()) is Tier.PUBLIC


def test_local_mode_ignores_identity_entirely():
    assert resolve_tier(Tier.FULL, None, set()) is Tier.FULL


@pytest.mark.parametrize(
    "raw,expected",
    [
        (None, set()),
        ("", set()),
        ("  ", set()),
        ("a@b.com", {"a@b.com"}),
        ("a@b.com,c@d.com", {"a@b.com", "c@d.com"}),
        (" A@B.com , c@d.com ", {"a@b.com", "c@d.com"}),
        ("a@b.com,,c@d.com", {"a@b.com", "c@d.com"}),
    ],
)
def test_allowlist_parsing(raw, expected):
    assert parse_allowlist(raw) == expected


def test_tier_parse_rejects_nonsense():
    assert Tier.parse("PUBLIC") is Tier.PUBLIC
    assert Tier.parse(" judge ") is Tier.JUDGE
    with pytest.raises(ValueError, match="Unknown tier"):
        Tier.parse("admin")


# --- session info ---------------------------------------------------------


def test_me_reports_local_operator_capability(client):
    server.set_mode(Tier.FULL)
    body = client.get("/api/me").json()
    assert body["tier"] == "full"
    assert body["can_mutate"] is True
    assert body["signed_in"] is False


def test_me_works_without_session_middleware_installed(client):
    """Local mode installs no session middleware; reading identity must not
    blow up because of that."""
    server.set_mode(Tier.PUBLIC)
    resp = client.get("/api/me")
    assert resp.status_code == 200
    assert resp.json()["email"] is None


# --- deployment info ------------------------------------------------------


def test_deployment_reports_the_snapshot_on_screen(client, tmp_path):
    """
    A shared link may be opened months later by someone who has never seen the
    tool, so the page has to be able to say which data it is showing.
    """
    (tmp_path / ".provisioned").write_text(
        "revision=v1.1.0\nprovisioned_at=2026-08-25T16:15:14Z\n", encoding="utf-8"
    )
    server.set_mode(Tier.PUBLIC)
    body = client.get("/api/deployment").json()

    assert body["mode"] == "public"
    assert body["data_revision"] == "v1.1.0"
    assert body["data_provisioned_at"] == "2026-08-25T16:15:14Z"
    assert body["results_are_precomputed"] is True
    assert body["toolkit_version"]


def test_deployment_info_is_readable_without_signing_in(client):
    """The UI needs it before anyone authenticates."""
    server.set_mode(Tier.PUBLIC)
    assert client.get("/api/deployment").status_code == 200


def test_deployment_copes_with_no_provisioning_marker(client, tmp_path):
    marker = tmp_path / ".provisioned"
    if marker.exists():
        marker.unlink()
    body = client.get("/api/deployment").json()
    assert body["data_revision"] is None
    assert body["data_provisioned_at"] is None


def test_deployment_copes_with_a_malformed_marker(client, tmp_path):
    (tmp_path / ".provisioned").write_text(
        "this is not key=value\n\n", encoding="utf-8"
    )
    body = client.get("/api/deployment").json()
    assert body["data_revision"] is None


def test_judge_availability_follows_the_kill_switch(client, monkeypatch):
    server.set_mode(Tier.JUDGE)
    assert client.get("/api/deployment").json()["judge_available"] is True

    monkeypatch.setenv("TEXT2SQL_JUDGE_DISABLED", "true")
    assert client.get("/api/deployment").json()["judge_available"] is False


def test_sign_in_availability_follows_configuration(client, monkeypatch):
    monkeypatch.delenv("GOOGLE_CLIENT_ID", raising=False)
    monkeypatch.delenv("GOOGLE_CLIENT_SECRET", raising=False)
    assert client.get("/api/deployment").json()["sign_in_available"] is False

    monkeypatch.setenv("GOOGLE_CLIENT_ID", "id")
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "secret")
    assert client.get("/api/deployment").json()["sign_in_available"] is True


# --- seeing every route ---------------------------------------------------


def test_routes_inside_an_included_router_are_visible():
    """
    Since Starlette 1.6, include_router leaves a wrapper in app.routes holding
    the real routes rather than splicing them in. A flat walk sees an object
    with no `path` and skips it -- so every route in that router becomes
    invisible to the classification audit and to template resolution.

    This asserts the walk descends. If it regresses, the SQL-execution routes
    stop being auditable, which is the opposite of why they were grouped into a
    module of their own.
    """
    paths = {getattr(r, "path", None) for r in iter_routes(server.app)}
    assert "/api/benchmarks/{benchmark_id}/execute" in paths
    assert "/api/benchmarks/{benchmark_id}/playground/evaluate" in paths


def test_the_tier_gate_resolves_a_template_for_a_router_hosted_route():
    """
    The gate matches on the route *template*, so an id in the URL cannot be
    used to dodge a rule. Falling back to the concrete path still fails closed
    for mutating methods, but it means the ROUTE_TIERS entry is never consulted
    -- a route deliberately placed below `full` would be unreachable rather
    than merely over-restricted, and nothing would say so.
    """
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/api/benchmarks/spider_dev/execute",
        "root_path": "",
        "headers": [],
        "query_string": b"",
        "app": server.app,
    }
    template = middleware._route_template(Request(scope))
    assert template == "/api/benchmarks/{benchmark_id}/execute"
    assert required_tier("POST", template) is Tier.FULL


def test_iter_routes_tolerates_something_with_no_routes_at_all():
    assert list(iter_routes(object())) == []
