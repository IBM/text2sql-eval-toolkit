#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Google sign-in.

The load-bearing check is ``email_verified``: Google returns an ``email`` claim
for unverified addresses too, and matching the allowlist on that alone would
make the allowlist meaningless. Everything else here guards the session and the
redirect.
"""

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from text2sql_eval_toolkit.ui import auth, server  # noqa: E402
from text2sql_eval_toolkit.ui.capabilities import Tier, resolve_tier  # noqa: E402

ALLOWED = "allowed@example.com"


@pytest.fixture
def client(monkeypatch):
    original = server.get_mode()
    try:
        yield TestClient(server.app)
    finally:
        server.set_mode(original)
        server.set_judge_allowlist(set())


# --- the verified-email rule ---------------------------------------------


@pytest.mark.parametrize(
    "claims,expected",
    [
        ({"email": "a@b.com", "email_verified": True}, "a@b.com"),
        ({"email": "A@B.COM", "email_verified": True}, "a@b.com"),
        ({"email": " a@b.com ", "email_verified": True}, "a@b.com"),
        # Some OIDC providers stringify the boolean.
        ({"email": "a@b.com", "email_verified": "true"}, "a@b.com"),
        ({"email": "a@b.com", "email_verified": "TRUE"}, "a@b.com"),
    ],
)
def test_verified_addresses_are_accepted(claims, expected):
    assert auth.extract_verified_email(claims) == expected


@pytest.mark.parametrize(
    "claims",
    [
        {"email": "a@b.com", "email_verified": False},
        {"email": "a@b.com", "email_verified": "false"},
        {"email": "a@b.com", "email_verified": None},
        {"email": "a@b.com"},  # claim absent entirely
        {"email": "", "email_verified": True},
        {"email": None, "email_verified": True},
        {},
        None,
        "not-a-dict",
    ],
)
def test_unverified_or_malformed_claims_are_refused(claims):
    assert auth.extract_verified_email(claims) is None


def test_an_unverified_address_cannot_reach_the_judge_tier():
    """The end-to-end consequence of the rule above."""
    claims = {"email": ALLOWED, "email_verified": False}
    email = auth.extract_verified_email(claims)
    assert email is None
    assert resolve_tier(Tier.JUDGE, email, {ALLOWED}) is Tier.PUBLIC


# --- identity hygiene -----------------------------------------------------


def test_logs_carry_a_hash_not_the_address():
    digest = auth.hash_identity(ALLOWED)
    assert ALLOWED not in digest
    assert "@" not in digest
    assert auth.hash_identity(ALLOWED.upper()) == digest  # case-stable


def test_hashes_differ_between_identities():
    assert auth.hash_identity("a@b.com") != auth.hash_identity("c@d.com")


# --- configuration --------------------------------------------------------


def test_is_configured_requires_both_credentials(monkeypatch):
    monkeypatch.delenv("GOOGLE_CLIENT_ID", raising=False)
    monkeypatch.delenv("GOOGLE_CLIENT_SECRET", raising=False)
    assert auth.is_configured() is False

    monkeypatch.setenv("GOOGLE_CLIENT_ID", "id")
    assert auth.is_configured() is False, "client id alone is not enough"

    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "secret")
    assert auth.is_configured() is True


def test_building_a_client_without_credentials_raises(monkeypatch):
    monkeypatch.delenv("GOOGLE_CLIENT_ID", raising=False)
    monkeypatch.delenv("GOOGLE_CLIENT_SECRET", raising=False)
    with pytest.raises(auth.AuthNotConfigured):
        auth.build_oauth_client()


def test_session_secret_prefers_the_configured_value(monkeypatch):
    configured = "x" * auth.MIN_SESSION_SECRET_LENGTH
    monkeypatch.setenv("TEXT2SQL_SESSION_SECRET", configured)
    assert auth.session_secret() == configured


@pytest.mark.parametrize("weak", ["x", "short", "a" * 31])
def test_a_weak_session_secret_is_refused(monkeypatch, weak):
    """
    Forging a session cookie of {"email": "<allowlisted>"} against a guessable
    key grants the judge tier outright, so a short key is not merely untidy.
    """
    monkeypatch.setenv("TEXT2SQL_SESSION_SECRET", weak)
    with pytest.raises(ValueError, match="at least"):
        auth.session_secret()


def test_session_secret_falls_back_to_an_ephemeral_key(monkeypatch):
    monkeypatch.delenv("TEXT2SQL_SESSION_SECRET", raising=False)
    first, second = auth.session_secret(), auth.session_secret()
    assert first and second and first != second, "fallback must not be a fixed key"


# --- routes ---------------------------------------------------------------


def test_login_is_unavailable_when_not_configured(client, monkeypatch):
    monkeypatch.setattr(auth, "is_configured", lambda: False)
    resp = client.get("/api/auth/login", follow_redirects=False)
    assert resp.status_code == 503
    assert "not configured" in resp.json()["detail"]


def test_logout_is_harmless_without_a_session(client):
    """Anonymous sign-out is a no-op, not an error."""
    resp = client.post("/api/auth/logout")
    assert resp.status_code == 200
    assert resp.json() == {"signed_out": True}


@pytest.mark.parametrize(
    "target",
    [
        "https://evil.example.com",
        "//evil.example.com",
        "http://evil.example.com",
        "/\\evil.example.com",
        "evil.example.com",
        "",
        None,
        123,
    ],
)
def test_offsite_redirect_targets_fall_back_to_root(target):
    """
    A crafted sign-in link must not bounce a freshly authenticated user to
    another origin.
    """
    assert auth.safe_redirect_target(target) == "/"


@pytest.mark.parametrize(
    "target",
    ["/", "/b/spider_dev/errors", "/b/x/errors?page=2&record=abc", "/run"],
)
def test_same_site_paths_are_preserved(target):
    assert auth.safe_redirect_target(target) == target


def test_login_refuses_clearly_when_no_session_middleware_is_installed(monkeypatch):
    """
    SessionMiddleware is installed by `main()`, so serving the ASGI app directly
    with Google credentials set produces a server that advertises sign-in and
    then raises deep inside Starlette. That surfaced as a 500 with no
    explanation for a misconfiguration that has an exact fix.
    """
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "id")
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "secret")

    from fastapi.testclient import TestClient

    from text2sql_eval_toolkit.ui import server

    client = TestClient(server.app, raise_server_exceptions=False)
    resp = client.get("/api/auth/login", follow_redirects=False)

    assert resp.status_code == 503
    assert "session middleware" in resp.text.lower()
    assert "text2sql-eval-dashboard" in resp.text


def test_callback_refuses_the_same_way(monkeypatch):
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "id")
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "secret")

    from fastapi.testclient import TestClient

    from text2sql_eval_toolkit.ui import server

    client = TestClient(server.app, raise_server_exceptions=False)
    resp = client.get("/api/auth/callback?code=x&state=y", follow_redirects=False)

    assert resp.status_code == 503
    assert "session middleware" in resp.text.lower()
