#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Google sign-in for the public dashboard.

Only a handful of people ever need more than read access, so this talks to
Google directly rather than through an identity service: for an allowlist this
small, Auth0 or Clerk would add cost and a dependency for nothing, and Google
OAuth itself is free.

What the rest of the app gets from this module is one thing -- a verified email
address in the session -- which `capabilities` turns into a tier.

Security notes that are easy to get wrong:

* **The ``email`` claim alone is not trustworthy.** Google also returns
  ``email_verified``; an account whose address has not been verified must not
  match the allowlist, or the allowlist means nothing.
* Authorization-code flow with PKCE and ``state``. Authlib handles both, but the
  ``state`` check depends on the session cookie surviving the round trip, so the
  cookie must be ``SameSite=Lax`` rather than ``Strict``.
* Sessions store the email and nothing else, so there is no user database to
  secure and signing out is simply clearing the cookie.
* Logs carry a hash of the address, never the address itself.
"""

from __future__ import annotations

import hashlib
import os
import secrets
from typing import Any, Optional

from text2sql_eval_toolkit.logging import get_logger

logger = get_logger(__name__)

GOOGLE_METADATA_URL = "https://accounts.google.com/.well-known/openid-configuration"

#: How long a session stays valid. Short enough that removing someone from the
#: allowlist takes effect quickly, long enough not to be a nuisance.
SESSION_MAX_AGE_SECONDS = 12 * 60 * 60

#: Minimum length for a session signing key.
MIN_SESSION_SECRET_LENGTH = 32


class AuthNotConfigured(RuntimeError):
    """Raised when a sign-in route is reached without OAuth credentials."""


def is_configured() -> bool:
    return bool(os.getenv("GOOGLE_CLIENT_ID") and os.getenv("GOOGLE_CLIENT_SECRET"))


def session_secret() -> str:
    """
    Key for signing the session cookie.

    A generated key is fine for a single-process local run, but it changes on
    restart and would silently sign everyone out -- and would not work at all
    across replicas -- so a deployment must set it explicitly.
    """
    secret = os.getenv("TEXT2SQL_SESSION_SECRET")
    if secret:
        # A short key is forgeable, and forging {"email": "<allowlisted>"}
        # grants the judge tier outright.
        if len(secret) < MIN_SESSION_SECRET_LENGTH:
            raise ValueError(
                "TEXT2SQL_SESSION_SECRET must be at least "
                f"{MIN_SESSION_SECRET_LENGTH} characters. Generate one with: "
                'python -c "import secrets; print(secrets.token_urlsafe(48))"'
            )
        return secret
    logger.warning(
        "TEXT2SQL_SESSION_SECRET is not set; generating an ephemeral key. "
        "Sessions will not survive a restart. Set it for any real deployment."
    )
    return secrets.token_urlsafe(48)


def hash_identity(email: str) -> str:
    """Stable, non-reversible id for logs."""
    return hashlib.sha256(email.strip().lower().encode("utf-8")).hexdigest()[:12]


def extract_verified_email(claims: Any) -> Optional[str]:
    """
    Pull a *verified* email out of Google's ID token claims.

    Returns None when the token carries no email or when Google has not verified
    it -- an unverified address must never match the allowlist.

    ``email_verified`` arrives as a bool from Google but as the string "true"
    from some OIDC providers, so both are accepted; anything else is a refusal.
    """
    if not isinstance(claims, dict):
        return None

    email = claims.get("email")
    if not isinstance(email, str) or not email.strip():
        return None

    verified = claims.get("email_verified")
    if verified is True:
        return email.strip().lower()
    if isinstance(verified, str) and verified.strip().lower() == "true":
        return email.strip().lower()

    logger.warning(
        "Refusing sign-in for identity %s: email_verified is %r",
        hash_identity(email),
        verified,
    )
    return None


def safe_redirect_target(raw: Optional[str]) -> str:
    """
    Sanitise the post-sign-in redirect.

    Only same-site absolute paths are allowed, so a crafted sign-in link cannot
    bounce a freshly authenticated user to another origin. Anything else -- an
    absolute URL, a protocol-relative ``//host`` path, a backslash variant that
    some browsers normalise to ``//`` -- falls back to the root.
    """
    if not raw or not isinstance(raw, str):
        return "/"
    candidate = raw.strip()
    if not candidate.startswith("/"):
        return "/"
    # "//host" and "/\host" are both off-site in practice.
    if candidate.startswith("//") or candidate.startswith("/\\"):
        return "/"
    if "\\" in candidate[:2]:
        return "/"
    return candidate


def build_oauth_client() -> Any:
    """
    Configure Authlib against Google's discovery document.

    Discovery means issuer, JWKS, and endpoints come from Google rather than
    being hardcoded, so key rotation needs no change here.
    """
    if not is_configured():
        raise AuthNotConfigured(
            "Google sign-in requires GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET."
        )

    from authlib.integrations.starlette_client import OAuth

    oauth = OAuth()
    oauth.register(
        name="google",
        client_id=os.getenv("GOOGLE_CLIENT_ID"),
        client_secret=os.getenv("GOOGLE_CLIENT_SECRET"),
        server_metadata_url=GOOGLE_METADATA_URL,
        client_kwargs={
            "scope": "openid email",
            # PKCE. Authlib generates and verifies the challenge.
            "code_challenge_method": "S256",
        },
    )
    return oauth
