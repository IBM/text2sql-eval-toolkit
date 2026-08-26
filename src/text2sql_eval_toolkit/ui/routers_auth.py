#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Google sign-in.

Identity exists for exactly one purpose: deciding whether a caller is on the
judge allowlist.  Nothing else in the dashboard is per-user, and no profile is
stored -- the session holds a verified email address and nothing more.
"""

from typing import Any

from fastapi import (
    APIRouter,
    HTTPException,
    Query,
    Request,
)
from fastapi.responses import RedirectResponse

import text2sql_eval_toolkit.env_loader  # noqa: F401 — load .env (WATSONX_*, etc.) before eval/inference

from text2sql_eval_toolkit.ui import auth
from text2sql_eval_toolkit.ui.capabilities import (
    resolve_tier,
)

from text2sql_eval_toolkit.ui.runtime import (
    get_judge_allowlist,
    get_mode,
)
from text2sql_eval_toolkit.logging import get_logger

logger = get_logger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Authentication (Google OIDC)
# ---------------------------------------------------------------------------

_OAUTH: Any = None


def get_oauth() -> Any:
    """Lazily built so importing the module never requires OAuth credentials."""
    global _OAUTH
    if _OAUTH is None:
        _OAUTH = auth.build_oauth_client()
    return _OAUTH


def _require_session(request: Request) -> None:
    """
    Refuse clearly when sign-in is configured but the session is not.

    SessionMiddleware is installed by ``main()``, so serving the ASGI app
    directly -- ``uvicorn text2sql_eval_toolkit.ui.server:app`` -- with Google
    credentials in the environment gives a server that advertises sign-in and
    then raises an AssertionError deep in Starlette on the first attempt. A 500
    with no explanation is the worst possible answer to a misconfiguration that
    has an exact fix.
    """
    if "session" not in request.scope:
        raise HTTPException(
            status_code=503,
            detail=(
                "Google sign-in is configured but no session middleware is "
                "installed, so the OAuth state cannot be stored. Start the "
                "dashboard through `text2sql-eval-dashboard` rather than "
                "serving the ASGI app directly."
            ),
        )


@router.get("/api/auth/login")
async def auth_login(request: Request, next: str = Query("/")):
    """Start the Google sign-in redirect."""
    if not auth.is_configured():
        raise HTTPException(
            status_code=503,
            detail=(
                "Google sign-in is not configured on this server "
                "(GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET are unset)."
            ),
        )
    _require_session(request)
    request.session["post_login_redirect"] = auth.safe_redirect_target(next)
    redirect_uri = str(request.url_for("auth_callback"))
    return await get_oauth().google.authorize_redirect(request, redirect_uri)


@router.get("/api/auth/callback", name="auth_callback")
async def auth_callback(request: Request):
    """
    Complete sign-in.

    Authlib verifies the ID token signature, issuer, audience, and nonce, and
    checks the PKCE and state values against the session. What is left for us is
    the claim that actually matters: the address must be *verified*.
    """
    if not auth.is_configured():
        raise HTTPException(status_code=503, detail="Google sign-in is not configured.")
    _require_session(request)

    try:
        token = await get_oauth().google.authorize_access_token(request)
    except Exception as e:
        logger.warning("Sign-in failed during token exchange: %s", e)
        raise HTTPException(status_code=401, detail="Sign-in failed.") from None

    email = auth.extract_verified_email(token.get("userinfo") or {})
    if not email:
        raise HTTPException(
            status_code=403,
            detail=("Sign-in requires a Google account with a verified email address."),
        )

    request.session["email"] = email
    tier = resolve_tier(get_mode(), email, get_judge_allowlist())
    logger.info(
        "Sign-in for identity %s granted tier %s",
        auth.hash_identity(email),
        tier.name.lower(),
    )

    target = request.session.pop("post_login_redirect", "/")
    return RedirectResponse(url=target, status_code=303)


@router.post("/api/auth/logout")
def auth_logout(request: Request):
    """Clear the session. The cookie is the whole of the state."""
    session = request.scope.get("session")
    if isinstance(session, dict):
        session.clear()
    return {"signed_out": True}
