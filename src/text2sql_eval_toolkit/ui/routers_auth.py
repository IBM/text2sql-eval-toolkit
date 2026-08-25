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

# Runtime state (deployment ceiling, judge allowlist, request identity) and the
# middleware stack live in their own modules.  Their names are re-exported here
# because ``server.set_mode`` / ``server.reset_rate_limits`` are the surface the
# CLI and the tests already use -- and re-exporting is only safe because they are
# accessors over module state rather than the state itself.
from text2sql_eval_toolkit.ui.indexes import (  # noqa: F401
    EVAL_INDEX_CACHE,
    get_index,
    invalidate_index_cache,
)
from text2sql_eval_toolkit.ui.paths import (  # noqa: F401
    _eval_not_found_detail,
    _summary_not_found_detail,
    count_records,
    get_data_root,
    get_results_dir,
    load_json,
)
from text2sql_eval_toolkit.ui.registry import (  # noqa: F401
    ALLOWED_DB_TYPES,
    ALLOWED_LOGO_EXTENSIONS,
    MAX_LOGO_UPLOAD_BYTES,
    STATIC_ASSET_SUBDIR,
    get_benchmark_registry_path,
    load_benchmark_registry,
    normalize_benchmark_config,
    normalize_benchmark_id,
    write_json_atomic,
)
from text2sql_eval_toolkit.ui.middleware import reset_rate_limits  # noqa: F401
from text2sql_eval_toolkit.ui.runtime import (  # noqa: F401
    _cookie_secure,
    current_user_email,
    get_judge_allowlist,
    get_mode,
    set_judge_allowlist,
    set_mode,
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
