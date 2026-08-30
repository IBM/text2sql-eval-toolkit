#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
The dashboard's middleware stack: authorization, rate limiting, and response
hardening.

Kept in one module because the three are ordered with respect to each other and
that ordering is load-bearing.  ``install()`` registers them in the sequence
they must run in; Starlette inserts each new entry at the *front* of the stack,
so registration order here is the reverse of execution order.  Execution is:

    add_security_headers -> rate_limit -> CORS -> enforce_capability_tier

Getting this wrong is not a style question.  Authorization innermost means CORS
preflights are answered without a tier check (correct -- they carry no
credentials and no side effects), and the security headers apply to every
response including the 403s and 429s the other two produce.
"""

from __future__ import annotations

import os
import threading
import time
from typing import Dict, Optional, Tuple

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.routing import get_route_path

from text2sql_eval_toolkit.ui import runtime
from text2sql_eval_toolkit.ui.capabilities import (
    Tier,
    iter_routes,
    required_tier,
    resolve_tier,
    requires_admin,
)
from text2sql_eval_toolkit.ui.roles import ROLE_TIERS, Role, effective_role


def _route_template(request: Request) -> Optional[str]:
    """
    The matched route's path template (``/api/benchmarks/{benchmark_id}/execute``).

    Returns None when nothing matches, which leaves the caller with the concrete
    path -- and therefore the fail-closed FULL default for mutating methods.

    The route table comes from ``request.app`` rather than a module-level
    reference, so this stays correct for any app the stack is installed on, and
    the walk descends into included routers -- which are wrapper objects with no
    ``path`` of their own, so a flat iteration would match none of their routes
    and silently fall back to the concrete path.
    """
    from starlette.routing import Match

    for route in iter_routes(request.app):
        try:
            match, _ = route.matches(request.scope)
        except Exception:  # pragma: no cover - defensive
            continue
        if match is Match.FULL:
            return getattr(route, "path", None)
    return None


async def enforce_capability_tier(request: Request, call_next):
    """
    Single choke point for authorization.

    Applied as middleware rather than per-handler so a route added later is
    covered whether or not its author remembered. Undeclared mutating routes
    require FULL, so the failure mode is a locked door rather than an open one.
    """
    # Starlette routes on get_route_path(), which strips scope["root_path"].
    # Gating on the raw scope path instead meant that under a non-empty
    # root_path -- an app mounted at a sub-path, or uvicorn --root-path -- this
    # check returned early while the router still dispatched the handler,
    # skipping authorization entirely.
    path = get_route_path(request.scope)
    if not path.startswith("/api/"):
        return await call_next(request)

    # HTTP middleware runs before routing, so scope["route"] is not set yet;
    # resolve the template ourselves. Matching the template rather than the
    # concrete path means ids in the URL cannot be used to dodge a rule.
    template = _route_template(request) or path
    email = runtime.current_user_email(request)
    role = effective_role(email, runtime.get_user_store(), runtime.get_admin_emails())

    # Admin is asked before the tier, and separately: user management must work
    # on a judge-mode host, where the ceiling denies full.
    #
    # A `full` deployment is the local operator tool, and they already control
    # the process -- the same reasoning that makes resolve_tier grant full there
    # without a sign-in. Requiring TEXT2SQL_ADMIN_EMAILS to use the console on a
    # laptop would be ceremony, not security.
    is_admin = role is Role.ADMIN or (
        runtime.get_mode() is Tier.FULL and not runtime.is_remote_deployment()
    )
    if requires_admin(request.method, template) and not is_admin:
        return JSONResponse(
            status_code=403,
            content={"detail": "This endpoint requires an administrator."},
        )

    needed = required_tier(request.method, template)
    granted = resolve_tier(
        runtime.get_mode(), email, ROLE_TIERS[role], runtime.is_remote_deployment()
    )

    if granted < needed:
        return JSONResponse(
            status_code=403,
            content={
                "detail": (
                    f"This endpoint requires the '{needed.name.lower()}' capability; "
                    f"this session has '{granted.name.lower()}'. "
                    "The public dashboard is read-only."
                )
            },
        )
    return await call_next(request)


# Allow local dev frontends by default
# The Vite dev server runs on a different port, so local development needs CORS
# with credentials. A shared deployment serves the UI from the same origin and
# needs neither -- and `allow_credentials` with a permissive origin list stops
# being theoretical once session cookies exist, so the allowance is withdrawn
# outside full mode by `configure_cors()` below.
_DEV_ORIGINS = [
    "http://localhost",
    "http://localhost:5173",
    "http://127.0.0.1:5173",
]


def configure_cors(app: FastAPI, mode: Tier) -> None:
    """
    Narrow CORS for shared deployments.

    Middleware cannot be removed once the app has started, so the instance is
    reconfigured in place: outside full mode the origin list is emptied and
    credentialed cross-origin requests are refused.
    """
    for entry in app.user_middleware:
        if entry.cls is not CORSMiddleware:
            continue
        kwargs = getattr(entry, "kwargs", None)
        if not isinstance(kwargs, dict):
            continue
        if mode is Tier.FULL:
            kwargs["allow_origins"] = list(_DEV_ORIGINS)
            kwargs["allow_credentials"] = True
        else:
            kwargs["allow_origins"] = []
            kwargs["allow_credentials"] = False
    # Clear rather than rebuild: Starlette refuses add_middleware() once the
    # stack exists, and eagerly rebuilding here made a later
    # add_middleware(SessionMiddleware) raise -- which crashed startup for every
    # deployment that configured Google sign-in. None lets it rebuild on demand
    # and keeps the ordering decision with whoever adds middleware last.
    app.middleware_stack = None


# In-process token buckets, keyed by client address. Adequate for a
# single-container deployment; a multi-replica setup would need shared state,
# and the reverse proxy should carry a limit of its own regardless.
_RATE_BUCKETS: Dict[str, Tuple[float, float]] = {}
_RATE_LOCK = threading.Lock()

#: Requests per second sustained, and the burst allowance above it.
RATE_LIMIT_RPS = float(os.getenv("TEXT2SQL_RATE_LIMIT_RPS", "20"))
RATE_LIMIT_BURST = float(os.getenv("TEXT2SQL_RATE_LIMIT_BURST", "60"))

#: Sign-in is cheaper to abuse than to serve, so it gets its own tighter bucket.
AUTH_RATE_LIMIT_RPS = float(os.getenv("TEXT2SQL_AUTH_RATE_LIMIT_RPS", "1"))
AUTH_RATE_LIMIT_BURST = float(os.getenv("TEXT2SQL_AUTH_RATE_LIMIT_BURST", "10"))


#: Peer addresses whose X-Forwarded-For header is believed. Empty by default:
#: a proxy *appends* to that header, so its leftmost value is whatever the
#: client sent. Honouring it unconditionally let anyone rotate the header to
#: get a fresh bucket per request, defeating the limit entirely and growing the
#: bucket map without bound.
TRUSTED_PROXIES = {
    addr.strip()
    for addr in os.getenv("TEXT2SQL_TRUSTED_PROXIES", "").split(",")
    if addr.strip()
}

#: Ceiling on tracked buckets, so the key space cannot grow without limit.
MAX_RATE_BUCKETS = 10_000


def _client_key(request: Request) -> str:
    client = request.client
    peer = client.host if client else "unknown"

    if peer in TRUSTED_PROXIES:
        forwarded = request.headers.get("x-forwarded-for")
        if forwarded:
            # Rightmost hop, which the trusted proxy appended itself; the
            # leftmost is client-supplied and therefore forgeable.
            return forwarded.split(",")[-1].strip() or peer
    return peer


def _take_token(key: str, rps: float, burst: float) -> bool:
    """Classic token bucket. Returns False when the caller is over budget."""
    if rps <= 0:
        return True
    now = time.monotonic()
    with _RATE_LOCK:
        if key not in _RATE_BUCKETS and len(_RATE_BUCKETS) >= MAX_RATE_BUCKETS:
            # First drop buckets that have refilled completely: they are
            # indistinguishable from a fresh one, so forgetting them costs
            # nothing.
            for stale, (t, ts) in list(_RATE_BUCKETS.items()):
                if min(burst, t + (now - ts) * rps) >= burst:
                    del _RATE_BUCKETS[stale]
            # If that was not enough, evict least-recently-seen until under the
            # cap. Sweeping alone is only a soft bound, and the key space is
            # attacker-influenced, so a hard ceiling is what is needed.
            if len(_RATE_BUCKETS) >= MAX_RATE_BUCKETS:
                for stale, _ in sorted(_RATE_BUCKETS.items(), key=lambda kv: kv[1][1])[
                    : len(_RATE_BUCKETS) - MAX_RATE_BUCKETS + 1
                ]:
                    del _RATE_BUCKETS[stale]
        tokens, last = _RATE_BUCKETS.get(key, (burst, now))
        tokens = min(burst, tokens + (now - last) * rps)
        if tokens < 1.0:
            _RATE_BUCKETS[key] = (tokens, now)
            return False
        _RATE_BUCKETS[key] = (tokens - 1.0, now)
        return True


def reset_rate_limits() -> None:
    with _RATE_LOCK:
        _RATE_BUCKETS.clear()


async def rate_limit(request: Request, call_next):
    """
    Coarse per-client rate limiting.

    Local mode is exempt: it is one operator on loopback, and throttling an
    interactive tool would be a regression for no benefit.
    """
    # Same root_path correction as the tier gate above: throttling must not
    # silently switch off on a sub-path deployment.
    path = get_route_path(request.scope)
    if runtime.get_mode() is Tier.FULL or not path.startswith("/api/"):
        return await call_next(request)

    is_auth = path.startswith("/api/auth/")
    rps = AUTH_RATE_LIMIT_RPS if is_auth else RATE_LIMIT_RPS
    burst = AUTH_RATE_LIMIT_BURST if is_auth else RATE_LIMIT_BURST
    key = f"{'auth' if is_auth else 'api'}:{_client_key(request)}"

    if not _take_token(key, rps, burst):
        return JSONResponse(
            status_code=429,
            content={"detail": "Too many requests. Please slow down."},
            headers={"Retry-After": "1"},
        )
    return await call_next(request)


async def add_security_headers(request: Request, call_next):
    """
    Baseline response hardening.

    The dashboard is self-contained -- its own bundle, its own API, no third
    party scripts or frames -- so a restrictive policy costs nothing here and
    removes a class of injection. Carbon injects styles at runtime, hence
    'unsafe-inline' for style-src only.
    """
    response = await call_next(request)
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("Referrer-Policy", "same-origin")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault(
        "Content-Security-Policy",
        "default-src 'self'; "
        "img-src 'self' data:; "
        "style-src 'self' 'unsafe-inline'; "
        "script-src 'self'; "
        "connect-src 'self'; "
        # Carbon's stylesheet references IBM Plex from IBM's CDN in 120 places.
        # Without this the policy blocks every one of them and the UI silently
        # falls back to system fonts -- which is what happened when the CSP was
        # first added. Self-hosting the fonts would remove this origin, and with
        # it the visitor IPs disclosed to that CDN; until then it is allowed
        # explicitly rather than by loosening default-src.
        "font-src 'self' data: https://1.www.s81c.com; "
        "frame-ancestors 'none'; "
        "base-uri 'self'; "
        "form-action 'self'",
    )
    return response


def install(app: FastAPI) -> None:
    """
    Register the middleware stack on ``app``.

    Order matters and is the reverse of execution order -- see the module
    docstring.  Called once, at import of ``server``.
    """
    app.middleware("http")(enforce_capability_tier)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_DEV_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.middleware("http")(rate_limit)
    app.middleware("http")(add_security_headers)
