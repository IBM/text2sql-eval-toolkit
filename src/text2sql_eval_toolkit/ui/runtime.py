#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Deployment-wide runtime state for the dashboard server.

Three things that every other module needs and that must have exactly one
value per process: the deployment ceiling, the judge allowlist, and how a
request's identity is read.  They live here rather than in ``server`` so that
the middleware, the path helpers and the routers can all reach them without
importing the module that imports them.

Mode and allowlist are read through accessors rather than exported as values.
That is deliberate: a module holding ``from ... import _MODE`` would capture
whatever the ceiling was at import time and never see ``set_mode``, which is
how a test -- or ``main()`` -- would silently run against the wrong tier.
"""

from __future__ import annotations

import os
from typing import Optional

from fastapi import Request

from text2sql_eval_toolkit.logging import get_logger
from text2sql_eval_toolkit.ui.capabilities import Tier
from text2sql_eval_toolkit.ui.roles import admin_emails_from_env

logger = get_logger(__name__)


def _cookie_secure(mode: Tier) -> bool:
    """Whether the session cookie carries `Secure`. Secure unless explicitly
    opted out for a local HTTP run."""
    raw = os.getenv("TEXT2SQL_COOKIE_SECURE")
    if raw is not None:
        return raw.strip().lower() in {"1", "true", "yes"}
    return mode is not Tier.FULL


def _mode_from_env() -> Tier:
    """
    Deployment ceiling from the environment.

    Resolved at import, not only inside main(), because the ASGI app is also
    served directly (``uvicorn text2sql_eval_toolkit.ui.server:app``). Reading it
    only in main() meant such a deployment silently ran at FULL -- every
    endpoint, including SQL execution, open to anonymous callers -- while the
    operator believed TEXT2SQL_DASHBOARD_MODE had taken effect.

    Unset means PUBLIC here, and that asymmetry with the CLI is the point. The
    "full is loopback-only" guarantee is enforced in main(), which refuses a
    non-loopback bind without --allow-remote-full. Serving the ASGI app directly
    skips that check, so defaulting *this* path to FULL would hand anonymous SQL
    execution to `uvicorn ... --host 0.0.0.0`, with the one guard that would have
    stopped it bypassed. main() sets the mode explicitly from its own --mode
    default of full, so the local operator tool is unaffected.
    """
    raw = os.getenv("TEXT2SQL_DASHBOARD_MODE")
    if not raw:
        return Tier.PUBLIC
    try:
        return Tier.parse(raw)
    except ValueError:
        logger.warning(
            "TEXT2SQL_DASHBOARD_MODE=%r is not a valid mode; refusing to guess "
            "and falling back to the most restrictive setting.",
            raw,
        )
        return Tier.PUBLIC


# Deployment ceiling. `text2sql-eval-dashboard` keeps every capability it has
# today -- main() sets FULL explicitly, behind the loopback guard. A public
# deployment lowers the ceiling and no sign-in can raise it back.
_MODE: Tier = _mode_from_env()

# Addresses that always hold admin, from TEXT2SQL_ADMIN_EMAILS. Read at every
# startup and never overridden by a stored row: this is the recovery path for a
# deployment whose role table is wrong, and removing TEXT2SQL_JUDGE_ALLOWLIST
# left it as the only one.
_ADMIN_EMAILS: set = admin_emails_from_env()

# The role table. None until a deployment configures one, in which case only
# TEXT2SQL_ADMIN_EMAILS grants anything above read-only.
_USER_STORE = None


def get_mode() -> Tier:
    return _MODE


def set_mode(mode: Tier) -> None:
    global _MODE
    _MODE = mode


def get_admin_emails() -> set:
    """Addresses that always hold admin, from the environment."""
    return _ADMIN_EMAILS


def get_user_store():
    """The role table, or ``None`` when none is configured."""
    return _USER_STORE


def set_user_store(store) -> None:
    global _USER_STORE
    _USER_STORE = store


def set_admin_emails(allowlist: set) -> None:
    global _ADMIN_EMAILS
    _ADMIN_EMAILS = allowlist


def current_user_email(request: Request) -> Optional[str]:
    """
    Identity for the request.

    Sign-in lands in a later step; until then only the local operator exists,
    and FULL mode does not consult identity at all.
    """
    # request.session raises unless SessionMiddleware is installed, and it is
    # not in local mode, so check the scope rather than the property.
    session = request.scope.get("session")
    if isinstance(session, dict):
        email = session.get("email")
        if isinstance(email, str):
            return email
    return None


# When True the /api/results/fetch endpoints are active.  Set by main() via the
# --enable-fetch CLI flag.  Off by default so a deployment is safe with no
# configuration at all: the endpoint downloads gigabytes into the data root.
_ENABLE_FETCH_ENDPOINT: bool = False


def fetch_endpoint_enabled() -> bool:
    return _ENABLE_FETCH_ENDPOINT


def set_fetch_endpoint_enabled(enabled: bool) -> None:
    global _ENABLE_FETCH_ENDPOINT
    _ENABLE_FETCH_ENDPOINT = enabled
