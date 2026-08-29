#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
"""
User management, for admins.

Changing who may reach the judge used to mean editing ``deploy/.env`` and
recreating the container. These routes do it from the dashboard.

Every route here is admin-only, enforced in middleware via
``capabilities.ADMIN_ROUTES`` rather than by a check in each handler -- the same
reason the tier system works that way, so a new route cannot be added without a
decision about who may call it.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from text2sql_eval_toolkit.logging import get_logger
from text2sql_eval_toolkit.ui import auth, runtime
from text2sql_eval_toolkit.ui.capabilities import Tier
from text2sql_eval_toolkit.ui.roles import ROLE_TIERS, Role

logger = get_logger(__name__)

router = APIRouter()


class GrantRequest(BaseModel):
    email: str
    role: str


def _store():
    store = runtime.get_user_store()
    if store is None:
        raise HTTPException(
            status_code=503,
            detail="No role store is configured on this deployment.",
        )
    return store


def _describe(email: str, role: Role, mode: Tier) -> Dict[str, Any]:
    """
    One row, plus whether this deployment can actually honour it.

    A grant the mode ceiling denies is accepted and recorded -- the operator may
    raise the ceiling later -- but it must not look effective. A permission that
    appears granted and is refused is how people conclude the system is broken.
    """
    wanted = ROLE_TIERS[role]
    effective = min(wanted, mode)
    return {
        "email": email,
        "role": role.value,
        "effective_tier": effective.name.lower(),
        "active": effective is wanted,
        "inactive_reason": (
            None
            if effective is wanted
            else (
                f"This deployment runs in '{mode.name.lower()}' mode, which is a "
                f"ceiling, so '{role.value}' grants only "
                f"'{effective.name.lower()}' here. Restart with a higher "
                "TEXT2SQL_DASHBOARD_MODE (and --allow-remote-full for full) to "
                "activate it."
            )
        ),
    }


@router.get("/api/users")
def list_users(request: Request) -> Dict[str, Any]:
    """
    Every stored role, and the addresses that hold admin from the environment.

    Returns:
        dict: ``users`` (stored rows) and ``env_admins`` (addresses from
        ``TEXT2SQL_ADMIN_EMAILS``, which cannot be edited here -- they are the
        recovery path and are deliberately only changeable with shell access).
    """
    mode = runtime.get_mode()
    rows: List[Dict[str, Any]] = []
    for row in _store().list_users():
        try:
            role = Role.parse(row["role"])
        except ValueError:
            continue
        described = _describe(row["email"], role, mode)
        described["granted_by"] = row.get("granted_by")
        described["granted_at"] = row.get("granted_at")
        rows.append(described)

    env_admins = sorted(runtime.get_admin_emails())
    return {
        "users": rows,
        "env_admins": env_admins,
        "mode": mode.name.lower(),
        "roles": [r.value for r in Role],
    }


@router.post("/api/users")
def grant_role(req: GrantRequest, request: Request) -> Dict[str, Any]:
    """
    Grant a role to an address.

    The address is matched against the verified ``email`` claim at sign-in, so it
    must be exactly what the identity provider returns -- Gmail's dot and ``+tag``
    variants reach the same inbox but are different strings.

    Raises:
        HTTPException: 400 for an unknown role or an empty address.
    """
    email = (req.email or "").strip().lower()
    if not email or "@" not in email:
        raise HTTPException(
            status_code=400, detail="A valid email address is required."
        )
    try:
        role = Role.parse(req.role)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None

    granted_by = runtime.current_user_email(request)
    _store().grant(email, role, granted_by)
    logger.info(
        "Role %s granted to %s by %s",
        role.value,
        auth.hash_identity(email),
        auth.hash_identity(granted_by or "unknown"),
    )
    return _describe(email, role, runtime.get_mode())


@router.delete("/api/users/{email}")
def revoke_role(email: str, request: Request) -> Dict[str, Any]:
    """
    Remove an address's stored role, returning it to read-only.

    An address named in ``TEXT2SQL_ADMIN_EMAILS`` keeps admin regardless: that is
    what makes it a recovery path. The response says so rather than reporting a
    revocation that did not take effect.
    """
    normalised = (email or "").strip().lower()
    removed = _store().revoke(normalised)
    still_admin = normalised in runtime.get_admin_emails()
    if still_admin:
        logger.info(
            "Stored role removed for %s, which still holds admin from %s",
            auth.hash_identity(normalised),
            "TEXT2SQL_ADMIN_EMAILS",
        )
    return {
        "email": normalised,
        "removed": removed,
        "still_admin_from_environment": still_admin,
    }


def current_role(request: Request) -> Optional[str]:
    """The caller's role name, for ``/api/me``."""
    from text2sql_eval_toolkit.ui.roles import effective_role

    email = runtime.current_user_email(request)
    return effective_role(
        email, runtime.get_user_store(), runtime.get_admin_emails()
    ).value
