#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
"""
A signed-in user's own provider credentials.

Every route is scoped to the caller: there is no way to read, write or delete
another user's key, and no admin override -- an administrator can set someone's
*spending cap*, which is a different thing from holding their credential.

Nothing here returns key material in any form.
"""

from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from text2sql_eval_toolkit.logging import get_logger
from text2sql_eval_toolkit.ui import runtime
from text2sql_eval_toolkit.ui.user_keys import (
    PROVIDERS,
    SECONDARY_LABELS,
    SecretsUnavailable,
)

logger = get_logger(__name__)

router = APIRouter()


class StoreKeyRequest(BaseModel):
    provider: str
    api_key: str
    label: str = ""
    # Companion value for providers that need one; watsonx's project id.
    secondary: str = ""


def _caller(request: Request) -> str:
    email = runtime.current_user_email(request)
    if not email:
        raise HTTPException(status_code=403, detail="Sign in to manage your API keys.")
    return email


def _store():
    store = runtime.get_user_key_store()
    if store is None:
        raise HTTPException(
            status_code=503,
            detail="This deployment does not store per-user API keys.",
        )
    return store


@router.get("/api/my/keys")
def list_my_keys(request: Request) -> Dict[str, Any]:
    """
    Which providers the caller has a key stored for.

    Returns presence, label and last-used time. **Never the key**, in any form:
    not masked, not truncated, not the last four characters. A reveal affordance
    is what turns a leaked backup into a breach, and there is deliberately no
    endpoint that could serve one.
    """
    return {
        "keys": _store().describe(_caller(request)),
        "providers": list(PROVIDERS),
        # So the form can ask for a project id where one is needed, rather than
        # accepting a credential the server will then refuse.
        "secondary_labels": SECONDARY_LABELS,
    }


@router.post("/api/my/keys")
def store_my_key(req: StoreKeyRequest, request: Request) -> Dict[str, Any]:
    """
    Save or replace the caller's key for one provider.

    Raises:
        HTTPException: 400 for an unknown provider or empty key, 503 when the
            deployment has no master key configured.
    """
    email = _caller(request)
    try:
        _store().store(email, req.provider, req.api_key, req.label, req.secondary)
    except SecretsUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from None
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    return {"provider": req.provider.strip().lower(), "stored": True}


@router.delete("/api/my/keys/{provider}")
def delete_my_key(provider: str, request: Request) -> Dict[str, Any]:
    """Remove the caller's key for one provider."""
    removed = _store().delete(_caller(request), provider)
    return {"provider": provider.strip().lower(), "removed": removed}
