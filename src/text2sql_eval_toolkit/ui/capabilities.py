#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Capability tiers for the dashboard.

The server began as a local operator tool for a trusted single user on
``127.0.0.1``: it will execute caller-supplied SQL against configured database
credentials, spend LLM budget, rewrite the benchmark registry, and overwrite
YAML inside its own installed package -- all unauthenticated.  None of that can
go on the public internet, and adding auth checks to twenty-eight handlers by
hand guarantees that the twenty-ninth is forgotten.

So capability is resolved once per request from the deployment mode and the
caller's identity, and enforced centrally:

``public``
    Anonymous, or signed in without an allowlist entry.  Read-only.
``judge``
    Signed in *and* allowlisted.  Adds on-demand LLM-as-judge, which needs no
    database -- only artifact data and a watsonx key.
``full``
    The local operator, bound to loopback.  Everything, as today.

Two rules keep this honest:

* **Deny by default.** A mutating route with no declared tier requires ``full``,
  so forgetting to classify a new endpoint fails closed rather than open.
* **Nothing is classified implicitly.** ``unclassified_routes()`` reports any
  mutating route missing from the table below, and a test fails on it.
"""

from __future__ import annotations

from enum import IntEnum
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Set, Tuple

if TYPE_CHECKING:  # pragma: no cover
    from fastapi import FastAPI


class Tier(IntEnum):
    """Ordered so a simple ``>=`` answers "is this caller allowed?"."""

    PUBLIC = 0
    JUDGE = 1
    FULL = 2

    @classmethod
    def parse(cls, value: str) -> "Tier":
        try:
            return cls[value.strip().upper().replace("-", "_")]
        except KeyError:
            raise ValueError(
                f"Unknown tier '{value}'. Expected one of: "
                + ", ".join(t.name.lower() for t in cls)
            ) from None


#: Methods that only read.  Everything else is treated as mutating.
SAFE_METHODS: Set[str] = {"GET", "HEAD", "OPTIONS"}

#: Minimum tier per mutating route.  Every mutating route must appear here --
#: see ``unclassified_routes``.  Read routes need no entry: they are PUBLIC.
ROUTE_TIERS: Dict[Tuple[str, str], Tier] = {
    # Signing out must work at whatever tier you signed in at, and an anonymous
    # sign-out is a harmless no-op.
    ("POST", "/api/auth/logout"): Tier.PUBLIC,
    # Runs the LLM judge for a single record using the server's watsonx key.
    # Needs no database, so it is safe to expose to an allowlisted signed-in
    # user on a deployment that holds no DB credentials at all.
    ("POST", "/api/benchmarks/{benchmark_id}/judge"): Tier.JUDGE,
    # --- Everything below touches a database, spends budget, or writes state.
    # Executes arbitrary caller-supplied SQL against configured credentials.
    ("POST", "/api/benchmarks/{benchmark_id}/execute"): Tier.FULL,
    ("POST", "/api/benchmarks/{benchmark_id}/playground/evaluate"): Tier.FULL,
    # Re-evaluates a whole benchmark and rewrites the shared artifacts.
    ("POST", "/api/benchmarks/{benchmark_id}/evaluate"): Tier.FULL,
    # Mutates benchmark data and the registry.
    ("POST", "/api/benchmarks/{benchmark_id}/ground-truth-sql"): Tier.FULL,
    ("POST", "/api/benchmarks"): Tier.FULL,
    ("PUT", "/api/benchmarks/{benchmark_id}"): Tier.FULL,
    ("POST", "/api/benchmarks/logo-upload"): Tier.FULL,
    # Writes YAML into the installed package directory.
    ("PUT", "/api/llm-judge/configs/{name}"): Tier.FULL,
    # User management. The tier is PUBLIC because the real gate is ADMIN_ROUTES,
    # checked separately: an admin must be able to grant roles on a judge-mode
    # host, and a FULL requirement here would deny exactly that. Nothing reaches
    # these without passing the admin check first.
    ("POST", "/api/users"): Tier.PUBLIC,
    ("DELETE", "/api/users/{email}"): Tier.PUBLIC,
    # Downloads gigabytes to the data root.
    ("POST", "/api/results/fetch"): Tier.FULL,
}


def required_tier(method: str, path: str) -> Tier:
    """
    Minimum tier for a request.

    Safe methods are ``PUBLIC``.  Mutating methods use the declared tier, and
    fall back to ``FULL`` when undeclared so an unclassified route fails closed.
    """
    if method.upper() in SAFE_METHODS:
        return Tier.PUBLIC
    return ROUTE_TIERS.get((method.upper(), path), Tier.FULL)


def iter_routes(target: Any, _depth: int = 0) -> Iterator[Any]:
    """
    Every concrete route reachable from an app, router or mount.

    A flat walk of ``app.routes`` is not enough.  Since Starlette 1.6,
    ``include_router`` leaves a wrapper object in ``app.routes`` that holds the
    real routes rather than splicing them in, so a flat walk sees a thing with
    no ``path`` and no ``methods`` and skips right past it.

    That is a security-relevant detail, not a cosmetic one: both callers of this
    function decide authorization.  A route the walk cannot see is a route the
    classification test cannot audit and the tier middleware cannot resolve a
    template for.

    Wrappers are unwrapped by attribute rather than by type, so this keeps
    working if the private class is renamed again; the depth cap is a guard
    against a self-referential mount rather than an expected case.
    """
    if _depth > 8:  # pragma: no cover - defensive
        return
    routes = getattr(target, "routes", None)
    if routes is None:
        inner = getattr(target, "original_router", None) or getattr(
            target, "router", None
        )
        routes = getattr(inner, "routes", None) if inner is not None else None
    if routes is None:
        return
    for route in routes:
        if getattr(route, "path", None) is not None:
            yield route
        else:
            yield from iter_routes(route, _depth + 1)


def unclassified_routes(app: "FastAPI") -> List[Tuple[str, str]]:
    """
    Mutating routes with no entry in :data:`ROUTE_TIERS`.

    Such a route still fails closed, but silently defaulting is how a route
    meant for the judge tier ends up unreachable in production, so a test
    surfaces them instead.
    """
    missing: List[Tuple[str, str]] = []
    for route in iter_routes(app):
        path = getattr(route, "path", None)
        methods = getattr(route, "methods", None)
        if not path or not methods or not path.startswith("/api"):
            continue
        for method in sorted(set(methods) - SAFE_METHODS):
            if (method, path) not in ROUTE_TIERS:
                missing.append((method, path))
    return missing


#: Routes only an admin may call, whatever the deployment mode.
#:
#: This is a separate question from the tier: user management has to work on a
#: judge-mode host, where the ceiling denies ``full``. Expressing it as a tier
#: would make the console unusable exactly where it is needed.
ADMIN_ROUTES: Set[Tuple[str, str]] = {
    ("GET", "/api/users"),
    ("POST", "/api/users"),
    ("DELETE", "/api/users/{email}"),
}


def requires_admin(method: str, path: str) -> bool:
    """Whether *method* and *path* may only be called by an admin."""
    return (method.upper(), path) in ADMIN_ROUTES


def resolve_tier(
    mode: Tier,
    email: Optional[str],
    requested: Tier = Tier.PUBLIC,
) -> Tier:
    """
    Effective tier for one request.

    ``mode`` is the ceiling set at startup -- a public deployment can never
    grant ``full`` regardless of who signs in, or what any stored role says.

    Args:
        mode: The deployment ceiling.
        email: The verified address, or ``None`` when not signed in.
        requested: The tier this caller's role asks for. Anonymous callers ask
            for ``public``; a stored role may ask for more, and the ceiling
            decides whether it is honoured.

    Returns:
        Tier: ``min(requested, mode)`` for a signed-in caller. A local operator
        (``mode`` is ``full``) keeps ``full`` without signing in, which is what
        makes the toolkit usable from a laptop.
    """
    if mode is Tier.FULL:
        return Tier.FULL
    if not email:
        return Tier.PUBLIC
    return min(requested, mode)


def parse_allowlist(raw: Optional[str]) -> Set[str]:
    """Parse ``a@b.com,c@d.com`` into a normalised set."""
    if not raw:
        return set()
    return {part.strip().lower() for part in raw.split(",") if part.strip()}
