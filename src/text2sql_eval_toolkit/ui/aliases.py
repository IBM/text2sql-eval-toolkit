#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Short aliases for pipeline identifiers.

A ``pipeline_id`` is derived, not chosen: ``{model_name}`` joined to the
inference strategy, giving strings like
``wxai:openai/gpt-oss-120b-agentic-baseline1-3attempts``.  Two of those in one
URL -- which the comparison views do -- plus a search term produces an address
that mail clients wrap and chat apps truncate.  An alias is a compact stand-in
that the dashboard accepts anywhere a full id is accepted.

What an alias is **not**: a way to survive a rename.  It is a hash *of* the id,
so if a model string changes, the alias changes with it and old links break
exactly as before.  Surviving a rename needs a persisted mapping from a stable
key to whatever the id is called today, which the toolkit has nowhere to store
-- the artifacts are keyed by the id itself.  Aliases address URL length; they
do not address identifier churn, and pretending otherwise would be worse than
not having them.

The alias is derived rather than assigned so that any two processes reading the
same artifacts agree on it without coordination, and so a link keeps working
across a re-fetch of the results snapshot.
"""

from __future__ import annotations

import hashlib
import re
from typing import Dict, Iterable, Optional, Set

#: Hex characters kept from the digest.  At 40 bits, a benchmark would need
#: roughly a million pipelines before a collision became likely; the largest
#: today has fewer than fifty.  Collisions are still *detected* rather than
#: assumed away -- see `resolve_pipeline_ref`.
ALIAS_LENGTH = 10

#: What an alias looks like in a URL.  A full pipeline id can never match this:
#: every one contains a ``-``, and most contain ``:`` or ``/``.  That keeps
#: resolution unambiguous without a marker prefix in the URL.
ALIAS_PATTERN = re.compile(rf"^[0-9a-f]{{{ALIAS_LENGTH}}}$")


def pipeline_alias(pipeline_id: str) -> str:
    """Short, stable alias for one pipeline id."""
    digest = hashlib.sha256(pipeline_id.encode("utf-8")).hexdigest()
    return digest[:ALIAS_LENGTH]


def looks_like_alias(ref: str) -> bool:
    """True if ``ref`` has the shape of an alias rather than a pipeline id."""
    return bool(ALIAS_PATTERN.match(ref))


def alias_map(pipeline_ids: Iterable[str]) -> Dict[str, str]:
    """
    ``{alias: pipeline_id}`` for a benchmark's pipelines.

    A colliding alias is dropped rather than resolving to an arbitrary one of
    the two ids: a link that says "not found" is recoverable, and a link that
    quietly opens the wrong pipeline is not.
    """
    mapping: Dict[str, str] = {}
    collisions: Set[str] = set()
    for pipeline_id in pipeline_ids:
        alias = pipeline_alias(pipeline_id)
        existing = mapping.get(alias)
        if existing is not None and existing != pipeline_id:
            collisions.add(alias)
        mapping[alias] = pipeline_id
    for alias in collisions:
        mapping.pop(alias, None)
    return mapping


def resolve_pipeline_ref(ref: str, pipeline_ids: Iterable[str]) -> Optional[str]:
    """
    Resolve a URL reference to a pipeline id, or ``None`` if it names none.

    An exact id wins over an alias, so a (currently impossible) pipeline id
    shaped like an alias still resolves to itself.
    """
    known = list(pipeline_ids)
    if ref in known:
        return ref
    if not looks_like_alias(ref):
        return None
    return alias_map(known).get(ref)
