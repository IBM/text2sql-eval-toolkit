#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Benchmarks whose details only a signed-in caller may see.

Beaver is the case this exists for. Its questions, SQL and schema are
distributed under a gated licence, and only what its own public leaderboard
shows -- overall scores -- may be published. So an anonymous visitor sees
Beaver's tile and its overall per-pipeline scores, and nothing that reveals a
question, a query, a result table, or the breakdown by SQL feature.

A tier says what a caller may *do*, not which data they may read, and an
anonymous visitor and a signed-in ``read_only`` user are the same ``public``
tier. So this is a check on identity rather than a tier setting -- but it is
enforced in the same place as the tiers, for the same reason: a check added to
twenty handlers by hand is missing from the twenty-first.

**What marks a benchmark.** A registry entry with ``"requires_sign_in": true``.
The flag is honoured in *every* registry copy the server can see -- the data
root's ``benchmarks.json`` and ``test-benchmarks.json`` and the copies packaged
with the toolkit -- not only the one it resolves for listing. That is the point
of the design: ``deploy/provision.sh`` seeds ``benchmarks.json`` into the data
root once and never overwrites it, so a deployment provisioned before the flag
existed would otherwise go on serving the details to anyone. Lifting a
restriction therefore means removing the flag from every copy.
``TEXT2SQL_SIGN_IN_BENCHMARKS`` adds ids without editing a file.

**Fail closed.** ``SUMMARY_ROUTES`` lists the routes such a benchmark still
answers anonymously. Every other route that names it is refused, so a route
added later is walled until someone decides otherwise and says so here.

**What a request names.** Read from the matched route's declared parameters, not
from the URL text: ``/api/compare?left_id=beaver`` names Beaver, while
``/api/benchmarks/spider_dev/errors?q=beaver`` is a search. Which parameter
names identify a benchmark is a table, and ``unclassified_params`` reports every
route parameter that is in neither half of it -- so a new route that takes a
benchmark under a new name fails a test instead of serving it.

**Who is exempt.** The local operator tool, on the reasoning ``resolve_tier``
already applies to ``full`` on loopback: the operator controls the process and
has the files.

**Crawlers.** ``mentions_restricted`` is deliberately looser than the refusal --
any path segment or query value naming a restricted benchmark -- and decides
``X-Robots-Tag: noindex``, which costs nothing when it over-matches. There is no
``robots.txt`` entry: it is advisory, and it would publish the very paths it
asks crawlers to skip.
"""

from __future__ import annotations

import json
import os
import re
import threading
from pathlib import Path
from typing import Any, Dict, FrozenSet, List, Mapping, Optional, Tuple
from urllib.parse import parse_qsl

from fastapi import Request
from fastapi.responses import JSONResponse

from text2sql_eval_toolkit.logging import get_logger
from text2sql_eval_toolkit.ui import runtime
from text2sql_eval_toolkit.ui.capabilities import Tier, iter_routes
from text2sql_eval_toolkit.ui.paths import get_data_root

logger = get_logger(__name__)

#: The registry field that restricts a benchmark's details to signed-in callers.
SIGN_IN_FLAG = "requires_sign_in"

#: Comma-separated benchmark ids restricted in addition to the registry flags.
SIGN_IN_BENCHMARKS_ENV = "TEXT2SQL_SIGN_IN_BENCHMARKS"

#: The value of ``X-Robots-Tag`` on anything that mentions such a benchmark.
NOINDEX = "noindex, nofollow"

#: The routes a restricted benchmark still answers for an anonymous caller: the
#: numbers its tile and summary page show, and nothing finer. Overall scores per
#: pipeline are what Beaver's own leaderboard publishes. The breakdown by query
#: category is *not* here: it is derived from the ground-truth SQL.
SUMMARY_ROUTES: FrozenSet[Tuple[str, str]] = frozenset(
    {
        ("GET", "/api/benchmarks/{benchmark_id}/summary"),
        # Pipeline ids and their short aliases, which the summary page's links
        # need. No record content.
        ("GET", "/api/benchmarks/{benchmark_id}/pipeline-aliases"),
        # Two summary files side by side: overall metrics only.
        ("GET", "/api/compare"),
    }
)

#: Route parameters whose value is a benchmark id. ``left_id`` and ``right_id``
#: are the two sides of ``/api/compare`` and name result files by benchmark id.
BENCHMARK_ID_PARAMS: FrozenSet[str] = frozenset({"benchmark_id", "left_id", "right_id"})

#: Route parameters that do not name a benchmark. Every parameter of every
#: ``/api`` route must be in this set or the one above; see
#: ``unclassified_params``.
NON_BENCHMARK_PARAMS: FrozenSet[str] = frozenset(
    {
        # Records, pipelines, metrics and filters *within* a benchmark the
        # route already names by one of the parameters above.
        "record_id",
        "pipeline",
        "pipeline2",
        "pipeline_left",
        "pipeline_right",
        "metric",
        "metric2",
        "metric_a",
        "metric_b",
        "metric_left",
        "metric_right",
        "value",
        "op",
        "disagree",
        "failed_only",
        "q",
        "page",
        "page_size",
        # Not benchmarks at all: judge configs and notes (``name``), a note's
        # image, a background job, a user, a provider key, a sign-in return path.
        "name",
        "filename",
        "job_id",
        "email",
        "provider",
        "next",
        # A benchmark logo under /api/static. The tile stays public for a
        # restricted benchmark, and a logo shows nothing of its content.
        "file_path",
    }
)

SIGN_IN_DETAIL = (
    "This benchmark's questions, SQL and per-record results are only available "
    "to signed-in users. Sign in to view them."
)

# The same rule ``registry.normalize_benchmark_id`` applies on create.
_VALID_ID = re.compile(r"[A-Za-z0-9_-]+")

_TRUTHY = {"1", "true", "yes"}


def _is_set(value: Any) -> bool:
    """A flag spelled ``"true"`` restricts too: failing closed is the safe misreading."""
    if value is True:
        return True
    return isinstance(value, str) and value.strip().lower() in _TRUTHY


# Per registry file: (size, mtime_ns) -> restricted ids. Every API request
# asks, so an unchanged file is not re-parsed.
_FLAG_CACHE: Dict[str, Tuple[Tuple[int, int], FrozenSet[str]]] = {}
_FLAG_LOCK = threading.Lock()
_EMPTY: FrozenSet[str] = frozenset()


def _registry_files() -> List[Path]:
    """Every registry copy the server could read a benchmark from."""
    from text2sql_eval_toolkit.utils import (
        BENCHMARKS_FILE,
        TEST_BENCHMARKS_FILE,
        get_benchmarks_file_path,
    )

    candidates = [
        get_benchmarks_file_path(is_test=False),
        get_benchmarks_file_path(is_test=True),
        # registry.get_benchmark_registry_path() falls back to this one.
        get_data_root() / "benchmarks.json",
        get_data_root() / "test-benchmarks.json",
        Path(str(BENCHMARKS_FILE)),
        Path(str(TEST_BENCHMARKS_FILE)),
    ]
    seen: Dict[str, Path] = {}
    for path in candidates:
        seen.setdefault(str(path), path)
    return list(seen.values())


def _flags_in(path: Path) -> FrozenSet[str]:
    try:
        stat = path.stat()
    except OSError:
        return _EMPTY
    key = (stat.st_size, stat.st_mtime_ns)

    with _FLAG_LOCK:
        hit = _FLAG_CACHE.get(str(path))
    if hit is not None and hit[0] == key:
        return hit[1]

    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError) as exc:
        # Keep the last answer this file gave: a registry that cannot be read
        # right now must not be what lifts a restriction.
        logger.warning("Could not read %s for sign-in restrictions: %s", path, exc)
        return hit[1] if hit is not None else _EMPTY

    ids = frozenset(
        str(benchmark_id).casefold()
        for benchmark_id, entry in (data.items() if isinstance(data, dict) else ())
        if isinstance(entry, dict) and _is_set(entry.get(SIGN_IN_FLAG))
    )
    with _FLAG_LOCK:
        _FLAG_CACHE[str(path)] = (key, ids)
    return ids


def restricted_benchmarks() -> FrozenSet[str]:
    """Casefolded ids of every benchmark whose details require sign-in."""
    ids = {
        part.strip().casefold()
        for part in os.getenv(SIGN_IN_BENCHMARKS_ENV, "").split(",")
        if part.strip()
    }
    for path in _registry_files():
        ids |= _flags_in(path)
    return frozenset(ids)


def is_restricted(benchmark_id: Optional[str]) -> bool:
    """
    Whether *benchmark_id*'s details require sign-in.

    Compared casefolded: on a case-insensitive filesystem ``BEAVER`` opens
    Beaver's files, so it has to be refused as Beaver.
    """
    if not benchmark_id:
        return False
    return benchmark_id.strip().casefold() in restricted_benchmarks()


def may_see_restricted(request: Request) -> bool:
    """Signed in, or the local operator."""
    if runtime.get_mode() is Tier.FULL and not runtime.is_remote_deployment():
        return True
    return bool(runtime.current_user_email(request))


def declared_params(route: Any) -> FrozenSet[str]:
    """
    Names of a route's path and query parameters, as they appear on the wire.

    Body fields are not included. Every route that takes a benchmark in its body
    requires the ``full`` tier, which nobody the wall refuses can hold; the test
    that enumerates routes checks that this stays true.
    """
    dependant = getattr(route, "dependant", None)
    if dependant is None:
        # A plain Starlette route, such as the OpenAPI schema.
        return frozenset(getattr(route, "param_convertors", None) or {})
    names = set()
    stack = [dependant]
    while stack:
        current = stack.pop()
        for field in list(current.path_params) + list(current.query_params):
            names.add(getattr(field, "alias", None) or field.name)
        stack.extend(current.dependencies)
    return frozenset(names)


def unclassified_params(app: Any) -> List[Tuple[str, str]]:
    """``(path, parameter)`` for every ``/api`` route parameter not classified above."""
    known = BENCHMARK_ID_PARAMS | NON_BENCHMARK_PARAMS
    missing: List[Tuple[str, str]] = []
    for route in iter_routes(app):
        path = getattr(route, "path", None)
        if not path or not path.startswith("/api"):
            continue
        for name in sorted(declared_params(route) - known):
            missing.append((path, name))
    return missing


def refusal(
    request: Request, route: Any, path_params: Mapping[str, Any]
) -> Optional[JSONResponse]:
    """
    The response that refuses this request, or ``None`` to let it through.

    401 rather than 403 or 404: the caller is not *forbidden*, they are not
    signed in, and the dashboard turns ``sign_in_required`` into a prompt to do
    so. A shared link that answered "not found" would read as a broken link.
    """
    if route is None or may_see_restricted(request):
        return None
    ids = restricted_benchmarks()
    if not ids:
        return None

    declared = declared_params(route)
    named: List[str] = []
    for param in sorted(BENCHMARK_ID_PARAMS & declared):
        values = list(request.query_params.getlist(param))
        if param in path_params:
            values.append(str(path_params[param]))
        for value in values:
            # An id is also a filename fragment -- `{id}-predictions_eval.json`
            # -- so `charts/../beaver` would read Beaver's files under a name
            # the check does not recognise. Ids have a fixed alphabet; refuse
            # anything outside it rather than try to predict what it resolves to.
            if not _VALID_ID.fullmatch(value):
                return JSONResponse(
                    status_code=400,
                    content={"detail": f"'{param}' is not a valid benchmark id."},
                )
            named.append(value.casefold())

    if (request.method.upper(), getattr(route, "path", "")) in SUMMARY_ROUTES:
        return None
    if any(value in ids for value in named):
        return JSONResponse(
            status_code=401,
            content={"detail": SIGN_IN_DETAIL, "sign_in_required": True},
            headers={"X-Robots-Tag": NOINDEX},
        )
    return None


def mentions_restricted(path: str, query_string: str) -> bool:
    """
    Whether an address mentions a restricted benchmark anywhere.

    Covers the dashboard's own client routes -- ``/benchmark/beaver``,
    ``/run/beaver/record/1``, ``/errors?benchmark=beaver``,
    ``/compare/profile?benchmarks=spider_dev,beaver`` -- without having to know
    them, so a new view is covered without an edit here.
    """
    ids = restricted_benchmarks()
    if not ids:
        return False
    for segment in path.split("/"):
        if segment.casefold().split(".", 1)[0] in ids:
            return True
    for _key, value in parse_qsl(query_string, keep_blank_values=False):
        if any(part.strip().casefold() in ids for part in value.split(",")):
            return True
    return False
