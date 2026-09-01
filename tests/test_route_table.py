#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
The route table is part of the dashboard's public surface.

Shared links resolve against these paths, so losing or renaming one breaks
addresses already pasted into issues and papers -- and the routes are now spread
across ten router modules, where a missing ``include_router`` deletes a whole
group at once without any single endpoint test necessarily failing first.

This is a snapshot, and it is meant to be edited: adding a route should add a
line here in the same commit. What it prevents is a route disappearing by
accident.
"""

import pytest

pytest.importorskip("fastapi")

from text2sql_eval_toolkit.ui import server  # noqa: E402
from text2sql_eval_toolkit.ui.capabilities import iter_routes  # noqa: E402

EXPECTED = {
    "GET /api/auth/callback",
    "GET /api/auth/login",
    "GET /api/benchmarks",
    "GET /api/benchmarks/{benchmark_id}/config",
    "GET /api/benchmarks/{benchmark_id}/errors",
    "GET /api/benchmarks/{benchmark_id}/errors/{record_id}",
    "GET /api/benchmarks/{benchmark_id}/errors/{record_id}/detail",
    "GET /api/benchmarks/{benchmark_id}/insights/binary-metric-confusion-by-pipeline",
    "GET /api/benchmarks/{benchmark_id}/insights/cross-pipeline-binary-metric-confusion",
    "GET /api/benchmarks/{benchmark_id}/pipeline-aliases",
    "GET /api/benchmarks/{benchmark_id}/playground/{record_id}",
    "GET /api/benchmarks/{benchmark_id}/record-ids",
    "GET /api/benchmarks/{benchmark_id}/summary",
    "GET /api/benchmarks/{benchmark_id}/summary/by-category",
    "GET /api/compare",
    "GET /api/deployment",
    "GET /api/evaluation-metric-definitions",
    "GET /api/jobs/{job_id}",
    "GET /api/llm-judge/configs",
    "GET /api/llm-judge/configs/{name}",
    "GET /api/me",
    "GET /api/results/fetch/{job_id}",
    "GET /api/results/status",
    "GET /api/static/{file_path:path}",
    "POST /api/auth/logout",
    "POST /api/benchmarks",
    "POST /api/benchmarks/logo-upload",
    "POST /api/benchmarks/{benchmark_id}/evaluate",
    "POST /api/benchmarks/{benchmark_id}/execute",
    "POST /api/benchmarks/{benchmark_id}/ground-truth-sql",
    "POST /api/benchmarks/{benchmark_id}/judge",
    "POST /api/benchmarks/{benchmark_id}/playground/evaluate",
    "POST /api/results/fetch",
    "PUT /api/benchmarks/{benchmark_id}",
    "PUT /api/llm-judge/configs/{name}",
    "DELETE /api/llm-judge/configs/{name}",
    "POST /api/llm-judge/configs/{name}/rename",
    # User management, added in 1.4.0. Admin-only, enforced by
    # capabilities.ADMIN_ROUTES rather than by tier.
    "GET /api/users",
    "POST /api/users",
    "DELETE /api/users/{email}",
    # A signed-in user's own provider credentials, added in 1.4.0. Scoped to
    # the caller; nothing here returns key material.
    "GET /api/my/keys",
    "POST /api/my/keys",
    "DELETE /api/my/keys/{provider}",
    # The docs view, added in 1.5.0. Read-only and public: documentation is
    # readable by anyone who can reach the dashboard, on any deployment mode.
    # Both are GET, so capabilities.py already treats them as PUBLIC.
    "GET /api/docs",
    "GET /api/docs/{name}",
    # Images a note references, from docs/notes/assets/.
    "GET /api/docs/assets/{filename}",
    # The OpenAPI schema. Moved off the framework default of /openapi.json in
    # 1.5.0, when /docs became the dashboard's documentation view and Swagger
    # UI had to give up the path. Both HTTP methods appear because Starlette's
    # own route helper registers HEAD alongside GET; the routers above use
    # FastAPI's decorators, which do not.
    #
    # The Swagger UI and ReDoc pages are off entirely -- they load their assets
    # from a CDN that this app's `script-src 'self'` blocks, so both rendered
    # blank. See server.py.
    "GET /api/openapi.json",
    "HEAD /api/openapi.json",
}


def _live_routes():
    found = set()
    for route in iter_routes(server.app):
        path = getattr(route, "path", "")
        if not path.startswith("/api"):
            continue
        for method in getattr(route, "methods", None) or []:
            found.add(f"{method} {path}")
    return found


def test_no_route_has_disappeared():
    missing = EXPECTED - _live_routes()
    assert not missing, (
        "these routes are gone -- a shared link pointing at one now 404s:\n  "
        + "\n  ".join(sorted(missing))
    )


def test_no_route_appeared_without_being_recorded():
    """
    The other direction is not about breakage but about review: a new endpoint
    should be a deliberate line in this file, seen by whoever reads the diff.
    """
    extra = _live_routes() - EXPECTED
    assert not extra, "new routes, not yet recorded here:\n  " + "\n  ".join(
        sorted(extra)
    )
