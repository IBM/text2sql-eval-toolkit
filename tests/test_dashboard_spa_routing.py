#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Deep links must survive a hard refresh.

The dashboard uses real paths so links can be shared, but those paths exist only
in the client router. Without a server fallback, opening a shared link returns
404 -- which would make the whole shareable-URL feature useless in practice.
"""

import json

import pytest

pytest.importorskip("fastapi")
from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from text2sql_eval_toolkit.ui import server  # noqa: E402
from text2sql_eval_toolkit.ui import runtime
from text2sql_eval_toolkit.ui.capabilities import Tier


@pytest.fixture
def spa_client(tmp_path, monkeypatch):
    dist = tmp_path / "dashboard" / "dist"
    (dist / "assets").mkdir(parents=True)
    (dist / "index.html").write_text(
        "<!doctype html><title>dash</title>", encoding="utf-8"
    )
    (dist / "assets" / "app.js").write_text("console.log(1)", encoding="utf-8")

    results = tmp_path / "results"
    results.mkdir()
    (results / "demo-predictions_eval.json").write_text(
        json.dumps([{"id": "r1", "question": "q", "predictions": {}}]), encoding="utf-8"
    )

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TEXT2SQL_DATA_ROOT", str(tmp_path))
    # These fixtures serve indices built on demand, which is the local operator path; a shared deployment refuses that and answers 503.
    runtime.set_mode(Tier.FULL)

    # Mount onto a fresh app so the test does not depend on import order.
    app = FastAPI()
    app.router.routes = list(server.app.router.routes)
    server.mount_static(app)
    server.invalidate_index_cache()
    try:
        yield TestClient(app)
    finally:
        server.invalidate_index_cache()


@pytest.mark.parametrize(
    "path",
    [
        "/",
        "/b/bird_mini_dev_sqlite",
        "/b/bird_mini_dev_sqlite/errors",
        "/b/bird_mini_dev_sqlite/insights",
        "/b/bird_mini_dev_sqlite/compare",
        "/b/bird_mini_dev_sqlite/compare/profile",
        "/b/bird_mini_dev_sqlite/pipeline/wxai%3Aopenai%2Fgpt-oss-120b-greedy-zero-shot-chatapi",
        "/llm-judge",
        "/llm-judge/default",
        "/run",
        # The playground carries its benchmark and record in the path, so these
        # are the addresses people paste into issues.
        "/run/bird_mini_dev_sqlite",
        "/run/bird_mini_dev_sqlite/record/1490",
        "/benchmarks",
        "/users",
        "/my-keys",
        # The docs view, added in 1.5.0. `/docs` had to be taken back from
        # FastAPI, which mounts Swagger UI there by default -- a real route,
        # so it won the match and this one was unreachable.
        "/docs",
        "/docs/state-of-the-art",
    ],
)
def test_deep_links_serve_the_app_shell(spa_client, path):
    resp = spa_client.get(path)
    assert resp.status_code == 200, path
    assert "text/html" in resp.headers["content-type"]
    assert "<title>dash</title>" in resp.text


def test_docs_is_the_dashboard_view_not_the_framework_default(spa_client):
    """
    FastAPI mounts Swagger UI at /docs unless told otherwise, and a real route
    beats the SPA fallback. When it did, `/docs/state-of-the-art` rendered the
    dashboard and `/docs` rendered a different application entirely -- which is
    the confusing half of that failure, because the deep link kept working.

    The Swagger page is off rather than moved: it loads its assets from a CDN
    that this app's own `script-src 'self'` blocks, so it had been rendering
    blank since the CSP was added.
    """
    resp = spa_client.get("/docs")
    assert "<title>dash</title>" in resp.text
    assert "swagger" not in resp.text.lower()

    # The schema is the half that works without a CDN, and it moved under /api
    # with everything else the server owns.
    assert spa_client.get("/api/openapi.json").status_code == 200
    assert spa_client.get("/openapi.json").status_code == 404


def test_deep_link_with_query_string_still_resolves(spa_client):
    resp = spa_client.get(
        "/b/demo/errors?pipeline=modelA&metric=execution_accuracy&value=0&page=3"
    )
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]


def test_real_assets_are_still_served(spa_client):
    resp = spa_client.get("/assets/app.js")
    assert resp.status_code == 200
    assert "console.log(1)" in resp.text


def test_missing_asset_stays_a_404_not_html(spa_client):
    """A typo'd bundle path must not return HTML, or the browser reports a
    confusing parse error instead of a missing file."""
    resp = spa_client.get("/assets/does-not-exist.js")
    assert resp.status_code == 404
    assert "<title>dash</title>" not in resp.text


def test_unknown_api_path_is_a_real_404(spa_client):
    """API 404s must stay JSON 404s, not silently become the app shell."""
    resp = spa_client.get("/api/definitely-not-a-route")
    assert resp.status_code == 404
    assert "<title>dash</title>" not in resp.text


def test_api_routes_still_work_behind_the_fallback(spa_client):
    resp = spa_client.get("/api/benchmarks/demo/errors")
    assert resp.status_code == 200
    assert resp.json()["total"] == 1


def _logo(tmp_path, name: str, data: bytes = b"\x89PNG-not-really"):
    target = tmp_path / "benchmarks" / "logos" / name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)
    return f"/api/static/benchmarks/logos/{name}"


def test_static_assets_revalidate_with_an_etag(spa_client, tmp_path):
    """Logos were sent with no-store, so every page view re-downloaded them."""
    url = _logo(tmp_path, "logo.png")

    first = spa_client.get(url)
    assert first.status_code == 200
    etag = first.headers.get("etag")
    assert etag, "asset responses must carry an ETag"
    assert "no-store" not in first.headers.get("cache-control", "")

    second = spa_client.get(url, headers={"If-None-Match": etag})
    assert second.status_code == 304
    assert second.content == b""


def test_changed_asset_gets_a_new_etag(spa_client, tmp_path):
    url = _logo(tmp_path, "logo2.png", b"one")
    asset = tmp_path / "benchmarks" / "logos" / "logo2.png"
    first_etag = spa_client.get(url).headers["etag"]

    # Force a distinct mtime so the fingerprint genuinely differs.
    import os

    os.utime(asset, (0, 0))
    asset.write_bytes(b"two-different-length")
    second = spa_client.get(url, headers={"If-None-Match": first_etag})
    assert second.status_code == 200, "an edited asset must not be served from cache"
    assert second.headers["etag"] != first_etag


@pytest.mark.parametrize(
    "suffix",
    [
        # Plain "../" is normalised away by well-behaved clients before it is
        # sent, so encoded forms are what actually reach the handler.
        "%2e%2e%2f%2e%2e%2fetc%2fpasswd",
        "..%2f..%2fetc%2fpasswd",
        "%2e%2e/%2e%2e/etc/passwd",
    ],
)
def test_encoded_asset_path_traversal_is_refused(spa_client, suffix):
    resp = spa_client.get(f"/api/static/{suffix}")
    assert resp.status_code in (403, 404), resp.status_code
    assert b"root:" not in resp.content


@pytest.mark.parametrize(
    "target",
    [
        # The judge spend store and the derived indices live under the data
        # root. This route is a GET, so it runs at the public tier -- serving
        # the whole data root would have handed them to any visitor.
        "judge/usage.sqlite",
        "results/demo-predictions_eval.json",
        "results/.index/demo-predictions_eval.sqlite",
        "benchmarks.json",
        "benchmarks/logos/../../judge/usage.sqlite",
        "%2e%2e%2fjudge%2fusage.sqlite",
    ],
)
def test_static_route_serves_nothing_outside_the_logo_directory(spa_client, target):
    resp = spa_client.get(f"/api/static/{target}")
    assert resp.status_code in (403, 404), resp.status_code
    assert b"SQLite format" not in resp.content


def test_static_route_refuses_non_image_types_inside_the_logo_directory(
    spa_client, tmp_path
):
    """A logo directory should only ever yield images."""
    path = tmp_path / "benchmarks" / "logos" / "notes.txt"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("internal")
    resp = spa_client.get("/api/static/benchmarks/logos/notes.txt")
    assert resp.status_code == 403
    assert b"internal" not in resp.content
