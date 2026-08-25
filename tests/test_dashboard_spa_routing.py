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
    monkeypatch.setattr(server, "get_data_root", lambda: tmp_path)

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
    ],
)
def test_deep_links_serve_the_app_shell(spa_client, path):
    resp = spa_client.get(path)
    assert resp.status_code == 200, path
    assert "text/html" in resp.headers["content-type"]
    assert "<title>dash</title>" in resp.text


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
