#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
A benchmark flagged ``requires_sign_in`` is refused to anonymous callers on
every route that names it.

The properties that matter, and why each is tested the way it is:

* **Every route, not a sample.** The refusal tests are parametrized over the
  live route table, filtered by which parameters identify a benchmark -- so a
  route added tomorrow is in the list without anyone editing this file.
* **No parameter goes unclassified.** That enumeration is only as good as the
  table of parameter names it filters on, so a parameter that is in neither half
  of the table fails a test. A route that took a benchmark as ``?bench=`` would
  otherwise be skipped by the refusal tests and serve the data.
* **Spellings that reach the same files.** ``SECRET_BENCH`` on a
  case-insensitive filesystem, ``charts/../secret_bench`` through a directory
  that exists.
* **The flag survives provisioning.** A data root seeded before the flag existed
  must still restrict a benchmark the packaged registry flags.
"""

import json
import os
import re
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from text2sql_eval_toolkit.ui import (
    benchmark_access,
    middleware,
    runtime,
    server,
)  # noqa: E402
from text2sql_eval_toolkit.ui.benchmark_access import (  # noqa: E402
    BENCHMARK_ID_PARAMS,
    NOINDEX,
    SIGN_IN_BENCHMARKS_ENV,
    declared_params,
    unclassified_params,
)
from text2sql_eval_toolkit.ui.capabilities import (  # noqa: E402
    SAFE_METHODS,
    Tier,
    iter_routes,
    required_tier,
)

REPO_ROOT = Path(__file__).resolve().parents[1]

SECRET = "secret_bench"
OPEN = "demo"
SIGNED_IN = "reader@example.com"


def _entry(benchmark_id, **extra):
    return {
        "name": benchmark_id,
        "description": f"{benchmark_id} description",
        "data": f"benchmarks/{benchmark_id}.json",
        "schema": f"benchmarks/{benchmark_id}-schema.json",
        "predictions": f"results/{benchmark_id}-predictions.json",
        "db_engine": {"db_type": "sqlite", "db_folder": "dbs"},
        **extra,
    }


@pytest.fixture
def data_root(tmp_path, monkeypatch):
    results = tmp_path / "results"
    # A real directory under results/, so `charts/../secret_bench` resolves to
    # the secret benchmark's files rather than failing for an unrelated reason.
    (results / "charts").mkdir(parents=True)
    for benchmark_id in (OPEN, SECRET):
        (results / f"{benchmark_id}-predictions_eval.json").write_text(
            json.dumps([{"id": "r1", "question": "q", "predictions": {}}]),
            encoding="utf-8",
        )
        (results / f"{benchmark_id}-predictions_eval_summary.json").write_text(
            json.dumps({"p1": {"execution_accuracy": {"average": 1.0}}}),
            encoding="utf-8",
        )

    registry = {
        OPEN: _entry(OPEN, logo="demo.png"),
        SECRET: _entry(SECRET, logo="secret-logo.png", requires_sign_in=True),
    }
    (tmp_path / "benchmarks.json").write_text(json.dumps(registry), encoding="utf-8")

    logos = tmp_path / "benchmarks" / "logos"
    logos.mkdir(parents=True)
    for name in ("demo.png", "secret-logo.png", f"{SECRET}.png"):
        (logos / name).write_bytes(b"\x89PNG-not-really")

    monkeypatch.setenv("TEXT2SQL_DATA_ROOT", str(tmp_path))
    monkeypatch.delenv(SIGN_IN_BENCHMARKS_ENV, raising=False)
    # Away from the checkout, so ./data does not supply the real test registry.
    monkeypatch.chdir(tmp_path)
    return tmp_path


@pytest.fixture
def shared_deployment():
    """A public deployment reachable beyond loopback."""
    mode, remote = runtime.get_mode(), runtime.is_remote_deployment()
    runtime.set_mode(Tier.PUBLIC)
    runtime.set_remote_deployment(True)
    server.reset_rate_limits()
    server.invalidate_index_cache()
    try:
        yield
    finally:
        runtime.set_mode(mode)
        runtime.set_remote_deployment(remote)
        server.reset_rate_limits()
        server.invalidate_index_cache()


@pytest.fixture
def client(data_root, shared_deployment):
    return TestClient(server.app)


@pytest.fixture
def signed_in(monkeypatch):
    monkeypatch.setattr(runtime, "current_user_email", lambda request: SIGNED_IN)


# --- which routes name a benchmark ---------------------------------------


def _benchmark_routes():
    """``(method, path, param)`` for every benchmark-identifying parameter."""
    out = []
    for route in iter_routes(server.app):
        path = getattr(route, "path", "")
        if not path.startswith("/api"):
            continue
        for param in sorted(declared_params(route) & BENCHMARK_ID_PARAMS):
            for method in sorted(getattr(route, "methods", None) or ()):
                out.append((method, path, param))
    return out


BENCHMARK_ROUTES = _benchmark_routes()


def _request(path, param, benchmark):
    """A URL and query naming *benchmark* through *param*, and OPEN elsewhere."""
    url = path.replace("{record_id}", "r1")
    query = {}
    for name in sorted(BENCHMARK_ID_PARAMS):
        value = benchmark if name == param else OPEN
        if "{" + name + "}" in url:
            url = url.replace("{" + name + "}", value)
        else:
            query[name] = value
    assert not re.search(r"{[^}]+}", url), f"unfilled placeholder in {url}"
    return url, query


def test_the_enumeration_finds_the_routes_it_exists_for():
    """
    Guards the guard. If `declared_params` stopped seeing parameters -- a
    FastAPI upgrade renaming `dependant`, say -- every refusal test below would
    pass over an empty list.
    """
    paths = {path for _, path, _ in BENCHMARK_ROUTES}
    assert len(paths) >= 18
    for expected in (
        "/api/benchmarks/{benchmark_id}/summary",
        "/api/benchmarks/{benchmark_id}/errors/{record_id}/detail",
        "/api/benchmarks/{benchmark_id}/playground/{record_id}",
        "/api/benchmarks/{benchmark_id}/judge",
        "/api/compare",
    ):
        assert expected in paths
    compare_params = {p for _, path, p in BENCHMARK_ROUTES if path == "/api/compare"}
    assert compare_params == {"benchmark_id", "left_id", "right_id"}


def test_every_route_parameter_is_classified():
    missing = unclassified_params(server.app)
    assert not missing, (
        "route parameters not classified in ui/benchmark_access.py -- add each to "
        "BENCHMARK_ID_PARAMS if it names a benchmark, or NON_BENCHMARK_PARAMS if "
        "it does not:\n  " + "\n  ".join(f"{p}: {n}" for p, n in missing)
    )


def test_no_route_an_anonymous_caller_can_reach_takes_a_benchmark_in_its_body():
    """
    The wall reads path and query parameters only. That is sound while every
    route with a benchmark in its body requires `full`, which a caller the wall
    would refuse can never hold. This keeps it sound.
    """
    offenders = []
    for route in iter_routes(server.app):
        path = getattr(route, "path", "")
        dependant = getattr(route, "dependant", None)
        if not path.startswith("/api") or dependant is None:
            continue
        for method in sorted((getattr(route, "methods", None) or set()) - SAFE_METHODS):
            if required_tier(method, path) >= Tier.FULL:
                continue
            for field in dependant.body_params:
                annotation = getattr(field, "type_", None) or getattr(
                    getattr(field, "field_info", None), "annotation", None
                )
                names = {field.name} | set(
                    getattr(annotation, "model_fields", {}) or {}
                )
                if any("benchmark" in n or n in BENCHMARK_ID_PARAMS for n in names):
                    offenders.append(f"{method} {path}")
    assert not offenders, offenders


# --- the refusal ----------------------------------------------------------


@pytest.mark.parametrize("spelling", [SECRET, SECRET.upper()])
@pytest.mark.parametrize("method,path,param", BENCHMARK_ROUTES)
def test_every_route_naming_it_refuses_an_anonymous_caller(
    client, method, path, param, spelling
):
    url, query = _request(path, param, spelling)
    resp = client.request(method, url, params=query, json={})
    assert resp.status_code == 401, (method, url, query, resp.text)
    assert resp.json()["sign_in_required"] is True
    assert resp.headers["x-robots-tag"] == NOINDEX


@pytest.mark.parametrize("method,path,param", BENCHMARK_ROUTES)
def test_the_same_routes_do_not_ask_a_signed_in_caller_to_sign_in(
    client, signed_in, method, path, param
):
    url, query = _request(path, param, SECRET)
    resp = client.request(method, url, params=query, json={})
    assert resp.status_code != 401, (method, url, resp.text)


@pytest.mark.parametrize("method,path,param", BENCHMARK_ROUTES)
def test_an_unrestricted_benchmark_is_not_walled(client, method, path, param):
    url, query = _request(path, param, OPEN)
    resp = client.request(method, url, params=query, json={})
    assert resp.status_code != 401, (method, url, resp.text)
    assert "x-robots-tag" not in resp.headers


def test_a_signed_in_caller_gets_the_data(client, signed_in):
    resp = client.get(f"/api/benchmarks/{SECRET}/summary")
    assert resp.status_code == 200
    assert resp.json()["pipelines"][0]["name"] == "p1"
    # Signed in or not, the address stays out of search results.
    assert resp.headers["x-robots-tag"] == NOINDEX


def test_the_local_operator_is_not_walled(data_root):
    mode, remote = runtime.get_mode(), runtime.is_remote_deployment()
    runtime.set_mode(Tier.FULL)
    runtime.set_remote_deployment(False)
    try:
        resp = TestClient(server.app).get(f"/api/benchmarks/{SECRET}/summary")
    finally:
        runtime.set_mode(mode)
        runtime.set_remote_deployment(remote)
    assert resp.status_code == 200


def test_full_mode_on_a_reachable_host_still_asks_for_sign_in(data_root):
    mode, remote = runtime.get_mode(), runtime.is_remote_deployment()
    runtime.set_mode(Tier.FULL)
    runtime.set_remote_deployment(True)
    try:
        resp = TestClient(server.app).get(f"/api/benchmarks/{SECRET}/summary")
    finally:
        runtime.set_mode(mode)
        runtime.set_remote_deployment(remote)
    assert resp.status_code == 401


def test_a_search_for_its_name_is_not_a_request_for_it(client):
    """
    The refusal reads route parameters, not the URL text. (The handler itself
    answers 503 here: a shared deployment does not build indices on request.)
    """
    resp = client.get(f"/api/benchmarks/{OPEN}/errors", params={"q": SECRET})
    assert resp.status_code != 401
    assert "sign_in_required" not in resp.json()


def test_the_traversal_the_id_check_exists_for_is_real(client, signed_in):
    """
    `/api/compare` builds a filename from `left_id`, so `charts/../secret_bench`
    reads the secret benchmark's summary under a name that is not
    `secret_bench`. Shown for a signed-in caller, who is let through to the
    handler, so the test below is known to guard something.
    """
    resp = client.get(
        "/api/compare",
        params={
            "benchmark_id": OPEN,
            "left_id": f"charts/../{SECRET}",
            "right_id": OPEN,
        },
    )
    assert resp.status_code == 200
    assert resp.json()["rows"]


@pytest.mark.parametrize(
    "left_id", [f"charts/../{SECRET}", f"{SECRET} ", f"./{SECRET}", ""]
)
def test_an_anonymous_id_outside_the_id_alphabet_is_refused(client, left_id):
    resp = client.get(
        "/api/compare",
        params={"benchmark_id": OPEN, "left_id": left_id, "right_id": OPEN},
    )
    assert resp.status_code == 400
    assert resp.json()["detail"] == "'left_id' is not a valid benchmark id."


@pytest.mark.parametrize(
    "logo",
    [
        "secret-logo.png",
        f"{SECRET}.png",
        f"{SECRET.upper()}.PNG",
        # Starlette decodes this to x/../secret-logo.png, which the handler's
        # resolve() turns into the real file.
        "x/%2e%2e/secret-logo.png",
    ],
)
def test_its_logo_is_walled_too(client, logo):
    resp = client.get(f"/api/static/benchmarks/logos/{logo}")
    assert resp.status_code == 401


def test_an_unrestricted_logo_is_served(client):
    assert client.get("/api/static/benchmarks/logos/demo.png").status_code == 200


# --- the listing ----------------------------------------------------------


def test_the_anonymous_listing_leaves_it_out(client):
    ids = [b["benchmark_id"] for b in client.get("/api/benchmarks").json()["items"]]
    assert OPEN in ids
    assert SECRET not in ids


def test_a_signed_in_listing_includes_it_and_says_why(client, signed_in):
    items = {
        b["benchmark_id"]: b for b in client.get("/api/benchmarks").json()["items"]
    }
    assert items[SECRET]["requires_sign_in"] is True
    assert items[OPEN]["requires_sign_in"] is False


# --- where the flag comes from --------------------------------------------


def test_the_packaged_flag_holds_when_the_data_root_registry_lacks_it(client):
    """
    provision.sh seeds benchmarks.json into the data root once and never
    overwrites it. This data root's registry has no Beaver entry at all -- as a
    deployment provisioned before 1.6.0 has one without the flag -- and Beaver
    is refused anyway, because the packaged copy flags it.
    """
    registry = json.loads(
        (Path(os.environ["TEXT2SQL_DATA_ROOT"]) / "benchmarks.json").read_text()
    )
    assert "beaver" not in registry
    resp = client.get("/api/benchmarks/beaver/summary")
    assert resp.status_code == 401


def test_the_environment_can_restrict_a_benchmark(client, monkeypatch):
    monkeypatch.setenv(SIGN_IN_BENCHMARKS_ENV, f" {OPEN.upper()} ,other")
    assert client.get(f"/api/benchmarks/{OPEN}/summary").status_code == 401


def test_an_unreadable_registry_keeps_its_last_answer(data_root):
    """A registry that cannot be parsed right now must not lift a restriction."""
    assert SECRET in benchmark_access.restricted_benchmarks()
    registry = data_root / "benchmarks.json"
    registry.write_text("{ not json", encoding="utf-8")
    os.utime(registry, ns=(1, 1))
    assert SECRET in benchmark_access.restricted_benchmarks()


def test_a_flag_written_as_a_string_still_restricts(data_root):
    registry = data_root / "benchmarks.json"
    entries = json.loads(registry.read_text())
    entries[OPEN]["requires_sign_in"] = "true"
    registry.write_text(json.dumps(entries), encoding="utf-8")
    os.utime(registry, ns=(2, 2))
    assert OPEN in benchmark_access.restricted_benchmarks()


@pytest.mark.parametrize(
    "registry, benchmark_id",
    [
        ("data/benchmarks.json", "beaver"),
        ("src/text2sql_eval_toolkit/data/benchmarks.json", "beaver"),
        ("data/test-benchmarks.json", "beaver_test_10"),
        ("src/text2sql_eval_toolkit/data/test-benchmarks.json", "beaver_test_10"),
    ],
)
def test_beaver_is_flagged_in_every_registry_copy(registry, benchmark_id):
    """
    Every copy, because the flag is honoured in every copy: removing it from one
    lifts nothing, and a reader of that one copy would believe otherwise.
    """
    entries = json.loads((REPO_ROOT / registry).read_text(encoding="utf-8"))
    assert entries[benchmark_id].get("requires_sign_in") is True


def test_editing_a_benchmark_keeps_its_flag(data_root):
    """The edit form has no field for the flag, so saving it must not drop it."""
    mode, remote = runtime.get_mode(), runtime.is_remote_deployment()
    runtime.set_mode(Tier.FULL)
    runtime.set_remote_deployment(False)
    try:
        body = {k: v for k, v in _entry(SECRET).items()}
        body["description"] = "edited"
        resp = TestClient(server.app).put(f"/api/benchmarks/{SECRET}", json=body)
    finally:
        runtime.set_mode(mode)
        runtime.set_remote_deployment(remote)
    assert resp.status_code == 200, resp.text
    saved = json.loads((data_root / "benchmarks.json").read_text())[SECRET]
    assert saved["description"] == "edited"
    assert saved["requires_sign_in"] is True


# --- crawlers and the app shell -------------------------------------------


@pytest.fixture
def spa_client(data_root, shared_deployment):
    dist = data_root / "dashboard" / "dist"
    dist.mkdir(parents=True)
    (dist / "index.html").write_text(
        "<!doctype html><title>dash</title>", encoding="utf-8"
    )

    # A fresh app with the real middleware and routes, so the shell is served
    # through the same header logic as production without mounting static files
    # onto the shared `server.app`.
    app = FastAPI()
    app.router.routes = list(server.app.router.routes)
    middleware.install(app)
    server.mount_static(app)
    return TestClient(app)


@pytest.mark.parametrize(
    "url",
    [
        f"/benchmark/{SECRET}",
        f"/benchmark/{SECRET}/pipeline/p1/record/r1",
        f"/run/{SECRET}/record/r1",
        f"/errors?benchmark={SECRET}&page=2",
        f"/insights?benchmark={SECRET.upper()}",
        f"/compare/profile?benchmarks={OPEN},{SECRET}",
    ],
)
def test_the_app_shell_for_it_is_noindex(spa_client, url):
    resp = spa_client.get(url)
    assert resp.status_code == 200
    assert "<title>dash</title>" in resp.text
    assert resp.headers["x-robots-tag"] == NOINDEX


@pytest.mark.parametrize(
    "url", ["/", f"/benchmark/{OPEN}", f"/errors?benchmark={OPEN}", "/docs"]
)
def test_other_pages_are_indexable(spa_client, url):
    resp = spa_client.get(url)
    assert resp.status_code == 200
    assert "x-robots-tag" not in resp.headers
