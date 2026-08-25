#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
End-to-end checks that the index-backed endpoints return what the previous
full-parse implementations returned.

``_reference_*`` helpers reimplement the old behaviour directly over the parsed
artifact; the API responses are compared against them. These are the tests that
justify having deleted the old code paths.
"""

import json

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from text2sql_eval_toolkit.ui import server  # noqa: E402

PIPE_A = "modelA-greedy-zero-shot-chatapi"
PIPE_B = "modelB-agentic-baseline1-3attempts"


def _records(n=60):
    out = []
    for i in range(n):
        ev_a = {
            "execution_accuracy": i % 2,
            "subset_non_empty_execution_accuracy": (i // 2) % 2,
            "llm_score": 0.5 if i % 5 == 0 else float(i % 2),
            "llm_explanation": f"note {i}",
        }
        ev_b = {"execution_accuracy": (i + 1) % 2}
        preds = {PIPE_A: {"predicted_sql": f"SELECT {i}", "evaluation": ev_a}}
        if i % 7:  # some records lack the second pipeline entirely
            preds[PIPE_B] = {"predicted_sql": f"SELECT {i} -- b", "evaluation": ev_b}
        out.append(
            {
                "id": f"rec-{i:03d}",
                "question": f"Question number {i} about {'orders' if i % 3 else 'customers'}",
                "db_id": "shop" if i % 2 else "hr",
                "sql": [f"SELECT {i} -- gold"],
                "gt_df": [],
                "predictions": preds,
            }
        )
    return out


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    root = tmp_path_factory.mktemp("api")
    results = root / "results"
    results.mkdir()
    data = _records()
    (results / "demo-predictions_eval.json").write_text(
        json.dumps(data), encoding="utf-8"
    )

    original = server.get_data_root
    server.get_data_root = lambda: root  # type: ignore[assignment]
    server.invalidate_index_cache()
    try:
        yield TestClient(server.app), data
    finally:
        server.get_data_root = original  # type: ignore[assignment]
        server.invalidate_index_cache()


def _reference_filter(
    data,
    *,
    q=None,
    pipeline=None,
    metric="execution_accuracy",
    value=None,
    op="eq",
    pipeline2=None,
    metric2=None,
    disagree=False,
    failed_only=False,
):
    def get_metric(rec, pl, m):
        preds = rec.get("predictions", {})
        if pl not in preds:
            return None
        v = preds[pl].get("evaluation", {}).get(m)
        return float(v) if isinstance(v, (int, float)) else None

    def op_ok(lhs, rhs, o):
        if lhs is None:
            return False
        return {
            "eq": lhs == rhs,
            "ne": lhs != rhs,
            "lt": lhs < rhs,
            "gt": lhs > rhs,
            "le": lhs <= rhs,
            "ge": lhs >= rhs,
        }.get(o, False)

    out = []
    for rec in data:
        if q:
            ql = q.lower()
            if ql not in str(rec["id"]).lower() and ql not in rec["question"].lower():
                continue
        if pipeline and value is not None:
            if not op_ok(get_metric(rec, pipeline, metric), value, op):
                continue
        if failed_only and get_metric(rec, pipeline, "execution_accuracy") != 0:
            continue
        if pipeline and pipeline2 and disagree:
            v1 = get_metric(rec, pipeline, metric)
            v2 = get_metric(rec, pipeline2, metric2 or metric)
            if v1 is None or v2 is None or v1 == v2:
                continue
        out.append(rec)
    return out


CASES = [
    {},
    {"q": "customers"},
    {"q": "REC-005"},
    {"pipeline": PIPE_A, "metric": "execution_accuracy", "value": 0},
    {"pipeline": PIPE_A, "metric": "execution_accuracy", "value": 1, "op": "eq"},
    {"pipeline": PIPE_A, "metric": "llm_score", "value": 0.5, "op": "ge"},
    {"pipeline": PIPE_A, "metric": "llm_score", "value": 1, "op": "lt"},
    {"pipeline": PIPE_A, "failed_only": True},
    {"pipeline": PIPE_A, "pipeline2": PIPE_B, "disagree": True},
    {"q": "orders", "pipeline": PIPE_A, "metric": "execution_accuracy", "value": 0},
]


@pytest.mark.parametrize("params", CASES, ids=range(len(CASES)))
def test_list_errors_matches_reference(client, params):
    api, data = client
    expected = [r["id"] for r in _reference_filter(data, **params)]
    resp = api.get("/api/benchmarks/demo/errors", params={**params, "page_size": 500})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["total"] == len(expected)
    assert [i["record_id"] for i in body["items"]] == expected


def test_list_errors_pagination_is_stable(client):
    api, data = client
    expected = [r["id"] for r in data]
    seen = []
    for page in range(1, 7):
        body = api.get(
            "/api/benchmarks/demo/errors", params={"page": page, "page_size": 10}
        ).json()
        assert body["total"] == len(expected)
        seen += [i["record_id"] for i in body["items"]]
    assert seen == expected


def test_list_errors_exposes_every_pipeline_evaluation(client):
    api, data = client
    by_id = {r["id"]: r for r in data}
    body = api.get("/api/benchmarks/demo/errors", params={"page_size": 20}).json()
    for item in body["items"]:
        expected = {
            p: pred["evaluation"]
            for p, pred in by_id[item["record_id"]]["predictions"].items()
        }
        assert item["predictions"] == expected


def test_failed_only_without_pipeline_is_rejected(client):
    api, _ = client
    resp = api.get("/api/benchmarks/demo/errors", params={"failed_only": True})
    assert resp.status_code == 400
    assert "pipeline is required" in resp.json()["detail"]


def test_record_detail_returns_the_full_record(client):
    api, data = client
    for record in (data[0], data[31], data[-1]):
        resp = api.get(f"/api/benchmarks/demo/errors/{record['id']}")
        assert resp.status_code == 200
        assert resp.json() == record


def test_record_detail_unknown_id_is_404(client):
    api, _ = client
    assert api.get("/api/benchmarks/demo/errors/nope").status_code == 404


def test_pipeline_detail_payload(client):
    api, data = client
    record = data[2]
    resp = api.get(
        f"/api/benchmarks/demo/errors/{record['id']}/detail",
        params={"pipeline": PIPE_A},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["record_id"] == record["id"]
    assert body["pipeline"] == PIPE_A
    assert body["question"] == record["question"]
    assert body["db_id"] == record["db_id"]
    assert body["predicted_sql"] == record["predictions"][PIPE_A]["predicted_sql"]
    assert body["ground_truth_sql"] == record["sql"]


def test_pipeline_detail_unknown_pipeline_is_404(client):
    api, data = client
    resp = api.get(
        f"/api/benchmarks/demo/errors/{data[0]['id']}/detail",
        params={"pipeline": "not-a-pipeline"},
    )
    assert resp.status_code == 404


def test_missing_benchmark_is_404_with_actionable_detail(client):
    api, _ = client
    resp = api.get("/api/benchmarks/absent/errors")
    assert resp.status_code == 404
    assert "results fetch" in resp.json()["detail"]


def test_index_rebuilds_when_the_artifact_changes(client, tmp_path_factory):
    """A re-run that rewrites results must not keep serving stale rows."""
    api, data = client
    path = server.get_results_dir() / "demo-predictions_eval.json"

    before = api.get("/api/benchmarks/demo/errors", params={"page_size": 1}).json()
    assert before["total"] == len(data)

    extended = data + [{"id": "rec-new", "question": "added later", "predictions": {}}]
    path.write_text(json.dumps(extended), encoding="utf-8")

    after = api.get("/api/benchmarks/demo/errors", params={"page_size": 1}).json()
    assert after["total"] == len(data) + 1

    path.write_text(json.dumps(data), encoding="utf-8")  # restore for other tests
    assert api.get("/api/benchmarks/demo/errors", params={"page_size": 1}).json()[
        "total"
    ] == len(data)


def _binary(v):
    return 1 if v == 1 else 0


def test_confusion_by_pipeline_matches_reference(client):
    api, data = client
    resp = api.get(
        "/api/benchmarks/demo/insights/binary-metric-confusion-by-pipeline",
        params={"metric_a": "execution_accuracy", "metric_b": "llm_score"},
    )
    assert resp.status_code == 200, resp.text

    expected = {}
    for rec in data:
        for pipeline, pred in rec["predictions"].items():
            ev = pred.get("evaluation", {})
            a, b = ev.get("execution_accuracy"), ev.get("llm_score")
            if not isinstance(a, (int, float)) or not isinstance(b, (int, float)):
                continue
            bucket = expected.setdefault(
                pipeline, {"a0b0": 0, "a0b1": 0, "a1b0": 0, "a1b1": 0}
            )
            bucket[f"a{_binary(a)}b{_binary(b)}"] += 1

    rows = {r["pipeline"]: r for r in resp.json()["per_pipeline"]}
    assert set(rows) == set(expected)
    for pipeline, counts in expected.items():
        assert rows[pipeline]["counts"] == counts


def test_cross_pipeline_confusion_matches_reference(client):
    api, data = client
    resp = api.get(
        "/api/benchmarks/demo/insights/cross-pipeline-binary-metric-confusion",
        params={
            "pipeline_left": PIPE_A,
            "pipeline_right": PIPE_B,
            "metric_left": "execution_accuracy",
        },
    )
    assert resp.status_code == 200, resp.text

    expected = {"left0right0": 0, "left0right1": 0, "left1right0": 0, "left1right1": 0}
    for rec in data:
        preds = rec["predictions"]
        if PIPE_A not in preds or PIPE_B not in preds:
            continue
        left = preds[PIPE_A]["evaluation"].get("execution_accuracy")
        right = preds[PIPE_B]["evaluation"].get("execution_accuracy")
        if not isinstance(left, (int, float)) or not isinstance(right, (int, float)):
            continue
        expected[f"left{_binary(left)}right{_binary(right)}"] += 1

    assert resp.json()["counts"] == expected


def test_record_count_cache_recounts_after_a_file_change(tmp_path):
    """Caching must key on content, not just path, or edits go unnoticed."""
    import json as _json

    from text2sql_eval_toolkit.ui import server as srv

    path = tmp_path / "bench.json"
    path.write_text(_json.dumps([{"id": 1}, {"id": 2}]))
    assert srv.count_records(path) == 2
    assert srv.count_records(path) == 2  # served from cache

    path.write_text(_json.dumps([{"id": 1}, {"id": 2}, {"id": 3}]))
    assert srv.count_records(path) == 3, "an edited file must be recounted"


def test_record_count_handles_missing_and_none(tmp_path):
    from text2sql_eval_toolkit.ui import server as srv

    assert srv.count_records(None) == 0
    assert srv.count_records(tmp_path / "absent.json") == 0


def test_no_async_handler_reaches_a_blocking_index_build():
    """
    get_index() can build an index, which takes seconds on a large artifact.
    Reaching it synchronously from an async handler would stall the event loop
    for every concurrent request, so async handlers must offload it.

    Enforced structurally: a new async endpoint that forgets this fails here
    rather than degrading the server under load.
    """
    import ast
    from pathlib import Path

    from text2sql_eval_toolkit.ui import server as srv

    source = Path(srv.__file__).read_text()
    tree = ast.parse(source)
    funcs = {
        n.name: n
        for n in ast.walk(tree)
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
    }

    def called_names(node):
        return {
            (
                c.func.attr
                if isinstance(c.func, ast.Attribute)
                else (c.func.id if isinstance(c.func, ast.Name) else "")
            )
            for c in ast.walk(node)
            if isinstance(c, ast.Call)
        }

    def reaches_sync(name, target, seen=None, depth=0):
        if seen is None:
            seen = set()
        if name in seen or depth > 6:
            return None
        seen.add(name)
        node = funcs.get(name)
        if node is None:
            return None
        names = called_names(node)
        if target in names:
            return [name, target]
        for callee in sorted(names):
            sub = funcs.get(callee)
            if sub is not None and not isinstance(sub, ast.AsyncFunctionDef):
                path = reaches_sync(callee, target, seen, depth + 1)
                if path:
                    return [name] + path
        return None

    offenders = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.AsyncFunctionDef):
            continue
        if not any(ast.unparse(d).startswith("app.") for d in node.decorator_list):
            continue
        # Offloading via asyncio.to_thread(get_index, ...) is the accepted fix.
        body = ast.unparse(node)
        if "to_thread(get_index" in body:
            continue
        path = reaches_sync(node.name, "get_index")
        if path:
            offenders.append(" -> ".join(path))

    assert (
        not offenders
    ), "async endpoints reach get_index() without offloading it:\n  " + "\n  ".join(
        offenders
    )
