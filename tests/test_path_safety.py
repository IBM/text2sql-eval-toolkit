#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Names from requests must not reach files outside the folder they belong to.

Covers `safe_join` and each place that now builds a path with it: benchmark
ids for the evaluation index, judge configs picked by path, SQLite database
ids, and the DB2 LIMIT rewrite that CodeQL flagged as a slow regex.
"""

import json
import os
import time

import pytest

from text2sql_eval_toolkit.execution.execution_tools import _normalize_sql_for_db2
from text2sql_eval_toolkit.indexing.store import EvalIndex
from text2sql_eval_toolkit.safe_paths import safe_join

HOSTILE_NAMES = ["..", ".", "", "../x", "a/b", "a\\b", "/etc/passwd", "x\0y"]


# --- safe_join -------------------------------------------------------------


def test_plain_names_join_inside_the_base(tmp_path):
    path = safe_join(tmp_path, "db", "db.sqlite")
    assert path == tmp_path / "db" / "db.sqlite"
    assert path.is_absolute()


@pytest.mark.parametrize("name", HOSTILE_NAMES)
def test_names_that_are_not_plain_are_refused(tmp_path, name):
    with pytest.raises(ValueError):
        safe_join(tmp_path, name)


def test_no_name_is_refused(tmp_path):
    with pytest.raises(ValueError):
        safe_join(tmp_path)


def test_a_symlinked_base_is_not_resolved(tmp_path):
    real = tmp_path / "real"
    real.mkdir()
    link = tmp_path / "link"
    link.symlink_to(real)
    assert safe_join(link, "f.json") == link / "f.json"


# --- benchmark ids -> evaluation index -------------------------------------


def test_for_benchmark_refuses_an_id_that_leaves_the_results_folder(tmp_path):
    results = tmp_path / "results"
    results.mkdir()
    (tmp_path / "x-predictions_eval.json").write_text("[]", encoding="utf-8")
    with pytest.raises(FileNotFoundError):
        EvalIndex.for_benchmark("../x", results)


def test_get_index_answers_404_for_an_id_with_a_separator(tmp_path, monkeypatch):
    pytest.importorskip("fastapi")
    from fastapi import HTTPException

    from text2sql_eval_toolkit.ui.indexes import get_index

    (tmp_path / "results").mkdir()
    (tmp_path / "x-predictions_eval.json").write_text("[]", encoding="utf-8")
    monkeypatch.setenv("TEXT2SQL_DATA_ROOT", str(tmp_path))
    with pytest.raises(HTTPException) as excinfo:
        get_index("../x")
    assert excinfo.value.status_code == 404


# --- SQLite database ids ---------------------------------------------------


def test_sqlite_db_id_must_be_a_plain_name(tmp_path, monkeypatch):
    pytest.importorskip("fastapi")
    from text2sql_eval_toolkit.ui.routers_execution import _resolve_sqlite_db_path

    monkeypatch.setenv("TEXT2SQL_DATA_ROOT", str(tmp_path))
    with pytest.raises(ValueError, match="Invalid db_id"):
        _resolve_sqlite_db_path(str(tmp_path / "dbs"), "../../elsewhere")


def test_sqlite_db_id_still_finds_a_real_database(tmp_path, monkeypatch):
    pytest.importorskip("fastapi")
    from text2sql_eval_toolkit.ui.routers_execution import _resolve_sqlite_db_path

    db = tmp_path / "dbs" / "concert" / "concert.sqlite"
    db.parent.mkdir(parents=True)
    db.write_bytes(b"")
    monkeypatch.setenv("TEXT2SQL_DATA_ROOT", str(tmp_path))
    assert _resolve_sqlite_db_path("dbs", "concert") == db


# --- judge config picked by path ---------------------------------------------


@pytest.fixture
def judge_dirs(tmp_path, monkeypatch):
    pytest.importorskip("fastapi")
    from text2sql_eval_toolkit.ui import routers_judge

    monkeypatch.setenv("TEXT2SQL_DATA_ROOT", str(tmp_path))
    user_dir = routers_judge._user_judge_config_dir()
    user_dir.mkdir(parents=True)
    (user_dir / "mine.yaml").write_text("model: {id: 'wxai:x'}\n", encoding="utf-8")
    return routers_judge, user_dir


def test_no_judge_config_means_the_default(judge_dirs):
    routers_judge, _ = judge_dirs
    assert routers_judge.judge_config_path_from_request(None) is None
    assert routers_judge.judge_config_path_from_request("  ") is None


def test_a_listed_judge_config_path_is_accepted(judge_dirs):
    routers_judge, user_dir = judge_dirs
    packaged = routers_judge._judge_config_dir() / "llm_judge_default_config.yaml"
    for path in (packaged, user_dir / "mine.yaml"):
        assert routers_judge.judge_config_path_from_request(str(path)) == (
            os.path.abspath(path)
        )


def test_a_bare_judge_config_name_is_accepted(judge_dirs):
    routers_judge, user_dir = judge_dirs
    assert routers_judge.judge_config_path_from_request("mine") == str(
        user_dir / "mine.yaml"
    )


def test_a_judge_config_outside_both_folders_is_refused(judge_dirs, tmp_path):
    routers_judge, user_dir = judge_dirs
    elsewhere = tmp_path / "elsewhere.yaml"
    elsewhere.write_text("model: {id: 'wxai:x'}\n", encoding="utf-8")
    for value in (
        str(elsewhere),
        "/etc/passwd",
        str(user_dir / ".." / "elsewhere.yaml"),
        "../elsewhere",
    ):
        with pytest.raises(FileNotFoundError):
            routers_judge.judge_config_path_from_request(value)


@pytest.fixture
def judge_client(judge_dirs, monkeypatch):
    from fastapi.testclient import TestClient

    from text2sql_eval_toolkit.ui import server
    from text2sql_eval_toolkit.ui.capabilities import Tier

    original = server.get_mode()
    server.set_mode(Tier.FULL)
    try:
        yield TestClient(server.app)
    finally:
        server.set_mode(original)


def test_a_judge_config_symlinked_outside_its_folder_is_refused(judge_dirs, tmp_path):
    routers_judge, user_dir = judge_dirs
    outside = tmp_path / "outside.yaml"
    outside.write_text("secret: 1\n", encoding="utf-8")
    (user_dir / "linked.yaml").symlink_to(outside)

    with pytest.raises(FileNotFoundError):
        routers_judge._resolve_judge_config_path("linked")
    with pytest.raises(FileNotFoundError):
        routers_judge._judge_config_write_path("linked")
    with pytest.raises(FileNotFoundError):
        routers_judge.judge_config_path_from_request(str(user_dir / "linked.yaml"))
    with pytest.raises(FileNotFoundError):
        routers_judge.judge_config_path_from_request("linked")


def test_a_dangling_symlink_cannot_be_used_to_write_outside(judge_dirs, tmp_path):
    routers_judge, user_dir = judge_dirs
    (user_dir / "linked.yaml").symlink_to(tmp_path / "not-yet.yaml")
    with pytest.raises(FileNotFoundError):
        routers_judge._judge_config_write_path("linked")


def test_the_config_endpoints_do_not_follow_a_symlink_outside(
    judge_client, judge_dirs, tmp_path
):
    _, user_dir = judge_dirs
    outside = tmp_path / "outside.yaml"
    outside.write_text("secret: 1\n", encoding="utf-8")
    (user_dir / "linked.yaml").symlink_to(outside)

    assert judge_client.get("/api/llm-judge/configs/linked").status_code == 404
    resp = judge_client.put(
        "/api/llm-judge/configs/linked",
        json={"model": {"id": "wxai:x"}, "prompt_template": "p"},
    )
    assert resp.status_code == 400
    assert outside.read_text(encoding="utf-8") == "secret: 1\n"

    # The rest of the list still loads; only the bad entry is left out.
    listed = judge_client.get("/api/llm-judge/configs")
    assert listed.status_code == 200
    names = {c["name"] for c in listed.json()["items"]}
    assert "mine" in names and "linked" not in names


def test_a_symlink_to_another_config_in_the_same_folder_still_works(judge_dirs):
    routers_judge, user_dir = judge_dirs
    (user_dir / "alias.yaml").symlink_to(user_dir / "mine.yaml")
    path = routers_judge._resolve_judge_config_path("alias")
    assert path.read_text(encoding="utf-8").startswith("model:")


def test_a_symlinked_judge_config_folder_still_works(tmp_path, monkeypatch):
    pytest.importorskip("fastapi")
    from text2sql_eval_toolkit.ui import routers_judge

    real = tmp_path / "elsewhere" / "configs"
    real.mkdir(parents=True)
    (real / "mine.yaml").write_text("model: {id: 'wxai:x'}\n", encoding="utf-8")
    root = tmp_path / "root"
    root.mkdir()
    (root / "llm_judge_config").symlink_to(real, target_is_directory=True)
    monkeypatch.setenv("TEXT2SQL_DATA_ROOT", str(root))

    assert routers_judge._resolve_judge_config_path("mine").is_file()
    assert routers_judge._judge_config_write_path("new").parent.resolve() == real
    # Either spelling of the folder is accepted in a request.
    for folder in (root / "llm_judge_config", real):
        loaded = routers_judge.judge_config_path_from_request(str(folder / "mine.yaml"))
        assert os.path.samefile(loaded, real / "mine.yaml")


def test_a_symlink_loop_is_not_found_rather_than_an_error(judge_client, judge_dirs):
    _, user_dir = judge_dirs
    (user_dir / "loop.yaml").symlink_to(user_dir / "loop.yaml")
    assert judge_client.get("/api/llm-judge/configs/loop").status_code == 404
    assert judge_client.get("/api/llm-judge/configs").status_code == 200


def test_evaluation_job_with_an_outside_judge_config_is_400(tmp_path, monkeypatch):
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    from text2sql_eval_toolkit.ui import server
    from text2sql_eval_toolkit.ui.capabilities import Tier

    results = tmp_path / "results"
    results.mkdir()
    (results / "demo-predictions_eval.json").write_text(
        json.dumps([{"id": "r1", "question": "q", "predictions": {}}]),
        encoding="utf-8",
    )
    monkeypatch.setenv("TEXT2SQL_DATA_ROOT", str(tmp_path))
    original = server.get_mode()
    server.set_mode(Tier.FULL)
    try:
        resp = TestClient(server.app).post(
            "/api/benchmarks/demo/evaluate",
            json={"use_llm": True, "llm_judge_config_path": "/etc/passwd"},
        )
    finally:
        server.set_mode(original)
    assert resp.status_code == 400
    assert "Not a judge config" in resp.json()["detail"]


# --- DB2 LIMIT rewrite -------------------------------------------------------


@pytest.mark.parametrize(
    "sql, expected",
    [
        ("SELECT * FROM t LIMIT 5", "SELECT * FROM t FETCH FIRST 5 ROWS ONLY"),
        ("SELECT * FROM t LIMIT 5;", "SELECT * FROM t FETCH FIRST 5 ROWS ONLY"),
        (
            "SELECT * FROM t\n  limit   10 ; ",
            "SELECT * FROM t FETCH FIRST 10 ROWS ONLY",
        ),
        ("SELECT a FROM t LIMIT x", "SELECT a FROM t LIMIT x"),
        ("SELECT 1", "SELECT 1"),
    ],
)
def test_db2_limit_is_rewritten(sql, expected):
    assert _normalize_sql_for_db2(sql) == expected


def test_db2_limit_rewrite_is_fast_on_long_whitespace():
    sql = "SELECT 1" + " " * 50_000 + "x"
    started = time.perf_counter()
    _normalize_sql_for_db2(sql)
    assert time.perf_counter() - started < 0.5
