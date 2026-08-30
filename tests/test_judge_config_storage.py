#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Judge configs are written to the data root, never into the installed package.

Saving one used to write straight into ``site-packages``. On the deployment that
is a root-owned tree and the server runs unprivileged, so every save returned a
bare 500; where the permissions do allow the write, the next pip upgrade
discards it. Writes now land in the data root and shadow the packaged config of
the same name, and deleting the copy restores the original.
"""

import json

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from text2sql_eval_toolkit.ui import routers_judge, server  # noqa: E402
from text2sql_eval_toolkit.ui.capabilities import Tier  # noqa: E402

PACKAGED = "llm_judge_default_config"
BODY = {
    "model": {"id": "anthropic:claude-sonnet-4-5"},
    "prompt_template": "judge: {question}",
}


@pytest.fixture
def client(tmp_path, monkeypatch):
    results = tmp_path / "results"
    results.mkdir()
    (results / "demo-predictions_eval.json").write_text(
        json.dumps([{"id": "r1", "question": "q", "predictions": {}}]), encoding="utf-8"
    )
    monkeypatch.setenv("TEXT2SQL_DATA_ROOT", str(tmp_path))
    server.invalidate_index_cache()
    server.reset_rate_limits()
    original = server.get_mode()
    server.set_mode(Tier.FULL)
    try:
        yield TestClient(server.app)
    finally:
        server.set_mode(original)
        server.configure_cors(original)
        server.invalidate_index_cache()
        server.reset_rate_limits()


def test_saving_does_not_touch_the_installed_package(client, tmp_path):
    packaged_dir = routers_judge._judge_config_dir()
    before = {p: p.read_bytes() for p in packaged_dir.glob("*.yaml")}

    resp = client.put(f"/api/llm-judge/configs/{PACKAGED}", json=BODY)
    assert resp.status_code == 200, resp.text

    after = {p: p.read_bytes() for p in packaged_dir.glob("*.yaml")}
    assert after == before, "the packaged config was modified"
    assert (tmp_path / "llm_judge_config" / f"{PACKAGED}.yaml").is_file()


def test_a_brand_new_config_can_be_created(client, tmp_path):
    """The case that prompted this: a Claude config, which did not exist before."""
    resp = client.put("/api/llm-judge/configs/llm_judge_claude", json=BODY)
    assert resp.status_code == 200, resp.text
    assert (tmp_path / "llm_judge_config" / "llm_judge_claude.yaml").is_file()

    got = client.get("/api/llm-judge/configs/llm_judge_claude")
    assert got.status_code == 200
    assert got.json()["model"]["id"] == "anthropic:claude-sonnet-4-5"

    names = {c["name"] for c in client.get("/api/llm-judge/configs").json()["items"]}
    assert "llm_judge_claude" in names


def test_the_saved_copy_shadows_the_packaged_config(client):
    original = client.get(f"/api/llm-judge/configs/{PACKAGED}").json()
    assert original["model"]["id"] != BODY["model"]["id"]

    client.put(f"/api/llm-judge/configs/{PACKAGED}", json=BODY)

    # Both the read endpoint and the judge's own loader must see the edit;
    # otherwise a saved config would appear to take and then be ignored.
    assert client.get(f"/api/llm-judge/configs/{PACKAGED}").json() == BODY
    assert routers_judge._load_judge_config_by_name(PACKAGED) == BODY


def test_listing_merges_the_two_directories_without_duplicating(client):
    client.put(f"/api/llm-judge/configs/{PACKAGED}", json=BODY)
    client.put("/api/llm-judge/configs/llm_judge_claude", json=BODY)

    items = client.get("/api/llm-judge/configs").json()["items"]
    names = [c["name"] for c in items]
    assert len(names) == len(set(names)), f"duplicate entries: {names}"

    by_name = {c["name"]: c for c in items}
    assert by_name[PACKAGED]["user_defined"] is True
    assert by_name["llm_judge_claude"]["user_defined"] is True
    assert by_name["llm_judge_alt_config"]["user_defined"] is False


def test_deleting_an_edit_restores_the_packaged_config(client):
    original = client.get(f"/api/llm-judge/configs/{PACKAGED}").json()
    client.put(f"/api/llm-judge/configs/{PACKAGED}", json=BODY)

    resp = client.delete(f"/api/llm-judge/configs/{PACKAGED}")
    assert resp.status_code == 200, resp.text
    assert resp.json()["reverted_to_packaged"] is True
    assert client.get(f"/api/llm-judge/configs/{PACKAGED}").json() == original


def test_deleting_a_user_config_removes_it_entirely(client):
    client.put("/api/llm-judge/configs/llm_judge_claude", json=BODY)
    resp = client.delete("/api/llm-judge/configs/llm_judge_claude")
    assert resp.status_code == 200
    assert resp.json()["reverted_to_packaged"] is False
    assert client.get("/api/llm-judge/configs/llm_judge_claude").status_code == 404


def test_deleting_a_packaged_config_that_was_never_edited_is_a_404(client):
    """The packaged files are not the dashboard's to delete."""
    resp = client.delete(f"/api/llm-judge/configs/{PACKAGED}")
    assert resp.status_code == 404
    assert routers_judge._resolve_judge_config_path(PACKAGED).is_file()


@pytest.mark.parametrize(
    "name", ["../../../etc/passwd", "..", ".", ".hidden", "", "a/../../b", "sub/dir"]
)
def test_hostile_names_are_refused_on_the_write_path(name):
    with pytest.raises(FileNotFoundError):
        routers_judge._judge_config_write_path(name)


def test_writing_a_traversal_name_over_http_is_refused(client, tmp_path):
    resp = client.put("/api/llm-judge/configs/..%2f..%2fowned", json=BODY)
    assert resp.status_code in (400, 404), resp.status_code
    assert not (tmp_path.parent / "owned.yaml").exists()


def test_deleting_a_traversal_name_over_http_is_refused(client):
    resp = client.delete("/api/llm-judge/configs/..%2f..%2fowned")
    assert resp.status_code in (400, 404), resp.status_code
