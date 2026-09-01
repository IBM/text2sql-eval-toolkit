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
import os
from pathlib import Path

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


# --- what the written file looks like ---------------------------------------


def test_a_multi_line_prompt_is_written_as_a_block_scalar(client, tmp_path):
    """
    ``yaml.safe_dump`` renders a long multi-line string as a single-quoted
    folded scalar: every line break becomes a blank line and the prose is
    rewrapped at 80 columns. It round-trips correctly and it is unreadable --
    and `prompt_template` is the bulk of every judge config, so every save
    through the dashboard turned a file that opened with `prompt_template: |`
    into one that did not.
    """
    prompt = "First line.\n\nSecond paragraph.\nThird line.\n"
    resp = client.put(
        "/api/llm-judge/configs/block_style",
        json={"model": {"id": "anthropic:x"}, "prompt_template": prompt},
    )
    assert resp.status_code == 200

    written = (tmp_path / "llm_judge_config" / "block_style.yaml").read_text()
    assert "prompt_template: |" in written
    # The folded form quotes the scalar and doubles every line break.
    assert "prompt_template: '" not in written

    # And the value is unchanged, which is the part that actually matters.
    import yaml

    assert yaml.safe_load(written)["prompt_template"] == prompt


def test_a_long_prompt_line_is_not_rewrapped(client, tmp_path):
    # A block scalar's line breaks are part of the prompt the judge is sent.
    # Folding one at 80 columns changes the prompt without anyone asking.
    line = "word " * 60
    client.put(
        "/api/llm-judge/configs/wide",
        json={"model": {"id": "anthropic:x"}, "prompt_template": f"{line}\nnext\n"},
    )
    written = (tmp_path / "llm_judge_config" / "wide.yaml").read_text()
    assert line.strip() in written


def test_a_single_line_string_stays_plain(client, tmp_path):
    # Block style on every string would make `id: |-` of a model name.
    client.put(
        "/api/llm-judge/configs/plain",
        json={"model": {"id": "anthropic:x"}, "prompt_template": "one line"},
    )
    written = (tmp_path / "llm_judge_config" / "plain.yaml").read_text()
    assert "id: anthropic:x" in written
    assert "prompt_template: one line" in written


def test_the_block_representer_is_not_registered_globally(client):
    """
    It is on a Dumper subclass, not on ``yaml.SafeDumper``. Registering it
    globally would change every other ``yaml.safe_dump`` in the process.
    """
    import yaml

    client.put(
        "/api/llm-judge/configs/scoped",
        json={"model": {"id": "anthropic:x"}, "prompt_template": "a\nb\n"},
    )
    assert "|" not in yaml.safe_dump({"k": "a\nb\n"})


# --- renaming --------------------------------------------------------------


def test_renaming_moves_the_config(client, tmp_path):
    client.put("/api/llm-judge/configs/before", json=BODY)
    resp = client.post(
        "/api/llm-judge/configs/before/rename", json={"new_name": "after"}
    )
    assert resp.status_code == 200
    names = {c["name"] for c in client.get("/api/llm-judge/configs").json()["items"]}
    assert "after" in names and "before" not in names
    assert client.get("/api/llm-judge/configs/after").json() == BODY


def test_renaming_a_packaged_config_is_refused(client):
    # It is read-only and shared with every install, so a "rename" would leave
    # the original sitting there under its own name.
    resp = client.post(
        f"/api/llm-judge/configs/{PACKAGED}/rename", json={"new_name": "mine"}
    )
    assert resp.status_code == 400
    assert "duplicate" in resp.json()["detail"].lower()


def test_renaming_onto_an_existing_name_is_refused(client):
    client.put("/api/llm-judge/configs/one", json=BODY)
    client.put("/api/llm-judge/configs/two", json=BODY)
    resp = client.post("/api/llm-judge/configs/one/rename", json={"new_name": "two"})
    assert resp.status_code == 409
    # Both survive; a refused rename must not consume either side.
    names = {c["name"] for c in client.get("/api/llm-judge/configs").json()["items"]}
    assert {"one", "two"} <= names


def test_renaming_onto_a_packaged_name_is_refused(client):
    # Writing onto one would silently shadow a shipped config.
    client.put("/api/llm-judge/configs/mine", json=BODY)
    resp = client.post(
        "/api/llm-judge/configs/mine/rename", json={"new_name": PACKAGED}
    )
    assert resp.status_code == 409


def test_renaming_to_an_invalid_name_is_refused(client):
    client.put("/api/llm-judge/configs/valid", json=BODY)
    for bad in ("../escape", "", "with space", ".hidden"):
        resp = client.post(
            "/api/llm-judge/configs/valid/rename", json={"new_name": bad}
        )
        assert resp.status_code == 400, bad
    assert client.get("/api/llm-judge/configs/valid").status_code == 200


def test_renaming_to_the_same_name_is_refused(client):
    client.put("/api/llm-judge/configs/same", json=BODY)
    resp = client.post("/api/llm-judge/configs/same/rename", json={"new_name": "same"})
    assert resp.status_code == 400


def test_renaming_an_edit_uncovers_the_packaged_config(client):
    # Same rule as deleting one: the shipped original becomes visible again.
    client.put(f"/api/llm-judge/configs/{PACKAGED}", json=BODY)
    resp = client.post(
        f"/api/llm-judge/configs/{PACKAGED}/rename", json={"new_name": "moved"}
    )
    assert resp.status_code == 200
    assert resp.json()["reverted_to_packaged"] is True
    names = {c["name"] for c in client.get("/api/llm-judge/configs").json()["items"]}
    assert {"moved", PACKAGED} <= names


def test_a_rename_cannot_be_raced_into_deleting_a_config(client, tmp_path, monkeypatch):
    """
    The existence check and the move are two operations, and POSIX rename()
    replaces its target silently. Two renames onto one name must not both
    succeed, because the second would delete the config the first just made.

    The competing request is simulated by creating the target in the window
    between the check and the claim -- which is exactly when it would arrive.
    """
    root = tmp_path / "llm_judge_config"
    client.put("/api/llm-judge/configs/one", json=BODY)
    winner = root / "taken.yaml"
    real_open = os.open

    def racing_open(path, flags, *args, **kwargs):
        if str(path) == str(winner) and not winner.exists():
            winner.write_text("model:\n  id: theirs\n", encoding="utf-8")
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(os, "open", racing_open)
    resp = client.post("/api/llm-judge/configs/one/rename", json={"new_name": "taken"})

    assert resp.status_code == 409, resp.text
    # Neither config was destroyed.
    assert (root / "one.yaml").is_file()
    assert winner.read_text(encoding="utf-8") == "model:\n  id: theirs\n"


def test_a_failed_move_leaves_no_empty_placeholder(client, tmp_path, monkeypatch):
    """The name is claimed before the move; a failed move must give it back."""
    root = tmp_path / "llm_judge_config"
    client.put("/api/llm-judge/configs/one", json=BODY)

    def failing_rename(self, target):
        raise OSError(13, "Permission denied")

    monkeypatch.setattr(Path, "rename", failing_rename)
    resp = client.post("/api/llm-judge/configs/one/rename", json={"new_name": "later"})

    assert resp.status_code == 500
    assert not (root / "later.yaml").exists(), "an empty config was left behind"
    assert (root / "one.yaml").is_file()
