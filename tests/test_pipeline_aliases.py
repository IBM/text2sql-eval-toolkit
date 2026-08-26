#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Aliases exist so a shared link stays intact when it is pasted into a chat
client or a mail body.  That makes them a link-integrity feature, and the two
ways they can fail are both silent: an alias that changes between processes
breaks links that used to work, and an alias that resolves to the wrong
pipeline shows the reader different numbers than the sender saw.
"""

import json

import pytest

from text2sql_eval_toolkit.ui.aliases import (
    ALIAS_LENGTH,
    alias_map,
    looks_like_alias,
    pipeline_alias,
    resolve_pipeline_ref,
)

PIPE_A = "wxai:openai/gpt-oss-120b-greedy-zero-shot-chatapi"
PIPE_B = "wxai:ibm/granite-4-h-small-agentic-baseline1-3attempts"


# --- the alias itself -----------------------------------------------------


def test_alias_is_stable_across_calls():
    """
    Derived rather than assigned, so two servers reading the same artifacts
    agree without coordinating -- and a link survives a re-fetch of the
    snapshot.
    """
    assert pipeline_alias(PIPE_A) == pipeline_alias(PIPE_A)


def test_alias_is_a_fixed_length_hex_string():
    alias = pipeline_alias(PIPE_A)
    assert len(alias) == ALIAS_LENGTH
    assert all(c in "0123456789abcdef" for c in alias)


def test_different_pipelines_get_different_aliases():
    assert pipeline_alias(PIPE_A) != pipeline_alias(PIPE_B)


def test_alias_is_shorter_than_what_it_replaces():
    # The whole point; a "short" form longer than the id would be worse than
    # nothing.
    assert len(pipeline_alias(PIPE_A)) < len(PIPE_A)


def test_alias_shape_is_distinguishable_from_a_pipeline_id():
    """
    Resolution has no marker prefix in the URL, so it relies on real ids never
    matching the alias shape. They contain `-`, and usually `:` and `/`.
    """
    assert looks_like_alias(pipeline_alias(PIPE_A))
    assert not looks_like_alias(PIPE_A)
    assert not looks_like_alias(PIPE_B)


@pytest.mark.parametrize(
    "ref",
    ["", "abc", "0123456789a", "0123456789A", "0123456789-", "zzzzzzzzzz"],
)
def test_things_that_are_not_aliases(ref):
    assert not looks_like_alias(ref)


# --- resolution -----------------------------------------------------------


def test_an_alias_resolves_to_its_pipeline():
    assert resolve_pipeline_ref(pipeline_alias(PIPE_A), [PIPE_A, PIPE_B]) == PIPE_A


def test_a_full_id_resolves_to_itself():
    assert resolve_pipeline_ref(PIPE_A, [PIPE_A, PIPE_B]) == PIPE_A


def test_an_unknown_reference_resolves_to_nothing():
    """
    The caller renders "not found" from this; returning an arbitrary pipeline
    would show the reader a view the link never pointed at.
    """
    assert resolve_pipeline_ref("deadbeef00", [PIPE_A]) is None
    assert resolve_pipeline_ref("some-other-pipeline", [PIPE_A]) is None


def test_an_alias_for_a_pipeline_that_is_not_in_this_benchmark_does_not_resolve():
    assert resolve_pipeline_ref(pipeline_alias(PIPE_B), [PIPE_A]) is None


def test_an_exact_id_wins_over_an_alias():
    """
    No current id has the alias shape, but if one ever did, the id it *is*
    must beat the id it hashes to.
    """
    weird = pipeline_alias(PIPE_A)  # a pipeline literally named like an alias
    assert resolve_pipeline_ref(weird, [PIPE_A, weird]) == weird


def test_a_colliding_alias_resolves_to_neither(monkeypatch):
    """
    Two pipelines sharing an alias must produce a "not found" rather than a
    coin flip: a wrong-but-plausible view is not recoverable by the reader,
    and a broken link is.
    """
    monkeypatch.setattr(
        "text2sql_eval_toolkit.ui.aliases.pipeline_alias", lambda _: "0000000000"
    )
    assert resolve_pipeline_ref("0000000000", [PIPE_A, PIPE_B]) is None


def test_alias_map_covers_every_pipeline():
    mapping = alias_map([PIPE_A, PIPE_B])
    assert set(mapping.values()) == {PIPE_A, PIPE_B}


def test_alias_map_of_nothing_is_empty():
    assert alias_map([]) == {}


def test_a_repeated_pipeline_is_not_a_collision():
    """The same id listed twice is one pipeline, not two colliding ones."""
    mapping = alias_map([PIPE_A, PIPE_A])
    assert mapping == {pipeline_alias(PIPE_A): PIPE_A}


# --- the endpoint ---------------------------------------------------------

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from text2sql_eval_toolkit.ui import server  # noqa: E402


@pytest.fixture
def client(tmp_path, monkeypatch):
    results = tmp_path / "results"
    results.mkdir()
    (results / "demo-predictions_eval_summary.json").write_text(
        json.dumps(
            {
                PIPE_A: {"execution_accuracy": {"average": 0.7}},
                PIPE_B: {"execution_accuracy": {"average": 0.6}},
                "llm_judge_config": {"model": {"id": "x"}},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("TEXT2SQL_DATA_ROOT", str(tmp_path))
    server.invalidate_index_cache()
    try:
        yield TestClient(server.app)
    finally:
        server.invalidate_index_cache()


def test_endpoint_returns_both_directions(client):
    body = client.get("/api/benchmarks/demo/pipeline-aliases").json()
    assert body["aliases"][pipeline_alias(PIPE_A)] == PIPE_A
    assert body["by_pipeline"][PIPE_A] == pipeline_alias(PIPE_A)


def test_judge_config_is_not_offered_as_a_pipeline(client):
    """It sits alongside the pipelines in the summary and is not one."""
    body = client.get("/api/benchmarks/demo/pipeline-aliases").json()
    assert "llm_judge_config" not in body["by_pipeline"]
    assert len(body["aliases"]) == 2


def test_endpoint_does_not_need_the_evaluation_artifact(client, tmp_path):
    """
    Resolving a link must not trigger an index build over a multi-GB file --
    the summary alone is enough, and here the artifact is not even present.
    """
    assert not (tmp_path / "results" / "demo-predictions_eval.json").exists()
    assert client.get("/api/benchmarks/demo/pipeline-aliases").status_code == 200


def test_an_unknown_benchmark_is_a_404(client):
    response = client.get("/api/benchmarks/nope/pipeline-aliases")
    assert response.status_code == 404
