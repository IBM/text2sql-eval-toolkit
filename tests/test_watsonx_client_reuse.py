#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
watsonx model handles are built once per model, parameters and credentials.

Building a handle requests the project's details and an IAM token, and watsonx
rate-limits both. The batch LLM judge built a new client for every call, and
re-judging the published results at a few calls a second had most calls refused
with "Exceeded limit of calls to endpoint" before any inference happened.
"""

import threading

import pytest

from text2sql_eval_toolkit.inference import inference_tools
from text2sql_eval_toolkit.inference.inference_tools import WXAIClientChatAPI

CREDENTIALS = {"api_key": "key-a", "url": "https://watsonx.example", "project_id": "p1"}


@pytest.fixture
def builds(monkeypatch):
    """Every model handle built, with the credentials it was built from."""
    calls = []

    class FakeModelInference:
        def __init__(self, **kwargs):
            calls.append(kwargs)

    monkeypatch.setattr(inference_tools, "ModelInference", FakeModelInference)
    monkeypatch.setattr(inference_tools, "Credentials", lambda **kwargs: kwargs)
    for field, names in inference_tools.WATSONX_ENV_VARS.items():
        for name in names:
            monkeypatch.delenv(name, raising=False)
        monkeypatch.setenv(names[0], CREDENTIALS[field])
    inference_tools._WATSONX_MODELS.clear()
    yield calls
    inference_tools._WATSONX_MODELS.clear()


def test_clients_with_the_same_configuration_share_one_handle(builds):
    clients = [
        WXAIClientChatAPI("openai/gpt-oss-120b", {"max_new_tokens": 8192})
        for _ in range(5)
    ]
    assert len(builds) == 1
    assert all(client.model is clients[0].model for client in clients)
    # The legacy parameter names are still translated before the build.
    assert builds[0]["params"] == {"max_tokens": 8192}


def test_different_parameters_or_models_get_their_own_handle(builds):
    WXAIClientChatAPI("openai/gpt-oss-120b", {"max_new_tokens": 8192})
    WXAIClientChatAPI("openai/gpt-oss-120b", {"max_new_tokens": 512})
    WXAIClientChatAPI("ibm/granite-4-h-small", {"max_new_tokens": 512})
    assert len(builds) == 3


def test_a_different_key_never_shares_a_handle(builds, monkeypatch):
    first = WXAIClientChatAPI("openai/gpt-oss-120b", {})
    monkeypatch.setenv(inference_tools.WATSONX_ENV_VARS["api_key"][0], "key-b")
    second = WXAIClientChatAPI("openai/gpt-oss-120b", {})
    assert first.model is not second.model
    assert [b["credentials"]["api_key"] for b in builds] == ["key-a", "key-b"]


def test_threads_starting_together_build_one_handle(builds):
    barrier = threading.Barrier(16)

    def build():
        barrier.wait()
        WXAIClientChatAPI("openai/gpt-oss-120b", {"temperature": 0})

    threads = [threading.Thread(target=build) for _ in range(16)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len(builds) == 1


def test_a_failed_build_is_not_cached(builds, monkeypatch):
    attempts = []

    class FlakyModelInference:
        def __init__(self, **kwargs):
            attempts.append(kwargs)
            if len(attempts) == 1:
                raise RuntimeError("429 from the project endpoint")

    monkeypatch.setattr(inference_tools, "ModelInference", FlakyModelInference)
    with pytest.raises(RuntimeError):
        WXAIClientChatAPI("openai/gpt-oss-120b", {})
    WXAIClientChatAPI("openai/gpt-oss-120b", {})
    WXAIClientChatAPI("openai/gpt-oss-120b", {})
    assert len(attempts) == 2


def test_the_cache_is_bounded(builds):
    limit = inference_tools._WATSONX_MODELS_MAX
    for i in range(limit + 5):
        WXAIClientChatAPI("openai/gpt-oss-120b", {"seed": i})
    assert len(inference_tools._WATSONX_MODELS) == limit
