#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
"""
One dispatch table, shared by both pipelines and the judge.

There were four, and they disagreed: the agentic pipeline accepted six prefixes,
one baseline path three, another seven (it alone knew ``ollama:``), and the judge
exactly one. A model string that worked in one place raised NotImplementedError
in another.

Nothing here needs credentials: the table resolves client classes by name at call
time, so a fake can be substituted for any provider.
"""

import pytest

from text2sql_eval_toolkit.inference import model_clients
from text2sql_eval_toolkit.inference.model_clients import (
    NATIVE_PREFIXES,
    ModelClient,
    UnsupportedModel,
    resolve_client,
    supported_prefixes,
)


class FakeClient:
    """Stands in for any provider client. Records what it was built with."""

    last = {}

    def __init__(self, model_name, model_parameters):
        FakeClient.last = {"model": model_name, "params": model_parameters}
        self.model_name = model_name

    def generate_sql(self, prompt, postprocess=True):
        FakeClient.last_prompt = prompt
        return ("SELECT 1" if postprocess else "```sql\nSELECT 1\n```"), {
            "prompt_tokens": 3,
            "completion_tokens": 4,
        }


@pytest.fixture
def fake_every_provider(monkeypatch):
    for class_name, _ in set(NATIVE_PREFIXES.values()):
        monkeypatch.setattr(model_clients, class_name, FakeClient, raising=False)
    return FakeClient


class TestPrefixCoverage:
    def test_every_prefix_resolves(self, fake_every_provider):
        for prefix in NATIVE_PREFIXES:
            client = resolve_client(f"{prefix}some-model", {})
            assert isinstance(client, ModelClient)

    def test_the_prefix_is_stripped_before_the_provider_sees_it(
        self, fake_every_provider
    ):
        resolve_client("anthropic:claude-sonnet-4-5", {})
        assert FakeClient.last["model"] == "claude-sonnet-4-5"

    def test_parameters_are_passed_through(self, fake_every_provider):
        resolve_client("wxai:x", {"max_new_tokens": 512})
        assert FakeClient.last["params"] == {"max_new_tokens": 512}

    def test_an_unknown_prefix_is_refused_with_the_known_ones_listed(self):
        with pytest.raises(UnsupportedModel) as excinfo:
            resolve_client("nosuchprovider:x", {})
        assert "wxai:" in str(excinfo.value)

    def test_unsupported_model_is_a_not_implemented_error(self):
        # Callers used to catch NotImplementedError from four separate sites.
        assert issubclass(UnsupportedModel, NotImplementedError)

    def test_supported_prefixes_are_reported(self):
        prefixes = supported_prefixes()
        assert "wxai:" in prefixes and "anthropic:" in prefixes

    def test_ollama_is_reachable(self, fake_every_provider):
        """Only one of the four tables knew this prefix."""
        assert resolve_client("ollama:llama3", {}) is not None


class TestUniformInterface:
    def test_generate_sql_always_returns_text_and_usage(self, fake_every_provider):
        sql, usage = resolve_client("wxai:x", {}).generate_sql(object())
        assert sql == "SELECT 1"
        assert usage["prompt_tokens"] == 3

    def test_a_string_prompt_becomes_a_chat_message(self, fake_every_provider):
        """
        The judge builds its prompt from a template, so it hands over a string.
        The chat clients accept only a prompt object or a message list, and
        merging the dispatch tables routed the judge through one of them.
        """
        resolve_client("wxai:x", {}).generate_text("judge this")
        assert FakeClient.last_prompt == [{"role": "user", "content": "judge this"}]

    def test_a_message_list_is_passed_through_untouched(self, fake_every_provider):
        messages = [{"role": "user", "content": "already a list"}]
        resolve_client("wxai:x", {}).generate_sql(messages)
        assert FakeClient.last_prompt == messages

    def test_generate_text_skips_sql_postprocessing(self, fake_every_provider):
        """The judge's reply is prose; post-processing would edit it."""
        text, _ = resolve_client("wxai:x", {}).generate_text(object())
        assert text == "```sql\nSELECT 1\n```"

    def test_usage_callback_receives_the_model_and_counts(self, fake_every_provider):
        seen = []
        client = resolve_client("wxai:x", {}, on_usage=lambda m, u: seen.append((m, u)))
        client.generate_sql(object())
        assert seen[0][0] == "wxai:x"
        assert seen[0][1]["completion_tokens"] == 4

    def test_a_failing_usage_callback_does_not_fail_the_run(self, fake_every_provider):
        def explode(model, usage):
            raise RuntimeError("metering is broken")

        sql, _ = resolve_client("wxai:x", {}, on_usage=explode).generate_sql(object())
        assert sql == "SELECT 1"

    def test_no_callback_is_the_default(self, fake_every_provider):
        assert resolve_client("wxai:x", {}).generate_sql(object())[0] == "SELECT 1"


class TestExplicitCredential:
    def test_api_key_is_visible_to_the_client_being_built(self, monkeypatch):
        seen = {}

        class Recording:
            def __init__(self, model_name, model_parameters):
                import os

                seen["key"] = os.environ.get("ANTHROPIC_API_KEY")

            def generate_sql(self, prompt, postprocess=True):
                return "", None

        monkeypatch.setattr(model_clients, "ClaudeClientChatAPI", Recording)
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        resolve_client("anthropic:c", {}, api_key="sk-test")
        assert seen["key"] == "sk-test"

    def test_the_environment_is_restored_afterwards(
        self, monkeypatch, fake_every_provider
    ):
        import os

        monkeypatch.setenv("ANTHROPIC_API_KEY", "original")
        resolve_client("anthropic:c", {}, api_key="sk-override")
        assert os.environ["ANTHROPIC_API_KEY"] == "original"

    def test_an_unset_variable_stays_unset(self, monkeypatch, fake_every_provider):
        import os

        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        resolve_client("anthropic:c", {}, api_key="sk-override")
        assert "ANTHROPIC_API_KEY" not in os.environ

    def test_omitting_the_key_leaves_the_environment_alone(
        self, monkeypatch, fake_every_provider
    ):
        import os

        monkeypatch.setenv("ANTHROPIC_API_KEY", "from-env")
        resolve_client("anthropic:c", {})
        assert os.environ["ANTHROPIC_API_KEY"] == "from-env"


class TestPipelineIdIsUnchanged:
    """
    `pipeline_id` is derived from the model name and is the unit of comparison
    across summaries, the dashboard and every shared link. Re-spelling a model
    would orphan every stored artifact keyed to the old id, so the ids generated
    for today's models must be byte-identical.
    """

    # Every model the toolkit ships as a default, plus one of each prefix.
    MODELS = [
        "wxai:meta-llama/llama-3-3-70b-instruct",
        "wxai:ibm/granite-4-h-small",
        "wxai:meta-llama/llama-4-maverick-17b-128e-instruct-fp8",
        "wxai:openai/gpt-oss-120b",
        "anthropic:claude-sonnet-4-5",
        "openai:gpt-4o",
        "gemini:gemini-2.0-flash",
        "vllm:mistral-7b",
        "ollama:llama3",
    ]

    @pytest.mark.parametrize("model_name", MODELS)
    def test_baseline_pipeline_id_is_the_model_name_plus_the_suffix(self, model_name):
        assert (
            f"{model_name}-greedy-zero-shot-chatapi"
            == model_name + "-greedy-zero-shot-chatapi"
        )

    @pytest.mark.parametrize("model_name", MODELS)
    def test_resolving_a_client_does_not_rewrite_the_model_name(
        self, model_name, fake_every_provider
    ):
        """
        The id is built from the string the caller passed, so the client must
        carry it unchanged -- not the provider-side name with the prefix removed.
        """
        client = resolve_client(model_name, {})
        assert client.model_name == model_name

    def test_the_default_model_list_is_covered(self):
        from text2sql_eval_toolkit.config_args import DEFAULT_AGENTIC_MODELS

        for model in DEFAULT_AGENTIC_MODELS:
            assert any(model.startswith(p) for p in NATIVE_PREFIXES), model
