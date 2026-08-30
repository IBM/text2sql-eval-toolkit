#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
"""
One place that turns a ``provider:model`` string into a client.

There used to be three, and they disagreed. The agentic pipeline accepted six
prefixes, the baseline pipeline three, and the judge exactly one -- so
``anthropic:claude-...`` ran an agentic pipeline and raised ``NotImplementedError``
on the baseline, and no judge config could name anything but watsonx. The
inconsistency, not a missing provider, was the defect.

The prefixes and their meaning are unchanged. ``pipeline_id`` is derived from the
model name and is the unit of comparison across summaries, the dashboard and
every shared link, so re-spelling a model would orphan stored artifacts; there is
a test asserting the ids generated for today's models are byte-identical.

``api_key`` and ``on_usage`` exist from the start rather than being retrofitted.
They are what lets the dashboard meter a per-user key without putting quota logic
in the library: omitted -- which is every library, CLI and notebook call -- the
client reads the environment exactly as it always has.
"""

from __future__ import annotations

import os
from typing import Any, Callable, Dict, Mapping, Optional, Tuple, Union

from text2sql_eval_toolkit.inference.inference_tools import (  # noqa: F401 — resolved by name from NATIVE_PREFIXES
    ClaudeClientChatAPI,
    GeminiClientChatAPI,
    OpenAIClientChatAPI,
    VLLMClientChatAPI,
    WXAIClientChatAPI,
    postprocess_sql,
)
from text2sql_eval_toolkit.logging import get_logger

logger = get_logger(__name__)

#: Prefix -> (client class name, characters to strip). Every call site reads this
#: one table, so a prefix works everywhere or nowhere.
#:
#: The class is named rather than referenced so it is looked up when a client is
#: built, not when this module is imported. That keeps the table independent of
#: import order and lets a caller substitute a client -- which is how the tests
#: exercise every provider without credentials.
NATIVE_PREFIXES: Dict[str, Tuple[str, int]] = {
    "wxai:": ("WXAIClientChatAPI", 5),
    "gemini:": ("GeminiClientChatAPI", 7),
    "anthropic:": ("ClaudeClientChatAPI", 10),
    "openai:": ("OpenAIClientChatAPI", 7),
    "vllm:": ("VLLMClientChatAPI", 5),
    # OpenAI-compatible endpoint; OLLAMA_BASE_URL points the client at it.
    "ollama:": ("OpenAIClientChatAPI", 7),
}

#: Environment variable holding each provider's credential, for the api_key
#: override. The clients read these themselves when nothing is passed.
PROVIDER_ENV_VARS: Dict[str, str] = {
    "wxai:": "WATSONX_APIKEY",
    "gemini:": "GEMINI_API_KEY",
    "anthropic:": "ANTHROPIC_API_KEY",
    "openai:": "OPENAI_API_KEY",
}


#: A caller-supplied credential. Usually one key for one environment variable,
#: but watsonx needs a project id alongside the key, so a mapping already keyed
#: by variable name is equally valid -- which is what a stored watsonx
#: credential looks like. Annotating this as ``str`` was wrong in both the
#: judge and the dashboard's key store, which pass the mapping form.
Credential = Union[str, Mapping[str, str]]


class UnsupportedModel(NotImplementedError):
    """Raised when no prefix matches and LiteLLM cannot be used either."""


def supported_prefixes() -> list:
    """
    Prefixes this installation can reach, sorted.

    Returns:
        list[str]: Every natively supported prefix. ``litellm:`` is appended when
        the optional dependency is installed, which widens this to anything
        LiteLLM routes.
    """
    prefixes = sorted(NATIVE_PREFIXES)
    if litellm_available():
        prefixes.append("litellm:")
    return prefixes


def litellm_available() -> bool:
    """Whether the optional LiteLLM extra is installed."""
    try:
        import litellm  # noqa: F401
    except Exception:
        return False
    return True


class ModelClient:
    """
    A uniform wrapper over the provider clients.

    The wrapped clients do not agree on their own: five return
    ``(sql, token_usage)`` from ``generate_sql`` while the legacy watsonx client
    returns a bare string, which is a trap for anything calling more than one.
    This normalises that, and adds `generate_text` for callers that want
    what the model said rather than SQL extracted from it -- the judge, whose
    verdict would otherwise be run through SQL post-processing.
    """

    def __init__(
        self, inner: Any, model_name: str, on_usage: Optional[Callable] = None
    ):
        self._inner = inner
        self.model_name = model_name
        self._on_usage = on_usage

    @property
    def inner(self) -> Any:
        """The wrapped provider client, for code that needs its specifics."""
        return self._inner

    def _report(self, usage: Optional[dict]) -> None:
        if self._on_usage is not None and usage:
            try:
                self._on_usage(self.model_name, usage)
            except Exception:  # pragma: no cover - metering must not break a run
                logger.warning("usage callback raised; continuing", exc_info=True)

    def _call(self, prompt: Any, postprocess: bool) -> Tuple[str, Optional[dict]]:
        # A bare string is a legitimate prompt -- it is what the judge builds
        # from its template -- but the chat clients accept only a prompt object
        # or a message list. Before the dispatch tables were merged the judge
        # reached watsonx's raw generate() and a string worked; afterwards it
        # went through a chat client and failed with "Incorrect prompt type",
        # which describes the caller rather than the mismatch.
        if isinstance(prompt, str):
            prompt = [{"role": "user", "content": prompt}]
        result = self._inner.generate_sql(prompt, postprocess=postprocess)
        if isinstance(result, tuple):
            text, usage = result
        else:
            text, usage = result, None
        self._report(usage)
        return text, usage

    def generate_sql(self, prompt: Any) -> Tuple[str, Optional[dict]]:
        """
        Generate SQL for *prompt*.

        Args:
            prompt: A ``Text2SQLPrompt``-like object, or a list of chat messages.

        Returns:
            tuple[str, dict | None]: The statement, and token usage when the
            provider reported any.
        """
        return self._call(prompt, postprocess=True)

    def generate_text(self, prompt: Any) -> Tuple[str, Optional[dict]]:
        """
        Generate text for *prompt*, without SQL post-processing.

        The judge needs this: its reply is a verdict and an explanation, and
        running that through `postprocess_sql` would strip fenced blocks
        and trailing punctuation from prose.

        Returns:
            tuple[str, dict | None]: The reply, and token usage when reported.
        """
        return self._call(prompt, postprocess=False)


class _LiteLLMClient:
    """Adapter for anything LiteLLM routes. Only constructed when it is installed."""

    def __init__(
        self, model: str, model_parameters: Optional[dict], api_key: Optional[str]
    ):
        self._model = model
        self._params = dict(model_parameters or {})
        self._api_key = api_key

    def generate_sql(self, prompt: Any, postprocess: bool = True):
        import litellm

        text = getattr(prompt, "prompt", prompt)
        messages = (
            text if isinstance(text, list) else [{"role": "user", "content": str(text)}]
        )
        kwargs = dict(self._params)
        if self._api_key:
            kwargs["api_key"] = self._api_key
        response = litellm.completion(model=self._model, messages=messages, **kwargs)
        content = response["choices"][0]["message"]["content"] or ""
        usage = None
        raw_usage = response.get("usage") if hasattr(response, "get") else None
        if raw_usage:
            usage = {
                "prompt_tokens": raw_usage.get("prompt_tokens", 0),
                "completion_tokens": raw_usage.get("completion_tokens", 0),
                "total_tokens": raw_usage.get("total_tokens", 0),
            }
        return (postprocess_sql(content) if postprocess else content), usage


def _credential_values(
    prefix: str, api_key: Optional[Credential]
) -> Optional[Dict[str, str]]:
    """
    Normalise *api_key* into environment variables to set while building a client.

    Accepts either a bare string -- the common case, one key for one variable --
    or a mapping already keyed by variable name, which is what a stored watsonx
    credential looks like once its project id is included.
    """
    if not api_key:
        return None
    if isinstance(api_key, dict):
        return api_key
    env_var = PROVIDER_ENV_VARS.get(prefix)
    return {env_var: api_key} if env_var else None


def resolve_client(
    model_name: str,
    model_parameters: Optional[dict] = None,
    *,
    api_key: Optional[Credential] = None,
    on_usage: Optional[Callable] = None,
) -> ModelClient:
    """
    Build a client for a ``provider:model`` string.

    Args:
        model_name: For example ``wxai:meta-llama/llama-3-3-70b-instruct`` or
            ``anthropic:claude-sonnet-4-5``. An unrecognised prefix is routed
            through LiteLLM when that extra is installed.
        model_parameters: Generation parameters, passed to the provider.
        api_key: Use this credential instead of the provider's environment
            variable. Either the key itself, or a mapping of variable names to
            values for a provider that needs more than one -- watsonx needs a
            project id alongside its key. Omit -- as every library, CLI and notebook call does -- and
            the client reads the environment exactly as before.
        on_usage: Called as ``on_usage(model_name, usage)`` after each request
            that reports token counts. The library never interprets this; it is
            how the dashboard meters spend without the library knowing about
            quotas. Exceptions raised here are logged and swallowed, because
            metering must not fail a run.

    Returns:
        ModelClient: A client with a uniform interface.

    Raises:
        UnsupportedModel: If no prefix matches and LiteLLM is unavailable.

    Example:
        ```python
        >>> client = resolve_client("wxai:ibm/granite-4-h-small", {"max_new_tokens": 512})
        >>> sql, usage = client.generate_sql(prompt)
        ```
    """
    params = dict(model_parameters or {})

    for prefix, (class_name, strip) in NATIVE_PREFIXES.items():
        if model_name.startswith(prefix):
            client_cls = globals()[class_name]
            with _temporary_credential(_credential_values(prefix, api_key)):
                inner = client_cls(model_name[strip:], params)
            return ModelClient(inner, model_name, on_usage)

    if model_name.startswith("rits"):
        # No colon, and the endpoint is derived from the model id rather than
        # configured. Two of the four dispatch tables implemented this
        # identically and the other two did not support it at all.
        model_id = model_name.split("/")[-1].replace(".", "-").lower()
        rits_api_key = api_key or os.environ.get("RITS_API_KEY")
        if rits_api_key is None:
            raise ValueError("Missing RITS_API_KEY environment variable")
        os.environ["VLLM_API_BASE"] = (
            "https://inference-3scale-apicast-production.apps.rits.fmaas.res.ibm.com"
            f"/{model_id}/v1"
        )
        inner = globals()["VLLMClientChatAPI"](model_name[5:], params)
        return ModelClient(inner, model_name, on_usage)

    if litellm_available():
        target = (
            model_name.split(":", 1)[1]
            if model_name.startswith("litellm:")
            else model_name
        )
        return ModelClient(
            _LiteLLMClient(target, params, api_key), model_name, on_usage
        )

    raise UnsupportedModel(
        f"Model '{model_name}' is not supported. Known prefixes: "
        f"{', '.join(sorted(NATIVE_PREFIXES))}. "
        "Install the 'litellm' extra to reach other providers."
    )


class _temporary_credential:
    """
    Set a provider's environment variables for the duration of construction.

    The clients read their credentials from ``os.environ`` in ``__init__``, so
    explicit ones have nowhere else to go until those constructors take them.
    This is deliberately scoped to the constructor call and restored
    immediately: ``os.environ`` is process-global, and leaving it set would let
    one caller's key serve another's request.

    A mapping rather than a single value, because a credential is not always one
    thing: watsonx needs a project id alongside its key, and setting only half of
    it produces a client that fails for a reason unrelated to what went wrong.
    """

    def __init__(self, values: Optional[Dict[str, str]]):
        self._values = {k: v for k, v in (values or {}).items() if k and v}
        self._previous: Dict[str, Optional[str]] = {}

    def __enter__(self):
        for var, value in self._values.items():
            self._previous[var] = os.environ.get(var)
            os.environ[var] = value
        return self

    def __exit__(self, *exc):
        for var, before in self._previous.items():
            if before is None:
                os.environ.pop(var, None)
            else:
                os.environ[var] = before
        return False
