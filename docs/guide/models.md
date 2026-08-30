# Models and providers

## Naming a model

Model names are `provider:model`, and the prefix chooses the client:

| Prefix | Goes to | Credentials |
|---|---|---|
| `wxai:` | IBM watsonx.ai | `WATSONX_APIKEY`, `WATSONX_API_BASE`, `WATSONX_PROJECTID` |
| `anthropic:` | Anthropic | `ANTHROPIC_API_KEY`, optionally `ANTHROPIC_WORKSPACE_ID` |
| `openai:` | OpenAI | `OPENAI_API_KEY` |
| `gemini:` | Google Gemini | `GEMINI_API_KEY` |
| `vllm:` | A local or remote vLLM server | `VLLM_API_BASE` |
| `ollama:` | Ollama, via its OpenAI-compatible endpoint | `OLLAMA_BASE_URL` |
| `litellm:` | Anything [LiteLLM](https://docs.litellm.ai/) routes | per LiteLLM |

So `anthropic:claude-sonnet-4-5` and `wxai:openai/gpt-oss-120b` are both valid,
and both work in **every** call site — baseline inference, the agentic pipeline
and the LLM judge alike.

!!! note "This was not always true"
    Before 1.4.0 each of the three call sites dispatched on the prefix
    separately and supported a different set: the judge accepted `wxai:` only,
    and `anthropic:` worked agentically but raised `NotImplementedError` on the
    baseline. They now share one dispatch table, so support is a property of the
    toolkit rather than of where you happened to call it from.

`litellm:` requires the optional extra:

```bash
pip install "text2sql-eval-toolkit[litellm]"
```

## Credentials

Credentials come from the environment. `env_loader.load_env()` runs on import and
searches upward from the working directory, then the checkout root, then
`~/.env`. Existing environment variables are never overridden — what is already
set wins.

`env.example` lists what each provider needs. Copy it to `.env` and fill in what
you use; nothing is required except for the providers you actually name.

### watsonx spelling

All three watsonx variables accept two spellings, because the toolkit's own are
the unusual ones:

| Accepted | Also accepted |
|---|---|
| `WATSONX_APIKEY` | `WATSONX_API_KEY` |
| `WATSONX_API_BASE` | `WATSONX_URL` |
| `WATSONX_PROJECTID` | `WATSONX_PROJECT_ID` |

Setting either spelling works. This is not tidiness — IBM's own documentation
mostly writes `WATSONX_PROJECT_ID`, and an error naming only `WATSONX_PROJECTID`
to someone who has already set the underscored one is a genuinely expensive
five-minute bug.

## Per-user keys in the dashboard

A signed-in dashboard user can store their own provider credentials, encrypted,
so their judge runs bill their account rather than the server's. That is a
dashboard feature and has no effect on the library: a library call with no
credential argument reads the environment exactly as it always has.
