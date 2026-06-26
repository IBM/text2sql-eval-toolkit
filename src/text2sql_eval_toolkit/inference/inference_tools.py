#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

import os
import re
from typing import Any
from text2sql_eval_toolkit.logging import get_logger

try:
    import litellm
except ImportError:
    litellm = None


logger = get_logger(__name__)

# Default system instruction shared by all providers.
DEFAULT_SYSTEM_PROMPT = (
    "You are a SQL expert. Your task is to convert natural language questions "
    "into accurate SQL queries using the given database schema and instructions."
)


def _require_litellm() -> None:
    if litellm is None:
        raise ImportError(
            "litellm is not installed. Install with: pip install litellm"
        )


class Text2SQLPrompt:
    """
    Constructs a prompt for SQL generation from a natural language question and a database schema.
    """

    def __init__(
        self, utterance: str, schema: dict[str, Any], db_type: str, evidence: str = None
    ):
        self.utterance = utterance
        self.schema = schema
        self.prompt = (
            # f"You are a SQL expert. Your task is to convert natural language questions into accurate SQL queries using the given {db_type} database schema.\n\n"
            f"Your task is to convert a natural language question into an accurate SQL query using the given {db_type} database schema.\n\n"
            f"**Question:**:\n{self.utterance}\n\n"
            f"**Database Engine / Dialect:**:\n{db_type}\n\n"
            f"**Schema:**\n{self.verbalize_schema(schema)}\n\n"
            "**Instructions:**\n"
            "- Only use columns listed in the schema.\n"
            "- Do not use any other columns or tables not mentioned in the schema.\n"
            "- Ensure the SQL query is valid and executable.\n"
            "- Use proper SQL syntax and conventions.\n"
            "- Generate a complete SQL query that answers the question.\n"
            f"- Use the correct SQL dialect for the database, i.e., {db_type}.\n"
            "- Do not include any explanations or comments in the SQL output.\n"
            "- Your output must start with ```sql and end with ```.\n\n"
        )
        if evidence:
            self.prompt += (
                "***Hints***\n"
                + "\n".join(f"- {hint}" for hint in evidence.split("; "))
                + "\n\n"
            )

        self.prompt += f"Question: {self.utterance}"  # \nSQL:\n```sql\n"

    def verbalize_schema(self, schema: dict[str, Any]) -> str:
        """
        Verbalizes a database schema dictionary into a readable string for LLM prompts,
        including sample values if present.
        """
        lines = []
        db_desc = schema.get("description", "")
        if db_desc:
            lines.append(f"Database description: {db_desc}\n")
        tables = []
        if not isinstance(schema.get("tables"), list) and isinstance(
            schema.get("tables"), dict
        ):
            for table_name, table_obj in schema.get("tables").items():
                tables.append(table_obj)
        else:
            tables = schema.get("tables")
        for table in tables:
            table_name = table.get("name")
            table_desc = table.get("description", "")
            lines.append(f"Table: {table_name}")
            if table_desc:
                lines.append(f"  Description: {table_desc}")
            lines.append("  Columns:")
            for col in table.get("columns"):
                col_name = col.get("name")
                col_type = col.get("type")
                col_desc = col.get("description", "")
                pk = " (Primary Key)" if col.get("primary_key", False) else ""
                # Prepare sample values if present
                samples = col.get("samples")
                if samples is None:
                    samples = col.get("value_samples")
                sample_str = ""
                if samples and isinstance(samples, list):
                    # Show up to 5 sample values
                    shown = samples[:5]
                    shown_str = ", ".join(str(s) for s in shown)
                    sample_str = f" # Example values: {shown_str}"
                elif samples and isinstance(samples, (str, int, float)):
                    sample_str = f" # Example value: {samples}"
                if col_desc:
                    lines.append(
                        f"    - {col_name} ({col_type}){pk}: {col_desc}{sample_str}"
                    )
                else:
                    lines.append(f"    - {col_name} ({col_type}){pk}{sample_str}")
            lines.append("")  # Blank line between tables
        return "\n".join(lines)


def postprocess_sql(text: str) -> str:
    """
    Post-processes the generated SQL text to extract and clean the SQL from markdown-style fenced blocks.
    Handles both properly fenced blocks and malformed ones.
    """
    stripped = text.strip()

    # Case-insensitive match for ```sql fenced block
    fenced_block = re.search(r"(?is)```sql\s*\n?(.*?)(?:\n)?```", stripped)
    if fenced_block:
        text = fenced_block.group(1)
    else:
        # Fallback: generic fenced block without language label
        generic_fenced = re.search(r"(?s)```\s*\n?(.*?)(?:\n)?```", stripped)
        if generic_fenced:
            text = generic_fenced.group(1)
        elif stripped.lower().startswith("```sql"):
            # Malformed SQL code block (unterminated)
            text = re.sub(r"(?is)^```sql\s*", "", stripped).strip("`").strip()
        elif stripped.startswith("```"):
            # Malformed unlabeled fenced block (unterminated)
            text = stripped.lstrip("`").strip()
        else:
            # Plain SQL
            text = stripped

    # Remove leading 'sql' or 'sql\n' (case-insensitive)
    text = re.sub(r"(?i)^\s*sql\s*\n?", "", text)

    # Remove trailing semicolons and whitespace
    return text.rstrip("; \n")

def extract_sql_from_reasoning(reasoning_text: str) -> str:
    """
    Extract SQL from reasoning_content using multiple fallback strategies.
    
    This handles cases where the model outputs reasoning with embedded SQL
    but doesn't provide a separate 'content' field.
    
    Strategies (in order of preference):
    1. Look for ```sql fenced blocks (partial or complete)
    2. Look for SELECT statements after "SQL:" marker
    3. Find the longest complete SELECT statement
    4. Extract any SELECT statement with cleanup
    
    Args:
        reasoning_text: The reasoning content from the model response
        
    Returns:
        Extracted SQL query string, or empty string if no SQL found
    """
    if not reasoning_text:
        return ""
    
    # Strategy 1: Try to find ```sql blocks (even if incomplete/cut off)
    sql_block = re.search(r'```sql\s*\n?(.*?)(?:```|$)', reasoning_text, re.DOTALL | re.IGNORECASE)
    if sql_block:
        sql = sql_block.group(1).strip()
        if sql and sql.upper().startswith('SELECT'):
            return sql.rstrip(';').strip()
    
    # Strategy 2: Look for "SQL:" marker followed by SELECT
    sql_marker = re.search(r'SQL:\s*\n+(SELECT.*?)(?:\n\n|;|\Z)', reasoning_text, re.DOTALL | re.IGNORECASE)
    if sql_marker:
        sql = sql_marker.group(1).strip()
        if sql:
            return sql.rstrip(';').strip()
    
    # Strategy 3: Find the last complete SELECT statement before cutoff
    # Look for SELECT...FROM...WHERE/GROUP/ORDER/LIMIT patterns
    select_statements = re.findall(
        r'(SELECT\s+.*?(?:FROM|JOIN).*?)(?=\n\n|;|\Z)',
        reasoning_text,
        re.DOTALL | re.IGNORECASE
    )
    
    if select_statements:
        # Return the longest one (likely most complete)
        longest_sql = max(select_statements, key=len).strip()
        return longest_sql.rstrip(';').strip()
    
    # Strategy 4: Last resort - find any SELECT statement
    select_match = re.search(r'(SELECT\s+.+)', reasoning_text, re.DOTALL | re.IGNORECASE)
    if select_match:
        sql = select_match.group(1).strip()
        # Clean up common trailing text
        sql = re.sub(r'\n\n.*$', '', sql)  # Remove text after double newline
        sql = re.sub(r'\n(That\'s|This|We need|The).*$', '', sql, flags=re.IGNORECASE)
        return sql.rstrip(';').strip()
    
    return ""


class LiteLLMClient:
    """
    Unified LLM client backed by `litellm`.

    A single implementation replaces the previous per-provider clients
    (watsonx, Gemini, Claude/Anthropic, vLLM, OpenAI/Ollama, RITS). The
    toolkit's ``<provider>:<model>`` naming convention is mapped onto the
    corresponding litellm model string and call arguments, so all providers
    are reached through litellm's OpenAI-compatible interface.

    Supported model name prefixes:
        - ``wxai:``        -> ``watsonx/<model>``
        - ``gemini:``      -> ``gemini/<model>``
        - ``anthropic:``   -> ``anthropic/<model>``
        - ``vllm:``        -> ``hosted_vllm/<model>`` (uses ``VLLM_API_BASE``)
        - ``openai:``      -> ``openai/<model>``     (uses ``OPENAI_BASE_URL``)
        - ``ollama:``      -> OpenAI-compatible (uses ``OLLAMA_BASE_URL``)
        - ``rits...``      -> ``hosted_vllm/<model>`` against the RITS endpoint
    """

    # Number of automatic retries litellm performs on transient errors
    # (e.g. rate limits / 429), with exponential backoff.
    DEFAULT_NUM_RETRIES = 5

    def __init__(self, model_name: str, model_parameters: dict | None = None):
        _require_litellm()
        # Drop provider-unsupported params instead of erroring out.
        litellm.drop_params = True

        self.original_model_name = model_name
        self.model, self.call_kwargs = self._resolve_model(model_name)
        self.model_parameters = self._normalize_parameters(model_parameters or {})

    # ------------------------------------------------------------------
    # Model / parameter resolution
    # ------------------------------------------------------------------
    def _resolve_model(self, model_name: str) -> tuple[str, dict]:
        """
        Map a toolkit ``<provider>:<model>`` name onto a litellm model string
        plus any provider-specific call kwargs (api_base, api_key, headers...).
        """
        call_kwargs: dict[str, Any] = {}

        if model_name.startswith("wxai:"):
            model = "watsonx/" + model_name[len("wxai:"):]
            self._configure_watsonx(call_kwargs)
            return model, call_kwargs

        if model_name.startswith("gemini:"):
            api_key = os.environ.get("GEMINI_API_KEY")
            if not api_key:
                raise ValueError("Missing GEMINI_API_KEY environment variable")
            # Handle quoted env var values in .env files gracefully.
            call_kwargs["api_key"] = api_key.strip().strip('"').strip("'")
            return "gemini/" + model_name[len("gemini:"):], call_kwargs

        if model_name.startswith("anthropic:"):
            api_key = os.environ.get("ANTHROPIC_API_KEY")
            if not api_key:
                raise ValueError("Missing ANTHROPIC_API_KEY environment variable")
            call_kwargs["api_key"] = api_key
            return "anthropic/" + model_name[len("anthropic:"):], call_kwargs

        if model_name.startswith("vllm:"):
            self._configure_vllm(call_kwargs)
            return "hosted_vllm/" + model_name[len("vllm:"):], call_kwargs

        if model_name.startswith("ollama:"):
            self._configure_ollama(call_kwargs)
            return "openai/" + model_name[len("ollama:"):], call_kwargs

        if model_name.startswith("openai:"):
            self._configure_openai(call_kwargs)
            return "openai/" + model_name[len("openai:"):], call_kwargs

        if model_name.startswith("rits"):
            # rits:<provider>/<model> -> derive the RITS endpoint from the model id.
            # Strip the 5-char "rits:" / "rits/" prefix (matches legacy behaviour).
            stripped = model_name[5:]
            logger.info(f"Getting RITS model endpoint for {model_name}")
            model_id = stripped.split("/")[-1].replace(".", "-").lower()
            rits_api_key = os.environ.get("RITS_API_KEY")
            if rits_api_key is None:
                raise ValueError("Missing RITS_API_KEY environment variable")
            call_kwargs["api_base"] = (
                f"https://inference-3scale-apicast-production.apps.rits.fmaas.res.ibm.com/{model_id}/v1"
            )
            call_kwargs["api_key"] = "rits"
            call_kwargs["extra_headers"] = {"RITS_API_KEY": rits_api_key}
            return "hosted_vllm/" + stripped, call_kwargs

        raise NotImplementedError(
            f"Model '{model_name}' is not supported. Supported prefixes: "
            "'wxai:', 'gemini:', 'anthropic:', 'vllm:', 'openai:', 'ollama:', 'rits:'."
        )

    def _configure_watsonx(self, call_kwargs: dict) -> None:
        env_vars = {
            "api_key": "WATSONX_APIKEY",
            "url": "WATSONX_API_BASE",
            "project_id": "WATSONX_PROJECTID",
        }
        values = {k: os.environ.get(v) for k, v in env_vars.items()}
        missing = [env_vars[k] for k, val in values.items() if not val]
        if missing:
            raise ValueError(
                f"Missing WATSONX.AI credentials in environment variables: {', '.join(missing)}"
            )
        # litellm reads these env vars for the watsonx provider; map the
        # toolkit's variable names onto the names litellm expects.
        os.environ.setdefault("WATSONX_URL", values["url"])
        os.environ.setdefault("WATSONX_PROJECT_ID", values["project_id"])
        call_kwargs["api_key"] = values["api_key"]
        call_kwargs["project_id"] = values["project_id"]

    def _configure_vllm(self, call_kwargs: dict) -> None:
        base_url = os.environ.get("VLLM_API_BASE")
        if not base_url:
            raise ValueError("Missing VLLM_API_BASE environment variable")
        call_kwargs["api_base"] = base_url.rstrip("/")
        # vLLM deployments may not require a real key; provide a placeholder.
        call_kwargs["api_key"] = os.environ.get("VLLM_API_KEY") or "vllm"
        rits_api_key = os.environ.get("RITS_API_KEY")
        if rits_api_key:
            call_kwargs["extra_headers"] = {"RITS_API_KEY": rits_api_key}

    def _configure_ollama(self, call_kwargs: dict) -> None:
        base_url = os.environ.get("OLLAMA_BASE_URL") or os.environ.get("OPENAI_BASE_URL")
        if not base_url:
            raise ValueError(
                "Missing OLLAMA_BASE_URL (or OPENAI_BASE_URL) environment variable"
            )
        call_kwargs["api_base"] = base_url.rstrip("/")
        # Ollama doesn't require a real API key.
        call_kwargs["api_key"] = os.environ.get("OLLAMA_API_KEY", "ollama")

    def _configure_openai(self, call_kwargs: dict) -> None:
        base_url = os.environ.get("OPENAI_BASE_URL")
        api_key = os.environ.get("OPENAI_API_KEY")
        if not base_url:
            raise ValueError("Missing OPENAI_BASE_URL environment variable")
        if not api_key:
            raise ValueError("Missing OPENAI_API_KEY environment variable")
        call_kwargs["api_base"] = base_url.rstrip("/")
        call_kwargs["api_key"] = api_key

    def _normalize_parameters(self, model_parameters: dict) -> dict:
        """
        Convert the toolkit's model parameters into litellm/OpenAI-compatible
        keyword arguments. Unknown parameters are passed through (and silently
        dropped by litellm if a provider doesn't support them).
        """
        params = dict(model_parameters)

        # WatsonX legacy "decoding_method" is not a chat parameter; treat
        # greedy decoding as temperature 0.
        decoding_method = params.pop("decoding_method", None)
        if decoding_method == "greedy" and "temperature" not in params:
            params["temperature"] = 0

        # max_new_tokens -> max_tokens
        if "max_new_tokens" in params and "max_tokens" not in params:
            params["max_tokens"] = params.pop("max_new_tokens")

        # stop_sequences -> stop (list)
        if "stop_sequences" in params:
            stop_seqs = params.pop("stop_sequences")
            if stop_seqs:
                params["stop"] = stop_seqs if isinstance(stop_seqs, list) else [stop_seqs]

        # Reasoning controls (mainly Gemini). Map onto litellm's unified params.
        thinking_level = params.pop("thinking_level", None)
        thinking_budget = params.pop("thinking_budget", None)
        if thinking_level is not None and thinking_budget is not None:
            raise ValueError(
                "Cannot set both thinking_level and thinking_budget in the same request"
            )
        if thinking_budget is not None:
            params["thinking"] = {"type": "enabled", "budget_tokens": thinking_budget}
        elif thinking_level is not None:
            params["reasoning_effort"] = thinking_level

        return params

    # ------------------------------------------------------------------
    # Messages
    # ------------------------------------------------------------------
    def _build_messages(self, prompt_text: str) -> list[dict[str, str]]:
        return [
            {"role": "system", "content": DEFAULT_SYSTEM_PROMPT},
            {"role": "user", "content": prompt_text},
        ]

    def _coerce_to_messages(self, prompt: Any) -> list[dict[str, str]]:
        if hasattr(prompt, "prompt"):  # Text2SQLPrompt-like object
            messages = self._build_messages(prompt.prompt)
            logger.debug(f"Inference with constructed chat prompt: {messages}\n")
            return messages
        if isinstance(prompt, list):
            logger.debug(f"Inference with provided chat prompt: {prompt}\n")
            return prompt
        raise ValueError(
            "Incorrect prompt type. Prompt must have a 'prompt' attribute or be a "
            f"list of chat messages: {prompt}"
        )

    # ------------------------------------------------------------------
    # Completion
    # ------------------------------------------------------------------
    def _completion(self, messages: list[dict[str, str]]):
        try:
            return litellm.completion(
                model=self.model,
                messages=messages,
                num_retries=self.DEFAULT_NUM_RETRIES,
                **self.model_parameters,
                **self.call_kwargs,
            )
        except Exception as e:
            logger.error(f"LiteLLM request failed for model '{self.model}': {e}")
            error = ValueError(
                f"Failed to get response from model '{self.original_model_name}': {e}"
            )
            error.response = str(e)
            raise error

    @staticmethod
    def _extract_message(response: Any) -> tuple[str, str]:
        """Return (content, reasoning_content) from a litellm response."""
        try:
            message = response.choices[0].message
        except (AttributeError, IndexError, KeyError) as e:
            raise ValueError(f"No message returned by the model: {e}")
        content = (getattr(message, "content", None) or "").strip()
        reasoning = (getattr(message, "reasoning_content", None) or "").strip()
        return content, reasoning

    @staticmethod
    def _extract_token_usage(response: Any) -> dict | None:
        try:
            usage = getattr(response, "usage", None)
            if not usage:
                return None
            return {
                "prompt_tokens": getattr(usage, "prompt_tokens", 0) or 0,
                "completion_tokens": getattr(usage, "completion_tokens", 0) or 0,
                "total_tokens": getattr(usage, "total_tokens", 0) or 0,
            }
        except Exception as e:
            logger.warning(f"Could not extract token usage: {e}")
            return None

    def chat(self, prompt: Any) -> tuple[str, dict | None]:
        """
        Run a chat completion and return the raw assistant text plus token usage.

        Used by agentic pipelines that parse the model output themselves. When
        the model only returns reasoning (no content), the reasoning text is
        returned so callers can still inspect it.
        """
        messages = self._coerce_to_messages(prompt)
        response = self._completion(messages)
        logger.debug(f"Raw response: {response}\n")

        content, reasoning = self._extract_message(response)
        text = content or reasoning
        if not text:
            error = ValueError("No content returned by the model.")
            error.response = str(response)
            raise error

        return text, self._extract_token_usage(response)

    def generate_sql(self, prompt: Any) -> tuple[str, dict | None]:
        """
        Generate a SQL query from a prompt and return (sql, token_usage).
        """
        messages = self._coerce_to_messages(prompt)
        response = self._completion(messages)
        logger.debug(f"Raw response: {response}\n")

        content, reasoning = self._extract_message(response)

        sql = content
        if not sql and reasoning:
            logger.debug("Attempting to extract SQL from reasoning_content")
            sql = extract_sql_from_reasoning(reasoning)
            if sql:
                logger.info("Successfully extracted SQL from reasoning_content")
            else:
                logger.warning("Could not extract valid SQL from reasoning_content")

        if not sql:
            error = ValueError("No SQL returned by the model.")
            error.response = str(response)
            raise error

        token_usage = self._extract_token_usage(response)

        sql = postprocess_sql(sql)
        logger.debug(f"Generated SQL: {sql}\n")
        return sql, token_usage


def create_llm_client(
    model_name: str, model_parameters: dict | None = None
) -> LiteLLMClient:
    """
    Factory that builds a :class:`LiteLLMClient` from a toolkit model name.

    The ``model_name`` keeps its ``<provider>:<model>`` prefix (e.g.
    ``wxai:ibm/granite-4-h-small``); provider routing is handled internally
    by litellm.
    """
    return LiteLLMClient(model_name, model_parameters)
