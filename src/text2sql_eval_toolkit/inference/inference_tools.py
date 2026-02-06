#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

import os
import re
import requests
from typing import Any
from ibm_watsonx_ai import Credentials
from ibm_watsonx_ai.foundation_models import ModelInference
from text2sql_eval_toolkit.logging import get_logger

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None


logger = get_logger(__name__)


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


class WXAIClient:
    """
    LLM API client using IBM watsonx.ai.
    """

    def __init__(self, model_name: str, model_parameters: dict):
        env_vars = {
            "api_key": "WATSONX_APIKEY",
            "url": "WATSONX_API_BASE",
            "project_id": "WATSONX_PROJECTID",
        }
        values = {k: os.environ.get(v) for k, v in env_vars.items()}
        missing = [env_vars[k] for k, val in values.items() if not val]
        api_key = values["api_key"]
        url = values["url"]
        project_id = values["project_id"]
        if missing:
            raise ValueError(
                f"Missing WATSONX.AI credentials in environment variables: {', '.join(missing)}"
            )

        creds = Credentials(api_key=api_key, url=url)
        self.model = ModelInference(
            model_id=model_name,
            credentials=creds,
            project_id=project_id,
            params=model_parameters,
        )

    def generate_sql(self, prompt: Text2SQLPrompt) -> str:
        logger.debug(f"Inference with prompt: {prompt.prompt}\n\n")
        # response = run_with_timeout(self.model.generate, prompt=prompt.prompt)
        response = self.model.generate(prompt.prompt)
        logger.debug(f"Response: {response}\n\n")
        sql = response.get("results", [{}])[0].get("generated_text", "").strip()
        if not sql:
            raise ValueError("No text generated by the model.")
        sql = prompt.postprocess_sql(sql)
        logger.debug(f"Generated SQL: {sql}\n\n")
        return sql


class WXAIClientChatAPI:
    """
    LLM API client using IBM watsonx.ai Chat API.
    """

    def __init__(self, model_name: str, model_parameters: dict):
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

        creds = Credentials(api_key=values["api_key"], url=values["url"])
        # model_parameters can be a plain dict **or**
        # a TextChatParameters instance – both are accepted.

        # Filter and convert parameters for WatsonX Chat API compatibility
        # WatsonX Chat API uses different parameter names than the legacy API:
        # - max_tokens (not max_new_tokens)
        # - Does not support: decoding_method, stop_sequences (legacy API only)
        filtered_params = dict(model_parameters)

        # Convert max_new_tokens -> max_tokens
        if "max_new_tokens" in filtered_params:
            filtered_params["max_tokens"] = filtered_params.pop("max_new_tokens")

        # Remove unsupported parameters (supported by legacy WatsonX API but not Chat API)
        for unsupported_param in ["decoding_method", "stop_sequences"]:
            filtered_params.pop(unsupported_param, None)

        self.model = ModelInference(
            model_id=model_name,
            credentials=creds,
            project_id=values["project_id"],
            params=filtered_params,
        )

    def _build_messages(self, prompt_text: str) -> list[dict]:
        """
        Convert the flat prompt text into the Chat API message format.
        You can customize the system message here if desired.
        """
        return [
            {
                "role": "system",
                "content": (
                    "You are a SQL expert. Your task is to convert natural language questions into accurate SQL queries using the given database schema and instructions."
                ),
            },
            {"role": "user", "content": prompt_text},
        ]

    def generate_sql(self, prompt: Any) -> tuple[str, dict]:
        if isinstance(prompt, Text2SQLPrompt):
            messages = self._build_messages(prompt.prompt)
            logger.debug(f"Inference with constructed chat prompt: {messages}\n")
            response = self.model.chat(messages=messages)
        elif isinstance(prompt, list):
            logger.debug(f"Inference with provided chat prompt: {prompt}\n")
            response = self.model.chat(messages=prompt)
        else:
            raise ValueError(
                f"Incorrect prompt type. Prompt must of Text2SQLPrompt or a list for chat prompt: {prompt}"
            )

        logger.debug(f"Raw response: {response}\n")

        try:
            message = response["choices"][0]["message"]
            
            # Try content first (normal case)
            sql = message.get("content", "").strip()
            
            # Fall back to reasoning_content if content is empty
            if not sql:
                reasoning = message.get("reasoning_content", "").strip()
                if reasoning:
                    logger.debug("Attempting to extract SQL from reasoning_content")
                    sql = extract_sql_from_reasoning(reasoning)
                    if sql:
                        logger.info("Successfully extracted SQL from reasoning_content")
                    else:
                        logger.warning("Could not extract valid SQL from reasoning_content")
            
            if not sql:
                error = ValueError("No SQL content found in response")
                error.response = str(response)  # Attach raw response to exception
                raise error
                
        except (KeyError, IndexError) as e:
            logger.error(f"SQL generation error: {repr(e)}. Raw response: {response}\n")
            error = ValueError("No SQL returned by the model.")
            error.response = str(response)  # Attach raw response to exception
            raise error

        # Extract token usage from WatsonX response
        token_usage = None
        try:
            # WatsonX returns usage in the response
            usage = response.get("usage", {})
            if usage:
                token_usage = {
                    "prompt_tokens": usage.get("prompt_tokens", 0),
                    "completion_tokens": usage.get("completion_tokens", 0),
                    "total_tokens": usage.get("total_tokens", 0),
                }
                logger.debug(f"Token usage: {token_usage}\n")
        except Exception as e:
            logger.warning(f"Could not extract token usage: {e}")
            token_usage = None

        sql = postprocess_sql(sql)
        logger.debug(f"Generated SQL: {sql}\n")
        return sql, token_usage


class VLLMClientChatAPI:
    """
    LLM API client using vLLM OpenAI-compatible Chat API.
    """

    def __init__(self, model_name: str, model_parameters: dict):
        # Environment variables for vLLM API
        env_vars = {
            "base_url": "VLLM_API_BASE",  # e.g., "http://localhost:8000/v1"
            "api_key": "VLLM_API_KEY",  # Optional, some vLLM deployments don't require this
            "rits_api_key": "RITS_API_KEY",  # Optional, for RITS
        }

        values = {k: os.environ.get(v) for k, v in env_vars.items()}

        # base_url is required, api_key is optional
        if not values["base_url"]:
            raise ValueError("Missing VLLM_API_BASE environment variable")

        self.base_url = values["base_url"].rstrip("/")
        self.api_key = values["api_key"]  # Can be None
        self.rits_api_key = values["rits_api_key"]  # Can be None
        self.model_name = model_name
        self.model_parameters = model_parameters

        # Set up headers
        self.headers = {
            "Content-Type": "application/json",
            "accept": "application/json",
        }
        if self.rits_api_key:
            self.headers["RITS_API_KEY"] = f"{self.rits_api_key}"
        elif self.api_key:
            self.headers["Authorization"] = f"Bearer {self.api_key}"

    def _build_messages(self, prompt_text: str) -> list[dict[str, str]]:
        """
        Convert the flat prompt text into the Chat API message format.
        You can customize the system message here if desired.
        """
        return [
            {
                "role": "system",
                "content": (
                    "You are a SQL expert. Your task is to convert natural language questions into accurate SQL queries using the given database schema and instructions."
                ),
            },
            {"role": "user", "content": prompt_text},
        ]

    def _make_chat_request(self, messages: list[dict[str, str]]) -> dict:
        """
        Make a request to the vLLM chat completions endpoint.
        """
        url = f"{self.base_url}/chat/completions"

        # Prepare the request payload
        payload = {
            "model": self.model_name,
            "messages": messages,
            **self.model_parameters,  # Include temperature, max_tokens, etc.
        }

        try:
            response = requests.post(
                url,
                headers=self.headers,
                json=payload,
                timeout=120,  # 2 minute timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"vLLM API request failed: {e}")
            raise ValueError(f"Failed to get response from vLLM API: {e}")

    def generate_sql(self, prompt: Any) -> tuple[str, dict]:
        if hasattr(prompt, "prompt"):  # Text2SQLPrompt-like object
            messages = self._build_messages(prompt.prompt)
            logger.debug(f"Inference with constructed chat prompt: {messages}\n")
        elif isinstance(prompt, list):
            messages = prompt
            logger.debug(f"Inference with provided chat prompt: {prompt}\n")
        else:
            raise ValueError(
                f"Incorrect prompt type. Prompt must have a 'prompt' attribute or be a list for chat prompt: {prompt}"
            )

        # Make the API request
        response = self._make_chat_request(messages)
        logger.debug(f"Raw response: {response}\n")

        try:
            sql = response["choices"][0]["message"]["content"].strip()
        except (KeyError, IndexError) as e:
            logger.error(f"SQL generation error: {repr(e)}. Raw response: {response}\n")
            raise ValueError("No SQL returned by the model.")

        # Extract token usage from vLLM response (OpenAI-compatible format)
        token_usage = None
        try:
            usage = response.get("usage", {})
            if usage:
                token_usage = {
                    "prompt_tokens": usage.get("prompt_tokens", 0),
                    "completion_tokens": usage.get("completion_tokens", 0),
                    "total_tokens": usage.get("total_tokens", 0),
                }
                logger.debug(f"Token usage: {token_usage}\n")
        except Exception as e:
            logger.warning(f"Could not extract token usage: {e}")
            token_usage = None

        # Apply post-processing
        sql = postprocess_sql(sql)
        logger.debug(f"Generated SQL: {sql}\n")
        return sql, token_usage


class ClaudeClientChatAPI:
    """
    LLM API client using Anthropic's Claude API.
    """

    def __init__(self, model_name: str, model_parameters: dict):
        # Environment variables for Claude API
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise ValueError("Missing ANTHROPIC_API_KEY environment variable")

        self.api_key = api_key
        self.base_url = "https://api.anthropic.com"
        self.model_name = model_name

        # Filter and convert parameters for Claude API compatibility
        # Claude uses: max_tokens, temperature, stop_sequences (as array)
        # Does not support: decoding_method (WatsonX-specific)
        self.model_parameters = dict(model_parameters)

        # Convert max_new_tokens -> max_tokens
        if "max_new_tokens" in self.model_parameters:
            self.model_parameters["max_tokens"] = self.model_parameters.pop(
                "max_new_tokens"
            )

        # Remove unsupported parameters
        self.model_parameters.pop("decoding_method", None)

        # Set up headers for Claude API
        self.headers = {
            "Content-Type": "application/json",
            "x-api-key": self.api_key,
            "anthropic-version": "2023-06-01",  # API version
        }

    def _build_messages(self, prompt_text: str) -> list[dict[str, str]]:
        """
        Convert the flat prompt text into Claude's message format.
        Claude uses a slightly different format than OpenAI.
        """
        return [{"role": "user", "content": prompt_text}]

    def _build_system_message(self) -> str:
        """
        Claude handles system messages separately from the messages array.
        """
        return (
            "You are a SQL expert. Your task is to convert natural language questions "
            "into accurate SQL queries using the given database schema and instructions."
        )

    def _make_chat_request(self, messages: list[dict[str, str]]) -> dict:
        """
        Make a request to Claude's messages endpoint.
        """
        url = f"{self.base_url}/v1/messages"

        # Prepare the request payload for Claude
        payload = {
            "model": self.model_name,
            "messages": messages,
            "system": self._build_system_message(),
            **self.model_parameters,
        }

        print(f"\n\n\n ******** \n payload:{payload} \n\n\n\n")

        try:
            response = requests.post(
                url, headers=self.headers, json=payload, timeout=120
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            # Try to extract error details from response
            error_detail = ""
            try:
                error_json = e.response.json()
                if "error" in error_json:
                    error_type = error_json["error"].get("type", "unknown")
                    error_msg = error_json["error"].get("message", "")
                    error_detail = f" - {error_type}: {error_msg}"
            except:
                pass
            
            # Provide specific guidance for common errors
            if e.response.status_code == 401:
                logger.error(f"Claude API authentication failed{error_detail}")
                raise ValueError(
                    f"Claude API authentication failed{error_detail}\n"
                    "Please check that your ANTHROPIC_API_KEY is valid.\n"
                    "Get a valid key at: https://console.anthropic.com/settings/keys"
                )
            elif e.response.status_code == 429:
                logger.error(f"Claude API rate limit exceeded{error_detail}")
                raise ValueError(f"Claude API rate limit exceeded{error_detail}")
            else:
                logger.error(f"Claude API request failed: {e}{error_detail}")
                raise ValueError(f"Failed to get response from Claude API: {e}{error_detail}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Claude API request failed: {e}")
            raise ValueError(f"Failed to get response from Claude API: {e}")

    def generate_sql(self, prompt: Any) -> tuple[str, dict]:
        if hasattr(prompt, "prompt"):  # Text2SQLPrompt-like object
            messages = self._build_messages(prompt.prompt)
            logger.debug(f"Inference with constructed chat prompt: {messages}\n")
        elif isinstance(prompt, list):
            # If already formatted messages, use as-is but ensure no system messages in array
            messages = [msg for msg in prompt if msg.get("role") != "system"]
            logger.debug(f"Inference with provided chat prompt: {messages}\n")
        else:
            raise ValueError(
                f"Incorrect prompt type. Prompt must have a 'prompt' attribute or be a list for chat prompt: {prompt}"
            )

        # Make the API request
        response = self._make_chat_request(messages)
        logger.debug(f"Raw response: {response}\n")

        try:
            sql = response["content"][0]["text"].strip()
        except (KeyError, IndexError) as e:
            logger.error(f"SQL generation error: {repr(e)}. Raw response: {response}\n")
            raise ValueError("No SQL returned by the model.")

        # Extract token usage from Claude response
        token_usage = None
        try:
            usage = response.get("usage", {})
            if usage:
                # Claude returns input_tokens and output_tokens
                prompt_tokens = usage.get("input_tokens", 0)
                completion_tokens = usage.get("output_tokens", 0)
                token_usage = {
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": completion_tokens,
                    "total_tokens": prompt_tokens + completion_tokens,
                }
                logger.debug(f"Token usage: {token_usage}\n")
        except Exception as e:
            logger.warning(f"Could not extract token usage: {e}")
            token_usage = None

        # Apply post-processing
        sql = postprocess_sql(sql)
        logger.debug(f"Generated SQL: {sql}\n")
        return sql, token_usage


class OpenAIClientChatAPI:
    """
    LLM API client using OpenAI-compatible API (e.g., LiteLLM proxy).
    """

    def __init__(self, model_name: str, model_parameters: dict):
        if OpenAI is None:
            raise ImportError(
                "openai package is required for OpenAI client. Install it with: pip install openai"
            )

        # Check if this is an Ollama model (will be passed without prefix after stripping in baseline_llm_pipeline)
        # For Ollama, try OLLAMA_* env vars first, fall back to OPENAI_* for compatibility
        ollama_base_url = os.environ.get("OLLAMA_BASE_URL")
        ollama_api_key = os.environ.get("OLLAMA_API_KEY", "ollama")  # Ollama doesn't require real API key
        
        if ollama_base_url:
            # Using Ollama
            self.base_url = ollama_base_url.rstrip("/")
            self.api_key = ollama_api_key
        else:
            # Using OpenAI or OpenAI-compatible API
            env_vars = {
                "base_url": "OPENAI_BASE_URL",
                "api_key": "OPENAI_API_KEY",
            }

            values = {k: os.environ.get(v) for k, v in env_vars.items()}

            # base_url and api_key are required
            if not values["base_url"]:
                raise ValueError("Missing OPENAI_BASE_URL environment variable")
            if not values["api_key"]:
                raise ValueError("Missing OPENAI_API_KEY environment variable")

            self.base_url = values["base_url"].rstrip("/")
            self.api_key = values["api_key"]
        
        self.model_name = model_name

        # Filter and convert parameters for OpenAI API compatibility
        # OpenAI uses: max_tokens, temperature, stop (as array or string)
        # Does not support: decoding_method (WatsonX-specific)
        self.model_parameters = dict(model_parameters)

        # Convert max_new_tokens -> max_tokens
        if "max_new_tokens" in self.model_parameters:
            self.model_parameters["max_tokens"] = self.model_parameters.pop(
                "max_new_tokens"
            )

        # Convert stop_sequences -> stop (OpenAI expects stop as a list or string)
        if "stop_sequences" in self.model_parameters:
            stop_seqs = self.model_parameters.pop("stop_sequences")
            if stop_seqs:
                # OpenAI accepts stop as a list or a single string
                if isinstance(stop_seqs, list):
                    self.model_parameters["stop"] = stop_seqs
                else:
                    self.model_parameters["stop"] = [stop_seqs]

        # Remove unsupported parameters
        self.model_parameters.pop("decoding_method", None)

        # Initialize OpenAI client
        self.client = OpenAI(
            api_key=self.api_key,
            base_url=self.base_url,
        )

    def _build_messages(self, prompt_text: str) -> list[dict[str, str]]:
        """
        Convert the flat prompt text into OpenAI Chat API message format.
        """
        return [
            {
                "role": "system",
                "content": (
                    "You are a SQL expert. Your task is to convert natural language questions "
                    "into accurate SQL queries using the given database schema and instructions."
                ),
            },
            {"role": "user", "content": prompt_text},
        ]

    def generate_sql(self, prompt: Any) -> tuple[str, dict]:
        if hasattr(prompt, "prompt"):  # Text2SQLPrompt-like object
            messages = self._build_messages(prompt.prompt)
            logger.debug(f"Inference with constructed chat prompt: {messages}\n")
        elif isinstance(prompt, list):
            messages = prompt
            logger.debug(f"Inference with provided chat prompt: {prompt}\n")
        else:
            raise ValueError(
                f"Incorrect prompt type. Prompt must have a 'prompt' attribute or be a list for chat prompt: {prompt}"
            )

        # Make the API request using OpenAI client
        try:
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=messages,
                **self.model_parameters,
            )
            logger.debug(f"Raw response: {response}\n")
        except Exception as e:
            logger.error(f"OpenAI API request failed: {e}")
            raise ValueError(f"Failed to get response from OpenAI API: {e}")

        try:
            sql = response.choices[0].message.content.strip()
        except (AttributeError, IndexError, KeyError) as e:
            logger.error(f"SQL generation error: {repr(e)}. Raw response: {response}\n")
            raise ValueError("No SQL returned by the model.")

        # Extract token usage from OpenAI response
        token_usage = None
        try:
            if hasattr(response, "usage") and response.usage:
                usage = response.usage
                token_usage = {
                    "prompt_tokens": usage.prompt_tokens if hasattr(usage, "prompt_tokens") else 0,
                    "completion_tokens": usage.completion_tokens if hasattr(usage, "completion_tokens") else 0,
                    "total_tokens": usage.total_tokens if hasattr(usage, "total_tokens") else 0,
                }
                logger.debug(f"Token usage: {token_usage}\n")
        except Exception as e:
            logger.warning(f"Could not extract token usage: {e}")
            token_usage = None

        # Apply post-processing
        sql = postprocess_sql(sql)
        logger.debug(f"Generated SQL: {sql}\n")
        return sql, token_usage
