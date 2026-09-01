#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from text2sql_eval_toolkit.inference.inference_tools import (
    Text2SQLPrompt,
    postprocess_sql,
)

# Dummy inputs for constructing the prompt object
DUMMY_UTTERANCE = "What are the names of all employees in the sales department?"
DUMMY_SCHEMA = {
    "description": "Employee database",
    "tables": [
        {
            "name": "employees",
            "description": "Details about employees",
            "columns": [
                {"name": "id", "type": "integer", "primary_key": True},
                {"name": "name", "type": "text"},
                {"name": "department", "type": "text"},
            ],
        }
    ],
}


@pytest.fixture
def prompt():
    return Text2SQLPrompt(DUMMY_UTTERANCE, DUMMY_SCHEMA, db_type="sqlite")


@pytest.mark.parametrize(
    "input_text, expected_sql",
    [
        # Clean fenced block
        ("```sql\nSELECT * FROM employees;```", "SELECT * FROM employees"),
        # Clean fenced block with leading/trailing spaces
        ("  ```sql\nSELECT * FROM employees;\n```  ", "SELECT * FROM employees"),
        # Fenced block with extra line breaks
        ("```sql\n\nSELECT * FROM employees;\n\n```", "SELECT * FROM employees"),
        # Malformed fenced block (no ending)
        ("```sql SELECT * FROM employees;", "SELECT * FROM employees"),
        # Only SQL, no fencing
        ("SELECT * FROM employees;", "SELECT * FROM employees"),
        # No semicolon
        ("```sql\nSELECT * FROM employees\n```", "SELECT * FROM employees"),
        # Double-fenced block
        (
            "Some explanation\n```sql\nSELECT * FROM employees;\n```\nOther text",
            "SELECT * FROM employees",
        ),
        # Block with multiple semicolons
        (
            "```sql\nSELECT id FROM employees; SELECT name FROM employees;\n```",
            "SELECT id FROM employees; SELECT name FROM employees",
        ),
        # Block with leading comment
        (
            "```sql\n-- this is a comment\nSELECT * FROM employees;\n```",
            "-- this is a comment\nSELECT * FROM employees",
        ),
        # Fenced block without newlines
        ("```sql SELECT * FROM employees;```", "SELECT * FROM employees"),
        # Fenced block with mixed casing
        ("```SQL\nSELECT * FROM employees;\n```", "SELECT * FROM employees"),
        # Block with backticks in query (e.g., MySQL-style)
        (
            "```sql\nSELECT `name` FROM `employees`;\n```",
            "SELECT `name` FROM `employees`",
        ),
        # No SQL at all
        ("```sql\n\n```", ""),
        # Completely empty string
        ("", ""),
        # Malformed string
        ("```", ""),
        # Malformed string
        ("`SELECT *`", "`SELECT *`"),
        # code block
        ("```SELECT * FROM TABLE```", "SELECT * FROM TABLE"),
        # code block malformed
        ("```SELECT *", "SELECT *"),
        # Extraneous ticks and symbols after removal
        ("```sql\nSELECT * FROM employees;````", "SELECT * FROM employees"),
        # With trailing spaces and newlines
        ("```sql\nSELECT * FROM employees;  \n\n```", "SELECT * FROM employees"),
        # Leading and trailing markdown noise
        (
            "## SQL Query:\n```sql\nSELECT * FROM employees;\n```\n# End",
            "SELECT * FROM employees",
        ),
    ],
)
def test_postprocess_sql(input_text, expected_sql):
    output = postprocess_sql(input_text)
    assert output == expected_sql, f"\nExpected:\n{expected_sql}\nGot:\n{output}"


class TestOpenAIBaseUrl:
    """
    `openai:` models used to require OPENAI_BASE_URL, so an OpenAI key alone was
    not enough to reach OpenAI and every call raised. The dispatch tests replace
    the client constructor, so nothing exercised this; these build the real one.
    """

    @staticmethod
    def _client(monkeypatch, **env):
        from text2sql_eval_toolkit.inference import inference_tools as it

        pytest.importorskip("openai")
        for name in ("OPENAI_API_KEY", "OPENAI_BASE_URL", "OLLAMA_BASE_URL"):
            monkeypatch.delenv(name, raising=False)
        for name, value in env.items():
            monkeypatch.setenv(name, value)
        return it.OpenAIClientChatAPI("gpt-4o-mini", {})

    def test_an_unset_base_url_reaches_openai(self, monkeypatch):
        from text2sql_eval_toolkit.inference import inference_tools as it

        client = self._client(monkeypatch, OPENAI_API_KEY="k")
        assert client.base_url == it.OPENAI_DEFAULT_BASE_URL

    def test_an_empty_base_url_is_the_same_as_unset(self, monkeypatch):
        # docker-compose passes these through as `${VAR:-}`, so "absent" arrives
        # as the empty string rather than as no variable at all.
        from text2sql_eval_toolkit.inference import inference_tools as it

        client = self._client(monkeypatch, OPENAI_API_KEY="k", OPENAI_BASE_URL="")
        assert client.base_url == it.OPENAI_DEFAULT_BASE_URL

    def test_a_compatible_server_still_overrides_it(self, monkeypatch):
        client = self._client(
            monkeypatch,
            OPENAI_API_KEY="k",
            OPENAI_BASE_URL="https://gateway.internal/v1",
        )
        assert client.base_url == "https://gateway.internal/v1"

    def test_a_trailing_slash_is_trimmed(self, monkeypatch):
        client = self._client(
            monkeypatch,
            OPENAI_API_KEY="k",
            OPENAI_BASE_URL="https://gateway.internal/v1/",
        )
        assert client.base_url == "https://gateway.internal/v1"

    def test_a_missing_key_is_still_an_error(self, monkeypatch):
        # The default endpoint does not make the request free.
        with pytest.raises(ValueError, match="OPENAI_API_KEY"):
            self._client(monkeypatch)
