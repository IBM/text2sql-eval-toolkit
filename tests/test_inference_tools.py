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
