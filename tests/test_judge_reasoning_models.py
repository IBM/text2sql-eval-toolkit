#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
The LLM judge with a reasoning model, and what the batch judge is shown.

Two properties, both found while moving the judge to gpt-oss-120b:

* **An empty answer is an error, not a verdict.** A reasoning model that spends
  its whole token budget thinking returns no answer text. The watsonx client's
  SQL path recovers a query from the reasoning; the judge's text path used to do
  the same, handing the judge a fragment of thought with no verdict in it, which
  scored N/A -- the same score as a rejection. It must raise instead.
* **The judge's inputs are built in one place**, so a calibration run and a real
  evaluation send the same text.
"""

from types import SimpleNamespace

import pandas as pd
import pytest

from text2sql_eval_toolkit.evaluation.judge_inputs import build_llm_judge_inputs
from text2sql_eval_toolkit.inference.inference_tools import WXAIClientChatAPI
from text2sql_eval_toolkit.inference.model_clients import ModelClient


def _chat_client(message, finish_reason="length"):
    """A watsonx chat client whose model returns *message*, with no network."""
    client = WXAIClientChatAPI.__new__(WXAIClientChatAPI)
    response = {
        "choices": [{"message": message, "finish_reason": finish_reason}],
        "usage": {"prompt_tokens": 10, "completion_tokens": 512, "total_tokens": 522},
    }
    client.model = SimpleNamespace(chat=lambda messages: response)
    return ModelClient(client, "wxai:openai/gpt-oss-120b")


REASONING_ONLY = {
    "role": "assistant",
    "content": "",
    "reasoning_content": "select month and consumption directly from yearmonth rows",
}


def test_text_generation_refuses_an_answer_salvaged_from_reasoning():
    with pytest.raises(ValueError, match="no answer text") as excinfo:
        _chat_client(REASONING_ONLY).generate_text("Is this SQL correct?")
    assert "finish_reason='length'" in str(excinfo.value)
    assert "max_new_tokens" in str(excinfo.value)


def test_sql_generation_still_recovers_a_query_from_reasoning():
    message = {
        "role": "assistant",
        "content": "",
        "reasoning_content": "The query is:\n```sql\nSELECT 1\n```",
    }
    sql, _ = _chat_client(message).generate_sql([{"role": "user", "content": "q"}])
    assert sql.strip().upper().startswith("SELECT 1")


def test_text_generation_returns_the_answer_when_there_is_one():
    message = {"role": "assistant", "content": "No\n\nIt does not aggregate."}
    text, usage = _chat_client(message, "stop").generate_text("Is this SQL correct?")
    assert text.startswith("No")
    assert usage["completion_tokens"] == 512


def test_the_judge_records_an_error_rather_than_scoring_an_empty_reply(monkeypatch):
    """End to end through the judge: an exception, never a silent N/A."""
    from text2sql_eval_toolkit.evaluation import llm_as_judge

    monkeypatch.setattr(
        llm_as_judge, "resolve_client", lambda *a, **k: _chat_client(REASONING_ONLY)
    )
    config = {
        "model": {"id": "wxai:openai/gpt-oss-120b"},
        "prompt_template": "{question}",
    }
    with pytest.raises(ValueError, match="no answer text"):
        llm_as_judge.evaluate_sql_prediction_with_llm(
            "q", "SELECT 1", "", "SELECT 2", "", "", config
        )


# --- what the judge is shown ------------------------------------------------

RECORD = {"question": "How many?", "schema": {"t": ["a"]}, "db_type": "sqlite"}
GOLD = pd.DataFrame({"n": range(30)})
PRED = pd.DataFrame({"n": [1]})


def test_a_baseline_prediction_is_judged_with_its_generation_prompt():
    prediction = {"predicted_sql": "SELECT 1", "prompt": "schema and hints"}
    inputs = build_llm_judge_inputs(RECORD, prediction, "SELECT n FROM t", GOLD, PRED)
    assert inputs["generation_prompt"] == "schema and hints"
    assert inputs["question"] == "How many?"
    assert inputs["predicted_sql"] == "SELECT 1"
    # Long results are cut to their first and last rows.
    assert len(inputs["ground_truth_df"]) == 21
    assert list(inputs["predicted_df"]["n"]) == [1]


def test_an_agentic_prediction_is_judged_with_its_whole_task_and_a_cut_trace():
    """
    The task an agent was given carries the schema and hints, so it is kept
    whole; what follows is cut, and what each step repeats is not shown again.
    """
    schema_and_hints = "CREATE TABLE t (a INT); -- hint: rich means a > 10 " + "s" * 900
    task = [
        {"role": "system", "content": schema_and_hints},
        {"role": "user", "content": "How many?"},
    ]
    prediction = {
        "predicted_sql": "SELECT 1",
        "prompt": "unused when a trace exists",
        "agent_trace": [
            None,
            {"step": "generate", "messages": task, "response": "r" * 900},
            {
                # A later step re-sends the conversation so far.
                "messages": task
                + [
                    {"role": "user", "content": "f" * 900},
                    {"role": "assistant", "content": "short enough to keep"},
                ],
                "response": "y" * 900,
            },
        ],
    }
    context = build_llm_judge_inputs(RECORD, prediction, "SELECT 1", GOLD, PRED)[
        "generation_prompt"
    ]
    assert context.startswith("Agent Interaction Trace:")
    assert "Step 2: generate" in context and "Step 3: unknown" in context
    assert context.count(schema_and_hints) == 1
    assert context.count("How many?") == 1
    for later in ("r", "f", "y"):
        assert later * 500 + "..." in context and later * 501 not in context
    # A message that fits is shown whole, and says nothing about being cut.
    assert "[assistant]: short enough to keep\n" in context
    assert "unused" not in context


def test_the_task_is_kept_whole_but_not_without_limit():
    """
    The schema and hints are what the judge needs from the task, but the prompt
    still goes to a model with a context window: one Beaver trace reaches
    160,000 characters.
    """
    from text2sql_eval_toolkit.evaluation.judge_inputs import TRACE_TASK_CHARS

    huge = "s" * (TRACE_TASK_CHARS + 5000)
    prediction = {
        "predicted_sql": "SELECT 1",
        "agent_trace": [
            {"step": "generate", "messages": [{"role": "system", "content": huge}]}
        ],
    }
    context = build_llm_judge_inputs(RECORD, prediction, "SELECT 1", GOLD, PRED)[
        "generation_prompt"
    ]
    assert "s" * TRACE_TASK_CHARS + "..." in context
    assert "s" * (TRACE_TASK_CHARS + 1) not in context

    # The budget is the whole task's, not each message's: three long messages
    # in one step used to render 120,000 characters against a 40,000 limit.
    crowded = build_llm_judge_inputs(
        RECORD,
        {
            "predicted_sql": "SELECT 1",
            "agent_trace": [
                {
                    "messages": [
                        {"role": "system", "content": "s" * 60000},
                        {"role": "user", "content": "u" * 60000},
                        {"role": "assistant", "content": "a" * 60000},
                    ]
                }
            ],
        },
        "SELECT 1",
        GOLD,
        PRED,
    )["generation_prompt"]
    assert len(crowded) < TRACE_TASK_CHARS + 2000
    # What is left after the budget still says something, rather than nothing.
    assert "[user]: " + "u" * 500 + "..." in crowded
    assert "[assistant]: " + "a" * 500 + "..." in crowded

    fits = "t" * (TRACE_TASK_CHARS - 1)
    whole = build_llm_judge_inputs(
        RECORD,
        {
            "predicted_sql": "SELECT 1",
            "agent_trace": [{"messages": [{"role": "system", "content": fits}]}],
        },
        "SELECT 1",
        GOLD,
        PRED,
    )["generation_prompt"]
    assert f"[system]: {fits}\n" in whole


def test_agent_reasoning_then_a_minimal_description_are_the_fallbacks():
    reasoning = build_llm_judge_inputs(
        RECORD,
        {"predicted_sql": "SELECT 1", "agent_reasoning": ["look", "answer"]},
        "SELECT 1",
        GOLD,
        PRED,
    )["generation_prompt"]
    assert reasoning == "Agent Reasoning:\n- look\n- answer"

    minimal = build_llm_judge_inputs(
        RECORD, {"predicted_sql": "SELECT 1"}, "SELECT 1", GOLD, PRED
    )["generation_prompt"]
    assert "Database Type: sqlite" in minimal and "How many?" in minimal
