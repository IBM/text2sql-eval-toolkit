#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
What the batch LLM judge is shown for one prediction.

Kept apart from ``evaluate_prediction`` so that anything measuring a judge
config -- ``scripts/analysis/judge_calibration.py`` -- sends the judge exactly
the text a real evaluation run does. Two copies of this logic would be a
calibration of the copy.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from text2sql_eval_toolkit.utils import get_question, truncate_dataframe

#: Characters kept of each later message, and of every response, in a trace.
TRACE_MESSAGE_CHARS = 500

#: Characters kept of the task an agent was given, shared by the whole first
#: step: a budget, not a per-message limit, so a step carrying several long
#: messages cannot grow the prompt without bound. Enough for the schema and
#: hints, which is what the judge needs from them: of the agentic predictions in
#: ``data/judge_calibration/``, 99% of whole traces are shorter than this. Not
#: unbounded, because the prompt goes to a model with a context window -- one
#: Beaver trace reaches 160,000 characters, and a judge config on a smaller
#: model would fail on exactly the records whose traces carry the most.
TRACE_TASK_CHARS = 40_000


def _cut(text: str, limit: int = TRACE_MESSAGE_CHARS) -> str:
    """
    *text* cut to *limit*, marked with an ellipsis only if it was cut.

    Marking every message said something was left out of messages that were
    shown whole -- and the judge reads that as evidence that it is not being
    shown everything.
    """
    if len(text) <= limit:
        return text
    return text[:limit] + "..."


def render_agent_trace(trace: List[Optional[Dict[str, Any]]]) -> str:
    """
    An agent's interaction trace as text for the judge.

    The first step's messages are the task the agent was given -- the schema,
    any hints and the question -- and are kept up to `TRACE_TASK_CHARS`, which
    is long enough for all but a handful of traces. They used to be cut to 500
    characters like everything else, which removed the schema and hints from
    every agentic prediction: a judge with no reference query then had nothing
    to check a filter value or a hinted formula against, and accepted wrong
    queries at several times the rate it did for baselines. Each later step
    repeats the conversation so far, so only messages not already shown are
    added, cut like every response to `TRACE_MESSAGE_CHARS`.
    """
    text = "Agent Interaction Trace:\n\n"
    seen = set()
    task_shown = False
    task_budget = TRACE_TASK_CHARS
    for i, interaction in enumerate(trace, 1):
        if interaction is None:
            continue
        text += f"Step {i}: {interaction.get('step') or 'unknown'}\n"
        messages = interaction.get("messages") or []
        for msg in messages:
            role = msg.get("role", "unknown")
            content = str(msg.get("content") or "")
            if (role, content) in seen:
                continue
            seen.add((role, content))
            # Every message keeps at least `TRACE_MESSAGE_CHARS`, so the
            # question -- short, and usually last -- survives a schema that
            # spent the whole budget before it.
            limit = (
                TRACE_MESSAGE_CHARS
                if task_shown
                else max(task_budget, TRACE_MESSAGE_CHARS)
            )
            shown = _cut(content, limit)
            task_budget -= len(shown)
            text += f"  [{role}]: {shown}\n"
        task_shown = task_shown or bool(messages)
        if "response" in interaction:
            text += f"  [response]: {_cut(str(interaction['response'] or ''))}\n"
        text += "\n"
    return text


def build_llm_judge_inputs(
    record: Dict[str, Any],
    prediction: Dict[str, Any],
    gold_sql: str,
    gold_df: Any,
    pred_df: Any,
) -> Dict[str, Any]:
    """
    The keyword arguments `evaluate_sql_prediction_with_llm` is called with.

    Args:
        record: The benchmark record the prediction answers.
        prediction: The prediction being judged.
        gold_sql: The reference query this prediction is compared against.
        gold_df: That query's result, already parsed into a DataFrame.
        pred_df: The prediction's result, already parsed into a DataFrame.

    Returns:
        dict: ``question``, ``ground_truth_sql``, ``ground_truth_df``,
        ``predicted_sql``, ``predicted_df`` and ``generation_prompt``. Both
        result tables are cut to their first and last rows. The context is the
        agent's trace for an agentic pipeline, its reasoning when there is no
        trace, the generation prompt for a baseline, and otherwise a minimal
        description built from the record.
    """
    question = get_question(record)

    if prediction.get("agent_trace"):
        context = render_agent_trace(prediction["agent_trace"])
    elif "agent_reasoning" in prediction:
        context = "Agent Reasoning:\n" + "\n".join(
            f"- {r}" for r in prediction["agent_reasoning"]
        )
    elif "prompt" in prediction:
        context = prediction["prompt"]
    else:
        schema_info = record.get("schema", {})
        db_type = record.get("db_type", "SQL")
        context = (
            f"Question: {question}\n\nDatabase Type: {db_type}\n\n"
            f"Schema: {schema_info}\n\nGenerate SQL to answer the question."
        )

    return {
        "question": question,
        "ground_truth_sql": gold_sql,
        "ground_truth_df": truncate_dataframe(gold_df),
        "predicted_sql": prediction["predicted_sql"],
        "predicted_df": truncate_dataframe(pred_df),
        "generation_prompt": context,
    }
