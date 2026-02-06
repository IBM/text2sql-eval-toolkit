#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

import json
import pandas as pd
from pathlib import Path
from text2sql_eval_toolkit.utils import parse_dataframe
from text2sql_eval_toolkit.logging import get_logger

logger = get_logger(__name__)


def get_pipeline_ids(records):
    if len(records) < 1 or "predictions" not in records[0]:
        return None
    return list(records[0]["predictions"].keys())


def get_failed_records(records, pipeline_id, metric="execution_accuracy"):
    failed_records = []
    for r in records:
        if pipeline_id not in r["predictions"]:
            failed_records.append(f"No predictions for {pipeline_id}")
        elif r["predictions"][pipeline_id]["evaluation"].get(metric) == 0:
            failed_records.append(r)
    return failed_records


def safe_snippet(text, head=4000, tail=4000):
    if len(text) <= head + tail:
        return text
    return text[:head] + "\n…\n" + text[-tail:]


def safe_code_block(text, max_length=10000):
    """
    Safely display text in a code block, handling nested backticks.
    Uses HTML pre tags to avoid markdown parsing issues.
    """
    import html

    # Truncate if too long
    if len(text) > max_length:
        text = text[:max_length] + "\n...(truncated)"
    # Escape HTML and wrap in pre tags
    escaped = html.escape(text)
    return f"<pre>{escaped}</pre>"


def head_tail_with_ellipsis(df: pd.DataFrame, k: int = 20) -> pd.DataFrame:
    """
    Returns the top k and bottom k rows of a DataFrame with ellipsis rows in between.

    Parameters:
        df (pd.DataFrame): The input DataFrame.
        k (int): Number of rows to show from the top and bottom. Default is 20.

    Returns:
        pd.DataFrame: A new DataFrame with top k rows, ellipsis, and bottom k rows.
    """
    if len(df) <= 2 * k:
        return df.copy()

    top = df.head(k)
    bottom = df.tail(k)

    # Create ellipsis rows with same columns
    ellipsis_rows = pd.DataFrame(
        [
            ["..."] * df.shape[1],
            ["... (truncated)"] * df.shape[1],
            ["..."] * df.shape[1],
        ],
        columns=df.columns,
    )

    return pd.concat([top, ellipsis_rows, bottom], ignore_index=True)


def chat_prompt_to_html(prompt):
    import html as html_module

    html_output = """
## Full Chat Prompt Conversation

<div style="max-height: 400px; overflow-y: auto; border: 1px solid #ccc; padding: 10px; background-color: #f9f9f9; font-family: monospace;">
"""

    for i, entry in enumerate(prompt):
        role = entry.get("role", "unknown").capitalize()
        content = entry.get("content", "").strip()

        # Escape HTML to safely display any content (handles backticks, quotes, etc.)
        escaped_content = html_module.escape(content)

        # Add message with visual separator
        html_output += f"<div style='margin-bottom: 15px;'>\n"
        html_output += f"<strong>{role} Message {i + 1}:</strong>\n"
        html_output += f"<pre style='margin: 5px 0; padding: 10px; background-color: #ffffff; border-left: 3px solid #007acc; white-space: pre-wrap; word-wrap: break-word;'>{escaped_content}</pre>\n"
        html_output += f"</div>\n"

    html_output += "</div>\n"
    return html_output


def format_inference_error_example(record, pred, example_index, total_failed):
    """Format a failed example where inference itself failed (no SQL generated)."""
    question_id = record.get("id", record.get("_id", f"example_{example_index}"))
    utterance = (
        record.get("page_content")
        or record.get("question")
        or record.get("utterance", "")
    )
    
    md = []
    md.append(
        f"### ⚠️  Inference Failed - Question #{example_index} (of {total_failed} examples) - Question ID: `{question_id}`\n"
    )
    md.append(f"**Question**: {utterance}\n")
    
    md.append("### ❌ Inference Error")
    md.append(f"```\n{pred.get('inference_error', 'Unknown error')}\n```")
    
    # Show raw response if available
    if pred.get('raw_response'):
        md.append("### 📄 Raw Model Response")
        md.append(f"```\n{pred['raw_response']}\n```")
    
    # Show prompt that was used
    if pred.get('prompt'):
        md.append("### 📝 Prompt Used")
        md.append(f"```\n{safe_snippet(pred['prompt'], head=500, tail=500)}\n```")
    
    md.append("---\n")
    return "\n".join(md)


def format_failed_example(record, pipeline_id, example_index, total_failed):
    try:
        pred = record["predictions"][pipeline_id]
    except Exception as e:
        logger.error(f"Error reading prediction in record: {record}")
        return f"⚠️  Error reading prediction in record: {record}\n\n"
    
    # Check for inference error
    if "inference_error" in pred:
        return format_inference_error_example(record, pred, example_index, total_failed)
    gt_sqls = (
        record.get("sql")
        or record.get("SQL")
        or record.get("metadata", {}).get("sql", [])
    )
    gt_sqls = [gt_sqls] if isinstance(gt_sqls, str) else gt_sqls
    question_id = record.get("id", record.get("_id", f"example_{example_index}"))
    utterance = (
        record.get("page_content")
        or record.get("question")
        or record.get("utterance", "")
    )

    # Ground truth DFs
    gt_dfs = []
    raw_gt_dfs = record.get("gt_df", [])
    if isinstance(raw_gt_dfs, str):
        gt_dfs = [parse_dataframe(raw_gt_dfs)]
    else:
        for df in raw_gt_dfs:
            try:
                gt_dfs.append(parse_dataframe(df))
            except Exception as e:
                gt_dfs.append(f"⚠️ Error loading GT DF: {e}")

    # Predicted DF
    pred_df = None
    pred_df_error = None
    if "predicted_df" in pred:
        try:
            pred_df = parse_dataframe(pred["predicted_df"])
        except Exception as e:
            pred_df_error = f"⚠️ Error loading predicted_df: {e}"

    # Build markdown string
    md = []
    md.append(
        f"### ❓ Failed Question #{example_index} (of {total_failed} examples) - Question ID: `{question_id}`\n"
    )
    md.append(f"**Question**: {utterance}\n")

    md.append("### ✅ Ground Truth SQL(s)")
    for sql in gt_sqls:
        md.append(f"```sql\n{sql}\n```")

    md.append("### ❌ Predicted SQL")
    md.append(f"```sql\n{pred.get('predicted_sql', '')}\n```")

    md.append("### 📊 Evaluation Metrics")
    eval_df = pd.DataFrame([pred.get("evaluation", {})])
    llm_explanation = None
    if "llm_explanation" in eval_df.columns:
        llm_explanation = eval_df.at[0, "llm_explanation"]
    columns_to_drop = ["gt_sql", "gt_df", "llm_explanation"]
    eval_df.drop(columns=columns_to_drop, errors="ignore", inplace=True)
    md.append(eval_df.to_markdown(index=False))

    md.append("### 📘 Ground Truth Result(s)")
    for i, df in enumerate(gt_dfs):
        md.append(f"**Result {i + 1}:**")
        if isinstance(df, pd.DataFrame):
            md.append(head_tail_with_ellipsis(df).to_markdown(index=False))
        else:
            md.append(df)

    md.append("### 📕 Predicted Result")
    if pred_df is not None:
        md.append(head_tail_with_ellipsis(pred_df).to_markdown(index=False))
    elif pred_df_error:
        md.append(pred_df_error)

    # Display agent trace for agentic pipelines, or prompt for standard baseline
    if "agent_trace" in pred and pred["agent_trace"]:
        md.append("### 🤖 Agent Interaction Trace")
        trace = pred["agent_trace"]
        if isinstance(trace, list):
            for i, interaction in enumerate(trace, 1):
                step_name = interaction.get("step", f"step_{i}")
                md.append(f"\n**Step {i}: {step_name}**\n")

                # Show messages (prompts sent to LLM)
                if "messages" in interaction:
                    md.append("<details>")
                    md.append("<summary>📝 Messages</summary>\n")
                    messages_html = chat_prompt_to_html(interaction["messages"])
                    md.append(messages_html)
                    md.append("</details>\n")

                # Show response
                if "response" in interaction:
                    md.append(
                        f"**Response:** `{safe_snippet(interaction['response'][:200])}`\n"
                    )

                # Show parsed SQL if available
                if "parsed_sql" in interaction:
                    md.append(
                        f"**Parsed SQL:** \n```sql\n{interaction['parsed_sql']}\n```\n"
                    )

                # Show LLM judge verdict if this is a validation step
                if "verdict" in interaction:
                    md.append(
                        f"**Verdict:** {interaction['verdict']} (Confidence: {interaction.get('confidence', 'N/A')})\n"
                    )
                    if "reasoning" in interaction:
                        md.append(
                            f"**Reasoning:** {safe_snippet(interaction['reasoning'][:300])}\n"
                        )

                # Show error if any
                if "error" in interaction:
                    md.append(f"**Error:** {interaction['error']}\n")
        else:
            md.append(safe_code_block(str(trace)))

        # Also show number of attempts if available
        if "agent_attempts" in pred:
            md.append(f"\n**Total Attempts:** {pred['agent_attempts']}")
    elif "agent_reasoning" in pred and pred["agent_reasoning"]:
        # Fallback to agent_reasoning if trace not available
        md.append("### 🤖 Agent Reasoning")
        reasoning_list = pred["agent_reasoning"]
        if isinstance(reasoning_list, list):
            reasoning_text = "\n".join(
                f"{i}. {reasoning}" for i, reasoning in enumerate(reasoning_list, 1)
            )
            md.append(safe_code_block(reasoning_text))
        else:
            md.append(safe_code_block(str(reasoning_list)))

        # Also show number of attempts if available
        if "agent_attempts" in pred:
            md.append(f"\n**Attempts:** {pred['agent_attempts']}")
    elif "prompt" in pred:
        md.append("### 🧠 Prompt")
        prompt = pred.get("prompt", "")
        if isinstance(prompt, list):
            html = chat_prompt_to_html(prompt)
            md.append(html)
        else:
            # Use HTML pre tags to avoid issues with nested backticks in prompt
            md.append(safe_code_block(prompt))
    else:
        md.append("### 🧠 Context")
        md.append("_No prompt or agent trace available_")

    if llm_explanation:
        md.append(
            f"### 🤖 LLM Judge Assessment\nLLM judge score: `{eval_df.at[0, 'llm_score']}`\n"
        )
        md.append("LLM judge explanation (if applicable):\n")
        md.append(safe_code_block(llm_explanation))

    return "\n\n".join(md)


def export_failed_examples_to_markdown(records, output_path, max_examples=20):
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    all_pipelines = get_pipeline_ids(records)
    if not all_pipelines:
        logger.error("No predictions found!")
        return
    markdown = "# ❌ Failed Examples by Pipeline\n\n"

    for pipeline_id in all_pipelines:
        all_failed = get_failed_records(records, pipeline_id)
        failed = all_failed[:max_examples]

        markdown += f"## 🔍 Pipeline/Model ID: `{pipeline_id}`\n\n"
        if not failed:
            markdown += "✅ No failed predictions found.\n\n"
        else:
            markdown += (
                f"{len(failed)} failed predictions shown (out of {len(all_failed)})\n\n"
            )
            for idx, record in enumerate(failed):
                markdown += format_failed_example(
                    record, pipeline_id, idx + 1, len(failed)
                )
                markdown += "\n\n---\n\n"

    output_path.write_text(markdown)
    logger.info(f"✅ Saved failed examples for error analysis to {output_path}")
