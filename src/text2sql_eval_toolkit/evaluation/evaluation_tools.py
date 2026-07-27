#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

import asyncio
import pandas as pd
from tqdm.asyncio import tqdm_asyncio
from text2sql_eval_toolkit.metrics.text2sql_utils import (
    compare_result_dfs,
    compare_dfs_bird_eval_logic,
    is_sqlglot_parsable,
    is_sqlparse_parsable,
    sqlglot_parsed_queries_equivalent,
    sqlglot_optimized_equivalence,
    sqlparse_queries_equivalent,
    sql_exact_match,
)
from text2sql_eval_toolkit.utils import (
    parse_dataframe,
    truncate_dataframe,
    get_gt_sqls,
    get_question,
    load_predictions_data,
    save_predictions_data,
    save_eval_summary,
    load_eval_summary,
)
from text2sql_eval_toolkit.database import jobs as db_jobs
from text2sql_eval_toolkit.database.session import get_connection
from text2sql_eval_toolkit.evaluation.llm_as_judge import (
    evaluate_sql_prediction_with_llm,
    load_llm_judge_config,
)
from text2sql_eval_toolkit.logging import get_logger


logger = get_logger(__name__)


def _build_llm_judge_generation_context(record, prediction, question: str) -> str:
    """Build the generation-context string passed to the LLM judge prompt."""
    if "agent_trace" in prediction and prediction["agent_trace"]:
        trace = prediction["agent_trace"]
        trace_text = "Agent Interaction Trace:\n\n"
        for i, interaction in enumerate(trace, 1):
            if interaction is None:
                continue
            trace_text += f"Step {i}: {interaction.get('step', 'unknown')}\n"
            if "messages" in interaction:
                for msg in interaction["messages"]:
                    role = msg.get("role", "unknown")
                    content = msg.get("content", "")[:500]
                    trace_text += f"  [{role}]: {content}...\n"
            if "response" in interaction:
                trace_text += f"  [response]: {interaction['response'][:500]}...\n"
            trace_text += "\n"
        return trace_text

    if "agent_reasoning" in prediction:
        reasoning_list = prediction["agent_reasoning"]
        return "Agent Reasoning:\n" + "\n".join(f"- {r}" for r in reasoning_list)

    if "prompt" in prediction:
        return prediction["prompt"]

    schema_info = record.get("schema", {})
    db_type = record.get("db_type", "SQL")
    return (
        f"Question: {question}\n\nDatabase Type: {db_type}\n\n"
        f"Schema: {schema_info}\n\nGenerate SQL to answer the question."
    )


def evaluate_llm_judge_for_prediction(
    record,
    prediction,
    llm_judge_config: dict,
    *,
    force_rerun_llm_judge: bool = False,
    ground_truth_sql: str | None = None,
    ground_truth_df=None,
) -> dict:
    """
    Run LLM-as-judge for a single prediction.

    Returns a dict with ``llm_score`` and ``llm_explanation``, or ``llm_judge_error``
    on failure. Existing evaluation fields are not modified.
    """
    result: dict = {}

    if prediction.get("inference_error"):
        result["llm_score"] = 0.0
        result["llm_explanation"] = (
            f"N/A (inference failed: {prediction['inference_error']})"
        )
        return result

    if not force_rerun_llm_judge:
        existing_eval = prediction.get("evaluation", {})
        if (
            "llm_score" in existing_eval
            and "llm_explanation" in existing_eval
            and "llm_judge_error" not in existing_eval
        ):
            try:
                cached_score = float(existing_eval["llm_score"])
                result["llm_score"] = cached_score
                result["llm_explanation"] = existing_eval["llm_explanation"]
                logger.info(f"Reusing cached LLM judge results (score: {cached_score})")
                return result
            except (ValueError, TypeError):
                logger.warning("Invalid cached llm_score, will re-run LLM judge")

    pred_df = None
    try:
        pred_df = parse_dataframe(prediction["predicted_df"])
    except Exception:
        pred_df = None

    if pred_df is None:
        result["llm_score"] = 0.0
        result["llm_explanation"] = (
            "N/A (did not use LLM due to missing prediction dataframe)"
        )
        return result

    if ground_truth_sql is None:
        gold_sqls = get_gt_sqls(record)
        ground_truth_sql = gold_sqls[0] if gold_sqls else ""

    if ground_truth_df is None:
        gold_dfs = record["gt_df"]
        if not isinstance(gold_dfs, list):
            gold_dfs = [gold_dfs]
        ground_truth_df = gold_dfs[0] if gold_dfs else None

    try:
        gold_df = parse_dataframe(ground_truth_df) if ground_truth_df is not None else None
    except Exception:
        gold_df = None

    question = get_question(record)
    generation_context = _build_llm_judge_generation_context(record, prediction, question)

    try:
        llm_as_judge_response = evaluate_sql_prediction_with_llm(
            question,
            ground_truth_sql,
            truncate_dataframe(gold_df) if gold_df is not None else ground_truth_df,
            prediction["predicted_sql"],
            truncate_dataframe(pred_df),
            generation_context,
            llm_judge_config,
        )
        result["llm_score"] = float(llm_as_judge_response["score"])
        result["llm_explanation"] = llm_as_judge_response["explanation"]
    except Exception as e:
        logger.error(f"LLM judge error: {repr(e)}")
        result["llm_judge_error"] = repr(e)

    return result


def evaluate_prediction(record, prediction, llm_judge_config=None, force_rerun_llm_judge=False):
    """
    Evaluates a predicted SQL query against one or more ground truth SQL queries and their corresponding result dataframes.

    This function supports multiple ground truth SQLs per record. It iterates through each ground truth SQL and its
    associated result dataframe, comparing them to the predicted SQL and its result dataframe. Evaluation stops early
    if a perfect (subset/super) execution match (subset_non_empty_execution_accuracy == 1) is found.

    Parameters
    ----------
    record : dict
        A dictionary containing the ground truth SQL(s) and their corresponding result dataframe(s).
        Expected keys:
            - "sql": str or List[str]
                One or more ground truth SQL queries.
            - "gt_df": dict or List[dict]
                One or more serialized dataframes corresponding to the ground truth SQL queries.

    prediction : dict
        A dictionary containing the predicted SQL and its result dataframe.
        Expected keys:
            - "predicted_sql": str
                The SQL query generated by the model.
            - "predicted_df": dict
                The serialized dataframe resulting from executing the predicted SQL.
            - "sql_execution_error" (optional): str
                An error message if the predicted SQL failed to execute.
            - "evaluation" (optional): dict
                Existing evaluation results. If present and contains valid LLM judge results,
                they will be reused unless force_rerun_llm_judge is True.

    llm_judge_config : dict, optional
        dictionary config object loaded from the YAML configuration file containing model parameters
        and prompt template for LLM-based evaluation. If not provided, LLM judge will not be used.

    force_rerun_llm_judge : bool, optional
        If True, forces re-evaluation with LLM judge even if cached results exist.
        If False (default), reuses existing LLM judge results when available.

    Returns
    -------
    result : dict
        A dictionary containing evaluation metrics and flags. Keys include:
            - "execution_accuracy": int
                Whether the predicted result matches the ground truth result exactly.
            - "non_empty_execution_accuracy": int
                Whether the predicted result matches the ground truth result and is non-empty.
            - "subset_non_empty_execution_accuracy": int
                Whether the predicted result is a non-empty subset or superset of the ground truth result.
            - "logic_execution_accuracy": int
                Execution accuracy of SQL logic if record as logic_df
                (result of running query with SELECT clause replaced with gt's SELECT clause).
            - "bird_execution_accuracy": int
                A relaxed match score based on BIRD evaluation logic.
            - "llm_score" (optional): float
                If llm_judge_config is provided, the score using LLM as judge
            - "is_sqlglot_parsable": int
                Whether the predicted SQL is parsable by SQLGlot.
            - "is_sqlparse_parsable": int
                Whether the predicted SQL is parsable by sqlparse.
            - "sqlglot_equivalence": int
                Whether the predicted SQL is equivalent to the ground truth SQL using SQLGlot parsing.
            - "sqlglot_optimized_equivalence": int
                Whether the predicted SQL is equivalent to the ground truth SQL using SQLGlot optimization.
            - "sqlparse_equivalence": int
                Whether the predicted SQL is equivalent to the ground truth SQL using sqlparse.
            - "sql_exact_match": int
                Whether the predicted SQL exactly matches the ground truth SQL string.
            - "sql_syntactic_equivalence": int
                Whether any of the syntactic equivalence checks passed.
            - "df_error": int
                Indicates if there was an error parsing the predicted dataframe.
            - "df_error_message" (optional): str
                Error message if dataframe parsing failed.
            - "eval_error": int
                Indicates if there was an error during evaluation.
            - "eval_error_message" (optional): str
                Error message if evaluation failed.
            - "llm_explanation" (optional): str
                If llm_judge_config is provided, LLM judge explanation of the accuracy of the prediction
            - "gt_sql" (optional): str
                The ground truth SQL query that was used for final evaluation, only present
                if subset_non_empty_execution_accuracy == 1.
            - "gt_df" (optional): DataFrame
                The parsed ground truth dataframe that was used for final evaluation, only present
                if subset_non_empty_execution_accuracy == 1.

    Notes
    -----
    - If the predicted dataframe cannot be parsed, the function returns early with a dataframe error.
    - If multiple ground truth SQLs are provided, the function evaluates them in order and stops at the first
      one that results in a perfect execution match.
    - The function uses several SQL equivalence and result comparison methods to assess prediction quality.
    - The final result reflects the evaluation against the first ground truth SQL that yields
      subset_non_empty_execution_accuracy == 1, or the last one evaluated if no perfect match is found.
    - The "gt_sql" and "gt_df" fields are only included in the result if a perfect execution match is found.
    - LLM judge caching: If the prediction already has an "evaluation" dict with valid "llm_score" and
      "llm_explanation" fields (and no "llm_judge_error"), those cached results will be reused unless
      force_rerun_llm_judge is True. This significantly improves performance when re-evaluating the same data.
    """
    result = {}
    
    # Check for inference error - skip evaluation if inference failed.
    # Use truthiness so store-backed nulls (inference_error: null) are not treated as failures.
    if prediction.get("inference_error"):
        return {
            "execution_accuracy": 0,
            "non_empty_execution_accuracy": 0,
            "subset_non_empty_execution_accuracy": 0,
            "logic_execution_accuracy": 0,
            "bird_execution_accuracy": 0,
            "is_sqlglot_parsable": 0,
            "is_sqlparse_parsable": 0,
            "sqlglot_equivalence": 0,
            "sqlglot_optimized_equivalence": 0,
            "sqlparse_equivalence": 0,
            "sql_exact_match": 0,
            "sql_syntactic_equivalence": 0,
            "df_error": 1,
            "df_error_message": f"Inference failed: {prediction['inference_error']}",
            "eval_error": 0,
        }
    
    pred_df = None
    predicted_sql = prediction["predicted_sql"]

    try:
        pred_df = parse_dataframe(prediction["predicted_df"])
        result["df_error"] = 0
    except Exception as e:
        result["df_error"] = 1
        result["df_error_message"] = prediction.get("sql_execution_error") or str(e)

    try:
        gold_sqls = get_gt_sqls(record)
        gold_dfs = record["gt_df"]

        if not isinstance(gold_dfs, list):
            gold_dfs = [gold_dfs]

        for gold_sql, gold_df_raw in zip(gold_sqls, gold_dfs):
            gold_df = parse_dataframe(gold_df_raw)

            match, non_empty_match, subset_match = (
                compare_result_dfs(gold_df, pred_df, gold_sql)
                if gold_sql and pred_df is not None
                else (0, 0, 0)
            )
            bird_match = (
                compare_dfs_bird_eval_logic(gold_df, pred_df)
                if gold_sql and pred_df is not None
                else 0
            )
            logic_match = subset_match
            if logic_match == 0:
                logic_df_raw = prediction.get("logic_df")
                if logic_df_raw is not None:
                    logic_df = parse_dataframe(logic_df_raw)
                    _, logic_match, _ = (
                        compare_result_dfs(gold_df, logic_df, gold_sql)
                        if gold_sql and logic_df is not None
                        else (0, 0, 0)
                    )

            is_glot_parsable = is_sqlglot_parsable(predicted_sql)
            sqlparse_parsable = is_sqlparse_parsable(predicted_sql)
            sqlglot_equivalence_score = (
                sqlglot_parsed_queries_equivalent(predicted_sql, gold_sql, dialect=record.get("dialect", ""))
                if is_glot_parsable
                else 0
            )
            sqlglot_optimized_equivalence_score = (
                sqlglot_optimized_equivalence(predicted_sql, gold_sql, dialect=record.get("dialect", ""))
                if is_glot_parsable
                else 0
            )
            sqlparse_equivalance = (
                sqlparse_queries_equivalent(predicted_sql, gold_sql)
                if sqlparse_parsable
                else 0
            )
            sql_exact_match_score = sql_exact_match(predicted_sql, gold_sql)

            result.update(
                {
                    "execution_accuracy": int(match),
                    "non_empty_execution_accuracy": int(non_empty_match),
                    "subset_non_empty_execution_accuracy": int(subset_match),
                    "logic_execution_accuracy": int(logic_match),
                    "bird_execution_accuracy": int(bird_match),
                    "is_sqlglot_parsable": int(is_glot_parsable),
                    "is_sqlparse_parsable": int(sqlparse_parsable),
                    "sqlglot_equivalence": int(sqlglot_equivalence_score),
                    "sqlglot_optimized_equivalence": int(
                        sqlglot_optimized_equivalence_score
                    ),
                    "sqlparse_equivalence": int(sqlparse_equivalance),
                    "sql_exact_match": int(sql_exact_match_score),
                    "sql_syntactic_equivalence": int(
                        any(
                            [
                                sqlglot_equivalence_score,
                                sqlglot_optimized_equivalence_score,
                                sqlparse_equivalance,
                                sql_exact_match_score,
                            ]
                        )
                    ),
                    "eval_error": 0,
                }
            )
            result["df_error"] = result.pop("df_error")
            
            # Add token usage metrics from prediction to evaluation result
            token_usage = prediction.get("token_usage")
            if token_usage:
                result["prompt_tokens"] = token_usage.get("prompt_tokens", 0)
                result["completion_tokens"] = token_usage.get("completion_tokens", 0)
                result["total_tokens"] = token_usage.get("total_tokens", 0)
            
            # Add timing metrics from prediction to evaluation result
            inference_time = prediction.get("inference_time_ms")
            if inference_time is not None:
                result["inference_time_ms"] = inference_time
            execution_time = prediction.get("execution_time_ms")
            if execution_time is not None:
                result["execution_time_ms"] = execution_time

            if llm_judge_config:
                llm_result = evaluate_llm_judge_for_prediction(
                    record,
                    prediction,
                    llm_judge_config,
                    force_rerun_llm_judge=force_rerun_llm_judge,
                    ground_truth_sql=gold_sql,
                    ground_truth_df=gold_df_raw,
                )
                result.update(llm_result)

            if result["subset_non_empty_execution_accuracy"] == 1:
                result["gt_sql"] = gold_sql
                result["gt_df"] = gold_df_raw
                break

    except Exception as e:
        result["eval_error"] = 1
        result["eval_error_message"] = repr(e)
        # raise e

    return result


def compute_summary(metrics_by_model, llm_judge_config, token_usage_by_model=None):
    summary = {}
    for model, records in metrics_by_model.items():
        num_records = len(records)
        num_eval_errors = sum(1 for r in records if "eval_error_message" in r)
        num_df_errors = sum(1 for r in records if "df_error_message" in r)
        # Count records with inference errors (failed to generate SQL)
        num_inference_errors = sum(
            1 for r in records
            if "df_error_message" in r and "Inference failed" in (r.get("df_error_message") or "")
        )
        # Count records with successful predictions (SQL was generated)
        num_predictions = num_records - num_inference_errors
        num_evaluated = num_records - num_eval_errors
        num_correct_non_empty_execution_accuracy = sum(
            r["non_empty_execution_accuracy"]
            for r in records
            if "eval_error_message" not in r
        )
        num_correct_subset_non_empty_execution_accuracy = sum(
            r["subset_non_empty_execution_accuracy"]
            for r in records
            if "eval_error_message" not in r
        )

        df = None
        if num_evaluated > 0:
            df = pd.DataFrame([r for r in records if "eval_error_message" not in r])
            # Calculate metrics based on num_records (total benchmark size) instead of num_evaluated
            # This ensures that failures to generate predictions or evaluation errors count as 0
            metric_stats = {}
            for metric in df.columns:
                if metric not in [
                    "eval_error_message",
                    "df_error_message",
                    "llm_judge_error",
                    "llm_explanation",
                    "gt_sql",
                    "gt_df",
                ]:
                    # For accuracy metrics, divide by num_records (not num_evaluated)
                    # This penalizes pipelines that fail to generate predictions
                    metric_sum = df[metric].sum()
                    metric_stats[metric] = {
                        "average": metric_sum / num_records,  # Changed from df[metric].mean()
                        "stddev": df[metric].std()
                    }
            
            # Token metrics are automatically calculated by pandas from the evaluation records
            # The statistics (average, stddev) are already in metric_stats from lines above
            # We just need to add the total sums as separate count metrics
            if "total_tokens" in df.columns:
                metric_stats["sum_total_tokens"] = int(df["total_tokens"].sum())
                metric_stats["sum_prompt_tokens"] = int(df["prompt_tokens"].sum())
                metric_stats["sum_completion_tokens"] = int(df["completion_tokens"].sum())
            
            # Timing metrics - add total sums
            if "inference_time_ms" in df.columns:
                metric_stats["sum_inference_time_ms"] = round(df["inference_time_ms"].sum(), 2)
            if "execution_time_ms" in df.columns:
                metric_stats["sum_execution_time_ms"] = round(df["execution_time_ms"].sum(), 2)
        else:
            metric_stats = {}

        metric_stats["num_records"] = num_records
        metric_stats["num_predictions"] = num_predictions
        metric_stats["num_evaluated"] = num_evaluated
        metric_stats["num_eval_errors"] = num_eval_errors
        metric_stats["num_df_errors"] = num_df_errors
        metric_stats["num_inference_errors"] = num_inference_errors
        metric_stats["num_correct_non_empty_execution_accuracy"] = (
            num_correct_non_empty_execution_accuracy
        )
        metric_stats["num_correct_subset_non_empty_execution_accuracy"] = (
            num_correct_subset_non_empty_execution_accuracy
        )
        
        if llm_judge_config:
            metric_stats["num_correct_llm"] = sum(
                1
                for r in records
                if "llm_judge_error" not in r
                and "eval_error_message" not in r
                and r.get("llm_score") == 1
            )
            metric_stats["num_llm_judge_errors"] = sum(
                1 for r in records if "llm_judge_error" in r
            )
            if "llm_judge_config" not in summary:
                summary["llm_judge_config"] = llm_judge_config

        summary[model] = metric_stats

    return summary


def summary_to_df_csv(summary, output_path, use_llm):
    rows = []
    for model, metrics in summary.items():
        if model == "llm_judge_config":
            continue
        row = {
            "Model": model,
            "Total": metrics.get("num_records", 0),
            "Evaluated": metrics.get("num_evaluated", 0),
            "Number of Correct Non-Empty Data Frames": metrics.get(
                "num_correct_non_empty_execution_accuracy"
            ),
            "Number of Correct Subset/Superset Non-Empty Data Frames": metrics.get(
                "num_correct_subset_non_empty_execution_accuracy"
            ),
            "Number of Correct Results According to LLM Judge": (
                metrics["num_correct_llm"] if use_llm else "N/A"
            ),
            "Evaluation Errors": metrics.get("num_eval_errors", 0),
            "Dataframe Errors": metrics.get("num_df_errors", 0),
            "LLM Judge Errors": metrics.get("num_llm_judge_errors", 0),
            "Total Tokens": metrics.get("sum_total_tokens", "N/A"),
            "Avg Tokens/Question": (
                round(metrics.get("total_tokens", {}).get("average", 0), 2)
                if isinstance(metrics.get("total_tokens"), dict)
                else "N/A"
            ),
            "Total Prompt Tokens": metrics.get("sum_prompt_tokens", "N/A"),
            "Total Completion Tokens": metrics.get("sum_completion_tokens", "N/A"),
            "Total Inference Time (ms)": metrics.get("sum_inference_time_ms", "N/A"),
            "Avg Inference Time (ms)": (
                round(metrics.get("inference_time_ms", {}).get("average", 0), 2)
                if isinstance(metrics.get("inference_time_ms"), dict)
                else "N/A"
            ),
            "Total Execution Time (ms)": metrics.get("sum_execution_time_ms", "N/A"),
            "Avg Execution Time (ms)": (
                round(metrics.get("execution_time_ms", {}).get("average", 0), 2)
                if isinstance(metrics.get("execution_time_ms"), dict)
                else "N/A"
            ),
        }

        for metric, stats in metrics.items():
            if isinstance(stats, dict):
                row[f"{metric}_avg"] = round(stats.get("average", 0), 4)
                row[f"{metric}_std"] = round(stats.get("stddev", 0), 4)

        rows.append(row)

    df = pd.DataFrame(rows)

    sort_col = "subset_non_empty_execution_accuracy_avg"
    if sort_col in df.columns:
        df.sort_values(by=sort_col, ascending=False, inplace=True)

    df.to_csv(output_path, index=False)
    logger.info(f"\nSummary written to: {output_path}")
    return df


def print_summary(summary, use_llm):
    print("\n=== Evaluation Summary ===")
    for pipeline, metrics in summary.items():
        if pipeline == "llm_judge_config":
            continue
        print(f"\n: {pipeline}")
        num_records = metrics.get("num_records", 0)
        num_evaluated = metrics.get("num_evaluated", 0)
        num_eval_errors = metrics.get("num_eval_errors", 0)
        num_df_errors = metrics.get("num_df_errors", 0)
        num_correct_non_empty_execution_accuracy = metrics.get(
            "num_correct_non_empty_execution_accuracy"
        )
        num_correct_subset_non_empty_execution_accuracy = metrics.get(
            "num_correct_subset_non_empty_execution_accuracy"
        )
        print(f"  Total Records       : {num_records}")
        print(f"  Successfully Evaluated: {num_evaluated}")
        print(
            f"  Number of Correct Non-Empty Data Frames: {num_correct_non_empty_execution_accuracy}"
        )
        print(
            f"  Number of Correct Subset/Superset Non-Empty Data Frames: {num_correct_subset_non_empty_execution_accuracy}"
        )
        if use_llm:
            print(
                f"  Number of Correct Results According to LLM Judge: {metrics.get('num_correct_llm')}"
            )
            print(
                f"  Number of LLM Judge errors: {metrics.get('num_llm_judge_errors')}"
            )
        print(f"  Evaluation Errors              : {num_eval_errors}")
        print(f"  Dataframe Errors              : {num_df_errors}")
        
        # Print token usage metrics if available
        if "sum_total_tokens" in metrics:
            print(f"  Token Usage Metrics:")
            print(f"    Total Tokens                 : {metrics.get('sum_total_tokens', 0):,}")
            total_tokens_stats = metrics.get('total_tokens', {})
            if isinstance(total_tokens_stats, dict):
                avg_val = total_tokens_stats.get('average', 0)
            else:
                avg_val = 0
            print(f"    Avg Tokens per Question      : {avg_val:.2f}")
            print(f"    Total Prompt Tokens          : {metrics.get('sum_prompt_tokens', 0):,}")
            print(f"    Total Completion Tokens      : {metrics.get('sum_completion_tokens', 0):,}")
        
        # Print timing metrics if available
        if "sum_inference_time_ms" in metrics or "sum_execution_time_ms" in metrics:
            print(f"  Performance Metrics:")
            if "sum_inference_time_ms" in metrics:
                inference_stats = metrics.get('inference_time_ms', {})
                if isinstance(inference_stats, dict):
                    avg_inference = inference_stats.get('average', 0)
                else:
                    avg_inference = 0
                print(f"    Total Inference Time         : {metrics.get('sum_inference_time_ms', 0):,.2f} ms")
                print(f"    Avg Inference Time per Query : {avg_inference:.2f} ms")
            
            if "sum_execution_time_ms" in metrics:
                execution_stats = metrics.get('execution_time_ms', {})
                if isinstance(execution_stats, dict):
                    avg_execution = execution_stats.get('average', 0)
                else:
                    avg_execution = 0
                print(f"    Total Execution Time         : {metrics.get('sum_execution_time_ms', 0):,.2f} ms")
                print(f"    Avg Execution Time per Query : {avg_execution:.2f} ms")
        
        for metric, stats in metrics.items():
            if metric in {
                "num_records",
                "num_predictions",
                "num_evaluated",
                "num_eval_errors",
                "num_df_errors",
                "num_inference_errors",
                "num_correct_non_empty_execution_accuracy",
                "num_correct_subset_non_empty_execution_accuracy",
                "num_correct_llm",
                "num_llm_judge_errors",
                "sum_total_tokens",
                "sum_prompt_tokens",
                "sum_completion_tokens",
                "sum_inference_time_ms",
                "sum_execution_time_ms",
                "inference_time_ms",
                "execution_time_ms",
            }:
                continue
            print(
                f"  {metric:<30} Avg: {stats['average']:.4f}  StdDev: {stats['stddev']:.4f}"
            )


async def async_evaluate_predictions(
    benchmark_id: str,
    llm_judge_config: dict = None,
    max_concurrency: int = 16,
    force_rerun_llm_judge: bool = False,
    force_rerun: bool = False,
    *,
    csv_summary_path: str | None = None,
    job_id: str | None = None,
):
    if not benchmark_id:
        raise ValueError("benchmark_id is required")

    job_type = db_jobs.resolve_eval_job_type(use_llm_judge=llm_judge_config is not None)
    job_params = {
        "use_llm_judge": llm_judge_config is not None,
        "force_rerun": force_rerun,
        "force_rerun_llm_judge": force_rerun_llm_judge,
    }
    conn = get_connection()
    with db_jobs.track_job(conn, job_type, benchmark_id, job_id=job_id, params=job_params):
        data = load_predictions_data(benchmark_id)

        semaphore = asyncio.Semaphore(max_concurrency)

        async def worker(record, prediction, llm_judge_config, force_rerun_llm_judge):
            async with semaphore:
                return await asyncio.to_thread(
                    evaluate_prediction,
                    record,
                    prediction,
                    llm_judge_config,
                    force_rerun_llm_judge,
                )

        existing_evaluations = {}
        if not force_rerun:
            try:
                existing_data = load_predictions_data(benchmark_id, include_eval=True)
                for record in existing_data:
                    record_id = record.get("id") or record.get("question_id")
                    if record_id:
                        existing_evaluations[record_id] = record.get("predictions", {})
            except Exception as e:
                logger.warning(f"Could not load existing evaluations from database: {e}")

        if not force_rerun:
            for record in data:
                record_id = record.get("id") or record.get("question_id")
                if record_id and record_id in existing_evaluations:
                    predictions = record.get("predictions", {})
                    for model_name, prediction in predictions.items():
                        if model_name in existing_evaluations[record_id]:
                            existing_eval = existing_evaluations[record_id][model_name].get(
                                "evaluation", {}
                            )
                            if existing_eval:
                                prediction["evaluation"] = existing_eval

        tasks = []
        prediction_references = []
        for record in data:
            predictions = record.get("predictions", {})
            for model_name, prediction in predictions.items():
                task = worker(record, prediction, llm_judge_config, force_rerun_llm_judge)
                tasks.append(task)
                prediction_references.append((record, model_name, prediction))

        evaluations = await tqdm_asyncio.gather(
            *tasks, desc=f"Evaluating (concurrency limit: {max_concurrency})"
        )

        metrics_by_model = {}
        token_usage_by_model = {}
        for i, evaluation in enumerate(evaluations):
            record, model_name, prediction = prediction_references[i]
            prediction["evaluation"] = evaluation

            if model_name not in metrics_by_model:
                metrics_by_model[model_name] = []
                token_usage_by_model[model_name] = []
            metrics_by_model[model_name].append(evaluation)

            token_usage = prediction.get("token_usage")
            if token_usage:
                token_usage_by_model[model_name].append(token_usage)

        summary = compute_summary(metrics_by_model, llm_judge_config, token_usage_by_model)

        save_predictions_data(
            benchmark_id,
            data,
            include_eval=True,
            status="evaluated",
            llm_judge_config=llm_judge_config,
        )
        save_eval_summary(benchmark_id, summary)

        use_llm = llm_judge_config is not None
        summary_df = summary_to_df_csv(summary, csv_summary_path, use_llm)
        print_summary(summary, use_llm)

        return data, summary_df


async def async_run_llm_judge(
    benchmark_id: str,
    llm_judge_config: dict,
    *,
    max_concurrency: int = 16,
    force_rerun_llm_judge: bool = False,
    csv_summary_path: str | None = None,
    job_id: str | None = None,
):
    """Run LLM-as-judge only on existing predictions (no deterministic metrics)."""
    if not benchmark_id:
        raise ValueError("benchmark_id is required")
    if not llm_judge_config:
        raise ValueError("llm_judge_config is required")

    job_params = {
        "use_llm_judge": True,
        "force_rerun_llm_judge": force_rerun_llm_judge,
        "llm_judge_only": True,
    }
    conn = get_connection()
    with db_jobs.track_job(conn, "llm_judge", benchmark_id, job_id=job_id, params=job_params):
        data = load_predictions_data(benchmark_id, include_eval=True)

        semaphore = asyncio.Semaphore(max_concurrency)

        async def worker(record, prediction):
            async with semaphore:
                return await asyncio.to_thread(
                    evaluate_llm_judge_for_prediction,
                    record,
                    prediction,
                    llm_judge_config,
                    force_rerun_llm_judge=force_rerun_llm_judge,
                )

        tasks = []
        prediction_references = []
        for record in data:
            for model_name, prediction in record.get("predictions", {}).items():
                tasks.append(worker(record, prediction))
                prediction_references.append((record, model_name, prediction))

        llm_results = await tqdm_asyncio.gather(
            *tasks,
            desc=f"LLM judge (concurrency limit: {max_concurrency})",
        )

        metrics_by_model: dict[str, list] = {}
        for i, llm_result in enumerate(llm_results):
            _, model_name, prediction = prediction_references[i]
            evaluation = prediction.setdefault("evaluation", {})
            evaluation.update(llm_result)
            if llm_result.get("llm_judge_error"):
                evaluation.pop("llm_score", None)
                evaluation.pop("llm_explanation", None)

            metrics_by_model.setdefault(model_name, []).append(evaluation)

        save_predictions_data(
            benchmark_id,
            data,
            include_eval=True,
            status="evaluated",
            llm_judge_config=llm_judge_config,
        )

        llm_summary = _compute_llm_judge_only_summary(metrics_by_model, llm_judge_config)

        can_compute_full_summary = all(
            _has_full_deterministic_metrics(records)
            for records in metrics_by_model.values()
            if records
        )
        if can_compute_full_summary:
            summary = compute_summary(metrics_by_model, llm_judge_config)
            if csv_summary_path:
                summary_to_df_csv(summary, csv_summary_path, use_llm=True)
            print_summary(summary, use_llm=True)
        else:
            try:
                existing_summary = load_eval_summary(benchmark_id) or {}
            except Exception:
                existing_summary = {}
            summary = _merge_llm_judge_summary(existing_summary, llm_summary)
            _print_llm_judge_only_summary(llm_summary)

        save_eval_summary(benchmark_id, summary)

        return data, summary


_FULL_SUMMARY_METRICS = frozenset(
    {
        "non_empty_execution_accuracy",
        "subset_non_empty_execution_accuracy",
    }
)


def _has_full_deterministic_metrics(records: list) -> bool:
    return bool(records) and all(
        _FULL_SUMMARY_METRICS.issubset(record) for record in records
    )


def _merge_llm_judge_summary(existing: dict, llm_summary: dict) -> dict:
    merged = dict(existing or {})
    if "llm_judge_config" in llm_summary:
        merged["llm_judge_config"] = llm_summary["llm_judge_config"]
    for pipeline, llm_metrics in llm_summary.items():
        if pipeline == "llm_judge_config":
            continue
        pipeline_summary = dict(merged.get(pipeline, {}))
        pipeline_summary["num_records"] = llm_metrics.get(
            "num_records", pipeline_summary.get("num_records")
        )
        pipeline_summary["num_correct_llm"] = llm_metrics["num_correct_llm"]
        pipeline_summary["num_llm_judge_errors"] = llm_metrics["num_llm_judge_errors"]
        pipeline_summary["llm_score"] = llm_metrics["llm_score"]
        pipeline_summary["num_evaluated"] = llm_metrics.get(
            "num_judged", pipeline_summary.get("num_evaluated")
        )
        merged[pipeline] = pipeline_summary
    return merged


def _compute_llm_judge_only_summary(metrics_by_model: dict, llm_judge_config: dict) -> dict:
    summary: dict = {"llm_judge_config": llm_judge_config}
    for pipeline, records in metrics_by_model.items():
        num_records = len(records)
        judged = [r for r in records if "llm_judge_error" not in r and "llm_score" in r]
        num_correct = sum(1 for r in judged if r.get("llm_score") == 1)
        num_errors = sum(1 for r in records if "llm_judge_error" in r)
        avg_score = (
            sum(r.get("llm_score", 0) for r in judged) / num_records if num_records else 0.0
        )
        summary[pipeline] = {
            "num_records": num_records,
            "num_judged": len(judged),
            "num_correct_llm": num_correct,
            "num_llm_judge_errors": num_errors,
            "llm_score": {"average": avg_score},
        }
    return summary


def _print_llm_judge_only_summary(summary: dict) -> None:
    print("\n=== LLM Judge Summary ===")
    for pipeline, metrics in summary.items():
        if pipeline == "llm_judge_config":
            continue
        print(f"\n: {pipeline}")
        print(f"  Total Records       : {metrics.get('num_records', 0)}")
        print(f"  Judged              : {metrics.get('num_judged', 0)}")
        print(f"  Correct (score=1)   : {metrics.get('num_correct_llm', 0)}")
        avg = metrics.get("llm_score", {}).get("average", 0)
        print(f"  Avg LLM Score       : {avg:.4f}")
        print(f"  LLM Judge Errors    : {metrics.get('num_llm_judge_errors', 0)}")


def run_llm_judge(
    benchmark_id: str,
    *,
    llm_judge_config_path: str | None = None,
    force_rerun_llm_judge: bool = False,
    max_concurrency: int = 16,
    csv_summary_path: str | None = None,
    job_id: str | None = None,
):
    """Run LLM-as-judge only on existing predictions stored in SQLite."""
    llm_judge_config = load_llm_judge_config(llm_judge_config_path)
    return asyncio.run(
        async_run_llm_judge(
            benchmark_id=benchmark_id,
            llm_judge_config=llm_judge_config,
            max_concurrency=max_concurrency,
            force_rerun_llm_judge=force_rerun_llm_judge,
            csv_summary_path=csv_summary_path,
            job_id=job_id,
        )
    )


def evaluate_predictions(
    benchmark_id: str,
    *,
    use_llm: bool = False,
    llm_judge_config_path: str = None,
    force_rerun_llm_judge: bool = False,
    force_rerun: bool = False,
    csv_summary_path: str | None = None,
):
    """Evaluate predictions for a benchmark stored in SQLite."""
    return run_evaluation(
        benchmark_id,
        use_llm=use_llm,
        llm_judge_config_path=llm_judge_config_path,
        force_rerun_llm_judge=force_rerun_llm_judge,
        force_rerun=force_rerun,
        csv_summary_path=csv_summary_path,
    )


# For running from script
def run_evaluation(
    benchmark_id: str,
    use_llm: bool = False,
    llm_judge_config_path: str = None,
    force_rerun_llm_judge: bool = False,
    force_rerun: bool = False,
    *,
    csv_summary_path: str | None = None,
    job_id: str | None = None,
):
    llm_judge_config = None
    if use_llm or llm_judge_config_path is not None:
        llm_judge_config = load_llm_judge_config(llm_judge_config_path)
    return asyncio.run(
        async_evaluate_predictions(
            benchmark_id=benchmark_id,
            llm_judge_config=llm_judge_config,
            force_rerun_llm_judge=force_rerun_llm_judge or force_rerun,
            force_rerun=force_rerun,
            csv_summary_path=csv_summary_path,
            job_id=job_id,
        )
    )
