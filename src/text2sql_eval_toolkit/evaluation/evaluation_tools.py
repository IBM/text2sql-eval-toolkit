#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

import asyncio
import json
import threading
import pandas as pd
from pathlib import Path
from typing import Any, Dict, Optional
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
    get_benchmark_info,
    parse_dataframe,
    get_gt_sqls,
    get_default_eval_filename,
    add_summary_json_suffix,
    add_summary_csv_suffix,
)
from text2sql_eval_toolkit.evaluation.llm_as_judge import (
    evaluate_sql_prediction_with_llm,
    judge_config_digest,
    load_llm_judge_config,
)
from text2sql_eval_toolkit.evaluation.judge_inputs import build_llm_judge_inputs
from text2sql_eval_toolkit.logging import get_logger

logger = get_logger(__name__)

#: Key under which a summary records the judge that produced its verdicts.
#: It sits alongside pipeline ids; see [`split_summary`][text2sql_eval_toolkit.split_summary].
JUDGE_CONFIG_KEY = "llm_judge_config"

#: Key under which a verdict the judge gave records the config that gave it.
JUDGE_DIGEST_KEY = "llm_judge_config_digest"

#: Which stored verdicts evaluation may reuse instead of calling the judge.
LLM_JUDGE_REUSE_MODES = ("matching", "any")

#: How an explanation starts when the judge was never asked.
_NOT_JUDGED = "N/A (did not use LLM"


def _is_judge_verdict(
    evaluation: Dict[str, Any], *, past_failure: bool = False
) -> bool:
    """
    Whether *evaluation* carries a verdict the judge itself gave.

    A stored ``llm_judge_error`` records that the *last* call failed. It does
    not mean there is no verdict: a run that fails keeps the one an earlier run
    stored, beside the error. The two questions are therefore different, and
    conflating them lost that verdict on the second failure in a row.

    By default the answer is no, which is what the reuse path wants: a
    prediction whose judge failed is asked again rather than settled from what
    is stored. ``past_failure=True`` asks only whether a verdict is there --
    what the handler for a failed call needs, and what counting verdicts by the
    config that gave them needs.
    """
    return (
        "llm_score" in evaluation
        and (past_failure or "llm_judge_error" not in evaluation)
        and not str(evaluation.get("llm_explanation", "")).startswith(_NOT_JUDGED)
    )


def _stored_verdict(
    evaluation: Optional[Dict[str, Any]],
    digest: str,
    reuse: str,
    *,
    past_failure: bool = False,
) -> Optional[Dict[str, Any]]:
    """
    A stored verdict that may stand in for calling the judge, or ``None``.

    Under ``"matching"`` only a verdict recorded with *digest* qualifies, which
    is what stops a score given by one judge being reported under another: the
    batch judge used to reuse any stored score, so evaluating Llama-judged
    results with a different config kept every Llama score and recorded the new
    config in the summary. Under ``"any"`` a verdict from any config qualifies,
    and keeps the digest it was stored with -- or none, if it predates them.

    ``past_failure`` passes through to `_is_judge_verdict`: set it only where
    the judge has just failed and the question is what is already known, not
    whether to skip a call.
    """
    if not evaluation or not _is_judge_verdict(evaluation, past_failure=past_failure):
        return None
    if "llm_explanation" not in evaluation:
        return None
    stored_digest = evaluation.get(JUDGE_DIGEST_KEY)
    if reuse == "matching" and stored_digest != digest:
        return None
    try:
        score = float(evaluation["llm_score"])
    except (TypeError, ValueError):
        logger.warning("Invalid stored llm_score, will re-run LLM judge")
        return None
    verdict = {"llm_score": score, "llm_explanation": evaluation["llm_explanation"]}
    if stored_digest is not None:
        verdict[JUDGE_DIGEST_KEY] = stored_digest
    return verdict


def _llm_judge_verdict(
    record: Dict[str, Any],
    prediction: Dict[str, Any],
    gold_sql: str,
    gold_df: Any,
    subset_match: int,
    pred_df: Any,
    llm_judge_config: Dict[str, Any],
    force_rerun_llm_judge: bool,
    llm_judge_reuse: str,
) -> Dict[str, Any]:
    """
    The judge's fields for one prediction, compared with one ground truth.

    Where the outcome does not need the judge -- no result to judge, or a result
    that already matches -- the score is decided without it and carries no
    digest, since no config gave it.
    """
    if pred_df is None:
        return {
            "llm_score": 0.0,
            "llm_explanation": f"{_NOT_JUDGED} due to missing prediction dataframe)",
        }
    if subset_match:
        return {
            "llm_score": 1.0,
            "llm_explanation": f"{_NOT_JUDGED} due to subset match)",
        }
    digest = judge_config_digest(llm_judge_config)
    if not force_rerun_llm_judge:
        stored = _stored_verdict(prediction.get("evaluation"), digest, llm_judge_reuse)
        if stored is not None:
            return stored
    # Built in one place so a judge calibration run sends the judge exactly
    # what this does.
    response = evaluate_sql_prediction_with_llm(
        llm_judge_config=llm_judge_config,
        **build_llm_judge_inputs(record, prediction, gold_sql, gold_df, pred_df),
    )
    return {
        "llm_score": float(response["score"]),
        "llm_explanation": response["explanation"],
        JUDGE_DIGEST_KEY: digest,
    }


def evaluate_prediction(
    record,
    prediction,
    llm_judge_config=None,
    force_rerun_llm_judge=False,
    llm_judge_reuse="matching",
):
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
        If True, the judge is called even where a stored verdict could be reused.

    llm_judge_reuse : {"matching", "any"}, optional
        Which stored verdicts may be reused instead of calling the judge.
        "matching" (the default) reuses only a verdict recorded under the same
        judge config -- model, parameters and prompt -- so evaluating with a
        different config judges again. "any" reuses a stored verdict whichever
        config gave it, and keeps the digest it was recorded with.

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
            - "llm_judge_config_digest" (optional): str
                The digest of the judge config that gave the verdict, present only when the judge
                was actually asked
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
    - LLM judge caching: a verdict the judge gives is stored with "llm_judge_config_digest". Evaluating
      again reuses it only under the same config (see llm_judge_reuse), so a score is never reported
      under a judge that did not give it; verdicts stored before 1.6.0 carry no digest and are judged
      again. The judge is asked once per prediction, about the ground truth that decided the result.
    """
    if llm_judge_reuse not in LLM_JUDGE_REUSE_MODES:
        raise ValueError(
            f"llm_judge_reuse must be one of {LLM_JUDGE_REUSE_MODES}, "
            f"not {llm_judge_reuse!r}"
        )
    result = {}

    # Check for inference error - skip evaluation if inference failed
    if "inference_error" in prediction:
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
        result["df_error_message"] = prediction.get("sql_execution_error", str(e))

    try:
        gold_sqls = get_gt_sqls(record)
        gold_dfs = record["gt_df"]

        if not isinstance(gold_dfs, list):
            gold_dfs = [gold_dfs]

        if gold_sqls and not gold_dfs:
            # A ground-truth SQL with no executed dataframe cannot be compared
            # against. Returning silently left the result as `{"df_error": 0}`
            # with no metrics and no error flag, which then raised KeyError in
            # compute_summary and took out the whole benchmark's summary.
            raise ValueError(
                "No ground-truth dataframe to evaluate against; run the "
                "execution stage for this record first."
            )

        decided_by = None
        matched_gold = None
        # strict=False deliberately: some records carry more ground-truth SQLs
        # than dataframes, and truncating is the behaviour the published
        # results were produced under.
        for gold_sql, gold_df_raw in zip(gold_sqls, gold_dfs, strict=False):
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
                sqlglot_parsed_queries_equivalent(predicted_sql, gold_sql)
                if is_glot_parsable
                else 0
            )
            sqlglot_optimized_equivalence_score = (
                sqlglot_optimized_equivalence(predicted_sql, gold_sql)
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

            # The judge is asked once, after the loop, about the ground truth
            # that decided the result. Asking inside the loop called it once
            # per ground truth and kept only the last answer.
            decided_by = (gold_sql, gold_df, subset_match)
            if result["subset_non_empty_execution_accuracy"] == 1:
                matched_gold = (gold_sql, gold_df_raw)
                break

        if llm_judge_config and decided_by is not None:
            gold_sql, gold_df, subset_match = decided_by
            try:
                result.update(
                    _llm_judge_verdict(
                        record,
                        prediction,
                        gold_sql,
                        gold_df,
                        subset_match,
                        pred_df,
                        llm_judge_config,
                        force_rerun_llm_judge,
                        llm_judge_reuse,
                    )
                )
            except Exception as e:
                logger.error(f"LLM judge error: {repr(e)}")
                result["llm_judge_error"] = repr(e)
                # A failed call must not erase what is already known. Re-judging
                # under a new config declines to reuse the stored verdict, and
                # without this a call the provider refused left the prediction
                # with no llm_score at all -- which `compute_summary` averages
                # as 0. A run interrupted by a rate limit then published scores
                # far below the ones the judge had actually given. The verdict
                # kept here keeps the digest of the config that gave it, so it
                # is still not reported as this config's work.
                #
                # past_failure, because the stored evaluation may itself be the
                # output of a run that failed -- which is exactly what the
                # warning above tells the operator to retry. Without it the
                # second refusal in a row dropped the verdict the first one had
                # kept, and the data loss this exists to prevent came back on
                # the retry.
                kept = _stored_verdict(
                    prediction.get("evaluation"), "", "any", past_failure=True
                )
                if kept is not None:
                    result.update(kept)

        if matched_gold is not None:
            result["gt_sql"], result["gt_df"] = matched_gold

    except Exception as e:
        result["eval_error"] = 1
        result["eval_error_message"] = repr(e)
        # raise e

    return result


def split_summary(summary):
    """
    Separate a summary's pipeline entries from its judge-config entry.

    [`compute_summary`][text2sql_eval_toolkit.compute_summary] returns a mapping keyed by ``pipeline_id``, with one
    exception: ``"llm_judge_config"`` entry recording which judge produced
    the verdicts. Every consumer therefore has to know to skip that key, and one
    that does not silently treats it as a pipeline.

    The shape itself is not changed, because it is the published artifact format:
    every ``*_eval_summary.json`` on the Hub carries that key, and re-shaping it
    would stop existing snapshots being readable. This is the supported way to
    take it apart.

    Args:
        summary (dict): A mapping from [`compute_summary`][text2sql_eval_toolkit.compute_summary], or the parsed
            contents of a ``*_eval_summary.json`` file.

    Returns:
        tuple[dict, dict | None]: The pipeline entries keyed by ``pipeline_id``,
        and the judge config (``None`` when the judge did not run).

    Example:
        ```python
        >>> pipelines, judge = split_summary(summary)
        >>> sorted(pipelines)
        ['modelA-greedy-zero-shot-chatapi', 'modelB-greedy-zero-shot-chatapi']
        ```
    """
    if not isinstance(summary, dict):
        return {}, None
    judge_config = summary.get(JUDGE_CONFIG_KEY)
    pipelines = {k: v for k, v in summary.items() if k != JUDGE_CONFIG_KEY}
    return pipelines, judge_config


def compute_summary(metrics_by_model, llm_judge_config, token_usage_by_model=None):
    """
    Aggregate per-record metrics into a per-pipeline summary.

    Averages are taken over records that could be evaluated, so a benchmark where
    some records failed to execute still yields a meaningful score rather than
    one dragged toward zero by errors. Failure counts are reported alongside, and
    should be read together with the averages -- a high score over few evaluated
    records is not the same claim as a high score over all of them.

    Note:
        The returned mapping carries a ``"llm_judge_config"`` key alongside the
        pipeline ids, recording which judge produced the verdicts. Consumers
        iterating pipelines must skip it -- [`print_summary`][text2sql_eval_toolkit.print_summary] and
        [`summary_to_df_csv`][text2sql_eval_toolkit.summary_to_df_csv] both do.

    Args:
        metrics_by_model (dict): Per-record metric dicts, keyed by ``pipeline_id``.
        llm_judge_config (dict | None): The judge config used. Recorded in the
            result so a summary says which judge produced its verdicts.
        token_usage_by_model (dict | None): Token counts keyed by ``pipeline_id``,
            folded into the summary when present.

    Returns:
        dict: ``pipeline_id`` to metrics, plus the ``"llm_judge_config"`` entry.
    """
    summary = {}
    for model, records in metrics_by_model.items():
        num_records = len(records)
        num_eval_errors = sum(1 for r in records if "eval_error_message" in r)
        num_df_errors = sum(1 for r in records if "df_error_message" in r)
        # Count records with inference errors (failed to generate SQL)
        num_inference_errors = sum(
            1
            for r in records
            if "df_error_message" in r
            and "Inference failed" in (r.get("df_error_message") or "")
        )
        # Count records with successful predictions (SQL was generated)
        num_predictions = num_records - num_inference_errors
        num_evaluated = num_records - num_eval_errors
        # .get(..., 0) rather than subscripting: a record that could not be
        # evaluated may be missing metrics entirely, and one such record used to
        # raise KeyError here and abort the summary for the whole benchmark.
        # Counting it as 0 matches the stated intent below.
        num_correct_non_empty_execution_accuracy = sum(
            r.get("non_empty_execution_accuracy", 0)
            for r in records
            if "eval_error_message" not in r
        )
        num_correct_subset_non_empty_execution_accuracy = sum(
            r.get("subset_non_empty_execution_accuracy", 0)
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
                    JUDGE_DIGEST_KEY,
                    "gt_sql",
                    "gt_df",
                ]:
                    # For accuracy metrics, divide by num_records (not num_evaluated)
                    # This penalizes pipelines that fail to generate predictions
                    metric_sum = df[metric].sum()
                    metric_stats[metric] = {
                        "average": metric_sum
                        / num_records,  # Changed from df[metric].mean()
                        "stddev": df[metric].std(),
                    }

            # Token metrics are automatically calculated by pandas from the evaluation records
            # The statistics (average, stddev) are already in metric_stats from lines above
            # We just need to add the total sums as separate count metrics
            if "total_tokens" in df.columns:
                metric_stats["sum_total_tokens"] = int(df["total_tokens"].sum())
                metric_stats["sum_prompt_tokens"] = int(df["prompt_tokens"].sum())
                metric_stats["sum_completion_tokens"] = int(
                    df["completion_tokens"].sum()
                )

            # Timing metrics - add total sums
            if "inference_time_ms" in df.columns:
                metric_stats["sum_inference_time_ms"] = round(
                    df["inference_time_ms"].sum(), 2
                )
            if "execution_time_ms" in df.columns:
                metric_stats["sum_execution_time_ms"] = round(
                    df["execution_time_ms"].sum(), 2
                )
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
            # Counted from the score, not from the absence of an error: a call
            # that failed can still carry the verdict a previous run stored,
            # and that verdict is in the `llm_score` average beside this count.
            # Requiring no error made the two disagree. A record whose judge
            # failed with nothing stored has no score and is counted by neither.
            metric_stats["num_correct_llm"] = sum(
                1
                for r in records
                if "eval_error_message" not in r and r.get("llm_score") == 1
            )
            metric_stats["num_llm_judge_errors"] = sum(
                1 for r in records if "llm_judge_error" in r
            )
            if "llm_judge_config" not in summary:
                summary["llm_judge_config"] = llm_judge_config

        summary[model] = metric_stats

    return summary


def summary_to_df_csv(summary, output_path, use_llm):
    """
    Render a summary as a DataFrame and write it to CSV.

    The ``"llm_judge_config"`` entry is skipped, so each row is one pipeline.

    Args:
        summary (dict): A mapping from [`compute_summary`][text2sql_eval_toolkit.compute_summary].
        output_path (str | Path): Where to write the CSV. Written with ``index=False``.
        use_llm (bool): Whether to include LLM-judge columns. When ``False`` the judge
            columns are filled with ``"N/A"`` rather than omitted, so the column
            set is stable across runs with and without the judge.

    Returns:
        pandas.DataFrame: The same table that was written, for callers that want
        it in memory as well as on disk.
    """
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
    """
    Print a summary to stdout in a human-readable form.

    For terminal use; [`summary_to_df_csv`][text2sql_eval_toolkit.summary_to_df_csv] is the machine-readable
    equivalent. The ``"llm_judge_config"`` entry is skipped.

    Args:
        summary (dict): A mapping from [`compute_summary`][text2sql_eval_toolkit.compute_summary].
        use_llm (bool): Whether to include LLM-judge columns. Pass ``False`` when
            the judge did not run, or its rows will read as zeros rather than as
            absent.

    Returns:
        None: Output goes to stdout.
    """
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
            print("  Token Usage Metrics:")
            print(
                f"    Total Tokens                 : {metrics.get('sum_total_tokens', 0):,}"
            )
            total_tokens_stats = metrics.get("total_tokens", {})
            if isinstance(total_tokens_stats, dict):
                avg_val = total_tokens_stats.get("average", 0)
            else:
                avg_val = 0
            print(f"    Avg Tokens per Question      : {avg_val:.2f}")
            print(
                f"    Total Prompt Tokens          : {metrics.get('sum_prompt_tokens', 0):,}"
            )
            print(
                f"    Total Completion Tokens      : {metrics.get('sum_completion_tokens', 0):,}"
            )

        # Print timing metrics if available
        if "sum_inference_time_ms" in metrics or "sum_execution_time_ms" in metrics:
            print("  Performance Metrics:")
            if "sum_inference_time_ms" in metrics:
                inference_stats = metrics.get("inference_time_ms", {})
                if isinstance(inference_stats, dict):
                    avg_inference = inference_stats.get("average", 0)
                else:
                    avg_inference = 0
                print(
                    f"    Total Inference Time         : {metrics.get('sum_inference_time_ms', 0):,.2f} ms"
                )
                print(f"    Avg Inference Time per Query : {avg_inference:.2f} ms")

            if "sum_execution_time_ms" in metrics:
                execution_stats = metrics.get("execution_time_ms", {})
                if isinstance(execution_stats, dict):
                    avg_execution = execution_stats.get("average", 0)
                else:
                    avg_execution = 0
                print(
                    f"    Total Execution Time         : {metrics.get('sum_execution_time_ms', 0):,.2f} ms"
                )
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
    input_file: str,
    output_file: str = None,
    summary_file: str = None,
    csv_summary_file: str = None,
    llm_judge_config: dict = None,
    max_concurrency: int = 16,
    force_rerun_llm_judge: bool = False,
    force_rerun: bool = False,
    llm_judge_reuse: str = "matching",
):
    """
    Evaluate a predictions file, awaitable.

    What [`evaluate_predictions`][text2sql_eval_toolkit.evaluate_predictions] wraps. Await this from code that already
    runs an event loop; the synchronous wrapper would raise there.

    Records are evaluated concurrently behind a semaphore of *max_concurrency*.
    Raising it increases pressure on whatever the judge model's endpoint will
    tolerate, not on local CPU.

    Args:
        input_file: Path to a predictions JSON file.
        output_file: Evaluation artifact path. Defaults to *input_file* with
            ``_eval`` inserted before the extension.
        summary_file: JSON summary path. Defaults to the output path with
            ``_summary.json``.
        csv_summary_file: CSV summary path. Defaults to the output path with
            ``_summary.csv``.
        llm_judge_config: An already-loaded judge config, as returned by
            [`load_llm_judge_config`][text2sql_eval_toolkit.load_llm_judge_config]. ``None`` skips the judge. Note that
            this takes the config itself, where the synchronous wrapper takes a
            path.
        max_concurrency: Records evaluated at once.
        force_rerun_llm_judge: Call the judge even where a stored verdict could
            be reused.
        force_rerun: Re-evaluate everything, ignoring stored results.
        llm_judge_reuse: ``"matching"`` (the default) reuses a stored verdict
            only if the same judge config gave it; ``"any"`` keeps a stored
            verdict whichever config gave it. See
            [`evaluate_prediction`][text2sql_eval_toolkit.evaluate_prediction].

    Returns:
        tuple[dict, pandas.DataFrame]: The full evaluation data, and the
        per-pipeline summary table.
    """
    if llm_judge_reuse not in LLM_JUDGE_REUSE_MODES:
        raise ValueError(
            f"llm_judge_reuse must be one of {LLM_JUDGE_REUSE_MODES}, "
            f"not {llm_judge_reuse!r}"
        )
    output_file = output_file or get_default_eval_filename(input_file)
    summary_file = summary_file or add_summary_json_suffix(output_file)
    csv_summary_file = csv_summary_file or add_summary_csv_suffix(output_file)

    semaphore = asyncio.Semaphore(max_concurrency)

    async def worker(record, prediction, llm_judge_config, force_rerun_llm_judge):
        async with semaphore:
            return await asyncio.to_thread(
                evaluate_prediction,
                record,
                prediction,
                llm_judge_config,
                force_rerun_llm_judge,
                llm_judge_reuse,
            )

    with open(input_file, "r") as f:
        data = json.load(f)

    # Load existing evaluations from output file if it exists (for caching)
    existing_evaluations = {}
    if not force_rerun and Path(output_file).exists():
        try:
            with open(output_file, "r") as f:
                existing_data = json.load(f)
                for record in existing_data:
                    record_id = record.get("id") or record.get("question_id")
                    if record_id:
                        existing_evaluations[record_id] = record.get("predictions", {})
        except Exception as e:
            logger.warning(
                f"Could not load existing evaluations from {output_file}: {e}"
            )

    # Copy existing evaluations to predictions for caching
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

        # Collect token usage from prediction
        token_usage = prediction.get("token_usage")
        if token_usage:
            token_usage_by_model[model_name].append(token_usage)

    if llm_judge_config:
        judge_errors = sum(1 for e in evaluations if "llm_judge_error" in e)
        if judge_errors:
            # Loud, because the summary written below counts a prediction with
            # no verdict as 0: a run the provider refused half of looks like a
            # much worse pipeline rather than a run to repeat.
            logger.warning(
                f"{judge_errors} LLM judge calls failed. Where a stored verdict "
                "existed it was kept, with the digest of the config that gave "
                "it; the rest have no llm_score, which a summary counts as 0. "
                "Run again to judge them."
            )

        # Reached two ways: llm_judge_reuse="any", and a failed call that kept
        # what an earlier config had stored -- which happens under the default
        # "matching" too, so past_failure here. Said out loud either way,
        # because the summary records the current config regardless.
        digest = judge_config_digest(llm_judge_config)
        foreign = sum(
            1
            for evaluation in evaluations
            if _is_judge_verdict(evaluation, past_failure=True)
            and evaluation.get(JUDGE_DIGEST_KEY) != digest
        )
        if foreign:
            logger.warning(
                f"{foreign} LLM judge verdicts were kept from a different judge "
                "config; the summary records the current config."
            )

    summary = compute_summary(metrics_by_model, llm_judge_config, token_usage_by_model)

    with open(output_file, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    with open(summary_file, "w") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    use_llm = True if llm_judge_config is not None else False
    summary_df = summary_to_df_csv(summary, csv_summary_file, use_llm)
    print_summary(summary, use_llm)

    return data, summary_df


def _run_coroutine(coro):
    """
    Run *coro* to completion from synchronous code, loop or no loop.

    ``asyncio.run`` raises when a loop is already running in this thread, which
    made the synchronous entry points unusable from notebooks and from any async
    server. Handing the coroutine to a worker thread with its own loop keeps one
    synchronous signature that works in both settings.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    result: Dict[str, Any] = {}

    def _worker() -> None:
        try:
            result["value"] = asyncio.run(coro)
        except BaseException as exc:  # re-raised on the calling thread below
            result["error"] = exc

    thread = threading.Thread(target=_worker, name="text2sql-eval", daemon=True)
    thread.start()
    thread.join()
    if "error" in result:
        raise result["error"]
    return result["value"]


def evaluate_predictions(
    input_file: str,
    output_file: str = None,
    summary_file: str = None,
    csv_summary_file: str = None,
    use_llm: bool = False,
    llm_judge_config_path: str = None,
    force_rerun_llm_judge: bool = False,
    force_rerun: bool = False,
):
    """
    Evaluate a predictions file and write the evaluation and summary artifacts.

    The synchronous entry point, and the one most callers want.

    Safe to call from inside a running event loop: it detects one and runs the
    work on a worker thread with its own loop. Async callers should still prefer
    awaiting [`async_evaluate_predictions`][text2sql_eval_toolkit.async_evaluate_predictions] directly, which avoids the extra
    thread.

    Evaluation is resumable: records that already carry results are left alone
    unless *force_rerun* is set.

    Args:
        input_file: Path to a predictions JSON file.
        output_file: Where to write the evaluation artifact. Defaults to the
            input path with ``_eval`` inserted before the extension.
        summary_file: Where to write the JSON summary. Defaults to the output
            path with ``_summary.json``.
        csv_summary_file: Where to write the CSV summary. Defaults to the output
            path with ``_summary.csv``.
        use_llm: Also run LLM-as-judge.
        llm_judge_config_path: Path to a judge config YAML. Passing this loads a
            judge config even when *use_llm* is ``False``.
        force_rerun_llm_judge: Re-run the judge for records that already have a
            verdict.
        force_rerun: Re-evaluate everything, ignoring stored results.

    Returns:
        tuple[dict, pandas.DataFrame]: The full evaluation data, and the
        per-pipeline summary table.

    Example:
        ```python
        >>> data, summary_df = evaluate_predictions(
        ...     "data/results/my-benchmark-predictions.json"
        ... )
        >>> summary_df.head()
        ```
    """
    llm_judge_config = None
    if use_llm or llm_judge_config_path is not None:
        llm_judge_config = load_llm_judge_config(llm_judge_config_path)
    return _run_coroutine(
        async_evaluate_predictions(
            input_file,
            output_file,
            summary_file,
            csv_summary_file,
            llm_judge_config,
            force_rerun_llm_judge=force_rerun_llm_judge,
            force_rerun=force_rerun,
        )
    )


# For running from script
def run_evaluation(
    benchmark_id: str,
    use_llm: bool = False,
    llm_judge_config_path: str = None,
    force_rerun_llm_judge: bool = False,
    force_rerun: bool = False,
):
    """
    Evaluate a registered benchmark's predictions.

    The registry-aware entry point: it resolves the predictions path for
    *benchmark_id* and hands off to [`evaluate_predictions`][text2sql_eval_toolkit.evaluate_predictions]. Use that one
    directly to evaluate a file that is not part of a registered benchmark.

    Args:
        benchmark_id: A benchmark from ``benchmarks.json`` or
            ``test-benchmarks.json``. See [`get_available_benchmarks`][text2sql_eval_toolkit.get_available_benchmarks].
        use_llm: Also run LLM-as-judge. Requires credentials for the model named
            in the judge config.
        llm_judge_config_path: Path to a judge config YAML. Passing this loads a
            judge config even when *use_llm* is ``False``.
        force_rerun_llm_judge: Re-run the judge for records that already carry a
            verdict.
        force_rerun: Re-evaluate everything, ignoring stored results. Implies
            *force_rerun_llm_judge*.

    Returns:
        tuple[dict, pandas.DataFrame]: The full evaluation data, and the
        per-pipeline summary table.

    Raises:
        ValueError: If *benchmark_id* is in neither registry.

    Example:
        ```python
        >>> data, summary_df = run_evaluation("bird_mini_dev_sqlite")
        >>> summary_df[["subset_non_empty_execution_accuracy_avg"]]
        ```
    """
    benchmark_info = get_benchmark_info(benchmark_id)
    predictions_path = str(Path(benchmark_info["predictions_path"]))
    return evaluate_predictions(
        predictions_path,
        use_llm=use_llm,
        llm_judge_config_path=llm_judge_config_path,
        force_rerun_llm_judge=force_rerun_llm_judge or force_rerun,
        force_rerun=force_rerun,
    )
