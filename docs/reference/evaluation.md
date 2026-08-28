# Evaluation

Scoring predictions. Three entry points at different levels: a whole registered
benchmark, a predictions file, or a single record in memory.

::: text2sql_eval_toolkit.run_evaluation
::: text2sql_eval_toolkit.evaluate_predictions
::: text2sql_eval_toolkit.async_evaluate_predictions
::: text2sql_eval_toolkit.evaluate_prediction

## LLM-as-judge

Complements the execution metrics for questions where more than one query is
defensible. Needs no database — everything it reads is already in the
evaluation artifacts.

::: text2sql_eval_toolkit.load_llm_judge_config
::: text2sql_eval_toolkit.evaluate_sql_prediction_with_llm

## Summaries

::: text2sql_eval_toolkit.compute_summary
::: text2sql_eval_toolkit.split_summary
::: text2sql_eval_toolkit.summary_to_df_csv
::: text2sql_eval_toolkit.print_summary
