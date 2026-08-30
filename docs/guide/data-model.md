# Data model

Everything the toolkit produces lives in **one JSON file per benchmark**, which
accumulates as each stage runs.

```
data/results/{benchmark}-predictions.json          # records + predictions + result sets
data/results/{benchmark}-predictions_eval.json     # the above, plus an evaluation block
data/results/{benchmark}-predictions_eval_summary.{json,csv,md}
```

This is the reason the stages compose. Any stage can run against a file another
stage produced — by someone else, on another machine — because the file carries
everything the next stage needs.

## A record

A record is one benchmark question. The fields the toolkit reads:

| Field | Meaning |
|---|---|
| `question` (or `utterance`, `page_content`) | The natural-language question |
| `sql` | Ground-truth SQL. A list: several formulations may be correct |
| `gt_df` | Ground-truth result sets, once execution has run |
| `db_id` | Which database in the benchmark to run against |
| `predictions` | A map of `pipeline_id` to prediction |

Field naming varies between benchmarks, which is why
[`normalize_record`][text2sql_eval_toolkit.normalize_record],
[`get_question`][text2sql_eval_toolkit.get_question] and
[`get_gt_sqls`][text2sql_eval_toolkit.get_gt_sqls] exist — use them rather than
indexing the dictionary directly, and a benchmark that spells things differently
keeps working.

## A prediction

Each entry under `predictions` holds what one pipeline produced for that
question:

`predicted_sql`, `prompt`, `model_name`, `model_parameters`, `token_usage`,
`inference_time_ms`, `predicted_df`, and — after evaluation — `evaluation`.

## `pipeline_id`

The unit of comparison everywhere: the dashboard, the summaries, the error
analysis. It is **derived, not configured**:

```
baseline:  {model_name}-greedy-zero-shot-chatapi
agentic:   {model_name}-agentic-baseline{0..5}-{max_attempts}attempts
```

So `wxai:openai/gpt-oss-120b-greedy-zero-shot-chatapi` names both the model and
how it was prompted. Being derived is what makes results from different runs
comparable without any bookkeeping — but it also means changing a model name
changes the id, and the old predictions stay under the old key.

## Dataframes

Result sets are stored as pandas JSON in `orient='split'` form. Read them with
[`parse_dataframe`][text2sql_eval_toolkit.parse_dataframe] rather than
`pd.read_json`, which will not round-trip the same way.

[`truncate_dataframe`][text2sql_eval_toolkit.truncate_dataframe] bounds a result
set before storing it — a query returning a million rows should not produce a
million-row artifact.

## Resume by default

Every stage reuses what is already in the file. Existing predictions are not
regenerated and existing evaluations are not recomputed unless you pass
`force_rerun=True` (or `--force_rerun` on the scripts). Failed inferences are the
exception: they are retried automatically, since a failure is not a result.

This is a cost decision as much as a convenience one. A run over a few thousand
questions is measured in dollars, and an interruption at question 1,900 should
not repeat the first 1,899.
