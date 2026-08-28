# Utilities

## Reading records

Benchmarks disagree on field names, so these accept the common spellings.

!!! warning
    `get_question_id`, `get_utterance` and `get_gt_sqls` **mutate the record**
    they are given, normalising the key they matched. Pass a copy if the original
    must stay untouched.

::: text2sql_eval_toolkit.get_question_id
::: text2sql_eval_toolkit.get_utterance
::: text2sql_eval_toolkit.get_question
::: text2sql_eval_toolkit.get_gt_sqls

Reading is side-effect free. To write the canonical `id`, `utterance` and `sql`
keys onto a record before storing it — which is what inference does before
appending to the predictions file — normalise explicitly:

::: text2sql_eval_toolkit.normalize_record

## Dataframes

Result sets are stored as pandas `orient="split"` JSON.

::: text2sql_eval_toolkit.parse_dataframe
::: text2sql_eval_toolkit.truncate_dataframe

## Paths

::: text2sql_eval_toolkit.get_default_eval_filename
::: text2sql_eval_toolkit.add_summary_json_suffix
::: text2sql_eval_toolkit.add_summary_csv_suffix

## Timeouts

::: text2sql_eval_toolkit.run_with_timeout
::: text2sql_eval_toolkit.run_with_timeout_async
