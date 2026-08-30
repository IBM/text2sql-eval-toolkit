# LLM-as-judge

Execution match answers "did these two result sets agree". It cannot answer "is
this a reasonable query for that question" — and when a question admits several
defensible answers, or the ground truth is itself arguable, that second question
is the one worth asking.

The judge complements the execution metrics rather than replacing them.

## Running it

```python
from text2sql_eval_toolkit import (
    load_llm_judge_config,
    evaluate_sql_prediction_with_llm,
)

config = load_llm_judge_config()          # the packaged default
result = evaluate_sql_prediction_with_llm(
    question="How many singers are there?",
    ground_truth_sql="SELECT count(*) FROM singer",
    ground_truth_df=gt_df,
    predicted_sql="SELECT COUNT(*) AS n FROM singer",
    predicted_df=pred_df,
    generation_prompt="",
    llm_judge_config=config,
)
result["verdict"], result["score"], result["explanation"]
```

To run it as part of a whole evaluation, pass `llm_judge_config_path` to
[`run_evaluation`][text2sql_eval_toolkit.run_evaluation], or an already-loaded
config as `llm_judge_config` to
[`evaluate_predictions`][text2sql_eval_toolkit.evaluate_predictions].

## Configs

A judge config is YAML: a model id and a prompt template.

```yaml
model:
  id: anthropic:claude-sonnet-4-5
  max_tokens: 1000
  temperature: 0
prompt_template: |
  Question: {question}
  Ground truth SQL: {ground_truth_sql}
  Predicted SQL: {predicted_sql}
  Answer Yes or No, then explain.
```

Keys under `model` other than `id` are passed through as generation parameters.
The model id follows the usual `provider:model` form — see
[Models and providers](models.md) — so any supported provider can judge.

Four configs ship with the package, differing in how much they are told and
whether they see the ground truth at all. Judging *without* the ground truth is
a genuinely different measurement: it asks whether the query answers the
question, rather than whether it matches someone else's answer.

## Editing configs

The dashboard has an editor for them. Configs written through it land in
`<data root>/llm_judge_config/` and **shadow** the packaged config of the same
name; deleting the copy restores the original. Nothing writes into the installed
package, which would not survive a `pip install --upgrade` even where the
permissions allow it.

## Cost

Judging costs money on every call, which shapes the design:

- Verdicts are **cached** against the record, the pipeline, the config name and
  a digest of the config's *contents*. Re-running a judge whose prompt has not
  changed costs nothing. Change the prompt and the digest changes, which is
  correct — it is a different question.
- The dashboard meters spend against a monthly ceiling that survives restarts.
- A shared link to a verdict only ever reads the cache. Opening a link someone
  sent you never starts an inference.
