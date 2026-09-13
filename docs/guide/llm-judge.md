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

## How a verdict is read

The prompt asks the model to begin its reply with `Yes`, `No` or `Maybe`, and
the verdict is read from the start of that reply — through whatever formatting
the model wrapped it in. `**Yes**`, `### Yes`, `> Yes`, `"Yes"` and
`Verdict: Yes` all read as `Yes`.

Only the *start* is examined. Scanning the whole reply would find the "No" in a
sentence like "No ground-truth SQL was available" and read a rejection out of an
explanation.

A reply whose head is not one of the three scores `N/A`, which carries the same
score as `No` — so `verdict`, not `score`, is the field to check when telling a
rejection apart from an unread reply. The reply itself is always returned as
`explanation`, including for `N/A`, so an unrecognised answer can be read rather
than guessed at.

!!! warning "Fixed in 1.5.0"
    The verdict previously had to be the literal first characters of the reply,
    so a model that emphasised it — Gemini 3 returns `**Yes**` — scored `N/A` on
    every record, and a whole benchmark run reported a plausible-looking 0%. The
    `explanation` was also discarded in that case, which made the failure hard
    to see. If you have judge results from before 1.5.0 produced by a model that
    formats its output, re-run them.

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

Two configs ship with the package, both using `wxai:openai/gpt-oss-120b`:

- **`llm_judge_default_config`** compares the prediction with the ground-truth
  query and its result. It is what `load_llm_judge_config()` loads.
- **`llm_judge_no_gt`** sees the question, the context the prediction was
  generated from, and the prediction with its result — no ground truth. That is
  a genuinely different measurement: it asks whether the query answers the
  question, rather than whether it matches someone else's answer. It is also a
  much harder one, and this judge accepts wrong queries several times as often
  as the default does. Use it where there is no ground truth, not in place of
  one.

Until 1.6.0 there were four, using Llama 3.3 70B and Llama 4 Maverick. Their
names still load: `llm_judge_alt_config` loads the default, and
`llm_judge_no_gt_v1` and `llm_judge_no_gt_v2` load `llm_judge_no_gt` — unless
you have a config of your own by that name, which is used as written.

gpt-oss-120b reasons before it answers, and the reasoning counts against
`max_new_tokens`. At the old configs' 512 it often used the whole budget before
giving a verdict, so the packaged configs allow 8192, and a reply with no
answer in it is an error rather than a verdict.

### What the judge is shown

The question; the context the prediction was generated from; for the default,
the ground-truth SQL and its result; and the predicted SQL and its result.
Result tables are cut to their first and last ten rows. For a baseline
pipeline the context is its generation prompt. For an agentic pipeline it is
the agent's trace: the task the agent was given — schema, hints and question —
in full, and each later message and response cut to 500 characters.

### How well they judge

Both configs were chosen against a labelled set of the predictions the batch
judge is actually asked about — execution mismatches from BIRD, Spider, Spider
Realistic and Archer — kept with the scoring script in the repository under
`data/judge_calibration/`. On its holdout, 52 items labelled before any judge
saw them:

| Config | Mean abs. error | Wrong accepted | Correct rejected |
| --- | --- | --- | --- |
| `llm_judge_default_config` (gpt-oss-120b) | 0.183 | 2 of 28 | 3 of 14 |
| the Llama 3.3 70B config it replaced | 0.269 | 7 of 28 | 2 of 14 |
| `llm_judge_no_gt` (gpt-oss-120b) | 0.250 | 7 of 28 | 1 of 14 |
| the Llama 3.3 70B config it replaced | 0.423 | 15 of 28 | 2 of 14 |

Fifty-two items is a small sample, and the labels were written by the
assistant that developed the prompts. Read the numbers as a comparison between
configs on the same items, not as a precise error rate.

### Stored verdicts

A verdict the judge gives is stored with `llm_judge_config_digest`, a digest of
the config — model, parameters and prompt — that gave it. Evaluating again
reuses a stored verdict only under the same digest, so changing the judge
judges again, and a score is never reported under a judge that did not give it.
Verdicts stored before 1.6.0 carry no digest and are judged again.

- `force_rerun_llm_judge` calls the judge even where the digest matches.
- `llm_judge_reuse="any"` — `rerun_metrics.py --preserve-llm-judge` — keeps
  stored verdicts whichever config gave them, each with the digest it was
  stored with, and the run warns that the summary records the current config.
- A score decided without the judge — 1 for a result that already matches, 0
  for a prediction with no result — carries no digest and is worked out afresh
  each time.
- A prediction with several ground-truth queries is judged once, about the
  query that decided its result.

!!! warning "Changed in 1.6.0"
    Until 1.6.0 the batch judge reused any stored verdict. Evaluating
    Llama-judged results with a different config kept every Llama score and
    recorded the new config in the summary.

## Editing configs

The dashboard has an editor for them, which edits the YAML directly — with
syntax highlighting, bracket matching, a **Format** action that reflows the
document, and a marker on the line and column of a syntax error rather than a
message saying only that there is one. It presented JSON until 1.5.0, which
meant the prompt template — the bulk of every config, and always multi-line —
appeared as one very long line with `\n` escapes through it.

Configs written through it land in `<data root>/llm_judge_config/` and
**shadow** the packaged config of the same name; deleting the copy restores the
original. Nothing writes into the installed package, which would not survive a
`pip install --upgrade` even where the permissions allow it.

**Duplicate** keeps the open document and offers to save it under a new name,
which is how you start from an existing judge rather than an empty editor.
**Rename** moves one of your own configs; a packaged config has no file of
yours to move, so it is duplicated instead, and renaming onto a name already in
use is refused rather than overwriting it.

A save is refused when `model.id` or `prompt_template` is missing. Highlighting
makes malformed YAML visible; it does nothing about a config that parses cleanly
and describes a useless judge.

Comments are not preserved. The server parses the file and writes it back out,
which has always been true; what changed in 1.5.0 is that it writes multi-line
strings as block scalars again, so a saved file still opens with
`prompt_template: |` rather than a folded quoted string.

## Cost

Judging costs money on every call, which shapes the design:

- Verdicts are **cached** against the record, the pipeline, the config name and
  a digest of the config's *contents*. Re-running a judge whose prompt has not
  changed costs nothing. Change the prompt and the digest changes, which is
  correct — it is a different question.
- **Judge again** ignores the cache and asks the model afresh, replacing the
  stored verdict. The digest only covers the case where you changed something;
  this is for the case where the inputs are identical and the stored verdict is
  the thing you want rid of. It costs an inference like any other call, so the
  ceiling still applies.
- An `N/A` is not cached at all. It records that nobody could read the reply
  rather than a judgement, and caching it would make that permanent.
- The dashboard meters spend against a monthly ceiling that survives restarts.
  An `N/A` is still metered: the tokens were spent either way.
- A shared link to a verdict only ever reads the cache. Opening a link someone
  sent you never starts an inference.
