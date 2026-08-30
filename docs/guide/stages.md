# The five stages

Each stage is usable on its own and each is resumable. They communicate through
one JSON file per benchmark, described in [Data model](data-model.md).

## Inference

An LLM turns each question into SQL. Two pipelines ship:

| Class | What it does |
|---|---|
| [`LLMSQLGenerationPipeline`][text2sql_eval_toolkit.LLMSQLGenerationPipeline] | One prompt, one answer — the greedy zero-shot baseline |
| [`AgenticSQLGenerationPipeline`][text2sql_eval_toolkit.AgenticSQLGenerationPipeline] | A LangGraph agent that inspects the schema, runs candidate queries and retries |
| [`LLMSQLGenerationPipelineSimple`][text2sql_eval_toolkit.LLMSQLGenerationPipelineSimple] | A minimal variant, useful as a template |

Predictions are keyed by `pipeline_id`, so several models accumulate in the same
file and can be compared afterwards without re-running any of them.

Failed inferences are retried automatically on the next run; successful ones are
left alone unless you force a re-run.

## Execution

Both the ground-truth and the predicted statements are run, and their result
sets are stored in the predictions file as serialized dataframes.

This is what lets evaluation happen anywhere. Once results are stored, scoring
needs no database at all — which is why the public dashboard can run
execution-match metrics on a host that holds no database credentials.

Execution is asyncio with a bounded semaphore (default concurrency 16) and a
per-query timeout, so one pathological query cannot stall a run.

## Evaluation

Scoring a prediction against its ground truth, by three different kinds of
check:

- **Execution match** — compare the result sets. Exact, subset, and BIRD-style
  comparisons are all available, because benchmarks disagree about what counts
  as a match. Column names are compared insensitively; the values are what
  matter.
- **SQL equivalence** — compare the statements without running them, via SQLGlot
  optimization, parse-tree comparison and normalized string match.
- **LLM-as-judge** — ask a model whether the prediction answers the question.
  See [LLM-as-judge](llm-judge.md).

The entry points are [`evaluate_prediction`][text2sql_eval_toolkit.evaluate_prediction]
for a single prediction, [`evaluate_predictions`][text2sql_eval_toolkit.evaluate_predictions]
for a whole file, and
[`async_evaluate_predictions`][text2sql_eval_toolkit.async_evaluate_predictions]
for the concurrent version.

Multiple ground truths are supported throughout: a question with several correct
formulations scores as correct if the prediction matches any of them.

## Profiling

Tags each statement with the SQL features it uses — joins, aggregation, nested
queries, set operations and so on.

This exists for slicing. "68% accuracy" says much less than "94% on single-table
selects, 31% once a correlated subquery is required", and the second statement
is the one that tells you what to fix.

## Analysis

Turns the accumulated file into something readable: Markdown summary reports,
charts, and the dashboard.

Summaries are written as `.json`, `.csv` and `.md` beside the evaluation file.
[`compute_summary`][text2sql_eval_toolkit.compute_summary] builds the per-pipeline
numbers and [`print_summary`][text2sql_eval_toolkit.print_summary] renders them.

Error analysis is a first-class part of this stage rather than an afterthought,
and it is where the dashboard earns its place: filtering to the questions one
pipeline got right and another got wrong is a query, not a spreadsheet.
