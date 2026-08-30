# Getting started

## Install

```bash
pip install "text2sql-eval-toolkit[dashboard]"
```

The base install carries the library and both command-line tools. Extras add
what a particular job needs:

| Extra | Adds |
|---|---|
| `dashboard` | The web UI and its FastAPI server |
| `mysql` | MySQL/MariaDB drivers |
| `presto` | Presto/Trino drivers |
| `litellm` | The `litellm:` model prefix, routing to anything LiteLLM supports |
| `notebook` | Jupyter and plotting for the analysis notebooks |
| `docs` | Building this documentation site |
| `dev` | Test and lint tooling |

Combine them as usual: `pip install "text2sql-eval-toolkit[dashboard,mysql]"`.

PostgreSQL and SQLite need no extra — their drivers are in the base install.

## Look at results without running anything

The fastest way to see what the toolkit produces is to fetch results that have
already been produced. Roughly 7 GB, from the Hugging Face Hub:

```bash
text2sql-eval-toolkit results fetch
text2sql-eval-dashboard --open-browser
```

That is enough for browsing benchmarks, comparing pipelines and doing error
analysis. No LLM credentials, no database, no inference.

`results list` shows what is available before you commit to the download, and
`results clear` removes it again.

## Run your own evaluation

Three things have to be in place first:

1. **A benchmark**, declared in the registry — see [Benchmarks](benchmarks.md).
2. **A model**, if you want the toolkit to generate SQL rather than score SQL
   you already have — see [Models and providers](models.md).
3. **A database**, if you want execution-based metrics. Evaluation can run
   without one, on stored result sets.

Then, from a source checkout:

```bash
python scripts/run_experiment.py bird_mini_dev_sqlite_test_50
```

That runs inference, execution and evaluation over a 50-question subset, which
is small enough to be a real check and cheap enough to be worth repeating.

## Or use the library directly

Scoring a prediction you already have needs no pipeline at all:

```python
from text2sql_eval_toolkit import evaluate_prediction

metrics = evaluate_prediction(record, prediction)
print(metrics["execution_accuracy"])
```

`record` is a benchmark entry and `prediction` is one entry from its
`predictions` map; [Data model](data-model.md) describes both. Every function is
documented in the [API reference](../reference/index.md).

## Where things are written

Outputs go to the **writable data root**, resolved in this order:

1. `$TEXT2SQL_EVAL_TOOLKIT_DATA_ROOT`
2. the nearest ancestor directory holding both `pyproject.toml` and `data/`
3. `./data`

Note this is a *different* variable from `TEXT2SQL_DATA_ROOT`, which is where
the dashboard reads from. They are easy to conflate and are not the same thing —
[Configuration](configuration.md) has the full list.
