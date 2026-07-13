# SQL Prediction Evaluation

This directory contains scripts for evaluating SQL query predictions against ground truth queries and their result sets:

* **`run_evaluation.py`** — full evaluation for a **benchmark id** (reads/writes SQLite).
* **`rerun_metrics.py`** — **metrics only**: re-score existing `predicted_sql` / `predicted_df` without inference or SQL execution.
* **`run_llm_judge.py`** — **LLM judge only**: score existing predictions with LLM-as-judge, skipping deterministic metrics.

## Requirements

Before running the script, install the required dependencies:

```bash
pip install -e .
```

> **Note on LLM Judge mode (`--use_llm_judge`)**
> If you enable `--use_llm_judge`, you must have your LLM provider configured (e.g., API key and endpoint) as described in the project's main README. LLM evaluation may incur API costs and can be non-deterministic unless you fix parameters (e.g., temperature).

## Storage model

Evaluation reads predictions from and writes results to **SQLite** (`TEXT2SQL_DATABASE_URL`, default `data/text2sql_eval.db`). Each run creates a durable `jobs` row with type `eval` (deterministic metrics only) or `llm_judge` (when `--use_llm_judge` is set).

Legacy JSON result files are **not** used at runtime. To import existing `*-predictions*.json` artifacts, use [`scripts/migration/import_json_to_db.py`](../migration/README.md).

## Usage

```bash
python scripts/evaluation/run_evaluation.py <benchmark_id> \
  [--use_llm_judge] \
  [--llm_judge_config_path PATH] \
  [--force-rerun] \
  [--force-rerun-llm-judge] \
  [--csv_summary_path PATH]
```

* `<benchmark_id>`: Benchmark id from `data/benchmarks.json` (e.g. `bird_mini_dev_sqlite`).
* `--use_llm_judge`: Enable LLM-as-judge metrics in addition to standard metrics.
* `--llm_judge_config_path`: Path to judge YAML config. Default: [`llm_judge_default_config.yaml`](../../src/text2sql_eval_toolkit/evaluation/llm_judge_config/llm_judge_default_config.yaml).
* `--force-rerun`: Recompute all evaluation metrics (ignore cached evaluations in DB).
* `--force-rerun-llm-judge`: Re-call LLM judge even when cached scores exist.
* `--csv_summary_path`: Optional path to write a summary CSV export.

### Library API

```python
from text2sql_eval_toolkit import run_evaluation, evaluate_predictions

# Equivalent to the script above
run_evaluation("bird_mini_dev_sqlite", use_llm=True)

# evaluate_predictions is an alias for run_evaluation(benchmark_id=...)
evaluate_predictions("bird_mini_dev_sqlite")
```

## Input data

The evaluator loads gold records and predictions from SQLite for the given benchmark. Each prediction block must contain:

* `predicted_sql` and `predicted_df` (from execution stage)
* Ground-truth `sql` and `gt_df` on the parent record

Run inference and execution first if predictions are missing:

```bash
python scripts/inference/run_inference.py bird_mini_dev_sqlite
python scripts/execution/run_execution.py bird_mini_dev_sqlite
```

## Output

* Per-prediction evaluation metrics are upserted into `evaluations` and (optionally) `llm_judge_evaluations`.
* Pipeline summaries are written to `eval_summaries` and `llm_judge_eval_summaries`.
* A summary table is printed to the console; optional CSV via `--csv_summary_path`.

## Metrics

* **execution\_accuracy**: Exact match between predicted and ground truth result sets.
* **non\_empty\_execution\_accuracy**: Match when both result sets are non-empty.
* **subset\_non\_empty\_execution\_accuracy**: Match even with missing or additional columns when both sets are non-empty.
* **bird\_execution\_accuracy**: Set-based match as in the BIRD benchmark.
* **is\_sqlglot\_parsable**: Whether the predicted SQL is parsable by SQLGlot.
* **is\_sqlparse\_parsable**: Whether the predicted SQL is parsable by sqlparse.
* **sqlglot\_equivalence**: SQLGlot-based equivalence.
* **sqlglot\_optimized\_equivalence**: Optimized SQLGlot equivalence.
* **sqlparse\_equivalence**: sqlparse-based equivalence.
* **sql\_exact\_match**: Exact string match of SQLs.
* **sql\_syntactic\_equivalence**: Any of the above SQL equivalence metrics is true.
* **llm\_score** *(only with `--use_llm_judge`)*: LLM-as-judge score indicating whether the prediction is correct per a rubric.
  * Depending on configuration, the per-prediction rationale (`llm_explanation`) may also be recorded.

## Re-run metrics only (`rerun_metrics.py`)

Use this after changing metric code (e.g. `sqlglot_equivalence` in `text2sql_utils.py`) or to refresh scores without re-running models or databases.

```bash
# By benchmark id (reads/writes SQLite)
python scripts/evaluation/rerun_metrics.py bird_mini_dev_sqlite

# Refresh sqlglot/execution metrics but keep cached LLM judge scores
python scripts/evaluation/rerun_metrics.py bird_mini_dev_sqlite --preserve-llm-judge
```

| Flag | Meaning |
|------|---------|
| `--preserve-llm-judge` | Re-run non-LLM metrics; reuse `llm_score` from DB |
| `--use-llm-judge` | Enable LLM-as-judge (may call the LLM) |
| `--force-rerun-llm-judge` | Re-call LLM judge even if cached |
| `--max-concurrency N` | Parallel workers (default 16) |
| `--csv-summary-path PATH` | Optional summary CSV export |

LLM-as-judge is **off by default**.

## LLM judge only (`run_llm_judge.py`)

Use this to run LLM-as-judge without recomputing deterministic metrics (execution accuracy, sqlglot equivalence, etc.):

```bash
python scripts/evaluation/run_llm_judge.py bird_mini_dev_sqlite

# Force re-score even when cached
python scripts/evaluation/run_llm_judge.py bird_mini_dev_sqlite --force-rerun-llm-judge

# Use a specific judge config
python scripts/evaluation/run_llm_judge.py bird_mini_dev_sqlite \
  --llm-judge-config-path src/text2sql_eval_toolkit/evaluation/llm_judge_config/llm_judge_no_gt_v1.yaml
```

| Flag | Meaning |
|------|---------|
| `--llm-judge-config-path` | Judge YAML config (default: `llm_judge_default_config.yaml`) |
| `--force-rerun-llm-judge` | Re-call LLM judge even if cached |
| `--max-concurrency N` | Parallel workers (default 16) |
| `--csv-summary-path PATH` | Optional summary CSV (works best after a full eval) |

Cached `llm_score` / `llm_explanation` values are reused unless `--force-rerun-llm-judge` is set. Existing deterministic evaluation fields are preserved.

## Example

Run without LLM:

```bash
python scripts/evaluation/run_evaluation.py bird_sqlite_test_benchmark
```

Run with LLM-as-judge:

```bash
python scripts/evaluation/run_evaluation.py bird_sqlite_test_benchmark --use_llm_judge
```

Results are stored in `data/text2sql_eval.db` for the benchmark's default `result_set`.
