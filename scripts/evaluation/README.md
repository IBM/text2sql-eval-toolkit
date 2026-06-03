# SQL Prediction Evaluation

This directory contains scripts for evaluating SQL query predictions against ground truth queries and their result sets:

* **`run_evaluation.py`** — full evaluation (same metrics; may enable LLM-as-judge).
* **`rerun_metrics.py`** — **metrics only**: re-score existing `predicted_sql` / `predicted_df` without inference or SQL execution.

## Requirements

Before running the script, install the required dependencies:

```bash
pip install -e .
```

or follow the instructions on the main README to setup your environment and do:

```bash
pip install -e .
```

> **Note on LLM Judge mode (`--use_llm_judge`)**
> If you enable `--use_llm_judge`, you must have your LLM provider configured (e.g., API key and endpoint) as described in the project's main README. LLM evaluation may incur API costs and can be non-deterministic unless you fix parameters (e.g., temperature).

## Usage

```bash
python scripts/evaluation/run_evaluation.py <input_file.json> [--output_file OUTPUT] [--summary_file SUMMARY] [--csv_summary_file CSV_SUMMARY] [--use_llm_judge] [--llm_judge_config_path]
```

* `<input_file.json>`: Path to the input JSON file containing records with ground truth and predicted SQL queries and their result DataFrames.
* `--output_file`: (Optional) Path to write the evaluated JSON output. Defaults to `<input_file>_eval.json`.
* `--summary_file`: (Optional) Path to write the evaluation summary in JSON format. Defaults to `<input_file>_eval_summary.json`.
* `--csv_summary_file`: (Optional) Path to write the evaluation summary in CSV format. Defaults to `<input_file>_eval_summary.csv`.
* `--use_llm_judge`: (Optional) Enables LLM-as-judge metrics in addition to the standard metrics.
* `--llm_judge_config_path`: (Optional) Enables LLM-as-judge metrics in addition to the standard metrics, and sets path to config yaml file for LLM-as-judge inference. See an example config file that is used as default: [here](../../src/text2sql_eval_toolkit/evaluation/llm_judge_config/llm_judge_default_config.yaml).

## Input Format

The input JSON should be a list of records. Each record must contain:

* `"sql"`: The ground truth SQL query.
* `"gt_df"`: The ground truth result DataFrame, encoded as a JSON string.
* `"predictions"`: A dictionary mapping model names to their predictions, each containing:

  * `"predicted_sql"`: The predicted SQL query.
  * `"predicted_df"`: The predicted result DataFrame, encoded as a JSON string.

For a sample input, see [`data/benchmarks/test_benchmarks/results/bird_sqlite_test_benchmark-predictions.json`](../../data/benchmarks/test_benchmarks/results/bird_sqlite_test_benchmark-predictions.json).

## Output

* The script adds evaluation metrics to each prediction in the output JSON.
* It also produces summary statistics (mean and standard deviation) for each metric, per model, in both JSON and CSV formats.
* A summary is printed to the console.

For sample outputs, see:

* [`data/benchmarks/test_benchmarks/results/bird_sqlite_test_benchmark-predictions_eval.json`](../../data/benchmarks/test_benchmarks/results/bird_sqlite_test_benchmark-predictions_eval.json)
* [`data/benchmarks/test_benchmarks/results/bird_sqlite_test_benchmark-predictions_eval_summary.json`](../../data/benchmarks/test_benchmarks/results/bird_sqlite_test_benchmark-predictions_eval_summary.json)
* [`data/benchmarks/test_benchmarks/results/bird_sqlite_test_benchmark-predictions_eval_summary.csv`](../../data/benchmarks/test_benchmarks/results/bird_sqlite_test_benchmark-predictions_eval_summary.csv)

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
# By benchmark id
python scripts/evaluation/rerun_metrics.py bird_mini_dev_sqlite

# By predictions file
python scripts/evaluation/rerun_metrics.py --input-file data/results/my_benchmark-predictions.json

# Update an existing eval file in place
python scripts/evaluation/rerun_metrics.py \
  --input-file data/results/my_benchmark-predictions_eval.json --in-place

# Refresh sqlglot/execution metrics but keep cached LLM judge scores
python scripts/evaluation/rerun_metrics.py bird_mini_dev_sqlite --preserve-llm-judge
```

| Flag | Meaning |
|------|---------|
| `--in-place` | Write back to the input `*_eval.json` |
| `--preserve-llm-judge` | Re-run non-LLM metrics; reuse `llm_score` from existing eval output |
| `--use-llm-judge` | Enable LLM-as-judge (may call the LLM) |
| `--force-rerun-llm-judge` | Re-call LLM judge even if cached |
| `--max-concurrency N` | Parallel workers (default 16) |

LLM-as-judge is **off by default** (unlike `run_evaluation.py` when you pass `--use_llm_judge`).

## Example (`run_evaluation.py`)

Run without LLM:

```bash
python scripts/evaluation/run_evaluation.py data/benchmarks/test_benchmarks/results/bird_sqlite_test_benchmark-predictions.json
```

Run with LLM-as-judge:

```bash
python scripts/evaluation/run_evaluation.py data/benchmarks/test_benchmarks/results/bird_sqlite_test_benchmark-predictions.json --use_llm_judge
```

This will produce:

* `data/benchmarks/test_benchmarks/results/bird_sqlite_test_benchmark-predictions_eval.json`
* `data/benchmarks/test_benchmarks/results/bird_sqlite_test_benchmark-predictions_eval_summary.json`
* `data/benchmarks/test_benchmarks/results/bird_sqlite_test_benchmark-predictions_eval_summary.csv`

