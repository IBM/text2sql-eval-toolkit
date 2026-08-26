# CLAUDE.md

Guidance for Claude Code when working in this repository.

## What this is

`text2sql-eval-toolkit` (IBM, Apache-2.0) — a Python library + CLI + web dashboard for
evaluating text-to-SQL systems. Five stages, each usable standalone or chained:

**inference** (LLM generates SQL) → **execution** (run GT + predicted SQL, store result
dataframes) → **evaluation** (execution-match, SQL-equivalence, LLM-as-judge metrics) →
**profiling** (SQL feature tags for slicing) → **analysis** (Markdown/chart reports, dashboard).

Package lives in `src/text2sql_eval_toolkit/`; thin argparse wrappers in `scripts/`;
React+Carbon frontend in `dashboard/`; benchmark data and outputs in `data/`.

## Core data model

One JSON file per benchmark holds *everything*, accumulating across stages:

```
data/results/{benchmark}-predictions.json        # records + per-pipeline predictions + result dfs
data/results/{benchmark}-predictions_eval.json   # above + "evaluation" block per prediction
data/results/{benchmark}-predictions_eval_summary.{json,csv,md}
```

A record is a benchmark question with `predictions: {pipeline_id: {predicted_sql, prompt,
model_name, model_parameters, token_usage, inference_time_ms, predicted_df, evaluation, ...}}`.
Dataframes are serialized as pandas `orient='split'` JSON; read them with `parse_dataframe`.

`pipeline_id` is the unit of comparison everywhere (dashboard, summaries, error analysis) and is
derived, not configured:
- baseline: `{model_name}-greedy-zero-shot-chatapi`
- agentic: `{model_name}-agentic-baseline{0..5}-{max_attempts}attempts` (v0…v5 → baseline0…5)

Every stage is **resume-by-default**: existing predictions/evaluations are reused unless
`force_rerun=True` (or `--force_rerun`). Failed inferences are retried automatically.

## Benchmark registry (read this before touching benchmark config)

Benchmarks are declared in `data/benchmarks.json` (full) and `data/test-benchmarks.json`
(small subsets for quick validation). `get_benchmarks_file_path()` resolves in order:
`$TEXT2SQL_DATA_ROOT/`, `./data/`, then the packaged copy under
`src/text2sql_eval_toolkit/data/`. **The two copies have already drifted** — when editing
`data/benchmarks.json`, mirror the change into `src/text2sql_eval_toolkit/data/benchmarks.json`
so pip-installed users see it too.

Path resolution: `data`/`schema` are relative to the registry file's directory; `predictions`
is relative to `get_writable_data_root()` (`$TEXT2SQL_EVAL_TOOLKIT_DATA_ROOT`, else the nearest
ancestor with `pyproject.toml` + `data/`, else `./data`). Two different roots — don't conflate them.

`get_benchmark_info(id)` falls back from the full registry to the test registry automatically.

## Environment

Credentials come from `.env` (see `env.example`); `env_loader.load_env()` auto-runs on import and
searches upward from cwd, then the checkout root, then `~/.env`. Never override existing env vars.

Model names are `provider:model` and routed by prefix in `baseline_llm_pipeline.py`:
`wxai:` (watsonx), `anthropic:`, `gemini:`, `openai:`, `vllm:`, `ollama:`, `rits`.
DB access uses `POSTGRES_CONNECTION_STRING`, `MYSQL_CONNECTION_STRING`,
`DB2_CONNECTION_STRING`, `PRESTO_CONNECTION_STRING`; SQLite benchmarks read local db folders
(not in git — see `data/benchmarks/dbs/README.md`).

Pre-computed results (~7 GB) are not in the repo: `text2sql-eval-toolkit results fetch`.

## Commands

```bash
uv pip install -e ".[dashboard]"          # add mysql,presto,db2,notebook extras as needed
pytest                                     # unit tests
RUN_NETWORK_TESTS=1 pytest tests/results/  # HF Hub integration tests (opt-in)
python scripts/run_experiment.py bird_mini_dev_sqlite_test_50   # inference+execution+eval
python scripts/run_all_benchmarks.py --test                     # all test benchmarks
python scripts/analysis/make_summary_report.py                  # regenerate data/results/README.md
text2sql-eval-dashboard --open-browser                          # FastAPI + built React UI on :8000
./scripts/run_dashboard.sh --dev                                # backend reload + Vite on :5173
```

`tests/test_run_experiment_integration.py` calls real LLM and DB endpoints — it needs
credentials and will fail offline. The rest of the suite is hermetic.

## Conventions

- Apache-2.0 SPDX header (`# Copyright IBM Corp. 2025 - 2026`) at the top of every source file.
- Black formatting (88 cols), Ruff lint, Google-style docstrings, type hints on signatures.
- DCO required: commit with `git commit -s`.
- Execution/evaluation are asyncio with a bounded semaphore (default concurrency 16) and
  per-query timeouts via `run_with_timeout*`; keep new DB work inside that pattern.
- Public API is curated in `src/text2sql_eval_toolkit/__init__.py` — add new exports to both the
  imports and `__all__`, and `tests/test_public_api.py` guards the key symbols.
- New per-record metrics must also be described in `evaluation/metric_definitions.py`; the
  dashboard renders its metric help from that list.
- LLM-judge prompts/models are YAML in `evaluation/llm_judge_config/` and editable from the UI.

## Dashboard

FastAPI app in `src/text2sql_eval_toolkit/ui/server.py` (all routes under `/api`) serves the
committed production build in `dashboard/dist/`, which **is checked in** — rebuild it
(`cd dashboard && npm run build`) and commit when changing frontend sources, or rely on the
server's `vite build --watch` in a dev checkout. React views live in `dashboard/src/views/`,
API client in `dashboard/src/lib/api.ts`.
