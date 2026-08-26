# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.3.0] - 2026-08-25

A dashboard release: every view is now addressable by URL, reads are served from
a derived index instead of by re-parsing artifacts, and the server can be run as
a read-only public deployment.

The library's public API is unchanged, the CLI is unchanged, and the on-disk
artifact format is unchanged — hence a minor bump rather than a major one. A
default `text2sql-eval-dashboard` on loopback keeps every capability it had,
which is enforced by a test rather than by intention.

### Packaging

- The wheel now contains the LLM-judge prompt configs. Every release to date
  shipped the judge code without them, so `load_llm_judge_config()` raised
  `FileNotFoundError` on any pip install.
- The wheel now contains the dashboard frontend, so `pip install
  "text2sql-eval-toolkit[dashboard]"` serves the UI instead of returning 404 at
  `/`. The Vite build is copied into the package at build time by `setup.py`.

### Added
- **Shareable URLs.** Every view has its own address — benchmark, pipeline
  detail, filtered error analysis, an individual record, and a record within a
  pipeline (`/benchmark/{id}/pipeline/{pipeline}/record/{record}`) — and
  reopening one restores the same view.
- **Short pipeline links.** `GET /api/benchmarks/{id}/pipeline-aliases` returns
  a derived alias per pipeline; the dashboard accepts an alias anywhere it
  accepts an id and expands it on arrival. A two-pipeline comparison link goes
  from 247 characters to 158. Aliases shorten links; they do not survive a model
  being renamed.
- **Query index.** A SQLite index built alongside each evaluation artifact
  (`text2sql-eval-toolkit index build` / `index status`). 1,915 MB of artifacts
  become 117 MB of indices; a record detail goes from 921 ms to 0.3 ms and peak
  memory from 2,151 MB to 170 MB.
- **Capability tiers** (`public` / `judge` / `full`), resolved per request from
  the deployment mode and the caller's identity and enforced centrally, so a new
  endpoint is safe by default. `full` is the default for a loopback bind.
- **Google sign-in** and a **scoped LLM-as-judge endpoint** for allowlisted
  users, metered against a monthly budget that persists across restarts.
- **Deployment artifacts**: container image, compose file with internal-only
  database networking, provisioning script, and an operations runbook
  (`docs/deployment-runbook.md`).
- **CI**: lint, format, type check, tests across Python 3.11–3.13, frontend
  build and lint, per-module coverage floors, and end-to-end tests.
- **Tests**: 548 backend, 77 frontend, and 9 Playwright end-to-end tests that
  copy a link and reopen it in a fresh browser context.

### Changed
- Dashboard reads are served from the index rather than by parsing whole
  evaluation files per request.
- SQLite execution opens the database read-only with `ATTACH` disabled.
- Security headers, per-client rate limiting, and CORS narrowed outside `full`
  mode.
- `data/benchmarks.json` in the checkout is now canonical; the packaged copy is
  generated from it and CI fails on divergence.
- `requirements.txt` is generated from `uv.lock`.
- The version is resolved in one place from the installed package metadata,
  rather than being repeated in `pyproject.toml` and `__init__.py`.

### Fixed
- Ranking window functions (`RANK`, `DENSE_ROW_NUMBER`, …) were counted as
  aggregations under sqlglot ≥ 28, corrupting profiling categories.
- `compute_summary` aborted a whole benchmark's summary on one record whose
  metrics were incomplete.
- `report_tools` aborted a whole report on a metric stored as a bare number.
- A ground-truth SQL with no executed dataframe silently produced a record with
  no metrics and no error flag.
- `sqlite_run_execution_async` resolved database paths against the packaged
  registry, so the documented SQLite setup could not work from a checkout.
- Error-analysis reports counted a record the pipeline never answered but could
  not say which one, and inlined an entire record — dataframes included — into
  the error note.
- The dashboard's back button did nothing in error analysis; a link to a
  benchmark the server does not have silently opened a different one; and a cold
  load wrote a default filter into the address without applying it to the
  results.

## [1.2.0] - 2026-05-13

### Added
- `text2sql-eval-toolkit results fetch` command to download pre-computed
  evaluation results from the Hugging Face Hub
  (`text2sql-eval-toolkit/text2sql-eval-results`).
- `text2sql_eval_toolkit.results` public API: `fetch_results`,
  `list_available_results`, `clear_cache`, `DEFAULT_REPO_ID`,
  `DEFAULT_REVISION`.
- `text2sql-eval-toolkit results list` — print the available results
  manifest as a table.
- `text2sql-eval-toolkit results clear` — remove downloaded results.
- Dashboard detects missing results on startup and logs an actionable hint.
- Dashboard `--enable-fetch` flag exposes `/api/results/fetch` endpoints
  and an in-UI "Fetch results" button (off by default).
- `scripts/curation/upload_results_to_hub.py` — maintainer script for
  pushing new result snapshots to the HF Hub.

## [1.1.0] - 2026-03-23

### Added
- Expanded dashboard navigation with dedicated views for Metric Insights, Pipeline Compare, Error Analysis, LLM Judge config management, and Run Evaluation.
- New dashboard views: `ToolkitInsightsView` and `PipelineCompareView` for confusion-matrix-driven metric analysis and cross-pipeline disagreement exploration.
- Richer error-analysis and pipeline-detail UX with contextual deep links, side-panel record details, SQL/result inspection, and raw JSON record viewing.
- New API endpoints for insight workflows:
  - per-pipeline binary metric confusion
  - cross-pipeline binary metric confusion
- Toolkit-owned metrics module (`text2sql_eval_toolkit.metrics`) with public exports for SQL parsing/equivalence helpers, execution utilities, connectors, and cache helpers.
- New tests for numeric-normalized subset comparison behavior in SQL result matching.

### Changed
- Internal evaluation imports now use toolkit-native metrics utilities instead of `unitxt.text2sql_utils`.
- Summary scripts/docs now recommend `pip install -e .` setup for local toolkit usage.
- Dashboard benchmark and pipeline pages now include direct actions into insights/compare/error analysis workflows.

### Fixed
- Improved subset matching robustness for mixed numeric representations (for example `4` vs `4.0`) in non-empty subset execution comparisons.

### Removed
- Direct `unitxt` dependency from project dependency manifests.

## [1.0.0] - 2026-03-11

### Added
- Pip-installable `text2sql-eval-toolkit` library with packaged benchmark metadata.
- Curated top-level Python API for evaluation (`evaluate_prediction`, `evaluate_predictions`, `run_evaluation`).
- Execution orchestration helper (`run_execution`) and benchmark discovery utilities (`get_available_benchmarks`, `get_benchmarks_info`, `get_benchmark_info`).
- Public inference pipelines (`LLMSQLGenerationPipeline`, `AgenticSQLGenerationPipeline`) for reproducing baseline and agentic experiments.
- Re-exported low-level SQL comparison and parsing helpers (`compare_result_dfs`, `sql_exact_match`, etc.) from toolkit-owned metrics utilities.
- Library-focused README examples showing record-level, file-level, and benchmark-level usage.

[1.1.0]: https://github.com/IBM/text2sql-eval-toolkit/releases/tag/v1.1.0
[1.0.0]: https://github.com/IBM/text2sql-eval-toolkit/releases/tag/v1.0.0