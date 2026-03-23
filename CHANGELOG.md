# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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