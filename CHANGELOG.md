# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-03-11

### Added
- Pip-installable `text2sql-eval-toolkit` library with packaged benchmark metadata.
- Curated top-level Python API for evaluation (`evaluate_prediction`, `evaluate_predictions`, `run_evaluation`).
- Execution orchestration helper (`run_execution`) and benchmark discovery utilities (`get_available_benchmarks`, `get_benchmarks_info`, `get_benchmark_info`).
- Public inference pipelines (`LLMSQLGenerationPipeline`, `AgenticSQLGenerationPipeline`) for reproducing baseline and agentic experiments.
- Re-exported low-level SQL comparison and parsing helpers (`compare_result_dfs`, `sql_exact_match`, etc.) from `unitxt.text2sql_utils`.
- Library-focused README examples showing record-level, file-level, and benchmark-level usage.

[1.0.0]: https://github.com/IBM/text2sql-eval-toolkit/releases/tag/v1.0.0