# API reference

Everything here is exported from the top level, so `from text2sql_eval_toolkit
import …` reaches all of it. The grouping below follows the pipeline stages
rather than the module layout, because that is how the functions are used.

| Section | What it covers |
|---|---|
| [Benchmarks](benchmarks.md) | Finding benchmarks and resolving their file paths |
| [Inference](inference.md) | Generating SQL with an LLM |
| [Execution](execution.md) | Running ground-truth and predicted SQL |
| [Evaluation](evaluation.md) | Scoring predictions, including LLM-as-judge |
| [Metrics](metrics.md) | Comparing SQL statements and result sets |
| [Utilities](utilities.md) | Reading records, paths, dataframes, timeouts |

Stages are usable standalone or chained. Each is resumable: existing results are
reused unless `force_rerun=True`, so a run that fails partway can be repeated
without redoing what already succeeded.

## Version and defaults

::: text2sql_eval_toolkit.__version__
::: text2sql_eval_toolkit.DEFAULT_REPO_ID
::: text2sql_eval_toolkit.DEFAULT_REVISION
