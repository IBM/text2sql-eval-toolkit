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

Three values are exported alongside the functions.

`__version__` is the installed distribution's version, read from package
metadata. It reports `0.0.0` when the package is imported from a source tree
that was never installed.

`DEFAULT_REPO_ID` is the Hugging Face dataset holding the pre-computed results,
about 4 GB in total.

`DEFAULT_REVISION` is the dataset revision fetched by default — `v` followed by
the toolkit version, so a given release reads the results published for it. When
that tag does not exist on the dataset yet, the fetch falls back to `main` and
warns rather than failing.

::: text2sql_eval_toolkit.__version__
::: text2sql_eval_toolkit.DEFAULT_REPO_ID
::: text2sql_eval_toolkit.DEFAULT_REVISION
