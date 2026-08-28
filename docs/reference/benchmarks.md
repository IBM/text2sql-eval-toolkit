# Benchmarks

Benchmarks are declared in `benchmarks.json` (full) and `test-benchmarks.json`
(small subsets for quick validation). These functions locate them and resolve the
paths of every artifact a benchmark owns.

Two roots are involved and they are not the same: benchmark *data* and *schema*
paths resolve relative to the registry file, while *predictions* resolve against
a writable data root. See [`get_benchmarks_info`][text2sql_eval_toolkit.get_benchmarks_info].

::: text2sql_eval_toolkit.get_available_benchmarks
::: text2sql_eval_toolkit.get_benchmark_info
::: text2sql_eval_toolkit.get_benchmarks_info

## Pre-computed results

Results for the packaged benchmarks are published on the Hugging Face Hub and
fetched rather than regenerated.

::: text2sql_eval_toolkit.fetch_results
::: text2sql_eval_toolkit.list_available_results
::: text2sql_eval_toolkit.clear_cache
