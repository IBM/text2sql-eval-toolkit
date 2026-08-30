# Benchmarks

## The registry

Benchmarks are declared in JSON, not discovered:

- `data/benchmarks.json` — the full set
- `data/test-benchmarks.json` — small subsets, for quick validation

[`get_benchmarks_info`][text2sql_eval_toolkit.get_benchmarks_info] resolves the
registry file in this order:

1. `$TEXT2SQL_DATA_ROOT/`
2. `./data/`
3. the copy packaged inside `text2sql_eval_toolkit/data/`

[`get_benchmark_info`][text2sql_eval_toolkit.get_benchmark_info] looks a single
benchmark up by id, falling back from the full registry to the test registry
automatically — so a test-subset id resolves without any change of
configuration.

[`get_available_benchmarks`][text2sql_eval_toolkit.get_available_benchmarks]
lists what is declared.

!!! warning "Two copies, and they have drifted before"
    The repository copy shadows the packaged one whenever the working directory
    is a checkout. A change made only to `data/benchmarks.json` is therefore
    invisible in development and missing for everyone who installed from PyPI.
    Mirror edits into `src/text2sql_eval_toolkit/data/benchmarks.json`.

## An entry

```json
{
  "my_benchmark": {
    "name": "my_benchmark",
    "description": "What this benchmark covers",
    "data": "benchmarks/my_benchmark.json",
    "schema": "benchmarks/schemas/my_benchmark.json",
    "predictions": "results/my_benchmark-predictions.json",
    "db_engine": {
      "db_type": "postgres",
      "connection_string_env_var": "POSTGRES_CONNECTION_STRING",
      "schema_name": "my_schema",
      "db_folder": "benchmarks/dbs/my_benchmark"
    }
  }
}
```

Supported `db_type` values: `sqlite`, `postgres`, `mysql`, `db2`, `presto`.

Note that the connection string is named by
**`connection_string_env_var`**, not fixed by the toolkit. Two benchmarks on
different Postgres servers name different variables; nothing is hardcoded.

## Path resolution, and the trap in it

Two different roots are in play, and conflating them is the usual cause of a
path that will not resolve:

| Key | Relative to |
|---|---|
| `data`, `schema` | the directory holding the registry file |
| `predictions` | the **writable data root** |

The writable data root is `$TEXT2SQL_EVAL_TOOLKIT_DATA_ROOT`, else the nearest
ancestor directory containing both `pyproject.toml` and `data/`, else `./data`.
That is a different variable from `$TEXT2SQL_DATA_ROOT` above, and deliberately
so: inputs may be read-only and shared, while outputs must be writable and are
usually not.

## SQLite databases

SQLite benchmarks read local database folders that are **not in the repository**
— they are large, and several have licences that do not permit redistribution.
`data/benchmarks/dbs/README.md` says where each one comes from.

## Pre-computed results

About 4 GB of results already exist and are fetched rather than shipped:

```bash
text2sql-eval-toolkit results fetch
```

See [Command line](cli.md) for listing and clearing them, and
[`fetch_results`][text2sql_eval_toolkit.fetch_results] for the library
equivalent.
