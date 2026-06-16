# JSON → SQLite Migration

This directory contains tooling to migrate legacy on-disk JSON artifacts into the relational database defined in [`data/database-schema/schema.sql`](../../data/database-schema/schema.sql).

**Runtime:** The dashboard and pipeline read/write **SQLite** (`data/text2sql_eval.db`). This script is the supported path to load legacy `data/results/*.json` (e.g. after `text2sql-eval-toolkit results fetch`) or to backfill an existing checkout. New experiments write directly to the database without producing result JSON files.

## Requirements

- Python 3.11+
- SQLite 3.35+ (JSON1 extension)
- A populated `data/` directory with benchmark JSON artifacts

The script uses only the Python standard library and does **not** require `pip install -e .`. It loads the importer from `src/text2sql_eval_toolkit/database/` directly.

## Quick start

Initialize the database and import everything (production + test benchmarks):

```bash
python3 scripts/migration/import_json_to_db.py --init
```

This creates `data/text2sql_eval.db` (unless overridden), applies the schema, and imports all benchmarks registered in `data/benchmarks.json` and `data/test-benchmarks.json`.

Import a single benchmark first (recommended for large suites):

```bash
python3 scripts/migration/import_json_to_db.py --init \
  --benchmark-id beaver_test_10
```

Verify the import:

```bash
sqlite3 data/text2sql_eval.db "SELECT benchmark_id, num_records FROM benchmarks;"
sqlite3 data/text2sql_eval.db "SELECT COUNT(*) FROM predictions;"
```

## Command reference

```bash
python3 scripts/migration/import_json_to_db.py [OPTIONS]
```

| Flag | Description |
|------|-------------|
| `--init` | Apply `data/database-schema/schema.sql` before importing. Also runs automatically when the database file does not exist. |
| `--data-root PATH` | Root data directory (default: repo `data/` or `TEXT2SQL_DATA_ROOT`). |
| `--database PATH` | SQLite file path (default: `TEXT2SQL_DATABASE_URL` or `data/text2sql_eval.db`). |
| `--benchmark-id ID` | Import only the given benchmark (repeatable). Default: all registered benchmarks. |
| `--production-only` | Skip test benchmarks from `test-benchmarks.json`. |
| `--skip-predictions` | Import catalog and gold records only (no result files). |
| `--skip-eval` | Import predictions without evaluation metrics. |
| `--skip-summaries` | Do not import `*-predictions_eval_summary.json`. |
| `--compute-category-summaries` | Recompute per-category rows in `eval_summaries` from imported evaluations and `record_categories`. |
| `--force` | Delete existing rows for each benchmark before re-importing. |

## Environment variables

| Variable | Purpose |
|----------|---------|
| `TEXT2SQL_DATA_ROOT` | Data directory containing `benchmarks.json`, `benchmarks/`, and `results/`. |
| `TEXT2SQL_EVAL_TOOLKIT_DATA_ROOT` | Writable data root (also set automatically by the script). |
| `TEXT2SQL_DATABASE_URL` | SQLite URL, e.g. `sqlite:///./data/text2sql_eval.db`. |

The script sets `TEXT2SQL_DATA_ROOT` and `TEXT2SQL_EVAL_TOOLKIT_DATA_ROOT` from `--data-root` when provided.

## What gets imported

Import runs in this order for each benchmark:

1. **Registry** — `benchmarks.json` / `test-benchmarks.json` → `benchmarks`, `benchmark_db_config`
2. **Gold records** — `benchmarks/{id}.json` → `benchmark_records`, `record_gt_sql`, `record_categories`, `record_features`
3. **Schema** — `benchmarks/{id}-schema.json` → `benchmark_schema_snapshots`
4. **Predictions & eval** — prefers `*-predictions_eval.json`, falls back to `*-predictions.json` → `result_sets`, `pipelines`, `predictions`, `prediction_inference`, `prediction_execution`, `result_dataframes`, `record_ground_truth_execution`, `evaluations`, `llm_judge_evaluations`
5. **Summaries** — `*-predictions_eval_summary.json` → `eval_summaries`, `llm_judge_eval_summaries`, `llm_judge_configs`
6. **Category summaries** (optional, `--compute-category-summaries`) — aggregates evaluations by `record_categories` into `eval_summaries` rows where `category IS NOT NULL`

### File → table mapping

| JSON artifact | Database tables |
|---------------|-----------------|
| `data/benchmarks.json` | `benchmarks`, `benchmark_db_config` |
| `data/benchmarks/{id}.json` | `benchmark_records`, `record_gt_sql`, `record_categories`, `record_features` |
| `data/benchmarks/{id}-schema.json` | `benchmark_schema_snapshots` |
| `data/results/{id}-predictions.json` | `predictions`, `prediction_inference`, `prediction_execution` |
| `data/results/{id}-predictions_eval.json` | above + `evaluations`, `result_dataframes`, `llm_judge_evaluations` |
| `data/results/{id}-predictions_eval_summary.json` | `eval_summaries`, `llm_judge_eval_summaries` |

Test benchmarks follow the same layout under `data/benchmarks/test_benchmarks/` with paths declared in `test-benchmarks.json`.

## Examples

### Production benchmarks only

```bash
python3 scripts/migration/import_json_to_db.py --init --production-only
```

### Catalog and gold data only (no predictions)

Useful for seeding the database before running inference directly against SQLite:

```bash
python3 scripts/migration/import_json_to_db.py --init \
  --skip-predictions \
  --production-only
```

### Full import with per-category dashboard summaries

```bash
python3 scripts/migration/import_json_to_db.py --init \
  --production-only \
  --compute-category-summaries
```

### Re-import one benchmark from scratch

```bash
python3 scripts/migration/import_json_to_db.py \
  --force \
  --benchmark-id spider_dev
```

### Custom paths

```bash
python3 scripts/migration/import_json_to_db.py --init \
  --data-root /path/to/data \
  --database /tmp/text2sql_eval.db \
  --benchmark-id bird_mini_dev_sqlite_test_50
```

## Idempotency and re-runs

The importer uses upserts (`INSERT … ON CONFLICT DO UPDATE`) for most entities. Re-running without `--force` updates existing rows in place.

Use `--force` when you need a clean re-import for a benchmark; this deletes the benchmark row (and cascades to all related data) before loading again.

If `--init` is passed but the database already has a `schema_migrations` table, schema application is skipped with a warning. Delete the database file or use a new path to apply a fresh schema.

## Performance and storage notes

- **Large eval files:** Full-benchmark `*-predictions_eval.json` files can be hundreds of MB to several GB. Import loads each file fully into memory. Migrate one benchmark at a time with `--benchmark-id` for large suites.
- **DataFrames:** Serialized result tables (`predicted_df`, `logic_df`, `gt_df`) are stored in `result_dataframes.payload_text`. Identical payloads are deduplicated by content hash within a single import run.
- **Disk usage:** Expect the SQLite file to be a significant fraction of the total JSON size, driven mainly by DataFrame storage.

## Programmatic use

The import logic lives in `src/text2sql_eval_toolkit/database/` and can be used from Python when the package is installed:

```python
from pathlib import Path
from text2sql_eval_toolkit.database import JsonToDbImporter, connect, apply_schema, resolve_schema_path

conn = connect("data/text2sql_eval.db")
apply_schema(conn, resolve_schema_path())
conn.commit()

importer = JsonToDbImporter(conn=conn, data_root=Path("data"))
stats = importer.import_all(benchmark_ids=["beaver_test_10"])
conn.commit()
conn.close()
```

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `Data root does not exist` | Wrong `--data-root` or missing `data/` | Point `--data-root` at the directory containing `benchmarks.json`. |
| `Unknown benchmark id` | Typo or benchmark only in `test-benchmarks.json` | Check `data/benchmarks.json` and `data/test-benchmarks.json`. |
| `Predictions/eval file missing` | Benchmark registered but no results yet | Run inference/evaluation first, or use `--skip-predictions`. |
| Schema apply skipped | Database already initialized | Use a new `--database` path or remove the existing file. |
| Slow import / high memory | Large eval JSON | Import one benchmark at a time with `--benchmark-id`. |

## Related documentation

- [Database schema design](../../data/database-schema/design.md) — entity model, layers, and migration roadmap
- [Database schema README](../../data/database-schema/README.md) — DDL files and SQLite setup
- [Evaluation scripts](../evaluation/README.md) — benchmark-based evaluation against SQLite

## Source layout

| Path | Role |
|------|------|
| `scripts/migration/import_json_to_db.py` | CLI entry point |
| `src/text2sql_eval_toolkit/database/connection.py` | Database path resolution and schema application |
| `src/text2sql_eval_toolkit/database/json_importer.py` | Import logic (`JsonToDbImporter`) |
| `data/database-schema/schema.sql` | Target SQLite DDL |
