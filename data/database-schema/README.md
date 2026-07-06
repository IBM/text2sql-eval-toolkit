# Database Schema Design

Relational database schema for text2sql-eval-toolkit results and benchmarks.

## Contents

| File | Description |
|------|-------------|
| [design.md](./design.md) | Full design document: goals, layers, API mapping, migration |
| [schema.sql](./schema.sql) | Complete DDL (SQLite 3.35+) |
| [schema.postgres.sql](./schema.postgres.sql) | PostgreSQL extras (nested JSON export view) |
| [diagrams/architecture.md](./diagrams/architecture.md) | System topology diagram |
| [diagrams/entity-relationship.md](./diagrams/entity-relationship.md) | ER diagram |
| [diagrams/pipeline-integration.md](./diagrams/pipeline-integration.md) | Toolkit pipeline write sequence |

## Rendering diagrams

Mermaid files can be previewed in GitHub, VS Code (Mermaid extension), or rendered to SVG/PNG:

```bash
npx @mermaid-js/mermaid-cli -i data/database-schema/diagrams/architecture.md -o data/database-schema/diagrams/architecture.svg
```

## Apply schema (SQLite)

```bash
sqlite3 data/text2sql_eval.db < data/database-schema/schema.sql
```

Default path from env: `TEXT2SQL_DATABASE_URL=sqlite:///./data/text2sql_eval.db`

## Incremental migrations

New checkouts use the full DDL in `schema.sql`. If you already have a
`text2sql_eval.db` from an older toolkit version, schema upgrades run
automatically when the app opens the database (`apply_pending_migrations()` in
`src/text2sql_eval_toolkit/database/migrations.py`).

The latest incremental change is **v4**: `pipelines` no longer has
`result_set_id`; each `pipeline_id` is globally unique. See
[`scripts/migration/README.md`](../../scripts/migration/README.md) for manual
upgrade steps and version history.

## Migrate from JSON

Legacy benchmark data on disk can be imported into SQLite with the migration script:

```bash
python3 scripts/migration/import_json_to_db.py --init
```

See [scripts/migration/README.md](../../scripts/migration/README.md) for full usage, options, and file-to-table mapping.

## SQLite vs PostgreSQL

| Feature | SQLite (`schema.sql`) | PostgreSQL (`schema.postgres.sql`) |
|---------|----------------------|-----------------------------------|
| JSON columns | `TEXT` + `json_valid()` | `JSONB` |
| Booleans | `INTEGER` (0/1) | `BOOLEAN` |
| Timestamps | `TEXT` (`datetime('now')`) | `TIMESTAMPTZ` |
| Enums | `TEXT` + `CHECK` | `CREATE TYPE ... AS ENUM` |
| Job IDs | `TEXT` (app-generated UUID) | `UUID DEFAULT gen_random_uuid()` |
| JSON export view | Flat `v_eval_records_flat` | Nested `v_eval_records_json` |

## Status

The toolkit uses **SQLite as the runtime store** for predictions, execution, evaluations, and summaries (`data/text2sql_eval.db` by default). The dashboard and pipeline read/write through `BenchmarkStore`.

Legacy `*-predictions*.json` files under `data/results/` are imported **only** via [`scripts/migration/import_json_to_db.py`](../../scripts/migration/import_json_to_db.py). See [design.md](./design.md) for jobs, LLM judge config filtering, and API mapping.
