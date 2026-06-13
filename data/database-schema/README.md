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

Schema and JSON import tooling are available. The dashboard and evaluation pipeline still read/write JSON files by default; database-backed storage is the target for future dashboard and pipeline integration.
