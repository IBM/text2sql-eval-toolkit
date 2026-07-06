# Database Schema Design for text2sql-eval-toolkit

This design describes the relational store for text2sql-eval-toolkit: benchmarks, the full pipeline (inference → execution → evaluation → LLM judge), dashboard queries, and durable job tracking.

**Status:** The toolkit uses **SQLite as the runtime source of truth** for predictions, execution results, evaluations, and summaries. The dashboard and pipeline read/write through `BenchmarkStore` (`data/text2sql_eval.db` by default). Legacy `*-predictions*.json` under `data/results/` is supported **only** via the one-time migration script [`scripts/migration/import_json_to_db.py`](../../scripts/migration/import_json_to_db.py).

## Diagrams

| Diagram | File |
|---------|------|
| System topology | [diagrams/architecture.md](./diagrams/architecture.md) |
| Entity relationships | [diagrams/entity-relationship.md](./diagrams/entity-relationship.md) |
| Pipeline integration | [diagrams/pipeline-integration.md](./diagrams/pipeline-integration.md) |

Full DDL: [schema.sql](./schema.sql)

---

## Design Goals

| Goal | How the schema addresses it |
|------|-----------------------------|
| Cover all benchmarks (BIRD, Spider, Beaver, Archer, …) | Normalized core fields + `JSONB` for benchmark-specific extras |
| Cover all result stages | Separate tables for predictions, execution, evaluation |
| Fast dashboard queries | Indexed columns, materialized summaries, category aggregates |
| Multi-pipeline per benchmark | `pipelines` (globally unique by `pipeline_id`) + unique `(record, pipeline)` constraints on predictions |
| Large DataFrames | Dedicated `result_dataframes` table with optional external blob refs |
| HF Hub distribution | `result_artifacts` + `hub_manifests` tables |
| Backward compatibility | JSON export/import views matching current file shapes |

---

## Recommended Database Topology

Use **one logical database** with two deployment profiles:

![Architecture](./diagrams/architecture.md)

| Profile | Engine | When |
|---------|--------|------|
| **Production / dashboard** | PostgreSQL 15+ | Concurrent writes, large eval files (GB), CI |
| **Local / single-user** | SQLite 3.35+ | Default DDL in [schema.sql](./schema.sql) |

PostgreSQL is recommended because eval files can exceed 100 MiB per benchmark, the dashboard does full-table scans with filters, and you need durable job state.

---

## Entity Relationship Overview

![Entity Relationship](./diagrams/entity-relationship.md)

**Key concept — `result_set`:** One logical container for all pipelines' predictions and evaluations for a benchmark run. Historically this corresponded to one `*-predictions_eval.json` file; the database supports multiple labeled result sets per benchmark (re-runs, different eval configs).

---

## Schema Layers

### Layer 1: Catalog & Benchmark Registry

Replaces `benchmarks.json` and gold question JSON files.

- `benchmarks` — registry entry per benchmark_id
- `benchmark_db_config` — db_type, db_folder, connection env var
- `benchmark_schema_snapshots` — full schema JSON (multi-db keyed by db_id)

### Layer 2: Benchmark Records (Gold Data)

Replaces per-benchmark question JSON arrays (one row per record).

- `benchmark_records` — `record_id`, question, db_id, evidence, difficulty, `extra_metadata` JSONB
- `record_gt_sql` — multiple ground-truth SQL variants per record
- `record_categories` — `meta.categories` tags (has_join, difficulty_simple, …)
- `record_features` — query complexity features from `meta.features`

### Layer 3: Result Sets & Pipelines

Models how multiple models/pipelines coexist in one predictions file.

- `result_sets` — one eval artifact container per benchmark (with optional label for history)
- `pipelines` — standalone definition table; `pipeline_id` is globally unique. Linked to result sets via `predictions`.

### Layer 4: Predictions, Execution & DataFrames

- `predictions` — one row per record × pipeline
- `prediction_inference` — predicted_sql, prompt, tokens, agentic trace fields
- `prediction_execution` — execution errors, logic_sql, DF references
- `record_ground_truth_execution` — gt_df shared across pipelines
- `result_dataframes` — large result tables (inline or external blob ref)

**Storage rule:** Inline `payload_text` when < 64 KiB; otherwise write to `storage_ref` and keep only a pointer in the DB.

### Layer 5: Evaluation Metrics

Wide table aligned with `METRIC_DEFINITIONS` in `src/text2sql_eval_toolkit/evaluation/metric_definitions.py`.

- `evaluations` — one row per prediction with columns for all binary/float/int/text metrics
- `llm_judge_configs` — registry of judge YAML configs (deduplicated by `config_hash`)
- `llm_judge_evaluations` — one row per `(prediction, llm_judge_config_ref)` with `llm_score`, `llm_explanation`
- `llm_judge_eval_summaries` — pre-aggregated LLM judge metrics per pipeline, per judge config, per category

### Layer 6: Pre-computed Aggregates

Replaces `*-predictions_eval_summary.json`.

- `eval_summaries` — per pipeline, per category (NULL category = overall)
- `metric_definitions` — DB-driven metric catalog for UI

### Layer 7: Operations, Jobs & Hub Sync

- `jobs` — durable pipeline job records (`inference`, `execution`, `eval`, `llm_judge`)
- `hub_manifests` — HF Hub manifest snapshots
- `result_artifacts` — exported file tracking (for Hub publish workflows)

#### `jobs` table

Each pipeline stage creates a row when it runs:

| `job_type` | Triggered by | Notes |
|------------|--------------|-------|
| `inference` | `run_pipeline()` (baseline or agentic) | One job per model/pipeline invocation |
| `execution` | `run_execution()` | SQL execution over stored predictions |
| `eval` | `run_evaluation()` without LLM judge | Deterministic metrics only |
| `llm_judge` | `run_evaluation(use_llm=True)` or `llm_judge_comparison.py` | Includes LLM-as-judge scoring |

Job lifecycle: `pending` → `running` → `completed` | `failed`. The dashboard evaluate endpoint creates a `pending` job, returns `job_id`, and polls `GET /api/jobs/{job_id}`.

---

## Dashboard API → SQL Query Mapping

| Dashboard endpoint | SQL approach |
|--------------------|--------------|
| `GET /api/benchmarks` | `benchmarks` JOIN `result_sets` + COUNT pipelines + `eval_summaries` |
| `GET /api/benchmarks/{id}/summary` | `eval_summaries WHERE category IS NULL`; optional `llm_judge_config_id` filters `llm_judge_eval_summaries` |
| `GET /api/benchmarks/{id}/summary/by-category` | `eval_summaries` + category aggregates; optional `llm_judge_config_id` |
| `GET /api/benchmarks/{id}/llm-judge-configs` | Distinct `llm_judge_configs` with stored results for this benchmark |
| `GET /api/benchmarks/{id}/errors` (paginated, filtered) | `predictions` JOIN `evaluations` JOIN `llm_judge_evaluations` (filtered by judge) |
| `GET /api/benchmarks/{id}/errors/{id}/detail` | Single-row join across inference + execution + evaluation + dataframes + selected judge |
| `GET /api/benchmarks/{id}/insights/binary-metric-confusion` | `GROUP BY pipeline` on two metric columns from `evaluations` |
| `GET /api/benchmarks/{id}/insights/cross-pipeline-binary-metric-confusion` | Self-join `evaluations` on `benchmark_record_id` |
| Profile compare (multi-benchmark) | Weighted merge of `eval_summaries` across benchmarks by category |
| `POST /api/benchmarks/{id}/playground/evaluate` | Read record + write ephemeral eval (or upsert `evaluations`) |
| `POST /api/benchmarks/{id}/evaluate` | Insert `jobs` row (`eval` or `llm_judge`); background worker runs `run_evaluation()` |
| `GET /api/jobs/{id}` | Poll job status from `jobs` table |
| `GET /api/benchmarks/{id}/jobs` | List recent jobs for a benchmark |

### Example — paginated error list with filters

```sql
SELECT
    br.record_id,
    br.question,
    p.pipeline_id AS pipeline_name,
    e.execution_accuracy,
    e.subset_non_empty_execution_accuracy,
    e.llm_score
FROM benchmark_records br
JOIN predictions pr ON pr.benchmark_record_id = br.id
JOIN pipelines p ON p.id = pr.pipeline_ref
JOIN evaluations e ON e.prediction_id = pr.id
JOIN record_categories rc ON rc.benchmark_record_id = br.id
WHERE br.benchmark_id = $1
  AND pr.result_set_id = $2
  AND ($3::text IS NULL OR br.question ILIKE '%' || $3 || '%' OR br.record_id ILIKE '%' || $3 || '%')
  AND ($4::text IS NULL OR rc.category = $4)
  AND ($5::bigint IS NULL OR p.id = $5)
  AND ($6::boolean IS FALSE OR e.execution_accuracy = 0)
ORDER BY br.sort_order
LIMIT $7 OFFSET $8;
```

---

## Toolkit Pipeline Integration

![Pipeline Integration](./diagrams/pipeline-integration.md)

| Stage | Tables written | Job type | Notes |
|-------|----------------|----------|-------|
| **Import benchmark** | `benchmarks`, `benchmark_db_config`, `benchmark_records`, … | — | Migration script or registry CRUD only |
| **Inference** | `result_sets`, `pipelines`, `predictions`, `prediction_inference` | `inference` | Upsert by `(benchmark_record_id, pipeline_id)` |
| **Execution** | `record_ground_truth_execution`, `prediction_execution`, `result_dataframes` | `execution` | GT DFs written once per record |
| **Evaluation** | `evaluations`, `eval_summaries` | `eval` | Deterministic metrics via `evaluate_prediction()` |
| **LLM judge** | `llm_judge_evaluations`, `llm_judge_eval_summaries` | `llm_judge` | When `use_llm=True` or multi-config comparison |
| **Export to JSON** | `result_artifacts` | — | Optional Hub publish artifact only |

---

## JSON Compatibility

SQLite uses a flat `v_eval_records_flat` view for dashboard queries; nested `*-predictions_eval.json` export can be built in application code for Hub publishing. PostgreSQL adds `v_eval_records_json` in [schema.postgres.sql](./schema.postgres.sql) for server-side JSON reconstruction.

**Runtime rule:** Pipeline stages persist to SQLite via `save_predictions_data()` / `save_eval_summary()`. JSON result files are not read or written during normal operation.

---

## Migration Path from Legacy JSON Files

| Step | Action |
|------|--------|
| 1 | Run DDL migrations (`schema_migrations` table tracks version) |
| 2 | Import `benchmarks.json` → `benchmarks` + `benchmark_db_config` |
| 3 | Import `benchmarks/*.json` → `benchmark_records` + related tables |
| 4 | Import `*-schema.json` → `benchmark_schema_snapshots` |
| 5 | Import `*-predictions.json` → `predictions` + inference + execution |
| 6 | Import `*-predictions_eval.json` → `evaluations` + `llm_judge_evaluations` |
| 7 | Import `*-predictions_eval_summary.json` → `eval_summaries` + `llm_judge_eval_summaries` |
| 8 | Compute category summaries from `evaluations` × `record_categories` (optional) |
| 9 | Run new experiments — inference/execution/eval write directly to SQLite |

Steps 1–8 are implemented by [`scripts/migration/import_json_to_db.py`](../../scripts/migration/import_json_to_db.py). See [`scripts/migration/README.md`](../../scripts/migration/README.md).

---

## Design Decisions & Trade-offs

| Decision | Rationale |
|----------|-----------|
| **Wide `evaluations` table** vs EAV | Fixed metric set (~25 fields); dashboard filters need column indexes |
| **`JSONB extra_metadata`** on records | Beaver `mapping`, Archer `reasoning_type`, Spider `query_toks` without schema churn |
| **Separate `result_dataframes`** | Keeps list queries fast; 90%+ of eval file size is DFs |
| **`result_sets` with labels** | Supports re-evaluation history; current files are implicitly one set |
| **Materialized `eval_summaries`** | Eliminates per-request full-file aggregation in `server.py` |
| **Single DB, not microservices** | Toolkit is monolithic; one connection string via `TEXT2SQL_DATABASE_URL` |

---

## Environment Configuration

```bash
# SQLite database (default)
TEXT2SQL_DATABASE_URL=sqlite:///./data/text2sql_eval.db
# production: postgresql://user:pass@localhost:5432/text2sql_eval

# Data root for benchmark catalog JSON and database files
TEXT2SQL_DATA_ROOT=./data
```

Benchmark catalog files (`benchmarks.json`, gold question JSON, schema JSON) remain on disk. **Result artifacts** (predictions, eval, summaries) live in SQLite.

---

## Estimated Scale

| Entity | Spider Dev (1,034 × 10 pipelines) | BIRD (500 × 10 pipelines) |
|--------|-----------------------------------|---------------------------|
| `benchmark_records` | 1,034 | 500 |
| `predictions` | ~10,340 | ~5,000 |
| `evaluations` | ~10,340 | ~5,000 |
| `result_dataframes` | ~20K–40K rows | ~10K–20K rows |
| `eval_summaries` | ~10 pipelines × ~30 categories | similar |

Row counts are manageable; **DataFrame storage** is the main size driver (plan for 1–10 GB per full benchmark suite with all pipelines).

---

## Mapping to Current File Model

| Legacy artifact (migration input only) | Database equivalent |
|--------------------------------------|---------------------|
| `data/benchmarks.json` | `benchmarks` + `benchmark_db_config` |
| `data/benchmarks/{id}.json` | `benchmark_records` + `record_gt_sql` + `record_categories` |
| `data/benchmarks/{id}-schema.json` | `benchmark_schema_snapshots` |
| `data/results/{id}-predictions.json` | `predictions` + `prediction_inference` + `prediction_execution` |
| `data/results/{id}-predictions_eval.json` | `evaluations` + `result_dataframes` + `llm_judge_evaluations` |
| `data/results/{id}-predictions_eval_summary.json` | `eval_summaries` + `llm_judge_eval_summaries` |
| HF Hub `manifest.json` | `hub_manifests` + `result_artifacts` |

---

## Implementation status

1. ~~Schema and `TEXT2SQL_DATABASE_URL`~~ — done ([schema.sql](./schema.sql), migration v4 decouples pipelines from result_sets)
2. ~~JSON import script~~ — [`scripts/migration/import_json_to_db.py`](../../scripts/migration/import_json_to_db.py)
3. ~~Repository layer (`BenchmarkStore`)~~ — used by pipeline, dashboard, and scripts
4. ~~Pipeline writes to SQLite~~ — inference, execution, eval, LLM judge
5. ~~Dashboard reads from SQLite~~ — summaries, errors, insights; optional `llm_judge_config_id` filter
6. ~~Durable `jobs` table~~ — pipeline stages + dashboard evaluate endpoint
7. **Optional:** Hub fetch → auto-import; export-from-DB before Hub upload
