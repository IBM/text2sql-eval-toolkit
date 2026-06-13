-- text2sql-eval-toolkit database schema
-- Target: SQLite 3.35+ (JSON1 extension)
-- Design doc: docs/database-schema/design.md
--
-- Apply:
--   sqlite3 data/text2sql_eval.db < docs/database-schema/schema.sql
--
-- PostgreSQL extras (JSON export view): schema.postgres.sql

PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

-- ============================================================
-- SCHEMA VERSIONING
-- ============================================================

CREATE TABLE schema_migrations (
    version     INTEGER PRIMARY KEY,
    applied_at  TEXT NOT NULL DEFAULT (datetime('now')),
    description TEXT
);

-- ============================================================
-- LAYER 1: CATALOG & BENCHMARK REGISTRY
-- Replaces benchmarks.json
-- ============================================================

CREATE TABLE benchmarks (
    benchmark_id    TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    description     TEXT,
    logo_path       TEXT,
    is_test_subset  INTEGER NOT NULL DEFAULT 0 CHECK (is_test_subset IN (0, 1)),
    num_records     INTEGER,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE benchmark_db_config (
    benchmark_id              TEXT PRIMARY KEY REFERENCES benchmarks(benchmark_id) ON DELETE CASCADE,
    db_type                   TEXT NOT NULL CHECK (db_type IN ('sqlite', 'postgres', 'mysql', 'db2', 'presto')),
    db_folder                 TEXT,
    schema_name               TEXT,
    connection_string_env_var TEXT,
    extra_config              TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(extra_config))
);

CREATE TABLE benchmark_schema_snapshots (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    benchmark_id TEXT NOT NULL REFERENCES benchmarks(benchmark_id) ON DELETE CASCADE,
    schema_json  TEXT NOT NULL CHECK (json_valid(schema_json)),
    source_path  TEXT,
    schema_hash  TEXT NOT NULL,
    is_current   INTEGER NOT NULL DEFAULT 1 CHECK (is_current IN (0, 1)),
    created_at   TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (benchmark_id, schema_hash)
);

-- Only one current schema per benchmark (SQLite partial unique index)
CREATE UNIQUE INDEX idx_schema_current ON benchmark_schema_snapshots(benchmark_id) WHERE is_current = 1;

-- ============================================================
-- LAYER 2: BENCHMARK RECORDS (GOLD DATA)
-- Replaces per-benchmark question JSON arrays (one row per record)
-- ============================================================

CREATE TABLE benchmark_records (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    benchmark_id   TEXT NOT NULL REFERENCES benchmarks(benchmark_id) ON DELETE CASCADE,
    record_id      TEXT NOT NULL,
    db_id          TEXT,
    question       TEXT NOT NULL,
    utterance      TEXT,
    evidence       TEXT,
    difficulty     TEXT,
    extra_metadata TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(extra_metadata)),
    sort_order     INTEGER,
    created_at     TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at     TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (benchmark_id, record_id)
);

CREATE INDEX idx_records_benchmark ON benchmark_records(benchmark_id);
CREATE INDEX idx_records_db ON benchmark_records(benchmark_id, db_id);

CREATE TABLE record_gt_sql (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    benchmark_record_id INTEGER NOT NULL REFERENCES benchmark_records(id) ON DELETE CASCADE,
    ordinal             INTEGER NOT NULL DEFAULT 0,
    sql_text            TEXT NOT NULL,
    is_canonical        INTEGER NOT NULL DEFAULT 0 CHECK (is_canonical IN (0, 1)),
    UNIQUE (benchmark_record_id, ordinal)
);

CREATE TABLE record_categories (
    benchmark_record_id INTEGER NOT NULL REFERENCES benchmark_records(id) ON DELETE CASCADE,
    category            TEXT NOT NULL,
    PRIMARY KEY (benchmark_record_id, category)
);

CREATE INDEX idx_record_categories_cat ON record_categories(category);

CREATE TABLE record_features (
    benchmark_record_id       INTEGER PRIMARY KEY REFERENCES benchmark_records(id) ON DELETE CASCADE,
    query_table_count         INTEGER,
    query_column_count        INTEGER,
    query_nested_count        INTEGER,
    query_aggregate_count     INTEGER,
    query_sort_count          INTEGER,
    query_window_func_count   INTEGER,
    query_join_count          INTEGER
);

-- ============================================================
-- LAYER 3: RESULT SETS & PIPELINES
-- ============================================================

CREATE TABLE result_sets (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    benchmark_id     TEXT NOT NULL REFERENCES benchmarks(benchmark_id) ON DELETE CASCADE,
    label            TEXT NOT NULL DEFAULT 'default',
    status           TEXT NOT NULL DEFAULT 'inference'
        CHECK (status IN ('inference', 'executed', 'evaluated', 'archived')),
    source           TEXT,
    file_stem        TEXT,
    created_at       TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at       TEXT NOT NULL DEFAULT (datetime('now')),
    evaluated_at     TEXT,
    UNIQUE (benchmark_id, label)
);

CREATE INDEX idx_result_sets_benchmark ON result_sets(benchmark_id);

CREATE TABLE pipelines (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    result_set_id    INTEGER NOT NULL REFERENCES result_sets(id) ON DELETE CASCADE,
    pipeline_id      TEXT NOT NULL,
    pipeline_type    TEXT NOT NULL DEFAULT 'zero_shot'
        CHECK (pipeline_type IN ('zero_shot', 'agentic', 'custom')),
    model_name       TEXT,
    model_parameters TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(model_parameters)),
    created_at       TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (result_set_id, pipeline_id)
);

CREATE INDEX idx_pipelines_result_set ON pipelines(result_set_id);

-- ============================================================
-- LAYER 4: DATAFRAMES (created before execution tables that reference it)
-- ============================================================

CREATE TABLE result_dataframes (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    format       TEXT NOT NULL DEFAULT 'pandas_split',
    payload      TEXT CHECK (payload IS NULL OR json_valid(payload)),
    payload_text TEXT,
    byte_size    INTEGER,
    is_truncated INTEGER NOT NULL DEFAULT 0 CHECK (is_truncated IN (0, 1)),
    storage_ref  TEXT,
    created_at   TEXT NOT NULL DEFAULT (datetime('now'))
);

-- ============================================================
-- LAYER 4 (cont.): PREDICTIONS, EXECUTION
-- ============================================================

CREATE TABLE predictions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    benchmark_record_id INTEGER NOT NULL REFERENCES benchmark_records(id) ON DELETE CASCADE,
    pipeline_ref        INTEGER NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    result_set_id       INTEGER NOT NULL REFERENCES result_sets(id) ON DELETE CASCADE,
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at          TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (benchmark_record_id, pipeline_ref)
);

CREATE INDEX idx_predictions_result_set ON predictions(result_set_id);
CREATE INDEX idx_predictions_pipeline ON predictions(pipeline_ref);

CREATE TABLE prediction_inference (
    prediction_id           INTEGER PRIMARY KEY REFERENCES predictions(id) ON DELETE CASCADE,
    predicted_sql           TEXT,
    prompt                  TEXT,
    inference_time_ms       REAL,
    inference_error         TEXT,
    response_info           TEXT CHECK (response_info IS NULL OR json_valid(response_info)),
    agent_attempts          TEXT CHECK (agent_attempts IS NULL OR json_valid(agent_attempts)),
    agent_reasoning         TEXT,
    agent_trace             TEXT CHECK (agent_trace IS NULL OR json_valid(agent_trace)),
    token_usage_per_attempt TEXT CHECK (token_usage_per_attempt IS NULL OR json_valid(token_usage_per_attempt)),
    prompt_tokens           INTEGER,
    completion_tokens       INTEGER,
    total_tokens            INTEGER
);

CREATE TABLE prediction_execution (
    prediction_id             INTEGER PRIMARY KEY REFERENCES predictions(id) ON DELETE CASCADE,
    sql_execution_error       TEXT,
    execution_time_ms         REAL,
    logic_sql                 TEXT,
    logic_sql_execution_error TEXT,
    logic_execution_time_ms   REAL,
    predicted_df_id           INTEGER REFERENCES result_dataframes(id),
    logic_df_id               INTEGER REFERENCES result_dataframes(id),
    predicted_df_truncated    INTEGER NOT NULL DEFAULT 0 CHECK (predicted_df_truncated IN (0, 1)),
    logic_df_truncated        INTEGER NOT NULL DEFAULT 0 CHECK (logic_df_truncated IN (0, 1))
);

CREATE TABLE record_ground_truth_execution (
    benchmark_record_id    INTEGER PRIMARY KEY REFERENCES benchmark_records(id) ON DELETE CASCADE,
    gt_sql_execution_error TEXT,
    gt_df_id               INTEGER REFERENCES result_dataframes(id),
    gt_df_ids              TEXT CHECK (gt_df_ids IS NULL OR json_valid(gt_df_ids))
);

-- ============================================================
-- LAYER 5: EVALUATION METRICS
-- Aligned with evaluation/metric_definitions.py
-- ============================================================

-- Registry of LLM-as-judge configurations (model + prompt template).
-- Deduplicated by config_hash so the same judge can score many predictions.
CREATE TABLE llm_judge_configs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    config_name  TEXT NOT NULL,
    config_hash  TEXT NOT NULL,
    model_id     TEXT,
    config_json  TEXT NOT NULL CHECK (json_valid(config_json)),
    created_at   TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (config_hash)
);

CREATE INDEX idx_llm_judge_configs_name ON llm_judge_configs(config_name);

-- Deterministic metrics: one row per prediction (judge-independent).
CREATE TABLE evaluations (
    prediction_id INTEGER PRIMARY KEY REFERENCES predictions(id) ON DELETE CASCADE,

    -- Execution match (binary 0/1)
    execution_accuracy                  INTEGER CHECK (execution_accuracy IN (0, 1)),
    non_empty_execution_accuracy        INTEGER CHECK (non_empty_execution_accuracy IN (0, 1)),
    subset_non_empty_execution_accuracy INTEGER CHECK (subset_non_empty_execution_accuracy IN (0, 1)),
    logic_execution_accuracy            INTEGER CHECK (logic_execution_accuracy IN (0, 1)),
    bird_execution_accuracy             INTEGER CHECK (bird_execution_accuracy IN (0, 1)),

    -- SQL equivalence
    sql_exact_match               INTEGER CHECK (sql_exact_match IN (0, 1)),
    sqlglot_equivalence           INTEGER CHECK (sqlglot_equivalence IN (0, 1)),
    sqlglot_optimized_equivalence INTEGER CHECK (sqlglot_optimized_equivalence IN (0, 1)),
    sqlparse_equivalence          INTEGER CHECK (sqlparse_equivalence IN (0, 1)),
    sql_syntactic_equivalence     INTEGER CHECK (sql_syntactic_equivalence IN (0, 1)),

    -- Parsing
    is_sqlglot_parsable INTEGER CHECK (is_sqlglot_parsable IN (0, 1)),
    is_sqlparse_parsable INTEGER CHECK (is_sqlparse_parsable IN (0, 1)),

    -- Timing & tokens (copied from prediction at eval time for export convenience)
    prompt_tokens     INTEGER,
    completion_tokens INTEGER,
    total_tokens      INTEGER,
    inference_time_ms REAL,
    execution_time_ms REAL,

    -- Errors
    df_error          INTEGER CHECK (df_error IN (0, 1)),
    df_error_message  TEXT,
    eval_error        INTEGER CHECK (eval_error IN (0, 1)),
    eval_error_message TEXT,

    -- Matched GT (when subset match succeeds)
    matched_gt_sql    TEXT,
    matched_gt_df_id  INTEGER REFERENCES result_dataframes(id),

    evaluated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_eval_exec_acc ON evaluations(execution_accuracy);
CREATE INDEX idx_eval_subset_acc ON evaluations(subset_non_empty_execution_accuracy);

-- LLM judge metrics: one row per (prediction, judge config).
CREATE TABLE llm_judge_evaluations (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    prediction_id        INTEGER NOT NULL REFERENCES predictions(id) ON DELETE CASCADE,
    llm_judge_config_ref INTEGER NOT NULL REFERENCES llm_judge_configs(id) ON DELETE RESTRICT,
    llm_score            REAL,
    llm_explanation      TEXT,
    llm_judge_error      TEXT,
    prompt_tokens        INTEGER,
    completion_tokens    INTEGER,
    total_tokens         INTEGER,
    judge_time_ms        REAL,
    evaluated_at         TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (prediction_id, llm_judge_config_ref)
);

CREATE INDEX idx_llm_judge_eval_prediction ON llm_judge_evaluations(prediction_id);
CREATE INDEX idx_llm_judge_eval_config ON llm_judge_evaluations(llm_judge_config_ref);
CREATE INDEX idx_llm_judge_eval_score ON llm_judge_evaluations(llm_score);

-- ============================================================
-- LAYER 6: PRE-COMPUTED AGGREGATES
-- Replaces *-predictions_eval_summary.json
-- ============================================================

CREATE TABLE eval_summaries (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    result_set_id     INTEGER NOT NULL REFERENCES result_sets(id) ON DELETE CASCADE,
    pipeline_ref      INTEGER NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    category          TEXT,
    num_records       INTEGER NOT NULL,
    num_predictions   INTEGER NOT NULL,
    num_evaluated     INTEGER NOT NULL,
    num_inference_errors INTEGER NOT NULL DEFAULT 0,
    num_df_errors     INTEGER NOT NULL DEFAULT 0,
    num_eval_errors   INTEGER NOT NULL DEFAULT 0,

    execution_accuracy_avg                     REAL,
    execution_accuracy_stddev                  REAL,
    non_empty_execution_accuracy_avg           REAL,
    non_empty_execution_accuracy_stddev        REAL,
    subset_non_empty_execution_accuracy_avg    REAL,
    subset_non_empty_execution_accuracy_stddev REAL,
    logic_execution_accuracy_avg               REAL,
    logic_execution_accuracy_stddev            REAL,
    bird_execution_accuracy_avg                REAL,
    bird_execution_accuracy_stddev             REAL,
    sql_exact_match_avg                        REAL,
    sql_exact_match_stddev                     REAL,
    sqlglot_equivalence_avg                    REAL,
    sqlglot_equivalence_stddev                 REAL,
    sqlglot_optimized_equivalence_avg          REAL,
    sqlglot_optimized_equivalence_stddev       REAL,
    sqlparse_equivalence_avg                   REAL,
    sqlparse_equivalence_stddev                REAL,
    sql_syntactic_equivalence_avg              REAL,
    sql_syntactic_equivalence_stddev           REAL,
    is_sqlglot_parsable_avg                    REAL,
    is_sqlglot_parsable_stddev                 REAL,
    is_sqlparse_parsable_avg                   REAL,
    is_sqlparse_parsable_stddev                REAL,
    eval_error_avg                             REAL,
    eval_error_stddev                          REAL,
    df_error_avg                               REAL,
    df_error_stddev                            REAL,

    sum_total_tokens      INTEGER,
    sum_inference_time_ms REAL,
    sum_execution_time_ms REAL,

    computed_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (result_set_id, pipeline_ref, category)
);

CREATE INDEX idx_eval_summaries_rs ON eval_summaries(result_set_id);
CREATE INDEX idx_eval_summaries_cat ON eval_summaries(result_set_id, category);

-- Per-judge aggregates (replaces llm_score_* columns formerly on eval_summaries).
CREATE TABLE llm_judge_eval_summaries (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    result_set_id        INTEGER NOT NULL REFERENCES result_sets(id) ON DELETE CASCADE,
    pipeline_ref         INTEGER NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    llm_judge_config_ref INTEGER NOT NULL REFERENCES llm_judge_configs(id) ON DELETE RESTRICT,
    category             TEXT,
    num_evaluated        INTEGER NOT NULL,
    num_judge_errors     INTEGER NOT NULL DEFAULT 0,
    llm_score_avg        REAL,
    llm_score_stddev     REAL,
    sum_judge_tokens     INTEGER,
    computed_at          TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (result_set_id, pipeline_ref, llm_judge_config_ref, category)
);

CREATE INDEX idx_llm_judge_summaries_rs ON llm_judge_eval_summaries(result_set_id);
CREATE INDEX idx_llm_judge_summaries_cat ON llm_judge_eval_summaries(result_set_id, category);

CREATE TABLE metric_definitions (
    name          TEXT PRIMARY KEY,
    display_group TEXT NOT NULL,
    description   TEXT NOT NULL,
    value_type    TEXT NOT NULL CHECK (value_type IN ('binary', 'float', 'int', 'text')),
    sort_order    INTEGER NOT NULL DEFAULT 0
);

-- ============================================================
-- LAYER 7: OPERATIONS, JOBS & HUB SYNC
-- ============================================================

CREATE TABLE jobs (
    id            TEXT PRIMARY KEY,
    job_type      TEXT NOT NULL
        CHECK (job_type IN ('evaluate', 'fetch_hub', 'import_json', 'export_json')),
    status        TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled')),
    benchmark_id  TEXT REFERENCES benchmarks(benchmark_id),
    result_set_id INTEGER REFERENCES result_sets(id),
    progress      REAL DEFAULT 0,
    message       TEXT,
    error         TEXT,
    params        TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(params)),
    started_at    TEXT,
    completed_at  TEXT,
    created_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_jobs_status ON jobs(status, created_at);

CREATE TABLE hub_manifests (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    schema_version         INTEGER NOT NULL,
    toolkit_version_compat TEXT,
    generated_at           TEXT,
    total_size_bytes       INTEGER,
    manifest_json          TEXT NOT NULL CHECK (json_valid(manifest_json)),
    synced_at              TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE result_artifacts (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    result_set_id INTEGER NOT NULL REFERENCES result_sets(id) ON DELETE CASCADE,
    artifact_type TEXT NOT NULL,
    file_name     TEXT NOT NULL,
    file_path     TEXT,
    byte_size     INTEGER,
    content_hash  TEXT,
    hub_url       TEXT,
    exported_at   TEXT,
    UNIQUE (result_set_id, artifact_type)
);

-- ============================================================
-- SQLITE VIEWS
-- Nested *-predictions_eval.json export is built in application code.
-- Flat helpers below support common dashboard queries.
-- ============================================================

-- One row per prediction; deterministic metrics only (no LLM judge columns).
CREATE VIEW v_eval_records_flat AS
SELECT
    rs.benchmark_id,
    rs.id AS result_set_id,
    br.id AS benchmark_record_internal_id,
    br.record_id,
    br.question,
    br.utterance,
    br.db_id,
    br.evidence,
    br.difficulty,
    br.extra_metadata,
    br.sort_order,
    pip.pipeline_id AS pipeline_name,
    pip.model_name,
    pi.predicted_sql,
    pi.prompt,
    pi.inference_time_ms,
    pi.inference_error,
    pi.prompt_tokens,
    pi.completion_tokens,
    pi.total_tokens,
    pe.execution_time_ms,
    pe.sql_execution_error,
    pe.logic_sql,
    pdf.payload_text AS predicted_df,
    ldf.payload_text AS logic_df,
    e.execution_accuracy,
    e.non_empty_execution_accuracy,
    e.subset_non_empty_execution_accuracy,
    e.logic_execution_accuracy,
    e.bird_execution_accuracy,
    e.sql_exact_match,
    e.sqlglot_equivalence,
    e.df_error,
    e.df_error_message,
    e.eval_error,
    e.eval_error_message
FROM result_sets rs
JOIN benchmark_records br ON br.benchmark_id = rs.benchmark_id
LEFT JOIN predictions pr ON pr.benchmark_record_id = br.id AND pr.result_set_id = rs.id
LEFT JOIN pipelines pip ON pip.id = pr.pipeline_ref
LEFT JOIN prediction_inference pi ON pi.prediction_id = pr.id
LEFT JOIN prediction_execution pe ON pe.prediction_id = pr.id
LEFT JOIN result_dataframes pdf ON pdf.id = pe.predicted_df_id
LEFT JOIN result_dataframes ldf ON ldf.id = pe.logic_df_id
LEFT JOIN evaluations e ON e.prediction_id = pr.id;

-- One row per (prediction, LLM judge config).
CREATE VIEW v_llm_judge_evaluations_flat AS
SELECT
    rs.benchmark_id,
    rs.id AS result_set_id,
    br.record_id,
    pip.pipeline_id AS pipeline_name,
    ljc.id AS llm_judge_config_id,
    ljc.config_name AS llm_judge_config_name,
    ljc.model_id AS llm_judge_model_id,
    lje.llm_score,
    lje.llm_explanation,
    lje.llm_judge_error,
    lje.prompt_tokens AS judge_prompt_tokens,
    lje.completion_tokens AS judge_completion_tokens,
    lje.total_tokens AS judge_total_tokens,
    lje.judge_time_ms,
    lje.evaluated_at AS judge_evaluated_at
FROM result_sets rs
JOIN benchmark_records br ON br.benchmark_id = rs.benchmark_id
JOIN predictions pr ON pr.benchmark_record_id = br.id AND pr.result_set_id = rs.id
JOIN pipelines pip ON pip.id = pr.pipeline_ref
JOIN llm_judge_evaluations lje ON lje.prediction_id = pr.id
JOIN llm_judge_configs ljc ON ljc.id = lje.llm_judge_config_ref;

-- ============================================================
-- SEED: metric_definitions (from metric_definitions.py)
-- ============================================================

INSERT OR IGNORE INTO metric_definitions (name, display_group, description, value_type, sort_order) VALUES
    ('execution_accuracy', 'Execution match', '1 if the predicted result table exactly matches the ground-truth result; 0 otherwise.', 'binary', 1),
    ('non_empty_execution_accuracy', 'Execution match', '1 if execution_accuracy holds and the result is non-empty; 0 otherwise.', 'binary', 2),
    ('subset_non_empty_execution_accuracy', 'Execution match', '1 if the predicted result is a non-empty subset or superset of the ground-truth result.', 'binary', 3),
    ('logic_execution_accuracy', 'Execution match', 'Compares logic_df to GT when present; otherwise same as subset match.', 'binary', 4),
    ('bird_execution_accuracy', 'Execution match', '1 if the BIRD-style relaxed dataframe comparison passes.', 'binary', 5),
    ('sql_exact_match', 'SQL equivalence', '1 if predicted SQL exactly matches ground-truth SQL after normalization.', 'binary', 10),
    ('sqlglot_equivalence', 'SQL equivalence', '1 if SQLGlot considers the queries equivalent.', 'binary', 11),
    ('sqlglot_optimized_equivalence', 'SQL equivalence', '1 if SQLGlot optimized forms are equivalent.', 'binary', 12),
    ('sqlparse_equivalence', 'SQL equivalence', '1 if sqlparse-based equivalence holds.', 'binary', 13),
    ('sql_syntactic_equivalence', 'SQL equivalence', '1 if any SQL equivalence check passes.', 'binary', 14),
    ('is_sqlglot_parsable', 'Parsing', '1 if the predicted SQL parses with SQLGlot.', 'binary', 20),
    ('is_sqlparse_parsable', 'Parsing', '1 if the predicted SQL parses with sqlparse.', 'binary', 21),
    ('llm_score', 'LLM judge', 'Model-based score (0, 0.5, or 1) when LLM-as-judge is enabled.', 'float', 30),
    ('llm_explanation', 'LLM judge', 'Short explanation from the LLM judge.', 'text', 31),
    ('llm_judge_error', 'LLM judge', 'Present if the LLM judge call failed.', 'text', 32),
    ('prompt_tokens', 'Timing and tokens', 'Prompt tokens from token_usage.', 'int', 40),
    ('completion_tokens', 'Timing and tokens', 'Completion tokens from token_usage.', 'int', 41),
    ('total_tokens', 'Timing and tokens', 'Total tokens from token_usage.', 'int', 42),
    ('inference_time_ms', 'Timing and tokens', 'Inference latency from the prediction.', 'float', 43),
    ('execution_time_ms', 'Timing and tokens', 'SQL execution time from the prediction.', 'float', 44),
    ('df_error', 'Errors', '1 if the predicted result could not be parsed or was missing.', 'binary', 50),
    ('df_error_message', 'Errors', 'Details when df_error is set.', 'text', 51),
    ('eval_error', 'Errors', '1 if evaluation raised an unexpected exception.', 'binary', 52),
    ('eval_error_message', 'Errors', 'Exception repr when eval_error is set.', 'text', 53),
    ('gt_sql', 'Ground truth (when matched)', 'GT SQL that produced the accepted match.', 'text', 60),
    ('gt_df', 'Ground truth (when matched)', 'Serialized GT dataframe for the matched GT SQL.', 'text', 61);

INSERT OR IGNORE INTO schema_migrations (version, description) VALUES (1, 'Initial schema design (SQLite)');
INSERT OR IGNORE INTO schema_migrations (version, description) VALUES (2, 'Multi LLM judge per prediction (llm_judge_configs, llm_judge_evaluations)');
