-- Migration v4: Decouple pipelines from result_sets
--
-- Previously, the pipelines table had a result_set_id FK, causing duplicate
-- rows for the same logical pipeline across result sets. This migration makes
-- pipelines a standalone definition table keyed only by pipeline_id.
--
-- The predictions table already carries result_set_id, so the link between
-- a pipeline's predictions and a result set is preserved there.
--
-- Applied automatically on startup via apply_pending_migrations() in
-- src/text2sql_eval_toolkit/database/migrations.py.
--
-- Manual run (optional):
--   sqlite3 data/text2sql_eval.db < scripts/migration/004_decouple_pipelines_from_result_sets.sql

PRAGMA foreign_keys = OFF;
BEGIN TRANSACTION;

-- Step 1: Create the new pipelines table (no result_set_id)
CREATE TABLE pipelines_new (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_id      TEXT NOT NULL UNIQUE,
    pipeline_type    TEXT NOT NULL DEFAULT 'zero_shot'
        CHECK (pipeline_type IN ('zero_shot', 'agentic', 'custom')),
    model_name       TEXT,
    model_parameters TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(model_parameters)),
    created_at       TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Step 2: Populate with deduplicated pipelines.
-- For each unique pipeline_id, take the row with the lowest id (earliest created).
INSERT INTO pipelines_new (id, pipeline_id, pipeline_type, model_name, model_parameters, created_at)
SELECT id, pipeline_id, pipeline_type, model_name, model_parameters, created_at
FROM pipelines
WHERE id IN (
    SELECT MIN(id) FROM pipelines GROUP BY pipeline_id
);

-- Step 3: Build a mapping from old duplicate pipeline ids to the canonical id.
-- We'll use this to update FK references.
CREATE TEMPORARY TABLE pipeline_id_remap (
    old_id      INTEGER PRIMARY KEY,
    canonical_id INTEGER NOT NULL
);

INSERT INTO pipeline_id_remap (old_id, canonical_id)
SELECT p.id AS old_id,
       canonical.id AS canonical_id
FROM pipelines p
JOIN (
    SELECT pipeline_id, MIN(id) AS id
    FROM pipelines
    GROUP BY pipeline_id
) canonical ON canonical.pipeline_id = p.pipeline_id;

-- Step 4: Update predictions to point to canonical pipeline ids
UPDATE predictions
SET pipeline_ref = (
    SELECT canonical_id FROM pipeline_id_remap WHERE old_id = predictions.pipeline_ref
)
WHERE pipeline_ref IN (
    SELECT old_id FROM pipeline_id_remap WHERE old_id != canonical_id
);

-- Step 5: Update eval_summaries to point to canonical pipeline ids
UPDATE eval_summaries
SET pipeline_ref = (
    SELECT canonical_id FROM pipeline_id_remap WHERE old_id = eval_summaries.pipeline_ref
)
WHERE pipeline_ref IN (
    SELECT old_id FROM pipeline_id_remap WHERE old_id != canonical_id
);

-- Step 6: Update llm_judge_eval_summaries to point to canonical pipeline ids
UPDATE llm_judge_eval_summaries
SET pipeline_ref = (
    SELECT canonical_id FROM pipeline_id_remap WHERE old_id = llm_judge_eval_summaries.pipeline_ref
)
WHERE pipeline_ref IN (
    SELECT old_id FROM pipeline_id_remap WHERE old_id != canonical_id
);

-- Step 7: Handle potential unique constraint violations in eval_summaries.
-- After remapping, (result_set_id, pipeline_ref, category) may have duplicates.
-- Keep the row with the latest computed_at.
DELETE FROM eval_summaries
WHERE id NOT IN (
    SELECT MAX(id)
    FROM eval_summaries
    GROUP BY result_set_id, pipeline_ref, category
);

-- Same for llm_judge_eval_summaries
DELETE FROM llm_judge_eval_summaries
WHERE id NOT IN (
    SELECT MAX(id)
    FROM llm_judge_eval_summaries
    GROUP BY result_set_id, pipeline_ref, llm_judge_config_ref, category
);

-- Step 8: Handle potential unique constraint violations in predictions.
-- After remapping, (benchmark_record_id, pipeline_ref) may have duplicates.
-- Keep the most recent prediction (highest id).
DELETE FROM predictions
WHERE id NOT IN (
    SELECT MAX(id)
    FROM predictions
    GROUP BY benchmark_record_id, pipeline_ref
);

-- Step 9: Drop views that depend on the old pipelines table
DROP VIEW IF EXISTS v_eval_records_flat;
DROP VIEW IF EXISTS v_llm_judge_evaluations_flat;

-- Step 10: Drop old table, rename new one
DROP TABLE pipelines;
ALTER TABLE pipelines_new RENAME TO pipelines;

-- Step 11: Recreate the index (no longer on result_set_id)
CREATE INDEX idx_pipelines_pipeline_id ON pipelines(pipeline_id);

-- Step 12: Clean up temp table
DROP TABLE IF EXISTS pipeline_id_remap;

-- Step 13: Recreate views against the new pipelines table
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

-- Step 14: Record migration
INSERT INTO schema_migrations (version, description)
VALUES (4, 'Decouple pipelines from result_sets — pipeline_id is now globally unique');

COMMIT;
PRAGMA foreign_keys = ON;
