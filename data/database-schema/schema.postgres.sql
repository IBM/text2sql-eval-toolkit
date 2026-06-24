-- text2sql-eval-toolkit PostgreSQL extensions
-- Apply after schema.sql concepts, or use as a Postgres-native variant.
-- Requires PostgreSQL 15+
--
-- Apply:
--   psql $TEXT2SQL_DATABASE_URL -f docs/database-schema/schema.postgres.sql
--
-- Type mapping vs SQLite schema.sql:
--   TEXT timestamps      -> TIMESTAMPTZ DEFAULT now()
--   TEXT JSON columns    -> JSONB
--   INTEGER booleans     -> BOOLEAN
--   INTEGER AUTOINCREMENT -> BIGSERIAL
--   TEXT enum CHECKs     -> CREATE TYPE ... AS ENUM

-- ============================================================
-- ENUM TYPES (optional; CHECK constraints in schema.sql suffice)
-- ============================================================

DO $$ BEGIN
    CREATE TYPE result_set_status AS ENUM ('inference', 'executed', 'evaluated', 'archived');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE pipeline_type AS ENUM ('zero_shot', 'agentic', 'custom');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE job_type AS ENUM ('inference', 'execution', 'eval', 'llm_judge');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE job_status AS ENUM ('pending', 'running', 'completed', 'failed', 'cancelled');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- ============================================================
-- JSON EXPORT VIEW (PostgreSQL-only nested aggregation)
-- Reconstructs *-predictions_eval.json shape for one result_set
-- ============================================================

CREATE OR REPLACE VIEW v_eval_records_json AS
SELECT
    rs.benchmark_id,
    rs.id AS result_set_id,
    jsonb_agg(
        jsonb_build_object(
            'id', br.record_id,
            'question', br.question,
            'utterance', br.utterance,
            'db_id', br.db_id,
            'evidence', br.evidence,
            'difficulty', br.difficulty,
            'extra_metadata', br.extra_metadata::jsonb,
            'sql', (
                SELECT jsonb_agg(gs.sql_text ORDER BY gs.ordinal)
                FROM record_gt_sql gs
                WHERE gs.benchmark_record_id = br.id
            ),
            'meta', jsonb_build_object(
                'categories', (
                    SELECT COALESCE(jsonb_agg(rc.category), '[]'::jsonb)
                    FROM record_categories rc
                    WHERE rc.benchmark_record_id = br.id
                ),
                'features', (
                    SELECT to_jsonb(rf.*) - 'benchmark_record_id'
                    FROM record_features rf
                    WHERE rf.benchmark_record_id = br.id
                )
            ),
            'predictions', (
                SELECT COALESCE(jsonb_object_agg(
                    pip.pipeline_id,
                    jsonb_strip_nulls(jsonb_build_object(
                        'predicted_sql', pi.predicted_sql,
                        'prompt', pi.prompt,
                        'model_name', pip.model_name,
                        'model_parameters', pip.model_parameters::jsonb,
                        'token_usage', jsonb_build_object(
                            'prompt_tokens', pi.prompt_tokens,
                            'completion_tokens', pi.completion_tokens,
                            'total_tokens', pi.total_tokens
                        ),
                        'inference_time_ms', pi.inference_time_ms,
                        'inference_error', pi.inference_error,
                        'predicted_df', pdf.payload_text,
                        'execution_time_ms', pe.execution_time_ms,
                        'sql_execution_error', pe.sql_execution_error,
                        'logic_sql', pe.logic_sql,
                        'logic_df', ldf.payload_text,
                        'evaluation', (
                            SELECT to_jsonb(ev.*)
                                - 'prediction_id'
                                - 'matched_gt_sql'
                                - 'matched_gt_df_id'
                                - 'evaluated_at'
                            FROM evaluations ev
                            WHERE ev.prediction_id = pr.id
                        )
                    ))
                ), '{}'::jsonb)
                FROM predictions pr
                JOIN pipelines pip ON pip.id = pr.pipeline_ref
                LEFT JOIN prediction_inference pi ON pi.prediction_id = pr.id
                LEFT JOIN prediction_execution pe ON pe.prediction_id = pr.id
                LEFT JOIN result_dataframes pdf ON pdf.id = pe.predicted_df_id
                LEFT JOIN result_dataframes ldf ON ldf.id = pe.logic_df_id
                WHERE pr.benchmark_record_id = br.id
                  AND pr.result_set_id = rs.id
            )
        ) ORDER BY br.sort_order
    ) AS records
FROM result_sets rs
JOIN benchmark_records br ON br.benchmark_id = rs.benchmark_id
GROUP BY rs.benchmark_id, rs.id;
