#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from statistics import stdev
from typing import Any

logger = logging.getLogger(__name__)

RECORD_CORE_FIELDS = frozenset(
    {
        "id",
        "question_id",
        "question",
        "db_id",
        "utterance",
        "evidence",
        "difficulty",
        "sql",
        "SQL",
        "meta",
        "oracle_sql",
        "gold_tables",
        "mapping",
        "join_keys",
        "original_sql",
        "predictions",
        "gt_df",
        "query",
    }
)

FEATURE_FIELDS = (
    "query_table_count",
    "query_column_count",
    "query_nested_count",
    "query_aggregate_count",
    "query_sort_count",
    "query_window_func_count",
    "query_join_count",
)

EVAL_BINARY_COLUMNS = (
    "execution_accuracy",
    "non_empty_execution_accuracy",
    "subset_non_empty_execution_accuracy",
    "logic_execution_accuracy",
    "bird_execution_accuracy",
    "sql_exact_match",
    "sqlglot_equivalence",
    "sqlglot_optimized_equivalence",
    "sqlparse_equivalence",
    "sql_syntactic_equivalence",
    "is_sqlglot_parsable",
    "is_sqlparse_parsable",
    "df_error",
    "eval_error",
)

SUMMARY_AVG_COLUMNS = (
    "execution_accuracy",
    "non_empty_execution_accuracy",
    "subset_non_empty_execution_accuracy",
    "logic_execution_accuracy",
    "bird_execution_accuracy",
    "sql_exact_match",
    "sqlglot_equivalence",
    "sqlglot_optimized_equivalence",
    "sqlparse_equivalence",
    "sql_syntactic_equivalence",
    "is_sqlglot_parsable",
    "is_sqlparse_parsable",
    "eval_error",
    "df_error",
)


@dataclass
class ImportStats:
    benchmarks: int = 0
    records: int = 0
    predictions: int = 0
    evaluations: int = 0
    dataframes: int = 0
    summaries: int = 0


@dataclass
class JsonToDbImporter:
    conn: sqlite3.Connection
    data_root: Path
    dataframe_inline_limit: int = 64 * 1024
    _df_cache: dict[str, int] = field(default_factory=dict, init=False, repr=False)

    def import_all(
        self,
        *,
        benchmark_ids: list[str] | None = None,
        include_test: bool = True,
        skip_predictions: bool = False,
        skip_eval: bool = False,
        skip_summaries: bool = False,
        compute_category_summaries: bool = False,
        force: bool = False,
    ) -> ImportStats:
        stats = ImportStats()
        registry = self._load_registry(include_test=include_test)
        if benchmark_ids:
            unknown = sorted(set(benchmark_ids) - set(registry))
            if unknown:
                raise ValueError(f"Unknown benchmark id(s): {', '.join(unknown)}")
            registry = {k: v for k, v in registry.items() if k in benchmark_ids}

        for benchmark_id, info in registry.items():
            logger.info("Importing benchmark %s", benchmark_id)
            if force:
                self._delete_benchmark(benchmark_id)
            self._import_benchmark_catalog(benchmark_id, info)
            stats.benchmarks += 1
            stats.records += self._import_benchmark_records(benchmark_id, info)
            self._import_schema_snapshot(benchmark_id, info)
            if not skip_predictions:
                pred_stats = self._import_result_artifacts(
                    benchmark_id,
                    info,
                    import_eval=not skip_eval,
                )
                stats.predictions += pred_stats["predictions"]
                stats.evaluations += pred_stats["evaluations"]
                stats.dataframes += pred_stats["dataframes"]
            if not skip_summaries:
                stats.summaries += self._import_eval_summary(benchmark_id, info)
            if compute_category_summaries and not skip_eval and not skip_predictions:
                self._compute_category_summaries(benchmark_id)

        self.conn.commit()
        return stats

    def _load_registry(self, *, include_test: bool) -> dict[str, dict[str, Any]]:
        registry: dict[str, dict[str, Any]] = {}
        for is_test in (False, True):
            if is_test and not include_test:
                continue
            filename = "test-benchmarks.json" if is_test else "benchmarks.json"
            registry_path = self.data_root / filename
            if not registry_path.is_file():
                continue
            with open(registry_path, encoding="utf-8") as handle:
                meta = json.load(handle)
            for benchmark_id, entry in meta.items():
                info = dict(entry)
                info["is_test_subset"] = is_test
                info["benchmark_json_path"] = str(
                    _resolve_data_path(self.data_root, info["data"])
                )
                info["schema_json_path"] = str(
                    _resolve_data_path(self.data_root, info["schema"])
                )
                predictions_path = _resolve_data_path(self.data_root, info["predictions"])
                info["predictions_path"] = str(predictions_path)
                info["eval_results_path"] = str(
                    predictions_path.with_name(predictions_path.stem + "_eval.json")
                )
                info["eval_summary_path"] = str(
                    predictions_path.with_name(predictions_path.stem + "_eval_summary.json")
                )
                registry[benchmark_id] = info
        return registry

    def _delete_benchmark(self, benchmark_id: str) -> None:
        self.conn.execute("DELETE FROM benchmarks WHERE benchmark_id = ?", (benchmark_id,))

    def _import_benchmark_catalog(self, benchmark_id: str, info: dict[str, Any]) -> None:
        data_path = Path(info["benchmark_json_path"])
        num_records = 0
        if data_path.is_file():
            with open(data_path, encoding="utf-8") as handle:
                records = json.load(handle)
            if isinstance(records, list):
                num_records = len(records)

        logo = info.get("logo")
        self.conn.execute(
            """
            INSERT INTO benchmarks (
                benchmark_id, name, description, logo_path, is_test_subset, num_records
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(benchmark_id) DO UPDATE SET
                name = excluded.name,
                description = excluded.description,
                logo_path = excluded.logo_path,
                is_test_subset = excluded.is_test_subset,
                num_records = excluded.num_records,
                updated_at = datetime('now')
            """,
            (
                benchmark_id,
                info.get("name", benchmark_id),
                info.get("description"),
                logo,
                1 if info.get("is_test_subset") else 0,
                num_records,
            ),
        )

        db_engine = info.get("db_engine") or {}
        registry_paths = {
            k: info.get(k)
            for k in ("data", "schema", "predictions")
            if info.get(k)
        }
        extra_config = {
            k: v
            for k, v in db_engine.items()
            if k
            not in {"db_type", "db_folder", "schema_name", "connection_string_env_var"}
        }
        extra_config.update(registry_paths)
        self.conn.execute(
            """
            INSERT INTO benchmark_db_config (
                benchmark_id, db_type, db_folder, schema_name,
                connection_string_env_var, extra_config
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(benchmark_id) DO UPDATE SET
                db_type = excluded.db_type,
                db_folder = excluded.db_folder,
                schema_name = excluded.schema_name,
                connection_string_env_var = excluded.connection_string_env_var,
                extra_config = excluded.extra_config
            """,
            (
                benchmark_id,
                db_engine.get("db_type", "sqlite"),
                db_engine.get("db_folder"),
                db_engine.get("schema_name"),
                db_engine.get("connection_string_env_var"),
                json.dumps(extra_config, ensure_ascii=False),
            ),
        )

    def _import_benchmark_records(self, benchmark_id: str, info: dict[str, Any]) -> int:
        data_path = Path(info["benchmark_json_path"])
        if not data_path.is_file():
            logger.warning("Benchmark data file missing for %s: %s", benchmark_id, data_path)
            return 0

        with open(data_path, encoding="utf-8") as handle:
            records = json.load(handle)
        if not isinstance(records, list):
            raise ValueError(f"Expected list in benchmark data file: {data_path}")

        count = 0
        for sort_order, record in enumerate(records):
            record_id = _record_id(record)
            extra = {
                k: v for k, v in record.items() if k not in RECORD_CORE_FIELDS and v is not None
            }
            internal_id = self._upsert_benchmark_record(
                benchmark_id=benchmark_id,
                record_id=record_id,
                record=record,
                extra_metadata=extra,
                sort_order=sort_order,
            )
            self._replace_gt_sql(internal_id, record)
            self._replace_categories(internal_id, record)
            self._replace_features(internal_id, record)
            count += 1
        return count

    def _upsert_benchmark_record(
        self,
        *,
        benchmark_id: str,
        record_id: str,
        record: dict[str, Any],
        extra_metadata: dict[str, Any],
        sort_order: int,
    ) -> int:
        question = record.get("question") or record.get("query") or ""
        self.conn.execute(
            """
            INSERT INTO benchmark_records (
                benchmark_id, record_id, db_id, question, utterance,
                evidence, difficulty, extra_metadata, sort_order
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(benchmark_id, record_id) DO UPDATE SET
                db_id = excluded.db_id,
                question = excluded.question,
                utterance = excluded.utterance,
                evidence = excluded.evidence,
                difficulty = excluded.difficulty,
                extra_metadata = excluded.extra_metadata,
                sort_order = excluded.sort_order,
                updated_at = datetime('now')
            """,
            (
                benchmark_id,
                record_id,
                record.get("db_id"),
                question,
                record.get("utterance"),
                record.get("evidence"),
                record.get("difficulty"),
                json.dumps(extra_metadata, ensure_ascii=False),
                sort_order,
            ),
        )
        row = self.conn.execute(
            """
            SELECT id FROM benchmark_records
            WHERE benchmark_id = ? AND record_id = ?
            """,
            (benchmark_id, record_id),
        ).fetchone()
        if row is None:
            raise RuntimeError(f"Failed to resolve benchmark record id for {record_id}")
        return int(row["id"])

    def _replace_gt_sql(self, internal_id: int, record: dict[str, Any]) -> None:
        self.conn.execute(
            "DELETE FROM record_gt_sql WHERE benchmark_record_id = ?",
            (internal_id,),
        )
        sql_values = _gt_sql_values(record)
        for ordinal, sql_text in enumerate(sql_values):
            self.conn.execute(
                """
                INSERT INTO record_gt_sql (
                    benchmark_record_id, ordinal, sql_text, is_canonical
                ) VALUES (?, ?, ?, ?)
                """,
                (internal_id, ordinal, sql_text, 1 if ordinal == 0 else 0),
            )

    def _replace_categories(self, internal_id: int, record: dict[str, Any]) -> None:
        self.conn.execute(
            "DELETE FROM record_categories WHERE benchmark_record_id = ?",
            (internal_id,),
        )
        meta = record.get("meta") or {}
        categories = meta.get("categories") or []
        for category in categories:
            if isinstance(category, str) and category:
                self.conn.execute(
                    """
                    INSERT OR IGNORE INTO record_categories (
                        benchmark_record_id, category
                    ) VALUES (?, ?)
                    """,
                    (internal_id, category),
                )

    def _replace_features(self, internal_id: int, record: dict[str, Any]) -> None:
        meta = record.get("meta") or {}
        features = meta.get("features") or {}
        if not isinstance(features, dict) or not features:
            self.conn.execute(
                "DELETE FROM record_features WHERE benchmark_record_id = ?",
                (internal_id,),
            )
            return
        values = [features.get(name) for name in FEATURE_FIELDS]
        self.conn.execute(
            """
            INSERT INTO record_features (
                benchmark_record_id,
                query_table_count,
                query_column_count,
                query_nested_count,
                query_aggregate_count,
                query_sort_count,
                query_window_func_count,
                query_join_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(benchmark_record_id) DO UPDATE SET
                query_table_count = excluded.query_table_count,
                query_column_count = excluded.query_column_count,
                query_nested_count = excluded.query_nested_count,
                query_aggregate_count = excluded.query_aggregate_count,
                query_sort_count = excluded.query_sort_count,
                query_window_func_count = excluded.query_window_func_count,
                query_join_count = excluded.query_join_count
            """,
            (internal_id, *values),
        )

    def _import_schema_snapshot(self, benchmark_id: str, info: dict[str, Any]) -> None:
        schema_path = Path(info["schema_json_path"])
        if not schema_path.is_file():
            logger.warning("Schema file missing for %s: %s", benchmark_id, schema_path)
            return

        with open(schema_path, encoding="utf-8") as handle:
            schema_json = json.load(handle)
        payload = json.dumps(schema_json, ensure_ascii=False, sort_keys=True)
        schema_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()

        self.conn.execute(
            """
            UPDATE benchmark_schema_snapshots
            SET is_current = 0
            WHERE benchmark_id = ? AND schema_hash != ?
            """,
            (benchmark_id, schema_hash),
        )
        self.conn.execute(
            """
            INSERT INTO benchmark_schema_snapshots (
                benchmark_id, schema_json, source_path, schema_hash, is_current
            ) VALUES (?, ?, ?, ?, 1)
            ON CONFLICT(benchmark_id, schema_hash) DO UPDATE SET
                source_path = excluded.source_path,
                is_current = 1
            """,
            (
                benchmark_id,
                payload,
                str(schema_path.relative_to(self.data_root))
                if schema_path.is_relative_to(self.data_root)
                else str(schema_path),
                schema_hash,
            ),
        )

    def _import_result_artifacts(
        self,
        benchmark_id: str,
        info: dict[str, Any],
        *,
        import_eval: bool,
    ) -> dict[str, int]:
        eval_path = Path(info["eval_results_path"])
        pred_path = Path(info["predictions_path"])
        source_path = eval_path if import_eval and eval_path.is_file() else pred_path
        if not source_path.is_file():
            logger.warning(
                "Predictions/eval file missing for %s: %s", benchmark_id, source_path
            )
            return {"predictions": 0, "evaluations": 0, "dataframes": 0}

        logger.info("Loading %s", source_path)
        with open(source_path, encoding="utf-8") as handle:
            records = json.load(handle)
        if not isinstance(records, list):
            raise ValueError(f"Expected list in predictions file: {source_path}")

        result_set_id = self._ensure_result_set(
            benchmark_id=benchmark_id,
            source_path=source_path,
            has_eval=import_eval and eval_path.is_file(),
        )
        record_map = self._record_id_map(benchmark_id)
        pipeline_map = self._ensure_pipelines(result_set_id, records)

        stats = {"predictions": 0, "evaluations": 0, "dataframes": 0}
        for record in records:
            record_id = _record_id(record)
            internal_id = record_map.get(record_id)
            if internal_id is None:
                logger.warning(
                    "Skipping predictions for unknown record %s in %s",
                    record_id,
                    benchmark_id,
                )
                continue

            self._import_gt_execution(internal_id, record, stats)
            predictions = record.get("predictions") or {}
            if not isinstance(predictions, dict):
                continue

            for pipeline_id, block in predictions.items():
                if not isinstance(block, dict):
                    continue
                pipeline_ref = pipeline_map[pipeline_id]
                prediction_id = self._upsert_prediction(
                    benchmark_record_id=internal_id,
                    pipeline_ref=pipeline_ref,
                    result_set_id=result_set_id,
                )
                self._upsert_prediction_inference(prediction_id, block)
                self._upsert_prediction_execution(prediction_id, block, stats)

                evaluation = block.get("evaluation")
                if import_eval and isinstance(evaluation, dict):
                    self._upsert_evaluation(prediction_id, block, evaluation, stats)
                    stats["evaluations"] += 1
                stats["predictions"] += 1

        if import_eval and eval_path.is_file():
            evaluated_at = datetime.fromtimestamp(
                eval_path.stat().st_mtime, tz=timezone.utc
            ).isoformat()
            self.conn.execute(
                """
                UPDATE result_sets
                SET status = 'evaluated', evaluated_at = ?, updated_at = datetime('now')
                WHERE id = ?
                """,
                (evaluated_at, result_set_id),
            )
        return stats

    def import_result_records(
        self,
        benchmark_id: str,
        records: list[dict[str, Any]],
        *,
        import_eval: bool = False,
        status: str = "inference",
        source: str = "pipeline",
    ) -> dict[str, int]:
        """Import an in-memory result record list (predictions and optional eval)."""
        if not records:
            return {"predictions": 0, "evaluations": 0, "dataframes": 0}

        result_set_id = self._ensure_result_set(
            benchmark_id=benchmark_id,
            source_path=Path(f"{benchmark_id}-predictions"),
            has_eval=import_eval,
        )
        self.conn.execute(
            """
            UPDATE result_sets
            SET status = ?, source = ?, updated_at = datetime('now')
            WHERE id = ?
            """,
            (status, source, result_set_id),
        )
        record_map = self._record_id_map(benchmark_id)
        pipeline_map = self._ensure_pipelines(result_set_id, records)

        stats = {"predictions": 0, "evaluations": 0, "dataframes": 0}
        for record in records:
            record_id = _record_id(record)
            internal_id = record_map.get(record_id)
            if internal_id is None:
                logger.warning(
                    "Skipping predictions for unknown record %s in %s",
                    record_id,
                    benchmark_id,
                )
                continue

            self._import_gt_execution(internal_id, record, stats)
            predictions = record.get("predictions") or {}
            if not isinstance(predictions, dict):
                continue

            for pipeline_id, block in predictions.items():
                if not isinstance(block, dict):
                    continue
                pipeline_ref = pipeline_map[pipeline_id]
                prediction_id = self._upsert_prediction(
                    benchmark_record_id=internal_id,
                    pipeline_ref=pipeline_ref,
                    result_set_id=result_set_id,
                )
                self._upsert_prediction_inference(prediction_id, block)
                self._upsert_prediction_execution(prediction_id, block, stats)

                evaluation = block.get("evaluation")
                if import_eval and isinstance(evaluation, dict):
                    self._upsert_evaluation(prediction_id, block, evaluation, stats)
                    stats["evaluations"] += 1
                stats["predictions"] += 1

        if import_eval:
            self.conn.execute(
                """
                UPDATE result_sets
                SET status = 'evaluated', evaluated_at = datetime('now'), updated_at = datetime('now')
                WHERE id = ?
                """,
                (result_set_id,),
            )
        return stats

    def _ensure_result_set(
        self,
        *,
        benchmark_id: str,
        source_path: Path,
        has_eval: bool,
    ) -> int:
        status = "evaluated" if has_eval else "inference"
        file_stem = source_path.stem
        self.conn.execute(
            """
            INSERT INTO result_sets (
                benchmark_id, label, status, source, file_stem
            ) VALUES (?, 'default', ?, 'json_import', ?)
            ON CONFLICT(benchmark_id, label) DO UPDATE SET
                status = excluded.status,
                source = excluded.source,
                file_stem = excluded.file_stem,
                updated_at = datetime('now')
            """,
            (benchmark_id, status, file_stem),
        )
        row = self.conn.execute(
            """
            SELECT id FROM result_sets
            WHERE benchmark_id = ? AND label = 'default'
            """,
            (benchmark_id,),
        ).fetchone()
        assert row is not None
        return int(row["id"])

    def _record_id_map(self, benchmark_id: str) -> dict[str, int]:
        rows = self.conn.execute(
            """
            SELECT id, record_id FROM benchmark_records
            WHERE benchmark_id = ?
            """,
            (benchmark_id,),
        ).fetchall()
        return {str(row["record_id"]): int(row["id"]) for row in rows}

    def _ensure_pipelines(
        self, result_set_id: int, records: list[dict[str, Any]]
    ) -> dict[str, int]:
        pipeline_ids: set[str] = set()
        for record in records:
            predictions = record.get("predictions") or {}
            if isinstance(predictions, dict):
                pipeline_ids.update(predictions.keys())

        pipeline_map: dict[str, int] = {}
        for pipeline_id in sorted(pipeline_ids):
            sample = self._find_pipeline_sample(records, pipeline_id)
            pipeline_type = (
                "agentic" if "agentic" in pipeline_id.lower() else "zero_shot"
            )
            model_name = sample.get("model_name")
            model_parameters = sample.get("model_parameters") or {}
            self.conn.execute(
                """
                INSERT INTO pipelines (
                    result_set_id, pipeline_id, pipeline_type, model_name, model_parameters
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(result_set_id, pipeline_id) DO UPDATE SET
                    pipeline_type = excluded.pipeline_type,
                    model_name = excluded.model_name,
                    model_parameters = excluded.model_parameters
                """,
                (
                    result_set_id,
                    pipeline_id,
                    pipeline_type,
                    model_name,
                    json.dumps(model_parameters, ensure_ascii=False),
                ),
            )
            row = self.conn.execute(
                """
                SELECT id FROM pipelines
                WHERE result_set_id = ? AND pipeline_id = ?
                """,
                (result_set_id, pipeline_id),
            ).fetchone()
            assert row is not None
            pipeline_map[pipeline_id] = int(row["id"])
        return pipeline_map

    def _find_pipeline_sample(
        self, records: list[dict[str, Any]], pipeline_id: str
    ) -> dict[str, Any]:
        for record in records:
            block = (record.get("predictions") or {}).get(pipeline_id)
            if isinstance(block, dict):
                return block
        return {}

    def _upsert_prediction(
        self,
        *,
        benchmark_record_id: int,
        pipeline_ref: int,
        result_set_id: int,
    ) -> int:
        self.conn.execute(
            """
            INSERT INTO predictions (
                benchmark_record_id, pipeline_ref, result_set_id
            ) VALUES (?, ?, ?)
            ON CONFLICT(benchmark_record_id, pipeline_ref) DO UPDATE SET
                result_set_id = excluded.result_set_id,
                updated_at = datetime('now')
            """,
            (benchmark_record_id, pipeline_ref, result_set_id),
        )
        row = self.conn.execute(
            """
            SELECT id FROM predictions
            WHERE benchmark_record_id = ? AND pipeline_ref = ?
            """,
            (benchmark_record_id, pipeline_ref),
        ).fetchone()
        assert row is not None
        return int(row["id"])

    def _upsert_prediction_inference(self, prediction_id: int, block: dict[str, Any]) -> None:
        token_usage = block.get("token_usage") or {}
        self.conn.execute(
            """
            INSERT INTO prediction_inference (
                prediction_id, predicted_sql, prompt, inference_time_ms,
                inference_error, response_info, agent_attempts, agent_reasoning,
                agent_trace, token_usage_per_attempt,
                prompt_tokens, completion_tokens, total_tokens
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(prediction_id) DO UPDATE SET
                predicted_sql = excluded.predicted_sql,
                prompt = excluded.prompt,
                inference_time_ms = excluded.inference_time_ms,
                inference_error = excluded.inference_error,
                response_info = excluded.response_info,
                agent_attempts = excluded.agent_attempts,
                agent_reasoning = excluded.agent_reasoning,
                agent_trace = excluded.agent_trace,
                token_usage_per_attempt = excluded.token_usage_per_attempt,
                prompt_tokens = excluded.prompt_tokens,
                completion_tokens = excluded.completion_tokens,
                total_tokens = excluded.total_tokens
            """,
            (
                prediction_id,
                block.get("predicted_sql"),
                block.get("prompt"),
                block.get("inference_time_ms"),
                block.get("inference_error"),
                _json_or_none(block.get("response_info")),
                _json_or_none(block.get("agent_attempts")),
                _json_or_none(block.get("agent_reasoning")),
                _json_or_none(block.get("agent_trace")),
                _json_or_none(block.get("token_usage_per_attempt")),
                token_usage.get("prompt_tokens"),
                token_usage.get("completion_tokens"),
                token_usage.get("total_tokens"),
            ),
        )

    def _upsert_prediction_execution(
        self, prediction_id: int, block: dict[str, Any], stats: dict[str, int]
    ) -> None:
        predicted_df_id = self._store_dataframe(block.get("predicted_df"), stats)
        logic_df_id = self._store_dataframe(block.get("logic_df"), stats)
        self.conn.execute(
            """
            INSERT INTO prediction_execution (
                prediction_id, sql_execution_error, execution_time_ms,
                logic_sql, logic_sql_execution_error, logic_execution_time_ms,
                predicted_df_id, logic_df_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(prediction_id) DO UPDATE SET
                sql_execution_error = excluded.sql_execution_error,
                execution_time_ms = excluded.execution_time_ms,
                logic_sql = excluded.logic_sql,
                logic_sql_execution_error = excluded.logic_sql_execution_error,
                logic_execution_time_ms = excluded.logic_execution_time_ms,
                predicted_df_id = excluded.predicted_df_id,
                logic_df_id = excluded.logic_df_id
            """,
            (
                prediction_id,
                block.get("sql_execution_error"),
                block.get("execution_time_ms"),
                block.get("logic_sql"),
                block.get("logic_sql_execution_error"),
                block.get("logic_execution_time_ms"),
                predicted_df_id,
                logic_df_id,
            ),
        )

    def _import_gt_execution(
        self, internal_id: int, record: dict[str, Any], stats: dict[str, int]
    ) -> None:
        gt_df = record.get("gt_df")
        if gt_df is None:
            return

        gt_df_ids: list[int] = []
        gt_payload: list[str] = []
        if isinstance(gt_df, str):
            df_id = self._store_dataframe(gt_df, stats)
            if df_id is not None:
                gt_df_ids.append(df_id)
                gt_payload.append(gt_df)
        elif isinstance(gt_df, list):
            for item in gt_df:
                if isinstance(item, str):
                    df_id = self._store_dataframe(item, stats)
                    if df_id is not None:
                        gt_df_ids.append(df_id)
                        gt_payload.append(item)

        primary_id = gt_df_ids[0] if gt_df_ids else None
        self.conn.execute(
            """
            INSERT INTO record_ground_truth_execution (
                benchmark_record_id, gt_df_id, gt_df_ids
            ) VALUES (?, ?, ?)
            ON CONFLICT(benchmark_record_id) DO UPDATE SET
                gt_df_id = excluded.gt_df_id,
                gt_df_ids = excluded.gt_df_ids
            """,
            (
                internal_id,
                primary_id,
                json.dumps(gt_payload, ensure_ascii=False) if gt_payload else None,
            ),
        )

    def _upsert_evaluation(
        self,
        prediction_id: int,
        block: dict[str, Any],
        evaluation: dict[str, Any],
        stats: dict[str, int],
    ) -> None:
        matched_gt_df_id = self._store_dataframe(
            evaluation.get("matched_gt_df"), stats
        )
        values: dict[str, Any] = {}
        for column in EVAL_BINARY_COLUMNS:
            values[column] = _to_binary_int(evaluation.get(column))
        values["prompt_tokens"] = _to_int(
            evaluation.get("prompt_tokens") or (block.get("token_usage") or {}).get("prompt_tokens")
        )
        values["completion_tokens"] = _to_int(
            evaluation.get("completion_tokens")
            or (block.get("token_usage") or {}).get("completion_tokens")
        )
        values["total_tokens"] = _to_int(
            evaluation.get("total_tokens")
            or (block.get("token_usage") or {}).get("total_tokens")
        )
        values["inference_time_ms"] = evaluation.get("inference_time_ms") or block.get(
            "inference_time_ms"
        )
        values["execution_time_ms"] = evaluation.get("execution_time_ms") or block.get(
            "execution_time_ms"
        )
        values["df_error_message"] = evaluation.get("df_error_message")
        values["eval_error_message"] = evaluation.get("eval_error_message")
        values["matched_gt_sql"] = evaluation.get("matched_gt_sql")
        values["matched_gt_df_id"] = matched_gt_df_id

        self.conn.execute(
            """
            INSERT INTO evaluations (
                prediction_id,
                execution_accuracy,
                non_empty_execution_accuracy,
                subset_non_empty_execution_accuracy,
                logic_execution_accuracy,
                bird_execution_accuracy,
                sql_exact_match,
                sqlglot_equivalence,
                sqlglot_optimized_equivalence,
                sqlparse_equivalence,
                sql_syntactic_equivalence,
                is_sqlglot_parsable,
                is_sqlparse_parsable,
                prompt_tokens,
                completion_tokens,
                total_tokens,
                inference_time_ms,
                execution_time_ms,
                df_error,
                df_error_message,
                eval_error,
                eval_error_message,
                matched_gt_sql,
                matched_gt_df_id
            ) VALUES (
                :prediction_id,
                :execution_accuracy,
                :non_empty_execution_accuracy,
                :subset_non_empty_execution_accuracy,
                :logic_execution_accuracy,
                :bird_execution_accuracy,
                :sql_exact_match,
                :sqlglot_equivalence,
                :sqlglot_optimized_equivalence,
                :sqlparse_equivalence,
                :sql_syntactic_equivalence,
                :is_sqlglot_parsable,
                :is_sqlparse_parsable,
                :prompt_tokens,
                :completion_tokens,
                :total_tokens,
                :inference_time_ms,
                :execution_time_ms,
                :df_error,
                :df_error_message,
                :eval_error,
                :eval_error_message,
                :matched_gt_sql,
                :matched_gt_df_id
            )
            ON CONFLICT(prediction_id) DO UPDATE SET
                execution_accuracy = excluded.execution_accuracy,
                non_empty_execution_accuracy = excluded.non_empty_execution_accuracy,
                subset_non_empty_execution_accuracy = excluded.subset_non_empty_execution_accuracy,
                logic_execution_accuracy = excluded.logic_execution_accuracy,
                bird_execution_accuracy = excluded.bird_execution_accuracy,
                sql_exact_match = excluded.sql_exact_match,
                sqlglot_equivalence = excluded.sqlglot_equivalence,
                sqlglot_optimized_equivalence = excluded.sqlglot_optimized_equivalence,
                sqlparse_equivalence = excluded.sqlparse_equivalence,
                sql_syntactic_equivalence = excluded.sql_syntactic_equivalence,
                is_sqlglot_parsable = excluded.is_sqlglot_parsable,
                is_sqlparse_parsable = excluded.is_sqlparse_parsable,
                prompt_tokens = excluded.prompt_tokens,
                completion_tokens = excluded.completion_tokens,
                total_tokens = excluded.total_tokens,
                inference_time_ms = excluded.inference_time_ms,
                execution_time_ms = excluded.execution_time_ms,
                df_error = excluded.df_error,
                df_error_message = excluded.df_error_message,
                eval_error = excluded.eval_error,
                eval_error_message = excluded.eval_error_message,
                matched_gt_sql = excluded.matched_gt_sql,
                matched_gt_df_id = excluded.matched_gt_df_id,
                evaluated_at = datetime('now')
            """,
            {"prediction_id": prediction_id, **values},
        )

        llm_score = evaluation.get("llm_score")
        if llm_score is not None or evaluation.get("llm_explanation") or evaluation.get(
            "llm_judge_error"
        ):
            judge_config_id = self._ensure_default_llm_judge_config()
            self.conn.execute(
                """
                INSERT INTO llm_judge_evaluations (
                    prediction_id, llm_judge_config_ref, llm_score,
                    llm_explanation, llm_judge_error
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(prediction_id, llm_judge_config_ref) DO UPDATE SET
                    llm_score = excluded.llm_score,
                    llm_explanation = excluded.llm_explanation,
                    llm_judge_error = excluded.llm_judge_error,
                    evaluated_at = datetime('now')
                """,
                (
                    prediction_id,
                    judge_config_id,
                    float(llm_score) if llm_score is not None else None,
                    evaluation.get("llm_explanation"),
                    evaluation.get("llm_judge_error"),
                ),
            )

    def _ensure_default_llm_judge_config(self) -> int:
        config_json = json.dumps({"source": "json_import", "config_name": "default"})
        config_hash = hashlib.sha256(config_json.encode("utf-8")).hexdigest()
        row = self.conn.execute(
            "SELECT id FROM llm_judge_configs WHERE config_hash = ?",
            (config_hash,),
        ).fetchone()
        if row is not None:
            return int(row["id"])

        self.conn.execute(
            """
            INSERT INTO llm_judge_configs (config_name, config_hash, config_json)
            VALUES ('default', ?, ?)
            """,
            (config_hash, config_json),
        )
        row = self.conn.execute(
            "SELECT id FROM llm_judge_configs WHERE config_hash = ?",
            (config_hash,),
        ).fetchone()
        assert row is not None
        return int(row["id"])

    def _store_dataframe(self, value: Any, stats: dict[str, int]) -> int | None:
        if value is None:
            return None
        if not isinstance(value, str):
            value = json.dumps(value, ensure_ascii=False)
        if not value.strip():
            return None

        content_hash = hashlib.sha256(value.encode("utf-8")).hexdigest()
        cached = self._df_cache.get(content_hash)
        if cached is not None:
            return cached

        byte_size = len(value.encode("utf-8"))
        is_truncated = 1 if byte_size > self.dataframe_inline_limit else 0
        cursor = self.conn.execute(
            """
            INSERT INTO result_dataframes (
                format, payload_text, byte_size, is_truncated
            ) VALUES ('pandas_split', ?, ?, ?)
            """,
            (value, byte_size, is_truncated),
        )
        df_id = int(cursor.lastrowid)
        self._df_cache[content_hash] = df_id
        stats["dataframes"] += 1
        return df_id

    def _import_eval_summary(self, benchmark_id: str, info: dict[str, Any]) -> int:
        summary_path = Path(info["eval_summary_path"])
        if not summary_path.is_file():
            logger.warning("Eval summary missing for %s: %s", benchmark_id, summary_path)
            return 0

        with open(summary_path, encoding="utf-8") as handle:
            summary = json.load(handle)
        if not isinstance(summary, dict):
            raise ValueError(f"Invalid eval summary format: {summary_path}")

        llm_cfg = summary.pop("llm_judge_config", None)
        judge_config_id = None
        if isinstance(llm_cfg, dict):
            judge_config_id = self._upsert_llm_judge_config(llm_cfg)

        row = self.conn.execute(
            """
            SELECT id FROM result_sets
            WHERE benchmark_id = ? AND label = 'default'
            """,
            (benchmark_id,),
        ).fetchone()
        if row is None:
            logger.warning(
                "Skipping eval summary for %s because result_set is missing",
                benchmark_id,
            )
            return 0
        result_set_id = int(row["id"])

        imported = 0
        for pipeline_id, metrics in summary.items():
            if not isinstance(metrics, dict):
                continue
            pipeline_row = self.conn.execute(
                """
                SELECT id FROM pipelines
                WHERE result_set_id = ? AND pipeline_id = ?
                """,
                (result_set_id, pipeline_id),
            ).fetchone()
            if pipeline_row is None:
                logger.warning(
                    "Skipping summary for unknown pipeline %s (%s)",
                    pipeline_id,
                    benchmark_id,
                )
                continue
            pipeline_ref = int(pipeline_row["id"])
            self._upsert_eval_summary_row(
                result_set_id=result_set_id,
                pipeline_ref=pipeline_ref,
                category=None,
                metrics=metrics,
            )
            imported += 1

            if judge_config_id is not None and "llm_score" in metrics:
                llm_metric = metrics["llm_score"]
                if isinstance(llm_metric, dict):
                    self.conn.execute(
                        """
                        INSERT INTO llm_judge_eval_summaries (
                            result_set_id, pipeline_ref, llm_judge_config_ref,
                            category, num_evaluated, num_judge_errors,
                            llm_score_avg, llm_score_stddev, sum_judge_tokens
                        ) VALUES (?, ?, ?, NULL, ?, ?, ?, ?, ?)
                        ON CONFLICT(result_set_id, pipeline_ref, llm_judge_config_ref, category)
                        DO UPDATE SET
                            num_evaluated = excluded.num_evaluated,
                            num_judge_errors = excluded.num_judge_errors,
                            llm_score_avg = excluded.llm_score_avg,
                            llm_score_stddev = excluded.llm_score_stddev,
                            sum_judge_tokens = excluded.sum_judge_tokens,
                            computed_at = datetime('now')
                        """,
                        (
                            result_set_id,
                            pipeline_ref,
                            judge_config_id,
                            _to_int(metrics.get("num_evaluated")) or 0,
                            _to_int(metrics.get("num_llm_judge_errors")) or 0,
                            llm_metric.get("average"),
                            llm_metric.get("stddev"),
                            _to_int(metrics.get("sum_total_tokens")),
                        ),
                    )
        return imported

    def _upsert_llm_judge_config(self, llm_cfg: dict[str, Any]) -> int:
        payload = json.dumps(llm_cfg, ensure_ascii=False, sort_keys=True)
        config_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        model = llm_cfg.get("model") or {}
        model_id = model.get("id") if isinstance(model, dict) else None
        self.conn.execute(
            """
            INSERT INTO llm_judge_configs (
                config_name, config_hash, model_id, config_json
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT(config_hash) DO UPDATE SET
                config_name = excluded.config_name,
                model_id = excluded.model_id,
                config_json = excluded.config_json
            """,
            ("imported", config_hash, model_id, payload),
        )
        row = self.conn.execute(
            "SELECT id FROM llm_judge_configs WHERE config_hash = ?",
            (config_hash,),
        ).fetchone()
        assert row is not None
        return int(row["id"])

    def _upsert_eval_summary_row(
        self,
        *,
        result_set_id: int,
        pipeline_ref: int,
        category: str | None,
        metrics: dict[str, Any],
    ) -> None:
        avg_values = {
            f"{name}_avg": (metrics.get(name) or {}).get("average")
            if isinstance(metrics.get(name), dict)
            else None
            for name in SUMMARY_AVG_COLUMNS
        }
        std_values = {
            f"{name}_stddev": (metrics.get(name) or {}).get("stddev")
            if isinstance(metrics.get(name), dict)
            else None
            for name in SUMMARY_AVG_COLUMNS
        }
        self.conn.execute(
            """
            INSERT INTO eval_summaries (
                result_set_id,
                pipeline_ref,
                category,
                num_records,
                num_predictions,
                num_evaluated,
                num_inference_errors,
                num_df_errors,
                num_eval_errors,
                execution_accuracy_avg,
                execution_accuracy_stddev,
                non_empty_execution_accuracy_avg,
                non_empty_execution_accuracy_stddev,
                subset_non_empty_execution_accuracy_avg,
                subset_non_empty_execution_accuracy_stddev,
                logic_execution_accuracy_avg,
                logic_execution_accuracy_stddev,
                bird_execution_accuracy_avg,
                bird_execution_accuracy_stddev,
                sql_exact_match_avg,
                sql_exact_match_stddev,
                sqlglot_equivalence_avg,
                sqlglot_equivalence_stddev,
                sqlglot_optimized_equivalence_avg,
                sqlglot_optimized_equivalence_stddev,
                sqlparse_equivalence_avg,
                sqlparse_equivalence_stddev,
                sql_syntactic_equivalence_avg,
                sql_syntactic_equivalence_stddev,
                is_sqlglot_parsable_avg,
                is_sqlglot_parsable_stddev,
                is_sqlparse_parsable_avg,
                is_sqlparse_parsable_stddev,
                eval_error_avg,
                eval_error_stddev,
                df_error_avg,
                df_error_stddev,
                sum_total_tokens,
                sum_inference_time_ms,
                sum_execution_time_ms
            ) VALUES (
                :result_set_id,
                :pipeline_ref,
                :category,
                :num_records,
                :num_predictions,
                :num_evaluated,
                :num_inference_errors,
                :num_df_errors,
                :num_eval_errors,
                :execution_accuracy_avg,
                :execution_accuracy_stddev,
                :non_empty_execution_accuracy_avg,
                :non_empty_execution_accuracy_stddev,
                :subset_non_empty_execution_accuracy_avg,
                :subset_non_empty_execution_accuracy_stddev,
                :logic_execution_accuracy_avg,
                :logic_execution_accuracy_stddev,
                :bird_execution_accuracy_avg,
                :bird_execution_accuracy_stddev,
                :sql_exact_match_avg,
                :sql_exact_match_stddev,
                :sqlglot_equivalence_avg,
                :sqlglot_equivalence_stddev,
                :sqlglot_optimized_equivalence_avg,
                :sqlglot_optimized_equivalence_stddev,
                :sqlparse_equivalence_avg,
                :sqlparse_equivalence_stddev,
                :sql_syntactic_equivalence_avg,
                :sql_syntactic_equivalence_stddev,
                :is_sqlglot_parsable_avg,
                :is_sqlglot_parsable_stddev,
                :is_sqlparse_parsable_avg,
                :is_sqlparse_parsable_stddev,
                :eval_error_avg,
                :eval_error_stddev,
                :df_error_avg,
                :df_error_stddev,
                :sum_total_tokens,
                :sum_inference_time_ms,
                :sum_execution_time_ms
            )
            ON CONFLICT(result_set_id, pipeline_ref, category) DO UPDATE SET
                num_records = excluded.num_records,
                num_predictions = excluded.num_predictions,
                num_evaluated = excluded.num_evaluated,
                num_inference_errors = excluded.num_inference_errors,
                num_df_errors = excluded.num_df_errors,
                num_eval_errors = excluded.num_eval_errors,
                execution_accuracy_avg = excluded.execution_accuracy_avg,
                execution_accuracy_stddev = excluded.execution_accuracy_stddev,
                non_empty_execution_accuracy_avg = excluded.non_empty_execution_accuracy_avg,
                non_empty_execution_accuracy_stddev = excluded.non_empty_execution_accuracy_stddev,
                subset_non_empty_execution_accuracy_avg = excluded.subset_non_empty_execution_accuracy_avg,
                subset_non_empty_execution_accuracy_stddev = excluded.subset_non_empty_execution_accuracy_stddev,
                logic_execution_accuracy_avg = excluded.logic_execution_accuracy_avg,
                logic_execution_accuracy_stddev = excluded.logic_execution_accuracy_stddev,
                bird_execution_accuracy_avg = excluded.bird_execution_accuracy_avg,
                bird_execution_accuracy_stddev = excluded.bird_execution_accuracy_stddev,
                sql_exact_match_avg = excluded.sql_exact_match_avg,
                sql_exact_match_stddev = excluded.sql_exact_match_stddev,
                sqlglot_equivalence_avg = excluded.sqlglot_equivalence_avg,
                sqlglot_equivalence_stddev = excluded.sqlglot_equivalence_stddev,
                sqlglot_optimized_equivalence_avg = excluded.sqlglot_optimized_equivalence_avg,
                sqlglot_optimized_equivalence_stddev = excluded.sqlglot_optimized_equivalence_stddev,
                sqlparse_equivalence_avg = excluded.sqlparse_equivalence_avg,
                sqlparse_equivalence_stddev = excluded.sqlparse_equivalence_stddev,
                sql_syntactic_equivalence_avg = excluded.sql_syntactic_equivalence_avg,
                sql_syntactic_equivalence_stddev = excluded.sql_syntactic_equivalence_stddev,
                is_sqlglot_parsable_avg = excluded.is_sqlglot_parsable_avg,
                is_sqlglot_parsable_stddev = excluded.is_sqlglot_parsable_stddev,
                is_sqlparse_parsable_avg = excluded.is_sqlparse_parsable_avg,
                is_sqlparse_parsable_stddev = excluded.is_sqlparse_parsable_stddev,
                eval_error_avg = excluded.eval_error_avg,
                eval_error_stddev = excluded.eval_error_stddev,
                df_error_avg = excluded.df_error_avg,
                df_error_stddev = excluded.df_error_stddev,
                sum_total_tokens = excluded.sum_total_tokens,
                sum_inference_time_ms = excluded.sum_inference_time_ms,
                sum_execution_time_ms = excluded.sum_execution_time_ms,
                computed_at = datetime('now')
            """,
            {
                "result_set_id": result_set_id,
                "pipeline_ref": pipeline_ref,
                "category": category,
                "num_records": _to_int(metrics.get("num_records")) or 0,
                "num_predictions": _to_int(metrics.get("num_predictions")) or 0,
                "num_evaluated": _to_int(metrics.get("num_evaluated")) or 0,
                "num_inference_errors": _to_int(metrics.get("num_inference_errors")) or 0,
                "num_df_errors": _to_int(metrics.get("num_df_errors")) or 0,
                "num_eval_errors": _to_int(metrics.get("num_eval_errors")) or 0,
                "sum_total_tokens": _to_int(metrics.get("sum_total_tokens")),
                "sum_inference_time_ms": metrics.get("sum_inference_time_ms"),
                "sum_execution_time_ms": metrics.get("sum_execution_time_ms"),
                **avg_values,
                **std_values,
            },
        )

    def _compute_category_summaries(self, benchmark_id: str) -> None:
        row = self.conn.execute(
            """
            SELECT id FROM result_sets
            WHERE benchmark_id = ? AND label = 'default'
            """,
            (benchmark_id,),
        ).fetchone()
        if row is None:
            return
        result_set_id = int(row["id"])

        rows = self.conn.execute(
            """
            SELECT
                pip.id AS pipeline_ref,
                pip.pipeline_id,
                rc.category,
                e.execution_accuracy,
                e.non_empty_execution_accuracy,
                e.subset_non_empty_execution_accuracy,
                e.logic_execution_accuracy,
                e.bird_execution_accuracy,
                e.sql_exact_match,
                e.sqlglot_equivalence,
                e.sqlglot_optimized_equivalence,
                e.sqlparse_equivalence,
                e.sql_syntactic_equivalence,
                e.is_sqlglot_parsable,
                e.is_sqlparse_parsable,
                e.df_error,
                e.eval_error,
                pi.total_tokens,
                pi.inference_time_ms,
                pe.execution_time_ms
            FROM predictions pr
            JOIN pipelines pip ON pip.id = pr.pipeline_ref
            JOIN evaluations e ON e.prediction_id = pr.id
            JOIN prediction_inference pi ON pi.prediction_id = pr.id
            LEFT JOIN prediction_execution pe ON pe.prediction_id = pr.id
            JOIN record_categories rc ON rc.benchmark_record_id = pr.benchmark_record_id
            WHERE pr.result_set_id = ?
            """,
            (result_set_id,),
        ).fetchall()

        grouped: dict[tuple[int, str, str], list[sqlite3.Row]] = {}
        for item in rows:
            key = (int(item["pipeline_ref"]), str(item["pipeline_id"]), str(item["category"]))
            grouped.setdefault(key, []).append(item)

        for (pipeline_ref, _pipeline_id, category), items in grouped.items():
            metrics = _aggregate_metric_dict(items)
            self._upsert_eval_summary_row(
                result_set_id=result_set_id,
                pipeline_ref=pipeline_ref,
                category=category,
                metrics=metrics,
            )


def _record_id(record: dict[str, Any]) -> str:
    if record.get("id") is not None:
        return str(record["id"])
    if record.get("question_id") is not None:
        return str(record["question_id"])
    raise ValueError("Record is missing both 'id' and 'question_id'")


def _gt_sql_values(record: dict[str, Any]) -> list[str]:
    sql = record.get("sql")
    if isinstance(sql, list):
        return [str(item) for item in sql if item is not None]
    if isinstance(sql, str) and sql:
        return [sql]
    legacy = record.get("SQL")
    if isinstance(legacy, str) and legacy:
        return [legacy]
    return []


def _json_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False)


def _to_binary_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, (int, float)):
        return 1 if float(value) == 1.0 else 0
    return None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return None


def _aggregate_metric_dict(rows: list[sqlite3.Row]) -> dict[str, Any]:
    metric_names = list(SUMMARY_AVG_COLUMNS)
    out: dict[str, Any] = {
        "num_records": len({row["category"] for row in rows}),
        "num_predictions": len(rows),
        "num_evaluated": len(rows),
    }
    for metric in metric_names:
        values = [
            float(row[metric])
            for row in rows
            if row[metric] is not None
        ]
        if not values:
            continue
        avg = sum(values) / len(values)
        sd = stdev(values) if len(values) > 1 else 0.0
        out[metric] = {"average": avg, "stddev": sd}

    token_values = [
        float(row["total_tokens"])
        for row in rows
        if row["total_tokens"] is not None
    ]
    if token_values:
        out["sum_total_tokens"] = int(sum(token_values))
    inference_values = [
        float(row["inference_time_ms"])
        for row in rows
        if row["inference_time_ms"] is not None
    ]
    if inference_values:
        out["sum_inference_time_ms"] = sum(inference_values)
    execution_values = [
        float(row["execution_time_ms"])
        for row in rows
        if row["execution_time_ms"] is not None
    ]
    if execution_values:
        out["sum_execution_time_ms"] = sum(execution_values)
    return out


def _resolve_data_path(data_root: Path, path_str: str) -> Path:
    path = Path(path_str)
    if path.is_absolute():
        return path
    return (data_root / path).resolve()


def default_data_root() -> Path:
    import os

    for env_name in ("TEXT2SQL_DATA_ROOT", "TEXT2SQL_EVAL_TOOLKIT_DATA_ROOT"):
        env = os.environ.get(env_name)
        if env:
            return Path(env).expanduser().resolve()

    cwd = Path.cwd().resolve()
    for candidate in [cwd, *cwd.parents]:
        if (candidate / "pyproject.toml").is_file() and (candidate / "data").is_dir():
            return (candidate / "data").resolve()
    return (cwd / "data").resolve()
