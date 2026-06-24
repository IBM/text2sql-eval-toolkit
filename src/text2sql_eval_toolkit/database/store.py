#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

from __future__ import annotations

import json
import logging
import sqlite3
import threading
from collections import defaultdict
from pathlib import Path
from typing import Any

from text2sql_eval_toolkit.database.jobs import get_job as _get_job
from text2sql_eval_toolkit.database.jobs import list_jobs as _list_jobs
from text2sql_eval_toolkit.database.json_importer import (
    EVAL_BINARY_COLUMNS,
    FEATURE_FIELDS,
    JsonToDbImporter,
    SUMMARY_AVG_COLUMNS,
    _record_id,
    default_data_root,
)
from text2sql_eval_toolkit.database.session import (
    get_connection,
    retry_on_locked,
    transaction,
)

logger = logging.getLogger(__name__)

_benchmark_write_locks: dict[str, threading.Lock] = defaultdict(threading.Lock)
_store_instance: BenchmarkStore | None = None


class BenchmarkStore:
    """SQLite-backed storage for benchmarks, records, predictions, and evaluations."""

    def __init__(self, *, data_root: Path | None = None) -> None:
        self.data_root = (data_root or default_data_root()).resolve()

    @property
    def conn(self) -> sqlite3.Connection:
        return get_connection()

    @retry_on_locked
    def ensure_benchmark_seeded(self, benchmark_id: str) -> None:
        row = self.conn.execute(
            "SELECT 1 FROM benchmarks WHERE benchmark_id = ?",
            (benchmark_id,),
        ).fetchone()
        if row is None:
            raise ValueError(f"Benchmark '{benchmark_id}' not found in database.")

    @retry_on_locked
    def get_benchmarks_info(self, *, is_test: bool = False) -> dict[str, dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT b.*, c.db_type, c.db_folder, c.schema_name,
                   c.connection_string_env_var, c.extra_config
            FROM benchmarks b
            JOIN benchmark_db_config c ON c.benchmark_id = b.benchmark_id
            WHERE b.is_test_subset = ?
            ORDER BY b.benchmark_id
            """,
            (1 if is_test else 0,),
        ).fetchall()
        return {
            row["benchmark_id"]: self._info_dict_from_row(row)
            for row in rows
        }

    @retry_on_locked
    def get_benchmark_info(self, benchmark_id: str) -> dict[str, Any]:
        row = self.conn.execute(
            """
            SELECT b.*, c.db_type, c.db_folder, c.schema_name,
                   c.connection_string_env_var, c.extra_config
            FROM benchmarks b
            JOIN benchmark_db_config c ON c.benchmark_id = b.benchmark_id
            WHERE b.benchmark_id = ?
            """,
            (benchmark_id,),
        ).fetchone()
        if row is not None:
            return self._info_dict_from_row(row)
        raise ValueError(f"Benchmark ID '{benchmark_id}' not found in database.")

    @retry_on_locked
    def list_benchmark_ids(self, *, include_test: bool = True) -> list[str]:
        ids: list[str] = []
        ids.extend(self.get_benchmarks_info(is_test=False).keys())
        if include_test:
            ids.extend(self.get_benchmarks_info(is_test=True).keys())
        return ids

    @retry_on_locked
    def load_schema(self, benchmark_id: str) -> dict[str, Any]:
        self.ensure_benchmark_seeded(benchmark_id)
        row = self.conn.execute(
            """
            SELECT schema_json FROM benchmark_schema_snapshots
            WHERE benchmark_id = ? AND is_current = 1
            ORDER BY id DESC LIMIT 1
            """,
            (benchmark_id,),
        ).fetchone()
        if row is None:
            raise ValueError(
                f"No schema snapshot found in database for benchmark '{benchmark_id}'."
            )
        return json.loads(row["schema_json"])

    @retry_on_locked
    def load_gold_records(self, benchmark_id: str) -> list[dict[str, Any]]:
        self.ensure_benchmark_seeded(benchmark_id)
        return self._export_gold_records(benchmark_id)

    @retry_on_locked
    def save_gold_records(self, benchmark_id: str, records: list[dict[str, Any]]) -> None:
        with _benchmark_write_locks[benchmark_id]:
            importer = JsonToDbImporter(conn=self.conn, data_root=self.data_root)
            info = self.get_benchmark_info(benchmark_id)
            with transaction(immediate=True):
                for sort_order, record in enumerate(records):
                    record_id = _record_id(record)
                    from text2sql_eval_toolkit.database.json_importer import (
                        RECORD_CORE_FIELDS,
                    )

                    extra = {
                        k: v
                        for k, v in record.items()
                        if k not in RECORD_CORE_FIELDS and v is not None
                    }
                    internal_id = importer._upsert_benchmark_record(
                        benchmark_id=benchmark_id,
                        record_id=record_id,
                        record=record,
                        extra_metadata=extra,
                        sort_order=sort_order,
                    )
                    importer._replace_gt_sql(internal_id, record)
                    importer._replace_categories(internal_id, record)
                    importer._replace_features(internal_id, record)
                self.conn.execute(
                    "UPDATE benchmarks SET num_records = ?, updated_at = datetime('now') WHERE benchmark_id = ?",
                    (len(records), benchmark_id),
                )

    @retry_on_locked
    def load_result_records(
        self,
        benchmark_id: str,
        *,
        include_eval: bool = False,
        llm_judge_config_id: int | None = None,
    ) -> list[dict[str, Any]]:
        self.ensure_benchmark_seeded(benchmark_id)
        records = self._export_gold_records(benchmark_id)
        result_set_id = self._default_result_set_id(benchmark_id)
        if result_set_id is None:
            return records

        pred_rows = self.conn.execute(
            """
            SELECT
                br.record_id,
                pip.pipeline_id,
                pi.predicted_sql,
                pi.prompt,
                pi.inference_time_ms,
                pi.inference_error,
                pi.response_info,
                pi.agent_attempts,
                pi.agent_reasoning,
                pi.agent_trace,
                pi.token_usage_per_attempt,
                pi.prompt_tokens,
                pi.completion_tokens,
                pi.total_tokens,
                pip.model_name,
                pip.model_parameters,
                pe.sql_execution_error,
                pe.execution_time_ms,
                pe.logic_sql,
                pe.logic_sql_execution_error,
                pe.logic_execution_time_ms,
                pdf.payload_text AS predicted_df,
                ldf.payload_text AS logic_df
            FROM predictions pr
            JOIN benchmark_records br ON br.id = pr.benchmark_record_id
            JOIN pipelines pip ON pip.id = pr.pipeline_ref
            LEFT JOIN prediction_inference pi ON pi.prediction_id = pr.id
            LEFT JOIN prediction_execution pe ON pe.prediction_id = pr.id
            LEFT JOIN result_dataframes pdf ON pdf.id = pe.predicted_df_id
            LEFT JOIN result_dataframes ldf ON ldf.id = pe.logic_df_id
            WHERE pr.result_set_id = ?
            ORDER BY br.sort_order, pip.pipeline_id
            """,
            (result_set_id,),
        ).fetchall()

        record_index = {str(r["id"]): r for r in records}
        for row in pred_rows:
            record = record_index.get(str(row["record_id"]))
            if record is None:
                continue
            predictions = record.setdefault("predictions", {})
            block: dict[str, Any] = {
                "predicted_sql": row["predicted_sql"],
                "prompt": row["prompt"],
                "model_name": row["model_name"],
                "model_parameters": json.loads(row["model_parameters"] or "{}"),
                "inference_time_ms": row["inference_time_ms"],
                "sql_execution_error": row["sql_execution_error"],
                "execution_time_ms": row["execution_time_ms"],
                "logic_sql": row["logic_sql"],
                "logic_sql_execution_error": row["logic_sql_execution_error"],
                "logic_execution_time_ms": row["logic_execution_time_ms"],
            }
            if row["predicted_df"]:
                block["predicted_df"] = row["predicted_df"]
            if row["logic_df"]:
                block["logic_df"] = row["logic_df"]
            if row["prompt_tokens"] is not None:
                block["token_usage"] = {
                    "prompt_tokens": row["prompt_tokens"],
                    "completion_tokens": row["completion_tokens"],
                    "total_tokens": row["total_tokens"],
                }
            for json_field in (
                "response_info",
                "agent_attempts",
                "agent_reasoning",
                "agent_trace",
                "token_usage_per_attempt",
            ):
                raw = row[json_field]
                if raw:
                    try:
                        block[json_field] = json.loads(raw)
                    except json.JSONDecodeError:
                        block[json_field] = raw
            predictions[row["pipeline_id"]] = block

        if include_eval:
            self._attach_evaluations(
                benchmark_id,
                records,
                result_set_id,
                llm_judge_config_id=llm_judge_config_id,
            )

        self._attach_gt_execution(records, benchmark_id)
        return records

    @retry_on_locked
    def save_result_records(
        self,
        benchmark_id: str,
        records: list[dict[str, Any]],
        *,
        include_eval: bool = False,
        status: str = "executed",
        source: str = "pipeline",
        llm_judge_config: dict[str, Any] | None = None,
    ) -> None:
        self.ensure_benchmark_seeded(benchmark_id)
        with _benchmark_write_locks[benchmark_id]:
            importer = JsonToDbImporter(conn=self.conn, data_root=self.data_root)
            with transaction(immediate=True):
                importer.import_result_records(
                    benchmark_id,
                    records,
                    import_eval=include_eval,
                    status=status,
                    source=source,
                    llm_judge_config=llm_judge_config,
                )

    @retry_on_locked
    def load_summary(
        self,
        benchmark_id: str,
        *,
        llm_judge_config_id: int | None = None,
    ) -> dict[str, Any]:
        self.ensure_benchmark_seeded(benchmark_id)
        result_set_id = self._default_result_set_id(benchmark_id)
        if result_set_id is None:
            raise FileNotFoundError(f"No result set for benchmark {benchmark_id}")

        summary: dict[str, Any] = {}
        resolved_judge_id = llm_judge_config_id or self._default_llm_judge_config_id(
            result_set_id
        )
        if resolved_judge_id is not None:
            judge_row = self.conn.execute(
                """
                SELECT config_json FROM llm_judge_configs
                WHERE id = ?
                """,
                (resolved_judge_id,),
            ).fetchone()
            if judge_row is not None:
                try:
                    summary["llm_judge_config"] = json.loads(judge_row["config_json"])
                    summary["llm_judge_config_id"] = resolved_judge_id
                except json.JSONDecodeError:
                    pass

        pipeline_rows = self.conn.execute(
            "SELECT id, pipeline_id FROM pipelines WHERE result_set_id = ?",
            (result_set_id,),
        ).fetchall()
        pipeline_name_by_id = {row["id"]: row["pipeline_id"] for row in pipeline_rows}

        summary_rows = self.conn.execute(
            """
            SELECT * FROM eval_summaries
            WHERE result_set_id = ? AND category IS NULL
            """,
            (result_set_id,),
        ).fetchall()
        for row in summary_rows:
            pipeline_name = pipeline_name_by_id.get(row["pipeline_ref"])
            if not pipeline_name:
                continue
            summary[pipeline_name] = self._summary_row_to_metrics(row)

        llm_summary_sql = """
            SELECT pipeline_ref, llm_score_avg, llm_score_stddev,
                   num_evaluated, num_judge_errors, sum_judge_tokens
            FROM llm_judge_eval_summaries
            WHERE result_set_id = ? AND category IS NULL
        """
        llm_summary_params: list[Any] = [result_set_id]
        if resolved_judge_id is not None:
            llm_summary_sql += " AND llm_judge_config_ref = ?"
            llm_summary_params.append(resolved_judge_id)
        llm_summary_rows = self.conn.execute(
            llm_summary_sql,
            llm_summary_params,
        ).fetchall()
        for row in llm_summary_rows:
            pipeline_name = pipeline_name_by_id.get(row["pipeline_ref"])
            if pipeline_name and pipeline_name in summary:
                if row["llm_score_avg"] is not None:
                    summary[pipeline_name]["llm_score"] = {
                        "average": row["llm_score_avg"],
                        "stddev": row["llm_score_stddev"] or 0.0,
                    }
        return summary

    @retry_on_locked
    def save_summary(self, benchmark_id: str, summary: dict[str, Any]) -> None:
        self.ensure_benchmark_seeded(benchmark_id)
        with _benchmark_write_locks[benchmark_id]:
            importer = JsonToDbImporter(conn=self.conn, data_root=self.data_root)
            payload = dict(summary)
            llm_cfg = payload.pop("llm_judge_config", None)
            with transaction(immediate=True):
                judge_config_id = None
                if isinstance(llm_cfg, dict):
                    judge_config_id = importer._upsert_llm_judge_config(llm_cfg)
                result_set_id = importer._ensure_result_set(
                    benchmark_id=benchmark_id,
                    source_path=Path(f"{benchmark_id}-predictions"),
                    has_eval=True,
                )
                for pipeline_id, metrics in payload.items():
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
                        continue
                    pipeline_ref = int(pipeline_row["id"])
                    importer._upsert_eval_summary_row(
                        result_set_id=result_set_id,
                        pipeline_ref=pipeline_ref,
                        category=None,
                        metrics=metrics,
                    )
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
                                    metrics.get("num_evaluated") or 0,
                                    metrics.get("num_llm_judge_errors") or 0,
                                    llm_metric.get("average"),
                                    llm_metric.get("stddev"),
                                    metrics.get("sum_total_tokens"),
                                ),
                            )
                self.conn.execute(
                    """
                    UPDATE result_sets
                    SET status = 'evaluated', evaluated_at = datetime('now'), updated_at = datetime('now')
                    WHERE id = ?
                    """,
                    (result_set_id,),
                )

    @retry_on_locked
    def get_cache_version(self, benchmark_id: str) -> str:
        row = self.conn.execute(
            """
            SELECT b.updated_at AS benchmark_updated,
                   rs.updated_at AS result_updated
            FROM benchmarks b
            LEFT JOIN result_sets rs
              ON rs.benchmark_id = b.benchmark_id AND rs.label = 'default'
            WHERE b.benchmark_id = ?
            """,
            (benchmark_id,),
        ).fetchone()
        if row is None:
            return "0"
        return f"{row['benchmark_updated']}|{row['result_updated']}"

    @retry_on_locked
    def load_registry(self, *, production_only: bool = True) -> dict[str, Any]:
        info = self.get_benchmarks_info(is_test=False)
        if not production_only:
            info.update(self.get_benchmarks_info(is_test=True))
        return {
            benchmark_id: self._registry_payload(entry)
            for benchmark_id, entry in info.items()
        }

    @retry_on_locked
    def save_registry_entry(
        self, benchmark_id: str, payload: dict[str, Any], *, is_test: bool = False
    ) -> None:
        with transaction(immediate=True):
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
                    updated_at = datetime('now')
                """,
                (
                    benchmark_id,
                    payload.get("name", benchmark_id),
                    payload.get("description"),
                    payload.get("logo"),
                    1 if is_test else 0,
                    payload.get("num_records", 0),
                ),
            )
            db_engine = payload.get("db_engine") or {}
            extra = dict(db_engine)
            for key in ("data", "schema", "predictions"):
                if payload.get(key):
                    extra[key] = payload[key]
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
                    json.dumps(extra, ensure_ascii=False),
                ),
            )

    @retry_on_locked
    def delete_registry_entry(self, benchmark_id: str) -> None:
        with transaction(immediate=True):
            self.conn.execute(
                "DELETE FROM benchmarks WHERE benchmark_id = ?",
                (benchmark_id,),
            )

    def _info_dict_from_row(self, row: sqlite3.Row) -> dict[str, Any]:
        extra = json.loads(row["extra_config"] or "{}")
        data_rel = extra.get("data") or f"benchmarks/{row['benchmark_id']}.json"
        schema_rel = extra.get("schema") or f"benchmarks/{row['benchmark_id']}-schema.json"
        predictions_rel = extra.get("predictions") or (
            f"results/{row['benchmark_id']}-predictions.json"
        )
        db_engine = {
            k: v
            for k, v in extra.items()
            if k in {
                "db_type",
                "db_folder",
                "schema_name",
                "connection_string_env_var",
                "query_timeout",
            }
            or k not in {"data", "schema", "predictions"}
        }
        db_engine.setdefault("db_type", row["db_type"])
        if row["db_folder"]:
            db_engine.setdefault("db_folder", row["db_folder"])
        if row["schema_name"]:
            db_engine.setdefault("schema_name", row["schema_name"])
        if row["connection_string_env_var"]:
            db_engine.setdefault(
                "connection_string_env_var", row["connection_string_env_var"]
            )

        data_path = self._resolve_path(data_rel)
        predictions_path = self._resolve_path(predictions_rel)
        return {
            "name": row["name"],
            "description": row["description"] or "",
            "data": data_rel,
            "schema": schema_rel,
            "predictions": predictions_rel,
            "db_engine": db_engine,
            "logo": row["logo_path"],
            "is_test_subset": bool(row["is_test_subset"]),
            "num_records": row["num_records"],
            "benchmark_json_path": str(data_path),
            "schema_json_path": str(self._resolve_path(schema_rel)),
            "predictions_path": str(predictions_path),
            "eval_results_path": str(
                predictions_path.with_name(predictions_path.stem + "_eval.json")
            ),
            "eval_summary_path": str(
                predictions_path.with_name(predictions_path.stem + "_eval_summary.json")
            ),
        }

    def _registry_payload(self, info: dict[str, Any]) -> dict[str, Any]:
        payload = {
            "name": info.get("name"),
            "description": info.get("description"),
            "data": info.get("data"),
            "schema": info.get("schema"),
            "predictions": info.get("predictions"),
            "db_engine": info.get("db_engine") or {},
        }
        if info.get("logo"):
            payload["logo"] = info["logo"]
        return payload

    def _resolve_path(self, rel: str) -> Path:
        path = Path(rel)
        if path.is_absolute():
            return path
        return (self.data_root / path).resolve()

    def _default_result_set_id(self, benchmark_id: str) -> int | None:
        row = self.conn.execute(
            """
            SELECT id FROM result_sets
            WHERE benchmark_id = ? AND label = 'default'
            """,
            (benchmark_id,),
        ).fetchone()
        return int(row["id"]) if row else None

    def _result_data_status(self, benchmark_id: str) -> dict[str, int | str | None]:
        result_set_id = self._default_result_set_id(benchmark_id)
        if result_set_id is None:
            return {
                "result_set_id": None,
                "predictions": 0,
                "summaries": 0,
                "status": None,
            }
        row = self.conn.execute(
            "SELECT status FROM result_sets WHERE id = ?",
            (result_set_id,),
        ).fetchone()
        predictions = self.conn.execute(
            "SELECT COUNT(*) AS c FROM predictions WHERE result_set_id = ?",
            (result_set_id,),
        ).fetchone()["c"]
        summaries = self.conn.execute(
            """
            SELECT COUNT(*) AS c FROM eval_summaries
            WHERE result_set_id = ? AND category IS NULL
            """,
            (result_set_id,),
        ).fetchone()["c"]
        return {
            "result_set_id": result_set_id,
            "predictions": int(predictions),
            "summaries": int(summaries),
            "status": row["status"] if row else None,
        }

    @retry_on_locked
    def get_job(self, job_id: str) -> dict[str, Any] | None:
        return _get_job(self.conn, job_id)

    @retry_on_locked
    def list_jobs(
        self,
        *,
        benchmark_id: str | None = None,
        job_type: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        return _list_jobs(
            self.conn,
            benchmark_id=benchmark_id,
            job_type=job_type,
            limit=limit,
        )

    @retry_on_locked
    def list_llm_judge_configs_for_benchmark(
        self, benchmark_id: str
    ) -> list[dict[str, Any]]:
        self.ensure_benchmark_seeded(benchmark_id)
        result_set_id = self._default_result_set_id(benchmark_id)
        if result_set_id is None:
            return []
        rows = self.conn.execute(
            """
            SELECT DISTINCT ljc.id, ljc.config_name, ljc.model_id
            FROM llm_judge_configs ljc
            WHERE ljc.id IN (
                SELECT llm_judge_config_ref
                FROM llm_judge_eval_summaries
                WHERE result_set_id = ?
                UNION
                SELECT lje.llm_judge_config_ref
                FROM llm_judge_evaluations lje
                JOIN predictions pr ON pr.id = lje.prediction_id
                WHERE pr.result_set_id = ?
            )
            ORDER BY ljc.id
            """,
            (result_set_id, result_set_id),
        ).fetchall()
        return [
            {
                "id": int(row["id"]),
                "name": row["config_name"],
                "model_id": row["model_id"],
            }
            for row in rows
        ]

    def _default_llm_judge_config_id(self, result_set_id: int) -> int | None:
        row = self.conn.execute(
            """
            SELECT llm_judge_config_ref
            FROM llm_judge_eval_summaries
            WHERE result_set_id = ?
            ORDER BY llm_judge_config_ref DESC
            LIMIT 1
            """,
            (result_set_id,),
        ).fetchone()
        if row is not None:
            return int(row["llm_judge_config_ref"])
        row = self.conn.execute(
            """
            SELECT lje.llm_judge_config_ref
            FROM llm_judge_evaluations lje
            JOIN predictions pr ON pr.id = lje.prediction_id
            WHERE pr.result_set_id = ?
            ORDER BY lje.llm_judge_config_ref DESC
            LIMIT 1
            """,
            (result_set_id,),
        ).fetchone()
        if row is not None:
            return int(row["llm_judge_config_ref"])
        return None

    def _export_gold_records(self, benchmark_id: str) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT id, record_id, db_id, question, utterance, evidence,
                   difficulty, extra_metadata, sort_order
            FROM benchmark_records
            WHERE benchmark_id = ?
            ORDER BY sort_order, id
            """,
            (benchmark_id,),
        ).fetchall()
        records: list[dict[str, Any]] = []
        for row in rows:
            record = json.loads(row["extra_metadata"] or "{}")
            record.update(
                {
                    "id": row["record_id"],
                    "db_id": row["db_id"],
                    "question": row["question"],
                    "utterance": row["utterance"],
                    "evidence": row["evidence"],
                    "difficulty": row["difficulty"],
                }
            )
            sql_rows = self.conn.execute(
                """
                SELECT sql_text FROM record_gt_sql
                WHERE benchmark_record_id = ?
                ORDER BY ordinal
                """,
                (row["id"],),
            ).fetchall()
            if sql_rows:
                record["sql"] = [item["sql_text"] for item in sql_rows]

            categories = self.conn.execute(
                """
                SELECT category FROM record_categories
                WHERE benchmark_record_id = ?
                ORDER BY category
                """,
                (row["id"],),
            ).fetchall()
            features = self.conn.execute(
                """
                SELECT * FROM record_features WHERE benchmark_record_id = ?
                """,
                (row["id"],),
            ).fetchone()
            meta: dict[str, Any] = dict(record.get("meta") or {})
            if categories:
                meta["categories"] = [item["category"] for item in categories]
            if features is not None:
                meta["features"] = {
                    name: features[name]
                    for name in FEATURE_FIELDS
                    if features[name] is not None
                }
            if meta:
                record["meta"] = meta
            records.append(record)
        return records

    def _attach_gt_execution(
        self, records: list[dict[str, Any]], benchmark_id: str
    ) -> None:
        rows = self.conn.execute(
            """
            SELECT br.record_id, g.gt_df_ids, df.payload_text
            FROM record_ground_truth_execution g
            JOIN benchmark_records br ON br.id = g.benchmark_record_id
            LEFT JOIN result_dataframes df ON df.id = g.gt_df_id
            WHERE br.benchmark_id = ?
            """,
            (benchmark_id,),
        ).fetchall()
        by_id = {str(row["record_id"]): row for row in rows}
        for record in records:
            row = by_id.get(str(record.get("id")))
            if row is None:
                continue
            if row["gt_df_ids"]:
                try:
                    gt_list = json.loads(row["gt_df_ids"])
                    record["gt_df"] = gt_list[0] if len(gt_list) == 1 else gt_list
                except json.JSONDecodeError:
                    pass
            elif row["payload_text"]:
                record["gt_df"] = row["payload_text"]

    def _attach_evaluations(
        self,
        benchmark_id: str,
        records: list[dict[str, Any]],
        result_set_id: int,
        *,
        llm_judge_config_id: int | None = None,
    ) -> None:
        resolved_judge_id = llm_judge_config_id or self._default_llm_judge_config_id(
            result_set_id
        )
        llm_join = "LEFT JOIN llm_judge_evaluations lje ON lje.prediction_id = pr.id"
        params: list[Any] = [result_set_id]
        if resolved_judge_id is not None:
            llm_join = (
                "LEFT JOIN llm_judge_evaluations lje ON lje.prediction_id = pr.id "
                "AND lje.llm_judge_config_ref = ?"
            )
            params = [resolved_judge_id, result_set_id]
        eval_rows = self.conn.execute(
            f"""
            SELECT br.record_id, pip.pipeline_id, e.*, mdf.payload_text AS matched_gt_df,
                   lje.llm_score, lje.llm_explanation, lje.llm_judge_error
            FROM evaluations e
            JOIN predictions pr ON pr.id = e.prediction_id
            JOIN benchmark_records br ON br.id = pr.benchmark_record_id
            JOIN pipelines pip ON pip.id = pr.pipeline_ref
            LEFT JOIN result_dataframes mdf ON mdf.id = e.matched_gt_df_id
            {llm_join}
            WHERE pr.result_set_id = ?
            """,
            params,
        ).fetchall()
        record_index = {str(r["id"]): r for r in records}
        for row in eval_rows:
            record = record_index.get(str(row["record_id"]))
            if record is None:
                continue
            block = record.setdefault("predictions", {}).setdefault(
                row["pipeline_id"], {}
            )
            evaluation: dict[str, Any] = {}
            for column in EVAL_BINARY_COLUMNS:
                value = row[column]
                if value is not None:
                    evaluation[column] = float(value)
            for column in (
                "prompt_tokens",
                "completion_tokens",
                "total_tokens",
                "inference_time_ms",
                "execution_time_ms",
                "df_error_message",
                "eval_error_message",
                "matched_gt_sql",
            ):
                if row[column] is not None:
                    evaluation[column] = row[column]
            if row["matched_gt_df"]:
                evaluation["matched_gt_df"] = row["matched_gt_df"]
            if row["llm_score"] is not None:
                evaluation["llm_score"] = row["llm_score"]
            if row["llm_explanation"]:
                evaluation["llm_explanation"] = row["llm_explanation"]
            if row["llm_judge_error"]:
                evaluation["llm_judge_error"] = row["llm_judge_error"]
            block["evaluation"] = evaluation

    def _summary_row_to_metrics(self, row: sqlite3.Row) -> dict[str, Any]:
        metrics: dict[str, Any] = {
            "num_records": row["num_records"],
            "num_predictions": row["num_predictions"],
            "num_evaluated": row["num_evaluated"],
            "num_inference_errors": row["num_inference_errors"],
            "num_df_errors": row["num_df_errors"],
            "num_eval_errors": row["num_eval_errors"],
            "sum_total_tokens": row["sum_total_tokens"],
            "sum_inference_time_ms": row["sum_inference_time_ms"],
            "sum_execution_time_ms": row["sum_execution_time_ms"],
        }
        for name in SUMMARY_AVG_COLUMNS:
            avg = row[f"{name}_avg"]
            sd = row[f"{name}_stddev"]
            if avg is not None:
                metrics[name] = {"average": avg, "stddev": sd or 0.0}
        return metrics

def get_store(*, data_root: Path | None = None) -> BenchmarkStore:
    global _store_instance
    if data_root is not None:
        return BenchmarkStore(data_root=data_root)
    if _store_instance is None:
        _store_instance = BenchmarkStore()
    return _store_instance
