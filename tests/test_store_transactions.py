import json
from pathlib import Path

import pytest

from text2sql_eval_toolkit.database import jobs as db_jobs
from text2sql_eval_toolkit.database import session
from text2sql_eval_toolkit.database.json_importer import JsonToDbImporter
from text2sql_eval_toolkit.database.store import get_store
from text2sql_eval_toolkit.utils import (
    get_writable_data_root,
    load_eval_summary,
    save_eval_summary,
    save_predictions_data,
)


def _write_minimal_test_benchmark(data_root: Path, benchmark_id: str) -> None:
    bench_dir = data_root / "benchmarks" / "test_benchmarks"
    bench_dir.mkdir(parents=True)
    (bench_dir / "results").mkdir()

    registry = {
        benchmark_id: {
            "name": benchmark_id,
            "description": "minimal benchmark for store transaction tests",
            "data": f"benchmarks/test_benchmarks/{benchmark_id}.json",
            "schema": f"benchmarks/test_benchmarks/{benchmark_id}-schema.json",
            "predictions": (
                f"benchmarks/test_benchmarks/results/{benchmark_id}-predictions.json"
            ),
            "db_engine": {
                "db_type": "sqlite",
                "db_folder": "benchmarks/test_benchmarks/db",
            },
        }
    }
    (data_root / "test-benchmarks.json").write_text(
        json.dumps(registry),
        encoding="utf-8",
    )
    (bench_dir / f"{benchmark_id}.json").write_text(
        json.dumps(
            [
                {
                    "question_id": 11,
                    "question": "test question",
                    "db_id": "test_db",
                    "SQL": "SELECT 1",
                }
            ]
        ),
        encoding="utf-8",
    )
    (bench_dir / f"{benchmark_id}-schema.json").write_text(
        json.dumps({"tables": []}),
        encoding="utf-8",
    )


def _seed_test_benchmark(data_root: Path, benchmark_id: str) -> None:
    conn = session.get_connection()
    data_rel = f"benchmarks/test_benchmarks/{benchmark_id}.json"
    schema_rel = f"benchmarks/test_benchmarks/{benchmark_id}-schema.json"
    predictions_rel = f"benchmarks/test_benchmarks/results/{benchmark_id}-predictions.json"
    info = {
        "name": benchmark_id,
        "description": "minimal benchmark for store transaction tests",
        "data": data_rel,
        "schema": schema_rel,
        "predictions": predictions_rel,
        "benchmark_json_path": str(data_root / data_rel),
        "schema_json_path": str(data_root / schema_rel),
        "db_engine": {
            "db_type": "sqlite",
            "db_folder": "benchmarks/test_benchmarks/db",
        },
        "is_test_subset": True,
    }
    importer = JsonToDbImporter(conn=conn, data_root=data_root)
    importer._import_benchmark_catalog(benchmark_id, info)
    importer._import_benchmark_records(benchmark_id, info)
    importer._import_schema_snapshot(benchmark_id, info)
    conn.commit()


@pytest.fixture
def isolated_db(tmp_path, monkeypatch):
    benchmark_id = "bird_sqlite_test_benchmark"
    _write_minimal_test_benchmark(tmp_path, benchmark_id)
    monkeypatch.setenv("TEXT2SQL_EVAL_TOOLKIT_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("TEXT2SQL_DATA_ROOT", str(tmp_path))
    db_path = tmp_path / "text2sql_eval.db"
    monkeypatch.setenv("TEXT2SQL_DATABASE_URL", f"sqlite:///{db_path}")
    session.close_thread_connection()
    session._schema_initialized = False
    _seed_test_benchmark(tmp_path, benchmark_id)
    yield
    session.close_thread_connection()
    session._schema_initialized = False


def test_save_eval_summary_after_predictions_with_llm_judge_config(isolated_db):
    """Regression: save_summary must not BEGIN while an implicit txn is open."""
    benchmark_id = "bird_sqlite_test_benchmark"
    pipeline_id = "test-pipeline"
    records = [
        {
            "question_id": 11,
            "predictions": {
                pipeline_id: {
                    "predicted_sql": "SELECT 1",
                    "evaluation": {
                        "sql_exact_match": 1.0,
                        "llm_score": 1.0,
                    },
                }
            },
        }
    ]
    summary = {
        pipeline_id: {"sql_exact_match": {"average": 1.0, "stddev": 0.0}},
        "llm_judge_config": {"model": {"id": "test-model"}},
    }

    save_predictions_data(
        benchmark_id, records, include_eval=True, status="evaluated"
    )
    save_eval_summary(benchmark_id, summary)

    loaded = load_eval_summary(benchmark_id)
    assert pipeline_id in loaded
    assert loaded[pipeline_id]["sql_exact_match"]["average"] == 1.0


def test_pipeline_job_lifecycle(isolated_db):
    benchmark_id = "bird_sqlite_test_benchmark"
    get_store(data_root=get_writable_data_root()).ensure_benchmark_seeded(benchmark_id)
    conn = session.get_connection()

    with db_jobs.track_job(conn, "inference", benchmark_id, params={"model": "test"}):
        pass

    rows = db_jobs.list_jobs(conn, benchmark_id=benchmark_id, job_type="inference")
    assert len(rows) == 1
    assert rows[0]["status"] == "completed"
    assert rows[0]["job_type"] == "inference"
    assert rows[0]["params"]["model"] == "test"


def test_pending_job_resumed_by_track_job(isolated_db):
    benchmark_id = "bird_sqlite_test_benchmark"
    get_store(data_root=get_writable_data_root()).ensure_benchmark_seeded(benchmark_id)
    conn = session.get_connection()
    job_id = db_jobs.create_pending_job(conn, "eval", benchmark_id)

    pending = db_jobs.get_job(conn, job_id)
    assert pending is not None
    assert pending["status"] == "pending"

    with db_jobs.track_job(conn, "eval", benchmark_id, job_id=job_id):
        pass

    completed = db_jobs.get_job(conn, job_id)
    assert completed is not None
    assert completed["status"] == "completed"


def test_load_result_records_slim_omits_payloads(isolated_db):
    """Dashboard slim loads must skip DF/prompt/trace payloads."""
    benchmark_id = "bird_sqlite_test_benchmark"
    pipeline_id = "test-pipeline"
    big_df = '{"columns":["a"],"index":[0],"data":[[1]]}'
    records = [
        {
            "question_id": 11,
            "predictions": {
                pipeline_id: {
                    "predicted_sql": "SELECT 1",
                    "prompt": "x" * 1000,
                    "predicted_df": big_df,
                    "agent_trace": {"steps": [1, 2, 3]},
                    "evaluation": {"execution_accuracy": 1.0},
                }
            },
        }
    ]
    save_predictions_data(
        benchmark_id, records, include_eval=True, status="evaluated"
    )
    store = get_store(data_root=get_writable_data_root())

    slim = store.load_result_records(
        benchmark_id, include_eval=True, include_payloads=False
    )
    assert len(slim) == 1
    block = slim[0]["predictions"][pipeline_id]
    assert "evaluation" in block
    assert block["evaluation"]["execution_accuracy"] == 1.0
    assert "prompt" not in block
    assert "predicted_df" not in block
    assert "agent_trace" not in block
    assert "gt_df" not in slim[0]

    full = store.load_result_records(
        benchmark_id,
        include_eval=True,
        include_payloads=True,
        record_ids=["11"],
    )
    assert len(full) == 1
    full_block = full[0]["predictions"][pipeline_id]
    assert full_block.get("prompt") == "x" * 1000
    assert full_block.get("predicted_df") == big_df
    assert full_block.get("agent_trace") == {"steps": [1, 2, 3]}


def test_estimate_eval_payload_bytes(isolated_db):
    benchmark_id = "bird_sqlite_test_benchmark"
    records = [
        {
            "question_id": 11,
            "predictions": {
                "p1": {
                    "predicted_sql": "SELECT 1",
                    "predicted_df": '{"columns":["a"],"index":[0],"data":[[1]]}',
                    "evaluation": {"execution_accuracy": 1.0},
                }
            },
        }
    ]
    save_predictions_data(
        benchmark_id, records, include_eval=True, status="evaluated"
    )
    store = get_store(data_root=get_writable_data_root())
    size = store.estimate_eval_payload_bytes(benchmark_id)
    assert size is not None and size > 0
