import json
from pathlib import Path

import pytest

from text2sql_eval_toolkit.database import jobs as db_jobs
from text2sql_eval_toolkit.database import session
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

    (data_root / "test-benchmarks.json").write_text(
        json.dumps(
            {
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
        ),
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


@pytest.fixture
def isolated_db(tmp_path, monkeypatch):
    _write_minimal_test_benchmark(tmp_path, "bird_sqlite_test_benchmark")
    monkeypatch.setenv("TEXT2SQL_EVAL_TOOLKIT_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("TEXT2SQL_DATA_ROOT", str(tmp_path))
    db_path = tmp_path / "text2sql_eval.db"
    monkeypatch.setenv("TEXT2SQL_DATABASE_URL", f"sqlite:///{db_path}")
    session.close_thread_connection()
    session._schema_initialized = False
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
