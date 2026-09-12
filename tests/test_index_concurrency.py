#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
One ``EvalIndex`` handle is cached per benchmark and the server runs its sync
endpoints in a threadpool, so several requests reach the same index at the same
moment. That is the normal case, not an edge case.

It used to share one sqlite3 connection across all of them.
``check_same_thread=False`` silences sqlite3's ownership check but does not make
a connection safe to use concurrently: under load this raised
``InterfaceError: bad parameter or other API misuse`` and -- worse -- returned
rows as ``None``, so a page of results could come back wrong rather than
failing. It surfaced only when ten browser contexts hit the dashboard at once.
"""

import json
import sqlite3
import threading

import pytest

from text2sql_eval_toolkit.indexing import build_index
from text2sql_eval_toolkit.indexing.store import EvalIndex, default_filters

PIPELINE = "wxai:openai/gpt-oss-120b-greedy-zero-shot-chatapi"
RECORD_COUNT = 400
THREADS = 12
ITERATIONS = 40


@pytest.fixture(scope="module")
def index(tmp_path_factory):
    root = tmp_path_factory.mktemp("concurrency")
    records = [
        {
            "id": f"rec-{i:04d}",
            "question": f"Question {i}",
            "db_id": "shop" if i % 2 else "hr",
            "meta": {"categories": ["has_join"] if i % 3 else []},
            "predictions": {
                PIPELINE: {
                    "predicted_sql": f"SELECT {i}",
                    "evaluation": {
                        "execution_accuracy": i % 2,
                        "llm_score": (i % 4) / 4,
                    },
                }
            },
        }
        for i in range(RECORD_COUNT)
    ]
    artifact = root / "demo-predictions_eval.json"
    artifact.write_text(json.dumps(records), encoding="utf-8")
    with EvalIndex(build_index(artifact), artifact) as handle:
        yield handle


def _run_concurrently(work):
    """Run `work` on many threads, returning whatever went wrong."""
    failures: list[str] = []
    lock = threading.Lock()

    def target():
        try:
            for _ in range(ITERATIONS):
                work()
        except Exception as exc:  # noqa: BLE001 - the point is to catch anything
            with lock:
                failures.append(f"{type(exc).__name__}: {exc}")

    threads = [threading.Thread(target=target) for _ in range(THREADS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return failures


def test_listing_records_concurrently_neither_fails_nor_lies(index):
    """
    Both halves matter. The assertion inside the worker is what catches the
    dangerous version of this bug: a shared connection returned `None` rows, so
    a caller could get a short page with no error at all.
    """

    def work():
        rows, total = index.list_records(page=1, page_size=25, **default_filters())
        assert total == RECORD_COUNT
        assert len(rows) == 25
        assert all(r.record_id for r in rows)

    assert _run_concurrently(work) == []


def test_reading_records_concurrently_returns_the_right_record(index):
    def work():
        record = index.read_record("rec-0100")
        assert record is not None
        assert record["id"] == "rec-0100"

    assert _run_concurrently(work) == []


def test_aggregating_concurrently_gives_the_same_answer_every_time(index):
    expected = index.metric_values_by_category()[0][PIPELINE]["execution_accuracy"]

    def work():
        overall, _by_category = index.metric_values_by_category()
        assert overall[PIPELINE]["execution_accuracy"] == expected

    assert _run_concurrently(work) == []


def test_mixed_traffic_is_safe(index):
    """What the dashboard actually does: different endpoints, same handle."""
    calls = [
        lambda: index.list_records(page=2, page_size=10, **default_filters()),
        lambda: index.read_record("rec-0007"),
        lambda: index.metric_values_by_category(),
        lambda: index.pipeline_ids(),
        lambda: index.record_count(),
        lambda: index.binary_confusion_by_pipeline("execution_accuracy", "llm_score"),
    ]
    counter = iter(range(10**6))

    def work():
        calls[next(counter) % len(calls)]()

    assert _run_concurrently(work) == []


def _tiny_handle(tmp_path):
    artifact = tmp_path / "demo-predictions_eval.json"
    artifact.write_text(
        json.dumps([{"id": "r1", "question": "q", "predictions": {}}]), encoding="utf-8"
    )
    return EvalIndex(build_index(artifact), artifact)


def test_closing_releases_every_thread_s_connection(tmp_path):
    """
    Each thread opens its own connection, so closing must reach all of them
    rather than just the closer's.

    The threads are held alive until after close(): a thread that has exited no
    longer owns its connection, and the next one opened reaps it.
    """
    handle = _tiny_handle(tmp_path)
    touched = threading.Barrier(5)  # four workers and this thread
    release = threading.Event()

    def touch():
        handle.record_count()
        touched.wait(timeout=10)
        release.wait(timeout=10)

    threads = [threading.Thread(target=touch) for _ in range(4)]
    for t in threads:
        t.start()
    touched.wait(timeout=10)
    try:
        assert len(handle._open_connections) == 4, "each thread should have its own"
        handle.close()
        assert handle._open_connections == []
    finally:
        release.set()
        for t in threads:
            t.join()


def test_a_retired_thread_s_connection_is_closed_not_kept(tmp_path):
    """
    The server's threadpool is not fixed. After a burst of concurrent requests
    anyio retires the workers left idle for ten seconds and starts new ones for
    the next burst, so over its life one cached handle is reached from far more
    threads than are ever alive at once.

    Each retired thread used to leave its connection open. The thread-local slot
    went with the thread, but the list close() walks still held the connection
    until the whole index was dropped.
    """
    handle = _tiny_handle(tmp_path)
    opened = []

    def touch():
        handle.record_count()
        opened.append(handle._local.conn)

    for _ in range(50):
        t = threading.Thread(target=touch)
        t.start()
        t.join()

    try:
        # Only the most recent thread's connection can still be waiting: it is
        # reaped when the next one is opened.
        assert len(handle._open_connections) <= 1
        for conn in opened[:-1]:
            with pytest.raises(sqlite3.ProgrammingError):
                conn.execute("SELECT 1")
    finally:
        handle.close()


def test_reaping_leaves_a_live_thread_s_connection_alone(tmp_path):
    """A thread between two queries still owns its connection."""
    handle = _tiny_handle(tmp_path)
    failures: list[str] = []
    opened = threading.Event()
    release = threading.Event()

    def hold():
        try:
            handle.record_count()
            opened.set()
            release.wait(timeout=10)
            handle.record_count()
        except Exception as exc:  # noqa: BLE001 - reported below
            failures.append(f"{type(exc).__name__}: {exc}")
            opened.set()

    holder = threading.Thread(target=hold)
    holder.start()
    opened.wait(timeout=10)
    for _ in range(5):
        t = threading.Thread(target=handle.record_count)
        t.start()
        t.join()
    release.set()
    holder.join()
    handle.close()

    assert failures == []
