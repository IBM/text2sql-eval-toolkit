#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Builds a queryable SQLite index beside an evaluation artifact.

The dashboard's list, filter, and aggregate endpoints previously re-parsed the
whole ``{benchmark}-predictions_eval.json`` on every request.  This module walks
that file once and records what those endpoints actually need:

* per record -- id, question, ``db_id``, file order, and the byte range of the
  record in the source file (so detail reads are a seek, not a parse);
* per (record, pipeline) -- the full ``evaluation`` block, which measures at
  roughly 7% of a record because the bulk is dataframes and prompts;
* per (record, pipeline, metric) -- numeric values only, in a tall table that
  SQLite can index for the filter and disagreement queries.

The index is derived and disposable: deleting it costs a rebuild, never data.
Staleness is detected from the source file's size and mtime.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Optional

from text2sql_eval_toolkit.indexing.scanner import iter_record_spans

# Bump when the table layout changes; a mismatch forces a rebuild.
SCHEMA_VERSION = 1

INDEX_DIR_NAME = ".index"

_SCHEMA = """
CREATE TABLE meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Pipeline ids average ~55 characters and metric names ~19, and both repeat on
-- every metric row.  Interning them to integers keeps the metrics table and its
-- index small enough to scale to the full published result set.
CREATE TABLE pipelines (
    pipeline_ref INTEGER PRIMARY KEY,
    pipeline_id  TEXT NOT NULL UNIQUE
);

CREATE TABLE metric_names (
    metric_ref INTEGER PRIMARY KEY,
    metric     TEXT NOT NULL UNIQUE
);

-- `ordinal` is the rowid and also the record's position in the source file, so
-- ordering by it reproduces the file order the endpoints paginate in.
CREATE TABLE records (
    ordinal    INTEGER PRIMARY KEY,
    record_id  TEXT NOT NULL UNIQUE,
    question   TEXT NOT NULL DEFAULT '',
    db_id      TEXT,
    byte_start INTEGER NOT NULL,
    byte_end   INTEGER NOT NULL
);

CREATE TABLE predictions (
    ordinal         INTEGER NOT NULL,
    pipeline_ref    INTEGER NOT NULL,
    evaluation_json TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (ordinal, pipeline_ref)
) WITHOUT ROWID;

CREATE TABLE metrics (
    ordinal      INTEGER NOT NULL,
    pipeline_ref INTEGER NOT NULL,
    metric_ref   INTEGER NOT NULL,
    value        REAL NOT NULL,
    PRIMARY KEY (ordinal, pipeline_ref, metric_ref)
) WITHOUT ROWID;

-- Covering index for "records where pipeline P metric M <op> V".  The primary
-- key already serves lookups keyed by record, so no second record index.
CREATE INDEX idx_metrics_filter ON metrics (pipeline_ref, metric_ref, value, ordinal);
"""


def index_dir_for(eval_path: Path) -> Path:
    return eval_path.parent / INDEX_DIR_NAME


def index_path_for(eval_path: Path) -> Path:
    """Location of the index for a given evaluation artifact."""
    return index_dir_for(eval_path) / f"{eval_path.stem}.sqlite"


def _source_fingerprint(eval_path: Path) -> Dict[str, str]:
    st = eval_path.stat()
    return {
        "source_name": eval_path.name,
        "source_size": str(st.st_size),
        "source_mtime_ns": str(st.st_mtime_ns),
        "schema_version": str(SCHEMA_VERSION),
    }


def is_stale(eval_path: Path, index_path: Optional[Path] = None) -> bool:
    """
    True when the index is missing, unreadable, built by another schema version,
    or built from different bytes than the artifact currently on disk.
    """
    index_path = index_path or index_path_for(eval_path)
    if not index_path.is_file():
        return True
    try:
        with sqlite3.connect(f"file:{index_path}?mode=ro", uri=True) as conn:
            rows = dict(conn.execute("SELECT key, value FROM meta").fetchall())
    except sqlite3.Error:
        return True
    expected = _source_fingerprint(eval_path)
    return any(rows.get(k) != v for k, v in expected.items())


def _extract_question(record: Dict[str, Any]) -> str:
    # Mirrors the field precedence used by the dashboard endpoints.
    for key in ("page_content", "question", "utterance"):
        value = record.get(key)
        if value:
            return str(value)
    return ""


def _extract_record_id(record: Dict[str, Any]) -> str:
    for key in ("id", "question_id", "qid", "_id"):
        value = record.get(key)
        if value is not None:
            return str(value)
    return ""


def build_index(
    eval_path: Path,
    index_path: Optional[Path] = None,
    force: bool = False,
    progress: Optional[Callable[[int], None]] = None,
) -> Path:
    """
    Build (or rebuild) the index for ``eval_path`` and return its location.

    Writes to a temporary file and renames, so a crashed or concurrent build can
    never leave a half-written index in place.  Returns early when a current
    index already exists and ``force`` is false.
    """
    eval_path = Path(eval_path)
    if not eval_path.is_file():
        raise FileNotFoundError(f"Evaluation artifact not found: {eval_path}")

    index_path = Path(index_path) if index_path else index_path_for(eval_path)
    if not force and not is_stale(eval_path, index_path):
        return index_path

    index_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = index_path.with_name(f".{index_path.name}.building")
    tmp_path.unlink(missing_ok=True)

    conn = sqlite3.connect(tmp_path)
    ok = False
    try:
        # Durability is irrelevant for a derived file that is rebuilt on any
        # mismatch, and turning it off roughly halves build time.
        conn.execute("PRAGMA journal_mode = OFF")
        conn.execute("PRAGMA synchronous = OFF")
        conn.executescript(_SCHEMA)

        count = _populate(conn, eval_path, progress)

        meta = _source_fingerprint(eval_path)
        meta["record_count"] = str(count)
        meta["built_at"] = str(int(time.time()))
        conn.executemany(
            "INSERT INTO meta (key, value) VALUES (?, ?)", sorted(meta.items())
        )
        conn.commit()
        conn.execute("PRAGMA optimize")
        ok = True
    finally:
        conn.close()
        if not ok:
            # A malformed artifact must not leave a partial file to confuse the
            # next build or fill the volume.
            tmp_path.unlink(missing_ok=True)

    os.replace(tmp_path, index_path)
    return index_path


def _populate(
    conn: sqlite3.Connection,
    eval_path: Path,
    progress: Optional[Callable[[int], None]],
) -> int:
    pipeline_refs: Dict[str, int] = {}
    metric_refs: Dict[str, int] = {}
    record_rows = []
    prediction_rows = []
    metric_rows = []
    count = 0

    def pipeline_ref(pipeline_id: str) -> int:
        ref = pipeline_refs.get(pipeline_id)
        if ref is None:
            ref = len(pipeline_refs) + 1
            pipeline_refs[pipeline_id] = ref
        return ref

    def metric_ref(metric: str) -> int:
        ref = metric_refs.get(metric)
        if ref is None:
            ref = len(metric_refs) + 1
            metric_refs[metric] = ref
        return ref

    def flush() -> None:
        if record_rows:
            conn.executemany(
                "INSERT OR REPLACE INTO records "
                "(ordinal, record_id, question, db_id, byte_start, byte_end) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                record_rows,
            )
            record_rows.clear()
        if prediction_rows:
            conn.executemany(
                "INSERT OR REPLACE INTO predictions "
                "(ordinal, pipeline_ref, evaluation_json) VALUES (?, ?, ?)",
                prediction_rows,
            )
            prediction_rows.clear()
        if metric_rows:
            conn.executemany(
                "INSERT OR REPLACE INTO metrics "
                "(ordinal, pipeline_ref, metric_ref, value) VALUES (?, ?, ?, ?)",
                metric_rows,
            )
            metric_rows.clear()

    with eval_path.open("rb") as fh:
        for ordinal, span in enumerate(iter_record_spans(fh)):
            record = json.loads(span.raw)
            record_id = _extract_record_id(record)
            if not record_id:
                # Skip rather than fail: one malformed record should not make an
                # entire benchmark unbrowsable.
                continue

            record_rows.append(
                (
                    ordinal,
                    record_id,
                    _extract_question(record),
                    record.get("db_id"),
                    span.start,
                    span.end,
                )
            )

            predictions = record.get("predictions")
            if isinstance(predictions, dict):
                for pipeline_id, pred in predictions.items():
                    evaluation = (
                        pred.get("evaluation") if isinstance(pred, dict) else {}
                    )
                    if not isinstance(evaluation, dict):
                        evaluation = {}
                    p_ref = pipeline_ref(pipeline_id)
                    prediction_rows.append(
                        (ordinal, p_ref, json.dumps(evaluation, ensure_ascii=False))
                    )
                    for metric, value in evaluation.items():
                        # Mirror the endpoints' `isinstance(v, (int, float))`
                        # test exactly.  bool passes that test in Python, so a
                        # boolean metric must be indexed as 1.0/0.0 rather than
                        # dropped -- dropping it would make a filter silently
                        # return nothing where the old code returned matches.
                        if not isinstance(value, (int, float)):
                            continue
                        metric_rows.append(
                            (ordinal, p_ref, metric_ref(metric), float(value))
                        )

            count += 1
            if count % 500 == 0:
                flush()
                if progress:
                    progress(count)

    flush()
    conn.executemany(
        "INSERT INTO pipelines (pipeline_ref, pipeline_id) VALUES (?, ?)",
        [(ref, pid) for pid, ref in pipeline_refs.items()],
    )
    conn.executemany(
        "INSERT INTO metric_names (metric_ref, metric) VALUES (?, ?)",
        [(ref, m) for m, ref in metric_refs.items()],
    )
    if progress:
        progress(count)
    return count


def build_all(
    results_dir: Path,
    benchmarks: Optional[Iterable[str]] = None,
    force: bool = False,
) -> Dict[str, Path]:
    """Build indices for every ``*-predictions_eval.json`` under ``results_dir``."""
    results_dir = Path(results_dir)
    wanted = set(benchmarks) if benchmarks else None
    built: Dict[str, Path] = {}
    for eval_path in sorted(results_dir.glob("*-predictions_eval.json")):
        benchmark_id = eval_path.name[: -len("-predictions_eval.json")]
        if wanted and benchmark_id not in wanted:
            continue
        built[benchmark_id] = build_index(eval_path, force=force)
    return built
