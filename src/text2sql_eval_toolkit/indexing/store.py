#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Read API over a built index.

This is what the dashboard endpoints query instead of parsing the evaluation
artifact.  Two properties matter more than speed:

* **Identical results.** Filtering, ordering, and pagination reproduce what the
  previous in-Python implementation returned, including its file ordering and
  its substring (not token) search semantics.
* **Bounded memory.** Nothing loads the artifact; record detail is a seek to a
  stored byte range.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Dict, Iterator, List, NamedTuple, Optional, Sequence, Tuple

from text2sql_eval_toolkit.indexing.builder import build_index, index_path_for, is_stale

# Comparison operators the dashboard exposes, mapped to SQL.
OPERATORS: Dict[str, str] = {
    "eq": "=",
    "ne": "!=",
    "lt": "<",
    "gt": ">",
    "le": "<=",
    "ge": ">=",
}


class RecordSummary(NamedTuple):
    record_id: str
    question: str
    predictions: Dict[str, Dict[str, Any]]  # pipeline_id -> evaluation block


class EvalIndex:
    """
    Query handle for one benchmark's index.

    Open with :meth:`for_benchmark`, which builds or rebuilds the index when it
    is missing or stale.
    """

    def __init__(self, index_path: Path, source_path: Path) -> None:
        self._index_path = Path(index_path)
        self._source_path = Path(source_path)
        # One connection per thread, not one shared connection.
        #
        # A single handle is cached per benchmark and the server runs sync
        # endpoints in a threadpool, so several requests reach the same
        # EvalIndex at once. `check_same_thread=False` only silences sqlite3's
        # ownership check -- it does not make a connection safe to use
        # concurrently, and doing so returned `InterfaceError: bad parameter or
        # other API misuse` and, worse, rows read as None: wrong answers rather
        # than errors.
        #
        # Read-only connections to the same file are cheap and SQLite allows any
        # number of concurrent readers, so a connection per thread costs almost
        # nothing and removes the sharing entirely.
        self._local = threading.local()
        self._open_connections: List[sqlite3.Connection] = []
        self._connections_lock = threading.Lock()

    @property
    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(
                f"file:{self._index_path}?mode=ro", uri=True, check_same_thread=False
            )
            conn.row_factory = sqlite3.Row
            self._local.conn = conn
            # Tracked so close() can release them all; threadpool workers are
            # long-lived, so this list stays as small as the pool.
            with self._connections_lock:
                self._open_connections.append(conn)
        return conn

    @classmethod
    def for_benchmark(
        cls, benchmark_id: str, results_dir: Path, auto_build: bool = True
    ) -> "EvalIndex":
        source = Path(results_dir) / f"{benchmark_id}-predictions_eval.json"
        if not source.is_file():
            raise FileNotFoundError(f"Evaluation artifact not found: {source}")
        path = index_path_for(source)
        if auto_build and is_stale(source, path):
            path = build_index(source)
        return cls(path, source)

    def close(self) -> None:
        """
        Release every connection this index opened, from whichever thread calls.

        ``check_same_thread=False`` is what makes closing another thread's
        connection legal here; nothing is using them by the time a handle is
        invalidated.
        """
        with self._connections_lock:
            for conn in self._open_connections:
                try:
                    conn.close()
                except sqlite3.Error:  # pragma: no cover - already closed
                    pass
            self._open_connections.clear()
        self._local = threading.local()

    def __enter__(self) -> "EvalIndex":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # -- metadata ---------------------------------------------------------

    @property
    def source_path(self) -> Path:
        return self._source_path

    def record_count(self) -> int:
        return self._conn.execute("SELECT COUNT(*) FROM records").fetchone()[0]

    def pipeline_ids(self) -> List[str]:
        return [
            r[0]
            for r in self._conn.execute(
                "SELECT pipeline_id FROM pipelines ORDER BY pipeline_ref"
            )
        ]

    def metric_names(self) -> List[str]:
        return [
            r[0]
            for r in self._conn.execute(
                "SELECT metric FROM metric_names ORDER BY metric"
            )
        ]

    def _pipeline_ref(self, pipeline_id: str) -> Optional[int]:
        row = self._conn.execute(
            "SELECT pipeline_ref FROM pipelines WHERE pipeline_id = ?", (pipeline_id,)
        ).fetchone()
        return row[0] if row else None

    def _metric_ref(self, metric: str) -> Optional[int]:
        row = self._conn.execute(
            "SELECT metric_ref FROM metric_names WHERE metric = ?", (metric,)
        ).fetchone()
        return row[0] if row else None

    # -- record detail ----------------------------------------------------

    def read_record(self, record_id: str) -> Optional[Dict[str, Any]]:
        """
        Return one full record by seeking to its byte range in the artifact.

        Cost is independent of file size -- this is the read that previously
        parsed the entire file to find a single record.
        """
        row = self._conn.execute(
            "SELECT byte_start, byte_end FROM records WHERE record_id = ?", (record_id,)
        ).fetchone()
        if row is None:
            return None
        with self._source_path.open("rb") as fh:
            fh.seek(row["byte_start"])
            raw = fh.read(row["byte_end"] - row["byte_start"])
        return json.loads(raw)

    def record_db_id(self, record_id: str) -> Optional[str]:
        """The record's ``db_id``, without reading the artifact."""
        row = self._conn.execute(
            "SELECT db_id FROM records WHERE record_id = ?", (record_id,)
        ).fetchone()
        return row["db_id"] if row else None

    # -- filtered listing -------------------------------------------------

    def _filter_sql(
        self,
        q: Optional[str],
        pipeline: Optional[str],
        metric: str,
        value: Optional[float],
        op: str,
        pipeline2: Optional[str],
        metric2: Optional[str],
        disagree: bool,
        failed_only: bool,
    ) -> Tuple[str, List[Any]]:
        clauses: List[str] = []
        params: List[Any] = []

        if q:
            # Substring match on id or question, matching the previous
            # `q.lower() in text.lower()` behaviour rather than tokenised search.
            like = f"%{q.lower()}%"
            clauses.append("(LOWER(r.record_id) LIKE ? OR LOWER(r.question) LIKE ?)")
            params += [like, like]

        def metric_clause(pipeline_id: str, metric_name: str, sql_op: str) -> str:
            p_ref = self._pipeline_ref(pipeline_id)
            m_ref = self._metric_ref(metric_name)
            if p_ref is None or m_ref is None:
                # Unknown pipeline or metric matched nothing before; keep that.
                return "0"
            params.extend([p_ref, m_ref])
            return (
                "EXISTS (SELECT 1 FROM metrics m WHERE m.ordinal = r.ordinal"
                f" AND m.pipeline_ref = ? AND m.metric_ref = ? AND m.value {sql_op} ?)"
            )

        if pipeline and value is not None:
            sql_op = OPERATORS.get(op)
            if sql_op is None:
                clauses.append("0")
            else:
                clause = metric_clause(pipeline, metric, sql_op)
                if clause == "0":
                    clauses.append("0")
                else:
                    params.append(float(value))
                    clauses.append(clause)

        if failed_only and pipeline:
            clause = metric_clause(pipeline, "execution_accuracy", "=")
            if clause == "0":
                clauses.append("0")
            else:
                params.append(0.0)
                clauses.append(clause)

        if pipeline and pipeline2 and disagree:
            p1 = self._pipeline_ref(pipeline)
            p2 = self._pipeline_ref(pipeline2)
            m1 = self._metric_ref(metric)
            m2 = self._metric_ref(metric2 or metric)
            if None in (p1, p2, m1, m2):
                clauses.append("0")
            else:
                # Both sides must be present and differ, matching the previous
                # "None on either side excludes the record" behaviour.
                clauses.append(
                    "EXISTS (SELECT 1 FROM metrics a JOIN metrics b"
                    "  ON a.ordinal = b.ordinal"
                    " WHERE a.ordinal = r.ordinal"
                    "   AND a.pipeline_ref = ? AND a.metric_ref = ?"
                    "   AND b.pipeline_ref = ? AND b.metric_ref = ?"
                    "   AND a.value != b.value)"
                )
                params += [p1, m1, p2, m2]

        where = " AND ".join(clauses) if clauses else "1"
        return where, params

    def count_records(self, **filters: Any) -> int:
        where, params = self._filter_sql(**filters)
        sql = f"SELECT COUNT(*) FROM records r WHERE {where}"
        return self._conn.execute(sql, params).fetchone()[0]

    def list_records(
        self,
        page: int = 1,
        page_size: int = 25,
        **filters: Any,
    ) -> Tuple[List[RecordSummary], int]:
        """
        Return one page of matching records plus the total match count.

        Ordering is by file position, reproducing the previous implementation's
        behaviour of iterating the artifact in order.
        """
        where, params = self._filter_sql(**filters)
        total = self._conn.execute(
            f"SELECT COUNT(*) FROM records r WHERE {where}", params
        ).fetchone()[0]

        offset = max(0, (page - 1) * page_size)
        rows = self._conn.execute(
            f"SELECT r.ordinal, r.record_id, r.question FROM records r"
            f" WHERE {where} ORDER BY r.ordinal LIMIT ? OFFSET ?",
            [*params, page_size, offset],
        ).fetchall()

        if not rows:
            return [], total

        ordinals = [r["ordinal"] for r in rows]
        placeholders = ",".join("?" * len(ordinals))
        eval_rows = self._conn.execute(
            "SELECT p.ordinal, pl.pipeline_id, p.evaluation_json"
            " FROM predictions p JOIN pipelines pl USING (pipeline_ref)"
            f" WHERE p.ordinal IN ({placeholders})",
            ordinals,
        ).fetchall()

        by_ordinal: Dict[int, Dict[str, Dict[str, Any]]] = {o: {} for o in ordinals}
        for row in eval_rows:
            by_ordinal[row["ordinal"]][row["pipeline_id"]] = json.loads(
                row["evaluation_json"]
            )

        items = [
            RecordSummary(
                record_id=row["record_id"],
                question=row["question"],
                predictions=by_ordinal[row["ordinal"]],
            )
            for row in rows
        ]
        return items, total

    # -- aggregates -------------------------------------------------------

    def metric_totals(self, pipeline_id: str, metric: str) -> Dict[str, float]:
        """Count, sum, and mean of a metric for one pipeline, computed in SQL."""
        p_ref = self._pipeline_ref(pipeline_id)
        m_ref = self._metric_ref(metric)
        if p_ref is None or m_ref is None:
            return {"count": 0, "sum": 0.0, "mean": 0.0}
        row = self._conn.execute(
            "SELECT COUNT(*) c, COALESCE(SUM(value), 0) s, COALESCE(AVG(value), 0) a"
            " FROM metrics WHERE pipeline_ref = ? AND metric_ref = ?",
            (p_ref, m_ref),
        ).fetchone()
        return {"count": row["c"], "sum": row["s"], "mean": row["a"]}

    def confusion(
        self, pipeline_a: str, metric_a: str, pipeline_b: str, metric_b: str
    ) -> Dict[Tuple[float, float], int]:
        """
        Joint distribution of two (pipeline, metric) pairs across records.

        Backs the cross-pipeline disagreement views without materialising rows
        in Python.
        """
        refs = (
            self._pipeline_ref(pipeline_a),
            self._metric_ref(metric_a),
            self._pipeline_ref(pipeline_b),
            self._metric_ref(metric_b),
        )
        if any(r is None for r in refs):
            return {}
        rows = self._conn.execute(
            "SELECT a.value av, b.value bv, COUNT(*) n"
            " FROM metrics a JOIN metrics b ON a.ordinal = b.ordinal"
            " WHERE a.pipeline_ref = ? AND a.metric_ref = ?"
            "   AND b.pipeline_ref = ? AND b.metric_ref = ?"
            " GROUP BY a.value, b.value",
            refs,
        ).fetchall()
        return {(r["av"], r["bv"]): r["n"] for r in rows}

    def binary_confusion_by_pipeline(
        self, metric_a: str, metric_b: str
    ) -> Dict[str, Dict[str, int]]:
        """
        Per pipeline, binary confusion counts for two metrics on the same record.

        "Binary" matches the endpoints' rule: exactly 1.0 is a success, anything
        else is a failure.  Only records where both metrics exist are counted,
        which the inner join enforces.
        """
        a_ref = self._metric_ref(metric_a)
        b_ref = self._metric_ref(metric_b)
        if a_ref is None or b_ref is None:
            return {}
        rows = self._conn.execute(
            "SELECT pl.pipeline_id AS pipeline,"
            "       CASE WHEN a.value = 1.0 THEN 1 ELSE 0 END AS ab,"
            "       CASE WHEN b.value = 1.0 THEN 1 ELSE 0 END AS bb,"
            "       COUNT(*) AS n"
            "  FROM metrics a"
            "  JOIN metrics b ON a.ordinal = b.ordinal"
            "                AND a.pipeline_ref = b.pipeline_ref"
            "  JOIN pipelines pl ON pl.pipeline_ref = a.pipeline_ref"
            " WHERE a.metric_ref = ? AND b.metric_ref = ?"
            " GROUP BY pl.pipeline_id, ab, bb",
            (a_ref, b_ref),
        ).fetchall()

        out: Dict[str, Dict[str, int]] = {}
        for row in rows:
            bucket = out.setdefault(
                row["pipeline"], {"a0b0": 0, "a0b1": 0, "a1b0": 0, "a1b1": 0}
            )
            bucket[f"a{row['ab']}b{row['bb']}"] += row["n"]
        return out

    def cross_pipeline_binary_confusion(
        self,
        pipeline_left: str,
        metric_left: str,
        pipeline_right: str,
        metric_right: str,
    ) -> Dict[str, int]:
        """Binary confusion counts across two pipelines, same rules as above."""
        counts = {
            "left0right0": 0,
            "left0right1": 0,
            "left1right0": 0,
            "left1right1": 0,
        }
        refs = (
            self._pipeline_ref(pipeline_left),
            self._metric_ref(metric_left),
            self._pipeline_ref(pipeline_right),
            self._metric_ref(metric_right),
        )
        if any(r is None for r in refs):
            return counts
        rows = self._conn.execute(
            "SELECT CASE WHEN a.value = 1.0 THEN 1 ELSE 0 END AS lb,"
            "       CASE WHEN b.value = 1.0 THEN 1 ELSE 0 END AS rb,"
            "       COUNT(*) AS n"
            "  FROM metrics a JOIN metrics b ON a.ordinal = b.ordinal"
            " WHERE a.pipeline_ref = ? AND a.metric_ref = ?"
            "   AND b.pipeline_ref = ? AND b.metric_ref = ?"
            " GROUP BY lb, rb",
            refs,
        ).fetchall()
        for row in rows:
            counts[f"left{row['lb']}right{row['rb']}"] += row["n"]
        return counts

    def metric_values_by_category(
        self,
    ) -> "Tuple[Dict[str, Dict[str, List[float]]], Dict[str, Dict[str, Dict[str, List[float]]]]]":
        """
        Every numeric metric value, grouped for the by-category summary.

        Returns ``(overall, by_category)`` where ``overall[pipeline][metric]``
        and ``by_category[category][pipeline][metric]`` are lists of values in
        record order.

        This exists because the summary was reading these numbers by parsing the
        whole source artifact: for Beaver that is 880 MB of JSON to collect a few
        tens of thousands of floats. The index stores each prediction's whole
        evaluation block, which is 8.5 MB for the same benchmark.

        Reading the stored blocks rather than the interned ``metrics`` table is
        deliberate, and so is ordering by ``position``. Iterating an evaluation
        block reproduces the record's own metric order, and ``position`` is the
        pipeline's index within that record's ``predictions`` object -- so the
        accumulated lists, the key order of the JSON response, and the order the
        floating-point sums accumulate in are all identical to what parsing the
        artifact produced. Ordering by the interned refs instead sorts by global
        first-appearance, which silently reshuffles both.
        """
        from collections import defaultdict

        categories_of: Dict[int, List[str]] = defaultdict(list)
        for row in self._conn.execute(
            "SELECT rc.ordinal AS ordinal, c.category AS category "
            "FROM record_categories rc "
            "JOIN categories c ON c.category_ref = rc.category_ref "
            "ORDER BY rc.ordinal, rc.category_ref"
        ):
            categories_of[row["ordinal"]].append(row["category"])

        overall: Dict[str, Dict[str, List[float]]] = defaultdict(
            lambda: defaultdict(list)
        )
        by_category: Dict[str, Dict[str, Dict[str, List[float]]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(list))
        )

        for row in self._conn.execute(
            "SELECT pr.ordinal AS ordinal, p.pipeline_id AS pipeline, "
            "       pr.evaluation_json AS evaluation "
            "FROM predictions pr "
            "JOIN pipelines p ON p.pipeline_ref = pr.pipeline_ref "
            "ORDER BY pr.ordinal, pr.position"
        ):
            evaluation = json.loads(row["evaluation"])
            if not isinstance(evaluation, dict):
                continue
            pipeline = row["pipeline"]
            categories = categories_of.get(row["ordinal"], ())
            for metric, value in evaluation.items():
                # The same `isinstance` test the endpoints applied. bool passes
                # it in Python, so a boolean metric counts as 1.0/0.0 rather
                # than being dropped.
                if not isinstance(value, (int, float)):
                    continue
                value = float(value)
                overall[pipeline][metric].append(value)
                for category in categories:
                    by_category[category][pipeline][metric].append(value)

        return overall, by_category

    def iter_records(self) -> "Iterator[Dict[str, Any]]":
        """
        Stream every record in file order, one at a time.

        For whole-corpus aggregations that the index does not model. Unlike
        ``json.load`` this holds one record in memory rather than the entire
        artifact.
        """
        from text2sql_eval_toolkit.indexing.scanner import iter_record_spans

        with self._source_path.open("rb") as fh:
            for span in iter_record_spans(fh):
                yield json.loads(span.raw)


def default_filters(**overrides: Any) -> Dict[str, Any]:
    """Filter defaults matching the dashboard's query parameters."""
    filters: Dict[str, Any] = {
        "q": None,
        "pipeline": None,
        "metric": "execution_accuracy",
        "value": None,
        "op": "eq",
        "pipeline2": None,
        "metric2": None,
        "disagree": False,
        "failed_only": False,
    }
    filters.update(overrides)
    return filters


__all__: Sequence[str] = ["EvalIndex", "RecordSummary", "OPERATORS", "default_filters"]
