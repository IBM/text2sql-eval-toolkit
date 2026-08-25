#!/usr/bin/env python
#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Build a tiny, self-contained data root for the end-to-end tests.

The real results snapshot is ~4 GB and lives on the Hugging Face Hub, so CI
cannot browse it.  What the E2E tests actually need is much smaller: a registry
entry, an evaluation artifact with enough records to paginate, and a summary --
all synthetic, all deterministic.

Deterministic matters more than realistic.  These tests assert that a URL
reproduces a view exactly, so the underlying data must not vary between the run
that copies the link and the run that opens it.

Usage::

    python scripts/ci/make_e2e_fixture.py /tmp/e2e-data
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

BENCHMARK = "e2e_demo"
PIPELINES = [
    "wxai:openai/gpt-oss-120b-greedy-zero-shot-chatapi",
    "wxai:ibm/granite-4-h-small-agentic-baseline1-3attempts",
]
RECORD_COUNT = 60


def _dataframe(rows: list[list[object]], columns: list[str]) -> str:
    """A dataframe as the toolkit stores them: pandas ``orient='split'``."""
    return json.dumps(
        {"columns": columns, "index": list(range(len(rows))), "data": rows}
    )


def _records() -> list[dict]:
    out = []
    for i in range(RECORD_COUNT):
        predictions = {}
        for offset, pipeline in enumerate(PIPELINES):
            # A fixed pattern rather than anything random: the same record must
            # pass or fail identically on every run, or a filtered link cannot
            # be asserted against.
            exact = (i + offset) % 2
            subset = (i + offset) % 3 != 0
            predictions[pipeline] = {
                "predicted_sql": f"SELECT {i} -- {pipeline.split(':')[-1][:12]}",
                "predicted_df": _dataframe([[i]], ["n"]),
                "model_name": pipeline.split("-")[0],
                "inference_time_ms": 100 + i,
                "evaluation": {
                    "execution_accuracy": exact,
                    "subset_non_empty_execution_accuracy": int(subset),
                    "llm_score": 1.0 if exact else 0.0,
                    "prompt_tokens": 500 + i,
                    "completion_tokens": 40 + i,
                },
            }
        out.append(
            {
                "id": f"rec-{i:03d}",
                "question": f"Question {i} about {'orders' if i % 3 else 'customers'}",
                "db_id": "shop" if i % 2 else "hr",
                "sql": [f"SELECT {i} -- gold"],
                "gt_df": [_dataframe([[i]], ["n"])],
                "predictions": predictions,
            }
        )
    return out


def _summary(records: list[dict]) -> dict:
    summary: dict = {}
    for pipeline in PIPELINES:
        metrics: dict = {}
        for metric in (
            "execution_accuracy",
            "subset_non_empty_execution_accuracy",
            "llm_score",
        ):
            values = [r["predictions"][pipeline]["evaluation"][metric] for r in records]
            average = sum(values) / len(values)
            metrics[metric] = {"average": average, "stddev": 0.5}
        metrics["num_records"] = len(records)
        metrics["num_evaluated"] = len(records)
        metrics["num_predictions"] = len(records)
        summary[pipeline] = metrics
    return summary


def build(root: Path) -> None:
    results = root / "results"
    benchmarks = root / "benchmarks"
    results.mkdir(parents=True, exist_ok=True)
    benchmarks.mkdir(parents=True, exist_ok=True)

    records = _records()

    (results / f"{BENCHMARK}-predictions_eval.json").write_text(
        json.dumps(records), encoding="utf-8"
    )
    (results / f"{BENCHMARK}-predictions_eval_summary.json").write_text(
        json.dumps(_summary(records)), encoding="utf-8"
    )
    (benchmarks / f"{BENCHMARK}.json").write_text(
        json.dumps(
            [
                {"id": r["id"], "question": r["question"], "sql": r["sql"]}
                for r in records
            ]
        ),
        encoding="utf-8",
    )
    (benchmarks / f"{BENCHMARK}-schema.json").write_text("{}", encoding="utf-8")

    (root / "benchmarks.json").write_text(
        json.dumps(
            {
                BENCHMARK: {
                    "name": BENCHMARK,
                    "description": "Synthetic fixture for the end-to-end tests.",
                    "data": f"benchmarks/{BENCHMARK}.json",
                    "schema": f"benchmarks/{BENCHMARK}-schema.json",
                    "predictions": f"results/{BENCHMARK}-predictions.json",
                    "db_engine": {
                        "db_type": "sqlite",
                        "db_folder": "benchmarks/dbs/none",
                        "schema_name": "",
                        "connection_string_env_var": "",
                    },
                }
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(
        f"E2E fixture written to {root} ({len(records)} records, {len(PIPELINES)} pipelines)"
    )


if __name__ == "__main__":
    build(Path(sys.argv[1] if len(sys.argv) > 1 else "e2e-data").resolve())
