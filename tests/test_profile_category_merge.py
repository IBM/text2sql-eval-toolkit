#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

import json
from pathlib import Path

from text2sql_eval_toolkit.profiling.profiling_tools import (
    merge_benchmark_categories_into_records,
)


def test_merge_benchmark_categories_into_records(tmp_path: Path):
    benchmark_path = tmp_path / "benchmark.json"
    benchmark_path.write_text(
        json.dumps(
            [
                {
                    "question_id": 1,
                    "sql": ["SELECT 1"],
                    "meta": {
                        "categories": [
                            "has_group_by",
                            "question_counting",
                        ]
                    },
                }
            ]
        ),
        encoding="utf-8",
    )

    eval_records = [
        {
            "question_id": 1,
            "sql": ["SELECT 1"],
            "meta": {"categories": ["has_aggregation", "difficulty_simple"]},
        }
    ]

    merge_benchmark_categories_into_records(eval_records, benchmark_path)

    assert set(eval_records[0]["meta"]["categories"]) == {
        "difficulty_simple",
        "has_aggregation",
        "has_group_by",
        "question_counting",
    }
