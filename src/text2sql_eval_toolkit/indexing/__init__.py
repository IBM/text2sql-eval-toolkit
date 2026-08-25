#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Derived indices over evaluation artifacts.

See :mod:`text2sql_eval_toolkit.indexing.builder` for what is stored and why.
Indices live in ``<results>/.index/`` and are safe to delete at any time.
"""

from text2sql_eval_toolkit.indexing.builder import (
    SCHEMA_VERSION,
    build_all,
    build_index,
    index_path_for,
    is_stale,
)
from text2sql_eval_toolkit.indexing.scanner import RecordSpan, iter_record_spans

__all__ = [
    "SCHEMA_VERSION",
    "build_all",
    "build_index",
    "index_path_for",
    "is_stale",
    "RecordSpan",
    "iter_record_spans",
]
