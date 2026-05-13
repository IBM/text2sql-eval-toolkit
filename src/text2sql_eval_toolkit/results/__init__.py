#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
"""
Results distribution: download pre-computed evaluation artefacts from the
Hugging Face Hub.

Usage::

    from text2sql_eval_toolkit.results import fetch_results
    fetch_results()                              # all results, default revision
    fetch_results(benchmarks=["bird_mini_dev_sqlite"])   # single benchmark
"""

from ._hub import (
    DEFAULT_REPO_ID,
    DEFAULT_REVISION,
    clear_cache,
    fetch_results,
    list_available_results,
)

__all__ = [
    "DEFAULT_REPO_ID",
    "DEFAULT_REVISION",
    "fetch_results",
    "list_available_results",
    "clear_cache",
]
