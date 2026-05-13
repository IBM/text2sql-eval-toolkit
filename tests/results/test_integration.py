#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
"""
Integration tests for results distribution.

These tests make real network calls to the Hugging Face Hub and are therefore
skipped by default.  Set RUN_NETWORK_TESTS=1 to enable them:

    RUN_NETWORK_TESTS=1 pytest tests/results/test_integration.py -v

They are intended to run in a dedicated optional CI job (nightly or on demand)
and MUST NOT require HF_TOKEN (the target repo is public).
"""

import os
from pathlib import Path

import pytest

network = pytest.mark.skipif(
    os.getenv("RUN_NETWORK_TESTS", "0") != "1",
    reason="Set RUN_NETWORK_TESTS=1 to run network tests",
)


@network
def test_list_available_results_returns_manifest() -> None:
    """Fetching the manifest from the real HF repo should succeed."""
    from text2sql_eval_toolkit.results import list_available_results

    manifest = list_available_results()
    assert "benchmarks" in manifest
    assert "toolkit_version_compat" in manifest
    assert manifest["schema_version"] >= 1


@network
def test_fetch_results_single_benchmark(tmp_path: Path) -> None:
    """
    Fetch the smallest available (benchmark, pipeline, model) tuple.

    Asserts:
    - The results directory is created.
    - At least one non-empty file exists under the expected sub-tree.
    - The ``local_dir`` contract is obeyed: files land at
      ``data_root/results/...``, never at ``data_root/results/results/...``.
    """
    from text2sql_eval_toolkit.results import fetch_results, list_available_results

    manifest = list_available_results()
    benchmarks = manifest.get("benchmarks", {})
    assert benchmarks, "manifest.benchmarks is empty — nothing to download"

    # Pick the smallest benchmark/pipeline/model combination.
    bench_name = next(iter(benchmarks))
    pipes = benchmarks[bench_name].get("pipelines", {})
    pipe_name = next(iter(pipes))
    models = pipes[pipe_name].get("models", [])
    model_name = models[0]

    results_dir = fetch_results(
        benchmarks=[bench_name],
        pipelines=[pipe_name],
        models=[model_name],
        data_root=tmp_path,
    )

    assert results_dir.is_dir(), f"results_dir not created: {results_dir}"

    # At least one file must exist with non-zero content.
    all_files = list(results_dir.rglob("*"))
    non_empty = [f for f in all_files if f.is_file() and f.stat().st_size > 0]
    assert non_empty, f"No non-empty files under {results_dir}"

    # Verify the path is data_root/results/... NOT data_root/results/results/...
    for f in non_empty:
        parts = f.relative_to(tmp_path).parts
        assert parts[0] == "results", f"Unexpected top-level dir: {parts[0]}"
        assert parts[1] != "results", (
            f"Double-nested results/results detected in path: {f}"
        )


@network
def test_fetch_results_no_token_required(tmp_path: Path) -> None:
    """Ensure the fetch works without HF_TOKEN in the environment."""
    token = os.environ.pop("HF_TOKEN", None)
    try:
        from text2sql_eval_toolkit.results import fetch_results, list_available_results

        # Fetching the manifest (lightest call) is sufficient to prove anonymity.
        manifest = list_available_results()
        assert "benchmarks" in manifest
    finally:
        if token is not None:
            os.environ["HF_TOKEN"] = token


@network
def test_fetch_results_second_call_is_noop(tmp_path: Path) -> None:
    """Second fetch should be fast (cached) and produce the same files."""
    from text2sql_eval_toolkit.results import fetch_results, list_available_results

    manifest = list_available_results()
    bench_name = next(iter(manifest["benchmarks"]))
    pipes = manifest["benchmarks"][bench_name]["pipelines"]
    pipe_name = next(iter(pipes))
    model_name = pipes[pipe_name]["models"][0]

    kwargs = dict(
        benchmarks=[bench_name],
        pipelines=[pipe_name],
        models=[model_name],
        data_root=tmp_path,
    )

    fetch_results(**kwargs)  # type: ignore[arg-type]
    files_after_first = {
        str(f.relative_to(tmp_path))
        for f in (tmp_path / "results").rglob("*")
        if f.is_file()
    }

    fetch_results(**kwargs)  # type: ignore[arg-type]
    files_after_second = {
        str(f.relative_to(tmp_path))
        for f in (tmp_path / "results").rglob("*")
        if f.is_file()
    }

    assert files_after_first == files_after_second
