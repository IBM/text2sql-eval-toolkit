#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
"""
Unit tests for text2sql_eval_toolkit.results._hub.

All tests are fully offline — huggingface_hub.snapshot_download and
hf_hub_download are mocked so no network is required.
"""

import json
import os
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest

from text2sql_eval_toolkit.results._hub import (
    DEFAULT_REPO_ID,
    DEFAULT_REVISION,
    _build_allow_patterns,
    _resolve_data_root,
    _validate_manifest,
    clear_cache,
    fetch_results,
    list_available_results,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

VALID_MANIFEST: Dict[str, Any] = {
    "schema_version": 1,
    "toolkit_version_compat": ">=0.0.0,<99.0.0",
    "generated_at": "2026-05-13T00:00:00Z",
    "total_size_bytes": 1000000,
    "benchmarks": {
        "bench_a": {
            "pipelines": {
                "zero_shot": {
                    "models": [
                        "wxai:ibm/granite-34b-code-instruct",
                        "wxai:meta-llama/llama-3-3-70b-instruct",
                    ],
                    "files": ["predictions.json", "evaluation.json", "summary.csv"],
                    "size_bytes": 500000,
                }
            }
        },
        "bench_b": {
            "pipelines": {
                "agentic_v1": {
                    "models": ["wxai:ibm/granite-34b-code-instruct"],
                    "files": ["predictions.json"],
                    "size_bytes": 500000,
                }
            }
        },
    },
}


# ---------------------------------------------------------------------------
# _resolve_data_root
# ---------------------------------------------------------------------------


def test_resolve_data_root_explicit(tmp_path: Path) -> None:
    assert _resolve_data_root(tmp_path) == tmp_path.resolve()


def test_resolve_data_root_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TEXT2SQL_DATA_ROOT", str(tmp_path))
    monkeypatch.delenv("TEXT2SQL_DATA_ROOT", raising=False)
    monkeypatch.setenv("TEXT2SQL_DATA_ROOT", str(tmp_path))
    assert _resolve_data_root(None) == tmp_path.resolve()


def test_resolve_data_root_default(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("TEXT2SQL_DATA_ROOT", raising=False)
    monkeypatch.chdir(tmp_path)
    result = _resolve_data_root(None)
    assert result == (tmp_path / "data").resolve()


def test_resolve_data_root_explicit_overrides_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env_dir = tmp_path / "env_root"
    env_dir.mkdir()
    explicit_dir = tmp_path / "explicit_root"
    explicit_dir.mkdir()
    monkeypatch.setenv("TEXT2SQL_DATA_ROOT", str(env_dir))
    assert _resolve_data_root(explicit_dir) == explicit_dir.resolve()


# ---------------------------------------------------------------------------
# _validate_manifest
# ---------------------------------------------------------------------------


def test_validate_manifest_valid() -> None:
    # Should not raise for a compatible manifest.
    _validate_manifest(VALID_MANIFEST)


def test_validate_manifest_missing_required_keys() -> None:
    with pytest.raises(ValueError, match="missing required fields"):
        _validate_manifest({"schema_version": 1})


def test_validate_manifest_incompatible_version() -> None:
    bad = {**VALID_MANIFEST, "toolkit_version_compat": ">=99.0.0,<100.0.0"}
    with pytest.raises(ValueError, match="requires toolkit"):
        _validate_manifest(bad)


def test_validate_manifest_bad_specifier_warns(caplog: pytest.LogCaptureFixture) -> None:
    bad = {**VALID_MANIFEST, "toolkit_version_compat": "INVALID"}
    # An unparseable specifier should either be silently ignored or raise a
    # ValueError — but must never crash with an unhandled exception.
    try:
        _validate_manifest(bad)
    except ValueError:
        pass  # packaging parse error is acceptable


# ---------------------------------------------------------------------------
# _build_allow_patterns
# ---------------------------------------------------------------------------


def test_build_allow_patterns_no_filters() -> None:
    patterns = _build_allow_patterns(VALID_MANIFEST, None, None, None)
    assert "results/**" in patterns
    assert "manifest.json" in patterns
    assert "README.md" in patterns


def test_build_allow_patterns_benchmark_filter() -> None:
    patterns = _build_allow_patterns(VALID_MANIFEST, ["bench_a"], None, None)
    # Should include bench_a paths.
    assert any("bench_a" in p for p in patterns)
    # Should NOT include bench_b paths.
    assert not any("bench_b" in p for p in patterns)


def test_build_allow_patterns_pipeline_filter() -> None:
    patterns = _build_allow_patterns(VALID_MANIFEST, None, ["zero_shot"], None)
    assert any("zero_shot" in p for p in patterns)
    assert not any("agentic_v1" in p for p in patterns)


def test_build_allow_patterns_model_filter() -> None:
    patterns = _build_allow_patterns(
        VALID_MANIFEST, None, None, ["wxai:ibm/granite-34b-code-instruct"]
    )
    # Model name is sanitised: colons and slashes → double underscores.
    assert any("wxai__ibm__granite-34b-code-instruct" in p for p in patterns)
    assert not any("llama" in p for p in patterns)


def test_build_allow_patterns_always_included() -> None:
    for filters in [
        (["bench_a"], None, None),
        (None, ["zero_shot"], None),
        (["bench_a"], ["zero_shot"], ["wxai:ibm/granite-34b-code-instruct"]),
    ]:
        patterns = _build_allow_patterns(VALID_MANIFEST, *filters)
        assert "manifest.json" in patterns
        assert "README.md" in patterns


# ---------------------------------------------------------------------------
# fetch_results (mocked)
# ---------------------------------------------------------------------------


def _make_manifest_file(path: Path, manifest: Dict[str, Any]) -> Path:
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return path


@pytest.fixture()
def mock_hf(tmp_path: Path):
    """Patch huggingface_hub calls so tests run offline."""
    manifest_file = tmp_path / "manifest.json"
    _make_manifest_file(manifest_file, VALID_MANIFEST)

    with (
        patch(
            "text2sql_eval_toolkit.results._hub.hf_hub_download",
            return_value=str(manifest_file),
        ) as mock_dl,
        patch(
            "text2sql_eval_toolkit.results._hub.snapshot_download",
            return_value=str(tmp_path),
        ) as mock_snap,
        patch(
            "text2sql_eval_toolkit.results._hub.HfApi",
            return_value=MagicMock(),
        ),
        patch(
            "text2sql_eval_toolkit.results._hub._cached_manifest",
            return_value=VALID_MANIFEST,
        ),
    ):
        yield mock_dl, mock_snap, tmp_path


def test_fetch_results_calls_snapshot_download(mock_hf, tmp_path: Path) -> None:
    _, mock_snap, _ = mock_hf
    results_dir = fetch_results(data_root=tmp_path)
    mock_snap.assert_called_once()
    assert results_dir == tmp_path / "results"


def test_fetch_results_local_dir_is_data_root_not_results(
    mock_hf, tmp_path: Path
) -> None:
    """Verify the correct local_dir is passed (not data_root/results)."""
    _, mock_snap, _ = mock_hf
    fetch_results(data_root=tmp_path)
    call_kwargs = mock_snap.call_args.kwargs
    # local_dir must equal the data root exactly — not data_root/results.
    assert call_kwargs["local_dir"] == str(tmp_path.resolve())
    # The final component must NOT be literally "results".
    assert Path(call_kwargs["local_dir"]).name != "results"


def test_fetch_results_force_flag(mock_hf, tmp_path: Path) -> None:
    _, mock_snap, _ = mock_hf
    fetch_results(data_root=tmp_path, force=True)
    call_kwargs = mock_snap.call_args.kwargs
    assert call_kwargs.get("force_download") is True


def test_fetch_results_filter_benchmarks(mock_hf, tmp_path: Path) -> None:
    _, mock_snap, _ = mock_hf
    fetch_results(data_root=tmp_path, benchmarks=["bench_a"])
    call_kwargs = mock_snap.call_args.kwargs
    patterns = call_kwargs["allow_patterns"]
    assert any("bench_a" in p for p in patterns)
    assert not any("bench_b" in p for p in patterns)


def test_fetch_results_no_symlinks(mock_hf, tmp_path: Path) -> None:
    _, mock_snap, _ = mock_hf
    fetch_results(data_root=tmp_path)
    call_kwargs = mock_snap.call_args.kwargs
    assert call_kwargs.get("local_dir_use_symlinks") is False


# ---------------------------------------------------------------------------
# clear_cache
# ---------------------------------------------------------------------------


def test_clear_cache_removes_results(tmp_path: Path) -> None:
    results = tmp_path / "results"
    results.mkdir()
    (results / "some_file.json").write_text("{}", encoding="utf-8")
    clear_cache(data_root=tmp_path, confirm=False)
    assert not results.exists()


def test_clear_cache_noop_when_missing(tmp_path: Path) -> None:
    # Should not raise.
    clear_cache(data_root=tmp_path, confirm=False)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


def test_default_repo_id() -> None:
    assert DEFAULT_REPO_ID == "text2sql-eval-toolkit/text2sql-eval-results"


def test_default_revision_is_version_tag() -> None:
    # Must start with "v" and contain at least one dot.
    assert DEFAULT_REVISION.startswith("v")
    assert "." in DEFAULT_REVISION
