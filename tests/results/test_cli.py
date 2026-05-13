#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
"""
Unit tests for text2sql_eval_toolkit.cli (argparse wiring and normaliser).
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from text2sql_eval_toolkit.cli import _build_parser, _normalise_list, main


# ---------------------------------------------------------------------------
# _normalise_list
# ---------------------------------------------------------------------------


def test_normalise_list_none() -> None:
    assert _normalise_list(None) is None


def test_normalise_list_empty() -> None:
    assert _normalise_list([]) is None


def test_normalise_list_single() -> None:
    assert _normalise_list(["bench_a"]) == ["bench_a"]


def test_normalise_list_comma_separated() -> None:
    assert _normalise_list(["bench_a,bench_b"]) == ["bench_a", "bench_b"]


def test_normalise_list_repeated() -> None:
    assert _normalise_list(["bench_a", "bench_b"]) == ["bench_a", "bench_b"]


def test_normalise_list_mixed() -> None:
    assert _normalise_list(["bench_a,bench_b", "bench_c"]) == [
        "bench_a",
        "bench_b",
        "bench_c",
    ]


def test_normalise_list_strips_whitespace() -> None:
    assert _normalise_list([" bench_a , bench_b "]) == ["bench_a", "bench_b"]


# ---------------------------------------------------------------------------
# Argument parser wiring
# ---------------------------------------------------------------------------


def test_parser_fetch_defaults() -> None:
    parser = _build_parser()
    args = parser.parse_args(["results", "fetch"])
    assert args.command == "results"
    assert args.results_command == "fetch"
    assert args.benchmarks == []
    assert args.pipelines == []
    assert args.models == []
    assert args.revision is None
    assert args.data_root is None
    assert args.force is False


def test_parser_fetch_benchmarks_comma() -> None:
    parser = _build_parser()
    args = parser.parse_args(["results", "fetch", "--benchmarks", "a,b"])
    assert args.benchmarks == ["a,b"]  # raw; normalised by _normalise_list


def test_parser_fetch_benchmarks_repeated() -> None:
    parser = _build_parser()
    args = parser.parse_args(
        ["results", "fetch", "--benchmarks", "a", "--benchmarks", "b"]
    )
    assert args.benchmarks == ["a", "b"]


def test_parser_fetch_force() -> None:
    parser = _build_parser()
    args = parser.parse_args(["results", "fetch", "--force"])
    assert args.force is True


def test_parser_fetch_revision() -> None:
    parser = _build_parser()
    args = parser.parse_args(["results", "fetch", "--revision", "v1.0.0"])
    assert args.revision == "v1.0.0"


def test_parser_fetch_data_root() -> None:
    parser = _build_parser()
    args = parser.parse_args(["results", "fetch", "--data-root", "/tmp/data"])
    assert args.data_root == "/tmp/data"


def test_parser_list() -> None:
    parser = _build_parser()
    args = parser.parse_args(["results", "list"])
    assert args.results_command == "list"


def test_parser_clear_defaults() -> None:
    parser = _build_parser()
    args = parser.parse_args(["results", "clear"])
    assert args.results_command == "clear"
    assert args.yes is False


def test_parser_clear_yes() -> None:
    parser = _build_parser()
    args = parser.parse_args(["results", "clear", "--yes"])
    assert args.yes is True


# ---------------------------------------------------------------------------
# main() integration (mocked)
# ---------------------------------------------------------------------------


def test_main_fetch_calls_fetch_results(tmp_path: Path) -> None:
    with patch(
        "text2sql_eval_toolkit.results.fetch_results",
        return_value=tmp_path / "results",
    ) as mock_fetch:
        with pytest.raises(SystemExit) as exc_info:
            main(
                [
                    "results",
                    "fetch",
                    "--benchmarks",
                    "bench_a",
                    "--data-root",
                    str(tmp_path),
                ]
            )
        assert exc_info.value.code == 0
        mock_fetch.assert_called_once()
        call_kwargs = mock_fetch.call_args.kwargs
        assert call_kwargs["benchmarks"] == ["bench_a"]
        assert call_kwargs["data_root"] == tmp_path


def test_main_fetch_propagates_error(capsys: pytest.CaptureFixture) -> None:
    with patch(
        "text2sql_eval_toolkit.results.fetch_results",
        side_effect=RuntimeError("boom"),
    ):
        with pytest.raises(SystemExit) as exc_info:
            main(["results", "fetch"])
        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "boom" in captured.err


def test_main_clear_calls_clear_cache(tmp_path: Path) -> None:
    with patch("text2sql_eval_toolkit.results.clear_cache") as mock_clear:
        with pytest.raises(SystemExit) as exc_info:
            main(["results", "clear", "--yes", "--data-root", str(tmp_path)])
        assert exc_info.value.code == 0
        mock_clear.assert_called_once_with(data_root=tmp_path, confirm=False)


def test_main_no_subcommand_exits_nonzero() -> None:
    with pytest.raises(SystemExit) as exc_info:
        main([])
    assert exc_info.value.code != 0
