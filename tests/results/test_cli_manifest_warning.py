#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
The published manifest can be stale.

A manifest generated before the flat-layout fix lists the ``results/``
sub-directories (``bak``, ``logs``, ``charts``) as if they were benchmarks.
Printing those as available data sends people looking for benchmarks that do not
exist, so the CLI has to say what is happening.
"""

from unittest.mock import patch

from text2sql_eval_toolkit import cli

STALE = {
    "schema_version": 1,
    "toolkit_version_compat": ">=1.1.0,<2.0.0",
    "generated_at": "2026-05-13T22:08:06Z",
    "total_size_bytes": 3_950_000_000,
    "benchmarks": {
        "bak": {"pipelines": {"default": {"models": ["default"]}}},
        "logs": {"pipelines": {"default": {"models": ["default"]}}},
        "charts": {"pipelines": {"default": {"models": ["default"]}}},
    },
}

HEALTHY = {
    **STALE,
    "benchmarks": {
        "bird_mini_dev_sqlite": {
            "pipelines": {"default": {"models": ["wxai:openai/gpt-oss-120b"]}}
        }
    },
}


def _run_list(manifest, capsys):
    args = type("Args", (), {"revision": None, "data_root": None})()
    with patch.object(cli, "__name__", cli.__name__):
        with patch(
            "text2sql_eval_toolkit.results.list_available_results",
            return_value=manifest,
        ):
            code = cli._cmd_list(args)
    return code, capsys.readouterr()


def test_stale_manifest_is_called_out(capsys):
    code, captured = _run_list(STALE, capsys)
    assert code == 0
    assert "lists no benchmarks" in captured.err
    # The user must also learn that downloading still works.
    assert "fetch" in captured.err


def test_healthy_manifest_produces_no_warning(capsys):
    code, captured = _run_list(HEALTHY, capsys)
    assert code == 0
    assert "lists no benchmarks" not in captured.err
    assert "bird_mini_dev_sqlite" in captured.out


def test_manifest_mixing_real_and_marker_names_is_not_flagged(capsys):
    """Only an entirely marker-named listing is conclusive evidence of staleness."""
    mixed = {
        **STALE,
        "benchmarks": {**STALE["benchmarks"], "beaver": {"pipelines": {}}},
    }
    _, captured = _run_list(mixed, capsys)
    assert "lists no benchmarks" not in captured.err
