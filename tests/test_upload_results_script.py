#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
What the Hub upload script is willing to publish.

A maintainer's results folder holds more than results: the derived query
indices, which carry every record's raw bytes; backups; logs; and local copies
of a gated benchmark's details. The script uploads to a public dataset, so the
list of files it sends is tested directly rather than trusted to a glob.
"""

import importlib.util
import json
from pathlib import Path

import pytest

SCRIPT = (
    Path(__file__).resolve().parents[1] / "scripts/curation/upload_results_to_hub.py"
)


@pytest.fixture(scope="module")
def upload():
    spec = importlib.util.spec_from_file_location("upload_results_to_hub", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _touch(root: Path, relative: str, text: str = "x") -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


@pytest.fixture
def data_root(tmp_path):
    (tmp_path / "benchmarks.json").write_text(
        json.dumps({"gated": {"requires_sign_in": True}, "open": {}}),
        encoding="utf-8",
    )
    results = tmp_path / "results"
    for name in (
        "open-predictions.json",
        "open-predictions_eval.json",
        "open-predictions_eval_errors.md",
        "open-predictions_eval_summary.json",
        "charts/open-predictions_eval_summary-has_join.png",
        "gated-predictions.json",
        "gated-predictions_eval.json",
        "gated-predictions_eval_errors.md",
        "gated-predictions_eval_summary.json",
        "gated-predictions_eval_summary.csv",
        "gated-predictions_eval_summary.md",
        "charts/gated-predictions_eval_summary.png",
        "charts/gated-predictions_eval_summary-has_join.png",
        ".index/gated-predictions_eval.sqlite",
        ".index/open-predictions_eval.sqlite",
        "bak/gated-predictions_eval.json",
        "logs/run.txt",
        "gated/pipeline/model/predictions.json",
        "README.md",
    ):
        _touch(results, name)
    return tmp_path


def test_restricted_benchmarks_come_from_the_registry(upload, data_root):
    assert "gated" in upload._restricted_benchmarks(data_root)
    assert "open" not in upload._restricted_benchmarks(data_root)


def test_packaged_registry_flags_beaver_even_without_a_data_root_registry(
    upload, tmp_path
):
    assert "beaver" in upload._restricted_benchmarks(tmp_path)


def test_only_publishable_files_are_listed(upload, data_root):
    files = upload._publishable_files(data_root / "results", {"gated"})
    assert files == [
        "README.md",
        "charts/gated-predictions_eval_summary.png",
        "charts/open-predictions_eval_summary-has_join.png",
        "gated-predictions_eval_summary.csv",
        "gated-predictions_eval_summary.json",
        "gated-predictions_eval_summary.md",
        "open-predictions.json",
        "open-predictions_eval.json",
        "open-predictions_eval_errors.md",
        "open-predictions_eval_summary.json",
    ]


def test_the_environment_restricts_a_benchmark_no_registry_flags(
    upload, data_root, monkeypatch
):
    """
    `TEXT2SQL_SIGN_IN_BENCHMARKS` is the other way a deployment marks details
    restricted. A benchmark marked only that way was published in full.
    """
    monkeypatch.setenv(upload.SIGN_IN_BENCHMARKS_ENV, " open , ")
    assert upload._restricted_benchmarks(data_root) >= {"gated", "open"}

    files = upload._publishable_files(
        data_root / "results", upload._restricted_benchmarks(data_root)
    )
    assert "open-predictions.json" not in files
    assert "open-predictions_eval.json" not in files
    assert "charts/open-predictions_eval_summary-has_join.png" not in files
    assert "open-predictions_eval_summary.json" in files


def test_a_restricted_id_in_another_case_still_restricts(upload, data_root):
    """`BEAVER-…` opens Beaver's files on a case-insensitive filesystem."""
    files = upload._publishable_files(data_root / "results", {"OPEN"})
    assert "open-predictions_eval.json" not in files
    assert "open-predictions_eval_summary.json" in files


def test_a_prefix_is_not_a_benchmark(upload, data_root):
    """`gated_other-…` is not `gated`'s file."""
    _touch(data_root / "results", "gated_other-predictions_eval.json")
    files = upload._publishable_files(data_root / "results", {"gated"})
    assert "gated_other-predictions_eval.json" in files


def test_the_manifest_lists_only_what_is_uploaded(upload, data_root):
    manifest = upload._generate_manifest(data_root / "results", {"gated"})
    gated = manifest["benchmarks"]["gated"]["pipelines"]["default"]["files"]
    assert gated == [
        "gated-predictions_eval_summary.csv",
        "gated-predictions_eval_summary.json",
        "gated-predictions_eval_summary.md",
    ]
    assert "gated" in manifest["benchmarks"]  # the flat entry, not the nested dir
    assert ".index" not in manifest["benchmarks"]


def test_a_summary_report_with_categories_is_refused(upload, data_root):
    _touch(
        data_root / "results",
        "gated-predictions_eval_summary.md",
        "# Summary\n\n## Overall\n\n## Category: `has_join`\n",
    )
    with pytest.raises(SystemExit, match="query category"):
        upload._check_restricted_summaries(data_root / "results", {"gated"})


def test_a_summary_report_with_categories_is_refused_whatever_case_the_id_is(
    upload, data_root
):
    """
    The check matches the file's spelling, as `_is_publishable` does.

    Building the path from the id's spelling instead asked about
    `GATED-…md`, which on a case-sensitive filesystem does not exist -- so the
    check passed and the real report was published, categories and all.
    """
    _touch(
        data_root / "results",
        "gated-predictions_eval_summary.md",
        "# Summary\n\n## Overall\n\n## Category: `has_join`\n",
    )
    assert "gated-predictions_eval_summary.md" in upload._publishable_files(
        data_root / "results", {"GATED"}
    )
    with pytest.raises(SystemExit, match="query category") as refusal:
        upload._check_restricted_summaries(data_root / "results", {"GATED"})
    # The report that would actually be uploaded, named as it is on disk. A
    # case-insensitive filesystem resolves the id's spelling to the same file,
    # so this -- not the refusal itself -- is what the test can check anywhere.
    assert "gated-predictions_eval_summary.md" in str(refusal.value)


def test_a_restricted_id_is_casefolded_at_the_source(upload, data_root, monkeypatch):
    """Every comparison downstream is then against one spelling."""
    monkeypatch.setenv(upload.SIGN_IN_BENCHMARKS_ENV, "OPEN")
    assert "open" in upload._restricted_benchmarks(data_root)


def test_the_manifest_skips_a_nested_dir_spelled_in_another_case(upload, tmp_path):
    """
    `_is_publishable` casefolds, so the manifest has to as well: naming a
    directory the upload then skips makes `results fetch` fail on it.
    """
    results = tmp_path / "results"
    _touch(results, "Secret/pipeline/model/predictions.json")
    assert upload._publishable_files(results, {"secret"}) == []
    manifest = upload._generate_manifest(results, {"secret"})
    assert manifest["benchmarks"] == {}


def test_the_dry_run_lists_no_gated_detail(upload, data_root, capsys):
    upload.main(["--data-root", str(data_root), "--dry-run"])
    out = capsys.readouterr().out
    for forbidden in (
        "gated-predictions.json",
        "gated-predictions_eval.json",
        "gated-predictions_eval_errors.md",
        "charts/gated-predictions_eval_summary-has_join.png",
        ".index",
        "bak/",
    ):
        assert forbidden not in out
    assert "results/gated-predictions_eval_summary.json" in out
