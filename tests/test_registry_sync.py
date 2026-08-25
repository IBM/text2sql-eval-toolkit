#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
The two copies of each benchmark registry must agree.

``get_benchmarks_file_path()`` prefers ``$TEXT2SQL_DATA_ROOT`` and then
``./data`` before the copy packaged in the wheel, so in a source checkout the
packaged copy is never read. Drift is therefore invisible during development and
only reaches pip-installed users -- which is how the packaged copy lost every
benchmark's ``logo`` without anyone noticing.
"""

import json
from pathlib import Path

import pytest

# Import the sync script by path: scripts/ is not a package.
import importlib.util

REPO_ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location(
    "sync_packaged_registry",
    REPO_ROOT / "scripts" / "curation" / "sync_packaged_registry.py",
)
sync_module = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(sync_module)


def test_packaged_registries_match_the_checkout_copies():
    """
    The regression guard. If this fails, run:
        python scripts/curation/sync_packaged_registry.py
    """
    stale = sync_module.diverged()
    assert not stale, (
        "packaged registry copies are out of sync with data/:\n  "
        + "\n  ".join(f"{packaged} != {canonical}" for canonical, packaged in stale)
        + "\nRun: python scripts/curation/sync_packaged_registry.py"
    )


def test_both_registry_files_are_covered():
    names = {canonical.name for canonical, _ in sync_module.registry_pairs()}
    assert names == {"benchmarks.json", "test-benchmarks.json"}


@pytest.mark.parametrize("name", ["benchmarks.json", "test-benchmarks.json"])
def test_packaged_copy_exists_for_installed_users(name):
    packaged = REPO_ROOT / "src" / "text2sql_eval_toolkit" / "data" / name
    assert packaged.is_file(), f"{name} would be missing from the wheel"


def test_every_benchmark_keeps_its_logo_in_the_packaged_copy():
    """
    The concrete thing that was lost: a pip-installed dashboard showed generic
    tiles because the packaged registry had no `logo` entries.
    """
    packaged = json.loads(
        (
            REPO_ROOT / "src" / "text2sql_eval_toolkit" / "data" / "benchmarks.json"
        ).read_text(encoding="utf-8")
    )
    missing = [name for name, cfg in packaged.items() if not cfg.get("logo")]
    assert not missing, f"benchmarks with no logo in the packaged registry: {missing}"


def test_comparison_ignores_formatting_not_content(tmp_path):
    """Indentation or key order alone must not read as drift."""
    a = tmp_path / "a.json"
    b = tmp_path / "b.json"
    a.write_text(json.dumps({"x": {"b": 1, "a": 2}}, indent=2), encoding="utf-8")
    b.write_text(json.dumps({"x": {"a": 2, "b": 1}}, indent=8), encoding="utf-8")
    assert sync_module.normalise(a) == sync_module.normalise(b)


def test_comparison_detects_a_real_difference(tmp_path):
    a = tmp_path / "a.json"
    b = tmp_path / "b.json"
    a.write_text(json.dumps({"x": {"logo": "a.png"}}), encoding="utf-8")
    b.write_text(json.dumps({"x": {}}), encoding="utf-8")
    assert sync_module.normalise(a) != sync_module.normalise(b)
