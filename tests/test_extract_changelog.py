#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Tests for ``scripts/ci/extract_changelog.py``.

The GitHub Release page is generated from the changelog from 1.5.0 onwards, so
this script is the difference between a release with notes and a release with an
empty page.  The case that matters most is the failure: a tag whose version has
no changelog section must stop the workflow rather than publish a blank page.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "ci" / "extract_changelog.py"


def _load():
    """Import the script by path; ``scripts/`` is not an importable package."""
    spec = importlib.util.spec_from_file_location("extract_changelog", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


extract_changelog = _load()
extract = extract_changelog.extract
NotFound = extract_changelog.NotFound


SAMPLE = """\
# Changelog

Preamble that belongs to no release.

## [2.0.0] - 2026-09-01

### Added

- The new thing.

## [1.9.0] - 2026-08-01

### Fixed

- The old thing.

## [1.8.0] - 2026-07-01

### Fixed

- The older thing.

[2.0.0]: https://example.invalid/releases/tag/v2.0.0
[1.9.0]: https://example.invalid/releases/tag/v1.9.0
"""


def test_extracts_the_requested_section_only():
    assert extract(SAMPLE, "1.9.0") == "### Fixed\n\n- The old thing."


def test_the_newest_section_stops_at_the_next_heading():
    # The first section is the one most likely to run on and swallow the rest.
    assert extract(SAMPLE, "2.0.0") == "### Added\n\n- The new thing."


def test_the_oldest_section_drops_the_link_definitions():
    # The last section runs to end-of-file, where the link references live.
    notes = extract(SAMPLE, "1.8.0")
    assert notes == "### Fixed\n\n- The older thing."
    assert "https://example.invalid" not in notes


def test_a_leading_v_is_accepted_so_a_tag_name_works():
    assert extract(SAMPLE, "v1.9.0") == extract(SAMPLE, "1.9.0")


def test_the_heading_itself_is_not_repeated_in_the_body():
    # GitHub titles the release with the tag; repeating it reads as a duplicate.
    assert "## [1.9.0]" not in extract(SAMPLE, "1.9.0")


def test_the_preamble_is_never_returned():
    for version in ("2.0.0", "1.9.0", "1.8.0"):
        assert "Preamble" not in extract(SAMPLE, version)


def test_an_unknown_version_is_an_error_and_names_what_it_found():
    with pytest.raises(NotFound) as excinfo:
        extract(SAMPLE, "3.0.0")
    message = str(excinfo.value)
    assert "3.0.0" in message
    # The list of known versions is what makes the CI failure actionable.
    assert "2.0.0" in message and "1.8.0" in message


def test_a_heading_with_an_empty_body_is_an_error_not_an_empty_page():
    empty = "# Changelog\n\n## [1.0.0] - 2026-01-01\n\n## [0.9.0] - 2025-12-01\n\n- x\n"
    with pytest.raises(NotFound, match="nothing under it"):
        extract(empty, "1.0.0")


def test_a_heading_with_no_date_still_extracts():
    text = "# Changelog\n\n## [1.0.0]\n\n- Released without a date.\n"
    assert extract(text, "1.0.0") == "- Released without a date."


def test_the_cli_reports_the_failure_and_exits_nonzero(tmp_path, capsys):
    changelog = tmp_path / "CHANGELOG.md"
    changelog.write_text(SAMPLE, encoding="utf-8")
    code = extract_changelog.main(["9.9.9", "--changelog", str(changelog)])
    assert code == 1
    assert "::error::" in capsys.readouterr().err


def test_the_cli_writes_the_notes_to_a_file(tmp_path):
    changelog = tmp_path / "CHANGELOG.md"
    changelog.write_text(SAMPLE, encoding="utf-8")
    out = tmp_path / "notes.md"
    code = extract_changelog.main(
        ["v1.9.0", "--changelog", str(changelog), "-o", str(out)]
    )
    assert code == 0
    assert out.read_text(encoding="utf-8") == "### Fixed\n\n- The old thing.\n"


def test_the_real_changelog_has_notes_for_the_declared_version():
    # The guard that would have caught 1.3.0 and 1.4.0 shipping without a page.
    import tomllib

    version = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["version"]
    notes = extract((ROOT / "CHANGELOG.md").read_text(encoding="utf-8"), version)
    assert notes.strip()
