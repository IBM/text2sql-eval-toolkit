#!/usr/bin/env python
#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Check that the version agrees with itself everywhere it appears.

This exists because it already went wrong once: ``pyproject.toml`` and
``__init__.py`` said 1.1.0 while ``CHANGELOG.md`` documented a 1.2.0 release
whose features were present in the code, and nothing noticed for months.  The
number now lives in ``pyproject.toml`` alone and is read back from the installed
metadata, so this checks the two places it cannot single-source: the changelog,
and the git tag.

Usage::

    python scripts/ci/check_version.py
"""

from __future__ import annotations

import os
import re
import subprocess
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CHANGELOG_HEADING = re.compile(r"^## \[(\d+\.\d+\.\d+)\]", re.MULTILINE)


def declared_version() -> str:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    return data["project"]["version"]


def installed_version() -> str | None:
    """What ``import text2sql_eval_toolkit`` reports, if it is installed."""
    try:
        from text2sql_eval_toolkit._version import UNKNOWN_VERSION, __version__
    except ImportError:
        return None
    return None if __version__ == UNKNOWN_VERSION else __version__


def latest_changelog_version() -> str | None:
    text = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    match = CHANGELOG_HEADING.search(text)
    return match.group(1) if match else None


def tag_on_head() -> str | None:
    """
    The release tag being built, if there is one.

    In CI a tag build sets GITHUB_REF_TYPE; locally, fall back to asking git
    whether a tag points at HEAD. Neither is an error -- most builds are not
    releases.
    """
    if os.environ.get("GITHUB_REF_TYPE") == "tag":
        return os.environ.get("GITHUB_REF_NAME")
    try:
        result = subprocess.run(
            ["git", "tag", "--points-at", "HEAD"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:  # pragma: no cover - git absent
        return None
    tags = [t for t in result.stdout.split() if t.startswith("v")]
    return tags[0] if tags else None


def main() -> int:
    version = declared_version()
    problems: list[str] = []

    installed = installed_version()
    if installed is None:
        print("note: the package is not installed; skipping the metadata check.")
    elif installed != version:
        problems.append(
            f"  pyproject says {version}, but the installed package reports "
            f"{installed}. Reinstall (`uv pip install -e .`) -- the version is "
            f"read from distribution metadata, so a stale install lies about it."
        )

    changelog = latest_changelog_version()
    if changelog is None:
        problems.append("  CHANGELOG.md has no `## [x.y.z]` heading.")
    elif changelog != version:
        problems.append(
            f"  pyproject says {version}, but the newest CHANGELOG entry is "
            f"{changelog}. A release with no changelog entry is a release "
            f"nobody can read."
        )

    tag = tag_on_head()
    if tag is not None and tag != f"v{version}":
        problems.append(
            f"  the tag on this commit is {tag}, but the version is {version}."
        )

    if problems:
        print("Version disagreement:")
        print("\n".join(problems))
        return 1

    tag_note = f", tagged {tag}" if tag else ""
    print(
        f"Version {version} agrees across pyproject, package and changelog{tag_note}."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
