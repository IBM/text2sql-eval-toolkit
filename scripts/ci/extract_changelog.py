#!/usr/bin/env python
#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Extract one version's section from ``CHANGELOG.md``, for the GitHub Release.

Until 1.5.0 the Release page was written by hand after the tag was pushed, and
it was forgotten on both 1.3.0 and 1.4.0 -- it is the kind of step a human
misses because nothing fails when they do.  ``release.yml`` now creates it from
the changelog, and this is the part worth having outside the YAML because it is
the part worth testing.

The contract is deliberately strict in one direction: **a version with no
changelog section is an error**, not an empty page.  Publishing a release whose
notes are blank is worse than publishing none, because it looks deliberate.

Usage::

    python scripts/ci/extract_changelog.py 1.5.0
    python scripts/ci/extract_changelog.py v1.5.0 -o notes.md

The argument may carry the ``v`` prefix, so a workflow can pass the tag name
straight through without stripping it.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

#: ``## [1.5.0] - 2026-08-30``.  The date is optional: an unreleased section
#: under a bare heading should still extract rather than silently miss.
HEADING = re.compile(r"^## \[(?P<version>\d+\.\d+\.\d+)\](?:\s*-\s*(?P<date>\S+))?\s*$")

#: ``[1.5.0]: https://github.com/...``.  These sit at the foot of the file and
#: belong to the document, not to any one release's notes.
LINK_DEFINITION = re.compile(r"^\[[^\]]+\]:\s*\S+\s*$")


class NotFound(LookupError):
    """No section in the changelog matches the requested version."""


def extract(text: str, version: str) -> str:
    """
    Return the body of ``text``'s section for ``version``.

    Args:
        text: The full contents of ``CHANGELOG.md``.
        version: A version like ``1.5.0``; a leading ``v`` is accepted and
            stripped, so a tag name works.

    Returns:
        The section body with surrounding blank lines and any trailing
        link-reference definitions removed.  The ``## [x.y.z]`` heading itself
        is not included -- GitHub shows the tag as the release's title, so
        repeating it in the body reads as a duplicate.

    Raises:
        NotFound: There is no heading for that version, or the section under it
            is empty.
    """
    wanted = version.lstrip("v")

    lines = text.splitlines()
    start: int | None = None
    end = len(lines)

    for index, line in enumerate(lines):
        match = HEADING.match(line)
        if match is None:
            continue
        if start is not None:
            # The next release's heading closes the one we are reading.
            end = index
            break
        if match.group("version") == wanted:
            start = index + 1

    if start is None:
        known = [m.group("version") for m in map(HEADING.match, lines) if m]
        raise NotFound(
            f"CHANGELOG.md has no section for {wanted}. "
            f"It documents: {', '.join(known) or '(nothing)'}."
        )

    body = [ln for ln in lines[start:end] if not LINK_DEFINITION.match(ln)]
    notes = "\n".join(body).strip()

    if not notes:
        raise NotFound(
            f"CHANGELOG.md has a heading for {wanted} but nothing under it. "
            f"A release page with no notes is worse than none at all."
        )
    return notes


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("version", help="version or tag, e.g. 1.5.0 or v1.5.0")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="write the notes here instead of stdout",
    )
    parser.add_argument(
        "--changelog",
        type=Path,
        default=ROOT / "CHANGELOG.md",
        help="path to the changelog (default: the repository's)",
    )
    args = parser.parse_args(argv)

    try:
        notes = extract(args.changelog.read_text(encoding="utf-8"), args.version)
    except NotFound as exc:
        print(f"::error::{exc}", file=sys.stderr)
        return 1

    if args.output:
        args.output.write_text(notes + "\n", encoding="utf-8")
        print(f"wrote {len(notes.splitlines())} lines to {args.output}")
    else:
        print(notes)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
