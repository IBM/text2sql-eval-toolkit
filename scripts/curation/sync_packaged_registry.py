#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Copy the benchmark registries into the package so wheels ship the same data.

There are two copies of each registry:

* ``data/benchmarks.json`` -- edited by hand, and the one a source checkout
  actually reads, because ``get_benchmarks_file_path()`` prefers
  ``$TEXT2SQL_DATA_ROOT`` and then ``./data`` before the packaged copy.
* ``src/text2sql_eval_toolkit/data/benchmarks.json`` -- shipped in the wheel,
  and the only one a pip-installed user has.

Because the checkout copy shadows the packaged one, drift is invisible during
development and only reaches installed users. That is exactly what happened: the
packaged copy fell behind and lost every benchmark's ``logo``.

The checkout copy is canonical. Run this after editing it:

    python scripts/curation/sync_packaged_registry.py

``tests/test_registry_sync.py`` fails if they diverge, so this cannot be
forgotten silently.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List, Tuple

REGISTRY_FILENAMES = ("benchmarks.json", "test-benchmarks.json")


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def registry_pairs() -> List[Tuple[Path, Path]]:
    """(canonical, packaged) for each registry file."""
    root = repo_root()
    source_dir = root / "data"
    packaged_dir = root / "src" / "text2sql_eval_toolkit" / "data"
    return [
        (source_dir / name, packaged_dir / name)
        for name in REGISTRY_FILENAMES
        if (source_dir / name).is_file()
    ]


def normalise(path: Path) -> str:
    """
    Canonical text for a registry file.

    Compared and written through JSON rather than byte-for-byte, so indentation
    or key order alone never counts as drift.
    """
    data = json.loads(path.read_text(encoding="utf-8"))
    return json.dumps(data, indent=4, ensure_ascii=False, sort_keys=True) + "\n"


def diverged() -> List[Tuple[Path, Path]]:
    """Pairs whose contents differ."""
    out = []
    for canonical, packaged in registry_pairs():
        if not packaged.is_file() or normalise(packaged) != normalise(canonical):
            out.append((canonical, packaged))
    return out


def sync() -> List[Path]:
    written = []
    for canonical, packaged in registry_pairs():
        packaged.parent.mkdir(parents=True, exist_ok=True)
        text = normalise(canonical)
        if not packaged.is_file() or packaged.read_text(encoding="utf-8") != text:
            packaged.write_text(text, encoding="utf-8")
            written.append(packaged)
    return written


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if the copies differ, without writing anything.",
    )
    args = parser.parse_args(argv)

    if args.check:
        stale = diverged()
        if stale:
            for canonical, packaged in stale:
                print(
                    f"out of sync: {packaged} differs from {canonical}", file=sys.stderr
                )
            print(
                "\nRun: python scripts/curation/sync_packaged_registry.py",
                file=sys.stderr,
            )
            return 1
        print("packaged registries are in sync")
        return 0

    written = sync()
    if written:
        for path in written:
            print(f"updated {path}")
    else:
        print("already in sync")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
