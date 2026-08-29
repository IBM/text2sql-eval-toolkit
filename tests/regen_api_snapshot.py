#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
"""
Regenerate the public API snapshot.

    python -m tests.regen_api_snapshot

Only run this for a deliberate, additive change. See the module docstring of
``tests/test_public_api_signatures.py`` for when it is the wrong answer.
"""

import json

from tests.test_public_api_signatures import SNAPSHOT_PATH, _describe

import text2sql_eval_toolkit as toolkit


def main() -> None:
    snapshot = {name: _describe(name) for name in sorted(toolkit.__all__)}
    SNAPSHOT_PATH.write_text(
        json.dumps(snapshot, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(f"Wrote {len(snapshot)} symbols to {SNAPSHOT_PATH}")


if __name__ == "__main__":
    main()
