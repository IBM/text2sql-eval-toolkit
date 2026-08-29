#!/usr/bin/env python
#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Enforce per-module coverage floors.

A single project-wide percentage is a weak guard: this repo's number is carried
by the modules that were written with tests, and it would happily stay flat
while the ones that decide published metrics lost coverage.  So the floor that
matters is per module, and only for modules where being wrong is expensive --
the metric computation, the artifact index, and the authorization layer.

Floors sit a few points under the level already reached, so this ratchets --
raising coverage raises the floor in the same commit -- while leaving room for
the small differences branch coverage shows across the Python matrix.  A drop
of more than a rounding error fails.
Modules with no entry are not checked at all; the project-wide ``fail_under`` in
``pyproject.toml`` covers those.

Usage::

    pytest --cov --cov-report=json
    python scripts/ci/check_coverage.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

#: Module path (relative to ``src/text2sql_eval_toolkit``) -> minimum percent.
#:
#: Every entry needs a reason to be here.  Grouped by what a regression would
#: cost, because that is the only thing that justifies the maintenance.
FLOORS: dict[str, int] = {
    # --- The numbers the toolkit publishes -------------------------------
    # A bug here produces a plausible wrong score that gets committed, cited,
    # and uploaded to the Hub.
    "evaluation/evaluation_tools.py": 55,
    "metrics/text2sql_utils.py": 46,
    "analysis/report_tools.py": 50,
    "analysis/error_analysis.py": 65,
    "evaluation/llm_as_judge.py": 90,
    # --- What a pip user imports -----------------------------------------
    # These are a published contract: other people call them from their own
    # code, and 1.4.0's requirement is that dashboard work does not change what
    # they see.  tests/test_public_api_signatures.py locks the signatures;
    # these floors keep the behaviour behind them exercised.
    "utils.py": 80,
    # --- Model output and SQL execution ----------------------------------
    # Deliberately low, and honest about it.  Most of these modules need an LLM
    # endpoint or a live database, so what is covered is the endpoint-free part:
    # parsing what a model returned, quoting identifiers, running SQLite.  That
    # is also where a silent regression would be worst, since it decides what
    # counts as the SQL a model produced.  The floors ratchet what exists rather
    # than claim it is enough; raising them means stubbing transports, which is
    # its own piece of work.
    "execution/execution_tools.py": 11,
    "inference/inference_tools.py": 10,
    "inference/baseline_llm_pipeline.py": 6,
    "inference/agentic_pipeline.py": 4,
    # --- The artifact index ------------------------------------------------
    # Every dashboard read goes through it.  An index that disagrees with its
    # source is worse than a slow dashboard: it is a confident wrong answer.
    "indexing/scanner.py": 95,
    "indexing/builder.py": 80,
    "indexing/store.py": 80,
    # --- Authorization -----------------------------------------------------
    # What stands between a public deployment and arbitrary SQL execution.
    "ui/capabilities.py": 90,
    "ui/middleware.py": 90,
    "ui/auth.py": 85,
    "ui/judge_budget.py": 85,
    "ui/aliases.py": 95,
    # Called at module scope across the package, so a failure here is an
    # import failure -- which is exactly how it reached a container.
    "logging.py": 90,
    # --- Data access -------------------------------------------------------
    # What a browser is asked to render. A regression here is a frozen tab.
    "ui/dataframes.py": 90,
    "ui/paths.py": 80,
    "ui/indexes.py": 80,
    "ui/routers_errors.py": 85,
    "ui/routers_judge.py": 80,
}

PACKAGE = "src/text2sql_eval_toolkit"


def load_report(path: Path) -> dict:
    if not path.exists():
        sys.exit(
            f"{path} not found. Run:  pytest --cov --cov-report=json  before this."
        )
    return json.loads(path.read_text(encoding="utf-8"))


def percent(report: dict, module: str) -> float | None:
    """
    Coverage for one module, or None if the report does not mention it.

    Keys are whatever path coverage recorded, which varies with the working
    directory, so match on the suffix rather than assuming a prefix.
    """
    wanted = f"{PACKAGE}/{module}"
    for key, entry in report.get("files", {}).items():
        if Path(key).as_posix().endswith(wanted):
            return entry["summary"]["percent_covered"]
    return None


def main() -> int:
    report = load_report(Path("coverage.json"))

    failures: list[str] = []
    missing: list[str] = []
    slack: list[str] = []

    for module, floor in sorted(FLOORS.items()):
        actual = percent(report, module)
        if actual is None:
            missing.append(module)
            continue
        if actual < floor:
            failures.append(f"  {module}: {actual:.0f}% < {floor}% floor")
        elif actual - floor >= 10:
            slack.append(f"  {module}: {actual:.0f}% (floor {floor}%)")

    if missing:
        # A module in the table that the report has never heard of is a typo or
        # a rename, and it silently enforces nothing -- which is the failure
        # mode this whole file exists to prevent.
        print("Coverage floors name modules that are not in the report:")
        print("\n".join(f"  {m}" for m in missing))
        return 1

    if failures:
        print("Coverage fell below the floor for these modules:")
        print("\n".join(failures))
        print("\nAdd tests, or lower the floor deliberately in this file.")
        return 1

    if slack:
        # Not a failure: a ratchet nobody tightens stops ratcheting, so say so.
        print("Coverage is well above the floor here -- consider raising it:")
        print("\n".join(slack))

    print(f"All {len(FLOORS)} coverage floors met.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
