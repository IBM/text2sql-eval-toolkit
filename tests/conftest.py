#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Shared pytest configuration.

The default suite is hermetic: no network, no credentials, no databases, so it
can gate CI.  Tests needing live services are marked ``integration`` and are
excluded by ``addopts`` in pyproject.toml.  Run them explicitly with::

    pytest -m integration

See docs/plan/04-code-quality.md item 4.3.
"""

import os

import pytest


def pytest_collection_modifyitems(config, items):
    """
    Belt-and-braces guard: anything under a path named ``*integration*`` is
    treated as an integration test even if the module forgot the marker, so a
    new file cannot silently start requiring credentials in the default run.
    """
    for item in items:
        if "integration" in str(item.fspath) and not any(
            m.name == "integration" for m in item.iter_markers()
        ):
            item.add_marker(pytest.mark.integration)


@pytest.fixture
def require_env():
    """Skip a test unless every named environment variable is set."""

    def _require(*names: str) -> None:
        missing = [n for n in names if not os.environ.get(n)]
        if missing:
            pytest.skip(f"missing required environment: {', '.join(missing)}")

    return _require
