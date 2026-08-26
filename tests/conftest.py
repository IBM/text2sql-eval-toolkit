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

See docs/attic/plan/04-code-quality.md item 4.3.
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


#: Environment that changes what the code does, and that a developer's `.env`
#: may well supply. `env_loader.load_env()` runs on import of the toolkit, so
#: importing `ui.server` in a test pulls the developer's real credentials into
#: the process -- which is how a suite documented as hermetic came to depend on
#: whether Google credentials happened to be configured locally. It passed in CI
#: (no .env there) and started failing the moment sign-in was set up for a
#: manual test.
CREDENTIAL_ENV = (
    # Deliberately not TEXT2SQL_DATA_ROOT: tests set that themselves, including
    # from module-scoped fixtures that run before this one, so clearing it per
    # test would fight them rather than isolate anything.
    "GOOGLE_CLIENT_ID",
    "GOOGLE_CLIENT_SECRET",
    "TEXT2SQL_SESSION_SECRET",
    "TEXT2SQL_JUDGE_ALLOWLIST",
    "TEXT2SQL_COOKIE_SECURE",
    "TEXT2SQL_DASHBOARD_MODE",
    "TEXT2SQL_FORWARDED_ALLOW_IPS",
    "TEXT2SQL_LOG_FILE",
    "WATSONX_APIKEY",
    "WATSONX_API_BASE",
    "WATSONX_PROJECTID",
    "OPENAI_API_KEY",
    "ANTHROPIC_API_KEY",
    "GEMINI_API_KEY",
    "HF_TOKEN",
    "POSTGRES_CONNECTION_STRING",
    "MYSQL_CONNECTION_STRING",
    "DB2_CONNECTION_STRING",
    "PRESTO_CONNECTION_STRING",
)


@pytest.fixture(autouse=True)
def hermetic_environment(request, monkeypatch):
    """
    Strip credentials from the environment for every non-integration test.

    Without this the default suite is only hermetic on a machine with no `.env`,
    which is to say: on CI, and nowhere a change is actually developed. A test
    that behaves differently depending on whether the developer has configured
    Google is not testing what it claims to.

    Integration tests are exempt -- they exist to use these.
    """
    if request.node.get_closest_marker("integration"):
        return
    for name in CREDENTIAL_ENV:
        monkeypatch.delenv(name, raising=False)
