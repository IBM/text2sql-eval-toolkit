#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
"""
Connection strings carry the database password, so they are logged redacted.

The toolkit's loggers default to DEBUG and, in a source checkout, also write to
a file. MySQL execution logged its connection string as is, which put the
password on the console of every run and into the log file.
"""

import asyncio
import json
import logging
from types import SimpleNamespace

import pytest

from text2sql_eval_toolkit.execution import execution_tools
from text2sql_eval_toolkit.execution.execution_tools import (
    mysql_run_execution_async,
    redact_url_password,
)


@pytest.mark.parametrize(
    "given, expected",
    [
        ("mysql://u:secret@h:3306/db", "mysql://u:***@h:3306/db"),
        (
            "mysql+aiomysql://u:p%40ss@h/db?charset=utf8mb4",
            "mysql+aiomysql://u:***@h/db?charset=utf8mb4",
        ),
        # Unencoded `@` and `:` in the password: the host follows the last `@`.
        ("postgresql://u:p@ss:w@h:5432/db", "postgresql://u:***@h:5432/db"),
        ("mysql://u:@h/db", "mysql://u:***@h/db"),
        ("mysql://u@h/db", "mysql://u@h/db"),
        ("sqlite:///path/to/x.db", "sqlite:///path/to/x.db"),
    ],
)
def test_the_password_is_redacted(given, expected):
    assert redact_url_password(given) == expected


class _Stop(Exception):
    pass


def test_mysql_execution_never_logs_the_password(monkeypatch, tmp_path):
    class FakeEngine:
        def begin(self):
            raise _Stop

        async def dispose(self):
            pass

    monkeypatch.setattr(
        execution_tools,
        "_require_mysql_deps",
        lambda: (
            SimpleNamespace(create_async_engine=lambda *a, **k: FakeEngine()),
            None,
        ),
    )
    predictions = tmp_path / "predictions.json"
    predictions.write_text(json.dumps([]))

    messages = []
    handler = logging.Handler(level=logging.DEBUG)
    handler.emit = lambda record: messages.append(record.getMessage())
    logger = execution_tools.logger
    monkeypatch.setattr(logger, "level", logging.DEBUG)
    logger.addHandler(handler)
    try:
        with pytest.raises(RuntimeError, match="Failed to connect"):
            asyncio.run(
                mysql_run_execution_async(
                    "mysql://reader:s3cret-pw@db.example.com:3306/base", predictions
                )
            )
    finally:
        logger.removeHandler(handler)

    assert any("reader:***@db.example.com" in m for m in messages)
    assert not any("s3cret-pw" in m for m in messages)
