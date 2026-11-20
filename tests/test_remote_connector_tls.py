#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
"""
`RemoteDatabaseConnector` sends its API key as a bearer token, so it verifies
the server's certificate.

Both of its requests used to pass `verify=False`, which let anyone between the
toolkit and the API present any certificate and read `SQL_API_KEY`.
"""

from text2sql_eval_toolkit.metrics import text2sql_utils
from text2sql_eval_toolkit.metrics.text2sql_utils import (
    RemoteDatabaseConnector,
    execute_query_remote,
)


class _Response:
    status_code = 200

    def raise_for_status(self):
        pass

    def json(self):
        return {"schema": {"tables": []}, "result": []}


def test_every_request_verifies_the_certificate(monkeypatch, tmp_path):
    calls = []

    def record(url, **kwargs):
        calls.append(kwargs)
        return _Response()

    monkeypatch.setattr(text2sql_utils.requests, "get", record)
    monkeypatch.setattr(text2sql_utils.requests, "post", record)
    monkeypatch.setenv("SQL_API_KEY", "test-key")
    monkeypatch.setenv("UNITXT_CACHE_LOCATION", str(tmp_path))

    connector = RemoteDatabaseConnector({"db_id": "https://api.example.com,db_id=d1"})
    connector.get_table_schema()
    execute_query_remote(
        "https://api.example.com", "d1", "test-key", "SELECT 'tls-check'"
    )

    assert len(calls) == 2
    assert all(kwargs.get("verify", True) is True for kwargs in calls)
