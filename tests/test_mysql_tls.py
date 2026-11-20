#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
"""
TLS for MySQL connections: on when asked for, and always verifying.

The connection string used to switch certificate checks off whenever TLS was on
(`verify_mode = CERT_NONE`, `check_hostname = False`), so anyone between the
toolkit and the server could present any certificate and read the credentials.
It also stripped `sslMode` before testing for it, which left a
`sslMode=VERIFY_IDENTITY` string on any host but IBM Cloud connecting without
TLS at all.
"""

import asyncio
import ssl
from types import SimpleNamespace

import certifi
import pytest

from text2sql_eval_toolkit.execution import execution_tools
from text2sql_eval_toolkit.execution.execution_tools import (
    _is_certificate_failure,
    mysql_tls_from_url,
    normalize_mysql_connection_string,
    run_sql_and_get_dataframe_mysql_async,
)


def _assert_verifies(context):
    assert isinstance(context, ssl.SSLContext)
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname is True


class TestTlsIsOnWhenAskedFor:
    @pytest.mark.parametrize("mode", ["REQUIRED", "VERIFY_CA", "VERIFY_IDENTITY"])
    @pytest.mark.parametrize("key", ["sslMode", "ssl-mode", "ssl_mode", "SSLMODE"])
    def test_every_mode_that_asks_for_tls_verifies(self, key, mode):
        """REQUIRED and VERIFY_CA are read as VERIFY_IDENTITY, not loosened."""
        url, args = normalize_mysql_connection_string(
            f"mysql://u:p@db.example.com:3306/base?{key}={mode}"
        )
        _assert_verifies(args["ssl"])
        assert url == "mysql+aiomysql://u:p@db.example.com:3306/base"

    def test_ssl_true_verifies(self):
        _, args = normalize_mysql_connection_string(
            "mysql://u:p@db.example.com:3306/base?ssl=true"
        )
        _assert_verifies(args["ssl"])

    def test_ibm_cloud_hosts_get_tls_unasked(self):
        url = "mysql://u:p@abc.databases.appdomain.cloud:31618/base"
        normalized, args = normalize_mysql_connection_string(url)
        _assert_verifies(args["ssl"])
        assert normalized.endswith("abc.databases.appdomain.cloud:31618/base")

    def test_a_named_ca_is_loaded_and_still_verifies(self):
        url, args = normalize_mysql_connection_string(
            f"mysql://u:p@db.example.com:3306/base?ssl_ca={certifi.where()}"
        )
        _assert_verifies(args["ssl"])
        assert args["ssl"].get_ca_certs()
        assert "ssl_ca" not in url

    def test_a_missing_ca_file_is_an_error_not_a_skipped_check(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            normalize_mysql_connection_string(
                f"mysql://u:p@h:3306/base?ssl_ca={tmp_path / 'absent.pem'}"
            )


class TestTlsIsOffWhenNotAskedFor:
    def test_a_plain_url_is_left_alone(self):
        url = "mysql+aiomysql://u:p@127.0.0.1:3306/base?charset=utf8mb4"
        assert normalize_mysql_connection_string(url) == (url, {})

    @pytest.mark.parametrize("option", ["sslMode=DISABLED", "ssl=false"])
    def test_an_explicit_off_wins_even_on_ibm_cloud(self, option):
        url, args = normalize_mysql_connection_string(
            f"mysql://u:p@abc.databases.appdomain.cloud:31618/base?{option}"
        )
        assert args == {}
        assert "ssl" not in url.lower()

    def test_preferred_is_mysqls_default_and_changes_nothing(self):
        _, args = normalize_mysql_connection_string(
            "mysql://u:p@127.0.0.1:3306/base?sslMode=PREFERRED"
        )
        assert args == {}


class TestTheUrlKeepsEverythingElse:
    def test_other_parameters_survive_a_leading_ssl_mode(self):
        """The old regex left `&charset=...` with no `?` in front of it."""
        url, _ = normalize_mysql_connection_string(
            "mysql://u:p@h:3306/base?sslMode=REQUIRED&charset=utf8mb4"
        )
        assert url == "mysql+aiomysql://u:p@h:3306/base?charset=utf8mb4"

    def test_the_driver_does_not_matter(self):
        url, context = mysql_tls_from_url(
            "mysql+pymysql://u:p@h:3306/base?sslMode=VERIFY_IDENTITY"
        )
        assert url == "mysql+pymysql://u:p@h:3306/base"
        _assert_verifies(context)


class TestNoWayToSwitchTheCheckOff:
    @pytest.mark.parametrize(
        "option",
        [
            "ssl_check_hostname=false",
            "ssl_verify_cert=false",
            "ssl_verify_identity=false",
            "ssl_disabled=false",
        ],
    )
    def test_unsupported_tls_options_are_refused(self, option):
        with pytest.raises(ValueError, match="Unsupported MySQL TLS option"):
            normalize_mysql_connection_string(f"mysql://u:p@h:3306/base?{option}")

    def test_an_unknown_mode_is_refused(self):
        with pytest.raises(ValueError, match="not a MySQL SSL mode"):
            normalize_mysql_connection_string("mysql://u:p@h:3306/b?sslMode=NONE")

    def test_a_key_without_its_certificate_is_refused(self):
        with pytest.raises(ValueError, match="ssl_key needs ssl_cert"):
            normalize_mysql_connection_string("mysql://u:p@h:3306/b?ssl_key=k.pem")


class _Stop(Exception):
    pass


def test_the_per_record_database_keeps_the_tls_context(monkeypatch):
    """
    Each record names its database, so the URL is rewritten per query. It used
    to be normalized from scratch there, but the TLS options are no longer in
    it by then, so they have to come from the connect_args passed in.
    """
    seen = {}

    class FakeEngine:
        def begin(self):
            raise _Stop

        async def dispose(self):
            pass

    def create_async_engine(url, **kwargs):
        seen["url"] = url
        seen["connect_args"] = kwargs["connect_args"]
        return FakeEngine()

    monkeypatch.setattr(
        execution_tools,
        "_require_mysql_deps",
        lambda: (SimpleNamespace(create_async_engine=create_async_engine), None),
    )
    url, connect_args = normalize_mysql_connection_string(
        "mysql://u:p@db.example.com:3306/base?sslMode=VERIFY_IDENTITY"
    )
    with pytest.raises(_Stop):
        asyncio.run(
            run_sql_and_get_dataframe_mysql_async(url, connect_args, "dw", "SELECT 1")
        )
    assert seen["url"] == "mysql+aiomysql://u:p@db.example.com:3306/dw"
    assert seen["connect_args"]["ssl"] is connect_args["ssl"]


def test_a_certificate_failure_is_recognized_through_wrapping():
    """aiomysql and SQLAlchemy each wrap the SSL error, which hides its text."""
    try:
        try:
            raise ssl.SSLCertVerificationError("certificate verify failed")
        except ssl.SSLCertVerificationError as inner:
            raise OSError("Can't connect to MySQL server") from inner
    except OSError as outer:
        assert _is_certificate_failure(outer)
    assert not _is_certificate_failure(OSError("Connection refused"))
