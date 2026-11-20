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
TLS at all. And the driver treats TLS as a preference: when the server's
handshake does not offer it, aiomysql logs in unencrypted, so whoever can
rewrite that handshake gets the credentials.
"""

import asyncio
import ssl
import struct
from types import SimpleNamespace

import certifi
import pytest

from text2sql_eval_toolkit.execution import execution_tools
from text2sql_eval_toolkit.execution.execution_tools import (
    _connect_requiring_tls,
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

    def test_tls_connections_go_through_the_tls_only_connector(self):
        _, args = normalize_mysql_connection_string(
            "mysql://u:p@db.example.com:3306/base?sslMode=REQUIRED"
        )
        assert args["async_creator_fn"] is _connect_requiring_tls

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


# A MySQL server's first packet: protocol 10, with or without CLIENT_SSL among
# its capabilities. Everything else is the minimum aiomysql parses.
def _handshake(offer_tls):
    caps = 0x1 | 0x8 | 0x200 | 0x8000 | 0x80000 | (0x800 if offer_tls else 0)
    payload = (
        b"\x0a8.0.0-fake\x00"
        + struct.pack("<I", 1)
        + b"abcdefgh\x00"
        + struct.pack("<HBHH", caps & 0xFFFF, 45, 2, caps >> 16)
        + bytes([21])
        + b"\x00" * 10
        + b"ijklmnopqrst\x00mysql_native_password\x00"
    )
    return struct.pack("<I", len(payload))[:3] + b"\x00" + payload


async def _login_attempt(offer_tls):
    """Connect with TLS requested; return the error and what the server got."""
    received, done = [], asyncio.Event()

    async def handle(reader, writer):
        writer.write(_handshake(offer_tls))
        await writer.drain()
        try:
            received.append(await asyncio.wait_for(reader.read(4096), 2))
        finally:
            writer.close()
            done.set()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    url, connect_args = normalize_mysql_connection_string(
        f"mysql://reader:s3cret@127.0.0.1:{port}/db?sslMode=VERIFY_IDENTITY"
    )
    async with server:
        with pytest.raises(Exception) as caught:
            await run_sql_and_get_dataframe_mysql_async(
                url, connect_args, None, "SELECT 1", timeout=5
            )
        await asyncio.wait_for(done.wait(), 5)
    return caught.value, received[0]


class TestTlsCannotBeStripped:
    """Through SQLAlchemy and aiomysql, against a fake server on localhost."""

    def test_a_server_that_does_not_offer_tls_never_gets_the_login(self):
        pytest.importorskip("aiomysql")
        error, received = asyncio.run(_login_attempt(offer_tls=False))
        assert "did not offer it, so the login was not sent" in str(error)
        assert received == b""

    def test_a_server_that_offers_tls_is_asked_to_switch_to_it(self):
        """The first reply is the 32-byte SSL request, not the login."""
        pytest.importorskip("aiomysql")
        _, received = asyncio.run(_login_attempt(offer_tls=True))
        assert int.from_bytes(received[:3], "little") == 32
        client_flags = struct.unpack("<I", received[4:8])[0]
        assert client_flags & 0x800
        assert b"reader" not in received


def test_the_guard_refuses_to_run_without_the_hook_it_overrides(monkeypatch):
    """A driver release that renamed the hook must fail loudly, not fall back."""
    aiomysql_connection = pytest.importorskip("aiomysql.connection")
    monkeypatch.setattr(execution_tools, "_tls_only_aiomysql_connection", None)
    monkeypatch.delattr(aiomysql_connection.Connection, "_request_authentication")
    with pytest.raises(RuntimeError, match="cannot refuse a server"):
        asyncio.run(_connect_requiring_tls(host="127.0.0.1", port=1))
