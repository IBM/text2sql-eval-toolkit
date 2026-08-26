#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Behind a TLS-terminating proxy the app must know the request arrived over
https, and must know who sent it.

Both come from ``X-Forwarded-*``, and uvicorn believes those headers only from
peers in its trust list -- which defaults to ``127.0.0.1``. A proxy running in
another container is not 127.0.0.1, so with the default the app sees:

* ``scheme == "http"``, so ``url_for("auth_callback")`` builds an http
  ``redirect_uri`` and Google refuses it with ``redirect_uri_mismatch`` --
  sign-in cannot work at all; and
* the proxy's address as the client, so every visitor on the internet shares one
  rate-limit bucket.

Neither is visible from a loopback run, which is why this is tested rather than
observed.
"""

import asyncio

import pytest

from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

PROXY_IN_ANOTHER_CONTAINER = "172.18.0.3"
REAL_CLIENT = "203.0.113.9"

#: What deploy/docker-compose.yml sets for the app service.
COMPOSE_TRUST = "10.0.0.0/8,172.16.0.0/12,192.168.0.0/16"


def observed(trusted_hosts, peer_ip):
    """The scope an app would see for a proxied https request."""
    seen = {}

    async def app(scope, receive, send):
        seen["scheme"] = scope["scheme"]
        seen["client"] = scope["client"][0] if scope["client"] else None

    scope = {
        "type": "http",
        "scheme": "http",
        "client": (peer_ip, 5000),
        "headers": [
            (b"x-forwarded-proto", b"https"),
            (b"x-forwarded-for", REAL_CLIENT.encode()),
        ],
    }
    asyncio.run(
        ProxyHeadersMiddleware(app, trusted_hosts=trusted_hosts)(scope, None, None)
    )
    return seen


def test_the_uvicorn_default_would_break_sign_in():
    """
    Not a test of our code -- a test of the assumption underneath the setting.
    If uvicorn ever starts trusting more by default, this fails and the extra
    configuration can go.
    """
    seen = observed("127.0.0.1", PROXY_IN_ANOTHER_CONTAINER)
    assert seen["scheme"] == "http", "if this changes, revisit the compose setting"
    assert seen["client"] == PROXY_IN_ANOTHER_CONTAINER


def test_the_compose_setting_restores_the_scheme():
    seen = observed(COMPOSE_TRUST, PROXY_IN_ANOTHER_CONTAINER)
    assert seen["scheme"] == "https", "an http redirect_uri is rejected by Google"


def test_the_compose_setting_restores_the_real_client():
    seen = observed(COMPOSE_TRUST, PROXY_IN_ANOTHER_CONTAINER)
    assert seen["client"] == REAL_CLIENT, "otherwise all visitors share one bucket"


@pytest.mark.parametrize("peer", ["10.1.2.3", "172.18.0.3", "192.168.5.5"])
def test_every_private_range_a_compose_network_might_use_is_covered(peer):
    assert observed(COMPOSE_TRUST, peer)["scheme"] == "https"


def test_loopback_is_deliberately_not_in_the_compose_list():
    """
    The only thing reaching the app from 127.0.0.1 inside the container is its
    own healthcheck, which sends no forwarded headers -- so excluding loopback
    costs nothing and keeps the trust bound as tight as the topology allows.
    """
    assert "127.0.0.1" not in COMPOSE_TRUST
    assert observed(COMPOSE_TRUST, "127.0.0.1")["scheme"] == "http"


def test_a_public_peer_is_not_trusted_under_the_compose_setting():
    """
    The service publishes no ports, so this should be unreachable anyway -- but
    the trust list is a bound, not a formality, and '*' would not have one.
    """
    seen = observed(COMPOSE_TRUST, "198.51.100.7")
    assert seen["scheme"] == "http"
    assert seen["client"] == "198.51.100.7"


# --- the wiring ------------------------------------------------------------


def test_the_dashboard_exposes_the_setting():
    """A flag nobody can set is not a fix."""
    import argparse
    import inspect

    from text2sql_eval_toolkit.ui import server

    source = inspect.getsource(server.main)
    assert "--forwarded-allow-ips" in source
    assert "forwarded_allow_ips=args.forwarded_allow_ips" in source
    assert isinstance(argparse.ArgumentParser(), argparse.ArgumentParser)


def test_the_default_leaves_a_local_run_alone():
    """Loopback development must not start trusting forwarded headers."""
    import inspect

    from text2sql_eval_toolkit.ui import server

    source = inspect.getsource(server.main)
    assert '"TEXT2SQL_FORWARDED_ALLOW_IPS", "127.0.0.1"' in source


def test_compose_configures_the_app_to_trust_its_proxy():
    """
    The setting only helps if the deployment actually sets it, and the
    deployment is a file in this repository.
    """
    from pathlib import Path

    compose = Path(__file__).resolve().parents[1] / "deploy" / "docker-compose.yml"
    text = compose.read_text(encoding="utf-8")
    assert "TEXT2SQL_FORWARDED_ALLOW_IPS" in text
    assert "172.16.0.0/12" in text, "docker's default bridge range must be covered"
