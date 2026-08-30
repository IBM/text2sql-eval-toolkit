#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
"""
Per-user provider credentials.

Storing other people's billable keys ends two properties this deployment used to
rely on: that there is no user database to secure, and that a public host holds
no credentials. The constraints that make that acceptable are asserted here,
because every one of them is invisible until it fails.
"""

import threading

import pytest

from text2sql_eval_toolkit.ui.judge_budget import JudgeStore
from text2sql_eval_toolkit.ui.user_keys import (
    SECRET_KEY_ENV,
    SecretsUnavailable,
    UserKeyStore,
    secrets_available,
)

USER = "user@example.com"
CANARY = "sk-canary-must-never-appear-anywhere"


@pytest.fixture
def master_key(monkeypatch):
    monkeypatch.setenv(SECRET_KEY_ENV, "k" * 48)


@pytest.fixture
def store(tmp_path, master_key):
    return UserKeyStore(tmp_path / "keys.sqlite")


class TestEncryptionAtRest:
    def test_the_key_is_not_on_disk_in_plaintext(self, store):
        """The database file alone must be worthless."""
        store.store(USER, "anthropic", CANARY)
        assert CANARY.encode() not in store._path.read_bytes()

    def test_it_round_trips_through_decryption(self, store):
        store.store(USER, "anthropic", CANARY)
        assert store.reveal_for_request(USER, "anthropic") == CANARY

    def test_a_changed_master_key_degrades_to_absent(self, store, monkeypatch):
        """
        Rotating TEXT2SQL_SECRET_KEY must mean "re-enter your key", not a broken
        server that 500s on every judge request.
        """
        store.store(USER, "anthropic", CANARY)
        monkeypatch.setenv(SECRET_KEY_ENV, "different" * 8)
        assert store.reveal_for_request(USER, "anthropic") is None

    def test_no_master_key_means_nothing_can_be_stored(self, tmp_path, monkeypatch):
        monkeypatch.delenv(SECRET_KEY_ENV, raising=False)
        assert secrets_available() is False
        store = UserKeyStore(tmp_path / "k.sqlite")
        with pytest.raises(SecretsUnavailable):
            store.store(USER, "anthropic", CANARY)

    def test_a_short_master_key_is_refused(self, tmp_path, monkeypatch):
        monkeypatch.setenv(SECRET_KEY_ENV, "tooshort")
        with pytest.raises(SecretsUnavailable):
            UserKeyStore(tmp_path / "k.sqlite").store(USER, "anthropic", CANARY)


class TestWriteOnly:
    def test_describe_returns_no_key_material(self, store):
        store.store(USER, "anthropic", CANARY, "work")
        described = store.describe(USER)
        blob = repr(described)
        assert CANARY not in blob
        # Not even a fragment: a masked tail is still key material.
        assert CANARY[-4:] not in blob
        assert described[0]["provider"] == "anthropic"
        assert described[0]["label"] == "work"

    def test_the_api_has_no_route_that_could_return_one(self):
        """
        Enforced structurally rather than by remembering: if no handler reads a
        key, none can leak one.
        """
        from pathlib import Path

        source = Path("src/text2sql_eval_toolkit/ui/routers_keys.py").read_text()
        assert "reveal_for_request" not in source

    def test_describe_is_scoped_to_one_user(self, store):
        store.store(USER, "anthropic", CANARY)
        store.store("other@example.com", "openai", "sk-other")
        assert [row["provider"] for row in store.describe(USER)] == ["anthropic"]


class TestNeverLogged:
    def test_the_key_does_not_reach_the_log(self, store, caplog):
        """The canary the plan asked for."""
        import logging

        with caplog.at_level(logging.DEBUG):
            store.store(USER, "anthropic", CANARY)
            store.reveal_for_request(USER, "anthropic")
        assert CANARY not in caplog.text

    def test_the_address_does_not_reach_the_log_either(self, store, caplog):
        import logging

        with caplog.at_level(logging.DEBUG):
            store.store(USER, "anthropic", CANARY)
        assert USER not in caplog.text


class TestLifecycle:
    def test_a_key_survives_reopening_the_store(self, tmp_path, master_key):
        path = tmp_path / "k.sqlite"
        UserKeyStore(path).store(USER, "anthropic", CANARY)
        assert UserKeyStore(path).reveal_for_request(USER, "anthropic") == CANARY

    def test_storing_again_replaces_rather_than_duplicates(self, store):
        store.store(USER, "anthropic", "first")
        store.store(USER, "anthropic", "second")
        assert store.reveal_for_request(USER, "anthropic") == "second"
        assert len(store.describe(USER)) == 1

    def test_deleting_removes_it(self, store):
        store.store(USER, "anthropic", CANARY)
        assert store.delete(USER, "anthropic") is True
        assert store.reveal_for_request(USER, "anthropic") is None

    def test_deleting_something_absent_reports_it(self, store):
        assert store.delete(USER, "openai") is False

    def test_using_a_key_records_when(self, store):
        store.store(USER, "anthropic", CANARY)
        assert store.describe(USER)[0]["last_used_at"] is None
        store.reveal_for_request(USER, "anthropic")
        assert store.describe(USER)[0]["last_used_at"] is not None


class TestProviderValidation:
    def test_an_unknown_provider_is_refused(self, store):
        with pytest.raises(ValueError):
            store.store(USER, "nosuchprovider", CANARY)

    def test_an_empty_key_is_refused(self, store):
        with pytest.raises(ValueError):
            store.store(USER, "anthropic", "   ")

    def test_there_is_no_base_url_field(self):
        """
        A user-supplied endpoint would turn the server into an open outbound
        proxy: the caller picks the host, the server makes the request from
        inside the network. The schema must not offer one.
        """
        from pathlib import Path

        source = Path("src/text2sql_eval_toolkit/ui/user_keys.py").read_text()
        assert "base_url" not in source.split('"""', 2)[2]


class TestSpendCapsUnderConcurrency:
    """
    Evaluation runs sixteen coroutines against one semaphore. Check-then-spend
    lets all sixteen read a cap that is still under budget and then spend.
    """

    @pytest.fixture
    def ledger(self, tmp_path):
        return JudgeStore(tmp_path / "usage.sqlite")

    def test_no_cap_means_no_refusal(self, ledger):
        assert ledger.reserve("alice", 10_000.0) is True

    def test_a_cap_bounds_concurrent_claims(self, ledger):
        ledger.set_user_cap("alice", 1.00)
        granted, lock = [], threading.Lock()

        def claim():
            ok = ledger.reserve("alice", 0.10)
            with lock:
                granted.append(ok)

        threads = [threading.Thread(target=claim) for _ in range(16)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert sum(granted) == 10, "reservations must not exceed the cap"

    def test_releasing_returns_headroom(self, ledger):
        ledger.set_user_cap("alice", 1.00)
        assert ledger.reserve("alice", 1.00) is True
        assert ledger.reserve("alice", 0.10) is False
        ledger.release("alice", 1.00)
        assert ledger.reserve("alice", 0.10) is True

    def test_a_cap_can_be_removed(self, ledger):
        ledger.set_user_cap("alice", 0.01)
        assert ledger.reserve("alice", 1.00) is False
        ledger.set_user_cap("alice", None)
        assert ledger.reserve("alice", 1.00) is True

    def test_caps_are_per_user(self, ledger):
        ledger.set_user_cap("alice", 0.01)
        assert ledger.reserve("bob", 100.0) is True


class TestStoredKeysAreOptional:
    def test_the_judge_falls_back_to_the_server_credential(self):
        """
        A user with no stored key must still be able to run a judge, billed to
        the server -- storing a key is an option, not a precondition.
        """
        from text2sql_eval_toolkit.ui.routers_judge import _user_api_key

        assert _user_api_key(None, "wxai:model") is None
        assert _user_api_key(USER, "model-without-a-prefix") is None
