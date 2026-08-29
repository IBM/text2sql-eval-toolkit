#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
"""
Roles in a database instead of an environment variable.

The invariants that had to survive the move are asserted here, because every one
of them is the sort of thing that looks fine until a deployment is public:

- mode is still a ceiling, and no stored row can raise a caller above it
- admin is a separate gate, so user management works on a judge-mode host
- TEXT2SQL_ADMIN_EMAILS always grants admin, so a bad table cannot lock everyone
  out -- it is the only recovery path left now the allowlist variable is gone
"""

import pytest

from text2sql_eval_toolkit.ui import runtime
from text2sql_eval_toolkit.ui.capabilities import Tier, requires_admin, resolve_tier
from text2sql_eval_toolkit.ui.roles import (
    ROLE_TIERS,
    Role,
    UserStore,
    admin_emails_from_env,
    effective_role,
)

ADMIN = "admin@example.com"
USER = "user@example.com"


@pytest.fixture
def store(tmp_path):
    return UserStore(tmp_path / "roles.sqlite")


class TestRoleStore:
    def test_a_granted_role_is_read_back(self, store):
        store.grant(USER, Role.JUDGE)
        assert store.role_for(USER) is Role.JUDGE

    def test_addresses_are_normalised_on_the_way_in_and_out(self, store):
        store.grant("  User@Example.COM ", Role.FULL)
        assert store.role_for("user@example.com") is Role.FULL

    def test_granting_again_replaces_rather_than_duplicates(self, store):
        store.grant(USER, Role.JUDGE)
        store.grant(USER, Role.FULL)
        assert store.role_for(USER) is Role.FULL
        assert len(store.list_users()) == 1

    def test_an_unknown_address_has_no_role(self, store):
        assert store.role_for("nobody@example.com") is None

    def test_revoking_removes_the_row(self, store):
        store.grant(USER, Role.JUDGE)
        assert store.revoke(USER) is True
        assert store.role_for(USER) is None

    def test_revoking_an_absent_row_reports_it(self, store):
        assert store.revoke("nobody@example.com") is False

    def test_an_unrecognised_stored_role_is_ignored_not_guessed(self, store):
        """A row from a newer version must not be resolved to something else."""
        with store._connect() as conn:
            conn.execute(
                "INSERT INTO roles (email, role) VALUES (?, ?)", (USER, "superuser")
            )
        assert store.role_for(USER) is None

    def test_a_blank_address_is_refused(self, store):
        with pytest.raises(ValueError):
            store.grant("   ", Role.JUDGE)

    def test_the_store_survives_being_reopened(self, tmp_path):
        path = tmp_path / "roles.sqlite"
        UserStore(path).grant(USER, Role.JUDGE)
        assert UserStore(path).role_for(USER) is Role.JUDGE


class TestEnvironmentAdmins:
    def test_addresses_are_parsed_and_normalised(self, monkeypatch):
        monkeypatch.setenv("TEXT2SQL_ADMIN_EMAILS", " A@b.com , C@d.com ")
        assert admin_emails_from_env() == {"a@b.com", "c@d.com"}

    def test_unset_means_nobody_not_everybody(self, monkeypatch):
        """The failure mode that would hand admin to the internet."""
        monkeypatch.delenv("TEXT2SQL_ADMIN_EMAILS", raising=False)
        assert admin_emails_from_env() == set()

    def test_the_environment_wins_over_the_table(self, store):
        """Otherwise a bad row could lock the operator out of their own host."""
        store.grant(ADMIN, Role.READ_ONLY)
        assert effective_role(ADMIN, store, {ADMIN}) is Role.ADMIN

    def test_it_grants_admin_with_no_row_at_all(self, store):
        assert effective_role(ADMIN, store, {ADMIN}) is Role.ADMIN

    def test_it_still_works_against_an_empty_table(self, tmp_path):
        assert (
            effective_role(ADMIN, UserStore(tmp_path / "r.sqlite"), {ADMIN})
            is Role.ADMIN
        )

    def test_it_works_with_no_store_configured(self):
        assert effective_role(ADMIN, None, {ADMIN}) is Role.ADMIN


class TestEffectiveRole:
    def test_anonymous_callers_are_read_only(self, store):
        assert effective_role(None, store, {ADMIN}) is Role.READ_ONLY

    def test_a_signed_in_stranger_is_read_only(self, store):
        assert effective_role("stranger@example.com", store, set()) is Role.READ_ONLY

    def test_a_stored_role_is_honoured(self, store):
        store.grant(USER, Role.FULL)
        assert effective_role(USER, store, set()) is Role.FULL


class TestModeRemainsACeiling:
    """The property the whole design rests on."""

    @pytest.mark.parametrize("role", list(Role))
    def test_no_role_exceeds_a_public_deployment(self, role):
        assert resolve_tier(Tier.PUBLIC, USER, ROLE_TIERS[role]) is Tier.PUBLIC

    @pytest.mark.parametrize("role", list(Role))
    def test_no_role_exceeds_a_judge_deployment(self, role):
        assert resolve_tier(Tier.JUDGE, USER, ROLE_TIERS[role]) <= Tier.JUDGE

    def test_a_full_role_is_recorded_but_inert_below_full_mode(self):
        """
        A grant the ceiling denies is accepted -- the operator may raise the
        ceiling later -- but must not take effect meanwhile.
        """
        assert ROLE_TIERS[Role.FULL] is Tier.FULL
        assert resolve_tier(Tier.JUDGE, USER, ROLE_TIERS[Role.FULL]) is Tier.JUDGE


class TestAdminIsASeparateGate:
    def test_user_routes_are_declared_admin_only(self):
        assert requires_admin("POST", "/api/users")
        assert requires_admin("DELETE", "/api/users/{email}")
        assert requires_admin("GET", "/api/users")

    def test_ordinary_routes_are_not(self):
        assert not requires_admin("GET", "/api/benchmarks")
        assert not requires_admin("POST", "/api/benchmarks/{benchmark_id}/judge")

    def test_admin_is_not_expressed_as_a_tier(self):
        """
        If it were, the mode ceiling would deny it on exactly the deployments
        where the console is needed.
        """
        assert ROLE_TIERS[Role.ADMIN] is Tier.FULL
        assert resolve_tier(Tier.JUDGE, ADMIN, ROLE_TIERS[Role.ADMIN]) is Tier.JUDGE


class TestRuntimeWiring:
    def test_the_store_is_settable_and_readable(self, store):
        previous = runtime.get_user_store()
        try:
            runtime.set_user_store(store)
            assert runtime.get_user_store() is store
        finally:
            runtime.set_user_store(previous)

    def test_admin_emails_round_trip(self):
        previous = runtime.get_admin_emails()
        try:
            runtime.set_admin_emails({ADMIN})
            assert runtime.get_admin_emails() == {ADMIN}
        finally:
            runtime.set_admin_emails(previous)
