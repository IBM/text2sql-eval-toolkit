#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
"""
Who may do what, stored rather than configured.

Roles used to live in ``TEXT2SQL_JUDGE_ALLOWLIST``, so changing who could reach
the judge meant editing ``deploy/.env`` and recreating the container. They now
live in a small SQLite table an admin edits from the dashboard.

Two things did not change, and must not:

- **Mode is still a ceiling.** A row in this table can never carry a caller above
  what the operator permitted at startup. A deployment in ``judge`` mode grants
  at most ``judge``, whatever any role says.
- **Only verified addresses match.** The role is keyed to the ``email`` claim
  Google returns alongside ``email_verified``; an unverified address matches
  nothing, or the table would mean nothing.

``TEXT2SQL_ADMIN_EMAILS`` is the standing recovery path, not a one-time seed. It
is read at every startup and always grants admin, so an operator with shell
access can always recover a deployment whose table is wrong -- which matters more
now that the allowlist variable is gone.
"""

from __future__ import annotations

import os
import sqlite3
import threading
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Set

from text2sql_eval_toolkit.logging import get_logger
from text2sql_eval_toolkit.ui.capabilities import Tier, parse_allowlist

logger = get_logger(__name__)

#: Environment variable naming the addresses that always hold admin.
ADMIN_EMAILS_ENV = "TEXT2SQL_ADMIN_EMAILS"

#: Removed in 1.4.0. Named here only so startup can warn when it is still set.
REMOVED_ALLOWLIST_ENV = "TEXT2SQL_JUDGE_ALLOWLIST"


class Role(str, Enum):
    """What an address is allowed to do, before the mode ceiling applies."""

    READ_ONLY = "read_only"
    JUDGE = "judge"
    FULL = "full"
    ADMIN = "admin"

    @classmethod
    def parse(cls, value: str) -> "Role":
        try:
            return cls(str(value).strip().lower())
        except ValueError:
            raise ValueError(
                f"Unknown role {value!r}. Known roles: {', '.join(r.value for r in cls)}"
            ) from None


#: Role -> the tier it asks for. The ceiling is applied separately, so a grant
#: that a deployment cannot honour is recorded and simply does not take effect.
ROLE_TIERS: Dict[Role, Tier] = {
    Role.READ_ONLY: Tier.PUBLIC,
    Role.JUDGE: Tier.JUDGE,
    Role.FULL: Tier.FULL,
    # Admin asks for full, and is separately allowed to manage users. Those are
    # different questions: user management has to work on a judge-mode host,
    # where the ceiling denies full.
    Role.ADMIN: Tier.FULL,
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS roles (
    email      TEXT PRIMARY KEY,
    role       TEXT NOT NULL,
    granted_by TEXT,
    granted_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"""


def admin_emails_from_env() -> Set[str]:
    """
    Addresses that always hold admin, from ``TEXT2SQL_ADMIN_EMAILS``.

    Addresses, not domains: a domain would be cheaper to administer and is the
    wrong tool here, since ``@example.com`` would make every colleague an admin
    of a host on the public internet.

    Normalised exactly like the allowlist it replaces -- comma-separated,
    stripped, lower-cased -- so an address that worked there works here.

    Returns:
        set[str]: Normalised addresses. Empty when the variable is unset.
    """
    return parse_allowlist(os.getenv(ADMIN_EMAILS_ENV))


class UserStore:
    """
    Roles on disk.

    Thread-local connections, as the judge ledger uses: a single shared
    connection returned rows from the wrong cursor under concurrency once, and
    the fix is not worth rediscovering.
    """

    def __init__(self, path: Path) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        with self._connect() as conn:
            conn.executescript(SCHEMA)

    def _connect(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self._path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            self._local.conn = conn
        return conn

    def role_for(self, email: Optional[str]) -> Optional[Role]:
        """
        The stored role for *email*, or ``None`` when there is no row.

        Args:
            email: A verified address. Normalised before lookup.

        Returns:
            Role | None
        """
        if not email:
            return None
        row = (
            self._connect()
            .execute("SELECT role FROM roles WHERE email = ?", (email.strip().lower(),))
            .fetchone()
        )
        if row is None:
            return None
        try:
            return Role.parse(row["role"])
        except ValueError:
            # A row written by a newer version, or edited by hand. Refusing to
            # guess is safer than granting the wrong thing.
            logger.warning("Ignoring unknown role %r for a stored user", row["role"])
            return None

    def grant(self, email: str, role: Role, granted_by: Optional[str] = None) -> None:
        """Set *email*'s role, replacing any existing one."""
        normalised = email.strip().lower()
        if not normalised:
            raise ValueError("email is required")
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO roles (email, role, granted_by) VALUES (?, ?, ?) "
                "ON CONFLICT(email) DO UPDATE SET role = excluded.role, "
                "granted_by = excluded.granted_by, granted_at = datetime('now')",
                (normalised, role.value, granted_by),
            )

    def revoke(self, email: str) -> bool:
        """
        Remove *email*'s row.

        Returns:
            bool: Whether a row was removed. An address named in
            ``TEXT2SQL_ADMIN_EMAILS`` keeps admin regardless -- that is the point
            of it being the recovery path.
        """
        with self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM roles WHERE email = ?", (email.strip().lower(),)
            )
            return cursor.rowcount > 0

    def list_users(self) -> List[dict]:
        """Every stored row, oldest grant first."""
        rows = (
            self._connect()
            .execute(
                "SELECT email, role, granted_by, granted_at FROM roles ORDER BY granted_at"
            )
            .fetchall()
        )
        return [dict(row) for row in rows]

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None


def effective_role(
    email: Optional[str],
    store: Optional[UserStore],
    admin_emails: Set[str],
) -> Role:
    """
    The role for *email*, before the mode ceiling is applied.

    ``TEXT2SQL_ADMIN_EMAILS`` wins over the table, so an operator cannot be
    locked out by a bad row.

    Args:
        email: The verified address, or ``None`` for an anonymous caller.
        store: The role table, or ``None`` when none is configured.
        admin_emails: Addresses from the environment.

    Returns:
        Role: ``READ_ONLY`` for anonymous callers and for addresses with no row.
    """
    if not email:
        return Role.READ_ONLY
    normalised = email.strip().lower()
    if normalised in admin_emails:
        return Role.ADMIN
    if store is not None:
        stored = store.role_for(normalised)
        if stored is not None:
            return stored
    return Role.READ_ONLY
