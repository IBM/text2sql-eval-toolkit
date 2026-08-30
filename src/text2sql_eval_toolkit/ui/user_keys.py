#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
"""
Provider API keys belonging to individual users.

This ends two properties the deployment previously relied on, and the
documentation says so rather than continuing to claim them: there *is* now a user
database to secure, and a public host *does* hold credentials. Storing other
people's billable keys is a different class of system from serving pre-computed
results, so the constraints below are not negotiable.

- **Encrypted at rest, with the key outside the database.** The master key comes
  from ``TEXT2SQL_SECRET_KEY``, so the SQLite file on its own is worthless.
- **Write-only.** Nothing here returns a stored key -- not masked, not truncated,
  not to the user who saved it. A reveal affordance is what turns a leaked
  backup into a breach. Callers get presence, a label and a last-used time.
- **Never logged.** Identities are hashed, as the auth code already does, and a
  test greps the log for a canary value.
- **Persist until deleted.** Re-pasting a key every visit would push people to
  leave a session open forever, which is worse.

There is deliberately no way to store a base URL alongside the key. LiteLLM will
happily take one, and a user-supplied endpoint turns the server into an open
outbound proxy: the caller picks the host, the server makes the request from
inside the network. Custom endpoints belong in server configuration.
"""

from __future__ import annotations

import base64
import hashlib
import os
import sqlite3
import threading
from pathlib import Path
from typing import Dict, List, Optional

from text2sql_eval_toolkit.logging import get_logger
from text2sql_eval_toolkit.ui import auth

logger = get_logger(__name__)

#: Master key for encrypting stored credentials.
SECRET_KEY_ENV = "TEXT2SQL_SECRET_KEY"

#: Providers a user may store a key for. Deliberately a fixed list: it maps onto
#: the dispatch table's prefixes, and an open-ended field would let a caller
#: name a provider the server never routes to.
PROVIDERS = ("wxai", "anthropic", "openai", "gemini")

SCHEMA = """
CREATE TABLE IF NOT EXISTS user_keys (
    email       TEXT NOT NULL,
    provider    TEXT NOT NULL,
    ciphertext  BLOB NOT NULL,
    label       TEXT,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    last_used_at TEXT,
    PRIMARY KEY (email, provider)
);
"""


class SecretsUnavailable(RuntimeError):
    """Raised when no master key is configured, so nothing can be stored."""


def _fernet():
    """
    Build the cipher from ``TEXT2SQL_SECRET_KEY``.

    The variable holds any passphrase; it is stretched to a Fernet key rather
    than required to be one, because an operator generating "a long random
    string" should not silently produce something unusable.
    """
    from cryptography.fernet import Fernet

    raw = os.getenv(SECRET_KEY_ENV, "").strip()
    if not raw:
        raise SecretsUnavailable(
            f"{SECRET_KEY_ENV} is not set, so per-user API keys cannot be stored. "
            'Generate one with: python -c "import secrets; print(secrets.token_urlsafe(48))"'
        )
    if len(raw) < 32:
        raise SecretsUnavailable(
            f"{SECRET_KEY_ENV} must be at least 32 characters. It encrypts other "
            "people's billable credentials."
        )
    digest = hashlib.sha256(raw.encode("utf-8")).digest()
    return Fernet(base64.urlsafe_b64encode(digest))


def secrets_available() -> bool:
    """Whether a usable master key is configured."""
    try:
        _fernet()
    except Exception:
        return False
    return True


class UserKeyStore:
    """
    Encrypted per-user provider credentials.

    Thread-local connections, matching the other stores: a shared connection
    returned rows from the wrong cursor under concurrency once.
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

    def store(self, email: str, provider: str, api_key: str, label: str = "") -> None:
        """
        Encrypt and save *api_key*, replacing any existing key for that provider.

        Raises:
            ValueError: For an unknown provider or an empty key.
            SecretsUnavailable: When no master key is configured.
        """
        provider = (provider or "").strip().lower()
        if provider not in PROVIDERS:
            raise ValueError(
                f"Unknown provider {provider!r}. Known: {', '.join(PROVIDERS)}"
            )
        if not (api_key or "").strip():
            raise ValueError("An API key is required.")

        ciphertext = _fernet().encrypt(api_key.strip().encode("utf-8"))
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO user_keys (email, provider, ciphertext, label) "
                "VALUES (?, ?, ?, ?) ON CONFLICT(email, provider) DO UPDATE SET "
                "ciphertext = excluded.ciphertext, label = excluded.label, "
                "created_at = datetime('now'), last_used_at = NULL",
                (email.strip().lower(), provider, ciphertext, (label or "").strip()),
            )
        # The identity is hashed and the key never appears. This log line exists
        # so an operator can see that a key was replaced without seeing either.
        logger.info("Stored a %s key for %s", provider, auth.hash_identity(email))

    def reveal_for_request(self, email: str, provider: str) -> Optional[str]:
        """
        Decrypt a key for immediate use in one request.

        Named to say what it is: the single place a stored key becomes plaintext.
        It is called on the request path and the result must never be logged,
        returned to a client, or stored anywhere else.

        Returns:
            str | None: The key, or ``None`` when none is stored or the master
            key cannot decrypt it -- which happens if ``TEXT2SQL_SECRET_KEY``
            changed, and is treated as "no key" rather than an error, so a
            rotated master key degrades to re-entry instead of a broken server.
        """
        row = (
            self._connect()
            .execute(
                "SELECT ciphertext FROM user_keys WHERE email = ? AND provider = ?",
                (email.strip().lower(), (provider or "").strip().lower()),
            )
            .fetchone()
        )
        if row is None:
            return None
        try:
            plaintext = _fernet().decrypt(row["ciphertext"]).decode("utf-8")
        except Exception:
            logger.warning(
                "A stored key for %s could not be decrypted; treating it as absent. "
                "Has %s changed?",
                auth.hash_identity(email),
                SECRET_KEY_ENV,
            )
            return None
        with self._connect() as conn:
            conn.execute(
                "UPDATE user_keys SET last_used_at = datetime('now') "
                "WHERE email = ? AND provider = ?",
                (email.strip().lower(), (provider or "").strip().lower()),
            )
        return plaintext

    def describe(self, email: str) -> List[Dict[str, object]]:
        """
        What this user has stored, without any key material.

        Returns:
            list[dict]: ``provider``, ``label``, ``created_at`` and
            ``last_used_at``. Never the key, in any form.
        """
        rows = (
            self._connect()
            .execute(
                "SELECT provider, label, created_at, last_used_at FROM user_keys "
                "WHERE email = ? ORDER BY provider",
                (email.strip().lower(),),
            )
            .fetchall()
        )
        return [dict(row) for row in rows]

    def delete(self, email: str, provider: str) -> bool:
        """Remove a stored key. Returns whether one was removed."""
        with self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM user_keys WHERE email = ? AND provider = ?",
                (email.strip().lower(), (provider or "").strip().lower()),
            )
        return cursor.rowcount > 0

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None
