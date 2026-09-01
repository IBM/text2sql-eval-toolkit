#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Spend metering and caching for on-demand LLM-as-judge.

The judge runs against a *personal* watsonx key on a public deployment, so an
unmetered endpoint is an open tap. Three decisions shape this module:

* **Meter tokens, not calls.** Judge prompts embed both result dataframes and
  vary enormously in size, so a call quota is a poor proxy for cost.
* **Persist to disk.** Counters live on the same volume as the artifacts. An
  in-memory counter would reset on every restart, which is precisely how a
  monthly ceiling silently stops being a ceiling.
* **Rates are configuration, not source.** The defaults below are an *estimate*
  and must be calibrated against a real watsonx invoice; a rate table that
  understates prices lets the meter permit more than the intended budget, so an
  independent budget alert on the watsonx account is still warranted.

Verdicts are cached on the inputs that determine them, so revisiting a record
costs nothing.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from text2sql_eval_toolkit.logging import get_logger

logger = get_logger(__name__)

DEFAULT_BUDGET_USD = 50.0

# USD per million tokens, keyed by model id, as (prompt, completion).
# ESTIMATE ONLY -- override with TEXT2SQL_JUDGE_RATES (JSON) once real billing
# figures are available. See the module docstring.
DEFAULT_RATES: Dict[str, Tuple[float, float]] = {
    "default": (0.60, 1.80),
}

WARN_FRACTION = 0.8


@dataclass(frozen=True)
class Usage:
    """Spend to date within the current billing month."""

    month: str
    spent_usd: float
    budget_usd: float
    calls: int

    @property
    def remaining_usd(self) -> float:
        return max(0.0, self.budget_usd - self.spent_usd)

    @property
    def fraction_used(self) -> float:
        if self.budget_usd <= 0:
            return 1.0
        return self.spent_usd / self.budget_usd

    @property
    def exhausted(self) -> bool:
        return self.spent_usd >= self.budget_usd

    @property
    def warning(self) -> bool:
        return not self.exhausted and self.fraction_used >= WARN_FRACTION


class BudgetExceeded(RuntimeError):
    """Raised when a call would run past the monthly ceiling."""


def budget_usd() -> float:
    raw = os.getenv("TEXT2SQL_JUDGE_MONTHLY_BUDGET_USD")
    if not raw:
        return DEFAULT_BUDGET_USD
    try:
        value = float(raw)
    except ValueError:
        logger.warning(
            "TEXT2SQL_JUDGE_MONTHLY_BUDGET_USD=%r is not a number; using %.2f",
            raw,
            DEFAULT_BUDGET_USD,
        )
        return DEFAULT_BUDGET_USD
    return max(0.0, value)


def judge_disabled() -> bool:
    """Kill switch: disables the judge tier without a redeploy."""
    return os.getenv("TEXT2SQL_JUDGE_DISABLED", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }


def rates() -> Dict[str, Tuple[float, float]]:
    raw = os.getenv("TEXT2SQL_JUDGE_RATES")
    if not raw:
        return dict(DEFAULT_RATES)
    try:
        parsed = json.loads(raw)
        table = {
            str(model): (float(values[0]), float(values[1]))
            for model, values in parsed.items()
        }
        table.setdefault("default", DEFAULT_RATES["default"])
        return table
    except Exception as exc:
        logger.warning(
            "Could not parse TEXT2SQL_JUDGE_RATES (%s); using default rates.", exc
        )
        return dict(DEFAULT_RATES)


def estimate_cost_usd(model: str, prompt_tokens: int, completion_tokens: int) -> float:
    table = rates()
    prompt_rate, completion_rate = table.get(model, table["default"])
    return (prompt_tokens * prompt_rate + completion_tokens * completion_rate) / 1e6


def current_month(now: Optional[float] = None) -> str:
    return time.strftime("%Y-%m", time.gmtime(now if now is not None else time.time()))


def verdict_cache_key(
    benchmark_id: str,
    record_id: str,
    pipeline_id: str,
    config_name: str,
    model: str,
    config: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Identify a verdict by everything that determines it.

    Changing the judge model or its prompt config must produce a new verdict
    rather than reusing one produced under different conditions.

    That is why `config` is the loaded YAML and not just its name. The prompts
    are editable from the UI and keep their filename across an edit, so keying on
    the name alone served the verdict from the *previous* prompt -- the one case
    where a stale answer is least acceptable, since editing the prompt is how you
    ask a different question.
    """
    # JSON-encoded rather than space-joined: joining is not injective, so
    # (record="r1", pipeline="p1") and (record="r1 p1", pipeline="") collided
    # and could serve one record's verdict for another.
    config_digest = ""
    if config is not None:
        # sort_keys so an unrelated reordering of the YAML does not invalidate
        # every cached verdict; default=str so an unexpected value cannot make
        # the key un-computable and take the whole endpoint down with it.
        config_digest = hashlib.sha256(
            json.dumps(config, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()
    payload = json.dumps(
        [benchmark_id, record_id, pipeline_id, config_name, model, config_digest],
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


_SCHEMA = """
CREATE TABLE IF NOT EXISTS spend (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    month             TEXT NOT NULL,
    user_hash         TEXT NOT NULL,
    created_at        INTEGER NOT NULL,
    model             TEXT NOT NULL,
    prompt_tokens     INTEGER NOT NULL,
    completion_tokens INTEGER NOT NULL,
    cost_usd          REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_spend_month ON spend (month);

CREATE TABLE IF NOT EXISTS user_caps (
    user_hash TEXT PRIMARY KEY,
    cap_usd   REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS verdicts (
    cache_key    TEXT PRIMARY KEY,
    benchmark_id TEXT NOT NULL,
    record_id    TEXT NOT NULL,
    pipeline_id  TEXT NOT NULL,
    config_name  TEXT NOT NULL,
    model        TEXT NOT NULL,
    verdict      TEXT NOT NULL,
    score        REAL,
    explanation  TEXT,
    user_hash    TEXT NOT NULL,
    created_at   INTEGER NOT NULL
);
"""


class JudgeStore:
    """
    Persistent spend counters and verdict cache.

    Deliberately separate from the evaluation artifacts: an on-demand verdict is
    attributable to the user who asked for it and must not alter what every
    other visitor sees, nor the numbers published in the snapshot.
    """

    def __init__(self, path: Path) -> None:
        # Amounts claimed but not yet committed, per user. In memory on
        # purpose: a reservation is only meaningful for the life of the
        # request that made it, and a crash must not leave one stuck.
        self._reserved: Dict[str, float] = {}
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        with self._connect() as conn:
            conn.executescript(_SCHEMA)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path, timeout=10)
        conn.row_factory = sqlite3.Row
        return conn

    # -- spend ------------------------------------------------------------

    def usage(self, month: Optional[str] = None) -> Usage:
        month = month or current_month()
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT COALESCE(SUM(cost_usd), 0) AS spent, COUNT(*) AS calls"
                " FROM spend WHERE month = ?",
                (month,),
            ).fetchone()
        return Usage(
            month=month,
            spent_usd=float(row["spent"]),
            budget_usd=budget_usd(),
            calls=int(row["calls"]),
        )

    def check_budget(self) -> Usage:
        """Raise if the ceiling is already reached; otherwise report usage."""
        current = self.usage()
        if current.exhausted:
            raise BudgetExceeded(
                f"The monthly LLM-judge budget of ${current.budget_usd:.2f} is "
                f"used up (${current.spent_usd:.2f} spent). It resets at the "
                "start of next month."
            )
        return current

    # --- per-user caps ---------------------------------------------------
    #
    # A user spending their own key is a different question from the global
    # ceiling on the server-held one: it is their provider account. The cap is
    # what this server will spend on their behalf, and the UI must say so --
    # the key keeps working everywhere else.

    def set_user_cap(self, user_hash: str, cap_usd: Optional[float]) -> None:
        """Set a monthly ceiling for one user, or ``None`` to remove it."""
        with self._lock, self._connect() as conn:
            if cap_usd is None:
                conn.execute("DELETE FROM user_caps WHERE user_hash = ?", (user_hash,))
            else:
                conn.execute(
                    "INSERT INTO user_caps (user_hash, cap_usd) VALUES (?, ?) "
                    "ON CONFLICT(user_hash) DO UPDATE SET cap_usd = excluded.cap_usd",
                    (user_hash, float(cap_usd)),
                )

    def user_cap(self, user_hash: str) -> Optional[float]:
        row = (
            self._connect()
            .execute("SELECT cap_usd FROM user_caps WHERE user_hash = ?", (user_hash,))
            .fetchone()
        )
        return None if row is None else float(row["cap_usd"])

    def user_spent(self, user_hash: str, month: Optional[str] = None) -> float:
        row = (
            self._connect()
            .execute(
                "SELECT COALESCE(SUM(cost_usd), 0) AS total FROM spend "
                "WHERE user_hash = ? AND month = ?",
                (user_hash, month or current_month()),
            )
            .fetchone()
        )
        return float(row["total"] or 0.0)

    def reserve(self, user_hash: str, estimate_usd: float) -> bool:
        """
        Claim *estimate_usd* against this user's cap before spending it.

        Checking and then spending overshoots: evaluation runs sixteen coroutines
        against one semaphore, so sixteen of them can each read a cap that is
        still under budget and then all spend. Reserving under the same lock that
        reads the total makes the in-flight amount visible to the others.

        The cap is read under that lock too, not before it. ``set_user_cap``
        writes under the same lock, so reading outside it decided against a
        value an administrator may already have changed -- lower the cap while a
        reservation is in flight and that one reservation would be granted
        against the old ceiling.

        Returns:
            bool: Whether the reservation fit under the cap. ``True`` when the
            user has no cap.
        """
        with self._lock:
            cap = self.user_cap(user_hash)
            if cap is None:
                return True
            committed = self.user_spent(user_hash)
            pending = self._reserved.get(user_hash, 0.0)
            if committed + pending + estimate_usd > cap:
                return False
            self._reserved[user_hash] = pending + estimate_usd
            return True

    def release(self, user_hash: str, estimate_usd: float) -> None:
        """Give back a reservation once the real cost is known, or on failure."""
        with self._lock:
            remaining = self._reserved.get(user_hash, 0.0) - estimate_usd
            if remaining > 0:
                self._reserved[user_hash] = remaining
            else:
                self._reserved.pop(user_hash, None)

    def record_spend(
        self,
        user_hash: str,
        model: str,
        prompt_tokens: int,
        completion_tokens: int,
    ) -> float:
        cost = estimate_cost_usd(model, prompt_tokens, completion_tokens)
        with self._lock, self._connect() as conn:
            conn.execute(
                "INSERT INTO spend (month, user_hash, created_at, model,"
                " prompt_tokens, completion_tokens, cost_usd)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    current_month(),
                    user_hash,
                    int(time.time()),
                    model,
                    int(prompt_tokens),
                    int(completion_tokens),
                    cost,
                ),
            )
            conn.commit()
        return cost

    # -- verdict cache ----------------------------------------------------

    def get_verdict(self, cache_key: str) -> Optional[Dict[str, Any]]:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT verdict, score, explanation, model, created_at"
                " FROM verdicts WHERE cache_key = ?",
                (cache_key,),
            ).fetchone()
        return dict(row) if row else None

    def delete_verdict(self, cache_key: str) -> bool:
        """
        Drop a stored verdict, if there is one.

        Used when a forced re-judge comes back unreadable: an ``N/A`` is not
        written, so without this the verdict the caller asked to replace would
        simply still be there on the next request.

        Args:
            cache_key: The key from ``verdict_cache_key``.

        Returns:
            Whether a row was removed.
        """
        with self._lock, self._connect() as conn:
            removed = conn.execute(
                "DELETE FROM verdicts WHERE cache_key = ?", (cache_key,)
            ).rowcount
            conn.commit()
        return bool(removed)

    def put_verdict(
        self,
        cache_key: str,
        *,
        benchmark_id: str,
        record_id: str,
        pipeline_id: str,
        config_name: str,
        model: str,
        verdict: str,
        score: Optional[float],
        explanation: Optional[str],
        user_hash: str,
    ) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO verdicts (cache_key, benchmark_id,"
                " record_id, pipeline_id, config_name, model, verdict, score,"
                " explanation, user_hash, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    cache_key,
                    benchmark_id,
                    record_id,
                    pipeline_id,
                    config_name,
                    model,
                    verdict,
                    score,
                    explanation,
                    user_hash,
                    int(time.time()),
                ),
            )
            conn.commit()
