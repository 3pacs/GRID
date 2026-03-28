"""
Strategy-level circuit breaker for the signal executor.

Tracks consecutive failures per strategy. After N consecutive errors or
bad trades, halts execution for that strategy until cooldown expires or
operator manually resets.

States:
    CLOSED   — normal operation, signals execute
    OPEN     — halted after threshold failures, signals skip
    HALF_OPEN — probation after cooldown, one trade allowed

Inspired by nofx's safe-mode auto-protection pattern.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from config import settings


class BreakerState(str, Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class StrategyCircuitBreaker:
    """Per-strategy circuit breaker for the signal executor.

    Parameters:
        engine: SQLAlchemy database engine.
        threshold: Consecutive failures before tripping (default from config).
        cooldown_hours: Hours before auto-transitioning OPEN → HALF_OPEN.
    """

    def __init__(
        self,
        engine: Engine,
        threshold: int | None = None,
        cooldown_hours: int | None = None,
    ) -> None:
        self.engine = engine
        self.threshold = threshold or settings.CIRCUIT_BREAKER_THRESHOLD
        self.cooldown_hours = cooldown_hours or settings.CIRCUIT_BREAKER_COOLDOWN_HOURS

    def _ensure_table(self) -> None:
        """Create the breaker state table if it doesn't exist."""
        with self.engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS paper_strategy_breaker_state (
                    strategy_id TEXT PRIMARY KEY,
                    state TEXT DEFAULT 'CLOSED',
                    consecutive_failures INT DEFAULT 0,
                    last_failure_at TIMESTAMPTZ,
                    opened_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))

    def get_state(self, strategy_id: str) -> dict[str, Any]:
        """Get current breaker state for a strategy.

        Returns dict with state, consecutive_failures, opened_at.
        Returns CLOSED defaults if no record exists.
        """
        with self.engine.connect() as conn:
            row = conn.execute(text(
                "SELECT state, consecutive_failures, last_failure_at, opened_at "
                "FROM paper_strategy_breaker_state "
                "WHERE strategy_id = :sid"
            ), {"sid": strategy_id}).fetchone()

        if row is None:
            return {
                "state": BreakerState.CLOSED,
                "consecutive_failures": 0,
                "last_failure_at": None,
                "opened_at": None,
            }

        return {
            "state": BreakerState(row[0]),
            "consecutive_failures": row[1],
            "last_failure_at": row[2],
            "opened_at": row[3],
        }

    def should_execute(self, strategy_id: str) -> bool:
        """Check if a strategy is allowed to execute.

        Handles auto-transition from OPEN → HALF_OPEN after cooldown.

        Returns:
            True if the strategy may execute, False if halted.
        """
        info = self.get_state(strategy_id)
        state = info["state"]

        if state == BreakerState.CLOSED:
            return True

        if state == BreakerState.HALF_OPEN:
            # Allow one probe trade
            return True

        # OPEN — check if cooldown has elapsed
        opened_at = info["opened_at"]
        if opened_at is not None:
            cooldown_end = opened_at + timedelta(hours=self.cooldown_hours)
            now = datetime.now(timezone.utc)
            if now >= cooldown_end:
                log.info(
                    "Circuit breaker cooldown elapsed for strategy {s} — transitioning to HALF_OPEN",
                    s=strategy_id,
                )
                self._set_state(strategy_id, BreakerState.HALF_OPEN)
                return True

        log.debug(
            "Circuit breaker OPEN for strategy {s} — skipping execution",
            s=strategy_id,
        )
        return False

    def record_success(self, strategy_id: str) -> None:
        """Record a successful execution. Resets breaker to CLOSED."""
        info = self.get_state(strategy_id)

        if info["state"] == BreakerState.HALF_OPEN:
            log.info(
                "Strategy {s} succeeded on probation — circuit breaker CLOSED",
                s=strategy_id,
            )

        self._upsert(strategy_id, BreakerState.CLOSED, consecutive_failures=0)

    def record_failure(self, strategy_id: str, error: str = "") -> None:
        """Record a failure. May trip the breaker to OPEN.

        Parameters:
            strategy_id: The strategy that failed.
            error: Description of the failure for logging.
        """
        info = self.get_state(strategy_id)
        new_failures = info["consecutive_failures"] + 1
        now = datetime.now(timezone.utc)

        # HALF_OPEN failure → back to OPEN immediately
        if info["state"] == BreakerState.HALF_OPEN:
            log.warning(
                "Strategy {s} failed on probation — circuit breaker re-OPENED: {e}",
                s=strategy_id, e=error,
            )
            self._upsert(
                strategy_id, BreakerState.OPEN,
                consecutive_failures=new_failures,
                last_failure_at=now, opened_at=now,
            )
            self._send_alert(strategy_id, new_failures, error)
            return

        # Threshold reached → OPEN
        if new_failures >= self.threshold:
            log.warning(
                "Strategy {s} hit {n} consecutive failures — circuit breaker OPEN: {e}",
                s=strategy_id, n=new_failures, e=error,
            )
            self._upsert(
                strategy_id, BreakerState.OPEN,
                consecutive_failures=new_failures,
                last_failure_at=now, opened_at=now,
            )
            self._send_alert(strategy_id, new_failures, error)
            return

        # Below threshold — stay CLOSED, increment count
        self._upsert(
            strategy_id, BreakerState.CLOSED,
            consecutive_failures=new_failures,
            last_failure_at=now,
        )

    def reset(self, strategy_id: str) -> None:
        """Manual operator reset — force breaker to CLOSED."""
        log.info("Circuit breaker manually reset for strategy {s}", s=strategy_id)
        self._upsert(strategy_id, BreakerState.CLOSED, consecutive_failures=0)

    def get_all_states(self) -> list[dict[str, Any]]:
        """Get breaker states for all strategies with records."""
        with self.engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT strategy_id, state, consecutive_failures, "
                "last_failure_at, opened_at, updated_at "
                "FROM paper_strategy_breaker_state "
                "ORDER BY updated_at DESC"
            )).fetchall()

        return [
            {
                "strategy_id": r[0],
                "state": r[1],
                "consecutive_failures": r[2],
                "last_failure_at": r[3].isoformat() if r[3] else None,
                "opened_at": r[4].isoformat() if r[4] else None,
                "updated_at": r[5].isoformat() if r[5] else None,
            }
            for r in rows
        ]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _set_state(self, strategy_id: str, state: BreakerState) -> None:
        """Update only the state column for an existing record."""
        with self.engine.begin() as conn:
            conn.execute(text(
                "UPDATE paper_strategy_breaker_state "
                "SET state = :state, updated_at = NOW() "
                "WHERE strategy_id = :sid"
            ), {"state": state.value, "sid": strategy_id})

    def _upsert(
        self,
        strategy_id: str,
        state: BreakerState,
        consecutive_failures: int = 0,
        last_failure_at: datetime | None = None,
        opened_at: datetime | None = None,
    ) -> None:
        """Insert or update breaker state."""
        with self.engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO paper_strategy_breaker_state
                    (strategy_id, state, consecutive_failures,
                     last_failure_at, opened_at, updated_at)
                VALUES (:sid, :state, :cf, :lfa, :oa, NOW())
                ON CONFLICT (strategy_id) DO UPDATE SET
                    state = :state,
                    consecutive_failures = :cf,
                    last_failure_at = COALESCE(:lfa, paper_strategy_breaker_state.last_failure_at),
                    opened_at = COALESCE(:oa, paper_strategy_breaker_state.opened_at),
                    updated_at = NOW()
            """), {
                "sid": strategy_id,
                "state": state.value,
                "cf": consecutive_failures,
                "lfa": last_failure_at,
                "oa": opened_at,
            })

    def _send_alert(self, strategy_id: str, failures: int, error: str) -> None:
        """Send operator alert when a circuit breaker trips."""
        try:
            from alerts.email import send_failure_alert
            send_failure_alert(
                subject=f"Circuit Breaker OPEN — strategy {strategy_id}",
                body=(
                    f"Strategy {strategy_id} has been halted after "
                    f"{failures} consecutive failures.\n\n"
                    f"Last error: {error}\n\n"
                    f"Cooldown: {self.cooldown_hours} hours before probation.\n"
                    f"Manual reset: POST /api/breaker/{strategy_id}/reset"
                ),
            )
        except Exception as exc:
            log.debug("Circuit breaker alert email skipped: {e}", e=str(exc))
