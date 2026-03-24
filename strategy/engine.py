"""Strategy engine — maintains regime-to-strategy mappings.

Strategies are independent from regimes: a regime is a detected market state,
a strategy is an action plan assigned to that regime that can change independently.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# Default strategies — fallbacks when no override exists in the DB.
# These match the previously hardcoded values from the frontend.
DEFAULT_STRATEGIES: dict[str, dict[str, Any]] = {
    "GROWTH": {
        "name": "Risk-On Growth",
        "posture": "Aggressive",
        "allocation": "70% equities, 15% commodities, 10% crypto, 5% cash",
        "risk_level": "Low",
        "action": "Stay long equities, add on dips",
        "rationale": "Broad expansion — momentum is your friend",
    },
    "NEUTRAL": {
        "name": "Balanced Diversification",
        "posture": "Balanced",
        "allocation": "40% equities, 25% bonds, 15% alternatives, 20% cash",
        "risk_level": "Medium",
        "action": "Diversify broadly, reduce conviction bets",
        "rationale": "Mixed signals — no clear edge, stay nimble",
    },
    "FRAGILE": {
        "name": "Defensive Quality",
        "posture": "Defensive",
        "allocation": "25% equities (quality), 35% bonds, 20% gold, 20% cash",
        "risk_level": "High",
        "action": "Reduce risk, move to quality",
        "rationale": "Deteriorating — protect capital, hedge tail risk",
    },
    "CRISIS": {
        "name": "Capital Preservation",
        "posture": "Capital Preservation",
        "allocation": "10% equities, 40% treasuries, 25% gold, 25% cash",
        "risk_level": "Extreme",
        "action": "Preserve capital, buy tail hedges",
        "rationale": "Active stress — survival mode, wait for opportunity",
    },
}

# SQL to create the strategy_assignments table
CREATE_TABLE_SQL = text("""
    CREATE TABLE IF NOT EXISTS strategy_assignments (
        id SERIAL PRIMARY KEY,
        regime_state TEXT NOT NULL,
        name TEXT NOT NULL,
        posture TEXT NOT NULL,
        allocation TEXT NOT NULL DEFAULT '',
        risk_level TEXT NOT NULL DEFAULT 'Medium',
        action TEXT NOT NULL DEFAULT '',
        rationale TEXT NOT NULL DEFAULT '',
        assigned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        active BOOLEAN NOT NULL DEFAULT TRUE,
        UNIQUE (regime_state, active) -- only one active strategy per regime
    )
""")


class StrategyEngine:
    """Manages strategy assignments for regime states."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine
        self._ensure_table()

    def _ensure_table(self) -> None:
        """Create strategy_assignments table if it does not exist."""
        try:
            with self._engine.begin() as conn:
                conn.execute(CREATE_TABLE_SQL)
            log.debug("strategy_assignments table ensured")
        except Exception as exc:
            log.warning("Could not create strategy_assignments table: {e}", e=str(exc))

    def get_active_strategies(self) -> list[dict[str, Any]]:
        """Return all active strategies, falling back to defaults for missing regimes."""
        db_strategies = self._load_active_from_db()

        # Build lookup by regime_state
        by_regime: dict[str, dict[str, Any]] = {}
        for s in db_strategies:
            by_regime[s["regime_state"]] = s

        # Merge defaults for regimes without DB overrides
        result: list[dict[str, Any]] = []
        for regime_state, defaults in DEFAULT_STRATEGIES.items():
            if regime_state in by_regime:
                result.append(by_regime[regime_state])
            else:
                result.append(self._default_to_dict(regime_state, defaults))

        # Include any DB strategies for non-default regimes
        for s in db_strategies:
            if s["regime_state"] not in DEFAULT_STRATEGIES:
                result.append(s)

        return result

    def get_strategy_for_regime(self, regime_state: str) -> dict[str, Any] | None:
        """Return the active strategy for a specific regime state."""
        # Check DB first
        db_strategy = self._load_for_regime(regime_state)
        if db_strategy is not None:
            return db_strategy

        # Fall back to defaults
        defaults = DEFAULT_STRATEGIES.get(regime_state)
        if defaults is not None:
            return self._default_to_dict(regime_state, defaults)

        return None

    def assign_strategy(
        self,
        regime_state: str,
        name: str,
        posture: str,
        allocation: str = "",
        risk_level: str = "Medium",
        action: str = "",
        rationale: str = "",
    ) -> dict[str, Any]:
        """Assign or update a strategy for a regime state.

        Deactivates any existing active strategy for that regime first.
        """
        now = datetime.now(timezone.utc)

        with self._engine.begin() as conn:
            # Deactivate existing active strategy for this regime
            conn.execute(
                text(
                    "UPDATE strategy_assignments "
                    "SET active = FALSE "
                    "WHERE regime_state = :regime_state AND active = TRUE"
                ).bindparams(regime_state=regime_state),
            )

            # Insert new strategy
            row = conn.execute(
                text(
                    "INSERT INTO strategy_assignments "
                    "(regime_state, name, posture, allocation, risk_level, action, rationale, assigned_at, active) "
                    "VALUES (:regime_state, :name, :posture, :allocation, :risk_level, :action, :rationale, :assigned_at, TRUE) "
                    "RETURNING id, regime_state, name, posture, allocation, risk_level, action, rationale, assigned_at, active"
                ).bindparams(
                    regime_state=regime_state,
                    name=name,
                    posture=posture,
                    allocation=allocation,
                    risk_level=risk_level,
                    action=action,
                    rationale=rationale,
                    assigned_at=now,
                ),
            ).fetchone()

        if row is None:
            raise RuntimeError("Failed to insert strategy assignment")

        log.info(
            "Strategy assigned: {name} for regime {regime}",
            name=name,
            regime=regime_state,
        )

        return self._row_to_dict(row)

    # -- internal helpers --

    def _load_active_from_db(self) -> list[dict[str, Any]]:
        """Load all active strategies from the database."""
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT id, regime_state, name, posture, allocation, risk_level, "
                        "action, rationale, assigned_at, active "
                        "FROM strategy_assignments "
                        "WHERE active = TRUE "
                        "ORDER BY assigned_at DESC"
                    )
                ).fetchall()
            return [self._row_to_dict(r) for r in rows]
        except Exception as exc:
            log.warning("Failed to load strategies from DB: {e}", e=str(exc))
            return []

    def _load_for_regime(self, regime_state: str) -> dict[str, Any] | None:
        """Load the active strategy for a specific regime from the database."""
        try:
            with self._engine.connect() as conn:
                row = conn.execute(
                    text(
                        "SELECT id, regime_state, name, posture, allocation, risk_level, "
                        "action, rationale, assigned_at, active "
                        "FROM strategy_assignments "
                        "WHERE regime_state = :regime_state AND active = TRUE "
                        "LIMIT 1"
                    ).bindparams(regime_state=regime_state),
                ).fetchone()
            if row is not None:
                return self._row_to_dict(row)
        except Exception as exc:
            log.warning(
                "Failed to load strategy for {regime}: {e}",
                regime=regime_state,
                e=str(exc),
            )
        return None

    @staticmethod
    def _row_to_dict(row: Any) -> dict[str, Any]:
        """Convert a DB row to a strategy dict."""
        return {
            "id": row[0],
            "regime_state": row[1],
            "name": row[2],
            "posture": row[3],
            "allocation": row[4],
            "risk_level": row[5],
            "action": row[6],
            "rationale": row[7],
            "assigned_at": row[8].isoformat() if row[8] else "",
            "active": row[9],
            "source": "database",
        }

    @staticmethod
    def _default_to_dict(regime_state: str, defaults: dict[str, Any]) -> dict[str, Any]:
        """Convert a default strategy to the standard dict format."""
        return {
            "id": None,
            "regime_state": regime_state,
            "name": defaults["name"],
            "posture": defaults["posture"],
            "allocation": defaults["allocation"],
            "risk_level": defaults["risk_level"],
            "action": defaults["action"],
            "rationale": defaults["rationale"],
            "assigned_at": "",
            "active": True,
            "source": "default",
        }
