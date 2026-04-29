"""
Database-backed state for the Solana exit manager and learner.

Three tables — all created on first use, no Alembic migration needed:

  * ``solana_exit_state`` — one row per open paper_trade tracking peak /
    trough pnl, remaining fraction, rungs hit, trailing arm status, and
    the policy variant the learner selected for this position.
  * ``solana_exit_events`` — append-only audit log of every partial
    close, stop, or arm-trailing event the manager fires. Immutable by
    convention (matches GRID's decision_journal pattern).
  * ``solana_policy_variants`` — the learner's posterior state per
    variant × source_type, stored as JSON so it survives schema changes.

This module is pure persistence — no decision logic, no RNG. The
manager and learner both read/write through it.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# Re-exported for the manager
SOURCE_UNKNOWN = "unknown"


# ----------------------------------------------------------------------
# DTOs
# ----------------------------------------------------------------------
@dataclass(frozen=True)
class PositionStateRow:
    """One row from ``solana_exit_state`` joined with ``paper_trades``."""

    trade_id: int
    ticker: str
    direction: str
    entry_price: float
    entry_time: datetime
    peak_pnl_pct: float
    trough_pnl_pct: float
    remaining_fraction: float
    tp_rungs_hit: int
    trailing_armed: bool
    filled_exit_value: float
    filled_exit_fraction: float
    policy_variant: str
    source_type: str


@dataclass(frozen=True)
class VariantStatsRow:
    variant_id: str
    source_type: str
    n_samples: int
    reward_mean: float
    reward_m2: float  # Welford sum-of-squared-deviations
    total_pnl_pct: float
    wins: int
    losses: int
    last_reward: float | None

    @property
    def reward_variance(self) -> float:
        return self.reward_m2 / self.n_samples if self.n_samples > 0 else 0.0

    @property
    def reward_stddev(self) -> float:
        return self.reward_variance**0.5


# ----------------------------------------------------------------------
# Store
# ----------------------------------------------------------------------
class ExitStateStore:
    """CRUD wrapper around the three exit-management tables."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self._ensure_tables()

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------
    def _ensure_tables(self) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS solana_exit_state (
                        trade_id              INTEGER PRIMARY KEY,
                        peak_pnl_pct          FLOAT NOT NULL DEFAULT 0,
                        trough_pnl_pct        FLOAT NOT NULL DEFAULT 0,
                        remaining_fraction    FLOAT NOT NULL DEFAULT 1.0,
                        tp_rungs_hit          INTEGER NOT NULL DEFAULT 0,
                        trailing_armed        BOOLEAN NOT NULL DEFAULT FALSE,
                        filled_exit_value     FLOAT NOT NULL DEFAULT 0,
                        filled_exit_fraction  FLOAT NOT NULL DEFAULT 0,
                        policy_variant        TEXT NOT NULL,
                        source_type           TEXT NOT NULL DEFAULT 'unknown',
                        first_tick_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        last_tick_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS solana_exit_events (
                        id              SERIAL PRIMARY KEY,
                        trade_id        INTEGER NOT NULL,
                        event_type      TEXT NOT NULL,
                        rung_index      INTEGER,
                        fraction        FLOAT NOT NULL,
                        price           FLOAT NOT NULL,
                        pnl_pct         FLOAT NOT NULL,
                        peak_pnl_pct    FLOAT NOT NULL,
                        policy_variant  TEXT NOT NULL,
                        source_type     TEXT NOT NULL DEFAULT 'unknown',
                        reason          TEXT,
                        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS solana_policy_variants (
                        variant_id      TEXT NOT NULL,
                        source_type     TEXT NOT NULL DEFAULT 'unknown',
                        n_samples       INTEGER NOT NULL DEFAULT 0,
                        reward_mean     FLOAT NOT NULL DEFAULT 0,
                        reward_m2       FLOAT NOT NULL DEFAULT 0,
                        total_pnl_pct   FLOAT NOT NULL DEFAULT 0,
                        wins            INTEGER NOT NULL DEFAULT 0,
                        losses          INTEGER NOT NULL DEFAULT 0,
                        last_reward     FLOAT,
                        params          JSONB,
                        updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (variant_id, source_type)
                    )
                    """
                )
            )

    # ------------------------------------------------------------------
    # Per-position state
    # ------------------------------------------------------------------
    def ensure_position(
        self,
        trade_id: int,
        policy_variant: str,
        source_type: str = SOURCE_UNKNOWN,
    ) -> bool:
        """Insert a fresh exit-state row for ``trade_id`` if none exists.

        Returns True if we inserted, False if a row was already present.
        """
        with self.engine.begin() as conn:
            existing = conn.execute(
                text("SELECT 1 FROM solana_exit_state WHERE trade_id = :tid"),
                {"tid": trade_id},
            ).fetchone()
            if existing:
                return False
            conn.execute(
                text(
                    "INSERT INTO solana_exit_state "
                    "(trade_id, policy_variant, source_type) "
                    "VALUES (:tid, :v, :s)"
                ),
                {"tid": trade_id, "v": policy_variant, "s": source_type},
            )
        return True

    def load_position(self, trade_id: int) -> PositionStateRow | None:
        """Load the joined state+trade row for ``trade_id``."""
        sql = text(
            """
            SELECT pt.id, pt.ticker, pt.direction, pt.entry_price,
                   pt.entry_date, pt.created_at,
                   s.peak_pnl_pct, s.trough_pnl_pct,
                   s.remaining_fraction, s.tp_rungs_hit, s.trailing_armed,
                   s.filled_exit_value, s.filled_exit_fraction,
                   s.policy_variant, s.source_type
            FROM solana_exit_state s
            JOIN paper_trades pt ON pt.id = s.trade_id
            WHERE s.trade_id = :tid
            """
        )
        with self.engine.connect() as conn:
            row = conn.execute(sql, {"tid": trade_id}).fetchone()
        if row is None:
            return None
        return _row_to_position(row)

    def list_open_positions(self, strategy_id: str) -> list[PositionStateRow]:
        """Return every open paper_trade for ``strategy_id`` joined with
        its exit-state row. Trades without state are skipped — the caller
        should call :meth:`ensure_position` first.
        """
        sql = text(
            """
            SELECT pt.id, pt.ticker, pt.direction, pt.entry_price,
                   pt.entry_date, pt.created_at,
                   s.peak_pnl_pct, s.trough_pnl_pct,
                   s.remaining_fraction, s.tp_rungs_hit, s.trailing_armed,
                   s.filled_exit_value, s.filled_exit_fraction,
                   s.policy_variant, s.source_type
            FROM paper_trades pt
            JOIN solana_exit_state s ON s.trade_id = pt.id
            WHERE pt.strategy_id = :sid AND pt.status = 'OPEN'
            ORDER BY pt.id
            """
        )
        with self.engine.connect() as conn:
            rows = conn.execute(sql, {"sid": strategy_id}).fetchall()
        return [_row_to_position(r) for r in rows]

    def update_position(
        self,
        trade_id: int,
        *,
        peak_pnl_pct: float | None = None,
        trough_pnl_pct: float | None = None,
        remaining_fraction: float | None = None,
        tp_rungs_hit: int | None = None,
        trailing_armed: bool | None = None,
        filled_exit_value: float | None = None,
        filled_exit_fraction: float | None = None,
        last_tick_at: datetime | None = None,
    ) -> None:
        updates: dict[str, Any] = {"tid": trade_id}
        sets: list[str] = []
        if peak_pnl_pct is not None:
            sets.append("peak_pnl_pct = :peak")
            updates["peak"] = peak_pnl_pct
        if trough_pnl_pct is not None:
            sets.append("trough_pnl_pct = :trough")
            updates["trough"] = trough_pnl_pct
        if remaining_fraction is not None:
            sets.append("remaining_fraction = :rem")
            updates["rem"] = remaining_fraction
        if tp_rungs_hit is not None:
            sets.append("tp_rungs_hit = :rungs")
            updates["rungs"] = tp_rungs_hit
        if trailing_armed is not None:
            sets.append("trailing_armed = :armed")
            updates["armed"] = trailing_armed
        if filled_exit_value is not None:
            sets.append("filled_exit_value = :fv")
            updates["fv"] = filled_exit_value
        if filled_exit_fraction is not None:
            sets.append("filled_exit_fraction = :ff")
            updates["ff"] = filled_exit_fraction
        if last_tick_at is not None:
            sets.append("last_tick_at = :tick")
            updates["tick"] = last_tick_at

        if not sets:
            return

        sql = text(
            "UPDATE solana_exit_state SET "
            + ", ".join(sets)
            + " WHERE trade_id = :tid"
        )
        with self.engine.begin() as conn:
            conn.execute(sql, updates)

    # ------------------------------------------------------------------
    # Exit event log — immutable
    # ------------------------------------------------------------------
    def record_event(
        self,
        *,
        trade_id: int,
        event_type: str,
        rung_index: int | None,
        fraction: float,
        price: float,
        pnl_pct: float,
        peak_pnl_pct: float,
        policy_variant: str,
        source_type: str,
        reason: str,
    ) -> int:
        sql = text(
            """
            INSERT INTO solana_exit_events
                (trade_id, event_type, rung_index, fraction, price,
                 pnl_pct, peak_pnl_pct, policy_variant, source_type, reason)
            VALUES
                (:tid, :et, :ri, :f, :p, :pnl, :peak, :v, :s, :r)
            RETURNING id
            """
        )
        with self.engine.begin() as conn:
            row = conn.execute(
                sql,
                {
                    "tid": trade_id,
                    "et": event_type,
                    "ri": rung_index,
                    "f": fraction,
                    "p": price,
                    "pnl": pnl_pct,
                    "peak": peak_pnl_pct,
                    "v": policy_variant,
                    "s": source_type,
                    "r": reason,
                },
            ).fetchone()
        return int(row[0]) if row else 0

    def list_events(self, trade_id: int) -> list[dict[str, Any]]:
        """Return every event for ``trade_id``, oldest first."""
        sql = text(
            "SELECT id, event_type, rung_index, fraction, price, "
            "pnl_pct, peak_pnl_pct, policy_variant, source_type, "
            "reason, created_at "
            "FROM solana_exit_events WHERE trade_id = :tid "
            "ORDER BY id ASC"
        )
        with self.engine.connect() as conn:
            rows = conn.execute(sql, {"tid": trade_id}).fetchall()
        return [
            {
                "id": r[0],
                "event_type": r[1],
                "rung_index": r[2],
                "fraction": float(r[3]),
                "price": float(r[4]),
                "pnl_pct": float(r[5]),
                "peak_pnl_pct": float(r[6]),
                "policy_variant": r[7],
                "source_type": r[8],
                "reason": r[9],
                "created_at": r[10],
            }
            for r in rows
        ]

    # ------------------------------------------------------------------
    # Learner variant stats
    # ------------------------------------------------------------------
    def get_variant_stats(
        self,
        variant_id: str,
        source_type: str = SOURCE_UNKNOWN,
    ) -> VariantStatsRow | None:
        sql = text(
            "SELECT variant_id, source_type, n_samples, reward_mean, "
            "reward_m2, total_pnl_pct, wins, losses, last_reward "
            "FROM solana_policy_variants "
            "WHERE variant_id = :v AND source_type = :s"
        )
        with self.engine.connect() as conn:
            row = conn.execute(
                sql, {"v": variant_id, "s": source_type}
            ).fetchone()
        if row is None:
            return None
        return VariantStatsRow(
            variant_id=row[0],
            source_type=row[1],
            n_samples=int(row[2] or 0),
            reward_mean=float(row[3] or 0.0),
            reward_m2=float(row[4] or 0.0),
            total_pnl_pct=float(row[5] or 0.0),
            wins=int(row[6] or 0),
            losses=int(row[7] or 0),
            last_reward=float(row[8]) if row[8] is not None else None,
        )

    def upsert_variant(
        self,
        variant_id: str,
        source_type: str,
        params: dict[str, Any] | None = None,
    ) -> None:
        params_json = json.dumps(params or {})
        sql = text(
            """
            INSERT INTO solana_policy_variants
                (variant_id, source_type, params, updated_at)
            VALUES
                (:v, :s, CAST(:p AS JSONB), NOW())
            ON CONFLICT (variant_id, source_type) DO UPDATE
                SET params = EXCLUDED.params,
                    updated_at = NOW()
            """
        )
        with self.engine.begin() as conn:
            conn.execute(sql, {"v": variant_id, "s": source_type, "p": params_json})

    def update_variant_stats(
        self,
        variant_id: str,
        source_type: str,
        new_reward: float,
        pnl_pct: float,
    ) -> None:
        """Apply one online Welford update to the variant's stats.

        The DB read+update runs inside a single BEGIN block so concurrent
        learners don't race. (Single-writer assumption matches the
        existing signal_executor loop.)
        """
        with self.engine.begin() as conn:
            row = conn.execute(
                text(
                    "SELECT n_samples, reward_mean, reward_m2, "
                    "total_pnl_pct, wins, losses "
                    "FROM solana_policy_variants "
                    "WHERE variant_id = :v AND source_type = :s "
                    "FOR UPDATE"
                ),
                {"v": variant_id, "s": source_type},
            ).fetchone()

            if row is None:
                # Auto-create the row so callers don't have to upsert first.
                conn.execute(
                    text(
                        "INSERT INTO solana_policy_variants "
                        "(variant_id, source_type) VALUES (:v, :s) "
                        "ON CONFLICT DO NOTHING"
                    ),
                    {"v": variant_id, "s": source_type},
                )
                n, mean, m2, total_pnl, wins, losses = 0, 0.0, 0.0, 0.0, 0, 0
            else:
                n = int(row[0] or 0)
                mean = float(row[1] or 0.0)
                m2 = float(row[2] or 0.0)
                total_pnl = float(row[3] or 0.0)
                wins = int(row[4] or 0)
                losses = int(row[5] or 0)

            n += 1
            delta = new_reward - mean
            mean += delta / n
            delta2 = new_reward - mean
            m2 += delta * delta2
            total_pnl += pnl_pct
            if pnl_pct > 0:
                wins += 1
            else:
                losses += 1

            conn.execute(
                text(
                    "UPDATE solana_policy_variants SET "
                    "n_samples = :n, reward_mean = :m, reward_m2 = :m2, "
                    "total_pnl_pct = :tp, wins = :w, losses = :l, "
                    "last_reward = :lr, updated_at = NOW() "
                    "WHERE variant_id = :v AND source_type = :s"
                ),
                {
                    "n": n,
                    "m": mean,
                    "m2": m2,
                    "tp": total_pnl,
                    "w": wins,
                    "l": losses,
                    "lr": new_reward,
                    "v": variant_id,
                    "s": source_type,
                },
            )

        log.debug(
            "Variant update {v}/{s}: n={n} mean={m:.4f} reward={r:.4f}",
            v=variant_id, s=source_type, n=n, m=mean, r=new_reward,
        )


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def _row_to_position(row: Any) -> PositionStateRow:
    """Translate a raw row tuple into a :class:`PositionStateRow`.

    Rows carry both ``entry_date`` (a Date) and ``created_at`` (a
    Timestamp). We prefer ``created_at`` because it has sub-second
    precision for the max-hold timer.
    """
    entry_time = row[5]
    if entry_time is None:
        entry_time = _coerce_date_to_datetime(row[4])
    return PositionStateRow(
        trade_id=int(row[0]),
        ticker=row[1],
        direction=row[2],
        entry_price=float(row[3]),
        entry_time=entry_time,
        peak_pnl_pct=float(row[6] or 0.0),
        trough_pnl_pct=float(row[7] or 0.0),
        remaining_fraction=float(row[8] or 0.0),
        tp_rungs_hit=int(row[9] or 0),
        trailing_armed=bool(row[10]),
        filled_exit_value=float(row[11] or 0.0),
        filled_exit_fraction=float(row[12] or 0.0),
        policy_variant=row[13],
        source_type=row[14] or SOURCE_UNKNOWN,
    )


def _coerce_date_to_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.combine(value, datetime.min.time(), tzinfo=timezone.utc)
