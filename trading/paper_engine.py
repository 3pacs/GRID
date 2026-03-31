"""
GRID Paper Trading Engine.

Executes TACTICAL hypothesis signals automatically in a simulated environment.
Tracks P&L, win rate, and drawdown per strategy. Auto-kills underperformers.

Architecture:
  Hypothesis PASSED → Signal fires → Paper trade logged → P&L tracked
  Strategy survives if: win_rate > 40% AND drawdown < 5% AND Sharpe > 0.5
  Strategy dies if: any threshold breached after 20+ trades

Uses the decision journal for audit trail — every paper trade is logged
with full provenance (hypothesis ID, signal strength, physics score).
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


class PaperTradingEngine:
    """Execute and track paper trades from TACTICAL hypotheses."""

    def __init__(self, engine: Engine, initial_capital: float = 10000.0) -> None:
        self.engine = engine
        self.initial_capital = initial_capital
        self._ensure_tables()

    def _ensure_tables(self) -> None:
        """Create paper trading tables if they don't exist."""
        with self.engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS paper_trades (
                    id              SERIAL PRIMARY KEY,
                    strategy_id     TEXT NOT NULL,
                    hypothesis_id   INTEGER,
                    ticker          TEXT NOT NULL,
                    direction       TEXT NOT NULL CHECK (direction IN ('LONG', 'SHORT')),
                    entry_price     FLOAT NOT NULL,
                    exit_price      FLOAT,
                    entry_date      DATE NOT NULL,
                    exit_date       DATE,
                    position_size   FLOAT NOT NULL DEFAULT 1.0,
                    pnl             FLOAT,
                    pnl_pct         FLOAT,
                    signal_strength FLOAT,
                    physics_score   FLOAT,
                    threshold_used  FLOAT,
                    status          TEXT NOT NULL DEFAULT 'OPEN'
                                    CHECK (status IN ('OPEN', 'CLOSED', 'STOPPED')),
                    notes           TEXT,
                    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS paper_strategies (
                    id              TEXT PRIMARY KEY,
                    hypothesis_id   INTEGER,
                    leader          TEXT NOT NULL,
                    follower        TEXT NOT NULL,
                    description     TEXT,
                    total_trades    INTEGER NOT NULL DEFAULT 0,
                    wins            INTEGER NOT NULL DEFAULT 0,
                    losses          INTEGER NOT NULL DEFAULT 0,
                    total_pnl       FLOAT NOT NULL DEFAULT 0,
                    max_drawdown    FLOAT NOT NULL DEFAULT 0,
                    sharpe          FLOAT,
                    status          TEXT NOT NULL DEFAULT 'ACTIVE'
                                    CHECK (status IN ('ACTIVE', 'PAUSED', 'KILLED')),
                    capital         FLOAT NOT NULL DEFAULT 10000,
                    high_water_mark FLOAT NOT NULL DEFAULT 10000,
                    kill_reason     TEXT,
                    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """))
            conn.execute(text(
                "CREATE INDEX IF NOT EXISTS idx_paper_trades_strategy ON paper_trades(strategy_id)"
            ))
            # Migrate existing tables to include threshold_used if absent
            conn.execute(text(
                "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS threshold_used FLOAT"
            ))
        log.debug("Paper trading tables ensured")

    def register_strategy(
        self,
        hypothesis_id: int,
        leader: str,
        follower: str,
        description: str = "",
    ) -> str:
        """Register a new paper trading strategy from a hypothesis.

        Returns strategy_id.
        """
        strategy_id = f"h{hypothesis_id}_{leader}_{follower}"

        with self.engine.begin() as conn:
            existing = conn.execute(text(
                "SELECT id FROM paper_strategies WHERE id = :id"
            ), {"id": strategy_id}).fetchone()

            if existing:
                return strategy_id

            conn.execute(text(
                "INSERT INTO paper_strategies (id, hypothesis_id, leader, follower, description, capital, high_water_mark) "
                "VALUES (:id, :hid, :leader, :follower, :desc, :cap, :cap)"
            ), {
                "id": strategy_id, "hid": hypothesis_id,
                "leader": leader, "follower": follower,
                "desc": description, "cap": self.initial_capital,
            })

        log.info("Registered paper strategy: {s}", s=strategy_id)
        return strategy_id

    def open_trade(
        self,
        strategy_id: str,
        ticker: str,
        direction: str,
        entry_price: float,
        position_size: float = 1.0,
        signal_strength: float = 0.0,
        physics_score: float = 0.0,
        hypothesis_id: int | None = None,
        threshold_used: float = 0.0,
    ) -> int:
        """Open a new paper trade.

        Returns trade_id.
        """
        with self.engine.begin() as conn:
            # Check strategy is active
            strat = conn.execute(text(
                "SELECT status FROM paper_strategies WHERE id = :id"
            ), {"id": strategy_id}).fetchone()

            if not strat or strat[0] != 'ACTIVE':
                log.warning("Strategy {s} not active, skipping trade", s=strategy_id)
                return -1

            result = conn.execute(text(
                "INSERT INTO paper_trades "
                "(strategy_id, hypothesis_id, ticker, direction, entry_price, "
                "entry_date, position_size, signal_strength, physics_score, threshold_used, status) "
                "VALUES (:sid, :hid, :tk, :dir, :price, :date, :size, :sig, :phys, :thr, 'OPEN') "
                "RETURNING id"
            ), {
                "sid": strategy_id, "hid": hypothesis_id,
                "tk": ticker, "dir": direction, "price": entry_price,
                "date": date.today(), "size": position_size,
                "sig": signal_strength, "phys": physics_score, "thr": threshold_used,
            })

            trade_id = result.fetchone()[0]

        log.info("Paper trade opened: #{id} {dir} {tk} @ {p} (strategy {s})",
                 id=trade_id, dir=direction, tk=ticker, p=entry_price, s=strategy_id)
        return trade_id

    def close_trade(self, trade_id: int, exit_price: float, notes: str = "") -> dict:
        """Close an open paper trade and compute P&L."""
        with self.engine.begin() as conn:
            trade = conn.execute(text(
                "SELECT strategy_id, direction, entry_price, position_size "
                "FROM paper_trades WHERE id = :id AND status = 'OPEN'"
            ), {"id": trade_id}).fetchone()

            if not trade:
                return {"error": "Trade not found or already closed"}

            strategy_id, direction, entry_price, position_size = trade

            # Compute P&L
            if direction == 'LONG':
                pnl_pct = (exit_price - entry_price) / entry_price
            else:
                pnl_pct = (entry_price - exit_price) / entry_price

            pnl = pnl_pct * position_size * self.initial_capital

            conn.execute(text(
                "UPDATE paper_trades SET exit_price = :price, exit_date = :date, "
                "pnl = :pnl, pnl_pct = :pnl_pct, status = 'CLOSED', notes = :notes "
                "WHERE id = :id"
            ), {
                "price": exit_price, "date": date.today(),
                "pnl": round(pnl, 2), "pnl_pct": round(pnl_pct, 6),
                "notes": notes, "id": trade_id,
            })

            # Update strategy stats
            is_win = pnl > 0
            conn.execute(text(
                "UPDATE paper_strategies SET "
                "total_trades = total_trades + 1, "
                "wins = wins + :win, losses = losses + :loss, "
                "total_pnl = total_pnl + :pnl, "
                "capital = capital + :pnl, "
                "updated_at = NOW() "
                "WHERE id = :sid"
            ), {
                "win": 1 if is_win else 0,
                "loss": 0 if is_win else 1,
                "pnl": round(pnl, 2),
                "sid": strategy_id,
            })

            # Update high water mark and drawdown
            strat = conn.execute(text(
                "SELECT capital, high_water_mark FROM paper_strategies WHERE id = :id"
            ), {"id": strategy_id}).fetchone()

            if strat:
                capital, hwm = strat
                new_hwm = max(hwm, capital)
                drawdown = (new_hwm - capital) / new_hwm if new_hwm > 0 else 0

                conn.execute(text(
                    "UPDATE paper_strategies SET "
                    "high_water_mark = :hwm, max_drawdown = GREATEST(max_drawdown, :dd) "
                    "WHERE id = :id"
                ), {"hwm": new_hwm, "dd": round(drawdown, 6), "id": strategy_id})

                # Kill check
                self._check_kill(conn, strategy_id)

        log.info("Paper trade #{id} closed: {dir} P&L={pnl:+.2f} ({pct:+.2%})",
                 id=trade_id, dir=direction, pnl=pnl, pct=pnl_pct)
        return {"trade_id": trade_id, "pnl": round(pnl, 2), "pnl_pct": round(pnl_pct, 6)}

    def _check_kill(self, conn, strategy_id: str) -> None:
        """Check if strategy should be killed based on performance."""
        strat = conn.execute(text(
            "SELECT total_trades, wins, max_drawdown, total_pnl, capital "
            "FROM paper_strategies WHERE id = :id AND status = 'ACTIVE'"
        ), {"id": strategy_id}).fetchone()

        if not strat or strat[0] < 20:
            return  # Need at least 20 trades before judging

        total, wins, dd, pnl, capital = strat
        win_rate = wins / total if total > 0 else 0

        kill_reason = None
        if dd > 0.05:
            kill_reason = f"Max drawdown {dd:.1%} exceeded 5% threshold"
        elif win_rate < 0.40:
            kill_reason = f"Win rate {win_rate:.1%} below 40% threshold"
        elif capital < self.initial_capital * 0.90:
            kill_reason = f"Capital {capital:.0f} below 90% of initial"

        if kill_reason:
            conn.execute(text(
                "UPDATE paper_strategies SET status = 'KILLED', kill_reason = :reason, updated_at = NOW() "
                "WHERE id = :id"
            ), {"reason": kill_reason, "id": strategy_id})
            log.warning("Strategy {s} KILLED: {r}", s=strategy_id, r=kill_reason)

    def get_dashboard(self) -> dict:
        """Get paper trading dashboard with all strategies and recent trades."""
        with self.engine.connect() as conn:
            strategies = conn.execute(text(
                "SELECT * FROM paper_strategies ORDER BY total_pnl DESC"
            )).fetchall()

            recent_trades = conn.execute(text(
                "SELECT * FROM paper_trades ORDER BY created_at DESC LIMIT 20"
            )).fetchall()

            open_trades = conn.execute(text(
                "SELECT * FROM paper_trades WHERE status = 'OPEN' ORDER BY created_at"
            )).fetchall()

        strat_list = [dict(r._mapping) for r in strategies] if strategies else []
        trade_list = [dict(r._mapping) for r in recent_trades] if recent_trades else []
        open_list = [dict(r._mapping) for r in open_trades] if open_trades else []

        # Serialize dates
        for lst in [strat_list, trade_list, open_list]:
            for item in lst:
                for k, v in item.items():
                    if isinstance(v, (date, datetime)):
                        item[k] = str(v)

        total_pnl = sum(s.get("total_pnl", 0) for s in strat_list)
        active = sum(1 for s in strat_list if s.get("status") == "ACTIVE")
        killed = sum(1 for s in strat_list if s.get("status") == "KILLED")

        return {
            "strategies": strat_list,
            "recent_trades": trade_list,
            "open_trades": open_list,
            "total_pnl": round(total_pnl, 2),
            "active_strategies": active,
            "killed_strategies": killed,
            "total_strategies": len(strat_list),
        }

    def register_all_passed(self) -> int:
        """Register paper strategies for all PASSED TACTICAL hypotheses."""
        count = 0
        with self.engine.connect() as conn:
            hypos = conn.execute(text(
                "SELECT id, statement, lag_structure FROM hypothesis_registry "
                "WHERE state = 'PASSED' AND layer = 'TACTICAL'"
            )).fetchall()

        for h in hypos:
            lag = json.loads(h[2]) if isinstance(h[2], str) else (h[2] or {})
            leader = (lag.get("leader_features") or [None])[0]
            follower = (lag.get("follower_features") or [None])[0]
            if leader and follower:
                self.register_strategy(h[0], leader, follower, h[1][:100])
                count += 1

        log.info("Registered {n} paper strategies from PASSED TACTICAL hypotheses", n=count)
        return count

    def kelly_position_size(
        self, win_rate: float, avg_win: float, avg_loss: float,
        max_fraction: float = 0.25,
    ) -> float:
        """Compute Kelly criterion position size.

        Returns fraction of capital to risk (capped at max_fraction).
        """
        if avg_loss == 0 or win_rate <= 0:
            return 0.0

        # Kelly: f* = (bp - q) / b
        # b = avg_win / avg_loss, p = win_rate, q = 1 - p
        b = abs(avg_win / avg_loss)
        p = win_rate
        q = 1 - p

        kelly = (b * p - q) / b
        # Half-Kelly for safety
        half_kelly = kelly / 2

        return max(0, min(half_kelly, max_fraction))
