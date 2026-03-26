"""
GRID Multi-Wallet Manager (EXCH-04).

Manages multiple trading wallets across exchanges with independent capital
tracking, risk limits, and automatic kill switches.

Each wallet is an isolated capital pool:
  - Tracks P&L, high-water mark, drawdown independently
  - Auto-kills when drawdown exceeds configured limit
  - Supports ACTIVE / PAUSED / KILLED lifecycle
  - Provides aggregated dashboard across all wallets
"""

from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


class WalletManager:
    """Manage multiple trading wallets with independent risk controls."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self._ensure_tables()

    def _ensure_tables(self) -> None:
        """Create wallet tables if they don't exist."""
        with self.engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS trading_wallets (
                    id              TEXT PRIMARY KEY,
                    exchange        TEXT NOT NULL,
                    wallet_type     TEXT NOT NULL DEFAULT 'paper',
                    initial_capital FLOAT NOT NULL,
                    current_capital FLOAT NOT NULL,
                    high_water_mark FLOAT NOT NULL,
                    max_drawdown    FLOAT NOT NULL DEFAULT 0,
                    total_pnl       FLOAT NOT NULL DEFAULT 0,
                    total_trades    INTEGER NOT NULL DEFAULT 0,
                    win_count       INTEGER NOT NULL DEFAULT 0,
                    loss_count      INTEGER NOT NULL DEFAULT 0,
                    status          TEXT NOT NULL DEFAULT 'ACTIVE'
                                    CHECK (status IN ('ACTIVE', 'PAUSED', 'KILLED')),
                    risk_limit_pct  FLOAT NOT NULL DEFAULT 0.05,
                    max_drawdown_limit FLOAT NOT NULL DEFAULT 0.20,
                    kill_reason     TEXT,
                    metadata        JSONB DEFAULT '{}',
                    created_at      TIMESTAMPTZ DEFAULT NOW(),
                    updated_at      TIMESTAMPTZ DEFAULT NOW()
                )
            """))
        log.debug("Trading wallets table ensured")

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    def create_wallet(
        self,
        exchange: str,
        wallet_type: str,
        initial_capital: float,
        risk_limit_pct: float = 0.05,
        max_drawdown_limit: float = 0.20,
    ) -> str:
        """Create a new trading wallet. Returns wallet_id."""
        short_uuid = uuid.uuid4().hex[:8]
        wallet_id = f"{exchange}_{wallet_type}_{short_uuid}"

        with self.engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO trading_wallets "
                "(id, exchange, wallet_type, initial_capital, current_capital, "
                "high_water_mark, risk_limit_pct, max_drawdown_limit) "
                "VALUES (:id, :exchange, :wtype, :cap, :cap, :cap, :risk, :dd)"
            ), {
                "id": wallet_id,
                "exchange": exchange,
                "wtype": wallet_type,
                "cap": initial_capital,
                "risk": risk_limit_pct,
                "dd": max_drawdown_limit,
            })

        log.info("Created wallet {id} — {exch}/{wtype} capital={cap}",
                 id=wallet_id, exch=exchange, wtype=wallet_type, cap=initial_capital)
        return wallet_id

    def get_wallet(self, wallet_id: str) -> dict:
        """Get a single wallet with all fields."""
        with self.engine.connect() as conn:
            row = conn.execute(text(
                "SELECT * FROM trading_wallets WHERE id = :id"
            ), {"id": wallet_id}).fetchone()

        if not row:
            return {"error": f"Wallet {wallet_id} not found"}

        return self._row_to_dict(row)

    def get_all_wallets(
        self,
        exchange: str | None = None,
        status: str | None = None,
    ) -> list[dict]:
        """Get all wallets, optionally filtered by exchange and/or status."""
        query = "SELECT * FROM trading_wallets WHERE 1=1"
        params: dict[str, Any] = {}

        if exchange:
            query += " AND exchange = :exchange"
            params["exchange"] = exchange
        if status:
            query += " AND status = :status"
            params["status"] = status

        query += " ORDER BY created_at DESC"

        with self.engine.connect() as conn:
            rows = conn.execute(text(query), params).fetchall()

        return [self._row_to_dict(r) for r in rows]

    # ------------------------------------------------------------------
    # P&L and risk
    # ------------------------------------------------------------------

    def update_pnl(self, wallet_id: str, pnl: float, is_win: bool) -> dict:
        """Record a trade result: update capital, P&L, HWM, drawdown, counts."""
        with self.engine.begin() as conn:
            # Update capital and counters
            conn.execute(text(
                "UPDATE trading_wallets SET "
                "current_capital = current_capital + :pnl, "
                "total_pnl = total_pnl + :pnl, "
                "total_trades = total_trades + 1, "
                "win_count = win_count + :win, "
                "loss_count = loss_count + :loss, "
                "updated_at = NOW() "
                "WHERE id = :id"
            ), {
                "pnl": round(pnl, 2),
                "win": 1 if is_win else 0,
                "loss": 0 if is_win else 1,
                "id": wallet_id,
            })

            # Update high water mark and drawdown
            wallet = conn.execute(text(
                "SELECT current_capital, high_water_mark "
                "FROM trading_wallets WHERE id = :id"
            ), {"id": wallet_id}).fetchone()

            if not wallet:
                return {"error": f"Wallet {wallet_id} not found"}

            capital, hwm = wallet
            new_hwm = max(hwm, capital)
            drawdown = (new_hwm - capital) / new_hwm if new_hwm > 0 else 0

            conn.execute(text(
                "UPDATE trading_wallets SET "
                "high_water_mark = :hwm, "
                "max_drawdown = GREATEST(max_drawdown, :dd) "
                "WHERE id = :id"
            ), {"hwm": new_hwm, "dd": round(drawdown, 6), "id": wallet_id})

            # Risk check
            self._check_risk(conn, wallet_id)

        log.info("Wallet {id} P&L update: {pnl:+.2f} (win={w})",
                 id=wallet_id, pnl=pnl, w=is_win)
        return self.get_wallet(wallet_id)

    def _check_risk(self, conn, wallet_id: str) -> None:
        """Auto-kill wallet if drawdown exceeds limit."""
        wallet = conn.execute(text(
            "SELECT current_capital, high_water_mark, max_drawdown_limit, status "
            "FROM trading_wallets WHERE id = :id"
        ), {"id": wallet_id}).fetchone()

        if not wallet or wallet[3] != "ACTIVE":
            return

        capital, hwm, dd_limit, _ = wallet
        current_dd = (hwm - capital) / hwm if hwm > 0 else 0

        if current_dd > dd_limit:
            kill_reason = (
                f"Drawdown {current_dd:.1%} exceeded limit {dd_limit:.1%}"
            )
            conn.execute(text(
                "UPDATE trading_wallets SET "
                "status = 'KILLED', kill_reason = :reason, updated_at = NOW() "
                "WHERE id = :id"
            ), {"reason": kill_reason, "id": wallet_id})
            log.warning("Wallet {id} KILLED: {r}", id=wallet_id, r=kill_reason)

    def check_risk(self, wallet_id: str) -> dict:
        """Public risk check — returns current drawdown %, headroom, status."""
        with self.engine.connect() as conn:
            wallet = conn.execute(text(
                "SELECT current_capital, high_water_mark, max_drawdown_limit, "
                "max_drawdown, status "
                "FROM trading_wallets WHERE id = :id"
            ), {"id": wallet_id}).fetchone()

        if not wallet:
            return {"error": f"Wallet {wallet_id} not found"}

        capital, hwm, dd_limit, max_dd, status = wallet
        current_dd = (hwm - capital) / hwm if hwm > 0 else 0
        headroom = dd_limit - current_dd

        return {
            "wallet_id": wallet_id,
            "status": status,
            "current_drawdown_pct": round(current_dd, 6),
            "max_drawdown_pct": round(max_dd, 6),
            "max_drawdown_limit": dd_limit,
            "headroom_pct": round(headroom, 6),
            "capital": round(capital, 2),
            "high_water_mark": round(hwm, 2),
        }

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def kill_wallet(self, wallet_id: str, reason: str) -> dict:
        """Manually kill a wallet."""
        with self.engine.begin() as conn:
            conn.execute(text(
                "UPDATE trading_wallets SET "
                "status = 'KILLED', kill_reason = :reason, updated_at = NOW() "
                "WHERE id = :id"
            ), {"reason": reason, "id": wallet_id})

        log.warning("Wallet {id} manually killed: {r}", id=wallet_id, r=reason)
        return self.get_wallet(wallet_id)

    def pause_wallet(self, wallet_id: str) -> dict:
        """Pause an active wallet."""
        with self.engine.begin() as conn:
            conn.execute(text(
                "UPDATE trading_wallets SET "
                "status = 'PAUSED', updated_at = NOW() "
                "WHERE id = :id AND status = 'ACTIVE'"
            ), {"id": wallet_id})

        log.info("Wallet {id} paused", id=wallet_id)
        return self.get_wallet(wallet_id)

    def resume_wallet(self, wallet_id: str) -> dict:
        """Resume a paused wallet (only if PAUSED)."""
        with self.engine.begin() as conn:
            result = conn.execute(text(
                "UPDATE trading_wallets SET "
                "status = 'ACTIVE', updated_at = NOW() "
                "WHERE id = :id AND status = 'PAUSED'"
            ), {"id": wallet_id})

            if result.rowcount == 0:
                log.warning("Cannot resume wallet {id} — not in PAUSED state", id=wallet_id)

        return self.get_wallet(wallet_id)

    # ------------------------------------------------------------------
    # Dashboard
    # ------------------------------------------------------------------

    def get_dashboard(self) -> dict:
        """Aggregated dashboard across all wallets."""
        with self.engine.connect() as conn:
            wallets = conn.execute(text(
                "SELECT * FROM trading_wallets ORDER BY total_pnl DESC"
            )).fetchall()

        wallet_list = [self._row_to_dict(r) for r in wallets]

        if not wallet_list:
            return {
                "wallets": [],
                "total_capital": 0,
                "total_pnl": 0,
                "per_exchange": {},
                "active_count": 0,
                "paused_count": 0,
                "killed_count": 0,
                "best_wallet": None,
                "worst_wallet": None,
            }

        total_capital = sum(w["current_capital"] for w in wallet_list)
        total_pnl = sum(w["total_pnl"] for w in wallet_list)

        # Per-exchange breakdown
        per_exchange: dict[str, dict] = {}
        for w in wallet_list:
            exch = w["exchange"]
            if exch not in per_exchange:
                per_exchange[exch] = {"capital": 0, "pnl": 0, "count": 0}
            per_exchange[exch]["capital"] += w["current_capital"]
            per_exchange[exch]["pnl"] += w["total_pnl"]
            per_exchange[exch]["count"] += 1

        # Round exchange totals
        for v in per_exchange.values():
            v["capital"] = round(v["capital"], 2)
            v["pnl"] = round(v["pnl"], 2)

        active = sum(1 for w in wallet_list if w["status"] == "ACTIVE")
        paused = sum(1 for w in wallet_list if w["status"] == "PAUSED")
        killed = sum(1 for w in wallet_list if w["status"] == "KILLED")

        best = max(wallet_list, key=lambda w: w["total_pnl"])
        worst = min(wallet_list, key=lambda w: w["total_pnl"])

        return {
            "wallets": wallet_list,
            "total_capital": round(total_capital, 2),
            "total_pnl": round(total_pnl, 2),
            "per_exchange": per_exchange,
            "active_count": active,
            "paused_count": paused,
            "killed_count": killed,
            "best_wallet": {"id": best["id"], "pnl": best["total_pnl"]},
            "worst_wallet": {"id": worst["id"], "pnl": worst["total_pnl"]},
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _row_to_dict(row) -> dict:
        """Convert a SQLAlchemy row to a dict with serialized dates."""
        d = dict(row._mapping)
        for k, v in d.items():
            if isinstance(v, (date, datetime)):
                d[k] = str(v)
        return d
