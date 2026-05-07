"""
Per-day trade caps for Solana trading.

A deliberately boring module — just a couple of SQL queries against the
existing ``paper_trades`` table to enforce:

  * max notional USD opened per UTC day
  * max number of trades opened per UTC day
  * max notional USD per individual mint per day

The caller passes a :class:`LimitConfig` with thresholds and the desired
``trade_usd`` for the *new* trade; :meth:`DailyLimits.check` returns a
:class:`LimitDecision` with either ``passed=True`` or a list of reasons.

This runs against the same ``paper_trades`` rows used by the existing
signal_executor, so paper-mode usage is automatically constrained — no
separate table, no schema migration.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class LimitConfig:
    """Caps enforced by :class:`DailyLimits`.

    Set any field to 0 to disable that specific cap.
    """

    max_daily_usd: float = 200.0
    max_daily_trades: int = 20
    max_per_mint_daily_usd: float = 75.0
    capital_per_trade_usd: float = 50.0


@dataclass(frozen=True)
class LimitDecision:
    """Outcome of :meth:`DailyLimits.check`."""

    passed: bool
    reasons: tuple[str, ...]
    daily_usd_used: float
    daily_trades_used: int
    mint_usd_used: float

    @property
    def summary(self) -> str:
        if self.passed:
            return (
                f"OK (daily_usd={self.daily_usd_used:.2f}, "
                f"trades={self.daily_trades_used})"
            )
        return "BLOCKED: " + "; ".join(self.reasons)


class DailyLimits:
    """Query :class:`sqlalchemy.Engine` for today's open/closed trade usage."""

    def __init__(
        self,
        engine: Engine,
        strategy_id: str,
        config: LimitConfig | None = None,
    ) -> None:
        self.engine = engine
        self.strategy_id = strategy_id
        self.config = config or LimitConfig()

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------
    def _today(self) -> date:
        return datetime.now(timezone.utc).date()

    def _fetch_usage(self, today: date, mint: str | None) -> tuple[float, int, float]:
        """Return (daily_usd, daily_count, mint_usd)."""
        with self.engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT "
                    "  COALESCE(SUM(entry_price * position_size), 0) AS usd, "
                    "  COUNT(*) AS n "
                    "FROM paper_trades "
                    "WHERE strategy_id = :sid AND entry_date = :today"
                ),
                {"sid": self.strategy_id, "today": today},
            ).fetchone()
            daily_usd = float(row[0] or 0.0) if row else 0.0
            daily_count = int(row[1] or 0) if row else 0

            mint_usd = 0.0
            if mint:
                mint_row = conn.execute(
                    text(
                        "SELECT COALESCE(SUM(entry_price * position_size), 0) "
                        "FROM paper_trades "
                        "WHERE strategy_id = :sid "
                        "AND entry_date = :today "
                        "AND ticker = :mint"
                    ),
                    {"sid": self.strategy_id, "today": today, "mint": mint},
                ).fetchone()
                mint_usd = float(mint_row[0] or 0.0) if mint_row else 0.0

        return daily_usd, daily_count, mint_usd

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def check(
        self,
        trade_usd: float,
        mint: str | None = None,
    ) -> LimitDecision:
        """Check whether a new trade would breach any daily cap.

        Args:
            trade_usd: notional size of the new trade in USD.
            mint: optional mint/ticker for the per-mint cap.

        Returns:
            A :class:`LimitDecision` — inspect ``passed`` and ``reasons``.
            ``check`` never raises on DB errors; it returns ``passed=False``
            with a diagnostic reason so callers can fail-closed.
        """
        if trade_usd < 0:
            return LimitDecision(
                passed=False,
                reasons=("trade_usd is negative",),
                daily_usd_used=0.0,
                daily_trades_used=0,
                mint_usd_used=0.0,
            )

        today = self._today()
        try:
            daily_usd, daily_count, mint_usd = self._fetch_usage(today, mint)
        except Exception as exc:  # noqa: BLE001 — fail-closed on DB error
            log.error("DailyLimits.check DB error: {e}", e=str(exc))
            return LimitDecision(
                passed=False,
                reasons=(f"DB error: {exc}",),
                daily_usd_used=0.0,
                daily_trades_used=0,
                mint_usd_used=0.0,
            )

        reasons: list[str] = []
        cfg = self.config

        if cfg.max_daily_usd > 0 and daily_usd + trade_usd > cfg.max_daily_usd:
            reasons.append(
                f"daily USD cap: {daily_usd:.2f}+{trade_usd:.2f} "
                f"> {cfg.max_daily_usd:.2f}"
            )
        if cfg.max_daily_trades > 0 and daily_count + 1 > cfg.max_daily_trades:
            reasons.append(
                f"daily trade count: {daily_count + 1} > {cfg.max_daily_trades}"
            )
        if (
            cfg.max_per_mint_daily_usd > 0
            and mint is not None
            and mint_usd + trade_usd > cfg.max_per_mint_daily_usd
        ):
            reasons.append(
                f"per-mint USD cap for {mint}: "
                f"{mint_usd:.2f}+{trade_usd:.2f} > {cfg.max_per_mint_daily_usd:.2f}"
            )

        return LimitDecision(
            passed=not reasons,
            reasons=tuple(reasons),
            daily_usd_used=daily_usd,
            daily_trades_used=daily_count,
            mint_usd_used=mint_usd,
        )
