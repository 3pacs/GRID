"""GRID Signal Adapter — Earnings Intel. Upcoming earnings + historical surprise signals."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import text
from sqlalchemy.engine import Engine

from intelligence.adapters.base import BaseAdapter, clamp, sid
from intelligence.signal_registry import RegisteredSignal, SignalType

_SRC = "earnings_intel"
_REFRESH = 12.0


class EarningsAdapter(BaseAdapter):
    SOURCE_MODULE = _SRC
    REFRESH_HOURS = _REFRESH
    LOG_NAME = "earnings_adapter"

    def _build_signals(self, engine: Engine, now: datetime) -> list[RegisteredSignal]:
        signals: list[RegisteredSignal] = []
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT ticker, earnings_date, eps_estimate, eps_actual
                FROM earnings_calendar
                WHERE earnings_date BETWEEN :now AND :future
                ORDER BY earnings_date
            """), {"now": now.date(), "future": (now + timedelta(days=14)).date()}).fetchall()
        for ticker, report_date, est_eps, act_eps in rows:
            if not ticker:
                continue
            rd = (
                datetime(report_date.year, report_date.month, report_date.day, tzinfo=timezone.utc)
                if hasattr(report_date, "year")
                else now
            )
            signals.append(RegisteredSignal(
                signal_id=sid(_SRC, "upcoming", ticker, str(report_date)),
                source_module=_SRC, signal_type=SignalType.PATTERN,
                ticker=ticker, direction="neutral", value=1.0,
                z_score=None, confidence=0.6,
                valid_from=now, valid_until=rd + timedelta(days=1),
                freshness_hours=0.0,
                metadata={"report_date": str(report_date), "estimated_eps": float(est_eps or 0)},
                provenance=f"earnings_calendar:upcoming:{ticker}",
            ))

        with engine.connect() as conn:
            recent = conn.execute(text("""
                SELECT ticker, earnings_date, eps_estimate, eps_actual
                FROM earnings_calendar
                WHERE earnings_date BETWEEN :past AND :now AND eps_actual IS NOT NULL
                ORDER BY earnings_date DESC
            """), {"past": (now - timedelta(days=7)).date(), "now": now.date()}).fetchall()
        for ticker, report_date, est, act in recent:
            if not ticker or est is None or act is None:
                continue
            surprise = float(act) - float(est)
            d = "bullish" if surprise > 0 else "bearish"
            pct = abs(surprise / float(est)) if float(est) != 0 else 0
            signals.append(RegisteredSignal(
                signal_id=sid(_SRC, "surprise", ticker, str(report_date)),
                source_module=_SRC, signal_type=SignalType.DIRECTIONAL,
                ticker=ticker, direction=d, value=round(surprise, 4),
                z_score=round(pct * 10, 2), confidence=clamp(0.5 + pct),
                valid_from=now, valid_until=now + timedelta(days=7),
                freshness_hours=0.0,
                metadata={"surprise": round(surprise, 4), "surprise_pct": round(pct, 4)},
                provenance=f"earnings_calendar:surprise:{ticker}",
            ))
        return signals
