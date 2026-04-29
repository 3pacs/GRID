"""GRID Signal Adapter — Lever Pullers. Per-ticker directional signals from actor events."""

from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import text
from sqlalchemy.engine import Engine

from intelligence.adapters.base import BaseAdapter, clamp, sid
from intelligence.signal_registry import RegisteredSignal, SignalType

_SOURCE_MODULE = "lever_pullers"
_REFRESH_HOURS = 6.0
_VALID_DAYS = {"fed": 30, "congress": 14, "insider": 7, "institutional": 14, "dealer": 7}
_BULLISH = frozenset({"BUY", "SPEECH_DOVISH", "POSITION_INCREASE", "COVER", "LONG"})
_BEARISH = frozenset({"SELL", "SPEECH_HAWKISH", "POSITION_DECREASE", "SHORT"})


class LeverPullersAdapter(BaseAdapter):
    SOURCE_MODULE = _SOURCE_MODULE
    REFRESH_HOURS = _REFRESH_HOURS
    LOG_NAME = "lever_pullers_adapter"

    def _build_signals(self, engine: Engine, now: datetime) -> list[RegisteredSignal]:
        lb = (now - timedelta(days=30)).date()
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT ss.ticker, ss.source_type, ss.source_id, ss.signal_type AS action,
                       ss.signal_date, ss.trust_score, lp.influence_rank, lp.category, lp.name
                FROM signal_sources ss
                LEFT JOIN lever_pullers lp ON lp.source_type=ss.source_type AND lp.source_id=ss.source_id
                WHERE ss.signal_date >= :lb AND ss.outcome IN ('PENDING','CORRECT') AND ss.ticker IS NOT NULL
                ORDER BY COALESCE(lp.influence_rank,0) * COALESCE(ss.trust_score,0.5) DESC
            """), {"lb": lb}).fetchall()
        signals: list[RegisteredSignal] = []
        for ticker, src_type, src_id, action, sig_date, trust, influence, cat, name in rows:
            a = (action or "").upper()
            if a in _BULLISH:
                d = "bullish"
            elif a in _BEARISH:
                d = "bearish"
            else:
                continue
            inf = clamp(float(influence or 0.3))
            if inf < 0.15:
                continue
            tf = clamp(float(trust or 0.5))
            c = (cat or src_type or "unknown").lower()
            vd = _VALID_DAYS.get(c, 14)
            vu = now + timedelta(days=vd)
            signals.append(RegisteredSignal(
                signal_id=sid(_SOURCE_MODULE, ticker, src_type, src_id, a, str(sig_date)),
                source_module=_SOURCE_MODULE, signal_type=SignalType.DIRECTIONAL,
                ticker=ticker, direction=d, value=round(inf, 4),
                z_score=round((inf - 0.5) / 0.17, 2), confidence=round(tf, 4),
                valid_from=now, valid_until=vu, freshness_hours=0.0,
                metadata={"source_type": src_type, "category": c, "action": a, "puller_name": name},
                provenance=f"lever_pullers:{c}:{src_id}:{ticker}",
            ))
        return signals
