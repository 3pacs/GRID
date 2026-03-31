"""GRID Signal Adapter — Lever Pullers. Per-ticker directional signals from actor events."""

from __future__ import annotations
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from intelligence.signal_registry import RegisteredSignal, SignalType

_SOURCE_MODULE = "lever_pullers"
_REFRESH_HOURS = 6.0
_VALID_DAYS = {"fed": 30, "congress": 14, "insider": 7, "institutional": 14, "dealer": 7}
_BULLISH = frozenset({"BUY", "SPEECH_DOVISH", "POSITION_INCREASE", "COVER", "LONG"})
_BEARISH = frozenset({"SELL", "SPEECH_HAWKISH", "POSITION_DECREASE", "SHORT"})

def _signal_id(*p): return hashlib.sha1(":".join(p).encode()).hexdigest()[:16]
def _now_utc(): return datetime.now(timezone.utc)
def _clamp(v, lo=0.0, hi=1.0): return max(lo, min(hi, v))

class LeverPullersAdapter:
    @property
    def source_module(self): return _SOURCE_MODULE
    @property
    def refresh_interval_hours(self): return _REFRESH_HOURS

    def extract_signals(self, engine: Engine) -> list[RegisteredSignal]:
        now = _now_utc()
        try:
            return self._extract(engine, now)
        except Exception as e:
            log.error("lever_pullers_adapter: {e}", e=e)
            return []

    def _extract(self, engine, now):
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
        signals = []
        for ticker, src_type, src_id, action, sig_date, trust, influence, cat, name in rows:
            a = (action or "").upper()
            if a in _BULLISH: d = "bullish"
            elif a in _BEARISH: d = "bearish"
            else: continue
            inf = _clamp(float(influence or 0.3))
            if inf < 0.15: continue
            tf = _clamp(float(trust or 0.5))
            c = (cat or src_type or "unknown").lower()
            vd = _VALID_DAYS.get(c, 14)
            vu = now + timedelta(days=vd)
            signals.append(RegisteredSignal(
                signal_id=_signal_id(_SOURCE_MODULE, ticker, src_type, src_id, a, str(sig_date)),
                source_module=_SOURCE_MODULE, signal_type=SignalType.DIRECTIONAL,
                ticker=ticker, direction=d, value=round(inf, 4),
                z_score=round((inf-0.5)/0.17, 2), confidence=round(tf, 4),
                valid_from=now, valid_until=vu, freshness_hours=0.0,
                metadata={"source_type": src_type, "category": c, "action": a, "puller_name": name},
                provenance=f"lever_pullers:{c}:{src_id}:{ticker}",
            ))
        log.info("lever_pullers_adapter: {n} signals", n=len(signals))
        return signals
