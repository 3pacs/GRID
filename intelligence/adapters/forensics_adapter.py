"""GRID Signal Adapter — Forensic Analyzer. Warning count + directional signals per ticker."""

from __future__ import annotations
import hashlib, math
from datetime import datetime, timedelta, timezone
from typing import Any
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from intelligence.signal_registry import RegisteredSignal, SignalType

_SOURCE_MODULE = "forensics"
_VALID_HOURS = 12.0
_REFRESH_HOURS = 12.0
_DIR_Z_THRESHOLD = 1.5

def _signal_id(*p): return hashlib.sha1(":".join(p).encode()).hexdigest()[:16]
def _now_utc(): return datetime.now(timezone.utc)
def _clamp(v, lo=0.0, hi=1.0): return max(lo, min(hi, v))

def _z_score(val, vals):
    if len(vals) < 2: return 0.0
    m = sum(vals)/len(vals)
    v = sum((x-m)**2 for x in vals)/len(vals)
    return (val - m) / (math.sqrt(v) if v > 0 else 1.0)

class ForensicsAdapter:
    @property
    def source_module(self): return _SOURCE_MODULE
    @property
    def refresh_interval_hours(self): return _REFRESH_HOURS

    def extract_signals(self, engine: Engine) -> list[RegisteredSignal]:
        now = _now_utc()
        vu = now + timedelta(hours=_VALID_HOURS)
        try:
            return self._extract(engine, now, vu)
        except Exception as e:
            log.error("forensics_adapter: {e}", e=e)
            return []

    def _extract(self, engine, now, vu):
        lb = (now - timedelta(days=30)).date()
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT DISTINCT ON (ticker) ticker, move_pct, warning_signals, confidence
                FROM forensic_reports WHERE move_date >= :lb ORDER BY ticker, move_date DESC
            """), {"lb": lb}).fetchall()
        signals = []
        for ticker, move_pct, warnings, conf in rows:
            w = int(warnings or 0)
            cf = _clamp(float(conf or 0.5))
            # Get history for z-score
            hlb = (now - timedelta(days=180)).date()
            with engine.connect() as conn:
                hist = [int(r[0]) for r in conn.execute(text(
                    "SELECT warning_signals FROM forensic_reports WHERE ticker=:t AND move_date>=:lb AND warning_signals IS NOT NULL"
                ), {"t": ticker, "lb": hlb}).fetchall()]
            z = round(_z_score(float(w), [float(h) for h in hist]), 2) if len(hist) >= 4 else 0.0
            signals.append(RegisteredSignal(
                signal_id=_signal_id(_SOURCE_MODULE, "mag", ticker, str(now.date())),
                source_module=_SOURCE_MODULE, signal_type=SignalType.MAGNITUDE,
                ticker=ticker, direction="neutral", value=float(w), z_score=z,
                confidence=cf, valid_from=now, valid_until=vu, freshness_hours=0.0,
                metadata={"warning_signals": w, "move_pct": round(float(move_pct or 0), 4)},
                provenance=f"forensics:{ticker}",
            ))
            if z >= _DIR_Z_THRESHOLD:
                d = "bullish" if float(move_pct or 0) >= 0 else "bearish"
                signals.append(RegisteredSignal(
                    signal_id=_signal_id(_SOURCE_MODULE, "dir", ticker, str(now.date())),
                    source_module=_SOURCE_MODULE, signal_type=SignalType.DIRECTIONAL,
                    ticker=ticker, direction=d, value=float(w), z_score=z,
                    confidence=_clamp(cf+0.05), valid_from=now, valid_until=vu,
                    freshness_hours=0.0, metadata={"trigger": "high_warning_count"},
                    provenance=f"forensics:directional:{ticker}",
                ))
        log.info("forensics_adapter: {n} signals", n=len(signals))
        return signals
