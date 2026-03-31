"""GRID Signal Adapter — Thesis Tracker. Latest market thesis direction + accuracy."""

from __future__ import annotations
import hashlib
from datetime import datetime, timedelta, timezone
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from intelligence.signal_registry import RegisteredSignal, SignalType

_SRC = "thesis_tracker"
_REFRESH = 4.0

def _sid(*p): return hashlib.sha1(":".join(p).encode()).hexdigest()[:16]
def _now(): return datetime.now(timezone.utc)
def _clamp(v, lo=0.0, hi=1.0): return max(lo, min(hi, v))

class ThesisTrackerAdapter:
    @property
    def source_module(self): return _SRC
    @property
    def refresh_interval_hours(self): return _REFRESH

    def extract_signals(self, engine: Engine) -> list[RegisteredSignal]:
        now = _now()
        vu = now + timedelta(hours=4)
        signals = []
        try:
            with engine.connect() as conn:
                row = conn.execute(text("""
                    SELECT overall_direction, conviction, key_drivers, narrative, timestamp
                    FROM thesis_snapshots ORDER BY timestamp DESC LIMIT 1
                """)).fetchone()
            if row:
                direction, conviction, drivers, narrative, ts = row
                d = (direction or "neutral").lower()
                if d not in ("bullish", "bearish"): d = "neutral"
                conv = _clamp(float(conviction or 50) / 100)
                signals.append(RegisteredSignal(
                    signal_id=_sid(_SRC, "current", str(now.date())),
                    source_module=_SRC, signal_type=SignalType.DIRECTIONAL,
                    ticker=None, direction=d, value=round(float(conviction or 50), 4),
                    z_score=None, confidence=round(conv, 4),
                    valid_from=now, valid_until=vu, freshness_hours=0.0,
                    metadata={"conviction": float(conviction or 50),
                              "key_drivers": drivers if isinstance(drivers, list) else [],
                              "narrative_preview": str(narrative)[:200] if narrative else ""},
                    provenance="thesis_tracker:current_snapshot",
                ))

            # Accuracy from postmortems (correct = direction matched)
            with engine.connect() as conn:
                acc = conn.execute(text("""
                    SELECT COUNT(*) FILTER (WHERE thesis_direction = actual_direction) as correct,
                           COUNT(*) FILTER (WHERE thesis_direction != actual_direction AND actual_direction IS NOT NULL) as wrong,
                           COUNT(*) as total
                    FROM thesis_postmortems WHERE generated_at >= :lb
                """), {"lb": now - timedelta(days=90)}).fetchone()
            if acc and int(acc[2] or 0) >= 5:
                total = int(acc[2])
                hr = int(acc[0] or 0) / total
                signals.append(RegisteredSignal(
                    signal_id=_sid(_SRC, "accuracy", str(now.date())),
                    source_module=_SRC, signal_type=SignalType.MAGNITUDE,
                    ticker=None, direction="neutral", value=round(hr, 4),
                    z_score=round((hr - 0.5) / 0.15, 2), confidence=_clamp(min(total / 20, 1.0)),
                    valid_from=now, valid_until=vu, freshness_hours=0.0,
                    metadata={"correct": int(acc[0] or 0), "wrong": int(acc[1] or 0), "total": total},
                    provenance="thesis_tracker:accuracy",
                ))
        except Exception as e:
            log.error("thesis_tracker_adapter: {e}", e=e)
        log.info("thesis_tracker_adapter: {n} signals", n=len(signals))
        return signals
