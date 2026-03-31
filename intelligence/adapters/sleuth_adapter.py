"""GRID Signal Adapter — Sleuth. Active investigation leads as signals."""

from __future__ import annotations
import hashlib
from datetime import datetime, timedelta, timezone
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from intelligence.signal_registry import RegisteredSignal, SignalType

_SRC = "sleuth"
_REFRESH = 6.0

def _sid(*p): return hashlib.sha1(":".join(p).encode()).hexdigest()[:16]
def _now(): return datetime.now(timezone.utc)
def _clamp(v, lo=0.0, hi=1.0): return max(lo, min(hi, v))

class SleuthAdapter:
    @property
    def source_module(self): return _SRC
    @property
    def refresh_interval_hours(self): return _REFRESH

    def extract_signals(self, engine: Engine) -> list[RegisteredSignal]:
        now = _now()
        vu = now + timedelta(hours=12)
        signals = []
        try:
            with engine.connect() as conn:
                rows = conn.execute(text("""
                    SELECT id, category, priority, question, status, created_at
                    FROM investigation_leads
                    WHERE status IN ('new', 'open', 'investigating')
                      AND created_at >= :lb
                    ORDER BY priority DESC LIMIT 50
                """), {"lb": now - timedelta(days=14)}).fetchall()
            for lid, cat, priority, question, status, created in rows:
                cf = _clamp(float(priority or 0.5))
                signals.append(RegisteredSignal(
                    signal_id=_sid(_SRC, str(lid)[:16], str(now.date())),
                    source_module=_SRC, signal_type=SignalType.PATTERN,
                    ticker=None, direction="neutral", value=cf, z_score=None,
                    confidence=cf, valid_from=now, valid_until=vu, freshness_hours=0.0,
                    metadata={"lead_id": str(lid)[:16], "category": cat,
                              "question": str(question)[:200] if question else "", "status": status},
                    provenance=f"sleuth:lead:{cat}:{str(lid)[:8]}",
                ))
        except Exception as e:
            log.error("sleuth_adapter: {e}", e=e)
        log.info("sleuth_adapter: {n} signals", n=len(signals))
        return signals
