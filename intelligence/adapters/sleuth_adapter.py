"""GRID Signal Adapter — Sleuth. Active investigation leads as signals."""

from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import text
from sqlalchemy.engine import Engine

from intelligence.adapters.base import BaseAdapter, clamp, sid
from intelligence.signal_registry import RegisteredSignal, SignalType

_SRC = "sleuth"
_REFRESH = 6.0


class SleuthAdapter(BaseAdapter):
    SOURCE_MODULE = _SRC
    REFRESH_HOURS = _REFRESH
    LOG_NAME = "sleuth_adapter"

    def _build_signals(self, engine: Engine, now: datetime) -> list[RegisteredSignal]:
        vu = now + timedelta(hours=12)
        signals: list[RegisteredSignal] = []
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT id, category, priority, question, status, created_at
                FROM investigation_leads
                WHERE status IN ('new', 'open', 'investigating')
                  AND created_at >= :lb
                ORDER BY priority DESC LIMIT 50
            """), {"lb": now - timedelta(days=14)}).fetchall()
        for lid, cat, priority, question, status, created in rows:
            cf = clamp(float(priority or 0.5))
            signals.append(RegisteredSignal(
                signal_id=sid(_SRC, str(lid)[:16], str(now.date())),
                source_module=_SRC, signal_type=SignalType.PATTERN,
                ticker=None, direction="neutral", value=cf, z_score=None,
                confidence=cf, valid_from=now, valid_until=vu, freshness_hours=0.0,
                metadata={"lead_id": str(lid)[:16], "category": cat,
                          "question": str(question)[:200] if question else "", "status": status},
                provenance=f"sleuth:lead:{cat}:{str(lid)[:8]}",
            ))
        return signals
