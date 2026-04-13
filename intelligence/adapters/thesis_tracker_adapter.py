"""GRID Signal Adapter — Thesis Tracker. Latest market thesis direction + accuracy."""

from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import text
from sqlalchemy.engine import Engine

from intelligence.adapters.base import BaseAdapter, clamp, sid
from intelligence.signal_registry import RegisteredSignal, SignalType

_SRC = "thesis_tracker"
_REFRESH = 4.0


class ThesisTrackerAdapter(BaseAdapter):
    SOURCE_MODULE = _SRC
    REFRESH_HOURS = _REFRESH
    LOG_NAME = "thesis_tracker_adapter"

    def _build_signals(self, engine: Engine, now: datetime) -> list[RegisteredSignal]:
        vu = now + timedelta(hours=4)
        signals: list[RegisteredSignal] = []
        with engine.connect() as conn:
            row = conn.execute(text("""
                SELECT overall_direction, conviction, key_drivers, narrative, timestamp
                FROM thesis_snapshots ORDER BY timestamp DESC LIMIT 1
            """)).fetchone()
        if row:
            direction, conviction, drivers, narrative, ts = row
            d = (direction or "neutral").lower()
            if d not in ("bullish", "bearish"):
                d = "neutral"
            conv = clamp(float(conviction or 50) / 100)
            signals.append(RegisteredSignal(
                signal_id=sid(_SRC, "current", str(now.date())),
                source_module=_SRC, signal_type=SignalType.DIRECTIONAL,
                ticker=None, direction=d, value=round(float(conviction or 50), 4),
                z_score=None, confidence=round(conv, 4),
                valid_from=now, valid_until=vu, freshness_hours=0.0,
                metadata={"conviction": float(conviction or 50),
                          "key_drivers": drivers if isinstance(drivers, list) else [],
                          "narrative_preview": str(narrative)[:200] if narrative else ""},
                provenance="thesis_tracker:current_snapshot",
            ))

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
                signal_id=sid(_SRC, "accuracy", str(now.date())),
                source_module=_SRC, signal_type=SignalType.MAGNITUDE,
                ticker=None, direction="neutral", value=round(hr, 4),
                z_score=round((hr - 0.5) / 0.15, 2), confidence=clamp(min(total / 20, 1.0)),
                valid_from=now, valid_until=vu, freshness_hours=0.0,
                metadata={"correct": int(acc[0] or 0), "wrong": int(acc[1] or 0), "total": total},
                provenance="thesis_tracker:accuracy",
            ))
        return signals
