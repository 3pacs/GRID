"""GRID Signal Adapter — Cross-Reference (Lie Detector). Divergence signals."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from intelligence.adapters.base import BaseAdapter, clamp, sid
from intelligence.signal_registry import RegisteredSignal, SignalType

_SOURCE_MODULE = "cross_reference"
_VALID_HOURS = 24.0
_REFRESH_HOURS = 24.0
_DIRECTIONAL_Z = 2.0
_MIN_Z = 0.5


class CrossReferenceAdapter(BaseAdapter):
    SOURCE_MODULE = _SOURCE_MODULE
    REFRESH_HOURS = _REFRESH_HOURS
    LOG_NAME = "cross_reference_adapter"

    def _build_signals(self, engine: Engine, now: datetime) -> list[RegisteredSignal]:
        vu = now + timedelta(hours=_VALID_HOURS)
        lb = now - timedelta(hours=26)
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT name, category, official_source, physical_source,
                       divergence_zscore, assessment, implication, confidence, checked_at
                FROM cross_reference_checks WHERE checked_at >= :lb
                ORDER BY ABS(divergence_zscore) DESC NULLS LAST
            """), {"lb": lb}).fetchall()
        signals: list[RegisteredSignal] = []
        for name, cat, off_src, phys_src, div_z, assess, impl, conf, checked_at in rows:
            if div_z is None:
                continue
            z = float(div_z)
            az = abs(z)
            cf = clamp(float(conf or 0.5))
            meta: dict[str, Any] = {"name": name, "category": cat, "assessment": assess, "implication": impl}
            if az >= _MIN_Z:
                signals.append(RegisteredSignal(
                    signal_id=sid(_SOURCE_MODULE, "mag", name, str(now.date())),
                    source_module=_SOURCE_MODULE, signal_type=SignalType.MAGNITUDE,
                    ticker=None, direction="neutral", value=round(az, 3), z_score=round(z, 3),
                    confidence=cf, valid_from=now, valid_until=vu, freshness_hours=0.0,
                    metadata=meta, provenance=f"cross_reference:{cat}:{name}",
                ))
            if az >= _DIRECTIONAL_Z:
                d = "bullish" if z > 0 else "bearish"
                signals.append(RegisteredSignal(
                    signal_id=sid(_SOURCE_MODULE, "dir", name, str(now.date())),
                    source_module=_SOURCE_MODULE, signal_type=SignalType.DIRECTIONAL,
                    ticker=None, direction=d, value=round(z, 3), z_score=round(z, 3),
                    confidence=clamp(cf + 0.1), valid_from=now, valid_until=vu,
                    freshness_hours=0.0, metadata={**meta, "divergence_class": assess},
                    provenance=f"cross_reference:divergence:{cat}:{name}",
                ))
        return signals
