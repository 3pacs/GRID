"""GRID Signal Adapter — Cross-Reference (Lie Detector). Divergence signals."""

from __future__ import annotations
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from intelligence.signal_registry import RegisteredSignal, SignalType

_SOURCE_MODULE = "cross_reference"
_VALID_HOURS = 24.0
_REFRESH_HOURS = 24.0
_DIRECTIONAL_Z = 2.0
_MIN_Z = 0.5

def _signal_id(*parts): return hashlib.sha1(":".join(parts).encode()).hexdigest()[:16]
def _now_utc(): return datetime.now(timezone.utc)
def _clamp(v, lo=0.0, hi=1.0): return max(lo, min(hi, v))

class CrossReferenceAdapter:
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
            log.error("cross_reference_adapter: {e}", e=e)
            return []

    def _extract(self, engine, now, vu):
        lb = now - timedelta(hours=26)
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT name, category, official_source, physical_source,
                       divergence_zscore, assessment, implication, confidence, checked_at
                FROM cross_reference_checks WHERE checked_at >= :lb
                ORDER BY ABS(divergence_zscore) DESC NULLS LAST
            """), {"lb": lb}).fetchall()
        signals = []
        for name, cat, off_src, phys_src, div_z, assess, impl, conf, checked_at in rows:
            if div_z is None: continue
            z = float(div_z)
            az = abs(z)
            cf = _clamp(float(conf or 0.5))
            meta = {"name": name, "category": cat, "assessment": assess, "implication": impl}
            if az >= _MIN_Z:
                signals.append(RegisteredSignal(
                    signal_id=_signal_id(_SOURCE_MODULE, "mag", name, str(now.date())),
                    source_module=_SOURCE_MODULE, signal_type=SignalType.MAGNITUDE,
                    ticker=None, direction="neutral", value=round(az, 3), z_score=round(z, 3),
                    confidence=cf, valid_from=now, valid_until=vu, freshness_hours=0.0,
                    metadata=meta, provenance=f"cross_reference:{cat}:{name}",
                ))
            if az >= _DIRECTIONAL_Z:
                d = "bullish" if z > 0 else "bearish"
                signals.append(RegisteredSignal(
                    signal_id=_signal_id(_SOURCE_MODULE, "dir", name, str(now.date())),
                    source_module=_SOURCE_MODULE, signal_type=SignalType.DIRECTIONAL,
                    ticker=None, direction=d, value=round(z, 3), z_score=round(z, 3),
                    confidence=_clamp(cf + 0.1), valid_from=now, valid_until=vu,
                    freshness_hours=0.0, metadata={**meta, "divergence_class": assess},
                    provenance=f"cross_reference:divergence:{cat}:{name}",
                ))
        log.info("cross_reference_adapter: {n} signals", n=len(signals))
        return signals
