"""GRID Signal Adapter — Feature Store bridge. Z-score signals from resolved_series."""

from __future__ import annotations
import hashlib, math
from datetime import datetime, timedelta, timezone
from typing import Any
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from intelligence.signal_registry import RegisteredSignal, SignalType

_SOURCE_MODULE_BASE = "feature"
_VALID_HOURS = 1.0
_REFRESH_HOURS = 1.0
_LOOKBACK_DAYS = 30
_MIN_OBS = 5
_MIN_Z = 0.5

def _signal_id(*p): return hashlib.sha1(":".join(p).encode()).hexdigest()[:16]
def _now_utc(): return datetime.now(timezone.utc)
def _clamp(v, lo=0.0, hi=1.0): return max(lo, min(hi, v))

def _z(vals):
    if len(vals) < 2: return 0.0
    m = sum(vals)/len(vals)
    v = sum((x-m)**2 for x in vals)/len(vals)
    return (vals[0] - m) / (math.sqrt(v) if v > 0 else 1.0)

class FeatureAdapter:
    @property
    def source_module(self): return _SOURCE_MODULE_BASE
    @property
    def refresh_interval_hours(self): return _REFRESH_HOURS

    def extract_signals(self, engine: Engine) -> list[RegisteredSignal]:
        now = _now_utc()
        vu = now + timedelta(hours=_VALID_HOURS)
        try:
            return self._extract(engine, now, vu)
        except Exception as e:
            log.error("feature_adapter: {e}", e=e)
            return []

    def _extract(self, engine, now, vu):
        lb = (now - timedelta(days=_LOOKBACK_DAYS)).date()
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT fr.id, fr.name, fr.family, rs.value, rs.obs_date
                FROM resolved_series rs JOIN feature_registry fr ON fr.id=rs.feature_id
                WHERE fr.model_eligible=TRUE AND rs.obs_date >= :lb AND rs.value IS NOT NULL
                ORDER BY fr.name, rs.obs_date DESC
            """), {"lb": lb}).fetchall()
        features = {}
        for fid, fname, family, val, od in rows:
            if fname not in features:
                features[fname] = {"id": fid, "family": (family or "unknown").lower(), "vals": [], "date": od}
            try:
                features[fname]["vals"].append(float(val))
            except (TypeError, ValueError):
                pass
        signals = []
        for fname, f in features.items():
            if len(f["vals"]) < _MIN_OBS: continue
            z = round(_z(f["vals"]), 3)
            if abs(z) < _MIN_Z: continue
            d = "bullish" if z > 0 else "bearish"
            conf = _clamp(0.5 + min(abs(z), 3.0) / 6.0)
            sm = f"{_SOURCE_MODULE_BASE}:{f['family']}"
            signals.append(RegisteredSignal(
                signal_id=_signal_id(sm, fname, str(now.date())),
                source_module=sm, signal_type=SignalType.DIRECTIONAL,
                ticker=None, direction=d, value=round(f["vals"][0], 6), z_score=z,
                confidence=round(conf, 4), valid_from=now, valid_until=vu,
                freshness_hours=0.0,
                metadata={"feature_id": f["id"], "feature_name": fname, "family": f["family"], "obs_count": len(f["vals"])},
                provenance=f"feature_registry:{fname}",
            ))
        log.info("feature_adapter: {n} signals from {f} features", n=len(signals), f=len(features))
        return signals
