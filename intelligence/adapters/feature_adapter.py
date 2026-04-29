"""GRID Signal Adapter — Feature Store bridge. Z-score signals from resolved_series."""

from __future__ import annotations

import math
from datetime import datetime, timedelta

from sqlalchemy import text
from sqlalchemy.engine import Engine
from loguru import logger as log

from intelligence.adapters.base import BaseAdapter, clamp, sid
from intelligence.signal_registry import RegisteredSignal, SignalType

_SOURCE_MODULE_BASE = "feature"
_VALID_HOURS = 1.0
_REFRESH_HOURS = 1.0
_LOOKBACK_DAYS = 30
_MIN_OBS = 5
_MIN_Z = 0.5


def _z(vals: list[float]) -> float:
    if len(vals) < 2:
        return 0.0
    m = sum(vals) / len(vals)
    v = sum((x - m) ** 2 for x in vals) / len(vals)
    return (vals[0] - m) / (math.sqrt(v) if v > 0 else 1.0)


class FeatureAdapter(BaseAdapter):
    SOURCE_MODULE = _SOURCE_MODULE_BASE
    REFRESH_HOURS = _REFRESH_HOURS
    LOG_NAME = "feature_adapter"

    def extract_signals(self, engine: Engine) -> list[RegisteredSignal]:
        # Override to preserve original multi-arg log line ("n signals from f features").
        from intelligence.adapters.base import now_utc
        now = now_utc()
        try:
            signals, feature_count = self._build_signals_and_count(engine, now)
        except Exception as e:
            log.error("feature_adapter: {e}", e=e)
            return []
        log.info("feature_adapter: {n} signals from {f} features", n=len(signals), f=feature_count)
        return signals

    def _build_signals_and_count(
        self, engine: Engine, now: datetime
    ) -> tuple[list[RegisteredSignal], int]:
        vu = now + timedelta(hours=_VALID_HOURS)
        lb = (now - timedelta(days=_LOOKBACK_DAYS)).date()
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT fr.id, fr.name, fr.family, rs.value, rs.obs_date
                FROM resolved_series rs JOIN feature_registry fr ON fr.id=rs.feature_id
                WHERE fr.model_eligible=TRUE AND rs.obs_date >= :lb AND rs.value IS NOT NULL
                ORDER BY fr.name, rs.obs_date DESC
            """), {"lb": lb}).fetchall()
        features: dict[str, dict] = {}
        for fid, fname, family, val, od in rows:
            if fname not in features:
                features[fname] = {"id": fid, "family": (family or "unknown").lower(), "vals": [], "date": od}
            try:
                features[fname]["vals"].append(float(val))
            except (TypeError, ValueError):
                pass
        signals: list[RegisteredSignal] = []
        for fname, f in features.items():
            if len(f["vals"]) < _MIN_OBS:
                continue
            z = round(_z(f["vals"]), 3)
            if abs(z) < _MIN_Z:
                continue
            # 2026-04-28: feature-family-aware direction mapping.
            # Old code used `bullish if z>0 else bearish` for EVERY feature,
            # regardless of family. That treats mean_reversion/volatility/value
            # families like momentum, which inverts their predictive sign.
            # Postmortem evidence: feature:* signals dominated wrong-prediction
            # lists. Route by family — momentum keeps the sign, contrarian
            # families flip it, ambiguous families publish NEUTRAL so the
            # signal stays in the payload (for trace_evolver to learn from)
            # but doesn't add directional noise.
            family = (f["family"] or "unknown").lower()
            if family in ("momentum", "trend"):
                d = "bullish" if z > 0 else "bearish"
            elif family in ("mean_reversion", "volatility", "vol"):
                # Contrarian: high z = overbought = bearish next-period
                d = "bearish" if z > 0 else "bullish"
            else:
                # Unknown / value / sentiment / breadth — direction relationship
                # is not categorically established. Publish NEUTRAL.
                d = "neutral"
            conf = clamp(0.5 + min(abs(z), 3.0) / 6.0)
            sm = f"{_SOURCE_MODULE_BASE}:{f['family']}"
            signals.append(RegisteredSignal(
                signal_id=sid(sm, fname, str(now.date())),
                source_module=sm, signal_type=SignalType.DIRECTIONAL,
                ticker=None, direction=d, value=round(f["vals"][0], 6), z_score=z,
                confidence=round(conf, 4), valid_from=now, valid_until=vu,
                freshness_hours=0.0,
                metadata={"feature_id": f["id"], "feature_name": fname, "family": f["family"], "obs_count": len(f["vals"])},
                provenance=f"feature_registry:{fname}",
            ))
        return signals, len(features)
