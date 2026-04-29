"""GRID Signal Adapter — Forensic Analyzer. Warning count + directional signals per ticker."""

from __future__ import annotations

import math
from datetime import datetime, timedelta

from sqlalchemy import text
from sqlalchemy.engine import Engine

from intelligence.adapters.base import BaseAdapter, clamp, sid
from intelligence.signal_registry import RegisteredSignal, SignalType

_SOURCE_MODULE = "forensics"
_VALID_HOURS = 12.0
_REFRESH_HOURS = 12.0
_DIR_Z_THRESHOLD = 1.5


def _z_score(val: float, vals: list[float]) -> float:
    if len(vals) < 2:
        return 0.0
    m = sum(vals) / len(vals)
    v = sum((x - m) ** 2 for x in vals) / len(vals)
    return (val - m) / (math.sqrt(v) if v > 0 else 1.0)


class ForensicsAdapter(BaseAdapter):
    SOURCE_MODULE = _SOURCE_MODULE
    REFRESH_HOURS = _REFRESH_HOURS
    LOG_NAME = "forensics_adapter"

    def _build_signals(self, engine: Engine, now: datetime) -> list[RegisteredSignal]:
        vu = now + timedelta(hours=_VALID_HOURS)
        lb = (now - timedelta(days=30)).date()
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT DISTINCT ON (ticker) ticker, move_pct, warning_signals, confidence
                FROM forensic_reports WHERE move_date >= :lb ORDER BY ticker, move_date DESC
            """), {"lb": lb}).fetchall()
        signals: list[RegisteredSignal] = []
        for ticker, move_pct, warnings, conf in rows:
            w = int(warnings or 0)
            cf = clamp(float(conf or 0.5))
            hlb = (now - timedelta(days=180)).date()
            with engine.connect() as conn:
                hist = [int(r[0]) for r in conn.execute(text(
                    "SELECT warning_signals FROM forensic_reports WHERE ticker=:t AND move_date>=:lb AND warning_signals IS NOT NULL"
                ), {"t": ticker, "lb": hlb}).fetchall()]
            z = round(_z_score(float(w), [float(h) for h in hist]), 2) if len(hist) >= 4 else 0.0
            signals.append(RegisteredSignal(
                signal_id=sid(_SOURCE_MODULE, "mag", ticker, str(now.date())),
                source_module=_SOURCE_MODULE, signal_type=SignalType.MAGNITUDE,
                ticker=ticker, direction="neutral", value=float(w), z_score=z,
                confidence=cf, valid_from=now, valid_until=vu, freshness_hours=0.0,
                metadata={"warning_signals": w, "move_pct": round(float(move_pct or 0), 4)},
                provenance=f"forensics:{ticker}",
            ))
            # 2026-04-28: warning_signals count is a RISK / VOLATILITY metric,
            # not a directional signal. Old code mapped it to bullish/bearish
            # using the BACKWARD-LOOKING move_pct, which is a leak: the
            # direction is just whatever the stock already did. Postmortem
            # evidence: forensics:directional appeared in wrong-prediction
            # clusters. Drop the directional emission entirely — the magnitude
            # signal above (forensics:mag) is preserved and that's what
            # trace_evolver should learn from.
            _ = z  # high warning count → trace_evolver sees it via the mag row
        return signals
