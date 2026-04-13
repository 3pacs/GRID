"""GRID Signal Adapter — Trust Scorer. Convergence + per-source trust signals."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from intelligence.adapters.base import BaseAdapter, clamp, now_utc, sid
from intelligence.signal_registry import RegisteredSignal, SignalType

_SOURCE_MODULE = "trust_scorer"
_VALID_HOURS = 4.0
_REFRESH_HOURS = 4.0
_MIN_CONVERGENCE_SOURCES = 3
_MIN_TOTAL_SIGNALS = 5


def _to_aware(val: Any) -> datetime:
    if isinstance(val, datetime):
        return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
    return now_utc()


class TrustScorerAdapter(BaseAdapter):
    SOURCE_MODULE = _SOURCE_MODULE
    REFRESH_HOURS = _REFRESH_HOURS
    LOG_NAME = "trust_scorer_adapter"

    def _build_signals(self, engine: Engine, now: datetime) -> list[RegisteredSignal]:
        vu = now + timedelta(hours=_VALID_HOURS)
        signals: list[RegisteredSignal] = []
        try:
            signals.extend(self._convergence(engine, now, vu))
        except Exception as e:
            log.error("trust_scorer_adapter convergence: {e}", e=e)
        try:
            signals.extend(self._source_trust(engine, now, vu))
        except Exception as e:
            log.error("trust_scorer_adapter source_trust: {e}", e=e)
        return signals

    def _convergence(self, engine: Engine, now: datetime, vu: datetime) -> list[RegisteredSignal]:
        lb = (now - timedelta(days=14)).date()
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT ticker, signal_type, source_type, AVG(trust_score) AS avg_trust
                FROM signal_sources WHERE signal_date >= :lb AND outcome IN ('PENDING','CORRECT')
                GROUP BY ticker, signal_type, source_type
            """), {"lb": lb}).fetchall()
        bucket: dict = defaultdict(dict)
        for ticker, sig_type, src_type, avg_trust in rows:
            bucket[(ticker, sig_type)][src_type] = clamp(float(avg_trust or 0.5))
        signals: list[RegisteredSignal] = []
        for (ticker, sig_type), sources in bucket.items():
            if len(sources) < _MIN_CONVERGENCE_SOURCES:
                continue
            conf = clamp(sum(sources.values()) / len(sources))
            direction = "bullish" if sig_type == "BUY" else "bearish"
            signals.append(RegisteredSignal(
                signal_id=sid(_SOURCE_MODULE, "conv", ticker, sig_type, str(now.date())),
                source_module=_SOURCE_MODULE, signal_type=SignalType.DIRECTIONAL,
                ticker=ticker, direction=direction, value=float(len(sources)),
                z_score=round(min(len(sources) - 2, 4) * 0.75, 2), confidence=conf,
                valid_from=now, valid_until=vu, freshness_hours=0.0,
                metadata={"convergence_type": sig_type, "source_count": len(sources)},
                provenance=f"trust_scorer.convergence:{sig_type}:{ticker}",
            ))
        return signals

    def _source_trust(self, engine: Engine, now: datetime, vu: datetime) -> list[RegisteredSignal]:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT source_type, source_id, AVG(trust_score) AS trust,
                       SUM(hit_count) AS hits, SUM(miss_count) AS misses, COUNT(*) AS total
                FROM signal_sources WHERE outcome IN ('CORRECT','WRONG','PENDING')
                GROUP BY source_type, source_id HAVING COUNT(*) >= :min
            """), {"min": _MIN_TOTAL_SIGNALS}).fetchall()
        signals: list[RegisteredSignal] = []
        for src_type, src_id, trust, hits, misses, total in rows:
            tf = clamp(float(trust or 0.5))
            if tf < 0.45:
                continue
            signals.append(RegisteredSignal(
                signal_id=sid(_SOURCE_MODULE, "trust", src_type, src_id, str(now.date())),
                source_module=_SOURCE_MODULE, signal_type=SignalType.MAGNITUDE,
                ticker=None, direction="neutral", value=round(tf, 4),
                z_score=round((tf - 0.5) / 0.15, 2), confidence=tf,
                valid_from=now, valid_until=vu, freshness_hours=0.0,
                metadata={"source_type": src_type, "source_id": src_id, "hits": int(hits or 0), "misses": int(misses or 0)},
                provenance=f"trust_scorer.source:{src_type}:{src_id}",
            ))
        return signals
