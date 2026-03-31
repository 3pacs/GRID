"""GRID Signal Adapter — Pattern Engine. Active recognized patterns with hit rates."""

from __future__ import annotations
import hashlib
from datetime import datetime, timedelta, timezone
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from intelligence.signal_registry import RegisteredSignal, SignalType

_SRC = "pattern_engine"
_REFRESH = 4.0

def _sid(*p): return hashlib.sha1(":".join(p).encode()).hexdigest()[:16]
def _now(): return datetime.now(timezone.utc)
def _clamp(v, lo=0.0, hi=1.0): return max(lo, min(hi, v))

class PatternAdapter:
    @property
    def source_module(self): return _SRC
    @property
    def refresh_interval_hours(self): return _REFRESH

    def extract_signals(self, engine: Engine) -> list[RegisteredSignal]:
        now = _now()
        signals = []
        try:
            with engine.connect() as conn:
                rows = conn.execute(text("""
                    SELECT id, description, hit_rate, occurrences,
                           avg_lead_time_hours, confidence, tickers_seen, updated_at
                    FROM event_patterns
                    WHERE updated_at >= :lb AND occurrences >= 3 AND actionable = TRUE
                    ORDER BY hit_rate * confidence DESC NULLS LAST LIMIT 100
                """), {"lb": now - timedelta(days=30)}).fetchall()

            for pat_id, desc, hit_rate, occurrences, avg_lead_hours, conf, tickers_seen, updated in rows:
                pat_name = desc or pat_id
                ticker = None  # market-wide patterns
                avg_lead = float(avg_lead_hours or 168) / 24  # hours to days
                direction = "neutral"  # patterns don't have inherent direction
                hr = float(hit_rate or 0.5)
                occ = int(occurrences or 0)
                lead = float(avg_lead_days or 7)
                cf = _clamp(float(conf or 0.5))
                d = (direction or "neutral").lower()
                if d not in ("bullish", "bearish"): d = "neutral"

                vu = now + timedelta(days=max(1, min(lead, 30)))

                signals.append(RegisteredSignal(
                    signal_id=_sid(_SRC, pat_name or "unknown", ticker or "market", str(now.date())),
                    source_module=_SRC, signal_type=SignalType.PATTERN,
                    ticker=ticker, direction=d, value=round(hr, 4),
                    z_score=round((hr - 0.5) / 0.15, 2) if hr != 0.5 else 0.0,
                    confidence=round(cf * min(occ / 10, 1.0), 4),  # confidence scales with sample size
                    valid_from=now, valid_until=vu, freshness_hours=0.0,
                    metadata={"pattern_name": pat_name, "hit_rate": round(hr, 4),
                              "occurrences": occ, "avg_lead_days": round(lead, 1)},
                    provenance=f"event_patterns:{pat_name}:{ticker}",
                ))
        except Exception as e:
            log.error("pattern_adapter: {e}", e=e)
        log.info("pattern_adapter: {n} signals", n=len(signals))
        return signals
