"""GRID Signal Adapter — Dollar Flows. Net flow direction + magnitude per sector."""

from __future__ import annotations
import hashlib
from datetime import datetime, timedelta, timezone
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from intelligence.signal_registry import RegisteredSignal, SignalType

_SRC = "dollar_flows"
_REFRESH = 4.0

def _sid(*p): return hashlib.sha1(":".join(p).encode()).hexdigest()[:16]
def _now(): return datetime.now(timezone.utc)
def _clamp(v, lo=0.0, hi=1.0): return max(lo, min(hi, v))

class DollarFlowsAdapter:
    @property
    def source_module(self): return _SRC
    @property
    def refresh_interval_hours(self): return _REFRESH

    def extract_signals(self, engine: Engine) -> list[RegisteredSignal]:
        now = _now()
        vu = now + timedelta(hours=4)
        signals = []
        try:
            with engine.connect() as conn:
                rows = conn.execute(text("""
                    SELECT ticker, source_type, direction,
                           SUM(amount_usd) as total_usd, COUNT(*) as flow_count,
                           0.5 as avg_conf  -- confidence is TEXT, use default
                    FROM dollar_flows
                    WHERE created_at >= :lb
                    GROUP BY ticker, source_type, direction
                    HAVING SUM(amount_usd) > 100000
                    ORDER BY SUM(amount_usd) DESC LIMIT 200
                """), {"lb": now - timedelta(days=7)}).fetchall()

            for ticker, src_type, direction, total_usd, count, avg_conf in rows:
                d = (direction or "neutral").lower()
                if d == "inflow": d = "bullish"
                elif d == "outflow": d = "bearish"
                elif d not in ("bullish", "bearish"): d = "neutral"

                usd = float(total_usd or 0)
                cf = _clamp(float(avg_conf or 0.5))

                # Magnitude signal: absolute dollar amount
                signals.append(RegisteredSignal(
                    signal_id=_sid(_SRC, "magnitude", ticker or "market", src_type or "", str(now.date())),
                    source_module=_SRC, signal_type=SignalType.MAGNITUDE,
                    ticker=ticker, direction="neutral", value=round(usd, 2),
                    z_score=None, confidence=cf,
                    valid_from=now, valid_until=vu, freshness_hours=0.0,
                    metadata={"source_type": src_type, "flow_count": int(count), "amount_usd": round(usd, 2)},
                    provenance=f"dollar_flows:{src_type}:{ticker}",
                ))
                # Directional signal
                if d != "neutral":
                    signals.append(RegisteredSignal(
                        signal_id=_sid(_SRC, "direction", ticker or "market", src_type or "", str(now.date())),
                        source_module=_SRC, signal_type=SignalType.DIRECTIONAL,
                        ticker=ticker, direction=d, value=round(usd, 2),
                        z_score=None, confidence=cf,
                        valid_from=now, valid_until=vu, freshness_hours=0.0,
                        metadata={"source_type": src_type, "amount_usd": round(usd, 2), "direction_raw": direction},
                        provenance=f"dollar_flows:direction:{src_type}:{ticker}",
                    ))
        except Exception as e:
            log.error("dollar_flows_adapter: {e}", e=e)
        log.info("dollar_flows_adapter: {n} signals", n=len(signals))
        return signals
