"""GRID Signal Adapter — Dollar Flows. Net flow direction + magnitude per sector."""

from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import text
from sqlalchemy.engine import Engine

from intelligence.adapters.base import BaseAdapter, clamp, sid
from intelligence.signal_registry import RegisteredSignal, SignalType

_SRC = "dollar_flows"
_REFRESH = 4.0


class DollarFlowsAdapter(BaseAdapter):
    SOURCE_MODULE = _SRC
    REFRESH_HOURS = _REFRESH
    LOG_NAME = "dollar_flows_adapter"

    def _build_signals(self, engine: Engine, now: datetime) -> list[RegisteredSignal]:
        vu = now + timedelta(hours=4)
        signals: list[RegisteredSignal] = []
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
            if d == "inflow":
                d = "bullish"
            elif d == "outflow":
                d = "bearish"
            elif d not in ("bullish", "bearish"):
                d = "neutral"

            usd = float(total_usd or 0)
            cf = clamp(float(avg_conf or 0.5))

            signals.append(RegisteredSignal(
                signal_id=sid(_SRC, "magnitude", ticker or "market", src_type or "", str(now.date())),
                source_module=_SRC, signal_type=SignalType.MAGNITUDE,
                ticker=ticker, direction="neutral", value=round(usd, 2),
                z_score=None, confidence=cf,
                valid_from=now, valid_until=vu, freshness_hours=0.0,
                metadata={"source_type": src_type, "flow_count": int(count), "amount_usd": round(usd, 2)},
                provenance=f"dollar_flows:{src_type}:{ticker}",
            ))
            if d != "neutral":
                signals.append(RegisteredSignal(
                    signal_id=sid(_SRC, "direction", ticker or "market", src_type or "", str(now.date())),
                    source_module=_SRC, signal_type=SignalType.DIRECTIONAL,
                    ticker=ticker, direction=d, value=round(usd, 2),
                    z_score=None, confidence=cf,
                    valid_from=now, valid_until=vu, freshness_hours=0.0,
                    metadata={"source_type": src_type, "amount_usd": round(usd, 2), "direction_raw": direction},
                    provenance=f"dollar_flows:direction:{src_type}:{ticker}",
                ))
        return signals
