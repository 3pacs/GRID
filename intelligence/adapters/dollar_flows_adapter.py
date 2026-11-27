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
        """Magnitude signals from the rows written in the seven days up to ``now``.

        The window is closed at ``now``: a signal is stamped ``valid_from=now``
        and must not carry a row written after that instant. ``created_at`` is
        the time of the table's last rebuild of that date, so for a ``now`` in
        the past this is only what was last written inside the window, not
        what the table held then; the registry calls it with the current time.

        No signal is built from a 13F row. A 13F row is a reported position
        (``direction`` "observed", or a legacy unqualified "inflow"), stored
        with the position's marked value, filed up to 45 days after the
        quarter it describes: it is not a dollar flow, and its sum is a
        portfolio total, not a flow magnitude. The directional readers of the
        table already leave 13F out (``OBSERVED_13F_DIRECTION`` in
        ``intelligence.dollar_flows``); this one summed it, and because the
        groups are cut to the 200 largest, 13F totals also pushed real flows
        out. Rows with no ticker made one "market" signal that every ticker's
        model received.
        """
        vu = now + timedelta(hours=4)
        signals: list[RegisteredSignal] = []
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT ticker, source_type, direction,
                       SUM(amount_usd) as total_usd, COUNT(*) as flow_count,
                       0.5 as avg_conf  -- confidence is TEXT, use default
                FROM dollar_flows
                WHERE created_at >= :lb AND created_at <= :now
                  AND source_type <> '13f'
                GROUP BY ticker, source_type, direction
                HAVING SUM(amount_usd) > 100000
                ORDER BY SUM(amount_usd) DESC LIMIT 200
            """), {"lb": now - timedelta(days=7), "now": now}).fetchall()

        # 2026-04-28: inflow/outflow → bullish/bearish is context-blind.
        # Institutional inflow into a stock down 50% is value-buying (bullish
        # long-term, often bearish short-term). Outflow from an extended winner
        # can be profit-taking or distribution. Direction depends on price
        # context that isn't captured here. Publish as NEUTRAL — the magnitude
        # row below carries the dollar amount, and trace_evolver can learn
        # the conditional sign with regime + price context.
        for ticker, src_type, direction, total_usd, count, avg_conf in rows:
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
