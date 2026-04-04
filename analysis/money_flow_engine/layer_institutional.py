"""GRID — Institutional Layer (Layer 3).

13F filings, pension fund allocations, sovereign wealth fund disclosures.
Quarterly data with intra-quarter estimates from ETF flow proxies.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from .types import FlowLayer, FlowNode

# ── Constants ────────────────────────────────────────────────────────────

LAYER_ID = "institutional"
LAYER_LABEL = "Institutional"
LAYER_ORDER = 2

_QUARTER_END_MONTHS = frozenset({3, 6, 9, 12})
_REBALANCING_WINDOW_DAYS = 14
_ESTIMATED_QUARTERLY_REBALANCING_USD = 200_000_000_000  # ~$200B

# Major sovereign wealth funds (AUM in USD, as of 2024-Q4 estimates).
_SWF_ESTIMATES: tuple[dict[str, Any], ...] = (
    {"id": "norway_gpfg", "label": "Norway GPFG", "aum_usd": 1_700_000_000_000},
    {"id": "abu_dhabi_adia", "label": "Abu Dhabi ADIA", "aum_usd": 900_000_000_000},
    {"id": "saudi_pif", "label": "Saudi PIF", "aum_usd": 700_000_000_000},
    {"id": "singapore_gic", "label": "Singapore GIC", "aum_usd": 700_000_000_000},
    {"id": "china_cic", "label": "China CIC", "aum_usd": 1_300_000_000_000},
)
_SWF_TOTAL_USD = sum(s["aum_usd"] for s in _SWF_ESTIMATES)


# ── Node Builders ────────────────────────────────────────────────────────

def _build_13f_aggregate(engine: Engine, as_of: date) -> FlowNode:
    """Sum net dollar flows from 13F filings over the trailing 90 days."""
    lookback = as_of - timedelta(days=90)
    value: float | None = None

    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("""
                    SELECT SUM(amount_usd) AS net_flow
                    FROM dollar_flows
                    WHERE source_type = '13f'
                      AND flow_date >= :start
                      AND flow_date <= :end
                """),
                {"start": lookback, "end": as_of},
            ).fetchone()
            if row and row[0] is not None:
                value = float(row[0])
    except Exception as exc:
        log.warning("13f_aggregate query failed: {}", exc)

    return FlowNode(
        id="13f_aggregate",
        label="13F Aggregate Net Flows",
        layer=LAYER_ID,
        value=value,
        change_1m=value,  # trailing 90d is the best proxy we have
        confidence="derived",
        source="SEC:13F-HR",
        metadata={"lookback_days": 90},
    )


def _build_pension_rebalancing(as_of: date) -> FlowNode:
    """Calendar-based detection of pension rebalancing windows.

    Quarter-end months (Mar, Jun, Sep, Dec) in the last 2 weeks
    of the month signal an active rebalancing window.
    """
    month = as_of.month
    days_left_in_month = (
        (as_of.replace(month=month % 12 + 1, day=1) - timedelta(days=1)).day
        - as_of.day
    ) if month != 12 else (31 - as_of.day)

    is_quarter_end_month = month in _QUARTER_END_MONTHS
    is_window_active = is_quarter_end_month and days_left_in_month <= _REBALANCING_WINDOW_DAYS

    status = "active" if is_window_active else "inactive"
    magnitude = _ESTIMATED_QUARTERLY_REBALANCING_USD if is_window_active else 0.0

    return FlowNode(
        id="pension_rebalancing",
        label="Pension Rebalancing",
        layer=LAYER_ID,
        value=magnitude,
        confidence="estimated",
        source="calendar:quarter_end",
        metadata={
            "status": status,
            "quarter_end_month": is_quarter_end_month,
            "days_left_in_month": days_left_in_month,
        },
    )


def _build_sovereign_wealth() -> FlowNode:
    """Hardcoded estimates for major sovereign wealth funds."""
    fund_breakdown = {s["id"]: s["aum_usd"] for s in _SWF_ESTIMATES}

    return FlowNode(
        id="sovereign_wealth",
        label="Sovereign Wealth Funds",
        layer=LAYER_ID,
        value=_SWF_TOTAL_USD,
        confidence="estimated",
        source="estimated:SWF_AUM",
        metadata={"funds": fund_breakdown, "fund_count": len(_SWF_ESTIMATES)},
    )


def _build_etf_flow_proxy(engine: Engine, as_of: date) -> FlowNode:
    """Sum latest 5-day ETF flow proxy values from raw_series.

    Looks for series matching ``ETF_FLOW:<ticker>:5d`` pattern.
    """
    value: float | None = None
    etf_count = 0
    lookback = as_of - timedelta(days=10)

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT series_id, value
                    FROM raw_series
                    WHERE series_id LIKE :pattern
                      AND obs_date >= :start
                      AND obs_date <= :end
                      AND pull_status = 'SUCCESS'
                    ORDER BY obs_date DESC
                """),
                {"pattern": "ETF_FLOW:%:5d", "start": lookback, "end": as_of},
            ).fetchall()

            if rows:
                # De-duplicate: keep the latest value per series_id
                seen: dict[str, float] = {}
                for row in rows:
                    sid = str(row[0])
                    if sid not in seen:
                        seen[sid] = float(row[1])
                value = sum(seen.values())
                etf_count = len(seen)
    except Exception as exc:
        log.warning("etf_flow_proxy query failed: {}", exc)

    return FlowNode(
        id="etf_flow_proxy",
        label="ETF Flow Proxy",
        layer=LAYER_ID,
        value=value,
        confidence="derived",
        source="raw_series:ETF_FLOW:*:5d",
        metadata={"etf_count": etf_count},
    )


def _build_qq_congressional(engine: Engine, as_of: date) -> FlowNode:
    """QQ Senate + House trading signals over 30 days."""
    lookback = as_of - timedelta(days=30)
    buy_count = 0
    sell_count = 0

    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT signal_type, COUNT(*)
                FROM signal_sources
                WHERE source_type IN ('quiverquant:senate', 'quiverquant:house')
                  AND signal_date >= :d30
                GROUP BY signal_type
            """), {"d30": lookback}).fetchall()
            for sig_type, cnt in rows:
                if sig_type and 'buy' in sig_type.lower():
                    buy_count += int(cnt)
                elif sig_type and 'sell' in sig_type.lower():
                    sell_count += int(cnt)
    except Exception as exc:
        log.warning("qq_congressional query failed: {}", exc)

    total = buy_count + sell_count
    ratio = buy_count / total if total > 0 else 0.5
    signal = "bullish" if ratio > 0.6 else ("bearish" if ratio < 0.4 else "neutral")

    return FlowNode(
        id="qq_congressional",
        label="Congressional Trading",
        layer=LAYER_ID,
        value=float(total) if total else None,
        confidence="derived" if total else "estimated",
        source="signal_sources:quiverquant:senate|house",
        metadata={
            "buy_count": buy_count,
            "sell_count": sell_count,
            "buy_ratio": round(ratio, 4),
            "signal": signal,
        },
    )


def _build_qq_insider(engine: Engine, as_of: date) -> FlowNode:
    """QQ insider trading signals over 30 days."""
    lookback = as_of - timedelta(days=30)
    buys = 0
    sells = 0

    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT signal_type, COUNT(*)
                FROM signal_sources
                WHERE source_type = 'quiverquant:insider'
                  AND signal_date >= :d30
                GROUP BY signal_type
            """), {"d30": lookback}).fetchall()
            for sig_type, cnt in rows:
                if sig_type and 'buy' in sig_type.lower():
                    buys += int(cnt)
                elif sig_type and 'sell' in sig_type.lower():
                    sells += int(cnt)
    except Exception as exc:
        log.warning("qq_insider query failed: {}", exc)

    total = buys + sells
    ratio = buys / (sells + 1) if sells or buys else 0
    signal = "accumulation" if ratio > 1.5 else ("distribution" if ratio < 0.67 else "neutral")

    return FlowNode(
        id="qq_insider",
        label="Insider Trading",
        layer=LAYER_ID,
        value=float(total) if total else None,
        confidence="derived" if total else "estimated",
        source="signal_sources:quiverquant:insider",
        metadata={
            "insider_buys": buys,
            "insider_sells": sells,
            "buy_sell_ratio": round(ratio, 4),
            "signal": signal,
        },
    )


# ── Layer Builder ────────────────────────────────────────────────────────

def build_institutional_layer(engine: Engine, as_of: date | None = None) -> FlowLayer:
    """Build the complete Institutional layer (Layer 3, order=2).

    Returns:
        Immutable ``FlowLayer`` with all institutional flow nodes.
    """
    if as_of is None:
        as_of = date.today()

    nodes: tuple[FlowNode, ...] = (
        _build_13f_aggregate(engine, as_of),
        _build_pension_rebalancing(as_of),
        _build_sovereign_wealth(),
        _build_etf_flow_proxy(engine, as_of),
        _build_qq_congressional(engine, as_of),
        _build_qq_insider(engine, as_of),
    )

    # Aggregate: sum confirmed/derived values for total
    node_values = [n.value for n in nodes if n.value is not None]
    total_value = sum(node_values) if node_values else None

    # Dominant confidence: use the lowest confidence present
    confidence_priority = ("confirmed", "derived", "estimated", "rumored")
    node_confidences = {n.confidence for n in nodes}
    dominant = next(
        (c for c in confidence_priority if c in node_confidences),
        "estimated",
    )

    return FlowLayer(
        id=LAYER_ID,
        label=LAYER_LABEL,
        order=LAYER_ORDER,
        nodes=nodes,
        total_value_usd=total_value,
        confidence=dominant,
    )
