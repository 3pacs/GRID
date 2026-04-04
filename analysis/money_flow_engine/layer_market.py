"""GRID — Market Layer (Layer 4).

ETF flows, options positioning, dark pool activity, CFTC positioning.
The highest-frequency junction point layer with daily observable data.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from .types import FlowLayer, FlowNode

# ── Constants ────────────────────────────────────────────────────────────

LAYER_ID = "market"
LAYER_LABEL = "Market"
LAYER_ORDER = 3

_PRICE_TICKERS: tuple[dict[str, Any], ...] = (
    {"id": "equities", "label": "Equities (SPY)", "series": "YF:SPY:close"},
    {"id": "bonds", "label": "Bonds (TLT)", "series": "YF:TLT:close"},
    {"id": "commodities", "label": "Commodities (GLD)", "series": "YF:GLD:close"},
)

_OPTIONS_TICKERS = ("SPY", "QQQ", "IWM")

_PCR_BEARISH_THRESHOLD = 1.0
_PCR_BULLISH_THRESHOLD = 0.7


# ── Helpers ──────────────────────────────────────────────────────────────

def _get_price_and_change(
    engine: Engine,
    series_id: str,
    as_of: date,
    change_days: int = 30,
) -> tuple[float | None, float | None]:
    """Fetch the latest price and its N-day change from raw_series.

    Uses UNION ALL for case-insensitive fallback (268M rows, needs index).
    Returns:
        (current_price, change_over_period) — both may be ``None``.
    """
    past_target = as_of - timedelta(days=change_days)
    lookback = as_of - timedelta(days=change_days + 5)

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT tag, value FROM (
                        (SELECT 'cur' AS tag, value, obs_date FROM raw_series
                         WHERE series_id = :sid AND obs_date <= :d AND pull_status = 'SUCCESS'
                         ORDER BY obs_date DESC LIMIT 1)
                        UNION ALL
                        (SELECT 'cur' AS tag, value, obs_date FROM raw_series
                         WHERE series_id = :sid_lower AND obs_date <= :d AND pull_status = 'SUCCESS'
                         ORDER BY obs_date DESC LIMIT 1)
                        UNION ALL
                        (SELECT 'past' AS tag, value, obs_date FROM raw_series
                         WHERE series_id = :sid AND obs_date <= :pt AND obs_date >= :lb AND pull_status = 'SUCCESS'
                         ORDER BY obs_date DESC LIMIT 1)
                        UNION ALL
                        (SELECT 'past' AS tag, value, obs_date FROM raw_series
                         WHERE series_id = :sid_lower AND obs_date <= :pt AND obs_date >= :lb AND pull_status = 'SUCCESS'
                         ORDER BY obs_date DESC LIMIT 1)
                    ) sub
                """),
                {"sid": series_id, "sid_lower": series_id.lower(), "d": as_of, "pt": past_target, "lb": lookback},
            ).fetchall()

            vals: dict[str, float] = {}
            for tag, value in rows:
                if tag not in vals:
                    vals[tag] = float(value)

            current = vals.get("cur")
            past = vals.get("past")
            change = round(current - past, 4) if current is not None and past is not None else None
            return current, change
    except Exception as exc:
        log.warning("price query failed for {}: {}", series_id, exc)
        return None, None


# ── Node Builders ────────────────────────────────────────────────────────

def _build_price_node(
    engine: Engine,
    as_of: date,
    node_id: str,
    label: str,
    series_id: str,
) -> FlowNode:
    """Build a price-based node (equities, bonds, commodities)."""
    current, change_1m = _get_price_and_change(engine, series_id, as_of, change_days=30)

    return FlowNode(
        id=node_id,
        label=label,
        layer=LAYER_ID,
        value=current,
        change_1m=change_1m,
        confidence="confirmed",
        unit="USD",
        source=series_id,
    )


def _build_etf_flows(engine: Engine, as_of: date) -> FlowNode:
    """Sum recent ETF flow data from etf_flows or dollar_flows tables."""
    value: float | None = None
    source_used = ""
    lookback = as_of - timedelta(days=7)

    try:
        with engine.connect() as conn:
            # Try etf_flows table first
            row = conn.execute(
                text("""
                    SELECT SUM(flow_value) AS total
                    FROM etf_flows
                    WHERE flow_date >= :start AND flow_date <= :end
                """),
                {"start": lookback, "end": as_of},
            ).fetchone()
            if row and row[0] is not None:
                value = float(row[0])
                source_used = "etf_flows"
    except Exception:
        pass  # table may not exist; fall through

    if value is None:
        try:
            with engine.connect() as conn:
                row = conn.execute(
                    text("""
                        SELECT SUM(amount_usd) AS total
                        FROM dollar_flows
                        WHERE source_type = 'etf_flow'
                          AND flow_date >= :start
                          AND flow_date <= :end
                    """),
                    {"start": lookback, "end": as_of},
                ).fetchone()
                if row and row[0] is not None:
                    value = float(row[0])
                    source_used = "dollar_flows:etf_flow"
        except Exception as exc:
            log.warning("etf_flows query failed: {}", exc)

    return FlowNode(
        id="etf_flows",
        label="ETF Flows",
        layer=LAYER_ID,
        value=value,
        confidence="derived",
        source=source_used or "etf_flows|dollar_flows",
        metadata={"lookback_days": 7},
    )


def _build_options_positioning(engine: Engine, as_of: date) -> FlowNode:
    """Aggregate put/call ratio across SPY, QQQ, IWM over last 7 days.

    PCR > 1.0 = bearish, PCR < 0.7 = bullish, otherwise neutral.
    """
    pcr: float | None = None
    signal = "neutral"
    lookback = as_of - timedelta(days=7)

    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("""
                    SELECT AVG(put_call_ratio) FROM options_daily_signals
                    WHERE ticker IN ('SPY', 'QQQ', 'IWM')
                      AND signal_date >= :d7
                """),
                {"d7": lookback},
            ).fetchone()
            if row and row[0] is not None:
                pcr = round(float(row[0]), 4)
                if pcr > _PCR_BEARISH_THRESHOLD:
                    signal = "bearish"
                elif pcr < _PCR_BULLISH_THRESHOLD:
                    signal = "bullish"
                else:
                    signal = "neutral"
    except Exception as exc:
        log.warning("options_positioning query failed: {}", exc)

    return FlowNode(
        id="options_positioning",
        label="Options Positioning",
        layer=LAYER_ID,
        value=pcr,
        confidence="confirmed",
        unit="ratio",
        source="options_daily_signals:put_call_ratio",
        metadata={"signal": signal, "tickers": list(_OPTIONS_TICKERS)},
    )


def _build_dark_pool_activity(engine: Engine, as_of: date) -> FlowNode:
    """Aggregate dark pool signals over last 7 days.

    Counts total signals and accumulation signals for a ratio.
    """
    total_count = 0
    acc_count = 0
    acc_ratio: float | None = None
    lookback = as_of - timedelta(days=7)

    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("""
                    SELECT COUNT(*),
                           SUM(CASE WHEN signal_type ILIKE '%%accumulation%%'
                                    THEN 1 ELSE 0 END) AS acc_count
                    FROM signal_sources
                    WHERE source_type = 'darkpool'
                      AND signal_date >= :d7
                """),
                {"d7": lookback},
            ).fetchone()
            if row and row[0]:
                total_count = int(row[0])
                acc_count = int(row[1]) if row[1] else 0
                acc_ratio = round(acc_count / total_count, 4) if total_count > 0 else 0.0
    except Exception as exc:
        log.warning("dark_pool_activity query failed: {}", exc)

    signal = "neutral"
    if acc_ratio is not None:
        if acc_ratio > 0.6:
            signal = "accumulation"
        elif acc_ratio < 0.3:
            signal = "distribution"

    return FlowNode(
        id="dark_pool_activity",
        label="Dark Pool Activity",
        layer=LAYER_ID,
        value=float(total_count) if total_count else None,
        confidence="derived",
        source="signal_sources:darkpool",
        metadata={
            "total_signals": total_count,
            "accumulation_signals": acc_count,
            "accumulation_ratio": acc_ratio,
            "signal": signal,
        },
    )


def _build_qq_offexchange(engine: Engine, as_of: date) -> FlowNode:
    """QQ off-exchange (dark pool / OTC) short volume signals over 7 days."""
    total_count = 0
    high_short = 0
    short_ratio: float | None = None
    lookback = as_of - timedelta(days=7)

    try:
        with engine.connect() as conn:
            row = conn.execute(text("""
                SELECT COUNT(*),
                       SUM(CASE WHEN (signal_value->>'ShortVolume')::float /
                           NULLIF((signal_value->>'TotalVolume')::float, 0) > 0.5
                           THEN 1 ELSE 0 END)
                FROM signal_sources
                WHERE source_type = 'quiverquant:offexchange'
                  AND signal_date >= :d7
            """), {"d7": lookback}).fetchone()
            if row and row[0]:
                total_count = int(row[0])
                high_short = int(row[1]) if row[1] else 0
                short_ratio = round(high_short / total_count, 4)
    except Exception as exc:
        log.warning("qq_offexchange query failed: {}", exc)

    signal = "neutral"
    if short_ratio is not None:
        signal = "heavy_shorting" if short_ratio > 0.7 else (
            "low_short_interest" if short_ratio < 0.3 else "neutral"
        )

    return FlowNode(
        id="qq_offexchange",
        label="QQ Off-Exchange (OTC/Dark Pool)",
        layer=LAYER_ID,
        value=float(total_count) if total_count else None,
        confidence="derived" if total_count else "estimated",
        source="signal_sources:quiverquant:offexchange",
        metadata={
            "total_signals": total_count,
            "high_short_signals": high_short,
            "short_ratio": short_ratio,
            "signal": signal,
        },
    )


# ── Layer Builder ────────────────────────────────────────────────────────

def build_market_layer(engine: Engine, as_of: date | None = None) -> FlowLayer:
    """Build the complete Market layer (Layer 4, order=3).

    Returns:
        Immutable ``FlowLayer`` with all market-level flow nodes.
    """
    if as_of is None:
        as_of = date.today()

    # Build price nodes from the ticker registry
    price_nodes = tuple(
        _build_price_node(engine, as_of, t["id"], t["label"], t["series"])
        for t in _PRICE_TICKERS
    )

    signal_nodes = (
        _build_etf_flows(engine, as_of),
        _build_options_positioning(engine, as_of),
        _build_dark_pool_activity(engine, as_of),
        _build_qq_offexchange(engine, as_of),
    )

    nodes = price_nodes + signal_nodes

    # Determine regime from options + dark pool signals
    regime = _determine_market_regime(nodes)

    # Dominant confidence
    confidence_priority = ("confirmed", "derived", "estimated", "rumored")
    node_confidences = {n.confidence for n in nodes}
    dominant = next(
        (c for c in confidence_priority if c in node_confidences),
        "estimated",
    )

    # Compute total_value_usd from ETF market proxy
    # US equity market ~$50T, bond market ~$46T, gold ~$5T
    # Use ETF prices as proxy for directional value
    _MARKET_CAPS = {"equities": 50_000_000_000_000, "bonds": 46_000_000_000_000, "commodities": 5_000_000_000_000}
    total_value = sum(_MARKET_CAPS.get(n.id, 0) for n in price_nodes)

    # net_flow_1m = sum of all node change_1m values that exist
    net_flow = sum(n.change_1m for n in nodes if n.change_1m is not None)

    # Stress from ETF flows — large negative flow = stress
    etf_node = next((n for n in signal_nodes if n.id == "etf_flows"), None)
    etf_val = etf_node.value if etf_node else None
    stress = 0.0
    if etf_val is not None and etf_val < 0:
        stress = min(abs(etf_val) / 1_000_000_000, 1.0)  # normalize to 0-1

    return FlowLayer(
        id=LAYER_ID,
        label=LAYER_LABEL,
        order=LAYER_ORDER,
        nodes=nodes,
        total_value_usd=total_value,
        net_flow_1m=net_flow,
        stress_score=stress,
        confidence=dominant,
        regime=regime,
    )


def _determine_market_regime(nodes: tuple[FlowNode, ...]) -> str:
    """Infer market regime from options and dark pool node metadata.

    Returns:
        One of: "risk_on", "risk_off", "transitioning", "neutral".
    """
    options_signal = "neutral"
    dark_pool_signal = "neutral"

    for node in nodes:
        if node.id == "options_positioning":
            options_signal = node.metadata.get("signal", "neutral")
        elif node.id == "dark_pool_activity":
            dark_pool_signal = node.metadata.get("signal", "neutral")

    if options_signal == "bullish" and dark_pool_signal == "accumulation":
        return "risk_on"
    if options_signal == "bearish" and dark_pool_signal == "distribution":
        return "risk_off"
    if options_signal == "bearish" or dark_pool_signal == "distribution":
        return "transitioning"
    if options_signal == "bullish" or dark_pool_signal == "accumulation":
        return "transitioning"

    return "neutral"
