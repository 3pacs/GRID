"""GRID -- Money Flow Engine: Retail Layer (Layer 7, order=6).

Margin debt, consumer sentiment, retail fund flows, prediction markets.
All values immutable (frozen dataclasses). Missing data falls back
to estimates -- never crashes.
"""

from __future__ import annotations

from datetime import date, timedelta

from loguru import logger
from sqlalchemy import text
from sqlalchemy.engine import Engine

from .helpers import (
    _get_series_latest,
    compute_changes,
    compute_z_score,
    dominant_confidence,
)
from .types import FlowLayer, FlowNode

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_LAYER_ID = "retail"
_LAYER_LABEL = "Retail"
_LAYER_ORDER = 6

# Margin debt estimate when no data available
_MARGIN_DEBT_EST = 750_000_000_000  # ~$750B

# FRED series
_UMCSENT_SERIES = "UMCSENT"

# AAII sentiment thresholds
_AAII_BULL_INFLOW_THRESHOLD = 50.0


# ---------------------------------------------------------------------------
# Node builders
# ---------------------------------------------------------------------------

def _build_margin_debt_node(engine: Engine, as_of: date) -> FlowNode:
    """Margin debt from margin_debt_monthly table."""
    value: float | None = None
    confidence = "estimated"
    source = "estimate"

    try:
        with engine.connect() as conn:
            row = conn.execute(text("""
                SELECT margin_debt, obs_date FROM margin_debt_monthly
                WHERE obs_date <= :d
                ORDER BY obs_date DESC LIMIT 1
            """), {"d": as_of}).fetchone()
            if row and row[0] is not None:
                value = float(row[0])
                confidence = "confirmed"
                source = "margin_debt_monthly"
    except Exception:
        logger.warning("margin_debt: failed to query margin_debt_monthly table")

    if value is None:
        value = _MARGIN_DEBT_EST

    # Compute changes from the table if possible
    prev_month: float | None = None
    try:
        with engine.connect() as conn:
            row = conn.execute(text("""
                SELECT margin_debt FROM margin_debt_monthly
                WHERE obs_date <= :d
                ORDER BY obs_date DESC LIMIT 1
            """), {"d": as_of - timedelta(days=30)}).fetchone()
            if row and row[0] is not None:
                prev_month = float(row[0])
    except Exception as exc:
        logger.warning("Margin debt lookup failed: {e}", e=exc)

    change_1m: float | None = None
    if value and prev_month and prev_month != 0:
        change_1m = round((value - prev_month) / abs(prev_month), 6)

    logger.debug(
        "margin_debt: value={:.0f}B, confidence={}, change_1m={}",
        value / 1e9, confidence, change_1m,
    )

    return FlowNode(
        id="margin_debt",
        label="Margin Debt",
        layer=_LAYER_ID,
        value=round(value, 2),
        change_1m=change_1m,
        confidence=confidence,
        unit="USD",
        source=source,
        metadata={"est_fallback": _MARGIN_DEBT_EST},
    )


def _build_consumer_sentiment_node(engine: Engine, as_of: date) -> FlowNode:
    """Consumer sentiment from FRED UMCSENT."""
    value = _get_series_latest(engine, _UMCSENT_SERIES, as_of)
    changes = compute_changes(engine, _UMCSENT_SERIES, as_of)

    if value is not None:
        confidence = "confirmed"
        source = f"FRED:{_UMCSENT_SERIES}"
    else:
        confidence = "estimated"
        source = "estimate"
        # Historical average ~70
        value = 70.0

    logger.debug(
        "consumer_sentiment: value={}, confidence={}", value, confidence,
    )

    return FlowNode(
        id="consumer_sentiment",
        label="Consumer Sentiment",
        layer=_LAYER_ID,
        value=round(value, 4),
        change_1d=changes.get("change_1d"),
        change_1w=changes.get("change_1w"),
        change_1m=changes.get("change_1m"),
        confidence=confidence,
        unit="INDEX",
        source=source,
        metadata={"series": _UMCSENT_SERIES},
    )


def _build_retail_fund_flows_node(engine: Engine, as_of: date) -> FlowNode:
    """Retail fund flows proxy from AAII sentiment."""
    bull_pct: float | None = None
    confidence = "estimated"
    source = "estimate"
    inflow_signal = False

    try:
        with engine.connect() as conn:
            row = conn.execute(text("""
                SELECT signal_value FROM signal_sources
                WHERE source_type = 'aaii'
                AND signal_date <= :d
                ORDER BY signal_date DESC LIMIT 1
            """), {"d": as_of}).fetchone()
            if row and row[0] is not None:
                bull_pct = float(row[0])
                inflow_signal = bull_pct > _AAII_BULL_INFLOW_THRESHOLD
                confidence = "derived"
                source = "signal_sources:aaii"
    except Exception:
        logger.warning("retail_fund_flows: failed to query AAII from signal_sources")

    # No USD value for sentiment proxy; use bull_pct as the value
    value = bull_pct if bull_pct is not None else 50.0

    logger.debug(
        "retail_fund_flows: bull_pct={}, inflow_signal={}, confidence={}",
        bull_pct, inflow_signal, confidence,
    )

    return FlowNode(
        id="retail_fund_flows",
        label="Retail Fund Flows",
        layer=_LAYER_ID,
        value=round(value, 4),
        confidence=confidence,
        unit="PCT",
        source=source,
        metadata={
            "bull_pct": bull_pct,
            "inflow_signal": inflow_signal,
            "threshold": _AAII_BULL_INFLOW_THRESHOLD,
        },
    )


def _build_prediction_markets_node(engine: Engine, as_of: date) -> FlowNode:
    """Prediction market flows from dollar_flows table."""
    total_flow: float | None = None
    confidence = "estimated"
    source = "estimate"

    try:
        with engine.connect() as conn:
            row = conn.execute(text("""
                SELECT COALESCE(SUM(amount_usd), 0) FROM dollar_flows
                WHERE source_type = 'prediction_market'
                AND flow_date >= :since AND flow_date <= :d
            """), {
                "since": as_of - timedelta(days=30),
                "d": as_of,
            }).fetchone()
            if row and row[0] is not None:
                total_flow = float(row[0])
                if total_flow != 0:
                    confidence = "derived"
                    source = "dollar_flows:prediction_market"
    except Exception:
        logger.warning("prediction_markets: failed to query dollar_flows")

    value = total_flow if total_flow is not None else 0.0

    logger.debug(
        "prediction_markets: flow_30d={:.0f}M, confidence={}",
        value / 1e6 if value else 0, confidence,
    )

    return FlowNode(
        id="prediction_markets",
        label="Prediction Markets",
        layer=_LAYER_ID,
        value=round(value, 2),
        confidence=confidence,
        unit="USD",
        source=source,
        metadata={"lookback_days": 30},
    )


def _build_qq_wsb_sentiment(engine: Engine, as_of: date) -> FlowNode:
    """QQ WallStreetBets sentiment over 7 days."""
    lookback = as_of - timedelta(days=7)
    bullish = 0
    bearish = 0

    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT signal_type, COUNT(*)
                FROM signal_sources
                WHERE source_type = 'quiverquant:wsb'
                  AND signal_date >= :d7
                GROUP BY signal_type
            """), {"d7": lookback}).fetchall()
            for sig_type, cnt in rows:
                if sig_type and 'bullish' in sig_type.lower():
                    bullish += int(cnt)
                elif sig_type and 'bearish' in sig_type.lower():
                    bearish += int(cnt)
    except Exception as exc:
        logger.warning("qq_wsb query failed: {}", exc)

    total = bullish + bearish
    net_sentiment = (bullish - bearish) / total if total > 0 else None

    return FlowNode(
        id="qq_wsb_sentiment",
        label="WSB Sentiment",
        layer=_LAYER_ID,
        value=net_sentiment if net_sentiment is not None else 0.0,
        confidence="derived" if total > 0 else "estimated",
        unit="INDEX",
        source="signal_sources:quiverquant:wsb",
        metadata={
            "bullish": bullish,
            "bearish": bearish,
            "total": total,
            "net_sentiment": net_sentiment,
        },
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_retail_layer(engine: Engine, as_of: date | None = None) -> FlowLayer:
    """Assemble the complete Retail layer."""
    if as_of is None:
        as_of = date.today()

    logger.info("Building retail layer as_of={}", as_of)

    nodes: list[FlowNode] = [
        _build_margin_debt_node(engine, as_of),
        _build_consumer_sentiment_node(engine, as_of),
        _build_retail_fund_flows_node(engine, as_of),
        _build_prediction_markets_node(engine, as_of),
        _build_qq_wsb_sentiment(engine, as_of),
    ]

    conf = dominant_confidence(nodes)

    # Total only for USD-denominated nodes (skip index/pct nodes)
    usd_nodes = [n for n in nodes if n.unit == "USD" and n.value is not None]
    total = sum(n.value for n in usd_nodes) if usd_nodes else None

    return FlowLayer(
        id=_LAYER_ID,
        label=_LAYER_LABEL,
        order=_LAYER_ORDER,
        nodes=tuple(nodes),
        total_value_usd=round(total, 2) if total else None,
        confidence=conf,
    )
