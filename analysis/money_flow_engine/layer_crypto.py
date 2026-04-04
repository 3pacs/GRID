"""GRID -- Money Flow Engine: Crypto Layer (Layer 8, order=7).

BTC flows, stablecoin supply, crypto fear/greed, BTC ETF flows.
All values immutable (frozen dataclasses). Missing data falls back
to estimates -- never crashes.
"""

from __future__ import annotations

from datetime import date, timedelta

from loguru import logger
from sqlalchemy import text
from sqlalchemy.engine import Engine

from .helpers import (
    _get_price,
    _get_price_change,
    _get_series_latest,
    compute_changes,
    compute_z_score,
    dominant_confidence,
)
from .types import FlowLayer, FlowNode

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_LAYER_ID = "crypto"
_LAYER_LABEL = "Crypto"
_LAYER_ORDER = 7

# BTC series
_BTC_SERIES = "YF:BTC-USD:close"

# Stablecoin estimates (USD)
_STABLECOIN_TOTAL_EST = 160_000_000_000   # ~$160B
_USDT_EST = 110_000_000_000               # ~$110B
_USDC_EST = 35_000_000_000               # ~$35B
_DAI_EST = 5_000_000_000                  # ~$5B

# BTC ETF tickers
_BTC_ETF_TICKERS = ("IBIT", "GBTC", "FBTC")


# ---------------------------------------------------------------------------
# Node builders
# ---------------------------------------------------------------------------

def _build_btc_flows_node(engine: Engine, as_of: date) -> FlowNode:
    """BTC-USD price and 30-day change."""
    btc_price = _get_price(engine, "BTC-USD", as_of)
    change_30d = _get_price_change(engine, "BTC-USD", 30, as_of)
    change_7d = _get_price_change(engine, "BTC-USD", 7, as_of)
    change_1d = _get_price_change(engine, "BTC-USD", 1, as_of)

    if btc_price is not None:
        confidence = "confirmed"
        source = _BTC_SERIES
    else:
        confidence = "estimated"
        source = "estimate"

    logger.debug(
        "btc_flows: price={}, 30d_chg={}, confidence={}",
        btc_price, change_30d, confidence,
    )

    return FlowNode(
        id="btc_flows",
        label="BTC Flows",
        layer=_LAYER_ID,
        value=btc_price,
        change_1d=change_1d,
        change_1w=change_7d,
        change_1m=change_30d,
        confidence=confidence,
        unit="USD",
        source=source,
        metadata={"ticker": "BTC-USD"},
    )


def _build_stablecoin_supply_node(engine: Engine, as_of: date) -> FlowNode:
    """Stablecoin total supply from DB or estimates."""
    db_total: float | None = None
    components: dict[str, float | None] = {}

    # Try querying stablecoin series from raw_series
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT series_id, value FROM raw_series
                WHERE series_id LIKE 'STABLECOIN:%'
                AND obs_date <= :d AND pull_status = 'SUCCESS'
                ORDER BY obs_date DESC
            """), {"d": as_of}).fetchall()

            if rows:
                # Deduplicate: keep latest per series_id
                seen: set[str] = set()
                total = 0.0
                for row in rows:
                    sid = str(row[0])
                    if sid not in seen:
                        seen.add(sid)
                        val = float(row[1])
                        components[sid] = val
                        total += val
                if total > 0:
                    db_total = total
    except Exception:
        logger.warning("stablecoin_supply: failed to query STABLECOIN:* series")

    if db_total is not None:
        value = db_total
        confidence = "derived"
        source = "raw_series:STABLECOIN:*"
    else:
        value = _STABLECOIN_TOTAL_EST
        confidence = "estimated"
        source = "estimate"
        components = {
            "USDT_est": _USDT_EST,
            "USDC_est": _USDC_EST,
            "DAI_est": _DAI_EST,
        }

    logger.debug(
        "stablecoin_supply: total={:.0f}B, confidence={}, components={}",
        value / 1e9, confidence, len(components),
    )

    return FlowNode(
        id="stablecoin_supply",
        label="Stablecoin Supply",
        layer=_LAYER_ID,
        value=round(value, 2),
        confidence=confidence,
        unit="USD",
        source=source,
        metadata={"components": components},
    )


def _build_crypto_fear_greed_node(engine: Engine, as_of: date) -> FlowNode:
    """Crypto Fear & Greed index (0-100)."""
    value = _get_series_latest(engine, "crypto_fear_greed", as_of)
    changes = compute_changes(engine, "crypto_fear_greed", as_of)

    if value is not None:
        confidence = "confirmed"
        source = "raw_series:crypto_fear_greed"
        # Clamp to valid range
        value = max(0.0, min(100.0, value))
    else:
        confidence = "estimated"
        source = "estimate"
        value = 50.0  # neutral default

    # Classify sentiment
    if value <= 25:
        sentiment = "extreme_fear"
    elif value <= 45:
        sentiment = "fear"
    elif value <= 55:
        sentiment = "neutral"
    elif value <= 75:
        sentiment = "greed"
    else:
        sentiment = "extreme_greed"

    logger.debug(
        "crypto_fear_greed: value={}, sentiment={}, confidence={}",
        value, sentiment, confidence,
    )

    return FlowNode(
        id="crypto_fear_greed",
        label="Crypto Fear & Greed",
        layer=_LAYER_ID,
        value=round(value, 2),
        change_1d=changes.get("change_1d"),
        change_1w=changes.get("change_1w"),
        change_1m=changes.get("change_1m"),
        confidence=confidence,
        unit="INDEX",
        source=source,
        metadata={"sentiment": sentiment, "range": "0-100"},
    )


def _build_btc_etf_flows_node(engine: Engine, as_of: date) -> FlowNode:
    """BTC ETF flows from etf_flows table (IBIT, GBTC, FBTC)."""
    total_flow: float | None = None
    per_ticker: dict[str, float] = {}

    try:
        with engine.connect() as conn:
            for ticker in _BTC_ETF_TICKERS:
                row = conn.execute(text("""
                    SELECT COALESCE(SUM(flow_value), 0) FROM etf_flows
                    WHERE ticker = :t
                    AND flow_date >= :since AND flow_date <= :d
                """), {
                    "t": ticker,
                    "since": as_of - timedelta(days=30),
                    "d": as_of,
                }).fetchone()
                if row and row[0] is not None:
                    val = float(row[0])
                    per_ticker[ticker] = val

        if per_ticker:
            total_flow = sum(per_ticker.values())
    except Exception:
        logger.warning("btc_etf_flows: failed to query etf_flows table")

    if total_flow is not None:
        confidence = "derived"
        source = "etf_flows"
        value = total_flow
    else:
        confidence = "estimated"
        source = "estimate"
        value = 0.0

    logger.debug(
        "btc_etf_flows: total_30d={:.0f}M, tickers={}, confidence={}",
        value / 1e6 if value else 0, per_ticker, confidence,
    )

    return FlowNode(
        id="btc_etf_flows",
        label="BTC ETF Flows",
        layer=_LAYER_ID,
        value=round(value, 2),
        confidence=confidence,
        unit="USD",
        source=source,
        metadata={
            "per_ticker_30d": per_ticker,
            "tickers": list(_BTC_ETF_TICKERS),
            "lookback_days": 30,
        },
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_crypto_layer(engine: Engine, as_of: date | None = None) -> FlowLayer:
    """Assemble the complete Crypto layer."""
    if as_of is None:
        as_of = date.today()

    logger.info("Building crypto layer as_of={}", as_of)

    nodes: list[FlowNode] = [
        _build_btc_flows_node(engine, as_of),
        _build_stablecoin_supply_node(engine, as_of),
        _build_crypto_fear_greed_node(engine, as_of),
        _build_btc_etf_flows_node(engine, as_of),
    ]

    conf = dominant_confidence(nodes)

    # Total only for USD-denominated nodes
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
