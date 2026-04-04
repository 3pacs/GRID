"""GRID -- Money Flow Engine: Corporate Layer (Layer 5, order=4).

Buybacks, dividends, M&A activity, and IPO pipeline.
All values immutable (frozen dataclasses). Missing data falls back
to calendar-based estimates -- never crashes.
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

_LAYER_ID = "corporate"
_LAYER_LABEL = "Corporate"
_LAYER_ORDER = 4

# S&P 500 buyback estimates (quarterly, USD)
_BUYBACK_QUARTERLY_EST = 200_000_000_000  # ~$200B/quarter
_BLACKOUT_WEEKS_BEFORE_EARNINGS = 2
_BLACKOUT_PCT_SP500 = 0.40  # ~40% of S&P in blackout at any given earnings window

# Dividend estimates
_DIVIDEND_ANNUAL_EST = 600_000_000_000  # ~$600B/year S&P 500
_DIVIDEND_QUARTERLY_EST = _DIVIDEND_ANNUAL_EST / 4
# Heavy months: March, June, September, December
_DIVIDEND_HEAVY_MONTHS = {3, 6, 9, 12}
_DIVIDEND_HEAVY_MULTIPLIER = 1.3
_DIVIDEND_LIGHT_MULTIPLIER = 0.7

# M&A estimates
_MA_ANNUAL_GLOBAL_EST = 3_000_000_000_000  # ~$3T/year global
_MA_QUARTERLY_EST = _MA_ANNUAL_GLOBAL_EST / 4

# IPO estimates
_IPO_HOT_QUARTERLY = 50_000_000_000   # $50B/quarter
_IPO_COLD_QUARTERLY = 10_000_000_000  # $10B/quarter
_VIX_HOT_THRESHOLD = 20.0


# ---------------------------------------------------------------------------
# Node builders
# ---------------------------------------------------------------------------

def _build_buyback_node(engine: Engine, as_of: date) -> FlowNode:
    """Estimate buyback activity based on calendar/blackout windows."""
    month = as_of.month
    # Earnings months (Jan, Apr, Jul, Oct) have heavier blackout periods
    earnings_months = {1, 4, 7, 10}
    in_blackout = month in earnings_months

    if in_blackout:
        effective_rate = _BUYBACK_QUARTERLY_EST * (1 - _BLACKOUT_PCT_SP500)
        meta = {"blackout_active": True, "blackout_pct": _BLACKOUT_PCT_SP500}
    else:
        effective_rate = _BUYBACK_QUARTERLY_EST
        meta = {"blackout_active": False, "blackout_pct": 0.0}

    logger.debug(
        "buyback_activity: blackout={}, effective_quarterly={:.0f}B",
        in_blackout, effective_rate / 1e9,
    )

    return FlowNode(
        id="buyback_activity",
        label="Buyback Activity",
        layer=_LAYER_ID,
        value=round(effective_rate, 2),
        confidence="estimated",
        unit="USD",
        source="calendar_estimate",
        metadata=meta,
    )


def _build_dividend_node(as_of: date) -> FlowNode:
    """Estimate dividend flows with quarterly seasonality."""
    if as_of.month in _DIVIDEND_HEAVY_MONTHS:
        quarterly_est = _DIVIDEND_QUARTERLY_EST * _DIVIDEND_HEAVY_MULTIPLIER
        season = "heavy"
    else:
        quarterly_est = _DIVIDEND_QUARTERLY_EST * _DIVIDEND_LIGHT_MULTIPLIER
        season = "light"

    logger.debug(
        "dividend_flows: month={}, season={}, est={:.0f}B",
        as_of.month, season, quarterly_est / 1e9,
    )

    return FlowNode(
        id="dividend_flows",
        label="Dividend Flows",
        layer=_LAYER_ID,
        value=round(quarterly_est, 2),
        confidence="estimated",
        unit="USD",
        source="calendar_estimate",
        metadata={"season": season, "annual_est": _DIVIDEND_ANNUAL_EST},
    )


def _build_ma_node(engine: Engine, as_of: date) -> FlowNode:
    """Proxy M&A from SEC filing velocity."""
    filing_count: int | None = None
    try:
        with engine.connect() as conn:
            row = conn.execute(text("""
                SELECT COUNT(*) FROM signal_sources
                WHERE source_type ILIKE :pat
                AND created_at >= :since
            """), {
                "pat": "%sec%",
                "since": as_of - timedelta(days=30),
            }).fetchone()
            if row:
                filing_count = int(row[0])
    except Exception:
        logger.warning("ma_activity: failed to query signal_sources for SEC filings")

    # Scale estimate by filing velocity (baseline ~50 filings/month)
    if filing_count is not None and filing_count > 0:
        velocity_ratio = filing_count / 50.0
        value = _MA_QUARTERLY_EST * min(velocity_ratio, 3.0)
        confidence = "derived"
    else:
        value = _MA_QUARTERLY_EST
        confidence = "estimated"

    logger.debug(
        "ma_activity: filings_30d={}, value={:.0f}B, confidence={}",
        filing_count, value / 1e9, confidence,
    )

    return FlowNode(
        id="ma_activity",
        label="M&A Activity",
        layer=_LAYER_ID,
        value=round(value, 2),
        confidence=confidence,
        unit="USD",
        source="sec_filing_proxy" if filing_count else "estimate",
        metadata={
            "filing_count_30d": filing_count,
            "annual_global_est": _MA_ANNUAL_GLOBAL_EST,
        },
    )


def _build_ipo_node(engine: Engine, as_of: date) -> FlowNode:
    """IPO pipeline estimate using VIX as hot/cold proxy."""
    vix = _get_series_latest(engine, "VIXCLS", as_of)

    if vix is not None:
        is_hot = vix < _VIX_HOT_THRESHOLD
        value = _IPO_HOT_QUARTERLY if is_hot else _IPO_COLD_QUARTERLY
        confidence = "derived"
    else:
        # No VIX data -- assume lukewarm
        is_hot = None
        value = (_IPO_HOT_QUARTERLY + _IPO_COLD_QUARTERLY) / 2
        confidence = "estimated"

    logger.debug(
        "ipo_pipeline: vix={}, hot={}, value={:.0f}B",
        vix, is_hot, value / 1e9,
    )

    return FlowNode(
        id="ipo_pipeline",
        label="IPO Pipeline",
        layer=_LAYER_ID,
        value=round(value, 2),
        confidence=confidence,
        unit="USD",
        source="FRED:VIXCLS" if vix is not None else "estimate",
        metadata={
            "vix_level": vix,
            "market_temp": "hot" if is_hot is True else ("cold" if is_hot is False else "unknown"),
        },
    )


def _build_qq_lobbying(engine: Engine, as_of: date) -> FlowNode:
    """QQ lobbying expenditures over 90 days."""
    lookback = as_of - timedelta(days=90)
    ticker_count = 0
    total_spend = 0.0

    try:
        with engine.connect() as conn:
            row = conn.execute(text("""
                SELECT COUNT(DISTINCT ticker),
                       SUM((signal_value->>'Amount')::float)
                FROM signal_sources
                WHERE source_type = 'quiverquant:lobbying'
                  AND signal_date >= :d90
            """), {"d90": lookback}).fetchone()
            if row:
                ticker_count = int(row[0]) if row[0] else 0
                total_spend = float(row[1]) if row[1] else 0.0
    except Exception as exc:
        logger.warning("qq_lobbying query failed: {}", exc)

    return FlowNode(
        id="qq_lobbying",
        label="Lobbying Activity",
        layer=_LAYER_ID,
        value=round(total_spend, 2) if total_spend else None,
        confidence="derived" if ticker_count else "estimated",
        unit="USD",
        source="signal_sources:quiverquant:lobbying",
        metadata={
            "companies_lobbying": ticker_count,
            "total_spend_90d": total_spend,
        },
    )


def _build_qq_gov_contracts(engine: Engine, as_of: date) -> FlowNode:
    """QQ government contracts over 90 days."""
    lookback = as_of - timedelta(days=90)
    count = 0
    total_value = 0.0

    try:
        with engine.connect() as conn:
            row = conn.execute(text("""
                SELECT COUNT(*),
                       SUM((signal_value->>'Amount')::float)
                FROM signal_sources
                WHERE source_type = 'quiverquant:gov_contracts'
                  AND signal_date >= :d90
            """), {"d90": lookback}).fetchone()
            if row:
                count = int(row[0]) if row[0] else 0
                total_value = float(row[1]) if row[1] else 0.0
    except Exception as exc:
        logger.warning("qq_gov_contracts query failed: {}", exc)

    return FlowNode(
        id="qq_gov_contracts",
        label="Government Contracts",
        layer=_LAYER_ID,
        value=round(total_value, 2) if total_value else None,
        confidence="derived" if count else "estimated",
        unit="USD",
        source="signal_sources:quiverquant:gov_contracts",
        metadata={
            "contract_count": count,
            "total_value_90d": total_value,
        },
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_corporate_layer(engine: Engine, as_of: date | None = None) -> FlowLayer:
    """Assemble the complete Corporate layer."""
    if as_of is None:
        as_of = date.today()

    logger.info("Building corporate layer as_of={}", as_of)

    nodes: list[FlowNode] = [
        _build_buyback_node(engine, as_of),
        _build_dividend_node(as_of),
        _build_ma_node(engine, as_of),
        _build_ipo_node(engine, as_of),
        _build_qq_lobbying(engine, as_of),
        _build_qq_gov_contracts(engine, as_of),
    ]

    total = sum(n.value for n in nodes if n.value is not None)
    conf = dominant_confidence(nodes)

    return FlowLayer(
        id=_LAYER_ID,
        label=_LAYER_LABEL,
        order=_LAYER_ORDER,
        nodes=tuple(nodes),
        total_value_usd=round(total, 2) if total else None,
        confidence=conf,
    )
