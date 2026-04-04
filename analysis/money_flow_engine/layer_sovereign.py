"""GRID -- Money Flow Engine: Sovereign Layer (Layer 6, order=5).

FX reserves, trade balance, foreign treasury holdings, tariff impact.
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

_LAYER_ID = "sovereign"
_LAYER_LABEL = "Sovereign"
_LAYER_ORDER = 5

# Global FX reserves estimate
_GLOBAL_FX_RESERVES_EST = 12_000_000_000_000  # ~$12T

# Foreign treasury holdings estimate
_FOREIGN_TREASURY_EST = 7_600_000_000_000  # ~$7.6T

# FRED series IDs
_TRADE_BALANCE_SERIES = "BOPGTB"
_TREASURY_HOLDINGS_SERIES = "BOGZ1FL263061103Q"
_INDIA_FX_SERIES = "india_fx_reserves"


# ---------------------------------------------------------------------------
# Node builders
# ---------------------------------------------------------------------------

def _build_fx_reserves_node(engine: Engine, as_of: date) -> FlowNode:
    """FX reserves: India from DB, others estimated."""
    india_reserves: float | None = None
    try:
        india_reserves = _get_series_latest(engine, _INDIA_FX_SERIES, as_of)
    except Exception:
        logger.warning("fx_reserves: failed to query india_fx_reserves")

    if india_reserves is not None:
        # India is ~$600-700B of $12T total; scale the rest
        others_est = _GLOBAL_FX_RESERVES_EST - 650_000_000_000 + india_reserves
        value = others_est
        meta = {"india_reserves": india_reserves, "india_source": _INDIA_FX_SERIES}
    else:
        value = _GLOBAL_FX_RESERVES_EST
        meta = {"india_reserves": None, "india_source": "unavailable"}

    changes = compute_changes(engine, _INDIA_FX_SERIES, as_of) if india_reserves else {}

    logger.debug(
        "fx_reserves: india={}, total_est={:.1f}T",
        india_reserves, value / 1e12,
    )

    return FlowNode(
        id="fx_reserves",
        label="FX Reserves",
        layer=_LAYER_ID,
        value=round(value, 2),
        change_1d=changes.get("change_1d"),
        change_1w=changes.get("change_1w"),
        change_1m=changes.get("change_1m"),
        confidence="estimated",
        unit="USD",
        source=_INDIA_FX_SERIES if india_reserves else "global_estimate",
        metadata=meta,
    )


def _build_trade_balance_node(engine: Engine, as_of: date) -> FlowNode:
    """US trade balance from FRED BOPGTB. Negative = deficit = capital inflows."""
    value = _get_series_latest(engine, _TRADE_BALANCE_SERIES, as_of)
    changes = compute_changes(engine, _TRADE_BALANCE_SERIES, as_of)

    if value is not None:
        confidence = "confirmed"
        source = f"FRED:{_TRADE_BALANCE_SERIES}"
        # BOPGTB is in millions; convert to USD
        value_usd = value * 1_000_000
        capital_inflow_signal = value < 0
    else:
        confidence = "estimated"
        source = "estimate"
        # US typically runs ~$70B/month deficit
        value_usd = -70_000_000_000.0
        capital_inflow_signal = True

    logger.debug(
        "trade_balance: raw={}, usd={:.0f}B, inflow_signal={}",
        value, value_usd / 1e9, capital_inflow_signal,
    )

    return FlowNode(
        id="trade_balance",
        label="Trade Balance",
        layer=_LAYER_ID,
        value=round(value_usd, 2),
        change_1d=changes.get("change_1d"),
        change_1w=changes.get("change_1w"),
        change_1m=changes.get("change_1m"),
        confidence=confidence,
        unit="USD",
        source=source,
        metadata={
            "raw_value_millions": value,
            "capital_inflow_signal": capital_inflow_signal,
        },
    )


def _build_foreign_treasury_node(engine: Engine, as_of: date) -> FlowNode:
    """Foreign holdings of US treasuries."""
    value = _get_series_latest(engine, _TREASURY_HOLDINGS_SERIES, as_of)
    changes = compute_changes(engine, _TREASURY_HOLDINGS_SERIES, as_of)

    if value is not None:
        confidence = "derived"
        source = f"FRED:{_TREASURY_HOLDINGS_SERIES}"
        # Series is in millions
        value_usd = value * 1_000_000
    else:
        confidence = "estimated"
        source = "estimate"
        value_usd = _FOREIGN_TREASURY_EST

    logger.debug(
        "foreign_treasury_holdings: raw={}, usd={:.1f}T",
        value, value_usd / 1e12,
    )

    return FlowNode(
        id="foreign_treasury_holdings",
        label="Foreign Treasury Holdings",
        layer=_LAYER_ID,
        value=round(value_usd, 2),
        change_1d=changes.get("change_1d"),
        change_1w=changes.get("change_1w"),
        change_1m=changes.get("change_1m"),
        confidence=confidence,
        unit="USD",
        source=source,
        metadata={"raw_value_millions": value, "est_total": _FOREIGN_TREASURY_EST},
    )


def _build_tariff_node(engine: Engine, as_of: date) -> FlowNode:
    """Tariff impact proxy from wiki_tariff feature or estimate."""
    tariff_val: float | None = None
    source = "estimate"

    # Try resolved_series for wiki_tariff
    try:
        tariff_val = _get_series_latest(engine, "wiki_tariff", as_of)
        if tariff_val is not None:
            source = "resolved_series:wiki_tariff"
    except Exception:
        logger.warning("tariff_impact: failed to query wiki_tariff")

    if tariff_val is not None:
        confidence = "derived"
        # Treat as an index/score; scale to rough USD impact
        # Average US tariff revenue ~$80B/year, scale by feature value
        value = tariff_val
    else:
        confidence = "estimated"
        # Baseline tariff revenue estimate ~$80B/year
        value = 80_000_000_000.0

    changes = compute_changes(engine, "wiki_tariff", as_of) if tariff_val else {}

    logger.debug(
        "tariff_impact: wiki_tariff={}, confidence={}", tariff_val, confidence,
    )

    return FlowNode(
        id="tariff_impact",
        label="Tariff Impact",
        layer=_LAYER_ID,
        value=round(value, 2),
        change_1d=changes.get("change_1d"),
        change_1w=changes.get("change_1w"),
        change_1m=changes.get("change_1m"),
        confidence=confidence,
        unit="USD",
        source=source,
        metadata={"wiki_tariff_raw": tariff_val},
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_sovereign_layer(engine: Engine, as_of: date | None = None) -> FlowLayer:
    """Assemble the complete Sovereign layer."""
    if as_of is None:
        as_of = date.today()

    logger.info("Building sovereign layer as_of={}", as_of)

    nodes: list[FlowNode] = [
        _build_fx_reserves_node(engine, as_of),
        _build_trade_balance_node(engine, as_of),
        _build_foreign_treasury_node(engine, as_of),
        _build_tariff_node(engine, as_of),
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
