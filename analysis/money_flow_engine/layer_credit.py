"""GRID -- Credit Layer (Layer 2).

Bank lending, bond spreads, and money markets.
Sits immediately downstream of the monetary layer -- central bank liquidity
flows through the banking/credit system before reaching markets.
"""

from __future__ import annotations

from datetime import date

from loguru import logger as log
from sqlalchemy.engine import Engine

from .helpers import (
    _get_series_history,
    _get_series_latest,
    compute_changes,
    compute_z_score,
    dominant_confidence,
)
from .types import FlowLayer, FlowNode


# ── Series Configuration ──────────────────────────────────────────────

_SERIES_BANK_CREDIT = "TOTBKCR"          # Total bank credit, all commercial banks
_SERIES_BANK_CREDIT_ALT = "H8B1023NCBCMG"  # H.8 bank credit fallback
_SERIES_HY_SPREAD = "BAMLH0A0HYM2"       # ICE BofA High Yield OAS
_SERIES_IG_SPREAD = "BAMLC0A0CM"          # ICE BofA Investment Grade OAS
_SERIES_SOFR = "SOFR"                     # Secured Overnight Financing Rate
_SERIES_RRP = "RRPONTSYD"                 # Reverse repo (for repo stress proxy)

# Estimated fallbacks
_EST_MONEY_MARKET_FUNDS = 6_000_000_000_000  # ~$6T in money market funds
_EST_BANK_CREDIT = 17_500_000_000_000        # ~$17.5T total US bank credit

# HY spread thresholds (basis points)
_HY_SPREAD_ELEVATED = 450   # above this = stress
_HY_SPREAD_CRISIS = 700     # above this = severe


# ── Node Builders ─────────────────────────────────────────────────────


def _build_bank_credit_node(engine: Engine, as_of: date) -> FlowNode:
    """Total bank credit node (FRED TOTBKCR or H.8 fallback)."""
    live_val = _get_series_latest(engine, _SERIES_BANK_CREDIT, as_of)
    series_used = _SERIES_BANK_CREDIT

    if live_val is None:
        live_val = _get_series_latest(engine, _SERIES_BANK_CREDIT_ALT, as_of)
        series_used = _SERIES_BANK_CREDIT_ALT

    if live_val is not None:
        value = live_val
        confidence = "confirmed"
    else:
        value = _EST_BANK_CREDIT
        confidence = "estimated"
        series_used = _SERIES_BANK_CREDIT

    changes = compute_changes(engine, series_used, as_of) if live_val is not None else {}
    history = _get_series_history(engine, series_used, as_of) if live_val is not None else []
    z = compute_z_score(history, live_val) if live_val is not None and history else None

    return FlowNode(
        id="bank_credit",
        label="Total Bank Credit",
        layer="credit",
        value=value,
        change_1d=changes.get("change_1d"),
        change_1w=changes.get("change_1w"),
        change_1m=changes.get("change_1m"),
        z_score=z,
        confidence=confidence,
        unit="USD",
        source=f"FRED:{series_used}",
    )


def _build_hy_spread_node(engine: Engine, as_of: date) -> FlowNode:
    """High yield corporate bond spread (OAS, basis points)."""
    live_val = _get_series_latest(engine, _SERIES_HY_SPREAD, as_of)
    confidence = "confirmed" if live_val is not None else "estimated"
    value = live_val if live_val is not None else 400.0  # ~400bps normal

    changes = compute_changes(engine, _SERIES_HY_SPREAD, as_of)
    history = _get_series_history(engine, _SERIES_HY_SPREAD, as_of)
    z = compute_z_score(history, value) if history else None

    return FlowNode(
        id="hy_spread",
        label="High Yield Spread (OAS)",
        layer="credit",
        value=value,
        change_1d=changes.get("change_1d"),
        change_1w=changes.get("change_1w"),
        change_1m=changes.get("change_1m"),
        z_score=z,
        confidence=confidence,
        unit="bps",
        source=f"FRED:{_SERIES_HY_SPREAD}",
    )


def _build_ig_spread_node(engine: Engine, as_of: date) -> FlowNode:
    """Investment grade corporate bond spread (OAS, basis points)."""
    live_val = _get_series_latest(engine, _SERIES_IG_SPREAD, as_of)
    confidence = "confirmed" if live_val is not None else "estimated"
    value = live_val if live_val is not None else 130.0  # ~130bps normal

    changes = compute_changes(engine, _SERIES_IG_SPREAD, as_of)
    history = _get_series_history(engine, _SERIES_IG_SPREAD, as_of)
    z = compute_z_score(history, value) if history else None

    return FlowNode(
        id="ig_spread",
        label="Investment Grade Spread (OAS)",
        layer="credit",
        value=value,
        change_1d=changes.get("change_1d"),
        change_1w=changes.get("change_1w"),
        change_1m=changes.get("change_1m"),
        z_score=z,
        confidence=confidence,
        unit="bps",
        source=f"FRED:{_SERIES_IG_SPREAD}",
    )


def _build_money_market_node() -> FlowNode:
    """Money market funds node (estimated, no live series yet)."""
    return FlowNode(
        id="money_market_funds",
        label="Money Market Funds",
        layer="credit",
        value=_EST_MONEY_MARKET_FUNDS,
        confidence="estimated",
        unit="USD",
        source="estimate",
        metadata={"note": "No live series; placeholder ~$6T"},
    )


def _build_repo_stress_node(engine: Engine, as_of: date) -> FlowNode:
    """Repo market stress proxy from SOFR rate or RRP volume.

    High SOFR relative to fed funds = funding stress.
    Falling RRP = reserves draining = potential stress.
    """
    sofr_val = _get_series_latest(engine, _SERIES_SOFR, as_of)
    rrp_val = _get_series_latest(engine, _SERIES_RRP, as_of)

    # Use SOFR as the primary value; fall back to RRP-derived proxy
    if sofr_val is not None:
        value = sofr_val
        confidence = "confirmed"
        source = f"FRED:{_SERIES_SOFR}"
        series_for_changes = _SERIES_SOFR
    elif rrp_val is not None:
        value = rrp_val
        confidence = "derived"
        source = f"FRED:{_SERIES_RRP}"
        series_for_changes = _SERIES_RRP
    else:
        value = None
        confidence = "estimated"
        source = "estimate"
        series_for_changes = None

    changes = compute_changes(engine, series_for_changes, as_of) if series_for_changes else {}
    history = _get_series_history(engine, series_for_changes, as_of) if series_for_changes else []
    z = compute_z_score(history, value) if value is not None and history else None

    return FlowNode(
        id="repo_stress",
        label="Repo Market Stress",
        layer="credit",
        value=value,
        change_1d=changes.get("change_1d"),
        change_1w=changes.get("change_1w"),
        change_1m=changes.get("change_1m"),
        z_score=z,
        confidence=confidence,
        unit="pct" if sofr_val is not None else "USD",
        source=source,
    )


def _build_cds_composite_node(engine: Engine, as_of: date) -> FlowNode:
    """Build a composite CDS node from the full credit spread term structure.

    Captures HY/IG compression, CCC distress gradient, and spread momentum
    from the CDS tracker module. Value = HY/IG compression ratio (risk appetite).
    """
    try:
        from intelligence.cds_tracker import build_cds_dashboard
        dashboard = build_cds_dashboard(engine, as_of)

        return FlowNode(
            id="cds_composite",
            label="CDS Composite",
            layer="credit",
            value=dashboard.hy_ig_compression,
            confidence="confirmed" if any(s.confidence == "confirmed" for s in dashboard.spreads) else "derived",
            unit="ratio",
            source="FRED:BAML_OAS+YF:HYG/LQD/TLT",
            metadata={
                "regime": dashboard.regime,
                "term_slope": dashboard.term_slope,
                "spread_momentum": dashboard.spread_momentum,
                "narrative": dashboard.narrative,
                "hy_oas": next((s.value for s in dashboard.spreads if s.series_key == "hy"), None),
                "ig_oas": next((s.value for s in dashboard.spreads if s.series_key == "ig"), None),
                "ccc_oas": next((s.value for s in dashboard.spreads if s.series_key == "ccc"), None),
                "ccc_z": next((s.z_score_2y for s in dashboard.spreads if s.series_key == "ccc"), None),
            },
        )
    except Exception as exc:
        log.warning("CDS composite node failed: {}", exc)
        return FlowNode(
            id="cds_composite",
            label="CDS Composite",
            layer="credit",
            value=None,
            confidence="stale",
            source="cds_tracker:error",
        )


# ── Stress & Regime ───────────────────────────────────────────────────


def _compute_credit_stress(hy_node: FlowNode) -> float:
    """Derive a 0-1 stress score primarily from HY spread z-score.

    z > 1   = elevated  -> 0.3-0.5
    z > 2   = high      -> 0.5-0.8
    z > 3   = crisis    -> 0.8-1.0
    Also uses absolute spread level as a secondary check.
    """
    stress = 0.0

    # Z-score component (0-0.7)
    if hy_node.z_score is not None:
        z = max(0.0, hy_node.z_score)  # only positive z = widening = stress
        stress += min(0.7, z / 4.0)

    # Absolute spread component (0-0.3)
    if hy_node.value is not None:
        if hy_node.value >= _HY_SPREAD_CRISIS:
            stress += 0.3
        elif hy_node.value >= _HY_SPREAD_ELEVATED:
            stress += 0.15

    return round(max(0.0, min(1.0, stress)), 4)


def _determine_regime(
    bank_credit_node: FlowNode, hy_node: FlowNode, ig_node: FlowNode,
) -> str:
    """Classify the credit regime.

    tightening: bank credit contracting AND spreads widening
    easing:     bank credit expanding AND spreads tightening
    neutral:    mixed signals
    transitioning: one signal flipping
    """
    credit_contracting = (
        bank_credit_node.change_1m is not None and bank_credit_node.change_1m < 0
    )
    credit_expanding = (
        bank_credit_node.change_1m is not None and bank_credit_node.change_1m > 0
    )

    # Spreads widening = HY or IG 1m change is positive
    spreads_widening = (
        (hy_node.change_1m is not None and hy_node.change_1m > 0)
        or (ig_node.change_1m is not None and ig_node.change_1m > 0)
    )
    spreads_tightening = (
        (hy_node.change_1m is not None and hy_node.change_1m < 0)
        and (ig_node.change_1m is not None and ig_node.change_1m < 0)
    )

    if credit_contracting and spreads_widening:
        return "tightening"
    if credit_expanding and spreads_tightening:
        return "easing"
    if credit_contracting or spreads_widening:
        return "transitioning"
    return "neutral"


# ── Layer Builder ─────────────────────────────────────────────────────


def build_credit_layer(
    engine: Engine, as_of: date | None = None,
) -> FlowLayer:
    """Build the credit junction point layer (order=1).

    Covers bank lending, corporate bond spreads, money markets,
    and repo stress.  Returns an immutable FlowLayer.
    """
    if as_of is None:
        as_of = date.today()

    log.info("Building credit layer as_of={}", as_of)

    # ── Build nodes ───────────────────────────────────────────────
    bank_credit_node = _build_bank_credit_node(engine, as_of)
    hy_node = _build_hy_spread_node(engine, as_of)
    ig_node = _build_ig_spread_node(engine, as_of)
    mm_node = _build_money_market_node()
    repo_node = _build_repo_stress_node(engine, as_of)
    cds_node = _build_cds_composite_node(engine, as_of)

    all_nodes = (bank_credit_node, hy_node, ig_node, mm_node, repo_node, cds_node)

    # ── Aggregates ────────────────────────────────────────────────
    # total_value = bank credit + money market (spread nodes are in bps, not USD)
    usd_nodes = [n for n in all_nodes if n.unit == "USD" and n.value is not None]
    total_value = round(sum(n.value for n in usd_nodes), 2) if usd_nodes else None

    # net_flow approximation from bank credit node (the main volume node)
    net_flow_1m = None
    if bank_credit_node.change_1m is not None and bank_credit_node.value is not None:
        net_flow_1m = round(bank_credit_node.value * bank_credit_node.change_1m, 2)

    stress = _compute_credit_stress(hy_node)
    regime = _determine_regime(bank_credit_node, hy_node, ig_node)
    confidence = dominant_confidence(list(all_nodes))

    layer = FlowLayer(
        id="credit",
        label="Credit",
        order=1,
        nodes=all_nodes,
        total_value_usd=total_value,
        net_flow_1m=net_flow_1m,
        stress_score=stress,
        regime=regime,
        confidence=confidence,
    )

    log.info(
        "Credit layer: {} nodes, total={}, stress={:.3f}, regime={}",
        len(all_nodes),
        _fmt_usd(total_value),
        stress,
        regime,
    )
    return layer


# ── Formatting ────────────────────────────────────────────────────────


def _fmt_usd(val: float | None) -> str:
    """Format a USD value for log output."""
    if val is None:
        return "N/A"
    if abs(val) >= 1e12:
        return f"${val / 1e12:.2f}T"
    if abs(val) >= 1e9:
        return f"${val / 1e9:.2f}B"
    return f"${val:,.0f}"
