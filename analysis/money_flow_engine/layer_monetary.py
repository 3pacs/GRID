"""GRID -- Monetary Layer (Layer 1).

Central bank balance sheets, reverse repo, TGA, and global M2.
This is the most upstream layer -- the source of all liquidity.
"""

from __future__ import annotations

from datetime import date, timedelta

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


# ── Central Bank Configs ──────────────────────────────────────────────
# (series, estimated_usd fallback, GDP weight for policy-score weighting)
CENTRAL_BANKS = {
    "fed":  {"label": "Federal Reserve",  "series": "WALCL",            "estimated_usd": None,                "gdp_weight": 0.26},
    "ecb":  {"label": "ECB",              "series": "ecb_total_assets", "estimated_usd": 7_000_000_000_000,   "gdp_weight": 0.17},
    "boj":  {"label": "Bank of Japan",    "series": "boj_total_assets", "estimated_usd": 4_500_000_000_000,   "gdp_weight": 0.05},
    "pboc": {"label": "PBOC",             "series": None,               "estimated_usd": 6_000_000_000_000,   "gdp_weight": 0.18},
    "boe":  {"label": "Bank of England",  "series": "boe_total_assets", "estimated_usd": 1_000_000_000_000,   "gdp_weight": 0.03},
}

# Series used for US monetary plumbing
_SERIES_RRP = "RRPONTSYD"
_SERIES_TGA = "WTREGEN"
_SERIES_M2 = "M2SL"

_RATE_SERIES = {"fed": "FEDFUNDS", "ecb": "ecb_main_refi_rate", "boj": "boj_policy_rate", "boe": "boe_bank_rate"}


# ── Node Builders ─────────────────────────────────────────────────────

def _resolve_cb_node(
    engine: Engine, cb_id: str, cfg: dict, as_of: date,
) -> FlowNode:
    """Build a FlowNode for a single central bank balance sheet."""
    series = cfg["series"]
    live_val = _get_series_latest(engine, series, as_of) if series else None

    if live_val is not None:
        value = live_val
        confidence = "confirmed"
    else:
        value = cfg["estimated_usd"]
        confidence = "estimated"

    changes = compute_changes(engine, series, as_of) if series else {}
    history = _get_series_history(engine, series, as_of) if series else []
    z = compute_z_score(history, live_val) if live_val is not None and history else None

    source = f"FRED:{series}" if series and series.isupper() else (series or "estimate")

    return FlowNode(
        id=f"{cb_id}_balance_sheet",
        label=f"{cfg['label']} Balance Sheet",
        layer="monetary",
        value=value,
        change_1d=changes.get("change_1d"),
        change_1w=changes.get("change_1w"),
        change_1m=changes.get("change_1m"),
        z_score=z,
        confidence=confidence,
        unit="USD",
        source=source,
        metadata={"gdp_weight": cfg["gdp_weight"]},
    )


def _build_series_node(
    engine: Engine, as_of: date, *,
    node_id: str, label: str, series: str, fallback: float,
    confidence_override: str | None = None, metadata: dict | None = None,
) -> FlowNode:
    """Generic builder for a single-series monetary node (RRP, TGA, etc.)."""
    live_val = _get_series_latest(engine, series, as_of)
    value = live_val if live_val is not None else fallback
    confidence = "confirmed" if live_val is not None else "estimated"
    if confidence_override and live_val is not None:
        confidence = confidence_override

    changes = compute_changes(engine, series, as_of)
    history = _get_series_history(engine, series, as_of)
    z = compute_z_score(history, value) if history else None

    return FlowNode(
        id=node_id, label=label, layer="monetary", value=value,
        change_1d=changes.get("change_1d"),
        change_1w=changes.get("change_1w"),
        change_1m=changes.get("change_1m"),
        z_score=z, confidence=confidence, unit="USD",
        source=f"FRED:{series}", metadata=metadata or {},
    )


def _build_m2_node(engine: Engine, as_of: date) -> FlowNode:
    """Global M2 proxy node (US M2 * 4.5 multiplier)."""
    m2_val = _get_series_latest(engine, _SERIES_M2, as_of)
    global_m2 = (m2_val * 4.5) if m2_val is not None else 95_000_000_000_000

    changes = compute_changes(engine, _SERIES_M2, as_of)
    history = _get_series_history(engine, _SERIES_M2, as_of)
    z = compute_z_score(history, m2_val) if m2_val is not None and history else None

    return FlowNode(
        id="global_m2", label="Global M2 (est.)", layer="monetary",
        value=global_m2,
        change_1d=changes.get("change_1d"),
        change_1w=changes.get("change_1w"),
        change_1m=changes.get("change_1m"),
        z_score=z,
        confidence="derived" if m2_val is not None else "estimated",
        unit="USD", source=f"FRED:{_SERIES_M2}",
        metadata={"us_m2": m2_val},
    )


# ── Policy Stance ─────────────────────────────────────────────────────

def _infer_stance_score(
    engine: Engine, cb_id: str, as_of: date,
) -> float:
    """Return -1 (tightening) to +1 (easing) for a single CB.

    Compares the current rate to 90 days ago.  Returns 0 if data is missing.
    """
    rate_series = _RATE_SERIES.get(cb_id)
    if not rate_series:
        return 0.0

    current = _get_series_latest(engine, rate_series, as_of)
    past = _get_series_latest(engine, rate_series, as_of - timedelta(days=90))
    if current is None or past is None:
        return 0.0

    diff = current - past
    if diff > 0.10:
        return -1.0  # tightening
    if diff < -0.10:
        return 1.0  # easing
    return 0.0


# ── Stress Score ──────────────────────────────────────────────────────

def _compute_stress(
    rrp_node: FlowNode, tga_node: FlowNode,
) -> float:
    """Compute a 0-1 stress score from RRP and TGA signals.

    High stress = RRP spiking (draining reserves) AND TGA draining fast.
    """
    stress = 0.0

    # RRP z-score contribution: high z = reserves fleeing to RRP = stress
    if rrp_node.z_score is not None:
        stress += max(0.0, min(0.5, rrp_node.z_score / 4.0))

    # TGA drain rate: large negative 1m change = Treasury spending = liquidity
    # but a *spike* in TGA (Treasury hoarding cash) = tightening = stress
    tga_change = tga_node.change_1m
    if tga_change is not None:
        if tga_change > 0.05:
            stress += 0.3  # TGA building = cash drain from system
        elif tga_change < -0.05:
            stress -= 0.1  # TGA spending = injection

    return round(max(0.0, min(1.0, stress)), 4)


# ── Layer Builder ─────────────────────────────────────────────────────

def build_monetary_layer(
    engine: Engine, as_of: date | None = None,
) -> FlowLayer:
    """Build the monetary junction point layer (order=0).

    Resolves all central bank balance sheets, RRP, TGA, and global M2.
    Returns an immutable FlowLayer with computed aggregates.
    """
    if as_of is None:
        as_of = date.today()

    log.info("Building monetary layer as_of={}", as_of)

    # ── Build nodes ───────────────────────────────────────────────
    cb_nodes: list[FlowNode] = []
    for cb_id, cfg in CENTRAL_BANKS.items():
        try:
            node = _resolve_cb_node(engine, cb_id, cfg, as_of)
            cb_nodes.append(node)
        except Exception as exc:
            log.warning("Failed to resolve CB {}: {}", cb_id, exc)

    rrp_node = _build_series_node(
        engine, as_of, node_id="reverse_repo", label="Reverse Repo (RRP)",
        series=_SERIES_RRP, fallback=500_000_000_000,
    )
    tga_node = _build_series_node(
        engine, as_of, node_id="tga_balance", label="Treasury General Account",
        series=_SERIES_TGA, fallback=700_000_000_000,
    )
    m2_node = _build_m2_node(engine, as_of)

    all_nodes = tuple(cb_nodes + [rrp_node, tga_node, m2_node])

    # ── Aggregates ────────────────────────────────────────────────
    total_value = _sum_values(cb_nodes)
    net_flow_1m = _sum_change_1m(cb_nodes)

    # GDP-weighted policy score
    policy_score = _compute_policy_score(engine, as_of)

    # Stress from RRP + TGA
    stress = _compute_stress(rrp_node, tga_node)

    # Regime: simple heuristic from policy score
    if policy_score > 0.2:
        regime = "risk_on"
    elif policy_score < -0.2:
        regime = "risk_off"
    else:
        regime = "neutral"

    confidence = dominant_confidence(list(all_nodes))

    layer = FlowLayer(
        id="monetary",
        label="Monetary",
        order=0,
        nodes=all_nodes,
        total_value_usd=total_value,
        net_flow_1m=net_flow_1m,
        stress_score=stress,
        regime=regime,
        confidence=confidence,
    )

    log.info(
        "Monetary layer: {} nodes, total={}, net_flow_1m={}, stress={:.3f}",
        len(all_nodes),
        _fmt_usd(total_value),
        _fmt_usd(net_flow_1m),
        stress,
    )
    return layer


# ── Internal Aggregation Helpers ──────────────────────────────────────

def _sum_values(nodes: list[FlowNode]) -> float | None:
    """Sum node values, skipping None."""
    vals = [n.value for n in nodes if n.value is not None]
    return round(sum(vals), 2) if vals else None


def _sum_change_1m(nodes: list[FlowNode]) -> float | None:
    """Sum 1-month changes across nodes, skipping None.

    Because compute_changes returns *percentage* changes we multiply back
    by the node value to get an approximate absolute delta.
    """
    total = 0.0
    found_any = False
    for n in nodes:
        if n.change_1m is not None and n.value is not None:
            total += n.value * n.change_1m
            found_any = True
    return round(total, 2) if found_any else None


def _compute_policy_score(engine: Engine, as_of: date) -> float:
    """GDP-weighted average policy stance across all CBs (-1 to +1)."""
    weighted_sum = 0.0
    weight_total = 0.0
    for cb_id, cfg in CENTRAL_BANKS.items():
        stance = _infer_stance_score(engine, cb_id, as_of)
        w = cfg["gdp_weight"]
        weighted_sum += stance * w
        weight_total += w

    if weight_total == 0:
        return 0.0
    score = weighted_sum / weight_total
    return round(max(-1.0, min(1.0, score)), 4)


def _fmt_usd(val: float | None) -> str:
    """Format a USD value for log output."""
    if val is None:
        return "N/A"
    if abs(val) >= 1e12:
        return f"${val / 1e12:.2f}T"
    if abs(val) >= 1e9:
        return f"${val / 1e9:.2f}B"
    return f"${val:,.0f}"
