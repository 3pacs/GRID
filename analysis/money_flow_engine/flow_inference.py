"""GRID — Multi-Directional Flow Inference Engine.

Computes flow edges between junction point layers based on
observable data and structural relationships. Each edge maps
a specific channel through which capital moves.

The adjacency matrix is hardcoded (not inferred) — only pre-approved
flow channels can emit edges. This prevents spurious correlations
from generating false flow arrows.
"""
from __future__ import annotations

from .types import FlowEdge, FlowLayer, FlowNode

# ── Flow Channel Definitions ─────────────────────────────────────
# Each channel defines a structural relationship between two layers.
# Channels are directional: source_layer → target_layer.

FLOW_CHANNELS: tuple[dict, ...] = (
    # Monetary → Credit (rate transmission: CB rates set borrowing costs)
    {"source_layer": "monetary", "target_layer": "credit",
     "source_node": "fed_balance_sheet", "target_node": "bank_credit",
     "channel": "rate_transmission", "weight": 0.50,
     "label": "Fed balance sheet → bank lending capacity"},

    # Monetary → Market (QE/QT direct: CB asset purchases hit markets directly)
    {"source_layer": "monetary", "target_layer": "market",
     "source_node": "fed_balance_sheet", "target_node": "equities",
     "channel": "qe_qt_direct", "weight": 0.30,
     "label": "QE/QT → equity market liquidity"},

    # Monetary → Sovereign (FX intervention)
    {"source_layer": "monetary", "target_layer": "sovereign",
     "source_node": "global_m2", "target_node": "fx_reserves",
     "channel": "fx_intervention", "weight": 0.20,
     "label": "Global M2 → FX reserve accumulation"},

    # Credit → Market (lending into equities/bonds)
    {"source_layer": "credit", "target_layer": "market",
     "source_node": "bank_credit", "target_node": "equities",
     "channel": "credit_to_market", "weight": 0.35,
     "label": "Bank credit expansion → equity market inflows"},

    # Credit → Corporate (bank lending to corps)
    {"source_layer": "credit", "target_layer": "corporate",
     "source_node": "bank_credit", "target_node": "buyback_activity",
     "channel": "corporate_borrowing", "weight": 0.25,
     "label": "Bank lending → corporate buyback financing"},

    # Institutional → Market (13F rebalancing)
    {"source_layer": "institutional", "target_layer": "market",
     "source_node": "13f_aggregate", "target_node": "equities",
     "channel": "institutional_allocation", "weight": 0.40,
     "label": "Institutional 13F rebalancing → equity flows"},

    # Institutional → Corporate (PE/VC deployment)
    {"source_layer": "institutional", "target_layer": "corporate",
     "source_node": "sovereign_wealth", "target_node": "ma_activity",
     "channel": "strategic_investment", "weight": 0.15,
     "label": "SWF/pension → M&A and strategic investment"},

    # Market → Retail (margin usage tracks market)
    {"source_layer": "market", "target_layer": "retail",
     "source_node": "equities", "target_node": "margin_debt",
     "channel": "margin_leverage", "weight": 0.30,
     "label": "Equity rally → margin debt increase"},

    # Market → Crypto (risk-on correlation)
    {"source_layer": "market", "target_layer": "crypto",
     "source_node": "equities", "target_node": "btc_flows",
     "channel": "risk_correlation", "weight": 0.20,
     "label": "Risk-on sentiment → crypto inflows"},

    # Corporate → Market (buybacks = demand)
    {"source_layer": "corporate", "target_layer": "market",
     "source_node": "buyback_activity", "target_node": "equities",
     "channel": "buyback_demand", "weight": 0.25,
     "label": "Corporate buybacks → equity demand"},

    # Sovereign → Market (FX reserve allocation)
    {"source_layer": "sovereign", "target_layer": "market",
     "source_node": "foreign_treasury_holdings", "target_node": "bonds",
     "channel": "reserve_allocation", "weight": 0.30,
     "label": "Foreign CB Treasury holdings → bond demand"},

    # Sovereign → Monetary (CB gold buying)
    {"source_layer": "sovereign", "target_layer": "monetary",
     "source_node": "fx_reserves", "target_node": "global_m2",
     "channel": "reserve_diversification", "weight": 0.10,
     "label": "FX reserve diversification → liquidity shifts"},

    # Retail → Market (retail options flow)
    {"source_layer": "retail", "target_layer": "market",
     "source_node": "retail_fund_flows", "target_node": "etf_flows",
     "channel": "retail_flow", "weight": 0.25,
     "label": "Retail fund flows → ETF demand"},

    # Retail → Crypto (retail speculation)
    {"source_layer": "retail", "target_layer": "crypto",
     "source_node": "prediction_markets", "target_node": "btc_flows",
     "channel": "retail_speculation", "weight": 0.15,
     "label": "Retail risk appetite → crypto flows"},

    # Crypto → Market (stablecoin ↔ fiat)
    {"source_layer": "crypto", "target_layer": "market",
     "source_node": "stablecoin_supply", "target_node": "equities",
     "channel": "stablecoin_bridge", "weight": 0.10,
     "label": "Stablecoin supply changes → fiat market flows"},
)

# ── Confidence ordering (higher = stronger) ──────────────────────
_CONF_RANK: dict[str, int] = {
    "confirmed": 3,
    "derived": 2,
    "estimated": 1,
    "rumored": 0,
}
_RANK_CONF: dict[int, str] = {v: k for k, v in _CONF_RANK.items()}

_MONTHLY_VALUE_FRACTION = 0.01  # 1% of value as monthly flow proxy


def infer_flow_edges(layers: tuple[FlowLayer, ...]) -> tuple[FlowEdge, ...]:
    """Compute flow edges between layers based on structural channels.

    For each defined channel, finds the source and target nodes in the
    provided layers and computes an estimated flow volume based on:
    1. The source node's change_1m (preferred) or a fraction of value
    2. The channel weight (structural proportion)
    3. The lower confidence of the two endpoint nodes

    Returns a tuple of FlowEdge objects (immutable).
    """
    # Build lookup: (layer_id, node_id) → FlowNode
    node_lookup: dict[tuple[str, str], FlowNode] = {}
    for layer in layers:
        for node in layer.nodes:
            node_lookup[(layer.id, node.id)] = node

    edges: list[FlowEdge] = []

    for ch in FLOW_CHANNELS:
        src_node = node_lookup.get((ch["source_layer"], ch["source_node"]))
        tgt_node = node_lookup.get((ch["target_layer"], ch["target_node"]))

        if src_node is None or tgt_node is None:
            continue  # node not present in this build (data unavailable)

        # Estimate flow volume from source node
        raw_flow, direction = _estimate_flow(src_node, ch["weight"])

        # Edge confidence = lower of source and target
        edge_conf = _RANK_CONF[min(
            _CONF_RANK.get(src_node.confidence, 1),
            _CONF_RANK.get(tgt_node.confidence, 1),
        )]

        edges.append(FlowEdge(
            source_node=ch["source_node"],
            target_node=ch["target_node"],
            source_layer=ch["source_layer"],
            target_layer=ch["target_layer"],
            value_usd=round(raw_flow, 2),
            direction=direction,
            confidence=edge_conf,
            label=ch["label"],
            channel=ch["channel"],
        ))

    return tuple(edges)


def _estimate_flow(node: FlowNode, weight: float) -> tuple[float, str]:
    """Derive (flow_usd, direction) from a source node and channel weight."""
    if node.change_1m is not None and node.change_1m != 0:
        return abs(node.change_1m) * weight, "inflow" if node.change_1m > 0 else "outflow"
    if node.value is not None:
        return abs(node.value) * _MONTHLY_VALUE_FRACTION * weight, "inflow"
    return 0.0, "inflow"
