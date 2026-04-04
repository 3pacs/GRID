"""GRID -- Money Flow Engine Types.

Frozen dataclasses representing the 8-layer junction point model.
All types are immutable to prevent hidden side effects.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class FlowNode:
    """A single node within a junction point layer."""

    id: str                          # e.g. "fed_balance_sheet"
    label: str                       # e.g. "Fed Balance Sheet"
    layer: str                       # e.g. "monetary"
    value: float | None = None       # current value in USD
    change_1d: float | None = None
    change_1w: float | None = None
    change_1m: float | None = None
    z_score: float | None = None     # vs 2-year history
    confidence: str = "estimated"    # "confirmed" | "derived" | "estimated" | "rumored"
    unit: str = "USD"
    source: str = ""                 # e.g. "FRED:WALCL"
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        return d


@dataclass(frozen=True)
class FlowLayer:
    """One of the 8 junction point layers."""

    id: str                          # e.g. "monetary"
    label: str                       # e.g. "Monetary"
    order: int                       # display order (0=leftmost in Sankey)
    nodes: tuple[FlowNode, ...] = ()  # immutable tuple of nodes
    total_value_usd: float | None = None
    net_flow_1m: float | None = None
    stress_score: float | None = None  # 0-1 composite
    regime: str = "neutral"          # "risk_on" | "risk_off" | "transitioning" | "neutral"
    confidence: str = "estimated"    # dominant confidence of child nodes

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "label": self.label,
            "order": self.order,
            "nodes": [n.to_dict() for n in self.nodes],
            "total_value_usd": self.total_value_usd,
            "net_flow_1m": self.net_flow_1m,
            "stress_score": self.stress_score,
            "regime": self.regime,
            "confidence": self.confidence,
        }


@dataclass(frozen=True)
class FlowEdge:
    """A directional flow between two nodes across layers."""

    source_node: str         # node ID
    target_node: str         # node ID
    source_layer: str        # layer ID
    target_layer: str        # layer ID
    value_usd: float         # estimated USD flow
    direction: str           # "inflow" | "outflow"
    confidence: str = "estimated"
    label: str = ""
    channel: str = ""        # e.g. "rate_transmission", "qe_direct"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FlowMap:
    """Complete 8-layer flow map with edges."""

    layers: tuple[FlowLayer, ...] = ()
    edges: tuple[FlowEdge, ...] = ()
    global_liquidity_total: float | None = None
    global_liquidity_change_1m: float | None = None
    global_policy_score: float | None = None
    generated_at: str = ""
    narrative: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "layers": [l.to_dict() for l in self.layers],
            "edges": [e.to_dict() for e in self.edges],
            "global_liquidity_total": self.global_liquidity_total,
            "global_liquidity_change_1m": self.global_liquidity_change_1m,
            "global_policy_score": self.global_policy_score,
            "generated_at": self.generated_at,
            "narrative": self.narrative,
        }
