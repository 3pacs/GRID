"""
Power Mapper — unified power-mapping layer over multiple relationship sources.

Translates raw relationship edges from LittleSis, ICIJ Offshore Leaks, board
interlock data, and the sector_networks YAML meshes into a single
``PowerEdge`` schema with type-weighted confidence scores, suitable for
driving the actor-network force-directed canvas and the lever_pullers
analytics.

Edge weights follow a "hard-to-hide" ranking: offshore structures and
direct ownership signal more durable power than transient business
relationships, because hidden and capital-backed ties are slower to
reverse than day-to-day commercial activity.
"""

from __future__ import annotations

from dataclasses import dataclass, field


# Edge-type weights in [0, 10]. Higher = harder to unwind / more
# power-durable. Ordering matters for tests: offshore >= business.
EDGE_WEIGHTS: dict[str, float] = {
    "ownership":   10.0,   # direct equity / voting control
    "offshore":     9.0,   # shell structures via ICIJ
    "board_seat":   8.0,   # board-level control
    "lobbying":     6.0,   # policy influence
    "donation":     5.0,   # political donation
    "business":     3.0,   # day-to-day commercial relationship
}


# LittleSis relationship category IDs → our canonical edge_type.
# https://littlesis.org/docs/relationships
_LITTLESIS_CATEGORY_MAP: dict[int, str] = {
    1: "board_seat",     # Position
    2: "board_seat",     # Education/ex-colleague — treat as governance
    3: "business",       # Membership
    4: "business",       # Family
    5: "donation",       # Donation / contribution
    6: "business",       # Transaction
    7: "lobbying",       # Lobbying
    8: "business",       # Social
    9: "business",       # Professional
    10: "ownership",     # Ownership / hierarchy
    11: "business",      # Generic
    12: "business",      # Other
}


def _categorize_littlesis(category_id: int | None) -> str:
    """Map a LittleSis category ID to our canonical edge_type.

    Unknown / missing categories collapse to 'business' so the mapper
    never emits a None edge_type (which would break downstream weight
    lookups).
    """
    if category_id is None:
        return "business"
    return _LITTLESIS_CATEGORY_MAP.get(int(category_id), "business")


@dataclass(frozen=True)
class PowerEdge:
    """One directed power relationship between two actors.

    Immutable by design so the force layout and deep_graph traversal can
    treat a batch of edges as a pure value set (no defensive copies).
    """

    source: str
    target: str
    edge_type: str
    weight: float
    data_source: str
    metadata: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.source or not self.target:
            raise ValueError("PowerEdge requires non-empty source and target")
        if self.edge_type not in EDGE_WEIGHTS and self.edge_type != "other":
            # Don't hard-fail — accept arbitrary custom types but they
            # won't resolve in EDGE_WEIGHTS.
            pass
        if not (0.0 <= self.weight <= 10.0):
            raise ValueError(f"PowerEdge weight must be in [0, 10], got {self.weight}")


def resolve_edge_weight(edge_type: str) -> float:
    """Return the canonical weight for ``edge_type`` or 1.0 if unknown."""
    return EDGE_WEIGHTS.get(edge_type, 1.0)
