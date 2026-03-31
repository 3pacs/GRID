"""
GRID Intelligence — Actor Network data models.

Defines the core dataclasses used across the actor network modules.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Actor:
    """A named individual or entity in the global financial power structure."""

    id: str
    name: str
    tier: str          # 'sovereign', 'regional', 'institutional', 'individual'
    category: str      # 'central_bank', 'government', 'fund', 'corporation',
                       # 'insider', 'politician', 'activist', 'swf'
    title: str         # "Chair of Federal Reserve", "CEO of BlackRock"

    # Wealth & influence
    net_worth_estimate: float | None = None   # USD, from public filings
    aum: float | None = None                  # assets under management (funds)
    influence_score: float = 0.5              # 0-1, computed

    # Connections
    connections: list[dict] = field(default_factory=list)
    board_seats: list[str] = field(default_factory=list)
    political_affiliations: list[dict] = field(default_factory=list)

    # Behavior
    recent_actions: list[dict] = field(default_factory=list)
    known_positions: list[dict] = field(default_factory=list)
    motivation_model: str = "unknown"
    trust_score: float = 0.5

    # Metadata
    data_sources: list[str] = field(default_factory=list)
    credibility: str = "inferred"  # 'hard_data', 'public_record', 'rumor', 'inferred'


@dataclass
class WealthFlow:
    """A tracked movement of capital between actors/entities."""

    from_actor: str
    to_actor: str          # can be a sector, company, or individual
    amount_estimate: float
    confidence: str        # 'confirmed', 'likely', 'rumored'
    evidence: list[str] = field(default_factory=list)
    timestamp: str = ""
    implication: str = ""
