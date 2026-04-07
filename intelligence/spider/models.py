"""Data models for the connection mapping spider."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class DiscoveredConnection:
    """A connection found by a source adapter."""

    target_name: str
    relationship: str
    evidence: list[dict[str, Any]]
    confidence_tier: int  # 1=hard_data, 2=public_record, 3=inferred, 4=rumor
    target_hint: dict[str, Any] = field(default_factory=dict)
    strength: float = 0.5


@dataclass(frozen=True)
class ConnectionMeta:
    """Lightweight connection metadata stored in the adjacency index."""

    relationship: str
    strength: float
    confidence_tier: int
    sources: list[str]


@dataclass(frozen=True)
class SpiderStats:
    """Current spider statistics."""

    total_actors: int
    total_connections: int
    by_degree: dict[int, int]
    by_source: dict[str, int]
    queue_depth: int
    max_degree_reached: int
