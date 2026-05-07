"""Composite-scored expansion queue for the spider daemon.

Ranks actors by: influence * w1 + evidence_density * w2 + frontier_ratio * w3.
Backed by an in-memory heap. Syncs to spider_queue table for persistence.
"""

from __future__ import annotations

import heapq
from typing import Optional



class PriorityQueue:
    """Max-priority queue for spider expansion targets."""

    def __init__(
        self,
        w_influence: float = 0.4,
        w_evidence: float = 0.3,
        w_frontier: float = 0.3,
    ) -> None:
        self._w = (w_influence, w_evidence, w_frontier)
        self._heap: list[tuple[float, str]] = []
        self._pending: set[str] = set()
        self._done: set[str] = set()

    def compute_score(
        self,
        influence: float = 0.0,
        evidence_density: float = 0.0,
        frontier_ratio: float = 0.0,
    ) -> float:
        """Compute composite priority score."""
        return (
            influence * self._w[0]
            + evidence_density * self._w[1]
            + frontier_ratio * self._w[2]
        )

    def push(self, actor_id: str, priority: float) -> None:
        """Add an actor to the queue (skip if already done or pending)."""
        if actor_id in self._done or actor_id in self._pending:
            return
        heapq.heappush(self._heap, (-priority, actor_id))
        self._pending.add(actor_id)

    def pop(self) -> Optional[str]:
        """Pop highest-priority actor. Returns None if empty."""
        while self._heap:
            neg_pri, actor_id = heapq.heappop(self._heap)
            if actor_id in self._pending:
                self._pending.discard(actor_id)
                return actor_id
        return None

    def mark_done(
        self,
        actor_id: str,
        connections_found: int = 0,
        actors_created: int = 0,
    ) -> None:
        """Mark an actor as fully explored."""
        self._pending.discard(actor_id)
        self._done.add(actor_id)

    @property
    def depth(self) -> int:
        return len(self._pending)

    @property
    def total_done(self) -> int:
        return len(self._done)
