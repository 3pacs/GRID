"""Entity resolution — matches discovered names to existing actors.

Uses exact match → fuzzy match → LLM disambiguation (future).
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any, Optional

from loguru import logger as log

from intelligence.spider.graph_engine import GraphEngine


def _normalize(name: str) -> str:
    """Normalize a name: lowercase, strip accents, collapse whitespace."""
    name = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))
    name = name.lower().strip()
    name = re.sub(r"[^a-z0-9\s]", "", name)
    return " ".join(name.split())


def _levenshtein(a: str, b: str) -> int:
    """Compute Levenshtein edit distance between two strings."""
    if len(a) < len(b):
        return _levenshtein(b, a)
    if len(b) == 0:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a):
        curr = [i + 1]
        for j, cb in enumerate(b):
            cost = 0 if ca == cb else 1
            curr.append(min(curr[j] + 1, prev[j + 1] + 1, prev[j] + cost))
        prev = curr
    return prev[len(b)]


class EntityResolver:
    """Resolve actor names to existing graph IDs or generate new ones."""

    def __init__(self, graph: GraphEngine, max_distance: int = 8) -> None:
        self._graph = graph
        self._max_distance = max_distance

    def resolve(self, name: str, hint: dict[str, Any]) -> Optional[str]:
        """Try to match a name to an existing actor. Returns actor_id or None."""
        exact = self._graph.resolve_name(name)
        if exact:
            return exact

        normalized = _normalize(name)
        best_id: Optional[str] = None
        best_dist = self._max_distance + 1

        for known_name, actor_id in self._graph._names.items():
            dist = _levenshtein(normalized, _normalize(known_name))
            if dist < best_dist:
                best_dist = dist
                best_id = actor_id

        if best_id and best_dist <= self._max_distance:
            log.debug("Fuzzy match: '{raw}' → '{match}' (dist={d})", raw=name, match=best_id, d=best_dist)
            return best_id

        return None

    def generate_id(self, name: str, category: str) -> str:
        """Generate a new actor ID from name and category."""
        slug = _normalize(name).replace(" ", "_")
        return f"{category}_{slug}"
