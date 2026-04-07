"""In-memory actor graph with microsecond traversal.

Holds all actors and connections in RAM. Provides BFS, shortest path,
subgraph extraction, and name resolution. Thread-safe for concurrent reads.
"""

from __future__ import annotations

import json
import threading
from collections import defaultdict
from typing import Any, Optional

from loguru import logger as log

from intelligence.spider.models import ConnectionMeta


def _normalize_name(name: str) -> str:
    """Lowercase, strip whitespace, collapse spaces."""
    return " ".join(name.lower().split())


def _parse_json(val: Any) -> list:
    """Safely parse a JSONB field."""
    if val is None:
        return []
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            parsed = json.loads(val)
            return parsed if isinstance(parsed, list) else [parsed]
        except (json.JSONDecodeError, TypeError):
            return []
    return []


class GraphEngine:
    """In-memory actor graph engine.

    Data structures:
        _actors:    dict[actor_id -> actor_dict]
        _adj:       dict[actor_id -> dict[neighbor_id -> ConnectionMeta]]
        _names:     dict[normalized_name -> actor_id]
    """

    def __init__(self) -> None:
        self._actors: dict[str, dict[str, Any]] = {}
        self._adj: dict[str, dict[str, ConnectionMeta]] = defaultdict(dict)
        self._names: dict[str, str] = {}
        self._lock = threading.RLock()

    # -- Mutation (called by spider daemon, behind lock) ---------------

    def add_actor(self, actor_id: str, data: dict[str, Any]) -> None:
        """Add or update an actor in the graph."""
        with self._lock:
            self._actors[actor_id] = data
            name = data.get("name", "")
            if name:
                self._names[_normalize_name(name)] = actor_id

    def add_connection(
        self, actor_a: str, actor_b: str, meta: ConnectionMeta
    ) -> None:
        """Add a bidirectional connection between two actors."""
        with self._lock:
            self._adj[actor_a][actor_b] = meta
            self._adj[actor_b][actor_a] = meta

    def remove_actor(self, actor_id: str) -> None:
        """Remove an actor and all its connections."""
        with self._lock:
            for neighbor in list(self._adj.get(actor_id, {})):
                self._adj[neighbor].pop(actor_id, None)
            self._adj.pop(actor_id, None)
            data = self._actors.pop(actor_id, None)
            if data and data.get("name"):
                self._names.pop(_normalize_name(data["name"]), None)

    # -- Read (thread-safe, no lock needed for dict reads) -------------

    @property
    def actor_count(self) -> int:
        return len(self._actors)

    @property
    def connection_count(self) -> int:
        return sum(len(neighbors) for neighbors in self._adj.values()) // 2

    def has_actor(self, actor_id: str) -> bool:
        return actor_id in self._actors

    def get_actor(self, actor_id: str) -> Optional[dict[str, Any]]:
        return self._actors.get(actor_id)

    def get_neighbors(self, actor_id: str) -> dict[str, ConnectionMeta]:
        """Return {neighbor_id: ConnectionMeta} for an actor."""
        return dict(self._adj.get(actor_id, {}))

    def resolve_name(self, name: str) -> Optional[str]:
        """Look up actor_id by name (case-insensitive)."""
        return self._names.get(_normalize_name(name))
