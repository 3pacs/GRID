# Connection Mapping Spider — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a continuous connection mapping spider that discovers actor relationships up to 11 degrees of separation, keeps the full graph in RAM, and serves subgraph queries in microseconds.

**Architecture:** 4-layer system — in-memory graph engine (Python dicts, ~8GB for 5M actors), spider daemon (systemd service, 4-stage pipeline), 13 data sources across 4 confidence tiers, PostgreSQL persistence. Spider runs 24/7 expanding the network.

**Tech Stack:** Python 3.10+, SQLAlchemy, FastAPI, NetworkX (for community detection), loguru, pydantic-settings. Local LLM via existing `llm/router.py`.

**Spec:** `docs/superpowers/specs/2026-04-07-connection-mapping-spider-design.md`

---

## File Structure

```
intelligence/spider/
  __init__.py              — public exports
  graph_engine.py          — in-memory graph: BFS, shortest_path, subgraph, communities
  priority_queue.py        — composite-scored expansion queue backed by spider_queue table
  discovery.py             — orchestrator: fans out to source adapters, deduplicates
  entity_resolver.py       — fuzzy name matching + LLM disambiguation
  daemon.py                — main loop: pop queue → discover → resolve → enrich → repeat
  models.py                — DiscoveredConnection, ConnectionMeta, SpiderStats dataclasses
  sources/
    __init__.py            — BaseSourceAdapter protocol
    wikidata.py            — Wikidata SPARQL queries for structured relationships
    sec.py                 — Form 4 + 13F cross-referencing for co-insiders/co-holders
    political.py           — FEC + LDA + FARA + congressional connection extraction
    icij.py                — ICIJ offshore leak graph traversal

api/routers/
  intelligence_spider.py   — spider status, inject, prioritize, neighborhood, path endpoints

tests/
  test_graph_engine.py     — unit tests for in-memory graph operations
  test_priority_queue.py   — unit tests for priority scoring + queue operations
  test_entity_resolver.py  — unit tests for name matching
  test_discovery.py        — integration tests for discovery orchestrator

server_setup/
  grid-spider.service      — systemd unit file
```

---

### Task 1: Spider Data Models

**Files:**
- Create: `intelligence/spider/__init__.py`
- Create: `intelligence/spider/models.py`
- Test: `tests/test_spider_models.py`

- [ ] **Step 1: Write failing test for DiscoveredConnection**

```python
# tests/test_spider_models.py
"""Tests for spider data models."""

from intelligence.spider.models import (
    DiscoveredConnection,
    ConnectionMeta,
    SpiderStats,
)


def test_discovered_connection_defaults():
    dc = DiscoveredConnection(
        target_name="Larry Fink",
        relationship="ceo",
        evidence=[{"source": "sec_form4", "date": "2026-01-01"}],
        confidence_tier=1,
    )
    assert dc.target_name == "Larry Fink"
    assert dc.strength == 0.5
    assert dc.confidence_tier == 1
    assert dc.target_hint == {}


def test_connection_meta():
    cm = ConnectionMeta(
        relationship="board_member",
        strength=0.9,
        confidence_tier=1,
        sources=["sec_form4", "edgar"],
    )
    assert cm.relationship == "board_member"
    assert cm.sources == ["sec_form4", "edgar"]


def test_spider_stats():
    ss = SpiderStats(
        total_actors=1000,
        total_connections=5000,
        by_degree={0: 489, 1: 511},
        by_source={"sec_form4": 300, "wikidata": 700},
        queue_depth=150,
        max_degree_reached=2,
    )
    assert ss.total_actors == 1000
    assert ss.by_degree[0] == 489
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_spider_models.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'intelligence.spider'`

- [ ] **Step 3: Implement models**

```python
# intelligence/spider/__init__.py
"""GRID Connection Mapping Spider — discovers and maps actor relationships."""

from intelligence.spider.models import (
    ConnectionMeta,
    DiscoveredConnection,
    SpiderStats,
)

__all__ = ["ConnectionMeta", "DiscoveredConnection", "SpiderStats"]
```

```python
# intelligence/spider/models.py
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_spider_models.py -v`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add intelligence/spider/__init__.py intelligence/spider/models.py tests/test_spider_models.py
git commit -m "feat: add spider data models — DiscoveredConnection, ConnectionMeta, SpiderStats"
```

---

### Task 2: In-Memory Graph Engine — Core Data Structures

**Files:**
- Create: `intelligence/spider/graph_engine.py`
- Test: `tests/test_graph_engine.py`

- [ ] **Step 1: Write failing tests for graph loading and adjacency**

```python
# tests/test_graph_engine.py
"""Tests for the in-memory graph engine."""

import pytest
from intelligence.spider.graph_engine import GraphEngine
from intelligence.spider.models import ConnectionMeta


@pytest.fixture
def engine():
    """Create a graph engine with test data (no DB)."""
    ge = GraphEngine()
    ge.add_actor("fed_chair", {"name": "Jerome Powell", "tier": "sovereign", "category": "central_bank", "influence_score": 0.95})
    ge.add_actor("blackrock_ceo", {"name": "Larry Fink", "tier": "institutional", "category": "fund", "influence_score": 0.90})
    ge.add_actor("senator_a", {"name": "Senator A", "tier": "regional", "category": "politician", "influence_score": 0.60})
    ge.add_actor("company_x", {"name": "Company X", "tier": "institutional", "category": "corporation", "influence_score": 0.40})
    ge.add_connection("fed_chair", "blackrock_ceo", ConnectionMeta("policy_influence", 0.8, 1, ["fed_minutes"]))
    ge.add_connection("blackrock_ceo", "senator_a", ConnectionMeta("donates_to", 0.6, 1, ["fec"]))
    ge.add_connection("senator_a", "company_x", ConnectionMeta("trades_stock_of", 0.7, 1, ["congressional"]))
    return ge


def test_actor_count(engine):
    assert engine.actor_count == 4


def test_connection_count(engine):
    assert engine.connection_count == 3


def test_get_neighbors(engine):
    neighbors = engine.get_neighbors("blackrock_ceo")
    assert "fed_chair" in neighbors
    assert "senator_a" in neighbors
    assert len(neighbors) == 2


def test_get_neighbors_unknown_actor(engine):
    assert engine.get_neighbors("nonexistent") == {}


def test_has_actor(engine):
    assert engine.has_actor("fed_chair")
    assert not engine.has_actor("nonexistent")


def test_name_lookup(engine):
    assert engine.resolve_name("Jerome Powell") == "fed_chair"
    assert engine.resolve_name("jerome powell") == "fed_chair"
    assert engine.resolve_name("Unknown Person") is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_graph_engine.py -v`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Implement GraphEngine core**

```python
# intelligence/spider/graph_engine.py
"""In-memory actor graph with microsecond traversal.

Holds all actors and connections in RAM. Provides BFS, shortest path,
subgraph extraction, and name resolution. Thread-safe for concurrent reads.
"""

from __future__ import annotations

import threading
from collections import defaultdict
from typing import Any, Optional

from loguru import logger as log

from intelligence.spider.models import ConnectionMeta


def _normalize_name(name: str) -> str:
    """Lowercase, strip whitespace, collapse spaces."""
    return " ".join(name.lower().split())


class GraphEngine:
    """In-memory actor graph engine.

    Data structures:
        _actors:    dict[actor_id → actor_dict]
        _adj:       dict[actor_id → dict[neighbor_id → ConnectionMeta]]
        _names:     dict[normalized_name → actor_id]
    """

    def __init__(self) -> None:
        self._actors: dict[str, dict[str, Any]] = {}
        self._adj: dict[str, dict[str, ConnectionMeta]] = defaultdict(dict)
        self._names: dict[str, str] = {}
        self._lock = threading.RLock()

    # ── Mutation (called by spider daemon, behind lock) ──────────

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
            # Remove from adjacency
            for neighbor in list(self._adj.get(actor_id, {})):
                self._adj[neighbor].pop(actor_id, None)
            self._adj.pop(actor_id, None)
            # Remove from name index
            data = self._actors.pop(actor_id, None)
            if data and data.get("name"):
                self._names.pop(_normalize_name(data["name"]), None)

    # ── Read (thread-safe, no lock needed for dict reads) ────────

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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_graph_engine.py -v`
Expected: 6 passed

- [ ] **Step 5: Commit**

```bash
git add intelligence/spider/graph_engine.py tests/test_graph_engine.py
git commit -m "feat: add in-memory graph engine — actors, connections, name resolution"
```

---

### Task 3: Graph Engine — BFS and Shortest Path

**Files:**
- Modify: `intelligence/spider/graph_engine.py`
- Modify: `tests/test_graph_engine.py`

- [ ] **Step 1: Write failing tests for BFS and shortest path**

```python
# Append to tests/test_graph_engine.py

def test_bfs_depth_1(engine):
    result = engine.bfs("fed_chair", max_depth=1)
    assert "fed_chair" in result
    assert "blackrock_ceo" in result
    assert result["fed_chair"] == 0
    assert result["blackrock_ceo"] == 1
    assert "senator_a" not in result


def test_bfs_depth_2(engine):
    result = engine.bfs("fed_chair", max_depth=2)
    assert result["senator_a"] == 2
    assert "company_x" not in result


def test_bfs_depth_3(engine):
    result = engine.bfs("fed_chair", max_depth=3)
    assert result["company_x"] == 3


def test_bfs_max_depth_11(engine):
    """11 degrees should capture entire small graph."""
    result = engine.bfs("fed_chair", max_depth=11)
    assert len(result) == 4


def test_shortest_path(engine):
    path = engine.shortest_path("fed_chair", "company_x")
    assert path == ["fed_chair", "blackrock_ceo", "senator_a", "company_x"]


def test_shortest_path_same_actor(engine):
    path = engine.shortest_path("fed_chair", "fed_chair")
    assert path == ["fed_chair"]


def test_shortest_path_no_path(engine):
    engine.add_actor("isolated", {"name": "Isolated Actor", "tier": "individual", "category": "insider", "influence_score": 0.1})
    path = engine.shortest_path("fed_chair", "isolated")
    assert path is None


def test_subgraph(engine):
    nodes, links = engine.subgraph("blackrock_ceo", depth=1, max_nodes=100)
    node_ids = {n["id"] for n in nodes}
    assert "blackrock_ceo" in node_ids
    assert "fed_chair" in node_ids
    assert "senator_a" in node_ids
    assert len(links) >= 2
```

- [ ] **Step 2: Run tests to verify new ones fail**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_graph_engine.py -v`
Expected: 6 pass, 5 new FAIL

- [ ] **Step 3: Implement BFS, shortest_path, subgraph**

Add to `intelligence/spider/graph_engine.py` inside the `GraphEngine` class:

```python
    # ── Traversal ────────────────────────────────────────────────

    def bfs(self, start: str, max_depth: int = 11) -> dict[str, int]:
        """Breadth-first search. Returns {actor_id: degree} for all reachable actors."""
        if start not in self._actors:
            return {}
        visited: dict[str, int] = {start: 0}
        queue: list[tuple[str, int]] = [(start, 0)]
        while queue:
            current, depth = queue.pop(0)
            if depth >= max_depth:
                continue
            for neighbor in self._adj.get(current, {}):
                if neighbor not in visited:
                    visited[neighbor] = depth + 1
                    queue.append((neighbor, depth + 1))
        return visited

    def shortest_path(self, start: str, end: str) -> Optional[list[str]]:
        """Dijkstra shortest path using 1/strength as edge weight. Returns actor_id list or None."""
        import heapq

        if start == end:
            return [start]
        if start not in self._actors or end not in self._actors:
            return None

        dist: dict[str, float] = {start: 0.0}
        prev: dict[str, Optional[str]] = {start: None}
        heap: list[tuple[float, str]] = [(0.0, start)]

        while heap:
            d, current = heapq.heappop(heap)
            if current == end:
                # Reconstruct path
                path = []
                node: Optional[str] = end
                while node is not None:
                    path.append(node)
                    node = prev.get(node)
                return list(reversed(path))
            if d > dist.get(current, float("inf")):
                continue
            for neighbor, meta in self._adj.get(current, {}).items():
                weight = 1.0 / max(meta.strength, 0.01)
                new_dist = d + weight
                if new_dist < dist.get(neighbor, float("inf")):
                    dist[neighbor] = new_dist
                    prev[neighbor] = current
                    heapq.heappush(heap, (new_dist, neighbor))

        return None  # no path

    def subgraph(
        self, center: str, depth: int = 3, max_nodes: int = 2000
    ) -> tuple[list[dict], list[dict]]:
        """Extract a neighborhood subgraph for frontend rendering.

        Returns (nodes, links) where each node is the actor dict + id,
        and each link is {source, target, relationship, strength, confidence_tier}.
        """
        reachable = self.bfs(center, max_depth=depth)

        # Limit to max_nodes by influence
        if len(reachable) > max_nodes:
            sorted_actors = sorted(
                reachable.items(),
                key=lambda kv: self._actors.get(kv[0], {}).get("influence_score", 0),
                reverse=True,
            )
            reachable = dict(sorted_actors[:max_nodes])

        kept = set(reachable.keys())
        nodes = []
        for aid in kept:
            data = self._actors.get(aid, {})
            nodes.append({**data, "id": aid, "degree": reachable[aid]})

        links = []
        seen: set[tuple[str, str]] = set()
        for aid in kept:
            for neighbor, meta in self._adj.get(aid, {}).items():
                if neighbor in kept and (aid, neighbor) not in seen:
                    links.append({
                        "source": aid,
                        "target": neighbor,
                        "relationship": meta.relationship,
                        "strength": meta.strength,
                        "confidence_tier": meta.confidence_tier,
                    })
                    seen.add((aid, neighbor))
                    seen.add((neighbor, aid))

        return nodes, links
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_graph_engine.py -v`
Expected: 11 passed

- [ ] **Step 5: Commit**

```bash
git add intelligence/spider/graph_engine.py tests/test_graph_engine.py
git commit -m "feat: add BFS, shortest path, subgraph extraction to graph engine"
```

---

### Task 4: Graph Engine — Load from Postgres

**Files:**
- Modify: `intelligence/spider/graph_engine.py`
- Test: `tests/test_graph_engine.py`

- [ ] **Step 1: Write failing test for DB loading**

```python
# Append to tests/test_graph_engine.py
from unittest.mock import MagicMock, patch


def test_load_from_db_populates_graph():
    """Test that load_from_db reads actors and actor_connections tables."""
    ge = GraphEngine()

    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.connect.return_value.__enter__ = lambda s: mock_conn
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    # Mock actors query
    mock_conn.execute.side_effect = [
        # First call: actors
        MagicMock(fetchall=lambda: [
            ("a1", "Actor One", "sovereign", "central_bank", "Chair", None, None, 0.9, 0.8, "informed", "[]", "[]", "[]", "[]", '["fed"]', "hard_data", 0, "seed"),
            ("a2", "Actor Two", "institutional", "fund", "CEO", None, None, 0.7, 0.6, "profit", "[]", "[]", "[]", "[]", '["sec"]', "hard_data", 1, "form4"),
        ]),
        # Second call: connections
        MagicMock(fetchall=lambda: [
            ("a1", "a2", "policy_influence", 0.8, '[{"source": "fed"}]', 1),
        ]),
    ]

    ge.load_from_db(mock_engine)
    assert ge.actor_count == 2
    assert ge.connection_count == 1
    assert ge.has_actor("a1")
    assert "a2" in ge.get_neighbors("a1")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_graph_engine.py::test_load_from_db_populates_graph -v`
Expected: FAIL with `AttributeError: 'GraphEngine' object has no attribute 'load_from_db'`

- [ ] **Step 3: Implement load_from_db**

Add to `GraphEngine` class in `intelligence/spider/graph_engine.py`:

```python
    # ── Database Loading ─────────────────────────────────────────

    def load_from_db(self, engine: Any) -> None:
        """Load full actor graph from Postgres into RAM.

        Reads actors table + actor_connections table. Clears existing
        in-memory data first.
        """
        from sqlalchemy import text

        with self._lock:
            self._actors.clear()
            self._adj.clear()
            self._names.clear()

        # Load actors
        actor_count = 0
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT id, name, tier, category, title,
                       net_worth_estimate, aum, influence_score,
                       trust_score, motivation_model,
                       connections, known_positions, board_seats,
                       political_affiliations, data_sources, credibility,
                       degree, source
                FROM actors
                ORDER BY influence_score DESC
            """)).fetchall()
            for r in rows:
                self.add_actor(r[0], {
                    "name": r[1],
                    "tier": r[2],
                    "category": r[3],
                    "title": r[4] or "",
                    "net_worth_estimate": float(r[5]) if r[5] is not None else None,
                    "aum": float(r[6]) if r[6] is not None else None,
                    "influence_score": float(r[7]) if r[7] is not None else 0.5,
                    "trust_score": float(r[8]) if r[8] is not None else 0.5,
                    "motivation_model": r[9] or "unknown",
                    "data_sources": _parse_json(r[14]),
                    "credibility": r[15] or "inferred",
                    "degree": r[16] if r[16] is not None else 0,
                    "source": r[17] or "unknown",
                })
                actor_count += 1

        # Load connections from actor_connections table
        conn_count = 0
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT actor_a, actor_b, relationship, strength, evidence,
                       CASE
                           WHEN evidence::text LIKE '%hard_data%' THEN 1
                           WHEN evidence::text LIKE '%public_record%' THEN 2
                           WHEN evidence::text LIKE '%inferred%' THEN 3
                           ELSE 4
                       END AS confidence_tier
                FROM actor_connections
            """)).fetchall()
            for r in rows:
                evidence = _parse_json(r[4])
                sources = list({e.get("source", "unknown") for e in evidence if isinstance(e, dict)})
                self.add_connection(
                    r[0], r[1],
                    ConnectionMeta(
                        relationship=r[2],
                        strength=float(r[3]) if r[3] is not None else 0.5,
                        confidence_tier=int(r[5]) if r[5] is not None else 3,
                        sources=sources,
                    ),
                )
                conn_count += 1

        log.info(
            "Graph loaded from DB: {a} actors, {c} connections",
            a=actor_count, c=conn_count,
        )
```

Also add this module-level helper at the top of `graph_engine.py` (below imports):

```python
import json

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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_graph_engine.py -v`
Expected: 12 passed

- [ ] **Step 5: Commit**

```bash
git add intelligence/spider/graph_engine.py tests/test_graph_engine.py
git commit -m "feat: add Postgres loading to graph engine — actors + connections into RAM"
```

---

### Task 5: Priority Queue

**Files:**
- Create: `intelligence/spider/priority_queue.py`
- Test: `tests/test_priority_queue.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_priority_queue.py
"""Tests for the spider priority queue."""

from intelligence.spider.priority_queue import PriorityQueue


def test_score_actor():
    pq = PriorityQueue()
    score = pq.compute_score(
        influence=0.9,
        evidence_density=0.6,
        frontier_ratio=0.3,
    )
    expected = 0.9 * 0.4 + 0.6 * 0.3 + 0.3 * 0.3
    assert abs(score - expected) < 0.001


def test_push_and_pop():
    pq = PriorityQueue()
    pq.push("actor_a", priority=0.8)
    pq.push("actor_b", priority=0.95)
    pq.push("actor_c", priority=0.5)

    assert pq.pop() == "actor_b"
    assert pq.pop() == "actor_a"
    assert pq.pop() == "actor_c"


def test_pop_empty_returns_none():
    pq = PriorityQueue()
    assert pq.pop() is None


def test_depth():
    pq = PriorityQueue()
    pq.push("a", priority=0.5)
    pq.push("b", priority=0.9)
    assert pq.depth == 2
    pq.pop()
    assert pq.depth == 1


def test_mark_done():
    pq = PriorityQueue()
    pq.push("a", priority=0.5)
    pq.mark_done("a", connections_found=5, actors_created=2)
    assert pq.depth == 0
    assert pq.pop() is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_priority_queue.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement PriorityQueue**

```python
# intelligence/spider/priority_queue.py
"""Composite-scored expansion queue for the spider daemon.

Ranks actors by: influence * w1 + evidence_density * w2 + frontier_ratio * w3.
Backed by an in-memory heap. Syncs to spider_queue table for persistence.
"""

from __future__ import annotations

import heapq
from typing import Optional

from loguru import logger as log


class PriorityQueue:
    """Max-priority queue for spider expansion targets."""

    def __init__(
        self,
        w_influence: float = 0.4,
        w_evidence: float = 0.3,
        w_frontier: float = 0.3,
    ) -> None:
        self._w = (w_influence, w_evidence, w_frontier)
        self._heap: list[tuple[float, str]] = []  # (-priority, actor_id)
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_priority_queue.py -v`
Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add intelligence/spider/priority_queue.py tests/test_priority_queue.py
git commit -m "feat: add spider priority queue with composite scoring"
```

---

### Task 6: Source Adapter Protocol + Wikidata Adapter

**Files:**
- Create: `intelligence/spider/sources/__init__.py`
- Create: `intelligence/spider/sources/wikidata.py`
- Test: `tests/test_discovery.py`

- [ ] **Step 1: Write failing test for Wikidata adapter**

```python
# tests/test_discovery.py
"""Tests for spider source adapters."""

from unittest.mock import patch, MagicMock
from intelligence.spider.sources import BaseSourceAdapter
from intelligence.spider.sources.wikidata import WikidataAdapter
from intelligence.spider.models import DiscoveredConnection


def test_wikidata_adapter_is_source():
    adapter = WikidataAdapter()
    assert isinstance(adapter, BaseSourceAdapter)


@patch("intelligence.spider.sources.wikidata.requests.get")
def test_wikidata_discovers_connections(mock_get):
    """Mock the Wikidata SPARQL endpoint to return board seats."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "results": {
            "bindings": [
                {
                    "relatedLabel": {"value": "BlackRock Inc."},
                    "relatedDescription": {"value": "investment management corporation"},
                    "propLabel": {"value": "member of board of directors of"},
                },
                {
                    "relatedLabel": {"value": "Council on Foreign Relations"},
                    "relatedDescription": {"value": "think tank"},
                    "propLabel": {"value": "member of"},
                },
            ]
        }
    }
    mock_get.return_value = mock_response

    adapter = WikidataAdapter()
    connections = adapter.discover("Larry Fink", {"category": "fund"})

    assert len(connections) == 2
    assert all(isinstance(c, DiscoveredConnection) for c in connections)
    assert connections[0].target_name == "BlackRock Inc."
    assert connections[0].relationship == "member of board of directors of"
    assert connections[0].confidence_tier == 2  # public record
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_discovery.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement BaseSourceAdapter + WikidataAdapter**

```python
# intelligence/spider/sources/__init__.py
"""Source adapter protocol for the connection mapping spider."""

from __future__ import annotations

from typing import Any, Protocol

from intelligence.spider.models import DiscoveredConnection


class BaseSourceAdapter(Protocol):
    """Protocol that all source adapters must implement."""

    name: str

    def discover(
        self, actor_name: str, actor_hint: dict[str, Any]
    ) -> list[DiscoveredConnection]:
        """Discover connections for a given actor. Returns list of discovered connections."""
        ...
```

```python
# intelligence/spider/sources/wikidata.py
"""Wikidata SPARQL adapter — discovers structured relationships for public figures.

Queries Wikidata for board seats, employers, education, family, political party.
Confidence tier: 2 (public record).
"""

from __future__ import annotations

from typing import Any

import requests
from loguru import logger as log

from intelligence.spider.models import DiscoveredConnection

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"

# Relationship properties to query
_RELATIONSHIP_PROPS = [
    "P108",   # employer
    "P463",   # member of
    "P39",    # position held
    "P102",   # member of political party
    "P69",    # educated at
    "P22",    # father
    "P25",    # mother
    "P26",    # spouse
    "P1037",  # director/manager
    "P3320",  # board member
]


class WikidataAdapter:
    """Discover actor connections via Wikidata SPARQL."""

    name = "wikidata"

    def discover(
        self, actor_name: str, actor_hint: dict[str, Any]
    ) -> list[DiscoveredConnection]:
        """Query Wikidata for relationships of the given actor."""
        props_filter = " ".join(f"wd:{p}" for p in _RELATIONSHIP_PROPS)
        query = f"""
        SELECT ?relatedLabel ?relatedDescription ?propLabel WHERE {{
          ?person rdfs:label "{actor_name}"@en .
          VALUES ?prop {{ {props_filter} }}
          ?person ?prop ?related .
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
        }}
        LIMIT 50
        """
        try:
            resp = requests.get(
                WIKIDATA_SPARQL,
                params={"query": query, "format": "json"},
                headers={"User-Agent": "GRID-Spider/1.0"},
                timeout=15,
            )
            if resp.status_code != 200:
                log.debug("Wikidata query failed for {a}: HTTP {s}", a=actor_name, s=resp.status_code)
                return []

            bindings = resp.json().get("results", {}).get("bindings", [])
            connections: list[DiscoveredConnection] = []
            for b in bindings:
                target = b.get("relatedLabel", {}).get("value", "")
                rel = b.get("propLabel", {}).get("value", "")
                desc = b.get("relatedDescription", {}).get("value", "")
                if not target or not rel:
                    continue
                connections.append(DiscoveredConnection(
                    target_name=target,
                    relationship=rel,
                    strength=0.7,
                    confidence_tier=2,
                    target_hint={"description": desc} if desc else {},
                    evidence=[{
                        "source": "wikidata",
                        "url": f"https://www.wikidata.org/wiki/Special:Search/{actor_name}",
                        "excerpt": f"{actor_name} → {rel} → {target}",
                    }],
                ))
            log.debug("Wikidata: {n} connections for {a}", n=len(connections), a=actor_name)
            return connections

        except Exception as exc:
            log.debug("Wikidata adapter error for {a}: {e}", a=actor_name, e=str(exc))
            return []
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_discovery.py -v`
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add intelligence/spider/sources/__init__.py intelligence/spider/sources/wikidata.py tests/test_discovery.py
git commit -m "feat: add source adapter protocol + Wikidata SPARQL adapter"
```

---

### Task 7: Entity Resolver

**Files:**
- Create: `intelligence/spider/entity_resolver.py`
- Test: `tests/test_entity_resolver.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_entity_resolver.py
"""Tests for entity resolution."""

import pytest
from intelligence.spider.entity_resolver import EntityResolver
from intelligence.spider.graph_engine import GraphEngine


@pytest.fixture
def resolver():
    ge = GraphEngine()
    ge.add_actor("larry_fink", {"name": "Larry Fink", "tier": "institutional", "category": "fund", "influence_score": 0.9})
    ge.add_actor("jpow", {"name": "Jerome Powell", "tier": "sovereign", "category": "central_bank", "influence_score": 0.95})
    return EntityResolver(ge)


def test_exact_match(resolver):
    result = resolver.resolve("Larry Fink", {})
    assert result == "larry_fink"


def test_case_insensitive_match(resolver):
    result = resolver.resolve("larry fink", {})
    assert result == "larry_fink"


def test_fuzzy_match(resolver):
    result = resolver.resolve("Laurence D. Fink", {})
    assert result == "larry_fink"


def test_no_match_returns_none(resolver):
    result = resolver.resolve("Completely Unknown Person", {})
    assert result is None


def test_generate_id():
    resolver = EntityResolver(GraphEngine())
    actor_id = resolver.generate_id("Janet Yellen", "government")
    assert actor_id == "government_janet_yellen"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_entity_resolver.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement EntityResolver**

```python
# intelligence/spider/entity_resolver.py
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

    def __init__(self, graph: GraphEngine, max_distance: int = 5) -> None:
        self._graph = graph
        self._max_distance = max_distance

    def resolve(
        self, name: str, hint: dict[str, Any]
    ) -> Optional[str]:
        """Try to match a name to an existing actor.

        Returns actor_id if matched, None if no match found.
        """
        # 1. Exact name lookup (case-insensitive)
        exact = self._graph.resolve_name(name)
        if exact:
            return exact

        # 2. Fuzzy match against all known names
        normalized = _normalize(name)
        best_id: Optional[str] = None
        best_dist = self._max_distance + 1

        for known_name, actor_id in self._graph._names.items():
            dist = _levenshtein(normalized, _normalize(known_name))
            if dist < best_dist:
                best_dist = dist
                best_id = actor_id

        if best_id and best_dist <= self._max_distance:
            log.debug(
                "Fuzzy match: '{raw}' → '{match}' (dist={d})",
                raw=name, match=best_id, d=best_dist,
            )
            return best_id

        return None

    def generate_id(self, name: str, category: str) -> str:
        """Generate a new actor ID from name and category."""
        slug = _normalize(name).replace(" ", "_")
        return f"{category}_{slug}"
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_entity_resolver.py -v`
Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add intelligence/spider/entity_resolver.py tests/test_entity_resolver.py
git commit -m "feat: add entity resolver — exact match, fuzzy match, ID generation"
```

---

### Task 8: Discovery Orchestrator

**Files:**
- Create: `intelligence/spider/discovery.py`
- Modify: `tests/test_discovery.py`

- [ ] **Step 1: Write failing test for discovery orchestrator**

```python
# Append to tests/test_discovery.py

from intelligence.spider.discovery import DiscoveryOrchestrator
from intelligence.spider.graph_engine import GraphEngine
from intelligence.spider.entity_resolver import EntityResolver
from intelligence.spider.models import ConnectionMeta


def test_orchestrator_runs_adapters_and_resolves():
    """Orchestrator discovers connections and resolves entities."""
    ge = GraphEngine()
    ge.add_actor("larry_fink", {"name": "Larry Fink", "tier": "institutional", "category": "fund", "influence_score": 0.9})

    resolver = EntityResolver(ge)
    orchestrator = DiscoveryOrchestrator(graph=ge, resolver=resolver, adapters=[])

    # Manually inject a mock adapter
    class MockAdapter:
        name = "mock"
        def discover(self, actor_name, actor_hint):
            return [
                DiscoveredConnection(
                    target_name="BlackRock Inc.",
                    relationship="ceo",
                    strength=0.9,
                    confidence_tier=1,
                    evidence=[{"source": "mock"}],
                ),
            ]

    orchestrator._adapters = [MockAdapter()]

    new_actors, new_connections = orchestrator.expand("larry_fink")

    # Should have created a new actor for BlackRock
    assert len(new_actors) == 1
    assert new_actors[0]["name"] == "BlackRock Inc."

    # Should have added a connection
    assert len(new_connections) == 1
    assert new_connections[0][0] == "larry_fink"  # source
    assert new_connections[0][2].relationship == "ceo"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_discovery.py::test_orchestrator_runs_adapters_and_resolves -v`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Implement DiscoveryOrchestrator**

```python
# intelligence/spider/discovery.py
"""Discovery orchestrator — fans out to source adapters and deduplicates results.

For each target actor, queries all registered adapters in sequence,
deduplicates discovered connections by target name, resolves entities
against the existing graph, and returns new actors + new connections.
"""

from __future__ import annotations

from typing import Any

from loguru import logger as log

from intelligence.spider.entity_resolver import EntityResolver
from intelligence.spider.graph_engine import GraphEngine
from intelligence.spider.models import ConnectionMeta, DiscoveredConnection
from intelligence.spider.sources import BaseSourceAdapter


class DiscoveryOrchestrator:
    """Orchestrates connection discovery across all source adapters."""

    def __init__(
        self,
        graph: GraphEngine,
        resolver: EntityResolver,
        adapters: list[Any],
    ) -> None:
        self._graph = graph
        self._resolver = resolver
        self._adapters: list[Any] = adapters

    def expand(
        self, actor_id: str
    ) -> tuple[list[dict[str, Any]], list[tuple[str, str, ConnectionMeta]]]:
        """Expand an actor's connections using all adapters.

        Returns:
            (new_actors, new_connections) where:
            - new_actors: list of actor dicts that were created
            - new_connections: list of (source_id, target_id, ConnectionMeta)
        """
        actor_data = self._graph.get_actor(actor_id)
        if not actor_data:
            log.warning("Cannot expand unknown actor: {a}", a=actor_id)
            return [], []

        actor_name = actor_data.get("name", "")
        actor_hint = {
            "category": actor_data.get("category", ""),
            "tier": actor_data.get("tier", ""),
            "title": actor_data.get("title", ""),
        }

        # Collect discoveries from all adapters
        all_discovered: list[DiscoveredConnection] = []
        for adapter in self._adapters:
            try:
                results = adapter.discover(actor_name, actor_hint)
                all_discovered.extend(results)
            except Exception as exc:
                log.debug(
                    "Adapter {n} failed for {a}: {e}",
                    n=getattr(adapter, "name", "?"), a=actor_name, e=str(exc),
                )

        # Deduplicate by (target_name, relationship)
        seen: set[tuple[str, str]] = set()
        unique: list[DiscoveredConnection] = []
        for dc in all_discovered:
            key = (dc.target_name.lower().strip(), dc.relationship.lower().strip())
            if key not in seen:
                seen.add(key)
                unique.append(dc)

        # Resolve entities and build connections
        new_actors: list[dict[str, Any]] = []
        new_connections: list[tuple[str, str, ConnectionMeta]] = []

        for dc in unique:
            target_id = self._resolver.resolve(dc.target_name, dc.target_hint)

            if target_id is None:
                # Create new actor
                category = dc.target_hint.get("category", "corporation")
                target_id = self._resolver.generate_id(dc.target_name, category)
                actor_degree = actor_data.get("degree", 0)
                new_actor = {
                    "name": dc.target_name,
                    "tier": "institutional",
                    "category": category,
                    "title": dc.target_hint.get("description", ""),
                    "influence_score": 0.3,
                    "trust_score": 0.5,
                    "degree": actor_degree + 1,
                    "source": dc.evidence[0].get("source", "unknown") if dc.evidence else "unknown",
                    "credibility": _tier_to_credibility(dc.confidence_tier),
                    "data_sources": [dc.evidence[0].get("source", "unknown")] if dc.evidence else [],
                }
                self._graph.add_actor(target_id, new_actor)
                new_actors.append({**new_actor, "id": target_id})

            # Build connection
            sources = [e.get("source", "unknown") for e in dc.evidence if isinstance(e, dict)]
            meta = ConnectionMeta(
                relationship=dc.relationship,
                strength=dc.strength,
                confidence_tier=dc.confidence_tier,
                sources=sources,
            )
            self._graph.add_connection(actor_id, target_id, meta)
            new_connections.append((actor_id, target_id, meta))

        log.info(
            "Expanded {a}: {d} discovered, {n} new actors, {c} connections",
            a=actor_name, d=len(unique), n=len(new_actors), c=len(new_connections),
        )
        return new_actors, new_connections


def _tier_to_credibility(tier: int) -> str:
    return {1: "hard_data", 2: "public_record", 3: "inferred", 4: "rumor"}.get(tier, "inferred")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_discovery.py -v`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add intelligence/spider/discovery.py tests/test_discovery.py
git commit -m "feat: add discovery orchestrator — multi-source expansion with entity resolution"
```

---

### Task 9: Spider Daemon

**Files:**
- Create: `intelligence/spider/daemon.py`
- Create: `intelligence/spider/db.py`

- [ ] **Step 1: Write DB schema migration helpers**

```python
# intelligence/spider/db.py
"""Database operations for the spider — queue persistence and connection storage."""

from __future__ import annotations

from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from intelligence.spider.models import ConnectionMeta


def ensure_spider_tables(engine: Engine) -> None:
    """Create spider_queue and spider_runs tables if they don't exist."""
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS spider_queue (
                actor_id        TEXT PRIMARY KEY,
                priority        NUMERIC NOT NULL DEFAULT 0,
                degree          INT NOT NULL DEFAULT 0,
                status          TEXT NOT NULL DEFAULT 'pending',
                queued_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                started_at      TIMESTAMPTZ,
                completed_at    TIMESTAMPTZ,
                sources_checked JSONB DEFAULT '[]',
                connections_found INT DEFAULT 0,
                actors_created  INT DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_spider_queue_priority
                ON spider_queue (priority DESC) WHERE status = 'pending';

            CREATE TABLE IF NOT EXISTS spider_runs (
                id               SERIAL PRIMARY KEY,
                started_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                completed_at     TIMESTAMPTZ,
                actors_processed INT DEFAULT 0,
                connections_found INT DEFAULT 0,
                new_actors       INT DEFAULT 0,
                max_degree_reached INT DEFAULT 0,
                errors           JSONB DEFAULT '[]'
            );
        """))
        conn.commit()
    log.info("Spider tables ensured")


def save_actor(engine: Engine, actor_id: str, data: dict[str, Any]) -> None:
    """Upsert an actor into the actors table."""
    import json

    with engine.connect() as conn:
        conn.execute(
            text("""
                INSERT INTO actors (id, name, tier, category, title, influence_score,
                    trust_score, degree, source, credibility, data_sources, updated_at)
                VALUES (:id, :name, :tier, :category, :title, :influence,
                    :trust, :degree, :source, :credibility, :data_sources, NOW())
                ON CONFLICT (id) DO UPDATE SET
                    influence_score = GREATEST(actors.influence_score, EXCLUDED.influence_score),
                    data_sources = EXCLUDED.data_sources,
                    updated_at = NOW()
            """),
            {
                "id": actor_id,
                "name": data.get("name", ""),
                "tier": data.get("tier", "institutional"),
                "category": data.get("category", "corporation"),
                "title": data.get("title", ""),
                "influence": data.get("influence_score", 0.3),
                "trust": data.get("trust_score", 0.5),
                "degree": data.get("degree", 0),
                "source": data.get("source", "spider"),
                "credibility": data.get("credibility", "inferred"),
                "data_sources": json.dumps(data.get("data_sources", [])),
            },
        )
        conn.commit()


def save_connection(
    engine: Engine, actor_a: str, actor_b: str, meta: ConnectionMeta
) -> None:
    """Upsert a connection into actor_connections."""
    import json

    with engine.connect() as conn:
        conn.execute(
            text("""
                INSERT INTO actor_connections (actor_a, actor_b, relationship, strength, evidence)
                VALUES (:a, :b, :rel, :strength, :evidence)
                ON CONFLICT (actor_a, actor_b, relationship)
                DO UPDATE SET
                    strength = GREATEST(actor_connections.strength, EXCLUDED.strength),
                    evidence = EXCLUDED.evidence
            """),
            {
                "a": actor_a,
                "b": actor_b,
                "rel": meta.relationship,
                "strength": meta.strength,
                "evidence": json.dumps([{"source": s} for s in meta.sources]),
            },
        )
        conn.commit()
```

- [ ] **Step 2: Write the spider daemon**

```python
# intelligence/spider/daemon.py
"""Spider daemon — continuous connection mapping loop.

Runs as a systemd service. Pops actors from the priority queue,
expands their connections via the discovery orchestrator, persists
results to Postgres, and updates the in-memory graph.

Usage:
    python -m intelligence.spider.daemon
"""

from __future__ import annotations

import sys
import time
from typing import Any

from loguru import logger as log


def run_spider(max_rounds: int = 0, sleep_between: float = 2.0) -> None:
    """Main spider loop.

    Args:
        max_rounds: 0 = run forever. >0 = stop after N expansions.
        sleep_between: seconds to sleep between expansions.
    """
    # Late imports to avoid circular dependencies at module level
    sys.path.insert(0, ".")
    from db import get_engine

    from intelligence.spider.db import ensure_spider_tables, save_actor, save_connection
    from intelligence.spider.discovery import DiscoveryOrchestrator
    from intelligence.spider.entity_resolver import EntityResolver
    from intelligence.spider.graph_engine import GraphEngine
    from intelligence.spider.priority_queue import PriorityQueue
    from intelligence.spider.sources.wikidata import WikidataAdapter

    engine = get_engine()
    ensure_spider_tables(engine)

    # Initialize graph from DB
    graph = GraphEngine()
    log.info("Loading actor graph from database...")
    graph.load_from_db(engine)
    log.info(
        "Graph loaded: {a} actors, {c} connections",
        a=graph.actor_count, c=graph.connection_count,
    )

    # Initialize components
    resolver = EntityResolver(graph)
    adapters = [WikidataAdapter()]
    orchestrator = DiscoveryOrchestrator(graph=graph, resolver=resolver, adapters=adapters)
    queue = PriorityQueue()

    # Seed queue with unexplored actors (sorted by influence)
    _seed_queue(graph, queue)
    log.info("Spider queue seeded: {d} actors pending", d=queue.depth)

    # Main loop
    rounds = 0
    while True:
        actor_id = queue.pop()
        if actor_id is None:
            log.info("Queue empty — spider sleeping 60s before re-seeding")
            time.sleep(60)
            _seed_queue(graph, queue)
            continue

        log.info(
            "Expanding: {a} (queue={q}, done={d})",
            a=actor_id, q=queue.depth, d=queue.total_done,
        )

        try:
            new_actors, new_connections = orchestrator.expand(actor_id)

            # Persist to DB
            for actor_data in new_actors:
                save_actor(engine, actor_data["id"], actor_data)

            for source_id, target_id, meta in new_connections:
                save_connection(engine, source_id, target_id, meta)

            # Queue new actors for expansion
            for actor_data in new_actors:
                degree = actor_data.get("degree", 0)
                if degree <= 11:
                    influence = actor_data.get("influence_score", 0.3)
                    queue.push(actor_data["id"], priority=queue.compute_score(
                        influence=influence,
                        evidence_density=len(actor_data.get("data_sources", [])) / 10.0,
                        frontier_ratio=1.0,  # new actor = fully unexplored
                    ))

            queue.mark_done(
                actor_id,
                connections_found=len(new_connections),
                actors_created=len(new_actors),
            )

        except Exception as exc:
            log.error("Spider expansion failed for {a}: {e}", a=actor_id, e=str(exc))
            queue.mark_done(actor_id)

        rounds += 1
        if max_rounds > 0 and rounds >= max_rounds:
            log.info("Spider completed {r} rounds, stopping", r=rounds)
            break

        time.sleep(sleep_between)


def _seed_queue(graph: GraphEngine, queue: PriorityQueue) -> None:
    """Seed the queue with all actors that haven't been fully explored."""
    for actor_id, data in graph._actors.items():
        if not queue._done.__contains__(actor_id):
            influence = data.get("influence_score", 0.3)
            evidence = len(data.get("data_sources", []))
            neighbors = len(graph.get_neighbors(actor_id))
            total_possible = max(neighbors, 1)
            frontier = 1.0 - (neighbors / max(total_possible, 10))
            queue.push(actor_id, priority=queue.compute_score(
                influence=influence,
                evidence_density=min(evidence / 10.0, 1.0),
                frontier_ratio=frontier,
            ))


if __name__ == "__main__":
    log.remove()
    log.add(sys.stderr, level="INFO")
    run_spider()
```

- [ ] **Step 3: Commit**

```bash
git add intelligence/spider/daemon.py intelligence/spider/db.py
git commit -m "feat: add spider daemon — main loop, DB persistence, queue seeding"
```

---

### Task 10: API Endpoints

**Files:**
- Create: `api/routers/intelligence_spider.py`
- Modify: `api/main.py` (add router registration)

- [ ] **Step 1: Write the spider API router**

```python
# api/routers/intelligence_spider.py
"""Spider API endpoints — status, stats, inject, neighborhood, path finding."""

from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger as log
from pydantic import BaseModel, Field

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(prefix="/api/v1/intelligence", tags=["spider"])

# Shared graph engine instance — loaded by startup preload in main.py
_graph_engine = None


def get_graph():
    """Return the shared graph engine instance."""
    if _graph_engine is None:
        raise HTTPException(503, "Graph engine not initialized yet — still loading")
    return _graph_engine


# ── Schemas ──────────────────────────────────────────────────────

class InjectActorRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=500)
    category: str = Field(default="corporation", max_length=50)
    tier: str = Field(default="institutional", max_length=50)
    title: str = Field(default="", max_length=500)
    connections: list[dict[str, Any]] = Field(default_factory=list)
    notes: str = Field(default="", max_length=2000)


# ── Endpoints ────────────────────────────────────────────────────

@router.get("/actor/{actor_id}/neighborhood")
async def get_neighborhood(
    actor_id: str,
    depth: int = Query(3, ge=1, le=11),
    max_nodes: int = Query(2000, ge=10, le=10000),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Get the subgraph around an actor for frontend rendering."""
    graph = get_graph()
    if not graph.has_actor(actor_id):
        raise HTTPException(404, f"Actor '{actor_id}' not found")

    nodes, links = graph.subgraph(actor_id, depth=depth, max_nodes=max_nodes)
    return {
        "center": actor_id,
        "depth": depth,
        "nodes": nodes,
        "links": links,
        "node_count": len(nodes),
        "link_count": len(links),
    }


@router.get("/actor/{actor_id}/path/{target_id}")
async def get_shortest_path(
    actor_id: str,
    target_id: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Find the shortest path between two actors."""
    graph = get_graph()
    for aid in (actor_id, target_id):
        if not graph.has_actor(aid):
            raise HTTPException(404, f"Actor '{aid}' not found")

    path = graph.shortest_path(actor_id, target_id)
    if path is None:
        return {"path": None, "degrees": -1, "message": "No connection found"}

    # Build path with actor details and connection details
    path_details = []
    for i, aid in enumerate(path):
        actor = graph.get_actor(aid) or {}
        entry: dict[str, Any] = {"id": aid, **actor, "degree": i}
        if i > 0:
            neighbors = graph.get_neighbors(path[i - 1])
            meta = neighbors.get(aid)
            if meta:
                entry["connection"] = {
                    "relationship": meta.relationship,
                    "strength": meta.strength,
                    "confidence_tier": meta.confidence_tier,
                }
        path_details.append(entry)

    return {
        "path": path_details,
        "degrees": len(path) - 1,
        "from": actor_id,
        "to": target_id,
    }


@router.get("/actor/{actor_id}/connections")
async def get_actor_connections(
    actor_id: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Get all connections for an actor with metadata."""
    graph = get_graph()
    if not graph.has_actor(actor_id):
        raise HTTPException(404, f"Actor '{actor_id}' not found")

    neighbors = graph.get_neighbors(actor_id)
    connections = []
    for nid, meta in neighbors.items():
        neighbor_data = graph.get_actor(nid) or {}
        connections.append({
            "actor_id": nid,
            "name": neighbor_data.get("name", nid),
            "category": neighbor_data.get("category", ""),
            "relationship": meta.relationship,
            "strength": meta.strength,
            "confidence_tier": meta.confidence_tier,
            "sources": meta.sources,
        })

    connections.sort(key=lambda c: c["strength"], reverse=True)
    return {"actor_id": actor_id, "connections": connections, "count": len(connections)}


@router.get("/spider/stats")
async def get_spider_stats(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Get overall graph statistics."""
    graph = get_graph()

    # Count by degree
    by_degree: dict[int, int] = {}
    by_source: dict[str, int] = {}
    for data in graph._actors.values():
        deg = data.get("degree", 0)
        by_degree[deg] = by_degree.get(deg, 0) + 1
        src = data.get("source", "unknown")
        by_source[src] = by_source.get(src, 0) + 1

    return {
        "total_actors": graph.actor_count,
        "total_connections": graph.connection_count,
        "by_degree": dict(sorted(by_degree.items())),
        "by_source": dict(sorted(by_source.items(), key=lambda kv: -kv[1])),
        "max_degree": max(by_degree.keys()) if by_degree else 0,
    }


@router.post("/spider/inject")
async def inject_actor(
    body: InjectActorRequest,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Operator injects a new actor or lead into the graph."""
    from intelligence.spider.db import save_actor
    from intelligence.spider.entity_resolver import EntityResolver

    graph = get_graph()
    engine = get_db_engine()
    resolver = EntityResolver(graph)

    # Check if already exists
    existing = resolver.resolve(body.name, {"category": body.category})
    if existing:
        return {"status": "exists", "actor_id": existing, "message": f"Actor already exists as '{existing}'"}

    actor_id = resolver.generate_id(body.name, body.category)
    actor_data = {
        "name": body.name,
        "category": body.category,
        "tier": body.tier,
        "title": body.title,
        "influence_score": 0.5,
        "trust_score": 0.5,
        "degree": 0,
        "source": "operator",
        "credibility": "rumor",
        "data_sources": ["operator"],
    }

    graph.add_actor(actor_id, actor_data)
    save_actor(engine, actor_id, actor_data)

    return {"status": "created", "actor_id": actor_id, "name": body.name}
```

- [ ] **Step 2: Register the router in main.py**

Add to the router list in `/Users/anikdang/dev/GRID/api/main.py` (after the `vault` entry around line 387):

```python
    ("spider", "api.routers.intelligence_spider", False),
```

- [ ] **Step 3: Commit**

```bash
git add api/routers/intelligence_spider.py api/main.py
git commit -m "feat: add spider API — neighborhood, path, connections, stats, inject endpoints"
```

---

### Task 11: Systemd Service + Update spider __init__.py exports

**Files:**
- Create: `server_setup/grid-spider.service`
- Modify: `intelligence/spider/__init__.py`

- [ ] **Step 1: Write systemd service file**

```ini
# server_setup/grid-spider.service
[Unit]
Description=GRID Connection Mapping Spider
After=network.target postgresql.service grid-api.service
Requires=postgresql.service

[Service]
Type=simple
User=grid
WorkingDirectory=/home/grid/grid_v4/grid_repo
EnvironmentFile=/home/grid/grid_v4/grid_repo/.env
ExecStart=/usr/bin/python3 -m intelligence.spider.daemon
Restart=always
RestartSec=30
StandardOutput=append:/data/grid/logs/spider.log
StandardError=append:/data/grid/logs/spider.log

[Install]
WantedBy=multi-user.target
```

- [ ] **Step 2: Update spider __init__.py with full exports**

```python
# intelligence/spider/__init__.py
"""GRID Connection Mapping Spider — discovers and maps actor relationships."""

from intelligence.spider.models import (
    ConnectionMeta,
    DiscoveredConnection,
    SpiderStats,
)
from intelligence.spider.graph_engine import GraphEngine
from intelligence.spider.priority_queue import PriorityQueue
from intelligence.spider.discovery import DiscoveryOrchestrator
from intelligence.spider.entity_resolver import EntityResolver

__all__ = [
    "ConnectionMeta",
    "DiscoveredConnection",
    "SpiderStats",
    "GraphEngine",
    "PriorityQueue",
    "DiscoveryOrchestrator",
    "EntityResolver",
]
```

- [ ] **Step 3: Commit**

```bash
git add server_setup/grid-spider.service intelligence/spider/__init__.py
git commit -m "feat: add spider systemd service + update module exports"
```

---

### Task 12: Run Full Test Suite

- [ ] **Step 1: Run all spider tests**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_spider_models.py tests/test_graph_engine.py tests/test_priority_queue.py tests/test_entity_resolver.py tests/test_discovery.py -v`
Expected: All pass (16+ tests)

- [ ] **Step 2: Run existing test suite to check for regressions**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/ -v --timeout=60 2>&1 | tail -20`
Expected: No new failures

- [ ] **Step 3: Final commit with all tests passing**

```bash
git add -A
git commit -m "test: verify all spider tests pass, no regressions"
```
