"""Tests for graph analytics batch — Phase 4.6.

Tests cover:
  - NetworkX graph building from mock data
  - PageRank computation on known graphs
  - Community detection validity
  - UPSERT logic
  - Store query functions
  - API endpoint responses
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Ensure project root is importable
_ROOT = str(Path(__file__).resolve().parent.parent)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import networkx as nx

# python-louvain (provides ``community.community_louvain``) is an optional
# heavy dependency that fails to build on some CI workers. Skip the whole
# module when it's unavailable so a single missing wheel doesn't take the
# Backend Tests run from green to red. Production code in
# ``scripts/graph_analytics.py`` already imports it lazily and degrades.
pytest.importorskip("community")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def small_graph() -> nx.DiGraph:
    """Build a small known directed graph for deterministic tests.

    Structure:
      A -> B -> C -> D
      A -> C
      D -> A  (cycle)
      E -> B
    """
    G = nx.DiGraph()
    G.add_edge("A", "B", weight=1.0, relationship="controls")
    G.add_edge("B", "C", weight=0.8, relationship="funds")
    G.add_edge("C", "D", weight=0.5, relationship="advises")
    G.add_edge("A", "C", weight=0.6, relationship="lobbies")
    G.add_edge("D", "A", weight=0.9, relationship="reports_to")
    G.add_edge("E", "B", weight=0.7, relationship="invests_in")

    # Node attributes
    for nid in G.nodes():
        G.nodes[nid]["name"] = f"Actor {nid}"
        G.nodes[nid]["category"] = "government" if nid in ("A", "D") else "finance"
        G.nodes[nid]["influence_score"] = 0.5

    return G


@pytest.fixture
def mock_edges() -> list[dict]:
    """Mock actor_connections rows as returned by execute_sql."""
    return [
        {"actor_a": "a1", "actor_b": "a2", "relationship": "controls", "strength": 1.0},
        {"actor_a": "a2", "actor_b": "a3", "relationship": "funds", "strength": 0.8},
        {"actor_a": "a3", "actor_b": "a1", "relationship": "advises", "strength": 0.5},
        {"actor_a": "a4", "actor_b": "a2", "relationship": "invests", "strength": 0.6},
    ]


@pytest.fixture
def mock_actors() -> list[dict]:
    """Mock actors rows as returned by execute_sql."""
    return [
        {"id": "a1", "name": "Alpha Corp", "category": "finance", "influence_score": 0.9},
        {"id": "a2", "name": "Beta Fund", "category": "hedge_fund", "influence_score": 0.7},
        {"id": "a3", "name": "Gamma Gov", "category": "government", "influence_score": 0.5},
        {"id": "a4", "name": "Delta Invest", "category": "finance", "influence_score": 0.3},
    ]


# ---------------------------------------------------------------------------
# Test: Graph building
# ---------------------------------------------------------------------------

class TestGraphBuilding:
    """Test NetworkX graph construction from mock data."""

    def test_build_graph_from_edges(self, mock_edges: list[dict]):
        """Graph should have correct nodes and edges from mock edge data."""
        G = nx.DiGraph()
        for edge in mock_edges:
            G.add_edge(
                edge["actor_a"],
                edge["actor_b"],
                weight=float(edge.get("strength") or 0.5),
                relationship=edge.get("relationship", "unknown"),
            )
        assert G.number_of_nodes() == 4
        assert G.number_of_edges() == 4
        assert G.has_edge("a1", "a2")
        assert G.has_edge("a3", "a1")
        assert not G.has_edge("a1", "a4")

    def test_edge_weights(self, mock_edges: list[dict]):
        """Edge weights should match strength values."""
        G = nx.DiGraph()
        for edge in mock_edges:
            G.add_edge(
                edge["actor_a"],
                edge["actor_b"],
                weight=float(edge.get("strength") or 0.5),
            )
        assert G["a1"]["a2"]["weight"] == 1.0
        assert G["a2"]["a3"]["weight"] == 0.8
        assert G["a3"]["a1"]["weight"] == 0.5

    def test_empty_edges(self):
        """Empty edge list should produce empty graph."""
        G = nx.DiGraph()
        assert G.number_of_nodes() == 0
        assert G.number_of_edges() == 0

    def test_node_attributes(self, mock_edges: list[dict], mock_actors: list[dict]):
        """Node attributes should be applied correctly from actor data."""
        G = nx.DiGraph()
        for edge in mock_edges:
            G.add_edge(edge["actor_a"], edge["actor_b"], weight=float(edge.get("strength") or 0.5))
        for actor in mock_actors:
            nid = actor["id"]
            if nid in G:
                G.nodes[nid]["name"] = actor.get("name", "")
                G.nodes[nid]["category"] = actor.get("category", "")
        assert G.nodes["a1"]["name"] == "Alpha Corp"
        assert G.nodes["a2"]["category"] == "hedge_fund"


# ---------------------------------------------------------------------------
# Test: PageRank
# ---------------------------------------------------------------------------

class TestPageRank:
    """Test PageRank computation on known graphs."""

    def test_pagerank_returns_all_nodes(self, small_graph: nx.DiGraph):
        """PageRank should return a score for every node."""
        pr = nx.pagerank(small_graph, weight="weight")
        assert set(pr.keys()) == set(small_graph.nodes())

    def test_pagerank_sums_to_one(self, small_graph: nx.DiGraph):
        """PageRank scores should sum to approximately 1.0."""
        pr = nx.pagerank(small_graph, weight="weight")
        total = sum(pr.values())
        assert abs(total - 1.0) < 1e-6

    def test_pagerank_values_positive(self, small_graph: nx.DiGraph):
        """All PageRank scores should be positive."""
        pr = nx.pagerank(small_graph, weight="weight")
        for score in pr.values():
            assert score > 0

    def test_cyclic_nodes_rank_higher(self):
        """Nodes in a cycle should have higher PageRank than isolated sinks."""
        G = nx.DiGraph()
        # Cycle: 1 -> 2 -> 3 -> 1
        G.add_edge("1", "2", weight=1.0)
        G.add_edge("2", "3", weight=1.0)
        G.add_edge("3", "1", weight=1.0)
        # Dangling: 4 -> 5 (no return)
        G.add_edge("4", "5", weight=1.0)

        pr = nx.pagerank(G, weight="weight")
        cycle_avg = (pr["1"] + pr["2"] + pr["3"]) / 3
        assert cycle_avg > pr["5"]


# ---------------------------------------------------------------------------
# Test: Community detection
# ---------------------------------------------------------------------------

class TestCommunityDetection:
    """Test Louvain community detection."""

    def test_partition_covers_all_nodes(self, small_graph: nx.DiGraph):
        """Partition should assign a community to every node."""
        from community import community_louvain
        G_undirected = small_graph.to_undirected()
        partition = community_louvain.best_partition(G_undirected, weight="weight")
        assert set(partition.keys()) == set(small_graph.nodes())

    def test_partition_values_are_ints(self, small_graph: nx.DiGraph):
        """Community IDs should be integers."""
        from community import community_louvain
        G_undirected = small_graph.to_undirected()
        partition = community_louvain.best_partition(G_undirected, weight="weight")
        for cid in partition.values():
            assert isinstance(cid, int)

    def test_connected_nodes_same_community(self):
        """Densely connected nodes should tend to be in the same community."""
        G = nx.DiGraph()
        # Clique 1: A-B-C fully connected
        for u, v in [("A", "B"), ("B", "A"), ("A", "C"), ("C", "A"), ("B", "C"), ("C", "B")]:
            G.add_edge(u, v, weight=1.0)
        # Clique 2: D-E-F fully connected
        for u, v in [("D", "E"), ("E", "D"), ("D", "F"), ("F", "D"), ("E", "F"), ("F", "E")]:
            G.add_edge(u, v, weight=1.0)
        # Weak link between cliques
        G.add_edge("C", "D", weight=0.1)

        from community import community_louvain
        partition = community_louvain.best_partition(G.to_undirected(), weight="weight")

        # A, B, C should be in same community
        assert partition["A"] == partition["B"] == partition["C"]
        # D, E, F should be in same community
        assert partition["D"] == partition["E"] == partition["F"]
        # The two cliques should be in different communities
        assert partition["A"] != partition["D"]

    def test_at_least_one_community(self, small_graph: nx.DiGraph):
        """There should be at least 1 community."""
        from community import community_louvain
        G_undirected = small_graph.to_undirected()
        partition = community_louvain.best_partition(G_undirected, weight="weight")
        n_communities = len(set(partition.values()))
        assert n_communities >= 1


# ---------------------------------------------------------------------------
# Test: Betweenness, eigenvector, degree, HITS
# ---------------------------------------------------------------------------

class TestCentralityMetrics:
    """Test betweenness, eigenvector, degree centrality, and HITS."""

    def test_betweenness_returns_all_nodes(self, small_graph: nx.DiGraph):
        bc = nx.betweenness_centrality(small_graph, weight="weight")
        assert set(bc.keys()) == set(small_graph.nodes())

    def test_betweenness_values_in_range(self, small_graph: nx.DiGraph):
        bc = nx.betweenness_centrality(small_graph, weight="weight")
        for score in bc.values():
            assert 0.0 <= score <= 1.0

    def test_degree_centrality_returns_all(self, small_graph: nx.DiGraph):
        dc = nx.degree_centrality(small_graph)
        assert set(dc.keys()) == set(small_graph.nodes())

    def test_hits_returns_all_nodes(self, small_graph: nx.DiGraph):
        hubs, auths = nx.hits(small_graph)
        assert set(hubs.keys()) == set(small_graph.nodes())
        assert set(auths.keys()) == set(small_graph.nodes())

    def test_hits_values_positive(self, small_graph: nx.DiGraph):
        # HITS is iterative and can produce values a few ULPs below zero
        # due to floating-point roundoff (we observe ~-2e-18). The
        # algorithmic invariant is "non-negative"; allow a tiny epsilon.
        eps = 1e-10
        hubs, auths = nx.hits(small_graph)
        for h in hubs.values():
            assert h >= -eps
        for a in auths.values():
            assert a >= -eps


# ---------------------------------------------------------------------------
# Test: UPSERT logic (mocked DB)
# ---------------------------------------------------------------------------

class TestUpsertLogic:
    """Test the store_results function with mocked database."""

    @patch("scripts.graph_analytics.get_connection")
    def test_store_calls_execute(self, mock_get_conn, small_graph: nx.DiGraph):
        """store_results should execute INSERT...ON CONFLICT for each batch."""
        # Setup mocks
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        from scripts.graph_analytics import store_results

        pr = {n: 0.1 for n in small_graph.nodes()}
        comms = {n: 0 for n in small_graph.nodes()}
        bc = {n: 0.05 for n in small_graph.nodes()}
        ev = {n: 0.02 for n in small_graph.nodes()}
        dc = {n: 0.3 for n in small_graph.nodes()}
        hubs = {n: 0.01 for n in small_graph.nodes()}
        auths = {n: 0.01 for n in small_graph.nodes()}

        stored = store_results(
            small_graph, pr, comms, bc, ev, dc, hubs, auths, batch_size=3
        )

        assert stored == small_graph.number_of_nodes()
        # CREATE TABLE + at least 1 INSERT batch
        assert mock_cursor.execute.call_count >= 2


# ---------------------------------------------------------------------------
# Test: Store query functions
# ---------------------------------------------------------------------------

class TestStoreQueries:
    """Test store/graph.py analytics query functions."""

    @patch("store.graph.text")
    def test_get_actor_analytics_returns_dict(self, mock_text):
        """get_actor_analytics should return a dict with expected keys."""
        from store.graph import get_actor_analytics

        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.execute.return_value.fetchone.return_value = (
            "actor-1", 0.05, 3, 0.02, 0.01, 0.3, 0.04, 0.03, "2026-04-08T12:00:00"
        )

        result = get_actor_analytics("actor-1", engine=mock_engine)
        assert result is not None
        assert result["actor_id"] == "actor-1"
        assert result["pagerank"] == 0.05
        assert result["community_id"] == 3
        assert result["betweenness"] == 0.02
        assert result["eigenvector"] == 0.01
        assert result["degree_centrality"] == 0.3
        assert result["hub_score"] == 0.04
        assert result["authority_score"] == 0.03

    @patch("store.graph.text")
    def test_get_actor_analytics_not_found(self, mock_text):
        """get_actor_analytics should return None for missing actor."""
        from store.graph import get_actor_analytics

        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.execute.return_value.fetchone.return_value = None

        result = get_actor_analytics("nonexistent", engine=mock_engine)
        assert result is None

    def test_get_top_actors_rejects_invalid_metric(self):
        """get_top_actors should raise ValueError for invalid metric."""
        from store.graph import get_top_actors

        mock_engine = MagicMock()
        with pytest.raises(ValueError, match="Invalid metric"):
            get_top_actors(metric="sql_injection", engine=mock_engine)

    def test_get_top_actors_accepts_valid_metrics(self):
        """get_top_actors should accept all valid metric names."""
        from store.graph import _VALID_METRICS

        valid = {"pagerank", "betweenness", "eigenvector", "degree_centrality", "hub_score", "authority_score"}
        assert _VALID_METRICS == valid


# ---------------------------------------------------------------------------
# Test: Community labeling
# ---------------------------------------------------------------------------

class TestCommunityLabeling:
    """Test community labeling by highest-PageRank member category."""

    def test_label_by_top_pr_category(self, small_graph: nx.DiGraph):
        from scripts.graph_analytics import label_communities

        communities = {"A": 0, "B": 0, "C": 1, "D": 1, "E": 1}
        pagerank = {"A": 0.3, "B": 0.1, "C": 0.2, "D": 0.25, "E": 0.15}

        labels = label_communities(small_graph, communities, pagerank)

        # Community 0: A has highest PR, category = "government"
        assert labels[0] == "government"
        # Community 1: D has highest PR, category = "government"
        assert labels[1] == "government"

    def test_empty_graph_labels(self):
        from scripts.graph_analytics import label_communities

        G = nx.DiGraph()
        labels = label_communities(G, {}, {})
        assert labels == {}


# ---------------------------------------------------------------------------
# Test: API endpoints (mocked)
# ---------------------------------------------------------------------------

class TestAPIEndpoints:
    """Test the analytics API endpoints with mocked store functions."""

    def test_actor_analytics_endpoint(self):
        """GET /actor/{id}/analytics should return analytics dict."""
        import asyncio
        from api.routers.intelligence_actors import get_actor_analytics_endpoint

        mock_analytics = {
            "actor_id": "test-1",
            "pagerank": 0.05,
            "community_id": 2,
            "betweenness": 0.01,
            "eigenvector": 0.02,
            "degree_centrality": 0.3,
            "hub_score": 0.04,
            "authority_score": 0.03,
            "computed_at": "2026-04-08",
        }

        with patch("api.routers.intelligence_actors.get_db_engine"):
            with patch("store.graph.get_actor_analytics", return_value=mock_analytics):
                result = asyncio.new_event_loop().run_until_complete(
                    get_actor_analytics_endpoint("test-1", _token="test")
                )

        assert "analytics" in result
        assert result["analytics"]["pagerank"] == 0.05

    def test_top_actors_endpoint(self):
        """GET /analytics/top should return actors list."""
        import asyncio
        from api.routers.intelligence_actors import get_top_actors_endpoint

        mock_actors = [
            {"actor_id": "a1", "name": "Alpha", "category": "finance", "score": 0.1, "community_id": 0, "pagerank": 0.1},
        ]

        with patch("api.routers.intelligence_actors.get_db_engine"):
            with patch("store.graph.get_top_actors", return_value=mock_actors):
                result = asyncio.new_event_loop().run_until_complete(
                    get_top_actors_endpoint(metric="pagerank", limit=20, _token="test")
                )

        assert "actors" in result
        assert result["count"] == 1

    def test_communities_endpoint(self):
        """GET /analytics/communities should return community list."""
        import asyncio
        from api.routers import intelligence_actors as router
        from api.routers.intelligence_actors import get_communities_endpoint

        router._community_list_cache.clear()

        mock_communities = [
            {"community_id": 0, "member_count": 50, "max_pagerank": 0.1, "top_member": "Alpha", "top_category": "finance"},
        ]

        with patch("api.routers.intelligence_actors.get_db_engine"):
            with patch("api.routers.intelligence_actors._read_community_summary", return_value=None):
                with patch("store.graph.get_community_list", return_value=mock_communities):
                    result = asyncio.new_event_loop().run_until_complete(
                        get_communities_endpoint(_token="test")
                    )

        assert "communities" in result
        assert result["count"] == 1

    def test_community_members_endpoint(self):
        """GET /analytics/community/{id} should return members list."""
        import asyncio
        from api.routers.intelligence_actors import get_community_members_endpoint

        mock_members = [
            {"actor_id": "a1", "name": "Alpha", "category": "finance",
             "pagerank": 0.1, "betweenness": 0.01, "eigenvector": 0.02,
             "hub_score": 0.01, "authority_score": 0.01},
        ]

        with patch("api.routers.intelligence_actors.get_db_engine"):
            with patch("store.graph.get_community_members", return_value=mock_members):
                result = asyncio.new_event_loop().run_until_complete(
                    get_community_members_endpoint(community_id=0, limit=50, _token="test")
                )

        assert "members" in result
        assert result["community_id"] == 0
        assert result["count"] == 1

    def test_top_actors_invalid_metric(self):
        """GET /analytics/top with invalid metric should return error."""
        import asyncio
        from api.routers.intelligence_actors import get_top_actors_endpoint

        with patch("api.routers.intelligence_actors.get_db_engine"):
            with patch("store.graph.get_top_actors", side_effect=ValueError("Invalid metric 'bad'")):
                result = asyncio.new_event_loop().run_until_complete(
                    get_top_actors_endpoint(metric="bad", limit=20, _token="test")
                )

        assert result["count"] == 0
        assert "error" in result


# ---------------------------------------------------------------------------
# Test: Full pipeline (mocked DB)
# ---------------------------------------------------------------------------

class TestFullPipeline:
    """Test the full run_graph_analytics pipeline with mocked DB."""

    @patch("scripts.graph_analytics.get_connection")
    @patch("scripts.graph_analytics.execute_sql")
    def test_pipeline_empty_graph(self, mock_exec, mock_conn):
        """Pipeline should handle empty graph gracefully."""
        mock_exec.return_value = []

        from scripts.graph_analytics import run_graph_analytics
        result = run_graph_analytics()
        assert result["nodes"] == 0

    @patch("scripts.graph_analytics.get_connection")
    @patch("scripts.graph_analytics.execute_sql")
    def test_pipeline_with_data(self, mock_exec, mock_conn):
        """Pipeline should compute analytics for a small graph."""
        # First call: edges, second call: actors
        mock_exec.side_effect = [
            [
                {"actor_a": "a1", "actor_b": "a2", "relationship": "funds", "strength": 1.0},
                {"actor_a": "a2", "actor_b": "a3", "relationship": "controls", "strength": 0.8},
                {"actor_a": "a3", "actor_b": "a1", "relationship": "advises", "strength": 0.5},
            ],
            [
                {"id": "a1", "name": "Alpha", "category": "finance", "influence_score": 0.9},
                {"id": "a2", "name": "Beta", "category": "tech", "influence_score": 0.7},
                {"id": "a3", "name": "Gamma", "category": "gov", "influence_score": 0.5},
            ],
        ]

        # Mock the DB write
        mock_conn_obj = MagicMock()
        mock_cursor = MagicMock()
        mock_conn_obj.__enter__ = MagicMock(return_value=mock_conn_obj)
        mock_conn_obj.__exit__ = MagicMock(return_value=False)
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn_obj.cursor.return_value = mock_cursor
        mock_conn.return_value = mock_conn_obj

        from scripts.graph_analytics import run_graph_analytics
        result = run_graph_analytics()

        assert result["nodes"] == 3
        assert result["edges"] == 3
        assert result["communities"] >= 1
        assert result["stored"] == 3
