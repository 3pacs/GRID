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
