import asyncio

import pytest

from api.routers import intelligence_spider
from fastapi import HTTPException
from intelligence.spider.graph_engine import GraphEngine
from intelligence.spider.models import ConnectionMeta


def teardown_function() -> None:
    intelligence_spider._graph_engine = None
    intelligence_spider._graph_load_error = None
    intelligence_spider._graph_loaded_at = None
    intelligence_spider._graph_is_full = False


def test_spider_stats_lazily_loads_graph_from_db(monkeypatch) -> None:
    loaded_with = []

    def fake_load(engine):
        loaded_with.append(engine)
        graph = GraphEngine()
        graph.add_actor("powell", {"name": "Jerome Powell", "degree": 0, "source": "seed"})
        return graph

    monkeypatch.setattr(intelligence_spider, "_load_stats_graph_from_db", fake_load)

    result = asyncio.run(
        intelligence_spider.get_spider_stats(engine=object(), _token="token")
    )

    assert len(loaded_with) == 1
    assert result["status"] == "ready"
    assert result["total_actors"] == 1
    assert result["total_connections"] == 0
    assert result["by_source"] == {"seed": 1}


def test_spider_stats_reports_degraded_state_when_bootstrap_fails(monkeypatch) -> None:
    def fake_load(_engine):
        raise RuntimeError("actor table unavailable")

    monkeypatch.setattr(intelligence_spider, "_load_stats_graph_from_db", fake_load)

    result = asyncio.run(
        intelligence_spider.get_spider_stats(engine=object(), _token="token")
    )

    assert result["status"] == "unavailable"
    assert result["total_actors"] == 0
    assert result["total_connections"] == 0
    assert "actor table unavailable" in result["error"]


def test_spider_stats_marks_actor_only_graph_as_degraded(monkeypatch) -> None:
    def fake_load(_engine):
        graph = GraphEngine()
        graph.add_actor("powell", {"name": "Jerome Powell", "degree": 0, "source": "legacy"})
        graph.load_warnings.append("actor_connections table missing")
        return graph

    monkeypatch.setattr(intelligence_spider, "_load_stats_graph_from_db", fake_load)

    result = asyncio.run(
        intelligence_spider.get_spider_stats(engine=object(), _token="token")
    )

    assert result["status"] == "degraded"
    assert result["total_actors"] == 1
    assert result["warnings"] == ["actor_connections table missing"]


# ──────────────────────────────────────────────────────────────────────
# Graph engine bridge (task #154)
# ──────────────────────────────────────────────────────────────────────


def _build_two_actor_graph() -> GraphEngine:
    graph = GraphEngine()
    graph.add_actor("powell", {"name": "Jerome Powell", "degree": 0, "source": "seed",
                                "category": "regulator", "influence_score": 0.9})
    graph.add_actor("yellen", {"name": "Janet Yellen", "degree": 1, "source": "seed",
                                "category": "regulator", "influence_score": 0.85})
    graph.add_connection("powell", "yellen", ConnectionMeta(
        relationship="successor", strength=0.9, confidence_tier=1, sources=["wikidata"]))
    return graph


def test_get_graph_lazy_loads_full_graph(monkeypatch) -> None:
    """get_graph() (used by neighborhood/path/connections) must lazy-load
    the FULL graph on first call, not 503 forever."""
    loaded_with = []

    def fake_full_load(engine):
        loaded_with.append(engine)
        return _build_two_actor_graph()

    monkeypatch.setattr(intelligence_spider, "_load_full_graph_from_db", fake_full_load)

    engine = object()
    graph = intelligence_spider.get_graph(engine)
    assert graph is not None
    assert graph.has_actor("powell")
    assert graph.has_actor("yellen")
    assert intelligence_spider._graph_is_full is True
    # Second call should reuse the loaded graph (no second DB hit).
    intelligence_spider.get_graph(engine)
    assert len(loaded_with) == 1


def test_get_graph_raises_503_when_db_unavailable(monkeypatch) -> None:
    """If the DB is unreachable, the endpoint must raise 503 (degraded),
    not 500."""

    def fake_full_load(_engine):
        raise RuntimeError("connection refused")

    monkeypatch.setattr(intelligence_spider, "_load_full_graph_from_db", fake_full_load)

    with pytest.raises(HTTPException) as exc_info:
        intelligence_spider.get_graph(object())
    assert exc_info.value.status_code == 503
    assert "connection refused" in exc_info.value.detail


def test_refresh_graph_swaps_in_new_graph(monkeypatch) -> None:
    """refresh_graph() (called by spider ping + periodic refresher)
    must atomically replace the in-memory graph with a fresh one."""
    calls = []
    graphs = [_build_two_actor_graph(), _build_two_actor_graph()]
    # Distinguish them: add a third actor to the second.
    graphs[1].add_actor("brainard", {"name": "Lael Brainard", "degree": 1, "source": "seed"})

    def fake_full_load(_engine):
        calls.append(1)
        return graphs[len(calls) - 1]

    monkeypatch.setattr(intelligence_spider, "_load_full_graph_from_db", fake_full_load)

    info1 = intelligence_spider.refresh_graph(object())
    assert info1["status"] == "ok"
    assert info1["actors"] == 2
    assert intelligence_spider._graph_engine.actor_count == 2

    info2 = intelligence_spider.refresh_graph(object())
    assert info2["status"] == "ok"
    assert info2["actors"] == 3
    assert intelligence_spider._graph_engine.has_actor("brainard")


def test_refresh_graph_reports_error_on_db_failure(monkeypatch) -> None:
    def fake_full_load(_engine):
        raise RuntimeError("postgres down")

    monkeypatch.setattr(intelligence_spider, "_load_full_graph_from_db", fake_full_load)
    info = intelligence_spider.refresh_graph(object())
    assert info["status"] == "error"
    assert "postgres down" in info["error"]


def test_get_graph_info_includes_load_metadata(monkeypatch) -> None:
    monkeypatch.setattr(
        intelligence_spider, "_load_full_graph_from_db",
        lambda _e: _build_two_actor_graph(),
    )
    intelligence_spider.refresh_graph(object())
    info = intelligence_spider.get_graph_info()
    assert info["loaded"] is True
    assert info["is_full_load"] is True
    assert info["actors"] == 2
    assert info["connections"] == 1
    assert info["loaded_at"] is not None
    assert "age_seconds" in info


def test_neighborhood_endpoint_uses_graph_engine(monkeypatch) -> None:
    """End-to-end: the neighborhood endpoint must serve from the in-memory
    graph (not a DB query) — proves the bridge is wired into FastAPI."""
    monkeypatch.setattr(
        intelligence_spider, "_load_full_graph_from_db",
        lambda _e: _build_two_actor_graph(),
    )
    result = asyncio.run(
        intelligence_spider.get_neighborhood(
            "powell", depth=2, max_nodes=100, engine=object(), _token="token"
        )
    )
    assert result["center"] == "powell"
    assert result["via"] == "spider_graph_engine"
    assert result["node_count"] == 2
    assert result["link_count"] == 1


def test_graph_reload_endpoint_returns_status(monkeypatch) -> None:
    monkeypatch.setattr(
        intelligence_spider, "_load_full_graph_from_db",
        lambda _e: _build_two_actor_graph(),
    )
    result = asyncio.run(
        intelligence_spider.graph_reload(engine=object(), _token="token")
    )
    assert result["status"] == "ok"
    assert result["actors"] == 2
    assert result["connections"] == 1
