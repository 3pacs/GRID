import asyncio

from api.routers import intelligence_spider
from intelligence.spider.graph_engine import GraphEngine


def teardown_function() -> None:
    intelligence_spider._graph_engine = None
    intelligence_spider._graph_load_error = None


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
