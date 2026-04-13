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
    assert connections[0].confidence_tier == 2


from intelligence.spider.discovery import DiscoveryOrchestrator
from intelligence.spider.graph_engine import GraphEngine
from intelligence.entity_resolver import SpiderEntityResolver as EntityResolver
from intelligence.spider.models import ConnectionMeta


def test_orchestrator_runs_adapters_and_resolves():
    """Orchestrator discovers connections and resolves entities."""
    ge = GraphEngine()
    ge.add_actor("larry_fink", {"name": "Larry Fink", "tier": "institutional", "category": "fund", "influence_score": 0.9})

    resolver = EntityResolver(ge, max_distance=3)
    orchestrator = DiscoveryOrchestrator(graph=ge, resolver=resolver, adapters=[])

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

    assert len(new_actors) == 1
    assert new_actors[0]["name"] == "BlackRock Inc."
    assert len(new_connections) == 1
    assert new_connections[0][0] == "larry_fink"
    assert new_connections[0][2].relationship == "ceo"
