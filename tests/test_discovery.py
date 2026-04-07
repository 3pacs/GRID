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
