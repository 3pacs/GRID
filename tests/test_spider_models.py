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
