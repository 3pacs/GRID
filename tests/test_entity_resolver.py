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
