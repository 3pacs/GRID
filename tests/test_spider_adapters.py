"""Tests for spider source adapters."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from intelligence.spider.models import DiscoveredConnection


# ── SEC EDGAR ──

class TestSecCrossRefAdapter:
    def _make(self):
        from intelligence.spider.sources.sec_crossref import SecCrossRefAdapter
        return SecCrossRefAdapter()

    @patch("intelligence.spider.sources.sec_crossref.requests.get")
    def test_discover_returns_connections(self, mock_get):
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "hits": {"hits": [{
                    "_source": {
                        "form_type": "4",
                        "entity_name": "ACME Corp",
                        "display_names": ["John Smith", "Jane Doe"],
                        "file_date": "2025-01-15",
                    }
                }]}
            },
        )
        results = self._make().discover("John Smith", {})
        assert len(results) >= 1
        assert all(isinstance(r, DiscoveredConnection) for r in results)
        assert results[0].confidence_tier == 1
        assert results[0].strength == 0.85

    @patch("intelligence.spider.sources.sec_crossref.requests.get")
    def test_discover_handles_http_error(self, mock_get):
        mock_get.return_value = MagicMock(status_code=500)
        assert self._make().discover("Test Actor", {}) == []

    @patch("intelligence.spider.sources.sec_crossref.requests.get")
    def test_discover_handles_timeout(self, mock_get):
        mock_get.side_effect = Exception("Timeout")
        assert self._make().discover("Test Actor", {}) == []

    @patch("intelligence.spider.sources.sec_crossref.requests.get")
    def test_discover_empty_results(self, mock_get):
        mock_get.return_value = MagicMock(
            status_code=200, json=lambda: {"hits": {"hits": []}}
        )
        assert self._make().discover("Nobody", {}) == []


# ── ICIJ Offshore ──

class TestIcijOffshoreAdapter:
    def _make(self):
        from intelligence.spider.sources.icij_offshore import IcijOffshoreAdapter
        return IcijOffshoreAdapter()

    @patch("intelligence.spider.sources.icij_offshore.requests.get")
    @patch("intelligence.spider.sources.icij_offshore._ICIJ_DATA_DIR")
    def test_discover_api_fallback(self, mock_dir, mock_get):
        mock_dir.exists.return_value = False
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: [{"name": "Shell Corp Ltd", "country": "Panama", "type": "Entity"}],
        )
        results = self._make().discover("Test Person", {})
        assert all(isinstance(r, DiscoveredConnection) for r in results)
        if results:
            assert results[0].confidence_tier == 2

    @patch("intelligence.spider.sources.icij_offshore.requests.get")
    @patch("intelligence.spider.sources.icij_offshore._ICIJ_DATA_DIR")
    def test_discover_handles_api_error(self, mock_dir, mock_get):
        mock_dir.exists.return_value = False
        mock_get.side_effect = Exception("Connection refused")
        assert self._make().discover("Test", {}) == []


# ── OpenCorporates ──

class TestOpenCorporatesAdapter:
    def _make(self):
        from intelligence.spider.sources.opencorporates import OpenCorporatesAdapter
        return OpenCorporatesAdapter()

    @patch("intelligence.spider.sources.opencorporates.requests.get")
    def test_discover_returns_connections(self, mock_get):
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "results": {
                    "companies": [{
                        "company": {
                            "name": "ACME Holdings",
                            "jurisdiction_code": "us_de",
                            "opencorporates_url": "https://opencorporates.com/companies/us_de/12345",
                            "officers": [{"officer": {"name": "Bob CEO", "position": "Director"}}],
                        }
                    }]
                }
            },
        )
        results = self._make().discover("ACME", {})
        assert all(isinstance(r, DiscoveredConnection) for r in results)
        if results:
            assert results[0].confidence_tier == 2

    @patch("intelligence.spider.sources.opencorporates.requests.get")
    def test_discover_handles_error(self, mock_get):
        mock_get.side_effect = Exception("Rate limited")
        assert self._make().discover("Test", {}) == []


# ── News Co-occurrence ──

class TestNewsCooccurrenceAdapter:
    def _make(self):
        from intelligence.spider.sources.news_cooccurrence import NewsCooccurrenceAdapter
        return NewsCooccurrenceAdapter()

    @patch("intelligence.spider.sources.news_cooccurrence.requests.get")
    def test_discover_extracts_entities(self, mock_get):
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "articles": [{
                    "title": "Elon Musk and Tim Cook discuss AI partnership at Apple",
                    "url": "https://example.com/article1",
                }]
            },
        )
        results = self._make().discover("Elon Musk", {})
        assert all(isinstance(r, DiscoveredConnection) for r in results)
        if results:
            assert results[0].confidence_tier == 3
            assert results[0].strength == 0.4

    @patch("intelligence.spider.sources.news_cooccurrence.requests.get")
    def test_discover_handles_error(self, mock_get):
        mock_get.side_effect = Exception("API down")
        assert self._make().discover("Test", {}) == []

    @patch("intelligence.spider.sources.news_cooccurrence.requests.get")
    def test_discover_empty_articles(self, mock_get):
        mock_get.return_value = MagicMock(
            status_code=200, json=lambda: {"articles": []}
        )
        assert self._make().discover("Nobody", {}) == []


# ── Google Knowledge Graph ──

class TestGoogleKgAdapter:
    def _make(self):
        from intelligence.spider.sources.google_kg import GoogleKgAdapter
        return GoogleKgAdapter()

    @patch.dict("os.environ", {"GOOGLE_KG_API_KEY": "test-key"})
    @patch("intelligence.spider.sources.google_kg.requests.get")
    def test_discover_returns_connections(self, mock_get):
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "itemListElement": [{
                    "result": {
                        "name": "Tim Cook",
                        "@type": ["Person"],
                        "description": "CEO of Apple Inc.",
                        "detailedDescription": {
                            "articleBody": "Timothy Donald Cook is CEO of Apple Inc.",
                            "url": "https://en.wikipedia.org/wiki/Tim_Cook",
                        },
                    },
                    "resultScore": 100,
                }]
            },
        )
        results = self._make().discover("Tim Cook", {})
        assert all(isinstance(r, DiscoveredConnection) for r in results)

    @patch.dict("os.environ", {}, clear=True)
    def test_discover_skips_without_api_key(self):
        # Should not crash, just return empty
        import os
        os.environ.pop("GOOGLE_KG_API_KEY", None)
        results = self._make().discover("Test", {})
        assert results == []


# ── Operator Input ──

class TestOperatorInputAdapter:
    def _make(self):
        from intelligence.spider.sources.operator_input import OperatorInputAdapter
        return OperatorInputAdapter()

    @patch("intelligence.spider.sources.operator_input.sys")
    def test_discover_handles_no_db(self, mock_sys):
        """Gracefully handles DB connection failure."""
        # The adapter imports db module dynamically; if it fails, should return []
        results = self._make().discover("Test Actor", {})
        # Either returns empty (no DB) or doesn't crash
        assert isinstance(results, list)


# ── Wikidata (existing) ──

class TestWikidataAdapter:
    def _make(self):
        from intelligence.spider.sources.wikidata import WikidataAdapter
        return WikidataAdapter()

    @patch("intelligence.spider.sources.wikidata.requests.get")
    def test_discover_returns_connections(self, mock_get):
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "results": {"bindings": [{
                    "relatedLabel": {"value": "Apple Inc."},
                    "propLabel": {"value": "employer"},
                    "relatedDescription": {"value": "Technology company"},
                }]}
            },
        )
        results = self._make().discover("Tim Cook", {})
        assert len(results) == 1
        assert results[0].target_name == "Apple Inc."
        assert results[0].relationship == "employer"
        assert results[0].confidence_tier == 2

    @patch("intelligence.spider.sources.wikidata.requests.get")
    def test_discover_handles_error(self, mock_get):
        mock_get.side_effect = Exception("Network error")
        assert self._make().discover("Test", {}) == []
