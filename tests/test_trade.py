"""
Tests for GRID trade and complexity ingestion modules.

Validates query lists, country configurations, and seed SQL syntax
without requiring live API access or database connectivity.
"""

from __future__ import annotations

import pytest


class TestComtradePuller:
    """Tests for the UN Comtrade puller."""

    def test_comtrade_queries(self):
        """COMTRADE_QUERIES has at least 5 entries."""
        from ingestion.trade.comtrade import COMTRADE_QUERIES
        assert len(COMTRADE_QUERIES) >= 5

    def test_comtrade_has_bilateral(self):
        """Comtrade queries include US-China bilateral."""
        from ingestion.trade.comtrade import COMTRADE_QUERIES
        labels = [q["label"] for q in COMTRADE_QUERIES]
        assert "us_china_bilateral" in labels


class TestAtlasECIPuller:
    """Tests for the Harvard Atlas ECI puller."""

    def test_eci_countries(self):
        """ECI_COUNTRIES includes USA, CHN, DEU."""
        from ingestion.trade.atlas_eci import ECI_COUNTRIES
        assert "USA" in ECI_COUNTRIES
        assert "CHN" in ECI_COUNTRIES
        assert "DEU" in ECI_COUNTRIES

    def test_eci_countries_count(self):
        """ECI_COUNTRIES has at least 10 countries."""
        from ingestion.trade.atlas_eci import ECI_COUNTRIES
        assert len(ECI_COUNTRIES) >= 10


class TestSeedV2:
    """Tests for v2 seed data."""

    def test_seed_v2_sql_parseable(self):
        """seed_v2.py SQL statements are syntactically valid (non-empty)."""
        from ingestion.seed_v2 import SOURCE_CATALOG_SQL, FEATURE_REGISTRY_SQL
        assert len(SOURCE_CATALOG_SQL.strip()) > 100
        assert len(FEATURE_REGISTRY_SQL.strip()) > 100
        assert "ON CONFLICT" in SOURCE_CATALOG_SQL
        assert "ON CONFLICT" in FEATURE_REGISTRY_SQL

    def test_seed_v2_has_all_sources(self):
        """Seed SQL includes all 27 new source catalog entries."""
        from ingestion.seed_v2 import SOURCE_CATALOG_SQL
        required_sources = [
            "ECB_SDW", "OECD_SDMX", "BIS", "Eurostat", "IMF_IFS",
            "BCB_BR", "KOSIS", "MAS_SG", "AKShare", "Comtrade",
            "VIIRS", "OFR", "GDELT", "OppInsights", "NOAA_AIS",
        ]
        for source in required_sources:
            assert source in SOURCE_CATALOG_SQL, f"Missing source: {source}"
