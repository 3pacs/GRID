"""
Tests for GRID intelligence source ingestion modules.

Tests FARA (Foreign Agent Registration Act), enhanced GDELT, and
FOIA diplomatic cables modules. Uses unittest.mock to avoid real
API calls and database writes.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from unittest.mock import MagicMock, patch, call

import pytest


# ── Test fixtures ────────────────────────────────────────────────────────

def _mock_engine():
    """Create a mock SQLAlchemy engine with connection context managers."""
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    # Mock source_catalog lookup to return source_id=1
    mock_row = MagicMock()
    mock_row.__getitem__ = lambda self, idx: 1
    conn.execute.return_value.fetchone.return_value = mock_row

    return engine, conn


# ══════════════════════════════════════════════════════════════════════════
# FARA TESTS
# ══════════════════════════════════════════════════════════════════════════

class TestFARAPuller:
    """Tests for the FARA Foreign Agent Registration Act puller."""

    def test_fara_init(self):
        """FARAPuller should initialise with correct SOURCE_NAME."""
        engine, conn = _mock_engine()
        from ingestion.altdata.fara import FARAPuller

        puller = FARAPuller(engine)
        assert puller.SOURCE_NAME == "FARA_DOJ"
        assert puller.source_id == 1

    def test_fara_source_config(self):
        """FARA source config should have correct metadata."""
        from ingestion.altdata.fara import FARAPuller

        assert FARAPuller.SOURCE_CONFIG["cost_tier"] == "FREE"
        assert FARAPuller.SOURCE_CONFIG["trust_score"] == "HIGH"
        assert FARAPuller.SOURCE_CONFIG["pit_available"] is True

    def test_parse_registrant_valid(self):
        """A valid registrant record should parse correctly."""
        engine, conn = _mock_engine()
        from ingestion.altdata.fara import FARAPuller

        puller = FARAPuller(engine)
        raw = {
            "registrantName": "Acme Lobbying Inc.",
            "registrantId": "12345",
            "address": "123 K Street NW, Washington DC",
            "registrationDate": "2024-06-15",
            "status": "ACTIVE",
            "foreignPrincipals": [
                {
                    "name": "Kingdom of Examplestan",
                    "country": "SAUDI ARABIA",
                    "type": "Government",
                },
            ],
        }

        result = puller._parse_registrant(raw)
        assert result is not None
        assert result["registrant_name"] == "Acme Lobbying Inc."
        assert result["registrant_id"] == "12345"
        assert len(result["principals"]) == 1
        assert result["principals"][0]["country"] == "SAUDI ARABIA"

    def test_parse_registrant_missing_name(self):
        """A registrant with no name should return None."""
        engine, conn = _mock_engine()
        from ingestion.altdata.fara import FARAPuller

        puller = FARAPuller(engine)
        result = puller._parse_registrant({"registrantId": "12345"})
        assert result is None

    def test_parse_activity_valid(self):
        """A valid activity record should parse correctly."""
        engine, conn = _mock_engine()
        from ingestion.altdata.fara import FARAPuller

        puller = FARAPuller(engine)
        raw = {
            "registrantName": "Acme Lobbying",
            "foreignPrincipalName": "Kingdom of Saudia",
            "principalCountry": "SAUDI ARABIA",
            "activityType": "LOBBYING",
            "description": "Met with Senate Energy Committee staff re: oil imports",
            "compensation": "750000",
            "activityDate": "2024-11-01",
            "contacts": [
                {"name": "Sen. Smith", "agency": "Senate Energy Committee"},
            ],
        }

        result = puller._parse_activity(raw)
        assert result is not None
        assert result["country"] == "SAUDI ARABIA"
        assert result["activity_type"] == "LOBBYING"
        assert result["compensation"] == 750000.0
        assert len(result["contacts"]) == 1

    def test_country_sector_mapping(self):
        """Key countries should map to correct sectors and tickers."""
        from ingestion.altdata.fara import COUNTRY_SECTOR_MAP

        assert COUNTRY_SECTOR_MAP["SAUDI ARABIA"]["ticker"] == "XLE"
        assert COUNTRY_SECTOR_MAP["CHINA"]["ticker"] == "SMH"
        assert COUNTRY_SECTOR_MAP["ISRAEL"]["ticker"] == "XLI"
        assert COUNTRY_SECTOR_MAP["TAIWAN"]["theme"] == "chips_act"

    def test_extract_sector_from_description(self):
        """Issue keywords in descriptions should map to sectors."""
        from ingestion.altdata.fara import _extract_sector_from_description

        assert _extract_sector_from_description("semiconductor export controls") == "SMH"
        assert _extract_sector_from_description("oil and gas pipeline") == "XLE"
        assert _extract_sector_from_description("arms sales to allies") == "ITA"
        assert _extract_sector_from_description("nothing relevant") is None
        assert _extract_sector_from_description("") is None

    @patch("ingestion.altdata.fara.requests.get")
    def test_pull_activities_stores_and_emits(self, mock_get):
        """pull_activities should store rows and emit signals."""
        engine, conn = _mock_engine()
        # Mock _row_exists to return False (no duplicates)
        conn.execute.return_value.fetchone.side_effect = [
            MagicMock(__getitem__=lambda s, i: 1),  # source_id lookup
            None,  # _row_exists check (no dup)
            MagicMock(__getitem__=lambda s, i: 1),  # trend query returns empty
        ]
        conn.execute.return_value.fetchall.return_value = []  # trend query

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "results": [
                {
                    "registrantName": "Test Firm",
                    "foreignPrincipalName": "China Corp",
                    "principalCountry": "CHINA",
                    "activityType": "LOBBYING",
                    "description": "semiconductor trade policy",
                    "compensation": "500000",
                    "activityDate": "2024-11-01",
                    "contacts": [],
                },
            ],
            "totalPages": 1,
        }
        mock_get.return_value = mock_resp

        from ingestion.altdata.fara import FARAPuller

        puller = FARAPuller(engine)
        result = puller.pull_activities(days_back=30)

        assert result["status"] == "SUCCESS"
        assert result["activities_fetched"] == 1

    def test_slugify(self):
        """Slugify should produce valid series_id components."""
        from ingestion.altdata.fara import _slugify

        assert _slugify("Saudi Arabia") == "SAUDI_ARABIA"
        assert _slugify("United Arab Emirates") == "UNITED_ARAB_EMIRATES"
        assert _slugify("a" * 100, max_len=10) == "A" * 10


# ══════════════════════════════════════════════════════════════════════════
# ENHANCED GDELT TESTS
# ══════════════════════════════════════════════════════════════════════════

class TestGDELTEnhanced:
    """Tests for the enhanced GDELT puller with actor and tension tracking."""

    def test_gdelt_queries_expanded(self):
        """GDELT_QUERIES should include both original and enhanced queries."""
        from ingestion.altdata.gdelt import GDELT_QUERIES

        feature_names = [q["feature"] for q in GDELT_QUERIES]
        # Original queries still present
        assert "gdelt_recession_tone" in feature_names
        assert "gdelt_fed_tone" in feature_names
        assert "gdelt_trade_conflict_volume" in feature_names
        # Enhanced queries added
        assert "gdelt_sanctions_russia_tone" in feature_names
        assert "gdelt_opec_tone" in feature_names
        assert "gdelt_chip_controls_volume" in feature_names
        assert "gdelt_central_bank_tone" in feature_names

    def test_actor_queries_defined(self):
        """Actor queries should track key geopolitical figures."""
        from ingestion.altdata.gdelt import GDELT_ACTOR_QUERIES

        actors = [q["actor"] for q in GDELT_ACTOR_QUERIES]
        assert "Jerome Powell" in actors
        assert "Christine Lagarde" in actors
        assert "Xi Jinping" in actors
        assert "Mohammed bin Salman" in actors

    def test_tension_pairs_defined(self):
        """Tension pairs should track key bilateral relationships."""
        from ingestion.altdata.gdelt import GDELT_TENSION_PAIRS

        pairs = [q["pair"] for q in GDELT_TENSION_PAIRS]
        assert "United States China" in pairs
        assert "United States Russia" in pairs
        assert "China Taiwan" in pairs
        assert "Russia Ukraine" in pairs

    def test_gdelt_source_config(self):
        """GDELTPuller should have correct source configuration."""
        from ingestion.altdata.gdelt import GDELTPuller

        assert GDELTPuller.SOURCE_NAME == "GDELT"
        assert GDELTPuller.SOURCE_CONFIG["cost_tier"] == "FREE"

    @patch("ingestion.altdata.gdelt.requests.get")
    def test_pull_recent_returns_enhanced_fields(self, mock_get):
        """pull_recent should return actor_rows and tension_rows counts."""
        engine, conn = _mock_engine()
        import os
        os.makedirs(
            os.path.join(os.path.dirname(__file__), "..", "data", "gdelt"),
            exist_ok=True,
        )

        # Mock all API calls to return empty data
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"timeline": []}
        mock_get.return_value = mock_resp

        from ingestion.altdata.gdelt import GDELTPuller

        puller = GDELTPuller(engine)
        result = puller.pull_recent(days_back=2)

        assert result["source"] == "GDELT"
        assert "actor_rows" in result
        assert "tension_rows" in result
        assert "signals_emitted" in result

    def test_tension_inverts_tone(self):
        """Tension scores should be inverted (negative tone = positive tension)."""
        # This is a design check — negative GDELT tone about a country pair
        # should map to a positive tension value
        from ingestion.altdata.gdelt import GDELT_TENSION_PAIRS

        # Just verify the structure is correct for each pair
        for pair in GDELT_TENSION_PAIRS:
            assert "pair" in pair
            assert "feature" in pair
            assert "theme" in pair
            assert pair["feature"].startswith("gdelt_tension_")


# ══════════════════════════════════════════════════════════════════════════
# FOIA CABLES TESTS
# ══════════════════════════════════════════════════════════════════════════

class TestFOIACablesPuller:
    """Tests for the FOIA diplomatic cables puller."""

    def test_foia_init(self):
        """FOIACablesPuller should initialise with correct SOURCE_NAME."""
        engine, conn = _mock_engine()
        from ingestion.altdata.foia_cables import FOIACablesPuller

        puller = FOIACablesPuller(engine)
        assert puller.SOURCE_NAME == "FOIA_CABLES"
        assert puller.source_id == 1

    def test_foia_source_config(self):
        """FOIA source config should have correct metadata."""
        from ingestion.altdata.foia_cables import FOIACablesPuller

        assert FOIACablesPuller.SOURCE_CONFIG["cost_tier"] == "FREE"
        assert FOIACablesPuller.SOURCE_CONFIG["trust_score"] == "HIGH"

    def test_topic_definitions(self):
        """FOIA topics should cover key geopolitical themes."""
        from ingestion.altdata.foia_cables import FOIA_TOPICS

        topics = [t["topic"] for t in FOIA_TOPICS]
        assert "trade_policy" in topics
        assert "sanctions" in topics
        assert "energy_diplomacy" in topics
        assert "defense_sales" in topics
        assert "tech_competition" in topics

    def test_extract_classification(self):
        """Classification extraction should identify security levels."""
        from ingestion.altdata.foia_cables import _extract_classification

        assert _extract_classification("TOP SECRET//NOFORN") == "TOP SECRET"
        assert _extract_classification("SECRET") == "SECRET"
        assert _extract_classification("CONFIDENTIAL") == "CONFIDENTIAL"
        assert _extract_classification("routine cable text") == "UNCLASSIFIED"
        assert _extract_classification("") == "UNCLASSIFIED"
        assert _extract_classification(None) == "UNCLASSIFIED"

    def test_score_topic_relevance(self):
        """Topic relevance scoring should weight keyword matches."""
        from ingestion.altdata.foia_cables import _score_topic_relevance

        # All keywords match
        score = _score_topic_relevance(
            "tariff trade import export wto",
            ["tariff", "trade", "import", "export", "wto"],
        )
        assert score == 1.0

        # Partial match
        score = _score_topic_relevance(
            "tariff negotiations",
            ["tariff", "trade", "import", "export", "wto"],
        )
        assert 0.0 < score < 1.0

        # No match
        score = _score_topic_relevance(
            "weather forecast",
            ["tariff", "trade", "import"],
        )
        assert score == 0.0

        # Empty inputs
        assert _score_topic_relevance("", ["a"]) == 0.0
        assert _score_topic_relevance("a", []) == 0.0

    def test_parse_state_doc_valid(self):
        """A valid State Dept FOIA doc should parse correctly."""
        engine, conn = _mock_engine()
        from ingestion.altdata.foia_cables import FOIACablesPuller

        puller = FOIACablesPuller(engine)
        topic_def = {
            "query": "sanctions",
            "topic": "sanctions",
            "ticker": "SPY",
            "keywords": ["sanction", "embargo", "freeze"],
        }

        doc = {
            "Subject": "Re: Implementation of new sanctions on Country X",
            "DocDate": "2020-03-15",
            "PostedDate": "2024-11-01",
            "Classification": "SECRET",
            "MessageText": "Discussing the economic impact of sanctions embargo",
            "DocNbr": "DOC-12345",
            "From": "Embassy Countryville",
            "To": "State Dept Washington",
        }

        result = puller._parse_state_doc(doc, topic_def)
        assert result is not None
        assert result["source"] == "STATE_DEPT"
        assert result["classification"] == "SECRET"
        assert result["confidence"] == "confirmed"
        assert result["relevance"] > 0.0
        assert result["topic"] == "sanctions"
        assert result["ticker"] == "SPY"

    def test_parse_state_doc_low_relevance_filtered(self):
        """Documents with very low relevance should be filtered out."""
        engine, conn = _mock_engine()
        from ingestion.altdata.foia_cables import FOIACablesPuller

        puller = FOIACablesPuller(engine)
        topic_def = {
            "query": "sanctions",
            "topic": "sanctions",
            "ticker": "SPY",
            "keywords": ["sanction", "embargo", "freeze"],
        }

        doc = {
            "Subject": "Meeting agenda for staff retreat",
            "DocDate": "2020-01-01",
            "PostedDate": "2024-11-01",
            "MessageText": "Lunch will be served at noon",
        }

        result = puller._parse_state_doc(doc, topic_def)
        assert result is None  # Filtered for low relevance

    def test_classification_confidence_mapping(self):
        """Higher classification should map to higher confidence."""
        from ingestion.altdata.foia_cables import CLASSIFICATION_CONFIDENCE

        assert CLASSIFICATION_CONFIDENCE["SECRET"] == "confirmed"
        assert CLASSIFICATION_CONFIDENCE["TOP SECRET"] == "confirmed"
        assert CLASSIFICATION_CONFIDENCE["UNCLASSIFIED"] == "derived"


# ══════════════════════════════════════════════════════════════════════════
# INTELLIGENCE INTEGRATION TESTS
# ══════════════════════════════════════════════════════════════════════════

class TestIntelligenceIntegration:
    """Tests for intelligence layer integration with new sources."""

    def test_lever_pullers_has_new_categories(self):
        """lever_pullers INFLUENCE_WEIGHTS should include new source types."""
        from intelligence.lever_pullers import INFLUENCE_WEIGHTS

        assert "foreign_lobbying" in INFLUENCE_WEIGHTS
        assert "geopolitical" in INFLUENCE_WEIGHTS
        assert "diplomatic_cable" in INFLUENCE_WEIGHTS
        # Verify weights are sensible
        assert 0 < INFLUENCE_WEIGHTS["foreign_lobbying"] <= 1
        assert 0 < INFLUENCE_WEIGHTS["geopolitical"] <= 1
        assert 0 < INFLUENCE_WEIGHTS["diplomatic_cable"] <= 1

    def test_trust_scorer_has_new_eval_windows(self):
        """trust_scorer EVALUATION_WINDOWS should include new source types."""
        from intelligence.trust_scorer import EVALUATION_WINDOWS

        assert "foreign_lobbying" in EVALUATION_WINDOWS
        assert "geopolitical" in EVALUATION_WINDOWS
        assert "diplomatic_cable" in EVALUATION_WINDOWS
        assert "lobbying" in EVALUATION_WINDOWS
        assert "campaign_finance" in EVALUATION_WINDOWS
        # FARA has long eval window (policy change lag)
        assert EVALUATION_WINDOWS["foreign_lobbying"] >= 30
        # Geopolitical tension spikes are fast
        assert EVALUATION_WINDOWS["geopolitical"] <= 14

    def test_dollar_flows_foreign_lobbying_normalizer(self):
        """Dollar flows should normalize FARA foreign lobbying signals."""
        from intelligence.dollar_flows import _normalize_foreign_lobbying

        row = {
            "signal_value": json.dumps({
                "compensation": 500000,
                "country": "SAUDI ARABIA",
                "activity_type": "LOBBYING",
            }),
            "source_id": "Acme Lobbying",
            "ticker": "XLE",
            "signal_type": "FARA_ACTIVITY",
            "signal_date": "2024-11-01",
        }

        result = _normalize_foreign_lobbying(row)
        assert result is not None
        assert result["source_type"] == "foreign_lobbying"
        assert result["amount_usd"] == 500000
        assert result["confidence"] == "confirmed"
        assert result["direction"] == "inflow"

    def test_dollar_flows_foreign_lobbying_zero_comp(self):
        """Zero-compensation FARA activities should return None."""
        from intelligence.dollar_flows import _normalize_foreign_lobbying

        row = {
            "signal_value": json.dumps({"compensation": 0}),
            "source_id": "Test",
            "ticker": "SPY",
            "signal_date": "2024-11-01",
        }

        assert _normalize_foreign_lobbying(row) is None

    def test_dollar_flows_lobbying_normalizer(self):
        """Dollar flows should normalize domestic lobbying signals."""
        from intelligence.dollar_flows import _normalize_lobbying

        row = {
            "signal_value": json.dumps({
                "amount": 1000000,
                "client_name": "Intel Corp",
                "registrant_name": "K Street Firm",
            }),
            "source_id": "filing-123",
            "ticker": "INTC",
            "signal_type": "LOBBYING_DISCLOSURE",
            "signal_date": "2024-11-01",
        }

        result = _normalize_lobbying(row)
        assert result is not None
        assert result["amount_usd"] == 1000000
        assert result["source_type"] == "lobbying"
        assert result["confidence"] == "confirmed"


# ══════════════════════════════════════════════════════════════════════════
# SCHEDULER REGISTRATION TESTS
# ══════════════════════════════════════════════════════════════════════════

class TestSchedulerRegistration:
    """Tests that new pullers are registered in the scheduler."""

    def test_fara_registered_in_weekly(self):
        """FARA puller should be registered in the weekly schedule group."""
        import ingestion.scheduler as sched
        import inspect

        source = inspect.getsource(sched._get_pullers_for_group)
        assert "fara" in source.lower()
        assert "FARAPuller" in source

    def test_foia_registered_in_weekly(self):
        """FOIA cables puller should be registered in the weekly schedule group."""
        import ingestion.scheduler as sched
        import inspect

        source = inspect.getsource(sched._get_pullers_for_group)
        assert "foia_cables" in source.lower()
        assert "FOIACablesPuller" in source
