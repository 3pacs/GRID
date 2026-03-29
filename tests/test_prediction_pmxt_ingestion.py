"""Unit tests for ingestion/altdata/prediction_pmxt.py PmxtPredictionPuller."""

from __future__ import annotations

import types
from datetime import date
from unittest.mock import MagicMock, patch, PropertyMock

import pytest


# ---------------------------------------------------------------------------
# Helpers — mock pmxt SDK objects
# ---------------------------------------------------------------------------


def _make_outcome(name: str = "Yes", price: float = 0.65, volume: float = 1000.0):
    """Create a mock pmxt Outcome object."""
    o = MagicMock()
    o.name = name
    o.yes_price = price
    o.price = price
    o.volume = volume
    return o


def _make_market(market_id: str = "m1", outcomes=None, volume: float = 5000.0):
    """Create a mock pmxt Market object."""
    m = MagicMock()
    m.id = market_id
    m.outcomes = outcomes or [_make_outcome()]
    m.volume = volume
    return m


def _make_event(
    event_id: str = "e1",
    title: str = "Will the Fed cut rates in 2026?",
    description: str = "Federal Reserve rate decision",
    platform: str = "polymarket",
    markets=None,
):
    """Create a mock pmxt Event object."""
    e = MagicMock()
    e.id = event_id
    e.title = title
    e.description = description
    e.platform = platform
    e.markets = markets or [_make_market()]
    return e


# ---------------------------------------------------------------------------
# Module-level patching
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _patch_config():
    """Patch config.settings for all tests."""
    mock_settings = MagicMock()
    mock_settings.PMXT_ENABLED = True
    mock_settings.PMXT_POLYMARKET_PRIVATE_KEY = "test-key"
    mock_settings.PMXT_KALSHI_API_KEY = "test-api"
    mock_settings.PMXT_KALSHI_PRIVATE_KEY_PATH = "/tmp/test.pem"
    with patch("ingestion.altdata.prediction_pmxt.settings", mock_settings):
        yield mock_settings


@pytest.fixture()
def mock_engine():
    """Create a mock SQLAlchemy engine."""
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    # _resolve_source_id returns a valid id
    conn.execute.return_value.fetchone.return_value = (1,)
    return engine


@pytest.fixture()
def mock_pmxt():
    """Patch the pmxt module."""
    fake_pmxt = MagicMock()
    fake_pmxt.fetch_events = MagicMock(return_value=[])
    with patch.dict("sys.modules", {"pmxt": fake_pmxt}):
        with patch("ingestion.altdata.prediction_pmxt.pmxt", fake_pmxt):
            with patch("ingestion.altdata.prediction_pmxt._PMXT_AVAILABLE", True):
                yield fake_pmxt


# ---------------------------------------------------------------------------
# Helper function tests
# ---------------------------------------------------------------------------


class TestSanitizeSlug:
    def test_basic(self):
        from ingestion.altdata.prediction_pmxt import _sanitize_slug

        assert _sanitize_slug("Hello World") == "hello_world"

    def test_special_chars_stripped(self):
        from ingestion.altdata.prediction_pmxt import _sanitize_slug

        assert _sanitize_slug("Fed rate? 2026!") == "fed_rate_2026"

    def test_truncation(self):
        from ingestion.altdata.prediction_pmxt import _sanitize_slug

        long_text = "a" * 200
        result = _sanitize_slug(long_text, max_len=80)
        assert len(result) <= 80

    def test_empty_string(self):
        from ingestion.altdata.prediction_pmxt import _sanitize_slug

        assert _sanitize_slug("") == ""


class TestIsEconomicallyRelevant:
    def test_matches_keyword(self):
        from ingestion.altdata.prediction_pmxt import _is_economically_relevant

        assert _is_economically_relevant("Will the Fed cut rates?") is True

    def test_no_match(self):
        from ingestion.altdata.prediction_pmxt import _is_economically_relevant

        assert _is_economically_relevant("Will it rain tomorrow?") is False

    def test_matches_in_description(self):
        from ingestion.altdata.prediction_pmxt import _is_economically_relevant

        assert _is_economically_relevant("Some title", "inflation data") is True


class TestSafeFloat:
    def test_valid(self):
        from ingestion.altdata.prediction_pmxt import _safe_float

        assert _safe_float(0.75) == 0.75
        assert _safe_float("0.5") == 0.5

    def test_none(self):
        from ingestion.altdata.prediction_pmxt import _safe_float

        assert _safe_float(None) is None

    def test_nan(self):
        from ingestion.altdata.prediction_pmxt import _safe_float

        assert _safe_float(float("nan")) is None

    def test_inf(self):
        from ingestion.altdata.prediction_pmxt import _safe_float

        assert _safe_float(float("inf")) is None

    def test_bad_string(self):
        from ingestion.altdata.prediction_pmxt import _safe_float

        assert _safe_float("not_a_number") is None


# ---------------------------------------------------------------------------
# PmxtPredictionPuller tests
# ---------------------------------------------------------------------------


class TestPmxtPredictionPuller:
    def test_pull_skips_when_pmxt_not_available(self, mock_engine):
        """Puller returns SKIPPED when pmxt SDK is not installed."""
        with patch("ingestion.altdata.prediction_pmxt._PMXT_AVAILABLE", False):
            from ingestion.altdata.prediction_pmxt import PmxtPredictionPuller

            puller = PmxtPredictionPuller(db_engine=mock_engine)
            result = puller.pull()

            assert result["status"] == "SKIPPED"
            assert result["events_scanned"] == 0

    def test_pull_skips_when_disabled(self, mock_engine, mock_pmxt, _patch_config):
        """Puller returns SKIPPED when PMXT_ENABLED is False."""
        _patch_config.PMXT_ENABLED = False
        from ingestion.altdata.prediction_pmxt import PmxtPredictionPuller

        puller = PmxtPredictionPuller(db_engine=mock_engine)
        result = puller.pull()

        assert result["status"] == "SKIPPED"
        assert "PMXT_ENABLED" in result.get("reason", "")

    def test_pull_success_with_events(self, mock_engine, mock_pmxt):
        """Puller stores events when pmxt returns data."""
        event = _make_event(title="Will the Fed cut rates?")
        mock_pmxt.fetch_events.return_value = [event]

        from ingestion.altdata.prediction_pmxt import PmxtPredictionPuller

        puller = PmxtPredictionPuller(db_engine=mock_engine)
        puller.source_id = 1

        # Patch _row_exists to always return False (no duplicates)
        puller._row_exists = lambda *a, **kw: False

        result = puller.pull()

        assert result["status"] == "SUCCESS"
        assert result["events_scanned"] > 0

    def test_pull_handles_empty_events(self, mock_engine, mock_pmxt):
        """Puller succeeds with zero events when nothing matches."""
        mock_pmxt.fetch_events.return_value = []

        from ingestion.altdata.prediction_pmxt import PmxtPredictionPuller

        puller = PmxtPredictionPuller(db_engine=mock_engine)
        result = puller.pull()

        assert result["status"] == "SUCCESS"
        assert result["events_scanned"] == 0
        assert result["series_stored"] == 0

    def test_pull_handles_api_exception(self, mock_engine, mock_pmxt):
        """Puller catches API exceptions per-platform and continues."""
        mock_pmxt.fetch_events.side_effect = ConnectionError("API down")

        from ingestion.altdata.prediction_pmxt import PmxtPredictionPuller

        puller = PmxtPredictionPuller(db_engine=mock_engine)
        result = puller.pull()

        # Should succeed overall even if individual platforms fail
        assert result["status"] == "SUCCESS"
        assert result["events_scanned"] == 0

    def test_pull_all_returns_list(self, mock_engine, mock_pmxt):
        """pull_all() returns a list containing the result dict."""
        mock_pmxt.fetch_events.return_value = []

        from ingestion.altdata.prediction_pmxt import PmxtPredictionPuller

        puller = PmxtPredictionPuller(db_engine=mock_engine)
        results = puller.pull_all()

        assert isinstance(results, list)
        assert len(results) == 1

    def test_fetch_platform_events_filters_irrelevant(self, mock_engine, mock_pmxt):
        """Only economically relevant events are returned."""
        irrelevant = _make_event(
            title="Will it snow in Miami?",
            description="Weather prediction",
        )
        mock_pmxt.fetch_events.return_value = [irrelevant]

        from ingestion.altdata.prediction_pmxt import PmxtPredictionPuller

        puller = PmxtPredictionPuller(db_engine=mock_engine)
        events = puller._fetch_platform_events("polymarket")

        assert len(events) == 0

    def test_fetch_platform_events_includes_relevant(self, mock_engine, mock_pmxt):
        """Economically relevant events are included."""
        relevant = _make_event(
            title="Will the Fed raise interest rates?",
            description="FOMC decision",
        )
        mock_pmxt.fetch_events.return_value = [relevant]

        from ingestion.altdata.prediction_pmxt import PmxtPredictionPuller

        puller = PmxtPredictionPuller(db_engine=mock_engine)
        events = puller._fetch_platform_events("polymarket")

        assert len(events) > 0
        assert events[0]["platform"] == "polymarket"
        assert events[0]["yes_price"] == 0.65

    def test_store_event_deduplicates(self, mock_engine, mock_pmxt):
        """_store_event returns False when row already exists."""
        conn_mock = MagicMock()
        # _row_exists returns True (duplicate found)
        conn_mock.execute.return_value.fetchone.return_value = (1,)

        from ingestion.altdata.prediction_pmxt import PmxtPredictionPuller

        puller = PmxtPredictionPuller(db_engine=mock_engine)
        puller.source_id = 1

        event = {
            "platform": "polymarket",
            "event_slug": "fed_rate_cut",
            "outcome": "yes",
            "title": "Fed rate cut",
            "yes_price": 0.7,
            "volume": 1000,
            "event_id": "e1",
            "market_id": "m1",
        }

        result = puller._store_event(conn_mock, event, date.today())
        assert result is False

    def test_none_price_skipped(self, mock_engine, mock_pmxt):
        """Events with None price are not included."""
        outcome = _make_outcome(price=None)
        outcome.yes_price = None
        outcome.price = None
        market = _make_market(outcomes=[outcome])
        event = _make_event(
            title="Fed rate cut?",
            markets=[market],
        )
        mock_pmxt.fetch_events.return_value = [event]

        from ingestion.altdata.prediction_pmxt import PmxtPredictionPuller

        puller = PmxtPredictionPuller(db_engine=mock_engine)
        events = puller._fetch_platform_events("polymarket")

        assert len(events) == 0
