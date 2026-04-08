"""Tests for intelligence.breaking_news — breaking news spike detection daemon."""

from __future__ import annotations

import json
import time
from unittest.mock import MagicMock, patch

import pytest

from intelligence.breaking_news import (
    COOLDOWN_SECONDS,
    GDELT_TIMESPAN_MINUTES,
    SPIKE_MULTIPLIER,
    WATCHLIST,
    check_gdelt,
    detect_spike,
    infer_direction,
    inject_signal,
    invalidate_caches,
    run_monitor,
)


# ---------------------------------------------------------------------------
# detect_spike
# ---------------------------------------------------------------------------

class TestDetectSpike:
    """Spike detection based on baseline article rates."""

    def test_spike_detected_at_threshold(self):
        # baseline 12/hr → expected 1.0 in 5 min → 3x = 3 articles triggers
        is_spike, ratio = detect_spike(3, baseline_per_hour=12.0, timespan_minutes=5, multiplier=3.0)
        assert is_spike is True
        assert ratio == pytest.approx(3.0)

    def test_spike_detected_above_threshold(self):
        is_spike, ratio = detect_spike(10, baseline_per_hour=12.0, timespan_minutes=5, multiplier=3.0)
        assert is_spike is True
        assert ratio == pytest.approx(10.0)

    def test_no_spike_below_threshold(self):
        is_spike, ratio = detect_spike(2, baseline_per_hour=12.0, timespan_minutes=5, multiplier=3.0)
        assert is_spike is False
        assert ratio == pytest.approx(2.0)

    def test_no_spike_zero_articles(self):
        is_spike, ratio = detect_spike(0, baseline_per_hour=12.0, timespan_minutes=5, multiplier=3.0)
        assert is_spike is False
        assert ratio == 0.0

    def test_zero_baseline_returns_no_spike(self):
        is_spike, ratio = detect_spike(100, baseline_per_hour=0.0)
        assert is_spike is False
        assert ratio == 0.0


# ---------------------------------------------------------------------------
# infer_direction
# ---------------------------------------------------------------------------

class TestInferDirection:
    """Direction inference from query and title keywords."""

    def test_bullish_query(self):
        assert infer_direction("ceasefire OR peace deal") == "buy"

    def test_bearish_query(self):
        assert infer_direction("crash OR circuit breaker") == "sell"

    def test_neutral_query(self):
        assert infer_direction("earthquake OR hurricane") == "neutral"

    def test_titles_override_neutral_query(self):
        result = infer_direction("some neutral topic", titles=["market crash", "stocks plunge"])
        assert result == "sell"

    def test_mixed_leans_on_count(self):
        # More bearish words
        result = infer_direction("crash plunge war", titles=["rally"])
        assert result == "sell"


# ---------------------------------------------------------------------------
# check_gdelt (mocked HTTP)
# ---------------------------------------------------------------------------

class TestCheckGdelt:
    """GDELT DOC API integration with mocked responses."""

    @patch("intelligence.breaking_news.requests.get")
    def test_artcount_format(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"artcount": 42}
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        result = check_gdelt("tariff OR trade war")
        assert result == 42

    @patch("intelligence.breaking_news.requests.get")
    def test_timeline_format(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "timeline": [{"data": [{"value": 10}, {"value": 5}]}]
        }
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        result = check_gdelt("test query")
        assert result == 15

    @patch("intelligence.breaking_news.requests.get")
    def test_network_error_returns_zero(self, mock_get):
        import requests as req
        mock_get.side_effect = req.ConnectionError("GDELT down")

        result = check_gdelt("test query")
        assert result == 0

    @patch("intelligence.breaking_news.requests.get")
    def test_invalid_json_returns_zero(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.side_effect = ValueError("bad json")
        mock_get.return_value = mock_resp

        result = check_gdelt("test query")
        assert result == 0


# ---------------------------------------------------------------------------
# inject_signal (mocked DB)
# ---------------------------------------------------------------------------

class TestInjectSignal:
    """Signal injection writes correct data to signal_data."""

    def test_inject_signal_executes_insert(self):
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        event = {
            "query": "tariff OR trade war",
            "category": "trade",
            "article_count": 50,
            "spike_ratio": 5.0,
            "direction": "sell",
        }
        inject_signal(mock_engine, event)

        assert mock_conn.execute.called
        call_args = mock_conn.execute.call_args
        params = call_args[0][1]
        assert params["signal_type"] == "breaking_news"
        assert params["ticker"] == "MACRO"
        assert params["direction"] == "sell"
        assert params["source_id"] == "gdelt_breaking"
        assert "spike_ratio" in params["data"]

    def test_magnitude_capped_at_10(self):
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        event = {
            "query": "test",
            "category": "test",
            "article_count": 999,
            "spike_ratio": 50.0,
            "direction": "neutral",
        }
        inject_signal(mock_engine, event)

        params = mock_conn.execute.call_args[0][1]
        assert params["magnitude"] == 10.0


# ---------------------------------------------------------------------------
# invalidate_caches
# ---------------------------------------------------------------------------

class TestInvalidateCaches:
    """Cache invalidation file-based signalling."""

    @patch("intelligence.breaking_news.CACHE_INVALIDATION_FILE")
    def test_writes_timestamp_file(self, mock_path):
        invalidate_caches()
        mock_path.write_text.assert_called_once()
        written = mock_path.write_text.call_args[0][0]
        # Should be an ISO timestamp
        assert "T" in written


# ---------------------------------------------------------------------------
# run_monitor cooldown behaviour
# ---------------------------------------------------------------------------

class TestRunMonitorCooldown:
    """Cooldown prevents duplicate detections within 15 minutes."""

    @patch("intelligence.breaking_news.invalidate_caches")
    @patch("intelligence.breaking_news.inject_signal")
    @patch("intelligence.breaking_news.check_gdelt")
    @patch("db.get_engine", return_value=MagicMock())
    def test_cooldown_prevents_duplicate(self, mock_engine, mock_gdelt, mock_inject, mock_invalidate):
        """Single cycle with spike should inject exactly once per query."""
        # Return high article count for all queries
        mock_gdelt.return_value = 999

        # Patch WATCHLIST to a single item for simplicity
        single_watch = [{"query": "test spike", "category": "test", "baseline_per_hour": 1}]
        with patch("intelligence.breaking_news.WATCHLIST", single_watch):
            # Run once — should detect and inject
            run_monitor(interval=1, once=True)
            assert mock_inject.call_count == 1


# ---------------------------------------------------------------------------
# Watchlist sanity
# ---------------------------------------------------------------------------

class TestWatchlist:
    """Watchlist structure validation."""

    def test_all_entries_have_required_keys(self):
        for item in WATCHLIST:
            assert "query" in item, f"Missing 'query' in {item}"
            assert "category" in item, f"Missing 'category' in {item}"
            assert "baseline_per_hour" in item, f"Missing 'baseline_per_hour' in {item}"
            assert item["baseline_per_hour"] > 0, f"Invalid baseline for {item['query']}"

    def test_at_least_10_watchlist_entries(self):
        assert len(WATCHLIST) >= 10
