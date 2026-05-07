"""
Tests for the AI-Trader signal adapter.

Covers:
  - Signal extraction with mocked HTTP responses
  - Direction mapping (buy/sell/short/cover)
  - Consensus signal generation
  - Confidence scaling by leaderboard rank
  - Graceful degradation on API errors
  - Disabled state
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from intelligence.adapters.ai_trader_adapter import (
    AITraderAdapter,
    _ACTION_MAP,
    _ACTION_VALUE,
)
from intelligence.signal_registry import Direction, SignalType


@pytest.fixture()
def adapter():
    return AITraderAdapter()


@pytest.fixture()
def mock_engine():
    return MagicMock()


# Sample API responses matching AI-Trader format
LEADERBOARD_RESPONSE = [
    {"agent_id": "agent_1", "name": "Alpha Bot", "unrealized_pnl": 5000},
    {"agent_id": "agent_2", "name": "Beta Bot", "unrealized_pnl": 3000},
    {"agent_id": "agent_3", "name": "Gamma Bot", "unrealized_pnl": 1000},
]

SIGNAL_FEED_RESPONSE = [
    {
        "agent_id": "agent_1",
        "symbol": "AAPL",
        "action": "buy",
        "price": 180.5,
        "quantity": 100,
        "market": "stocks",
        "executed_at": "2026-04-07T12:00:00Z",
    },
    {
        "agent_id": "agent_2",
        "symbol": "AAPL",
        "action": "buy",
        "price": 181.0,
        "quantity": 50,
        "market": "stocks",
        "executed_at": "2026-04-07T12:05:00Z",
    },
    {
        "agent_id": "agent_3",
        "symbol": "TSLA",
        "action": "sell",
        "price": 250.0,
        "quantity": 30,
        "market": "stocks",
        "executed_at": "2026-04-07T12:10:00Z",
    },
]


class TestAITraderAdapterProperties:
    def test_source_module(self, adapter):
        assert adapter.source_module == "ai_trader"

    def test_refresh_interval(self, adapter):
        assert adapter.refresh_interval_hours == 4.0


class TestActionMapping:
    def test_buy_is_bullish(self):
        assert _ACTION_MAP["buy"] == Direction.BULLISH

    def test_sell_is_bearish(self):
        assert _ACTION_MAP["sell"] == Direction.BEARISH

    def test_short_is_bearish(self):
        assert _ACTION_MAP["short"] == Direction.BEARISH

    def test_cover_is_bullish(self):
        assert _ACTION_MAP["cover"] == Direction.BULLISH

    def test_action_values(self):
        assert _ACTION_VALUE["buy"] == 1.0
        assert _ACTION_VALUE["sell"] == -1.0
        assert _ACTION_VALUE["short"] == -0.6
        assert _ACTION_VALUE["cover"] == 0.6


class TestDisabledState:
    @patch("intelligence.adapters.ai_trader_adapter.settings")
    def test_returns_empty_when_disabled(self, mock_settings, adapter, mock_engine):
        mock_settings.AI_TRADER_ENABLED = False
        signals = adapter.extract_signals(mock_engine)
        assert signals == []

    @patch("intelligence.adapters.ai_trader_adapter.settings")
    def test_returns_empty_when_no_url(self, mock_settings, adapter, mock_engine):
        mock_settings.AI_TRADER_ENABLED = True
        mock_settings.AI_TRADER_BASE_URL = ""
        signals = adapter.extract_signals(mock_engine)
        assert signals == []


class TestSignalExtraction:
    @patch("intelligence.adapters.ai_trader_adapter.requests")
    @patch("intelligence.adapters.ai_trader_adapter.settings")
    def test_produces_directional_and_consensus_signals(
        self, mock_settings, mock_requests, adapter, mock_engine
    ):
        mock_settings.AI_TRADER_ENABLED = True
        mock_settings.AI_TRADER_BASE_URL = "http://ai-trader:8000"
        mock_settings.AI_TRADER_API_KEY = ""
        mock_settings.AI_TRADER_TOP_AGENTS = 10
        mock_settings.AI_TRADER_MAX_SIGNALS = 200
        mock_settings.AI_TRADER_MARKET_FILTER = "stocks"

        # Mock leaderboard response
        leaderboard_resp = MagicMock()
        leaderboard_resp.json.return_value = LEADERBOARD_RESPONSE
        leaderboard_resp.raise_for_status = MagicMock()

        # Mock signal feed response
        feed_resp = MagicMock()
        feed_resp.json.return_value = SIGNAL_FEED_RESPONSE
        feed_resp.raise_for_status = MagicMock()

        mock_requests.get.side_effect = [leaderboard_resp, feed_resp]
        mock_requests.RequestException = Exception

        signals = adapter.extract_signals(mock_engine)

        # Should have: 3 directional (one per signal) + 1 consensus (AAPL has 2+ agents)
        assert len(signals) == 4

        # Check directional signals
        directional = [s for s in signals if s.signal_type == SignalType.DIRECTIONAL]
        assert len(directional) == 3

        aapl_signals = [s for s in directional if s.ticker == "AAPL"]
        assert len(aapl_signals) == 2
        assert all(s.direction == Direction.BULLISH for s in aapl_signals)

        tsla_signals = [s for s in directional if s.ticker == "TSLA"]
        assert len(tsla_signals) == 1
        assert tsla_signals[0].direction == Direction.BEARISH

        # Check consensus signal
        consensus = [s for s in signals if s.signal_type == SignalType.MAGNITUDE]
        assert len(consensus) == 1
        assert consensus[0].ticker == "AAPL"
        assert consensus[0].direction == Direction.BULLISH  # 2 bullish, 0 bearish
        assert consensus[0].value == 1.0  # (2-0)/2

    @patch("intelligence.adapters.ai_trader_adapter.requests")
    @patch("intelligence.adapters.ai_trader_adapter.settings")
    def test_confidence_scales_by_rank(
        self, mock_settings, mock_requests, adapter, mock_engine
    ):
        mock_settings.AI_TRADER_ENABLED = True
        mock_settings.AI_TRADER_BASE_URL = "http://ai-trader:8000"
        mock_settings.AI_TRADER_API_KEY = ""
        mock_settings.AI_TRADER_TOP_AGENTS = 10
        mock_settings.AI_TRADER_MAX_SIGNALS = 200
        mock_settings.AI_TRADER_MARKET_FILTER = ""

        leaderboard_resp = MagicMock()
        leaderboard_resp.json.return_value = LEADERBOARD_RESPONSE
        leaderboard_resp.raise_for_status = MagicMock()

        # Single signal from rank-1 agent
        feed_resp = MagicMock()
        feed_resp.json.return_value = [SIGNAL_FEED_RESPONSE[0]]  # agent_1 only
        feed_resp.raise_for_status = MagicMock()

        mock_requests.get.side_effect = [leaderboard_resp, feed_resp]
        mock_requests.RequestException = Exception

        signals = adapter.extract_signals(mock_engine)
        assert len(signals) == 1
        # Rank 1 agent → confidence = 0.85 - (1-1)*0.02 = 0.85
        assert signals[0].confidence == 0.85

    @patch("intelligence.adapters.ai_trader_adapter.requests")
    @patch("intelligence.adapters.ai_trader_adapter.settings")
    def test_rank_3_confidence(
        self, mock_settings, mock_requests, adapter, mock_engine
    ):
        mock_settings.AI_TRADER_ENABLED = True
        mock_settings.AI_TRADER_BASE_URL = "http://ai-trader:8000"
        mock_settings.AI_TRADER_API_KEY = ""
        mock_settings.AI_TRADER_TOP_AGENTS = 10
        mock_settings.AI_TRADER_MAX_SIGNALS = 200
        mock_settings.AI_TRADER_MARKET_FILTER = ""

        leaderboard_resp = MagicMock()
        leaderboard_resp.json.return_value = LEADERBOARD_RESPONSE
        leaderboard_resp.raise_for_status = MagicMock()

        # Single signal from rank-3 agent
        feed_resp = MagicMock()
        feed_resp.json.return_value = [SIGNAL_FEED_RESPONSE[2]]  # agent_3
        feed_resp.raise_for_status = MagicMock()

        mock_requests.get.side_effect = [leaderboard_resp, feed_resp]
        mock_requests.RequestException = Exception

        signals = adapter.extract_signals(mock_engine)
        assert len(signals) == 1
        # Rank 3 → confidence = 0.85 - (3-1)*0.02 = 0.81
        assert abs(signals[0].confidence - 0.81) < 1e-9


class TestGracefulDegradation:
    @patch("intelligence.adapters.ai_trader_adapter.requests")
    @patch("intelligence.adapters.ai_trader_adapter.settings")
    def test_empty_leaderboard_returns_empty(
        self, mock_settings, mock_requests, adapter, mock_engine
    ):
        mock_settings.AI_TRADER_ENABLED = True
        mock_settings.AI_TRADER_BASE_URL = "http://ai-trader:8000"
        mock_settings.AI_TRADER_API_KEY = ""
        mock_settings.AI_TRADER_TOP_AGENTS = 10

        resp = MagicMock()
        resp.json.return_value = []
        resp.raise_for_status = MagicMock()
        mock_requests.get.return_value = resp
        mock_requests.RequestException = Exception

        signals = adapter.extract_signals(mock_engine)
        assert signals == []

    @patch("intelligence.adapters.ai_trader_adapter.requests")
    @patch("intelligence.adapters.ai_trader_adapter.settings")
    def test_http_error_returns_empty(
        self, mock_settings, mock_requests, adapter, mock_engine
    ):
        mock_settings.AI_TRADER_ENABLED = True
        mock_settings.AI_TRADER_BASE_URL = "http://ai-trader:8000"
        mock_settings.AI_TRADER_API_KEY = ""
        mock_settings.AI_TRADER_TOP_AGENTS = 10

        mock_requests.get.side_effect = Exception("Connection refused")
        mock_requests.RequestException = Exception

        signals = adapter.extract_signals(mock_engine)
        assert signals == []

    @patch("intelligence.adapters.ai_trader_adapter.requests")
    @patch("intelligence.adapters.ai_trader_adapter.settings")
    def test_filters_out_non_top_agents(
        self, mock_settings, mock_requests, adapter, mock_engine
    ):
        mock_settings.AI_TRADER_ENABLED = True
        mock_settings.AI_TRADER_BASE_URL = "http://ai-trader:8000"
        mock_settings.AI_TRADER_API_KEY = ""
        mock_settings.AI_TRADER_TOP_AGENTS = 10
        mock_settings.AI_TRADER_MAX_SIGNALS = 200
        mock_settings.AI_TRADER_MARKET_FILTER = ""

        leaderboard_resp = MagicMock()
        leaderboard_resp.json.return_value = [
            {"agent_id": "agent_1", "name": "Top", "unrealized_pnl": 5000}
        ]
        leaderboard_resp.raise_for_status = MagicMock()

        # Feed has signal from agent_1 (top) and agent_99 (not on leaderboard)
        feed_resp = MagicMock()
        feed_resp.json.return_value = [
            {"agent_id": "agent_1", "symbol": "SPY", "action": "buy", "market": "stocks"},
            {"agent_id": "agent_99", "symbol": "SPY", "action": "sell", "market": "stocks"},
        ]
        feed_resp.raise_for_status = MagicMock()

        mock_requests.get.side_effect = [leaderboard_resp, feed_resp]
        mock_requests.RequestException = Exception

        signals = adapter.extract_signals(mock_engine)
        # Only agent_1's signal should be included
        assert len(signals) == 1
        assert signals[0].metadata["agent_id"] == "agent_1"


class TestSignalIntegrity:
    @patch("intelligence.adapters.ai_trader_adapter.requests")
    @patch("intelligence.adapters.ai_trader_adapter.settings")
    def test_signals_have_valid_timestamps(
        self, mock_settings, mock_requests, adapter, mock_engine
    ):
        mock_settings.AI_TRADER_ENABLED = True
        mock_settings.AI_TRADER_BASE_URL = "http://ai-trader:8000"
        mock_settings.AI_TRADER_API_KEY = ""
        mock_settings.AI_TRADER_TOP_AGENTS = 10
        mock_settings.AI_TRADER_MAX_SIGNALS = 200
        mock_settings.AI_TRADER_MARKET_FILTER = ""

        leaderboard_resp = MagicMock()
        leaderboard_resp.json.return_value = LEADERBOARD_RESPONSE
        leaderboard_resp.raise_for_status = MagicMock()

        feed_resp = MagicMock()
        feed_resp.json.return_value = SIGNAL_FEED_RESPONSE
        feed_resp.raise_for_status = MagicMock()

        mock_requests.get.side_effect = [leaderboard_resp, feed_resp]
        mock_requests.RequestException = Exception

        signals = adapter.extract_signals(mock_engine)
        for sig in signals:
            assert sig.valid_from.tzinfo is not None, "valid_from must be UTC-aware"
            assert sig.valid_until.tzinfo is not None, "valid_until must be UTC-aware"
            assert sig.valid_until > sig.valid_from
            assert 0.0 <= sig.confidence <= 1.0
            assert sig.source_module == "ai_trader"

    @patch("intelligence.adapters.ai_trader_adapter.requests")
    @patch("intelligence.adapters.ai_trader_adapter.settings")
    def test_signal_ids_are_deterministic(
        self, mock_settings, mock_requests, adapter, mock_engine
    ):
        """Same input should produce same signal IDs (idempotent refresh)."""
        mock_settings.AI_TRADER_ENABLED = True
        mock_settings.AI_TRADER_BASE_URL = "http://ai-trader:8000"
        mock_settings.AI_TRADER_API_KEY = ""
        mock_settings.AI_TRADER_TOP_AGENTS = 10
        mock_settings.AI_TRADER_MAX_SIGNALS = 200
        mock_settings.AI_TRADER_MARKET_FILTER = ""

        def make_responses():
            lb = MagicMock()
            lb.json.return_value = LEADERBOARD_RESPONSE
            lb.raise_for_status = MagicMock()
            feed = MagicMock()
            feed.json.return_value = SIGNAL_FEED_RESPONSE
            feed.raise_for_status = MagicMock()
            return [lb, feed]

        mock_requests.RequestException = Exception
        mock_requests.get.side_effect = make_responses()
        signals_1 = adapter.extract_signals(mock_engine)

        mock_requests.get.side_effect = make_responses()
        signals_2 = adapter.extract_signals(mock_engine)

        # Signal IDs should match across runs (same date → same IDs)
        ids_1 = {s.signal_id for s in signals_1}
        ids_2 = {s.signal_id for s in signals_2}
        assert ids_1 == ids_2
