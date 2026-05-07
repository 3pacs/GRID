"""Tests for prediction market historical data sync and backtesting."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from ingestion.altdata.prediction_market_history import (
    PredictionMarketHistoryPuller,
    _json_safe,
    _polymarket_status,
    _safe_int,
    _safe_numeric,
    _safe_timestamp,
)


# ── Utility function tests ──────────────────────────────────────────


class TestSafeNumeric:
    def test_valid_float(self):
        assert _safe_numeric(42.5) == 42.5

    def test_valid_int(self):
        assert _safe_numeric(10) == 10.0

    def test_string_number(self):
        assert _safe_numeric("3.14") == 3.14

    def test_none(self):
        assert _safe_numeric(None) is None

    def test_nan(self):
        assert _safe_numeric(float("nan")) is None

    def test_inf(self):
        assert _safe_numeric(float("inf")) is None

    def test_bad_string(self):
        assert _safe_numeric("not_a_number") is None


class TestSafeInt:
    def test_valid(self):
        assert _safe_int(42) == 42

    def test_float_truncates(self):
        assert _safe_int(42.9) == 42

    def test_none(self):
        assert _safe_int(None) is None

    def test_bad_string(self):
        assert _safe_int("abc") is None


class TestSafeTimestamp:
    def test_iso_format(self):
        ts = _safe_timestamp("2024-01-15T10:30:00Z")
        assert isinstance(ts, datetime)
        assert ts.year == 2024

    def test_datetime_passthrough(self):
        dt = datetime(2024, 1, 15, tzinfo=timezone.utc)
        assert _safe_timestamp(dt) is dt

    def test_epoch_seconds(self):
        ts = _safe_timestamp(1705312200)
        assert isinstance(ts, datetime)

    def test_epoch_milliseconds(self):
        ts = _safe_timestamp(1705312200000)
        assert isinstance(ts, datetime)

    def test_none(self):
        assert _safe_timestamp(None) is None

    def test_bad_value(self):
        assert _safe_timestamp("not-a-date") is None

    def test_pandas_timestamp(self):
        pts = pd.Timestamp("2024-01-15 10:30:00")
        result = _safe_timestamp(pts)
        assert isinstance(result, datetime)


class TestPolymarketStatus:
    def test_closed(self):
        row = pd.Series({"closed": True, "active": False})
        assert _polymarket_status(row) == "closed"

    def test_resolved(self):
        row = pd.Series({"resolved": True})
        assert _polymarket_status(row) == "closed"

    def test_active(self):
        row = pd.Series({"active": True, "closed": False, "resolved": False})
        assert _polymarket_status(row) == "active"

    def test_unknown(self):
        row = pd.Series({"status": "pending"})
        assert _polymarket_status(row) == "pending"


class TestJsonSafe:
    def test_datetime(self):
        dt = datetime(2024, 1, 15, 10, 30)
        assert "2024-01-15" in _json_safe(dt)

    def test_nan(self):
        assert _json_safe(float("nan")) is None

    def test_bytes(self):
        assert _json_safe(b"\xde\xad") == "dead"

    def test_normal_value(self):
        assert _json_safe(42) == 42


# ── Market normalization tests ──────────────────────────────────────


class TestMarketNormalization:
    @pytest.fixture
    def puller(self):
        """Create puller with mocked engine."""
        engine = MagicMock()
        engine.connect.return_value.__enter__ = MagicMock(return_value=MagicMock())
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        engine.begin.return_value.__enter__ = MagicMock(return_value=MagicMock())
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        # Mock source_id resolution
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = (1,)
        engine.connect.return_value.__enter__.return_value = mock_conn

        with patch.object(PredictionMarketHistoryPuller, "_ensure_tables"):
            p = PredictionMarketHistoryPuller(engine)
            p.source_id = 1
            return p

    def test_normalize_kalshi_market(self, puller):
        row = pd.Series({
            "ticker": "RECESSION-2024",
            "title": "Will there be a US recession in 2024?",
            "category": "economics",
            "status": "active",
            "yes_bid": 25,
            "yes_ask": 27,
            "no_bid": 73,
            "no_ask": 75,
            "volume": 50000,
            "open_interest": 12000,
            "created_time": "2024-01-01T00:00:00Z",
            "close_time": "2024-12-31T23:59:59Z",
        })
        result = puller._normalize_kalshi_market(row)
        assert result["platform"] == "kalshi"
        assert result["market_id"] == "RECESSION-2024"
        assert result["volume"] == 50000
        assert result["yes_bid"] == 25

    def test_normalize_polymarket_market(self, puller):
        row = pd.Series({
            "id": "abc123",
            "question": "Will BTC hit $100k?",
            "category": "crypto",
            "active": True,
            "closed": False,
            "resolved": False,
            "outcomes": '["Yes", "No"]',
            "outcomePrices": '[0.35, 0.65]',
            "volumeNum": 1000000,
            "liquidity": 50000,
            "startDate": "2024-01-01",
        })
        result = puller._normalize_polymarket_market(row)
        assert result["platform"] == "polymarket"
        assert result["market_id"] == "abc123"
        assert result["title"] == "Will BTC hit $100k?"
        assert result["status"] == "active"


# ── Trade normalization tests ───────────────────────────────────────


class TestTradeNormalization:
    @pytest.fixture
    def puller(self):
        engine = MagicMock()
        engine.connect.return_value.__enter__ = MagicMock(return_value=MagicMock())
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        engine.begin.return_value.__enter__ = MagicMock(return_value=MagicMock())
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = (1,)
        engine.connect.return_value.__enter__.return_value = mock_conn

        with patch.object(PredictionMarketHistoryPuller, "_ensure_tables"):
            p = PredictionMarketHistoryPuller(engine)
            p.source_id = 1
            return p

    def test_normalize_kalshi_trade(self, puller):
        row = pd.Series({
            "ticker": "RECESSION-2024",
            "trade_id": "t123",
            "yes_price": 25,
            "count": 10,
            "taker_side": "yes",
            "created_time": "2024-06-15T14:30:00Z",
        })
        result = puller._normalize_kalshi_trade(row)
        assert result["platform"] == "kalshi"
        assert result["market_id"] == "RECESSION-2024"
        assert result["price"] == 0.25  # Converted from cents
        assert result["size"] == 10

    def test_normalize_polymarket_trade(self, puller):
        row = pd.Series({
            "market": "abc123",
            "id": "t456",
            "price": 0.65,
            "size": 100,
            "side": "buy",
            "maker": "0x1234567890abcdef1234567890abcdef12345678",
            "taker": "0xabcdef1234567890abcdef1234567890abcdef12",
            "fee": 0.5,
            "transactionHash": "0xdeadbeef",
            "blockNumber": 12345678,
            "timestamp": "2024-06-15T14:30:00Z",
        })
        result = puller._normalize_polymarket_trade(row)
        assert result["platform"] == "polymarket"
        assert result["market_id"] == "abc123"
        assert result["price"] == 0.65
        assert result["maker_address"].startswith("0x")

    def test_kalshi_price_conversion(self, puller):
        """Kalshi prices in cents (1-99) should be converted to 0-1."""
        row = pd.Series({
            "ticker": "TEST",
            "trade_id": "t1",
            "yes_price": 75,
            "count": 1,
            "taker_side": "yes",
            "created_time": "2024-01-01T00:00:00Z",
        })
        result = puller._normalize_kalshi_trade(row)
        assert result["price"] == 0.75


# ── Backtest strategy tests ────────────────────────────────────────


class TestBacktestStrategies:
    def test_list_strategies(self):
        from trading.prediction_backtest import list_strategies
        strategies = list_strategies()
        names = [s["name"] for s in strategies]
        assert "momentum_reversal" in names
        assert "maker_flow" in names
        assert "value_divergence" in names
        assert "liquidity_spike" in names

    def test_momentum_reversal_buy_on_drop(self):
        from trading.prediction_backtest import MomentumReversalStrategy
        strat = MomentumReversalStrategy({"lookback": 5, "threshold": 0.05})
        trade = pd.Series({"price": 0.4})
        # Price dropped from 0.5 to 0.4
        state = {"recent_prices": [0.5, 0.48, 0.45, 0.42, 0.4]}
        action = strat.on_trade(trade, state)
        assert action == "buy_yes"

    def test_momentum_reversal_buy_no_on_spike(self):
        from trading.prediction_backtest import MomentumReversalStrategy
        strat = MomentumReversalStrategy({"lookback": 5, "threshold": 0.05})
        trade = pd.Series({"price": 0.6})
        state = {"recent_prices": [0.5, 0.52, 0.55, 0.58, 0.6]}
        action = strat.on_trade(trade, state)
        assert action == "buy_no"

    def test_momentum_reversal_no_action_in_range(self):
        from trading.prediction_backtest import MomentumReversalStrategy
        strat = MomentumReversalStrategy({"lookback": 5, "threshold": 0.05})
        trade = pd.Series({"price": 0.51})
        state = {"recent_prices": [0.5, 0.505, 0.51]}
        action = strat.on_trade(trade, state)
        assert action is None

    def test_maker_flow_fade_yes_bias(self):
        from trading.prediction_backtest import MakerFlowStrategy
        strat = MakerFlowStrategy({"window": 10, "imbalance_threshold": 0.7})
        trade = pd.Series({"price": 0.5})
        # 8/10 takers buying yes = 0.8 imbalance > 0.7 threshold
        state = {"recent_taker_sides": ["yes"] * 8 + ["no"] * 2}
        action = strat.on_trade(trade, state)
        assert action == "buy_no"  # Fade the crowd

    def test_maker_flow_insufficient_data(self):
        from trading.prediction_backtest import MakerFlowStrategy
        strat = MakerFlowStrategy({"window": 50})
        trade = pd.Series({"price": 0.5})
        state = {"recent_taker_sides": ["yes"] * 5}
        action = strat.on_trade(trade, state)
        assert action is None

    def test_liquidity_spike_follows_direction(self):
        from trading.prediction_backtest import LiquiditySpikeStrategy
        strat = LiquiditySpikeStrategy({"vol_multiple": 3.0})
        trade = pd.Series({"price": 0.6, "size": 1000, "taker_side": "yes"})
        state = {"recent_volumes": [100] * 20}  # avg = 100, current = 1000 = 10x
        action = strat.on_trade(trade, state)
        assert action == "buy_yes"


# ── Pull/skip behavior tests ───────────────────────────────────────


class TestPullerSkipBehavior:
    def test_skip_when_no_data_dir(self):
        engine = MagicMock()
        engine.connect.return_value.__enter__ = MagicMock(return_value=MagicMock())
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        engine.begin.return_value.__enter__ = MagicMock(return_value=MagicMock())
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = (1,)
        engine.connect.return_value.__enter__.return_value = mock_conn

        with patch.object(PredictionMarketHistoryPuller, "_ensure_tables"):
            with patch(
                "ingestion.altdata.prediction_market_history._DATA_ROOT",
                new=MagicMock(exists=MagicMock(return_value=False)),
            ):
                p = PredictionMarketHistoryPuller(engine)
                p.source_id = 1
                result = p.pull_all()
                assert result[0]["status"] == "SKIP"
