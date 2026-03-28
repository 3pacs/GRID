"""Unit tests for trading/prediction_pmxt.py PmxtTrader."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _patch_config():
    """Patch config.settings for all tests."""
    mock_settings = MagicMock()
    mock_settings.PMXT_POLYMARKET_PRIVATE_KEY = ""
    mock_settings.PMXT_KALSHI_API_KEY = ""
    mock_settings.PMXT_KALSHI_PRIVATE_KEY_PATH = ""
    with patch("trading.prediction_pmxt.settings", mock_settings):
        yield mock_settings


@pytest.fixture()
def mock_pmxt():
    """Patch the pmxt module as available."""
    fake_pmxt = MagicMock()
    fake_pmxt.fetch_events = MagicMock(return_value=[])
    fake_pmxt.fetch_balance = MagicMock(return_value=None)
    fake_pmxt.create_order = MagicMock(return_value={"order_id": "test123"})
    fake_pmxt.configure = MagicMock()
    with patch.dict("sys.modules", {"pmxt": fake_pmxt}):
        with patch("trading.prediction_pmxt.pmxt", fake_pmxt):
            with patch("trading.prediction_pmxt._PMXT_AVAILABLE", True):
                yield fake_pmxt


def _make_outcome(name="Yes", price=0.65):
    o = MagicMock()
    o.name = name
    o.yes_price = price
    o.price = price
    return o


def _make_market(market_id="m1", outcomes=None, volume=5000):
    m = MagicMock()
    m.id = market_id
    m.outcomes = outcomes or [_make_outcome()]
    m.volume = volume
    return m


def _make_event(event_id="e1", title="Test event", platform="polymarket", markets=None):
    e = MagicMock()
    e.id = event_id
    e.title = title
    e.description = "Test description"
    e.platform = platform
    e.markets = markets or [_make_market()]
    return e


# ---------------------------------------------------------------------------
# Graceful degradation
# ---------------------------------------------------------------------------


class TestPmxtTraderDegradation:
    def test_returns_empty_when_pmxt_not_available(self):
        """All methods return safe defaults when pmxt is not installed."""
        with patch("trading.prediction_pmxt._PMXT_AVAILABLE", False):
            from trading.prediction_pmxt import PmxtTrader

            trader = PmxtTrader()

            assert trader.get_markets("test") == []
            assert "error" in trader.get_market("e1")
            assert "error" in trader.get_portfolio()
            assert "error" in trader.buy("e1", "Yes", 100)
            assert "error" in trader.sell("e1", "Yes", 100)

    def test_portfolio_error_when_not_configured(self, mock_pmxt):
        """get_portfolio returns error when no platforms are configured."""
        from trading.prediction_pmxt import PmxtTrader

        trader = PmxtTrader()
        trader._configured = False

        result = trader.get_portfolio()
        assert "error" in result
        assert result["positions"] == []


# ---------------------------------------------------------------------------
# Risk limits
# ---------------------------------------------------------------------------


class TestRiskLimits:
    def test_buy_rejects_over_single_limit(self, mock_pmxt):
        """Buy rejects amounts exceeding MAX_SINGLE_TRADE_USD."""
        from trading.prediction_pmxt import PmxtTrader, MAX_SINGLE_TRADE_USD

        trader = PmxtTrader()
        trader._configured = True

        result = trader.buy("e1", "Yes", MAX_SINGLE_TRADE_USD + 1)
        assert "error" in result
        assert "exceeds" in result["error"].lower() or "limit" in result["error"].lower()

    def test_buy_rejects_zero_amount(self, mock_pmxt):
        """Buy rejects zero or negative amounts."""
        from trading.prediction_pmxt import PmxtTrader

        trader = PmxtTrader()
        trader._configured = True

        assert "error" in trader.buy("e1", "Yes", 0)
        assert "error" in trader.buy("e1", "Yes", -50)

    def test_sell_rejects_over_single_limit(self, mock_pmxt):
        """Sell rejects amounts exceeding MAX_SINGLE_TRADE_USD."""
        from trading.prediction_pmxt import PmxtTrader, MAX_SINGLE_TRADE_USD

        trader = PmxtTrader()
        trader._configured = True

        result = trader.sell("e1", "Yes", MAX_SINGLE_TRADE_USD + 1)
        assert "error" in result

    def test_sell_rejects_zero_amount(self, mock_pmxt):
        """Sell rejects zero or negative amounts."""
        from trading.prediction_pmxt import PmxtTrader

        trader = PmxtTrader()
        trader._configured = True

        assert "error" in trader.sell("e1", "Yes", 0)
        assert "error" in trader.sell("e1", "Yes", -10)

    def test_buy_rejects_when_portfolio_limit_exceeded(self, mock_pmxt):
        """Buy rejects when trade would exceed MAX_PORTFOLIO_USD."""
        from trading.prediction_pmxt import PmxtTrader, MAX_PORTFOLIO_USD

        balance_mock = MagicMock()
        balance_mock.total = MAX_PORTFOLIO_USD - 100  # Near the limit
        balance_mock.available = 1000
        balance_mock.positions = []
        mock_pmxt.fetch_balance.return_value = balance_mock

        trader = PmxtTrader()
        trader._configured = True

        # Try to buy $200 which would push over MAX_PORTFOLIO_USD
        result = trader.buy("e1", "Yes", 200)
        assert "error" in result
        assert "portfolio" in result["error"].lower()

    def test_buy_succeeds_within_limits(self, mock_pmxt):
        """Buy succeeds when within all risk limits."""
        balance_mock = MagicMock()
        balance_mock.total = 100
        balance_mock.available = 100
        balance_mock.positions = []
        mock_pmxt.fetch_balance.return_value = balance_mock

        from trading.prediction_pmxt import PmxtTrader

        trader = PmxtTrader()
        trader._configured = True

        result = trader.buy("e1", "Yes", 50)
        assert result.get("status") == "submitted"
        assert result["amount_usd"] == 50

    def test_sell_succeeds_within_limits(self, mock_pmxt):
        """Sell succeeds when within risk limits."""
        from trading.prediction_pmxt import PmxtTrader

        trader = PmxtTrader()
        trader._configured = True

        result = trader.sell("e1", "Yes", 50)
        assert result.get("status") == "submitted"
        assert result["amount_usd"] == 50


# ---------------------------------------------------------------------------
# Market discovery
# ---------------------------------------------------------------------------


class TestMarketDiscovery:
    def test_get_markets_returns_list(self, mock_pmxt):
        """get_markets returns a list of market dicts."""
        event = _make_event()
        mock_pmxt.fetch_events.return_value = [event]

        from trading.prediction_pmxt import PmxtTrader

        trader = PmxtTrader()
        markets = trader.get_markets("fed rate")

        assert isinstance(markets, list)
        assert len(markets) > 0
        assert "event_id" in markets[0]
        assert "title" in markets[0]

    def test_get_markets_empty_on_no_results(self, mock_pmxt):
        """get_markets returns empty list when no events found."""
        mock_pmxt.fetch_events.return_value = []

        from trading.prediction_pmxt import PmxtTrader

        trader = PmxtTrader()
        markets = trader.get_markets("nonexistent query")

        assert markets == []

    def test_get_markets_handles_exception(self, mock_pmxt):
        """get_markets returns empty list on API failure."""
        mock_pmxt.fetch_events.side_effect = ConnectionError("API down")

        from trading.prediction_pmxt import PmxtTrader

        trader = PmxtTrader()
        markets = trader.get_markets("fed")

        assert markets == []

    def test_get_market_returns_details(self, mock_pmxt):
        """get_market returns event details dict."""
        event = _make_event()
        mock_pmxt.fetch_events.return_value = [event]

        from trading.prediction_pmxt import PmxtTrader

        trader = PmxtTrader()
        result = trader.get_market("e1")

        assert "event_id" in result
        assert "markets" in result

    def test_get_market_returns_error_on_not_found(self, mock_pmxt):
        """get_market returns error dict when event not found."""
        mock_pmxt.fetch_events.return_value = []

        from trading.prediction_pmxt import PmxtTrader

        trader = PmxtTrader()
        result = trader.get_market("nonexistent")

        assert "error" in result

    def test_get_markets_respects_limit(self, mock_pmxt):
        """get_markets respects the limit parameter."""
        events = [_make_event(event_id=f"e{i}") for i in range(10)]
        mock_pmxt.fetch_events.return_value = events

        from trading.prediction_pmxt import PmxtTrader

        trader = PmxtTrader()
        markets = trader.get_markets("fed", limit=3)

        assert len(markets) <= 3


# ---------------------------------------------------------------------------
# Auth configuration
# ---------------------------------------------------------------------------


class TestAuthConfiguration:
    def test_configures_polymarket(self, mock_pmxt, _patch_config):
        """Configures Polymarket when private key is available."""
        _patch_config.PMXT_POLYMARKET_PRIVATE_KEY = "0xtest"

        from trading.prediction_pmxt import PmxtTrader

        trader = PmxtTrader()
        assert trader._configured is True
        mock_pmxt.configure.assert_called()

    def test_configures_kalshi(self, mock_pmxt, _patch_config):
        """Configures Kalshi when API key and private key path are set."""
        _patch_config.PMXT_KALSHI_API_KEY = "test-api-key"
        _patch_config.PMXT_KALSHI_PRIVATE_KEY_PATH = "/tmp/test.pem"

        from trading.prediction_pmxt import PmxtTrader

        trader = PmxtTrader()
        assert trader._configured is True

    def test_not_configured_when_no_keys(self, mock_pmxt, _patch_config):
        """Not configured when no platform keys are set."""
        _patch_config.PMXT_POLYMARKET_PRIVATE_KEY = ""
        _patch_config.PMXT_KALSHI_API_KEY = ""
        _patch_config.PMXT_KALSHI_PRIVATE_KEY_PATH = ""

        from trading.prediction_pmxt import PmxtTrader

        trader = PmxtTrader()
        assert trader._configured is False

    def test_buy_fails_when_not_configured(self, mock_pmxt):
        """Buy returns error when no platforms are configured."""
        from trading.prediction_pmxt import PmxtTrader

        trader = PmxtTrader()
        trader._configured = False

        result = trader.buy("e1", "Yes", 50)
        assert "error" in result
        assert "configured" in result["error"].lower()


# ---------------------------------------------------------------------------
# Order execution edge cases
# ---------------------------------------------------------------------------


class TestOrderExecution:
    def test_buy_catches_api_exception(self, mock_pmxt):
        """Buy returns error dict on API exception."""
        balance_mock = MagicMock()
        balance_mock.total = 0
        balance_mock.available = 1000
        balance_mock.positions = []
        mock_pmxt.fetch_balance.return_value = balance_mock
        mock_pmxt.create_order.side_effect = RuntimeError("Order failed")

        from trading.prediction_pmxt import PmxtTrader

        trader = PmxtTrader()
        trader._configured = True

        result = trader.buy("e1", "Yes", 50)
        assert "error" in result

    def test_sell_catches_api_exception(self, mock_pmxt):
        """Sell returns error dict on API exception."""
        mock_pmxt.create_order.side_effect = RuntimeError("Order failed")

        from trading.prediction_pmxt import PmxtTrader

        trader = PmxtTrader()
        trader._configured = True

        result = trader.sell("e1", "Yes", 50)
        assert "error" in result

    def test_buy_exact_limit(self, mock_pmxt):
        """Buy at exactly MAX_SINGLE_TRADE_USD succeeds."""
        balance_mock = MagicMock()
        balance_mock.total = 0
        balance_mock.available = 5000
        balance_mock.positions = []
        mock_pmxt.fetch_balance.return_value = balance_mock

        from trading.prediction_pmxt import PmxtTrader, MAX_SINGLE_TRADE_USD

        trader = PmxtTrader()
        trader._configured = True

        result = trader.buy("e1", "Yes", MAX_SINGLE_TRADE_USD)
        assert result.get("status") == "submitted"
