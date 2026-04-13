"""ALPHA-14 — sector network adapter tests."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from intelligence.adapters.sector_network_adapter import (
    SectorNetworkAdapter,
    _extract_ticker_entries,
    _score_market_power,
)
from intelligence.signal_registry import SignalType


_FAKE_NETWORK = {
    "meta": {"ignored": True},
    "mag7": {
        "AAPL": {
            "name": "Apple Inc.",
            "ticker": "AAPL",
            "market_cap_usd": 3_000_000_000_000,
            "market_power": {"assessment": "monopoly_gatekeeper"},
        },
        "MSFT": {
            "name": "Microsoft",
            "ticker": "MSFT",
            "market_cap_usd": 3_000_000_000_000,
            "market_power": {"assessment": "dominant"},
        },
    },
    "semis": [
        {
            "ticker": "NVDA",
            "market_cap_usd": 2_000_000_000_000,
            "influence": 0.8,
        },
    ],
    "no_ticker_entry": {"name": "Private Co", "market_cap_usd": 1_000},
}


class TestExtractTickerEntries:
    def test_finds_all_tickers(self):
        entries = _extract_ticker_entries(_FAKE_NETWORK)
        tickers = {t for t, _ in entries}
        assert tickers == {"AAPL", "MSFT", "NVDA"}

    def test_dedupes_repeat_tickers(self):
        net = {"a": {"ticker": "X"}, "b": {"ticker": "X"}}
        entries = _extract_ticker_entries(net)
        assert len(entries) == 1

    def test_empty_network(self):
        assert _extract_ticker_entries({}) == []


class TestScoreMarketPower:
    def test_monopoly(self):
        assert _score_market_power({"market_power": {"assessment": "monopoly_gatekeeper"}}) == 1.0

    def test_dominant(self):
        assert _score_market_power({"market_power": {"assessment": "dominant"}}) == 0.7

    def test_influence_fallback(self):
        assert _score_market_power({"influence": 0.6}) == 0.6

    def test_influence_rescale(self):
        # influence > 1 is rescaled by /10
        assert _score_market_power({"influence": 8}) == 0.8

    def test_unknown_assessment_none(self):
        assert _score_market_power({"market_power": {"assessment": "obscure"}}) is None

    def test_no_fields(self):
        assert _score_market_power({"name": "X"}) is None


class TestAdapterExtractSignals:
    def test_emits_density_plus_per_ticker(self):
        adapter = SectorNetworkAdapter()
        with patch(
            "intelligence.adapters.sector_network_adapter.SECTOR_MODULES",
            [("tech", "mod", "NAME")],
        ), patch(
            "intelligence.adapters.sector_network_adapter.get_sector_data",
            return_value=_FAKE_NETWORK,
        ):
            signals = adapter.extract_signals(MagicMock())

        kinds = [s.metadata.get("signal_kind") for s in signals]
        assert any(s.ticker is None for s in signals), "density signal missing"
        assert "sector_share" in kinds
        assert "market_power" in kinds

        shares = [s for s in signals if s.metadata.get("signal_kind") == "sector_share"]
        share_by_ticker = {s.ticker: s.value for s in shares}
        assert set(share_by_ticker) == {"AAPL", "MSFT", "NVDA"}
        assert abs(sum(share_by_ticker.values()) - 1.0) < 1e-9

        power = [s for s in signals if s.metadata.get("signal_kind") == "market_power"]
        power_by_ticker = {s.ticker: s.value for s in power}
        assert power_by_ticker["AAPL"] == 1.0
        assert power_by_ticker["MSFT"] == 0.7
        assert power_by_ticker["NVDA"] == 0.8

    def test_all_signals_are_magnitude(self):
        adapter = SectorNetworkAdapter()
        with patch(
            "intelligence.adapters.sector_network_adapter.SECTOR_MODULES",
            [("tech", "mod", "NAME")],
        ), patch(
            "intelligence.adapters.sector_network_adapter.get_sector_data",
            return_value=_FAKE_NETWORK,
        ):
            signals = adapter.extract_signals(MagicMock())
        assert all(s.signal_type == SignalType.MAGNITUDE for s in signals)

    def test_empty_sector_skipped(self):
        adapter = SectorNetworkAdapter()
        with patch(
            "intelligence.adapters.sector_network_adapter.SECTOR_MODULES",
            [("empty", "mod", "NAME")],
        ), patch(
            "intelligence.adapters.sector_network_adapter.get_sector_data",
            return_value={},
        ):
            signals = adapter.extract_signals(MagicMock())
        assert signals == []
