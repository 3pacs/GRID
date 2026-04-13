"""ALPHA-14 — sector network adapter + oracle wiring tests."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from intelligence.adapters.sector_network_adapter import (
    SectorNetworkAdapter,
    _extract_ticker_entries,
    _score_market_power,
    _sector_weight,
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


class TestSectorWeight:
    def test_prefers_market_cap(self):
        w = _sector_weight({"market_cap_usd": 1e12, "revenue_2025": 5e10})
        assert w == (1e12, "market_cap_usd")

    def test_revenue_fallback(self):
        w = _sector_weight({"total_revenue_2025": 65e9})
        assert w == (65e9, "total_revenue_2025")

    def test_no_fields(self):
        assert _sector_weight({"name": "X"}) is None


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

    def test_uniform_source_module(self):
        """All signals share source_module='sector_network' so one entry in an
        oracle model's signal_sources list consumes every sector."""
        adapter = SectorNetworkAdapter()
        with patch(
            "intelligence.adapters.sector_network_adapter.SECTOR_MODULES",
            [("tech", "mod", "NAME"), ("banking", "mod", "NAME")],
        ), patch(
            "intelligence.adapters.sector_network_adapter.get_sector_data",
            return_value=_FAKE_NETWORK,
        ):
            signals = adapter.extract_signals(MagicMock())
        assert all(s.source_module == "sector_network" for s in signals)

    def test_revenue_fallback_pharma_like(self):
        """Sector with no market_cap_usd still emits per-ticker signals via
        revenue fallback (pharma YAML shape)."""
        pharma_like = {
            "big_pharma": {
                "LLY": {"ticker": "LLY", "total_revenue_2025": 65e9},
                "PFE": {"ticker": "PFE", "total_revenue_2025": 62e9},
            },
        }
        adapter = SectorNetworkAdapter()
        with patch(
            "intelligence.adapters.sector_network_adapter.SECTOR_MODULES",
            [("pharma", "mod", "NAME")],
        ), patch(
            "intelligence.adapters.sector_network_adapter.get_sector_data",
            return_value=pharma_like,
        ):
            signals = adapter.extract_signals(MagicMock())
        shares = [s for s in signals if s.metadata.get("signal_kind") == "sector_share"]
        assert {s.ticker for s in shares} == {"LLY", "PFE"}
        assert all(s.metadata["weight_basis"] == "total_revenue_2025" for s in shares)

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


class TestOracleWiring:
    """ALPHA-14: verify the adapter's source_module is consumed by at
    least one default oracle model, and that migrate_default_models
    appends `sector_network` into warm rows whose signal_sources list
    predated the adapter wiring."""

    def test_default_model_sources_include_sector_network(self):
        from oracle.model_factory import _DEFAULT_SIGNAL_SOURCES

        consumers = [
            name for name, sources in _DEFAULT_SIGNAL_SOURCES.items()
            if "sector_network" in sources
        ]
        assert "flow_momentum" in consumers
        assert "regime_contrarian" in consumers

    def test_migrate_appends_to_warm_row(self):
        """A model row that already has signal_sources (without
        sector_network) should have sector_network appended when
        migrate_default_models runs."""
        from oracle.model_factory import migrate_default_models

        warm_sources = [
            "feature:equity", "feature:flows", "feature:breadth",
            "feature:vol", "flow_thesis", "dollar_flows",
        ]
        state = {
            "flow_momentum": warm_sources,
            "regime_contrarian": ["feature:rates"],
            "options_flow": None,
            "cross_asset": None,
            "news_energy": None,
            "ai_trader_crowd": None,
        }
        captured: list[tuple[str, dict]] = []

        class FakeConn:
            def execute(self, query, params=None):
                sql = str(query)
                if "ALTER TABLE" in sql:
                    return MagicMock()
                captured.append((sql, params or {}))
                if "SELECT signal_sources" in sql:
                    name = (params or {}).get("n")
                    val = state.get(name)
                    row = MagicMock()
                    row.__getitem__ = lambda self, i: val
                    row.__bool__ = lambda self: val is not None
                    return MagicMock(fetchone=lambda: row if val is not None else None)
                if sql.strip().startswith("UPDATE oracle_models") and "signal_sources = :ss" in sql:
                    name = (params or {}).get("n")
                    ss = params.get("ss")
                    try:
                        state[name] = json.loads(ss) if isinstance(ss, str) else ss
                    except Exception:
                        state[name] = ss
                    return MagicMock(rowcount=1)
                return MagicMock()

            def __enter__(self): return self
            def __exit__(self, *a): return False

        fake_engine = MagicMock()
        fake_engine.begin.return_value = FakeConn()
        # _ensure_columns() uses engine.begin() too — same context manager is fine.

        migrate_default_models(fake_engine)

        assert "sector_network" in state["flow_momentum"]
        # Pre-existing sources preserved
        for s in warm_sources:
            assert s in state["flow_momentum"]
