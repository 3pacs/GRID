"""Unit tests for scripts/live_rotation_trader.py (2026-09-24 rotation-inputs fix).

Covers:
* FIX C — an unavailable regime (missing/short inputs) must skip the live
  trading cycle loudly and leave positions untouched. Before this fix,
  ``REGIME_ALLOCATIONS.get("unavailable", {})`` silently resolved to the same
  empty-dict target as "risk-off" — a data outage would have been read as a
  risk-off signal and liquidated every position.
* FIX E — TOTAL_CAPITAL is now the ROBINHOOD_ROTATION_CAPITAL_USD setting
  (default 100.0, so behavior is unchanged unless an operator raises it).
"""

from __future__ import annotations

from datetime import date

import pytest

import scripts.live_rotation_trader as lrt
from alpha_research.strategies.adaptive_rotation import RegimeState, RotationResult


def _available_regime(label: str = "risk-on") -> RegimeState:
    return RegimeState(
        label=label, spy_trend=0.08, vix_zscore=0.2, fast_risk_off=False,
        max_groups=2, cash_floor=0.0, available=True, reason=None,
    )


def _unavailable_regime(reason: str = "VIX history has 3 obs, need >= 20") -> RegimeState:
    return RegimeState(
        label="unavailable", spy_trend=None, vix_zscore=None, fast_risk_off=False,
        max_groups=0, cash_floor=1.0, available=False, reason=reason,
    )


class _FakeTrader:
    """Minimal trader double for execute_rotation_live's happy path."""

    def __init__(self):
        self.mode = "DRY_RUN"
        self.max_position_usd = 100.0
        self.closed: list[str] = []
        self.opened: list[dict] = []

    def get_balance(self):
        return {"equity_usd": 500.0}

    def get_positions(self):
        return []

    def check_risk_limits(self):
        return {"drawdown_breached": False}

    def tradable_assets(self):
        return {"BTC", "ETH", "SOL"}

    def close_position(self, coin, **kwargs):
        self.closed.append(coin)
        return {"status": "ok"}

    def open_position(self, ticker, direction, size_usd, **kwargs):
        self.opened.append({"ticker": ticker, "direction": direction, "size_usd": size_usd})
        return {"status": "ok"}


@pytest.fixture(autouse=True)
def _patch_engine(monkeypatch):
    monkeypatch.setattr(lrt, "get_engine", lambda: object())


class TestUnavailableRegimeSkipsLiveTrading:
    def test_blocked_not_treated_as_risk_off(self, monkeypatch):
        fake_trader = _FakeTrader()
        monkeypatch.setattr(lrt, "_get_trader", lambda mainnet=False, venue="hyperliquid": fake_trader)
        monkeypatch.setattr(
            lrt, "run_rotation",
            lambda engine, as_of_date=None: RotationResult(
                weights={}, regime=_unavailable_regime(), active_groups=[],
                group_scores=[], stopped_tickers=[], as_of_date=as_of_date,
            ),
        )

        result = lrt.execute_rotation_live(venue="hyperliquid")

        assert result["status"] == "BLOCKED"
        assert result["regime"] == "unavailable"
        assert "VIX history" in result["reason"]
        # Positions must be left untouched — no balance/positions/rebalance calls.
        assert fake_trader.closed == []
        assert fake_trader.opened == []

    def test_available_regime_still_executes(self, monkeypatch):
        fake_trader = _FakeTrader()
        monkeypatch.setattr(lrt, "_get_trader", lambda mainnet=False, venue="hyperliquid": fake_trader)
        monkeypatch.setattr(
            lrt, "run_rotation",
            lambda engine, as_of_date=None: RotationResult(
                weights={}, regime=_available_regime("risk-off"), active_groups=[],
                group_scores=[], stopped_tickers=[], as_of_date=as_of_date,
            ),
        )

        result = lrt.execute_rotation_live(venue="hyperliquid")

        assert result["status"] == "OK"
        assert result["regime"] == "risk-off"

    def test_unavailable_regime_does_not_crash_on_none_fields(self, monkeypatch):
        """Regression guard: the old log line formatted spy_trend/vix_zscore
        with {t:.4f}/{v:.2f} — that raises TypeError once they can be None."""
        fake_trader = _FakeTrader()
        monkeypatch.setattr(lrt, "_get_trader", lambda mainnet=False, venue="hyperliquid": fake_trader)
        monkeypatch.setattr(
            lrt, "run_rotation",
            lambda engine, as_of_date=None: RotationResult(
                weights={}, regime=_unavailable_regime(), active_groups=[],
                group_scores=[], stopped_tickers=[], as_of_date=as_of_date,
            ),
        )
        # Must not raise.
        lrt.execute_rotation_live(venue="hyperliquid")


class TestRotationCapitalSetting:
    def test_total_capital_reads_the_setting(self, monkeypatch):
        import config

        monkeypatch.setattr(config.settings, "ROBINHOOD_ROTATION_CAPITAL_USD", 250.0)
        assert lrt._total_capital() == 250.0

    def test_total_capital_defaults_to_100(self):
        """Default value matches the previous hardcoded TOTAL_CAPITAL=100.0."""
        import config

        assert config.settings.ROBINHOOD_ROTATION_CAPITAL_USD == 100.0
        assert lrt._total_capital() == 100.0

    def test_rebalance_spot_sizes_against_configured_capital(self, monkeypatch):
        monkeypatch.setattr(lrt, "_total_capital", lambda: 1000.0)
        trader = _FakeTrader()
        trader.max_position_usd = 10_000.0  # don't let the per-coin cap bind here

        lrt._rebalance_spot(trader, engine=object(), target={"BTC": 0.5}, regime="risk-on",
                            current={}, venue="robinhood")

        assert trader.opened == [{"ticker": "BTC", "direction": "LONG", "size_usd": 500.0}]

    def test_rebalance_perps_sizes_against_configured_capital(self, monkeypatch):
        monkeypatch.setattr(lrt, "_total_capital", lambda: 1000.0)
        trader = _FakeTrader()

        lrt._rebalance_perps(trader, engine=object(), target={"BTC": 0.5}, regime="risk-on",
                             current_positions=[], venue="hyperliquid")

        # MAX_POSITION_USD (100.0, per-coin cap) binds before the capital setting does.
        assert trader.opened == [{"ticker": "BTC", "direction": "LONG", "size_usd": lrt.MAX_POSITION_USD}]
