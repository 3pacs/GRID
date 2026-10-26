"""Unit tests for scripts/rotation_paper_trader.py (2026-09-24 rotation-inputs fix).

VERIFIED FACT 5: run_paper_trading() only checked the WALLET's risk status
(trading_wallets), never paper_strategies.status. paper_strategies.
adaptive_rotation_live has been KILLED since 2026-05-27, so trading.
paper_engine.PaperTradingEngine.open_trade() silently returned -1 for every
open attempt and every daily run reported "OK, 0 trades" — indistinguishable
from a genuinely quiet day. Also: 19 of 23 closed trades matched their entry
price to the penny because _get_latest_price had no staleness check.

These tests exercise run_paper_trading()'s control flow with the heavy
dependencies (PaperTradingEngine, WalletManager, run_rotation, DB helpers)
replaced by small fakes/monkeypatches — the DB-table-creation machinery
inside the real classes isn't the thing under test here.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

import scripts.rotation_paper_trader as rpt
from alpha_research.strategies.adaptive_rotation import RegimeState, RotationResult


# ── Fakes ────────────────────────────────────────────────────────────


class _FakeWalletManager:
    def __init__(self, engine=None) -> None:
        self.update_pnl_calls: list[tuple] = []

    def check_risk(self, wallet_id):
        return {"status": "OK"}

    def get_wallet(self, wallet_id):
        return {"current_capital": 10_000.0, "total_pnl": 0.0, "max_drawdown": 0.0}

    def update_pnl(self, wallet_id, pnl, is_win):
        self.update_pnl_calls.append((wallet_id, pnl, is_win))


class _FakePaperEngine:
    def __init__(self, engine=None, initial_capital=10_000.0) -> None:
        self.open_calls: list[dict] = []
        self.open_return = 1  # override per-test
        self._next_id = 1

    def open_trade(self, **kwargs):
        self.open_calls.append(kwargs)
        if self.open_return > 0:
            trade_id = self._next_id
            self._next_id += 1
            return trade_id
        return self.open_return

    def close_trade(self, **kwargs):
        return {"pnl": 0.0}


def _available_regime(label="neutral") -> RegimeState:
    return RegimeState(
        label=label, spy_trend=0.01, vix_zscore=0.5, fast_risk_off=False,
        max_groups=2, cash_floor=0.2, available=True, reason=None,
    )


def _unavailable_regime(reason="SPY history has 10 obs, need >= 130") -> RegimeState:
    return RegimeState(
        label="unavailable", spy_trend=None, vix_zscore=None, fast_risk_off=False,
        max_groups=0, cash_floor=1.0, available=False, reason=reason,
    )


@pytest.fixture(autouse=True)
def _patch_wallet_and_engine(monkeypatch):
    """Common wiring every test needs: fake WalletManager/PaperTradingEngine,
    a fixed wallet id, and a no-op _ensure_strategy."""
    fake_wm = _FakeWalletManager()
    fake_pe = _FakePaperEngine()
    monkeypatch.setattr(rpt, "WalletManager", lambda engine: fake_wm)
    monkeypatch.setattr(rpt, "PaperTradingEngine", lambda engine, initial_capital=10_000.0: fake_pe)
    monkeypatch.setattr(rpt, "_get_or_create_wallet", lambda wm: "wallet-1")
    monkeypatch.setattr(rpt, "_ensure_strategy", lambda engine: None)
    monkeypatch.setattr(rpt, "_get_open_positions", lambda engine: {})
    return fake_wm, fake_pe


# ── Strategy-status BLOCKED gate ─────────────────────────────────────


class TestStrategyStatusBlocksTrading:
    def test_killed_strategy_returns_blocked_not_ok(self, monkeypatch):
        monkeypatch.setattr(rpt, "_get_strategy_status", lambda engine: ("KILLED", "win rate below 40%"))
        run_rotation_calls = []
        monkeypatch.setattr(rpt, "run_rotation", lambda *a, **kw: run_rotation_calls.append(1))

        result = rpt.run_paper_trading(engine=object())

        assert result["status"] == "BLOCKED"
        assert "KILLED" in result["reason"]
        assert "win rate below 40%" in result["reason"]
        # The whole point: don't even run the strategy once blocked up front.
        assert run_rotation_calls == []

    def test_paused_strategy_also_blocks(self, monkeypatch):
        monkeypatch.setattr(rpt, "_get_strategy_status", lambda engine: ("PAUSED", None))
        result = rpt.run_paper_trading(engine=object())
        assert result["status"] == "BLOCKED"
        assert "PAUSED" in result["reason"]

    def test_active_strategy_proceeds(self, monkeypatch):
        monkeypatch.setattr(rpt, "_get_strategy_status", lambda engine: ("ACTIVE", None))
        monkeypatch.setattr(
            rpt, "run_rotation",
            lambda engine, as_of_date=None: RotationResult(
                weights={}, regime=_available_regime(), active_groups=[],
                group_scores=[], stopped_tickers=[], as_of_date=as_of_date,
            ),
        )
        result = rpt.run_paper_trading(engine=object())
        assert result["status"] == "OK"

    def test_no_strategy_row_yet_proceeds(self, monkeypatch):
        """A brand-new strategy (no row until _ensure_strategy inserts one
        with status ACTIVE) must not be treated as blocked."""
        monkeypatch.setattr(rpt, "_get_strategy_status", lambda engine: (None, None))
        monkeypatch.setattr(
            rpt, "run_rotation",
            lambda engine, as_of_date=None: RotationResult(
                weights={}, regime=_available_regime(), active_groups=[],
                group_scores=[], stopped_tickers=[], as_of_date=as_of_date,
            ),
        )
        result = rpt.run_paper_trading(engine=object())
        assert result["status"] == "OK"


# ── Unavailable-regime BLOCKED gate ──────────────────────────────────


class TestUnavailableRegimeBlocksTrading:
    def test_unavailable_regime_returns_blocked(self, monkeypatch):
        monkeypatch.setattr(rpt, "_get_strategy_status", lambda engine: ("ACTIVE", None))
        monkeypatch.setattr(
            rpt, "run_rotation",
            lambda engine, as_of_date=None: RotationResult(
                weights={}, regime=_unavailable_regime(), active_groups=[],
                group_scores=[], stopped_tickers=[], as_of_date=as_of_date,
            ),
        )
        result = rpt.run_paper_trading(engine=object())

        assert result["status"] == "BLOCKED"
        assert result["regime"] == "unavailable"
        assert "SPY history" in result["reason"]

    def test_unavailable_regime_never_crashes_on_none_vix_zscore(self, monkeypatch):
        """Regression guard: the old log line formatted vix_zscore with
        {v:.2f} — that raises TypeError once vix_zscore can be None."""
        monkeypatch.setattr(rpt, "_get_strategy_status", lambda engine: ("ACTIVE", None))
        monkeypatch.setattr(
            rpt, "run_rotation",
            lambda engine, as_of_date=None: RotationResult(
                weights={}, regime=_unavailable_regime(), active_groups=[],
                group_scores=[], stopped_tickers=[], as_of_date=as_of_date,
            ),
        )
        # Must not raise.
        rpt.run_paper_trading(engine=object())


# ── open_trade() == -1 counted and logged ────────────────────────────


class TestFailedOpenIsCountedNotSilent:
    def test_minus_one_counted_as_failed_not_silently_skipped(self, monkeypatch):
        monkeypatch.setattr(rpt, "_get_strategy_status", lambda engine: ("ACTIVE", None))
        monkeypatch.setattr(
            rpt, "run_rotation",
            lambda engine, as_of_date=None: RotationResult(
                weights={"AAPL": 0.5, "MSFT": 0.5}, regime=_available_regime(),
                active_groups=["growth_tech"], group_scores=[], stopped_tickers=[],
                as_of_date=as_of_date,
            ),
        )
        monkeypatch.setattr(rpt, "_get_latest_price", lambda engine, ticker: 100.0)

        fake_pe = rpt.PaperTradingEngine(object())
        fake_pe.open_return = -1  # every open_trade call fails

        result = rpt.run_paper_trading(engine=object())

        assert result["trades_opened"] == 0
        assert result["trades_failed"] == 2
        assert result["status"] == "PARTIAL"

    def test_successful_opens_not_counted_as_failed(self, monkeypatch):
        monkeypatch.setattr(rpt, "_get_strategy_status", lambda engine: ("ACTIVE", None))
        monkeypatch.setattr(
            rpt, "run_rotation",
            lambda engine, as_of_date=None: RotationResult(
                weights={"AAPL": 0.5}, regime=_available_regime(),
                active_groups=["growth_tech"], group_scores=[], stopped_tickers=[],
                as_of_date=as_of_date,
            ),
        )
        monkeypatch.setattr(rpt, "_get_latest_price", lambda engine, ticker: 100.0)

        result = rpt.run_paper_trading(engine=object())

        assert result["trades_opened"] == 1
        assert result["trades_failed"] == 0
        assert result["status"] == "OK"


# ── Stale-price guard on _get_latest_price ───────────────────────────


class _FakeQueryConn:
    def __init__(self, value, obs_date):
        self._value = value
        self._obs_date = obs_date

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, stmt, params=None):
        class _R:
            def __init__(self, row):
                self._row = row

            def fetchone(self):
                return self._row

        if self._value is None:
            return _R(None)
        return _R((self._value, self._obs_date))


class _FakePriceEngine:
    def __init__(self, value, obs_date):
        self._value = value
        self._obs_date = obs_date

    def connect(self):
        return _FakeQueryConn(self._value, self._obs_date)


class TestStalePriceGuard:
    def test_fresh_price_is_returned(self):
        engine = _FakePriceEngine(123.45, date.today())
        assert rpt._get_latest_price(engine, "SPY") == 123.45

    def test_price_within_limit_is_returned(self):
        # STALE_PRICE_MAX_TRADING_DAYS trading days back is still acceptable.
        stale_date = date.today() - timedelta(days=1)
        engine = _FakePriceEngine(50.0, stale_date)
        assert rpt._get_latest_price(engine, "XLB") == 50.0

    def test_price_older_than_limit_is_refused(self):
        # Well beyond STALE_PRICE_MAX_TRADING_DAYS trading days.
        stale_date = date.today() - timedelta(days=14)
        engine = _FakePriceEngine(49.97, stale_date)
        assert rpt._get_latest_price(engine, "XLB") is None

    def test_no_row_returns_none(self):
        engine = _FakePriceEngine(None, None)
        assert rpt._get_latest_price(engine, "ZZZZ") is None

    def test_datetime_obs_date_is_coerced(self):
        """The DB driver may return a datetime instead of a date."""
        engine = _FakePriceEngine(10.0, datetime.combine(date.today(), datetime.min.time()))
        assert rpt._get_latest_price(engine, "GLD") == 10.0
