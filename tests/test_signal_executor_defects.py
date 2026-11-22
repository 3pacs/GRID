"""Regression tests for verified defects 1–6 in the legacy paper signal executor slice.

Covers:
1. Kelly sizing floor/fallback, exploration threshold (MIN_TRADES_FOR_KELLY=20),
   convergence cap, and portfolio gross-exposure cap (MAX_GROSS_EXPOSURE=1.0).
2. Robinhood mirror fail-closed (VenueTag, build_venue_tag, execute_signals, API route)
   and never mirroring exploration-sized trades.
3. Circuit breaker OPEN skips entries but still runs Section 6 auto-closes.
4. Missing or zero leader prices skip entries but still run Section 6 auto-closes.
5. PIT bounds + retraction anti-join in _get_latest_prices and trading-session
   freshness checks for leader, entry, and exit prices.
6. Missing paper_strategy_breaker_state table / DB error in should_execute fails
   closed for entries with error log, still runs closes, and runs no runtime DDL.
"""

from __future__ import annotations

import os
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest

from store.pit import retraction_cutoff
from trading import circuit_breaker as cb
from trading import paper_engine as pe_mod
from trading import signal_executor as se


os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("GRID_JWT_SECRET", "test-secret-key-for-testing-only-32bytes")
os.environ.setdefault("GRID_JWT_EXPIRE_HOURS", "1")
os.environ.setdefault(
    "GRID_MASTER_PASSWORD_HASH",
    "$2b$12$abcdefghijklmnopqrstuuFb1mY3p5oXq0rN8sxqf6vV2QcVx1zSi",
)


# ── Helpers / Fakes ────────────────────────────────────────────────────────


class _FakeConn:
    """In-memory SQL router for signal_executor unit tests (no DB)."""

    def __init__(
        self,
        *,
        strategies: list[tuple] | None = None,
        closed_pnls_by_sid: dict[str, list[float]] | None = None,
        open_trades_by_sid: dict[str, list[tuple]] | None = None,
        initial_gross_exposure: float = 0.0,
    ) -> None:
        self.strategies = strategies or []
        self.closed_pnls_by_sid = closed_pnls_by_sid or {}
        self.open_trades_by_sid = open_trades_by_sid or {}
        self.initial_gross_exposure = initial_gross_exposure
        self.queries: list[tuple[str, dict | None]] = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, stmt, params=None):
        sql = str(stmt)
        self.queries.append((sql, params))
        result = MagicMock()
        result.fetchall.return_value = []
        result.fetchone.return_value = None

        if "FROM paper_strategies WHERE status = 'ACTIVE'" in sql:
            result.fetchall.return_value = list(self.strategies)
        elif "COUNT(*) FROM paper_strategies" in sql:
            result.fetchone.return_value = (0,)
        elif "SUM(" in sql and "paper_trades" in sql and "OPEN" in sql:
            result.fetchone.return_value = (self.initial_gross_exposure,)
        elif "SELECT pnl_pct FROM paper_trades" in sql:
            sid = (params or {}).get("sid")
            pnls = self.closed_pnls_by_sid.get(sid, [])
            result.fetchall.return_value = [(p,) for p in pnls]
        elif "SELECT id FROM paper_trades" in sql and "OPEN" in sql:
            sid = (params or {}).get("sid")
            opens = self.open_trades_by_sid.get(sid, [])
            result.fetchone.return_value = (opens[0][0],) if opens else None
        elif "FROM paper_trades" in sql and "status = 'OPEN'" in sql:
            sid = (params or {}).get("sid")
            opens = self.open_trades_by_sid.get(sid, [])
            if "position_size" in sql:
                result.fetchall.return_value = [
                    (t[0], t[1], t[2], t[3] if len(t) > 3 else 0.1) for t in opens
                ]
            else:
                result.fetchall.return_value = [(t[0], t[1], t[2]) for t in opens]
        return result


def _make_engine(conn_factory):
    engine = MagicMock()
    engine.connect.side_effect = conn_factory
    return engine


class _StubPaperEngine:
    """Fake PaperTradingEngine delegating Kelly math to the real implementation."""

    def __init__(self, initial_capital: float = 10_000.0) -> None:
        self.initial_capital = initial_capital
        self.opened: list[dict] = []
        self.closed: list[dict] = []
        self._next_id = 100

    def kelly_position_size(self, win_rate, avg_win, avg_loss, max_fraction=0.25):
        return pe_mod.PaperTradingEngine.kelly_position_size(
            self, win_rate, avg_win, avg_loss, max_fraction=max_fraction
        )

    def open_trade(self, **kwargs) -> int:
        self._next_id += 1
        self.opened.append({"trade_id": self._next_id, **kwargs})
        return self._next_id

    def close_trade(self, trade_id: int, exit_price: float, notes: str = "") -> dict:
        rec = {"trade_id": trade_id, "exit_price": exit_price, "notes": notes}
        self.closed.append(rec)
        return {"trade_id": trade_id, "pnl": 25.0, "pnl_pct": 0.025}


class _StubTrader:
    def __init__(self, tradable=("BTC", "ETH"), max_position_usd=100.0, mode="DRY_RUN"):
        self._tradable = set(tradable)
        self.max_position_usd = max_position_usd
        self.mode = mode
        self.configured = True
        self.orders: list[dict] = []

    def is_tradable(self, ticker: str) -> bool:
        return ticker.upper().startswith(("BTC", "ETH"))

    def open_position(self, ticker, direction, size_usd, client_order_id=None, **_kw):
        self.orders.append({"ticker": ticker, "direction": direction, "size_usd": size_usd})
        return {"status": "dry_run", "symbol": f"{ticker}-USD", "size_usd": size_usd}


def _fresh_dates() -> tuple[date, date]:
    """Return (fresh_today, prev_day) guaranteed fresh relative to date.today()."""
    today = date.today()
    return today, today - timedelta(days=1)


# ── Defect 1: Kelly sizing, exploration threshold, convergence cap, gross cap ─


class TestDefect1KellyAndExposure:
    def test_fewer_than_min_trades_returns_exploration_size(self):
        assert se.MIN_TRADES_FOR_KELLY == 20
        assert pe_mod.MIN_TRADES_FOR_KELLY == 20

        # 19 losing trades is still below MIN_TRADES_FOR_KELLY → paper exploration default
        conn = _FakeConn(closed_pnls_by_sid={"s1": [-0.02] * 19})
        stub_pe = _StubPaperEngine()
        size, is_exploration = se._compute_position_size(stub_pe, conn, "s1")
        assert size == se._DEFAULT_POSITION_SIZE
        assert is_exploration is True
        assert se._compute_kelly_size(stub_pe, conn, "s1") == se._DEFAULT_POSITION_SIZE

    def test_negative_edge_at_min_trades_returns_zero_and_skips_even_with_convergence(self, monkeypatch):
        # 20 closed trades: 6 wins (+1%), 14 losses (-2%) -> negative Kelly
        pnls = [0.01] * 6 + [-0.02] * 14
        conn = _FakeConn(
            strategies=[("s_neg", None, "spy_close", "qqq_close", None)],
            closed_pnls_by_sid={"s_neg": pnls},
        )
        stub_pe = _StubPaperEngine()
        assert se._compute_kelly_size(stub_pe, conn, "s_neg") == 0.0

        engine = _make_engine(lambda: conn)
        d0, d1 = _fresh_dates()
        prices = {
            "spy_close": [(d0, 103.0), (d1, 100.0)],
            "qqq_close": [(d0, 500.0)],
        }
        breaker = MagicMock()
        breaker.should_execute.return_value = True
        breaker.last_error = None

        monkeypatch.setattr(se, "PaperTradingEngine", lambda _e: stub_pe)
        monkeypatch.setattr(se, "StrategyCircuitBreaker", lambda _e: breaker)
        monkeypatch.setattr(se, "_get_latest_prices", lambda _c, name, n=2: prices[name][:n])
        monkeypatch.setattr(se, "_get_expected_lag", lambda *_a: 1)
        monkeypatch.setattr(
            "intelligence.trust_scorer.detect_convergence",
            lambda *_a, **_k: [{"signal_type": "BUY", "source_count": 6, "combined_confidence": 1.0}],
        )

        summary = se.execute_signals(engine)
        assert summary["trades_opened"] == 0
        assert stub_pe.opened == []
        skips = [d for d in summary["details"] if d.get("reason") == "non_positive_kelly"]
        assert len(skips) == 1
        assert skips[0]["strategy_id"] == "s_neg"

    def test_zero_wins_at_min_trades_returns_zero_and_skips(self, monkeypatch):
        pnls = [-0.015] * 20
        conn = _FakeConn(
            strategies=[("s_zero", None, "spy_close", "qqq_close", None)],
            closed_pnls_by_sid={"s_zero": pnls},
        )
        stub_pe = _StubPaperEngine()
        assert se._compute_kelly_size(stub_pe, conn, "s_zero") == 0.0

        engine = _make_engine(lambda: conn)
        d0, d1 = _fresh_dates()
        prices = {
            "spy_close": [(d0, 102.0), (d1, 100.0)],
            "qqq_close": [(d0, 500.0)],
        }
        breaker = MagicMock()
        breaker.should_execute.return_value = True
        breaker.last_error = None

        monkeypatch.setattr(se, "PaperTradingEngine", lambda _e: stub_pe)
        monkeypatch.setattr(se, "StrategyCircuitBreaker", lambda _e: breaker)
        monkeypatch.setattr(se, "_get_latest_prices", lambda _c, name, n=2: prices[name][:n])
        monkeypatch.setattr(se, "_get_expected_lag", lambda *_a: 1)
        monkeypatch.setattr("intelligence.trust_scorer.detect_convergence", lambda *_a, **_k: [])

        summary = se.execute_signals(engine)
        assert summary["trades_opened"] == 0
        assert any(d.get("reason") == "non_positive_kelly" for d in summary["details"])

    def test_kelly_helper_edge_cases_and_convergence_caps(self, monkeypatch):
        stub_pe = _StubPaperEngine()
        # Non-positive / invalid inputs fail closed to 0.0 without ZeroDivisionError
        assert stub_pe.kelly_position_size(0.6, 0.0, -0.02) == 0.0
        assert stub_pe.kelly_position_size(0.6, -0.01, -0.02) == 0.0
        assert stub_pe.kelly_position_size(0.6, 0.02, 0.0) == 0.0
        assert stub_pe.kelly_position_size(float("nan"), 0.02, -0.02) == 0.0
        # 100% win rate over validated sample -> positive, capped at max_fraction (0.25)
        assert stub_pe.kelly_position_size(1.0, 0.02, 0.0) == pytest.approx(0.25)
        # Positive edge: 60% WR, 1:1 payoff -> kelly=0.20, half-kelly=0.10
        assert stub_pe.kelly_position_size(0.60, 0.02, -0.02) == pytest.approx(0.10)

        # Positive edge at 0.25 cap + strong convergence must NOT jump to 0.50;
        # exploration (<20 trades) + strong convergence must stay at 0.10 (not 0.20).
        strong_pnls = [0.04] * 17 + [-0.01] * 3  # 85% WR, b=4 -> half-kelly capped at 0.25
        conn = _FakeConn(
            strategies=[
                ("s_strong", None, "spy_close", "qqq_close", None),
                ("s_explore", None, "spy_close", "iwm_close", None),
            ],
            closed_pnls_by_sid={"s_strong": strong_pnls, "s_explore": [0.02] * 5},
        )
        engine = _make_engine(lambda: conn)
        d0, d1 = _fresh_dates()
        prices = {
            "spy_close": [(d0, 103.0), (d1, 100.0)],
            "qqq_close": [(d0, 500.0)],
            "iwm_close": [(d0, 200.0)],
        }
        breaker = MagicMock()
        breaker.should_execute.return_value = True
        breaker.last_error = None

        monkeypatch.setattr(se, "PaperTradingEngine", lambda _e: stub_pe)
        monkeypatch.setattr(se, "StrategyCircuitBreaker", lambda _e: breaker)
        monkeypatch.setattr(se, "_get_latest_prices", lambda _c, name, n=2: prices[name][:n])
        monkeypatch.setattr(se, "_get_expected_lag", lambda *_a: 1)
        monkeypatch.setattr(
            "intelligence.trust_scorer.detect_convergence",
            lambda *_a, **_k: [{"signal_type": "BUY", "source_count": 10, "combined_confidence": 1.0}],
        )

        summary = se.execute_signals(engine)
        assert summary["trades_opened"] == 2
        by_sid = {t["strategy_id"]: t["position_size"] for t in stub_pe.opened}
        assert by_sid["s_strong"] <= 0.25
        assert by_sid["s_explore"] == pytest.approx(se._DEFAULT_POSITION_SIZE)

    def test_portfolio_gross_exposure_cap_blocks_entries_above_one(self, monkeypatch):
        assert se.MAX_GROSS_EXPOSURE == 1.0

        # Portfolio already has 0.90 gross open exposure across other strategies.
        # s1 (0.10) fits at 1.00; s2 (0.10) would push to 1.10 and must be skipped.
        conn = _FakeConn(
            strategies=[
                ("s1", None, "spy_close", "qqq_close", None),
                ("s2", None, "spy_close", "iwm_close", None),
            ],
            closed_pnls_by_sid={
                "s1": [0.02] * 12 + [-0.02] * 8,  # half-Kelly = 0.10
                "s2": [0.02] * 12 + [-0.02] * 8,  # half-Kelly = 0.10
            },
            initial_gross_exposure=0.90,
        )
        engine = _make_engine(lambda: conn)
        stub_pe = _StubPaperEngine()
        d0, d1 = _fresh_dates()
        prices = {
            "spy_close": [(d0, 103.0), (d1, 100.0)],
            "qqq_close": [(d0, 500.0)],
            "iwm_close": [(d0, 200.0)],
        }
        breaker = MagicMock()
        breaker.should_execute.return_value = True
        breaker.last_error = None

        monkeypatch.setattr(se, "PaperTradingEngine", lambda _e: stub_pe)
        monkeypatch.setattr(se, "StrategyCircuitBreaker", lambda _e: breaker)
        monkeypatch.setattr(se, "_get_latest_prices", lambda _c, name, n=2: prices[name][:n])
        monkeypatch.setattr(se, "_get_expected_lag", lambda *_a: 1)
        monkeypatch.setattr("intelligence.trust_scorer.detect_convergence", lambda *_a, **_k: [])

        summary = se.execute_signals(engine)
        assert summary["trades_opened"] == 1
        assert [t["strategy_id"] for t in stub_pe.opened] == ["s1"]
        cap_skips = [d for d in summary["details"] if d.get("reason") == "max_gross_exposure"]
        assert len(cap_skips) == 1
        assert cap_skips[0]["strategy_id"] == "s2"


# ── Defect 2: Robinhood mirror fail-closed & never mirror exploration trades ─


class TestDefect2RobinhoodFailClosed:
    def test_venue_tag_robinhood_refuses_open_with_clear_error(self):
        trader = _StubTrader()
        tag = se.VenueTag("robinhood", trader, {"id": "w1", "current_capital": 500.0})
        res = tag.submit("btc_close", "LONG", 0.10)
        assert trader.orders == []
        assert res is not None
        assert res.get("error") == "robinhood mirror disabled: close mirroring not implemented"

    def test_build_venue_tag_robinhood_fails_closed(self):
        with pytest.raises(ValueError, match="robinhood mirror disabled: close mirroring not implemented"):
            se.build_venue_tag(MagicMock(), "robinhood")

    def test_execute_signals_robinhood_refuses_and_returns_error(self, monkeypatch):
        stub_pe = _StubPaperEngine()
        conn = _FakeConn(strategies=[("s1", None, "spy_close", "btc_close", None)])
        engine = _make_engine(lambda: conn)
        monkeypatch.setattr(se, "PaperTradingEngine", lambda _e: stub_pe)

        summary = se.execute_signals(engine, venue="robinhood", venue_wallet_id="w1")
        assert summary.get("error") == "robinhood mirror disabled: close mirroring not implemented"
        assert summary["trades_opened"] == 0
        assert summary["venue_orders"] == []
        assert stub_pe.opened == []

    def test_api_execute_signals_robinhood_returns_400_error(self):
        from fastapi.testclient import TestClient

        from api.auth import create_token
        from api.main import app

        client = TestClient(app)
        headers = {"Authorization": f"Bearer {create_token(expires_hours=1)}"}
        with patch("trading.signal_executor.execute_signals") as run:
            resp = client.post(
                "/api/v1/trading/execute-signals?venue=robinhood&wallet_id=w1",
                headers=headers,
            )
        assert resp.status_code == 400
        assert "robinhood mirror disabled: close mirroring not implemented" in resp.json()["detail"]
        run.assert_not_called()

    def test_exploration_sized_trade_never_mirrored_to_any_venue(self, monkeypatch):
        trader = _StubTrader()
        # Non-robinhood venue tag to isolate the exploration-mirror prohibition
        tag = se.VenueTag("sandbox_venue", trader, {"id": "w1", "current_capital": 500.0})
        assert tag.submit("btc_close", "LONG", 0.10, is_exploration=True) is None
        assert trader.orders == []

        # End-to-end through execute_signals with < 20 closed trades
        conn = _FakeConn(
            strategies=[("s_exp", None, "spy_close", "btc_close", None)],
            closed_pnls_by_sid={"s_exp": [0.02] * 5},
        )
        engine = _make_engine(lambda: conn)
        stub_pe = _StubPaperEngine()
        d0, d1 = _fresh_dates()
        prices = {
            "spy_close": [(d0, 103.0), (d1, 100.0)],
            "btc_close": [(d0, 60000.0)],
        }
        breaker = MagicMock()
        breaker.should_execute.return_value = True
        breaker.last_error = None

        monkeypatch.setattr(se, "PaperTradingEngine", lambda _e: stub_pe)
        monkeypatch.setattr(se, "StrategyCircuitBreaker", lambda _e: breaker)
        monkeypatch.setattr(se, "build_venue_tag", lambda *_a, **_k: tag)
        monkeypatch.setattr(se, "_get_latest_prices", lambda _c, name, n=2: prices[name][:n])
        monkeypatch.setattr(se, "_get_expected_lag", lambda *_a: 1)
        monkeypatch.setattr("intelligence.trust_scorer.detect_convergence", lambda *_a, **_k: [])

        summary = se.execute_signals(engine, venue="sandbox_venue")
        assert summary["trades_opened"] == 1
        assert summary["venue_orders"] == []
        assert trader.orders == []


# ── Defect 3: Breaker OPEN skips entries but still runs closes ──────────────


class TestDefect3BreakerOpenStillCloses:
    def test_breaker_open_runs_auto_close_and_does_not_reset_breaker(self, monkeypatch):
        d0, d1 = _fresh_dates()
        expired_entry = d0 - timedelta(days=5)
        conn = _FakeConn(
            strategies=[("s_halted", None, "spy_close", "qqq_close", None)],
            open_trades_by_sid={"s_halted": [(42, "qqq_close", expired_entry, 0.10)]},
        )
        engine = _make_engine(lambda: conn)
        stub_pe = _StubPaperEngine()
        prices = {
            "spy_close": [(d0, 105.0), (d1, 100.0)],
            "qqq_close": [(d0, 510.0)],
        }
        breaker = MagicMock()
        breaker.should_execute.return_value = False
        breaker.last_error = None

        monkeypatch.setattr(se, "PaperTradingEngine", lambda _e: stub_pe)
        monkeypatch.setattr(se, "StrategyCircuitBreaker", lambda _e: breaker)
        monkeypatch.setattr(se, "_get_latest_prices", lambda _c, name, n=2: prices[name][:n])
        monkeypatch.setattr(se, "_get_expected_lag", lambda *_a: 1)

        summary = se.execute_signals(engine)
        assert summary["strategies_halted"] == 1
        assert summary["trades_opened"] == 0
        assert summary["trades_closed"] == 1
        assert stub_pe.opened == []
        assert len(stub_pe.closed) == 1 and stub_pe.closed[0]["trade_id"] == 42
        # Must NOT call record_success on an OPEN breaker
        breaker.record_success.assert_not_called()
        actions = [d["action"] for d in summary["details"]]
        assert "HALTED" in actions and "CLOSE" in actions


# ── Defect 4: Missing / zero leader prices still run closes ────────────────


class TestDefect4MissingLeaderStillCloses:
    @pytest.mark.parametrize(
        "leader_series",
        [
            [],
            [(date.today(), 100.0)],
            [(date.today(), 102.0), (date.today() - timedelta(days=1), 0.0)],
        ],
    )
    def test_missing_or_zero_leader_data_still_runs_closes(self, monkeypatch, leader_series):
        d0, _ = _fresh_dates()
        expired_entry = d0 - timedelta(days=3)
        conn = _FakeConn(
            strategies=[("s_miss", None, "spy_close", "qqq_close", None)],
            open_trades_by_sid={"s_miss": [(55, "qqq_close", expired_entry, 0.10)]},
        )
        engine = _make_engine(lambda: conn)
        stub_pe = _StubPaperEngine()
        prices = {
            "spy_close": leader_series,
            "qqq_close": [(d0, 512.0)],
        }
        breaker = MagicMock()
        breaker.should_execute.return_value = True
        breaker.last_error = None

        monkeypatch.setattr(se, "PaperTradingEngine", lambda _e: stub_pe)
        monkeypatch.setattr(se, "StrategyCircuitBreaker", lambda _e: breaker)
        monkeypatch.setattr(se, "_get_latest_prices", lambda _c, name, n=2: prices[name][:n])
        monkeypatch.setattr(se, "_get_expected_lag", lambda *_a: 1)

        summary = se.execute_signals(engine)
        assert summary["trades_opened"] == 0
        assert summary["trades_closed"] == 1
        assert len(stub_pe.closed) == 1 and stub_pe.closed[0]["trade_id"] == 55


# ── Defect 5: Price freshness & PIT bounds ─────────────────────────────────


class TestDefect5PriceFreshnessAndPIT:
    def test_get_latest_prices_enforces_pit_bounds_and_retractions(self):
        as_of = date(2026, 11, 20)
        captured: list[tuple[str, dict]] = []
        conn = MagicMock()

        def _exec(stmt, params=None):
            captured.append((str(stmt), params))
            res = MagicMock()
            res.fetchall.return_value = [(date(2026, 11, 20), 500.5), (date(2026, 11, 19), 495.0)]
            return res

        conn.execute.side_effect = _exec
        rows = se._get_latest_prices(conn, "spy_close", 2, as_of=as_of)
        assert rows == [(date(2026, 11, 20), 500.5), (date(2026, 11, 19), 495.0)]
        assert len(captured) == 1
        sql, params = captured[0]
        assert "obs_date <= :as_of" in sql
        assert "release_date <= :as_of" in sql
        assert "resolved_series_retractions" in sql
        assert params["as_of"] == as_of
        assert params["retraction_cutoff"] == retraction_cutoff(as_of)

    def test_is_price_fresh_respects_trading_calendar(self):
        # Regular Tuesday 2026-11-17: prior trading session is Monday 2026-11-16
        assert se._is_price_fresh(date(2026, 11, 17), date(2026, 11, 17)) is True
        assert se._is_price_fresh(date(2026, 11, 16), date(2026, 11, 17)) is True
        assert se._is_price_fresh(date(2026, 11, 13), date(2026, 11, 17)) is False

        # Monday 2026-11-16 after weekend: prior trading session is Friday 2026-11-13
        assert se._is_price_fresh(date(2026, 11, 13), date(2026, 11, 16)) is True
        assert se._is_price_fresh(date(2026, 11, 12), date(2026, 11, 16)) is False

        # Tuesday 2026-09-08 after Labor Day Mon 2026-09-07: prior session is Fri 2026-09-04
        assert se._is_price_fresh(date(2026, 9, 4), date(2026, 9, 8)) is True
        assert se._is_price_fresh(date(2026, 9, 3), date(2026, 9, 8)) is False

        # Future date (lookahead) and None fail closed
        assert se._is_price_fresh(date(2026, 11, 18), date(2026, 11, 17)) is False
        assert se._is_price_fresh(None, date(2026, 11, 17)) is False

    def test_stale_leader_skips_entry_with_reason_and_still_closes(self, monkeypatch):
        d0, _ = _fresh_dates()
        stale_d0 = d0 - timedelta(days=10)
        stale_d1 = d0 - timedelta(days=11)
        expired_entry = d0 - timedelta(days=4)

        conn = _FakeConn(
            strategies=[("s_stale_ldr", None, "spy_close", "qqq_close", None)],
            open_trades_by_sid={"s_stale_ldr": [(71, "qqq_close", expired_entry, 0.10)]},
        )
        engine = _make_engine(lambda: conn)
        stub_pe = _StubPaperEngine()
        prices = {
            "spy_close": [(stale_d0, 110.0), (stale_d1, 100.0)],  # +10% but stale!
            "qqq_close": [(d0, 520.0)],  # fresh exit price
        }
        breaker = MagicMock()
        breaker.should_execute.return_value = True
        breaker.last_error = None

        monkeypatch.setattr(se, "PaperTradingEngine", lambda _e: stub_pe)
        monkeypatch.setattr(se, "StrategyCircuitBreaker", lambda _e: breaker)
        monkeypatch.setattr(se, "_get_latest_prices", lambda _c, name, n=2: prices[name][:n])
        monkeypatch.setattr(se, "_get_expected_lag", lambda *_a: 1)

        summary = se.execute_signals(engine)
        assert summary["trades_opened"] == 0
        assert summary["trades_closed"] == 1
        assert any(d.get("reason") == "stale_price" for d in summary["details"])

    def test_stale_exit_price_keeps_trade_open(self, monkeypatch):
        d0, d1 = _fresh_dates()
        stale_exit_date = d0 - timedelta(days=10)
        expired_entry = d0 - timedelta(days=4)

        conn = _FakeConn(
            strategies=[("s_stale_exit", None, "spy_close", "qqq_close", None)],
            open_trades_by_sid={"s_stale_exit": [(88, "qqq_close", expired_entry, 0.10)]},
        )
        engine = _make_engine(lambda: conn)
        stub_pe = _StubPaperEngine()
        prices = {
            "spy_close": [(d0, 100.2), (d1, 100.0)],  # no entry signal
            "qqq_close": [(stale_exit_date, 500.0)],  # stale exit price
        }
        breaker = MagicMock()
        breaker.should_execute.return_value = True
        breaker.last_error = None

        monkeypatch.setattr(se, "PaperTradingEngine", lambda _e: stub_pe)
        monkeypatch.setattr(se, "StrategyCircuitBreaker", lambda _e: breaker)
        monkeypatch.setattr(se, "_get_latest_prices", lambda _c, name, n=2: prices[name][:n])
        monkeypatch.setattr(se, "_get_expected_lag", lambda *_a: 1)

        summary = se.execute_signals(engine)
        assert summary["trades_closed"] == 0
        assert stub_pe.closed == []
        assert any(d.get("reason") == "stale_price" for d in summary["details"])

    def test_stale_follower_entry_price_skips_entry(self, monkeypatch):
        d0, d1 = _fresh_dates()
        stale_entry_date = d0 - timedelta(days=10)

        conn = _FakeConn(
            strategies=[("s_stale_flw", None, "spy_close", "qqq_close", None)],
            closed_pnls_by_sid={"s_stale_flw": [0.02] * 5},
        )
        engine = _make_engine(lambda: conn)
        stub_pe = _StubPaperEngine()
        prices = {
            "spy_close": [(d0, 105.0), (d1, 100.0)],  # fresh +5% leader
            "qqq_close": [(stale_entry_date, 500.0)],  # stale follower
        }
        breaker = MagicMock()
        breaker.should_execute.return_value = True
        breaker.last_error = None

        monkeypatch.setattr(se, "PaperTradingEngine", lambda _e: stub_pe)
        monkeypatch.setattr(se, "StrategyCircuitBreaker", lambda _e: breaker)
        monkeypatch.setattr(se, "_get_latest_prices", lambda _c, name, n=2: prices[name][:n])
        monkeypatch.setattr(se, "_get_expected_lag", lambda *_a: 1)

        summary = se.execute_signals(engine)
        assert summary["trades_opened"] == 0
        assert any(d.get("reason") == "stale_price" for d in summary["details"])


# ── Defect 6: Missing breaker table fails closed for entries, still closes ─


class TestDefect6MissingBreakerTable:
    def test_breaker_should_execute_missing_table_fails_closed_without_ddl(self):
        engine = MagicMock()
        conn_mock = MagicMock()
        conn_mock.execute.side_effect = RuntimeError(
            'relation "paper_strategy_breaker_state" does not exist'
        )
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn_mock)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        breaker = cb.StrategyCircuitBreaker(engine, threshold=3, cooldown_hours=1)
        assert breaker.should_execute("s1") is False
        assert breaker.last_error is not None and "paper_strategy_breaker_state" in breaker.last_error
        # Must NOT run DDL / create tables at runtime
        engine.begin.assert_not_called()

    def test_execute_signals_missing_breaker_table_skips_entries_and_runs_closes(self, monkeypatch):
        d0, d1 = _fresh_dates()
        expired_entry = d0 - timedelta(days=4)

        conn = _FakeConn(
            strategies=[("s_no_tbl", None, "spy_close", "qqq_close", None)],
            open_trades_by_sid={"s_no_tbl": [(99, "qqq_close", expired_entry, 0.10)]},
        )
        engine = _make_engine(lambda: conn)
        stub_pe = _StubPaperEngine()
        prices = {
            "spy_close": [(d0, 105.0), (d1, 100.0)],
            "qqq_close": [(d0, 515.0)],
        }

        # Case A: breaker.should_execute raises directly (e.g. DB error / missing table)
        failing_breaker = MagicMock()
        failing_breaker.should_execute.side_effect = RuntimeError(
            'relation "paper_strategy_breaker_state" does not exist'
        )

        monkeypatch.setattr(se, "PaperTradingEngine", lambda _e: stub_pe)
        monkeypatch.setattr(se, "StrategyCircuitBreaker", lambda _e: failing_breaker)
        monkeypatch.setattr(se, "_get_latest_prices", lambda _c, name, n=2: prices[name][:n])
        monkeypatch.setattr(se, "_get_expected_lag", lambda *_a: 1)

        summary = se.execute_signals(engine)
        assert summary["trades_opened"] == 0
        assert summary["trades_closed"] == 1
        assert summary["strategies_halted"] == 1
        assert len(stub_pe.closed) == 1 and stub_pe.closed[0]["trade_id"] == 99
        failing_breaker.record_success.assert_not_called()
        failing_breaker.record_failure.assert_not_called()
        assert any(
            d.get("action") == "HALTED" and d.get("reason") == "circuit_breaker_error"
            for d in summary["details"]
        )

    def test_execute_signals_real_breaker_missing_table_skips_entries_and_runs_closes(self, monkeypatch):
        d0, d1 = _fresh_dates()
        expired_entry = d0 - timedelta(days=4)

        class _MissingBreakerTableConn(_FakeConn):
            def execute(self, stmt, params=None):
                if "paper_strategy_breaker_state" in str(stmt):
                    raise RuntimeError('relation "paper_strategy_breaker_state" does not exist')
                return super().execute(stmt, params)

        conn = _MissingBreakerTableConn(
            strategies=[("s_real_cb", None, "spy_close", "qqq_close", None)],
            open_trades_by_sid={"s_real_cb": [(101, "qqq_close", expired_entry, 0.10)]},
        )
        engine = _make_engine(lambda: conn)
        stub_pe = _StubPaperEngine()
        prices = {
            "spy_close": [(d0, 105.0), (d1, 100.0)],
            "qqq_close": [(d0, 515.0)],
        }
        monkeypatch.setattr(se, "PaperTradingEngine", lambda _e: stub_pe)
        monkeypatch.setattr(se, "_get_latest_prices", lambda _c, name, n=2: prices[name][:n])
        monkeypatch.setattr(se, "_get_expected_lag", lambda *_a: 1)

        summary = se.execute_signals(engine)
        assert summary["trades_opened"] == 0
        assert summary["trades_closed"] == 1
        assert summary["strategies_halted"] == 1
        assert len(stub_pe.closed) == 1 and stub_pe.closed[0]["trade_id"] == 101
        assert any(
            d.get("action") == "HALTED" and d.get("reason") == "circuit_breaker_error"
            for d in summary["details"]
        )
        engine.begin.assert_not_called()

