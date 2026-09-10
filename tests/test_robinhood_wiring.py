"""Robinhood wiring: tradable-pair discovery, venue tag, rotation, health.

Hand-off 06 (2026-09-10) wires the Robinhood crypto connector into the
paper → live chain. Nothing here touches the network or a real database:
the connector runs on a fake session, and the executor/rotation paths run
against a stub trader.
"""

from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs, urlparse

import pytest

from trading import robinhood as rh

pytest.importorskip("cryptography")

ROOT = Path(__file__).resolve().parents[1]


# ── fakes ──────────────────────────────────────────────────────────────────


class _Resp:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status_code = status
        self.content = json.dumps(payload).encode()
        self.text = json.dumps(payload)

    def json(self):
        return self._payload


class FakeSession:
    """Routes by URL path; records every call for assertions."""

    def __init__(self, routes=None):
        self.routes = routes or {}
        self.calls: list[dict] = []

    def request(self, method, url, headers=None, data=None, timeout=None):
        parsed = urlparse(url)
        self.calls.append({"method": method, "path": parsed.path,
                           "query": parse_qs(parsed.query), "data": data})
        handler = self.routes.get((method, parsed.path))
        if handler is None:
            return _Resp({"detail": "not found"}, status=404)
        payload = handler(parse_qs(parsed.query), data) if callable(handler) else handler
        return _Resp(payload)


PRIVATE_B64, _PUBLIC_B64 = rh.generate_keypair()


def _paged_pairs():
    """Two pages of trading pairs; page 2 is only reachable via the cursor."""
    page1 = {
        "next": "https://trading.robinhood.com/api/v1/crypto/trading/trading_pairs/?cursor=abc123",
        "results": [
            {"symbol": "BTC-USD", "status": "tradable", "quantity_increment": "0.000001"},
            {"symbol": "ETH-USD", "status": "tradable", "quantity_increment": "0.00001"},
            {"symbol": "XYZ-USD", "status": "unavailable"},
        ],
    }
    page2 = {
        "next": None,
        "results": [
            {"symbol": "SOL-USD", "status": "active"},
            {"symbol": "DOGE-USD", "status": "tradable"},
        ],
    }

    def handler(query, _body):
        return page2 if query.get("cursor") else page1

    return {("GET", rh.PATH_TRADING_PAIRS): handler}


def _trader(live=False, session=None, **kw):
    return rh.RobinhoodCryptoTrader(
        api_key="rh-key", private_key_b64=PRIVATE_B64, live=live,
        session=session or FakeSession(_paged_pairs()), **kw,
    )


class StubTrader:
    """Minimal stand-in for RobinhoodCryptoTrader in the wiring paths."""

    def __init__(self, tradable=("BTC", "ETH", "SOL"), max_position_usd=100.0,
                 mode="DRY_RUN", configured=True):
        self._tradable = set(tradable)
        self.max_position_usd = max_position_usd
        self.mode = mode
        self.configured = configured
        self.orders: list[dict] = []
        self.closed: list[str] = []

    def tradable_assets(self, refresh: bool = False) -> set[str]:
        return set(self._tradable)

    def is_tradable(self, ticker: str) -> bool:
        code = rh.crypto_asset_code(ticker)
        return bool(code) and code in self._tradable

    def open_position(self, ticker, direction, size_usd):
        self.orders.append({"ticker": ticker, "direction": direction, "size_usd": size_usd})
        return {"status": "dry_run", "symbol": f"{ticker}-USD", "size_usd": size_usd}

    def close_position(self, ticker):
        self.closed.append(ticker)
        return {"status": "dry_run", "direction": "CLOSE", "symbol": f"{ticker}-USD"}


# ── connector: feature-name mapping and pair discovery ─────────────────────


class TestAssetCodes:
    @pytest.mark.parametrize("name,expected", [
        ("BTC", "BTC"), ("BTC-USD", "BTC"), ("BTCUSDT", "BTC"), ("btc_close", "BTC"),
        ("btc_full", "BTC"), ("eth_usd_full", "ETH"), ("sol_usd_full", "SOL"),
        ("doge_usd_full", "DOGE"), ("ETH-PERP", "ETH"), ("sp500_close", "SP500"),
    ])
    def test_crypto_asset_code(self, name, expected):
        assert rh.crypto_asset_code(name) == expected

    @pytest.mark.parametrize("name", ["", "   ", "close", "adj_close", "_", "9", "1_close"])
    def test_no_plausible_code(self, name):
        assert rh.crypto_asset_code(name) is None

    @pytest.mark.parametrize("link,expected", [
        ("https://x/api/?cursor=abc123", "abc123"),
        ("https://x/api/?limit=10&cursor=z9", "z9"),
        ("https://x/api/", None), ("", None), (None, None), (123, None),
        ("https://x/api/?cursor=", None),
    ])
    def test_cursor_from_next(self, link, expected):
        assert rh.cursor_from_next(link) == expected


class TestTradablePairs:
    def test_follows_the_next_cursor_and_filters_by_status(self):
        session = FakeSession(_paged_pairs())
        assets = _trader(session=session).tradable_assets()
        assert assets == {"BTC", "ETH", "SOL", "DOGE"}  # XYZ is "unavailable"
        assert len(session.calls) == 2
        assert session.calls[1]["query"]["cursor"] == ["abc123"]

    def test_result_is_cached_until_refresh(self):
        session = FakeSession(_paged_pairs())
        trader = _trader(session=session)
        trader.tradable_assets()
        trader.tradable_assets()
        assert len(session.calls) == 2  # both from the first call's paging
        trader.tradable_assets(refresh=True)
        assert len(session.calls) == 4

    def test_paging_is_bounded(self):
        """A cursor that never terminates stops at max_pages instead of spinning."""
        loop = {("GET", rh.PATH_TRADING_PAIRS): {
            "next": "https://trading.robinhood.com/x/?cursor=same",
            "results": [{"symbol": "BTC-USD", "status": "tradable"}],
        }}
        session = FakeSession(loop)
        assert _trader(session=session).tradable_assets() == {"BTC"}
        assert len(session.calls) == 10

    def test_unconfigured_reports_nothing_tradable(self):
        session = FakeSession(_paged_pairs())
        trader = rh.RobinhoodCryptoTrader(session=session)
        assert trader.tradable_assets() == set()
        assert trader.is_tradable("BTC") is False
        assert session.calls == []

    def test_is_tradable_accepts_feature_names(self):
        trader = _trader()
        assert trader.is_tradable("btc_close") is True
        assert trader.is_tradable("doge_usd_full") is True
        assert trader.is_tradable("sp500_close") is False
        assert trader.is_tradable("") is False


# ── signal executor: the venue tag ─────────────────────────────────────────


class TestVenueTag:
    def _tag(self, wallet=None, **kw):
        from trading.signal_executor import VenueTag

        return VenueTag("robinhood", StubTrader(**kw), wallet)

    def test_long_crypto_signal_is_routed(self):
        tag = self._tag(wallet={"id": "w1", "current_capital": 500.0, "status": "ACTIVE"})
        order = tag.submit("btc_close", "LONG", 0.1)
        assert order["venue"] == "robinhood" and order["wallet_id"] == "w1"
        assert order["asset"] == "BTC" and order["size_usd"] == 50.0
        assert order["result"]["status"] == "dry_run"
        assert tag.trader.orders == [{"ticker": "BTC", "direction": "LONG", "size_usd": 50.0}]

    def test_short_signal_never_reaches_a_spot_venue(self):
        tag = self._tag(wallet={"id": "w1", "current_capital": 500.0})
        assert tag.submit("btc_close", "SHORT", 0.1) is None
        assert tag.trader.orders == []

    def test_non_crypto_ticker_stays_paper_only(self):
        tag = self._tag(wallet={"id": "w1", "current_capital": 500.0})
        assert tag.submit("sp500_close", "LONG", 0.1) is None
        assert tag.trader.orders == []

    def test_size_is_capped_by_the_per_order_limit(self):
        tag = self._tag(wallet={"id": "w1", "current_capital": 10_000.0})
        assert tag.order_size_usd(0.5) == 100.0  # 5,000 clipped to the $100 cap
        assert tag.submit("eth_usd_full", "LONG", 0.5)["size_usd"] == 100.0

    def test_capital_falls_back_to_the_order_cap_without_a_wallet(self):
        tag = self._tag()
        assert tag.capital == 100.0
        assert tag.order_size_usd(0.1) == 10.0

    def test_dust_orders_are_skipped(self):
        tag = self._tag(wallet={"id": "w1", "current_capital": 5.0})
        assert tag.submit("btc_close", "LONG", 0.1) is None  # $0.50 < $1 minimum
        assert tag.trader.orders == []

    def test_connector_failure_is_reported_not_raised(self):
        tag = self._tag(wallet={"id": "w1", "current_capital": 500.0})
        tag.trader.open_position = MagicMock(side_effect=RuntimeError("boom"))
        order = tag.submit("btc_close", "LONG", 0.1)
        assert order["result"] == {"error": "boom"}

    def test_tradable_lookup_failure_routes_nothing(self):
        tag = self._tag(wallet={"id": "w1", "current_capital": 500.0})
        tag.trader.is_tradable = MagicMock(side_effect=RuntimeError("offline"))
        assert tag.submit("btc_close", "LONG", 0.1) is None


class TestBuildVenueTag:
    def test_no_venue_is_paper_only(self, mock_engine):
        from trading import signal_executor as se

        assert se.build_venue_tag(mock_engine, None) is None

    def test_unknown_venue_raises(self, mock_engine):
        from trading import signal_executor as se

        with pytest.raises(ValueError, match="Unsupported venue"):
            se.build_venue_tag(mock_engine, "coinbase")

    def test_unconfigured_connector_degrades_to_paper(self, mock_engine):
        from trading import signal_executor as se

        with patch("trading.robinhood.get_robinhood_trader",
                   return_value=StubTrader(configured=False)):
            assert se.build_venue_tag(mock_engine, "robinhood") is None

    def test_killed_wallet_routes_nothing(self, mock_engine):
        from trading import signal_executor as se

        wm = MagicMock()
        wm.return_value.get_wallet.return_value = {"id": "w1", "status": "KILLED"}
        with patch("trading.robinhood.get_robinhood_trader", return_value=StubTrader()), \
             patch("trading.wallet_manager.WalletManager", wm):
            assert se.build_venue_tag(mock_engine, "robinhood", "w1") is None

    def test_active_wallet_builds_a_tag(self, mock_engine):
        from trading import signal_executor as se

        wallet = {"id": "w1", "status": "ACTIVE", "current_capital": 250.0}
        wm = MagicMock()
        wm.return_value.get_wallet.return_value = wallet
        with patch("trading.robinhood.get_robinhood_trader", return_value=StubTrader()), \
             patch("trading.wallet_manager.WalletManager", wm):
            tag = se.build_venue_tag(mock_engine, "Robinhood", "w1")
        assert tag is not None and tag.venue == "robinhood"
        assert tag.wallet_id == "w1" and tag.capital == 250.0


# ── signal executor: end-to-end signal → paper trade + venue order ─────────


class _FakeConn:
    """Routes execute() by SQL fragment; everything else is empty."""

    def __init__(self, strategies):
        self.strategies = strategies

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, stmt, params=None):
        sql = str(stmt)
        result = MagicMock()
        result.fetchall.return_value = []
        result.fetchone.return_value = None
        if "FROM paper_strategies WHERE status = 'ACTIVE'" in sql:
            result.fetchall.return_value = self.strategies
        elif "COUNT(*) FROM paper_strategies" in sql:
            result.fetchone.return_value = (0,)
        return result


def _executor_engine(strategies):
    engine = MagicMock()
    engine.connect.side_effect = lambda: _FakeConn(strategies)
    return engine


def _run_executor(monkeypatch, tag, follower="btc_close", leader_prices=(102.0, 100.0)):
    from trading import signal_executor as se

    engine = _executor_engine([("s1", None, "spy_close", follower, None)])

    paper = MagicMock()
    paper.open_trade.return_value = 7
    monkeypatch.setattr(se, "PaperTradingEngine", lambda _e: paper)

    breaker = MagicMock()
    breaker.should_execute.return_value = True
    monkeypatch.setattr(se, "StrategyCircuitBreaker", lambda _e: breaker)

    monkeypatch.setattr(se, "build_venue_tag", lambda *_a, **_k: tag)
    monkeypatch.setattr(se, "_compute_kelly_size", lambda *_a: 0.1)
    monkeypatch.setattr(se, "_get_expected_lag", lambda *_a: 1)

    prices = {
        "spy_close": [(date(2026, 9, 10), leader_prices[0]), (date(2026, 9, 9), leader_prices[1])],
        follower: [(date(2026, 9, 10), 60000.0)],
    }
    monkeypatch.setattr(se, "_get_latest_prices", lambda _c, name, n=2: prices[name][:n])
    monkeypatch.setattr("intelligence.trust_scorer.detect_convergence", lambda *_a, **_k: [])

    return se.execute_signals(engine, venue="robinhood", venue_wallet_id="w1"), paper


class TestExecutorVenueRouting:
    def test_paper_trade_and_venue_order_both_happen(self, monkeypatch):
        from trading.signal_executor import VenueTag

        tag = VenueTag("robinhood", StubTrader(), {"id": "w1", "current_capital": 400.0})
        summary, paper = _run_executor(monkeypatch, tag)

        assert summary["trades_opened"] == 1
        assert paper.open_trade.call_args.kwargs["direction"] == "LONG"
        assert summary["venue"] == "robinhood" and summary["venue_mode"] == "DRY_RUN"
        assert len(summary["venue_orders"]) == 1
        order = summary["venue_orders"][0]
        assert order["asset"] == "BTC" and order["trade_id"] == 7
        assert order["size_usd"] == 40.0
        assert summary["details"][0]["venue_order"]["result"]["status"] == "dry_run"

    def test_short_signal_opens_paper_only(self, monkeypatch):
        from trading.signal_executor import VenueTag

        tag = VenueTag("robinhood", StubTrader(), {"id": "w1", "current_capital": 400.0})
        # Leader down 2% → SHORT signal → nothing reaches the spot venue.
        summary, paper = _run_executor(monkeypatch, tag, leader_prices=(98.0, 100.0))

        assert summary["trades_opened"] == 1
        assert paper.open_trade.call_args.kwargs["direction"] == "SHORT"
        assert summary["venue_orders"] == []
        assert tag.trader.orders == []

    def test_paper_only_run_is_unchanged(self, monkeypatch):
        summary, paper = _run_executor(monkeypatch, None)
        assert summary["trades_opened"] == 1 and paper.open_trade.called
        assert summary["venue"] is None and summary["venue_orders"] == []


# ── rotation trader: --venue robinhood ─────────────────────────────────────


class TestRotationVenue:
    def _module(self):
        import importlib

        return importlib.import_module("scripts.live_rotation_trader")

    def test_cli_exposes_the_venue_flag(self):
        src = (ROOT / "scripts/live_rotation_trader.py").read_text(encoding="utf-8")
        assert '"--venue", choices=VENUES, default="hyperliquid"' in src
        mod = self._module()
        assert mod.VENUES == ("hyperliquid", "robinhood")
        assert "robinhood" in mod.SPOT_VENUES and "hyperliquid" not in mod.SPOT_VENUES

    def test_unknown_venue_is_refused(self):
        mod = self._module()
        with pytest.raises(ValueError, match="Unknown venue"):
            mod.execute_rotation_live(venue="coinbase")

    def test_targets_drop_coins_the_venue_does_not_list(self):
        mod = self._module()
        trader = StubTrader(tradable=("BTC", "ETH"))
        target = {"BTC": 0.60, "ETH": 0.25, "SOL": 0.15}
        assert mod._tradable_targets(trader, target, "robinhood") == {"BTC": 0.60, "ETH": 0.25}
        # Perps are unaffected — Hyperliquid lists its own universe.
        assert mod._tradable_targets(trader, target, "hyperliquid") == target

    def test_no_tradable_pairs_allocates_nothing(self):
        mod = self._module()
        assert mod._tradable_targets(StubTrader(tradable=()), {"BTC": 1.0}, "robinhood") == {}

    def test_spot_rebalance_buys_the_delta(self, mock_engine):
        mod = self._module()
        trader = StubTrader()
        held = {"BTC": {"coin": "BTC", "size_usd": 20.0, "direction": "LONG"}}
        results = mod._rebalance_spot(trader, mock_engine, {"BTC": 0.60}, "risk-on",
                                      held, "robinhood")
        assert trader.closed == []
        assert trader.orders == [{"ticker": "BTC", "direction": "LONG", "size_usd": 40.0}]
        assert results[0]["action"] == "OPEN"

    def test_spot_rebalance_trims_an_oversized_holding(self, mock_engine):
        mod = self._module()
        trader = StubTrader()
        held = {"BTC": {"coin": "BTC", "size_usd": 90.0, "direction": "LONG"}}
        results = mod._rebalance_spot(trader, mock_engine, {"BTC": 0.50}, "neutral",
                                      held, "robinhood")
        # SHORT on spot sells held quantity — it never goes net short.
        assert trader.orders == [{"ticker": "BTC", "direction": "SHORT", "size_usd": 40.0}]
        assert results[0]["action"] == "TRIM"

    def test_risk_off_sells_everything_to_cash(self, mock_engine):
        mod = self._module()
        trader = StubTrader()
        held = {"BTC": {"coin": "BTC", "size_usd": 60.0, "direction": "LONG"},
                "ETH": {"coin": "ETH", "size_usd": 25.0, "direction": "LONG"}}
        results = mod._rebalance_spot(trader, mock_engine, {}, "risk-off", held, "robinhood")
        assert sorted(trader.closed) == ["BTC", "ETH"]
        assert trader.orders == []
        assert [r["action"] for r in results] == ["CLOSE", "CLOSE"]

    def test_holding_within_tolerance_is_left_alone(self, mock_engine):
        mod = self._module()
        trader = StubTrader()
        held = {"BTC": {"coin": "BTC", "size_usd": 59.0, "direction": "LONG"}}
        results = mod._rebalance_spot(trader, mock_engine, {"BTC": 0.60}, "risk-on",
                                      held, "robinhood")
        assert results == [] and trader.orders == [] and trader.closed == []

    def test_mode_label_reports_the_connector_mode_for_spot(self):
        mod = self._module()
        assert mod._mode_label("robinhood", False, StubTrader(mode="DRY_RUN")) == "DRY_RUN"
        assert mod._mode_label("hyperliquid", True, StubTrader()) == "MAINNET"
        assert mod._mode_label("hyperliquid", False, StubTrader()) == "TESTNET"

    def test_robinhood_trader_requires_credentials(self):
        mod = self._module()
        with patch("trading.robinhood.get_robinhood_trader",
                   return_value=StubTrader(configured=False)):
            with pytest.raises(ValueError, match="ROBINHOOD_API_KEY"):
                mod._get_trader(venue="robinhood")


# ── API: wallet creation payload and the health block ──────────────────────

os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("GRID_JWT_SECRET", "test-secret-key-for-testing-only")
os.environ.setdefault("GRID_JWT_EXPIRE_HOURS", "1")
os.environ.setdefault(
    "GRID_MASTER_PASSWORD_HASH",
    "$2b$12$abcdefghijklmnopqrstuuFb1mY3p5oXq0rN8sxqf6vV2QcVx1zSi",
)


@pytest.fixture(scope="module")
def client():
    from fastapi.testclient import TestClient

    from api.main import app

    return TestClient(app)


def _auth_header() -> dict[str, str]:
    from api.auth import create_token

    return {"Authorization": f"Bearer {create_token(expires_hours=1)}"}


class TestWalletCreation:
    def test_robinhood_wallet_payload_reaches_the_manager(self, client):
        wm = MagicMock()
        wm.create_wallet.return_value = "robinhood_live_abcd1234"
        with patch("api.routers.trading._get_wallet_manager", return_value=wm):
            resp = client.post("/api/v1/trading/wallets", headers=_auth_header(), json={
                "exchange": "robinhood", "wallet_type": "live",
                "initial_capital": 100.0, "max_drawdown_limit": 0.20,
            })
        assert resp.status_code == 200
        assert resp.json()["wallet_id"] == "robinhood_live_abcd1234"
        kwargs = wm.create_wallet.call_args.kwargs
        assert kwargs["exchange"] == "robinhood"
        assert kwargs["initial_capital"] == 100.0
        assert kwargs["max_drawdown_limit"] == 0.20

    def test_wallet_creation_requires_auth(self, client):
        resp = client.post("/api/v1/trading/wallets",
                           json={"exchange": "robinhood", "initial_capital": 100.0})
        assert resp.status_code == 401


class TestExecuteSignalsEndpoint:
    def test_venue_is_passed_through(self, client):
        with patch("trading.signal_executor.execute_signals",
                   return_value={"venue": "robinhood"}) as run:
            resp = client.post(
                "/api/v1/trading/execute-signals?venue=robinhood&wallet_id=w1",
                headers=_auth_header(),
            )
        assert resp.status_code == 200 and resp.json()["venue"] == "robinhood"
        assert run.call_args.kwargs == {"venue": "robinhood", "venue_wallet_id": "w1"}

    def test_unknown_venue_is_rejected_at_the_boundary(self, client):
        resp = client.post("/api/v1/trading/execute-signals?venue=coinbase",
                           headers=_auth_header())
        assert resp.status_code == 400
        assert "Unsupported venue" in resp.json()["detail"]


class TestHealthRobinhoodBlock:
    def test_block_reports_mode_without_calling_robinhood(self, client, mock_engine):
        session = FakeSession(_paged_pairs())
        trader = _trader(session=session)
        with patch("api.routers.system.get_db_engine", return_value=mock_engine), \
             patch("trading.robinhood.get_robinhood_trader", return_value=trader):
            resp = client.get("/api/v1/system/health")
        assert resp.status_code == 200
        block = resp.json()["checks"]["robinhood"]
        assert block == {"mode": "DRY_RUN", "configured": True, "live_trading": False,
                         "max_position_usd": 100.0, "max_drawdown_pct": 0.20}
        assert session.calls == []  # health never reaches out to the venue

    def test_unconfigured_is_reported_not_degraded(self, client, mock_engine):
        with patch("api.routers.system.get_db_engine", return_value=mock_engine), \
             patch("trading.robinhood.get_robinhood_trader",
                   return_value=rh.RobinhoodCryptoTrader()):
            resp = client.get("/api/v1/system/health")
        data = resp.json()
        assert data["checks"]["robinhood"]["mode"] == "UNCONFIGURED"
        assert not any("robinhood" in r for r in data["degraded_reasons"])

    def test_bad_credentials_surface_as_degraded(self, client, mock_engine):
        with patch("api.routers.system.get_db_engine", return_value=mock_engine), \
             patch("trading.robinhood.get_robinhood_trader",
                   side_effect=ValueError("bad key")):
            resp = client.get("/api/v1/system/health")
        data = resp.json()
        assert data["checks"]["robinhood"]["mode"] == "ERROR"
        assert "robinhood connector misconfigured" in data["degraded_reasons"]
        assert data["status"] == "degraded"
