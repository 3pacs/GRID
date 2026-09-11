"""Tests for the Robinhood crypto connector (trading/robinhood.py).

No network: a fake session routes signed requests by path and records them.
"""

from __future__ import annotations

import base64
import json
from decimal import Decimal
from urllib.parse import parse_qs, urlparse

import pytest

from trading import robinhood as rh

pytest.importorskip("cryptography")


# ── fixtures ───────────────────────────────────────────────────────────────


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
        self.calls.append({
            "method": method, "path": parsed.path, "query": parse_qs(parsed.query),
            "headers": headers, "data": data,
        })
        handler = self.routes.get((method, parsed.path))
        if handler is None:
            return _Resp({"detail": "not found"}, status=404)
        payload = handler(parse_qs(parsed.query), data) if callable(handler) else handler
        return _Resp(payload)


PRIVATE_B64, PUBLIC_B64 = rh.generate_keypair()


def _routes(buying_power="1000", btc_qty="0.5", price="60000"):
    return {
        ("GET", rh.PATH_ACCOUNT): {"account_number": "RH123456789", "status": "active",
                                   "buying_power": buying_power, "buying_power_currency": "USD"},
        ("GET", rh.PATH_HOLDINGS): {"results": [
            {"asset_code": "BTC", "total_quantity": btc_qty, "quantity_available_for_trading": btc_qty},
            {"asset_code": "ETH", "total_quantity": "0", "quantity_available_for_trading": "0"},
        ]},
        ("GET", rh.PATH_BEST_BID_ASK): lambda q, _b: {"results": [
            {"symbol": s, "price": price, "bid_inclusive_of_sell_spread": str(float(price) * 0.999),
             "ask_inclusive_of_buy_spread": str(float(price) * 1.001)} for s in q.get("symbol", [])
        ]},
        ("GET", rh.PATH_TRADING_PAIRS): {"results": [
            {"symbol": "BTC-USD", "status": "tradable", "min_order_size": "0.000001",
             "max_order_size": "100", "quantity_increment": "0.000001"},
        ]},
        ("GET", rh.PATH_ORDERS): {"results": [
            {"id": "o1", "symbol": "BTC-USD", "side": "buy", "type": "market", "state": "filled",
             "filled_asset_quantity": "0.001", "average_price": "59990", "created_at": "2026-09-10T10:00:00Z"},
        ]},
        ("POST", rh.PATH_ORDERS): lambda _q, body: {"id": "o-new", "state": "open", **json.loads(body)},
    }


def _trader(live=False, session=None, **kw):
    return rh.RobinhoodCryptoTrader(
        api_key="rh-key", private_key_b64=PRIVATE_B64, live=live,
        session=session or FakeSession(_routes()), **kw,
    )


# ── pure helpers ───────────────────────────────────────────────────────────


class TestHelpers:
    @pytest.mark.parametrize("ticker,expected", [
        ("BTC", "BTC-USD"), ("btc-usd", "BTC-USD"), ("ETH-PERP", "ETH-USD"),
        ("SOL/USDT", "SOL-USD"), (" doge ", "DOGE-USD"),
    ])
    def test_normalize_symbol(self, ticker, expected):
        assert rh.normalize_symbol(ticker) == expected

    def test_normalize_symbol_rejects_empty(self):
        with pytest.raises(ValueError):
            rh.normalize_symbol("-")

    def test_keypair_roundtrip_and_signature_verifies(self):
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

        key = rh.load_signing_key(PRIVATE_B64)
        sig = rh.sign_request(key, "k", 1700000000, "/api/v1/crypto/trading/accounts/", "GET", "")
        pub = Ed25519PublicKey.from_public_bytes(base64.b64decode(PUBLIC_B64))
        pub.verify(base64.b64decode(sig), b"k1700000000/api/v1/crypto/trading/accounts/GET")

    def test_load_signing_key_rejects_bad_length(self):
        with pytest.raises(ValueError):
            rh.load_signing_key(base64.b64encode(b"short").decode())

    def test_round_down_and_format(self):
        assert rh.round_down_to_increment(0.00123456789, "0.000001") == Decimal("0.001234")
        assert rh.round_down_to_increment(1.5, None) == Decimal("1.50000000")
        assert rh.round_down_to_increment(0, "0.01") == Decimal("0")
        assert rh.format_quantity(Decimal("0.001234000")) == "0.001234"
        assert rh.format_quantity(Decimal("2.0")) == "2"
        assert rh.format_quantity(Decimal("0.0000001")) == "0.0000001"


# ── transport ──────────────────────────────────────────────────────────────


class TestTransport:
    def test_unconfigured_trader_never_calls_network(self):
        session = FakeSession(_routes())
        trader = rh.RobinhoodCryptoTrader(session=session)
        assert trader.mode == "UNCONFIGURED"
        assert "error" in trader.get_account()
        assert trader.get_positions() == []
        assert "error" in trader.open_position("BTC", "LONG", 10)
        assert session.calls == []
        assert trader.status()["configured"] is False

    def test_signed_headers_cover_path_query_method_and_body(self):
        session = FakeSession(_routes())
        trader = _trader(session=session)
        trader.get_best_bid_ask(["BTC-USD", "ETH-USD"])
        call = session.calls[-1]
        assert call["method"] == "GET"
        assert call["query"]["symbol"] == ["BTC-USD", "ETH-USD"]
        headers = call["headers"]
        assert headers["x-api-key"] == "rh-key"
        assert headers["x-timestamp"].isdigit()
        expected = rh.sign_request(
            rh.load_signing_key(PRIVATE_B64), "rh-key", int(headers["x-timestamp"]),
            f"{rh.PATH_BEST_BID_ASK}?symbol=BTC-USD&symbol=ETH-USD", "GET", "",
        )
        assert headers["x-signature"] == expected

    def test_http_error_becomes_error_dict(self):
        session = FakeSession({})  # everything 404s
        trader = _trader(session=session)
        out = trader.get_account()
        assert out["error"].startswith("HTTP 404")


# ── reads ──────────────────────────────────────────────────────────────────


class TestReads:
    def test_account_masks_number(self):
        acct = _trader().get_account()
        assert acct["account_number"] == "…6789"
        assert acct["buying_power_usd"] == 1000.0

    def test_positions_value_holdings_at_mid(self):
        positions = _trader().get_positions()
        assert [p["coin"] for p in positions] == ["BTC"]  # zero ETH holding dropped
        assert positions[0]["size_usd"] == 30000.0
        assert positions[0]["direction"] == "LONG"

    def test_balance_and_high_water_mark(self):
        trader = _trader()
        bal = trader.get_balance()
        assert bal["equity_usd"] == 31000.0 and bal["high_water_mark"] == 31000.0
        assert bal["mode"] == "DRY_RUN" and bal["open_positions"] == 1

    def test_orders_are_flattened(self):
        orders = _trader().get_orders(limit=5)
        assert orders == [{
            "id": "o1", "symbol": "BTC-USD", "side": "buy", "type": "market", "state": "filled",
            "filled_quantity": "0.001", "average_price": "59990", "created_at": "2026-09-10T10:00:00Z",
        }]


# ── trades ─────────────────────────────────────────────────────────────────


class TestTrades:
    def test_dry_run_builds_order_without_posting(self):
        session = FakeSession(_routes())
        trader = _trader(session=session)
        out = trader.open_position("btc", "LONG", 30)
        assert out["status"] == "dry_run"
        order = out["order"]
        assert order["side"] == "buy" and order["symbol"] == "BTC-USD" and order["type"] == "market"
        # $30 at the ask (60060) floored to the 1e-6 increment
        assert order["market_order_config"]["asset_quantity"] == "0.000499"
        assert out["reference_price"] == pytest.approx(60060.0)
        assert not any(c["method"] == "POST" for c in session.calls)

    def test_live_posts_signed_json_body(self):
        session = FakeSession(_routes())
        trader = _trader(live=True, session=session)
        out = trader.open_position("BTC", "LONG", 30)
        assert out["status"] == "submitted" and out["order_id"] == "o-new"
        post = next(c for c in session.calls if c["method"] == "POST")
        body = json.loads(post["data"])
        assert body["market_order_config"]["asset_quantity"] == "0.000499"
        expected = rh.sign_request(
            rh.load_signing_key(PRIVATE_B64), "rh-key", int(post["headers"]["x-timestamp"]),
            rh.PATH_ORDERS, "POST", post["data"],
        )
        assert post["headers"]["x-signature"] == expected

    def test_size_cap_and_direction_validation(self):
        trader = _trader()
        assert "exceeds max_position_usd" in trader.open_position("BTC", "LONG", 500)["error"]
        assert "Invalid direction" in trader.open_position("BTC", "SIDEWAYS", 10)["error"]
        assert "positive" in trader.open_position("BTC", "LONG", 0)["error"]

    def test_short_sells_held_quantity_only(self):
        # Hold 0.0001 BTC, ask to "short" $30 (0.0005 BTC): sells only what is held.
        trader = _trader(session=FakeSession(_routes(btc_qty="0.0001")))
        out = trader.open_position("BTC", "SHORT", 30)
        assert out["status"] == "dry_run"
        assert out["order"]["side"] == "sell"
        assert out["order"]["market_order_config"]["asset_quantity"] == "0.0001"

    def test_short_without_holding_is_refused(self):
        trader = _trader(session=FakeSession(_routes(btc_qty="0")))
        out = trader.open_position("BTC", "SHORT", 30)
        assert "sells holdings only" in out["error"]

    def test_close_sells_whole_available_holding(self):
        out = _trader().close_position("BTC-USD")
        assert out["status"] == "dry_run" and out["direction"] == "CLOSE"
        assert out["order"]["market_order_config"]["asset_quantity"] == "0.5"

    def test_close_without_position(self):
        out = _trader(session=FakeSession(_routes(btc_qty="0"))).close_position("BTC")
        assert "No open position" in out["error"]

    def test_drawdown_breach_halts_trading(self):
        trader = _trader()
        trader._high_water_mark = 100000.0  # equity is 31,000 -> 69% drawdown
        out = trader.open_position("BTC", "LONG", 10)
        assert "Max drawdown breached" in out["error"]
        assert trader.check_risk_limits()["drawdown_breached"] is True

    def test_cancel_dry_run_and_live(self):
        assert _trader().cancel_order("o1")["status"] == "dry_run"
        session = FakeSession({**_routes(), ("POST", f"{rh.PATH_ORDERS}o1/cancel/"): {"ok": True}})
        assert _trader(live=True, session=session).cancel_order("o1")["status"] == "cancel_requested"


# ── factory / config ───────────────────────────────────────────────────────


class TestFactory:
    def test_factory_reads_settings_and_defaults_to_dry_run(self, monkeypatch):
        from config import settings

        monkeypatch.setattr(settings, "ROBINHOOD_API_KEY", "", raising=False)
        monkeypatch.setattr(settings, "ROBINHOOD_PRIVATE_KEY_B64", "", raising=False)
        monkeypatch.setattr(settings, "ROBINHOOD_LIVE_TRADING", False, raising=False)
        trader = rh.get_robinhood_trader()
        assert trader.mode == "UNCONFIGURED" and trader.live is False
        assert trader.max_position_usd == 100.0

    def test_settings_defaults(self):
        from config import Settings

        fields = Settings.model_fields
        assert fields["ROBINHOOD_LIVE_TRADING"].default is False
        assert fields["ROBINHOOD_MAX_POSITION_USD"].default == 100.0
        assert fields["ROBINHOOD_BASE_URL"].default == rh.ROBINHOOD_BASE_URL
