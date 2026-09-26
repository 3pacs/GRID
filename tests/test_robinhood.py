"""Tests for the Robinhood crypto connector (trading/robinhood.py).

No network: a fake session routes signed requests by path and records them.
"""

from __future__ import annotations

import base64
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import MagicMock
from urllib.parse import parse_qs, urlparse

import pytest

from trading import robinhood as rh
from trading.robinhood_risk_store import InMemoryRiskStore, utc_today

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


_FRESH = object()


def _routes(buying_power="1000", btc_qty="0.5", price="60000", spread_pct=0.001, quote_timestamp=_FRESH):
    """*quote_timestamp* is echoed back as the best_bid_ask row's own
    ``timestamp`` field (Robinhood's real schema). Default: a fresh venue
    timestamp stamped at request time. An ISO string simulates a stale (or
    fresh) quote; ``None`` omits the field entirely — which the connector
    must reject (no fallback to its local fetch time)."""

    def _quote_row():
        row = {"price": price, "bid_inclusive_of_sell_spread": str(float(price) * (1 - spread_pct)),
               "ask_inclusive_of_buy_spread": str(float(price) * (1 + spread_pct))}
        if quote_timestamp is _FRESH:
            row["timestamp"] = datetime.now(timezone.utc).isoformat()
        elif quote_timestamp is not None:
            row["timestamp"] = quote_timestamp
        return row
    return {
        ("GET", rh.PATH_ACCOUNT): {"account_number": "RH123456789", "status": "active",
                                   "buying_power": buying_power, "buying_power_currency": "USD"},
        ("GET", rh.PATH_HOLDINGS): {"results": [
            {"asset_code": "BTC", "total_quantity": btc_qty, "quantity_available_for_trading": btc_qty},
            {"asset_code": "ETH", "total_quantity": "0", "quantity_available_for_trading": "0"},
        ]},
        ("GET", rh.PATH_BEST_BID_ASK): lambda q, _b: {"results": [
            {"symbol": s, **_quote_row()} for s in q.get("symbol", [])
        ]},
        ("GET", rh.PATH_TRADING_PAIRS): {"results": [
            {"symbol": "BTC-USD", "status": "tradable", "min_order_size": "0.000001",
             "max_order_size": "100", "quantity_increment": "0.000001"},
        ]},
        ("GET", rh.PATH_ORDERS): {"results": [
            {"id": "o1", "symbol": "BTC-USD", "side": "buy", "type": "market", "state": "filled",
             "filled_asset_quantity": "0.001", "average_price": "59990", "created_at": "2026-09-10T10:00:00Z"},
        ]},
        ("POST", rh.PATH_ORDERS): lambda _q, body: {"id": "o-new", "state": "open", "average_price": price,
                                                     **json.loads(body)},
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
        # Marketable limit by default (ROBINHOOD_USE_LIMIT_ORDERS=True) —
        # Robinhood's Crypto Trading API supports type=limit / time_in_force=gtc.
        assert order["side"] == "buy" and order["symbol"] == "BTC-USD" and order["type"] == "limit"
        cfg = order["limit_order_config"]
        # $30 at the ask (60060, unpadded) floored to the 1e-6 increment —
        # quantity is sized off the touch price, not the slippage-padded limit.
        assert cfg["asset_quantity"] == "0.000499"
        assert cfg["time_in_force"] == "gtc"
        # limit_price = ask * (1 + slippage_bps/10000), 25bps default.
        assert float(cfg["limit_price"]) == pytest.approx(60060.0 * 1.0025, rel=1e-6)
        assert out["reference_price"] == pytest.approx(60060.0)
        assert out["executable_price"] == pytest.approx(60060.0 * 1.0025, rel=1e-6)
        assert out["bid"] == pytest.approx(59940.0) and out["ask"] == pytest.approx(60060.0)
        assert out["spread_bps"] == pytest.approx(20.0, rel=1e-3)
        assert not any(c["method"] == "POST" for c in session.calls)

    def test_dry_run_uses_market_order_when_limit_orders_disabled(self):
        session = FakeSession(_routes())
        trader = _trader(session=session, use_limit_orders=False)
        out = trader.open_position("btc", "LONG", 30)
        order = out["order"]
        assert order["type"] == "market"
        assert order["market_order_config"]["asset_quantity"] == "0.000499"

    def test_live_posts_signed_json_body(self):
        session = FakeSession(_routes())
        trader = _trader(live=True, session=session)
        out = trader.open_position("BTC", "LONG", 30)
        assert out["status"] == "submitted" and out["order_id"] == "o-new"
        post = next(c for c in session.calls if c["method"] == "POST")
        body = json.loads(post["data"])
        assert body["type"] == "limit"
        assert body["limit_order_config"]["asset_quantity"] == "0.000499"
        assert body["limit_order_config"]["time_in_force"] == "gtc"
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
        assert out["order"]["limit_order_config"]["asset_quantity"] == "0.0001"

    def test_short_without_holding_is_refused(self):
        trader = _trader(session=FakeSession(_routes(btc_qty="0")))
        out = trader.open_position("BTC", "SHORT", 30)
        assert "sells holdings only" in out["error"]

    def test_close_sells_whole_available_holding(self):
        out = _trader().close_position("BTC-USD")
        assert out["status"] == "dry_run" and out["direction"] == "CLOSE"
        assert out["order"]["limit_order_config"]["asset_quantity"] == "0.5"
        assert out["order"]["limit_order_config"]["time_in_force"] == "gtc"

    def test_close_without_position(self):
        out = _trader(session=FakeSession(_routes(btc_qty="0"))).close_position("BTC")
        assert "No open position" in out["error"]

    def test_drawdown_breach_halts_trading(self):
        trader = _trader()
        # Equity is 31,000 (see _routes() defaults) -> seeding a 100,000 peak
        # through the persisted store is a 69% drawdown from the high-water
        # mark, same scenario the old `trader._high_water_mark = 100000.0`
        # attribute assignment used to set up before that attribute moved
        # into RiskStore.
        trader.risk_store.touch(trader.venue, 100000.0, utc_today())
        out = trader.open_position("BTC", "LONG", 10)
        assert "Max drawdown breached" in out["error"]
        assert out["status"] == "blocked" and out["guard"] == "drawdown"
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
        assert fields["ROBINHOOD_MAX_DAILY_LOSS_PCT"].default == 0.05
        assert fields["ROBINHOOD_MAX_ORDERS_PER_DAY"].default == 6
        assert fields["ROBINHOOD_USE_LIMIT_ORDERS"].default is True


# ── persisted risk state (drawdown HWM, daily loss, order rate) ────────────


class TestPersistedRiskState:
    def test_peak_survives_a_new_trader_instance(self):
        """The bug this module exists to fix: get_robinhood_trader() builds a
        fresh RobinhoodCryptoTrader on every call, so the drawdown high-water
        mark has to live outside that object to survive it — here, a
        RiskStore shared across two independently-constructed traders,
        exactly as two requests handled by the factory would share the same
        Postgres-backed store."""
        store = InMemoryRiskStore()
        trader1 = _trader(risk_store=store)
        seeded = trader1.check_risk_limits()
        assert seeded["high_water_mark"] == pytest.approx(31000.0)

        # A brand new instance, same store — as if get_robinhood_trader() had
        # been called again on the next request after equity dropped to 0.
        trader2 = _trader(session=FakeSession(_routes(buying_power="0", btc_qty="0")), risk_store=store)
        risk = trader2.check_risk_limits()
        assert risk["high_water_mark"] == pytest.approx(31000.0)  # survived the new instance
        assert risk["current_drawdown_pct"] == pytest.approx(1.0)
        assert risk["drawdown_breached"] is True

    def test_daily_loss_cap_blocks_new_buys_not_sells(self):
        trader = _trader(max_daily_loss_pct=0.05)
        # Day started at 33,000; equity is 31,000 (_routes() defaults) — a
        # 6.06% loss from the open, over the 5% cap. Drawdown from the same
        # reference is also 6.06%, comfortably under the 20% default
        # drawdown limit, so this trips ONLY the daily loss cap.
        trader.risk_store.touch(trader.venue, 33000.0, utc_today())
        out = trader.open_position("BTC", "LONG", 10)
        assert out["status"] == "blocked" and out["guard"] == "daily_loss"
        assert "Daily loss cap breached" in out["error"]
        assert trader.check_risk_limits()["drawdown_breached"] is False
        # Sells/closes stay allowed.
        assert trader.close_position("BTC-USD")["status"] == "dry_run"

    def test_daily_loss_resets_on_a_new_day(self):
        trader = _trader(max_daily_loss_pct=0.05)
        yesterday = utc_today() - timedelta(days=1)
        # Yesterday's start (33,000) would itself be a daily-loss breach
        # against today's 31,000 equity if it were still the reference —
        # peak stays 33,000 either way (a high-water mark never resets).
        trader.risk_store.touch(trader.venue, 33000.0, yesterday)
        # check_risk_limits() (called inside open_position) uses utc_today(),
        # which has moved on from `yesterday` -> day_start_equity rolls to
        # TODAY's actual equity (31,000) in that same touch, so there is no
        # loss "from today" even though peak/drawdown history is unaffected.
        out = trader.open_position("BTC", "LONG", 10)
        assert out["status"] == "dry_run"
        risk = trader.check_risk_limits()
        assert risk["day_start_equity"] == pytest.approx(31000.0)
        assert risk["daily_loss_breached"] is False

    def test_order_rate_cap_blocks_after_the_limit(self):
        trader = _trader(max_orders_per_day=2)
        assert trader.open_position("BTC", "LONG", 10)["status"] == "dry_run"
        assert trader.open_position("BTC", "LONG", 10)["status"] == "dry_run"
        out = trader.open_position("BTC", "LONG", 10)
        assert out["status"] == "blocked" and out["guard"] == "order_rate"
        assert "Order-rate cap reached" in out["error"]

    def test_order_rate_cap_does_not_block_closes(self):
        trader = _trader(max_orders_per_day=1)
        trader.risk_store.touch(trader.venue, 31000.0, utc_today(), increment_order=True)
        assert trader.check_risk_limits()["order_rate_breached"] is True
        assert trader.close_position("BTC-USD")["status"] == "dry_run"


# ── idempotency ──────────────────────────────────────────────────────────


class TestIdempotency:
    def test_duplicate_client_order_id_is_rejected(self):
        trader = _trader()
        first = trader.open_position("BTC", "LONG", 10, client_order_id="decision-1")
        assert first["status"] == "dry_run"
        second = trader.open_position("BTC", "LONG", 10, client_order_id="decision-1")
        assert second["status"] == "duplicate"
        assert second["client_order_id"] == "decision-1"

    def test_different_keys_are_independent(self):
        trader = _trader()
        assert trader.open_position("BTC", "LONG", 10, client_order_id="a")["status"] == "dry_run"
        assert trader.open_position("BTC", "LONG", 10, client_order_id="b")["status"] == "dry_run"

    def test_close_position_is_deduplicated_too(self):
        trader = _trader()
        first = trader.close_position("BTC-USD", client_order_id="close-1")
        assert first["status"] == "dry_run"
        second = trader.close_position("BTC-USD", client_order_id="close-1")
        assert second["status"] == "duplicate"

    def test_a_blocked_attempt_does_not_burn_the_key(self):
        """A guard rejection never reaches the order log, so the SAME
        decision can retry once whatever guard tripped clears — see the
        _CONSUMED_STATUSES note in trading/robinhood_risk_store.py."""
        store = InMemoryRiskStore()
        wallet_state = {"active": False}
        trader = _trader(risk_store=store,
                         wallet_lookup=lambda: {"id": "w1", "status": "ACTIVE"} if wallet_state["active"] else None)
        blocked = trader.open_position("BTC", "LONG", 10, client_order_id="retry-me")
        assert blocked["status"] == "blocked" and blocked["guard"] == "wallet"
        assert store.is_duplicate("robinhood", "retry-me") is False

        wallet_state["active"] = True
        retried = trader.open_position("BTC", "LONG", 10, client_order_id="retry-me")
        assert retried["status"] == "dry_run"


# ── stale-quote and spread guards ───────────────────────────────────────


class TestQuoteGuards:
    def test_stale_quote_is_rejected(self):
        old_ts = (datetime.now(timezone.utc) - timedelta(seconds=90)).isoformat()
        trader = _trader(session=FakeSession(_routes(quote_timestamp=old_ts)), max_quote_age_s=30.0)
        out = trader.open_position("BTC", "LONG", 10)
        assert out["status"] == "blocked" and out["guard"] == "stale_quote"
        assert "old" in out["error"]

    def test_fresh_quote_passes(self):
        fresh_ts = datetime.now(timezone.utc).isoformat()
        trader = _trader(session=FakeSession(_routes(quote_timestamp=fresh_ts)), max_quote_age_s=30.0)
        assert trader.open_position("BTC", "LONG", 10)["status"] == "dry_run"

    def test_missing_timestamp_is_rejected_not_backfilled(self):
        """No venue `timestamp` field at all: the quote's age cannot be
        verified, so the stale-quote guard must fail closed. Falling back to
        GRID's local fetch time would measure how recently GRID asked, not
        how old Robinhood's price is."""
        trader = _trader(session=FakeSession(_routes(quote_timestamp=None)), max_quote_age_s=5.0)
        out = trader.open_position("BTC", "LONG", 10)
        assert out["status"] == "blocked" and out["guard"] == "stale_quote"
        assert "timestamp" in out["error"]

    def test_explicit_null_timestamp_is_rejected(self):
        """The shape observed 2026-09-24 14:23Z via a read-only quote pull
        through the deployed connector: the best_bid_ask row's `timestamp`
        field present but null. Same fail-closed rule as a missing field."""
        routes = _routes()
        routes[("GET", rh.PATH_BEST_BID_ASK)] = lambda q, _b: {"results": [
            {"symbol": s, "price": "60000", "bid_inclusive_of_sell_spread": "59940",
             "ask_inclusive_of_buy_spread": "60060", "timestamp": None}
            for s in q.get("symbol", [])
        ]}
        trader = _trader(session=FakeSession(routes), max_quote_age_s=5.0)
        out = trader.open_position("BTC", "LONG", 10)
        assert out["status"] == "blocked" and out["guard"] == "stale_quote"

    @pytest.mark.parametrize("bid,ask", [("0", "60060"), ("59940", "0"), ("", "60060"), ("59940", None)])
    def test_one_sided_quote_is_rejected_not_scored_as_zero_spread(self, bid, ask):
        """A missing/zero side makes the spread unmeasurable. It used to
        score as spread_bps=0 (passing the spread guard) and price off
        `mid`; it must be blocked instead — for opens and closes alike."""
        routes = _routes()
        fresh = datetime.now(timezone.utc).isoformat()
        routes[("GET", rh.PATH_BEST_BID_ASK)] = lambda q, _b: {"results": [
            {"symbol": s, "price": "60000", "bid_inclusive_of_sell_spread": bid,
             "ask_inclusive_of_buy_spread": ask, "timestamp": fresh}
            for s in q.get("symbol", [])
        ]}
        trader = _trader(session=FakeSession(routes))
        for out in (trader.open_position("BTC", "LONG", 10), trader.close_position("BTC-USD")):
            assert out["status"] == "blocked" and out["guard"] == "spread"
            assert "one-sided" in out["error"]

    def test_wide_spread_is_rejected(self):
        trader = _trader(session=FakeSession(_routes(spread_pct=0.05)), max_spread_bps=250.0)
        out = trader.open_position("BTC", "LONG", 10)
        assert out["status"] == "blocked" and out["guard"] == "spread"
        assert "spread" in out["error"]

    def test_spread_within_cap_passes(self):
        trader = _trader(session=FakeSession(_routes(spread_pct=0.001)), max_spread_bps=250.0)
        assert trader.open_position("BTC", "LONG", 10)["status"] == "dry_run"

    def test_realistic_robinhood_spread_passes_with_the_new_default(self):
        """Matches the live read-only observation on 2026-09-24 14:23Z (BTC
        188.7bps, ETH 189.7bps, SOL 187.8bps -- ~190bps is NORMAL Robinhood
        crypto pricing, matching its own ~95bps-per-side published fee).
        The guard must not block normal spread, only abnormal widening --
        see config.py's ROBINHOOD_MAX_SPREAD_BPS comment. Uses the
        connector's real default (no explicit max_spread_bps override) so
        this tracks whatever ROBINHOOD_MAX_SPREAD_BPS actually defaults to."""
        trader = _trader(session=FakeSession(_routes(spread_pct=0.0095)))  # (ask-bid)/mid ~= 190bps
        out = trader.open_position("BTC", "LONG", 10)
        assert out["status"] == "dry_run"
        assert out["spread_bps"] == pytest.approx(190.0, rel=1e-3)

    def test_close_is_also_guarded_by_stale_quote_and_spread(self):
        old_ts = (datetime.now(timezone.utc) - timedelta(seconds=90)).isoformat()
        trader = _trader(session=FakeSession(_routes(quote_timestamp=old_ts)), max_quote_age_s=30.0)
        out = trader.close_position("BTC-USD")
        assert out["status"] == "blocked" and out["guard"] == "stale_quote"

        wide = _trader(session=FakeSession(_routes(spread_pct=0.05)), max_spread_bps=250.0)
        out2 = wide.close_position("BTC-USD")
        assert out2["status"] == "blocked" and out2["guard"] == "spread"


# ── wallet gate ──────────────────────────────────────────────────────────


class TestWalletGate:
    def test_wallet_not_wired_never_blocks(self):
        """Default state for a directly-constructed trader (no wallet_lookup)
        — every pre-existing caller that never opted in keeps working."""
        trader = _trader()
        assert trader.wallet_lookup is None
        assert trader.open_position("BTC", "LONG", 10)["status"] == "dry_run"

    def test_killed_wallet_blocks_open(self):
        trader = _trader(wallet_lookup=lambda: {"id": "w1", "status": "KILLED"})
        out = trader.open_position("BTC", "LONG", 10)
        assert out["status"] == "blocked" and out["guard"] == "wallet"

    def test_no_active_wallet_blocks_open(self):
        trader = _trader(wallet_lookup=lambda: None)
        out = trader.open_position("BTC", "LONG", 10)
        assert out["status"] == "blocked" and out["guard"] == "wallet"

    def test_killed_wallet_blocks_close(self):
        trader = _trader(wallet_lookup=lambda: {"id": "w1", "status": "KILLED"})
        out = trader.close_position("BTC-USD")
        assert out["status"] == "blocked" and out["guard"] == "wallet"

    def test_active_wallet_allows_orders(self):
        trader = _trader(wallet_lookup=lambda: {"id": "w1", "status": "ACTIVE"})
        assert trader.open_position("BTC", "LONG", 10)["status"] == "dry_run"

    def test_wallet_lookup_failure_fails_closed(self):
        def _boom():
            raise RuntimeError("db down")

        trader = _trader(wallet_lookup=_boom)
        out = trader.open_position("BTC", "LONG", 10)
        assert out["status"] == "blocked" and out["guard"] == "wallet"

    def test_cancel_is_not_wallet_gated(self):
        """Cancelling only reduces risk — it stays available even when the
        wallet is KILLED."""
        session = FakeSession({**_routes(), ("POST", f"{rh.PATH_ORDERS}o1/cancel/"): {"ok": True}})
        trader = _trader(live=True, session=session, wallet_lookup=lambda: {"id": "w1", "status": "KILLED"})
        assert trader.cancel_order("o1")["status"] == "cancel_requested"


# ── alerts ───────────────────────────────────────────────────────────────


class TestAlerts:
    def test_alert_fires_on_live_order(self):
        session = FakeSession(_routes())
        alert_fn = MagicMock()
        trader = _trader(live=True, session=session, alert_fn=alert_fn)
        out = trader.open_position("BTC", "LONG", 10)
        assert out["status"] == "submitted"
        assert alert_fn.called
        assert "LIVE" in alert_fn.call_args_list[-1].args[0]

    def test_no_alert_on_a_clean_dry_run_by_default(self):
        alert_fn = MagicMock()
        trader = _trader(alert_fn=alert_fn)
        assert trader.open_position("BTC", "LONG", 10)["status"] == "dry_run"
        assert not alert_fn.called

    def test_dry_run_alert_is_opt_in(self):
        alert_fn = MagicMock()
        trader = _trader(alert_fn=alert_fn, alert_on_dry_run=True)
        trader.open_position("BTC", "LONG", 10)
        assert alert_fn.called

    def test_alert_fires_on_guard_trip(self):
        alert_fn = MagicMock()
        trader = _trader(alert_fn=alert_fn)
        trader.risk_store.touch(trader.venue, 100000.0, utc_today())
        trader.open_position("BTC", "LONG", 10)
        assert alert_fn.called
        assert "guard tripped" in alert_fn.call_args_list[-1].args[0].lower()

    def test_alert_failure_never_breaks_the_order(self):
        def _boom(*_a, **_kw):
            raise RuntimeError("smtp down")

        trader = _trader(session=FakeSession(_routes()), live=True, alert_fn=_boom)
        out = trader.open_position("BTC", "LONG", 10)
        assert out["status"] == "submitted"


# ── wallet P&L settlement ────────────────────────────────────────────────


class TestWalletPnl:
    def test_dry_run_sell_never_touches_the_wallet(self):
        """A DRY-RUN round trip must leave the wallet untouched: update_pnl
        mutates current_capital/total_pnl/win-loss/max_drawdown and can mark
        the wallet KILLED, which would then block every later live order.
        The estimate is recorded, labelled, in the sell's own order-log row."""
        store = InMemoryRiskStore()
        pnl_fn = MagicMock()
        wallet = {"id": "w1", "status": "ACTIVE"}

        buyer = _trader(session=FakeSession(_routes(price="60000")),
                        risk_store=store, wallet_lookup=lambda: wallet, wallet_pnl_fn=pnl_fn)
        assert buyer.open_position("BTC", "LONG", 100, client_order_id="buy-1")["status"] == "dry_run"
        seller = _trader(session=FakeSession(_routes(price="66000", btc_qty="0.5")),
                         risk_store=store, wallet_lookup=lambda: wallet, wallet_pnl_fn=pnl_fn)
        assert seller.close_position("BTC-USD", client_order_id="sell-1")["status"] == "dry_run"

        assert not pnl_fn.called
        sell_row = next(r for r in store._log if r["client_order_id"] == "sell-1")
        assert sell_row["simulated"] is True
        estimate = sell_row["raw_response"]
        assert estimate["label"].startswith("SIMULATED")
        assert estimate["simulated_pnl_estimate_usd"] > 0

    def test_simulated_buys_do_not_enter_the_real_cost_basis(self):
        """A LIVE sell after only dry-run buys has no real basis -> no
        wallet P&L is booked at all (rather than one measured against a
        price that was never paid)."""
        store = InMemoryRiskStore()
        pnl_fn = MagicMock()
        wallet = {"id": "w1", "status": "ACTIVE"}
        buyer = _trader(session=FakeSession(_routes(price="60000")),
                        risk_store=store, wallet_lookup=lambda: wallet, wallet_pnl_fn=pnl_fn)
        assert buyer.open_position("BTC", "LONG", 100, client_order_id="buy-1")["status"] == "dry_run"
        assert store.average_cost("robinhood", "BTC-USD") is None
        assert store.average_cost("robinhood", "BTC-USD", include_simulated=True) is not None

        seller = _trader(live=True, session=FakeSession(_routes(price="66000", btc_qty="0.5")),
                         risk_store=store, wallet_lookup=lambda: wallet, wallet_pnl_fn=pnl_fn)
        assert seller.close_position("BTC-USD", client_order_id="sell-1")["status"] == "submitted"
        assert not pnl_fn.called

    def test_live_sell_settles_against_real_buys_only(self):
        store = InMemoryRiskStore()
        pnl_fn = MagicMock()
        wallet = {"id": "w1", "status": "ACTIVE"}
        # A real (submitted) buy at 60000, then a simulated buy far below it
        # that must NOT drag the basis down.
        store.log_order("robinhood", "real-buy", status="submitted", ticker="BTC-USD", side="buy",
                        quantity="0.001", fill_price=60000.0)
        store.log_order("robinhood", "sim-buy", status="dry_run", ticker="BTC-USD", side="buy",
                        quantity="1.0", fill_price=1000.0, simulated=True)
        assert store.average_cost("robinhood", "BTC-USD") == pytest.approx(60000.0)

        seller = _trader(live=True, session=FakeSession(_routes(price="66000", btc_qty="0.5")),
                         risk_store=store, wallet_lookup=lambda: wallet, wallet_pnl_fn=pnl_fn)
        assert seller.close_position("BTC-USD", client_order_id="sell-1")["status"] == "submitted"
        assert pnl_fn.called
        wallet_id, pnl, is_win = pnl_fn.call_args.args
        assert wallet_id == "w1"
        # POST route echoes average_price=66000 -> (66000 - 60000) * 0.5 held.
        assert pnl == pytest.approx(3000.0)
        assert is_win is True

    def test_no_update_pnl_without_prior_cost_history(self):
        pnl_fn = MagicMock()
        trader = _trader(wallet_lookup=lambda: {"id": "w1", "status": "ACTIVE"}, wallet_pnl_fn=pnl_fn)
        trader.close_position("BTC-USD")  # nothing bought through this connector yet
        assert not pnl_fn.called

    def test_no_update_pnl_without_a_wallet(self):
        pnl_fn = MagicMock()
        trader = _trader(wallet_pnl_fn=pnl_fn)  # wallet_lookup not wired -> wallet is always None
        trader.close_position("BTC-USD")
        assert not pnl_fn.called


# ── simulate_order / simulate_close (the forward-paper-log API) ────────────


class TestSimulate:
    def test_simulate_order_never_submits_or_touches_counters(self):
        trader = _trader(live=True)  # even a LIVE-configured trader
        out = trader.simulate_order("BTC", "LONG", 10)
        assert out["status"] == "dry_run"
        assert not any(c["method"] == "POST" for c in trader._session.calls)
        assert trader.risk_store.get_state(trader.venue).orders_today == 0

    def test_simulate_still_evaluates_every_guard(self):
        trader = _trader()
        trader.risk_store.touch(trader.venue, 100000.0, utc_today())
        out = trader.simulate_order("BTC", "LONG", 10)
        assert out["status"] == "blocked" and out["guard"] == "drawdown"

    def test_simulate_does_not_burn_the_idempotency_key(self):
        trader = _trader()
        trader.simulate_order("BTC", "LONG", 10, client_order_id="probe-1")
        real = trader.open_position("BTC", "LONG", 10, client_order_id="probe-1")
        assert real["status"] == "dry_run"  # not reported as a duplicate of the simulation

    def test_simulate_close(self):
        trader = _trader(live=True)
        out = trader.simulate_close("BTC-USD")
        assert out["status"] == "dry_run"
        assert not any(c["method"] == "POST" for c in trader._session.calls)


# ── reconcile_stale_orders (explicit cancel-after for gtc limit orders) ────


class TestReconcileStaleOrders:
    def _routes_with_open_order(self, created_at: str, state: str = "open"):
        routes = _routes()
        routes[("GET", rh.PATH_ORDERS)] = {"results": [
            {"id": "stale-1", "symbol": "BTC-USD", "side": "buy", "type": "limit", "state": state,
             "filled_asset_quantity": "0", "average_price": None, "created_at": created_at},
        ]}
        routes[("POST", f"{rh.PATH_ORDERS}stale-1/cancel/")] = {"ok": True}
        return routes

    def test_noop_in_dry_run(self):
        trader = _trader(live=False, use_limit_orders=True)
        assert trader.reconcile_stale_orders() == {"checked": 0, "cancelled": [], "error": None}

    def test_noop_for_market_orders(self):
        trader = _trader(live=True, use_limit_orders=False)
        assert trader.reconcile_stale_orders() == {"checked": 0, "cancelled": [], "error": None}

    def test_cancels_an_order_older_than_the_cutoff(self):
        old = (datetime.now(timezone.utc) - timedelta(seconds=60)).strftime("%Y-%m-%dT%H:%M:%SZ")
        session = FakeSession(self._routes_with_open_order(old))
        trader = _trader(live=True, session=session, use_limit_orders=True, limit_cancel_after_s=15.0)
        result = trader.reconcile_stale_orders()
        assert result == {"checked": 1, "cancelled": ["stale-1"], "error": None}

    def test_leaves_a_recent_order_alone(self):
        recent = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        session = FakeSession(self._routes_with_open_order(recent))
        trader = _trader(live=True, session=session, use_limit_orders=True, limit_cancel_after_s=15.0)
        result = trader.reconcile_stale_orders()
        assert result == {"checked": 1, "cancelled": [], "error": None}

    def test_ignores_filled_orders(self):
        old = (datetime.now(timezone.utc) - timedelta(seconds=60)).strftime("%Y-%m-%dT%H:%M:%SZ")
        session = FakeSession(self._routes_with_open_order(old, state="filled"))
        trader = _trader(live=True, session=session, use_limit_orders=True, limit_cancel_after_s=15.0)
        assert trader.reconcile_stale_orders() == {"checked": 0, "cancelled": [], "error": None}

    def test_listing_failure_is_reported_as_an_error(self):
        """A blip fetching the order list must not look like "nothing to
        reconcile" — the caller (open_position/close_position) treats any
        error here as "can't currently prove the book is clean"."""
        trader = _trader(live=True, session=FakeSession({}), use_limit_orders=True)  # every route 404s
        result = trader.reconcile_stale_orders()
        assert result["checked"] == 0 and result["cancelled"] == []
        assert result["error"] and "could not list orders" in result["error"]

    def test_a_cancel_that_is_not_confirmed_is_reported_as_an_error(self):
        old = (datetime.now(timezone.utc) - timedelta(seconds=60)).strftime("%Y-%m-%dT%H:%M:%SZ")
        routes = self._routes_with_open_order(old)
        routes[("POST", f"{rh.PATH_ORDERS}stale-1/cancel/")] = {"error": "not found"}
        session = FakeSession(routes)
        trader = _trader(live=True, session=session, use_limit_orders=True, limit_cancel_after_s=15.0)
        result = trader.reconcile_stale_orders()
        assert result["checked"] == 1 and result["cancelled"] == []
        assert result["error"] and "failed to cancel" in result["error"]


class TestReconcileWiredIntoOrders:
    """reconcile_stale_orders() runs automatically at the start of every LIVE
    (non-simulated) open_position/close_position call, and a reconcile
    failure blocks the new order — see RobinhoodCryptoTrader._reconcile_guard."""

    def test_live_open_calls_reconcile_before_submitting(self):
        session = FakeSession(_routes())
        trader = _trader(live=True, session=session)
        out = trader.open_position("BTC", "LONG", 10)
        assert out["status"] == "submitted"
        calls = [(c["method"], c["path"]) for c in session.calls]
        assert calls.index(("GET", rh.PATH_ORDERS)) < calls.index(("POST", rh.PATH_ORDERS))

    def test_live_close_calls_reconcile_before_submitting(self):
        session = FakeSession(_routes())
        trader = _trader(live=True, session=session)
        out = trader.close_position("BTC-USD")
        assert out["status"] == "submitted"
        calls = [(c["method"], c["path"]) for c in session.calls]
        assert calls.index(("GET", rh.PATH_ORDERS)) < calls.index(("POST", rh.PATH_ORDERS))

    def test_reconcile_error_blocks_a_new_open(self):
        session = FakeSession(_routes())
        trader = _trader(live=True, session=session)
        trader.reconcile_stale_orders = MagicMock(
            return_value={"checked": 1, "cancelled": [], "error": "failed to cancel stale order(s): x: boom"})
        out = trader.open_position("BTC", "LONG", 10)
        assert out["status"] == "blocked" and out["guard"] == "reconcile_failed"
        assert "Could not reconcile" in out["error"]
        assert not any(c["method"] == "POST" for c in session.calls)  # never reached the order POST

    def test_reconcile_error_blocks_a_close_too(self):
        session = FakeSession(_routes())
        trader = _trader(live=True, session=session)
        trader.reconcile_stale_orders = MagicMock(
            return_value={"checked": 0, "cancelled": [], "error": "could not list orders to reconcile: boom"})
        out = trader.close_position("BTC-USD")
        assert out["status"] == "blocked" and out["guard"] == "reconcile_failed"
        assert not any(c["method"] == "POST" for c in session.calls)

    def test_reconcile_error_alerts(self):
        session = FakeSession(_routes())
        alert_fn = MagicMock()
        trader = _trader(live=True, session=session, alert_fn=alert_fn)
        trader.reconcile_stale_orders = MagicMock(
            return_value={"checked": 0, "cancelled": [], "error": "boom"})
        trader.open_position("BTC", "LONG", 10)
        assert alert_fn.called
        assert "reconcile_failed" in alert_fn.call_args_list[-1].args[0]

    def test_dry_run_never_calls_reconcile(self):
        trader = _trader(live=False)  # normal dry-run mode (ROBINHOOD_LIVE_TRADING=false)
        trader.reconcile_stale_orders = MagicMock()
        assert trader.open_position("BTC", "LONG", 10)["status"] == "dry_run"
        assert trader.close_position("BTC-USD")["status"] == "dry_run"
        trader.reconcile_stale_orders.assert_not_called()

    def test_simulate_never_calls_reconcile_even_when_live_configured(self):
        trader = _trader(live=True, session=FakeSession(_routes()))
        trader.reconcile_stale_orders = MagicMock()
        assert trader.simulate_order("BTC", "LONG", 10)["status"] == "dry_run"
        assert trader.simulate_close("BTC-USD")["status"] == "dry_run"
        trader.reconcile_stale_orders.assert_not_called()
