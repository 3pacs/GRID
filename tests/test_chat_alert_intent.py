"""Tests for the deterministic price-alert intent parser in api.routers.chat.

Regression coverage for the percent-phrasing bug: live QA on grid-svr showed
"tell me when NVDA drops 5 percent" being parsed as "drops below $5" (the
number "5" matched the bare-price pattern before anyone checked for a percent
cue), silently creating a junk $5 alert on a $212 stock. See
api/routers/chat.py::_parse_alert_intent / _ALERT_PERCENT and the
compose_layout branch that resolves a percent move to a price.

Stubs api.auth to avoid heavy transitive deps (psycopg2/jose/cryptography)
that may not be installed in a lightweight test environment — mirrors the
pattern already used in tests/test_canvas_api.py. api.routers.price_alerts
is stubbed outright (rather than imported for real) since chat.py only ever
reaches it via a deferred `from api.routers.price_alerts import ...` inside
the function body, and the real module pulls in pandas via journal.log.
"""

from __future__ import annotations

import asyncio
import sys
from types import ModuleType

import pytest

try:
    import api.auth  # noqa: F401 — trigger real import if possible
except Exception:
    _auth_stub = ModuleType("api.auth")
    _auth_stub.require_auth = lambda: None  # type: ignore[attr-defined]
    _auth_stub.decode_token = lambda token: None  # type: ignore[attr-defined]
    sys.modules["api.auth"] = _auth_stub

from api.routers import chat as chat_module
from api.routers.chat import ChatComposeRequest, _parse_alert_intent

NVDA_PRICE = 212.50


# ── _parse_alert_intent: pure function, no mocking needed ──────────────────


class TestParseAlertIntentPercent:
    def test_drops_n_percent_is_relative(self):
        got = _parse_alert_intent("tell me when NVDA drops 5 percent")
        assert got == {"ticker": "NVDA", "direction": "below", "pct": 0.05}

    def test_drops_n_pct_sign_is_relative(self):
        got = _parse_alert_intent("tell me when NVDA drops 5%")
        assert got == {"ticker": "NVDA", "direction": "below", "pct": 0.05}

    def test_drops_n_pct_word_is_relative(self):
        got = _parse_alert_intent("tell me when NVDA drops 5 pct")
        assert got == {"ticker": "NVDA", "direction": "below", "pct": 0.05}

    def test_goes_up_n_percent_is_relative_above(self):
        got = _parse_alert_intent("tell me when NVDA goes up 3 percent")
        assert got == {"ticker": "NVDA", "direction": "above", "pct": 0.03}

    def test_bare_number_still_a_price(self):
        got = _parse_alert_intent("tell me when NVDA drops below 5")
        assert got == {"ticker": "NVDA", "direction": "below", "threshold": 5.0}

    def test_bare_dollar_amount_still_a_price(self):
        got = _parse_alert_intent("tell me when NVDA drops below $5.50")
        assert got == {"ticker": "NVDA", "direction": "below", "threshold": 5.5}

    def test_percent_without_direction_cue_is_ambiguous(self):
        assert _parse_alert_intent("NVDA is up 5 percent today") is None

    def test_percent_without_ticker_is_ambiguous(self):
        assert _parse_alert_intent("tell me when it drops 5 percent") is None


# ── compose_layout: exercise the branch that resolves pct -> price ─────────


def _install_price_alerts_stub(monkeypatch, *, current_price_fn=None, create_alert_fn=None):
    calls = {"create_alert": []}

    def _default_current_price(ticker, prefer_live=False):
        return None, "none"

    def _default_create_alert(owner, ticker, direction, threshold, note=None):
        return {"ok": False, "error": "stub not configured"}

    def _wrapped_create_alert(owner, ticker, direction, threshold, note=None):
        calls["create_alert"].append(
            {"owner": owner, "ticker": ticker, "direction": direction, "threshold": threshold}
        )
        return (create_alert_fn or _default_create_alert)(owner, ticker, direction, threshold, note=note)

    stub = ModuleType("api.routers.price_alerts")
    stub.current_price = current_price_fn or _default_current_price
    stub.create_alert_record = _wrapped_create_alert
    monkeypatch.setitem(sys.modules, "api.routers.price_alerts", stub)
    return calls


def _fake_alert_record(current_price=None):
    def _create(owner, ticker, direction, threshold, note=None):
        already_met = current_price is not None and (
            (direction == "above" and current_price >= threshold)
            or (direction == "below" and current_price <= threshold)
        )
        return {
            "ok": True,
            "id": 1,
            "ticker": ticker,
            "direction": direction,
            "threshold": threshold,
            "current_price": current_price,
            "already_met": already_met,
        }

    return _create


def _compose(monkeypatch, question, *, current_price_fn=None, create_alert_fn=None):
    monkeypatch.setattr(chat_module, "_user_id_from_token", lambda token: "dad")
    monkeypatch.setattr(chat_module, "_log_capability_gap", lambda **kwargs: 999)
    calls = _install_price_alerts_stub(
        monkeypatch, current_price_fn=current_price_fn, create_alert_fn=create_alert_fn
    )
    req = ChatComposeRequest(question=question)
    resp = asyncio.run(chat_module.compose_layout(req, token="test-token"))
    return resp, calls


class TestComposePercentAlert:
    def test_drops_5_percent_resolves_from_live_price(self, monkeypatch):
        resp, calls = _compose(
            monkeypatch,
            "tell me when NVDA drops 5 percent",
            current_price_fn=lambda ticker, prefer_live=False: (NVDA_PRICE, "grid"),
            create_alert_fn=_fake_alert_record(current_price=NVDA_PRICE),
        )
        assert resp.alert_created is True
        assert len(calls["create_alert"]) == 1
        assert calls["create_alert"][0]["ticker"] == "NVDA"
        assert calls["create_alert"][0]["direction"] == "below"
        assert calls["create_alert"][0]["threshold"] == pytest.approx(201.875)
        # Reply names both the computed threshold and the price it was set from.
        assert "201.88" in resp.spoken_reply
        assert "212.50" in resp.spoken_reply

    def test_drops_5_pct_sign_resolves_same_as_percent_word(self, monkeypatch):
        resp, calls = _compose(
            monkeypatch,
            "tell me when NVDA drops 5%",
            current_price_fn=lambda ticker, prefer_live=False: (NVDA_PRICE, "grid"),
            create_alert_fn=_fake_alert_record(current_price=NVDA_PRICE),
        )
        assert resp.alert_created is True
        assert calls["create_alert"][0]["threshold"] == pytest.approx(201.875)
        assert "201.88" in resp.spoken_reply
        assert "212.50" in resp.spoken_reply

    def test_goes_up_3_percent_resolves_above_threshold(self, monkeypatch):
        resp, calls = _compose(
            monkeypatch,
            "tell me when NVDA goes up 3 percent",
            current_price_fn=lambda ticker, prefer_live=False: (NVDA_PRICE, "grid"),
            create_alert_fn=_fake_alert_record(current_price=NVDA_PRICE),
        )
        assert resp.alert_created is True
        assert calls["create_alert"][0]["direction"] == "above"
        assert calls["create_alert"][0]["threshold"] == pytest.approx(218.875)
        assert "218.88" in resp.spoken_reply
        assert "212.50" in resp.spoken_reply

    def test_drops_below_5_is_unchanged_bare_price(self, monkeypatch):
        resp, calls = _compose(
            monkeypatch,
            "tell me when NVDA drops below 5",
            create_alert_fn=_fake_alert_record(current_price=NVDA_PRICE),
        )
        assert resp.alert_created is True
        assert len(calls["create_alert"]) == 1
        assert calls["create_alert"][0]["threshold"] == 5.0

    def test_percent_with_no_price_available_creates_no_alert(self, monkeypatch):
        resp, calls = _compose(
            monkeypatch,
            "tell me when NVDA drops 5 percent",
            current_price_fn=lambda ticker, prefer_live=False: (None, "none"),
        )
        assert resp.alert_created is False
        assert calls["create_alert"] == []
        assert resp.cannot_fulfill is True
        assert "NVDA" in resp.spoken_reply
        assert "price" in resp.spoken_reply.lower()
