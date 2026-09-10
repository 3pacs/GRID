"""Tests for ``intelligence.trust_scorer._infer_signal_direction``.

Regression coverage for the 2026-05-13 fix: pre-fix, ``score_pending_signals``
only recognised literal ``"BUY"`` / ``"SELL"`` signal_types and defaulted
every other type to WRONG, regardless of actual price action. That made
99.3% of all scored signals WRONG (167K / 168K) and broke every
downstream calibration consumer (trust_scorer, lever_pullers,
hypothesis_engine.boost). The direction inference here is what each
non-trivial signal_type is expected to mean — these tests pin it so the
inference can grow safely as new sources land.
"""

from __future__ import annotations

import json

import pytest

from intelligence.trust_scorer import _infer_signal_direction


# ── Bullish ───────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "signal_type,signal_value",
    [
        ("BUY", None),
        ("CLUSTER_BUY", None),
        ("insider_buy", None),
        ("wsb_bullish", None),
        ("trade_idea_long", None),
        ("gov_contracts", None),
        ("CONTRACT_AWARD", None),
    ],
)
def test_infer_bullish_signal_types(signal_type, signal_value):
    assert _infer_signal_direction(signal_type, signal_value) == "bullish"


# ── Bearish ───────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "signal_type,signal_value",
    [
        ("SELL", None),
        ("UNUSUAL_SELL", None),
        ("insider_sell", None),
        ("wsb_bearish", None),
        ("trade_idea_short", None),
    ],
)
def test_infer_bearish_signal_types(signal_type, signal_value):
    assert _infer_signal_direction(signal_type, signal_value) == "bearish"


# ── Direction recoverable from signal_value ───────────────────────────────


def test_unusual_options_uses_value_direction_long_to_bullish():
    assert _infer_signal_direction(
        "UNUSUAL_OPTIONS", {"direction": "long", "notional": 250_000}
    ) == "bullish"


def test_unusual_options_uses_value_direction_short_to_bearish():
    assert _infer_signal_direction(
        "UNUSUAL_OPTIONS", {"direction": "short"}
    ) == "bearish"


# unusual_whales writes the option side, not a long/short word. A CALL tape
# expects the underlying up over the window, a PUT tape expects it down.
@pytest.mark.parametrize("direction", ["CALL", "call", " Call ", "CALLS"])
def test_unusual_options_call_is_bullish(direction):
    assert _infer_signal_direction(
        "UNUSUAL_OPTIONS",
        {"signals": ["OI_SPIKE"], "notional": 5416.0, "oi_ratio": 4.05,
         "direction": direction, "volume_ratio": 0.0006},
    ) == "bullish"


@pytest.mark.parametrize("direction", ["PUT", "put", "PUTS"])
def test_unusual_options_put_is_bearish(direction):
    assert _infer_signal_direction(
        "UNUSUAL_OPTIONS", {"direction": direction, "notional": 250_000}
    ) == "bearish"


@pytest.mark.parametrize(
    "signal_value",
    [
        {"notional": 5416.0, "oi_ratio": 4.05},   # direction key missing
        {"direction": ""},
        {"direction": None},
        {"direction": "STRADDLE"},                # no directional bet
    ],
)
def test_unusual_options_without_a_readable_direction_is_unknown(signal_value):
    assert _infer_signal_direction("UNUSUAL_OPTIONS", signal_value) == "unknown"


def test_heat_spike_uses_value_direction():
    assert _infer_signal_direction("HEAT_SPIKE", {"direction": "bullish"}) == "bullish"
    assert _infer_signal_direction("HEAT_SPIKE", {"direction": "bearish"}) == "bearish"


def test_heat_spike_feed_vocabulary_is_upper_case():
    # smart_money writes BULLISH / BEARISH / NEUTRAL verbatim.
    assert _infer_signal_direction("HEAT_SPIKE", {"direction": "BULLISH"}) == "bullish"
    assert _infer_signal_direction("HEAT_SPIKE", {"direction": "BEARISH"}) == "bearish"
    assert _infer_signal_direction("HEAT_SPIKE", {"direction": "NEUTRAL"}) == "unknown"


def test_json_text_payload_is_parsed():
    # A text column (or a caller that bound json.dumps(...)) hands the
    # document back as a string; it must classify like the dict.
    assert _infer_signal_direction(
        "UNUSUAL_OPTIONS", json.dumps({"direction": "PUT", "oi_ratio": 3.0})
    ) == "bearish"
    assert _infer_signal_direction("UNUSUAL_OPTIONS", "[1, 2, 3]") == "unknown"
    assert _infer_signal_direction("UNUSUAL_OPTIONS", "{not json") == "unknown"


def test_net_position_delta_uses_value_direction():
    assert _infer_signal_direction(
        "NET_POSITION_DELTA", {"direction": "up"}
    ) == "bullish"
    assert _infer_signal_direction(
        "NET_POSITION_DELTA", {"direction": "down"}
    ) == "bearish"


# ── Congressional trading — Transaction text ──────────────────────────────


def test_house_trading_purchase_is_bullish():
    assert _infer_signal_direction(
        "house_trading", {"Transaction": "Purchase", "Representative": "X"}
    ) == "bullish"


def test_senate_trading_sale_is_bearish():
    assert _infer_signal_direction(
        "senate_trading", {"Transaction": "Sale (Full)"}
    ) == "bearish"


def test_senate_trading_without_transaction_is_unknown():
    # If the Transaction field is missing, we can't tell. Keep PENDING.
    assert _infer_signal_direction("senate_trading", {}) == "unknown"


# ── Unknown / unscoreable ─────────────────────────────────────────────────


@pytest.mark.parametrize(
    "signal_type,signal_value",
    [
        ("lobbying", {}),          # depends on what they lobby for
        ("off_exchange", {}),      # no directional commitment
        ("LEGISLATION_NEW", {}),   # bill content varies
        ("wsb_neutral", {}),       # explicit neutral
        ("political_beta", {}),    # a beta number, not a direction
        (None, {}),                # missing type
        ("", {}),                  # empty type
        ("xyz_unknown", {}),       # unrecognised + no value direction
    ],
)
def test_infer_unknown_returns_unknown(signal_type, signal_value):
    # Unknown direction → caller must leave the row PENDING, not WRONG.
    assert _infer_signal_direction(signal_type, signal_value) == "unknown"


# ── Robustness ────────────────────────────────────────────────────────────


def test_infer_tolerates_non_dict_signal_value():
    # signal_value can be a list, string, None — must not crash.
    assert _infer_signal_direction("BUY", None) == "bullish"
    assert _infer_signal_direction("BUY", "not-a-dict") == "bullish"
    assert _infer_signal_direction("BUY", ["list", "of", "stuff"]) == "bullish"
    assert _infer_signal_direction("HEAT_SPIKE", "not-a-dict") == "unknown"


def test_infer_value_direction_overrides_unknown_type():
    # A type we don't recognise but whose payload declares a direction
    # should still classify. Lets future sources participate without
    # changing the static type sets.
    assert _infer_signal_direction(
        "future_alpha_alert", {"direction": "bullish"}
    ) == "bullish"
