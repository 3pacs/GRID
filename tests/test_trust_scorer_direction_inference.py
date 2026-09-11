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

from datetime import date, timedelta
from unittest.mock import patch

import pytest

from intelligence import trust_scorer
from intelligence.trust_scorer import _extract_price, _infer_signal_direction


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


# ── Options tapes: CALL / PUT ─────────────────────────────────────────────
#
# 2026-09-10: the unusual_whales puller writes a literal "CALL" / "PUT" into
# signal_value["direction"] (ingestion/altdata/unusual_whales.py::
# _emit_whale_signal). Neither token was in the direction lists, so every
# options_flow row inferred "unknown" — one half of why ~223K tapes had
# never produced a single CORRECT outcome.


@pytest.mark.parametrize(
    "direction",
    ["CALL", "call", "Call", "calls", "CALLS"],
)
def test_unusual_options_call_is_bullish(direction):
    assert _infer_signal_direction(
        "UNUSUAL_OPTIONS",
        {"direction": direction, "oi_ratio": 4.1, "notional": 5_000_000},
    ) == "bullish"


@pytest.mark.parametrize(
    "direction",
    ["PUT", "put", "Put", "puts", "PUTS"],
)
def test_unusual_options_put_is_bearish(direction):
    assert _infer_signal_direction(
        "UNUSUAL_OPTIONS", {"direction": direction, "oi_ratio": 2.0}
    ) == "bearish"


@pytest.mark.parametrize(
    "direction,expected",
    [
        ("CALL_SWEEP", "bullish"),
        ("BULLISH_CALL", "bullish"),
        ("PUT_SWEEP", "bearish"),
        ("BEARISH_PUT", "bearish"),
        ("call spread", "bullish"),
        ("put spread", "bearish"),
    ],
)
def test_unusual_options_compound_directions(direction, expected):
    assert _infer_signal_direction(
        "UNUSUAL_OPTIONS", {"direction": direction}
    ) == expected


@pytest.mark.parametrize(
    "direction,expected",
    [
        # Explicit sentiment outranks the contract type: a bear call spread
        # is bearish even though it is built out of calls, and a bull put
        # spread is bullish.
        ("BEAR_CALL_SPREAD", "bearish"),
        ("BULL_PUT_SPREAD", "bullish"),
    ],
)
def test_sentiment_outranks_contract_type(direction, expected):
    assert _infer_signal_direction(
        "UNUSUAL_OPTIONS", {"direction": direction}
    ) == expected


def test_unusual_options_without_direction_is_unknown():
    # No direction in the payload → unscoreable, must stay PENDING.
    assert _infer_signal_direction(
        "UNUSUAL_OPTIONS", {"oi_ratio": 4.0, "notional": 1_000_000}
    ) == "unknown"


def test_heat_spike_uses_value_direction():
    assert _infer_signal_direction("HEAT_SPIKE", {"direction": "bullish"}) == "bullish"
    assert _infer_signal_direction("HEAT_SPIKE", {"direction": "bearish"}) == "bearish"


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


# ── Entry price on options rows ───────────────────────────────────────────
#
# An options payload's "price" is the premium (or the strike), never the
# underlying spot. Scoring it against a later *underlying* close compares two
# different instruments, so options rows must fall through to
# _get_price_near_date unless the payload marks a spot explicitly.


@pytest.mark.parametrize("source_type", ["options_flow", "whale_options"])
def test_extract_price_ignores_premium_on_options_rows(source_type):
    assert _extract_price({"price": 3.25, "direction": "CALL"}, source_type) is None


@pytest.mark.parametrize("source_type", ["options_flow", "whale_options"])
def test_extract_price_ignores_bare_scalar_on_options_rows(source_type):
    # A bare scalar on an options row is the notional premium.
    assert _extract_price(5_000_000.0, source_type) is None


@pytest.mark.parametrize(
    "key", ["spot", "spot_price", "underlying_price", "underlying"],
)
def test_extract_price_accepts_explicit_spot_on_options_rows(key):
    assert _extract_price({key: 182.4, "price": 3.25}, "options_flow") == 182.4


def test_extract_price_options_rejects_nonpositive_and_unparseable_spot():
    assert _extract_price({"spot": 0}, "options_flow") is None
    assert _extract_price({"spot": "n/a"}, "options_flow") is None


def test_extract_price_unchanged_for_non_options_sources():
    # Non-options behaviour must be untouched by the guard.
    assert _extract_price({"price": 42.0}, "insider") == 42.0
    assert _extract_price({"price": 42.0}) == 42.0
    assert _extract_price(42.0, "darkpool") == 42.0
    assert _extract_price({"notional": 1_000}, "insider") is None
    assert _extract_price(None, "options_flow") is None


# ── score_pending_signals wiring ──────────────────────────────────────────
#
# 2026-09-10 root cause: _infer_signal_direction existed and was tested, but
# score_pending_signals never called it — it compared signal_type against the
# literal strings "BUY"/"SELL", so UNUSUAL_OPTIONS fell to the else branch and
# was recorded WRONG on every single row regardless of price action. That is
# why ~223K options tapes had 0 CORRECT across ~1,900 scored rows.


class _FakeResult:
    def __init__(self, rows=None):
        self._rows = rows or []
        self.rowcount = len(self._rows)

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def scalar_one(self):
        return self._rows[0][0] if self._rows else 0


class _FakeConn:
    """Returns the pending rows for the SELECT, records every UPDATE."""

    def __init__(self, pending_rows):
        self._pending = pending_rows
        self.updates = []

    def execute(self, statement, params=None):
        sql = str(statement)
        if sql.strip().upper().startswith("SELECT"):
            return _FakeResult(self._pending)
        if "UPDATE" in sql.upper():
            self.updates.append(params or {})
        return _FakeResult()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakeEngine:
    def __init__(self, pending_rows):
        self.conn = _FakeConn(pending_rows)

    def begin(self):
        return self.conn

    def connect(self):
        return self.conn


def _run_scoring(pending_rows, prices):
    """Run score_pending_signals against fake rows and a price lookup table."""
    engine = _FakeEngine(pending_rows)

    def fake_price(_engine, ticker, target_date, as_of=None):
        return prices.get((ticker, target_date))

    with patch.object(trust_scorer, "_ensure_tables", lambda _e: None), \
         patch.object(trust_scorer, "_get_price_near_date", fake_price), \
         patch.object(trust_scorer, "is_priceable_ticker", lambda _t: True):
        summary = trust_scorer.score_pending_signals(engine)
    return summary, engine.conn.updates


def _options_row(direction, signal_date):
    # (id, source_type, source_id, ticker, signal_type, signal_date, signal_value)
    return (
        1, "options_flow", "whale_aapl_200", "AAPL", "UNUSUAL_OPTIONS",
        signal_date, {"direction": direction, "oi_ratio": 4.1, "notional": 5e6},
    )


def test_options_call_with_underlying_up_scores_correct():
    sig_date = date.today() - timedelta(days=10)
    eval_date = sig_date + timedelta(days=7)   # options_flow window is 7d
    summary, updates = _run_scoring(
        [_options_row("CALL", sig_date)],
        {("AAPL", sig_date): 100.0, ("AAPL", eval_date): 105.0},
    )
    assert summary["scored"] == 1
    assert summary["correct"] == 1
    assert summary["wrong"] == 0
    assert updates[0]["outcome"] == "CORRECT"


def test_options_put_with_underlying_down_scores_correct():
    sig_date = date.today() - timedelta(days=10)
    eval_date = sig_date + timedelta(days=7)
    summary, updates = _run_scoring(
        [_options_row("PUT", sig_date)],
        {("AAPL", sig_date): 100.0, ("AAPL", eval_date): 94.0},
    )
    assert summary["correct"] == 1
    assert updates[0]["outcome"] == "CORRECT"


def test_options_call_with_underlying_down_scores_wrong():
    # The fix must not turn every options row CORRECT — a call into a
    # falling underlying is still WRONG.
    sig_date = date.today() - timedelta(days=10)
    eval_date = sig_date + timedelta(days=7)
    summary, updates = _run_scoring(
        [_options_row("CALL", sig_date)],
        {("AAPL", sig_date): 100.0, ("AAPL", eval_date): 94.0},
    )
    assert summary["correct"] == 0
    assert summary["wrong"] == 1
    assert updates[0]["outcome"] == "WRONG"


def test_options_move_below_threshold_scores_wrong():
    # MOVE_THRESHOLD_PCT stays at 1.0 — a 0.5% move is not a hit.
    sig_date = date.today() - timedelta(days=10)
    eval_date = sig_date + timedelta(days=7)
    summary, _ = _run_scoring(
        [_options_row("CALL", sig_date)],
        {("AAPL", sig_date): 100.0, ("AAPL", eval_date): 100.5},
    )
    assert summary["correct"] == 0
    assert summary["wrong"] == 1


def test_unknown_direction_row_stays_pending():
    # No direction in the payload → no UPDATE at all, row stays PENDING
    # rather than being forced to WRONG.
    sig_date = date.today() - timedelta(days=10)
    row = (
        2, "options_flow", "whale_aapl_200", "AAPL", "UNUSUAL_OPTIONS",
        sig_date, {"oi_ratio": 4.1},
    )
    summary, updates = _run_scoring(
        [row], {("AAPL", sig_date): 100.0},
    )
    assert summary["skipped_unknown_direction"] == 1
    assert summary["scored"] == 0
    assert summary["wrong"] == 0
    assert updates == []


def test_options_entry_price_uses_underlying_not_premium():
    # Payload carries a premium under "price"; the scorer must price the
    # underlying instead. Entry 100 -> 105 is +5% (CORRECT). Had the premium
    # (3.25) been used as entry, the return would have been ~+3130%.
    sig_date = date.today() - timedelta(days=10)
    eval_date = sig_date + timedelta(days=7)
    row = (
        3, "options_flow", "whale_aapl_200", "AAPL", "UNUSUAL_OPTIONS",
        sig_date, {"direction": "CALL", "price": 3.25},
    )
    summary, updates = _run_scoring(
        [row], {("AAPL", sig_date): 100.0, ("AAPL", eval_date): 105.0},
    )
    assert summary["correct"] == 1
    assert updates[0]["ret"] == pytest.approx(5.0)


def test_plain_buy_and_sell_still_score():
    # Regression guard: the types that already worked must keep working.
    sig_date = date.today() - timedelta(days=10)
    eval_date = sig_date + timedelta(days=7)
    summary, updates = _run_scoring(
        [(4, "scanner", "s1", "AAPL", "BUY", sig_date, None)],
        {("AAPL", sig_date): 100.0, ("AAPL", eval_date): 105.0},
    )
    assert summary["correct"] == 1
    assert updates[0]["outcome"] == "CORRECT"
