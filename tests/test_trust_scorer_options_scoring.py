"""Outcome scoring for rows whose direction lives in ``signal_value``.

Regression coverage for the 2026-09-10 fix: ``score_pending_signals``
compared ``signal_type`` to the literals ``"BUY"`` / ``"SELL"`` and never
called ``_infer_signal_direction``, so every ``options_flow`` /
``UNUSUAL_OPTIONS`` row (direction ``CALL`` / ``PUT`` in the JSONB
payload) scored WRONG — 0 CORRECT out of ~217K on grid-svr — and the
options tapes sat at Bayesian trust 0.001 in ``lever_pullers``. The same
literal test hit social ``HEAT_SPIKE`` rows (``BULLISH`` / ``BEARISH``).

Mirrors ``tests/test_signal_sources_write_path.py``: a capture-only
connection stands in for PostgreSQL so the tests pin the exact SQL and
bound params the scorer writes. Price lookups are replaced by an
in-memory ``(ticker, date) -> close`` table so the point-in-time path is
exercised only through its call signature; the PIT SQL itself is checked
separately against a captured statement.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from intelligence import trust_scorer
from intelligence.trust_scorer import (
    EVALUATION_WINDOWS,
    MOVE_THRESHOLD_PCT,
    PRICE_LOOKBACK_DAYS,
    _get_price_near_date,
    score_pending_signals,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TODAY = date.today()
#: Past every evaluation window in the table, inside the 90-day expiry.
SIGNAL_DAY = TODAY - timedelta(days=30)
OPTIONS_WINDOW = EVALUATION_WINDOWS["options_flow"]
SOCIAL_WINDOW = EVALUATION_WINDOWS["social"]
INSIDER_WINDOW = EVALUATION_WINDOWS["insider"]


class _ScorerConn:
    """Capture-only DB connection stub.

    Serves ``pending_rows`` for the scorer's pending-signal SELECT and
    records every ``execute(stmt, params)`` on ``.calls`` as
    ``(sql_text, params_dict)`` so tests can assert on the exact SQL and
    bound params. Everything else (table setup, price reads) returns an
    empty result.
    """

    def __init__(self, pending_rows: list[tuple] | None = None) -> None:
        self.pending_rows = list(pending_rows or [])
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def execute(self, stmt: Any, params: dict[str, Any] | None = None):
        sql = getattr(stmt, "text", None) or str(stmt)
        self.calls.append((sql, dict(params or {})))
        result = MagicMock()
        if "FROM signal_sources" in sql and "PENDING" in sql:
            result.fetchall.return_value = self.pending_rows
        else:
            result.fetchall.return_value = []
        result.fetchone.return_value = None
        return result

    def outcome_updates(self) -> list[dict[str, Any]]:
        return [p for sql, p in self.calls if "SET outcome = :outcome" in sql]

    def expiries(self) -> list[dict[str, Any]]:
        return [p for sql, p in self.calls if "SET outcome = 'EXPIRED'" in sql]


def _engine_for(conn: _ScorerConn) -> MagicMock:
    engine = MagicMock()
    cm = MagicMock()
    cm.__enter__.return_value = conn
    cm.__exit__.return_value = False
    engine.begin.return_value = cm
    engine.connect.return_value = cm
    return engine


def _row(
    sig_id: int,
    source_type: str,
    signal_type: str,
    signal_value: Any,
    *,
    ticker: str = "AAPL",
    source_id: str = "whale_aapl_200",
    signal_date: date = SIGNAL_DAY,
) -> tuple:
    # Column order of the scorer's SELECT:
    #   id, source_type, source_id, ticker, signal_type, signal_date, signal_value
    return (sig_id, source_type, source_id, ticker, signal_type, signal_date, signal_value)


def _options_row(sig_id: int, direction: str | None, **kw: Any) -> tuple:
    """An unusual_whales UNUSUAL_OPTIONS row exactly as the puller writes it."""
    payload: dict[str, Any] = {
        "signals": ["OI_SPIKE"],
        "notional": 5416.0,
        "oi_ratio": 4.05,
        "volume_ratio": 0.0006,
    }
    if direction is not None:
        payload["direction"] = direction
    return _row(sig_id, "options_flow", "UNUSUAL_OPTIONS", payload, **kw)


def _heat_row(sig_id: int, direction: str, **kw: Any) -> tuple:
    """A smart_money social HEAT_SPIKE row exactly as the puller writes it."""
    payload = {
        "mentions_z": 2.5,
        "sentiment": 1.0,
        "ticker_rank": 1,
        "platform": "reddit",
        "username": "roaring_kitty",
        "subreddit": "wallstreetbets",
        "direction": direction,
    }
    kw.setdefault("source_id", "reddit:roaring_kitty")
    return _row(sig_id, "social", "HEAT_SPIKE", payload, **kw)


@pytest.fixture
def prices(monkeypatch):
    """In-memory close table plus a spy on every lookup the scorer makes."""
    table: dict[tuple[str, date], float | None] = {}
    lookups: list[tuple[str, date, datetime | None]] = []

    def fake_price(engine, ticker, target_date, as_of=None):
        lookups.append((ticker, target_date, as_of))
        return table.get((ticker, target_date))

    monkeypatch.setattr(trust_scorer, "_get_price_near_date", fake_price)
    return SimpleNamespace(table=table, lookups=lookups)


def _set_move(prices, ticker: str, entry: float, exit_: float, window_days: int) -> None:
    prices.table[(ticker, SIGNAL_DAY)] = entry
    prices.table[(ticker, SIGNAL_DAY + timedelta(days=window_days))] = exit_


def _run(rows: list[tuple]) -> tuple[_ScorerConn, dict[str, Any]]:
    conn = _ScorerConn(rows)
    summary = score_pending_signals(_engine_for(conn))
    return conn, summary


# ---------------------------------------------------------------------------
# UNUSUAL_OPTIONS — direction from signal_value.direction (CALL / PUT)
# ---------------------------------------------------------------------------


def test_call_scores_correct_when_underlying_rises(prices):
    _set_move(prices, "AAPL", 100.0, 105.0, OPTIONS_WINDOW)

    conn, summary = _run([_options_row(1, "CALL")])

    updates = conn.outcome_updates()
    assert len(updates) == 1
    assert updates[0]["id"] == 1
    assert updates[0]["outcome"] == "CORRECT"
    assert updates[0]["ret"] == pytest.approx(5.0)
    assert summary["scored"] == 1
    assert summary["correct"] == 1
    assert summary["wrong"] == 0


def test_call_scores_wrong_when_underlying_falls(prices):
    _set_move(prices, "AAPL", 100.0, 95.0, OPTIONS_WINDOW)

    conn, summary = _run([_options_row(1, "CALL")])

    updates = conn.outcome_updates()
    assert len(updates) == 1
    assert updates[0]["outcome"] == "WRONG"
    assert updates[0]["ret"] == pytest.approx(-5.0)
    assert summary["wrong"] == 1
    assert summary["correct"] == 0


def test_put_scores_correct_when_underlying_falls(prices):
    _set_move(prices, "AAPL", 100.0, 95.0, OPTIONS_WINDOW)

    conn, summary = _run([_options_row(1, "PUT")])

    updates = conn.outcome_updates()
    assert len(updates) == 1
    assert updates[0]["outcome"] == "CORRECT"
    assert updates[0]["ret"] == pytest.approx(-5.0)
    assert summary["correct"] == 1


def test_put_scores_wrong_when_underlying_rises(prices):
    _set_move(prices, "AAPL", 100.0, 105.0, OPTIONS_WINDOW)

    conn, summary = _run([_options_row(1, "PUT")])

    updates = conn.outcome_updates()
    assert len(updates) == 1
    assert updates[0]["outcome"] == "WRONG"
    assert summary["wrong"] == 1


def test_missing_direction_stays_pending_without_a_price_lookup(prices):
    """No direction in the payload → the row is left PENDING, never WRONG,
    and the scorer does not spend a price lookup (or a yfinance call) on it."""
    _set_move(prices, "AAPL", 100.0, 105.0, OPTIONS_WINDOW)

    conn, summary = _run([_options_row(1, None)])

    assert conn.outcome_updates() == []
    assert conn.expiries() == []
    assert prices.lookups == []
    assert summary["skipped_unknown_direction"] == 1
    assert summary["scored"] == 0


@pytest.mark.parametrize(
    "direction, move_pct",
    [
        ("CALL", MOVE_THRESHOLD_PCT / 2),
        ("PUT", -MOVE_THRESHOLD_PCT / 2),
    ],
)
def test_move_inside_threshold_band_is_wrong_for_either_side(prices, direction, move_pct):
    _set_move(prices, "AAPL", 100.0, 100.0 * (1 + move_pct / 100.0), OPTIONS_WINDOW)

    conn, _ = _run([_options_row(1, direction)])

    assert [u["outcome"] for u in conn.outcome_updates()] == ["WRONG"]


def test_lowercase_and_json_text_payloads_are_read(prices):
    """psycopg2 hands JSONB back as a dict; a text column or a caller that
    bound ``json.dumps(...)`` hands back a string. Both must classify."""
    _set_move(prices, "AAPL", 100.0, 105.0, OPTIONS_WINDOW)
    _set_move(prices, "TSM", 100.0, 90.0, OPTIONS_WINDOW)
    rows = [
        _row(1, "options_flow", "UNUSUAL_OPTIONS", {"direction": "call", "oi_ratio": 3.2}),
        _row(
            2, "options_flow", "UNUSUAL_OPTIONS",
            json.dumps({"direction": "PUT", "notional": 250_000.0}),
            ticker="TSM", source_id="whale_tsm_180",
        ),
    ]

    conn, summary = _run(rows)

    by_id = {u["id"]: u["outcome"] for u in conn.outcome_updates()}
    assert by_id == {1: "CORRECT", 2: "CORRECT"}
    assert summary["correct"] == 2


def test_options_rows_use_the_seven_day_window_and_one_pit_snapshot(prices):
    """options_flow keeps the scanner window (7 d): entry close at
    signal_date, exit close at signal_date + 7, both read as of one
    cycle-start instant."""
    _set_move(prices, "AAPL", 100.0, 105.0, OPTIONS_WINDOW)

    _run([_options_row(1, "CALL")])

    assert EVALUATION_WINDOWS["options_flow"] == 7
    assert EVALUATION_WINDOWS["scanner"] == 7
    assert [(t, d) for t, d, _ in prices.lookups] == [
        ("AAPL", SIGNAL_DAY),
        ("AAPL", SIGNAL_DAY + timedelta(days=7)),
    ]
    as_ofs = {a for _, _, a in prices.lookups}
    assert len(as_ofs) == 1
    (as_of,) = as_ofs
    assert isinstance(as_of, datetime) and as_of.tzinfo is not None


def test_options_row_never_uses_payload_price_as_entry(prices):
    """A ``price`` on an options row is the premium (or the strike); the
    entry must be the underlying's PIT close at signal_date."""
    _set_move(prices, "AAPL", 100.0, 105.0, OPTIONS_WINDOW)
    row = _row(
        1, "options_flow", "UNUSUAL_OPTIONS",
        {"direction": "CALL", "price": 2.35, "strike": 200.0, "oi_ratio": 4.05},
    )

    conn, _ = _run([row])

    assert [(u["outcome"], u["ret"]) for u in conn.outcome_updates()] == [
        ("CORRECT", pytest.approx(5.0)),
    ]
    assert [(t, d) for t, d, _ in prices.lookups] == [
        ("AAPL", SIGNAL_DAY),
        ("AAPL", SIGNAL_DAY + timedelta(days=OPTIONS_WINDOW)),
    ]


def test_options_row_with_explicit_spot_skips_the_entry_lookup(prices):
    prices.table[("AAPL", SIGNAL_DAY + timedelta(days=OPTIONS_WINDOW))] = 95.0
    row = _row(
        1, "options_flow", "UNUSUAL_OPTIONS",
        {"direction": "PUT", "spot": 100.0, "price": 3.10},
    )

    conn, _ = _run([row])

    assert [(u["outcome"], u["ret"]) for u in conn.outcome_updates()] == [
        ("CORRECT", pytest.approx(-5.0)),
    ]
    assert [(t, d) for t, d, _ in prices.lookups] == [
        ("AAPL", SIGNAL_DAY + timedelta(days=OPTIONS_WINDOW)),
    ]


def test_non_options_payload_price_still_serves_as_entry(prices):
    prices.table[("XOM", SIGNAL_DAY + timedelta(days=INSIDER_WINDOW))] = 90.0
    row = _row(
        1, "insider", "insider_sell", {"price": 100.0, "shares": 5000},
        ticker="XOM", source_id="Jane Doe",
    )

    conn, _ = _run([row])

    assert [(u["outcome"], u["ret"]) for u in conn.outcome_updates()] == [
        ("CORRECT", pytest.approx(-10.0)),
    ]
    assert len(prices.lookups) == 1


def test_many_tapes_score_independently(prices):
    """A mixed batch — the whole options_flow shape on grid-svr — no longer
    collapses to all-WRONG."""
    _set_move(prices, "AAPL", 100.0, 108.0, OPTIONS_WINDOW)   # up
    _set_move(prices, "TSM", 100.0, 92.0, OPTIONS_WINDOW)     # down
    rows = [
        _options_row(1, "CALL", ticker="AAPL", source_id="whale_aapl_200"),
        _options_row(2, "PUT", ticker="AAPL", source_id="whale_aapl_180"),
        _options_row(3, "CALL", ticker="TSM", source_id="whale_tsm_200"),
        _options_row(4, "PUT", ticker="TSM", source_id="whale_tsm_170"),
    ]

    conn, summary = _run(rows)

    by_id = {u["id"]: u["outcome"] for u in conn.outcome_updates()}
    assert by_id == {1: "CORRECT", 2: "WRONG", 3: "WRONG", 4: "CORRECT"}
    assert summary["correct"] == 2
    assert summary["wrong"] == 2
    # One lookup per (ticker, date) — the memo still holds.
    assert len(prices.lookups) == 4


# ---------------------------------------------------------------------------
# HEAT_SPIKE — direction from signal_value.direction (BULLISH / BEARISH)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "direction, entry, exit_, expected",
    [
        ("BULLISH", 100.0, 106.0, "CORRECT"),
        ("BULLISH", 100.0, 94.0, "WRONG"),
        ("BEARISH", 100.0, 94.0, "CORRECT"),
        ("BEARISH", 100.0, 106.0, "WRONG"),
    ],
)
def test_heat_spike_scores_by_payload_direction(prices, direction, entry, exit_, expected):
    _set_move(prices, "GME", entry, exit_, SOCIAL_WINDOW)

    conn, summary = _run([_heat_row(1, direction, ticker="GME")])

    updates = conn.outcome_updates()
    assert [u["outcome"] for u in updates] == [expected]
    assert updates[0]["ret"] == pytest.approx((exit_ - entry) / entry * 100.0)
    assert summary["scored"] == 1


def test_neutral_heat_spike_stays_pending(prices):
    _set_move(prices, "GME", 100.0, 120.0, SOCIAL_WINDOW)

    conn, summary = _run([_heat_row(1, "NEUTRAL", ticker="GME")])

    assert conn.outcome_updates() == []
    assert prices.lookups == []
    assert summary["skipped_unknown_direction"] == 1


# ---------------------------------------------------------------------------
# Legacy BUY / SELL rows (register_signal writers) are unchanged
# ---------------------------------------------------------------------------


def test_legacy_buy_sell_rows_keep_their_stored_entry_price(prices):
    """register_signal stores the entry price as the scalar signal_value;
    only the exit close is looked up, over the source's own window."""
    prices.table[("XOM", SIGNAL_DAY + timedelta(days=INSIDER_WINDOW))] = 110.0
    rows = [
        _row(1, "insider", "BUY", 100.0, ticker="XOM", source_id="Jane Doe"),
        _row(2, "insider", "SELL", 100.0, ticker="XOM", source_id="John Roe"),
    ]

    conn, summary = _run(rows)

    by_id = {u["id"]: (u["outcome"], u["ret"]) for u in conn.outcome_updates()}
    assert by_id == {1: ("CORRECT", pytest.approx(10.0)), 2: ("WRONG", pytest.approx(10.0))}
    assert summary["correct"] == 1
    assert summary["wrong"] == 1
    assert prices.lookups == [("XOM", SIGNAL_DAY + timedelta(days=INSIDER_WINDOW), prices.lookups[0][2])]


def test_rows_inside_their_window_are_left_alone(prices):
    """A tape from yesterday has no 7-day outcome yet — untouched, no lookup."""
    fresh = _options_row(1, "CALL", signal_date=TODAY - timedelta(days=1))

    conn, summary = _run([fresh])

    assert conn.outcome_updates() == []
    assert prices.lookups == []
    assert summary["scored"] == 0


# ---------------------------------------------------------------------------
# The price read the scorer relies on is point-in-time bounded
# ---------------------------------------------------------------------------


def test_price_lookup_sql_is_pit_bounded(monkeypatch):
    """Same lookup the insider rule uses: ``obs_date <= target`` and
    ``pull_timestamp <= as_of`` on raw_series, ``signal_date <= target`` on
    options_daily_signals, all as bound parameters."""
    monkeypatch.setattr(trust_scorer, "_fetch_yfinance_price", lambda ticker, d: None)
    conn = _ScorerConn()
    target = date(2026, 9, 1)
    as_of = datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc)

    price = _get_price_near_date(_engine_for(conn), "AAPL", target, as_of=as_of)

    assert price is None  # stub has no closes; yfinance fallback stubbed out
    opts = [(sql, p) for sql, p in conn.calls if "FROM options_daily_signals" in sql]
    assert len(opts) == 1
    assert "signal_date <= :d" in opts[0][0]
    assert opts[0][1] == {"t": "AAPL", "d": target}

    raw = [(sql, p) for sql, p in conn.calls if "FROM raw_series" in sql]
    assert len(raw) == 1
    sql, params = raw[0]
    assert "obs_date <= :d" in sql
    assert "pull_timestamp <= :as_of" in sql
    assert "pull_status = 'SUCCESS'" in sql
    assert "ORDER BY obs_date DESC, pull_timestamp DESC" in sql
    assert params == {
        "sid": "YF:AAPL:close",
        "d": target,
        "lo": target - timedelta(days=PRICE_LOOKBACK_DAYS),
        "as_of": as_of,
    }
    # No value is ever interpolated into the statement text.
    assert "AAPL" not in sql
    assert "2026" not in sql


def test_price_lookup_defaults_as_of_to_now(monkeypatch):
    monkeypatch.setattr(trust_scorer, "_fetch_yfinance_price", lambda ticker, d: None)
    conn = _ScorerConn()
    before = datetime.now(timezone.utc)

    _get_price_near_date(_engine_for(conn), "AAPL", date(2026, 9, 1))

    raw = [p for sql, p in conn.calls if "FROM raw_series" in sql]
    assert len(raw) == 1
    assert raw[0]["as_of"].tzinfo is not None
    assert before <= raw[0]["as_of"] <= datetime.now(timezone.utc)
