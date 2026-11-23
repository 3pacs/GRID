"""Synthetic MOVE proxy: variance interpolation, quote hygiene, Schwab exclusion.

Pure functions are tested directly; the writer runs against a MagicMock
connection. Sockets are refused, so nothing reaches the network or a database.
"""

from __future__ import annotations

import math
import socket
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from ingestion.options_rates_vol import (
    CHAIN_SQL,
    MAX_IV,
    MIN_IV,
    OptionQuoteRow,
    OptionsRatesVolPuller,
    _plausible_iv,
    compute_expiry_atm_iv,
    interpolate_constant_maturity_iv,
    quote_rejection,
    screen_quotes,
)
from store.options_provenance import VISIBLE_SNAPSHOTS, visible_sql


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    calls: list[object] = []

    def _refuse(*args, **kwargs):
        calls.append(args)
        raise AssertionError("network access attempted in an offline test")

    monkeypatch.setattr(socket.socket, "connect", _refuse)
    monkeypatch.setattr(socket, "create_connection", _refuse)
    monkeypatch.setattr(socket, "getaddrinfo", _refuse)
    yield
    assert calls == []


SNAP = date(2026, 11, 20)
EXP20 = SNAP + timedelta(days=20)
EXP40 = SNAP + timedelta(days=40)


def _quotes() -> list[OptionQuoteRow]:
    return [
        # 20 DTE ATM at strike 82 -> IV 0.16 (schema spells types call/put)
        OptionQuoteRow(EXP20, "call", 82.0, 1.40, 1.50, 0.16),
        OptionQuoteRow(EXP20, "put", 82.0, 1.38, 1.48, 0.16),
        # 40 DTE ATM at strike 82 -> IV 0.20
        OptionQuoteRow(EXP40, "C", 82.0, 2.20, 2.35, 0.20),
        OptionQuoteRow(EXP40, "P", 82.0, 2.18, 2.33, 0.20),
        # Garbage quote (zero bid, wide spread, absurd IV) must be ignored
        OptionQuoteRow(EXP20, "call", 80.0, 0.0, 5.0, 1.90),
    ]


def test_synthetic_move_variance_interpolation_and_hygiene() -> None:
    quotes = _quotes()
    assert compute_expiry_atm_iv([q for q in quotes if q.expiry == EXP20]) == pytest.approx(0.16)
    iv30 = interpolate_constant_maturity_iv(SNAP, quotes, target_dte=30)
    expected = ((0.5 * (0.16**2 * 20) + 0.5 * (0.20**2 * 40)) / 30.0) ** 0.5
    assert iv30 == pytest.approx(expected, rel=1e-6)


def test_no_clean_pair_and_far_one_sided_expiry_fail_closed() -> None:
    assert compute_expiry_atm_iv([OptionQuoteRow(EXP20, "call", 82.0, 1.40, 1.50, 0.16)]) is None
    far = [OptionQuoteRow(SNAP + timedelta(days=60), t, 82.0, 3.0, 3.1, 0.2) for t in ("call", "put")]
    assert interpolate_constant_maturity_iv(SNAP, far, target_dte=30) is None


def test_chain_reads_through_the_schwab_filtered_fragment() -> None:
    assert ("FROM " + VISIBLE_SNAPSHOTS + " os ") in CHAIN_SQL
    sql = visible_sql(CHAIN_SQL, include_schwab=False).text
    assert "NOT EXISTS (SELECT 1 FROM options_capture_batches schwab_pb" in sql
    assert "FROM options_snapshots schwab_v" in sql  # the latest-batch view, not options_snapshots_all


def _chain_rows() -> list[tuple]:
    done = datetime(2026, 11, 20, 21, 5, tzinfo=timezone.utc)
    return [(q.expiry, q.opt_type, q.strike, q.bid, q.ask, q.iv, "b-1", done) for q in _quotes()]


def test_writer_runs_filtered_sql_and_keeps_batch_provenance() -> None:
    puller = OptionsRatesVolPuller.__new__(OptionsRatesVolPuller)
    conn = MagicMock()
    executed: list[str] = []

    def _execute(statement, params=None):
        executed.append(str(statement))
        result = MagicMock()
        result.fetchall.return_value = _chain_rows() if params and params["tk"] == "TLT" else []
        return result

    conn.execute.side_effect = _execute
    puller.engine = MagicMock()
    puller.engine.begin.return_value.__enter__.return_value = conn
    puller._get_existing_dates = lambda sid, c, start_date=None, end_date=None: set()
    inserted: list[dict] = []
    puller._insert_raw = lambda **kw: inserted.append(kw)

    out = puller.pull_for_date(SNAP)

    assert executed and all("schwab_pb" in s for s in executed)
    assert all("options_snapshots_all" not in s for s in executed)
    assert [r["series_id"] for r in inserted] == ["options_rates_vol:tlt_iv_30d"]
    payload = inserted[0]["raw_payload"]
    assert payload["capture_batch_ids"] == ["b-1"]
    assert payload["capture_completed_at"] == "2026-11-20T21:05:00+00:00"
    assert out["metrics"]["options_rates_vol:ief_iv_30d"] is None


def test_writer_refuses_a_future_day() -> None:
    puller = OptionsRatesVolPuller.__new__(OptionsRatesVolPuller)
    puller.engine = MagicMock()
    out = puller.pull_for_date(date.today() + timedelta(days=1))
    assert out["status"] == "FAILED" and out["rows_inserted"] == 0
    puller.engine.begin.assert_not_called()


# ── Non-finite and out-of-range quotes (Codex 6011729288) ──────────────────

_NAN, _INF = float("nan"), float("inf")


def _q(**over) -> OptionQuoteRow:
    base = {"expiry": EXP20, "opt_type": "call", "strike": 82.0, "bid": 1.40, "ask": 1.50, "iv": 0.16}
    base.update(over)
    return OptionQuoteRow(**base)


BAD_QUOTES: list[tuple[OptionQuoteRow, str]] = [
    (_q(strike=_NAN), "non_finite_strike"),
    (_q(strike=_INF), "non_finite_strike"),
    (_q(strike=0.0), "non_positive_strike"),
    (_q(strike=-82.0), "non_positive_strike"),
    (_q(bid=_NAN), "non_finite_bid"),
    (_q(bid=_INF), "non_finite_bid"),
    (_q(bid=True), "non_finite_bid"),
    (_q(bid=0.0), "non_positive_bid"),
    (_q(bid=-0.1), "non_positive_bid"),
    (_q(ask=_NAN), "non_finite_ask"),
    (_q(ask=_INF), "non_finite_ask"),       # passed the old spread test: inf/inf is NaN, NaN > 0.25 is False
    (_q(ask=1.40), "locked_or_crossed_quote"),
    (_q(ask=1.30), "locked_or_crossed_quote"),
    (_q(bid=1e308, ask=1.7e308), "non_positive_mid"),   # the mid overflows to inf
    (_q(bid=1.00, ask=1.50), "wide_spread"),
    (_q(iv=_NAN), "non_finite_iv"),
    (_q(iv=_INF), "non_finite_iv"),
    (_q(iv=-_INF), "non_finite_iv"),
    (_q(iv=0.0), "iv_out_of_range"),
    (_q(iv=-0.16), "iv_out_of_range"),
    (_q(iv=16.0), "iv_out_of_range"),       # a percent-scaled feed fails closed
    (_q(iv=2.5), "iv_out_of_range"),
    (_q(opt_type="X"), "unknown_option_type"),
    (_q(opt_type="callx"), "unknown_option_type"),
    (_q(opt_type=None), "unknown_option_type"),
    (_q(expiry=datetime(2026, 12, 10, 16, 0)), "bad_expiry"),
    (_q(expiry="2026-12-10"), "bad_expiry"),
]


@pytest.mark.parametrize("quote,reason", BAD_QUOTES, ids=lambda v: v if isinstance(v, str) else "")
def test_every_bad_quote_is_rejected_with_its_reason(quote: OptionQuoteRow, reason: str) -> None:
    assert quote_rejection(quote) == reason
    clean, reasons = screen_quotes([quote])
    assert clean == [] and reasons == {reason: 1}


def test_bad_quotes_at_the_atm_strike_cannot_move_the_result() -> None:
    expected = interpolate_constant_maturity_iv(SNAP, _quotes(), target_dte=30)
    poison = []
    for expiry in (EXP20, EXP40):
        for side in ("call", "put"):
            poison += [OptionQuoteRow(expiry, side, q.strike, q.bid, q.ask, q.iv)
                       for q, _ in BAD_QUOTES if isinstance(q.strike, float) and q.strike == 82.0
                       and isinstance(q.expiry, date) and not isinstance(q.expiry, datetime)
                       and q.opt_type in ("call",)]
    assert len(poison) == 72  # 18 bad value shapes x 2 expiries x 2 sides, all at strike 82
    assert all(quote_rejection(q) is not None for q in poison)
    got = interpolate_constant_maturity_iv(SNAP, _quotes() + poison, target_dte=30)
    assert got == pytest.approx(expected) and math.isfinite(got)


def test_a_chain_of_only_bad_quotes_emits_nothing() -> None:
    assert interpolate_constant_maturity_iv(SNAP, [q for q, _ in BAD_QUOTES], target_dte=30) is None


def test_conflicting_duplicate_quotes_drop_the_strike_in_any_order() -> None:
    dup = [OptionQuoteRow(EXP20, "call", 82.0, 1.40, 1.50, 0.16),
           OptionQuoteRow(EXP20, "C", 82.0, 1.41, 1.49, 0.30),   # same expiry/side/strike, disagrees
           OptionQuoteRow(EXP20, "put", 82.0, 1.38, 1.48, 0.16),
           OptionQuoteRow(EXP20, "call", 84.0, 0.60, 0.70, 0.17),
           OptionQuoteRow(EXP20, "put", 84.0, 2.50, 2.60, 0.17)]
    forward, reverse = compute_expiry_atm_iv(dup), compute_expiry_atm_iv(list(reversed(dup)))
    assert forward == reverse == pytest.approx(0.17)   # 82 dropped for calls; 84 is the only pair left
    _, reasons = screen_quotes(dup)
    assert reasons == {"conflicting_duplicate_quote": 2}
    # An exact duplicate is not a conflict.
    assert compute_expiry_atm_iv(dup[:1] + dup[:1] + dup[2:3]) == pytest.approx(0.16)


def test_expiries_on_or_before_the_capture_day_never_count() -> None:
    stale = [OptionQuoteRow(SNAP + timedelta(days=d), t, 82.0, 1.40, 1.50, 0.50)
             for d in (-5, 0) for t in ("call", "put")]
    assert interpolate_constant_maturity_iv(SNAP, stale, target_dte=30) is None
    assert interpolate_constant_maturity_iv(SNAP, _quotes() + stale, target_dte=30) == pytest.approx(
        interpolate_constant_maturity_iv(SNAP, _quotes(), target_dte=30))


@pytest.mark.parametrize("kwargs", [
    {"min_dte": 0}, {"min_dte": -3}, {"target_dte": 5}, {"target_dte": 200}, {"max_dte": 20},
])
def test_interpolation_refuses_non_positive_or_inconsistent_maturities(kwargs) -> None:
    with pytest.raises(ValueError):
        interpolate_constant_maturity_iv(SNAP, _quotes(), **kwargs)
    with pytest.raises(ValueError):
        interpolate_constant_maturity_iv(datetime(2026, 11, 20, 16), _quotes())


def test_compute_expiry_atm_iv_refuses_mixed_expiries() -> None:
    with pytest.raises(ValueError):
        compute_expiry_atm_iv(_quotes())


def test_implausible_results_are_not_emitted() -> None:
    assert _plausible_iv(_INF) is None and _plausible_iv(_NAN) is None
    assert _plausible_iv(MAX_IV * 1.01) is None and _plausible_iv(0.0) is None
    assert _plausible_iv(0.2) == 0.2


def _writer(rows_by_ticker: dict[str, list[tuple]]) -> tuple[OptionsRatesVolPuller, list[dict]]:
    puller = OptionsRatesVolPuller.__new__(OptionsRatesVolPuller)
    conn = MagicMock()

    def _execute(statement, params=None):
        result = MagicMock()
        result.fetchall.return_value = rows_by_ticker.get(params["tk"], []) if params else []
        return result

    conn.execute.side_effect = _execute
    puller.engine = MagicMock()
    puller.engine.begin.return_value.__enter__.return_value = conn
    puller._get_existing_dates = lambda sid, c, start_date=None, end_date=None: set()
    inserted: list[dict] = []
    puller._insert_raw = lambda **kw: inserted.append(kw)
    return puller, inserted


def test_writer_skips_unconvertible_and_bad_rows_and_writes_only_finite_values() -> None:
    done = datetime(2026, 11, 20, 21, 5, tzinfo=timezone.utc)
    garbage = [
        ("not-a-date", "call", 82.0, 1.4, 1.5, 0.16, "b-1", done),
        (EXP20, "call", "abc", 1.4, 1.5, 0.16, "b-1", done),
        (EXP20, "call", 82.0, None, 1.5, 0.16, "b-1", done),
        (None, "put", 82.0, 1.4, 1.5, 0.16, "b-1", done),
        (EXP20, "put", 82.0, "nan", "inf", 0.16, "b-1", done),
        (EXP40, "call", 82.0, 2.2, 2.35, "inf", "b-1", done),
        (EXP40, "put", 82.0, 2.18, 2.33, 16.0, "b-1", done),
    ]
    puller, inserted = _writer({"TLT": _chain_rows() + garbage, "IEF": garbage})
    out = puller.pull_for_date(SNAP)
    assert [r["series_id"] for r in inserted] == ["options_rates_vol:tlt_iv_30d"]
    assert all(math.isfinite(r["value"]) and MIN_IV <= r["value"] <= MAX_IV for r in inserted)
    assert out["metrics"]["options_rates_vol:ief_iv_30d"] is None
    ief = out["rejected"]["IEF"]
    assert ief["bad_expiry"] == 1 and ief["non_numeric_field"] == 2 and ief["missing_expiry_or_type"] == 1
    assert ief["non_finite_bid"] == 1 and ief["non_finite_iv"] == 1 and ief["iv_out_of_range"] == 1
    # TLT's chain also carries the zero-bid garbage quote of _quotes().
    assert inserted[0]["raw_payload"]["rejected_quotes"] == {"non_positive_bid": 1, "non_finite_bid": 1,
                                                             "non_finite_iv": 1, "iv_out_of_range": 1}


def test_writer_with_only_bad_rows_writes_nothing() -> None:
    bad = [(EXP20, t, 82.0, _NAN, _INF, 0.16, "b-1", None) for t in ("call", "put")]
    puller, inserted = _writer({"TLT": bad, "IEF": bad})
    out = puller.pull_for_date(SNAP)
    assert inserted == [] and out["status"] == "NO_DATA"
    with pytest.raises(ValueError):
        puller.pull_for_date(datetime(2026, 11, 20, 16))


def test_numeric_expiries_are_refused_not_read_as_dates() -> None:
    from ingestion.options_rates_vol import _as_date

    for bad in (20261210, 20261210.0, 1765324800, "20261210", "Dec 10 2026", True):
        with pytest.raises(ValueError):
            _as_date(bad)
    assert _as_date("2026-12-10") == date(2026, 12, 10)
    assert _as_date("2026-12-10T00:00:00") == date(2026, 12, 10)
    assert _as_date(datetime(2026, 12, 10, 16)) == date(2026, 12, 10)

    rows = [(20261210, t, 82.0, 1.4, 1.5, 0.16, "b-1", None) for t in ("call", "put")]
    puller, inserted = _writer({"TLT": rows, "IEF": rows})
    out = puller.pull_for_date(SNAP)
    assert inserted == [] and out["rejected"]["TLT"]["bad_expiry"] == 2
