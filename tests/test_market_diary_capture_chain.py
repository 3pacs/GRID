"""05:15Z capture -> qualified close -> 05:30Z previous-session diary (PR #858 job B).

Synthetic, invented data only: no DB, network, provider, clock or host. A fake
connection implements the ``store.observations`` window query (SUCCESS only,
source, start / as_of / as_of_ts bounds) so the REAL ``read_daily_policy_n``,
``is_daily_policy_capture`` and ``intelligence.market_diary._fresh_pair`` run
over rows built with the REAL ``price_close_contract`` capture payloads.

The scheduler half of the chain (the 05:15Z slot itself) lives in
tests/test_market_diary_capture_slot_composition.py; it needs the tree that
contains ``run_yfinance_morning_slot``.

Clock slots alone prove nothing about the first step, so the assertions are
about *eligibility*: a capture taken at D+1 05:15Z is qualified for session D in
every 2026 EDT/EST case, anything earlier or mis-identified is refused, and a
missing/failed/late capture surfaces as "no close for date" plus
``capture_ready=False`` instead of a stale value.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from ingestion.market_calendar import is_market_open
from price_close_contract import (
    daily_capture_boundary,
    daily_capture_payload,
    is_daily_policy_capture,
    raw_capture_payload,
)
from store.observations import read_daily_policy_n

REPO = Path(__file__).resolve().parent.parent
SERIES = "YF:^GSPC:close"
UTC = timezone.utc


def _load_runner():
    spec = importlib.util.spec_from_file_location("run_market_diary_chain_test", REPO / "scripts" / "run_market_diary.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def runner():
    return _load_runner()


def _at(d: date, hh: int, mm: int) -> datetime:
    return datetime(d.year, d.month, d.day, hh, mm, tzinfo=UTC)


def _sessions_2026() -> list[date]:
    d, out = date(2026, 1, 2), []
    while d <= date(2026, 12, 30):
        if is_market_open(d):
            out.append(d)
        d += timedelta(days=1)
    return out


# ── fake raw_series ───────────────────────────────────────────────────────


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _Conn:
    """Serves store.observations' window SQL from invented rows.

    A row is ``dict(sid, obs, value, pulled, status, payload)``. Bounds are
    applied exactly as ``_WINDOW_SQL`` does: SUCCESS only, source name,
    ``obs_date >= start``, ``obs_date <= as_of``, ``pull_timestamp <= as_of_ts``,
    ordered ``obs_date ASC, pull_timestamp DESC``.
    """

    def __init__(self, rows):
        self.rows = rows
        self.executed = []

    def execute(self, stmt, params=None):
        params = params or {}
        self.executed.append(params)
        sel = [
            r for r in self.rows
            if r["sid"] == params["sid"] and r["status"] == params["ok"]
            and (params["start"] is None or r["obs"] >= params["start"])
            and (params["as_of"] is None or r["obs"] <= params["as_of"])
            and (params["as_of_ts"] is None or r["pulled"] <= params["as_of_ts"])
        ]
        sel.sort(key=lambda r: (r["obs"], -r["pulled"].timestamp()))
        return _Result([(r["obs"], r["value"], r["pulled"], "yfinance", r["payload"]) for r in sel])

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _Engine:
    def __init__(self, rows):
        self.conn = _Conn(rows)

    def connect(self):
        return self.conn


def _row(obs: date, value: float, pulled: datetime, *, status="SUCCESS", payload="auto", sid=SERIES):
    if payload == "auto":
        payload = raw_capture_payload(sid.split(":")[1], sid.split(":")[2], obs, pulled, "1d")
    return {"sid": sid, "obs": obs, "value": value, "pulled": pulled, "status": status, "payload": payload}


def _captured(obs: date, value: float, *, sid=SERIES):
    """The row the 05:15Z slot writes for session ``obs``: pulled at obs+1 05:15Z."""
    return _row(obs, value, _at(obs + timedelta(days=1), 5, 15), sid=sid)


# ── POSITIVE ──────────────────────────────────────────────────────────────


def test_positive_every_2026_session_is_qualified_by_its_0515z_capture():
    """EDT and EST, every session, including both DST changeovers."""
    sessions = _sessions_2026()
    assert len(sessions) > 245
    for d in sessions:
        pulled = _at(d + timedelta(days=1), 5, 15)
        boundary = daily_capture_boundary("^GSPC", d)
        margin = pulled - boundary
        assert timedelta(minutes=15) <= margin <= timedelta(minutes=75), (d, margin)
        payload = daily_capture_payload("^GSPC", "close", d, pulled)
        assert payload is not None and is_daily_policy_capture(payload, SERIES, d, pulled), d


@pytest.mark.parametrize(
    "session, boundary_utc",
    [
        (date(2026, 3, 6), (2026, 3, 7, 5, 0)),    # Fri, EST: NY midnight = 05:00Z (smallest 15 min margin)
        (date(2026, 3, 9), (2026, 3, 10, 4, 0)),   # Mon right after spring-forward (Mar 8): EDT
        (date(2026, 10, 30), (2026, 10, 31, 4, 0)),  # Fri before fall-back (Nov 1): EDT
        (date(2026, 11, 2), (2026, 11, 3, 5, 0)),  # Mon right after fall-back: EST
    ],
)
def test_positive_boundary_tracks_new_york_midnight_across_dst(session, boundary_utc):
    assert daily_capture_boundary("^GSPC", session) == datetime(*boundary_utc, tzinfo=UTC)


def test_positive_timer_day_target_is_exactly_the_session_the_slot_captures(runner):
    """For every Tue..Sat in 2026: the 05:15Z slot captures iff the previous day
    was a session, and the 05:30Z diary then targets that same session; where the
    slot captures nothing, the diary skips. (Calendar-level proof of the chain's
    clock coupling; the slot's own code is in the composition test.)"""
    d = date(2026, 1, 1)
    checked = 0
    while d <= date(2026, 12, 31):
        if d.weekday() in (1, 2, 3, 4, 5):  # Tue..Sat: the timer days
            capture_expected = is_market_open(d - timedelta(days=1))
            skip = runner.previous_session_skip_reason(d)
            if capture_expected:
                assert skip is None, d
                assert runner.previous_session_date(d) == d - timedelta(days=1), d
            else:
                assert skip is not None, d
            checked += 1
        d += timedelta(days=1)
    assert checked > 250


def test_positive_diary_reads_the_captured_close_for_the_target(monkeypatch):
    from intelligence import market_diary

    monkeypatch.setattr(market_diary, "PRICES_ENABLED", True)
    d_prev, d = date(2026, 9, 3), date(2026, 9, 4)  # Thu, Fri
    rows = [_captured(d_prev, 7000.0), _captured(d, 7050.5)]
    conn = _Conn(rows)
    pair = market_diary._fresh_pair(conn, SERIES, d)
    assert pair is not None
    today, prior = pair
    assert (today.obs_date, today.value, prior.obs_date) == (d, 7050.5, d_prev)


def test_positive_qualified_close_ready_true_when_capture_landed(runner):
    d = date(2026, 9, 4)
    rows = [_captured(date(2026, 9, 3), 7000.0), _captured(d, 7050.5)]
    assert runner.qualified_close_ready(_Engine(rows), d, now_utc=_at(date(2026, 9, 5), 5, 30)) is True


def test_positive_main_reports_capture_ready_and_targets_the_session(runner, monkeypatch):
    monkeypatch.setattr(runner, "_utc_now", lambda: _at(date(2026, 9, 5), 5, 30))  # Sat
    d = date(2026, 9, 4)
    fake = MagicMock(return_value={"date": str(d), "thesis_accuracy": {}, "market_moves": {}})
    monkeypatch.setattr("intelligence.market_diary.write_diary_entry", fake)
    rows = [_captured(date(2026, 9, 3), 1.0), _captured(d, 2.0)]
    assert runner.main(["--previous-session", "--json"], engine=_Engine(rows)) == 0
    assert fake.call_args.kwargs["target_date"] == d


# ── REFUSAL ───────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "session, pulled_utc",
    [
        (date(2026, 9, 4), (2026, 9, 5, 3, 59)),    # EDT boundary 04:00Z
        (date(2026, 11, 6), (2026, 11, 7, 4, 59)),  # EST boundary 05:00Z
        (date(2026, 9, 4), (2026, 9, 4, 13, 30)),   # same-day 13:30Z snapshot
        (date(2026, 9, 4), (2026, 9, 4, 22, 0)),    # same-day 22:00Z snapshot
    ],
)
def test_refusal_capture_before_the_new_york_midnight_boundary(session, pulled_utc):
    pulled = datetime(*pulled_utc, tzinfo=UTC)
    assert daily_capture_payload("^GSPC", "close", session, pulled) is None
    assert raw_capture_payload("^GSPC", "close", session, pulled, "1d")["capture_state"] == "INCOMPLETE"


@pytest.mark.parametrize("label", [
    date(2026, 9, 5),    # Saturday
    date(2026, 9, 7),    # Labor Day
    date(2027, 1, 4),    # outside the audited year
])
def test_refusal_labels_that_cannot_be_qualified(label):
    assert daily_capture_boundary("^GSPC", label) is None
    assert daily_capture_payload("^GSPC", "close", label, _at(label + timedelta(days=2), 5, 15)) is None


def _mutated(payload, **changes):
    out = dict(payload)
    out.update(changes)
    return out


@pytest.mark.parametrize("name, mutate", [
    ("other_ticker", lambda p: _mutated(p, price_basis="YF:^DJI:close")),
    ("other_field", lambda p: _mutated(p, price_basis="YF:^GSPC:open")),
    ("other_obs_date", lambda p: _mutated(p, obs_date="2026-09-03")),
    ("provider_final_claimed", lambda p: _mutated(p, provider_certified_final=True)),
    ("incomplete_state", lambda p: _mutated(p, capture_state="INCOMPLETE")),
    ("unknown_state", lambda p: _mutated(p, capture_state="UNKNOWN")),
    ("other_interval", lambda p: _mutated(p, interval="1h")),
    ("other_contract", lambda p: _mutated(p, price_contract_version="spy_close_v1")),
])
def test_refusal_capture_identity_must_match_series_date_and_contract(name, mutate):
    d = date(2026, 9, 4)
    pulled = _at(date(2026, 9, 5), 5, 15)
    good = daily_capture_payload("^GSPC", "close", d, pulled)
    assert is_daily_policy_capture(good, SERIES, d, pulled)
    assert not is_daily_policy_capture(mutate(good), SERIES, d, pulled), name


def test_refusal_payload_of_another_series_never_qualifies_this_one():
    d, pulled = date(2026, 9, 4), _at(date(2026, 9, 5), 5, 15)
    dji = daily_capture_payload("^DJI", "close", d, pulled)
    rows = [_row(d, 40000.0, pulled, payload=dji)]
    assert read_daily_policy_n(_Conn(rows), SERIES, 1, start=d - timedelta(days=14), as_of=d,
                               as_of_ts=_at(date(2026, 9, 6), 0, 0)) == []


def test_refusal_unmarked_legacy_and_incomplete_rows_are_not_closes():
    d = date(2026, 9, 4)
    rows = [
        _row(d, 7100.0, _at(d, 13, 32), payload=None),              # legacy unmarked opening-minutes pull
        _row(d, 7090.0, _at(d, 20, 4)),                              # INCOMPLETE same-day pull
    ]
    got = read_daily_policy_n(_Conn(rows), SERIES, 1, start=d - timedelta(days=14), as_of=d,
                              as_of_ts=_at(date(2026, 9, 6), 0, 0))
    assert got == []


def test_refusal_failed_capture_is_no_close_for_date(runner, monkeypatch):
    """The 05:15Z pull FAILED (value 0): the reader excludes it; the diary says
    'no close for date' and capture_ready is False, never a 0 or a prior value."""
    from intelligence import market_diary

    monkeypatch.setattr(market_diary, "PRICES_ENABLED", True)
    d_prev, d = date(2026, 9, 3), date(2026, 9, 4)
    rows = [_captured(d_prev, 7000.0), _row(d, 0.0, _at(date(2026, 9, 5), 5, 15), status="FAILED", payload=None)]
    assert market_diary._fresh_pair(_Conn(rows), SERIES, d) is None
    assert runner.qualified_close_ready(_Engine(rows), d, now_utc=_at(date(2026, 9, 5), 5, 30)) is False


def test_refusal_missing_capture_does_not_fall_back_to_the_prior_close(runner, monkeypatch):
    from intelligence import market_diary

    monkeypatch.setattr(market_diary, "PRICES_ENABLED", True)
    d_prev, d = date(2026, 9, 3), date(2026, 9, 4)
    rows = [_captured(date(2026, 9, 2), 6990.0), _captured(d_prev, 7000.0)]  # nothing for d
    assert market_diary._fresh_pair(_Conn(rows), SERIES, d) is None
    assert runner.qualified_close_ready(_Engine(rows), d, now_utc=_at(date(2026, 9, 5), 5, 30)) is False


def test_refusal_capture_that_lands_after_the_diary_is_not_visible_to_it(runner):
    """Late 05:15Z slot (e.g. released 07:00Z behind a long job): the diary run at
    05:30Z reads as_of_ts=now, so the later capture is invisible to that run."""
    d = date(2026, 9, 4)
    late = _row(d, 7050.5, _at(date(2026, 9, 5), 7, 0))
    assert runner.qualified_close_ready(_Engine([late, _captured(date(2026, 9, 3), 7000.0)]), d,
                                        now_utc=_at(date(2026, 9, 5), 5, 30)) is False
    assert runner.qualified_close_ready(_Engine([late]), d, now_utc=_at(date(2026, 9, 5), 7, 5)) is True


@pytest.mark.parametrize("today, why", [
    (date(2026, 9, 8), "Tue after Labor Day"),
    (date(2026, 11, 27), "Fri after Thanksgiving"),
    (date(2026, 12, 26), "Sat after Christmas Friday"),
    (date(2026, 4, 4), "Sat after Good Friday"),
])
def test_refusal_post_holiday_run_skips_instead_of_rewriting_the_written_session(runner, monkeypatch, today, why):
    monkeypatch.setattr(runner, "_utc_now", lambda: _at(today, 5, 30))
    fake = MagicMock()
    monkeypatch.setattr("intelligence.market_diary.write_diary_entry", fake)
    assert runner.main(["--previous-session"], engine=_Engine([])) == 0, why
    fake.assert_not_called()


def test_refusal_skip_is_reported_in_json(runner, monkeypatch, capsys):
    monkeypatch.setattr(runner, "_utc_now", lambda: _at(date(2026, 9, 8), 5, 30))
    monkeypatch.setattr("intelligence.market_diary.write_diary_entry", MagicMock())
    runner.main(["--previous-session", "--json"], engine=_Engine([]))
    out = json.loads(capsys.readouterr().out)
    assert out["skipped"] is True and "NYSE holiday" in out["reason"]


# ── UNKNOWN ───────────────────────────────────────────────────────────────


def test_unknown_probe_failure_is_none_and_never_blocks_the_write(runner, monkeypatch):
    class _Broken:
        def connect(self):
            raise RuntimeError("db unavailable")

    assert runner.qualified_close_ready(_Broken(), date(2026, 9, 4)) is None

    monkeypatch.setattr(runner, "_utc_now", lambda: _at(date(2026, 9, 5), 5, 30))
    fake = MagicMock(return_value={"date": "2026-09-04", "thesis_accuracy": {}, "market_moves": {}})
    monkeypatch.setattr("intelligence.market_diary.write_diary_entry", fake)
    assert runner.main(["--previous-session"], engine=_Broken()) == 0
    fake.assert_called_once()


def test_unknown_capture_not_ready_still_writes_and_says_so(runner, monkeypatch, capsys):
    monkeypatch.setattr(runner, "_utc_now", lambda: _at(date(2026, 9, 5), 5, 30))
    fake = MagicMock(return_value={"date": "2026-09-04", "thesis_accuracy": {}, "market_moves": {}})
    monkeypatch.setattr("intelligence.market_diary.write_diary_entry", fake)
    assert runner.main(["--previous-session", "--json"], engine=_Engine([])) == 0
    fake.assert_called_once()
    assert json.loads(capsys.readouterr().out)["capture_ready"] is False


def test_unknown_session_policy_ticker_has_no_boundary():
    assert daily_capture_boundary("NOT-A-LISTED-TICKER", date(2026, 9, 4)) is None
    assert daily_capture_payload("NOT-A-LISTED-TICKER", "close", date(2026, 9, 4), _at(date(2026, 9, 5), 5, 15)) is None


@pytest.mark.parametrize("now, expected", [
    (_at(date(2026, 10, 21), 2, 0), date(2026, 10, 19)),   # Wed 02:00Z = Tue 22:00 EDT: Tue not eligible yet -> Mon
    (_at(date(2026, 10, 21), 5, 30), date(2026, 10, 20)),  # the timer slot: Tue
    (_at(date(2026, 11, 4), 4, 30), date(2026, 11, 2)),    # Wed 04:30Z = Tue 23:30 EST -> Mon
    (_at(date(2026, 11, 4), 5, 30), date(2026, 11, 3)),    # EST timer slot: Tue
])
def test_unknown_late_catch_up_after_utc_midnight_targets_an_eligible_session(runner, monkeypatch, now, expected):
    """Persistent=true catch-up before the New York midnight must not target the
    session whose close is not eligible yet (it would skip one and write
    'no close for date')."""
    monkeypatch.setattr(runner, "_utc_now", lambda: now)
    assert runner.previous_session_date() == expected


def test_unknown_explicit_date_wins_even_on_a_holiday_skip_day(runner, monkeypatch):
    monkeypatch.setattr(runner, "_utc_now", lambda: _at(date(2026, 9, 8), 5, 30))
    fake = MagicMock(return_value={"date": "x", "thesis_accuracy": {}, "market_moves": {}})
    monkeypatch.setattr("intelligence.market_diary.write_diary_entry", fake)
    assert runner.main(["--previous-session", "--date", "2026-09-04"], engine=_Engine([])) == 0
    assert fake.call_args.kwargs["target_date"] == date(2026, 9, 4)


# ── mutual exclusion / duplicate adoption (source-structural) ─────────────


def test_duplicate_runs_for_one_target_converge_on_one_row_key():
    src = (REPO / "intelligence" / "market_diary.py").read_text()
    assert "ON CONFLICT (date) DO UPDATE" in src  # a repeat run replaces, never duplicates


def test_duplicate_adoption_one_calendar_one_exec_one_nonblocking_lock():
    svc = (REPO / "deploy" / "systemd" / "grid-market-diary.service.template").read_text()
    tmr = (REPO / "deploy" / "systemd" / "grid-market-diary.timer.template").read_text()
    on_cal = [l for l in tmr.splitlines() if l.startswith("OnCalendar=")]
    assert on_cal == ["OnCalendar=Tue..Sat *-*-* 05:30:00 UTC"]
    exec_lines = [l for l in svc.splitlines() if l.startswith("ExecStart=")]
    assert len(exec_lines) == 1
    assert "flock -n /tmp/grid-market-diary.lock" in exec_lines[0]
    assert "--previous-session" in exec_lines[0]


def test_legacy_flow_materializer_never_writes_the_series_the_diary_reads():
    """The legacy 30-min flow timer and the in-process 23:15Z job share
    ingestion/flow_materializer.py. It reads raw_series (DARKPOOL/ETF_FLOW/FRED)
    but writes none of it and touches no YF: series or market_diary, so it cannot
    produce or disturb a qualified close."""
    src = (REPO / "ingestion" / "flow_materializer.py").read_text()
    for forbidden in ("INSERT INTO raw_series", "UPDATE raw_series", "DELETE FROM raw_series", "YF:", "market_diary"):
        assert forbidden not in src, forbidden
