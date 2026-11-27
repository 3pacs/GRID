"""The short ETF flow run writes what the wide run writes, from the floor on.

Codex review 6086917359 on the morning slot: the short run starts 14 calendar
days back and ``_pull_etf_flows`` fetched 30 calendar days before that for the
rolling windows. The windows are counted in ROWS (bars the provider returned):
the 20-day sum of a date needs the 19 rows before it, the acceleration (a
5-row sum, then its change over 5 rows) needs 9. Thirty calendar days do not
promise 19 rows. Before 2027-01-19 they hold 18 sessions (Christmas, New
Year's Day and Martin Luther King Jr. Day fall inside), so a short run on
2027-02-02 left that date's 20-day value out while the wide run wrote it, and
said nothing. A bar the provider does not return shortens it further.

The fetch now goes back 60 calendar days, the rows it brought before the first
date are counted, and an ETF with fewer than 19 is fetched again from where
the wide run starts. These cases run the two real entry points,
``pull_etf_recent`` (short) and ``pull_etf_only`` (wide), each on its own
database, against one fake provider, and compare every row from the floor on
for all three series. Controls switch each of the two measures off and show
the difference the comparison then finds.

Replaced: the provider (a fixed bar history, sliced by the request's start)
and New York's date. The fake provider returns bars up to the case's New York
date whatever ``end`` the writer asks for: the writer's ``end`` is the host's
date, which in production is never before New York's.
"""

from __future__ import annotations

import json
import math
import sys
import types
from datetime import date, timedelta

import pandas as pd
import pytest
from sqlalchemy import text

from ingestion.altdata import institutional_flows as inst
from tests import thirteenf_holdings_support as s

TICKERS = ["SPY", "TLT", "XLE"]
METRICS = ("5d", "20d", "accel")
HISTORY_FROM = date(2019, 10, 1)
# The exchange's full-day closures in the months the cases look at. Earlier
# years carry none: the comparison needs a history, not a faithful 2021.
HOLIDAYS = {
    date(2026, 9, 7), date(2026, 11, 26), date(2026, 12, 25),
    date(2027, 1, 1), date(2027, 1, 18), date(2027, 2, 15),
}


def _sessions(first: date, last: date) -> list[date]:
    days = pd.bdate_range(pd.Timestamp(first), pd.Timestamp(last))
    return [d.date() for d in days if d.date() not in HOLIDAYS]


def _history(today: date, missing: dict[str, set[date]] | None = None,
             cents: bool = False) -> dict[str, pd.DataFrame]:
    """Every ETF's bars up to ``today`` (its own bar is the session still open).

    Closes are whole dollars and volumes whole shares, so every rolling sum is
    exact in a float and the two runs can be compared with ``==``. ``cents``
    gives closes with cents, for the case compared at the writer's tolerance.
    ``missing`` names bars the provider does not have for an ETF.
    """
    out = {}
    for offset, ticker in enumerate(TICKERS):
        days = [d for d in _sessions(HISTORY_FROM, today)
                if d not in (missing or {}).get(ticker, set())]
        closes = [90.0 + offset * 40 + (d.toordinal() * 7) % 23 + (0.37 if cents else 0.0) for d in days]
        volumes = [1_000_000 + (d.toordinal() * 7919 + offset * 101) % 900_000 for d in days]
        out[ticker] = pd.DataFrame({"Close": closes, "Volume": volumes},
                                   index=pd.DatetimeIndex([pd.Timestamp(d) for d in days]))
    return out


class _Provider:
    """yfinance.download over a fixed history; remembers what was asked."""

    def __init__(self, history: dict[str, pd.DataFrame], fail_from_before: date | None = None):
        self.history = history
        self.fail_from_before = fail_from_before
        self.asked: list[tuple[str, date]] = []

    def download(self, ticker, start, end, progress=False, auto_adjust=True):
        first = date.fromisoformat(start)
        self.asked.append((ticker, first))
        if self.fail_from_before is not None and first < self.fail_from_before:
            return pd.DataFrame()
        frame = self.history[ticker]
        return frame[frame.index >= pd.Timestamp(first)].copy()


def _run(monkeypatch, today: date, provider: _Provider, which: str, engine=None):
    """Run the short or the wide entry point on its own database."""
    monkeypatch.setattr(inst, "ETF_FLOW_TICKERS", TICKERS)
    monkeypatch.setattr(inst.time, "sleep", lambda *_a: None)
    monkeypatch.setattr(inst, "_new_york_today", lambda: today)
    monkeypatch.setitem(sys.modules, "yfinance", types.SimpleNamespace(download=provider.download))
    engine = engine or s.make_sqlite_engine()
    puller = inst.InstitutionalFlowsPuller(engine)
    results = puller.pull_etf_recent() if which == "short" else puller.pull_etf_only()
    return engine, results


def _floor(today: date) -> date:
    return today - timedelta(days=inst._ETF_REVISION_DAYS)


def _newest_from(engine, floor: date) -> dict[tuple[str, date], tuple[float, float | None]]:
    """(series, date) -> (newest value, the value it revised) for dates >= floor."""
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT series_id, obs_date, value, raw_payload FROM raw_series "
            "WHERE series_id LIKE 'ETF_FLOW:%' ORDER BY id")).fetchall()
    out = {}
    for series_id, obs_date, value, payload in rows:
        if isinstance(obs_date, str):
            obs_date = date.fromisoformat(obs_date[:10])
        if obs_date < floor:
            continue
        payload = json.loads(payload) if isinstance(payload, str) else payload
        out[(series_id, obs_date)] = (float(value), payload.get("revises_value"))
    return out


def _every_series_date(today: date, history: dict[str, pd.DataFrame]) -> set[tuple[str, date]]:
    """Each ETF's finished bars from the floor on, for all three series."""
    floor = _floor(today)
    return {
        (f"ETF_FLOW:{ticker}:{metric}", stamp.date())
        for ticker, frame in history.items()
        for stamp in frame.index if floor <= stamp.date() < today
        for metric in METRICS
    }


def _widened(results: list[dict]) -> set[str]:
    return {r["feature"].split(":")[1] for r in results if r.get("warmup_widened")}


# ── What the windows need ──────────────────────────────────────────────────


def test_the_windows_need_nineteen_rows_before_the_first_date():
    """20 rows end the 20-day sum; 10 end the acceleration. Not 25: the
    acceleration is the 5-row sum's change over 5 rows, not the 20-row sum's."""
    assert (inst._FLOW_WINDOW_5D, inst._FLOW_WINDOW_20D) == (5, 20)
    assert inst._ETF_WARMUP_ROWS == 19
    frame = pd.DataFrame({"v": [1.0] * 30})
    five = frame["v"].rolling(window=5, min_periods=5).sum()
    assert five.diff(periods=5).first_valid_index() == 9          # the 10th row
    assert frame["v"].rolling(window=20, min_periods=20).sum().first_valid_index() == 19


def test_thirty_calendar_days_hold_eighteen_sessions_before_2027_01_19():
    first_date = date(2027, 1, 19)
    assert len(_sessions(first_date - timedelta(days=30), first_date - timedelta(days=1))) == 18
    assert len(_sessions(first_date - timedelta(days=inst._ETF_WARMUP_CALENDAR_DAYS),
                         first_date - timedelta(days=1))) >= inst._ETF_WARMUP_ROWS


# ── Short equals wide from the floor ───────────────────────────────────────


@pytest.mark.parametrize("today, label", [
    (date(2026, 11, 28), "the review's modelled date, Thanksgiving inside the window"),
    (date(2027, 2, 2), "floor 01-19: three holidays inside the old 30-day fetch"),
    (date(2027, 1, 5), "Christmas and New Year's Day inside the window itself"),
    (date(2026, 9, 22), "floor 09-08, the day after Labor Day"),
    (date(2026, 11, 20), "an ordinary Friday, the session open"),
])
def test_the_short_run_writes_the_wide_runs_rows_from_the_floor(monkeypatch, today, label):
    history = _history(today)
    wide_engine, _ = _run(monkeypatch, today, _Provider(history), "wide")
    short_provider = _Provider(history)
    short_engine, short_results = _run(monkeypatch, today, short_provider, "short")

    wide = _newest_from(wide_engine, _floor(today))
    short = _newest_from(short_engine, _floor(today))
    assert short == wide, label
    assert set(short) == _every_series_date(today, history), label   # all three series, every finished session
    assert _widened(short_results) == set()
    assert len(short_provider.asked) == len(TICKERS)                   # one request per ETF
    # The short run wrote nothing the wide run reads as older than the floor.
    assert _newest_from(short_engine, date.min).keys() == short.keys()


def test_with_closes_in_cents_the_two_runs_agree_within_the_writers_own_tolerance(monkeypatch):
    today = date(2027, 2, 2)
    history = _history(today, cents=True)
    wide_engine, _ = _run(monkeypatch, today, _Provider(history), "wide")
    short_engine, _ = _run(monkeypatch, today, _Provider(history), "short")
    wide, short = _newest_from(wide_engine, _floor(today)), _newest_from(short_engine, _floor(today))
    assert short.keys() == wide.keys() == _every_series_date(today, history)
    for key, (value, _revised) in wide.items():
        assert math.isclose(short[key][0], value, rel_tol=inst._ETF_REVISION_REL_TOL, abs_tol=0.0), key


def test_both_runs_revise_the_same_stored_rows_to_the_same_values(monkeypatch):
    """Rows an earlier run left from partial bars, the same in both databases."""
    today = date(2027, 2, 2)
    history = _history(today)
    stale = [(series, day, 1234.0 + n) for n, (series, day) in enumerate(sorted(_every_series_date(today, history)))]
    engines = []
    for which in ("wide", "short"):
        engine = s.make_sqlite_engine()
        source_id = inst.InstitutionalFlowsPuller(engine).source_id
        with engine.begin() as conn:
            for series, day, value in stale:
                conn.execute(text(
                    "INSERT INTO raw_series (series_id, source_id, obs_date, value, raw_payload, pull_status, "
                    "pull_timestamp) VALUES (:sid, :src, :d, :v, :p, 'SUCCESS', '2026-01-01 00:00:00')"),
                    {"sid": series, "src": source_id, "d": day,
                     "v": value, "p": json.dumps({"ticker": series.split(":")[1]})})
        _run(monkeypatch, today, _Provider(history), which, engine=engine)
        engines.append(engine)
    wide, short = (_newest_from(engine, _floor(today)) for engine in engines)
    assert short == wide
    assert {key: revised for key, (_v, revised) in short.items()} == {
        (series, day): value for series, day, value in stale}


# ── Bars the provider does not have ────────────────────────────────────────


def test_one_missing_bar_in_the_warmup_changes_nothing(monkeypatch):
    today = date(2027, 2, 2)
    history = _history(today, missing={"TLT": {date(2027, 1, 12)}})
    wide_engine, _ = _run(monkeypatch, today, _Provider(history), "wide")
    short_engine, short_results = _run(monkeypatch, today, _Provider(history), "short")
    assert _newest_from(short_engine, _floor(today)) == _newest_from(wide_engine, _floor(today))
    assert _widened(short_results) == set()


def test_a_missing_bar_inside_the_window_is_the_same_gap_in_both_runs(monkeypatch):
    today = date(2027, 2, 2)
    gap = date(2027, 1, 27)
    history = _history(today, missing={"XLE": {gap}})
    wide_engine, _ = _run(monkeypatch, today, _Provider(history), "wide")
    short_engine, _ = _run(monkeypatch, today, _Provider(history), "short")
    wide, short = _newest_from(wide_engine, _floor(today)), _newest_from(short_engine, _floor(today))
    assert short == wide
    assert not any(day == gap for series, day in short if ":XLE:" in series)
    assert any(day == gap for series, day in short if ":SPY:" in series)


def _gap_leaving(today: date, rows_before_floor: int) -> set[date]:
    """Bars to drop so the 60-day fetch holds exactly that many rows before the floor."""
    floor = _floor(today)
    before = _sessions(floor - timedelta(days=inst._ETF_WARMUP_CALENDAR_DAYS), floor - timedelta(days=1))
    return set(before[: len(before) - rows_before_floor])


@pytest.mark.parametrize("rows_before_floor, expect_widened", [(19, False), (18, True), (3, True)])
def test_an_etf_short_of_rows_is_read_as_the_wide_run_reads_it(monkeypatch, rows_before_floor, expect_widened):
    """A provider gap that leaves TLT 19, 18 or 3 rows before the floor.

    With 19 the fetch is enough. With fewer, TLT alone is fetched again from
    where the wide run starts; the rows are the wide run's either way.
    """
    today = date(2026, 11, 28)
    history = _history(today, missing={"TLT": _gap_leaving(today, rows_before_floor)})
    wide_engine, _ = _run(monkeypatch, today, _Provider(history), "wide")
    provider = _Provider(history)
    short_engine, short_results = _run(monkeypatch, today, provider, "short")

    assert _newest_from(short_engine, _floor(today)) == _newest_from(wide_engine, _floor(today))
    assert _widened(short_results) == ({"TLT"} if expect_widened else set())
    asked_tlt = [first for ticker, first in provider.asked if ticker == "TLT"]
    short_start = _floor(today) - timedelta(days=inst._ETF_WARMUP_CALENDAR_DAYS)
    wide_start = inst._ETF_HISTORY_START - timedelta(days=inst._ETF_WARMUP_CALENDAR_DAYS)
    assert asked_tlt == ([short_start, wide_start] if expect_widened else [short_start])
    assert [first for ticker, first in provider.asked if ticker == "SPY"] == [short_start]


def test_when_the_second_fetch_fails_nothing_is_written_for_that_etf(monkeypatch):
    today = date(2026, 11, 28)
    history = _history(today, missing={"TLT": _gap_leaving(today, 3)})
    short_start = _floor(today) - timedelta(days=inst._ETF_WARMUP_CALENDAR_DAYS)
    engine, results = _run(monkeypatch, today, _Provider(history, fail_from_before=short_start), "short")
    written = _newest_from(engine, date.min)
    assert not any(":TLT:" in series for series, _day in written)
    assert any(":SPY:" in series for series, _day in written)
    (failed,) = [r for r in results if r["status"] == "FAILED"]
    assert failed["feature"] == "ETF_FLOW:TLT" and "no bars" in failed["error"]


def test_the_wide_run_asks_once_per_etf_and_is_never_fetched_twice(monkeypatch):
    today = date(2026, 11, 28)
    provider = _Provider(_history(today, missing={"TLT": _gap_leaving(today, 3)}))
    _engine, results = _run(monkeypatch, today, provider, "wide")
    assert len(provider.asked) == len(TICKERS)
    assert _widened(results) == set()


# ── Controls: what the comparison finds with a measure switched off ────────


def test_control_the_old_thirty_day_fetch_leaves_out_a_row_the_wide_run_writes(monkeypatch):
    """Both measures off: the fetch as it was. The comparison must see it."""
    today = date(2027, 2, 2)
    history = _history(today)
    wide_engine, _ = _run(monkeypatch, today, _Provider(history), "wide")
    monkeypatch.setattr(inst, "_ETF_WARMUP_CALENDAR_DAYS", 30)
    monkeypatch.setattr(inst, "_ETF_WARMUP_ROWS", 0)
    short_engine, short_results = _run(monkeypatch, today, _Provider(history), "short")

    wide, short = _newest_from(wide_engine, _floor(today)), _newest_from(short_engine, _floor(today))
    assert set(wide) - set(short) == {(f"ETF_FLOW:{t}:20d", date(2027, 1, 19)) for t in TICKERS}
    assert all(r["status"] == "SUCCESS" for r in short_results)       # and it said nothing


def test_control_counting_rows_alone_closes_it(monkeypatch):
    """The 30-day fetch again, the row count on: every ETF is read as the wide run reads it."""
    today = date(2027, 2, 2)
    history = _history(today)
    wide_engine, _ = _run(monkeypatch, today, _Provider(history), "wide")
    monkeypatch.setattr(inst, "_ETF_WARMUP_CALENDAR_DAYS", 30)
    short_engine, short_results = _run(monkeypatch, today, _Provider(history), "short")
    assert _newest_from(short_engine, _floor(today)) == _newest_from(wide_engine, _floor(today))
    assert _widened(short_results) == set(TICKERS)


def test_control_without_the_row_count_a_provider_gap_changes_values(monkeypatch):
    """The 60-day fetch alone does not cover a gap in the provider's bars."""
    today = date(2026, 11, 28)
    history = _history(today, missing={"TLT": _gap_leaving(today, 3)})
    wide_engine, _ = _run(monkeypatch, today, _Provider(history), "wide")
    monkeypatch.setattr(inst, "_ETF_WARMUP_ROWS", 0)
    short_engine, _ = _run(monkeypatch, today, _Provider(history), "short")
    wide, short = _newest_from(wide_engine, _floor(today)), _newest_from(short_engine, _floor(today))
    missing = set(wide) - set(short)
    assert missing and all(":TLT:" in series for series, _day in missing)
    assert {series.rsplit(":", 1)[1] for series, _day in missing} == {"5d", "20d", "accel"}


# ── The other caller of the same writer ────────────────────────────────────


def test_the_operator_ticks_seven_day_start_gets_the_same_rows(monkeypatch):
    """``pull_all`` starts ETF flows 7 days before the newest stored date and
    goes through the same fetch, so it is covered by the same count."""
    today = date(2027, 2, 2)
    history = _history(today)
    wide_engine, _ = _run(monkeypatch, today, _Provider(history), "wide")
    monkeypatch.setattr(inst, "ETF_FLOW_TICKERS", TICKERS)
    engine = s.make_sqlite_engine()
    puller = inst.InstitutionalFlowsPuller(engine)
    start = date(2027, 1, 19)                                           # 18 sessions in the old fetch
    results = puller._pull_etf_flows(start, today)
    assert _newest_from(engine, start) == _newest_from(wide_engine, start)
    assert all(r["status"] == "SUCCESS" for r in results)
