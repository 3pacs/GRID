"""PR #827 source strengthening: edge cases of the VIX basis contract.

Companion to ``tests/test_regime_vix_fallback.py`` (which pins precedence,
whole-series fallback, source pinning, the trailing-gap fill and the F1/F2
review corrections). This file adds the cases that suite does not reach,
against the contract as written in ``intelligence/regime/state_vector.py``
at 7e0c0d7f and the owner decision on the trailing-gap fill:

* the fill is bounded by VIXCLS's own modeled publication lag (US federal
  business days), checked against an independent horizon oracle for every
  calendar day 2024-2026, and never uses a close dated after ``as_of``;
* leading gaps are never back-filled, interior holes stay holes except for
  the one date the next vector's trailing gap covers;
* holiday, weekend, Good Friday and timezone boundaries;
* non-finite, missing (FAILED / foreign-source) and partial backup data;
* provenance labels in memory and through the cache.

Every test is tagged in its docstring:

* ``[positive]`` the documented behaviour holds;
* ``[negative]`` the code must refuse, ignore or not fill;
* ``[unknown]`` the contract does not say; the test pins the CURRENT
  behaviour so a change is visible, and the docstring says what a contract
  decision would have to settle. These are not endorsements.

Data are invented (closed-form functions of the date). Fixture: real
``raw_series`` + ``source_catalog`` tables on in-memory SQLite, the same
shape as the companion suite; no network, no provider or ingestion import.
"""

from __future__ import annotations

import json
import math
import sqlite3
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import pytest
from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    text,
)

sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_adapter(datetime, lambda d: d.isoformat(sep=" "))

FRED_SRC = 1
CBOE_SRC = 5
OTHER_SRC = 7
HIST_START = date(2018, 1, 2)
# Backfilled rows are pulled long after every as_of used here, so they are
# visible only through the modeled next-business-day lag.
LATE_TS = datetime(2026, 10, 20, 6, 0)
AS_OF = date(2025, 6, 10)  # Tuesday
NEW_YORK = ZoneInfo("America/New_York")


# ── Fixture and helpers ──────────────────────────────────────────────────


@pytest.fixture()
def engine():
    return _make_engine()


def _make_engine():
    eng = create_engine("sqlite://")
    md = MetaData()
    Table(
        "source_catalog", md,
        Column("id", Integer, primary_key=True),
        Column("name", String, nullable=False),
    )
    Table(
        "raw_series", md,
        Column("series_id", String, nullable=False),
        Column("source_id", Integer, ForeignKey("source_catalog.id"), nullable=False),
        Column("obs_date", Date, nullable=False),
        Column("pull_timestamp", DateTime, nullable=False),
        Column("value", Float, nullable=False),
        Column("raw_payload", Text),
        Column("pull_status", String, nullable=False),
    )
    md.create_all(eng)
    with eng.begin() as c:
        c.execute(
            text("INSERT INTO source_catalog (id, name) VALUES (:id, :name)"),
            [
                {"id": FRED_SRC, "name": "FRED"},
                {"id": CBOE_SRC, "name": "CBOE"},
                {"id": OTHER_SRC, "name": "yfinance"},
            ],
        )
    return eng


def _vix(d: date) -> float:
    return 18.0 + 4.0 * math.sin(d.toordinal() / 23.0)


def _cboe(d: date) -> float:
    # Deliberately different from VIXCLS so a wrong source is visible.
    return 30.0 + 9.0 * math.cos(d.toordinal() / 11.0)


def _weekdays(start: date, end: date, skip=()) -> list[date]:
    out = []
    d = start
    while d <= end:
        if d.weekday() < 5 and d not in skip:
            out.append(d)
        d += timedelta(days=1)
    return out


def _insert(engine, rows, status: str = "SUCCESS") -> None:
    with engine.begin() as c:
        c.execute(
            text(
                "INSERT INTO raw_series (series_id, source_id, obs_date, "
                "pull_timestamp, value, raw_payload, pull_status) "
                "VALUES (:sid, :src, :d, :ts, :v, '{}', :status)"
            ),
            [{**r, "status": status} for r in rows],
        )


def _seed(engine, sid, src, fn, *, start=HIST_START, end, skip=(), ts=LATE_TS, status="SUCCESS"):
    _insert(
        engine,
        [{"sid": sid, "src": src, "d": d, "ts": ts, "v": fn(d)} for d in _weekdays(start, end, skip)],
        status,
    )


def _vix_dims(sv):
    from intelligence.regime.state_vector import DIM_NAMES

    vals = dict(zip(DIM_NAMES, sv.values))
    return vals["vix_level"], vals["vix_percentile"]


def _expected(points: dict, as_of: date) -> tuple[float, float]:
    """Independent recomputation of (vix_level, vix_percentile) from known points."""
    from intelligence.regime.state_vector import NORM_LOOKBACK_DAYS, VALUE_LOOKBACK_DAYS

    full = pd.Series(points, dtype=float).sort_index()
    full = full[full.index <= as_of]
    norm = full[full.index >= as_of - timedelta(days=NORM_LOOKBACK_DAYS)]
    window = full[full.index >= as_of - timedelta(days=VALUE_LOOKBACK_DAYS)]
    level = (float(window.iloc[-1]) - float(norm.mean())) / float(norm.std())
    tail = window.iloc[-504:]
    pct = float((tail < tail.iloc[-1]).sum() / len(tail))
    return level, pct


def _points(fn, dates) -> dict:
    return {d: fn(d) for d in dates}


def _record_reads(monkeypatch) -> list[str]:
    import intelligence.regime.state_vector as sv_mod

    seen: list[str] = []
    real = sv_mod._fetch_series

    def spy(engine, series_id, as_of, lookback_days=sv_mod.VALUE_LOOKBACK_DAYS):
        seen.append(series_id)
        return real(engine, series_id, as_of, lookback_days=lookback_days)

    monkeypatch.setattr(sv_mod, "_fetch_series", spy)
    return seen


# Independent oracle for VIXCLS's modeled release: a close dated d is public
# at the end of the next US federal business day (a weekend/holiday-dated
# close first rolls back to the prior business day). Written without
# numpy.busday_offset so it does not share the product's implementation;
# only the federal holiday list itself is the shared contract.
_FED_HOLIDAYS: set[date] | None = None


def _fed_holidays() -> set[date]:
    global _FED_HOLIDAYS
    if _FED_HOLIDAYS is None:
        from pandas.tseries.holiday import USFederalHolidayCalendar

        _FED_HOLIDAYS = {
            ts.date() for ts in USFederalHolidayCalendar().holidays("2017-01-01", "2028-12-31")
        }
    return _FED_HOLIDAYS


def _is_business(d: date) -> bool:
    return d.weekday() < 5 and d not in _fed_holidays()


def _oracle_known(d: date) -> date:
    b = d
    while not _is_business(b):
        b -= timedelta(days=1)
    n = b + timedelta(days=1)
    while not _is_business(n):
        n += timedelta(days=1)
    return n


def _oracle_horizon(as_of: date) -> date:
    d = as_of
    while d.weekday() >= 5 or _oracle_known(d) > as_of:
        d -= timedelta(days=1)
    return d


# ── Lag bound and lookahead ──────────────────────────────────────────────


def test_fill_matches_an_independent_horizon_on_every_calendar_day_2024_2026():
    """[positive] Fill window == (VIXCLS last, independent horizon], every day 2024-2026.

    For each calendar as_of (weekends, every federal holiday, Good Friday),
    VIXCLS ends 0..9 days earlier and the backup window holds a close for
    EVERY weekday through as_of itself, including holidays and the same-day
    close, i.e. strictly more than the fill may use. The fill must take
    exactly the backup dates in (last, horizon], each modeled-public by
    as_of under the oracle; the backup must not even be read when VIXCLS
    is on time.
    """
    import intelligence.regime.state_vector as sv_mod

    class _Reader(sv_mod._AsOfReader):
        def __init__(self, as_of, windows):
            super().__init__(None, as_of)
            self.reads: list[str] = []
            for sid, s in windows.items():
                self._windows[sid] = s
                self._full[sid] = s.copy()

        def window(self, series_id):
            self.reads.append(series_id)
            return super().window(series_id)

    as_of = date(2024, 1, 1)
    checked = filled_days = 0
    while as_of <= date(2026, 12, 31):
        horizon = _oracle_horizon(as_of)
        assert sv_mod._vix_publication_horizon(as_of) == horizon, as_of
        assert horizon < as_of
        last = as_of - timedelta(days=as_of.toordinal() % 10)
        while last.weekday() >= 5:
            last -= timedelta(days=1)
        vixcls = pd.Series(_points(_vix, _weekdays(last - timedelta(days=20), last)), dtype=float)
        backup = pd.Series(
            _points(_cboe, _weekdays(last - timedelta(days=20), as_of)), dtype=float,
        )
        reader = _Reader(as_of, {"VIXCLS": vixcls, "CBOE:VIX": backup})

        n = sv_mod._fill_vix_trailing_gap(reader)

        want = [d for d in backup.index if last < d <= horizon]
        got = [d for d in reader.window("VIXCLS").index if d > last]
        assert got == want, as_of
        assert [d for d in reader.full("VIXCLS").index if d > last] == want, as_of
        assert n == len(want)
        assert all(_oracle_known(d) <= as_of for d in got), as_of
        assert ("CBOE:VIX" in reader.reads) == (last < horizon), as_of
        checked += 1
        filled_days += bool(want)
        as_of += timedelta(days=1)
    assert checked == 1096 and filled_days > 300


@pytest.mark.parametrize("basis", ["CBOE:VIX", "VIXCLS+CBOE:VIX"])
def test_a_close_dated_after_as_of_is_never_used(engine, basis):
    """[negative] A backup row dated after as_of (even if 'pulled' before it) never enters.

    Corrupt-by-construction rows dated as_of+1 and as_of+5 with pull
    timestamps before as_of: neither the whole fallback nor the hybrid fill
    may see them, and the vector is identical to the one without them.
    """
    import intelligence.regime.state_vector as sv_mod

    if basis == "VIXCLS+CBOE:VIX":
        _seed(engine, "VIXCLS", FRED_SRC, _vix, end=date(2025, 6, 4))
    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe, end=date(2025, 6, 9))
    before = sv_mod.compute_state_vector(engine, AS_OF)
    _insert(engine, [
        {"sid": "CBOE:VIX", "src": CBOE_SRC, "d": AS_OF + timedelta(days=k),
         "ts": datetime(2025, 6, 1, 12, 0), "v": 99.0}
        for k in (1, 5)
    ])

    after = sv_mod.compute_state_vector(engine, AS_OF)
    reader = sv_mod._AsOfReader(engine, AS_OF)

    assert before.vix_basis == after.vix_basis == basis
    assert after.values == before.values
    assert reader.window("CBOE:VIX").index.max() <= AS_OF
    assert reader.full("CBOE:VIX").index.max() <= AS_OF


def test_weekend_as_of_does_not_take_fridays_live_close(engine):
    """[negative] Sat as_of: horizon is Thu; Friday's close, pulled live Friday, is not filled.

    The backup knows Friday's close (pull evidence), but VIXCLS's own lag
    would only publish it on Monday, so the hybrid stops at Thursday.
    """
    import intelligence.regime.state_vector as sv_mod

    as_of = date(2025, 6, 14)  # Saturday
    _seed(engine, "VIXCLS", FRED_SRC, _vix, end=date(2025, 6, 10))
    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe, end=date(2025, 6, 12))
    _insert(engine, [{"sid": "CBOE:VIX", "src": CBOE_SRC, "d": date(2025, 6, 13),
                      "ts": datetime(2025, 6, 13, 22, 0), "v": 66.0}])

    reader = sv_mod._AsOfReader(engine, as_of)
    assert date(2025, 6, 13) in reader.window("CBOE:VIX").index
    assert sv_mod._fill_vix_trailing_gap(reader) == 2
    assert list(reader.window("VIXCLS").index[-2:]) == [date(2025, 6, 11), date(2025, 6, 12)]

    sv = sv_mod.compute_state_vector(engine, as_of)
    assert sv.vix_basis == "VIXCLS+CBOE:VIX"
    points = _points(_vix, _weekdays(HIST_START, date(2025, 6, 10)))
    points.update(_points(_cboe, [date(2025, 6, 11), date(2025, 6, 12)]))
    assert _vix_dims(sv) == pytest.approx(_expected(points, as_of), rel=1e-12)


@pytest.mark.parametrize("with_vixcls", [False, True])
def test_same_day_live_close_feeds_whole_fallback_but_not_the_hybrid(engine, with_vixcls):
    """[positive] Documented asymmetry: pull evidence admits a same-day close for whole fallback only.

    Module doc: the whole-series fallback uses "the same known-at rule" as
    VIXCLS (pull evidence or modeled lag), so a close pulled the evening of
    as_of is current. The hybrid fill is additionally capped at VIXCLS's
    modeled horizon, so the same close is excluded there.
    """
    import intelligence.regime.state_vector as sv_mod

    if with_vixcls:
        _seed(engine, "VIXCLS", FRED_SRC, _vix, end=date(2025, 6, 4))
    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe, end=date(2025, 6, 9))
    _insert(engine, [{"sid": "CBOE:VIX", "src": CBOE_SRC, "d": AS_OF,
                      "ts": datetime(2025, 6, 10, 21, 30), "v": 55.0}])

    sv = sv_mod.compute_state_vector(engine, AS_OF)

    if with_vixcls:
        assert sv.vix_basis == "VIXCLS+CBOE:VIX"
        points = _points(_vix, _weekdays(HIST_START, date(2025, 6, 4)))
        points.update(_points(_cboe, _weekdays(date(2025, 6, 5), date(2025, 6, 9))))
    else:
        assert sv.vix_basis == "CBOE:VIX"
        points = _points(_cboe, _weekdays(HIST_START, date(2025, 6, 9)))
        points[AS_OF] = 55.0
    assert _vix_dims(sv) == pytest.approx(_expected(points, AS_OF), rel=1e-12)


def test_gap_close_pulled_after_as_of_fills_exactly_like_one_pulled_before(engine):
    """[positive] Documented PIT limit: modeled-lag eligibility, not first-pull custody.

    A backup close for a gap date is admitted through the modeled lag even
    when GRID pulled it long after as_of (owner-approved PIT basis, same as
    VIXCLS backfill; D2 vintage provenance is out of scope). The vector is
    the same whether the close was pulled live or 16 months later.
    """
    import intelligence.regime.state_vector as sv_mod

    gap = _weekdays(date(2025, 6, 5), date(2025, 6, 9))
    _seed(engine, "VIXCLS", FRED_SRC, _vix, end=date(2025, 6, 4))
    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe, end=date(2025, 6, 4))
    _insert(engine, [{"sid": "CBOE:VIX", "src": CBOE_SRC, "d": d,
                      "ts": datetime.combine(d, datetime.min.time()) + timedelta(hours=22),
                      "v": _cboe(d)} for d in gap])
    live = sv_mod.compute_state_vector(engine, AS_OF)

    other = _make_engine()
    _seed(other, "VIXCLS", FRED_SRC, _vix, end=date(2025, 6, 4))
    _seed(other, "CBOE:VIX", CBOE_SRC, _cboe, end=date(2025, 6, 9))  # all pulled 2026-10-20
    late = sv_mod.compute_state_vector(other, AS_OF)

    assert live.vix_basis == late.vix_basis == "VIXCLS+CBOE:VIX"
    assert late.values == live.values


def test_vixcls_landing_later_turns_a_hybrid_vector_back_into_vixcls(engine):
    """[positive] Documented limit: a later first insertion of the gap dates changes a past vector.

    VIXCLS's own late closes for 06-05..06-09, pulled long after as_of, are
    modeled-public by as_of, so a recompute uses them and the label goes
    back to plain VIXCLS (read_window_known_at docstring; review 7e0c0d7f
    "Retained behavior"). Not vintage provenance.
    """
    import intelligence.regime.state_vector as sv_mod

    _seed(engine, "VIXCLS", FRED_SRC, _vix, end=date(2025, 6, 4))
    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe, end=date(2025, 12, 31))
    hybrid = sv_mod.compute_state_vector(engine, AS_OF)
    _seed(engine, "VIXCLS", FRED_SRC, _vix, start=date(2025, 6, 5), end=date(2025, 6, 9),
          ts=datetime(2026, 10, 21, 6, 0))

    plain = sv_mod.compute_state_vector(engine, AS_OF)

    assert hybrid.vix_basis == "VIXCLS+CBOE:VIX"
    assert plain.vix_basis == "VIXCLS"
    expected = _expected(_points(_vix, _weekdays(HIST_START, date(2025, 6, 9))), AS_OF)
    assert _vix_dims(plain) == pytest.approx(expected, rel=1e-12)


# ── Leading and interior gaps ────────────────────────────────────────────


def test_late_starting_vixcls_leading_gap_is_never_backfilled(engine, monkeypatch):
    """[negative] VIXCLS starting late (but >= min_history) is used alone; earlier Cboe history never enters.

    On time at as_of, so the backup is not read at all; z-score stats come
    from VIXCLS's own short history.
    """
    import intelligence.regime.state_vector as sv_mod

    start = date(2024, 9, 2)
    _seed(engine, "VIXCLS", FRED_SRC, _vix, start=start, end=date(2025, 12, 31))
    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe, end=date(2025, 12, 31))
    seen = _record_reads(monkeypatch)

    sv = sv_mod.compute_state_vector(engine, AS_OF)

    assert sv.vix_basis == "VIXCLS"
    assert "CBOE:VIX" not in seen
    expected = _expected(_points(_vix, _weekdays(start, date(2025, 6, 9))), AS_OF)
    assert _vix_dims(sv) == pytest.approx(expected, rel=1e-12)


def test_late_starting_vixcls_with_a_trailing_gap_fills_only_the_tail(engine):
    """[negative] With a leading AND a trailing gap, only the trailing gap is filled."""
    import intelligence.regime.state_vector as sv_mod

    start = date(2024, 9, 2)
    _seed(engine, "VIXCLS", FRED_SRC, _vix, start=start, end=date(2025, 6, 4))
    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe, end=date(2025, 12, 31))
    reader = sv_mod._AsOfReader(engine, AS_OF)

    assert sv_mod._fill_vix_trailing_gap(reader) == 3
    assert reader.full("VIXCLS").index[0] == start
    assert reader.window("VIXCLS").index[0] == start

    sv = sv_mod.compute_state_vector(engine, AS_OF)
    assert sv.vix_basis == "VIXCLS+CBOE:VIX"
    points = _points(_vix, _weekdays(start, date(2025, 6, 4)))
    points.update(_points(_cboe, _weekdays(date(2025, 6, 5), date(2025, 6, 9))))
    assert _vix_dims(sv) == pytest.approx(_expected(points, AS_OF), rel=1e-12)


@pytest.mark.parametrize(("n_vixcls", "basis"), [(100, "VIXCLS+CBOE:VIX"), (99, "CBOE:VIX")])
def test_min_history_is_judged_before_the_fill(engine, n_vixcls, basis):
    """[positive] 100 late VIXCLS closes -> hybrid; 99 -> whole Cboe series, never 99 + fill.

    The resolver decides on VIXCLS's own known window; a trailing fill that
    would lift 99 closes past min_history must not rescue a too-short
    VIXCLS into a hybrid (no splicing of a short primary).
    """
    import intelligence.regime.state_vector as sv_mod

    vix_dates = _weekdays(date(2024, 6, 3), date(2025, 6, 4))[-n_vixcls:]
    _insert(engine, [{"sid": "VIXCLS", "src": FRED_SRC, "d": d, "ts": LATE_TS, "v": _vix(d)}
                     for d in vix_dates])
    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe, end=date(2025, 12, 31))

    sv = sv_mod.compute_state_vector(engine, AS_OF)

    assert sv.vix_basis == basis
    if basis == "CBOE:VIX":
        points = _points(_cboe, _weekdays(HIST_START, date(2025, 6, 9)))
    else:
        points = _points(_vix, vix_dates)
        points.update(_points(_cboe, _weekdays(date(2025, 6, 5), date(2025, 6, 9))))
    assert _vix_dims(sv) == pytest.approx(_expected(points, AS_OF), rel=1e-12)


def test_interior_hole_is_filled_only_for_the_next_dates_vector(engine, monkeypatch):
    """[positive] A one-day VIXCLS hole is the trailing gap for exactly one as_of.

    Same shape as griddb 2026-07-28: VIXCLS lacks Tue 2025-07-29. as_of Wed
    07-30 fills 07-29 from Cboe (hybrid); as_of Thu 07-31 sees VIXCLS 07-30,
    is on time, reads no backup, and 07-29 stays a hole.
    """
    import intelligence.regime.state_vector as sv_mod

    hole = date(2025, 7, 29)
    _seed(engine, "VIXCLS", FRED_SRC, _vix, end=date(2025, 8, 29), skip={hole})
    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe, end=date(2025, 8, 29))

    wed = sv_mod.compute_state_vector(engine, date(2025, 7, 30))
    assert wed.vix_basis == "VIXCLS+CBOE:VIX"
    points = _points(_vix, _weekdays(HIST_START, date(2025, 7, 28)))
    points[hole] = _cboe(hole)
    assert _vix_dims(wed) == pytest.approx(_expected(points, date(2025, 7, 30)), rel=1e-12)

    seen = _record_reads(monkeypatch)
    thu = sv_mod.compute_state_vector(engine, date(2025, 7, 31))
    assert thu.vix_basis == "VIXCLS"
    assert "CBOE:VIX" not in seen
    points = _points(_vix, _weekdays(HIST_START, date(2025, 7, 30), skip={hole}))
    assert _vix_dims(thu) == pytest.approx(_expected(points, date(2025, 7, 31)), rel=1e-12)


def test_interior_hole_is_not_filled_alongside_a_trailing_fill(engine):
    """[negative] An older interior hole stays a hole while the trailing gap is filled."""
    import intelligence.regime.state_vector as sv_mod

    hole = date(2025, 6, 2)
    _seed(engine, "VIXCLS", FRED_SRC, _vix, end=date(2025, 6, 4), skip={hole})
    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe, end=date(2025, 12, 31))
    reader = sv_mod._AsOfReader(engine, AS_OF)

    assert sv_mod._fill_vix_trailing_gap(reader) == 3
    for series in (reader.full("VIXCLS"), reader.window("VIXCLS")):
        assert hole not in series.index
        assert series.index[-1] == date(2025, 6, 9)


# ── Holiday, session and timezone boundaries ─────────────────────────────


def test_good_friday_2026_reads_the_backup_and_fills_nothing(engine, monkeypatch):
    """[positive] as_of Mon 2026-04-06: horizon is Good Friday 04-03 (not a federal holiday).

    VIXCLS ends Thu 04-02 (on time); Cboe has no 04-03 close (exchange
    closed). The backup is read, nothing is filled, and the vector and its
    label are exactly the plain VIXCLS ones (native validation 2026-10-08).
    """
    import intelligence.regime.state_vector as sv_mod

    as_of, good_friday = date(2026, 4, 6), date(2026, 4, 3)
    assert _oracle_horizon(as_of) == good_friday
    _seed(engine, "VIXCLS", FRED_SRC, _vix, end=date(2026, 4, 2))
    plain = sv_mod.compute_state_vector(engine, as_of)
    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe, end=date(2026, 4, 30), skip={good_friday})
    seen = _record_reads(monkeypatch)

    sv = sv_mod.compute_state_vector(engine, as_of)

    assert "CBOE:VIX" in seen
    assert sv.vix_basis == plain.vix_basis == "VIXCLS"
    assert sv.values == plain.values
    assert sv.stale_dimensions == plain.stale_dimensions


def test_thanksgiving_2025_holiday_horizon_then_half_day_fill(engine):
    """[positive] Fri 11-28 horizon is the holiday itself (nothing to fill); Mon 12-01 fills the half day.

    VIXCLS stops Wed 11-26. At as_of 11-28 the horizon is Thanksgiving 11-27
    (VIXCLS 11-26 publishes 11-28), Cboe has no close for it, so the vector
    is plain VIXCLS. At as_of 12-01 the horizon is the half-day session
    11-28, which Cboe has: exactly that one date is filled.
    """
    import intelligence.regime.state_vector as sv_mod

    thanksgiving = date(2025, 11, 27)
    _seed(engine, "VIXCLS", FRED_SRC, _vix, end=date(2025, 11, 26))
    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe, end=date(2025, 12, 31), skip={thanksgiving})

    fri = sv_mod.compute_state_vector(engine, date(2025, 11, 28))
    assert _oracle_horizon(date(2025, 11, 28)) == thanksgiving
    assert fri.vix_basis == "VIXCLS"

    reader = sv_mod._AsOfReader(engine, date(2025, 12, 1))
    assert sv_mod._fill_vix_trailing_gap(reader) == 1
    assert reader.window("VIXCLS").index[-1] == date(2025, 11, 28)
    mon = sv_mod.compute_state_vector(engine, date(2025, 12, 1))
    assert mon.vix_basis == "VIXCLS+CBOE:VIX"
    points = _points(_vix, _weekdays(HIST_START, date(2025, 11, 26)))
    points[date(2025, 11, 28)] = _cboe(date(2025, 11, 28))
    assert _vix_dims(mon) == pytest.approx(_expected(points, date(2025, 12, 1)), rel=1e-12)


def test_market_open_federal_holiday_is_fillable(engine):
    """[positive] Columbus Day 2025-10-13 (federal holiday, exchanges open) can be the filled date.

    Horizon at Tue 10-14 is the holiday (its close rolls back to Fri and
    publishes Tue); VIXCLS stops Fri 10-10, Cboe has a 10-13 close.
    """
    import intelligence.regime.state_vector as sv_mod

    columbus = date(2025, 10, 13)
    assert columbus in _fed_holidays()
    assert _oracle_horizon(date(2025, 10, 14)) == columbus
    _seed(engine, "VIXCLS", FRED_SRC, _vix, end=date(2025, 10, 10))
    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe, end=date(2025, 10, 31))

    sv = sv_mod.compute_state_vector(engine, date(2025, 10, 14))

    assert sv.vix_basis == "VIXCLS+CBOE:VIX"
    points = _points(_vix, _weekdays(HIST_START, date(2025, 10, 10)))
    points[columbus] = _cboe(columbus)
    assert _vix_dims(sv) == pytest.approx(_expected(points, date(2025, 10, 14)), rel=1e-12)


@pytest.mark.parametrize(
    ("as_of", "vix_end", "odd_date"),
    [
        (date(2025, 6, 10), date(2025, 6, 4), date(2025, 6, 7)),  # Saturday-dated backup row
        (date(2026, 4, 6), date(2026, 4, 2), date(2026, 4, 3)),   # Good-Friday-dated backup row
    ],
)
def test_backup_row_dated_on_a_non_session_day_inside_the_gap_is_filled(engine, as_of, vix_end, odd_date):
    """[unknown] No exchange calendar: a weekend/Good-Friday-dated Cboe row in the gap is filled.

    The fill checks only (last, horizon] and finiteness, not whether the
    date was a trading session. Real Cboe history has no such rows, and
    VIXCLS itself would be read the same way, so this pins current
    behaviour. Contract question: should the fill (or the reader) reject
    non-session dates? Not decided by R4 or the owner decision.
    """
    import intelligence.regime.state_vector as sv_mod

    _seed(engine, "VIXCLS", FRED_SRC, _vix, end=vix_end)
    _insert(engine, [{"sid": "CBOE:VIX", "src": CBOE_SRC, "d": odd_date, "ts": LATE_TS, "v": 50.0}])
    reader = sv_mod._AsOfReader(engine, as_of)

    assert sv_mod._fill_vix_trailing_gap(reader) == 1
    assert reader.window("VIXCLS").index[-1] == odd_date
    assert sv_mod.compute_state_vector(engine, as_of).vix_basis == "VIXCLS+CBOE:VIX"


@pytest.mark.parametrize(
    ("pulled", "visible"),
    [
        (datetime(2025, 6, 10, 16, 30, tzinfo=NEW_YORK), True),    # 20:30Z on as_of
        (datetime(2025, 6, 10, 20, 30, tzinfo=NEW_YORK), False),   # 00:30Z the next UTC day
        (datetime(2025, 6, 10, 20, 30), True),                     # naive = UTC
        (datetime(2025, 6, 11, 0, 30, tzinfo=timezone.utc), False),
    ],
)
def test_pull_timestamp_is_judged_on_its_utc_date(engine, pulled, visible):
    """[positive] as_of is the end of a UTC day: an evening-NY pull after 00:00Z is the next day.

    Whole fallback (VIXCLS absent), as_of close pulled live. A tz-aware
    timestamp is converted to UTC; a naive one is UTC (griddb runs Etc/UTC).
    """
    import intelligence.regime.state_vector as sv_mod

    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe, end=date(2025, 6, 9))
    _insert(engine, [{"sid": "CBOE:VIX", "src": CBOE_SRC, "d": AS_OF, "ts": pulled, "v": 55.0}])

    window = sv_mod._AsOfReader(engine, AS_OF).window("CBOE:VIX")
    assert (window.index[-1] == AS_OF) is visible

    sv = sv_mod.compute_state_vector(engine, AS_OF)
    assert sv.vix_basis == "CBOE:VIX"
    points = _points(_cboe, _weekdays(HIST_START, date(2025, 6, 9)))
    if visible:
        points[AS_OF] = 55.0
    assert _vix_dims(sv) == pytest.approx(_expected(points, AS_OF), rel=1e-12)


def test_datetime_as_of_is_outside_the_accepted_input(engine):
    """[unknown] ``as_of`` is typed ``date``; a ``datetime`` raises TypeError (pre-existing).

    The unchanged known-at reader compares obs dates with as_of and Python
    refuses date-vs-datetime comparison, so the whole computation raises
    before any VIX choice (base 988280b8 behaves the same; #827 adds no
    datetime handling). The horizon helper also raises on a datetime.
    Contract question: should callers be guaranteed a ``date`` (status quo)
    or should the function coerce? Pinned so a silent coercion is visible.
    """
    import intelligence.regime.state_vector as sv_mod

    _seed(engine, "VIXCLS", FRED_SRC, _vix, end=date(2025, 6, 4))
    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe, end=date(2025, 6, 9))

    with pytest.raises(TypeError):
        sv_mod.compute_state_vector(engine, datetime(2025, 6, 10, 12, 0))
    with pytest.raises(TypeError):
        sv_mod._vix_publication_horizon(datetime(2025, 6, 10, 12, 0))


# ── Non-finite, missing and partial data ─────────────────────────────────


def test_failed_status_backup_rows_do_not_fill(engine):
    """[negative] Cboe rows for the gap dates with pull_status FAILED are not observations."""
    import intelligence.regime.state_vector as sv_mod

    _seed(engine, "VIXCLS", FRED_SRC, _vix, end=date(2025, 6, 4))
    plain = sv_mod.compute_state_vector(engine, AS_OF)
    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe, start=date(2025, 6, 5), end=date(2025, 6, 9),
          status="FAILED")

    sv = sv_mod.compute_state_vector(engine, AS_OF)

    assert sv.vix_basis == "VIXCLS"
    assert sv.values == plain.values


def test_foreign_source_backup_rows_in_the_gap_do_not_fill(engine):
    """[negative] CBOE:VIX rows written under another source never fill VIXCLS's gap."""
    import intelligence.regime.state_vector as sv_mod

    _seed(engine, "VIXCLS", FRED_SRC, _vix, end=date(2025, 6, 4))
    plain = sv_mod.compute_state_vector(engine, AS_OF)
    _seed(engine, "CBOE:VIX", OTHER_SRC, _cboe, start=date(2025, 6, 5), end=date(2025, 6, 9))

    sv = sv_mod.compute_state_vector(engine, AS_OF)

    assert sv.vix_basis == "VIXCLS"
    assert sv.values == plain.values


def test_partial_backup_tail_fills_what_exists(engine):
    """[positive] Cboe has only the first gap date: one date filled, label hybrid, rest left open."""
    import intelligence.regime.state_vector as sv_mod

    _seed(engine, "VIXCLS", FRED_SRC, _vix, end=date(2025, 6, 4))
    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe, end=date(2025, 6, 5))
    reader = sv_mod._AsOfReader(engine, AS_OF)

    assert sv_mod._fill_vix_trailing_gap(reader) == 1
    assert reader.window("VIXCLS").index[-1] == date(2025, 6, 5)

    sv = sv_mod.compute_state_vector(engine, AS_OF)
    assert sv.vix_basis == "VIXCLS+CBOE:VIX"
    assert "vix_level" not in sv.stale_dimensions
    points = _points(_vix, _weekdays(HIST_START, date(2025, 6, 4)))
    points[date(2025, 6, 5)] = _cboe(date(2025, 6, 5))
    assert _vix_dims(sv) == pytest.approx(_expected(points, AS_OF), rel=1e-12)


def test_stale_vixcls_with_a_stale_backup_is_hybrid_and_still_stale(engine):
    """[positive] Fill moves the stale age to the last filled date; still > 30 days -> both VIX dims stale.

    Without any backup the same VIXCLS is plain and stale (pre-R4 behaviour).
    """
    import intelligence.regime.state_vector as sv_mod

    _seed(engine, "VIXCLS", FRED_SRC, _vix, end=date(2025, 3, 31))
    plain = sv_mod.compute_state_vector(engine, AS_OF)
    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe, end=date(2025, 4, 30))

    sv = sv_mod.compute_state_vector(engine, AS_OF)

    assert plain.vix_basis == "VIXCLS"
    assert {"vix_level", "vix_percentile"} <= set(plain.stale_dimensions)
    assert sv.vix_basis == "VIXCLS+CBOE:VIX"
    assert {"vix_level", "vix_percentile"} <= set(sv.stale_dimensions)  # 41 days > 30
    points = _points(_vix, _weekdays(HIST_START, date(2025, 3, 31)))
    points.update(_points(_cboe, _weekdays(date(2025, 4, 1), date(2025, 4, 30))))
    assert _vix_dims(sv) == pytest.approx(_expected(points, AS_OF), rel=1e-12)


def test_long_stale_vixcls_gap_is_filled_without_a_length_bound(engine):
    """[unknown] A 10-week VIXCLS outage is filled end to end from Cboe; no max-gap rule exists.

    VIXCLS stops 2025-03-31 but still has >= min_history known closes, so it
    stays the basis and its whole trailing gap (49 weekdays) comes from
    Cboe; the VIX dims are then not stale and carry the hybrid label.
    Contract question: should a gap this long switch to the whole-series
    fallback (Cboe-normalized) or be flagged, rather than fill a VIXCLS
    z-score history? R4 / the owner decision approve the fill without a
    length bound; this pins that.
    """
    import intelligence.regime.state_vector as sv_mod

    _seed(engine, "VIXCLS", FRED_SRC, _vix, end=date(2025, 3, 31))
    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe, end=date(2025, 12, 31))
    reader = sv_mod._AsOfReader(engine, AS_OF)

    gap = _weekdays(date(2025, 4, 1), date(2025, 6, 9))
    assert sv_mod._fill_vix_trailing_gap(reader) == len(gap) == 50
    sv = sv_mod.compute_state_vector(engine, AS_OF)
    assert sv.vix_basis == "VIXCLS+CBOE:VIX"
    assert "vix_level" not in sv.stale_dimensions
    assert "vix_percentile" not in sv.stale_dimensions


def _inject_nan_backup(monkeypatch, nan_dates):
    """Wrap the known-at reader so CBOE:VIX also returns NaN observations (any route)."""
    import intelligence.regime.state_vector as sv_mod
    from store.observations import Observation

    real = sv_mod.read_window_known_at

    def wrapped(conn, series_id, **kw):
        out = list(real(conn, series_id, **kw))
        if series_id == "CBOE:VIX":
            start = kw.get("start")
            out += [
                Observation(series_id, d, float("nan"), LATE_TS, "CBOE", d, "modeled_lag")
                for d in nan_dates
                if (start is None or d >= start) and d <= kw["as_of"]
            ]
            out.sort(key=lambda o: o.obs_date)
        return out

    monkeypatch.setattr(sv_mod, "read_window_known_at", wrapped)


def test_nan_backup_closes_never_enter_the_whole_fallback(engine, monkeypatch):
    """[positive] F2 on the whole-fallback path: NaN Cboe closes are dropped from BOTH reads.

    SQLite cannot store NaN in this NOT NULL schema, so NaN observations
    are injected at the reader boundary: the as_of-1 close and one old close
    inside the 10000-day normalization window only. Value, percentile,
    z-score stats and stale age must equal the finite-only computation.
    """
    import intelligence.regime.state_vector as sv_mod

    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe, end=date(2025, 6, 6))
    clean = sv_mod.compute_state_vector(engine, AS_OF)
    nan_dates = [date(2025, 6, 9), date(2010, 3, 1)]
    _inject_nan_backup(monkeypatch, nan_dates)

    reader = sv_mod._AsOfReader(engine, AS_OF)
    for series in (reader.window("CBOE:VIX"), reader.full("CBOE:VIX")):
        assert series.notna().all()
        assert not set(nan_dates) & set(series.index)
    assert reader.window("CBOE:VIX").index[-1] == date(2025, 6, 6)

    sv = sv_mod.compute_state_vector(engine, AS_OF)
    assert sv.vix_basis == clean.vix_basis == "CBOE:VIX"
    assert sv.values == clean.values
    assert sv.stale_dimensions == clean.stale_dimensions


def test_old_infinite_backup_close_stays_out_of_the_fallback_zscore_stats(engine):
    """[positive] F2 at the normalization read: an inf older than the value window is excluded too.

    The +inf is dated 2012 (inside the 10000-day normalization window,
    outside the 2520-day value window), so only the full() read sees it.
    """
    import intelligence.regime.state_vector as sv_mod

    old = date(2012, 5, 1)
    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe, start=date(2010, 1, 4), end=date(2025, 6, 9), skip={old})
    clean = sv_mod.compute_state_vector(engine, AS_OF)
    _insert(engine, [{"sid": "CBOE:VIX", "src": CBOE_SRC, "d": old, "ts": LATE_TS, "v": float("inf")}])

    sv = sv_mod.compute_state_vector(engine, AS_OF)

    assert old < AS_OF - timedelta(days=sv_mod.VALUE_LOOKBACK_DAYS)
    assert old not in sv_mod._AsOfReader(engine, AS_OF).full("CBOE:VIX").index
    assert sv.vix_basis == "CBOE:VIX"
    assert sv.values == clean.values
    level, _pct = _vix_dims(sv)
    assert math.isfinite(level)
    points = _points(_cboe, _weekdays(date(2010, 1, 4), date(2025, 6, 9), skip={old}))
    assert _vix_dims(sv) == pytest.approx(_expected(points, AS_OF), rel=1e-12)


def test_primary_vixcls_nonfinite_close_is_left_as_before(engine, monkeypatch):
    """[unknown] #827 hardens only the backup: a +inf VIXCLS close still yields a non-finite level.

    VIXCLS (primary) keeps the reader's unchanged behaviour (state_vector
    FINITE_ONLY_SERIES comment; review of 7e0c0d7f "general primary
    hardening is not claimed"). With +inf on its on-time last date the
    backup is not read and vix_level is NaN (mean inf, std NaN), which
    still counts as present for completeness. Contract question: primary
    non-finite handling, a separate change from R4.
    """
    import intelligence.regime.state_vector as sv_mod

    _seed(engine, "VIXCLS", FRED_SRC, _vix, end=date(2025, 6, 6))
    _insert(engine, [{"sid": "VIXCLS", "src": FRED_SRC, "d": date(2025, 6, 9), "ts": LATE_TS,
                      "v": float("inf")}])
    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe, end=date(2025, 12, 31))
    seen = _record_reads(monkeypatch)

    sv = sv_mod.compute_state_vector(engine, AS_OF)

    level, pct = _vix_dims(sv)
    assert sv.vix_basis == "VIXCLS"
    assert "CBOE:VIX" not in seen
    assert level is not None and math.isnan(level)
    assert pct is not None and math.isfinite(pct)


# ── Provenance and failure labels ────────────────────────────────────────


@pytest.mark.parametrize("basis", ["VIXCLS", "VIXCLS+CBOE:VIX", "CBOE:VIX", None])
def test_every_vix_basis_round_trips_through_the_cache(monkeypatch, basis):
    """[positive] All four label states survive to_dict, the cache writer and the loader.

    None writes no ``__vix_basis__`` key (it reads back None, like a
    pre-R4 row). Writer is exercised with a recording engine double; native
    PostgreSQL JSONB is out of scope.
    """
    import intelligence.regime.state_vector as sv_mod

    sv = sv_mod.StateVector(
        as_of_date=AS_OF,
        values=tuple([0.25] * len(sv_mod.DIM_NAMES)),
        completeness=1.0,
        stale_dimensions=("vix_level",),
        price_basis=None,
        vix_basis=basis,
    )
    assert sv.to_dict()["vix_basis"] == basis

    written: dict = {}

    class _Conn:
        def execute(self, _stmt, params):
            written.update(params)

    class _Begin:
        def __enter__(self):
            return _Conn()

        def __exit__(self, *exc):
            return False

    class _Engine:
        def begin(self):
            return _Begin()

    monkeypatch.setattr(sv_mod, "_ensure_cache_table", lambda _e: None)
    sv_mod.cache_state_vector(_Engine(), sv)
    vec = json.loads(written["vec"])
    assert ("__vix_basis__" in vec) is (basis is not None)
    assert vec.get("__vix_basis__") == basis
    back = sv_mod._row_to_state_vector((AS_OF, vec, 1.0, ["vix_level"]), cached=True)
    assert back.vix_basis == basis
    assert back.values == sv.values


def test_persistent_vixcls_window_failure_leaves_label_and_dims_unavailable(engine, monkeypatch):
    """[positive] VIXCLS value-window reads always fail: label None and both VIX dims None.

    The backup is present and usable, but a failed primary read is not
    evidence the primary is absent, so there is no fallback and no fill.
    """
    import intelligence.regime.state_vector as sv_mod

    _seed(engine, "VIXCLS", FRED_SRC, _vix, end=date(2025, 6, 4))
    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe, end=date(2025, 12, 31))
    real = sv_mod._fetch_series

    def failing(engine, sid, as_of, lookback_days=sv_mod.VALUE_LOOKBACK_DAYS):
        if sid == "VIXCLS" and lookback_days == sv_mod.VALUE_LOOKBACK_DAYS:
            raise RuntimeError("synthetic persistent VIXCLS window failure")
        return real(engine, sid, as_of, lookback_days)

    monkeypatch.setattr(sv_mod, "_fetch_series", failing)
    sv = sv_mod.compute_state_vector(engine, AS_OF)

    assert sv.vix_basis is None
    assert _vix_dims(sv) == (None, None)


def test_whole_fallback_normalization_read_failure_aborts_the_vector(engine, monkeypatch):
    """[unknown] A failing Cboe 10000-day read in whole fallback raises out of compute_state_vector.

    VIXCLS is absent; the Cboe value window reads fine, its normalization
    read raises. Pre-R4 this vector (no VIX source) would have been returned
    with the VIX dims None; now the whole vector raises, whereas the hybrid
    path swallows backup-read failures. Reviews of ae990424 / 7e0c0d7f list
    this as "not newly exercised ... universal graceful degradation is not
    claimed". Contract question: should an optional backup normalization
    failure degrade to VIX dims None (label None) instead of aborting?
    Pinned as current behaviour, not endorsed.
    """
    import intelligence.regime.state_vector as sv_mod

    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe, end=date(2025, 12, 31))
    real = sv_mod._fetch_series

    def failing(engine, sid, as_of, lookback_days=sv_mod.VALUE_LOOKBACK_DAYS):
        if sid == "CBOE:VIX" and lookback_days == sv_mod.NORM_LOOKBACK_DAYS:
            raise RuntimeError("synthetic Cboe normalization read failure")
        return real(engine, sid, as_of, lookback_days)

    monkeypatch.setattr(sv_mod, "_fetch_series", failing)
    with pytest.raises(RuntimeError, match="normalization read failure"):
        sv_mod.compute_state_vector(engine, AS_OF)


def test_transient_vix_basis_failure_still_labels_the_vixcls_vector(engine, monkeypatch):
    """[positive] Regression: a one-shot failure resolving the basis must not leave a VIXCLS vector unlabelled.

    Counterexample on 7e0c0d7f: the FIRST VIXCLS value-window read (inside
    ``_resolve_vix_series``) raises once; the memo is not set, the dimension
    loop re-reads VIXCLS successfully and computes both VIX dims from plain
    VIXCLS, yet ``vix_basis`` stayed None, which the field documents as
    "neither had enough known history" and which the cache writes as no
    ``__vix_basis__`` at all (indistinguishable from a pre-R4 row). The
    vector itself must stay the plain VIXCLS one (no gap fill is attempted
    after a failed resolve); only the label is corrected to "VIXCLS".
    """
    import intelligence.regime.state_vector as sv_mod

    _seed(engine, "VIXCLS", FRED_SRC, _vix, end=date(2025, 6, 4))
    plain = sv_mod.compute_state_vector(engine, AS_OF)  # no backup yet: plain VIXCLS
    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe, end=date(2025, 12, 31))
    real = sv_mod._fetch_series
    failures = 0

    def flaky(engine, sid, as_of, lookback_days=sv_mod.VALUE_LOOKBACK_DAYS):
        nonlocal failures
        if sid == "VIXCLS" and lookback_days == sv_mod.VALUE_LOOKBACK_DAYS and failures == 0:
            failures += 1
            raise RuntimeError("synthetic one-shot VIXCLS window failure")
        return real(engine, sid, as_of, lookback_days)

    monkeypatch.setattr(sv_mod, "_fetch_series", flaky)
    sv = sv_mod.compute_state_vector(engine, AS_OF)

    assert failures == 1
    assert sv.values == plain.values  # plain VIXCLS dims, no fill
    assert all(v is not None for v in _vix_dims(sv))
    assert sv.vix_basis == plain.vix_basis == "VIXCLS"
