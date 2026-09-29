"""Point-in-time tests for the regime state vector (GRID-REGIME-HISTORY-REBUILD-20260929).

The 2026-09-29 history rebuild exposed two look-ahead defects:

1. Macro reads were bounded by *observation* date, so a monthly series dated
   the 1st (UNRATE, INDPRO, TCU, M2SL, UMCSENT) was visible weeks before it
   was published. Fixed by ``store.observations.read_window_known_at`` +
   ``state_vector.PUBLICATION_LAGS``.
2. Z-score mean/std were computed over full history as of the *run* date and
   cached per process, so a historical row depended on later data and on when
   (and in which process) it was computed. Fixed by PIT stats over a rolling
   window ending at ``as_of``, no process cache.

Fixture: a real ``raw_series`` + ``source_catalog`` on in-memory SQLite (same
shape as ``tests/test_regime_state_vector.py``), seeded the way griddb looks:
every historical row backfilled by one pull on 2026-03-24.
"""

from __future__ import annotations

import math
import sqlite3
from datetime import date, datetime, timedelta

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
YF_SRC = 2
BACKFILL_TS = datetime(2026, 3, 24, 6, 0, 0)  # griddb: all FRED history pulled >= this
HIST_START = date(2018, 1, 1)
HIST_END = date(2026, 3, 20)
AS_OF = date(2025, 6, 10)  # a historical as_of: pure modeled-lag path

DAILY = ("VIXCLS", "T10Y2Y", "DFF", "BAMLH0A0HYM2", "BAMLC0A0CM", "T5YIE")
MONTHLY = ("UNRATE", "INDPRO", "TCU", "M2SL", "UMCSENT")


@pytest.fixture()
def engine():
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
            [{"id": FRED_SRC, "name": "fred"}, {"id": YF_SRC, "name": "yfinance"}],
        )
    return eng


def _insert_many(engine, rows) -> None:
    with engine.begin() as c:
        c.execute(
            text(
                "INSERT INTO raw_series (series_id, source_id, obs_date, "
                "pull_timestamp, value, raw_payload, pull_status) "
                "VALUES (:sid, :src, :d, :ts, :v, '{}', 'SUCCESS')"
            ),
            rows,
        )


def _row(sid, d, v, ts=BACKFILL_TS, src=FRED_SRC) -> dict:
    return {"sid": sid, "src": src, "d": d, "ts": ts, "v": v}


def _bdays(start: date, end: date):
    d = start
    while d <= end:
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)


def _months(start: date, end: date):
    y, m = start.year, start.month
    while date(y, m, 1) <= end:
        yield date(y, m, 1)
        m += 1
        if m == 13:
            y, m = y + 1, 1


def _seed_history(engine, *, end: date = HIST_END, ts: datetime = BACKFILL_TS) -> None:
    """Deterministic macro + SPY history, all pulled in one backfill at ``ts``."""
    rows = []
    for k, sid in enumerate(DAILY):
        for i, d in enumerate(_bdays(HIST_START, end)):
            rows.append(_row(sid, d, 2.0 + k + math.sin(i / 17.0 + k), ts))
    for i, d in enumerate(_bdays(HIST_START, end)):
        rows.append(_row("YF:SPY:close", d, 300.0 + i * 0.1 + 5 * math.sin(i / 9.0), ts, YF_SRC))
    for k, sid in enumerate(MONTHLY):
        for i, d in enumerate(_months(HIST_START, end)):
            rows.append(_row(sid, d, 50.0 + 3 * k + i * 0.05 + math.sin(i / 3.0 + k), ts))
    d = HIST_START + timedelta(days=(5 - HIST_START.weekday()) % 7)  # Saturdays
    i = 0
    while d <= end:
        rows.append(_row("ICSA", d, 220000.0 + 1000 * math.sin(i / 5.0), ts))
        d += timedelta(days=7)
        i += 1
    _insert_many(engine, rows)


def _vec(sv) -> dict:
    from intelligence.regime.state_vector import DIM_NAMES

    return {
        "values": dict(zip(DIM_NAMES, sv.values)),
        "completeness": sv.completeness,
        "stale": sv.stale_dimensions,
        "basis": sv.price_basis,
    }


# ── store.observations.read_window_known_at ──────────────────────────────


class TestReadWindowKnownAt:
    def _read(self, engine, sid, as_of, lag):
        from store.observations import read_window_known_at

        with engine.connect() as conn:
            return read_window_known_at(conn, sid, as_of=as_of, lag=lag)

    def test_monthly_backfill_visible_only_after_modeled_release(self, engine):
        from store.observations import PublicationLag

        lag = PublicationLag(40, "calendar")
        _insert_many(engine, [_row("X", date(2025, 5, 1), 4.1), _row("X", date(2025, 6, 1), 4.2)])

        # obs 2025-06-01 is modeled public on 07-11; obs 05-01 on 06-10.
        assert [o.obs_date for o in self._read(engine, "X", date(2025, 6, 9), lag)] == []
        got = self._read(engine, "X", date(2025, 6, 10), lag)
        assert [(o.obs_date, o.known_at, o.known_at_basis) for o in got] == [
            (date(2025, 5, 1), date(2025, 6, 10), "modeled_lag"),
        ]
        assert len(self._read(engine, "X", date(2025, 7, 11), lag)) == 2

    def test_observation_date_alone_never_makes_a_value_visible(self, engine):
        """The pre-fix read (read_window) returns a monthly value at an as_of
        before its release; the known-at read must not."""
        from store.observations import PublicationLag, read_window

        _insert_many(engine, [_row("X", date(2025, 6, 1), 4.2)])
        as_of = date(2025, 6, 20)
        with engine.connect() as conn:
            assert len(read_window(conn, "X", as_of=as_of)) == 1  # the defect
        assert self._read(engine, "X", as_of, PublicationLag(40, "calendar")) == []

    def test_pull_evidence_beats_the_modeled_lag(self, engine):
        from store.observations import PublicationLag

        # Pulled 5 days after the obs date: known then, even though the
        # conservative model says day 6.
        _insert_many(engine, [_row("X", date(2026, 9, 12), 1.0, ts=datetime(2026, 9, 17, 13, 0))])
        got = self._read(engine, "X", date(2026, 9, 17), PublicationLag(6, "calendar"))
        assert [(o.known_at, o.known_at_basis) for o in got] == [(date(2026, 9, 17), "pulled")]
        assert self._read(engine, "X", date(2026, 9, 16), PublicationLag(6, "calendar")) == []

    def test_latest_vintage_pulled_by_as_of_revisions_after_ignored(self, engine):
        from store.observations import PublicationLag

        d = date(2026, 9, 1)
        _insert_many(engine, [
            _row("X", d, 1.0, ts=datetime(2026, 9, 2, 9)),
            _row("X", d, 1.1, ts=datetime(2026, 9, 5, 9)),   # revision, by as_of
            _row("X", d, 9.9, ts=datetime(2026, 9, 12, 9)),  # revision after as_of
        ])
        got = self._read(engine, "X", date(2026, 9, 6), PublicationLag(1, "calendar"))
        assert [o.value for o in got] == [1.1]

    def test_modeled_path_uses_earliest_vintage_so_revisions_never_rewrite_history(self, engine):
        from store.observations import PublicationLag

        d = date(2015, 3, 1)
        _insert_many(engine, [_row("X", d, 5.0)])
        before = self._read(engine, "X", date(2015, 6, 1), PublicationLag(40, "calendar"))
        _insert_many(engine, [_row("X", d, 7.0, ts=datetime(2026, 9, 28, 6))])  # later re-pull
        after = self._read(engine, "X", date(2015, 6, 1), PublicationLag(40, "calendar"))
        assert [o.value for o in before] == [o.value for o in after] == [5.0]

    def test_no_lag_means_pull_evidence_only(self, engine):
        _insert_many(engine, [_row("X", date(2020, 1, 1), 1.0)])  # backfilled 2026-03-24
        assert self._read(engine, "X", date(2021, 1, 1), None) == []
        assert len(self._read(engine, "X", date(2026, 3, 24), None)) == 1

    def test_business_day_lag_friday_known_monday(self, engine):
        from store.observations import PublicationLag

        lag = PublicationLag(1, "business")
        _insert_many(engine, [_row("X", date(2025, 6, 6), 1.0)])  # Friday
        assert self._read(engine, "X", date(2025, 6, 8), lag) == []  # Sunday
        assert len(self._read(engine, "X", date(2025, 6, 9), lag)) == 1  # Monday

    def test_second_source_appearing_later_does_not_break_a_past_read(self, engine):
        from store.observations import MixedSourceError, PublicationLag

        lag = PublicationLag(1, "calendar")
        _insert_many(engine, [
            _row("X", date(2026, 9, 1), 1.0, ts=datetime(2026, 9, 2, 9)),
            _row("X", date(2026, 9, 1), 1.0, ts=datetime(2026, 9, 20, 9), src=YF_SRC),
        ])
        assert [o.source for o in self._read(engine, "X", date(2026, 9, 3), lag)] == ["fred"]
        with pytest.raises(MixedSourceError):
            _insert_many(engine, [_row("X", date(2026, 9, 21), 2.0, ts=datetime(2026, 9, 21, 9), src=YF_SRC)])
            self._read(engine, "X", date(2026, 9, 22), lag)


# ── Publication-lag table ────────────────────────────────────────────────


def test_every_macro_series_has_a_declared_publication_lag():
    from intelligence.regime.state_vector import PUBLICATION_LAGS, STATE_DIMENSIONS

    needed = {d.series_id for d in STATE_DIMENSIONS if not d.series_id.startswith("DERIVED:")}
    needed |= {"T5YIE", "DFF"}  # read by DERIVED:T5YIE / DERIVED:REAL_FF
    assert needed <= set(PUBLICATION_LAGS)
    for sid in MONTHLY:
        assert PUBLICATION_LAGS[sid].unit == "calendar" and PUBLICATION_LAGS[sid].days >= 40


def test_daily_lags_match_the_reviewer_verified_publication_table():
    from analysis.research_real_panel import PUBLICATIONS
    from intelligence.regime.state_vector import PUBLICATION_LAGS

    for sid, pub in (
        ("VIXCLS", "CBOE_VIX"), ("T10Y2Y", "FRED_H15_SPREAD"), ("T5YIE", "FRED_H15_SPREAD"),
        ("DFF", "FRB_H15"), ("BAMLH0A0HYM2", "ICE_BOFA"), ("BAMLC0A0CM", "ICE_BOFA"),
    ):
        ours = PUBLICATION_LAGS[sid]
        assert (ours.days, ours.unit) == (PUBLICATIONS[pub].lag, PUBLICATIONS[pub].unit), sid


# ── State vector: fix 1, monthly value released after as_of ──────────────


def test_monthly_value_released_after_as_of_does_not_affect_vector(engine):
    from intelligence.regime.state_vector import compute_state_vector

    _seed_history(engine, end=date(2025, 5, 31))
    before = _vec(compute_state_vector(engine, AS_OF))
    assert before["values"]["unemployment_level"] is not None

    # UNRATE for May 2025 (dated 05-01, published 06-06): an extreme value.
    # At AS_OF = 06-10 it IS public by the model (05-01 + 40d = 06-10).
    # June 2025 (dated 06-01) is not published until July; the pre-fix
    # obs_date <= as_of read would have used it at 06-10.
    _insert_many(engine, [_row("UNRATE", date(2025, 6, 1), 99.0)])
    after = _vec(compute_state_vector(engine, AS_OF))
    assert after == before

    # Once released (06-01 + 40d = 07-11), it does move the dimension.
    later = _vec(compute_state_vector(engine, date(2025, 7, 11)))
    assert later["values"]["unemployment_level"] > 5  # 99.0 z-scored against ~50s


def test_stale_flag_uses_the_newest_known_observation(engine):
    """A monthly series whose newest known value is >30 days old is stale
    (the old 60-day staleness window reported it fresh when it was >60)."""
    from intelligence.regime.state_vector import compute_state_vector

    _seed_history(engine, end=date(2025, 5, 31))
    sv = compute_state_vector(engine, AS_OF)
    # Newest UNRATE known at 06-10 is dated 05-01: 40 days old.
    assert "unemployment_level" in sv.stale_dimensions
    # Daily VIX known through the prior business day: fresh.
    assert "vix_level" not in sv.stale_dimensions


# ── State vector: fix 2, determinism under appended history ──────────────


def test_appending_future_data_does_not_change_a_past_vector(engine):
    from intelligence.regime import state_vector as sv_mod

    _seed_history(engine)
    past = _vec(sv_mod.compute_state_vector(engine, AS_OF))
    past_stats = sv_mod._get_normalization_stats(engine, AS_OF)

    # Append what the next months/years bring: new observation dates with
    # extreme values (would drag a run-date mean/std), live pulls,
    # revisions of observation dates the past vector already used, and a
    # value for a date just before as_of that was not public by as_of.
    # (A brand-new observation date older than as_of - lag is a late
    # backfill of history, not future data: by design it enters past
    # vectors through the modeled lag, and a history rebuild picks it up.)
    live_ts = datetime(2026, 9, 28, 23, 0)
    rows = []
    for sid in DAILY + MONTHLY + ("ICSA",):
        for d in _bdays(date(2026, 3, 23), date(2026, 9, 25)):
            rows.append(_row(sid, d, 1e4, ts=live_ts))
        existing = date(2025, 5, 3) if sid == "ICSA" else date(2025, 5, 1)
        rows.append(_row(sid, existing, -1e4, ts=live_ts))           # revision
        rows.append(_row(sid, date(2025, 6, 9), -1e4, ts=live_ts))   # not yet public at as_of
    for d in _bdays(date(2026, 3, 23), date(2026, 9, 25)):
        rows.append(_row("YF:SPY:close", d, 1e4, ts=live_ts, src=YF_SRC))
    _insert_many(engine, rows)

    assert sv_mod._get_normalization_stats(engine, AS_OF) == past_stats
    assert _vec(sv_mod.compute_state_vector(engine, AS_OF)) == past


def test_vector_does_not_depend_on_run_order_or_process_cache(engine):
    from intelligence.regime import state_vector as sv_mod

    _seed_history(engine)
    alone = _vec(sv_mod.compute_state_vector(engine, AS_OF))
    sv_mod.compute_state_vector(engine, date(2026, 3, 20))  # a later as_of first
    assert _vec(sv_mod.compute_state_vector(engine, AS_OF)) == alone
    assert not hasattr(sv_mod, "_NORM_CACHE")


# ── Nightly job path == series path ──────────────────────────────────────


def test_nightly_job_and_series_paths_produce_identical_vectors(engine, monkeypatch):
    import scripts.run_regime_state_vectors as job
    from intelligence.regime import state_vector as sv_mod

    _seed_history(engine)
    target = date(2025, 6, 11)

    # Series path, as the history rebuild uses it: other dates first, in
    # the same process, then the target.
    series = sv_mod.compute_state_vector_series(engine, date(2025, 5, 22), target, freq_days=5)
    by_date = {sv.as_of_date: sv for sv in series}
    assert target in by_date
    single = sv_mod.compute_state_vector_series(engine, target, target)
    assert len(single) == 1

    # Nightly job path: scripts/run_regime_state_vectors.main ->
    # get_or_compute_state_vector -> compute_state_vector (dry run: no write).
    seen = []
    real = sv_mod.compute_state_vector

    def spy(e, a=None):
        sv = real(e, a)
        seen.append(sv)
        return sv

    monkeypatch.setattr(sv_mod, "compute_state_vector", spy)
    assert job.main(["--as-of", target.isoformat(), "--dry-run", "--json"], engine=engine) == 0
    assert len(seen) == 1
    nightly = seen[0]

    assert nightly.completeness >= sv_mod.MIN_CACHE_COMPLETENESS
    assert _vec(nightly) == _vec(by_date[target]) == _vec(single[0])
    assert nightly.price_basis == "YF:SPY:close"  # resolved spy_full absent here -> documented fallback
