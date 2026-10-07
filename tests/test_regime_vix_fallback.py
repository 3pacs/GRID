"""VIX basis for the regime state vector (owner ruling R4, 2026-10-07).

``vix_level`` / ``vix_percentile`` read FRED ``VIXCLS``; only when VIXCLS
cannot produce them at ``as_of`` does the whole series switch to the
Cboe-published close ``CBOE:VIX`` (source ``CBOE``), read point-in-time with
the same next-business-day lag. These tests pin:

* VIXCLS precedence: a usable VIXCLS gives exactly the pre-R4 vector and
  CBOE:VIX is never read.
* Whole-series fallback: value, percentile window and z-score stats all come
  from CBOE:VIX, never mixed with VIXCLS.
* Point-in-time: the fallback obeys ``read_window_known_at`` (pull evidence
  or the modeled lag), so later rows never change a past vector.
* Provenance: only source ``CBOE`` rows count, and ``vix_basis`` records the
  series used, in memory and through the cache.
* Late VIXCLS: only its trailing gap is filled from CBOE:VIX, never past the
  date VIXCLS itself would be modeled as published by ``as_of``, and never
  over a close VIXCLS has.

Fixture: real ``raw_series`` + ``source_catalog`` on in-memory SQLite, the
same shape as ``tests/test_regime_state_vector_pit.py``.
"""

from __future__ import annotations

import json
import math
import sqlite3
from datetime import date, datetime, timedelta

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
BACKFILL_TS = datetime(2026, 3, 24, 6, 0, 0)  # griddb: FRED history pulled >= this
HIST_START = date(2018, 1, 1)
HIST_END = date(2026, 3, 20)
AS_OF = date(2025, 6, 10)  # Tuesday; modeled next-business-day lag -> obs <= 2025-06-09
LAST_KNOWN = date(2025, 6, 9)


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
            [
                {"id": FRED_SRC, "name": "FRED"},
                {"id": CBOE_SRC, "name": "CBOE"},
                {"id": OTHER_SRC, "name": "yfinance"},
            ],
        )
    return eng


def _bdays(start: date, end: date):
    d = start
    while d <= end:
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)


def _vixcls_value(i: int) -> float:
    return 18.0 + 4.0 * math.sin(i / 23.0)


def _cboe_value(i: int) -> float:
    # Deliberately different from VIXCLS so a wrong source is visible.
    return 30.0 + 9.0 * math.cos(i / 11.0)


def _seed(engine, sid: str, src: int, fn, *, start=HIST_START, end=HIST_END, ts=BACKFILL_TS) -> None:
    rows = [
        {"sid": sid, "src": src, "d": d, "ts": ts, "v": fn(i)}
        for i, d in enumerate(_bdays(start, end))
    ]
    _insert(engine, rows)


def _insert(engine, rows) -> None:
    with engine.begin() as c:
        c.execute(
            text(
                "INSERT INTO raw_series (series_id, source_id, obs_date, "
                "pull_timestamp, value, raw_payload, pull_status) "
                "VALUES (:sid, :src, :d, :ts, :v, '{}', 'SUCCESS')"
            ),
            rows,
        )


def _vix_dims(sv) -> tuple[float | None, float | None]:
    from intelligence.regime.state_vector import DIM_NAMES

    vals = dict(zip(DIM_NAMES, sv.values))
    return vals["vix_level"], vals["vix_percentile"]


def _expected_vix_dims(fn, *, as_of_known: date = LAST_KNOWN) -> tuple[float, float]:
    """Independent recomputation of the two VIX dims from a seeded series."""
    return _expected_from(
        {d: fn(i) for i, d in enumerate(_bdays(HIST_START, HIST_END)) if d <= as_of_known}
    )


def _expected_from(points: dict) -> tuple[float, float]:
    from intelligence.regime.state_vector import NORM_LOOKBACK_DAYS, VALUE_LOOKBACK_DAYS

    full = pd.Series(points, dtype=float).sort_index()
    norm = full[full.index >= AS_OF - timedelta(days=NORM_LOOKBACK_DAYS)]
    window = full[full.index >= AS_OF - timedelta(days=VALUE_LOOKBACK_DAYS)]
    level = (float(window.iloc[-1]) - float(norm.mean())) / float(norm.std())
    tail = window.iloc[-504:]
    pct = float((tail < tail.iloc[-1]).sum() / len(tail))
    return level, pct


def _record_reads(monkeypatch) -> list[str]:
    import intelligence.regime.state_vector as sv_mod

    seen: list[str] = []
    real = sv_mod._fetch_series

    def spy(engine, series_id, as_of, lookback_days=sv_mod.VALUE_LOOKBACK_DAYS):
        seen.append(series_id)
        return real(engine, series_id, as_of, lookback_days=lookback_days)

    monkeypatch.setattr(sv_mod, "_fetch_series", spy)
    return seen


# ── Precedence ───────────────────────────────────────────────────────────


def test_usable_vixcls_is_used_and_cboe_is_never_read(engine, monkeypatch):
    from intelligence.regime.state_vector import compute_state_vector

    _seed(engine, "VIXCLS", FRED_SRC, _vixcls_value)
    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe_value)
    seen = _record_reads(monkeypatch)

    sv = compute_state_vector(engine, AS_OF)

    assert sv.vix_basis == "VIXCLS"
    assert "CBOE:VIX" not in seen
    level, pct = _vix_dims(sv)
    exp_level, exp_pct = _expected_vix_dims(_vixcls_value)
    assert level == pytest.approx(exp_level, rel=1e-12)
    assert pct == pytest.approx(exp_pct, rel=1e-12)


def test_cboe_rows_do_not_change_a_vixcls_vector(engine):
    """Adding CBOE:VIX history must leave a VIXCLS-backed vector bit-identical."""
    from intelligence.regime.state_vector import compute_state_vector

    _seed(engine, "VIXCLS", FRED_SRC, _vixcls_value)
    before = compute_state_vector(engine, AS_OF)
    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe_value)
    after = compute_state_vector(engine, AS_OF)

    assert after.values == before.values
    assert after.stale_dimensions == before.stale_dimensions
    assert after.vix_basis == before.vix_basis == "VIXCLS"


# ── Whole-series fallback ────────────────────────────────────────────────


def test_absent_vixcls_falls_back_to_cboe_as_a_whole_series(engine):
    from intelligence.regime.state_vector import compute_state_vector

    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe_value)

    sv = compute_state_vector(engine, AS_OF)

    assert sv.vix_basis == "CBOE:VIX"
    level, pct = _vix_dims(sv)
    exp_level, exp_pct = _expected_vix_dims(_cboe_value)
    # z-scored against CBOE:VIX's own PIT history, percentile over its window
    assert level == pytest.approx(exp_level, rel=1e-12)
    assert pct == pytest.approx(exp_pct, rel=1e-12)
    assert "vix_level" not in sv.stale_dimensions


def test_too_short_vixcls_falls_back_without_splicing(engine):
    """VIXCLS below min_history -> CBOE:VIX for both dims; no VIXCLS value leaks in."""
    from intelligence.regime.state_vector import compute_state_vector

    _seed(engine, "VIXCLS", FRED_SRC, _vixcls_value, start=date(2025, 4, 1))  # ~50 obs known
    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe_value)

    sv = compute_state_vector(engine, AS_OF)

    assert sv.vix_basis == "CBOE:VIX"
    assert _vix_dims(sv) == pytest.approx(_expected_vix_dims(_cboe_value), rel=1e-12)


def test_mixed_source_vixcls_falls_back(engine):
    """A VIXCLS read that spans two sources is unusable, so CBOE:VIX takes over.

    The second source ties FRED's backfill timestamp: a source pulled strictly
    later cannot make a past read mixed (``read_window_known_at``).
    """
    from intelligence.regime.state_vector import compute_state_vector

    _seed(engine, "VIXCLS", FRED_SRC, _vixcls_value)
    _seed(engine, "VIXCLS", OTHER_SRC, _vixcls_value, start=date(2025, 1, 2))
    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe_value)

    sv = compute_state_vector(engine, AS_OF)

    assert sv.vix_basis == "CBOE:VIX"
    assert _vix_dims(sv) == pytest.approx(_expected_vix_dims(_cboe_value), rel=1e-12)


def test_neither_series_usable_leaves_vix_dims_unavailable(engine):
    from intelligence.regime.state_vector import compute_state_vector

    _seed(engine, "VIXCLS", FRED_SRC, _vixcls_value, start=date(2025, 5, 1))
    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe_value, start=date(2025, 5, 1))

    sv = compute_state_vector(engine, AS_OF)

    assert sv.vix_basis is None
    assert _vix_dims(sv) == (None, None)


# ── Provenance: source pinned to CBOE ────────────────────────────────────


def test_fallback_reads_only_the_cboe_source(engine):
    """CBOE:VIX rows written under any other source are ignored, not mixed in."""
    from intelligence.regime.state_vector import compute_state_vector

    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe_value)
    _seed(engine, "CBOE:VIX", OTHER_SRC, lambda i: 999.0, start=date(2024, 1, 1), ts=datetime(2026, 3, 25))

    sv = compute_state_vector(engine, AS_OF)

    assert sv.vix_basis == "CBOE:VIX"
    assert _vix_dims(sv) == pytest.approx(_expected_vix_dims(_cboe_value), rel=1e-12)


def test_cboe_vix_under_a_foreign_source_only_is_not_a_fallback(engine):
    from intelligence.regime.state_vector import compute_state_vector

    _seed(engine, "CBOE:VIX", OTHER_SRC, _cboe_value)

    sv = compute_state_vector(engine, AS_OF)

    assert sv.vix_basis is None
    assert _vix_dims(sv) == (None, None)


# ── Point-in-time ────────────────────────────────────────────────────────


def test_fallback_backfilled_close_waits_for_the_next_business_day(engine):
    """Backfilled history (no pull by as_of) is visible only via the modeled lag."""
    from intelligence.regime.state_vector import _AsOfReader

    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe_value)

    # Monday 2025-06-09: Monday's backfilled close is not yet known, Friday's is.
    assert _AsOfReader(engine, LAST_KNOWN).window("CBOE:VIX").index[-1] == date(2025, 6, 6)
    # Tuesday: Monday's close is now known.
    assert _AsOfReader(engine, AS_OF).window("CBOE:VIX").index[-1] == LAST_KNOWN


def test_fallback_live_pull_is_visible_from_its_pull_date(engine):
    from intelligence.regime.state_vector import _AsOfReader

    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe_value, end=date(2025, 6, 6))
    # Monday's close pulled live the same evening (Cboe publishes after the close)
    _insert(engine, [{"sid": "CBOE:VIX", "src": CBOE_SRC, "d": LAST_KNOWN,
                      "ts": datetime(2025, 6, 9, 23, 30), "v": 42.0}])

    window = _AsOfReader(engine, LAST_KNOWN).window("CBOE:VIX")
    assert window.index[-1] == LAST_KNOWN
    assert window.iloc[-1] == 42.0


def test_later_cboe_rows_never_change_a_past_fallback_vector(engine):
    from intelligence.regime.state_vector import compute_state_vector

    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe_value, end=AS_OF - timedelta(days=1))
    before = compute_state_vector(engine, AS_OF)
    # Rows for later dates, and a re-pull of an existing date, both after as_of.
    later = datetime(2026, 10, 7, 22, 0)
    _seed(engine, "CBOE:VIX", CBOE_SRC, lambda i: 80.0, start=AS_OF, end=date(2025, 12, 31), ts=later)
    _insert(engine, [{"sid": "CBOE:VIX", "src": CBOE_SRC, "d": date(2025, 6, 6), "ts": later, "v": 77.0}])
    after = compute_state_vector(engine, AS_OF)

    assert before.vix_basis == after.vix_basis == "CBOE:VIX"
    assert after.values == before.values


def test_fallback_lag_matches_vixcls_and_the_reviewed_publication_table():
    from analysis.research_real_panel import PUBLICATIONS
    from intelligence.regime.state_vector import (
        PUBLICATION_LAGS,
        SERIES_SOURCES,
        VIX_FALLBACK_SERIES,
        VIX_SERIES,
    )

    ours = PUBLICATION_LAGS[VIX_FALLBACK_SERIES]
    assert ours == PUBLICATION_LAGS[VIX_SERIES]
    assert (ours.days, ours.unit) == (PUBLICATIONS["CBOE_VIX"].lag, PUBLICATIONS["CBOE_VIX"].unit)
    assert SERIES_SOURCES == {VIX_FALLBACK_SERIES: "CBOE"}
    assert VIX_SERIES not in SERIES_SOURCES  # VIXCLS read path unchanged


# ── Label: in memory and through the cache ───────────────────────────────


def test_vix_basis_is_reported_and_survives_the_cache(monkeypatch):
    import intelligence.regime.state_vector as sv_mod

    sv = sv_mod.StateVector(
        as_of_date=AS_OF,
        values=tuple([0.5] * len(sv_mod.DIM_NAMES)),
        completeness=1.0,
        stale_dimensions=(),
        price_basis="spy_full",
        vix_basis="CBOE:VIX",
    )
    assert sv.to_dict()["vix_basis"] == "CBOE:VIX"

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
    assert vec["__vix_basis__"] == "CBOE:VIX"
    assert vec["__price_basis__"] == "spy_full"

    back = sv_mod._row_to_state_vector((AS_OF, vec, 1.0, []), cached=True)
    assert back.vix_basis == "CBOE:VIX"
    assert back.values == sv.values

    legacy = {name: 0.5 for name in sv_mod.DIM_NAMES}  # row written before R4
    assert sv_mod._row_to_state_vector((AS_OF, legacy, 1.0, []), cached=True).vix_basis is None


# ── Late VIXCLS: trailing gap filled from CBOE:VIX ───────────────────────


def _cboe_by_date() -> dict:
    return {d: _cboe_value(i) for i, d in enumerate(_bdays(HIST_START, HIST_END))}


def _vixcls_by_date(end: date) -> dict:
    return {d: _vixcls_value(i) for i, d in enumerate(_bdays(HIST_START, HIST_END)) if d <= end}


def test_late_vixcls_trailing_gap_is_filled_from_cboe(engine):
    """VIXCLS stops at Wed 06-04; Thu, Fri and Mon come from Cboe, nothing else changes."""
    from intelligence.regime.state_vector import compute_state_vector

    _seed(engine, "VIXCLS", FRED_SRC, _vixcls_value, end=date(2025, 6, 4))
    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe_value)

    sv = compute_state_vector(engine, AS_OF)

    assert sv.vix_basis == "VIXCLS+CBOE:VIX"
    cboe = _cboe_by_date()
    points = _vixcls_by_date(date(2025, 6, 4))
    points.update({d: cboe[d] for d in (date(2025, 6, 5), date(2025, 6, 6), LAST_KNOWN)})
    assert _vix_dims(sv) == pytest.approx(_expected_from(points), rel=1e-12)


def test_gap_fill_never_replaces_a_vixcls_close(engine):
    import intelligence.regime.state_vector as sv_mod

    _seed(engine, "VIXCLS", FRED_SRC, _vixcls_value, end=date(2025, 6, 4))
    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe_value)
    reader = sv_mod._AsOfReader(engine, AS_OF)

    assert sv_mod._fill_vix_trailing_gap(reader) == 3
    filled = reader.full("VIXCLS")
    vixcls = _vixcls_by_date(date(2025, 6, 4))
    assert {d: filled[d] for d in vixcls if d in filled.index} == {
        d: v for d, v in vixcls.items() if d in filled.index
    }
    assert filled.index.is_monotonic_increasing and filled.index.is_unique
    assert reader.window("VIXCLS").index[-1] == LAST_KNOWN


def test_gap_fill_stops_at_vixcls_own_publication_horizon(engine):
    """A Cboe close pulled live on as_of is fresher than VIXCLS could be: not used."""
    import intelligence.regime.state_vector as sv_mod

    _seed(engine, "VIXCLS", FRED_SRC, _vixcls_value, end=date(2025, 6, 4))
    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe_value, end=LAST_KNOWN)
    _insert(engine, [{"sid": "CBOE:VIX", "src": CBOE_SRC, "d": AS_OF,
                      "ts": datetime(2025, 6, 10, 21, 30), "v": 55.0}])
    reader = sv_mod._AsOfReader(engine, AS_OF)

    assert AS_OF in reader.window("CBOE:VIX").index  # Cboe knows it ...
    sv_mod._fill_vix_trailing_gap(reader)
    assert reader.window("VIXCLS").index[-1] == LAST_KNOWN  # ... the fill does not use it


@pytest.mark.parametrize(
    ("as_of", "horizon"),
    [
        (date(2025, 6, 10), date(2025, 6, 9)),   # Tue -> Mon
        (date(2025, 6, 9), date(2025, 6, 6)),    # Mon -> Fri
        (date(2025, 6, 14), date(2025, 6, 12)),  # Sat -> Thu (Fri close known Mon)
        (date(2025, 9, 2), date(2025, 9, 1)),    # Tue after Labor Day -> the holiday, nothing to fill
    ],
)
def test_vix_publication_horizon(as_of, horizon):
    from intelligence.regime.state_vector import (
        PUBLICATION_LAGS,
        VIX_SERIES,
        _vix_publication_horizon,
    )

    assert _vix_publication_horizon(as_of) == horizon
    assert PUBLICATION_LAGS[VIX_SERIES].known_dates([horizon])[0] <= as_of


def test_on_time_vixcls_over_a_weekend_reads_no_cboe(engine, monkeypatch):
    from intelligence.regime.state_vector import compute_state_vector

    _seed(engine, "VIXCLS", FRED_SRC, _vixcls_value)
    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe_value)
    seen = _record_reads(monkeypatch)

    for as_of in (date(2025, 6, 9), date(2025, 6, 14), date(2025, 6, 15)):  # Mon, Sat, Sun
        assert compute_state_vector(engine, as_of).vix_basis == "VIXCLS"
    assert "CBOE:VIX" not in seen


def test_later_rows_never_change_a_past_gap_filled_vector(engine):
    from intelligence.regime.state_vector import compute_state_vector

    _seed(engine, "VIXCLS", FRED_SRC, _vixcls_value, end=date(2025, 6, 4))
    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe_value, end=LAST_KNOWN)
    before = compute_state_vector(engine, AS_OF)
    later = datetime(2026, 10, 7, 22, 0)
    _seed(engine, "CBOE:VIX", CBOE_SRC, lambda i: 80.0, start=AS_OF, end=date(2025, 12, 31), ts=later)
    _insert(engine, [{"sid": "CBOE:VIX", "src": CBOE_SRC, "d": date(2025, 6, 6), "ts": later, "v": 77.0}])
    after = compute_state_vector(engine, AS_OF)

    assert before.vix_basis == after.vix_basis == "VIXCLS+CBOE:VIX"
    assert after.values == before.values


def test_failed_gap_fill_keeps_plain_vixcls(engine, monkeypatch):
    import intelligence.regime.state_vector as sv_mod

    _seed(engine, "VIXCLS", FRED_SRC, _vixcls_value, end=date(2025, 6, 4))
    unfilled = sv_mod.compute_state_vector(engine, AS_OF)  # no CBOE rows: nothing to fill

    def boom(_reader):
        raise RuntimeError("cboe read failed")

    monkeypatch.setattr(sv_mod, "_fill_vix_trailing_gap", boom)
    _seed(engine, "CBOE:VIX", CBOE_SRC, _cboe_value)
    sv = sv_mod.compute_state_vector(engine, AS_OF)

    assert unfilled.vix_basis == sv.vix_basis == "VIXCLS"
    assert sv.values == unfilled.values
