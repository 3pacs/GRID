"""Tests for the regime state-vector fix (Wave 3 W3.2).

Covers the three things ``GRID-WAVE3-HELD-WRITERS-TRIAGE-20260927.md``
§4.2 asked for:

1. PIT readers — ``_fetch_series``, ``_fetch_spy_prices`` and
   ``_get_insider_sentiment`` go through ``store.observations.read_window``
   (SUCCESS-only, vintage-collapsed), not a direct, un-collapsed
   ``raw_series`` read.
2. ``get_or_compute_state_vector(..., persist=False)`` (the GET routes'
   contract) never issues DDL and never calls ``cache_state_vector`` —
   proven against a real SQLite engine, not just a mock, so an accidental
   INSERT would actually fail the test.
3. A future-dated ``as_of`` is never cached or served from the cache.

Fixture shape mirrors ``tests/test_store_observations.py``: a real
``raw_series`` + ``source_catalog`` pair on an in-memory SQLite engine, so
``read_window``'s SQL runs for real instead of being mocked away.
"""

from __future__ import annotations

import json
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

AS_OF = date(2026, 9, 20)
T0 = datetime(2026, 9, 1, 6, 0, 0)
FRED_SRC = 1
YF_SRC = 2


@pytest.fixture()
def engine():
    eng = create_engine("sqlite://")
    md = MetaData()
    source_catalog = Table(
        "source_catalog", md,
        Column("id", Integer, primary_key=True),
        Column("name", String, nullable=False),
    )
    raw = Table(
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
        c.execute(source_catalog.insert(), [
            {"id": FRED_SRC, "name": "fred"},
            {"id": YF_SRC, "name": "yfinance"},
        ])
    return eng


def _create_sqlite_cache_table(engine) -> None:
    """A SQLite-compatible ``regime_state_vectors`` table.

    The real DDL in ``state_vector._CACHE_TABLE_SQL`` uses Postgres-only
    types (``BIGSERIAL``, ``JSONB``, ``TEXT[]``, ``TIMESTAMPTZ``,
    ``NOW()``) and ``cache_state_vector`` binds ``stale_dims`` as a native
    Python list, which the sqlite3 driver can't bind either — both are
    fine in production (Postgres-only, same class of gotcha as ``DISTINCT
    ON`` in ``store/pit.py``) but mean tests here create the table
    directly with plain types instead of exercising that DDL/INSERT.
    """
    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE IF NOT EXISTS regime_state_vectors ("
            "id INTEGER PRIMARY KEY, as_of_date DATE NOT NULL UNIQUE, "
            "vector TEXT NOT NULL, completeness REAL NOT NULL, "
            "stale_dims TEXT, computed_at TEXT)"
        ))


def _seed_cache_row(engine, as_of, vector: dict, completeness: float, stale=None) -> None:
    _create_sqlite_cache_table(engine)
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO regime_state_vectors (as_of_date, vector, completeness, stale_dims) "
                "VALUES (:dt, :vec, :comp, :stale)"
            ),
            {"dt": as_of, "vec": json.dumps(vector), "comp": completeness, "stale": stale},
        )


def _insert(engine, sid, d, v, *, status="SUCCESS", ts_offset_h=0, source_id=FRED_SRC):
    with engine.begin() as c:
        c.execute(
            text(
                "INSERT INTO raw_series (series_id, source_id, obs_date, "
                "pull_timestamp, value, raw_payload, pull_status) "
                "VALUES (:sid, :src, :d, :ts, :v, '{}', :status)"
            ),
            {
                "sid": sid, "src": source_id, "d": d,
                "ts": T0 + timedelta(hours=ts_offset_h), "v": v, "status": status,
            },
        )


# ── 1. PIT readers ──────────────────────────────────────────────────────


class TestFetchSeriesVintageCollapse:
    def test_uses_latest_vintage_and_drops_failed(self, engine):
        from intelligence.regime.state_vector import _fetch_series

        # Two vintages for the same obs_date (a revision re-pull): the
        # earlier pull said 4.10, the later one corrected it to 4.05.
        _insert(engine, "T10Y2Y", date(2026, 9, 18), 4.10, ts_offset_h=0)
        _insert(engine, "T10Y2Y", date(2026, 9, 18), 4.05, ts_offset_h=10)
        # A FAILED marker dated after both — must never be returned.
        _insert(engine, "T10Y2Y", date(2026, 9, 18), 0.0, status="FAILED", ts_offset_h=20)
        _insert(engine, "T10Y2Y", date(2026, 9, 19), 4.02, ts_offset_h=0)

        series = _fetch_series(engine, "T10Y2Y", AS_OF)

        assert list(series.items()) == [
            (date(2026, 9, 18), 4.05),
            (date(2026, 9, 19), 4.02),
        ]

    def test_excludes_rows_after_as_of(self, engine):
        from intelligence.regime.state_vector import _fetch_series

        _insert(engine, "T10Y2Y", date(2026, 9, 19), 4.02, ts_offset_h=0)
        _insert(engine, "T10Y2Y", date(2026, 9, 21), 4.03, ts_offset_h=0)  # after AS_OF

        series = _fetch_series(engine, "T10Y2Y", AS_OF)

        assert list(series.index) == [date(2026, 9, 19)]

    def test_empty_when_no_rows(self, engine):
        from intelligence.regime.state_vector import _fetch_series

        assert _fetch_series(engine, "NOPE", AS_OF).empty

    def test_mixed_source_degrades_to_empty_not_raise(self, engine):
        """A series_id written by two sources with no ``source=`` pin fails
        closed inside read_window; state_vector must degrade, not crash."""
        from intelligence.regime.state_vector import _fetch_series

        _insert(engine, "WEIRD", date(2026, 9, 18), 1.0, source_id=FRED_SRC)
        _insert(engine, "WEIRD", date(2026, 9, 19), 2.0, source_id=YF_SRC)

        assert _fetch_series(engine, "WEIRD", AS_OF).empty


class TestFetchSpyPrices:
    def test_falls_back_to_raw_yf_close_when_resolved_unavailable(self, engine):
        """No feature_registry/resolved_series tables in this fixture (the
        re-resolve infra this task can't verify locally) — must fall back
        to the raw YF:SPY:close observation series, not crash."""
        from intelligence.regime.state_vector import _fetch_spy_prices

        _insert(engine, "YF:SPY:close", date(2026, 9, 18), 570.0, source_id=YF_SRC)
        _insert(engine, "YF:SPY:close", date(2026, 9, 19), 572.0, source_id=YF_SRC)

        series, basis = _fetch_spy_prices(engine, AS_OF)

        assert basis == "YF:SPY:close"
        assert list(series.items()) == [
            (date(2026, 9, 18), 570.0),
            (date(2026, 9, 19), 572.0),
        ]

    def test_unavailable_when_neither_basis_has_data(self, engine):
        from intelligence.regime.state_vector import _fetch_spy_prices

        series, basis = _fetch_spy_prices(engine, AS_OF)

        assert series.empty
        assert basis is None

    def test_vintage_collapse_on_raw_fallback(self, engine):
        from intelligence.regime.state_vector import _fetch_spy_prices

        _insert(engine, "YF:SPY:close", date(2026, 9, 19), 500.0, source_id=YF_SRC, ts_offset_h=0)
        _insert(engine, "YF:SPY:close", date(2026, 9, 19), 501.5, source_id=YF_SRC, ts_offset_h=5)

        series, basis = _fetch_spy_prices(engine, AS_OF)

        assert basis == "YF:SPY:close"
        assert list(series.items()) == [(date(2026, 9, 19), 501.5)]


class TestInsiderSentimentVintageCollapse:
    def test_sums_latest_vintage_per_series(self, engine):
        from intelligence.regime.state_vector import _get_insider_sentiment

        # A revised BUY filing: 1000 shares corrected up to 1200.
        _insert(engine, "INSIDER:AAA:jdoe:BUY", date(2026, 9, 10), 1000.0, ts_offset_h=0)
        _insert(engine, "INSIDER:AAA:jdoe:BUY", date(2026, 9, 10), 1200.0, ts_offset_h=5)
        _insert(engine, "INSIDER:BBB:msmith:SELL", date(2026, 9, 12), 400.0, ts_offset_h=0)
        # A FAILED marker must never contribute.
        _insert(engine, "INSIDER:CCC:x:BUY", date(2026, 9, 12), 0.0, status="FAILED")

        sentiment = _get_insider_sentiment(engine, AS_OF)

        # buy=1200, sell=400 -> (1200-400)/1600 = 0.5
        assert sentiment == pytest.approx(0.5)

    def test_none_when_no_filings_in_window(self, engine):
        from intelligence.regime.state_vector import _get_insider_sentiment

        assert _get_insider_sentiment(engine, AS_OF) is None

    def test_ignores_filings_outside_30d_window(self, engine):
        from intelligence.regime.state_vector import _get_insider_sentiment

        _insert(engine, "INSIDER:OLD:jdoe:BUY", AS_OF - timedelta(days=60), 999.0)

        assert _get_insider_sentiment(engine, AS_OF) is None


# ── 2. GET must never write ──────────────────────────────────────────────


class TestPersistFalseNeverWrites:
    def test_persist_false_computes_in_memory_and_never_caches(self, engine, monkeypatch):
        from intelligence.regime import state_vector as sv_mod

        full = sv_mod.StateVector(
            as_of_date=AS_OF, values=tuple([0.1] * len(sv_mod.DIM_NAMES)),
            completeness=1.0, stale_dimensions=(), price_basis="YF:SPY:close",
        )
        monkeypatch.setattr(sv_mod, "compute_state_vector", lambda e, a: full)

        out = sv_mod.get_or_compute_state_vector(engine, AS_OF, persist=False)

        assert out is full
        assert out.cached is False
        # No DDL and no row written — proven against a real SQLite engine:
        # the table must not exist, because _ensure_cache_table was never
        # called on this path.
        with engine.connect() as conn:
            exists = conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name='regime_state_vectors'")
            ).fetchone()
        assert exists is None

    def test_persist_false_serves_existing_cache_hit_without_writing(self, engine, monkeypatch):
        from intelligence.regime import state_vector as sv_mod

        # Simulate a row the nightly job already wrote (seeded directly —
        # see _seed_cache_row's docstring on why the real DDL/INSERT don't
        # run on SQLite).
        vec = {name: 0.2 for name in sv_mod.DIM_NAMES}
        vec["__price_basis__"] = "spy_full"
        _seed_cache_row(engine, AS_OF, vec, 0.9)

        cache_spy = []
        monkeypatch.setattr(sv_mod, "cache_state_vector", lambda e, s: cache_spy.append(s))
        ensure_spy = []
        monkeypatch.setattr(sv_mod, "_ensure_cache_table", lambda e: ensure_spy.append(e))

        served = sv_mod.get_or_compute_state_vector(engine, AS_OF, persist=False)

        assert served.cached is True
        assert served.completeness == 0.9
        assert served.price_basis == "spy_full"
        assert cache_spy == []
        assert ensure_spy == []

    def test_low_completeness_get_returns_available_false_shape(self, engine, monkeypatch):
        """Mirrors what api/routers/intelligence_regime.py does with this
        result: below MIN_CACHE_COMPLETENESS means 'available: false', not
        a partial vector served as if it were good."""
        from intelligence.regime import state_vector as sv_mod

        thin = sv_mod.StateVector(
            as_of_date=AS_OF, values=tuple([None] * len(sv_mod.DIM_NAMES)),
            completeness=0.1, stale_dimensions=(),
        )
        monkeypatch.setattr(sv_mod, "compute_state_vector", lambda e, a: thin)

        out = sv_mod.get_or_compute_state_vector(engine, AS_OF, persist=False)

        assert out.completeness < sv_mod.MIN_CACHE_COMPLETENESS
        assert out.cached is False


# ── 3. Future-dated as_of is never cached or served ──────────────────────


class TestFutureDatedGuard:
    def test_future_as_of_computes_in_memory_never_cached(self, monkeypatch):
        from intelligence.regime import state_vector as sv_mod

        future = date.today() + timedelta(days=5)
        calls = []

        def _fake_compute(e, a):
            calls.append(a)
            return sv_mod.StateVector(
                as_of_date=a, values=tuple([1.0] * len(sv_mod.DIM_NAMES)),
                completeness=1.0, stale_dimensions=(),
            )

        monkeypatch.setattr(sv_mod, "compute_state_vector", _fake_compute)
        cache_spy = []
        monkeypatch.setattr(sv_mod, "cache_state_vector", lambda e, s: cache_spy.append(s))

        out = sv_mod.get_or_compute_state_vector(object(), future, persist=True)

        assert calls == [future]
        assert cache_spy == []  # never persisted, even with persist=True
        assert out.as_of_date == future

    def test_existing_future_dated_row_is_never_served(self, engine):
        """Defensive: even if a future-dated row somehow exists in the
        table, the cache read must exclude it."""
        from intelligence.regime import state_vector as sv_mod

        future = date.today() + timedelta(days=3)
        _seed_cache_row(engine, future, {}, 1.0)

        row = sv_mod._read_cached_row(engine, future, date.today())
        assert row is None

    def test_load_cached_vectors_excludes_future_dated_rows(self, engine):
        from intelligence.regime import state_vector as sv_mod

        future = date.today() + timedelta(days=3)
        _seed_cache_row(engine, future, {}, 1.0)

        assert sv_mod.load_cached_vectors(engine) == []


# ── cache_state_vector round-trips price_basis without a migration ──────


def test_cache_round_trip_preserves_price_basis(engine, monkeypatch):
    """``cache_state_vector``'s real INSERT is Postgres-only (see
    ``_create_sqlite_cache_table``'s docstring), so this stands a
    SQLite-compatible writer in for it to prove *this module's* read-after-
    write logic — including the "__price_basis__" convention that avoids a
    migration — round-trips correctly. ``price_basis`` is threaded through
    ``dim_dict`` exactly as the real ``cache_state_vector`` does."""
    from intelligence.regime import state_vector as sv_mod

    sv = sv_mod.StateVector(
        as_of_date=AS_OF, values=tuple([0.3] * len(sv_mod.DIM_NAMES)),
        completeness=0.8, stale_dimensions=(), price_basis="spy_full",
    )
    monkeypatch.setattr(sv_mod, "compute_state_vector", lambda e, a: sv)

    def _fake_cache_state_vector(e, s):
        dim_dict = {sv_mod.DIM_NAMES[i]: s.values[i] for i in range(len(s.values))}
        if s.price_basis is not None:
            dim_dict["__price_basis__"] = s.price_basis
        _seed_cache_row(e, s.as_of_date, dim_dict, s.completeness)

    monkeypatch.setattr(sv_mod, "cache_state_vector", _fake_cache_state_vector)
    monkeypatch.setattr(sv_mod, "_ensure_cache_table", lambda e: None)

    written = sv_mod.get_or_compute_state_vector(engine, AS_OF, persist=True)
    assert written.price_basis == "spy_full"

    reloaded = sv_mod._read_cached_row(engine, AS_OF, date.today())
    assert reloaded is not None
    vec_dict = json.loads(reloaded[1])
    assert vec_dict["__price_basis__"] == "spy_full"

    served = sv_mod.get_or_compute_state_vector(engine, AS_OF, persist=False)
    assert served.price_basis == "spy_full"
    assert served.cached is True
