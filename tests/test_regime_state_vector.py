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


def _reference_insider_sentiment(engine, as_of: date) -> float | None:
    """Pre-#713-review reference implementation: one ``read_window`` call
    per distinct ``INSIDER:*`` series_id, summed by ``:BUY``/``:SELL``
    suffix. Kept only as a parity oracle for the batched query that
    replaced it — same filters (SUCCESS-only, ``[cutoff, as_of]``), same
    vintage rule (latest pull wins), same fail-closed mixed-source skip.
    """
    from store.observations import MixedSourceError, read_window

    cutoff = as_of - timedelta(days=30)
    with engine.connect() as conn:
        series_ids = [
            r[0]
            for r in conn.execute(
                text(
                    "SELECT DISTINCT series_id FROM raw_series "
                    "WHERE series_id LIKE :insider_pat AND pull_status = 'SUCCESS' "
                    "AND obs_date >= :cutoff AND obs_date <= :as_of"
                ),
                {"insider_pat": "INSIDER:%", "cutoff": cutoff, "as_of": as_of},
            ).fetchall()
        ]

    buy_vol = 0.0
    sell_vol = 0.0
    with engine.connect() as conn:
        for sid in series_ids:
            is_buy = sid.endswith(":BUY")
            is_sell = sid.endswith(":SELL")
            if not (is_buy or is_sell):
                continue
            try:
                obs = read_window(conn, sid, start=cutoff, as_of=as_of)
            except MixedSourceError:
                continue
            total_val = sum(o.value for o in obs)
            if is_buy:
                buy_vol += total_val
            else:
                sell_vol += total_val

    total = buy_vol + sell_vol
    if total == 0:
        return None
    return float((buy_vol - sell_vol) / total)


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

    def test_mixed_source_series_excluded_from_sum(self, engine):
        """A series_id whose accepted rows come from two sources (the
        MixedSourceError scenario) must be dropped entirely, not mixed in —
        same fail-closed rule store.observations enforces one series at a
        time, now applied inside the batched query."""
        from intelligence.regime.state_vector import _get_insider_sentiment

        _insert(engine, "INSIDER:MIXED:x:BUY", date(2026, 9, 11), 5000.0, source_id=FRED_SRC)
        _insert(engine, "INSIDER:MIXED:x:BUY", date(2026, 9, 12), 5000.0, source_id=YF_SRC)
        _insert(engine, "INSIDER:CLEAN:y:SELL", date(2026, 9, 12), 300.0, source_id=FRED_SRC)

        sentiment = _get_insider_sentiment(engine, AS_OF)

        # MIXED contributes nothing; only CLEAN's 300 SELL counts ->
        # (0 - 300) / 300 = -1.0.
        assert sentiment == pytest.approx(-1.0)

    def test_batched_query_matches_reference_per_series_implementation(self, engine):
        """Parity check against the pre-#713-review per-series_id
        read_window loop this replaced: same fixture, same result."""
        from intelligence.regime.state_vector import _get_insider_sentiment

        _insert(engine, "INSIDER:AAA:jdoe:BUY", date(2026, 9, 10), 1000.0, ts_offset_h=0)
        _insert(engine, "INSIDER:AAA:jdoe:BUY", date(2026, 9, 10), 1200.0, ts_offset_h=5)  # revision
        _insert(engine, "INSIDER:BBB:msmith:SELL", date(2026, 9, 12), 400.0, ts_offset_h=0)
        _insert(engine, "INSIDER:CCC:x:BUY", date(2026, 9, 12), 0.0, status="FAILED")
        _insert(engine, "INSIDER:DDD:z:SELL", date(2026, 9, 5), 250.0, ts_offset_h=0)
        _insert(engine, "INSIDER:DDD:z:SELL", date(2026, 9, 6), 50.0, ts_offset_h=0)
        _insert(engine, "INSIDER:EEE:q:BUY", date(2026, 9, 1), 800.0, ts_offset_h=0)
        _insert(engine, "INSIDER:MIXED:x:BUY", date(2026, 9, 11), 9999.0, source_id=FRED_SRC)
        _insert(engine, "INSIDER:MIXED:x:BUY", date(2026, 9, 13), 9999.0, source_id=YF_SRC)
        _insert(engine, "INSIDER:OLD:jdoe:BUY", AS_OF - timedelta(days=60), 999.0)

        batched = _get_insider_sentiment(engine, AS_OF)
        reference = _reference_insider_sentiment(engine, AS_OF)

        assert batched == pytest.approx(reference)

    def test_only_one_query_executes(self, engine):
        """The N+1 this replaced issued one read_window call per distinct
        INSIDER:* series_id (~1,500 round trips at production volume for a
        single uncached GET) — this must now be exactly one statement."""
        from sqlalchemy import event

        from intelligence.regime.state_vector import _get_insider_sentiment

        _insert(engine, "INSIDER:AAA:jdoe:BUY", date(2026, 9, 10), 1000.0)
        _insert(engine, "INSIDER:BBB:msmith:SELL", date(2026, 9, 12), 400.0)
        _insert(engine, "INSIDER:CCC:z:BUY", date(2026, 9, 13), 200.0)

        statements: list[str] = []

        def record(_conn, _cursor, statement, _params, _context, _many):
            statements.append(statement.strip())

        event.listen(engine, "before_cursor_execute", record)
        try:
            _get_insider_sentiment(engine, AS_OF)
        finally:
            event.remove(engine, "before_cursor_execute", record)

        assert len(statements) == 1, statements
        assert statements[0].upper().startswith(("SELECT", "WITH"))


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


# ── Cadence-aware staleness (GRID-STALE-SOURCES-AUDIT-20260929.md §4) ────


class TestStaleThresholdDays:
    """Pure unit coverage of the per-series threshold table itself."""

    def test_monthly_fred_series_get_70_days(self):
        from intelligence.regime.state_vector import _stale_threshold_days

        for sid in ("UNRATE", "INDPRO", "TCU", "M2SL", "UMCSENT"):
            assert _stale_threshold_days(sid) == 70

    def test_unclassified_series_keeps_old_30_day_rule(self):
        from intelligence.regime.state_vector import _stale_threshold_days

        assert _stale_threshold_days("T10Y2Y") == 30
        assert _stale_threshold_days("VIXCLS") == 30
        assert _stale_threshold_days("DERIVED:SPY_MA_RATIO") == 30

    def test_quarterly_series_get_a_wider_window_than_monthly(self):
        from intelligence.regime.state_vector import (
            QUARTERLY_FRED_SERIES,
            _stale_threshold_days,
        )

        # No quarterly series in STATE_DIMENSIONS today, but the tier must
        # already behave correctly for the day one is added.
        assert QUARTERLY_FRED_SERIES == frozenset()
        assert _stale_threshold_days("SOME_QUARTERLY_SERIES") == 30  # unmapped -> default
        from intelligence.regime import state_vector as sv_mod

        try:
            sv_mod.QUARTERLY_FRED_SERIES = frozenset({"SOME_QUARTERLY_SERIES"})
            assert _stale_threshold_days("SOME_QUARTERLY_SERIES") == 160
        finally:
            sv_mod.QUARTERLY_FRED_SERIES = frozenset()


class TestComputeStateVectorMonthlyStaleness:
    """End-to-end: a monthly FRED series ~50 days old (always-stale under
    the old flat 30-day rule) must not appear in stale_dimensions, while a
    non-monthly series at the same age still would. Mirrors the audit's
    UNRATE finding — GRID had the September release within minutes, but
    the old rule called it stale every single day of the month.
    """

    def test_unrate_at_50_days_is_not_flagged_but_would_have_been_under_old_rule(
        self, engine, monkeypatch,
    ):
        from intelligence.regime import state_vector as sv_mod

        # _get_normalization_stats caches globally by design (module-level
        # _NORM_CACHE) — reset around this test so an earlier/later test's
        # engine never leaks in, and so this test's data doesn't leak out.
        monkeypatch.setattr(sv_mod, "_NORM_CACHE", None)

        as_of = date(2026, 9, 20)
        latest_obs = as_of - timedelta(days=50)  # >30d (old rule) but <70d (new rule)
        first_obs = latest_obs - timedelta(days=34)
        d = first_obs
        value = 4.0
        while d <= latest_obs:
            _insert(engine, "UNRATE", d, value, source_id=FRED_SRC)
            d += timedelta(days=1)
            value += 0.01  # tiny drift so std != 0

        sv = sv_mod.compute_state_vector(engine, as_of)

        assert "unemployment_level" not in sv.stale_dimensions
        assert "unemployment_dir" not in sv.stale_dimensions
        # Sanity: the old unconditional rule (30 days) would have caught
        # this — confirms the fix is doing real work, not a no-op.
        assert (as_of - latest_obs).days > 30

    def test_non_monthly_series_at_the_same_age_is_still_flagged(
        self, engine, monkeypatch,
    ):
        from intelligence.regime import state_vector as sv_mod

        monkeypatch.setattr(sv_mod, "_NORM_CACHE", None)

        as_of = date(2026, 9, 20)
        latest_obs = as_of - timedelta(days=50)
        first_obs = latest_obs - timedelta(days=104)  # ICSA needs 100 rows (default min_history)
        d = first_obs
        value = 200.0
        while d <= latest_obs:
            _insert(engine, "ICSA", d, value, source_id=FRED_SRC)
            d += timedelta(days=1)
            value += 0.5

        sv = sv_mod.compute_state_vector(engine, as_of)

        assert "initial_claims" in sv.stale_dimensions

    def test_unrate_at_90_days_is_flagged_stale(self, engine, monkeypatch):
        """The other side of the fix: 70 days is a wider window than the
        old flat 30, not an unconditional exemption. A monthly series
        genuinely missing its release for ~90 days (beyond the ~4-8 week
        publication lag the threshold covers) must still be flagged."""
        from intelligence.regime import state_vector as sv_mod

        monkeypatch.setattr(sv_mod, "_NORM_CACHE", None)

        as_of = date(2026, 9, 20)
        latest_obs = as_of - timedelta(days=90)  # > MONTHLY_STALE_DAYS (70)
        first_obs = latest_obs - timedelta(days=34)
        d = first_obs
        value = 4.0
        while d <= latest_obs:
            _insert(engine, "UNRATE", d, value, source_id=FRED_SRC)
            d += timedelta(days=1)
            value += 0.01

        sv = sv_mod.compute_state_vector(engine, as_of)

        assert "unemployment_level" in sv.stale_dimensions
        assert "unemployment_dir" in sv.stale_dimensions

    def test_pretend_quarterly_series_at_200_days_is_flagged_stale(self, engine, monkeypatch):
        """No dimension in STATE_DIMENSIONS is quarterly today, so this
        proves QUARTERLY_STALE_DAYS (160) is enforced -- not skipped or
        infinite -- the moment a series is classified quarterly, using
        ICSA (already wired to the 'initial_claims' dimension) as a
        stand-in via QUARTERLY_FRED_SERIES."""
        from intelligence.regime import state_vector as sv_mod

        monkeypatch.setattr(sv_mod, "_NORM_CACHE", None)
        monkeypatch.setattr(sv_mod, "QUARTERLY_FRED_SERIES", frozenset({"ICSA"}))

        as_of = date(2026, 9, 20)
        latest_obs = as_of - timedelta(days=200)  # > QUARTERLY_STALE_DAYS (160)
        first_obs = latest_obs - timedelta(days=104)  # ICSA needs 100 rows (default min_history)
        d = first_obs
        value = 200.0
        while d <= latest_obs:
            _insert(engine, "ICSA", d, value, source_id=FRED_SRC)
            d += timedelta(days=1)
            value += 0.5

        sv = sv_mod.compute_state_vector(engine, as_of)

        assert "initial_claims" in sv.stale_dimensions
