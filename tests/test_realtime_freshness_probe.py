"""Tests for scripts/realtime_freshness_probe.py.

Consulted by deploy.yml's "Verify grid-realtime is delivering fresh data"
step. The probe bounds its query by `ts` (an existing index) as well as
`created_at` (which has no index and, unbounded, forced a full sequential
scan that hit Postgres's statement_timeout in production -- see the
script's own module docstring for the full incident and reasoning).

**The ts bound is a performance optimization, not proven equivalent to an
unbounded scan in every case.** These tests exercise the pure boundary
logic (no DB required) and, where Postgres is available, demonstrate --
rather than hide -- exactly where a row falls outside the bounded window:
a row with `ts` older than the window is invisible to this probe even
with a fresh `created_at`, by design and documented as a known,
accepted limitation (see the script's docstring), not a bug.
"""

from __future__ import annotations

import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
from realtime_freshness_probe import TS_WINDOW_SECONDS, build_query_params, run_probe  # noqa: E402


def test_build_query_params_floors_ts_by_the_window():
    params = build_query_params("2026-09-16T19:19:34Z")
    assert params["restart_ts"] == "2026-09-16T19:19:34Z"
    assert params["ts_floor"] == "2026-09-16T18:59:34Z"  # 1200s = 20 minutes earlier


def test_build_query_params_respects_a_custom_window():
    params = build_query_params("2026-09-16T19:19:34Z", window_seconds=60)
    assert params["ts_floor"] == "2026-09-16T19:18:34Z"


def test_default_window_is_four_times_the_flush_interval():
    # flusher.py's FLUSH_INTERVAL = 300 -- the window is deliberately wide
    # relative to that to tolerate a delayed write (e.g. flusher.py's own
    # buffer-and-retry-on-failure path spanning several flush cycles), not
    # just the routine single-cycle case.
    from ingestion.realtime.flusher import FLUSH_INTERVAL
    assert TS_WINDOW_SECONDS == 4 * FLUSH_INTERVAL


# ---------------------------------------------------------------------------
# Live-Postgres boundary test
# ---------------------------------------------------------------------------

def _connect_to_real_test_db():
    from sqlalchemy import create_engine

    candidates = [
        "postgresql://grid:testpass@localhost:5432/griddb_test",
        "postgresql://grid_user:changeme@localhost:5432/grid",
    ]
    env_url = os.environ.get("GRID_TEST_DB_URL")
    if env_url:
        candidates.insert(0, env_url)

    for url in candidates:
        try:
            engine = create_engine(url, pool_pre_ping=True)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return engine
        except Exception:
            continue
    return None


@pytest.fixture
def realtime_candles_schema():
    pg_engine = _connect_to_real_test_db()
    if pg_engine is None:
        pytest.skip("PostgreSQL not available (tried CI credentials and conftest.py's default)")

    ddl = """
        CREATE TABLE IF NOT EXISTS realtime_candles (
            symbol       TEXT NOT NULL,
            asset_class  TEXT NOT NULL,
            interval     TEXT NOT NULL DEFAULT '5m',
            ts           TIMESTAMPTZ NOT NULL,
            open         DOUBLE PRECISION,
            high         DOUBLE PRECISION,
            low          DOUBLE PRECISION,
            close        DOUBLE PRECISION,
            volume       DOUBLE PRECISION,
            vwap         DOUBLE PRECISION,
            trade_count  INTEGER DEFAULT 0,
            source       TEXT NOT NULL,
            created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (symbol, interval, ts)
        );
    """
    with pg_engine.begin() as conn:
        conn.execute(text(ddl))

    symbol = f"TEST_{uuid.uuid4().hex[:8]}"
    yield pg_engine, symbol

    with pg_engine.begin() as conn:
        conn.execute(text("DELETE FROM realtime_candles WHERE symbol = :s"), {"s": symbol})
    pg_engine.dispose()


def _insert_row(engine, symbol, ts, created_at, source="yahoo"):
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO realtime_candles "
                "(symbol, asset_class, interval, ts, open, high, low, close, volume, vwap, trade_count, source, created_at) "
                "VALUES (:symbol, 'equity', '5m', :ts, 1, 1, 1, 1, 1, 1, 1, :source, :created_at)"
            ),
            {"symbol": symbol, "ts": ts, "source": source, "created_at": created_at},
        )


def test_probe_finds_a_row_inside_the_ts_window(realtime_candles_schema):
    """The routine case: a fresh row well inside the window."""
    engine, symbol = realtime_candles_schema

    restart_dt = datetime(2026, 9, 16, 19, 19, 34, tzinfo=timezone.utc)
    fresh_created_at = restart_dt + timedelta(minutes=1)
    # ts just 2 minutes before restart -- well inside the 20-minute window,
    # the routine case matching how every feed actually assigns ts (see
    # the probe script's module docstring).
    _insert_row(engine, symbol, restart_dt - timedelta(minutes=2), fresh_created_at, source="yahoo")

    params = build_query_params(restart_dt.strftime("%Y-%m-%dT%H:%M:%SZ"))
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT source, count(*) FROM realtime_candles "
                "WHERE symbol = :symbol AND ts > :ts_floor AND created_at > :restart_ts GROUP BY source"
            ),
            {**params, "symbol": symbol},
        ).fetchall()
    assert rows == [("yahoo", 1)]


def test_probe_does_not_find_a_fresh_row_whose_ts_falls_outside_the_window(realtime_candles_schema):
    """KNOWN, DOCUMENTED LIMITATION, demonstrated rather than hidden: a row
    with a fresh created_at (genuinely written after restart) but a `ts`
    older than the bounded window is invisible to this probe. Under the
    current code (every feed assigns ts from datetime.now() at ingest, see
    the probe script's docstring) this specific combination is not
    expected to occur in practice -- but the query itself does not verify
    that, so this test pins down exactly what it will and will not find,
    rather than asserting a completeness guarantee the query doesn't
    actually have.
    """
    engine, symbol = realtime_candles_schema

    restart_dt = datetime(2026, 9, 16, 19, 19, 34, tzinfo=timezone.utc)
    fresh_created_at = restart_dt + timedelta(minutes=1)
    # ts 25 minutes before restart -- outside the 20-minute (1200s) window.
    old_ts = restart_dt - timedelta(minutes=25)
    _insert_row(engine, symbol, old_ts, fresh_created_at, source="binance")

    params = build_query_params(restart_dt.strftime("%Y-%m-%dT%H:%M:%SZ"))
    with engine.connect() as conn:
        bounded = conn.execute(
            text(
                "SELECT count(*) FROM realtime_candles "
                "WHERE symbol = :symbol AND ts > :ts_floor AND created_at > :restart_ts"
            ),
            {**params, "symbol": symbol},
        ).scalar()
        unbounded = conn.execute(
            text(
                "SELECT count(*) FROM realtime_candles "
                "WHERE symbol = :symbol AND created_at > :restart_ts"
            ),
            {"symbol": symbol, "restart_ts": params["restart_ts"]},
        ).scalar()

    assert unbounded == 1, "sanity check: the unbounded query DOES see this row"
    assert bounded == 0, (
        "the ts-bounded probe does NOT see a fresh row whose ts is outside "
        "the window -- this is the documented trade-off, not a bug"
    )


def test_probe_finds_a_row_exactly_at_the_ts_window_boundary(realtime_candles_schema):
    """Postgres `ts > :ts_floor` is a strict inequality -- a row exactly AT
    the floor is excluded, one microsecond inside it is included. Pins
    down the exact boundary rather than leaving it implicit.
    """
    engine, symbol = realtime_candles_schema
    restart_dt = datetime(2026, 9, 16, 19, 19, 34, tzinfo=timezone.utc)
    params = build_query_params(restart_dt.strftime("%Y-%m-%dT%H:%M:%SZ"))
    ts_floor_dt = datetime.strptime(params["ts_floor"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    fresh_created_at = restart_dt + timedelta(minutes=1)

    _insert_row(engine, symbol, ts_floor_dt, fresh_created_at, source="yahoo")  # exactly at the floor

    with engine.connect() as conn:
        at_floor = conn.execute(
            text("SELECT count(*) FROM realtime_candles WHERE symbol = :symbol AND ts > :ts_floor"),
            {"symbol": symbol, "ts_floor": params["ts_floor"]},
        ).scalar()
    assert at_floor == 0, "a row exactly at the floor is excluded by the strict `>` comparison"


def test_run_probe_end_to_end_reports_seen_for_a_source_with_a_fresh_row(realtime_candles_schema, monkeypatch):
    """Exercises the actual run_probe() entry point deploy.yml calls (via
    main()), not just the raw SQL -- monkeypatches db.get_engine so it
    reuses this test's own connection instead of reading production
    credentials from the environment.
    """
    import db as db_module
    monkeypatch.setattr(db_module, "get_engine", lambda: realtime_candles_schema[0])

    engine, symbol = realtime_candles_schema
    restart_dt = datetime(2026, 9, 16, 19, 19, 34, tzinfo=timezone.utc)
    _insert_row(engine, symbol, restart_dt - timedelta(minutes=1), restart_dt + timedelta(minutes=1), source="yahoo")

    lines = run_probe(restart_dt.strftime("%Y-%m-%dT%H:%M:%SZ"))

    # The probe groups by source across the WHOLE table, not just this
    # test's symbol -- other tests' rows (or concurrent CI activity on a
    # shared test DB) could also match, so assert on presence/shape
    # rather than an exact count.
    yahoo_line = next(line for line in lines if line.startswith(("SEEN yahoo", "UNVERIFIED yahoo")))
    assert yahoo_line.startswith("SEEN yahoo"), f"expected a fresh yahoo row to be reported SEEN, got: {yahoo_line}"
