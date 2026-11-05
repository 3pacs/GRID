"""Real-PostgreSQL contract for the yf_latest_daily_vintage materialized view.

Proves, in a disposable schema with a tiny ``raw_series`` fixture, that
``migrations/versions/yf_latest_daily_vintage_20261019.py``:

* creates the view empty (``WITH NO DATA``) with the unique index, and the view
  refuses reads until populated;
* after ``scripts/refresh_yf_latest_vintage.py`` (first run: plain REFRESH),
  holds exactly one row per (series_id, obs_date) with the LATEST SUCCESS
  vintage of source 2 -- ignoring FAILED / QUARANTINED rows, other sources,
  dates before 2026-01-01 and tickers outside the list;
* carries capture_state / capture_policy / policy_marked / repair_run_tag from
  raw_payload (NULL / FALSE on legacy unmarked rows);
* refreshes CONCURRENTLY afterwards (under the planner hints) and picks up a
  newer vintage; and downgrades cleanly.

Skipped without GRID_TEST_DB_URL / DB_URL (disposable PostgreSQL).
"""

from __future__ import annotations

import os
import uuid
from contextlib import contextmanager
from datetime import date, datetime, timezone

import pytest
from sqlalchemy import create_engine, text

os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("DB_PASSWORD", "testpass")

_DB_URL = os.environ.get("GRID_TEST_DB_URL") or os.environ.get("DB_URL")
pytestmark = [
    pytest.mark.skipif(not _DB_URL, reason="requires disposable PostgreSQL via GRID_TEST_DB_URL or DB_URL"),
    pytest.mark.xdist_group("postgres"),
]

UTC = timezone.utc
TICKERS = ["SPY", "^GSPC", "HG=F"]


def _ts(day: int, hour: int) -> datetime:
    return datetime(2026, 10, day, hour, 0, tzinfo=UTC)


@contextmanager
def _schema():
    admin = create_engine(_DB_URL)
    name = "yfv_" + uuid.uuid4().hex[:12]
    with admin.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{name}"'))
    engine = create_engine(_DB_URL, connect_args={"options": f"-csearch_path={name}"})
    try:
        yield engine, name
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text(f'DROP SCHEMA "{name}" CASCADE'))
        admin.dispose()


def _seed(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE raw_series (
                id BIGSERIAL PRIMARY KEY, series_id TEXT NOT NULL, source_id INTEGER NOT NULL,
                obs_date DATE NOT NULL, pull_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                value DOUBLE PRECISION NOT NULL, raw_payload JSONB,
                pull_status TEXT NOT NULL
            )
        """))
        conn.execute(text("CREATE UNIQUE INDEX uq_raw_series_composite "
                          "ON raw_series (series_id, source_id, obs_date, pull_timestamp)"))
        conn.execute(text("CREATE INDEX idx_raw_series_dedup ON raw_series "
                          "(series_id, obs_date, pull_timestamp DESC) INCLUDE (value) "
                          "WHERE pull_status = 'SUCCESS'"))
        ins = text(
            "INSERT INTO raw_series (series_id, source_id, obs_date, pull_timestamp, value, raw_payload, pull_status) "
            "VALUES (:s, :src, :d, :ts, :v, CAST(:p AS JSONB), :st)"
        )
        rows = [
            # SPY close 10-14: three vintages; the latest SUCCESS one (v=102) is marked.
            ("YF:SPY:close", 2, date(2026, 10, 14), _ts(14, 15), 100.0, None, "SUCCESS"),
            ("YF:SPY:close", 2, date(2026, 10, 14), _ts(15, 6), 101.0,
             '{"capture_state": "INCOMPLETE", "capture_policy": "post_utc_day_end_v1"}', "SUCCESS"),
            ("YF:SPY:close", 2, date(2026, 10, 14), _ts(16, 6), 102.0,
             '{"capture_state": "POLICY_ELIGIBLE", "capture_policy": "post_utc_day_end_v1",'
             ' "repair_run_tag": "phase3"}', "SUCCESS"),
            # A FAILED zero written after the good vintage must never win.
            ("YF:SPY:close", 2, date(2026, 10, 14), _ts(17, 6), 0.0, None, "FAILED"),
            ("YF:SPY:close", 2, date(2026, 10, 14), _ts(17, 7), 999.0, None, "QUARANTINED"),
            # SPY close 10-15: one legacy unmarked row.
            ("YF:SPY:close", 2, date(2026, 10, 15), _ts(15, 21), 103.0, None, "SUCCESS"),
            # Other field, same day.
            ("YF:SPY:volume", 2, date(2026, 10, 14), _ts(16, 6), 5000.0, None, "SUCCESS"),
            # ^GSPC and HG=F (symbols with special characters).
            ("YF:^GSPC:close", 2, date(2026, 10, 14), _ts(16, 6), 5800.0, None, "SUCCESS"),
            ("YF:HG=F:close", 2, date(2026, 10, 14), _ts(16, 6), 4.5, '{"capture_policy": "x"}', "SUCCESS"),
            # Excluded: before 2026-01-01, other source, ticker not in the list, unknown field.
            ("YF:SPY:close", 2, date(2025, 12, 31), _ts(16, 6), 1.0, None, "SUCCESS"),
            ("YF:SPY:close", 3, date(2026, 10, 14), _ts(18, 6), 7777.0, None, "SUCCESS"),
            ("YF:ZZZZ:close", 2, date(2026, 10, 14), _ts(16, 6), 3.0, None, "SUCCESS"),
            ("YF:SPY:dividends", 2, date(2026, 10, 14), _ts(16, 6), 0.5, None, "SUCCESS"),
        ]
        conn.execute(ins, [
            {"s": s, "src": src, "d": d, "ts": ts, "v": v, "p": p, "st": st}
            for s, src, d, ts, v, p, st in rows
        ])


@pytest.fixture()
def env(monkeypatch):
    from migrations.versions import yf_latest_daily_vintage_20261019 as mig
    from scripts import refresh_yf_latest_vintage as refresh

    with _schema() as (engine, name):
        _seed(engine)
        qualified = f'"{name}".yf_latest_daily_vintage'
        # The migration hard-codes public.*; point both modules at the test schema.
        monkeypatch.setattr(mig, "VIEW_NAME", qualified)
        monkeypatch.setattr(refresh, "VIEW_NAME", qualified)
        monkeypatch.setattr(refresh, "REFRESH_SQL", f"REFRESH MATERIALIZED VIEW {qualified}")
        monkeypatch.setattr(refresh, "REFRESH_CONCURRENTLY_SQL",
                            f"REFRESH MATERIALIZED VIEW CONCURRENTLY {qualified}")
        monkeypatch.setattr(refresh, "_COUNT_SQL", f"SELECT count(*) FROM {qualified}")
        monkeypatch.setattr(mig, "load_tickers", lambda: list(TICKERS))
        yield mig, refresh, engine, qualified


def _migrate(mig, engine, fn: str) -> None:
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    with engine.connect() as conn:
        trans = conn.begin()
        try:
            real = mig.op
            mig.op = Operations(MigrationContext.configure(conn))
            try:
                getattr(mig, fn)()
            finally:
                mig.op = real
            trans.commit()
        except Exception:
            trans.rollback()
            raise


def _rows(engine, qualified):
    with engine.connect() as conn:
        res = conn.execute(text(
            f"SELECT series_id, obs_date, value, raw_series_id, capture_state, capture_policy, "
            f"policy_marked, repair_run_tag, ticker, field FROM {qualified} ORDER BY series_id, obs_date"
        ))
        return {(r.series_id, r.obs_date): r for r in res}


def test_create_populate_refresh_downgrade(env):
    mig, refresh, engine, qualified = env

    _migrate(mig, engine, "upgrade")
    _migrate(mig, engine, "upgrade")  # re-applicable (IF NOT EXISTS)

    with engine.connect() as conn:
        assert conn.execute(text(
            "SELECT relispopulated FROM pg_class WHERE oid = to_regclass(:v)"), {"v": qualified}).scalar() is False
        idx = conn.execute(text(
            "SELECT indexdef FROM pg_indexes WHERE indexname = 'uq_yf_latest_daily_vintage_series_obs' "
            "AND schemaname = current_schema()")).scalar()
        assert idx is not None and "UNIQUE" in idx and "(series_id, obs_date)" in idx
    with engine.connect() as conn, pytest.raises(Exception, match="has not been populated"):
        conn.execute(text(f"SELECT 1 FROM {qualified}"))

    # First run: unpopulated -> plain REFRESH.
    assert refresh.main([], engine=engine) == 0
    rows = _rows(engine, qualified)
    assert set(rows) == {
        ("YF:SPY:close", date(2026, 10, 14)), ("YF:SPY:close", date(2026, 10, 15)),
        ("YF:SPY:volume", date(2026, 10, 14)), ("YF:^GSPC:close", date(2026, 10, 14)),
        ("YF:HG=F:close", date(2026, 10, 14)),
    }
    spy14 = rows[("YF:SPY:close", date(2026, 10, 14))]
    assert spy14.value == 102.0                     # latest SUCCESS vintage, not FAILED 0 / QUARANTINED 999
    assert (spy14.ticker, spy14.field) == ("SPY", "close")
    assert spy14.capture_state == "POLICY_ELIGIBLE"
    assert spy14.capture_policy == "post_utc_day_end_v1"
    assert spy14.policy_marked is True
    assert spy14.repair_run_tag == "phase3"
    with engine.connect() as conn:
        picked = conn.execute(text("SELECT value FROM raw_series WHERE id = :i"),
                              {"i": spy14.raw_series_id}).scalar()
    assert picked == 102.0

    legacy = rows[("YF:SPY:close", date(2026, 10, 15))]
    assert legacy.value == 103.0
    assert legacy.capture_state is None and legacy.capture_policy is None
    assert legacy.policy_marked is False and legacy.repair_run_tag is None
    assert rows[("YF:HG=F:close", date(2026, 10, 14))].policy_marked is True
    assert rows[("YF:HG=F:close", date(2026, 10, 14))].capture_state is None
    assert (rows[("YF:^GSPC:close", date(2026, 10, 14))].ticker == "^GSPC")

    # A newer vintage lands; the second run is populated -> CONCURRENTLY (needs the unique index).
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO raw_series (series_id, source_id, obs_date, pull_timestamp, value, raw_payload, pull_status) "
            "VALUES ('YF:SPY:close', 2, DATE '2026-10-15', :ts, 104.5, "
            "CAST('{\"capture_state\": \"POLICY_ELIGIBLE\", \"capture_policy\": \"p\"}' AS JSONB), 'SUCCESS')"),
            {"ts": _ts(16, 22)})
    assert refresh.main([], engine=engine) == 0
    rows = _rows(engine, qualified)
    assert len(rows) == 5
    updated = rows[("YF:SPY:close", date(2026, 10, 15))]
    assert updated.value == 104.5 and updated.capture_state == "POLICY_ELIGIBLE"
    assert updated.policy_marked is True

    _migrate(mig, engine, "downgrade")
    with engine.connect() as conn:
        assert conn.execute(text("SELECT to_regclass(:v)"), {"v": qualified}).scalar() is None
    _migrate(mig, engine, "downgrade")              # idempotent
    assert refresh.main([], engine=engine) == 2     # view gone -> exit 2


def test_refresh_runs_inside_one_transaction_with_planner_hints(env):
    mig, refresh, engine, qualified = env
    _migrate(mig, engine, "upgrade")
    seen: dict[str, str] = {}
    original = refresh.refresh_statements

    def spy(populated):
        stmts = original(populated)
        seen["last"] = stmts[-1]
        return stmts

    refresh.refresh_statements = spy
    try:
        assert refresh.main([], engine=engine) == 0   # unpopulated -> plain
        assert refresh.main([], engine=engine) == 0   # populated -> concurrently
    finally:
        refresh.refresh_statements = original
    assert seen["last"].startswith("REFRESH MATERIALIZED VIEW CONCURRENTLY")
    # SET LOCAL must not leak out of the refresh transaction.
    with engine.connect() as conn:
        assert conn.execute(text("SHOW enable_hashjoin")).scalar() == "on"
        assert conn.execute(text("SHOW statement_timeout")).scalar() in ("0", "0ms")
