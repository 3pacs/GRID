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
def scan(monkeypatch):
    """The scan-fix revision, pointed at the same test schema/tickers as `env`."""
    from migrations.versions import yf_latest_vintage_scan_20261026 as scan_mig

    return scan_mig


@pytest.fixture()
def env(monkeypatch, scan):
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
        monkeypatch.setattr(scan, "VIEW_NAME", qualified)
        monkeypatch.setattr(scan, "load_tickers", lambda: list(TICKERS))
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

    def spy(populated, *args, **kwargs):
        stmts = original(populated, *args, **kwargs)
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


# ── scan fix (yf_latest_vintage_scan_20261026) ───────────────────────────


def _many_vintages(engine) -> None:
    """Dozens of vintages per date, with every awkward case the definition must handle."""
    with engine.begin() as conn:
        ins = text(
            "INSERT INTO raw_series (series_id, source_id, obs_date, pull_timestamp, value, raw_payload, pull_status) "
            "VALUES (:s, :src, :d, :ts, :v, CAST(:p AS JSONB), :st)"
        )
        rows = []
        base = datetime(2026, 3, 1, tzinfo=timezone.utc)
        from datetime import timedelta

        for k in range(40):                                  # 40 pulls, each re-writes 5 dates of SPY close
            for day in range(5):
                rows.append(("YF:SPY:close", 2, date(2026, 3, 2 + day), base + timedelta(hours=3 * k, minutes=day),
                             100.0 + k, '{"capture_state": "INCOMPLETE", "capture_policy": "p"}' if k % 2 else None,
                             "SUCCESS"))
        # Newest vintage of one date is FAILED / PARTIAL / QUARANTINED: the latest SUCCESS one must win.
        rows += [
            ("YF:SPY:close", 2, date(2026, 3, 2), base + timedelta(days=30), 0.0, None, "FAILED"),
            ("YF:SPY:close", 2, date(2026, 3, 3), base + timedelta(days=30), 1.0, None, "PARTIAL"),
            ("YF:SPY:close", 2, date(2026, 3, 4), base + timedelta(days=30), 2.0, None, "QUARANTINED"),
            # A date whose only vintages are not SUCCESS yields no row.
            ("YF:SPY:close", 2, date(2026, 3, 20), base + timedelta(days=1), 0.0, None, "FAILED"),
            ("YF:SPY:close", 2, date(2026, 3, 20), base + timedelta(days=2), 0.0, None, "QUARANTINED"),
            # Another source's SUCCESS vintage that falls BETWEEN source 2's latest SUCCESS and its newer
            # PARTIAL one (2026-03-03): the probe must not pick it up (volume collision case).
            ("YF:SPY:close", 3, date(2026, 3, 3), base + timedelta(days=29), 4242.0, None, "SUCCESS"),
            # Another source, newer than every source-2 vintage: must be ignored (volume collision case).
            ("YF:SPY:close", 3, date(2026, 3, 5), base + timedelta(days=40), 9999.0, None, "SUCCESS"),
            ("YF:^GSPC:close", 3, date(2026, 3, 5), base + timedelta(days=40), 9999.0, None, "SUCCESS"),
            # Before the first date.
            ("YF:SPY:close", 2, date(2025, 12, 31), base, 1.0, None, "SUCCESS"),
            # Repair vintage with a run tag, newest SUCCESS of its date.
            ("YF:HG=F:close", 2, date(2026, 3, 2), base, 4.0, None, "SUCCESS"),
            ("YF:HG=F:close", 2, date(2026, 3, 2), base + timedelta(days=2), 4.4,
             '{"repair_run_tag": "phase3", "capture_policy": "p", "capture_state": "POLICY_ELIGIBLE"}', "SUCCESS"),
        ]
        conn.execute(ins, [
            {"s": s, "src": src, "d": d, "ts": ts, "v": v, "p": p, "st": st}
            for s, src, d, ts, v, p, st in rows
        ])
        conn.execute(text("ANALYZE raw_series"))


def _select_rows(engine, select_sql):
    with engine.connect() as conn:
        res = conn.execute(text(select_sql))
        return sorted(tuple(r) for r in res)


def test_scan_fix_returns_exactly_the_rows_of_the_old_definition(env, scan):
    mig, _refresh, engine, _q = env
    _many_vintages(engine)
    old_rows = _select_rows(engine, mig.build_select_sql(list(TICKERS)))
    new_rows = _select_rows(engine, scan.build_select_sql(list(TICKERS)))
    assert len(old_rows) > 10
    assert new_rows == old_rows                   # every column of every row, both directions
    keys = [(r[0], r[3]) for r in new_rows]       # (series_id, obs_date)
    assert len(keys) == len(set(keys))            # one row per series/date
    by_key = {(r[0], r[3]): r for r in new_rows}
    assert ("YF:SPY:close", date(2026, 3, 20)) not in by_key            # only non-SUCCESS vintages
    assert ("YF:SPY:close", date(2025, 12, 31)) not in by_key           # before 2026-01-01
    assert by_key[("YF:SPY:close", date(2026, 3, 2))][6] == 139.0       # newest FAILED ignored: k=39 SUCCESS
    assert by_key[("YF:SPY:close", date(2026, 3, 5))][6] == 139.0       # source 3 (9999) ignored
    assert by_key[("YF:SPY:close", date(2026, 3, 3))][6] == 139.0       # source 3 (4242) between source 2's vintages ignored
    hg = by_key[("YF:HG=F:close", date(2026, 3, 2))]
    assert hg[6] == 4.4 and hg[10] == "phase3"


def test_scan_fix_plan_reads_the_index_only_and_probes_per_date(env, scan):
    _mig, _refresh, engine, _q = env
    _many_vintages(engine)
    with engine.connect() as conn:
        for hint in scan.PLANNER_HINTS:
            conn.execute(text(hint))
        plan = "\n".join(r[0] for r in conn.execute(text("EXPLAIN " + scan.build_select_sql(list(TICKERS)))))
        conn.rollback()
    assert "GroupAggregate" in plan or "HashAggregate" in plan
    assert "Index Only Scan using uq_raw_series_composite" in plan      # newest-per-date step: no heap
    assert "Limit" in plan                                              # one winner probe per date
    assert "Seq Scan" not in plan


def test_scan_fix_upgrade_downgrade_and_refresh(env, scan):
    mig, refresh, engine, qualified = env
    _many_vintages(engine)

    _migrate(mig, engine, "upgrade")                  # historical definition (V1)
    assert refresh.main([], engine=engine) == 0
    old_rows = _rows(engine, qualified)

    _migrate(scan, engine, "upgrade")                 # redefinition: drop + create WITH NO DATA
    with engine.connect() as conn:
        assert conn.execute(text("SELECT relispopulated FROM pg_class WHERE oid = to_regclass(:v)"),
                            {"v": qualified}).scalar() is False
        definition = conn.execute(text("SELECT pg_get_viewdef(to_regclass(:v))"), {"v": qualified}).scalar()
        idx = conn.execute(text("SELECT indexdef FROM pg_indexes WHERE indexname = :n AND schemaname = current_schema()"),
                           {"n": scan.UNIQUE_INDEX_NAME}).scalar()
    assert "max(r.pull_timestamp)" in definition and "DISTINCT ON" not in definition
    assert idx is not None and "UNIQUE" in idx and "(series_id, obs_date)" in idx
    _migrate(scan, engine, "upgrade")                 # re-applicable

    assert refresh.main([], engine=engine) == 0       # first run after the redefinition: plain REFRESH
    new_rows = _rows(engine, qualified)
    assert new_rows.keys() == old_rows.keys()
    assert {k: tuple(v) for k, v in new_rows.items()} == {k: tuple(v) for k, v in old_rows.items()}
    assert refresh.main([], engine=engine) == 0       # populated: REFRESH ... CONCURRENTLY (needs the unique index)

    _migrate(scan, engine, "downgrade")               # back to the historical definition, unpopulated
    with engine.connect() as conn:
        assert conn.execute(text("SELECT relispopulated FROM pg_class WHERE oid = to_regclass(:v)"),
                            {"v": qualified}).scalar() is False
        definition = conn.execute(text("SELECT pg_get_viewdef(to_regclass(:v))"), {"v": qualified}).scalar()
    assert "DISTINCT ON" in definition
    assert refresh.main([], engine=engine) == 0
    assert {k: tuple(v) for k, v in _rows(engine, qualified).items()} == {k: tuple(v) for k, v in old_rows.items()}
