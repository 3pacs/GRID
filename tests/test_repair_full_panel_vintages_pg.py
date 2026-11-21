"""scripts/repair_full_panel_vintages.py on PostgreSQL: dry run writes nothing; every apply entry point is disabled.

Same fixture discipline as tests/test_audit_backdated_vintages_pg.py: only
``GRID_TEST_DB_URL`` (a disposable ``*_test`` database; production names are
refused before connecting) and a throwaway schema. Unset or unreachable:
skipped, unless ``REQUIRE_FULL_PANEL_REPAIR_PG=1`` (the CI step), where a skip
is a failure.

Fixture: ``xlp_full`` (IN_SCOPE, alternating vintages + one dark cell with an
adj_close raw row), ``hon_full`` (IN_SCOPE but the adj_close feed starts late
-> NEEDS_BACKFILL), ``gold_full`` (allowlisted futures -> skipped, never read).
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

import scripts.repair_full_panel_vintages as rep

_PROD_NAMES = {"grid", "griddb", "grid_obsidian", "grid_v4", "gridprod", "postgres"}

_DDL = (
    """CREATE TABLE source_catalog (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)""",
    """CREATE TABLE feature_registry (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)""",
    """CREATE TABLE raw_series (
           id BIGSERIAL PRIMARY KEY, series_id TEXT NOT NULL,
           source_id INTEGER NOT NULL REFERENCES source_catalog(id), obs_date DATE NOT NULL,
           pull_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(), value DOUBLE PRECISION NOT NULL,
           raw_payload JSONB, pull_status TEXT NOT NULL)""",
    """CREATE INDEX idx_raw_series_series_id ON raw_series (series_id)""",
    """CREATE TABLE resolved_series (
           id BIGSERIAL PRIMARY KEY, feature_id INTEGER NOT NULL REFERENCES feature_registry(id),
           obs_date DATE NOT NULL, release_date DATE NOT NULL, vintage_date DATE NOT NULL,
           value DOUBLE PRECISION NOT NULL,
           source_priority_used INTEGER NOT NULL REFERENCES source_catalog(id),
           conflict_flag BOOLEAN NOT NULL DEFAULT FALSE)""",
    """CREATE UNIQUE INDEX uq_resolved_series_composite ON resolved_series (feature_id, obs_date, vintage_date)""",
    """CREATE TABLE resolved_series_retractions (
           id BIGSERIAL PRIMARY KEY, feature_id INTEGER NOT NULL, obs_date DATE NOT NULL,
           vintage_date DATE NOT NULL, retracted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
           reason TEXT NOT NULL, run_tag TEXT NOT NULL,
           UNIQUE (feature_id, obs_date, vintage_date),
           FOREIGN KEY (feature_id, obs_date, vintage_date)
               REFERENCES resolved_series (feature_id, obs_date, vintage_date))""",
)

YF, TIINGO = 2, 524
XLP, HON, GOLD = 7, 8, 9
V1, V2, V3 = date(2026, 4, 24), date(2026, 4, 29), date(2026, 5, 2)
_OK_CLOCK = lambda: datetime(2026, 11, 10, 15, 0, tzinfo=timezone.utc)  # noqa: E731


def _bdays(start: date, n: int) -> list[date]:
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


DAYS = _bdays(date(2024, 1, 2), 7)


@pytest.fixture()
def pg_engine():
    require = os.environ.get("REQUIRE_FULL_PANEL_REPAIR_PG") == "1"
    url = os.environ.get("GRID_TEST_DB_URL", "").strip()
    if not url:
        if require:
            pytest.fail("REQUIRE_FULL_PANEL_REPAIR_PG=1 but GRID_TEST_DB_URL is unset")
        pytest.skip("GRID_TEST_DB_URL unset")
    name = (make_url(url).database or "").lower()
    if not name.endswith("_test"):
        pytest.fail(f"refusing scratch schemas in {name!r}: not a *_test database")
    try:
        admin = create_engine(url, pool_pre_ping=True)
        with admin.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:  # pragma: no cover - environment dependent
        if require:
            pytest.fail(f"REQUIRE_FULL_PANEL_REPAIR_PG=1 but PostgreSQL is unreachable: {type(exc).__name__}")
        pytest.skip("PostgreSQL not available")
    schema = f"fp_repair_{uuid4().hex[:12]}"
    with admin.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(admin.url, connect_args={"options": f"-csearch_path={schema} -ctimezone=UTC"})
    try:
        with engine.begin() as conn:
            for ddl in _DDL:
                conn.execute(text(ddl))
        yield engine
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        admin.dispose()


def _ts(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, 6, tzinfo=timezone.utc)


def _seed(engine) -> None:
    raw_sql = text("INSERT INTO raw_series (series_id, source_id, obs_date, pull_timestamp, value, pull_status) "
                   "VALUES (:s, :src, :od, :ts, :v, 'SUCCESS')")
    res_sql = text("INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value, "
                   "source_priority_used) VALUES (:f, :od, :vd, :vd, :v, :src)")
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO source_catalog VALUES (2, 'yfinance'), (524, 'TIINGO')"))
        conn.execute(text("INSERT INTO feature_registry VALUES (7, 'xlp_full'), (8, 'hon_full'), (9, 'gold_full')"))
        for i, d in enumerate(DAYS):
            adj, close = 70.0 + i, 78.0 + i
            # xlp_full: both fields pulled on V1 and V3, close only on V2
            for vd in (V1, V3):
                conn.execute(raw_sql, {"s": "YF:XLP:adj_close", "src": TIINGO, "od": d, "ts": _ts(vd), "v": adj})
            for vd in (V1, V2, V3):
                conn.execute(raw_sql, {"s": "YF:XLP:close", "src": TIINGO, "od": d, "ts": _ts(vd), "v": close})
            if i < 6:
                conn.execute(res_sql, {"f": XLP, "od": d, "vd": V1, "v": adj, "src": TIINGO})
                conn.execute(res_sql, {"f": XLP, "od": d, "vd": V2, "v": close, "src": TIINGO})
                conn.execute(res_sql, {"f": XLP, "od": d, "vd": V3, "v": close if i % 2 == 0 else adj, "src": TIINGO})
            else:
                # the 7th cell: raw-close vintage only -> goes dark, re-vintaged from adj_close
                conn.execute(res_sql, {"f": XLP, "od": d, "vd": V2, "v": close, "src": TIINGO})
            # hon_full: every vintage from the raw close; adj_close feed exists for the last 2 cells only
            conn.execute(raw_sql, {"s": "YF:HON:close", "src": TIINGO, "od": d, "ts": _ts(V2), "v": 200.0 + i})
            if i >= 5:
                conn.execute(raw_sql, {"s": "YF:HON:adj_close", "src": TIINGO, "od": d, "ts": _ts(V3), "v": 190.0 + i})
            conn.execute(res_sql, {"f": HON, "od": d, "vd": V2, "v": 200.0 + i, "src": TIINGO})
            # gold_full rows exist too; the script must never plan them against Barrick
            conn.execute(res_sql, {"f": GOLD, "od": d, "vd": V2, "v": 2000.0 + i, "src": YF})
            conn.execute(raw_sql, {"s": "YF:GOLD:adj_close", "src": TIINGO, "od": d, "ts": _ts(V3), "v": 30.0})


def _counts(engine) -> dict[str, int]:
    with engine.connect() as conn:
        return {
            "resolved": conn.execute(text("SELECT count(*) FROM resolved_series")).scalar(),
            "retractions": conn.execute(text("SELECT count(*) FROM resolved_series_retractions")).scalar(),
            "resolved_checksum": conn.execute(text("SELECT coalesce(sum(value * id), 0) FROM resolved_series")).scalar(),
        }


FEATURES = ["xlp_full", "hon_full", "gold_full"]


def test_dry_run_plans_and_writes_nothing(pg_engine):
    _seed(pg_engine)
    before = _counts(pg_engine)
    report = rep.run(pg_engine, FEATURES, start=date(2010, 1, 1), apply=False, run_tag=None, clock=_OK_CLOCK)
    assert _counts(pg_engine) == before
    xlp, hon, gold = report["features"]
    assert xlp["status"] == rep.IN_SCOPE and xlp["applicable"] is True
    assert xlp["ticker"] == "XLP" and xlp["feed"] == "static"
    # 6 raw-only V2 rows + 3 even-cell V3 raw rows + the 7th cell's V2 row
    assert xlp["by_class"] == {rep.ON_BASIS: 9, rep.OFF_BASIS: 10}
    assert xlp["retract"] == 10 and xlp["revintage"] == 1
    assert (xlp["cells_dark_before"], xlp["cells_newly_dark"], xlp["cells_restored"], xlp["cells_left_dark"]) == (0, 0, 0, 0)
    assert xlp["served_after"]["ratio"] == pytest.approx(76.0 / 70.0)
    assert hon["status"] == rep.NEEDS_BACKFILL and hon["applicable"] is False
    assert hon["retract"] == 7 and hon["revintage"] == 2 and hon["cells_newly_dark"] == 5
    assert hon["newly_dark_range"] == {"obs_min": DAYS[0].isoformat(), "obs_max": DAYS[4].isoformat(), "n": 5}
    assert "backfill YF:HON:adj_close for obs_date" in hon["backfill_hint"]
    assert gold["status"] == rep.NO_BASIS_SPLIT and gold["ticker"] == "GC=F"
    assert gold["rows"] == 0 and gold["retract"] == 0 and gold["feature_id"] is None
    assert report["by_status"] == {rep.IN_SCOPE: 1, rep.NEEDS_BACKFILL: 1, rep.NO_BASIS_SPLIT: 1}
    assert report["applicable_totals"] == {"features": 1, "retract": 10, "revintage": 1, "newly_dark": 0}
    assert report["totals"]["phantom_cleared"] == 0 and report["totals"]["newly_dark"] == 5
    assert report["complete"] is True and report["errors"] == 0 and report["mode"] == "dry_run"


def test_apply_is_disabled_and_writes_nothing(pg_engine):
    _seed(pg_engine)
    before = _counts(pg_engine)
    with pytest.raises(RuntimeError, match="apply disabled"):
        rep.run(pg_engine, FEATURES, start=date(2010, 1, 1), apply=True, run_tag="pgtest", clock=_OK_CLOCK)
    plan = rep.plan_feature_db(pg_engine, "xlp_full", start=date(2010, 1, 1),
                               repair_day=date(2026, 11, 10), timeout_ms=5000)
    with pytest.raises(RuntimeError, match="apply disabled"):
        rep.apply_plan(pg_engine, plan, run_tag="pgtest", timeout_ms=5000)
    assert _counts(pg_engine) == before


def test_exclude_keeps_an_in_scope_feature_untouched(pg_engine):
    _seed(pg_engine)
    before = _counts(pg_engine)
    report = rep.run(pg_engine, ["xlp_full"], start=date(2010, 1, 1), apply=False, run_tag="t",
                     exclude=["xlp_full"], clock=_OK_CLOCK)
    assert report["features"][0]["status"] == rep.EXCLUDED and report["features"][0]["rows"] == 0
    assert _counts(pg_engine) == before


def test_raw_reads_use_one_short_read_only_transaction_per_chunk(pg_engine, monkeypatch):
    _seed(pg_engine)
    seen: list[tuple[str, str, date, date]] = []
    real = rep.read_raw_chunk

    def spy(conn, sid, lo, hi, into):
        seen.append((conn.execute(text("SHOW transaction_read_only")).scalar(), sid, lo, hi))
        return real(conn, sid, lo, hi, into)

    monkeypatch.setattr(rep, "read_raw_chunk", spy)
    raw = rep.read_raw_by_field(pg_engine, "XLP", date(2023, 6, 1), date(2024, 3, 1), timeout_ms=5_000)
    # 2 fields x 2 calendar-year chunks, each in its own READ ONLY transaction
    assert len(seen) == 4 and all(s[0] == "on" for s in seen)
    assert {s[1] for s in seen} == {"YF:XLP:close", "YF:XLP:adj_close"}
    assert sorted({(s[2], s[3]) for s in seen}) == [(date(2023, 6, 1), date(2023, 12, 31)), (date(2024, 1, 1), date(2024, 3, 1))]
    assert len(raw["adj_close"]) == 7 and all(len(v) == 2 for v in raw["adj_close"].values())


def test_missing_retractions_table_refuses_apply_per_feature(pg_engine):
    _seed(pg_engine)
    with pg_engine.begin() as conn:
        conn.execute(text("DROP TABLE resolved_series_retractions"))
    with pytest.raises(RuntimeError, match="apply disabled"):
        rep.run(pg_engine, ["xlp_full"], start=date(2010, 1, 1), apply=True, run_tag="t", clock=_OK_CLOCK)
    with pg_engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM resolved_series WHERE vintage_date = '2026-11-10'")).scalar() == 0


def test_run_stops_at_the_db_window(pg_engine):
    _seed(pg_engine)
    ticks = iter([datetime(2026, 10, 1, 3, 29, tzinfo=timezone.utc),
                  datetime(2026, 10, 1, 3, 29, tzinfo=timezone.utc),
                  datetime(2026, 10, 1, 3, 30, tzinfo=timezone.utc)])
    report = rep.run(pg_engine, ["xlp_full", "hon_full"], start=date(2010, 1, 1), apply=False, run_tag=None,
                     clock=lambda: next(ticks))
    assert [f["feature"] for f in report["features"]] == ["xlp_full"]
    assert report["complete"] is False and report["stopped"].startswith("db_window")
