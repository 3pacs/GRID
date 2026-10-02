"""scripts/audit_backdated_vintages.py: exact counts on PostgreSQL, read-only, DB-window refusal.

The PostgreSQL tests use only ``GRID_TEST_DB_URL`` (a disposable ``*_test``
database; production names are refused before connecting) and a throwaway
schema. Unset or unreachable: skipped, unless ``REQUIRE_BACKDATED_AUDIT_PG=1``
(the CI step), where a skip is a failure.
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from scripts import audit_backdated_vintages as audit

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
    """CREATE INDEX idx_raw_series_pull_timestamp ON raw_series (pull_timestamp DESC)""",
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

YF, TIINGO, FILL = 2, 524, 7
SPY, OTHER = 2791, 99
FIRST_PULL = datetime(2026, 3, 26, 4, 34, 21, tzinfo=timezone.utc)


@pytest.fixture()
def pg_engine():
    require = os.environ.get("REQUIRE_BACKDATED_AUDIT_PG") == "1"
    url = os.environ.get("GRID_TEST_DB_URL", "").strip()
    if not url:
        if require:
            pytest.fail("REQUIRE_BACKDATED_AUDIT_PG=1 but GRID_TEST_DB_URL is unset")
        pytest.skip("GRID_TEST_DB_URL unset")
    name = (make_url(url).database or "").lower()
    if not name.endswith("_test") and (name in _PROD_NAMES or name.startswith("grid") or not name):
        pytest.fail(f"refusing scratch schemas in {name!r}: not a *_test database")
    try:
        admin = create_engine(url, pool_pre_ping=True)
        with admin.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:  # pragma: no cover - environment dependent
        if require:
            pytest.fail(f"REQUIRE_BACKDATED_AUDIT_PG=1 but PostgreSQL is unreachable: {type(exc).__name__}")
        pytest.skip("PostgreSQL not available")
    schema = f"dfa_audit_{uuid4().hex[:12]}"
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


def _seed(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO source_catalog VALUES (2, 'yfinance'), (524, 'TIINGO'), (7, 'fill')"))
        conn.execute(text("INSERT INTO feature_registry VALUES (2791, 'spy_full'), (99, 'other_full')"))
        raw = [
            # first SUCCESS pull of a mapped series: 2026-03-26 04:34:21Z
            ("YF:SPY:close", YF, date(2021, 3, 26), FIRST_PULL, "SUCCESS"),
            ("YF:SPY:close", TIINGO, date(2026, 4, 1), datetime(2026, 4, 2, 6, tzinfo=timezone.utc), "SUCCESS"),
            # earlier, but not SUCCESS: must not count as the first pull
            ("YF:SPY:close", YF, date(2020, 1, 2), datetime(2025, 1, 1, tzinfo=timezone.utc), "FAILED"),
            # an unmapped series pulled earlier: must not count either
            ("YF:QQQ:close", YF, date(2020, 1, 2), datetime(2024, 1, 1, tzinfo=timezone.utc), "SUCCESS"),
        ]
        for sid, src, od, ts, status in raw:
            conn.execute(text("INSERT INTO raw_series (series_id, source_id, obs_date, pull_timestamp, value, "
                              "pull_status) VALUES (:s, :src, :od, :ts, 1.0, :st)"),
                         {"s": sid, "src": src, "od": od, "ts": ts, "st": status})
        rows = []
        # 3 backdated fill rows before the first pull (release = vintage = obs)
        for od in (date(2021, 3, 26), date(2021, 3, 29), date(2026, 3, 25)):
            rows.append((SPY, od, od, od, FILL))
        # 1 backdated row with an honest vintage, before the first pull
        rows.append((SPY, date(2024, 6, 3), date(2024, 6, 3), date(2026, 4, 2), FILL))
        # 1 same-day row after the first pull (release = obs, but GRID had the series)
        rows.append((SPY, date(2026, 4, 1), date(2026, 4, 1), date(2026, 4, 1), TIINGO))
        # 2 honest rows (release = vintage = pull date)
        rows.append((SPY, date(2021, 3, 26), date(2026, 3, 26), date(2026, 3, 26), YF))
        rows.append((SPY, date(2026, 4, 1), date(2026, 4, 2), date(2026, 4, 2), TIINGO))
        # another feature's backdated row: never counted for spy_full
        rows.append((OTHER, date(2021, 3, 26), date(2021, 3, 26), date(2021, 3, 26), FILL))
        for fid, od, rd, vd, src in rows:
            conn.execute(text("INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, "
                              "value, source_priority_used) VALUES (:f, :od, :rd, :vd, 1.0, :src)"),
                         {"f": fid, "od": od, "rd": rd, "vd": vd, "src": src})
        # one backdated fill row is already retracted; one honest row too (not counted)
        for od, vd in ((date(2021, 3, 29), date(2021, 3, 29)), (date(2021, 3, 26), date(2026, 3, 26))):
            conn.execute(text("INSERT INTO resolved_series_retractions (feature_id, obs_date, vintage_date, "
                              "reason, run_tag) VALUES (2791, :od, :vd, 'test', 'test')"), {"od": od, "vd": vd})


_OK_CLOCK = lambda: datetime(2026, 10, 1, 11, 0, tzinfo=timezone.utc)  # noqa: E731


def test_counts_are_exact(pg_engine):
    _seed(pg_engine)
    report = audit.run(pg_engine, ["spy_full", "missing_feature"], clock=_OK_CLOCK)
    assert report["read_only"] is True
    spy, missing = report["features"]
    assert "YF:SPY:close" in spy["series"]
    assert spy["feature_id"] == SPY
    assert spy["first_raw_pull"] == FIRST_PULL.isoformat()
    assert spy["total_rows"] == 7
    assert spy["backdated_rows"] == 5
    assert spy["backdated_vintage_too"] == 4
    assert spy["backdated_before_first_pull"] == 4
    assert (spy["backdated_obs_min"], spy["backdated_obs_max"]) == ("2021-03-26", "2026-04-01")
    by = {r["source_name"]: r for r in spy["by_source"]}
    assert by["fill"]["rows"] == 4 and by["fill"]["before_first_pull"] == 4 and by["fill"]["vintage_also_obs"] == 3
    assert by["TIINGO"]["rows"] == 1 and by["TIINGO"]["before_first_pull"] == 0
    assert spy["already_retracted"] == 1
    assert spy["error"] is None
    assert missing["error"] == "feature not in feature_registry"
    assert report["complete"] is True and report["stopped"] is None


def test_feature_without_raw_mapping_reports_no_first_pull(pg_engine):
    _seed(pg_engine)
    other = audit.run(pg_engine, ["other_full"], clock=_OK_CLOCK)["features"][0]
    assert other["series"] == ["YF:OTHER:close"]
    assert other["first_raw_pull"] is None
    assert other["backdated_rows"] == 1 and other["backdated_before_first_pull"] is None
    assert other["already_retracted"] == 0


def test_missing_retractions_table_reads_as_zero(pg_engine):
    _seed(pg_engine)
    with pg_engine.begin() as conn:
        conn.execute(text("DROP TABLE resolved_series_retractions"))
    spy = audit.run(pg_engine, ["spy_full"], clock=_OK_CLOCK)["features"][0]
    assert spy["error"] is None and spy["already_retracted"] == 0 and spy["backdated_rows"] == 5


def test_audit_transactions_are_read_only(pg_engine):
    _seed(pg_engine)
    with pg_engine.connect() as conn, conn.begin():
        audit._readonly(conn, 30_000)
        assert conn.execute(text("SHOW transaction_read_only")).scalar() == "on"
        assert conn.execute(text("SHOW statement_timeout")).scalar() == "30s"
        with pytest.raises(Exception, match="read-only"):
            conn.execute(text("INSERT INTO source_catalog VALUES (1, 'x')"))


def test_a_statement_timeout_is_reported_per_feature_not_raised(pg_engine, monkeypatch):
    _seed(pg_engine)
    # Force a statement that outlives the timeout; the next feature still runs.
    monkeypatch.setattr(audit, "_FIRST_PULL_SQL", text("SELECT pg_sleep(2), :sid"))
    report = audit.run(pg_engine, ["spy_full", "missing_feature"], timeout_ms=200, clock=_OK_CLOCK)
    slow, nxt = report["features"]
    assert slow["error"] is not None and "statement timeout" in slow["error"].lower(), slow["error"]
    assert nxt["error"] == "feature not in feature_registry"


def test_run_stops_when_it_reaches_the_db_window(pg_engine):
    _seed(pg_engine)
    ticks = iter([datetime(2026, 10, 1, 3, 29, tzinfo=timezone.utc),   # start: outside
                  datetime(2026, 10, 1, 3, 29, tzinfo=timezone.utc),   # feature 1: outside
                  datetime(2026, 10, 1, 3, 30, tzinfo=timezone.utc)])  # feature 2: inside
    report = audit.run(pg_engine, ["spy_full", "other_full"], clock=lambda: next(ticks))
    assert [f["feature"] for f in report["features"]] == ["spy_full"]
    assert report["complete"] is False and report["stopped"].startswith("db_window")


# ── no database needed ──────────────────────────────────────────────────


@pytest.mark.parametrize("hhmm, inside", [
    ((3, 29), False), ((3, 30), True), ((7, 0), True), ((10, 29), True), ((10, 30), False), ((23, 0), False),
])
def test_db_window_boundaries(hhmm, inside):
    now = datetime(2026, 10, 1, *hhmm, tzinfo=timezone.utc)
    assert audit.in_db_window(now) is inside


def test_refuses_inside_the_db_window_with_a_frozen_clock():
    frozen = lambda: datetime(2026, 10, 1, 5, 30, tzinfo=timezone.utc)  # noqa: E731

    class _NoEngine:
        def connect(self):
            raise AssertionError("connected inside the DB window")

    with pytest.raises(audit.DbWindowRefused):
        audit.run(_NoEngine(), ["spy_full"], clock=frozen)
    # a non-UTC clock inside the window (01:30 New York = 05:30Z) is refused too
    ny = datetime(2026, 10, 1, 1, 30, tzinfo=timezone(timedelta(hours=-4)))
    assert audit.in_db_window(ny)


def test_cli_refuses_inside_the_window(monkeypatch, capsys):
    class _Frozen(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 10, 1, 6, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(audit, "datetime", _Frozen)
    assert audit.main(["--feature", "spy_full", "--db-url", "postgresql://x@nowhere/none_test"]) == 2
    assert "refusing" in capsys.readouterr().err


def test_mapped_series_for_spy_full_and_overrides():
    assert "YF:SPY:close" in audit.mapped_series("spy_full")
    assert "YF:BRK-B:close" in audit.mapped_series("brk_b_full")
    assert "X:Y" in audit.mapped_series("spy_full", {"spy_full": ["X:Y"]})
