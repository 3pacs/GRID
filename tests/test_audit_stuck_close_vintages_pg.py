"""#1194 on a real PostgreSQL: the 2026-10-27 sp500_full shape, resolver and audit.

* An intraday pull resolved first, then a later window holding the
  POLICY_ELIGIBLE close: the resolver must add the close's vintage, with the
  real ``ON CONFLICT DO NOTHING`` skipping only the vintage that exists.
* ``scripts/audit_stuck_close_vintages.py`` reports the stuck cell read-only,
  and a resolver pass over the eligible pull's day clears it, insert-only.

Uses only ``GRID_TEST_DB_URL`` (a disposable ``*_test`` database; production
names are refused before connecting) and a throwaway schema. Unset or
unreachable: skipped, unless ``REQUIRE_STUCK_CLOSE_AUDIT_PG=1`` (the CI step),
where a skip is a failure.
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from normalization.resolver import Resolver
from price_close_contract import raw_capture_payload
from scripts import audit_stuck_close_vintages as audit

_PROD_NAMES = {"grid", "griddb", "grid_obsidian", "grid_v4", "gridprod", "postgres"}

_DDL = (
    """CREATE TABLE source_catalog (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE,
           priority_rank INTEGER NOT NULL)""",
    """CREATE TABLE feature_registry (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE,
           family TEXT NOT NULL DEFAULT 'equity')""",
    """CREATE TABLE raw_series (
           id BIGSERIAL PRIMARY KEY, series_id TEXT NOT NULL,
           source_id INTEGER NOT NULL REFERENCES source_catalog(id), obs_date DATE NOT NULL,
           pull_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(), value DOUBLE PRECISION NOT NULL,
           raw_payload JSONB, pull_status TEXT NOT NULL)""",
    """CREATE INDEX idx_raw_series_series_obs ON raw_series (series_id, obs_date)""",
    """CREATE TABLE resolved_series (
           id BIGSERIAL PRIMARY KEY, feature_id INTEGER NOT NULL REFERENCES feature_registry(id),
           obs_date DATE NOT NULL, release_date DATE NOT NULL, vintage_date DATE NOT NULL,
           value DOUBLE PRECISION NOT NULL,
           source_priority_used INTEGER NOT NULL REFERENCES source_catalog(id),
           conflict_flag BOOLEAN NOT NULL DEFAULT FALSE, conflict_detail JSONB,
           resolution_version INTEGER NOT NULL DEFAULT 1)""",
    """CREATE UNIQUE INDEX uq_resolved_series_composite ON resolved_series (feature_id, obs_date, vintage_date)""",
    """CREATE TABLE resolved_series_retractions (
           id BIGSERIAL PRIMARY KEY, feature_id INTEGER NOT NULL, obs_date DATE NOT NULL,
           vintage_date DATE NOT NULL, retracted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
           reason TEXT NOT NULL, run_tag TEXT NOT NULL,
           UNIQUE (feature_id, obs_date, vintage_date),
           FOREIGN KEY (feature_id, obs_date, vintage_date)
               REFERENCES resolved_series (feature_id, obs_date, vintage_date))""",
)

UTC = timezone.utc
GSPC = "YF:^GSPC:close"
YF = 2
SP500 = 686
OD = date(2026, 10, 27)
INTRADAY = datetime(2026, 10, 27, 13, 31, tzinfo=UTC)
LATE_SAME_DAY = datetime(2026, 10, 27, 22, 2, tzinfo=UTC)
ELIGIBLE = datetime(2026, 10, 28, 13, 31, tzinfo=UTC)
_OK_CLOCK = lambda: datetime(2026, 11, 14, 11, 0, tzinfo=UTC)  # noqa: E731


@pytest.fixture()
def pg_engine():
    require = os.environ.get("REQUIRE_STUCK_CLOSE_AUDIT_PG") == "1"
    url = os.environ.get("GRID_TEST_DB_URL", "").strip()
    if not url:
        if require:
            pytest.fail("REQUIRE_STUCK_CLOSE_AUDIT_PG=1 but GRID_TEST_DB_URL is unset")
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
            pytest.fail(f"REQUIRE_STUCK_CLOSE_AUDIT_PG=1 but PostgreSQL is unreachable: {type(exc).__name__}")
        pytest.skip("PostgreSQL not available")
    schema = f"stuck_close_{uuid4().hex[:12]}"
    with admin.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(admin.url, connect_args={"options": f"-csearch_path={schema} -ctimezone=UTC"})
    try:
        with engine.begin() as conn:
            for ddl in _DDL:
                conn.execute(text(ddl))
            conn.execute(text("INSERT INTO source_catalog VALUES (2, 'yfinance', 2)"))
            conn.execute(text("INSERT INTO feature_registry (id, name) VALUES (686, 'sp500_full')"))
        yield engine
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        admin.dispose()


def _pull(engine, ts: datetime, value: float) -> None:
    payload = raw_capture_payload("^GSPC", "close", OD, ts, "1d")
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO raw_series (series_id, source_id, obs_date, pull_timestamp, value, "
                          "raw_payload, pull_status) VALUES (:s, :src, :od, :ts, :v, CAST(:p AS jsonb), "
                          "'SUCCESS')"),
                     {"s": GSPC, "src": YF, "od": OD, "ts": ts, "v": value, "p": json.dumps(payload)})


def _resolved(engine) -> dict[date, float]:
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT vintage_date, value FROM resolved_series "
                                 "WHERE feature_id = :f AND obs_date = :od ORDER BY vintage_date"),
                            {"f": SP500, "od": OD}).fetchall()
    return {r[0]: r[1] for r in rows}


def _resolve(engine, since: datetime, until: datetime) -> dict:
    return Resolver(db_engine=engine).resolve_pending(since=since, until=until, series_ids=[GSPC], workers=1)


def test_an_intraday_pull_then_a_close_pull_yields_a_close_vintage(pg_engine):
    _pull(pg_engine, INTRADAY, 7701.57)
    first = _resolve(pg_engine, datetime(2026, 10, 27, tzinfo=UTC), datetime(2026, 10, 27, 14, tzinfo=UTC))
    assert first["errors"] == 0
    assert _resolved(pg_engine) == {date(2026, 10, 27): 7701.57}

    _pull(pg_engine, LATE_SAME_DAY, 7722.60)
    _pull(pg_engine, ELIGIBLE, 7722.60)
    # A wide catch-up window holding both vintages: the shape that lost the close before #978.
    second = _resolve(pg_engine, datetime(2026, 10, 27, tzinfo=UTC), datetime(2026, 10, 29, tzinfo=UTC))
    assert second["errors"] == 0
    assert second["resolved"] == 1  # the 10-27 vintage hit ON CONFLICT DO NOTHING; 10-28 is new
    assert _resolved(pg_engine) == {date(2026, 10, 27): 7701.57, date(2026, 10, 28): 7722.60}

    report = audit.run(pg_engine, [GSPC], date(2026, 10, 1), date(2026, 11, 1), clock=_OK_CLOCK)
    assert report["complete"] is True
    assert report["totals"]["stuck_cells"] == 0


def test_audit_finds_the_stuck_cell_and_a_resolver_pass_clears_it(pg_engine):
    # Production state since 2026-10-28: the three pulls, but only the intraday vintage resolved.
    for ts, value in ((INTRADAY, 7701.57), (LATE_SAME_DAY, 7722.60), (ELIGIBLE, 7722.60)):
        _pull(pg_engine, ts, value)
    with pg_engine.begin() as conn:
        conn.execute(text("INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value, "
                          "source_priority_used) VALUES (:f, :od, :od, :od, 7701.57, :src)"),
                     {"f": SP500, "od": OD, "src": YF})

    report = audit.run(pg_engine, [GSPC], date(2026, 10, 1), date(2026, 11, 1), clock=_OK_CLOCK)
    assert report["read_only"] is True and report["complete"] is True
    assert report["unmapped_series"] == []
    (feature,) = report["features"]
    assert feature["feature"] == "sp500_full" and feature["error"] is None
    assert feature["raw_rows"] == 3 and feature["cells_with_eligible"] == 1
    assert feature["stuck_cells"] == 1 and feature["eligible_vintage_missing_cells"] == 1
    (cell,) = feature["stuck"]
    assert (cell["served_value"], cell["eligible_value"]) == (7701.57, 7722.60)
    assert (cell["served_vintage"], cell["eligible_vintage"]) == ("2026-10-27", "2026-10-28")
    assert _resolved(pg_engine) == {date(2026, 10, 27): 7701.57}  # the audit wrote nothing

    # Insert-only repair: resolve the eligible pull's day again.
    fix = _resolve(pg_engine, datetime(2026, 10, 28, tzinfo=UTC), datetime(2026, 10, 29, tzinfo=UTC))
    assert fix["errors"] == 0 and fix["resolved"] == 1
    assert _resolved(pg_engine) == {date(2026, 10, 27): 7701.57, date(2026, 10, 28): 7722.60}
    after = audit.run(pg_engine, [GSPC], date(2026, 10, 1), date(2026, 11, 1), clock=_OK_CLOCK)
    assert after["totals"]["stuck_cells"] == 0


def test_audit_reports_unmapped_series_and_no_features(pg_engine):
    report = audit.run(pg_engine, ["YF:NOT_A_TICKER_1194:close"], date(2026, 10, 1), date(2026, 11, 1),
                       clock=_OK_CLOCK)
    assert report["unmapped_series"] == ["YF:NOT_A_TICKER_1194:close"]
    assert report["features"] == [] and report["complete"] is True
