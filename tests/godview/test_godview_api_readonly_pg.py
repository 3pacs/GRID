"""PostgreSQL contract for the god-view API (G8): real reads, PIT, honest states, SELECT-only.

Runs only against ``GRID_TEST_DB_URL`` (a disposable PostgreSQL; CI's
"Godview API read-only PostgreSQL contract" step fails when this file is
skipped). Every test builds its own throwaway schema from the real
``god_view_market_tables_20260918`` migration and, unless it is testing the
pre-G2 state, the real ``godview_writers_20260926`` (G2) migration. So the
columns the API reads are the production ones, not a hand-copied copy.

What is proven here:

* Before G2 (production today) every pillar is ``unavailable`` /
  ``schema_not_migrated`` and no legacy value leaks into the payload.
* After G2, legacy (``provenance IS NULL``) rows are never served; with no
  writer run the reason is ``never_run``.
* Point in time: a row is invisible before its ``available_at``; a ledger
  run is invisible before its ``finished_at``.
* Staleness, CFTC partial coverage, ledger-driven reasons (``writer_failed``,
  ``blocked_by_legacy_rows``) and the modeled GEX label.
* The real G3 writer's rows round-trip through the API with their values.
* Every statement the API issued was a ``SELECT``.
"""

from __future__ import annotations

import importlib
import json
import os
import uuid
from datetime import date, datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine

os.environ.setdefault("DB_PASSWORD", "testpass")

from api.auth import require_auth
from api.routers import godview as godview_router

pytestmark = pytest.mark.integration

_BASE_MIGRATION = "migrations.versions.god_view_market_tables_20260918"
_G2_MIGRATION = "migrations.versions.godview_writers_20260926"
_SHA = "test-sha-godview-g8"

_PREREQ_DDL = (
    """
    CREATE TABLE market_briefings (
        id SERIAL PRIMARY KEY, briefing_type TEXT NOT NULL, briefing_date DATE NOT NULL,
        content TEXT NOT NULL, snapshot_data JSONB, created_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    "CREATE TABLE insider_trades (id SERIAL PRIMARY KEY, trade_date DATE, trade_type TEXT)",
    """
    CREATE TABLE source_catalog (
        id SERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE,
        base_url TEXT NOT NULL DEFAULT 'https://fred.stlouisfed.org',
        cost_tier TEXT NOT NULL DEFAULT 'FREE', latency_class TEXT NOT NULL DEFAULT 'WEEKLY',
        pit_available BOOLEAN NOT NULL DEFAULT TRUE, revision_behavior TEXT NOT NULL DEFAULT 'RARE',
        trust_score TEXT NOT NULL DEFAULT 'HIGH', priority_rank INTEGER NOT NULL DEFAULT 10
    )
    """,
    """
    CREATE TABLE raw_series (
        id BIGSERIAL PRIMARY KEY, series_id TEXT NOT NULL,
        source_id INTEGER NOT NULL REFERENCES source_catalog(id), obs_date DATE NOT NULL,
        pull_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(), value DOUBLE PRECISION NOT NULL,
        raw_payload JSONB,
        pull_status TEXT NOT NULL CHECK (pull_status IN ('SUCCESS', 'PARTIAL', 'FAILED'))
    )
    """,
    "INSERT INTO source_catalog (name) VALUES ('FRED')",
)


def _run_migration(engine: Engine, module: str, fn_name: str) -> None:
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    migration = importlib.import_module(module)
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            real_op = migration.op
            migration.op = Operations(MigrationContext.configure(conn))
            try:
                getattr(migration, fn_name)()
            finally:
                migration.op = real_op
            trans.commit()
        except Exception:
            trans.rollback()
            raise


def _make_engine(*, with_g2: bool):
    db_url = os.environ.get("GRID_TEST_DB_URL")
    if not db_url:
        pytest.skip("GRID_TEST_DB_URL not set")
    root = create_engine(db_url, pool_pre_ping=True)
    try:
        with root.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:  # noqa: BLE001
        root.dispose()
        pytest.skip(f"GRID_TEST_DB_URL set but unreachable: {exc}")
    schema = f"godview_api_{uuid.uuid4().hex[:12]}"
    with root.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(
        db_url, pool_size=1, max_overflow=0, connect_args={"options": f"-csearch_path={schema}"}
    )
    with engine.begin() as conn:
        for ddl in _PREREQ_DDL:
            conn.execute(text(ddl))
    _run_migration(engine, _BASE_MIGRATION, "upgrade")
    if with_g2:
        _run_migration(engine, _G2_MIGRATION, "upgrade")
    return root, engine, schema


def _teardown(root, engine, schema) -> None:
    engine.dispose()
    with root.begin() as conn:
        conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    root.dispose()


@pytest.fixture
def g2_engine():
    root, engine, schema = _make_engine(with_g2=True)
    try:
        yield engine
    finally:
        _teardown(root, engine, schema)


@pytest.fixture
def pre_g2_engine():
    root, engine, schema = _make_engine(with_g2=False)
    try:
        yield engine
    finally:
        _teardown(root, engine, schema)


class _Api:
    """TestClient over the real router, recording every SQL statement it issues."""

    def __init__(self, engine: Engine):
        self.engine = engine
        self.statements: list[str] = []
        app = FastAPI()
        app.include_router(godview_router.router)
        app.dependency_overrides[require_auth] = lambda: "test-token"
        self.client = TestClient(app)

    def _record(self, _conn, _cursor, statement, _params, _context, _many):
        self.statements.append(statement.strip().upper())

    def get(self, path: str):
        event.listen(self.engine, "before_cursor_execute", self._record)
        try:
            with patch.object(godview_router, "get_db_engine", return_value=self.engine):
                resp = self.client.get(path)
        finally:
            event.remove(self.engine, "before_cursor_execute", self._record)
        assert resp.status_code == 200, resp.text
        return resp.json()

    def assert_select_only(self) -> None:
        assert self.statements, "the API issued no SQL at all"
        offenders = [s for s in self.statements if not s.startswith("SELECT")]
        assert not offenders, offenders


# ── fixture rows ───────────────────────────────────────────────────────────

T0 = datetime(2026, 9, 24, 21, 2, tzinfo=timezone.utc)  # H.4.1 for Wed 09-23 pulled Thu


def _run(conn, pillar: str, status: str, finished: datetime) -> str:
    run_id = str(uuid.uuid4())
    conn.execute(
        text(
            "INSERT INTO godview_runs (run_id, pillar, started_at, finished_at, status, "
            "rows_written, rows_skipped, reasons, code_sha) VALUES (CAST(:r AS UUID), :p, :s, :f, :st, "
            "0, 0, CAST(:reasons AS JSONB), :sha)"
        ),
        {"r": run_id, "p": pillar, "s": finished - timedelta(minutes=1), "f": finished, "st": status,
         "reasons": json.dumps({"x": 1}), "sha": _SHA},
    )
    return run_id


def _fed(conn, obs: date, net: float, *, run_id: str | None, available_at: datetime | None = None) -> None:
    """``run_id=None`` inserts a legacy (NULL-provenance) row."""
    release = datetime.combine(obs + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc) + timedelta(hours=20, minutes=30)
    conn.execute(
        text(
            "INSERT INTO fed_net_liquidity_daily (obs_date, fed_assets_walcl, treasury_tga_wtregen, "
            "reverse_repo_rrp, net_liquidity_usd_m, delta_1w_m, liquidity_regime, release_at, available_at, "
            "availability_basis, provenance, source_ref, run_id, code_sha) VALUES (:d, 6780000, 790000, 5375, :net, "
            ":d1w, :regime, :rel, :avail, :basis, :prov, CAST(:sref AS JSONB), CAST(:run AS UUID), :sha)"
        ),
        {
            "d": obs, "net": net, "d1w": None if run_id is None else -1000.0,
            "regime": "STABLE" if run_id is None else "insufficient_history",
            "rel": None if run_id is None else release,
            "avail": None if run_id is None else (available_at or release + timedelta(minutes=32)),
            "basis": None if run_id is None else "observed_acquisition",
            "prov": None if run_id is None else "measured",
            "sref": None if run_id is None else json.dumps({"inputs": [{"series_id": "WALCL"}]}),
            "run": run_id, "sha": None if run_id is None else _SHA,
        },
    )


def _cftc(conn, root: str, code: str | None, report: date, z3: float | None, *, run_id: str | None) -> None:
    release = datetime.combine(report + timedelta(days=3), datetime.min.time(), tzinfo=timezone.utc) + timedelta(hours=19, minutes=30)
    conn.execute(
        text(
            "INSERT INTO cftc_positioning_daily (report_date, contract_code, contract_name, asset_class, "
            "total_open_interest, commercial_long, commercial_short, commercial_net, noncommercial_long, "
            "noncommercial_short, noncommercial_net, spec_net_pct_oi, z_score_3y, percentile_3y, crowding_regime, "
            "release_at, available_at, availability_basis, provenance, source_ref, run_id, code_sha, "
            "cftc_market_code, market_name) VALUES (:d, :root, 'n', 'EQUITY', 1000, 400, 500, -100, 300, 100, 200, "
            "20.0, :z, :pct, :regime, :rel, :avail, :basis, :prov, CAST(:sref AS JSONB), CAST(:run AS UUID), :sha, "
            ":code, :mname)"
        ),
        {
            "d": report, "root": root, "z": z3, "pct": None if z3 is None else 90.0,
            "regime": ("NEUTRAL" if run_id is None else (None if z3 is None else "ELEVATED_LONG")),
            "rel": None if run_id is None else release,
            "avail": None if run_id is None else release + timedelta(hours=1),
            "basis": None if run_id is None else "observed_acquisition",
            "prov": None if run_id is None else "measured",
            "sref": None if run_id is None else json.dumps({"inputs": []}),
            "run": run_id, "sha": None if run_id is None else _SHA,
            "code": code, "mname": None if run_id is None else f"{root} market",
        },
    )


def _legacy_gex(conn, obs: date) -> None:
    conn.execute(
        text(
            "INSERT INTO dealer_gex_daily (obs_date, ticker, spot_price, net_gex_usd_m, call_gex_usd_m, "
            "put_gex_usd_m, gamma_flip_strike, spot_to_flip_pct, gex_regime, max_pain_strike, "
            "put_call_oi_ratio, atm_iv) VALUES (:d, 'SPY', 750, 123.0, 1, 1, 300, 1, 'LONG_GAMMA', 1, 1, 0.25)"
        ),
        {"d": obs},
    )


def _modeled_gex(conn, obs: date, run_id: str, completed: datetime) -> None:
    conn.execute(
        text(
            "INSERT INTO dealer_gex_daily (obs_date, ticker, spot, gex_aggregate, gamma_flip, regime, "
            "model_basis, sign_convention, chain_capture_batch_id, chain_capture_completed_at, spot_source, "
            "spot_receipt_id, release_at, available_at, availability_basis, provenance, source_ref, run_id, code_sha) "
            "VALUES (:d, 'SPY', 661.5, -2.5e9, NULL, 'SHORT_GAMMA', 'bs_own_dte', 'long calls / short puts', "
            "'batch-1', :c, 'astrogrid.price_close_receipt', 7, :c, :c, 'observed_acquisition', 'modeled', "
            "CAST('{}' AS JSONB), CAST(:run AS UUID), :sha)"
        ),
        {"d": obs, "c": completed, "run": run_id, "sha": _SHA},
    )


# ── tests ──────────────────────────────────────────────────────────────────


def test_pre_g2_schema_is_unavailable_and_serves_no_legacy_value(pre_g2_engine):
    with pre_g2_engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO fed_net_liquidity_daily (obs_date, fed_assets_walcl, treasury_tga_wtregen, "
            "reverse_repo_rrp, net_liquidity_usd_m, liquidity_regime) VALUES ('2026-09-16', 6780000, 790000, 5375, 5984625, 'STABLE')"
        ))
    api = _Api(pre_g2_engine)
    body = api.get("/api/v1/godview/latest")
    assert body["schema"]["migrated"] is False
    assert "godview_runs" in body["schema"]["missing"]
    for pillar in ("fed_liquidity", "cftc", "dealer_gex"):
        p = body["pillars"][pillar]
        assert p["status"] == "unavailable" and p["reason"] == "schema_not_migrated" and p["data"] is None
    assert "5984625" not in json.dumps(body) and "6780000" not in json.dumps(body)
    hist = api.get("/api/v1/godview/history?pillar=fed_liquidity&from=2026-01-01&to=2026-09-30")
    assert hist["entries"] == [] and hist["total"] == 0 and hist["reason"] == "schema_not_migrated"
    api.assert_select_only()


def test_legacy_rows_are_never_served_and_no_run_reads_never_run(g2_engine):
    with g2_engine.begin() as conn:
        _fed(conn, date(2026, 9, 16), 5_984_625.0, run_id=None)
        _cftc(conn, "ES", None, date(2026, 9, 8), -2.11, run_id=None)
        _legacy_gex(conn, date(2026, 9, 18))
    api = _Api(g2_engine)
    body = api.get("/api/v1/godview/latest")
    assert body["schema"] == {"migrated": True, "missing": []}
    for pillar in ("fed_liquidity", "cftc", "dealer_gex"):
        assert body["pillars"][pillar]["status"] == "unavailable"
        assert body["pillars"][pillar]["reason"] == "never_run"
    text_body = json.dumps(body)
    assert "5984625" not in text_body and "-2.11" not in text_body and "123.0" not in text_body
    for pillar in ("fed_liquidity", "cftc", "dealer_gex"):
        hist = api.get(f"/api/v1/godview/history?pillar={pillar}&from=2026-01-01&to=2026-09-30")
        assert hist["total"] == 0 and hist["entries"] == [] and hist["reason"] == "never_run"
    api.assert_select_only()


def test_fed_point_in_time_staleness_and_history(g2_engine):
    with g2_engine.begin() as conn:
        run = _run(conn, "fed_liquidity", "complete", T0 + timedelta(minutes=5))
        _fed(conn, date(2026, 9, 16), 5_900_000.0, run_id=run)
        _fed(conn, date(2026, 9, 23), 5_864_145.0, run_id=run, available_at=T0)
    api = _Api(g2_engine)

    # Before 09-23's row was acquired: only 09-16 is visible, and the run
    # (finished T0+5m) is not visible yet either.
    before = api.get("/api/v1/godview/latest?as_of=2026-09-24T21:00:00Z")["pillars"]["fed_liquidity"]
    assert before["status"] == "available" and before["as_of"] == "2026-09-16"
    assert before["data"]["net_liquidity_usd_m"] == 5_900_000.0
    assert before["last_run"] is None

    after = api.get("/api/v1/godview/latest?as_of=2026-09-24T21:10:00Z")["pillars"]["fed_liquidity"]
    assert after["status"] == "available" and after["as_of"] == "2026-09-23"
    assert after["available_at"] == "2026-09-24T21:02:00+00:00"
    assert after["availability_basis"] == "observed_acquisition" and after["provenance"] == "measured"
    assert after["data"]["delta_4w_m"] is None and after["data"]["unit"] == "USD millions"
    assert after["last_run"]["status"] == "complete" and after["last_run"]["code_sha"] == _SHA

    later = api.get("/api/v1/godview/latest?as_of=2026-09-26T12:00:00Z")["pillars"]["fed_liquidity"]
    assert later["as_of"] == "2026-09-23" and later["status"] == "available"
    old_only = api.get("/api/v1/godview/latest?as_of=2026-09-12")  # before 09-16 existed
    assert old_only["pillars"]["fed_liquidity"]["status"] == "unavailable"

    hist = api.get("/api/v1/godview/history?pillar=fed_liquidity&from=2026-09-01&to=2026-09-30&as_of=2026-09-24T21:00:00Z")
    assert [e["obs_date"] for e in hist["entries"]] == ["2026-09-16"]
    page = api.get("/api/v1/godview/history?pillar=fed_liquidity&from=2026-09-01&to=2026-09-30&limit=1")
    assert page["total"] == 2 and page["has_more"] is True and page["entries"][0]["obs_date"] == "2026-09-16"
    api.assert_select_only()


def test_fed_stale_after_nine_days(g2_engine):
    with g2_engine.begin() as conn:
        run = _run(conn, "fed_liquidity", "complete", datetime(2026, 9, 3, 22, tzinfo=timezone.utc))
        _fed(conn, date(2026, 9, 2), 5_950_000.0, run_id=run)
    api = _Api(g2_engine)
    fresh = api.get("/api/v1/godview/latest?as_of=2026-09-11")["pillars"]["fed_liquidity"]
    assert fresh["status"] == "available"
    stale = api.get("/api/v1/godview/latest?as_of=2026-09-12")["pillars"]["fed_liquidity"]
    assert stale["status"] == "stale" and stale["reason"] == "stale" and stale["available"] is True
    assert stale["data"]["net_liquidity_usd_m"] == 5_950_000.0
    api.assert_select_only()


def test_cftc_partial_coverage_and_market_history(g2_engine):
    with g2_engine.begin() as conn:
        run = _run(conn, "cftc", "complete", datetime(2026, 9, 25, 21, tzinfo=timezone.utc))
        _cftc(conn, "ES", "13874A", date(2026, 9, 15), 1.0, run_id=run)
        _cftc(conn, "ES", "13874A", date(2026, 9, 22), 1.5, run_id=run)
        _cftc(conn, "GC", "088691", date(2026, 9, 22), None, run_id=run)
        _cftc(conn, "CL", None, date(2026, 9, 22), 9.9, run_id=None)  # legacy: excluded
    api = _Api(g2_engine)
    p = api.get("/api/v1/godview/latest?as_of=2026-09-26")["pillars"]["cftc"]
    assert p["status"] == "partial" and p["coverage"]["available"] == 2 and p["coverage"]["tracked"] == 16
    markets = {m["market"]: m for m in p["data"]["markets"]}
    assert markets["ES"]["z_score_3y"] == 1.5 and markets["ES"]["report_date"] == "2026-09-22"
    assert markets["GC"]["z_score_3y"] is None and markets["GC"]["crowding_regime"] is None
    assert markets["CL"]["status"] == "unavailable" and "z_score_3y" not in markets["CL"]

    # Friday 09-25 19:30Z release: as of Friday morning only the 09-15 report exists.
    early = api.get("/api/v1/godview/latest?as_of=2026-09-25T12:00:00Z")["pillars"]["cftc"]
    assert {m["market"]: m.get("report_date") for m in early["data"]["markets"]}["ES"] == "2026-09-15"

    hist = api.get("/api/v1/godview/history?pillar=cftc&market=ES&from=2026-09-01&to=2026-09-30")
    assert [e["report_date"] for e in hist["entries"]] == ["2026-09-15", "2026-09-22"]
    assert all(e["market"] == "ES" for e in hist["entries"])
    api.assert_select_only()


def test_ledger_reasons_when_no_row_qualifies(g2_engine):
    with g2_engine.begin() as conn:
        _run(conn, "fed_liquidity", "partial_blocked_by_legacy", T0)
        _run(conn, "cftc", "failed", T0)
        _run(conn, "dealer_gex", "no_completed_capture", T0)
    api = _Api(g2_engine)
    pillars = api.get("/api/v1/godview/latest")["pillars"]
    assert pillars["fed_liquidity"]["reason"] == "blocked_by_legacy_rows"
    assert pillars["cftc"]["reason"] == "writer_failed"
    assert pillars["dealer_gex"]["reason"] == "no_completed_capture"
    assert pillars["dealer_gex"]["last_run"]["status"] == "no_completed_capture"
    assert all(p["data"] is None for p in pillars.values())
    api.assert_select_only()


def test_modeled_gex_row_is_served_as_an_estimate(g2_engine):
    completed = datetime(2026, 9, 17, 20, 31, tzinfo=timezone.utc)  # Thursday session
    with g2_engine.begin() as conn:
        run = _run(conn, "dealer_gex", "complete", completed + timedelta(minutes=20))
        _modeled_gex(conn, date(2026, 9, 17), run, completed)
    api = _Api(g2_engine)
    before = api.get("/api/v1/godview/latest?as_of=2026-09-17T20:00:00Z")["pillars"]["dealer_gex"]
    assert before["status"] == "unavailable" and before["data"] is None
    p = api.get("/api/v1/godview/latest?as_of=2026-09-18T15:00:00Z")["pillars"]["dealer_gex"]
    assert p["status"] == "available" and p["estimated"] is True and p["provenance"] == "modeled"
    assert p["basis"] == "bs_own_dte" and "not measured" in p["model_note"]
    assert p["data"]["gamma_flip"] is None and p["data"]["chain_capture_batch_id"] == "batch-1"
    # Monday 09-21: the previous session is Friday 09-18, so Thursday's row is stale.
    stale = api.get("/api/v1/godview/latest?as_of=2026-09-21T15:00:00Z")["pillars"]["dealer_gex"]
    assert stale["status"] == "stale"
    api.assert_select_only()


def test_real_fed_writer_rows_round_trip_through_the_api(g2_engine):
    from godview.fed_liquidity import compute_release_at, materialize_fed_liquidity

    obs = date(2026, 9, 16)
    release_at, _ = compute_release_at(obs)
    pulled = release_at + timedelta(hours=1)
    with g2_engine.begin() as conn:
        for sid, value in (("WALCL", 7_500_000.0), ("WTREGEN", 700_000.0), ("RRPONTSYD", 300.0)):
            conn.execute(
                text(
                    "INSERT INTO raw_series (series_id, source_id, obs_date, value, pull_timestamp, pull_status) "
                    "VALUES (:s, (SELECT id FROM source_catalog WHERE name = 'FRED'), :d, :v, :p, 'SUCCESS')"
                ),
                {"s": sid, "d": obs, "v": value, "p": pulled},
            )
    result = materialize_fed_liquidity(g2_engine, code_sha=_SHA, as_of_ts=pulled + timedelta(minutes=5))
    assert result.rows_written == 1

    api = _Api(g2_engine)
    as_of = (pulled + timedelta(minutes=10)).isoformat().replace("+00:00", "Z")
    p = api.get(f"/api/v1/godview/latest?as_of={as_of}")["pillars"]["fed_liquidity"]
    assert p["status"] == "available" and p["as_of"] == "2026-09-16"
    assert p["data"]["net_liquidity_usd_m"] == pytest.approx(6_500_000.0)
    assert p["data"]["rrp_usd_m"] == pytest.approx(300_000.0)
    assert p["data"]["liquidity_regime"] == "insufficient_history"
    assert [i["series_id"] for i in p["data"]["inputs"]] == ["WALCL", "WTREGEN", "RRPONTSYD"]
    api.assert_select_only()
