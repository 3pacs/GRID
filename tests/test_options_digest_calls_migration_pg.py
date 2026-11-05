"""Real-PostgreSQL proof for migrations/versions/options_digest_calls_20261104.py.

Runs in a throwaway schema (random name, dropped afterwards). Proves:

* upgrade creates the ledger and ``alerts.hundredx_digest._persist_calls``
  inserts into it, and ``_load_recent_calls`` reads it back;
* UPDATE, DELETE and TRUNCATE are refused (append-only, like the journal);
* the CHECKs refuse a one-sided quote and an out-of-range probability;
* downgrade refuses while any call is recorded.

Uses the shared ``pg_engine`` fixture's URL (``GRID_TEST_DB_URL``); skips when
no PostgreSQL is reachable.
"""

from __future__ import annotations

import importlib
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError

_MIGRATION_MODULE = "migrations.versions.options_digest_calls_20261104"


@pytest.fixture()
def scratch(pg_engine: Engine):
    schema = f"digest_calls_mig_{uuid4().hex[:12]}"
    with pg_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(pg_engine.url, connect_args={"options": f"-csearch_path={schema}"})
    try:
        yield engine
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


def _run(engine: Engine, fn_name: str) -> None:
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    migration = importlib.import_module(_MIGRATION_MODULE)
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


def _row(**overrides):
    from alerts import hundredx_digest as hx

    contract = {"strike": 17.0, "bid": 1.35, "ask": 2.40, "iv": 0.78, "oi": 53,
                "expiry": "2027-01-15", "quote_at": datetime.now(timezone.utc),
                "quote_source": "yfinance option_chain"}
    contract.update(overrides.pop("contract", {}))
    econ = hx._call_economics("PUT", contract["strike"], contract["bid"], contract["ask"],
                              18.27, 0.78, date(2027, 1, 15), date(2026, 11, 3))
    econ.update(overrides.pop("economics", {}))
    row = {"ticker": "OMER", "direction": "PUT", "contract": contract, "economics": econ,
           "spot": 18.27, "spot_source": "yfinance info.regularMarketPrice", "score": 6.07,
           "signal_date": date(2026, 11, 3),
           "signals": {"iv_skew": {"score": 10, "direction": "PUT", "value": 0.726}},
           "catalyst": {"status": "none_on_file"}, "llm_verdict": "PASS",
           "provenance": {"scanner": "test"}}
    row.update(overrides)
    return row


def test_persist_and_read_back(scratch):
    from alerts import hundredx_digest as hx

    _run(scratch, "upgrade")
    assert hx._persist_calls(scratch, "run-1", [_row(), _row(ticker="BEAM")]) == 2

    recent = hx._load_recent_calls(scratch, datetime.now(timezone.utc) - timedelta(hours=1))
    assert {r["ticker"] for r in recent} == {"OMER", "BEAM"}
    assert recent[0]["strike"] == 17.0 and recent[0]["expiry"] == date(2027, 1, 15)
    with scratch.connect() as conn:
        sig = conn.execute(text("SELECT signals->'iv_skew'->>'direction', breakeven "
                                "FROM options_digest_calls WHERE ticker = 'OMER'")).one()
    assert sig[0] == "PUT" and sig[1] == pytest.approx(14.60)


@pytest.mark.parametrize("stmt", [
    "UPDATE options_digest_calls SET ask = 0.01",
    "DELETE FROM options_digest_calls",
    "TRUNCATE options_digest_calls",
])
def test_ledger_is_append_only(scratch, stmt):
    from alerts import hundredx_digest as hx

    _run(scratch, "upgrade")
    hx._persist_calls(scratch, "run-1", [_row()])
    with pytest.raises(DBAPIError, match="append-only"):
        with scratch.begin() as conn:
            conn.execute(text(stmt))


@pytest.mark.parametrize("overrides", [
    {"contract": {"bid": 0.0}},
    {"contract": {"bid": 2.5, "ask": 2.4}},
    {"economics": {"prob_beyond_breakeven": 1.5}},
    {"direction": "STRADDLE"},
])
def test_checks_refuse_impossible_calls(scratch, overrides):
    from alerts import hundredx_digest as hx

    _run(scratch, "upgrade")
    with pytest.raises(DBAPIError):
        hx._persist_calls(scratch, "run-1", [_row(**overrides)])


def test_downgrade_refuses_with_recorded_calls(scratch):
    from alerts import hundredx_digest as hx

    _run(scratch, "upgrade")
    _run(scratch, "downgrade")      # empty: drops cleanly
    _run(scratch, "upgrade")
    hx._persist_calls(scratch, "run-1", [_row()])
    with pytest.raises(RuntimeError, match="append-only"):
        _run(scratch, "downgrade")


def test_track_record_queries_run_on_postgres(scratch):
    """load_digest_track_record's three reads parse and bind on real PG."""
    from alerts import hundredx_digest as hx
    from trading.options_tracker import load_digest_track_record

    _run(scratch, "upgrade")
    with scratch.begin() as conn:
        conn.execute(text("CREATE TABLE options_daily_signals (ticker TEXT, signal_date DATE, "
                          "spot_price DOUBLE PRECISION)"))
        conn.execute(text("CREATE TABLE options_snapshots (ticker TEXT, snap_date DATE, expiry DATE, "
                          "opt_type TEXT, strike DOUBLE PRECISION, bid DOUBLE PRECISION, "
                          "ask DOUBLE PRECISION)"))
        conn.execute(text("INSERT INTO options_snapshots VALUES "
                          "('OMER', :d, DATE '2027-01-15', 'put', 17, 1.6, 2.0)"),
                     {"d": datetime.now(timezone.utc).date()})
        conn.execute(text("INSERT INTO options_daily_signals VALUES "
                          "('BEAM', DATE '2026-10-15', 20.0)"))
    hx._persist_calls(scratch, "run-1", [
        _row(),
        _row(ticker="BEAM", contract={"strike": 27.0, "bid": 1.0, "ask": 1.2,
                                      "expiry": "2026-10-16"}),
    ])

    rec = load_digest_track_record(scratch, as_of=datetime.now(timezone.utc).date())

    by = {c["ticker"]: c for c in rec["calls"]}
    assert by["OMER"]["status"] == "OPEN"
    assert by["OMER"]["return"] == pytest.approx(1.8 / 2.40 - 1)
    assert by["BEAM"]["status"] == "SETTLED"
    assert by["BEAM"]["return"] == pytest.approx(7.0 / 1.2 - 1)
    assert rec["settled_hit_rate"] == 1.0
