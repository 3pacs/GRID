"""Synthetic PostgreSQL G5 proof on the real G2 and append-only migrations.

Requires an explicitly supplied local disposable test DB. The unchanged shared
SPY receipt verifier is stubbed with synthetic receipts; existing receipt tests
own raw/resolved lineage verification. The engine's batch loader, spot gates,
computation, writer SQL, constraints and read-model mapping run unchanged.
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import date, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine.url import make_url

from godview import dealer_gex as dg
from godview.read_model import build_gex_pillar
from scripts import run_godview_writers as runner
from store.astrogrid import AstroGridStore
from tests.godview.test_dealer_gex_writer import BATCH_ID, COMPLETE, CUTOFF, DAY, SHA, START
from tests.test_godview_schema_migration_pg import _PREREQ_DDL, _run

pytestmark = pytest.mark.integration
_OPTIONS_DDL = """
    CREATE TABLE options_snapshots (
        id BIGSERIAL PRIMARY KEY, ticker TEXT NOT NULL, snap_date DATE NOT NULL,
        expiry DATE NOT NULL, opt_type TEXT NOT NULL CHECK (opt_type IN ('call', 'put')),
        strike DOUBLE PRECISION NOT NULL, last_price DOUBLE PRECISION, bid DOUBLE PRECISION,
        ask DOUBLE PRECISION, volume INTEGER, open_interest INTEGER, implied_vol DOUBLE PRECISION,
        in_the_money BOOLEAN, created_at TIMESTAMPTZ DEFAULT NOW(), capture_batch_id TEXT,
        capture_ordinal BIGINT, capture_started_at TIMESTAMPTZ, capture_completed_at TIMESTAMPTZ,
        provider_regular_market_at TIMESTAMPTZ, UNIQUE(ticker, snap_date, expiry, opt_type, strike)
    )
"""


@pytest.fixture
def gex_pg(monkeypatch):
    db_url = os.environ.get("GRID_TEST_DB_URL")
    if not db_url:
        pytest.skip("GRID_TEST_DB_URL required for disposable PostgreSQL G5 proof")
    url = make_url(db_url)
    if url.host not in {"localhost", "127.0.0.1"} or "test" not in (url.database or ""):
        pytest.fail("G5 proof requires a local disposable test database")
    admin = create_engine(url)
    schema = "godview_g5_" + uuid4().hex[:12]
    with admin.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(url, pool_size=1, max_overflow=0, pool_timeout=2,
                           connect_args={"options": f"-csearch_path={schema}"})
    try:
        with engine.begin() as conn:
            for sql in _PREREQ_DDL:
                conn.execute(text(sql))
            conn.execute(text(_OPTIONS_DDL))
        _run(engine, "migrations.versions.god_view_market_tables_20260918", "upgrade")
        _run(engine, "migrations.versions.godview_writers_20260926", "upgrade")
        _run(engine, "migrations.versions.options_append_only_20260930", "upgrade")
        monkeypatch.setattr(AstroGridStore, "_verified_spy_receipt", lambda self, conn, **kw: {
            "price": 600.0, "receipt_id": 11, "obs_date": DAY - timedelta(days=1),
            "available_at": START, "receipt_created_at": START, "release_date": DAY,
            "vintage_date": DAY, "conflict_flag": False,
        })
        yield engine
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        admin.dispose()


def seed(engine, *, batch_id=BATCH_ID, ordinal=1, registered=None, row_count=2, mixed=False,
         completed=COMPLETE, created=START):
    registered = registered or completed + timedelta(seconds=1)
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO options_capture_batches (capture_batch_id, ticker, snap_date,
                capture_ordinal, capture_started_at, capture_completed_at, row_count,
                capture_source, registered_at)
            VALUES (:batch, 'SPY', :day, :ordinal, :started, :completed, :count, 'synthetic_test', :registered)
        """), dict(batch=batch_id, day=DAY, ordinal=ordinal, started=START,
                   completed=completed, count=row_count, registered=registered))
        for leg in ("call", "put"):
            conn.execute(text("""
                INSERT INTO options_snapshots_all (ticker, snap_date, expiry, opt_type,
                    strike, open_interest, implied_vol, created_at, capture_batch_id,
                    capture_ordinal, capture_started_at, capture_completed_at, provider_regular_market_at)
                VALUES ('SPY', :day, :expiry, :leg, 600, 100, 0.2, :created, :batch,
                        :ordinal, :started, :completed, :quoted)
            """), dict(day=DAY, expiry=DAY + timedelta(days=7), leg=leg, created=created,
                       batch=batch_id, ordinal=ordinal,
                       started=START + (timedelta(seconds=1) if mixed and leg == "put" else timedelta()),
                       completed=completed, quoted=START))


def run(engine, **kwargs):
    return dg.materialize_dealer_gex(engine, code_sha=SHA, as_of_ts=CUTOFF, **kwargs)


def rows(engine, table):
    statements = {"gex": "SELECT * FROM dealer_gex_daily ORDER BY id",
                  "runs": "SELECT * FROM godview_runs ORDER BY started_at"}
    with engine.connect() as conn:
        return [dict(r) for r in conn.execute(text(statements[table])).mappings()]


def test_real_engine_native_row_read_model_and_idempotence(gex_pg):
    seed(gex_pg)
    assert run(gex_pg).rows_written == 1
    row = rows(gex_pg, "gex")[0]
    assert row["provenance"] == "modeled" and row["gex_aggregate"] == 0
    assert row["gamma_flip"] is None and row["coverage_fraction"] == 1
    assert row["net_gex_usd_m"] is None and row["atm_iv"] is None
    assert row["release_at"] == COMPLETE and row["available_at"] > COMPLETE
    card = build_gex_pillar(row, rows(gex_pg, "runs")[0], CUTOFF)
    assert card["estimated"] is True and card["data"]["gex_aggregate"] == 0
    assert run(gex_pg).run_status == "noop" and rows(gex_pg, "gex") == [row]


def test_newer_batch_replaces_one_row_and_older_cutoff_cannot_overwrite(gex_pg):
    seed(gex_pg)
    run(gex_pg)
    newer = str(uuid4())
    seed(gex_pg, batch_id=newer, ordinal=2, registered=CUTOFF)
    assert run(gex_pg).rows_written == 1
    old = dg.materialize_dealer_gex(gex_pg, code_sha=SHA,
                                   as_of_ts=COMPLETE + timedelta(seconds=2))
    assert old.rows_written == 0 and old.run_status == "noop"
    assert len(rows(gex_pg, "gex")) == 1 and rows(gex_pg, "gex")[0]["chain_capture_batch_id"] == newer


@pytest.mark.parametrize("earlier_completion", [True, False])
def test_higher_ordinal_with_regressing_clocks_preserves_existing_row(gex_pg, earlier_completion):
    # Completion regression and independently availability regression.
    seed(gex_pg, registered=COMPLETE + timedelta(seconds=1) if earlier_completion else CUTOFF)
    assert run(gex_pg).rows_written == 1
    before = rows(gex_pg, "gex")
    seed(gex_pg, batch_id=str(uuid4()), ordinal=2,
         completed=START + timedelta(seconds=30) if earlier_completion else COMPLETE)
    result = run(gex_pg)
    assert result.run_status == "inputs_stale" and result.rows[0].reason == "nonmonotonic_capture_clocks"
    assert rows(gex_pg, "gex") == before


@pytest.mark.parametrize("kwargs", [{"row_count": 3}, {"mixed": True}])
def test_incomplete_or_mixed_latest_batch_never_falls_back(gex_pg, kwargs):
    seed(gex_pg)
    seed(gex_pg, batch_id=str(uuid4()), ordinal=2, **kwargs)
    assert run(gex_pg).run_status == "no_completed_capture"
    assert rows(gex_pg, "gex") == []


@pytest.mark.parametrize("receipt", [None, {"price": 600.0, "receipt_id": 11,
                                          "obs_date": date(2026, 9, 1)}])
def test_missing_or_stale_spot_never_creates_value(gex_pg, monkeypatch, receipt):
    seed(gex_pg)
    monkeypatch.setattr(AstroGridStore, "_verified_spy_receipt", lambda *a, **kw: receipt)
    assert run(gex_pg).run_status == "no_verified_spot"
    assert rows(gex_pg, "gex") == []


def test_cutoff_before_registration_preserves_acquisition_clock(gex_pg):
    seed(gex_pg, registered=CUTOFF + timedelta(seconds=1))
    assert run(gex_pg).run_status == "no_completed_capture"
    assert rows(gex_pg, "gex") == []


def test_snapshot_acquisition_after_cutoff_is_refused(gex_pg):
    seed(gex_pg, created=CUTOFF + timedelta(seconds=1))
    result = run(gex_pg)
    assert result.run_status == "no_completed_capture"
    assert rows(gex_pg, "gex") == []


def test_legacy_row_remains_identical(gex_pg):
    seed(gex_pg)
    with gex_pg.begin() as conn:
        conn.execute(text("INSERT INTO dealer_gex_daily (obs_date, ticker) VALUES (:day, 'SPY')"), {"day": DAY})
    before = rows(gex_pg, "gex")
    assert run(gex_pg).run_status == "partial_blocked_by_legacy"
    assert rows(gex_pg, "gex") == before


def test_ledger_failure_rolls_back_business_row_and_retry_recovers(gex_pg):
    seed(gex_pg)
    failed = False

    def reject_first_ledger(conn, cursor, statement, parameters, context, executemany):
        nonlocal failed
        if "INSERT INTO godview_runs" in statement and not failed:
            failed = True
            raise RuntimeError("synthetic ledger interruption")

    event.listen(gex_pg, "before_cursor_execute", reject_first_ledger)
    try:
        result = run(gex_pg)
    finally:
        event.remove(gex_pg, "before_cursor_execute", reject_first_ledger)
    assert result.status == "FAILED" and result.ledger_recorded
    assert rows(gex_pg, "gex") == [] and rows(gex_pg, "runs")[0]["rows_written"] == 0
    assert run(gex_pg).rows_written == 1


def test_runner_dry_run_rolls_back_both_and_requires_no_second_connection(gex_pg):
    seed(gex_pg)
    args = runner.parse_args(["--pillar", "gex", "--code-sha", SHA, "--dry-run",
                              "--session-date", DAY.isoformat(), "--as-of-ts", CUTOFF.isoformat()])
    code, summary = runner.run(args, gex_pg)
    assert code == 0 and summary["rows_written"] == 1 and summary["ledger_recorded"] is False
    assert rows(gex_pg, "gex") == [] and rows(gex_pg, "runs") == []


@pytest.mark.parametrize("fault,unavailable", [
    ("durable_reply_lost", False), ("durable_reply_lost", True),
    ("actual_rollback", False), ("deferred_fk_failure", False),
])
def test_real_pg_commit_outcome_reconciliation_and_separate_replay(gex_pg, fault, unavailable):
    """Synthetic reply loss after real SQL, not a real transport interruption.

    The deferred-FK case really fails at server COMMIT. All schemas/data are
    disposable fixtures; no live inputs, actual network fault or PG proof here
    is claimed until this test executes on the authorized test database.
    """
    seed(gex_pg)
    queries = []

    def observe(conn, cursor, statement, parameters, context, executemany):
        queries.append(statement)

    class FaultEngine:
        first = True

        @contextmanager
        def begin(self):
            first, self.first = self.first, False
            if not first and unavailable:
                raise RuntimeError("synthetic reconciliation unavailable")
            source = runner.TransactionEngine(gex_pg, commit=False) if first and fault == "actual_rollback" else gex_pg
            with source.begin() as conn:
                yield conn
                if first and fault == "deferred_fk_failure":
                    # Break the real deferred row->run FK only in this fixture.
                    conn.execute(text("DELETE FROM godview_runs"))
            if first:
                raise RuntimeError("synthetic COMMIT reply lost")

    event.listen(gex_pg, "before_cursor_execute", observe)
    try:
        result = run(FaultEngine())
    finally:
        event.remove(gex_pg, "before_cursor_execute", observe)
    durable = fault == "durable_reply_lost"
    before = rows(gex_pg, "gex")
    assert len(before) == int(durable)
    assert len(rows(gex_pg, "runs")) == int(durable)
    assert sum("INSERT INTO godview_runs" in query for query in queries) == 1
    if durable and not unavailable:
        assert result.status == "SUCCESS" and result.reconciled and result.rows_written == 1
        assert result.run_id == str(rows(gex_pg, "runs")[0]["run_id"])
        assert "SET TRANSACTION READ ONLY" in queries
    else:
        assert result.status == "OUTCOME_UNKNOWN" and result.rows_written is None
        assert runner.summarize("gex", result, dry_run=False)["persisted"] is None
    replay = run(gex_pg)  # a new, separately requested invocation
    assert replay.run_status == ("noop" if durable else "complete")
    assert len(rows(gex_pg, "gex")) == 1
    if durable:
        assert rows(gex_pg, "gex") == before
