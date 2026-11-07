"""Opt-in real PostgreSQL tests, always using a newly created private cluster.

Run with --noconftest to avoid repository-wide database setup. Set
SEC_TEST_POSTGRES_BIN to an installed PostgreSQL bin directory to opt in.
No connection URL, existing database, provider API, or default engine is used.
The invoking account must be non-root on POSIX. PostgreSQL is stopped in finally.
"""
from __future__ import annotations

import os
from pathlib import Path
import socket
import shutil
import subprocess
import sys
import tempfile

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

from ingestion.altdata import sec_xbrl_financials as sec

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def private_pg():
    configured = os.environ.get("SEC_TEST_POSTGRES_BIN")
    if not configured:
        pytest.skip("Set SEC_TEST_POSTGRES_BIN to opt into a new private PG cluster")
    bindir = Path(configured)
    if not bindir.is_absolute():
        pytest.fail("SEC_TEST_POSTGRES_BIN must be an absolute installed bin path")
    suffix = ".exe" if os.name == "nt" else ""
    binaries = {name: bindir / (name + suffix) for name in ("initdb", "pg_ctl")}
    for executable in binaries.values():
        if not executable.is_file():
            pytest.fail(f"Installed PostgreSQL executable missing: {executable}")
    if hasattr(os, "geteuid") and os.geteuid() == 0:
        pytest.fail("Run the private cluster tests as a non-root account")

    def command(name, *args):
        # On Windows postmaster can inherit pg_ctl pipe handles, preventing
        # communicate() from returning even after pg_ctl has exited.
        with tempfile.TemporaryFile(mode="w+b") as output:
            try:
                return subprocess.run(
                    [str(binaries[name]), *map(str, args)], check=True,
                    stdout=output, stderr=subprocess.STDOUT,
                    stdin=subprocess.DEVNULL, timeout=60,
                    env={k: v for k, v in os.environ.items() if not k.startswith("PG")},
                )
            except subprocess.SubprocessError as exc:
                output.seek(0)
                raise RuntimeError(output.read().decode("utf-8", errors="replace")) from exc

    root = Path(tempfile.mkdtemp(prefix="sec_financial_pg_"))
    try:
        data = root / "data"
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        command("initdb", "-D", data, "-U", "sec_test", "--auth=trust",
                "--encoding=UTF8", "--no-locale")
        # Disable Unix sockets, preventing accidental default-socket connections.
        with (data / "postgresql.conf").open("a", encoding="utf-8") as config:
            config.write(
                "\nlisten_addresses = '127.0.0.1'\n"
                f"port = {port}\n"
                "unix_socket_directories = ''\n"
                "timezone = 'UTC'\n"
            )
        guard = sys.modules.get("_sec_pg_guard")
        if guard is not None:
            guard.allowed.add(("127.0.0.1", port))
        started = False
        engine = None
        try:
            command("pg_ctl", "-D", data, "-l", root / "postgres.log", "-w", "start")
            started = True
            engine = create_engine(
                f"postgresql+psycopg2://sec_test@127.0.0.1:{port}/postgres",
                connect_args={"connect_timeout": 5},
            )
            with engine.connect() as conn:
                actual = Path(conn.execute(text("SHOW data_directory")).scalar_one())
                assert actual.resolve() == data.resolve()
            yield engine
        finally:
            if guard is not None:
                guard.allowed.discard(("127.0.0.1", port))
            if engine is not None:
                engine.dispose()
            if started or (data / "postmaster.pid").exists():
                command("pg_ctl", "-D", data, "-m", "immediate", "-w", "stop")
    finally:
        # Keep failed-start/failed-stop evidence and never delete a live cluster.
        if not (root / "data" / "postmaster.pid").exists():
            shutil.rmtree(root)


@pytest.fixture
def pg_table(private_pg):
    engine = private_pg
    currency = ", currency TEXT NOT NULL DEFAULT 'USD'"
    with engine.begin() as conn:
        conn.execute(text(f"""CREATE TABLE capital_flows (
            id SERIAL PRIMARY KEY, actor_id TEXT NOT NULL,
            fiscal_period DATE NOT NULL, period_type TEXT NOT NULL,
            flow_type TEXT NOT NULL, direction TEXT NOT NULL,
            amount_usd NUMERIC NOT NULL, counterparty_id TEXT,
            source_filing TEXT, confidence TEXT NOT NULL,
            as_of TIMESTAMPTZ DEFAULT NOW(){currency},
            UNIQUE (actor_id, fiscal_period, period_type, flow_type,
                    counterparty_id, source_filing))"""))
        conn.execute(text("""CREATE UNIQUE INDEX capital_flows_dedup_nullable_cp_key
                ON capital_flows (actor_id, fiscal_period, period_type, flow_type,
                (COALESCE(NULLIF(counterparty_id, ''), '__none__')), source_filing)"""))
    try:
        yield engine, True
    finally:
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE capital_flows"))


def revenue(value=100, *, amendment=False):
    tag = "RevenueFromContractWithCustomerExcludingAssessedTax"
    fact = dict(start="2026-04-27", end="2026-07-26", val=value,
                accn="0001045810-26-000076" if amendment else "0001045810-26-000075",
                filed="2026-09-01" if amendment else "2026-08-26",
                form="10-Q/A" if amendment else "10-Q", fp="Q2")
    payload = {"facts": {"us-gaap": {tag: {"units": {"USD": [fact]}}}}}
    return sec._collect_simple_flow(payload, "revenue", "in", [tag], [], {"USD": 1.0})


def stored(engine):
    with engine.connect() as conn:
        return list(conn.execute(text(
            "SELECT actor_id, amount_usd, source_filing FROM capital_flows "
            "ORDER BY actor_id, amount_usd"
        )))


def test_pg_amendment_replacement_is_idempotent(pg_table):
    engine, _ = pg_table
    assert sec._write_rows(engine, "nvda", revenue()) == 1
    assert sec._write_rows(engine, "nvda", revenue(90, amendment=True)) == 1
    assert sec._write_rows(engine, "nvda", revenue(90, amendment=True)) == 1
    rows = stored(engine)
    assert len(rows) == 1
    assert rows[0][1] == 90
    assert "0001045810-26-000076" in rows[0][2]


def test_pg_replacement_keeps_legacy_and_other_actor(pg_table):
    engine, _ = pg_table
    row = revenue()[0]
    assert sec._write_rows(engine, "other", [row]) == 1
    assert sec._write_rows(engine, "nvda", [{**row, "source_filing": "10-Q Q2"}]) == 1
    assert sec._write_rows(engine, "nvda", [row]) == 1
    assert sec._write_rows(engine, "nvda", revenue(90, amendment=True)) == 1
    rows = stored(engine)
    assert len(rows) == 3
    assert sum(row[1] == 90 for row in rows) == 1
    assert any(row[2] == "10-Q Q2" for row in rows)


def test_pg_failed_replacement_rolls_back_original(pg_table):
    engine, _ = pg_table
    assert sec._write_rows(engine, "nvda", revenue()) == 1
    before = stored(engine)
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE capital_flows ADD CONSTRAINT positive_test CHECK (amount_usd >= 0)"))
    bad = {**revenue(90, amendment=True)[0], "amount_usd": -1}
    with pytest.raises(IntegrityError):
        sec._write_rows(engine, "nvda", [bad])
    assert stored(engine) == before
    assert sec._write_rows(engine, "nvda", revenue(80, amendment=True)) == 1
    assert stored(engine)[0][1] == 80


def test_pg_nullable_counterparty_index_is_enforced(pg_table):
    engine, _ = pg_table
    assert sec._write_rows(engine, "nvda", revenue()) == 1
    with pytest.raises(IntegrityError):
        with engine.begin() as conn:
            conn.execute(text("""INSERT INTO capital_flows (
                actor_id, fiscal_period, period_type, flow_type, direction,
                amount_usd, counterparty_id, source_filing, confidence)
                SELECT actor_id, fiscal_period, period_type, flow_type, direction,
                amount_usd, '', source_filing, confidence FROM capital_flows"""))
    assert len(stored(engine)) == 1


def test_pg_missing_currency_schema_fails_without_deleting_original(pg_table):
    from sqlalchemy.exc import ProgrammingError

    engine, _ = pg_table
    assert sec._write_rows(engine, "nvda", revenue()) == 1
    before = stored(engine)
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE capital_flows DROP COLUMN currency"))
    with pytest.raises(ProgrammingError):
        sec._write_rows(engine, "nvda", revenue(90, amendment=True))
    assert stored(engine) == before


@pytest.fixture
def snapshot_db(pg_table):
    """Apply only the new migration to the already private cluster."""
    import importlib.util
    from types import SimpleNamespace
    from unittest.mock import patch

    engine, _ = pg_table
    path = Path(__file__).resolve().parents[1] / "migrations" / "versions" / "sec_financial_snapshots_20261107.py"
    spec = importlib.util.spec_from_file_location("_sec_snapshot_test_migration", path)
    migration = importlib.util.module_from_spec(spec)
    with engine.begin() as conn:
        conn.execute(text("CREATE ROLE grid NOLOGIN NOSUPERUSER NOINHERIT"))
        fake_alembic = SimpleNamespace(op=SimpleNamespace(
            execute=lambda sql: conn.execute(text(sql))))
        with patch.dict(sys.modules, {"alembic": fake_alembic}):
            spec.loader.exec_module(migration)
            migration.upgrade()
    try:
        yield engine
    finally:
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE sec_financial_snapshot_heads"))
            conn.execute(text("DROP TABLE sec_financial_snapshots"))
            conn.execute(text("DROP ROLE grid"))


def snapshot(value=100, *, second=0, amended=False, empty=False, actor="nvda"):
    from datetime import datetime, timedelta, timezone
    import json

    from ingestion.altdata.sec_financial_snapshots import prepare_snapshot

    tag = "RevenueFromContractWithCustomerExcludingAssessedTax"
    fact = dict(start="2026-04-27", end="2026-07-26", val=value,
                accn="0001045810-26-000076" if amended else "0001045810-26-000075",
                filed="2026-09-01" if amended else "2026-08-26",
                form="10-Q/A" if amended else "10-Q", fp="Q2")
    if empty:
        fact["start"] = "2026-01-26"  # Valid YTD-only response, no supported quarter.
    body = json.dumps({"cik": 1045810, "entityName": "Synthetic Test Issuer",
                       "facts": {"us-gaap": {tag: {"units": {"USD": [fact]}}}}}).encode()
    started = datetime(2026, 11, 1, tzinfo=timezone.utc) + timedelta(seconds=second)
    return prepare_snapshot(source_body=body, actor_id=actor, cik="1045810",
                            request_started_at=started,
                            received_at=started + timedelta(seconds=1),
                            fx_rates={"USD": 1.0})


def archive_rows(engine):
    with engine.connect() as conn:
        return list(conn.execute(text(
            "SELECT snapshot_id, source_sha256, source_gzip, normalized_rows, "
            "first_request_started_at, first_received_at "
            "FROM sec_financial_snapshots ORDER BY snapshot_id"
        )).mappings())


def current_head(engine):
    with engine.connect() as conn:
        return conn.execute(text(
            "SELECT actor_id, snapshot_id, request_started_at, received_at "
            "FROM sec_financial_snapshot_heads WHERE actor_id='nvda'"
        )).mappings().first()


def test_pg_snapshot_preserves_original_bytes_and_duplicate_identity(snapshot_db):
    import gzip
    import hashlib

    from store.sec_financial_snapshots import write_snapshot

    first = snapshot()
    assert write_snapshot(snapshot_db, first)["status"] == "applied"
    archived = archive_rows(snapshot_db)
    assert len(archived) == 1
    assert gzip.decompress(bytes(archived[0]["source_gzip"])) == first.source_body
    assert hashlib.sha256(first.source_body).hexdigest() == archived[0]["source_sha256"]
    assert archived[0]["normalized_rows"] == first.rows
    later = snapshot(second=10)
    assert first.snapshot_id == later.snapshot_id
    assert write_snapshot(snapshot_db, later)["status"] == "unchanged"
    assert archive_rows(snapshot_db) == archived
    assert current_head(snapshot_db)["request_started_at"] == later.request_started_at
    assert len(stored(snapshot_db)) == 1


def test_pg_snapshot_changed_and_empty_replace_only_owned_rows(snapshot_db):
    from store.sec_financial_snapshots import write_snapshot

    engine = snapshot_db
    row = revenue()[0]
    sec._write_rows(engine, "nvda", [{**row, "source_filing": "10-Q Q2"}])
    sec._write_rows(engine, "other", [row])
    write_snapshot(engine, snapshot())
    change = snapshot(90, second=10, amended=True)
    assert write_snapshot(engine, change)["status"] == "applied"
    assert len(stored(engine)) == 3
    assert any(r[0] == "nvda" and r[1] == 90 for r in stored(engine))
    empty = snapshot(second=20, empty=True)
    result = write_snapshot(engine, empty)
    assert result["rows_written"] == 0
    assert result["rows_removed"] == 1
    assert len(stored(engine)) == 2
    assert any(r[2] == "10-Q Q2" for r in stored(engine))
    assert any(r[0] == "other" for r in stored(engine))
    assert current_head(engine)["snapshot_id"] == empty.snapshot_id
    assert len(archive_rows(engine)) == 3


def test_pg_older_snapshot_is_archived_without_regressing_projection(snapshot_db):
    from store.sec_financial_snapshots import write_snapshot

    newest = snapshot(90, second=10, amended=True)
    write_snapshot(snapshot_db, newest)
    before = stored(snapshot_db)
    head = current_head(snapshot_db)
    result = write_snapshot(snapshot_db, snapshot())
    assert result["status"] == "ignored_older_capture"
    assert stored(snapshot_db) == before
    assert current_head(snapshot_db) == head
    assert len(archive_rows(snapshot_db)) == 2


def test_pg_equal_time_conflict_does_not_change_archive_head_or_projection(snapshot_db):
    from store.sec_financial_snapshots import write_snapshot

    write_snapshot(snapshot_db, snapshot())
    before = stored(snapshot_db), archive_rows(snapshot_db), current_head(snapshot_db)
    with pytest.raises(ValueError, match="same request-start"):
        write_snapshot(snapshot_db, snapshot(90))
    assert (stored(snapshot_db), archive_rows(snapshot_db), current_head(snapshot_db)) == before


def test_pg_new_capture_with_older_filing_does_not_regress_projection(snapshot_db):
    from store.sec_financial_snapshots import write_snapshot

    write_snapshot(snapshot_db, snapshot(90, amended=True))
    before = stored(snapshot_db), current_head(snapshot_db)
    result = write_snapshot(snapshot_db, snapshot(100, second=10))
    assert result["status"] == "rejected_revision_regression"
    assert (stored(snapshot_db), current_head(snapshot_db)) == before
    assert len(archive_rows(snapshot_db)) == 2


def test_pg_snapshot_projection_failure_rolls_back_archive_head_and_rows(snapshot_db):
    from store.sec_financial_snapshots import write_snapshot

    write_snapshot(snapshot_db, snapshot())
    before = stored(snapshot_db), archive_rows(snapshot_db), current_head(snapshot_db)
    with snapshot_db.begin() as conn:
        conn.execute(text("ALTER TABLE capital_flows ADD CONSTRAINT bounded_test CHECK (amount_usd <= 100)"))
    with pytest.raises(IntegrityError):
        write_snapshot(snapshot_db, snapshot(200, second=10, amended=True))
    assert (stored(snapshot_db), archive_rows(snapshot_db), current_head(snapshot_db)) == before


def test_pg_snapshot_writer_waits_for_actor_transaction_lock(snapshot_db):
    from concurrent.futures import ThreadPoolExecutor, TimeoutError
    from threading import Event

    from store.sec_financial_snapshots import write_snapshot

    entered = Event()

    def worker():
        entered.set()
        return write_snapshot(snapshot_db, snapshot())

    with ThreadPoolExecutor(max_workers=1) as pool:
        with snapshot_db.begin() as conn:
            conn.execute(text("SELECT pg_advisory_xact_lock(hashtextextended('nvda', 7363))"))
            future = pool.submit(worker)
            assert entered.wait(timeout=5)
            with pytest.raises(TimeoutError):
                future.result(timeout=0.3)
        assert future.result(timeout=10)["status"] == "applied"
    assert len(stored(snapshot_db)) == 1


def test_pg_concurrent_captures_leave_newest_projection(snapshot_db):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    from store.sec_financial_snapshots import write_snapshot

    barrier = Barrier(2)
    captures = [snapshot(), snapshot(90, second=10, amended=True)]

    def worker(capture):
        barrier.wait(timeout=5)
        return write_snapshot(snapshot_db, capture)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(worker, captures))
    assert all(r["status"] in {"applied", "ignored_older_capture"} for r in results)
    assert current_head(snapshot_db)["snapshot_id"] == captures[1].snapshot_id
    assert len(stored(snapshot_db)) == 1
    assert stored(snapshot_db)[0][1] == 90
    assert len(archive_rows(snapshot_db)) == 2


def test_pg_migration_archive_privileges_are_append_only_for_application_role(snapshot_db):
    from sqlalchemy.exc import ProgrammingError

    from store.sec_financial_snapshots import write_snapshot

    engine = snapshot_db
    write_snapshot(engine, snapshot())
    before = archive_rows(engine)
    with engine.begin() as conn:
        conn.execute(text("SET LOCAL ROLE grid"))
        assert conn.execute(text("SELECT COUNT(*) FROM sec_financial_snapshots")).scalar_one() == 1
        conn.execute(text(
            "INSERT INTO sec_financial_snapshots SELECT * "
            "FROM sec_financial_snapshots WHERE false"
        ))
        for privilege in ("SELECT", "INSERT"):
            assert conn.execute(text(
                "SELECT has_table_privilege(current_user, 'sec_financial_snapshots', :privilege)"
            ), {"privilege": privilege}).scalar_one()
        for privilege in ("UPDATE", "DELETE", "TRUNCATE"):
            assert not conn.execute(text(
                "SELECT has_table_privilege(current_user, 'sec_financial_snapshots', :privilege)"
            ), {"privilege": privilege}).scalar_one()
        assert conn.execute(text(
            "SELECT has_table_privilege(current_user, 'sec_financial_snapshot_heads', 'UPDATE')"
        )).scalar_one()
    for statement in (
        "UPDATE sec_financial_snapshots SET normalizer_version='unauthorized'",
        "DELETE FROM sec_financial_snapshots",
        "TRUNCATE sec_financial_snapshots CASCADE",
    ):
        with pytest.raises(ProgrammingError) as caught:
            with engine.begin() as conn:
                conn.execute(text("SET LOCAL ROLE grid"))
                conn.execute(text(statement))
        assert caught.value.orig.pgcode == "42501"
    assert archive_rows(engine) == before
