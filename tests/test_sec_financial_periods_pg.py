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
            conn.execute(text("""CREATE TABLE capital_flows_ttm_state (
                actor_id TEXT PRIMARY KEY, quarter_fingerprint TEXT,
                computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"""))
            current_path = path.with_name('sec_financial_current_20261108.py')
            current_spec = importlib.util.spec_from_file_location('_sec_current_migration', current_path)
            current_migration = importlib.util.module_from_spec(current_spec)
            current_spec.loader.exec_module(current_migration)
            current_migration.upgrade()
            evidence_path = path.with_name('sec_financial_report_dates_20261127.py')
            evidence_spec = importlib.util.spec_from_file_location('_sec_evidence_migration', evidence_path)
            evidence_migration = importlib.util.module_from_spec(evidence_spec)
            evidence_spec.loader.exec_module(evidence_migration)
            evidence_migration.upgrade()
            evidence_migration.upgrade()                 # idempotent
    try:
        yield engine
    finally:
        with engine.begin() as conn:
            conn.execute(text("DROP VIEW capital_flows_current"))
            conn.execute(text("DROP TABLE capital_flows_ttm_state"))
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


# Current-projection acceptance: original ledger rows remain intact, but no
# consumer may resurrect legacy/seed values missing from a qualified snapshot.
def current_rows(engine, actor="nvda", period="quarter"):
    with engine.connect() as conn:
        return list(conn.execute(text("""SELECT amount_usd, source_filing FROM capital_flows_current
            WHERE lower(actor_id)=:actor AND period_type=:period ORDER BY id"""),
            {"actor": actor, "period": period}))


def seed_legacy(engine, *, actor="nvda", value=177, period="quarter", source="10-Q Q2",
                counterparty=None, flow="revenue"):
    with engine.begin() as conn:
        conn.execute(text("""INSERT INTO capital_flows (actor_id, fiscal_period, period_type,
            flow_type, direction, amount_usd, counterparty_id, source_filing, confidence, currency)
            VALUES (:actor, '2026-07-26', :period, :flow, 'in', :value, :counterparty,
                    :source, 'confirmed', 'USD')"""),
            dict(actor=actor, period=period, flow=flow, value=value,
                 counterparty=counterparty, source=source))


def test_pg_current_view_hides_legacy_seed_and_case_variant(snapshot_db):
    from store.sec_financial_snapshots import write_snapshot
    for actor, source in [('nvda', '10-Q Q2'), ('nvda', 'seed'), ('NVDA', '10-Q Q2')]:
        seed_legacy(snapshot_db, actor=actor, source=source)
    write_snapshot(snapshot_db, snapshot(96))
    assert [r[0] for r in current_rows(snapshot_db)] == [96]
    assert len(stored(snapshot_db)) == 4  # No destructive historical reconciliation.
    write_snapshot(snapshot_db, snapshot(empty=True, second=10))
    assert current_rows(snapshot_db) == []  # Unavailable, not legacy fallback/zero.
    assert len(stored(snapshot_db)) == 3


def test_pg_current_view_preserves_unrelated_scopes(snapshot_db):
    from store.sec_financial_snapshots import write_snapshot
    seed_legacy(snapshot_db, actor='aapl')
    seed_legacy(snapshot_db, counterparty='supplier')
    seed_legacy(snapshot_db, period='announcement')
    seed_legacy(snapshot_db, flow='working_capital_delta')
    write_snapshot(snapshot_db, snapshot(empty=True))
    assert len(current_rows(snapshot_db, actor='aapl')) == 1
    assert len(current_rows(snapshot_db)) == 2  # Counterparty and unmapped flow.
    assert len(current_rows(snapshot_db, period='announcement')) == 1


def test_pg_current_view_rejects_mutated_owned_amount(snapshot_db):
    from store.sec_financial_snapshots import write_snapshot
    write_snapshot(snapshot_db, snapshot(96))
    with snapshot_db.begin() as conn:
        conn.execute(text("UPDATE capital_flows SET amount_usd=999"))
    assert current_rows(snapshot_db) == []


def test_pg_current_view_dedups_unmanaged_annuals_before_sector_sum(snapshot_db):
    seed_legacy(snapshot_db, period='annual', value=100, source='10-K FY')
    seed_legacy(snapshot_db, period='annual', value=120, source='seed')
    with snapshot_db.connect() as conn:
        assert conn.execute(text("""SELECT SUM(amount_usd) FROM capital_flows_current
            WHERE actor_id='nvda' AND period_type='annual'""")).scalar_one() == 100
    assert len(stored(snapshot_db)) == 2


def multi_quarter_snapshot(*, second=0, last=40):
    from datetime import date, timedelta
    import json
    from ingestion.altdata.sec_financial_snapshots import prepare_snapshot
    original = snapshot(second=second)
    body = json.loads(original.source_body)
    entries = body['facts']['us-gaap']['RevenueFromContractWithCustomerExcludingAssessedTax']['units']['USD']
    prototype = entries[0]
    entries[:] = [dict(prototype, start=(date.fromisoformat(end)-timedelta(days=90)).isoformat(),
                      end=end, val=value)
                 for end, value in [('2025-10-26',10), ('2026-01-25',20),
                                    ('2026-04-26',30), ('2026-07-26',last)]]
    return prepare_snapshot(source_body=json.dumps(body).encode(), actor_id='nvda', cik='1045810',
                            request_started_at=original.request_started_at,
                            received_at=original.received_at, fx_rates={'USD':1.0})


def test_pg_ttm_hidden_until_same_snapshot_recomputed(snapshot_db):
    from store.sec_financial_snapshots import write_snapshot
    from intelligence.company_financial_rollups import compute_ttm
    seed_legacy(snapshot_db, period='ttm', source='ttm_rollup', value=999)
    first = multi_quarter_snapshot()
    write_snapshot(snapshot_db, first)
    assert current_rows(snapshot_db, period='ttm') == []
    compute_ttm(snapshot_db)
    assert [r[0] for r in current_rows(snapshot_db, period='ttm')] == [100]
    newer = multi_quarter_snapshot(second=10, last=50)
    write_snapshot(snapshot_db, newer)
    assert current_rows(snapshot_db, period='ttm') == []
    compute_ttm(snapshot_db)
    assert [r[0] for r in current_rows(snapshot_db, period='ttm')] == [110]
    # Newer complete-but-unsupported capture immediately suppresses cached TTM.
    write_snapshot(snapshot_db, snapshot(second=20, empty=True))
    assert current_rows(snapshot_db, period='ttm') == []
    compute_ttm(snapshot_db)
    with snapshot_db.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM capital_flows WHERE period_type='ttm'")).scalar_one() == 0
    assert compute_ttm(snapshot_db).rows_written == 0


def test_pg_first_empty_snapshot_reconciles_old_rollup_state(snapshot_db):
    from store.sec_financial_snapshots import write_snapshot
    from intelligence.company_financial_rollups import compute_ttm
    seed_legacy(snapshot_db, period='ttm', source='ttm_rollup', value=999)
    empty = snapshot(empty=True)
    write_snapshot(snapshot_db, empty)
    compute_ttm(snapshot_db)
    with snapshot_db.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM capital_flows WHERE period_type='ttm'")).scalar_one() == 0
        assert conn.execute(text("SELECT sec_snapshot_id FROM capital_flows_ttm_state")).scalar_one() == empty.snapshot_id



def test_pg_same_values_new_snapshot_still_rebinds_ttm(snapshot_db):
    import json
    from datetime import timedelta
    from ingestion.altdata.sec_financial_snapshots import prepare_snapshot
    from store.sec_financial_snapshots import write_snapshot
    from intelligence.company_financial_rollups import compute_ttm
    first = multi_quarter_snapshot()
    write_snapshot(snapshot_db, first)
    compute_ttm(snapshot_db)
    body = json.loads(first.source_body)
    body['entityName'] = 'Same Financial Values New Capture'
    second = prepare_snapshot(source_body=json.dumps(body).encode(), actor_id='nvda', cik='1045810',
        request_started_at=first.request_started_at+timedelta(seconds=10),
        received_at=first.received_at+timedelta(seconds=10), fx_rates={'USD':1.0})
    assert second.normalized_json == first.normalized_json
    write_snapshot(snapshot_db, second)
    assert current_rows(snapshot_db, period='ttm') == []
    compute_ttm(snapshot_db)
    assert [r[0] for r in current_rows(snapshot_db, period='ttm')] == [100]
    # An older racing rollup cannot publish against a newer snapshot head.
    with snapshot_db.begin() as conn:
        conn.execute(text("UPDATE capital_flows_ttm_state SET sec_snapshot_id=:old"),
                     {'old':first.snapshot_id})
    assert current_rows(snapshot_db, period='ttm') == []
    compute_ttm(snapshot_db)
    assert [r[0] for r in current_rows(snapshot_db, period='ttm')] == [100]


def test_pg_current_view_is_readable_by_application_role(snapshot_db):
    from store.sec_financial_snapshots import write_snapshot
    write_snapshot(snapshot_db, snapshot())
    with snapshot_db.begin() as conn:
        conn.execute(text('SET LOCAL ROLE grid'))
        assert conn.execute(text('SELECT count(*) FROM capital_flows_current')).scalar_one() == 1



def test_pg_current_view_public_fixture_query_plan(snapshot_db):
    import gzip
    from ingestion.altdata.sec_financial_snapshots import prepare_snapshot
    from store.sec_financial_snapshots import write_snapshot
    receipt = snapshot()
    original = gzip.decompress((Path(__file__).parent / 'fixtures/sec_financial_periods/nvda.companyfacts.json.gz').read_bytes())
    real = prepare_snapshot(source_body=original, actor_id='nvda', cik='1045810',
        request_started_at=receipt.request_started_at, received_at=receipt.received_at, fx_rates={'USD':1.0})
    write_snapshot(snapshot_db, real)
    with snapshot_db.begin() as conn:
        # Deliberately retain one competing historical value for every qualified key.
        conn.execute(text("""INSERT INTO capital_flows (actor_id, fiscal_period, period_type,
            flow_type, direction, amount_usd, counterparty_id, source_filing, confidence, currency)
            SELECT actor_id, fiscal_period, period_type, flow_type, direction, amount_usd * 2,
                   counterparty_id, '10-Q legacy', confidence, currency FROM capital_flows"""))
        conn.execute(text('ANALYZE capital_flows'))
        assert conn.execute(text("SELECT count(*) FROM capital_flows_current WHERE actor_id='nvda'")).scalar_one() == len(real.rows)
        plan = conn.execute(text("""EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
            SELECT flow_type, SUM(amount_usd) FROM capital_flows_current
            WHERE actor_id='nvda' AND period_type='annual' GROUP BY flow_type""")).scalar_one()
        print('Current-view public NVDA fixture:', len(real.rows), 'qualified rows,',
              2*len(real.rows), 'ledger rows; annual aggregate execution ms:', plan[0]['Execution Time'])



def test_pg_overlapping_rollup_cannot_publish_after_head_advance(snapshot_db):
    from concurrent.futures import ThreadPoolExecutor
    import time
    from store.sec_financial_snapshots import write_snapshot
    from intelligence.company_financial_rollups import compute_ttm
    first = multi_quarter_snapshot()
    newer = multi_quarter_snapshot(second=10, last=50)
    write_snapshot(snapshot_db, first)
    with snapshot_db.begin() as conn:
        conn.execute(text("""CREATE FUNCTION sec_test_pause_ttm() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN PERFORM pg_advisory_xact_lock(7363001); RETURN NEW; END $$"""))
        conn.execute(text("""CREATE TRIGGER sec_test_pause_ttm BEFORE INSERT ON capital_flows
            FOR EACH ROW WHEN (NEW.period_type='ttm') EXECUTE FUNCTION sec_test_pause_ttm()"""))
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            with snapshot_db.connect() as lock:
                lock.execute(text('SELECT pg_advisory_lock(7363001)'))
                try:
                    future = pool.submit(compute_ttm, snapshot_db)
                    deadline = time.monotonic() + 10
                    while True:
                        blocked = lock.execute(text("""SELECT count(*) FROM pg_locks
                            WHERE locktype='advisory' AND objid=7363001 AND NOT granted""")).scalar_one()
                        if blocked:
                            break
                        assert time.monotonic() < deadline, 'Rollup did not reach the controlled insert barrier'
                        time.sleep(0.02)
                    # The real rollup statement has already read H1; commit H2
                    # while its insert is blocked, then let H1 finish last.
                    write_snapshot(snapshot_db, newer)
                finally:
                    lock.execute(text('SELECT pg_advisory_unlock(7363001)'))
                future.result(timeout=10)
        assert current_rows(snapshot_db, period='ttm') == []
        with snapshot_db.connect() as conn:
            assert conn.execute(text('SELECT sec_snapshot_id FROM capital_flows_ttm_state')).scalar_one() == first.snapshot_id
        compute_ttm(snapshot_db)
        assert [r[0] for r in current_rows(snapshot_db, period='ttm')] == [110]
    finally:
        with snapshot_db.begin() as conn:
            conn.execute(text('DROP TRIGGER sec_test_pause_ttm ON capital_flows'))
            conn.execute(text('DROP FUNCTION sec_test_pause_ttm()'))



def test_pg_current_migration_bootstraps_missing_raw_ledger(private_pg):
    import importlib.util
    from types import SimpleNamespace
    from unittest.mock import patch
    engine = private_pg
    root = Path(__file__).resolve().parents[1] / 'migrations/versions'
    try:
        with engine.begin() as conn:
            assert conn.execute(text("SELECT to_regclass('public.capital_flows')")).scalar_one() is None
            conn.execute(text('CREATE ROLE grid NOLOGIN NOSUPERUSER NOINHERIT'))
            conn.execute(text("""CREATE TABLE capital_flows_ttm_state (
                actor_id TEXT PRIMARY KEY, quarter_fingerprint TEXT,
                computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"""))
            fake_alembic = SimpleNamespace(op=SimpleNamespace(execute=lambda sql: conn.execute(text(sql))))
            with patch.dict(sys.modules, {'alembic':fake_alembic}):
                for filename in ('sec_financial_snapshots_20261107.py', 'sec_financial_current_20261108.py'):
                    spec = importlib.util.spec_from_file_location('_sec_clean_migration', root/filename)
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    module.upgrade()
            assert conn.execute(text('SELECT count(*) FROM capital_flows_current')).scalar_one() == 0
            conn.execute(text('SET LOCAL ROLE grid'))
            conn.execute(text("""INSERT INTO capital_flows (actor_id,fiscal_period,period_type,
                flow_type,direction,amount_usd,source_filing,confidence)
                VALUES ('clean','2026-07-26','quarter','revenue','in',10,'10-Q test','confirmed')"""))
            assert conn.execute(text('SELECT amount_usd FROM capital_flows_current')).scalar_one() == 10
        # Upgrade's empty-ledger helper is harmless when a populated ledger exists.
        with engine.begin() as conn:
            conn.execute(text(module.EMPTY_LEDGER_SQL))
            assert conn.execute(text('SELECT count(*) FROM capital_flows')).scalar_one() == 1
    finally:
        with engine.begin() as conn:
            conn.execute(text('DROP VIEW IF EXISTS capital_flows_current'))
            for name in ('sec_financial_snapshot_heads','sec_financial_snapshots',
                         'capital_flows_ttm_state','capital_flows'):
                conn.execute(text('DROP TABLE IF EXISTS '+name))
            conn.execute(text('DROP ROLE IF EXISTS grid'))


# Output-generation tests use only the same disposable database above. These
# helpers deliberately neither discover a default engine nor call providers.
def _apply_output_migration(engine):
    import importlib.util
    from types import SimpleNamespace
    from unittest.mock import patch

    path = Path(__file__).resolve().parents[1] / "migrations/versions/sec_financial_outputs_20261108.py"
    spec = importlib.util.spec_from_file_location("_sec_outputs_test_migration", path)
    migration = importlib.util.module_from_spec(spec)
    with engine.begin() as conn:
        fake_alembic = SimpleNamespace(op=SimpleNamespace(execute=lambda sql: conn.execute(text(sql))))
        with patch.dict(sys.modules, {"alembic": fake_alembic}):
            spec.loader.exec_module(migration)
            migration.upgrade()
    return migration


def _drop_test_outputs(engine):
    with engine.begin() as conn:
        for name in ("fundamental_divergence_current", "sector_health_snapshots_current", "sec_financial_generation"):
            conn.execute(text("DROP VIEW IF EXISTS " + name))
        for name in ("fundamental_divergence", "sector_health_snapshots"):
            conn.execute(text("DROP TABLE IF EXISTS " + name))


@pytest.fixture
def output_db(snapshot_db):
    try:
        _apply_output_migration(snapshot_db)
        yield snapshot_db
    finally:
        _drop_test_outputs(snapshot_db)


def _generation(engine):
    from store.financial_generation import get_financial_generation
    with engine.connect() as conn:
        return get_financial_generation(conn)


def _divergence_candidate(generation, *, ticker="NVDA", score=70):
    from datetime import date
    return dict(ticker=ticker, as_of=date(2026, 11, 1), sector="Technology",
                fundamental_score=score, price_score=50, divergence=score-50,
                classification="long_candidate", narrative="Synthetic financial test",
                sec_generation=generation)


def test_pg_output_generation_tracks_heads_reobservations_and_peers(output_db):
    from store.sec_financial_snapshots import write_snapshot
    empty_generation = _generation(output_db)
    assert len(empty_generation) == 32
    assert _generation(output_db) == empty_generation
    # A first empty/unsupported source still changes financial availability.
    first = snapshot(empty=True)
    write_snapshot(output_db, first)
    first_generation = _generation(output_db)
    assert first_generation != empty_generation
    again = snapshot(second=10, empty=True)
    assert again.snapshot_id == first.snapshot_id
    write_snapshot(output_db, again)
    repeated_generation = _generation(output_db)
    assert repeated_generation == first_generation
    # Ignored old observations must not perturb current cache/report identity.
    assert write_snapshot(output_db, first)["status"] == "ignored_older_capture"
    assert _generation(output_db) == repeated_generation
    write_snapshot(output_db, snapshot(actor="aapl"))
    assert _generation(output_db) != repeated_generation
    # Session display timezone must not create a false generation change.
    with output_db.begin() as conn:
        from store.financial_generation import get_financial_generation
        utc_generation = get_financial_generation(conn)
        conn.execute(text("SET LOCAL TIME ZONE 'America/New_York'"))
        assert get_financial_generation(conn) == utc_generation


def test_pg_output_generation_tracks_same_values_new_identity(output_db):
    import json
    from datetime import timedelta
    from ingestion.altdata.sec_financial_snapshots import prepare_snapshot
    from store.sec_financial_snapshots import write_snapshot
    first = snapshot()
    write_snapshot(output_db, first)
    before = _generation(output_db)
    body = json.loads(first.source_body)
    body["entityName"] = "Same Values New Evidence"
    newer = prepare_snapshot(
        source_body=json.dumps(body).encode(), actor_id="nvda", cik="1045810",
        request_started_at=first.request_started_at+timedelta(seconds=10),
        received_at=first.received_at+timedelta(seconds=10), fx_rates={"USD": 1.0},
    )
    assert newer.normalized_json == first.normalized_json
    assert newer.snapshot_id != first.snapshot_id
    write_snapshot(output_db, newer)
    assert _generation(output_db) != before


def test_pg_unchanged_refetch_with_moving_fx_keeps_reports_current(output_db):
    import json
    from datetime import timedelta
    from ingestion.altdata.sec_financial_snapshots import prepare_snapshot
    from intelligence.fundamental_divergence import _upsert_divergence_chunk
    from store.sec_financial_snapshots import write_snapshot

    first = snapshot()
    write_snapshot(output_db, first)
    generation = _generation(output_db)
    with output_db.begin() as conn:
        assert _upsert_divergence_chunk(conn, [_divergence_candidate(generation)]) == 1
        conn.execute(text("""INSERT INTO sector_health_snapshots
            (sector_name,snapshot_date,score,sec_generation)
            VALUES ('Technology','2026-11-01',70,:generation)"""),
            {"generation": generation})

    later = prepare_snapshot(
        source_body=first.source_body, actor_id=first.actor_id, cik=first.cik,
        request_started_at=first.request_started_at+timedelta(days=1),
        received_at=first.received_at+timedelta(days=1),
        fx_rates={"USD": 1.0, "EUR": 1.2, "JPY": 0.008},
    )
    assert later.snapshot_id == first.snapshot_id
    result = write_snapshot(output_db, later)
    assert result["status"] == "unchanged"
    assert result["rows_written"] == 0
    assert _generation(output_db) == generation
    head = current_head(output_db)
    assert head["request_started_at"] == later.request_started_at
    assert head["received_at"] == later.received_at
    assert json.loads(later.fx_json) == {}
    with output_db.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM sec_financial_snapshots")).scalar_one() == 1
        assert conn.execute(text("SELECT ticker FROM fundamental_divergence_current")).scalar_one() == "NVDA"
        assert conn.execute(text("SELECT sector_name FROM sector_health_snapshots_current")).scalar_one() == "Technology"


def test_pg_output_generation_tracks_ttm_publication_without_head_change(output_db):
    from store.sec_financial_snapshots import write_snapshot
    from intelligence.company_financial_rollups import compute_ttm
    write_snapshot(output_db, multi_quarter_snapshot())
    before_head = current_head(output_db)
    before_generation = _generation(output_db)
    compute_ttm(output_db)
    after_generation = _generation(output_db)
    assert current_head(output_db) == before_head
    assert after_generation != before_generation
    compute_ttm(output_db)
    assert _generation(output_db) == after_generation
    # Recomputing identical content only advances operational metadata.
    with output_db.begin() as conn:
        conn.execute(text("UPDATE capital_flows_ttm_state SET computed_at=computed_at + interval '1 second'"))
    assert _generation(output_db) == after_generation
    # Every mutable readiness dimension participates, including old late work.
    for assignment in (
        "quarter_fingerprint='changed-test-fingerprint'",
        "sec_snapshot_id=NULL",
    ):
        before_generation = _generation(output_db)
        with output_db.begin() as conn:
            conn.execute(text("UPDATE capital_flows_ttm_state SET " + assignment))
        assert _generation(output_db) != before_generation


def test_pg_output_views_hide_stale_and_unbound_rows_preserving_raw(output_db):
    from store.sec_financial_snapshots import write_snapshot
    current = _generation(output_db)
    with output_db.begin() as conn:
        for ticker, generation in (("UNBOUND", None), ("STALE", "f" * 32), ("CURRENT", current)):
            conn.execute(text("""INSERT INTO fundamental_divergence
                (ticker,as_of,fundamental_score,sec_generation)
                VALUES (:ticker,'2026-11-01',70,:generation)"""),
                {"ticker": ticker, "generation": generation})
            conn.execute(text("""INSERT INTO sector_health_snapshots
                (sector_name,snapshot_date,score,sec_generation)
                VALUES (:ticker,'2026-11-01',70,:generation)"""),
                {"ticker": ticker, "generation": generation})
        assert conn.execute(text("SELECT ticker FROM fundamental_divergence_current")).scalars().all() == ["CURRENT"]
        assert conn.execute(text("SELECT sector_name FROM sector_health_snapshots_current")).scalars().all() == ["CURRENT"]
        before_div = conn.execute(text("SELECT * FROM fundamental_divergence ORDER BY id")).all()
        before_sector = conn.execute(text("SELECT * FROM sector_health_snapshots ORDER BY id")).all()
    write_snapshot(output_db, snapshot(empty=True))
    with output_db.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM fundamental_divergence_current")).scalar_one() == 0
        assert conn.execute(text("SELECT count(*) FROM sector_health_snapshots_current")).scalar_one() == 0
        assert conn.execute(text("SELECT * FROM fundamental_divergence ORDER BY id")).all() == before_div
        assert conn.execute(text("SELECT * FROM sector_health_snapshots ORDER BY id")).all() == before_sector


def test_pg_divergence_upsert_filters_stale_unbound_and_mixed_candidates(output_db):
    from intelligence.fundamental_divergence import _upsert_divergence_chunk
    from store.sec_financial_snapshots import write_snapshot
    old_generation = _generation(output_db)
    with output_db.begin() as conn:
        assert _upsert_divergence_chunk(conn, [_divergence_candidate(old_generation)]) == 1
    write_snapshot(output_db, snapshot(empty=True))
    current = _generation(output_db)
    with output_db.begin() as conn:
        assert _upsert_divergence_chunk(conn, [_divergence_candidate(old_generation, score=99)]) == 0
        assert _upsert_divergence_chunk(conn, [_divergence_candidate(None, ticker="UNBOUND")]) == 0
        assert conn.execute(text("SELECT fundamental_score FROM fundamental_divergence WHERE ticker='NVDA'")).scalar_one() == 70
        assert _upsert_divergence_chunk(conn, [
            _divergence_candidate(old_generation, ticker="STALE"),
            _divergence_candidate(current, score=80),
        ]) == 1
        rows = conn.execute(text("SELECT ticker,fundamental_score,sec_generation FROM fundamental_divergence_current")).all()
        assert rows == [("NVDA", 80, current)]
        assert conn.execute(text("SELECT count(*) FROM fundamental_divergence")).scalar_one() == 1


def test_pg_output_migration_clean_bootstrap_role_grants(output_db):
    from store.financial_generation import get_financial_generation
    from intelligence.fundamental_divergence import _upsert_divergence_chunk
    with output_db.begin() as conn:
        conn.execute(text("SET LOCAL ROLE grid"))
        generation = get_financial_generation(conn)
        assert _upsert_divergence_chunk(conn, [_divergence_candidate(generation)]) == 1
        conn.execute(text("""INSERT INTO sector_health_snapshots
            (sector_name,snapshot_date,score,sec_generation)
            VALUES ('Technology','2026-11-01',70,:g)"""), {"g": generation})
        assert conn.execute(text("SELECT count(*) FROM fundamental_divergence_current")).scalar_one() == 1
        assert conn.execute(text("SELECT count(*) FROM sector_health_snapshots_current")).scalar_one() == 1


def test_pg_output_migration_preserves_preexisting_report_rows(snapshot_db):
    root = Path(__file__).resolve().parents[1]
    try:
        with snapshot_db.begin() as conn:
            for filename in ("0033_fundamental_divergence.sql", "0028_sector_health_snapshots.sql"):
                conn.execute(text((root / "migrations" / filename).read_text(encoding="utf-8")))
            conn.execute(text("""INSERT INTO fundamental_divergence
                (ticker,as_of,fundamental_score) VALUES ('LEGACY','2026-11-01',70)"""))
            conn.execute(text("""INSERT INTO sector_health_snapshots
                (sector_name,snapshot_date,score) VALUES ('Technology','2026-11-01',70)"""))
            before_div = conn.execute(text("SELECT row_to_json(d) FROM fundamental_divergence d")).scalar_one()
            before_sector = conn.execute(text("SELECT row_to_json(s) FROM sector_health_snapshots s")).scalar_one()
        _apply_output_migration(snapshot_db)
        _apply_output_migration(snapshot_db)  # Re-run empty-schema guards safely.
        with snapshot_db.connect() as conn:
            after_div = conn.execute(text("SELECT row_to_json(d) FROM fundamental_divergence d")).scalar_one()
            after_sector = conn.execute(text("SELECT row_to_json(s) FROM sector_health_snapshots s")).scalar_one()
            assert after_div.pop("sec_generation") is None
            assert after_sector.pop("sec_generation") is None
            assert after_div == before_div
            assert after_sector == before_sector
            assert conn.execute(text("SELECT count(*) FROM fundamental_divergence_current")).scalar_one() == 0
            assert conn.execute(text("SELECT count(*) FROM sector_health_snapshots_current")).scalar_one() == 0
    finally:
        _drop_test_outputs(snapshot_db)


def test_pg_output_cache_tracks_real_peer_and_ttm_generation(output_db):
    from utils.financial_cache import get_cached_financial, FinancialCacheUnavailable
    from utils.ttl_cache import TTLCache
    from store.sec_financial_snapshots import write_snapshot
    from intelligence.company_financial_rollups import compute_ttm
    write_snapshot(output_db, multi_quarter_snapshot())
    cache = TTLCache()
    calls = []

    def build():
        calls.append(len(calls))
        return {"build": len(calls)}

    assert get_cached_financial(cache, "ttm", output_db, build) == {"build": 1}
    assert get_cached_financial(cache, "ttm", output_db, build) == {"build": 1}
    compute_ttm(output_db)
    assert get_cached_financial(cache, "ttm", output_db, build) == {"build": 2}
    write_snapshot(output_db, snapshot(actor="aapl", empty=True))
    assert get_cached_financial(cache, "ttm", output_db, build) == {"build": 3}
    with output_db.begin() as conn:
        conn.execute(text("ALTER VIEW sec_financial_generation RENAME TO sec_financial_generation_test_hidden"))
    try:
        with pytest.raises(FinancialCacheUnavailable):
            get_cached_financial(cache, "ttm", output_db, build)
        assert len(calls) == 3
    finally:
        with output_db.begin() as conn:
            conn.execute(text("ALTER VIEW sec_financial_generation_test_hidden RENAME TO sec_financial_generation"))


def test_pg_late_divergence_write_is_hidden_after_head_advance(output_db):
    from concurrent.futures import ThreadPoolExecutor
    import time
    from intelligence.fundamental_divergence import _upsert_divergence_chunk
    from store.sec_financial_snapshots import write_snapshot
    old_generation = _generation(output_db)
    with output_db.begin() as conn:
        conn.execute(text("""CREATE FUNCTION sec_test_pause_report() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN PERFORM pg_advisory_xact_lock(7363002); RETURN NEW; END $$"""))
        conn.execute(text("""CREATE TRIGGER sec_test_pause_report BEFORE INSERT ON fundamental_divergence
            FOR EACH ROW EXECUTE FUNCTION sec_test_pause_report()"""))

    def late_write():
        with output_db.begin() as conn:
            return _upsert_divergence_chunk(conn, [_divergence_candidate(old_generation)])

    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            with output_db.connect() as lock:
                lock.execute(text("SELECT pg_advisory_lock(7363002)"))
                try:
                    future = pool.submit(late_write)
                    deadline = time.monotonic() + 10
                    while True:
                        waiting = lock.execute(text("""SELECT count(*) FROM pg_locks
                            WHERE locktype='advisory' AND objid=7363002 AND NOT granted""")).scalar_one()
                        if waiting:
                            break
                        assert time.monotonic() < deadline, "Report did not reach controlled insert barrier"
                        time.sleep(0.02)
                    write_snapshot(output_db, snapshot(empty=True))
                finally:
                    lock.execute(text("SELECT pg_advisory_unlock(7363002)"))
                assert future.result(timeout=10) == 1
        with output_db.connect() as conn:
            assert conn.execute(text("SELECT count(*) FROM fundamental_divergence")).scalar_one() == 1
            assert conn.execute(text("SELECT count(*) FROM fundamental_divergence_current")).scalar_one() == 0
    finally:
        with output_db.begin() as conn:
            conn.execute(text("DROP TRIGGER sec_test_pause_report ON fundamental_divergence"))
            conn.execute(text("DROP FUNCTION sec_test_pause_report()"))


def test_pg_sector_snapshot_writer_rejects_newer_time_with_stale_generation(output_db, monkeypatch):
    from datetime import date, datetime, timedelta, timezone
    from intelligence import sector_health
    from store.sec_financial_snapshots import write_snapshot
    monkeypatch.setattr("analysis.sector_map.SECTOR_MAP", {"Technology": {}})
    old_generation = _generation(output_db)
    result = {"score": 60, "trend_30d": "stable", "components": {"margin": 0.6},
              "sec_generation": old_generation}
    monkeypatch.setattr(sector_health, "compute_sector_health", lambda *_: dict(result))
    started = datetime(2026, 11, 1, tzinfo=timezone.utc)

    def save(seconds):
        return sector_health.snapshot_all_sectors(
            output_db, snapshot_date=date(2026, 11, 1),
            computed_at=started+timedelta(seconds=seconds),
        )

    assert save(0)["snapshots_written"] == 1
    with output_db.connect() as conn:
        before_report = conn.execute(text("SELECT * FROM sector_health_snapshots")).one()
    write_snapshot(output_db, snapshot(empty=True))
    result["score"] = 99
    # A later wall clock does not authorize an old-source result to overwrite.
    # The scheduler must retry the due day, not mark this as superseded/done.
    with pytest.raises(sector_health.FinancialGenerationRetry):
        save(10)
    with output_db.connect() as conn:
        assert conn.execute(text("SELECT * FROM sector_health_snapshots")).one() == before_report
        assert conn.execute(text("SELECT count(*) FROM sector_health_snapshots_current")).scalar_one() == 0
    result.update(score=80, sec_generation=_generation(output_db))
    assert save(20)["snapshots_written"] == 1
    with output_db.connect() as conn:
        assert conn.execute(text("SELECT score FROM sector_health_snapshots_current")).scalar_one() == 80
    # Keep existing timestamp/first-commit ordering protection too.
    result["score"] = 90
    assert save(20)["snapshots_stale_skipped"] == 1
    with output_db.connect() as conn:
        assert conn.execute(text("SELECT score FROM sector_health_snapshots_current")).scalar_one() == 80


@pytest.fixture
def journal_db(private_pg):
    """Use the actual journal column types/constraints in the private cluster."""
    schema = (Path(__file__).resolve().parents[1] / "schema.sql").read_text(encoding="utf-8")
    start = schema.index("CREATE TABLE IF NOT EXISTS decision_journal (")
    end = schema.index("\n);", start) + len("\n);")
    with private_pg.begin() as conn:
        # Only the journal's referenced primary key is needed by these reads.
        conn.execute(text("CREATE TABLE model_registry (id INTEGER PRIMARY KEY)"))
        conn.execute(text("INSERT INTO model_registry(id) VALUES (1)"))
        conn.execute(text(schema[start:end]))
        conn.execute(text("ALTER TABLE decision_journal ADD COLUMN source_contract_id UUID"))
    try:
        yield private_pg
    finally:
        with private_pg.begin() as conn:
            conn.execute(text("DROP TABLE decision_journal"))
            conn.execute(text("DROP TABLE model_registry"))


def _insert_journal(engine, *, second=0, **overrides):
    import json
    from datetime import datetime, timedelta, timezone
    fields = dict(
        decision_timestamp=datetime(2026, 11, 8, tzinfo=timezone.utc)+timedelta(seconds=second),
        inferred_state="EXPANSION", state_confidence=0.75, transition_probability=0.2,
        contradiction_flags={}, grid_recommendation="HOLD", baseline_recommendation="HOLD",
        action_taken="HOLD", counterfactual="S=0.25, dS/dt=0.01", operator_confidence="HIGH",
        outcome_value=None, outcome_recorded_at=None, verdict=None, annotation="Synthetic evidence",
        source_contract_id=None,
    )
    fields.update(overrides)
    fields["contradiction_flags"] = json.dumps(fields["contradiction_flags"])
    with engine.begin() as conn:
        return conn.execute(text("""INSERT INTO decision_journal (
            decision_timestamp, model_version_id, inferred_state, state_confidence,
            transition_probability, contradiction_flags, grid_recommendation,
            baseline_recommendation, action_taken, counterfactual, operator_confidence,
            outcome_value, outcome_recorded_at, verdict, annotation, source_contract_id
        ) VALUES (
            :decision_timestamp, 1, :inferred_state, :state_confidence,
            :transition_probability, CAST(:contradiction_flags AS JSONB), :grid_recommendation,
            :baseline_recommendation, :action_taken, :counterfactual, :operator_confidence,
            :outcome_value, :outcome_recorded_at, :verdict, :annotation, CAST(:source_contract_id AS UUID)
        ) RETURNING id"""), fields).scalar_one()


def _raw_journal_evidence(engine):
    with engine.connect() as conn:
        return conn.execute(text("SELECT row_to_json(d)::text FROM decision_journal d ORDER BY id")).scalars().all()


def _latest_qualified_journal(engine):
    from store.journal_qualification import (
        DECISION_JOURNAL, non_provisional_journal_select,
    )
    # Exercise the shared clause builder used by current-regime consumers.
    query = (
        non_provisional_journal_select("id", "inferred_state", "state_confidence")
        .order_by(DECISION_JOURNAL.c.decision_timestamp.desc())
        .limit(1)
    )
    with engine.connect() as conn:
        return conn.execute(query).first()


def test_pg_provisional_journal_does_not_replace_latest_genuine_regime(journal_db):
    _insert_journal(journal_db, second=0, inferred_state="EXPANSION")
    genuine = _insert_journal(journal_db, second=10, inferred_state="CONTRACTION", state_confidence=0.65)
    provisional = _insert_journal(
        journal_db, second=20, inferred_state="PROVISIONAL_OPTIONS_FLOW", state_confidence=0.99,
        action_taken="PROVISIONAL OPTIONS_FLOW NVDA",
        counterfactual="provisional-from-contract:00000000-0000-0000-0000-000000000001",
        contradiction_flags={"signal_type": "options_flow", "source": "options",
                             "producer_module": "intelligence.options", "context": {}},
        source_contract_id="00000000-0000-0000-0000-000000000001",
    )
    before = _raw_journal_evidence(journal_db)
    with journal_db.connect() as conn:
        assert conn.execute(text("SELECT id FROM decision_journal ORDER BY decision_timestamp DESC LIMIT 1")).scalar_one() == provisional
    assert tuple(_latest_qualified_journal(journal_db)) == (genuine, "CONTRACTION", 0.65)
    assert _raw_journal_evidence(journal_db) == before


_JOURNAL_PROVISIONAL_MARKERS = [
    pytest.param({"inferred_state": "PROVISIONAL_FUNDAMENTAL_DIVERGENCE"}, id="state-financial"),
    pytest.param({"inferred_state": "provisional_options_flow"}, id="state-case-insensitive-unrelated"),
    pytest.param({"inferred_state": "PROVISIONAL_"}, id="state-empty-suffix"),
    pytest.param({"action_taken": "PROVISIONAL"}, id="action-exact"),
    pytest.param({"action_taken": "PROVISIONAL INSIDER AAPL"}, id="action-prefix"),
    pytest.param({"action_taken": "provisional options NVDA"}, id="action-case-insensitive"),
    pytest.param({"counterfactual": "provisional-from-contract:abc"}, id="counterfactual-prefix"),
    pytest.param({"counterfactual": "PROVISIONAL-FROM-CONTRACT:abc"}, id="counterfactual-case-insensitive"),
]
for _location in ("top-level", "context"):
    for _key, _value, _label in (
        ("signal_type", "fundamental_divergence", "signal-type"),
        ("producer_module", "intelligence.fundamental_divergence", "producer"),
        ("source", "fundamental_divergence", "source-exact"),
        ("source", "fundamental_divergence:Technology", "source-sector"),
    ):
        _flags = {_key: _value}
        if _location == "context":
            _flags = {"context": _flags}
        _JOURNAL_PROVISIONAL_MARKERS.append(pytest.param(
            {"contradiction_flags": _flags}, id=f"{_location}-{_label}",
        ))


@pytest.mark.parametrize("marker", _JOURNAL_PROVISIONAL_MARKERS)
def test_pg_each_provisional_signature_alone_is_ineligible(journal_db, marker):
    _insert_journal(journal_db, **marker)
    before = _raw_journal_evidence(journal_db)
    assert _latest_qualified_journal(journal_db) is None
    assert _raw_journal_evidence(journal_db) == before


@pytest.mark.parametrize("flags", [
    pytest.param(None, id="json-null"),
    pytest.param({}, id="empty-object"),
    pytest.param([], id="empty-array"),
    pytest.param("fundamental_divergence", id="string"),
    pytest.param(1, id="number"),
    pytest.param(True, id="boolean"),
    pytest.param([{"signal_type": "fundamental_divergence"}], id="object-in-array"),
    pytest.param({"context": None}, id="null-context"),
    pytest.param({"context": ["fundamental_divergence"]}, id="array-context"),
    pytest.param({"context": "fundamental_divergence"}, id="string-context"),
    pytest.param({"signal_type": ["fundamental_divergence"]}, id="wrong-type-signal"),
    pytest.param({"producer_module": {"name": "intelligence.fundamental_divergence"}}, id="wrong-type-producer"),
    pytest.param({"source": None}, id="null-source"),
    pytest.param({"source": "fundamental_divergence_extra:Technology"}, id="source-prefix-boundary"),
    pytest.param({"signal_type": "fundamental_divergence_extra"}, id="signal-boundary"),
    pytest.param({"producer_module": "intelligence.fundamental_divergence_extra"}, id="producer-boundary"),
    pytest.param({"source": "options", "signal_type": "insider"}, id="unrelated-source"),
])
def test_pg_nonmatching_json_shapes_are_eligible_without_evidence_guessing(journal_db, flags):
    genuine = _insert_journal(journal_db, contradiction_flags=flags)
    before = _raw_journal_evidence(journal_db)
    assert _latest_qualified_journal(journal_db)[0] == genuine
    assert _raw_journal_evidence(journal_db) == before


def test_pg_ordinary_journal_and_contract_link_are_not_blanket_excluded(journal_db):
    genuine = _insert_journal(
        journal_db, inferred_state="NON_PROVISIONAL_EXPANSION",
        action_taken="REVIEW PROVISIONAL signal", counterfactual="Consider provisional-from-contract:abc",
        source_contract_id="00000000-0000-0000-0000-000000000002",
        outcome_value=12.5, verdict="HELPED", annotation="Genuine decision and realized outcome",
    )
    before = _raw_journal_evidence(journal_db)
    assert _latest_qualified_journal(journal_db)[0] == genuine
    assert _raw_journal_evidence(journal_db) == before


def test_pg_invalid_json_syntax_cannot_enter_journal_evidence(journal_db):
    from sqlalchemy.exc import DataError
    genuine = _insert_journal(journal_db)
    before = _raw_journal_evidence(journal_db)
    with pytest.raises(DataError):
        with journal_db.begin() as conn:
            conn.execute(text("""INSERT INTO decision_journal (
                model_version_id,inferred_state,state_confidence,transition_probability,
                contradiction_flags,grid_recommendation,baseline_recommendation,
                action_taken,counterfactual,operator_confidence)
                VALUES (1,'EXPANSION',0.7,0.1,CAST(:flags AS JSONB),
                        'HOLD','HOLD','HOLD','synthetic','HIGH')"""), {"flags": "{not valid json"})
    assert _latest_qualified_journal(journal_db)[0] == genuine
    assert _raw_journal_evidence(journal_db) == before


@pytest.mark.parametrize("location", ("top-level", "context"))
@pytest.mark.parametrize("key,canonical", [
    pytest.param("signal_type", "fundamental_divergence", id="signal-type"),
    pytest.param("producer_module", "intelligence.fundamental_divergence", id="producer"),
    pytest.param("source", "fundamental_divergence", id="source-exact"),
    pytest.param("source", "fundamental_divergence:Technology", id="source-sector"),
])
@pytest.mark.parametrize("variant", (
    "uppercase", "mixed-case", "outer-spaces", "outer-tabs", "outer-newlines",
    "mixed-outer-whitespace", "unicode-outer-whitespace",
))
def test_pg_normalized_financial_markers_cannot_become_current(journal_db, location, key, canonical, variant):
    from types import SimpleNamespace
    from contracts.financial_signal_guard import is_financial_divergence_signal

    # All 29 whitespace code points stripped by Python, including controls and
    # Unicode separators. One untrimmed code point must fail this parity check.
    whitespace = (
        "\t\n\v\f\r\x1c\x1d\x1e\x1f \x85\xa0\u1680"
        "\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a"
        "\u2028\u2029\u202f\u205f\u3000"
    )
    assert len(whitespace) == 29 and whitespace.strip() == ""
    value = {
        "unicode-outer-whitespace": whitespace + canonical.upper() + whitespace,
        "uppercase": canonical.upper(),
        "mixed-case": canonical.title(),
        "outer-spaces": "  " + canonical + "  ",
        "outer-tabs": "\t" + canonical.upper() + "\t",
        "outer-newlines": "\n" + canonical.upper() + "\r\n",
        "mixed-outer-whitespace": " \t\r\n\v\f" + canonical.title() + "\f\v\n\r\t ",
    }[variant]
    markers = {key: value}
    # The SQL read guard must recognize the same string marker as event replay.
    assert is_financial_divergence_signal(SimpleNamespace(**markers))
    flags = markers if location == "top-level" else {"context": markers}
    _insert_journal(journal_db, second=20, contradiction_flags=flags, state_confidence=0.99)
    before = _raw_journal_evidence(journal_db)
    assert _latest_qualified_journal(journal_db) is None
    assert _raw_journal_evidence(journal_db) == before

    genuine = _insert_journal(journal_db, second=10, inferred_state="CONTRACTION", state_confidence=0.65)
    before = _raw_journal_evidence(journal_db)
    assert tuple(_latest_qualified_journal(journal_db)) == (genuine, "CONTRACTION", 0.65)
    assert _raw_journal_evidence(journal_db) == before


@pytest.mark.parametrize("location", ("top-level", "context"))
@pytest.mark.parametrize("key,value", [
    pytest.param("signal_type", "FUNDAMENTAL_DIVERGENCE_EXTRA", id="signal-suffix"),
    pytest.param("signal_type", "FUNDAMENTAL_DIVERGENCE:Technology", id="signal-colon"),
    pytest.param("producer_module", "INTELLIGENCE.FUNDAMENTAL_DIVERGENCE_EXTRA", id="producer-suffix"),
    pytest.param("producer_module", "OTHER.INTELLIGENCE.FUNDAMENTAL_DIVERGENCE", id="producer-prefix"),
    pytest.param("producer_module", "INTELLIGENCE.FUNDAMENTAL_DIVERGENCE.EXTRA", id="producer-child"),
    pytest.param("source", "FUNDAMENTAL_DIVERGENCE_EXTRA:Technology", id="source-suffix"),
    pytest.param("source", "OTHER:FUNDAMENTAL_DIVERGENCE", id="source-prefix"),
    pytest.param("source", "FUNDAMENTAL_DIVERGENCE-NOTICE", id="source-hyphen"),
    pytest.param("source", "FUNDAMENTAL_DIVERGENCE :Technology", id="source-inner-space"),
    pytest.param("source", "FUNDAMENTAL_DIVERGENCE\t:Technology", id="source-inner-tab"),
    pytest.param("source", "FUNDAMENTAL_DIVERGENCE\n:Technology", id="source-inner-newline"),
    pytest.param("source", "PRE_FUNDAMENTAL_DIVERGENCE", id="source-other-family"),
    pytest.param("signal_type", "FUNDAMENTAL_D\u0130VERGENCE", id="signal-dotted-i"),
    pytest.param("signal_type", "fundamental_d\u0131vergence", id="signal-dotless-i"),
    pytest.param("producer_module", "\u0130NTELLIGENCE.FUNDAMENTAL_DIVERGENCE", id="producer-dotted-i"),
    pytest.param("source", "\uff26UNDAMENTAL_DIVERGENCE:Technology", id="source-fullwidth-f"),
    pytest.param("source", "FUNDAMENTAL_DIVERGENCE\uff1aTechnology", id="source-fullwidth-colon"),
    pytest.param("source", "\u200bFUNDAMENTAL_DIVERGENCE", id="source-zero-width-space"),
])
def test_pg_normalized_near_matches_remain_eligible(journal_db, location, key, value):
    from types import SimpleNamespace
    from contracts.financial_signal_guard import is_financial_divergence_signal

    markers = {key: " \t\n" + value + "\r\t "}
    assert not is_financial_divergence_signal(SimpleNamespace(**markers))
    flags = markers if location == "top-level" else {"context": markers}
    genuine = _insert_journal(journal_db, contradiction_flags=flags)
    before = _raw_journal_evidence(journal_db)
    assert _latest_qualified_journal(journal_db)[0] == genuine
    assert _raw_journal_evidence(journal_db) == before


@pytest.mark.parametrize("location", ("top-level", "context"))
@pytest.mark.parametrize("key", ("signal_type", "producer_module", "source"))
@pytest.mark.parametrize("value", [
    pytest.param([" \tFUNDAMENTAL_DIVERGENCE\n"], id="marker-in-array"),
    pytest.param({"name": "\nINTELLIGENCE.FUNDAMENTAL_DIVERGENCE\t"}, id="marker-in-object"),
    pytest.param(None, id="null-marker"),
    pytest.param(123, id="numeric-marker"),
    pytest.param(True, id="boolean-marker"),
])
def test_pg_normalization_never_coerces_nonstring_markers(journal_db, location, key, value):
    from types import SimpleNamespace
    from contracts.financial_signal_guard import is_financial_divergence_signal

    markers = {key: value}
    assert not is_financial_divergence_signal(SimpleNamespace(**markers))
    flags = markers if location == "top-level" else {"context": markers}
    genuine = _insert_journal(journal_db, contradiction_flags=flags)
    before = _raw_journal_evidence(journal_db)
    assert _latest_qualified_journal(journal_db)[0] == genuine
    assert _raw_journal_evidence(journal_db) == before


def _snapshot_with_extra_facts(extra: dict, *, second: int, report_dates=None):
    """The ``snapshot()`` capture plus extra Company Facts concepts, at a later request time."""
    import json
    from datetime import datetime, timedelta, timezone

    from ingestion.altdata.sec_financial_snapshots import prepare_snapshot

    base = snapshot()
    body = json.loads(base.source_body)
    body["facts"]["us-gaap"].update(extra)
    started = datetime(2026, 11, 1, tzinfo=timezone.utc) + timedelta(seconds=second)
    return prepare_snapshot(source_body=json.dumps(body).encode(), actor_id="nvda", cik="1045810",
                            request_started_at=started, received_at=started + timedelta(seconds=1),
                            fx_rates={"USD": 1.0}, report_dates=report_dates)


def test_pg_tolerated_legacy_entry_cannot_remove_an_existing_current_row(snapshot_db):
    """Codex 6061699121: a newly tolerated legacy no-start entry changes nothing the
    normalizer emits, so the writer replaces the projection with the same current rows;
    a current-period no-start entry is refused before the writer and leaves them in place."""
    import pytest
    from store.sec_financial_snapshots import write_snapshot

    engine = snapshot_db
    first = snapshot()
    assert write_snapshot(engine, first)["status"] == "applied"
    before = stored(engine)
    assert len(before) == 1

    legacy = {"PaymentsToAcquireBusinessesNetOfCashAcquired": {"units": {"USD": [dict(
        end="2020-03-18", val=1000, accn="0001045810-21-000012", filed="2021-02-22", form="10-K", fp="FY")]}}}
    later = _snapshot_with_extra_facts(legacy, second=10)
    assert later.tolerated_legacy_entries == 1 and later.snapshot_id != first.snapshot_id
    assert later.rows == first.rows
    result = write_snapshot(engine, later)
    assert result["status"] == "applied" and result["rows_written"] == len(first.rows)
    assert result["rows_removed"] == len(first.rows)             # the same owned rows replaced by the same rows
    assert stored(engine) == before                              # no current row lost or changed

    current_period = {"PaymentsOfDividends": {"units": {"USD": [dict(
        end="2026-07-26", val=5, accn="0001045810-26-000075", filed="2026-08-26", form="10-Q", fp="Q2")]}}}
    with pytest.raises(ValueError, match="duration"):
        _snapshot_with_extra_facts(current_period, second=20)
    assert stored(engine) == before                              # refused before the writer: projection retained


_OWN = "0001045810-26-000075"


def _rd():
    from datetime import date
    return {_OWN: date(2026, 7, 26)}                              # EDGAR submissions reportDate for the accession


def _submissions_body(dates=None, cik=1045810):
    """A raw EDGAR submissions response whose recent filings carry the given accession -> reportDate pairs."""
    import json
    dates = dates or {_OWN: "2026-07-26"}
    return json.dumps({"cik": cik, "name": "Synthetic Test Issuer", "filings": {"recent": {
        "accessionNumber": list(dates), "reportDate": list(dates.values())}}}).encode()


def _with_provenance(snapshot_, *, url="https://data.sec.gov/submissions/CIK0001045810.json",
                     received="2026-11-02T00:00:00+00:00", body=None, digest=None):
    """Attach the complete evidence provenance a puller would: matched url, lowercase sha256, aware receipt, response."""
    import dataclasses
    import hashlib

    body = _submissions_body() if body is None else body
    return dataclasses.replace(snapshot_, report_date_source=(url, digest or hashlib.sha256(body).hexdigest(), received),
                               report_date_source_body=body)


def _stray_fact(tag="PaymentsOfDividends", end="2026-06-15"):
    return {tag: {"units": {"USD": [dict(end=end, val=1000, accn=_OWN, filed="2026-08-26", form="10-Q", fp="Q2")]}}}


def _custom_snapshot(facts: dict, *, second: int, report_dates=None):
    import json
    from datetime import datetime, timedelta, timezone

    from ingestion.altdata.sec_financial_snapshots import prepare_snapshot

    started = datetime(2026, 11, 1, tzinfo=timezone.utc) + timedelta(seconds=second)
    return prepare_snapshot(
        source_body=json.dumps({"cik": 1045810, "entityName": "Synthetic Test Issuer", "facts": {"us-gaap": facts}}).encode(),
        actor_id="nvda", cik="1045810", request_started_at=started, received_at=started + timedelta(seconds=1),
        fx_rates={"USD": 1.0}, report_dates=report_dates)


_REV = "RevenueFromContractWithCustomerExcludingAssessedTax"


def test_pg_tolerated_stray_point_with_edgar_evidence_cannot_remove_an_existing_current_row(snapshot_db):
    """An event-dated start-less point, with EDGAR's reportDate for its accession, a qualifying flow
    at that date and the point not a period end, changes nothing the normalizer emits: the writer
    replaces the owned projection with the same rows. Without the evidence, or at the report date,
    it is refused before the writer."""
    import pytest
    from store.sec_financial_snapshots import write_snapshot

    engine = snapshot_db
    first = snapshot()
    assert write_snapshot(engine, first)["status"] == "applied"
    before = stored(engine)
    assert len(before) == 1

    with pytest.raises(ValueError, match="duration"):
        _snapshot_with_extra_facts(_stray_fact(), second=25)                      # no evidence: fail closed
    later = _with_provenance(_snapshot_with_extra_facts(_stray_fact(), second=30, report_dates=_rd()))
    assert later.tolerated_stray_points == (("PaymentsOfDividends", "2026-06-15", _OWN),)
    assert later.report_dates == ((_OWN, "2026-07-26"),)
    assert later.rows == first.rows and later.snapshot_id != first.snapshot_id
    result = write_snapshot(engine, later)
    assert result["status"] == "applied" and result["rows_written"] == len(first.rows)
    assert result["rows_removed"] == len(first.rows)             # the same owned rows replaced by the same rows
    assert stored(engine) == before                              # no current row lost or changed

    with pytest.raises(ValueError, match="duration"):
        _snapshot_with_extra_facts(_stray_fact(end="2026-07-26"), second=40, report_dates=_rd())
    assert stored(engine) == before                              # refused before the writer: projection retained


def test_pg_nearer_comparative_and_comparative_only_evidence_cannot_hide_the_current_period(snapshot_db):
    """Codex 6081201501 / 6081574064: the filing's real report date is 2026-07-26 (EDGAR) but its only
    qualifying flows are earlier comparatives; the 2026 Revenue is start-less. Refused, owned row kept."""
    import pytest
    from store.sec_financial_snapshots import write_snapshot

    engine = snapshot_db
    assert write_snapshot(engine, snapshot())["status"] == "applied"
    before = stored(engine)
    for comparative in (dict(start="2025-04-28", end="2025-07-27"), dict(start="2026-01-27", end="2026-04-26")):
        facts = {_REV: {"units": {"USD": [
            dict(val=1, accn=_OWN, filed="2026-08-26", form="10-Q", fp="Q2", **comparative),
            dict(end="2026-07-26", val=100, accn=_OWN, filed="2026-08-26", form="10-Q", fp="Q2")]}}}
        with pytest.raises(ValueError, match="duration"):
            _custom_snapshot(facts, second=50, report_dates=_rd())
    assert stored(engine) == before                              # the 2026 owned row is still there


def test_pg_fresh_insert_with_only_comparative_evidence_stores_nothing(snapshot_db):
    import pytest

    engine = snapshot_db
    facts = {_REV: {"units": {"USD": [
        dict(start="2026-01-27", end="2026-04-26", val=1, accn=_OWN, filed="2026-08-26", form="10-Q", fp="Q2"),
        dict(end="2026-07-26", val=100, accn=_OWN, filed="2026-08-26", form="10-Q", fp="Q2")]}}}
    with pytest.raises(ValueError, match="duration"):
        _custom_snapshot(facts, second=100, report_dates=_rd())
    assert stored(engine) == []


def test_pg_a_nonqualifying_duration_cannot_lead_to_an_empty_projection(snapshot_db):
    """Only a two-day 8-K span supports the accession; the normalizer rejects it, so a tolerated point
    would otherwise yield an empty projection and clear owned rows."""
    import pytest
    from store.sec_financial_snapshots import write_snapshot

    engine = snapshot_db
    assert write_snapshot(engine, snapshot())["status"] == "applied"
    before = stored(engine)
    facts = {_REV: {"units": {"USD": [dict(start="2026-07-24", end="2026-07-26", val=1, accn=_OWN,
                                           filed="2026-08-26", form="8-K", fp="Q2")]}}, **_stray_fact()}
    with pytest.raises(ValueError, match="duration"):
        _custom_snapshot(facts, second=60, report_dates=_rd())
    assert stored(engine) == before


def test_pg_a_tolerating_capture_never_erases_a_stored_period_but_a_clean_one_may(snapshot_db):
    """The writer's own guard (no inference involved): a capture that tolerated start-less points and
    lacks a previously owned key is rejected with the projection retained; the same removal in a capture
    with no tolerated point is the ordinary replacement and still applies."""
    from store.sec_financial_snapshots import write_snapshot

    engine = snapshot_db
    dividends = {"PaymentsOfDividends": {"units": {"USD": [dict(
        start="2026-04-27", end="2026-07-26", val=40, accn=_OWN, filed="2026-08-26", form="10-Q", fp="Q2")]}}}
    first = _snapshot_with_extra_facts(dividends, second=70)
    assert len(first.rows) == 2 and write_snapshot(engine, first)["status"] == "applied"
    before = stored(engine)
    assert len(before) == 2

    tolerating = _with_provenance(_snapshot_with_extra_facts(_stray_fact("PaymentsForRepurchaseOfCommonStock"), second=80, report_dates=_rd()))
    assert tolerating.tolerated_stray_points and len(tolerating.rows) == 1       # the dividends row is gone from the source
    result = write_snapshot(engine, tolerating)
    assert result["status"] == "rejected_stray_would_remove_rows" and result["rows_written"] == 0
    assert stored(engine) == before                                             # nothing erased

    clean = _snapshot_with_extra_facts({}, second=90)                           # same removal, nothing tolerated
    assert not clean.tolerated_stray_points
    assert write_snapshot(engine, clean)["status"] == "applied"
    assert len(stored(engine)) == 1


def test_pg_dropping_tolerance_metadata_or_evidence_cannot_bypass_the_writer_guard(snapshot_db):
    """With owned rows stored, the honest tolerating capture is rejected by the guard; the same snapshot
    with tolerated_stray_points or report_dates removed (same bytes) must fail the write-boundary replay."""
    from dataclasses import replace

    import pytest
    from store.sec_financial_snapshots import write_snapshot

    engine = snapshot_db
    dividends = {"PaymentsOfDividends": {"units": {"USD": [dict(
        start="2026-04-27", end="2026-07-26", val=40, accn=_OWN, filed="2026-08-26", form="10-Q", fp="Q2")]}}}
    assert write_snapshot(engine, _snapshot_with_extra_facts(dividends, second=110))["status"] == "applied"
    before = stored(engine)
    assert len(before) == 2

    tolerating = _with_provenance(_snapshot_with_extra_facts(_stray_fact("PaymentsForRepurchaseOfCommonStock"), second=120, report_dates=_rd()))
    assert tolerating.tolerated_stray_points and len(tolerating.rows) == 1
    for drifted in (replace(tolerating, tolerated_stray_points=()), replace(tolerating, report_dates=())):
        with pytest.raises(ValueError):
            write_snapshot(engine, drifted)
        assert stored(engine) == before                          # the owned dividends row survives
    assert write_snapshot(engine, tolerating)["status"] == "rejected_stray_would_remove_rows"
    assert stored(engine) == before


def _archive_row(engine, snapshot_id):
    with engine.connect() as conn:
        return dict(conn.execute(text(
            "SELECT report_date_evidence, report_date_source_gzip FROM sec_financial_snapshots WHERE snapshot_id = :i"),
            {"i": snapshot_id}).mappings().one())


def test_pg_the_archive_row_alone_reproduces_a_snapshot_and_its_complete_provenance(snapshot_db):
    """Codex 6081926340 / 6082027007: the used mapping, the matched source reference AND the raw
    submissions response are archived; a replay from the immutable row alone restores all of it and gives
    the same identity. An ordinary snapshot stores NULLs and still replays."""
    import gzip

    import pytest
    from store.sec_financial_snapshots import replay_archived_snapshot, write_snapshot

    engine = snapshot_db
    ordinary = snapshot()
    assert write_snapshot(engine, ordinary)["status"] == "applied"
    row = _archive_row(engine, ordinary.snapshot_id)
    assert row["report_date_evidence"] is None and row["report_date_source_gzip"] is None
    assert replay_archived_snapshot(engine, ordinary.snapshot_id) == ordinary

    body = _submissions_body()
    tolerating = _with_provenance(_snapshot_with_extra_facts(_stray_fact(), second=30, report_dates=_rd()), body=body)
    assert write_snapshot(engine, tolerating)["status"] == "applied"
    row = _archive_row(engine, tolerating.snapshot_id)
    assert row["report_date_evidence"] == {
        "report_dates": {_OWN: "2026-07-26"},
        "source": {"url": "https://data.sec.gov/submissions/CIK0001045810.json",
                   "sha256": tolerating.report_date_source[1], "received_at": "2026-11-02T00:00:00+00:00"}}
    assert gzip.decompress(bytes(row["report_date_source_gzip"])) == body
    replayed = replay_archived_snapshot(engine, tolerating.snapshot_id)
    assert replayed == tolerating and replayed.snapshot_id == tolerating.snapshot_id
    assert replayed.report_date_source == tolerating.report_date_source                 # the complete triple
    assert replayed.report_date_source_body == body
    assert replayed.tolerated_stray_points == tolerating.tolerated_stray_points and replayed.rows == tolerating.rows

    # without the archived evidence the identity cannot be reproduced; without the response it cannot be verified
    with engine.begin() as conn:
        conn.execute(text("UPDATE sec_financial_snapshots SET report_date_source_gzip = NULL, report_date_evidence = NULL "
                          "WHERE snapshot_id = :i"), {"i": tolerating.snapshot_id})
    with pytest.raises(ValueError):
        replay_archived_snapshot(engine, tolerating.snapshot_id)
    with pytest.raises(ValueError, match="No archived snapshot"):
        replay_archived_snapshot(engine, "0" * 64)


def test_pg_the_write_boundary_requires_complete_matched_provenance_whenever_evidence_was_used(snapshot_db):
    """Codex 6082027007: evidence-dependent snapshots no longer apply with source=None, a malformed
    url / hash / receipt clock, a url for another CIK, a response that does not hash to the reference or
    does not yield the used pairs; provenance without evidence is refused too. Nothing is stored."""
    import dataclasses

    import pytest
    from store.sec_financial_snapshots import write_snapshot

    engine = snapshot_db
    good = _with_provenance(_snapshot_with_extra_facts(_stray_fact(), second=30, report_dates=_rd()))
    url = "https://data.sec.gov/submissions/CIK0001045810.json"
    other = _submissions_body({_OWN: "2026-04-26"})                   # a response that yields different pairs
    cases = {
        "no source": dataclasses.replace(good, report_date_source=None),
        "no response": dataclasses.replace(good, report_date_source_body=None),
        "empty response": dataclasses.replace(good, report_date_source_body=b""),
        "url for another cik": _with_provenance(good, url="https://data.sec.gov/submissions/CIK0000000099.json"),
        "malformed url": _with_provenance(good, url="http://example.com/x"),
        "uppercase hash": dataclasses.replace(good, report_date_source=(url, good.report_date_source[1].upper(), "2026-11-02T00:00:00+00:00")),
        "short hash": dataclasses.replace(good, report_date_source=(url, "abc", "2026-11-02T00:00:00+00:00")),
        "hash not of the response": dataclasses.replace(good, report_date_source=(url, "0" * 64, "2026-11-02T00:00:00+00:00")),
        "naive receipt": _with_provenance(good, received="2026-11-27T00:00:00"),
        "garbled receipt": _with_provenance(good, received="yesterday"),
        "response yields other pairs": _with_provenance(good, body=other),
        "response not json": _with_provenance(good, body=b"not json"),
        "response for another cik": _with_provenance(good, body=_submissions_body(cik=99)),
    }
    for name, bad in cases.items():
        with pytest.raises(ValueError):
            write_snapshot(engine, bad)
        assert stored(engine) == [], name
    ordinary = snapshot()
    with pytest.raises(ValueError, match="without report-date evidence"):
        write_snapshot(engine, _with_provenance(ordinary))
    assert write_snapshot(engine, good)["status"] == "applied"


def test_pg_the_archive_checks_reject_invalid_evidence_rows_and_unpaired_columns(snapshot_db):
    """The CHECKs are an evidence validation, not only an object test."""
    import json

    import pytest
    from sqlalchemy.exc import IntegrityError

    engine = snapshot_db
    good = {"report_dates": {_OWN: "2026-07-26"},
            "source": {"url": "https://data.sec.gov/submissions/CIK0001045810.json", "sha256": "a" * 64,
                       "received_at": "2026-11-02T00:00:00+00:00"}}

    def insert(evidence, gz, key):
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO sec_financial_snapshots (snapshot_id, actor_id, cik, source_url, source_sha256, normalized_sha256, "
                "normalizer_version, source_gzip, normalized_rows, fx_inputs, first_request_started_at, first_received_at, "
                "report_date_evidence, report_date_source_gzip) VALUES (:k, 'x', '1', 'u', :h, :h, 'v', :g, "
                "CAST('[]' AS JSONB), CAST('{}' AS JSONB), NOW(), NOW(), CAST(:e AS JSONB), :z)"),
                {"k": key.ljust(64, "0"), "h": "b" * 64, "g": b"x",
                 "e": None if evidence is None else json.dumps(evidence), "z": gz})

    insert(good, b"z", "ok")                                          # a valid pair inserts
    insert(None, None, "ordinary")                                    # NULL, NULL inserts
    bads = {
        "empty mapping": {**good, "report_dates": {}},
        "no source": {"report_dates": good["report_dates"]},
        "uppercase hash": {**good, "source": {**good["source"], "sha256": "A" * 64}},
        "short hash": {**good, "source": {**good["source"], "sha256": "a" * 63}},
        "url for a non-sec host": {**good, "source": {**good["source"], "url": "https://example.com/submissions/CIK0001045810.json"}},
        "short cik in url": {**good, "source": {**good["source"], "url": "https://data.sec.gov/submissions/CIK1045810.json"}},
        "naive receipt": {**good, "source": {**good["source"], "received_at": "2026-11-27T00:00:00"}},
        "not an object": [1],
    }
    for i, (name, evidence) in enumerate(bads.items()):
        with pytest.raises(IntegrityError):
            insert(evidence, b"z", f"bad{i}")
    with pytest.raises(IntegrityError):
        insert(good, None, "nobody")                                  # evidence without the response
    with pytest.raises(IntegrityError):
        insert(None, b"z", "nometa")                                  # response without the evidence


def _head(engine):
    with engine.connect() as conn:
        return dict(conn.execute(text(
            "SELECT h.request_started_at, h.received_at, s.first_received_at "
            "FROM sec_financial_snapshot_heads h JOIN sec_financial_snapshots s ON s.snapshot_id = h.snapshot_id "
            "WHERE h.actor_id = 'nvda'")).mappings().one())


def test_pg_evidence_receipt_is_bounded_by_the_write_clock_and_keeps_the_facts_receipt_separate(snapshot_db):
    """Codex 6082240621: a future evidence receipt is refused (nothing applied, no head); a normal receipt
    later than the Company Facts receipt is accepted and replays; the facts receipt stays in the archive
    while the head's availability time is the later of the two receipts."""
    from datetime import datetime, timezone

    import pytest
    from store.sec_financial_snapshots import available_at, replay_archived_snapshot, write_snapshot

    engine = snapshot_db
    base = _snapshot_with_extra_facts(_stray_fact(), second=30, report_dates=_rd())     # facts received 2026-11-01 00:00:31
    for future in ("2099-01-01T00:00:00+00:00", "2099-01-01T00:00:00Z"):
        with pytest.raises(ValueError, match="future"):
            write_snapshot(engine, _with_provenance(base, received=future))
    with engine.connect() as conn:
        assert stored(engine) == [] and conn.execute(text("SELECT count(*) FROM sec_financial_snapshot_heads")).scalar() == 0

    later = _with_provenance(base, received="2026-11-02T00:00:00+00:00")                # evidence arrived a day after the facts
    assert available_at(later) == datetime(2026, 11, 2, tzinfo=timezone.utc) > later.received_at
    assert write_snapshot(engine, later)["status"] == "applied"
    head = _head(engine)
    assert head["first_received_at"] == later.received_at                              # the facts receipt, retained
    assert head["received_at"] == datetime(2026, 11, 2, tzinfo=timezone.utc)           # usable only after BOTH receipts
    assert head["request_started_at"] == later.request_started_at
    assert replay_archived_snapshot(engine, later.snapshot_id) == later


def test_pg_an_evidence_receipt_before_the_facts_receipt_leaves_availability_at_the_facts_receipt(snapshot_db):
    from store.sec_financial_snapshots import available_at, write_snapshot

    engine = snapshot_db
    earlier = _with_provenance(_snapshot_with_extra_facts(_stray_fact(), second=30, report_dates=_rd()),
                               received="2026-10-30T00:00:00+00:00")
    assert available_at(earlier) == earlier.received_at
    assert write_snapshot(engine, earlier)["status"] == "applied"
    assert _head(engine)["received_at"] == earlier.received_at


def test_pg_ordinary_snapshots_keep_the_facts_receipt_as_their_availability(snapshot_db):
    from store.sec_financial_snapshots import available_at, write_snapshot

    engine = snapshot_db
    ordinary = snapshot()
    assert available_at(ordinary) == ordinary.received_at
    assert write_snapshot(engine, ordinary)["status"] == "applied"
    head = _head(engine)
    assert head["received_at"] == ordinary.received_at == head["first_received_at"]
