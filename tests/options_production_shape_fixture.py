"""Explicitly owned private PG14 fixture. Never falls back to another database.

Private catalog/schema inputs are required and remain outside the repository.
Only original relation/function OIDs adapt; names, owners, ACL and guard logic
retain the captured public shape. Each test database is retained for diagnosis.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import time
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import DBAPIError

CATALOG_SHA = "7ae5deaf4e7e54c8c3e74421a98b1cfcaee4d320ef8578fc221c5b177d8e3785"


def split_sql(sql):
    start = i = 0
    quote = dollar = None
    line = block = False
    while i < len(sql):
        if line:
            if sql[i] == "\n":
                line = False
        elif block:
            if sql[i:i+2] == "*/":
                block = False
                i += 1
        elif quote:
            if sql[i] == quote:
                if sql[i:i+2] == quote * 2:
                    i += 1
                else:
                    quote = None
        elif dollar:
            if sql.startswith(dollar, i):
                i += len(dollar) - 1
                dollar = None
        elif sql[i:i+2] == "--":
            line = True
            i += 1
        elif sql[i:i+2] == "/*":
            block = True
            i += 1
        elif sql[i] in "'\"":
            quote = sql[i]
        elif sql[i] == "$":
            match = re.match(r"\$[A-Za-z_0-9]*\$", sql[i:])
            if match:
                dollar = match[0]
                i += len(dollar) - 1
        elif sql[i] == ";":
            yield sql[start:i+1]
            start = i + 1
        i += 1
    if sql[start:].strip():
        yield sql[start:]


def owned_fixture(root, *, legacy_atomic=False, initial_drift=None):
    context = os.environ.get("OPTIONS_PRODUCTION_SHAPE_CONTEXT")
    if not context:
        pytest.skip("explicit owned PG14 production-shape context/private catalog required")
    ctx = json.loads(Path(context).read_text())
    task = Path(ctx["task"])
    assert task.is_absolute() and task.resolve() == task
    assert task.name.startswith("codex-options-production-shape-")
    assert task.parent == Path("/tmp")
    for path in (task, task / "data", task / "socket"):
        assert path.stat().st_uid == os.getuid() and path.stat().st_mode & 0o777 == 0o700
    assert (task / "data/PG_VERSION").read_text().strip() == "14"
    pid = (task / "data/postmaster.pid").read_text().splitlines()
    assert pid[1] == str(task / "data") and int(pid[3]) == ctx["port"]
    assert Path(f"/proc/{int(pid[0])}/exe").resolve() == Path("/usr/lib/postgresql/14/bin/postgres")
    assert str(task / "data").encode() in Path(f"/proc/{int(pid[0])}/cmdline").read_bytes().split(b"\0")
    catalog_path = Path(ctx["catalog"])
    schema_path = Path(ctx["schema"])
    assert catalog_path.parent == task and schema_path.parent == task
    assert hashlib.sha256(catalog_path.read_bytes()).hexdigest() == CATALOG_SHA
    assert hashlib.sha256(schema_path.read_bytes()).hexdigest() == ctx["schema_sha256"]
    catalog = json.loads(catalog_path.read_text())
    assert catalog["db_writes"] == 0 and catalog["enabled_event_triggers"] == 0
    import psycopg2
    admin = psycopg2.connect(host="127.0.0.1", hostaddr="127.0.0.1", port=ctx["port"],
                            dbname="postgres", user="postgres", connect_timeout=5)
    with admin.cursor() as cur:
        cur.execute("SELECT current_database(),current_user,host(inet_server_addr()),inet_server_port(),"
                    "current_setting('data_directory'),current_setting('server_version_num')::int")
        assert cur.fetchone() == ("postgres", "postgres", "127.0.0.1", ctx["port"], str(task / "data"), ctx["server_version_num"])
    admin.rollback()
    admin.autocommit = True
    database = "options_shape_" + uuid4().hex[:12]
    with admin.cursor() as cur:
        cur.execute(f'CREATE DATABASE "{database}" OWNER grid')
    admin.close()
    admin = psycopg2.connect(host="127.0.0.1", hostaddr="127.0.0.1", port=ctx["port"],
                            dbname=database, user="postgres", connect_timeout=5)
    with admin.cursor() as cur:
        cur.execute("SET ROLE grid")
        cur.execute(schema_path.read_text())
        cur.execute("SELECT COALESCE(SUM(n_tup_ins+n_tup_upd+n_tup_del),0) FROM pg_stat_xact_user_tables")
        assert cur.fetchone()[0] == 0
    admin.commit()
    admin.close()
    engine = create_engine(f"postgresql+psycopg2://grid@127.0.0.1:{ctx['port']}/{database}")
    counts, operations = [], []

    def counter(conn):
        cursor = conn.connection.cursor()
        try:
            cursor.execute("SELECT COALESCE(SUM(n_tup_ins+n_tup_upd+n_tup_del),0)::bigint FROM pg_stat_xact_user_tables")
            return cursor.fetchone()[0]
        finally:
            cursor.close()

    def begin(conn):
        conn.info["fixture_baseline"] = counter(conn)
        conn.info["fixture_rows"] = 0
        conn.info["fixture_failed"] = False

    def after(conn, _cursor, statement, _parameters, _context, _many):
        rows = counter(conn) - conn.info["fixture_baseline"]
        assert 0 <= rows <= 50, "actual global DATA writes exceeded 50"
        conn.info["fixture_rows"] = rows
        operations.append({"phase": "SQL_ACK", "actual_global_data_rows": rows})

    def failed(exception_context):
        conn = exception_context.connection
        if conn is not None:
            conn.info["fixture_failed"] = True
            operations.append({"phase": "SQL_ERROR", "actual_global_data_rows_before_error": conn.info.get("fixture_rows", 0),
                               "error_type": type(exception_context.original_exception).__name__})

    def commit(conn):
        rows = counter(conn) - conn.info["fixture_baseline"]
        assert 0 <= rows <= 50
        counts.append(int(rows))
        operations.append({"phase": "COMMIT_ATTEMPT", "actual_global_data_rows": rows})

    def rollback(conn):
        rows = conn.info.get("fixture_rows", 0)
        if not conn.info.get("fixture_failed"):
            rows = counter(conn) - conn.info["fixture_baseline"]
        assert 0 <= rows <= 50
        operations.append({"phase": "ROLLBACK_ATTEMPT", "actual_global_data_rows_observed": rows,
                           "aborted_statement_counter_unavailable": conn.info.get("fixture_failed", False)})

    for name, callback in (("begin", begin), ("after_cursor_execute", after), ("handle_error", failed),
                           ("commit", commit), ("rollback", rollback)):
        event.listen(engine, name, callback)
    try:
        with engine.begin() as conn:
            row = conn.exec_driver_sql("SELECT current_user, current_database(), inet_server_port()")
            assert row.one() == ("grid", database, ctx["port"])
            assert conn.exec_driver_sql("SELECT rolsuper OR rolcreatedb OR rolcreaterole FROM pg_roles WHERE rolname=current_user").scalar_one() is False
            source_id = conn.exec_driver_sql("INSERT INTO source_catalog(name,base_url,cost_tier,latency_class,revision_behavior,trust_score,priority_rank) "
                "VALUES ('YFINANCE_OPTIONS','https://synthetic.invalid','FREE','EOD','OVERWRITE','MED',10) RETURNING id").scalar_one()
        legacy_images = None
        if legacy_atomic:
            with engine.begin() as conn:
                conn.exec_driver_sql("INSERT INTO options_capture_batches(capture_batch_id,ticker,snap_date,capture_ordinal,capture_started_at,capture_completed_at,row_count,spot_price,capture_source) VALUES ('synthetic-legacy','SPY','2026-10-02',1,'2026-10-02 14:00Z','2026-10-02 14:02Z',8,100,'options_puller')")
                for strike in range(8):
                    conn.execute(text("INSERT INTO options_snapshots_all(ticker,snap_date,expiry,opt_type,strike,capture_batch_id,capture_ordinal,capture_started_at,capture_completed_at,provider_regular_market_at) VALUES ('SPY','2026-10-02','2026-10-16','call',:strike,'synthetic-legacy',1,'2026-10-02 14:00Z','2026-10-02 14:02Z','2026-10-02 13:59Z')"), {"strike": strike + 90})
                conn.exec_driver_sql("INSERT INTO options_daily_signals(ticker,signal_date,put_call_ratio) VALUES ('SPY','2026-10-02',17)")
                fid = conn.exec_driver_sql("INSERT INTO feature_registry(name,family,description,transformation,transformation_version,normalization,missing_data_policy,eligible_from_date,model_eligible) VALUES ('spy_pcr','sentiment','preserved synthetic legacy metric','RAW',1,'ZSCORE','FORWARD_FILL','2024-04-01',true) RETURNING id").scalar_one()
                conn.execute(text("INSERT INTO resolved_series(feature_id,obs_date,release_date,vintage_date,value,source_priority_used) VALUES (:fid,'2026-10-02','2026-10-02','2026-10-02',17,:source)"), {"fid": fid, "source": source_id})
            with engine.connect() as conn:
                legacy_images = {name: conn.exec_driver_sql("SELECT row_to_json(t)::text FROM " + name + " t ORDER BY 1").scalars().all()
                                 for name in ("options_capture_batches", "options_snapshots_all", "options_daily_signals", "feature_registry", "resolved_series", "source_catalog")}
        if initial_drift in ("guard_body", "guard_missing"):
            # This function is postgres-owned in the capture. Administration is
            # confined to the just-created, checked private database.
            private_admin = psycopg2.connect(host="127.0.0.1", hostaddr="127.0.0.1", port=ctx["port"],
                                             dbname=database, user="postgres", connect_timeout=5)
            with private_admin.cursor() as cur:
                cur.execute("SELECT current_setting('data_directory'),current_database(),inet_server_port()")
                assert cur.fetchone() == (str(task / "data"), database, ctx["port"])
                if initial_drift == "guard_body":
                    cur.execute("CREATE OR REPLACE FUNCTION feature_registry_check_transformation_version() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$")
                else:
                    cur.execute("DROP FUNCTION feature_registry_check_transformation_version() CASCADE")
                cur.execute("SELECT COALESCE(SUM(n_tup_ins+n_tup_upd+n_tup_del),0) FROM pg_stat_xact_user_tables")
                assert cur.fetchone()[0] == 0
            private_admin.commit()
            private_admin.close()
        elif initial_drift and initial_drift != "counts":
            with engine.begin() as conn:
                conn.exec_driver_sql(initial_drift)
        packet = (root / "docs/handoffs/2026-10-02/options-bounded-publication.sql").read_text()

        def apply_packet(conn):
            nonlocal packet
            for name, relation in catalog["relations"].items():
                if relation["exists"]:
                    oid = conn.execute(text("SELECT CAST(:name AS regclass)::oid"), {"name": name}).scalar_one()
                    packet = packet.replace(f"'public.{name}'::regclass::oid <> {relation['oid']}",
                                            f"'public.{name}'::regclass::oid <> {oid}")
            for fid, func in catalog["trigger_functions"].items():
                if func[0] == "public":
                    oid = conn.execute(text("SELECT to_regprocedure(:name)::oid"), {"name": func[1] + "()"}).scalar_one()
                    if oid is not None:
                        packet = packet.replace(f"'public.{func[1]}()'::regprocedure::oid <> {fid}",
                                                f"'public.{func[1]}()'::regprocedure::oid <> {oid}")
            initial = {name: int(conn.exec_driver_sql("SELECT count(*) FROM " + name).scalar_one()) for name in
                       ("source_catalog", "feature_registry", "resolved_series", "options_daily_signals", "options_capture_batches", "options_snapshots_all")}
            if initial_drift == "counts":
                initial["source_catalog"] += 1
            conn.execute(text("SELECT set_config('grid.options_expected_counts',:counts,true)"), {"counts": json.dumps(initial)})
            for statement in split_sql(packet):
                # No parameter interpolation or SQLAlchemy bind interpretation of
                # catalog JSON. Observe actual counters after every statement.
                cursor = conn.connection.cursor()
                try:
                    cursor.execute(statement)
                except psycopg2.Error as exc:
                    conn.info["fixture_failed"] = True
                    operations.append({"phase": "SQL_ERROR", "actual_global_data_rows_before_error": conn.info["fixture_rows"],
                                       "error_type": type(exc).__name__})
                    raise
                finally:
                    cursor.close()
                after(conn, None, statement, None, None, False)
            assert conn.info["fixture_rows"] == 17
        try:
            with engine.begin() as conn:
                apply_packet(conn)
        except (DBAPIError, psycopg2.Error):
            if initial_drift is None:
                raise
            # Refusal must precede the first destructive DDL and first DATA.
            assert operations[-1]["phase"] == "ROLLBACK_ATTEMPT"
            assert operations[-1]["actual_global_data_rows_observed"] == 0
            with engine.connect() as conn:
                assert conn.exec_driver_sql("SELECT relkind FROM pg_class WHERE oid='options_capture_batches'::regclass").scalar_one() == "r"
                assert conn.exec_driver_sql("SELECT to_regclass('options_capture_batches_all'),to_regclass('options_capture_publications'),to_regclass('options_bounded_schema_contract')").one() == (None, None, None)
                assert conn.exec_driver_sql("SELECT count(*) FROM source_catalog").scalar_one() == 1
            yield engine, None, counts
            return
        assert initial_drift is None, "initial drift was silently accepted"
        from ingestion.options import OptionsPuller
        puller = OptionsPuller.__new__(OptionsPuller)
        puller.engine, puller.source_id = engine, source_id
        puller._private_legacy_images = legacy_images
        yield engine, puller, counts
    finally:
        engine.dispose()
        # PG14 cannot query xact counters after a statement aborts the tx. The
        # retained, uniquely owned test database provides an independent server
        # total including failed/rolled-back heap operations after backends close.
        # Assign all otherwise-unassigned operations to each failed tx as a
        # conservative upper bound; never equate an unavailable counter with zero.
        known_rollback = sum(op["actual_global_data_rows_observed"] for op in operations
                             if op["phase"] == "ROLLBACK_ATTEMPT")
        expected_minimum = sum(counts) + known_rollback
        audit = psycopg2.connect(host="127.0.0.1", hostaddr="127.0.0.1", port=ctx["port"],
                                 dbname=database, user="postgres", connect_timeout=5)
        audit.autocommit = True
        totals = []
        with audit.cursor() as cursor:
            cursor.execute("SELECT current_setting('data_directory'),current_database(),inet_server_port()")
            assert cursor.fetchone() == (str(task / "data"), database, ctx["port"])
            cursor.execute("SET statement_timeout='1s'")
            for _ in range(10):
                cursor.execute("SELECT pg_stat_clear_snapshot()")
                cursor.execute("SELECT COALESCE(SUM(n_tup_ins+n_tup_upd+n_tup_del),0)::bigint FROM pg_stat_user_tables")
                totals.append(int(cursor.fetchone()[0]))
                if len(totals) >= 3 and len(set(totals[-3:])) == 1 and totals[-1] >= expected_minimum:
                    break
                time.sleep(.5)
        audit.close()
        assert totals[-1] >= expected_minimum, "private server DATA operation total did not converge"
        unassigned = totals[-1] - expected_minimum
        failed_bound = max((op["actual_global_data_rows_observed"] + unassigned for op in operations
                            if op["phase"] == "ROLLBACK_ATTEMPT" and op["aborted_statement_counter_unavailable"]), default=0)
        assert failed_bound <= 50, "global failed/rolled-back DATA operation upper bound exceeded 50"
        # Retain all databases and raw operations, including failed setup/tests.
        output = task / (database + ".operations.private.json")
        output.write_text(json.dumps({"database": database, "counts": counts, "operations": operations,
            "server_total_actual_DATA_operations": totals[-1], "known_commit_and_rollback_operations": expected_minimum,
            "unassigned_failed_DATA_operations": unassigned, "failed_transaction_DATA_upper_bound": failed_bound,
            "server_total_read_observations": totals}, indent=2) + "\n")
