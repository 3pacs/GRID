"""Captured-shape behavior and refusal gates; synthetic ordinary-role data only."""
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Event
from contextlib import contextmanager

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from ingestion import options, options_publication as pub
from tests.test_options_bounded_publication_pg import bounded_pg_engine as bounded_pg_engine, packet, DAY, visible, ROOT, FaultEngine
from tests.options_production_shape_fixture import owned_fixture


@pytest.mark.parametrize("drift", [
    "guard_body", "guard_missing", "counts",
    "ALTER TABLE feature_registry DISABLE TRIGGER trg_feature_registry_transformation_version",
    "DROP TRIGGER trg_feature_registry_transformation_version ON feature_registry",
    "ALTER TABLE feature_registry DROP CONSTRAINT chk_transformation_version_positive",
    "GRANT SELECT ON source_catalog TO PUBLIC",
    "ALTER TABLE resolved_series ENABLE ROW LEVEL SECURITY",
    "ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO PUBLIC",
    "CREATE FUNCTION private_unreviewed_guard() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO source_catalog(name,base_url,cost_tier,latency_class,revision_behavior,trust_score,priority_rank) VALUES ('unreviewed','https://synthetic.invalid','FREE','EOD','NEVER','LOW',99); RETURN NEW; END $$; CREATE TRIGGER private_unreviewed_guard BEFORE INSERT ON options_capture_batches FOR EACH ROW EXECUTE FUNCTION private_unreviewed_guard()",
])
def test_initial_packet_drift_refuses_before_rename_and_data(drift):
    with contextmanager(owned_fixture)(ROOT, initial_drift=drift) as (_, puller, counts):
        assert puller is None
        assert max(counts) == 1  # synthetic source seed only


def test_new_registry_date_and_existing_metadata_are_preserved(bounded_pg_engine):
    engine, puller, counts = bounded_pg_engine
    signals = {str(i): ("vol", "synthetic options metric", i + 1.) for i in range(10)}
    first, rows = packet(122, 10)
    result = pub.publish(engine, first, rows, lambda conn: puller._push_to_resolved(conn, "SPY", DAY.isoformat(), signals))
    assert result["published"] and result["transaction_rows"] == [1, 50, 50, 22, 21]
    with engine.connect() as conn:
        metadata = conn.exec_driver_sql("SELECT name,family,description,transformation,transformation_version,lag_days,normalization,missing_data_policy,eligible_from_date,model_eligible FROM feature_registry ORDER BY name").all()
        assert len(metadata) == 10
        assert all(row[3:] == ("RAW", 1, 0, "ZSCORE", "FORWARD_FILL", DAY, True) for row in metadata)
        assert conn.exec_driver_sql("SELECT count(*) FROM resolved_series WHERE conflict_flag=false AND resolution_version=1 AND source_priority_used=1").scalar_one() == 10
    later, later_rows = packet(62, 11)
    tomorrow = DAY + timedelta(days=1)
    later["snap_date"] = tomorrow.isoformat()
    for row in later_rows:
        row["snap_date"] = tomorrow.isoformat()
    result = pub.publish(engine, later, later_rows, lambda conn: puller._push_to_resolved(conn, "SPY", tomorrow.isoformat(), signals))
    assert result["published"] and result["transaction_rows"] == [1, 50, 12, 11]
    with engine.connect() as conn:
        assert conn.exec_driver_sql("SELECT name,family,description,transformation,transformation_version,lag_days,normalization,missing_data_policy,eligible_from_date,model_eligible FROM feature_registry ORDER BY name").all() == metadata
        assert conn.exec_driver_sql("SELECT count(*) FROM resolved_series").scalar_one() == 20
    assert max(counts) == 50


@pytest.mark.parametrize("drift", [
    "ALTER TABLE feature_registry DISABLE TRIGGER trg_feature_registry_transformation_version",
    "DROP TRIGGER trg_feature_registry_transformation_version ON feature_registry",
    "ALTER TABLE source_catalog ALTER COLUMN active DROP NOT NULL",
    "ALTER TABLE feature_registry DROP CONSTRAINT chk_transformation_version_positive",
    "GRANT SELECT ON source_catalog TO PUBLIC",
    "CREATE OR REPLACE FUNCTION options_bounded_catalog_image(relation_name text) RETURNS jsonb LANGUAGE plpgsql AS $$ BEGIN RETURN '{}'::jsonb; END $$",
    "DROP FUNCTION options_bounded_catalog_image(text)",
    "GRANT EXECUTE ON FUNCTION options_bounded_catalog_image(text) TO PUBLIC",
    "ALTER TABLE resolved_series ENABLE ROW LEVEL SECURITY",
    "CREATE FUNCTION private_sidewrite() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO source_catalog(name,base_url,cost_tier,latency_class,revision_behavior,trust_score,priority_rank) VALUES ('private-audit','https://synthetic.invalid','FREE','EOD','NEVER','LOW',99); RETURN NEW; END $$; CREATE TRIGGER private_sidewrite BEFORE INSERT ON options_capture_batches_all FOR EACH ROW EXECUTE FUNCTION private_sidewrite()",
])
def test_guard_schema_acl_and_sidewrite_drift_refuse_before_data(bounded_pg_engine, drift):
    engine, _, counts = bounded_pg_engine
    with engine.begin() as conn:
        conn.execute(text(drift))
    header, _ = packet(62)
    marker = len(counts)
    result = pub.transaction(engine, lambda conn: conn.execute(pub._HEADER, header))
    assert result.commit_ack == "NOT_COMMITTED" and result.rows == 0
    assert len(counts) == marker
    with engine.connect() as conn:
        assert conn.exec_driver_sql("SELECT count(*) FROM options_capture_batches_all").scalar_one() == 0
        assert conn.exec_driver_sql("SELECT count(*) FROM source_catalog").scalar_one() == 1


def test_actual_transformation_guard_rejects_conflicting_version(bounded_pg_engine):
    engine, puller, _ = bounded_pg_engine
    registry = "INSERT INTO feature_registry(name,family,description,transformation,transformation_version,normalization,missing_data_policy,eligible_from_date) VALUES (:name,'vol','synthetic fixture','RAW',:version,'RAW','NAN',:day)"
    with engine.begin() as conn:
        conn.execute(text(registry), {"name": "existing_raw", "version": 2, "day": DAY})
    with pytest.raises(DBAPIError, match="transformation_version mismatch"):
        with engine.begin() as conn:
            conn.execute(text(registry), {"name": "conflicting_raw", "version": 1, "day": DAY})
    header, rows = packet(62)
    result = pub.publish(engine, header, rows, lambda conn: puller._push_to_resolved(conn, "SPY", DAY.isoformat(), {"pcr": ("sentiment", "synthetic fixture", 1.)}))
    assert not result["published"] and result["stop_scope"]
    assert result["data_rows_acknowledged"] == 63
    with engine.connect() as conn:
        assert conn.exec_driver_sql("SELECT count(*) FROM feature_registry").scalar_one() == 1
        assert conn.exec_driver_sql("SELECT count(*) FROM resolved_series").scalar_one() == 0
        assert conn.exec_driver_sql("SELECT count(*) FROM options_capture_batches").scalar_one() == 0


@pytest.mark.parametrize("column,value", [
    ("cost_tier", "INVALID"), ("latency_class", "INVALID"),
    ("revision_behavior", "INVALID"), ("trust_score", "INVALID"),
])
def test_actual_source_catalog_checks_remain_enforced(bounded_pg_engine, column, value):
    engine, _, _ = bounded_pg_engine
    with pytest.raises(DBAPIError, match="check constraint"):
        with engine.begin() as conn:
            conn.execute(text(f"UPDATE source_catalog SET {column}=:value WHERE id=1"), {"value": value})
    with engine.connect() as conn:
        assert conn.exec_driver_sql("SELECT count(*) FROM source_catalog").scalar_one() == 1


@pytest.mark.parametrize("column,value", [("family", "INVALID"), ("normalization", "INVALID"),
                                          ("missing_data_policy", "INVALID")])
def test_actual_feature_checks_remain_enforced(bounded_pg_engine, column, value):
    engine, puller, _ = bounded_pg_engine
    with engine.begin() as conn:
        puller._push_to_resolved(conn, "SPY", DAY.isoformat(), {"pcr": ("sentiment", "synthetic fixture", 1.)})
    with pytest.raises(DBAPIError, match="check constraint"):
        with engine.begin() as conn:
            conn.execute(text(f"UPDATE feature_registry SET {column}=:value WHERE id=1"), {"value": value})


@pytest.mark.parametrize("sql,expected", [
    ("INSERT INTO options_daily_signals(ticker,signal_date) VALUES (NULL,'2026-10-02')", "not-null constraint"),
    ("INSERT INTO options_daily_signals(ticker,signal_date) VALUES ('SPY','2026-10-02'),('SPY','2026-10-02')", "unique constraint"),
    ("INSERT INTO resolved_series(feature_id,obs_date,release_date,vintage_date,value,source_priority_used) VALUES (99999,'2026-10-02','2026-10-02','2026-10-02',1,1)", "foreign key constraint"),
    ("INSERT INTO resolved_series(feature_id,obs_date,release_date,vintage_date,value,source_priority_used) VALUES (1,'2026-10-02',NULL,'2026-10-02',1,1)", "not-null constraint"),
    ("INSERT INTO feature_registry(name,family,transformation,transformation_version,normalization,missing_data_policy,eligible_from_date) VALUES ('bad-version','vol','RAW',0,'RAW','NAN','2026-10-02')", "check constraint"),
])
def test_signal_resolved_and_positive_version_constraints(bounded_pg_engine, sql, expected):
    engine, _, counts = bounded_pg_engine
    with pytest.raises(DBAPIError, match=expected):
        with engine.begin() as conn:
            conn.exec_driver_sql(sql)
    assert max(counts) <= 50


@pytest.mark.parametrize("field,value", [("ordinal", 0), ("row_count", 0), ("completed_at", "2026-10-01 14:00Z")])
def test_original_header_checks_refuse_invalid_capture(bounded_pg_engine, field, value):
    engine, _, counts = bounded_pg_engine
    header, _ = packet(62)
    header[field] = value
    receipt = pub.transaction(engine, lambda conn: conn.execute(pub._HEADER, header))
    assert receipt.commit_ack == "NOT_COMMITTED" and receipt.rows == 0
    with engine.connect() as conn:
        assert conn.exec_driver_sql("SELECT count(*) FROM options_capture_batches_all").scalar_one() == 0
    assert max(counts) <= 50


def test_completion_serializes_late_append_and_seals_capture(bounded_pg_engine):
    engine, _, counts = bounded_pg_engine
    header, rows = packet(62)
    assert pub.transaction(engine, lambda conn: conn.execute(pub._HEADER, header)).commit_ack == "ACKNOWLEDGED"
    for chunk in (rows[:50], rows[50:]):
        def append(conn):
            for row in chunk:
                conn.execute(pub._CONTRACT, {**row, "ordinal": header["ordinal"], "started_at": header["started_at"], "completed_at": header["completed_at"]})
        assert pub.transaction(engine, append).commit_ack == "ACKNOWLEDGED"
    locked, release, late_started = Event(), Event(), Event()
    def complete(conn):
        conn.execute(text("SELECT capture_batch_id FROM options_capture_batches_all WHERE capture_batch_id=:batch_id FOR UPDATE"), header)
        locked.set()
        assert release.wait(2)
        conn.execute(text("INSERT INTO options_capture_publications(capture_batch_id) VALUES (:batch_id)"), header)
    def late():
        late_started.set()
        return pub.transaction(engine, lambda conn: conn.execute(pub._CONTRACT, {**rows[0], "strike": 777., "ordinal": header["ordinal"], "started_at": header["started_at"], "completed_at": header["completed_at"]}))
    with ThreadPoolExecutor(max_workers=2) as workers:
        completion = workers.submit(pub.transaction, engine, complete)
        assert locked.wait(2)
        insert = workers.submit(late)
        assert late_started.wait(2)
        assert visible(engine) == []
        release.set()
        assert completion.result(timeout=5).commit_ack == "ACKNOWLEDGED"
        assert insert.result(timeout=5).commit_ack == "NOT_COMMITTED"
    with engine.connect() as conn:
        assert conn.exec_driver_sql("SELECT count(*) FROM options_snapshots_all").scalar_one() == 62
        assert conn.exec_driver_sql("SELECT count(*) FROM options_capture_publications").scalar_one() == 1
    assert max(counts) == 50


def test_legacy_images_survive_cutover_and_real_uncertain_prefix():
    with contextmanager(owned_fixture)(ROOT, legacy_atomic=True) as (engine, puller, counts):
        prior = puller._private_legacy_images
        def verify():
            with engine.connect() as conn:
                for name, images in prior.items():
                    if name == "options_capture_batches":
                        assert conn.exec_driver_sql("SELECT row_to_json(t)::text FROM options_capture_batches t WHERE capture_batch_id='synthetic-legacy'").scalars().all() == images
                        assert conn.exec_driver_sql("SELECT requires_publication FROM options_capture_batches_all WHERE capture_batch_id='synthetic-legacy'").scalar_one() is False
                    else:
                        where = " WHERE capture_batch_id='synthetic-legacy'" if name == "options_snapshots_all" else ""
                        assert conn.exec_driver_sql("SELECT row_to_json(t)::text FROM " + name + " t" + where + " ORDER BY 1").scalars().all() == images
                assert conn.exec_driver_sql("SELECT count(*) FROM options_snapshots").scalar_one() == 8
                assert conn.exec_driver_sql("SELECT value FROM resolved_series WHERE release_date<='2026-10-02' AND vintage_date<='2026-10-02'").scalar_one() == 17
        verify()
        header, rows = packet(122)
        def forbidden(conn):
            raise AssertionError("unknown prefix cannot publish")
        result = pub.publish(FaultEngine(engine, 2), header, rows, forbidden)
        assert result["commit_ack"] == "UNKNOWN" and result["data_rows_acknowledged"] == 1
        verify()
        assert max(counts) == 50


def test_source_freshness_retains_three_second_lock_limit(bounded_pg_engine, monkeypatch):
    engine, puller, counts = bounded_pg_engine
    from sqlalchemy import event
    limits = []
    def observe(_conn, _cur, sql, _params, _ctx, _many):
        if "lock_timeout" in sql:
            limits.append(sql)
    monkeypatch.setattr(options, "create_engine", lambda *args, **kwargs: engine)
    event.listen(engine, "before_cursor_execute", observe)
    try:
        assert puller._mark_catalog_pulled() is True
    finally:
        event.remove(engine, "before_cursor_execute", observe)
    assert "SET LOCAL lock_timeout = '3s'" in limits
    assert puller._catalog_receipt.rows == 1 and puller._catalog_receipt.commit_ack == "ACKNOWLEDGED"
    assert max(counts) <= 50


def test_deadline_in_closure_setup_refuses_before_work(bounded_pg_engine, monkeypatch):
    engine, _, counts = bounded_pg_engine
    clock = [100.]
    original = pub.assert_function_sources
    monkeypatch.setattr(pub.time, "monotonic", lambda: clock[0])

    def expire_during_closure(conn):
        original(conn)
        clock[0] += 16

    monkeypatch.setattr(pub, "assert_function_sources", expire_during_closure)
    marker = len(counts)

    def forbidden(_conn):
        raise AssertionError("deadline expired before work")

    receipt = pub.transaction(engine, forbidden)
    assert receipt.commit_ack == "NOT_COMMITTED" and receipt.error == "PublicationBudgetExpired"
    assert len(counts) == marker
