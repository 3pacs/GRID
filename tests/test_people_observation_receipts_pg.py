"""Disposable PostgreSQL only: committed-state receipts and failure recovery."""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timezone
from unittest.mock import patch

import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError

from store import people_event_observations as O
from test_people_events_v2_migration_pg import schema_engine, v2, _ROW, _insert, _run


@pytest.fixture
def observed(v2):
    _run(v2, 'migrations.versions.people_observations_20261113')
    return v2


def capture(engine, request='r1', key='k1'):
    return O.capture_observation(engine, request_id=request, acts=[('form4', key)])


def test_committed_exact_versions_hash_and_retry(observed):
    eid = _insert(observed)
    first = capture(observed)
    payload = first.verify()
    assert payload['events'][0]['id'] == eid
    assert payload['events'][0]['source'] == 'sec_form345'
    assert payload['revisions'] == []
    assert first.observed_at >= datetime.fromisoformat(payload['events'][0]['ingested_at'])
    assert capture(observed) == first
    with observed.connect() as conn:
        assert conn.execute(text('SELECT count(*) FROM people_event_observations')).scalar_one() == 1
    with pytest.raises(ValueError, match='scope'):
        capture(observed, key='other')
    with pytest.raises(ValueError, match='hash'):
        replace(first, payload_text=first.payload_text+' ').verify()
    with pytest.raises(ValueError, match='hash'):
        replace(first, request_id='tampered').verify()


def test_uncommitted_source_not_captured_and_retry_does_not_move_clock(observed):
    with observed.connect() as writer:
        tx = writer.begin()
        writer.execute(text(_ROW), dict(key='k1', known='2026-04-04T02:00:00Z', refs='[]', sid=None))
        empty = capture(observed)
        assert empty.verify()['events'] == []
        tx.commit()
    assert capture(observed) == empty  # durable idempotency, not a fresh observation
    fresh = capture(observed, 'r2')
    assert len(fresh.verify()['events']) == 1
    assert fresh.observed_at >= empty.observed_at


def test_retraction_is_versioned_without_overwriting_prior_snapshot(observed):
    eid = _insert(observed)
    before = capture(observed)
    with observed.connect() as writer:
        tx = writer.begin()
        writer.execute(text("UPDATE people_events SET retracted_at=clock_timestamp(), retraction_reason='test' WHERE id=:i"), dict(i=eid))
        pending = capture(observed, 'pending')
        assert pending.verify()['events'][0]['retracted_at'] is None
        assert pending.verify()['revisions'] == []
        tx.commit()
    after = capture(observed, 'after')
    assert after.verify()['events'][0]['retracted_at'] is not None
    assert after.verify()['revisions'][0]['op'] == 'retract'
    assert after.payload_sha256 != before.payload_sha256
    assert capture(observed) == before


def test_correction_keeps_exact_old_and_new_versions(observed):
    old = _insert(observed)
    first = capture(observed)
    with observed.begin() as conn:
        conn.execute(text("UPDATE people_events SET superseded_at='2026-05-01T00:00:00Z' WHERE id=:i"), dict(i=old))
    new = _insert(observed, known='2026-05-01T00:00:00Z')
    corrected = capture(observed, 'corrected').verify()
    assert [e['id'] for e in corrected['events']] == [old, new]
    assert corrected['events'][0]['superseded_at'] is not None
    assert corrected['revisions'][0]['event_id'] == old
    assert corrected['revisions'][0]['op'] == 'supersede'
    assert first.verify()['events'][0]['superseded_at'] is None


def test_source_rollback_never_publishes_and_connection_restart_recovers(observed):
    with observed.connect() as writer:
        tx = writer.begin()
        writer.execute(text(_ROW), dict(key='rolled', known='2026-04-04T02:00:00Z', refs='[]', sid=None))
        tx.rollback()
    missing = capture(observed, key='rolled')
    assert missing.verify()['events'] == []
    # Model lost ACK/restarted client: close all pooled connections, recover durable identity.
    observed.dispose()
    assert capture(observed, key='rolled') == missing


def test_failure_after_source_read_before_receipt_commit_leaves_nothing(observed):
    _insert(observed)
    def fail(conn, cursor, stmt, params, ctx, executemany):
        if stmt.lstrip().startswith('INSERT INTO people_event_observations'):
            raise RuntimeError('injected crash before receipt persistence')
    event.listen(observed, 'before_cursor_execute', fail)
    try:
        with pytest.raises(RuntimeError, match='injected'):
            capture(observed)
    finally:
        event.remove(observed, 'before_cursor_execute', fail)
    with observed.connect() as conn:
        assert conn.execute(text('SELECT count(*) FROM people_event_observations')).scalar_one() == 0
    assert len(capture(observed).verify()['events']) == 1


def test_parallel_same_request_returns_single_exact_receipt(observed):
    _insert(observed)
    with ThreadPoolExecutor(max_workers=2) as pool:
        a, b = list(pool.map(lambda _: capture(observed), range(2)))
    assert a == b
    with observed.connect() as conn:
        assert conn.execute(text('SELECT count(*) FROM people_event_observations')).scalar_one() == 1


def test_append_only_and_nonempty_downgrade_refused(observed):
    capture(observed)
    for sql in ['DELETE FROM people_event_observations',
                'TRUNCATE people_event_observations',
                "UPDATE people_event_observations SET payload_text='{}'"]:
        with pytest.raises(DBAPIError):
            with observed.begin() as conn: conn.execute(text(sql))
    with pytest.raises(DBAPIError, match='refusing'):
        _run(observed, 'migrations.versions.people_observations_20261113', 'downgrade')
    assert capture(observed).verify()['events'] == []


def test_migration_reapply_and_empty_downgrade(observed):
    _run(observed, 'migrations.versions.people_observations_20261113')
    _run(observed, 'migrations.versions.people_observations_20261113', 'downgrade')
    with pytest.raises(DBAPIError): capture(observed)


def test_capture_bounds_refuse_not_truncate(observed):
    _insert(observed)
    with patch.object(O, 'MAX_ROWS', 0):
        with pytest.raises(ValueError, match='bounded'): capture(observed)
    with patch.object(O, 'MAX_BYTES', 1):
        with pytest.raises(ValueError, match='bounded'): capture(observed)
    with observed.connect() as conn:
        assert conn.execute(text('SELECT count(*) FROM people_event_observations')).scalar_one() == 0


@pytest.mark.parametrize('acts', [[], [('form4', '')], [('form4',)], [('form4', 'x')]*101])
def test_invalid_scope_no_database_read(observed, acts):
    with pytest.raises(ValueError): O.capture_observation(observed, request_id='r', acts=acts)


def test_failure_after_insert_before_commit_rolls_back(observed):
    _insert(observed)
    def fail(conn):
        raise RuntimeError('injected pre-commit crash')
    event.listen(observed, 'commit', fail)
    try:
        with pytest.raises(RuntimeError, match='pre-commit'):
            capture(observed)
    finally:
        event.remove(observed, 'commit', fail)
    assert O.read_observation(observed, request_id='r1') is None
    assert capture(observed).verify()['events']


def test_process_dies_after_commit_before_ack_retry_recovers(observed):
    import multiprocessing
    import os
    _insert(observed)
    def worker():
        observed.dispose(close=False)
        capture(observed, 'lost-ack')
        os._exit(23)  # receipt committed; deliberately no IPC/result delivered
    process = multiprocessing.get_context('fork').Process(target=worker)
    process.start()
    process.join(10)
    if process.is_alive():
        process.terminate(); process.join()
        pytest.fail('bounded crash witness did not finish')
    assert process.exitcode == 23
    persisted = O.read_observation(observed, request_id='lost-ack')
    assert persisted is not None
    assert capture(observed, 'lost-ack') == persisted
    with observed.connect() as conn:
        assert conn.execute(text('SELECT count(*) FROM people_event_observations')).scalar_one() == 1


def test_read_is_select_only_and_missing_is_none(observed):
    capture(observed)
    statements = []
    def record(conn, cursor, stmt, params, ctx, many): statements.append(stmt.strip())
    event.listen(observed, 'before_cursor_execute', record)
    try:
        assert O.read_observation(observed, request_id='r1') is not None
        assert O.read_observation(observed, request_id='missing') is None
    finally:
        event.remove(observed, 'before_cursor_execute', record)
    assert statements and all(s.startswith('SELECT') for s in statements)


def test_retraction_committing_after_source_statement_does_not_rewrite_snapshot(observed):
    eid = _insert(observed)
    def concurrent_change(conn, cursor, stmt, params, ctx, many):
        if stmt.lstrip().startswith('WITH selected'):
            with observed.begin() as other:
                other.execute(text("UPDATE people_events SET retracted_at=clock_timestamp(), retraction_reason='race' WHERE id=:i"), dict(i=eid))
    event.listen(observed, 'after_cursor_execute', concurrent_change)
    try:
        captured = capture(observed)
    finally:
        event.remove(observed, 'after_cursor_execute', concurrent_change)
    payload = captured.verify()
    assert payload['events'][0]['retracted_at'] is None
    assert payload['revisions'] == []
    assert datetime.fromisoformat(payload['source_statement_at']) <= captured.observed_at
    # A receipt represents one earlier consistent snapshot, not continuous freshness.
    assert capture(observed, 'next').verify()['events'][0]['retracted_at'] is not None


def test_durable_receipt_overrides_async_session_commit_locally(observed):
    with observed.begin() as conn:
        conn.execute(text('SET synchronous_commit = off'))
    settings = []
    def inspect_setting(conn, cursor, stmt, params, ctx, many):
        if stmt.lstrip().startswith('INSERT INTO people_event_observations'):
            settings.append(conn.exec_driver_sql('SHOW synchronous_commit').scalar_one())
    event.listen(observed, 'before_cursor_execute', inspect_setting)
    try:
        receipt = capture(observed)
    finally:
        event.remove(observed, 'before_cursor_execute', inspect_setting)
    assert settings == ['on']
    assert O.read_observation(observed, request_id='r1') == receipt
    with observed.connect() as conn:
        assert conn.exec_driver_sql('SHOW synchronous_commit').scalar_one() == 'off'
