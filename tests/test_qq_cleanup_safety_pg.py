"""Real server cleanup failures after acknowledged COMMIT must stop without replay."""
import json
import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import OperationalError
from ingestion.altdata import quiverquant as qq
from ingestion.altdata import quiverquant_transactions as tx
from scripts import qq_rekey_signal_sources as rekey
from scripts import qq_gov_contracts_redate as redate
from scripts import qq_transition_common as common
from intelligence import trust_scorer as trust
from tests.test_qq_short_transactions import records, quarter_moves
from tests.test_qq_short_transactions_pg import pg as _pg, seed, all_rows, rekey_plan

pg = _pg

def cleanup_server_rejection(engine):
    """Inject real server SQLSTATE57014 from a checkin cleanup after real COMMIT."""
    calls = [0]
    def checkin(dbapi_connection, connection_record):
        calls[0] += 1
        if calls[0] != 1:
            return
        try:
            with dbapi_connection.cursor() as cursor:
                cursor.execute("DO $$ BEGIN RAISE EXCEPTION 'synthetic cleanup cancellation after acknowledged COMMIT' USING ERRCODE='57014'; END $$")
        except Exception as original:
            dbapi_connection.rollback()
            assert original.pgcode == '57014' and dbapi_connection.get_transaction_status() == 0
            raise OperationalError('CHECKIN CLEANUP AFTER COMMIT', {}, original)
    event.listen(engine.pool, 'checkin', checkin)
    return calls, checkin


def test_successful_commit_then_cleanup_57014_must_stop_writer_without_replay(pg, monkeypatch):
    engine, counts = pg
    monkeypatch.setattr(qq, 'STORE_BATCH_ROWS', 2)
    calls, callback = cleanup_server_rejection(engine)
    caught = None
    try:
        try:
            qq._store_signals(engine, records(3), 'quiverquant:lobbying', 'lobbying')
        except qq.QuiverStoreAborted as exc:
            caught = exc
    finally:
        event.remove(engine.pool, 'checkin', callback)
    actual = all_rows(engine)
    evidence = {'checkins':calls[0], 'actual_rows':len(actual), 'write_transactions':counts,
                'aborted':caught is not None, 'uncertain':getattr(caught,'commit_uncertain',None),
                'stored':getattr(caught,'stored',None)}
    print('REAL_COMMIT_THEN_CLEANUP57014_WRITER', json.dumps(evidence))
    assert caught is not None and not caught.commit_uncertain and caught.stored == 2
    assert calls[0] == 1 and len(actual) == 2


@pytest.mark.parametrize('script', [rekey, redate])
def test_successful_commit_then_cleanup_57014_must_stop_transition_not_skip(pg, tmp_path, script):
    engine, counts = pg
    if script is rekey:
        seed(engine, [{'ticker':'FIRST'}, {'ticker':'LAST'}])
        moves = rekey_plan(engine)
    else:
        rows = sum((quarter_moves(1,t)[0] for t in ['FIRST','LAST']), [])
        seed(engine, [{**r,'source_type':redate.SOURCE_TYPE} for r in rows])
        with engine.connect() as conn:
            moves = redate.plan_redate(redate.load_rows(conn)).moves
    moves = sorted(moves, key=lambda m: m.ticker)
    calls, callback = cleanup_server_rejection(engine)
    caught, result = None, None
    try:
        try:
            result = script.apply_moves(engine, moves, audit_path=tmp_path/'audit', **({'batch_size':1} if script is rekey else {}))
        except tx.CommitAcknowledgedCleanupError as exc:
            caught = exc
    finally:
        event.remove(engine.pool, 'checkin', callback)
    actual = all_rows(engine)
    print('REAL_COMMIT_THEN_CLEANUP57014_TRANSITION', script.__name__, json.dumps({
        'checkins':calls[0], 'reported':result, 'stopped':caught is not None,
        'commit_uncertain':getattr(caught, 'commit_uncertain', None),
        'acknowledged_moved':getattr(caught, 'committed_rows', None),
        'audit_rows':len((tmp_path/'audit').read_text().splitlines()),
        'actual_changed':sum(r['source_id']!='qq_house_trading' for r in actual) if script is rekey else sum(r['signal_date']==moves[0].new_date for r in actual)}))
    assert caught is not None, 'Cleanup SQLSTATE57014 AFTER actual COMMIT was reported as a skipped rollback, leaving committed transition row absent from audit and continuing'
    assert calls[0] == 1
    assert caught.committed_rows == 1 and not caught.commit_uncertain
    assert len((tmp_path / 'audit').read_text().splitlines()) == 1


def test_trust_cleanup_after_ack_preserves_current_page_and_stops(pg, monkeypatch):
    engine, counts = pg
    with engine.begin() as conn:
        conn.execute(text('ALTER TABLE signal_sources ADD outcome_return float, ADD trust_score float, ADD hit_count integer, ADD miss_count integer, ADD avg_lead_time_hours float'))
    for start in range(0, 101, 50):
        with engine.begin() as conn:
            for i in range(start, min(start + 50, 101)):
                conn.execute(text("INSERT INTO signal_sources(source_type,source_id,ticker,signal_type,signal_date,signal_value,outcome,outcome_return) VALUES ('quiverquant:house',:sid,'SYNTHETIC','house_trading',CURRENT_DATE,'{}','CORRECT',.01)"), {'sid': 'qq_house_trading:' + str(i)})
    monkeypatch.setattr(trust, '_ensure_tables', lambda _: None)
    original = tx.write_transaction
    calls, callback = [0], [None]

    def install_at_first_write(*args, **kwargs):
        # The initial read connection closes normally; inject into write cleanup.
        if callback[0] is None:
            observed, callback[0] = cleanup_server_rejection(engine)
            calls[:] = [observed]
        return original(*args, **kwargs)

    monkeypatch.setattr(tx, 'write_transaction', install_at_first_write)
    counts.clear()
    try:
        with pytest.raises(tx.CommitAcknowledgedCleanupError) as caught:
            trust.update_trust_scores(engine)
    finally:
        event.remove(engine.pool, 'checkin', callback[0])
    assert caught.value.trust_rows_updated == 50
    assert sum(r['trust_score'] is not None for r in all_rows(engine)) == 50
    assert [n for n in counts if n] == [50] and calls[0] == [1]


@pytest.mark.parametrize('script', [rekey, redate])
def test_cleanup_then_audit_failure_keeps_ack_and_stops(pg, tmp_path, monkeypatch, script):
    engine, counts = pg
    if script is rekey:
        seed(engine, [{'ticker': 'FIRST'}, {'ticker': 'LAST'}])
        moves = rekey_plan(engine)
    else:
        rows = sum((quarter_moves(1, t)[0] for t in ['FIRST', 'LAST']), [])
        seed(engine, [{**r, 'source_type': redate.SOURCE_TYPE} for r in rows])
        with engine.connect() as conn:
            moves = redate.plan_redate(redate.load_rows(conn)).moves
    moves = sorted(moves, key=lambda m: m.ticker)

    def fail_audit(*args):
        raise OSError('synthetic audit unavailable after acknowledged cleanup failure')

    monkeypatch.setattr(common, 'append_audit', fail_audit)
    calls, callback = cleanup_server_rejection(engine)
    try:
        with pytest.raises(OSError) as caught:
            script.apply_moves(engine, moves, audit_path=tmp_path/'audit', **({'batch_size': 1} if script is rekey else {}))
    finally:
        event.remove(engine.pool, 'checkin', callback)
    assert calls == [1] and caught.value.committed_rows == 1 and not caught.value.commit_uncertain
    assert isinstance(caught.value.__cause__, tx.CommitAcknowledgedCleanupError)
    assert (tmp_path/'audit').read_text() == ''
