"""Synthetic batch failure, count and watermark contract; no external I/O."""
from datetime import date, datetime, timezone
from unittest.mock import MagicMock

import normalization.resolver as resolver
import pytest


class Result:
    def __init__(self, rows=(), rowcount=0, sane=True):
        self.rows, self.rowcount, self.sane = list(rows), rowcount, sane
    def fetchall(self): return self.rows
    def supports_sane_multi_rowcount(self): return self.sane


class Engine:
    def __init__(self, mode):
        self.mode, self.rows = mode, []
        self.exits = []
    def connect(self): return Connection(self)
    def begin(self): return Connection(self)


class Connection:
    def __init__(self, engine):
        self.engine, self.pending = engine, []
    def __enter__(self): return self
    def __exit__(self, typ, exc, tb):
        self.engine.exits.append((typ, exc))
        if typ:
            self.pending.clear()
            if self.engine.mode in ('rollback_unknown', 'interrupt_cleanup_failure'):
                raise RuntimeError('rollback failed')
            if self.engine.mode == 'interrupt_suppressed': return True
        elif self.pending:
            if self.engine.mode == 'commit_unknown_before': raise RuntimeError('commit ack lost')
            self.engine.rows.extend(self.pending)
            if self.engine.mode == 'commit_unknown_after': raise RuntimeError('commit ack lost')
        return False
    def execute(self, statement, params=None):
        sql = str(statement)
        if 'SELECT id, family FROM feature_registry' in sql: return Result([(1, 'rates')])
        if 'FROM raw_series rs' in sql:
            return Result([('DFF', date(2026, 1, 2), 4.5, 1,
                            datetime(2026, 1, 3, tzinfo=timezone.utc), 1, 'FRED', 10, None)])
        if 'INSERT INTO resolved_series' not in sql: raise AssertionError('unexpected SQL')
        if self.engine.mode in ('interrupt_execute', 'interrupt_cleanup_failure', 'interrupt_suppressed'):
            raise Interrupted('synthetic interruption')
        if self.engine.mode in ('insert_failure', 'rollback_unknown'): raise RuntimeError('write failed')
        if self.engine.mode == 'duplicate': return Result(rowcount=0)
        self.pending.extend(params)
        if self.engine.mode == 'interrupt_count':
            class InterruptedCount:
                @property
                def rowcount(self): raise Interrupted('count interruption')
            return InterruptedCount()
        if self.engine.mode == 'interrupt_support':
            class InterruptedSupport:
                rowcount = 1
                def supports_sane_multi_rowcount(self): raise Interrupted('capability interruption')
            return InterruptedSupport()
        if self.engine.mode == 'bad_count': return Result(rowcount=len(params) + 1)
        if self.engine.mode == 'boolean_count': return Result(rowcount=True)
        if self.engine.mode == 'count_inspection_failure':
            class BrokenCount:
                @property
                def rowcount(self): raise RuntimeError('unavailable count')
            return BrokenCount()
        return Result(rowcount=-1 if self.engine.mode == 'negative_count' else len(params),
                      sane=self.engine.mode != 'unsupported_count')


class EntityMap:
    def __init__(self, engine): pass
    def get_feature_id(self, sid): return 1
    def missing_feature_report(self):
        return {'lookups_missed': 0, 'series_ids': 0, 'unregistered_features': [], 'top_series': []}


class Interrupted(BaseException):
    pass


def run(monkeypatch, mode):
    monkeypatch.setattr(resolver, 'EntityMap', EntityMap)
    monkeypatch.setattr(resolver.Resolver, '_set_statement_timeout', lambda *a: None)
    monkeypatch.setattr(resolver.Resolver, '_scan_series_ids', lambda *a, **k: (
        ['DFF'], datetime(2026, 1, 4, tzinfo=timezone.utc), True))
    engine = Engine(mode)
    result = resolver.Resolver(engine).resolve_pending(
        since=datetime(2026, 1, 1, tzinfo=timezone.utc), workers=1)
    return engine, result


def test_confirmed_success_and_duplicate_noop(monkeypatch):
    for mode, count in [('success', 1), ('duplicate', 0)]:
        engine, result = run(monkeypatch, mode)
        assert (len(engine.rows), result['resolved'], result['resolved_count_status'], result['errors']) == (
            count, count, 'confirmed', 0)


@pytest.mark.parametrize('mode', ['negative_count', 'unsupported_count', 'bad_count',
                                  'boolean_count', 'count_inspection_failure'])
def test_unknown_driver_count_is_not_claimed(monkeypatch, mode):
    engine, result = run(monkeypatch, mode)
    assert len(engine.rows) == 1
    assert (result['resolved'], result['resolved_count_status'], result['errors']) == (None, 'unknown', 0)


def test_insert_failure_is_worker_error_with_acknowledged_rollback(monkeypatch):
    engine, result = run(monkeypatch, 'insert_failure')
    assert not engine.rows
    assert (result['resolved'], result['errors'], result['batch_failure_outcomes']['rolled_back']) == (0, 1, 1)


@pytest.mark.parametrize('mode,rows', [('rollback_unknown', 0), ('commit_unknown_before', 0),
                                      ('commit_unknown_after', 1)])
def test_unacknowledged_transaction_is_unknown_and_worker_error(monkeypatch, mode, rows):
    engine, result = run(monkeypatch, mode)
    assert len(engine.rows) == rows
    assert (result['resolved'], result['resolved_count_status'], result['errors'],
            result['batch_failure_outcomes']['unknown']) == (None, 'unknown', 1, 1)


@pytest.mark.parametrize('mode', ['interrupt_execute', 'interrupt_count', 'interrupt_support',
                                  'interrupt_cleanup_failure', 'interrupt_suppressed'])
def test_base_exception_runs_exit_and_propagates(mode):
    engine = Engine(mode)
    with pytest.raises(Interrupted) as caught:
        resolver._flush_batch(engine, [{'fid': 1}])
    assert len(engine.exits) == 1
    assert engine.exits[0][0] is Interrupted
    assert engine.exits[0][1] is caught.value
    assert not engine.rows


def test_other_writer_exception_is_unknown_and_range_propagates(monkeypatch):
    monkeypatch.setattr(resolver, 'EntityMap', EntityMap)
    monkeypatch.setattr(resolver.Resolver, '_set_statement_timeout', lambda *a: None)
    monkeypatch.setattr(resolver.Resolver, '_scan_series_ids', lambda *a, **k: (
        ['YF:SPY:close'], datetime(2026, 1, 4, tzinfo=timezone.utc), True))
    monkeypatch.setattr(resolver, 'is_policy_capture', lambda *a: True, raising=False)
    engine = Engine('success')
    original_execute = Connection.execute
    def execute(self, statement, params=None):
        if 'FROM raw_series rs' in str(statement):
            return Result([('YF:SPY:close', date(2026, 1, 2), 100, 1,
                            datetime(2026, 1, 3, tzinfo=timezone.utc), 1, 'FRED', 10, {})])
        return original_execute(self, statement, params)
    monkeypatch.setattr(Connection, 'execute', execute)
    def committed_then_failed(*args):
        engine.rows.append({'synthetic_receipt': 'possible_commit'})
        raise RuntimeError('synthetic exit failure')
    monkeypatch.setattr(resolver, '_resolve_spy_close_receipt', committed_then_failed)
    instance = resolver.Resolver(engine)
    result = instance.resolve_pending(since=datetime(2026, 1, 1, tzinfo=timezone.utc), workers=1)
    assert len(engine.rows) == 1
    assert result['resolved'] is None
    assert (result['resolved_count_status'], result['errors'],
            result['other_writer_outcomes_unknown']) == ('unknown', 1, 1)
    monkeypatch.setattr(instance, 'resolve_pending', lambda **kwargs: result)
    result_range = instance.resolve_range(datetime(2026, 1, 1), datetime(2026, 1, 2), workers=1)
    assert (result_range['resolved'], result_range['resolved_count_status'], result_range['errors'],
            result_range['other_writer_outcomes_unknown'], result_range['chunks_failed']) == (None, 'unknown', 1, 1, 1)


def test_enter_failure_has_no_exit_and_outcome_is_conservative():
    class Manager:
        exits = 0
        def __enter__(self): raise RuntimeError('enter failed')
        def __exit__(self, *args): self.exits += 1
    manager = Manager()
    engine = MagicMock()
    engine.begin.return_value = manager
    with pytest.raises(resolver._ResolvedBatchError) as caught:
        resolver._flush_batch(engine, [{'fid': 1}])
    assert caught.value.outcome == 'unknown'
    assert manager.exits == 0
