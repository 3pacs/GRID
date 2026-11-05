"""Offline functional regressions: fake commands, SQL, HTTP and email only."""
from __future__ import annotations

import ast
import copy
import sys
import types
from pathlib import Path

import pytest

import intelligence.fleet_heartbeat as mod


class Result:
    def __init__(self, row=None):
        self.row = row

    def fetchone(self):
        return self.row


class Engine:
    """Recognizes actual module SQL without any database or SQL execution."""
    def __init__(self, previous=None, *, age=None, count=0, futile=False):
        self.previous = previous or {}
        self.age, self.count, self.futile = age, count, futile
        self.records, self.sql = [], []

    def begin(self): return self
    def connect(self): return self
    def __enter__(self): return self
    def __exit__(self, *args): pass

    def exec_driver_sql(self, sql, params=None):
        self.sql.append(sql)
        if sql.lstrip().startswith('CREATE '): return Result()
        if sql.startswith('INSERT INTO fleet_heartbeats'):
            self.records.append(params)
            return Result()
        if sql.startswith('SELECT status,'):
            return Result(self.previous.get(params[0]))
        if sql.startswith('SELECT COUNT(*)'): return Result((self.count,))
        if sql.startswith('SELECT EXTRACT'): return Result((self.age,))
        if sql.startswith('SELECT heal_detail,'):
            return Result(('module is in use', 'synthetic time') if self.futile else None)
        if sql.startswith('SELECT 1 FROM fleet_heartbeats'): return Result()
        raise AssertionError('unexpected SQL in fake engine: ' + sql)


@pytest.fixture
def fenced(monkeypatch):
    calls, emails = [], []
    monkeypatch.setattr(mod, 'FLEET', copy.deepcopy(mod.FLEET))
    monkeypatch.setattr(mod, 'ENDPOINTS', copy.deepcopy(mod.ENDPOINTS))
    def run(cmd, **kwargs):
        calls.append(('run', cmd, kwargs))
        return 0, 'synthetic result'
    def ssh(host, cmd, **kwargs):
        calls.append(('ssh', host, cmd, kwargs))
        return 0, 'synthetic result'
    monkeypatch.setattr(mod, '_run', run)
    monkeypatch.setattr(mod, '_ssh', ssh)
    monkeypatch.setattr(mod, '_email', lambda *args: emails.append(args))
    for name in ('probe_nodes', 'probe_gpus', 'probe_units', 'probe_endpoints', 'probe_freshness'):
        monkeypatch.setattr(mod, name, lambda: [])
    return calls, emails


def bad_sweep(monkeypatch, entity, kind, status='down', strikes=1, **engine_options):
    monkeypatch.setattr(mod, 'probe_gpus', lambda: [(entity, kind, status, 'synthetic diagnostic', 17)])
    return Engine({entity: ('down', strikes, None)}, **engine_options)


@pytest.mark.parametrize('entity,kind', [('grid-svr:gpu', 'gpu'), ('ordinary-unit', 'unit'), ('grid-api', 'endpoint')])
@pytest.mark.parametrize('status', ['down', 'degraded'])
def test_default_sweep_never_heals(monkeypatch, fenced, entity, kind, status):
    engine = bad_sweep(monkeypatch, entity, kind, status)
    result = mod.run_fleet_heartbeat(engine)
    assert result['heals'] == 0
    assert fenced[0] == []
    assert result['alerts'] == 1 and len(fenced[1]) == 1
    assert engine.records == [(entity, kind, status, 'synthetic diagnostic', 17, 2, False, None)]


@pytest.mark.parametrize('entity,kind', [('grid-svr:gpu', 'gpu'), ('grid-llamacpp-oracle', 'unit'), ('ollama', 'unit'), ('oracle-27b', 'endpoint')])
@pytest.mark.parametrize('status', ['down', 'degraded'])
@pytest.mark.parametrize('strikes', [1, 287])
@pytest.mark.parametrize('do_heal', [True, 1, 'force'])
def test_explicit_heal_preserves_primary_alert_and_record(monkeypatch, fenced, entity, kind, status, strikes, do_heal):
    engine = bad_sweep(monkeypatch, entity, kind, status, strikes)
    mod.FLEET['grid-svr']['heal'] = True
    result = mod.run_fleet_heartbeat(engine, do_heal=do_heal)
    assert fenced[0] == [] and result['heals'] == 0
    assert result['alerts'] == 1 and len(fenced[1]) == 1
    assert engine.records[0][6] is False
    assert 'requires operator handling' in engine.records[0][7]
    assert 'synthetic diagnostic' in fenced[1][0][1]


@pytest.mark.parametrize('entity', ['grid-svr:gpu', 'GRID-SVR:gpu', 'grid-svr.tailnet.example:gpu', 'grid-svr.:gpu', 'localhost:gpu', 'LOCALHOST.localdomain:gpu', '127.0.0.1:gpu', 'grid-svr', 'grid-svr:GPU', ':gpu', 'grid-svr:gpu:extra', 'unknown:gpu', '', None, 1])
def test_direct_primary_gpu_never_constructs_commands(fenced, entity):
    attempted, reason = mod._heal(entity, 'gpu', 'synthetic')
    assert attempted is False and fenced[0] == []
    assert 'refused' in reason


@pytest.mark.parametrize('cfg', [{'host': None, 'heal': True}, {'host': 'grid-svr', 'heal': True}, {'host': 'GRID-SVR.tailnet.example.', 'heal': True}, {'host': 'localhost', 'heal': True}, {'host': '127.0.0.2', 'heal': True}, {'host': '::1', 'heal': True}, {'host': 'user@grid-svr', 'heal': True}, {'host': '', 'heal': True}, {'heal': True}, None, 'malformed', {'host': [], 'heal': True}])
def test_registry_alias_cannot_target_local_gpu(fenced, cfg):
    mod.FLEET['remote-alias'] = cfg
    attempted, reason = mod._heal('remote-alias:gpu', 'gpu', 'synthetic')
    assert attempted is False and fenced[0] == [] and 'refused' in reason


@pytest.mark.parametrize('unit', ['ollama', 'ollama.service', 'OLLAMA', 'ollama@primary', 'grid-llamacpp-oracle', 'grid-llamacpp-oracle.service', 'grid-llamacpp', 'grid-llamacpp-chat', 'grid-worker', 'grid-worker@gpu', 'nvidia-persistenced', 'oracle-27b', 'oracle-27b.service', 'grid-svr:ollama', '--all', 'ollama;restart', '', None, []])
def test_direct_protected_unit_aliases_never_restart(fenced, unit):
    attempted, reason = mod._heal(unit, 'unit', 'synthetic')
    assert attempted is False and fenced[0] == [] and 'refused' in reason


@pytest.mark.parametrize('binding', [('http://localhost:8081/health', 'llamacpp', 'ordinary-unit'), ('http://127.0.0.1:8086/health', 'llamacpp', 'ordinary-unit'), ('http://grid-svr.tailnet.example:11434/api/tags', 'ollama', 'ordinary-unit'), ('http://remote:80/health', 'http_ok', 'ollama.service'), ('http://remote:80/health', 'http_ok', 'grid-llamacpp-oracle'), ('http://[::1]:8081/health', 'llamacpp', 'ordinary-unit'), ('http://localhost:bad/health', 'llamacpp', 'ordinary-unit'), ('http://localhost:80', 'http_ok', '--all'), ('http://localhost:80', 'http_ok', None), (None, None, None), ('short',), [], 'bad'])
def test_endpoint_alias_binding_is_guarded(fenced, binding):
    mod.ENDPOINTS['endpoint-alias'] = binding
    attempted, _ = mod._heal('endpoint-alias', 'endpoint', 'synthetic')
    assert attempted is False and fenced[0] == []


def test_fixed_primary_identity_survives_registry_remapping(fenced):
    mod.FLEET['grid-svr'] = {'host': 'redbox', 'heal': True}
    mod.ENDPOINTS['oracle-27b'] = ('http://remote:80', 'http_ok', 'ordinary-unit')
    for entity, kind in [('grid-svr:gpu', 'gpu'), ('oracle-27b', 'endpoint'), ('grid-llamacpp-oracle', 'unit')]:
        assert mod._heal(entity, kind, '')[0] is False
    assert fenced[0] == []


def test_unit_kind_cannot_bypass_registered_endpoint_alias(fenced):
    mod.ENDPOINTS['alias-service'] = ('http://remote:80', 'llamacpp', 'grid-llamacpp-oracle')
    assert mod._heal('alias-service', 'unit', '')[0] is False
    assert fenced[0] == []


def test_refusal_never_retries_or_falls_back(monkeypatch, fenced):
    def unavailable(*args, **kwargs):
        raise AssertionError('refused target must never reach command helper')
    monkeypatch.setattr(mod, '_run', unavailable)
    monkeypatch.setattr(mod, '_ssh', unavailable)
    for _ in range(3):
        for entity, kind in [('grid-svr:gpu', 'gpu'), ('ollama', 'unit'), ('oracle-27b', 'endpoint')]:
            assert mod._heal(entity, kind, 'driver timeout/version mismatch/is in use')[0] is False


def test_gpu_probe_preserves_passive_error_diagnostics(monkeypatch):
    commands=[]
    monkeypatch.setattr(mod, 'FLEET', {'grid-svr': {'host': None, 'gpu': 2, 'heal': True}})
    monkeypatch.setattr(mod, '_run', lambda cmd, **kwargs: commands.append(cmd) or (1, 'synthetic driver error'))
    assert mod.probe_gpus() == [('grid-svr:gpu', 'gpu', 'down', 'synthetic driver error', None)]
    assert len(commands) == 1 and 'nvidia-smi --query-gpu=' in commands[0][-1]
    assert 'modprobe' not in commands[0][-1] and 'systemctl' not in commands[0][-1]


def test_unit_probe_preserves_failed_owner_diagnostics(monkeypatch):
    commands=[]
    monkeypatch.setattr(mod, '_run', lambda cmd, **kwargs: commands.append(cmd) or (0, 'grid-llamacpp-oracle.service loaded failed failed'))
    assert mod.probe_units() == [('grid-llamacpp-oracle', 'unit', 'down', 'systemd failed', None)]
    assert commands == [['systemctl', '--failed', '--no-legend', '--plain']]


def test_endpoint_probe_preserves_error_diagnostics(monkeypatch):
    import urllib.request
    seen=[]
    def fail(request, **kwargs):
        seen.append(request.full_url)
        raise RuntimeError('synthetic listener error')
    monkeypatch.setattr(mod, 'ENDPOINTS', {'oracle-27b': ('http://synthetic.invalid:8081/health', 'llamacpp', 'grid-llamacpp-oracle')})
    monkeypatch.setattr(urllib.request, 'urlopen', fail)
    row=mod.probe_endpoints()[0]
    assert row[:4] == ('oracle-27b', 'endpoint', 'down', 'synthetic listener error')
    assert seen == ['http://synthetic.invalid:8081/health']


@pytest.mark.parametrize('age,count,futile', [(1, 0, False), (None, 6, False), (None, 0, True)])
def test_existing_cooldown_cap_and_futile_gates(monkeypatch, fenced, age, count, futile):
    engine = bad_sweep(monkeypatch, 'ordinary-unit', 'unit', age=age, count=count, futile=futile)
    result = mod.run_fleet_heartbeat(engine, do_heal=True)
    assert result['heals'] == 0 and fenced[0] == []
    assert result['alerts'] == 1


@pytest.mark.parametrize('entity,kind', [('ordinary-unit', 'unit'), ('redbox:gpu', 'gpu'), ('grid-api', 'endpoint')])
def test_nonpertinent_explicit_heals_are_preserved(monkeypatch, fenced, entity, kind):
    engine = bad_sweep(monkeypatch, entity, kind)
    result = mod.run_fleet_heartbeat(engine, do_heal=True)
    assert result['heals'] == 1 and len(fenced[0]) == 1
    assert engine.records[0][6] is True


def test_first_strike_and_recovery_debounce(monkeypatch, fenced):
    engine = bad_sweep(monkeypatch, 'grid-svr:gpu', 'gpu', strikes=0)
    result = mod.run_fleet_heartbeat(engine, do_heal=True)
    assert result['alerts'] == 0 and result['heals'] == 0 and fenced == ([], [])
    engine = bad_sweep(monkeypatch, 'grid-svr:gpu', 'gpu', status='up', strikes=2)
    result = mod.run_fleet_heartbeat(engine)
    assert result['alert_lines'][0].startswith('[RECOVERED]')
    assert engine.records[0][5] == 0


def test_dry_run_and_probe_error_keep_monitoring(monkeypatch, fenced):
    engine = bad_sweep(monkeypatch, 'grid-svr:gpu', 'gpu')
    def broken(): raise RuntimeError('synthetic probe failure')
    monkeypatch.setattr(mod, 'probe_nodes', broken)
    result = mod.run_fleet_heartbeat(engine, do_heal=True, dry_run=True)
    assert result['checked'] == 1 and result['down'] == 1
    assert engine.records == [] and fenced == ([], [])


@pytest.mark.parametrize('args,expected', [([], False), (['--heal'], True), (['--no-heal'], False), (['--heal', '--no-heal'], False)])
def test_cli_default_requires_heal_opt_in(monkeypatch, args, expected):
    # Execute the actual CLI AST in fake globals; no target import or DSN access.
    tree = ast.parse(Path(mod.__file__).read_text())
    block = tree.body[-1]
    assert isinstance(block, ast.If)
    seen = []
    monkeypatch.setitem(sys.modules, 'sqlalchemy', types.SimpleNamespace(create_engine=lambda dsn: Engine()))
    monkeypatch.setattr(sys, 'argv', ['fleet_heartbeat', *args])
    import os
    monkeypatch.setattr(os, 'environ', {})
    namespace = dict(mod.__dict__, __name__='__main__')
    namespace['run_fleet_heartbeat'] = lambda engine, **kwargs: seen.append(kwargs) or {}
    exec(compile(ast.Module(body=block.body, type_ignores=[]), 'fleet-cli', 'exec'), namespace)
    assert seen == [{'dry_run': False, 'do_heal': expected}]


@pytest.mark.parametrize('identity', ['fresh-oracle-owner', 'fresh-oracle-owner.service', 'FRESH-ORACLE-OWNER'])
@pytest.mark.parametrize('sweep', [False, True])
def test_r1_declared_primary_owner_is_protected(monkeypatch, fenced, identity, sweep):
    mod.ENDPOINTS['fresh-primary-endpoint'] = ('http://localhost:8081/health', 'llamacpp', 'fresh-oracle-owner')
    assert mod._heal('fresh-primary-endpoint', 'endpoint', '')[0] is False
    assert fenced[0] == []
    if sweep:
        engine = bad_sweep(monkeypatch, identity, 'unit')
        result = mod.run_fleet_heartbeat(engine, do_heal=True)
        assert result['heals'] == 0 and fenced[0] == []
        assert result['alerts'] == 1 and len(fenced[1]) == 1
        assert engine.records[0][6] is False and 'requires operator handling' in engine.records[0][7]
    else:
        assert mod._heal(identity, 'unit', '')[0] is False
        assert fenced[0] == []


def test_r1_reverse_lookup_checks_every_declared_owner(fenced):
    mod.ENDPOINTS['fresh-remote'] = ('http://remote:8081/health', 'llamacpp', 'fresh-shared-owner')
    mod.ENDPOINTS['fresh-primary'] = ('http://localhost:8081/health', 'llamacpp', 'fresh-shared-owner.service')
    assert mod._heal('fresh-shared-owner', 'unit', '')[0] is False
    assert fenced[0] == []


def test_r1_failed_unit_probe_reaches_protected_owner(monkeypatch):
    monkeypatch.setattr(mod, 'ENDPOINTS', dict(mod.ENDPOINTS, **{'fresh-primary': ('http://localhost:8081/health', 'llamacpp', 'fresh-oracle-owner')}))
    captured=[]; emails=[]
    def run(command, **kwargs):
        captured.append(command)
        assert command == ['systemctl', '--failed', '--no-legend', '--plain']
        return 0, 'fresh-oracle-owner.service loaded failed failed'
    monkeypatch.setattr(mod, '_run', run)
    monkeypatch.setattr(mod, '_email', lambda *args: emails.append(args))
    for name in ('probe_nodes', 'probe_gpus', 'probe_endpoints', 'probe_freshness'):
        monkeypatch.setattr(mod, name, lambda: [])
    engine = Engine({'fresh-oracle-owner': ('down', 1, None)})
    result = mod.run_fleet_heartbeat(engine, do_heal=True)
    assert result['heals'] == 0 and result['alerts'] == 1
    assert len(captured) == 1 and len(emails) == 1
    assert engine.records[0][6] is False


@pytest.mark.parametrize('url', ['http://remote:bad/health', 'http://remote:65536/health', 'http://remote:-1/health', 'http://remote:/health', 'http://remote:0/health'])
@pytest.mark.parametrize('kind', ['endpoint', 'unit'])
@pytest.mark.parametrize('sweep', [False, True])
def test_r2_malformed_remote_port_is_refused(monkeypatch, fenced, url, kind, sweep):
    mod.ENDPOINTS['fresh-malformed'] = (url, 'llamacpp', 'fresh-unrelated-owner')
    identity = 'fresh-malformed' if kind == 'endpoint' else 'fresh-unrelated-owner'
    if sweep:
        engine = bad_sweep(monkeypatch, identity, kind)
        result = mod.run_fleet_heartbeat(engine, do_heal=True)
        assert result['heals'] == 0 and fenced[0] == []
        assert engine.records[0][6] is False and result['alerts'] == 1
    else:
        assert mod._heal(identity, kind, '')[0] is False
        assert fenced[0] == []


@pytest.mark.parametrize('host', ['0.0.0.0', '0.0.0.0.', '999.1.2.3', '01.2.3.4', '255.255.255.255', '224.0.0.1', 'remote..example', 'remote-', '-remote', 'remote.-bad.example', 'a' * 64 + '.example'])
@pytest.mark.parametrize('sweep', [False, True])
def test_r3_unspecified_or_invalid_gpu_target_refused(monkeypatch, fenced, host, sweep):
    mod.FLEET['fresh-target'] = {'host': host, 'heal': True}
    if sweep:
        engine = bad_sweep(monkeypatch, 'fresh-target:gpu', 'gpu')
        result = mod.run_fleet_heartbeat(engine, do_heal=True)
        assert result['heals'] == 0 and fenced[0] == []
        assert engine.records[0][6] is False and result['alerts'] == 1
    else:
        assert mod._heal('fresh-target:gpu', 'gpu', '')[0] is False
        assert fenced[0] == []


@pytest.mark.parametrize('url', ['http://remote:8081/health', 'http://remote:8000/health', 'http://remote/health', 'https://remote/health', 'http://localhost:8000/health'])
@pytest.mark.parametrize('kind', ['endpoint', 'unit'])
@pytest.mark.parametrize('sweep', [False, True])
def test_v2_valid_endpoint_and_owner_heals_preserved(monkeypatch, fenced, url, kind, sweep):
    mod.ENDPOINTS['fresh-api'] = (url, 'http_ok', 'fresh-api-owner')
    identity = 'fresh-api' if kind == 'endpoint' else 'fresh-api-owner'
    if sweep:
        engine = bad_sweep(monkeypatch, identity, kind)
        result = mod.run_fleet_heartbeat(engine, do_heal=True)
        assert result['heals'] == 1 and result['alerts'] == 1
        assert engine.records[0][6] is True
    else:
        assert mod._heal(identity, kind, '')[0] is True
    assert fenced[0][0][1] == ['sudo', '-n', 'systemctl', 'restart', 'fresh-api-owner.service']


@pytest.mark.parametrize('host', ['remote-known', 'remote-known.example', 'remote-known.example.', '192.0.2.7'])
@pytest.mark.parametrize('sweep', [False, True])
def test_v2_valid_remote_gpu_heals_preserved(monkeypatch, fenced, host, sweep):
    mod.FLEET['fresh-remote'] = {'host': host, 'heal': True}
    if sweep:
        engine = bad_sweep(monkeypatch, 'fresh-remote:gpu', 'gpu')
        result = mod.run_fleet_heartbeat(engine, do_heal=True)
        assert result['heals'] == 1 and result['alerts'] == 1
    else:
        assert mod._heal('fresh-remote:gpu', 'gpu', '')[0] is True
    assert len(fenced[0]) == 1 and fenced[0][0][0:2] == ('ssh', host)
