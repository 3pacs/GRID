"""Synthetic actual-source coverage; never import the application or providers.

Run directly with unittest or pytest. Every external dependency used by the
extracted route is fake. Scoped module replacement restores prior modules,
including preexisting package objects, on success and exceptions.
"""
from __future__ import annotations

import ast
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
import json
import os
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
OLD_LLM_BLOCK = '''    # --- LLM availability ---
    try:
        from llm.router import get_llm, Tier
        client = get_llm(Tier.LOCAL)
        checks["llm_available"] = client.is_available
    except Exception as exc:
        log.debug("Health: LLM availability check failed: {e}", e=str(exc))
        checks["llm_available"] = False
'''
NEW_LLM_BLOCK = '''    # --- Optional LLM: stored state only; never select or initialize a client. ---
    llm_state = _cached_local_llm_state()
    checks["llm"] = llm_state
    checks["llm_available"] = llm_state["state"] == "available"
'''


def load_nodes(path, names, namespace):
    """Compile actual named definitions with no ambient top-level execution."""
    tree = ast.parse(path.read_text())
    nodes = [n for n in tree.body if getattr(n, 'name', None) in names]
    assert {n.name for n in nodes} == set(names)
    for node in nodes:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            node.decorator_list = []
    tree = ast.Module(body=[ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0), *nodes], type_ignores=[])
    exec(compile(ast.fix_missing_locations(tree), str(path), 'exec'), namespace)
    return namespace


def module(name, **values):
    result = ModuleType(name)
    result.__dict__.update(values)
    return result


class SchemaBase:
    def __init__(self, **values):
        self.__dict__.update(values)


class FakeConnection:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, statement):
        return SimpleNamespace(fetchone=lambda: (5,), scalar=lambda: True)


class FakeEngine:
    def __init__(self, checked_out=15, overflow=-5, fail=False):
        self.fail = fail
        self.pool = SimpleNamespace(size=lambda: 20, checkedout=lambda: checked_out,
                                    checkedin=lambda: 20-checked_out, overflow=lambda: overflow,
                                    _max_overflow=30)

    def connect(self):
        if self.fail:
            raise RuntimeError('synthetic database failure')
        return FakeConnection()


@contextmanager
def route_fixture(cache=None, *, router_present=True, config_settings=None,
                  engine=None, thread_names=('ingestion',), systemd=False, old=False):
    engine = engine or FakeEngine()
    settings = config_settings if config_settings is not None else SimpleNamespace(LLM_LOCAL_PROVIDER='llamacpp_quick')
    # Runtime route consumers of settings also use fakes.
    if type(settings) is SimpleNamespace:
        settings.AGENTS_ENABLED = False
        settings.AGENTS_SCHEDULE_ENABLED = False
        settings.audit_api_keys = lambda: {'synthetic': True}
    noop = lambda *args, **kwargs: None
    log = SimpleNamespace(debug=noop, warning=noop)
    pool = load_nodes(ROOT/'db.py', ['get_pool_stats'], {'get_pool_peak_checked_out': lambda: 0,
                       'get_checkout_attribution': lambda: {}})['get_pool_stats']
    schema = load_nodes(ROOT/'api/schemas/system.py', ['HealthResponse'], {'BaseModel': SchemaBase})['HealthResponse']
    robinhood = SimpleNamespace(**{k: False for k in ['configured', 'live', 'use_limit_orders']},
                               **{k: 0 for k in ['max_position_usd', 'max_drawdown_pct', 'max_daily_loss_pct',
                                    'max_orders_per_day', 'max_quote_age_s', 'max_spread_bps', 'limit_slippage_bps']}, mode='PAPER')
    calls = []
    def forbidden(*args, **kwargs):
        calls.append('forbidden_provider_probe')
        raise RuntimeError('forbidden synthetic provider probe')
    router = module('llm.router', _client_cache={} if cache is None else cache,
                    _PROVIDER_BACKOFF_UNTIL={'synthetic': 999},
                    _PROVIDER_BACKOFF_REASONS={'synthetic': 'keep'}, get_llm=forbidden,
                    _create_client=forbidden, Tier=SimpleNamespace(LOCAL='local'))
    modules = {
        'config': module('config', settings=settings),
        'db': module('db', get_pool_stats=pool),
        'api': module('api'), 'api.main': module('api.main', _ws_clients=[]),
        'llm': module('llm'),
        'trading': module('trading'),
        'trading.robinhood': module('trading.robinhood', get_robinhood_trader=lambda: robinhood),
        'threading': module('threading', enumerate=lambda: [SimpleNamespace(name=n) for n in thread_names]),
        'shutil': module('shutil', disk_usage=lambda _: SimpleNamespace(total=100, used=10, free=90)),
    }
    if router_present:
        modules['llm.router'] = router
    namespace = {'get_db_engine': lambda: engine, 'text': lambda s: s, 'log': log,
                 'HealthResponse': schema, '_systemd_service_active': lambda _: systemd}
    source = ROOT/'api/routers/system.py'
    if old:
        original = os.environ.get('GRID_HEALTH_ORIGINAL_SOURCE')
        if original:
            source = Path(original)
        else:
            # Portable baseline reconstructed from the authenticated old block.
            tree = ast.parse(source.read_text().replace(NEW_LLM_BLOCK, OLD_LLM_BLOCK, 1))
            node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'health')
            node.decorator_list = []
            exec(compile(ast.fix_missing_locations(ast.Module(body=[node], type_ignores=[])), '<original-health-block>', 'exec'), namespace)
    if not old or os.environ.get('GRID_HEALTH_ORIGINAL_SOURCE'):
        names = ['health'] if old else ['_cached_local_llm_state', 'health']
        load_nodes(source, names, namespace)
    with patch.dict(sys.modules, modules):
        if not router_present:
            sys.modules.pop('llm.router', None)
        yield namespace['health'], router, calls, namespace


class CachedHealthTests(unittest.TestCase):
    def test_original_actual_route_reaches_forbidden_probe(self):
        for cache in [{}, {'llamacpp_quick': SimpleNamespace(is_available=False),
                           'ollama': SimpleNamespace(is_available=True)}]:
            with self.subTest(cache=list(cache)), route_fixture(cache, old=True) as (health, _, calls, _):
                # Use the real router selection mechanism with only its factory
                # replaced by a forbidden-probe stub; no provider module loads.
                router = sys.modules['llm.router']
                def forbidden_factory(provider):
                    calls.append('forbidden_provider_probe')
                    raise RuntimeError('forbidden synthetic provider probe')
                real_router = load_nodes(ROOT/'llm/router.py',
                    ['Tier', '_gemma_or_default', '_fallback_chain', 'get_llm'],
                    {'Enum': Enum, '_client_cache': cache,
                     '_provider_in_backoff': lambda _: False,
                     '_create_client': forbidden_factory,
                     'log': SimpleNamespace(warning=lambda *a, **k: None)})
                router.get_llm = real_router['get_llm']
                router.Tier = real_router['Tier']
                response = health()
                self.assertEqual(calls, ['forbidden_provider_probe'])
                self.assertFalse(response.checks['llm_available'])

    def test_primary_states_no_provider_calls_or_mutation(self):
        cases = [({}, 'not_initialized', False),
                 ({'llamacpp_quick': SimpleNamespace(is_available=False), 'ollama': SimpleNamespace(is_available=True)}, 'unavailable', False),
                 ({'llamacpp_quick': SimpleNamespace(is_available=True)}, 'available', True),
                 ({'llamacpp_oracle': SimpleNamespace(is_available=True)}, 'not_initialized', False),
                 ({'llamacpp_quick': None}, 'unknown', False)]
        for cache, state, available in cases:
            before = cache.copy()
            with self.subTest(state=state), route_fixture(cache) as (health, router, calls, _):
                response = health()
                self.assertEqual(response.status, 'ok')
                self.assertEqual(response.degraded_reasons, [])
                self.assertIs(response.checks['llm_available'], available)
                self.assertEqual(response.checks['llm'], {'state': state, 'provider': 'llamacpp_quick',
                                 'scope': 'configured_local_primary', 'source': 'process_cache', 'fresh': False})
                self.assertEqual(cache, before)
                self.assertEqual(router._PROVIDER_BACKOFF_UNTIL, {'synthetic': 999})
                self.assertEqual(router._PROVIDER_BACKOFF_REASONS, {'synthetic': 'keep'})
                self.assertEqual(calls, [])

    def test_router_absent_never_imported(self):
        with route_fixture(router_present=False) as (health, _, calls, _):
            self.assertEqual(health().checks['llm']['state'], 'not_initialized')
            self.assertNotIn('llm.router', sys.modules)
            self.assertEqual(calls, [])

    def test_bad_cache_and_nonboolean_states(self):
        class BadDict(dict):
            def get(self, *args): raise AssertionError('cache callback')
            def __contains__(self, *args): raise AssertionError('cache callback')
        for cache in [[], BadDict(), {1: SimpleNamespace(is_available=True)},
                      {'llamacpp_quick': SimpleNamespace(is_available=1)},
                      {'llamacpp_quick': SimpleNamespace(is_available=lambda: True)}]:
            with self.subTest(cache_type=type(cache).__name__), route_fixture(cache) as (health, _, calls, _):
                self.assertEqual(health().checks['llm']['state'], 'unknown')
                self.assertEqual(calls, [])

    def test_descriptors_and_dynamic_attributes_never_execute(self):
        hits = []
        class ProbingAvailability:
            @property
            def is_available(self):
                hits.append('is_available'); raise AssertionError('probe')
        class ProbingDict:
            @property
            def __dict__(self):
                hits.append('__dict__'); raise AssertionError('probe')
            is_available = True
        class Dynamic:
            def __getattribute__(self, name):
                hits.append(name); raise AssertionError('probe')
        for client in [ProbingAvailability(), ProbingDict(), Dynamic()]:
            with route_fixture({'llamacpp_quick': client}) as (health, _, calls, _):
                self.assertEqual(health().checks['llm']['state'], 'unknown')
                self.assertEqual(calls, [])
        with route_fixture() as (_, _, _, namespace):
            class ConfigProbe:
                @property
                def LLM_LOCAL_PROVIDER(self):
                    hits.append('selector'); raise AssertionError('probe')
            for settings in [ConfigProbe(), ProbingDict(), Dynamic()]:
                sys.modules['config'].settings = settings
                self.assertEqual(namespace['_cached_local_llm_state']()['state'], 'unknown')
        self.assertEqual(hits, [])

    def test_legacy_config_primary_resolution(self):
        cases = [(SimpleNamespace(LLM_LOCAL_PROVIDER='', LLM_QUICK_PROVIDER='ollama'), 'ollama'),
                 (SimpleNamespace(GEMMA_PRIMARY=True, GEMMA_ENABLED=True), 'gemma'),
                 (SimpleNamespace(), 'llamacpp')]
        for settings, primary in cases:
            with route_fixture({primary: SimpleNamespace(is_available=True)}, config_settings=settings) as (health, _, _, _):
                self.assertTrue(health().checks['llm_available'])
                self.assertEqual(health().checks['llm']['provider'], primary)

    def test_partial_module_state_and_exception_are_unknown(self):
        with route_fixture() as (_, router, _, namespace):
            helper = namespace['_cached_local_llm_state']
            del router._client_cache
            self.assertEqual(helper()['state'], 'unknown')
            router._client_cache = {}
            sys.modules['config'].settings = None
            self.assertEqual(helper()['state'], 'unknown')
            sys.modules['config'].settings = SimpleNamespace(LLM_LOCAL_PROVIDER='llamacpp_quick')
            with patch('inspect.getattr_static', side_effect=RuntimeError('synthetic read failure')):
                self.assertEqual(helper()['state'], 'unknown')
            sys.modules['llm.router'] = SimpleNamespace(_client_cache={})
            self.assertEqual(helper()['state'], 'unknown')

    def test_essential_db_pool_and_scheduler_behavior(self):
        for engine, reason in [(FakeEngine(fail=True), 'database unreachable'),
                               (FakeEngine(checked_out=50, overflow=30), 'connection pool exhausted')]:
            with route_fixture(engine=engine) as (health, _, _, _):
                response = health()
                self.assertEqual(response.status, 'degraded')
                self.assertIn(reason, response.degraded_reasons)
        for checked_out, overflow in [(15, -5), (12, -10)]:
            with route_fixture(engine=FakeEngine(checked_out, overflow)) as (health, _, _, _):
                self.assertTrue(health().checks['pool_healthy'])
                self.assertEqual(health().checks['pool_size'], 20)
                self.assertEqual(health().checks['pool_checked_out'], checked_out)
                self.assertEqual(health().checks['pool_overflow'], overflow)
        with route_fixture(thread_names=()) as (health, _, _, _):
            response = health()
            self.assertEqual(response.status, 'degraded')
            self.assertIn("thread 'ingestion' not running", response.degraded_reasons)
        with route_fixture(thread_names=(), systemd=True) as (health, _, _, _):
            self.assertEqual(health().status, 'ok')

    def test_actual_response_consumers_preserve_absence(self):
        @dataclass
        class StepResult:
            name: str
            status: str
            latency_ms: float | None = None
            note: str = ''
            data: dict = field(default_factory=dict)
        smoke = load_nodes(ROOT/'scripts/smoke_dad_path.py', ['step_health'], {'StepResult': StepResult})['step_health']
        operator = (ROOT/'pwa/src/views/Operator.jsx').read_text()
        self.assertIn("{ key: 'llm_available', label: 'LLM', ok: health.checks?.llm_available }", operator)
        self.assertIn('background: sub.ok ? colors.green : colors.red', operator)
        schema = ast.parse((ROOT/'api/schemas/system.py').read_text())
        health_schema = next(n for n in schema.body if isinstance(n, ast.ClassDef) and n.name == 'HealthResponse')
        fields = {n.target.id: ast.unparse(n.annotation) for n in health_schema.body if isinstance(n, ast.AnnAssign)}
        self.assertEqual(fields, {'status': 'str', 'checks': 'dict[str, object]', 'degraded_reasons': 'list[str]'})
        for engine, expected in [(FakeEngine(), 'ok'), (FakeEngine(fail=True), 'degraded')]:
            with route_fixture(engine=engine) as (health, _, _, _):
                response = health()
                payload = json.loads(json.dumps(response.__dict__))
                self.assertIs(payload['checks']['llm_available'], False)
                self.assertEqual(payload['checks']['llm']['state'], 'not_initialized')
                request_calls = []
                def request(*args, **kwargs):
                    request_calls.append((args, kwargs))
                    return SimpleNamespace(status_code=200, json=lambda: payload), 0
                result = smoke(SimpleNamespace(request=request), 5000)
                self.assertEqual(result.status, expected)
                self.assertIn('llm_available', result.data['not_ok'])
                self.assertEqual(request_calls, [(('GET', '/api/v1/system/health'), {'timeout_s': 5.0})])

    def test_scoped_modules_restore_on_success_and_exception(self):
        previous = dict(sys.modules)
        with route_fixture() as (health, _, _, _):
            health()
        self.assertEqual(dict(sys.modules), previous)
        with self.assertRaisesRegex(RuntimeError, 'synthetic teardown'):
            with route_fixture():
                raise RuntimeError('synthetic teardown')
        self.assertEqual(dict(sys.modules), previous)


if __name__ == '__main__':
    unittest.main(verbosity=2)
