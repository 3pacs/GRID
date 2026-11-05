"""Offline source-level integration tests; no full imports, providers, DB or scheduler.

Execute with Python directly. Exact candidate AST definitions are executed with
explicit inert dependencies. These checks do not qualify native PostgreSQL,
runtime identity, owner admission, or the separately owned PR819 publisher.
"""
from __future__ import annotations

import ast
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import os
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
from typing import Any
import unittest
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]


def definitions(path, names, globals_):
    globals_.setdefault('Any', Any)
    tree = ast.parse((ROOT / path).read_text(encoding='utf-8'))
    nodes = []
    for name in names:
        if '.' in name:
            cls, method = name.split('.')
            node = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == cls)
            node = next(n for n in node.body if isinstance(n, ast.FunctionDef) and n.name == method)
        else:
            node = next(n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.ClassDef)) and n.name == name)
        nodes.append(node)
    module = ast.Module(body=[ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0)] + nodes, type_ignores=[])
    ast.fix_missing_locations(module)
    exec(compile(module, str(ROOT/path), 'exec'), globals_)
    return globals_


class SyntheticEngine:
    def __init__(self, row=(0, 0), failure=None):
        self.row = row
        self.failure = failure
        self.calls = []
        self.connections = 0
    def connect(self):
        self.connections += 1
        if self.failure == 'connect':
            raise RuntimeError('SYNTHETIC_PRIVATE_DETAIL')
        return self
    def __enter__(self):
        return self
    def __exit__(self, *args):
        return False
    def execute(self, sql, params=None):
        self.calls.append((sql, params))
        if self.failure == 'query':
            raise RuntimeError('SYNTHETIC_PRIVATE_DETAIL')
        return self
    def fetchone(self):
        return self.row


class IntegrationTests(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(os.environ, {'GRID_OPTIONS_AUTOMATIC_WRITER_POLICY': 'daily_scheduler'})
        self.env.start()
        self.addCleanup(self.env.stop)
        self.now = datetime(2026, 10, 27, 15, tzinfo=timezone.utc)
        self.log = MagicMock()
        self.g = definitions('ingestion/options.py', ['automatic_options_capture_guard', 'OptionsPullResults', '_before_regular_open', 'OptionsPuller.pull_all'], {
            'os': os, 'text': lambda x: x, 'log': self.log,
            '_utc_now': lambda: self.now, '_EQUITY_TZ': ZoneInfo('America/New_York'),
            'REGULAR_SESSION_OPEN_ET': (9, 30), 'MAX_EXPIRATIONS': 12,
            'DEFAULT_CAPTURE_SOURCE': 'options_puller', 'EQUITY_TICKERS': ['SPY', 'QQQ'],
            'catalyst_options_universe': lambda *a, **k: [], 'is_market_open': lambda day: True,
            'YahooOptionsClient': MagicMock(return_value=SimpleNamespace(is_available=True)),
            'time': SimpleNamespace(sleep=lambda seconds: None),
        })
        self.guard = self.g['automatic_options_capture_guard']
        self.constructor = MagicMock(side_effect=AssertionError('writer constructed'))
        options = ModuleType('ingestion.options')
        options.OptionsPuller = self.constructor
        options.automatic_options_capture_guard = self.guard
        ingestion = ModuleType('ingestion')
        ingestion.options = options
        modules = patch.dict(sys.modules, {'ingestion': ingestion, 'ingestion.options': options})
        modules.start()
        self.addCleanup(modules.stop)
        self.adapter = definitions('ingestion/smart_scheduler.py', ['_OptionsSchedulerAdapter'], {})['_OptionsSchedulerAdapter']
        self.lock = MagicMock()
        self.lock.__enter__.side_effect = AssertionError('repair admitted')
        self.h = definitions('scripts/hermes_fixers.py', ['_retry_source', 'retry_not_fresh_reason'], {
            '_REPAIRS_LOCK': self.lock,
            '_NOT_FRESH_OUTCOMES': frozenset({'SKIPPED', 'FAILED', 'PARTIAL', 'NO_NEW_DATA'}),
        })

    def assert_refused(self, result):
        self.assertEqual(result['status'], 'SKIPPED')
        self.assertEqual(result['outcome'], 'SKIPPED')
        self.assertEqual(result['rows_inserted'], 0)
        self.assertNotIn('last_pull_at', result)

    def test_unset_default_is_legacy_without_observation(self):
        del os.environ['GRID_OPTIONS_AUTOMATIC_WRITER_POLICY']
        e = SyntheticEngine()
        self.assertIsNone(self.guard(e))
        self.assertEqual(e.connections, 0)

    def test_blank_and_legacy_are_explicit_compatibility(self):
        for policy in ('', 'legacy', ' LEGACY '):
            with self.subTest(policy=policy):
                os.environ['GRID_OPTIONS_AUTOMATIC_WRITER_POLICY'] = policy
                e = SyntheticEngine()
                self.assertIsNone(self.guard(e))
                self.assertEqual(e.connections, 0)

    def test_invalid_policy_refuses_without_connect(self):
        os.environ['GRID_OPTIONS_AUTOMATIC_WRITER_POLICY'] = 'UNKNOWN'
        e = SyntheticEngine()
        r = self.guard(e)
        self.assert_refused(r)
        self.assertEqual(r['reason'], 'invalid_options_writer_policy')
        self.assertEqual(e.connections, 0)

    def test_empty_partial_complete_duplicate_counts_all_refuse(self):
        for row in ((0, 0), (1, 1), (122, 122), (366, 122)):
            with self.subTest(row=row):
                r = self.guard(SyntheticEngine(row))
                self.assert_refused(r)
                self.assertEqual(r['registered_non_gem_batches'], row[0])
                self.assertEqual(r['captured_tickers'], row[1])

    def test_missing_or_malformed_metadata_never_grants_fallback(self):
        for row in (None, (), (None, None), ('UNKNOWN', 1)):
            with self.subTest(row=row):
                r = self.guard(SyntheticEngine(row))
                self.assert_refused(r)
                self.assertIsNone(r['captured_tickers'])
                self.assertIn('observation_error', r)

    def test_connect_and_query_failure_hide_driver_detail(self):
        for failure in ('connect', 'query'):
            r = self.guard(SyntheticEngine(failure=failure))
            self.assert_refused(r)
            self.assertEqual(r['observation_error'], 'RuntimeError')
            self.assertNotIn('SYNTHETIC_PRIVATE_DETAIL', repr(r) + repr(self.log.mock_calls))

    def test_read_only_query_is_bounded_and_excludes_gem(self):
        e = SyntheticEngine()
        self.guard(e)
        self.assertEqual([c[0] for c in e.calls[:3]], [
            'SET TRANSACTION READ ONLY', "SET LOCAL statement_timeout = '5s'", "SET LOCAL lock_timeout = '1s'"])
        sql, params = e.calls[3]
        self.assertIn("capture_source <> 'gem'", sql)
        self.assertIn('snap_date = :day', sql)
        self.assertEqual(params, {'day': '2026-10-27'})
        self.assertEqual(len(e.calls), 4)

    def test_utc_rollover_rebinds_observation_without_admitting_writer(self):
        for now, day in ((datetime(2026, 10, 27, 23, 59, 59, tzinfo=timezone.utc), '2026-10-27'),
                         (datetime(2026, 10, 28, 0, 0, tzinfo=timezone.utc), '2026-10-28')):
            self.now = now
            e = SyntheticEngine()
            self.assert_refused(self.guard(e))
            self.assertEqual(e.calls[-1][1]['day'], day)

    def test_smart_refuses_before_constructor_even_without_engine(self):
        for e in (None, SyntheticEngine(), SyntheticEngine(failure='query')):
            self.assert_refused(self.adapter(e).pull())
        self.constructor.assert_not_called()

    def test_hermes_aliases_refuse_before_token_lock_or_catalog(self):
        for source in ('options', 'yfinance_options', 'YFINANCE_OPTIONS', 'yfinance-options', 'yfinance options'):
            r = self.h['_retry_source'](source, SyntheticEngine(), attempt=3, state=object())
            self.assert_refused(r)
            self.assertEqual(self.h['retry_not_fresh_reason'](r), 'puller reported SKIPPED')
        self.lock.__enter__.assert_not_called()
        self.constructor.assert_not_called()

    def test_legacy_hermes_reaches_existing_repair_admission(self):
        os.environ['GRID_OPTIONS_AUTOMATIC_WRITER_POLICY'] = 'legacy'
        with self.assertRaisesRegex(AssertionError, 'repair admitted'):
            self.h['_retry_source']('options', SyntheticEngine())

    def test_non_options_hermes_reaches_existing_admission(self):
        with self.assertRaisesRegex(AssertionError, 'repair admitted'):
            self.h['_retry_source']('fred', SyntheticEngine())

    def test_legacy_smart_preserves_label_and_deadline_callback(self):
        os.environ['GRID_OPTIONS_AUTOMATIC_WRITER_POLICY'] = 'legacy'
        self.constructor.side_effect = None
        self.constructor.return_value.pull_all.return_value.summary = {'status': 'SUCCESS', 'rows_inserted': 3}
        e, callback = SyntheticEngine(), lambda: True
        a = self.adapter(e)
        self.constructor.assert_not_called()
        self.assertEqual(a.pull(callback)['status'], 'SUCCESS')
        a.pull(callback)
        self.constructor.assert_called_once_with(db_engine=e)
        self.constructor.return_value.pull_all.assert_called_with(should_continue=callback, capture_source='smart_scheduler')
        self.assertEqual(e.connections, 0)

    def test_policy_rechecked_for_already_constructed_adapter(self):
        os.environ['GRID_OPTIONS_AUTOMATIC_WRITER_POLICY'] = 'legacy'
        self.constructor.side_effect = None
        self.constructor.return_value.pull_all.return_value.summary = {'status': 'SUCCESS'}
        a = self.adapter(SyntheticEngine())
        a.pull()
        os.environ['GRID_OPTIONS_AUTOMATIC_WRITER_POLICY'] = 'daily_scheduler'
        self.assert_refused(a.pull())
        self.assertEqual(self.constructor.return_value.pull_all.call_count, 1)

    def test_concurrent_secondary_calls_and_repeats_have_zero_writers(self):
        def call(i):
            e = SyntheticEngine((366, 122) if i % 3 else None)
            return self.adapter(e).pull() if i % 2 else self.h['_retry_source']('options', e)
        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(call, range(64)))
        for r in results:
            self.assert_refused(r)
        self.constructor.assert_not_called()
        self.lock.__enter__.assert_not_called()

    def puller(self, results=None, publish=True):
        obj = SimpleNamespace(engine=object(), _pull_ticker=MagicMock(), _mark_catalog_pulled=MagicMock(return_value=publish))
        obj._pull_ticker.side_effect = results or [{'status': 'SUCCESS', 'rows_inserted': 3}, {'status': 'SUCCESS', 'rows_inserted': 4}]
        return obj

    def test_primary_complete_success_keeps_catalog_publication(self):
        obj = self.puller()
        r = self.g['pull_all'](obj, capture_source='daily_scheduler')
        self.assertEqual(r.summary['status'], 'SUCCESS')
        self.assertEqual(r.summary['rows_inserted'], 7)
        obj._mark_catalog_pulled.assert_called_once()
        self.assertEqual(obj._pull_ticker.call_args.kwargs['capture_source'], 'daily_scheduler')

    def test_explicit_gem_subset_captures_without_full_freshness(self):
        obj = self.puller()
        r = self.g['pull_all'](obj, ['SPY'], include_catalyst_universe=False, capture_source='gem')
        self.assertEqual(r[0]['status'], 'SUCCESS')
        self.assertEqual(r.summary['status'], 'PARTIAL')
        obj._mark_catalog_pulled.assert_not_called()
        self.assertEqual(obj._pull_ticker.call_args.kwargs['capture_source'], 'gem')

    def test_partial_failed_unknown_ack_counts_never_advance_catalog(self):
        for item in ({'status': 'FAILED', 'rows_inserted': None}, {'status': 'SUCCESS', 'rows_inserted': None},
                     {'status': 'DEFERRED', 'rows_inserted': 0}, {'status': 'FAILED', 'rows_inserted': 0}):
            with self.subTest(item=item):
                obj = self.puller([{'status': 'SUCCESS', 'rows_inserted': 3}, item])
                r = self.g['pull_all'](obj, capture_source='daily_scheduler')
                self.assertEqual(r.summary['status'], 'PARTIAL')
                obj._mark_catalog_pulled.assert_not_called()
                self.assert_refused(self.guard(SyntheticEngine()))

    def test_catalog_failure_downgrades_success_without_fallback(self):
        obj = self.puller(publish=False)
        r = self.g['pull_all'](obj, capture_source='daily_scheduler')
        self.assertEqual(r.summary['status'], 'PARTIAL')
        self.assertEqual(r.publication_error, 'catalog publication deferred or failed')
        self.assert_refused(self.guard(SyntheticEngine()))

    def test_budget_refusal_preserves_no_publication(self):
        obj = self.puller()
        r = self.g['pull_all'](obj, capture_source='daily_scheduler', should_continue=lambda: False)
        self.assertEqual({i['status'] for i in r}, {'DEFERRED'})
        obj._pull_ticker.assert_not_called()
        obj._mark_catalog_pulled.assert_not_called()

    def test_preopen_main_regression_preserved(self):
        self.now = datetime(2026, 10, 27, 13, 29, tzinfo=timezone.utc)
        obj = self.puller()
        r = self.g['pull_all'](obj, capture_source='daily_scheduler')
        self.assertEqual(r.summary['status'], 'SKIPPED')
        self.assertEqual(r[0]['reason'], 'pre-session')
        self.g['YahooOptionsClient'].assert_not_called()
        obj._pull_ticker.assert_not_called()

    def test_utc_et_date_disagreement_main_regression_preserved(self):
        self.now = datetime(2026, 10, 28, 0, 1, tzinfo=timezone.utc)
        obj = self.puller()
        r = self.g['pull_all'](obj, capture_source='daily_scheduler')
        self.assertEqual(r.summary['status'], 'SKIPPED')
        self.assertEqual(r[0]['reason'], 'non-equity-session')
        self.g['YahooOptionsClient'].assert_not_called()


if __name__ == '__main__':
    unittest.main(verbosity=2)
