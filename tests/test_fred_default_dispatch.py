"""Offline bulk dispatch regression, without importing fedfred/config/DB.

Compile only the two unchanged bulk-dispatch methods and literal defaults.
Provider/latest-date functions are explicit doubles; no real puller constructor
or transport is invoked. The fixture pins all unique default IDs and order.
"""
import ast
from collections import Counter
from datetime import date, timedelta
import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock


def fixture_puller():
    source = Path(os.environ.get('FRED_SOURCE_UNDER_TEST',
                  str(Path(__file__).resolve().parents[1] / 'ingestion/fred.py')))
    tree = ast.parse(source.read_text(encoding='utf8'))
    defaults = next(ast.literal_eval(n.value) for n in tree.body
        if isinstance(n, ast.AnnAssign) and isinstance(n.target, ast.Name)
        and n.target.id == 'FRED_SERIES_LIST')
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'FREDPuller')
    methods = [n for n in cls.body if isinstance(n, ast.FunctionDef)
               and n.name in ('pull_all', '_pull_all_impl')]
    assert len(methods) == 2
    body = [ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0)]
    body.append(ast.ClassDef(name='OfflineBulk', bases=[], keywords=[], body=methods, decorator_list=[]))
    module = ast.fix_missing_locations(ast.Module(body=body, type_ignores=[]))
    state = SimpleNamespace(deadline=None, within_budget=None)
    namespace = {'FRED_SERIES_LIST': defaults, 'FRED_JOB_BUDGET_S': 300.0,
        'FRED_DEADLINE_MARGIN_S': 15.0, '_deadline_state': state,
        'time': SimpleNamespace(monotonic=lambda: 100.0), 'date': date, 'timedelta': timedelta,
        'log': MagicMock()}
    namespace['_deadline_ok'] = lambda: state.within_budget is None or state.within_budget()
    namespace['_deadline_result'] = lambda sid, note: {'series_id': sid, 'rows_inserted': 0,
        'rows_failed': 0, 'status': 'SKIPPED', 'errors': [note], 'deadline': True}
    exec(compile(module, str(source), 'exec'), namespace)
    puller = namespace['OfflineBulk']()
    calls = []
    puller._get_latest_date = lambda sid: None
    def pull(sid, start, end):
        calls.append((sid, start, end))
        return {'series_id': sid, 'rows_inserted': 0, 'rows_failed': 0,
                'status': 'SUCCESS', 'errors': []}
    puller.pull_series = pull
    return puller, calls, state


def expected_ids():
    return json.loads((Path(__file__).parent/'fixtures/fred_default_series_ids.json').read_text())


def test_default_bulk_dispatches_each_existing_identity_once_in_original_order():
    puller, calls, state = fixture_puller()
    result = puller.pull_all(start_date='2026-10-21', end_date='2026-10-28')
    assert [c[0] for c in calls] == expected_ids()
    assert len(calls) == 86 and Counter(c[0] for c in calls)['RRPONTSYD'] == 1
    assert [r['series_id'] for r in result] == expected_ids()
    assert all(c[1:] == ('2026-10-21', '2026-10-28') for c in calls)
    assert state.deadline is None and state.within_budget is None


def test_explicit_caller_list_keeps_its_existing_per_entry_contract():
    puller, calls, _ = fixture_puller()
    explicit = ['DFF', 'DFF', 'T10Y2Y']
    result = puller.pull_all(series_list=explicit)
    assert [c[0] for c in calls] == explicit == [r['series_id'] for r in result]


def test_explicit_empty_list_does_not_expand_to_default_workload():
    puller, calls, _ = fixture_puller()
    assert puller.pull_all(series_list=[]) == []
    assert calls == []


def test_deadline_does_not_dispatch_or_reclassify_remaining_sources():
    puller, calls, _ = fixture_puller()
    result = puller.pull_all(should_continue=lambda: False)
    assert calls == [] and len(result) == 86
    assert all(r['status'] == 'SKIPPED' and r['deadline'] for r in result)


def test_database_abort_still_stops_next_source_dispatch():
    puller, calls, _ = fixture_puller()
    def aborted(sid, start, end):
        calls.append((sid, start, end))
        return {'series_id': sid, 'rows_inserted': 0, 'status': 'FAILED',
                'aborted': True, 'errors': ['synthetic database abort']}
    puller.pull_series = aborted
    result = puller.pull_all()
    assert len(calls) == 1 and len(result) == 86
    assert result[0]['status'] == 'FAILED'
    assert all(r['status'] == 'SKIPPED' and r['aborted'] for r in result[1:])


def test_partial_empty_window_outcome_is_not_promoted_to_success():
    puller, calls, _ = fixture_puller()
    def partial(sid, start, end):
        calls.append((sid, start, end))
        return {'series_id': sid, 'rows_inserted': 0, 'status': 'PARTIAL',
                'errors': ['No data returned']}
    puller.pull_series = partial
    result = puller.pull_all()
    assert len(calls) == 86
    assert all(r['status'] == 'PARTIAL' and r['rows_inserted'] == 0 for r in result)
