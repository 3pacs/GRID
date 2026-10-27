"""Actual _inspect_source function/queries, SQLite dialect-adapted synthetic rows; PostgreSQL in the _pg sibling.

Regression for the independent-MAX pairing bug: the pre-fix failures query took
MAX(pull_timestamp) and MAX(raw_payload::text) independently, so a newer failure
could be reported with an older, lexically larger error text. The query has no
GROUP BY and must keep returning exactly one aggregate row (0, NULL, NULL) when
nothing is eligible. Run with --source <path> to exercise another product file.
"""
import argparse
import ast
import contextlib
import datetime as dt
from pathlib import Path
import sqlite3
import sys
import types
import unittest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / 'scripts/hermes_fixers.py'
NOW = dt.datetime(2026, 10, 23, 1, tzinfo=dt.timezone.utc)
WINDOW = (NOW - dt.timedelta(hours=24)).isoformat()
PROJECTED = ('_require_engine', '_truncate', '_source_catalog_column_exists', '_inspect_source')
TIMESTAMP_COLUMNS = ('last_fail', 'last_pull_at', 'pull_timestamp')
# Pre-fix literal at 60446c6 (+ accepted 632cf68c patch, which did not touch it): in-test negative control.
BASELINE_INSPECT_QUERY = (
    "SELECT COUNT(*) AS fail_count, "
    "       MAX(rs.pull_timestamp) AS last_fail, "
    "       MAX(rs.raw_payload::text) AS last_error "
    "FROM raw_series rs "
    "JOIN source_catalog sc ON sc.id = rs.source_id "
    "WHERE LOWER(sc.name) = LOWER(:source) "
    "AND rs.pull_status = 'FAILED' "
    "AND rs.pull_timestamp > NOW() - INTERVAL '24 hours'"
)


def row(source=1, hours=1, error='SYNTH_NEW', status='FAILED'):
    return (source, status, (NOW - dt.timedelta(hours=hours)).isoformat(), error)


def projection(path):
    tree = ast.parse(path.read_bytes())
    wanted = {n.name: n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in PROJECTED}
    assert set(wanted) == set(PROJECTED), sorted(set(PROJECTED) - set(wanted))
    future = ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0)
    mod = ast.fix_missing_locations(ast.Module(body=[future, *(wanted[n] for n in PROJECTED)], type_ignores=[]))
    ns = {}
    exec(compile(mod, str(path), 'exec'), ns)
    return ns['_inspect_source']


def adapt(query):
    """Sole dialect substitutions: TEXT fixture already represents payload::text;
    fixed synthetic window replaces the PostgreSQL current-time expression."""
    if not isinstance(query, str) or not query.startswith('SELECT '):
        raise AssertionError('SELECT_ONLY_INERT_FIXTURE')
    if 'NOW()' in query and query.count("NOW() - INTERVAL '24 hours'") != 1:
        raise AssertionError('UNEXPECTED_WINDOW_DIALECT')
    adapted = query.replace('::text', '').replace("NOW() - INTERVAL '24 hours'", ':window_start')
    if '::' in adapted or 'NOW()' in adapted:
        raise AssertionError('UNSUPPORTED_DIALECT')
    return adapted


class Result:
    def __init__(self, cursor):
        names = [d[0] for d in (cursor.description or [])]
        self.rows = [tuple(self._convert(names[i], v) for i, v in enumerate(r)) for r in cursor.fetchall()]

    @staticmethod
    def _convert(name, value):
        if isinstance(value, str) and any(token in name for token in TIMESTAMP_COLUMNS):
            return dt.datetime.fromisoformat(value)
        return value

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows

    def scalar(self):
        return self.rows[0][0] if self.rows else None


class Adapter:
    def __init__(self, db):
        self.db = db
        self.raw_queries = []
        self.adapted_queries = []
        self.params = []
        self.results = []

    def connect(self):
        return contextlib.nullcontext(self)

    def execute(self, query, params=None):
        self.raw_queries.append(query)
        adapted = adapt(query)
        bound = dict(params or {})
        if ':window_start' in adapted:
            bound['window_start'] = WINDOW
        self.adapted_queries.append(adapted)
        self.params.append(bound)
        result = Result(self.db.execute(adapted, bound))
        self.results.append(result)
        return result


def build_db(rows, catalog):
    db = sqlite3.connect(':memory:')
    db.execute("ATTACH DATABASE ':memory:' AS information_schema")
    db.execute('CREATE TABLE information_schema.columns(table_schema TEXT, table_name TEXT, column_name TEXT)')
    db.execute('CREATE TABLE source_catalog(id INTEGER PRIMARY KEY, name TEXT, active INTEGER, last_pull_at TEXT)')
    db.execute('CREATE TABLE raw_series(source_id INTEGER, pull_status TEXT, pull_timestamp TEXT, raw_payload TEXT)')
    db.executemany('INSERT INTO source_catalog(id, name, active) VALUES(?,?,1)', catalog)
    db.executemany('INSERT INTO raw_series VALUES(?,?,?,?)', rows)
    return db


def exercise(rows, catalog=((1, 'SYNTH_A'), (2, 'SYNTH_B')), source='SYNTH_A', product=None):
    fn = projection(product or SOURCE)
    db = build_db(rows, catalog)
    try:
        adapter = Adapter(db)
        fake = types.ModuleType('sqlalchemy')
        fake.text = lambda text: text
        previous = sys.modules.get('sqlalchemy')
        sys.modules['sqlalchemy'] = fake
        try:
            result = fn(adapter, source)
        finally:
            if previous is None:
                sys.modules.pop('sqlalchemy', None)
            else:
                sys.modules['sqlalchemy'] = previous
        return adapter, result
    finally:
        db.close()


def failures_row(adapter):
    """The third query is the failures aggregate; it must return exactly one row."""
    rows = adapter.results[2].fetchall()
    assert len(rows) == 1, rows
    return rows[0]


def iso(hours):
    return (NOW - dt.timedelta(hours=hours)).isoformat()


class InspectSourceLatestFailureTests(unittest.TestCase):
    def test_older_lexically_larger_error_cannot_masquerade_as_latest(self):
        rows = [row(hours=2, error='ZZZ_SYNTH_OLDER'), row(hours=1, error='AAA_SYNTH_NEWER')]
        # Negative control: the pre-fix literal pairs the newest timestamp with the older payload.
        db = build_db(rows, ((1, 'SYNTH_A'),))
        try:
            legacy = Adapter(db).execute(BASELINE_INSPECT_QUERY, {'source': 'SYNTH_A'}).fetchone()
        finally:
            db.close()
        self.assertEqual(legacy, (2, NOW - dt.timedelta(hours=1), 'ZZZ_SYNTH_OLDER'))
        a, result = exercise(rows)
        self.assertEqual(failures_row(a), (2, NOW - dt.timedelta(hours=1), 'AAA_SYNTH_NEWER'))
        self.assertEqual(result['recent_failures'], {'count_24h': 2, 'last_fail': iso(1), 'last_error': 'AAA_SYNTH_NEWER'})

    def test_latest_null_error_is_unknown_not_an_older_message(self):
        a, result = exercise([row(hours=2, error='ZZZ_SYNTH_OLDER'), row(hours=1, error=None)])
        self.assertEqual(failures_row(a), (2, NOW - dt.timedelta(hours=1), None))
        self.assertEqual(result['recent_failures'], {'count_24h': 2, 'last_fail': iso(1), 'last_error': None})

    def test_equal_latest_timestamp_preserves_lexical_max_tie_policy(self):
        a, _ = exercise([row(error='AAA'), row(error='ZZZ'), row(error=None), row(hours=3, error='ZZZZ_OLDER')])
        self.assertEqual(failures_row(a), (4, NOW - dt.timedelta(hours=1), 'ZZZ'))

    def test_case_insensitive_name_grain_spans_catalog_ids(self):
        catalog = ((1, 'synth_a'), (2, 'SYNTH_A'), (3, 'SYNTH_B'))
        rows = [row(1, 3, 'Z_OLDER'), row(2, 1, 'A_NEWER'), row(3, 1, 'OTHER_SOURCE')]
        a, result = exercise(rows, catalog=catalog, source='Synth_A')
        self.assertEqual(failures_row(a), (2, NOW - dt.timedelta(hours=1), 'A_NEWER'))
        self.assertEqual(result['status'], 'ok')
        self.assertEqual(result['source']['id'], 1)
        self.assertEqual(result['recent_failures']['count_24h'], 2)
        self.assertEqual(a.params[2], {'source': 'Synth_A', 'window_start': WINDOW})

    def test_failed_only_and_strict_24h_filter_preserved(self):
        a, result = exercise([
            row(hours=1),
            row(hours=0, error='SUCCESS_MUST_NOT_COUNT', status='SUCCESS'),
            row(hours=24, error='BOUNDARY_EXCLUDED'),
            row(hours=25, error='OLDER_EXCLUDED'),
            (1, 'FAILED', None, 'NULL_TIME_EXCLUDED'),
            row(hours=1, error='OTHER_SOURCE_EXCLUDED', source=2),
        ])
        self.assertEqual(failures_row(a), (1, NOW - dt.timedelta(hours=1), 'SYNTH_NEW'))
        # Sibling query unchanged: latest SUCCESS pull feeds source.latest_success.
        self.assertEqual(result['source']['latest_success'], iso(0))

    def test_single_failure_payload_and_newest_timestamp_are_unchanged(self):
        a, result = exercise([row(error='ONE_SYNTHETIC_ERROR')])
        self.assertEqual(failures_row(a), (1, NOW - dt.timedelta(hours=1), 'ONE_SYNTHETIC_ERROR'))
        self.assertEqual(result['recent_failures']['last_error'], 'ONE_SYNTHETIC_ERROR')

    def test_last_error_truncation_policy_unchanged(self):
        long_error = 'E' * 900
        _, result = exercise([row(error=long_error)])
        self.assertEqual(len(result['recent_failures']['last_error']), 700)
        self.assertTrue(result['recent_failures']['last_error'].endswith('...'))

    def test_future_timestamp_policy_is_not_silently_changed(self):
        # Existing query has no upper bound. Preserve that policy; this is not PIT acceptance.
        a, _ = exercise([row(hours=-1, error='FUTURE_SYNTH')])
        self.assertEqual(failures_row(a), (1, NOW + dt.timedelta(hours=1), 'FUTURE_SYNTH'))

    def test_empty_is_exactly_one_zero_row_not_no_row(self):
        a, result = exercise([])
        self.assertEqual(failures_row(a), (0, None, None))
        self.assertEqual(result['status'], 'ok')
        self.assertEqual(result['recent_failures'], {'count_24h': 0, 'last_fail': None, 'last_error': None})
        self.assertIsNone(result['source']['latest_success'])

    def test_missing_source_keeps_missing_status_and_zero_row(self):
        a, result = exercise([row(error='UNRELATED')], source='NO_SUCH_SOURCE')
        self.assertEqual(failures_row(a), (0, None, None))
        self.assertEqual(result['status'], 'missing')
        self.assertEqual(result['cmd'], 'INSPECT_SOURCE:NO_SUCH_SOURCE')
        self.assertEqual(result['source']['id'], None)
        self.assertEqual(result['source']['name'], 'NO_SUCH_SOURCE')
        self.assertEqual(result['recent_failures'], {'count_24h': 0, 'last_fail': None, 'last_error': None})

    def test_exactly_four_select_queries_and_sibling_queries_untouched(self):
        a, _ = exercise([row()])
        self.assertEqual(len(a.raw_queries), 4)
        self.assertIn('information_schema.columns', a.raw_queries[0])
        self.assertTrue(a.raw_queries[1].startswith('SELECT id, name, active, last_pull_at, NULL AS frequency FROM source_catalog '))
        self.assertTrue(a.raw_queries[2].startswith('SELECT COUNT(*) AS fail_count, '))
        self.assertEqual(a.raw_queries[2].count("NOW() - INTERVAL '24 hours'"), 1)
        self.assertIn("rs.pull_status = 'FAILED'", a.raw_queries[2])
        self.assertTrue(a.raw_queries[3].startswith('SELECT MAX(rs.pull_timestamp) FROM raw_series rs '))
        self.assertIn("rs.pull_status = 'SUCCESS'", a.raw_queries[3])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', type=Path, default=SOURCE)
    args, rest = parser.parse_known_args()
    SOURCE = args.source
    unittest.main(argv=[__file__] + rest, verbosity=2)
