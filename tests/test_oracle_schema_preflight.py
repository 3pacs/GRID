"""Catalog preflight regression tests, without importing application startup."""
from contextlib import contextmanager

import pytest

from oracle.model_factory import ModelFactory


class Engine:
    def __init__(self, columns, fail=False):
        self.columns = columns
        self.fail = fail
        self.sql = []

    @contextmanager
    def begin(self):
        yield self

    def execute(self, statement):
        sql = str(statement)
        self.sql.append(sql)
        if 'pg_attribute' in sql:
            if self.fail:
                raise RuntimeError('catalog unavailable')
            return [(c,) for c in self.columns]
        return []


def check(engine):
    factory = ModelFactory.__new__(ModelFactory)
    factory.engine = engine
    factory._ensure_columns()


def test_complete_schema_never_alters():
    e = Engine(ModelFactory._ALLOWED_COLUMNS)
    check(e)
    assert len(e.sql) == 1
    assert "to_regclass('oracle_models')" in e.sql[0]
    assert 'NOT attisdropped' in e.sql[0]


def test_missing_column_only_keeps_original_definition():
    e = Engine(ModelFactory._ALLOWED_COLUMNS - {'active'})
    check(e)
    assert e.sql[1:] == ['ALTER TABLE oracle_models ADD COLUMN IF NOT EXISTS active BOOLEAN']


def test_catalog_failure_propagates_without_ddl():
    e = Engine([], fail=True)
    with pytest.raises(RuntimeError, match='catalog unavailable'):
        check(e)
    assert len(e.sql) == 1


def test_missing_table_keeps_compatibility_ddl():
    e = Engine([])
    check(e)
    assert len(e.sql) == 10
    assert all('ADD COLUMN IF NOT EXISTS' in sql for sql in e.sql[1:])


def test_failed_preflight_not_cached(monkeypatch):
    import schema_guard
    key = 'ModelFactory.columns'
    monkeypatch.setattr(schema_guard, '_done', set())
    e = Engine([], fail=True)
    with pytest.raises(RuntimeError):
        ModelFactory(e)
    assert key not in schema_guard._done
    e.fail = False
    e.columns = ModelFactory._ALLOWED_COLUMNS
    ModelFactory(e)
    assert key in schema_guard._done


def test_hermes_failure_visible_without_exception_payload():
    import ast
    from pathlib import Path
    tree = ast.parse((Path(__file__).parents[1] / 'scripts/hermes_operator.py').read_text())
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
             and n.args and isinstance(n.args[0], ast.Constant)
             and isinstance(n.args[0].value, str)
             and n.args[0].value.startswith('migrate_default_models failed:')]
    assert len(calls) == 1
    assert calls[0].func.attr == 'warning'
    assert {kw.arg for kw in calls[0].keywords} == {'t', 'c'}
