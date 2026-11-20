"""The flow materializer issues no DDL on a pass when the schema is current.

Every ``ALTER TABLE ... ADD COLUMN IF NOT EXISTS`` and ``CREATE INDEX IF NOT
EXISTS`` takes a table lock before its existence check. Run on every pass they
queued behind readers and hit statement_timeout (19 times on
``ALTER TABLE insider_trades`` in the 2026-11-15..17 journal). The bootstrap
now reads the catalog first and runs only what is missing, once per process,
under a short lock_timeout.
"""

from __future__ import annotations

import re
from typing import Any

import pytest

import schema_guard
from ingestion import flow_materializer as fm

_DDL_RE = re.compile(r"^\s*(ALTER|CREATE|DROP|SET\s+LOCAL\s+lock_timeout)\b", re.I)


class _Result:
    def __init__(self, rows: list[tuple] | None = None) -> None:
        self._rows = rows or []

    def fetchall(self) -> list[tuple]:
        return list(self._rows)

    def scalar(self) -> Any:
        return 0


class _Conn:
    def __init__(self, log: list[str], catalog: set[tuple[str, str]]) -> None:
        self.log = log
        self.catalog = catalog

    def execute(self, stmt: Any, params: Any = None) -> _Result:
        sql = str(stmt)
        self.log.append(sql)
        if "information_schema.columns" in sql and "pg_indexes" in sql:
            return _Result(sorted(self.catalog))
        return _Result()


class _Begin:
    def __init__(self, conn: _Conn) -> None:
        self.conn = conn

    def __enter__(self) -> _Conn:
        return self.conn

    def __exit__(self, *exc: Any) -> bool:
        return False


class _Engine:
    """Fake engine that records every SQL statement it is asked to run."""

    def __init__(self, catalog: set[tuple[str, str]]) -> None:
        self.sql: list[str] = []
        self.catalog = catalog

    def begin(self) -> _Begin:
        return _Begin(_Conn(self.sql, self.catalog))

    connect = begin


def _full_catalog() -> set[tuple[str, str]]:
    return {req for req, _ in fm._DDL_STEPS}


def _ddl(sql: list[str]) -> list[str]:
    return [s for s in sql if _DDL_RE.match(s)]


@pytest.fixture(autouse=True)
def _fresh_guard():
    schema_guard.reset_for_tests()
    yield
    schema_guard.reset_for_tests()


def test_every_ddl_statement_is_catalog_checkable() -> None:
    assert len(fm._DDL_STEPS) == len(fm._DDL_STATEMENTS)
    reqs = {req for req, _ in fm._DDL_STEPS}
    assert ("column", "insider_trades.filing_date") in reqs
    assert ("index", "idx_junction_key") in reqs
    assert ("table", "junction_point_readings") in reqs
    with pytest.raises(ValueError):
        fm._ddl_requirement("ALTER TABLE insider_trades ALTER COLUMN value TYPE NUMERIC")


def test_sync_all_issues_no_ddl_when_schema_is_current() -> None:
    engine = _Engine(_full_catalog())
    result = fm.sync_all(engine)

    assert result["status"] == "SUCCESS", result
    assert _ddl(engine.sql) == []
    # The catalog is read once per pass, not once per sync function.
    catalog_reads = [s for s in engine.sql if "pg_indexes" in s]
    assert len(catalog_reads) == 1


def test_missing_column_runs_only_that_alter_under_lock_timeout() -> None:
    catalog = _full_catalog() - {("column", "insider_trades.filing_date")}
    engine = _Engine(catalog)
    fm._ensure_tables(engine)

    ddl = _ddl(engine.sql)
    assert len(ddl) == 2
    assert ddl[0].startswith("SET LOCAL lock_timeout")
    assert ddl[1] == "ALTER TABLE insider_trades ADD COLUMN IF NOT EXISTS filing_date DATE"


def test_missing_table_and_index_are_created() -> None:
    catalog = {
        req for req in _full_catalog()
        if req not in {("table", "etf_flows"), ("index", "idx_etf_flows_ticker")}
    }
    engine = _Engine(catalog)
    fm._ensure_tables(engine)

    ddl = _ddl(engine.sql)[1:]
    assert len(ddl) == 2
    assert ddl[0].startswith("CREATE TABLE IF NOT EXISTS etf_flows")
    assert ddl[1].startswith("CREATE INDEX IF NOT EXISTS idx_etf_flows_ticker")


def test_failed_ensure_is_retried_on_the_next_call(monkeypatch) -> None:
    calls = {"n": 0}

    def boom(_engine: Any) -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("canceling statement due to lock timeout")

    monkeypatch.setattr(fm, "_ensure_tables_now", boom)
    with pytest.raises(RuntimeError):
        fm._ensure_tables(object())
    fm._ensure_tables(object())
    fm._ensure_tables(object())
    assert calls["n"] == 2
