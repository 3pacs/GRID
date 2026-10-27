"""check_db_health counts pull_log FAILED/PARTIAL rows, not only raw_series.

grid-hermes journal 2026-10-26: ``Health: … failed_24h=0`` every cycle while
BIS, RBI, repo_market, ADS and Baltic (snapshot fallback) failed every cycle.
Those failure paths write ``pull_log`` rows (or nothing), never a FAILED
``raw_series`` row, and the count read ``raw_series`` only.

The unit tests drive the real ``check_db_health`` with a deterministic fake
connection; the PostgreSQL test reuses the throwaway-schema fixture from
``tests/test_hermes_health_stale_sources_pg.py`` (skips without
``GRID_TEST_DB_URL``).
"""

from __future__ import annotations

import contextlib
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from sqlalchemy import text

from alerts import health_alerter
from scripts.hermes_health import check_db_health
from tests.test_hermes_health_stale_sources_pg import health_engine  # noqa: F401


class _Result:
    def __init__(self, rows: list[tuple[Any, ...]] | None = None) -> None:
        self._rows = rows or []

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._rows[0] if self._rows else None

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)

    def scalar(self) -> Any:
        return self._rows[0][0] if self._rows else None


class _Conn:
    """Answers check_db_health's statements by shape; records them."""

    def __init__(self, raw: tuple[int, int], pull_log: tuple[int, int] | None) -> None:
        self.raw = raw
        self.pull_log = pull_log
        self.statements: list[str] = []

    def execute(self, statement: Any, params: dict[str, Any] | None = None) -> _Result:
        sql = " ".join(str(statement).split())
        self.statements.append(sql)
        if "to_regclass(:relation)" in sql:
            assert params == {"relation": "pull_log"}
            return _Result([(self.pull_log is not None,)])
        if "FROM pull_log" in sql:
            assert self.pull_log is not None, "pull_log read without existence check"
            return _Result([self.pull_log])
        if "FROM raw_series WHERE pull_status = 'FAILED'" in sql:
            return _Result([(self.raw[0] if "'24 hours'" in sql else self.raw[1],)])
        if "pg_attribute" in sql:
            return _Result([(False,)])
        if sql.startswith("SELECT sc.name"):
            return _Result([])
        return _Result([(0,)])


class _Engine:
    def __init__(self, conn: _Conn) -> None:
        self.conn = conn

    def connect(self) -> contextlib.nullcontext[_Conn]:
        return contextlib.nullcontext(self.conn)


def test_pull_log_only_failures_are_counted() -> None:
    conn = _Conn(raw=(0, 0), pull_log=(7, 3))
    db = check_db_health(_Engine(conn))
    assert "error" not in db
    assert db["failed_pulls_24h"] == 7 and db["failed_pulls_1h"] == 3
    assert db["failed_pulls_24h_raw_series"] == 0 and db["failed_pulls_1h_raw_series"] == 0
    assert db["failed_pulls_24h_pull_log"] == 7 and db["failed_pulls_1h_pull_log"] == 3


def test_both_sources_are_summed_and_split_is_kept() -> None:
    db = check_db_health(_Engine(_Conn(raw=(4, 1), pull_log=(6, 2))))
    assert (db["failed_pulls_24h"], db["failed_pulls_1h"]) == (10, 3)
    assert (db["failed_pulls_24h_raw_series"], db["failed_pulls_24h_pull_log"]) == (4, 6)
    assert (db["failed_pulls_1h_raw_series"], db["failed_pulls_1h_pull_log"]) == (1, 2)


def test_missing_pull_log_is_unknown_not_zero_and_does_not_break_health() -> None:
    conn = _Conn(raw=(5, 2), pull_log=None)
    db = check_db_health(_Engine(conn))
    assert "error" not in db and db["healthy"] is True
    assert (db["failed_pulls_24h"], db["failed_pulls_1h"]) == (5, 2)
    assert db["failed_pulls_24h_pull_log"] is None and db["failed_pulls_1h_pull_log"] is None
    assert not any("FROM pull_log" in s for s in conn.statements)
    # The stale-source read after it still ran.
    assert any(s.startswith("SELECT sc.name") for s in conn.statements)


def test_pull_log_count_filters_status_and_window_in_sql() -> None:
    conn = _Conn(raw=(0, 0), pull_log=(0, 0))
    check_db_health(_Engine(conn))
    statement = next(s for s in conn.statements if "FROM pull_log" in s)
    assert "status IN ('FAILED', 'PARTIAL')" in statement
    assert "COALESCE(completed_at, started_at) > NOW() - INTERVAL '24 hours'" in statement
    assert "INTERVAL '1 hour'" in statement


def test_alerter_fires_on_pull_log_failures_alone() -> None:
    db = check_db_health(_Engine(_Conn(
        raw=(0, 0), pull_log=(50, health_alerter.FAILED_PULLS_1H_THRESHOLD + 1))))
    assert health_alerter._failed_pulls({"db": db}) is True
    _subject, body = health_alerter._failed_pulls_render({"db": db})
    assert "pull_log" in body
    quiet = check_db_health(_Engine(_Conn(raw=(0, 0), pull_log=(50, 0))))
    assert health_alerter._failed_pulls({"db": quiet}) is False


@pytest.mark.integration
def test_native_pull_log_counts(health_engine) -> None:  # noqa: F811
    now = datetime.now(timezone.utc)
    with health_engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE pull_log ("
            " id SERIAL PRIMARY KEY, puller_name TEXT NOT NULL,"
            " source_id INTEGER REFERENCES source_catalog(id),"
            " started_at TIMESTAMPTZ NOT NULL, completed_at TIMESTAMPTZ,"
            " status TEXT NOT NULL CHECK (status IN ('RUNNING','SUCCESS','PARTIAL','FAILED')),"
            " rows_inserted INTEGER DEFAULT 0, error_message TEXT)"
        ))
        rows = [
            ("bis", now - timedelta(minutes=40), now - timedelta(minutes=30), "FAILED"),
            ("rbi", now - timedelta(hours=3), now - timedelta(hours=2), "PARTIAL"),
            ("ads", now - timedelta(hours=5), None, "FAILED"),
            ("ok", now - timedelta(minutes=10), now - timedelta(minutes=5), "SUCCESS"),
            ("run", now - timedelta(minutes=10), None, "RUNNING"),
            ("old", now - timedelta(hours=30), now - timedelta(hours=26), "FAILED"),
        ]
        for name, started, completed, status in rows:
            conn.execute(text(
                "INSERT INTO pull_log (puller_name, started_at, completed_at, status) "
                "VALUES (:n, :s, :c, :st)"
            ), {"n": name, "s": started, "c": completed, "st": status})

    db = check_db_health(health_engine)

    assert "error" not in db and db["healthy"] is True
    assert db["failed_pulls_24h_pull_log"] == 3
    assert db["failed_pulls_1h_pull_log"] == 1
    assert db["failed_pulls_24h_raw_series"] == 0
    assert (db["failed_pulls_24h"], db["failed_pulls_1h"]) == (3, 1)


@pytest.mark.integration
def test_native_schema_without_pull_log_reports_unknown(health_engine) -> None:  # noqa: F811
    db = check_db_health(health_engine)
    assert "error" not in db and db["healthy"] is True
    assert db["failed_pulls_24h_pull_log"] is None
    assert db["stale_sources"] == []
