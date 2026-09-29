"""Disposable PostgreSQL proof: SmartScheduler's pull_log / freshness writes are honest.

The SQLite suite (tests/test_smart_scheduler_honest_success.py) covers the
classification rules; this file proves the pull_log rows and the
source_catalog.last_pull_at bumps SmartScheduler writes against real
PostgreSQL, with production's ``pull_log_status_check`` constraint
(RUNNING/SUCCESS/PARTIAL/FAILED only -- there is no SKIPPED or
NO_NEW_DATA status, and no migration adds one).

CI runs this file on its own step and fails if any test skipped.
"""

from __future__ import annotations

import os
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url

import ingestion.smart_scheduler as ss
from ingestion import options
from ingestion.smart_scheduler import SMART_PULL_LOG_PREFIX, SmartScheduler


class _ClosedSessionOptions(options.OptionsPuller):
    """The real OptionsPuller.pull_all, minus the DDL in __init__."""

    def __init__(self, db_engine) -> None:
        self.engine = db_engine


class _Returns:
    RETURN: Any = None

    def __init__(self, db_engine) -> None:
        self.db_engine = db_engine

    def pull_all(self) -> Any:
        return self.RETURN


class _Partial(_Returns):
    RETURN = [
        {"ticker": "SPY", "status": "SUCCESS", "rows_inserted": 120},
        {"ticker": "QQQ", "status": "FAILED", "rows_inserted": 0, "error": "Yahoo 503"},
    ]


class _AllFailed(_Returns):
    RETURN = [{"ticker": "SPY", "status": "FAILED", "rows_inserted": 0, "error": "Yahoo 503"}]


class _NothingNew(_Returns):
    RETURN = {"status": "SUCCESS", "rows_inserted": 0}


class _Raises(_Returns):
    def pull_all(self) -> Any:
        raise RuntimeError("upstream exploded")


@pytest.fixture
def pg_engine() -> Engine:
    url = os.environ.get("GRID_TEST_DB_URL")
    if not url:
        pytest.skip("GRID_TEST_DB_URL is required for disposable PostgreSQL proof")
    parsed = make_url(url)
    if parsed.host not in {"localhost", "127.0.0.1"} or "test" not in (parsed.database or ""):
        pytest.fail("SmartScheduler pull_log proof requires a local disposable test database")

    schema = "smart_honest_" + uuid4().hex[:12]
    admin = create_engine(url)
    with admin.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA {schema}"))
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema}"})
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE source_catalog (
                    id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE,
                    last_pull_at TIMESTAMPTZ)
            """))
            # Mirrors production's pull_log, including its CHECK constraint.
            conn.execute(text("""
                CREATE TABLE pull_log (
                    id SERIAL PRIMARY KEY, puller_name TEXT NOT NULL,
                    source_id INTEGER REFERENCES source_catalog(id),
                    started_at TIMESTAMPTZ NOT NULL, completed_at TIMESTAMPTZ,
                    status TEXT NOT NULL, rows_inserted INTEGER DEFAULT 0,
                    rows_expected INTEGER, error_message TEXT,
                    features_affected INTEGER[], node_name TEXT DEFAULT 'grid-svr',
                    CONSTRAINT pull_log_status_check CHECK (status = ANY (ARRAY[
                        'RUNNING'::text, 'SUCCESS'::text, 'PARTIAL'::text, 'FAILED'::text])))
            """))
        yield engine
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        admin.dispose()


def test_pull_log_and_freshness_match_what_each_run_wrote(pg_engine, monkeypatch) -> None:
    monkeypatch.setattr(options, "is_market_open", lambda _day: False)
    monkeypatch.setattr(
        options, "YahooOptionsClient",
        lambda: pytest.fail("a closed session must not contact the provider"),
    )
    entries = [
        {"name": "options", "cls": "_ClosedSessionOptions", "catalog": "YFINANCE_OPTIONS",
         "kwargs": {"tickers": ["SPY", "QQQ"]}},
        {"name": "partial", "cls": "_Partial", "catalog": "PARTIAL_SRC"},
        {"name": "allfail", "cls": "_AllFailed", "catalog": "ALLFAIL_SRC"},
        {"name": "nothingnew", "cls": "_NothingNew", "catalog": "NOTHINGNEW_SRC"},
        {"name": "raiser", "cls": "_Raises", "catalog": "RAISER_SRC"},
    ]
    registry = [
        {"name": e["name"], "mod": __name__, "cls": e["cls"], "method": "pull_all",
         "freq_h": 6, "timeout_s": 30, "kwargs": e.get("kwargs", {})}
        for e in entries
    ]
    monkeypatch.setattr(ss, "PULLER_REGISTRY", registry)
    monkeypatch.setattr(ss, "REGISTRY_CATALOG_NAMES", {e["name"]: e["catalog"] for e in entries})
    monkeypatch.setattr(ss, "MAX_PULLERS_PER_TICK", 50)
    monkeypatch.setattr(SmartScheduler, "_warn_registry_divergence", lambda self: None)
    with pg_engine.begin() as conn:
        for i, e in enumerate(entries, start=1):
            conn.execute(
                text("INSERT INTO source_catalog (id, name) VALUES (:i, :n)"),
                {"i": i, "n": e["catalog"]},
            )

    summary = SmartScheduler(pg_engine).tick()

    statuses = {r["name"]: r["status"] for r in summary["results"]}
    assert statuses == {
        "options": "SKIPPED", "partial": "PARTIAL", "allfail": "FAILED",
        "nothingnew": "NO_NEW_DATA", "raiser": "FAILED",
    }
    with pg_engine.connect() as conn:
        fresh = dict(conn.execute(text(
            "SELECT name, last_pull_at IS NOT NULL FROM source_catalog"
        )).fetchall())
        logged = {
            r[0][len(SMART_PULL_LOG_PREFIX):]: (r[1], r[2], r[3], r[4])
            for r in conn.execute(text(
                "SELECT puller_name, status, rows_inserted, error_message, source_id "
                "FROM pull_log ORDER BY id"
            )).fetchall()
        }
    # Only the run that wrote rows makes its source look fresh.
    assert fresh == {
        "YFINANCE_OPTIONS": False, "PARTIAL_SRC": False, "ALLFAIL_SRC": False,
        "NOTHINGNEW_SRC": False, "RAISER_SRC": False,
    }
    assert "options" not in logged  # all tickers skipped: not an attempt
    assert logged["partial"][:2] == ("PARTIAL", 120)
    assert "1 of 2 items failed or partial" in logged["partial"][2]
    assert logged["partial"][3] == 2
    assert logged["allfail"][:2] == ("FAILED", 0)
    assert "Yahoo 503" in logged["allfail"][2]
    assert logged["nothingnew"][:2] == ("SUCCESS", 0)
    assert logged["nothingnew"][2].startswith("NO_NEW_DATA:")
    assert logged["raiser"][:2] == ("FAILED", 0)
    assert "upstream exploded" in logged["raiser"][2]

    # Restart on the same database: completed checks keep their cadence,
    # the skipped options job is retried, failures keep their cooldown.
    restarted = SmartScheduler(pg_engine)
    assert {p["name"] for p in restarted._get_due_pullers()} == {"options"}
    assert restarted._state["allfail"]["consecutive_fails"] == 1
    assert restarted._state["raiser"]["consecutive_fails"] == 1


class _TiingoLike:
    def __init__(self, result: Any) -> None:
        self.result = result

    def pull(self) -> Any:
        return self.result


@pytest.mark.parametrize(
    ("items", "log_status", "log_rows", "note_prefix", "fresh"),
    [
        ([{"ticker": "SPY", "status": "SKIPPED", "reason": "non-equity-session"}] * 3,
         "SUCCESS", 0, "SKIPPED: all 3 items skipped", False),
        ([{"ticker": "SPY", "status": "FAILED", "rows_inserted": 0, "errors": ["HTTP 503"]}] * 2,
         "FAILED", 0, "PullerReportedFailure", False),
        ([{"ticker": "SPY", "status": "SUCCESS", "rows_inserted": 0}] * 2,
         "SUCCESS", 0, "NO_NEW_DATA:", False),
        ([{"ticker": "SPY", "status": "SUCCESS", "rows_inserted": 6},
          {"ticker": "ZZZ", "status": "FAILED", "rows_inserted": 0}],
         "PARTIAL", 6, None, False),
    ],
)
def test_grid_scheduler_pull_group_is_honest_on_postgres(
    pg_engine, monkeypatch, items, log_status, log_rows, note_prefix, fresh,
) -> None:
    """ingestion/scheduler.py::run_pull_group (grid-scheduler) through PullContext."""
    from ingestion import scheduler

    with pg_engine.begin() as conn:
        conn.execute(text("INSERT INTO source_catalog (id, name) VALUES (524, 'TIINGO')"))
    monkeypatch.setattr(scheduler, "_get_pullers_for_group", lambda *_args: [
        ("Tiingo_Prices", _TiingoLike(items), "pull", {}),
    ])

    scheduler.run_pull_group("daily", pg_engine, config={})

    with pg_engine.connect() as conn:
        status, rows, note, source_id = conn.execute(text(
            "SELECT status, rows_inserted, error_message, source_id FROM pull_log "
            "WHERE puller_name = 'Tiingo_Prices'"
        )).one()
        bumped = conn.execute(text(
            "SELECT last_pull_at IS NOT NULL FROM source_catalog WHERE id = 524"
        )).scalar_one()
    assert (status, rows, source_id) == (log_status, log_rows, 524)
    if note_prefix is None:
        assert note is None
    else:
        assert note_prefix in note
    assert bumped is fresh
