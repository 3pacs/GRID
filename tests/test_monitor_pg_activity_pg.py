"""Real-PostgreSQL proof for scripts/monitor_pg_activity_during_migration.py -- the
continuous lock/activity sampler that answers the remediation plan's precondition 2
(00-Agent-Reports/2026-09-22/claude__ANIK__grid-596-remediation-plan.md):

    "Continuous lock/activity observability wired up for the retry window
    specifically -- closing the named gap [pg_locks/pg_stat_activity checked
    only before, never during, the failed rerun's execution window]."

Proves the script's own query actually observes a REAL held lock from a second,
independent connection -- not a mock of what pg_locks/pg_stat_activity would say.
Also proves the redaction pass on a REAL literal value captured in pg_stat_activity's
own query text (psycopg2's default parameter style substitutes bound values
client-side, so the server -- and this sampler -- would otherwise see them verbatim;
confirmed directly while building this file, not assumed), and that the duration bound
actually stops the run.

Uses the shared ``pg_engine`` fixture (tests/conftest.py) -- skips cleanly if no
PostgreSQL is reachable.
"""

from __future__ import annotations

import importlib
import threading
import time

import psycopg2
import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine

_SCRIPT_MODULE = "scripts.monitor_pg_activity_during_migration"

_MINIMAL_DDL = """
CREATE TABLE IF NOT EXISTS monitor_pg_activity_test_target (
    id INT PRIMARY KEY
)
"""


@pytest.fixture(autouse=True)
def _target_table(pg_engine: Engine):
    with pg_engine.begin() as conn:
        conn.execute(text(_MINIMAL_DDL))
    yield


def test_sample_query_observes_a_real_held_lock(pg_engine: Engine):
    script = importlib.import_module(_SCRIPT_MODULE)

    holder_ready = threading.Event()
    release_holder = threading.Event()

    def _hold_lock():
        with pg_engine.begin() as conn:
            conn.execute(text(
                "LOCK TABLE monitor_pg_activity_test_target IN ACCESS EXCLUSIVE MODE"
            ))
            holder_ready.set()
            release_holder.wait(timeout=10)
        # transaction ends (commit) here, releasing the lock

    holder_thread = threading.Thread(target=_hold_lock, daemon=True)
    holder_thread.start()
    assert holder_ready.wait(timeout=5), "lock-holder thread never acquired the lock"

    try:
        query = text(script._QUERY_FILTERED)
        with pg_engine.connect() as conn:
            rows = conn.execute(
                query, {"relation": "monitor_pg_activity_test_target"},
            ).mappings().all()

        matching = [
            r for r in rows
            if r["relation"] == "monitor_pg_activity_test_target" and r["granted"] is True
        ]
        assert matching, (
            "monitor query did not observe the real ACCESS EXCLUSIVE lock held by "
            "the second connection -- the sampler would have missed a real incident"
        )
        assert matching[0]["mode"] == "AccessExclusiveLock"
    finally:
        release_holder.set()
        holder_thread.join(timeout=5)


def test_sample_query_runs_clean_with_no_relevant_activity(pg_engine: Engine):
    """Baseline: the query itself must run without error and return an empty
    (or irrelevant-only) result when nothing is touching the filtered relation."""
    script = importlib.import_module(_SCRIPT_MODULE)
    query = text(script._QUERY_FILTERED)
    with pg_engine.connect() as conn:
        rows = conn.execute(
            query, {"relation": "monitor_pg_activity_test_target"},
        ).mappings().all()
    assert all(
        r["relation"] in (None, "monitor_pg_activity_test_target") for r in rows
    )


def test_redaction_removes_a_real_literal_value_captured_in_query_text(pg_engine: Engine):
    """Proves the redaction pass against a REAL literal captured from
    pg_stat_activity.query on a genuine second connection -- not a synthetic string
    handed straight to _redact_query(). Confirms psycopg2's client-side parameter
    substitution really does put the literal in the server-visible query text (the
    exact finding that motivated adding redaction at all), then confirms the
    script's own function removes it.
    """
    script = importlib.import_module(_SCRIPT_MODULE)
    url = pg_engine.url
    dsn = (
        f"dbname={url.database} user={url.username} password={url.password} "
        f"host={url.host or 'localhost'} port={url.port or 5432}"
    )

    holder_ready = threading.Event()
    release_holder = threading.Event()
    secret_marker = "MONITOR_REDACTION_TEST_SECRET_9f3a"

    def _hold_with_literal():
        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        cur = conn.cursor()
        try:
            cur.execute("SELECT pg_sleep(3), %s", (secret_marker,))
        except Exception:
            pass
        finally:
            conn.close()

    def _wait_until_visible():
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            with pg_engine.connect() as conn:
                found = conn.execute(text(
                    "SELECT 1 FROM pg_stat_activity WHERE query ILIKE '%pg_sleep%'"
                )).first()
            if found:
                holder_ready.set()
                return
            time.sleep(0.05)

    holder_thread = threading.Thread(target=_hold_with_literal, daemon=True)
    holder_thread.start()
    watcher_thread = threading.Thread(target=_wait_until_visible, daemon=True)
    watcher_thread.start()
    try:
        assert holder_ready.wait(timeout=5), "the pg_sleep query never became visible in pg_stat_activity"

        with pg_engine.connect() as conn:
            raw_row = conn.execute(text(
                "SELECT query FROM pg_stat_activity WHERE query ILIKE '%pg_sleep%'"
            )).first()
        assert raw_row is not None
        assert secret_marker in raw_row.query, (
            "fixture check: the literal must genuinely be present in the server's own "
            "query text, or this test proves nothing about the redaction pass"
        )

        redacted = script._redact_query(raw_row.query)
        assert secret_marker not in redacted, "the redaction pass must remove a real captured literal"
        assert "pg_sleep" in redacted, "redaction must not destroy the query STRUCTURE, only literal values"
    finally:
        release_holder.set()
        holder_thread.join(timeout=6)
        watcher_thread.join(timeout=6)


def test_redact_query_handles_none_and_short_text():
    script = importlib.import_module(_SCRIPT_MODULE)
    assert script._redact_query(None) is None
    assert script._redact_query("") == ""
    assert "pg_sleep" in script._redact_query("SELECT pg_sleep(1)")


def test_main_stops_at_the_duration_bound(pg_engine: Engine, monkeypatch):
    """The bound is the default, not opt-in -- this proves main() actually
    respects --max-duration-seconds and returns instead of running forever."""
    script = importlib.import_module(_SCRIPT_MODULE)
    monkeypatch.setattr(script, "get_engine", lambda: pg_engine)
    monkeypatch.setattr(
        "sys.argv",
        ["monitor_pg_activity_during_migration.py", "--interval", "0.05", "--max-duration-seconds", "0.3"],
    )

    start = time.monotonic()
    rc = script.main()
    elapsed = time.monotonic() - start

    assert rc == 0
    assert elapsed < 2.0, f"main() ran {elapsed:.2f}s, well past its 0.3s bound -- the duration cap did not stop it"
