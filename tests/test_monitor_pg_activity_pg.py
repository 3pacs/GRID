"""Real-PostgreSQL proof for scripts/monitor_pg_activity_during_migration.py -- the
continuous lock/activity sampler that answers the remediation plan's precondition 2
(00-Agent-Reports/2026-09-22/claude__ANIK__grid-596-remediation-plan.md):

    "Continuous lock/activity observability wired up for the retry window
    specifically -- closing the named gap [pg_locks/pg_stat_activity checked
    only before, never during, the failed rerun's execution window]."

Proves the script's own query actually observes a REAL held lock from a second,
independent connection -- not a mock of what pg_locks/pg_stat_activity would say.

Uses the shared ``pg_engine`` fixture (tests/conftest.py) -- skips cleanly if no
PostgreSQL is reachable.
"""

from __future__ import annotations

import importlib
import threading
import time

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
