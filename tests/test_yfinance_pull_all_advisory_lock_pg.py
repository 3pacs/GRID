"""Real PostgreSQL proof of pull_all's cross-process single-flight guard.

ingestion/yfinance_pull.py's ``_PULL_ALL_LOCK`` (a threading.Lock) only
protects against a second concurrent ``pull_all()`` call INSIDE ONE
process. Two separate, independently-deployed OS processes both call
``pull_all()`` in production:

  - grid-scheduler (systemd unit ``python3 -m ingestion.scheduler``) ->
    ``run_daily_pulls()`` -> ``pull_all()``, 4x/day (ingestion/scheduler.py).
  - grid-hermes (systemd unit, scripts/hermes_operator.py) ->
    ingestion/smart_scheduler.py's SmartScheduler -> ``pull_all()``, every
    4h.

A ``threading.Lock`` cannot see across that process boundary. This module
proves the cross-process guard added on top of it — a Postgres
session-level advisory lock (``pg_try_advisory_lock`` /
``pg_advisory_unlock``, keyed by
``ingestion.yfinance_pull._PULL_ALL_ADVISORY_LOCK_KEY``) — actually blocks
a second, INDEPENDENT connection (standing in for the second process) from
acquiring the same lock while the first holds it, and that
``YFinancePuller.pull_all()`` itself skips its entire run (no ticker
attempted, ``yf.download`` never called) when that happens.

This test must run with GRID_TEST_DB_URL pointing to a throwaway database.
The dedicated CI step treats a skip as a failure (REQUIRE_YFINANCE_LOCK_PG=1).
"""

from __future__ import annotations

import os
import sys
import types

# Some sandboxed environments cannot build `multitasking` (C extension).
# yfinance imports it unconditionally at module load — install a minimal
# shim before it is imported so this file is runnable anywhere.
if "multitasking" not in sys.modules:
    _shim = types.ModuleType("multitasking")
    _shim.task = lambda f: f
    _shim.set_max_threads = lambda n: None
    _shim.wait_for_tasks = lambda *a, **k: None
    sys.modules["multitasking"] = _shim

from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, text

os.environ.setdefault("DB_PASSWORD", "test-password")

_DB_URL = os.environ.get("GRID_TEST_DB_URL")


@pytest.fixture
def two_engines():
    if not _DB_URL:
        if os.environ.get("REQUIRE_YFINANCE_LOCK_PG") == "1":
            pytest.fail(
                "GRID_TEST_DB_URL is required for the pull_all advisory-lock "
                "PostgreSQL proof"
            )
        pytest.skip("GRID_TEST_DB_URL is not configured")

    # Two separate engines (=> two separate physical backend connections)
    # standing in for grid-scheduler and grid-hermes: what matters for a
    # session-level advisory lock is which BACKEND CONNECTION holds it, not
    # which Python process — two engines in one test process is a faithful
    # stand-in for two OS processes sharing one Postgres database.
    engine_a = create_engine(_DB_URL, pool_pre_ping=True)
    engine_b = create_engine(_DB_URL, pool_pre_ping=True)
    try:
        yield engine_a, engine_b
    finally:
        engine_a.dispose()
        engine_b.dispose()


def test_second_connection_is_blocked_by_the_advisory_lock(two_engines) -> None:
    """Direct proof at the SQL level, independent of YFinancePuller: a
    second, independent connection must NOT be able to acquire the same
    advisory-lock key while the first connection holds it, and must be able
    to acquire it again once released."""
    from ingestion.yfinance_pull import _PULL_ALL_ADVISORY_LOCK_KEY

    engine_a, engine_b = two_engines
    holder = engine_a.connect()
    try:
        acquired = holder.execute(
            text("SELECT pg_try_advisory_lock(:key)"),
            {"key": _PULL_ALL_ADVISORY_LOCK_KEY},
        ).scalar()
        holder.commit()
        assert acquired is True, "test setup: the first connection must acquire the lock"

        with engine_b.connect() as contender:
            blocked = contender.execute(
                text("SELECT pg_try_advisory_lock(:key)"),
                {"key": _PULL_ALL_ADVISORY_LOCK_KEY},
            ).scalar()
            contender.commit()
        assert blocked is False, (
            "a second, independent connection acquired the SAME advisory "
            "lock while the first still held it — the cross-process guard "
            "pull_all relies on would not actually work"
        )
    finally:
        holder.execute(
            text("SELECT pg_advisory_unlock(:key)"),
            {"key": _PULL_ALL_ADVISORY_LOCK_KEY},
        )
        holder.commit()
        holder.close()

    # Once released, a fresh connection must be able to acquire it again —
    # proves the lock doesn't leak/starve forever after a clean release.
    with engine_b.connect() as conn:
        reacquired = conn.execute(
            text("SELECT pg_try_advisory_lock(:key)"),
            {"key": _PULL_ALL_ADVISORY_LOCK_KEY},
        ).scalar()
        conn.commit()
        assert reacquired is True
        conn.execute(
            text("SELECT pg_advisory_unlock(:key)"),
            {"key": _PULL_ALL_ADVISORY_LOCK_KEY},
        )
        conn.commit()


def test_pull_all_via_real_engine_skips_while_another_connection_holds_the_lock(
    two_engines,
) -> None:
    """End-to-end: the REAL YFinancePuller.pull_all() (yf.download mocked —
    no network involved) against engine_b, while engine_a's connection
    holds the advisory lock exactly as a first process's in-flight pull_all
    would. pull_all() must skip its entire run."""
    from ingestion.yfinance_pull import YFinancePuller, _PULL_ALL_ADVISORY_LOCK_KEY

    engine_a, engine_b = two_engines
    holder = engine_a.connect()
    try:
        acquired = holder.execute(
            text("SELECT pg_try_advisory_lock(:key)"),
            {"key": _PULL_ALL_ADVISORY_LOCK_KEY},
        ).scalar()
        holder.commit()
        assert acquired is True

        with patch.object(YFinancePuller, "_resolve_source_id", return_value=2), \
             patch("ingestion.yfinance_pull.yf.download") as mock_download:
            puller = YFinancePuller(engine_b)
            result = puller.pull_all(ticker_list=["AAA"], start_date="2026-09-11")

        assert result == [], "pull_all must skip entirely while the advisory lock is held"
        mock_download.assert_not_called()
    finally:
        holder.execute(
            text("SELECT pg_advisory_unlock(:key)"),
            {"key": _PULL_ALL_ADVISORY_LOCK_KEY},
        )
        holder.commit()
        holder.close()


def test_pull_all_via_real_engine_proceeds_once_the_lock_is_free(two_engines) -> None:
    """Sanity counterpart: with nothing else holding the lock, pull_all()
    must attempt its tickers and clean up its own advisory-lock connection
    (leaving the lock acquirable again immediately after)."""
    import pandas as pd

    from ingestion.yfinance_pull import YFinancePuller, _PULL_ALL_ADVISORY_LOCK_KEY

    engine_a, engine_b = two_engines
    frame = pd.DataFrame(
        {"Open": [87.35]},
        index=pd.DatetimeIndex([pd.Timestamp("2026-09-11")], name="Date"),
    )

    with patch.object(YFinancePuller, "_resolve_source_id", return_value=2), \
         patch.object(YFinancePuller, "_get_existing_dates", return_value=set()), \
         patch("ingestion.yfinance_pull.yf.download", return_value=frame) as mock_download:
        puller = YFinancePuller(engine_b)
        result = puller.pull_all(ticker_list=["AAA"], start_date="2026-09-11")

    assert len(result) == 1
    mock_download.assert_called_once()

    # pull_all's own advisory-lock connection must have released the lock
    # and returned to the pool — a fresh connection can take it immediately.
    with engine_a.connect() as conn:
        reacquired = conn.execute(
            text("SELECT pg_try_advisory_lock(:key)"),
            {"key": _PULL_ALL_ADVISORY_LOCK_KEY},
        ).scalar()
        conn.commit()
        assert reacquired is True, "pull_all left the advisory lock held after returning"
        conn.execute(
            text("SELECT pg_advisory_unlock(:key)"),
            {"key": _PULL_ALL_ADVISORY_LOCK_KEY},
        )
        conn.commit()
