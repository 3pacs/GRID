"""Wave 3 triage report item #22: `dad_ticker_summary_cache` has no writer
on `main` (4 leftover rows, 2026-09-21..23, from older code) -- the DB
SELECT in `_read_summary_cache` was a dead fallback read. It has been
removed; the function now only ever consults the in-memory
`_GOLD_MEMORY_CACHE`.

This is a pure unit test (no Postgres, no SQLite) -- `engine` is mocked and
asserted to never be touched.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import api.routers.dad as dad


def _clear_cache():
    with dad._GOLD_MEMORY_CACHE_LOCK:
        dad._GOLD_MEMORY_CACHE.clear()


def test_miss_returns_none_and_never_touches_the_engine():
    _clear_cache()
    engine = MagicMock()

    result = dad._read_summary_cache(engine, "AAPL", Path("/missing/research.duckdb"))

    assert result is None
    engine.connect.assert_not_called()
    engine.begin.assert_not_called()


def test_fresh_in_memory_entry_is_served_without_touching_the_engine():
    _clear_cache()
    engine = MagicMock()
    db_path = Path("/missing/research.duckdb")
    payload = {"ticker": "AAPL", "status": "ready", "gold": {"score": 42}}
    dad._remember_summary_cache("AAPL", db_path, payload)

    result = dad._read_summary_cache(engine, "AAPL", db_path)

    assert result is not None
    assert result["gold"]["score"] == 42
    assert result["cache"]["hit"] is True
    assert result["cache"]["stale"] is False
    engine.connect.assert_not_called()
    engine.begin.assert_not_called()


def test_entry_older_than_max_age_seconds_is_a_miss_not_a_db_fallback():
    _clear_cache()
    engine = MagicMock()
    db_path = Path("/missing/research.duckdb")
    key = ("AAPL", dad.DAD_CACHE_VERSION, *dad._research_db_fingerprint(db_path))
    old = datetime.now(timezone.utc) - timedelta(seconds=dad.SUMMARY_CACHE_TTL_SECONDS + 1)
    with dad._GOLD_MEMORY_CACHE_LOCK:
        dad._GOLD_MEMORY_CACHE[key] = (old, {"ticker": "AAPL", "status": "ready"})

    result = dad._read_summary_cache(
        engine, "AAPL", db_path, max_age_seconds=dad.SUMMARY_CACHE_TTL_SECONDS,
    )

    assert result is None
    engine.connect.assert_not_called()


def test_stale_tier_read_still_serves_an_older_in_memory_entry():
    """max_age_seconds=GOLD_STALE_MAX_AGE_SECONDS is the stale-while-
    revalidate tier -- an entry within that wider window is still a hit,
    and cache.stale is set once it has outlived the fresh TTL."""
    _clear_cache()
    engine = MagicMock()
    db_path = Path("/missing/research.duckdb")
    key = ("AAPL", dad.DAD_CACHE_VERSION, *dad._research_db_fingerprint(db_path))
    aged = datetime.now(timezone.utc) - timedelta(seconds=dad.SUMMARY_CACHE_TTL_SECONDS + 5)
    with dad._GOLD_MEMORY_CACHE_LOCK:
        dad._GOLD_MEMORY_CACHE[key] = (aged, {"ticker": "AAPL", "status": "ready"})

    result = dad._read_summary_cache(
        engine, "AAPL", db_path, max_age_seconds=dad.GOLD_STALE_MAX_AGE_SECONDS,
    )

    assert result is not None
    assert result["cache"]["hit"] is True
    assert result["cache"]["stale"] is True
    engine.connect.assert_not_called()


def test_different_ticker_is_a_miss_and_never_touches_the_engine():
    _clear_cache()
    engine = MagicMock()
    db_path = Path("/missing/research.duckdb")
    dad._remember_summary_cache("AAPL", db_path, {"ticker": "AAPL", "status": "ready"})

    result = dad._read_summary_cache(engine, "MSFT", db_path)

    assert result is None
    engine.connect.assert_not_called()
