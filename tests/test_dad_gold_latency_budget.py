"""Tests for the dad:gold (GET /api/v1/dad/ticker/{ticker}/gold) request budget.

Context: preserved dad-smoke artifacts (00-Agent-Reports/2026-09-19 and -20,
releases packet1/batch3/#583/#585 in the 3pacs/obsidian-vault repo) show
`dad:gold`'s compute path (_load_workbook_context + _load_grid_payload) taking
10.8s-13.0s on "ok" runs and 21.1s-25.1s on runs the smoke graded "broken"
(`dad:gold Read timed out (15.0s)`), measured via the endpoint's own
`_log_slow_ticker`. All those measurements were taken in the ~2-minute window
after an API restart, alongside `api.main:_sync_deferred_startup` holding a DB
connection for a fixed 120s -- but every captured sample is from that same
post-restart window (the smoke always runs right after a deploy restarts the
API), so the artifacts alone cannot separate "cold-cache compute cost" from
"startup-warmer pool contention" the way a controlled local repro can.

These tests reproduce the slow path locally with mocked, controlled-delay
dependencies (no live DB, no production access) and exercise the fix: a
bounded request budget (GOLD_COMPACT_BUDGET_SECONDS) around the compute, a
stale-cache fallback, an honest "unavailable" response with no fabricated
values, and single-flight de-duplication for concurrent requests.
"""

from __future__ import annotations

import concurrent.futures
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

import api.routers.dad as dad


def _fresh_payload(ticker: str) -> dict:
    return {
        "ticker": ticker,
        "status": "ready",
        "gold": {"verdict": "test", "score": 42, "tone": "watch", "one_liner": "x"},
        "performance": {"timings_ms": {}, "total_ms": 5.0},
    }


@pytest.fixture(autouse=True)
def _reset_inflight():
    """Each test gets a clean single-flight table regardless of prior test outcomes."""
    dad._GOLD_INFLIGHT.clear()
    yield
    dad._GOLD_INFLIGHT.clear()


# --- warm cache -------------------------------------------------------------


def test_warm_cache_hit_returns_immediately_without_compute():
    """A fresh cache row must short-circuit before any compute path runs."""
    cached = _fresh_payload("AAPL")

    def _boom(*a, **k):
        raise AssertionError("compute path must not run on a fresh cache hit")

    with patch.object(dad, "_read_summary_cache", return_value=dict(cached)), \
         patch.object(dad, "_load_workbook_context", side_effect=_boom), \
         patch.object(dad, "_load_grid_payload", side_effect=_boom), \
         patch.object(dad, "get_db_engine", return_value=MagicMock()):
        result = dad._build_compact_dad_response("AAPL", use_cache=True)

    assert result["ticker"] == "AAPL"
    assert result["status"] == "ready"
    assert "cache_read_ms" in result["performance"]


# --- cold start --------------------------------------------------------------


def test_cold_start_within_budget_computes_and_writes_cache():
    """No cache row, compute finishes well inside budget -> normal fresh payload, cache written."""
    write_calls = []

    def _fake_workbook(ticker, **kwargs):
        return {"status": "ready", "summary": {"mentions": 1}, "workbook": {"files": [], "sheets": [], "evidence": []},
                "source_lanes": [], "dad_stats": [], "fit_signals": [], "source": {"attached": True, "db_path": "x"}}

    def _fake_grid(ticker, **kwargs):
        return {"status": "ready", "finviz": {"status": "ready"}, "grid": {"status": "ready"},
                "options": None, "signals": {"signal_sources": [], "tradingview_signals": [], "regime": None}}

    def _fake_write(engine, ticker, db_path, payload, timings):
        write_calls.append(ticker)

    with patch.object(dad, "_read_summary_cache", return_value=None), \
         patch.object(dad, "_load_workbook_context", side_effect=_fake_workbook), \
         patch.object(dad, "_load_grid_payload", side_effect=_fake_grid), \
         patch.object(dad, "_write_summary_cache", side_effect=_fake_write), \
         patch.object(dad, "get_db_engine", return_value=MagicMock()):
        result = dad._build_compact_dad_response("MSFT", use_cache=True)

    assert result["ticker"] == "MSFT"
    assert result["status"] == "ready"
    assert write_calls == ["MSFT"]
    assert result["performance"]["total_ms"] >= 0


# --- dependency timeout, no fallback data ------------------------------------


def test_dependency_timeout_with_no_cache_returns_honest_unavailable(monkeypatch):
    """Compute exceeds the budget and there's no fresh or stale cache: honest, unbounded-free, no fabricated numbers."""
    monkeypatch.setattr(dad, "GOLD_COMPACT_BUDGET_SECONDS", 0.15)

    def _slow_workbook(ticker, **kwargs):
        time.sleep(0.6)  # controlled delayed dependency, well past the 0.15s budget
        return {"status": "ready", "summary": None, "workbook": {"files": [], "sheets": [], "evidence": []},
                "source_lanes": [], "dad_stats": [], "fit_signals": [], "source": {"attached": True, "db_path": "x"}}

    with patch.object(dad, "_read_summary_cache", return_value=None), \
         patch.object(dad, "_load_workbook_context", side_effect=_slow_workbook), \
         patch.object(dad, "_load_grid_payload", return_value=dad._empty_grid_payload("unused")), \
         patch.object(dad, "_write_summary_cache", return_value=None), \
         patch.object(dad, "get_db_engine", return_value=MagicMock()):
        start = time.perf_counter()
        result = dad._build_compact_dad_response("SLOW", use_cache=True)
        elapsed = time.perf_counter() - start
        # The orphaned compute is still running past the budget (that's the
        # point -- it finishes and populates the cache for the next caller).
        # Drain it here, mocks still active, so it can't call a *later*
        # test's patched functions after this `with` block exits (module-
        # level lookups are late-bound: an orphan calling `_load_grid_payload`
        # after this context closes would hit whatever is patched at that
        # moment, contaminating an unrelated test).
        orphan = dad._GOLD_INFLIGHT.get("SLOW:False")
        if orphan is not None:
            orphan.result(timeout=5)

    # The request must come back close to the budget, not wait out the 0.6s dependency.
    assert elapsed < 0.5
    assert result["ticker"] == "SLOW"
    assert result["status"] == "unavailable"
    assert result["performance"]["budget_exceeded"] is True
    assert result["cache"] == {"hit": False, "stale": False, "ttl_seconds": dad.SUMMARY_CACHE_TTL_SECONDS}
    # No fabricated values: the honest-empty gold/decision-stack shape, same as
    # a genuinely missing-workbook ticker (_gold_from_summary(None)).
    assert result["gold"] == dad._gold_from_summary(None)


def test_dependency_timeout_falls_back_to_stale_cache(monkeypatch):
    """Compute exceeds the budget but a stale (past-TTL) cache row exists: return it, not a blank unavailable shape."""
    monkeypatch.setattr(dad, "GOLD_COMPACT_BUDGET_SECONDS", 0.15)
    stale_payload = _fresh_payload("STALE")
    stale_payload["cache"] = {"hit": True, "stale": True, "generated_at": "2026-09-01T00:00:00+00:00",
                               "ttl_seconds": dad.SUMMARY_CACHE_TTL_SECONDS}

    def _slow_workbook(ticker, **kwargs):
        time.sleep(0.6)
        return {"status": "ready", "summary": None, "workbook": {"files": [], "sheets": [], "evidence": []},
                "source_lanes": [], "dad_stats": [], "fit_signals": [], "source": {"attached": True, "db_path": "x"}}

    read_calls = []

    def _fake_read(engine, ticker, db_path, *, max_age_seconds=dad.SUMMARY_CACHE_TTL_SECONDS):
        read_calls.append(max_age_seconds)
        if max_age_seconds == dad.SUMMARY_CACHE_TTL_SECONDS:
            return None  # fresh miss
        return dict(stale_payload)  # stale fallback hit

    with patch.object(dad, "_read_summary_cache", side_effect=_fake_read), \
         patch.object(dad, "_load_workbook_context", side_effect=_slow_workbook), \
         patch.object(dad, "_load_grid_payload", return_value=dad._empty_grid_payload("unused")), \
         patch.object(dad, "_write_summary_cache", return_value=None), \
         patch.object(dad, "get_db_engine", return_value=MagicMock()):
        result = dad._build_compact_dad_response("STALE", use_cache=True)
        orphan = dad._GOLD_INFLIGHT.get("STALE:False")
        if orphan is not None:
            orphan.result(timeout=5)  # drain before mocks are reverted -- see test above

    assert result["ticker"] == "STALE"
    assert result["cache"]["stale"] is True
    # Confirms the fallback read actually used the wider stale window, not the fresh TTL.
    assert dad.GOLD_STALE_MAX_AGE_SECONDS in read_calls


# --- concurrent requests / single-flight -------------------------------------


def test_concurrent_requests_for_same_ticker_share_one_compute(monkeypatch):
    """N concurrent cold requests for the same ticker must trigger exactly one compute."""
    monkeypatch.setattr(dad, "GOLD_COMPACT_BUDGET_SECONDS", 2.0)
    call_count = 0
    call_lock = threading.Lock()

    def _counted_workbook(ticker, **kwargs):
        nonlocal call_count
        with call_lock:
            call_count += 1
        time.sleep(0.2)  # controlled delay so concurrent callers overlap the same compute
        return {"status": "ready", "summary": {"mentions": 1}, "workbook": {"files": [], "sheets": [], "evidence": []},
                "source_lanes": [], "dad_stats": [], "fit_signals": [], "source": {"attached": True, "db_path": "x"}}

    with patch.object(dad, "_read_summary_cache", return_value=None), \
         patch.object(dad, "_load_workbook_context", side_effect=_counted_workbook), \
         patch.object(dad, "_load_grid_payload", return_value=dad._empty_grid_payload("unused")), \
         patch.object(dad, "_write_summary_cache", return_value=None), \
         patch.object(dad, "get_db_engine", return_value=MagicMock()):
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(dad._build_compact_dad_response, "SHARED", use_cache=True) for _ in range(8)]
            results = [f.result(timeout=5) for f in futures]

    assert call_count == 1, f"expected a single shared compute, got {call_count}"
    for result in results:
        assert result["ticker"] == "SHARED"
        assert result["status"] == "ready"


def test_concurrent_requests_bounded_executor_does_not_grow_unbounded():
    """The shared compute executor is a fixed-size pool, not one thread per request."""
    assert dad._GOLD_COMPACT_EXECUTOR._max_workers == int(
        __import__("os").environ.get("GRID_DAD_GOLD_EXECUTOR_WORKERS", "4")
    )


def test_inflight_key_distinguishes_refresh_finviz(monkeypatch):
    """A plain request and a refresh_finviz request for the same ticker must not share a compute."""
    monkeypatch.setattr(dad, "GOLD_COMPACT_BUDGET_SECONDS", 2.0)
    seen_refresh_values = []

    def _fake_workbook(ticker, **kwargs):
        return {"status": "ready", "summary": None, "workbook": {"files": [], "sheets": [], "evidence": []},
                "source_lanes": [], "dad_stats": [], "fit_signals": [], "source": {"attached": True, "db_path": "x"}}

    def _fake_grid(ticker, *, refresh_finviz=False, **kwargs):
        seen_refresh_values.append(refresh_finviz)
        time.sleep(0.1)
        return dad._empty_grid_payload("unused")

    with patch.object(dad, "_read_summary_cache", return_value=None), \
         patch.object(dad, "_load_workbook_context", side_effect=_fake_workbook), \
         patch.object(dad, "_load_grid_payload", side_effect=_fake_grid), \
         patch.object(dad, "_write_summary_cache", return_value=None), \
         patch.object(dad, "get_db_engine", return_value=MagicMock()):
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            f1 = pool.submit(dad._build_compact_dad_response, "BOTH", use_cache=True, refresh_finviz=False)
            f2 = pool.submit(dad._build_compact_dad_response, "BOTH", use_cache=False, refresh_finviz=True)
            f1.result(timeout=5)
            f2.result(timeout=5)

    assert sorted(seen_refresh_values) == [False, True]


# --- SQL parameter-binding guard (security.md: never interpolate a dynamic
#     interval into the SQL string) -----------------------------------------


def test_read_summary_cache_binds_max_age_as_a_parameter():
    """max_age_seconds must be a bound param via make_interval, never string-interpolated."""
    executed = []

    class _FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execute(self, statement, params=None):
            executed.append((str(statement), params))

            class _Result:
                def fetchone(self_inner):
                    return None

            return _Result()

    class _FakeEngine:
        def begin(self):
            return _FakeConn()

        def connect(self):
            return _FakeConn()

    dad._read_summary_cache(_FakeEngine(), "AAPL", __import__("pathlib").Path("/tmp/does-not-exist.duckdb"),
                             max_age_seconds=999)

    select_calls = [(sql, params) for sql, params in executed if "SELECT payload" in sql]
    assert len(select_calls) == 1, executed
    sql, params = select_calls[0]
    assert "make_interval" in sql
    assert "999" not in sql
    assert params["max_age_seconds"] == 999
