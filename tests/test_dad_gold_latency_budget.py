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
    dad._GOLD_MEMORY_CACHE.clear()
    yield
    dad._GOLD_INFLIGHT.clear()
    dad._GOLD_MEMORY_CACHE.clear()


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


def test_cold_start_within_budget_computes_and_remembers_cache():
    """No cache row, compute finishes well inside budget -> normal fresh payload, cache remembered."""
    write_calls = []

    def _fake_workbook(ticker, **kwargs):
        return {"status": "ready", "summary": {"mentions": 1}, "workbook": {"files": [], "sheets": [], "evidence": []},
                "source_lanes": [], "dad_stats": [], "fit_signals": [], "source": {"attached": True, "db_path": "x"}}

    def _fake_grid(ticker, **kwargs):
        return {"status": "ready", "finviz": {"status": "ready"}, "grid": {"status": "ready"},
                "options": None, "signals": {"signal_sources": [], "tradingview_signals": [], "regime": None}}

    def _fake_write(ticker, db_path, payload):
        write_calls.append(ticker)

    with patch.object(dad, "_read_summary_cache", return_value=None), \
         patch.object(dad, "_load_workbook_context", side_effect=_fake_workbook), \
         patch.object(dad, "_load_grid_payload", side_effect=_fake_grid), \
         patch.object(dad, "_remember_summary_cache", side_effect=_fake_write), \
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
         patch.object(dad, "_remember_summary_cache", return_value=None), \
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
    assert result["performance"]["capacity_exceeded"] is False
    assert result["cache"] == {"hit": False, "stale": False, "ttl_seconds": dad.SUMMARY_CACHE_TTL_SECONDS}
    # No fabricated values, AND not a measured zero: gold.score is None (a
    # measurement was never taken), not 0 (a measurement of exactly zero) --
    # see test_unavailable_gold_is_distinguishable_from_a_measured_zero below
    # for why this distinction is the point, not an implementation detail.
    assert result["gold"]["score"] is None
    assert result["gold"]["tone"] == "unknown"
    assert result["gold"] != dad._gold_from_summary(None)


def test_budget_only_ends_the_wait_the_compute_keeps_running_and_still_caches(monkeypatch):
    """The 8s budget bounds the caller's wait, not the compute: it still fills local cache."""
    monkeypatch.setattr(dad, "GOLD_COMPACT_BUDGET_SECONDS", 0.1)
    write_calls = []

    def _slow_workbook(ticker, **kwargs):
        time.sleep(0.4)  # comfortably past the 0.1s budget
        return {"status": "ready", "summary": None, "workbook": {"files": [], "sheets": [], "evidence": []},
                "source_lanes": [], "dad_stats": [], "fit_signals": [], "source": {"attached": True, "db_path": "x"}}

    def _fake_write(ticker, db_path, payload):
        write_calls.append(ticker)

    with patch.object(dad, "_read_summary_cache", return_value=None), \
         patch.object(dad, "_load_workbook_context", side_effect=_slow_workbook), \
         patch.object(dad, "_load_grid_payload", return_value=dad._empty_grid_payload("unused")), \
         patch.object(dad, "_remember_summary_cache", side_effect=_fake_write), \
         patch.object(dad, "get_db_engine", return_value=MagicMock()):
        result = dad._build_compact_dad_response("ORPHAN", use_cache=True)
        assert result["performance"]["budget_exceeded"] is True
        assert write_calls == []  # not remembered yet -- the caller gave up before compute finished

        # Single-flight ownership is retained until the orphan actually
        # exits: the same key must still map to the (still-running) future.
        orphan = dad._GOLD_INFLIGHT.get("ORPHAN:False")
        assert orphan is not None
        assert not orphan.done()

        orphan.result(timeout=5)  # let it finish while mocks are still active

    # The abandoned compute ran to completion and populated the cache, even
    # though its own caller had already received a degraded response.
    assert write_calls == ["ORPHAN"]
    # And single-flight cleaned itself up once the orphan exited.
    assert dad._GOLD_INFLIGHT.get("ORPHAN:False") is None


def test_dependency_timeout_falls_back_to_stale_cache(monkeypatch):
    """Compute exceeds the budget but a stale (past-TTL) cache row exists: return it, visibly marked stale."""
    monkeypatch.setattr(dad, "GOLD_COMPACT_BUDGET_SECONDS", 0.15)
    stale_payload = _fresh_payload("STALE")
    stale_payload["message"] = None
    stale_payload["decision_stack"] = {"stance": "Watchlist with checks", "tone": "watch", "score": 50,
                                        "cards": [], "reasons": [], "blockers": [], "method": "x"}
    two_hours_ago_seconds = 2 * 3600
    stale_payload["cache"] = {"hit": True, "stale": True, "generated_at": "2026-09-01T00:00:00+00:00",
                               "age_seconds": two_hours_ago_seconds, "ttl_seconds": dad.SUMMARY_CACHE_TTL_SECONDS}

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
         patch.object(dad, "_remember_summary_cache", return_value=None), \
         patch.object(dad, "get_db_engine", return_value=MagicMock()):
        result = dad._build_compact_dad_response("STALE", use_cache=True)
        orphan = dad._GOLD_INFLIGHT.get("STALE:False")
        if orphan is not None:
            orphan.result(timeout=5)  # drain before mocks are reverted -- see test above

    assert result["ticker"] == "STALE"
    assert result["cache"]["stale"] is True
    # Confirms the fallback read actually used the wider stale window, not the fresh TTL.
    assert dad.GOLD_STALE_MAX_AGE_SECONDS in read_calls

    # A stale response must not appear current: age/stale status visible at
    # the top level (not just buried in `cache`), and reaching the actual
    # displayed guidance -- the gold verdict text and the decision-stack
    # blockers a person would read as advice -- not only a nested flag.
    assert result["stale"] is True
    assert "2.0h" in result["message"]
    assert "cached snapshot" in result["message"].lower()
    assert "2.0h" in result["gold"]["one_liner"]
    assert result["gold"]["one_liner"] != _fresh_payload("STALE")["gold"]["one_liner"]
    assert "cached snapshot" in result["decision_stack"]["blockers"][0].lower()
    assert "2.0h" in result["decision_stack"]["blockers"][0]


def test_stale_marking_does_not_mutate_the_cached_row(monkeypatch):
    """_mark_stale_response must return a new dict, never rewrite the row _read_summary_cache handed back."""
    original = _fresh_payload("KEEP")
    original["cache"] = {"hit": True, "stale": True, "age_seconds": 5400, "generated_at": "x",
                          "ttl_seconds": dad.SUMMARY_CACHE_TTL_SECONDS}
    original_one_liner = original["gold"]["one_liner"]

    marked = dad._mark_stale_response(original, ticker="KEEP")

    assert marked is not original
    assert marked["gold"] is not original["gold"]
    assert original["gold"]["one_liner"] == original_one_liner  # untouched
    assert marked["gold"]["one_liner"] != original_one_liner


def test_stale_flag_reflects_actual_row_age_not_the_query_window():
    """A stale-tier query (wide max_age_seconds) landing on an actually-fresh row must not be mislabeled stale."""
    from datetime import datetime, timedelta, timezone

    very_recent = datetime.now(timezone.utc) - timedelta(seconds=5)

    class _FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execute(self, statement, params=None):
            class _Result:
                def fetchone(self_inner):
                    if "SELECT payload" not in str(statement):
                        return None
                    return ('{"ticker": "FRESHROW"}', very_recent, "{}")

            return _Result()

    class _FakeEngine:
        def begin(self):
            return _FakeConn()

        def connect(self):
            return _FakeConn()

    result = dad._read_summary_cache(
        _FakeEngine(), "FRESHROW", __import__("pathlib").Path("/tmp/does-not-exist.duckdb"),
        max_age_seconds=dad.GOLD_STALE_MAX_AGE_SECONDS,  # the wide stale-tier window
    )

    assert result is not None
    assert result["cache"]["stale"] is False  # the row itself is only 5s old
    assert result["cache"]["age_seconds"] < 60


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
         patch.object(dad, "_remember_summary_cache", return_value=None), \
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
         patch.object(dad, "_remember_summary_cache", return_value=None), \
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


# --- bounding background work across *different* tickers --------------------


def test_max_inflight_cap_bounds_background_work_across_tickers(monkeypatch):
    """Single-flight only dedupes repeats of one ticker; this caps the total across all tickers."""
    monkeypatch.setattr(dad, "GOLD_COMPACT_MAX_INFLIGHT", 2)
    gate = threading.Event()
    started = []

    def _blocked_workbook(ticker, **kwargs):
        started.append(ticker)
        gate.wait(timeout=5)  # held open until the test releases it
        return {"status": "ready", "summary": None, "workbook": {"files": [], "sheets": [], "evidence": []},
                "source_lanes": [], "dad_stats": [], "fit_signals": [], "source": {"attached": True, "db_path": "x"}}

    with patch.object(dad, "_load_workbook_context", side_effect=_blocked_workbook), \
         patch.object(dad, "_load_grid_payload", return_value=dad._empty_grid_payload("unused")), \
         patch.object(dad, "_remember_summary_cache", return_value=None):
        try:
            engine = MagicMock()
            f1 = dad._get_or_start_gold_compact("TICKER_A", refresh_finviz=False, engine=engine, db_path=None)
            f2 = dad._get_or_start_gold_compact("TICKER_B", refresh_finviz=False, engine=engine, db_path=None)
            assert f1 is not None and f2 is not None
            # Wait for both to actually be occupying the cap (not just queued)
            # before asserting the cap -- avoids a race against the executor
            # scheduling them.
            for _ in range(200):
                if len(started) >= 2:
                    break
                time.sleep(0.01)
            assert len(started) == 2

            # A third, DIFFERENT ticker must be refused a background compute
            # outright -- the cap is across tickers, not per-ticker.
            f3 = dad._get_or_start_gold_compact("TICKER_C", refresh_finviz=False, engine=engine, db_path=None)
            assert f3 is None
            assert "TICKER_C" not in started  # never even started
        finally:
            gate.set()
            for f in (f1, f2):
                if f is not None:
                    f.result(timeout=5)  # drain before mocks are reverted

    assert dad._GOLD_INFLIGHT == {}


def test_capacity_exceeded_falls_back_honestly_without_waiting(monkeypatch):
    """When the inflight cap is already full, a new ticker's request gets an honest fallback, not a wait."""
    monkeypatch.setattr(dad, "GOLD_COMPACT_MAX_INFLIGHT", 1)
    monkeypatch.setattr(dad, "GOLD_COMPACT_BUDGET_SECONDS", 5.0)  # would be slow if we actually waited
    gate = threading.Event()

    def _blocked_workbook(ticker, **kwargs):
        gate.wait(timeout=5)
        return {"status": "ready", "summary": None, "workbook": {"files": [], "sheets": [], "evidence": []},
                "source_lanes": [], "dad_stats": [], "fit_signals": [], "source": {"attached": True, "db_path": "x"}}

    with patch.object(dad, "_read_summary_cache", return_value=None), \
         patch.object(dad, "_load_workbook_context", side_effect=_blocked_workbook), \
         patch.object(dad, "_load_grid_payload", return_value=dad._empty_grid_payload("unused")), \
         patch.object(dad, "_remember_summary_cache", return_value=None), \
         patch.object(dad, "get_db_engine", return_value=MagicMock()):
        try:
            occupying = dad._get_or_start_gold_compact("OCCUPY", refresh_finviz=False, engine=MagicMock(), db_path=None)
            for _ in range(200):
                if occupying.running():
                    break
                time.sleep(0.01)

            start = time.perf_counter()
            result = dad._build_compact_dad_response("REFUSED", use_cache=True)
            elapsed = time.perf_counter() - start
        finally:
            gate.set()
            occupying.result(timeout=5)

    assert elapsed < 1.0  # did not wait out the 5s budget -- refused immediately
    assert result["status"] == "unavailable"
    assert result["performance"]["capacity_exceeded"] is True
    assert result["performance"]["budget_exceeded"] is False
    assert "REFUSED" not in dad._GOLD_INFLIGHT  # nothing was ever queued for it


# --- DB/DuckDB resource release on the timed-out (orphaned) path ------------


def test_orphaned_compute_always_releases_db_and_duckdb_via_context_managers():
    """Static guard: every engine checkout in the compute path is inside a `with` block, so it releases whenever
    the *worker's own call* returns or raises -- a normal return or an internal exception.

    This is a claim about the worker only. The caller's request timeout
    (GOLD_COMPACT_BUDGET_SECONDS via `future.result(timeout=...)`) is a
    separate thread giving up on waiting; it does not run, signal, cancel,
    or otherwise touch the worker in any way, and is therefore NOT one of
    the exit paths this guards. A worker that is single-flighted after its
    original caller already timed out releases its connection on exactly
    the same schedule it always would have -- when its own blocking call
    returns. There is no `engine.connect()`/`engine.begin()` call in this
    file that is not immediately a context manager, and DuckDB's connection
    is closed in a `finally`. This guards against a future edit
    reintroducing an un-released checkout in the orphaned-worker code path
    this PR made reachable.
    """
    import re
    from pathlib import Path as _Path

    src = _Path(dad.__file__).read_text(encoding="utf-8")

    # Every `engine.connect(`/`engine.begin(` call must be preceded on the
    # same line by `with ` (SQLAlchemy releases the checkout in
    # Connection.__exit__, on success or exception).
    bad_checkouts = [
        line for line in src.splitlines()
        if re.search(r"engine\.(connect|begin)\s*\(", line) and "with " not in line
    ]
    assert bad_checkouts == [], f"non-context-managed engine checkout(s): {bad_checkouts}"

    # _connect_duckdb's result must be closed in a `finally` inside
    # _load_workbook_context (the only caller in this file).
    workbook_src = src[src.index("def _load_workbook_context("):]
    workbook_src = workbook_src[: workbook_src.index("\n\n\ndef ")]
    assert "finally:" in workbook_src and "conn.close()" in workbook_src

    # Honesty about what this static check does NOT prove: it is a
    # structural claim (every checkout site is guarded), not a runtime
    # demonstration of prompt cancellation. It says nothing about *how long*
    # a currently-blocked call holds its connection -- context managers and
    # `finally` blocks only run once the call they wrap returns or raises;
    # they do not interrupt a call that is still in progress, and this
    # codebase has no mechanism that does. The actual bound on a stuck call
    # is the DB's own statement/connection timeout (already present in
    # production; see the preserved "canceling statement due to statement
    # timeout" log lines) -- not this test, not `with`, and not
    # GOLD_COMPACT_BUDGET_SECONDS. That dependency predates this PR and is
    # unchanged by it.


# --- unavailable must not read as a measured zero/neutral --------------------


def test_unavailable_gold_is_distinguishable_from_a_measured_zero():
    """A budget/capacity timeout must not produce the same gold card as a genuinely-checked, empty-history ticker.

    _gold_from_summary(None) is what the real (non-timeout) path returns
    for a ticker that WAS checked and truly has no workbook footprint --
    score 0, tone "neutral", verdict "No workbook history yet". If the
    degraded/unavailable path reused that verbatim, a consumer looking only
    at the gold card could not tell "Dad's corpus was searched and this
    ticker isn't in it" (a real, measured finding) from "we don't know
    because the compute didn't finish in time" (no finding at all). These
    must differ structurally, not just in a footnote.
    """
    measured_empty = dad._gold_from_summary(None)
    unavailable = dad._build_degraded_gold_response("NOPE", elapsed_ms=123.0, reason="budget_exceeded")["gold"]

    assert measured_empty["score"] == 0          # a real measurement: exactly zero
    assert unavailable["score"] is None           # no measurement was taken at all
    assert measured_empty["tone"] == "neutral"    # one of _gold_from_summary's real tones
    assert unavailable["tone"] == "unknown"       # not a tone _gold_from_summary ever produces
    assert unavailable["tone"] not in {"strong", "watch", "light", "neutral"}
    assert measured_empty["verdict"] != unavailable["verdict"]
    assert "not" in unavailable["one_liner"].lower() and "measur" in unavailable["one_liner"].lower()


def test_unavailable_decision_stack_card_and_blocker_also_say_unmeasured():
    """The decision-stack's Dad-workbooks card and blockers must carry the same distinction as the gold card."""
    payload = dad._build_degraded_gold_response("NOPE", elapsed_ms=50.0, reason="capacity_exceeded")

    dad_card = payload["decision_stack"]["cards"][0]
    assert dad_card["source"] == "Dad workbooks"
    assert dad_card["state"] == "unknown"
    assert dad_card["points"] is None             # not 0.0 -- no score was computed

    assert any("did not complete in time" in b and "not" in b.lower() and "verified" in b.lower()
               for b in payload["decision_stack"]["blockers"])


def test_capacity_exceeded_also_produces_unmeasured_gold():
    """Both refusal reasons (budget_exceeded, capacity_exceeded) get the same honest-unmeasured gold, not a zero."""
    for reason in ("budget_exceeded", "capacity_exceeded"):
        gold = dad._build_degraded_gold_response("X", elapsed_ms=1.0, reason=reason)["gold"]
        assert gold["score"] is None
        assert gold["tone"] == "unknown"
