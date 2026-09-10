"""Tests for the /api/v1/flows/sectors stale-while-revalidate cache.

Context: AGENTS.md (2026-05-30) recorded money_flow -> /flows/sectors as
"40s cold but 20ms warm" and left it as-is. A 2026-07-15 TTLCache patch was
applied directly on the server and never committed, so a cold process
(fresh deploy, or an idle process whose cache entry expired) still blocked
the request for the full ~40s compute. This fixes it at the root:

  * the N+1 per-ticker price loop in ``_compute_sectors_payload`` becomes a
    single batched, bounded (``obs_date >= :lookback``), parameterized query
  * the request handler never runs the computation inline — it only ever
    reads from a fresh cache, a stale-while-revalidate cache, or returns an
    explicit empty/unavailable payload; a background loop is the only thing
    that ever calls ``_compute_sectors_payload``

Uses a MagicMock Engine that dispatches SQL to a side-effect function,
matching the pattern in tests/test_sector_health.py.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from api.routers import flows as flows_router
from utils.ttl_cache import TTLCache


# ── Mock engine helper (mirrors tests/test_sector_health.py) ──────────────


def _make_router_engine(side_effect):
    engine = MagicMock()
    conn = MagicMock()

    def execute(stmt, *args, **kwargs):
        sql = str(getattr(stmt, "text", stmt))
        params = {}
        try:
            params = stmt.compile().params  # type: ignore[attr-defined]
        except Exception:
            pass
        return side_effect(sql, params)

    conn.execute.side_effect = execute
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return engine, conn


def _res(rows=None, one=None):
    m = MagicMock()
    m.fetchall.return_value = rows if rows is not None else []
    m.fetchone.return_value = one if one is not None else (rows[0] if rows else None)
    return m


@pytest.fixture(autouse=True)
def _clear_caches_and_thread_flag():
    flows_router._sector_cache.clear()
    flows_router._sector_stale_cache.clear()
    yield
    flows_router._sector_cache.clear()
    flows_router._sector_stale_cache.clear()
    flows_router._sector_warm_thread_started = False


# ── Cache tiers are the shared TTLCache utility ────────────────────────────


def test_sector_caches_are_ttlcache_instances():
    assert isinstance(flows_router._sector_cache, TTLCache)
    assert isinstance(flows_router._sector_stale_cache, TTLCache)


def test_stale_cache_ttl_is_much_longer_than_fresh_ttl():
    # The whole point of the stale tier is to outlive the fresh TTL by a
    # wide margin so a cold process still has *something* to serve.
    assert flows_router._sector_stale_cache._ttl > flows_router._sector_cache._ttl * 10


# ── get_sectors(): fresh hit, stale hit, cold miss ─────────────────────────


def test_get_sectors_fresh_cache_hit_never_computes(monkeypatch):
    def _boom():
        raise AssertionError("must not compute on a fresh cache hit")

    monkeypatch.setattr(flows_router, "_compute_sectors_payload", _boom)
    monkeypatch.setattr(flows_router, "_ensure_sector_warm_thread", lambda: None)

    payload = {"sectors": {"Technology": {"etf": "XLK"}}}
    flows_router._sector_cache.set(flows_router._SECTOR_CACHE_KEY, payload)

    result = flows_router.get_sectors("test-token")
    assert result == payload


def test_get_sectors_falls_back_to_stale_when_fresh_expired(monkeypatch):
    """Stale-while-revalidate: fresh cache empty, stale cache has the last
    good payload -> serve it immediately, don't block on a recompute."""

    def _boom():
        raise AssertionError("must not compute inline while serving stale")

    monkeypatch.setattr(flows_router, "_compute_sectors_payload", _boom)
    monkeypatch.setattr(flows_router, "_ensure_sector_warm_thread", lambda: None)

    stale_payload = {"sectors": {"Consumer Staples": {"etf": "XLP"}}}
    flows_router._sector_stale_cache.set(flows_router._SECTOR_CACHE_KEY, stale_payload)

    result = flows_router.get_sectors("test-token")
    assert result == stale_payload
    # Response shape for a real (even if stale) payload is unchanged —
    # no extra flags are injected onto genuine data.
    assert "stale" not in result
    assert "unavailable" not in result


def test_get_sectors_cold_process_returns_explicit_unavailable_without_blocking(monkeypatch):
    """True cold start: nothing computed yet in this process. Must return
    fast with an explicit empty/unavailable payload, never fabricate data,
    and never block on the ~40s computation."""
    warm_calls = []
    monkeypatch.setattr(
        flows_router, "_ensure_sector_warm_thread", lambda: warm_calls.append(1),
    )

    def _boom():
        raise AssertionError("must not compute inline on a cold miss")

    monkeypatch.setattr(flows_router, "_compute_sectors_payload", _boom)

    result = flows_router.get_sectors("test-token")

    assert result == {"sectors": {}, "stale": True, "unavailable": True}
    # The warm loop must still be (lazily) kicked off so the process
    # self-heals for the next request.
    assert warm_calls == [1]


def test_get_sectors_always_ensures_warm_thread(monkeypatch):
    calls = []
    monkeypatch.setattr(
        flows_router, "_ensure_sector_warm_thread", lambda: calls.append(1),
    )
    flows_router._sector_cache.set(
        flows_router._SECTOR_CACHE_KEY, {"sectors": {}},
    )
    flows_router.get_sectors("test-token")
    assert calls == [1]


# ── Warm loop: one cycle at a time, success and failure ────────────────────


def test_sector_warm_cycle_success_populates_both_tiers(monkeypatch):
    payload = {"sectors": {"Energy": {"etf": "XLE"}}}
    monkeypatch.setattr(flows_router, "_compute_sectors_payload", lambda: payload)

    sleep_for = flows_router._sector_warm_cycle()

    assert flows_router._sector_cache.get(flows_router._SECTOR_CACHE_KEY) == payload
    assert flows_router._sector_stale_cache.get(flows_router._SECTOR_CACHE_KEY) == payload
    # Re-warm comfortably inside the fresh TTL, never at/above it.
    assert flows_router._SECTOR_WARM_RETRY_SECONDS <= sleep_for < flows_router._SECTOR_CACHE_TTL


def test_sector_warm_cycle_failure_leaves_caches_untouched_and_retries_soon(monkeypatch):
    def _boom():
        raise RuntimeError("DB unreachable")

    monkeypatch.setattr(flows_router, "_compute_sectors_payload", _boom)

    sleep_for = flows_router._sector_warm_cycle()

    assert flows_router._sector_cache.get(flows_router._SECTOR_CACHE_KEY) is None
    assert flows_router._sector_stale_cache.get(flows_router._SECTOR_CACHE_KEY) is None
    assert sleep_for == flows_router._SECTOR_WARM_RETRY_SECONDS


def test_ensure_sector_warm_thread_starts_exactly_once(monkeypatch):
    monkeypatch.setattr(flows_router, "_sector_warm_loop", lambda: None)
    thread_mock = MagicMock()
    monkeypatch.setattr(flows_router.threading, "Thread", thread_mock)

    flows_router._ensure_sector_warm_thread()
    flows_router._ensure_sector_warm_thread()
    flows_router._ensure_sector_warm_thread()

    assert thread_mock.call_count == 1
    thread_mock.return_value.start.assert_called_once()


# ── _compute_sectors_payload: bounded, parameterized batched query ─────────


def test_compute_sectors_payload_uses_bounded_parameterized_price_query(monkeypatch):
    """Root-cause regression guard: the price fetch must be a single batched
    query with an ``obs_date`` lower bound and bound params, not the old
    per-ticker N+1 loop with unbounded history scans or string-built SQL."""
    executed_sql: list[str] = []

    def side_effect(sql: str, params: dict):
        executed_sql.append(sql)
        s = sql.lower()
        if "to_regclass" in s:
            return _res(one=(None,))
        if "feature_registry" in s:
            return _res(rows=[])
        if "options_daily_signals" in s:
            return _res(rows=[])
        if "raw_series" in s and "distinct on" in s:
            return _res(rows=[])
        return _res(rows=[])

    engine, _ = _make_router_engine(side_effect)
    monkeypatch.setattr(flows_router, "get_db_engine", lambda: engine)
    monkeypatch.setattr(
        "api.dependencies.get_pit_store", lambda: MagicMock(get_feature_matrix=lambda **kw: None),
    )

    result = flows_router._compute_sectors_payload()

    assert "sectors" in result
    assert isinstance(result["sectors"], dict) and len(result["sectors"]) > 0

    price_queries = [
        sql for sql in executed_sql
        if "raw_series" in sql.lower() and "distinct on" in sql.lower()
    ]
    # One batched "latest" + one batched "prev" query — not one pair per ticker.
    assert len(price_queries) == 2
    for sql in price_queries:
        assert "%" not in sql and ".format(" not in sql  # no f-string/format SQL
        assert ":lookback" in sql  # obs_date lower bound is a bound parameter
        assert "obs_date >= :lookback" in sql


def test_compute_sectors_payload_survives_price_query_failure(monkeypatch):
    """Never fabricate data: if the price batch query blows up, the sector
    payload still comes back (degraded, not a 500)."""

    def side_effect(sql: str, params: dict):
        s = sql.lower()
        if "raw_series" in s:
            raise RuntimeError("connection reset")
        if "to_regclass" in s:
            return _res(one=(None,))
        return _res(rows=[])

    engine, _ = _make_router_engine(side_effect)
    monkeypatch.setattr(flows_router, "get_db_engine", lambda: engine)
    monkeypatch.setattr(
        "api.dependencies.get_pit_store", lambda: MagicMock(get_feature_matrix=lambda **kw: None),
    )

    result = flows_router._compute_sectors_payload()
    assert "sectors" in result
    assert isinstance(result["sectors"], dict) and len(result["sectors"]) > 0
    for sector in result["sectors"].values():
        assert sector["etf_price"] is None
