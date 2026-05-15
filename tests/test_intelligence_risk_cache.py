"""Regression tests for the thread-safe TTLCache wiring on /risk-map and /globe.

The risk router previously held two plain dicts (``_risk_map_cache`` and
``_globe_cache``) for cached payload + timestamp. Under the multi-threaded
uvicorn worker model a concurrent reader could observe a half-updated
``data``/``ts`` pair and serve stale-flagged-as-fresh content. Swapping in
``utils.ttl_cache.TTLCache`` gives us atomic get/set under an internal lock
with proper expiry.

These tests are pure / async-free: they exercise the module-level cache
objects directly and never call into the real ``_build_risk_map`` /
``_build_globe_data`` DB-backed builders.
"""

from __future__ import annotations

import threading
import time

import pytest

from api.routers import intelligence_risk
from utils.ttl_cache import TTLCache


# ── /risk-map cache ─────────────────────────────────────────────────────


def test_risk_map_cache_is_ttlcache_instance():
    assert isinstance(intelligence_risk._risk_map_cache, TTLCache)


def test_risk_map_cache_ttl_matches_constant():
    assert intelligence_risk._risk_map_cache._ttl == intelligence_risk._RISK_MAP_TTL


def test_risk_map_cache_set_then_get_roundtrip():
    cache = intelligence_risk._risk_map_cache
    cache.clear()
    payload = {"dealer_risk": {"risk_level": "moderate"}, "overall_score": 0.42}
    cache.set(intelligence_risk._RISK_MAP_CACHE_KEY, payload)
    assert cache.get(intelligence_risk._RISK_MAP_CACHE_KEY) == payload


def test_risk_map_cache_returns_none_when_missing():
    cache = intelligence_risk._risk_map_cache
    cache.clear()
    assert cache.get(intelligence_risk._RISK_MAP_CACHE_KEY) is None


# ── /globe cache ────────────────────────────────────────────────────────


def test_globe_cache_is_ttlcache_instance():
    assert isinstance(intelligence_risk._globe_cache, TTLCache)


def test_globe_cache_ttl_matches_constant():
    assert intelligence_risk._globe_cache._ttl == intelligence_risk._GLOBE_TTL


def test_globe_cache_set_then_get_roundtrip():
    cache = intelligence_risk._globe_cache
    cache.clear()
    payload = {"countries": [{"iso": "US", "score": 1.0}]}
    cache.set(intelligence_risk._GLOBE_CACHE_KEY, payload)
    assert cache.get(intelligence_risk._GLOBE_CACHE_KEY) == payload


def test_globe_cache_returns_none_when_missing():
    cache = intelligence_risk._globe_cache
    cache.clear()
    assert cache.get(intelligence_risk._GLOBE_CACHE_KEY) is None


# ── Shared TTL / concurrency behaviour ──────────────────────────────────


def test_ttl_cache_expires_after_ttl():
    short_cache = TTLCache(ttl=0.05, max_size=1)
    short_cache.set("k", {"v": 1})
    assert short_cache.get("k") == {"v": 1}
    time.sleep(0.1)
    assert short_cache.get("k") is None


def test_ttl_cache_concurrent_set_and_get_is_safe():
    """Concurrent writers + readers must not raise and must end in a
    self-consistent state -- the whole reason for the migration."""
    cache = TTLCache(ttl=60.0, max_size=1)
    errors: list[BaseException] = []
    stop = threading.Event()

    def writer():
        i = 0
        while not stop.is_set():
            try:
                cache.set("k", {"i": i})
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)
            i += 1

    def reader():
        while not stop.is_set():
            try:
                val = cache.get("k")
                assert val is None or isinstance(val, dict)
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

    threads = [threading.Thread(target=writer) for _ in range(3)] + \
              [threading.Thread(target=reader) for _ in range(5)]
    for t in threads:
        t.start()
    time.sleep(0.2)
    stop.set()
    for t in threads:
        t.join(timeout=2.0)

    assert not errors, f"thread-safety failure: {errors[:3]}"


@pytest.fixture(autouse=True)
def _clear_after_each():
    yield
    intelligence_risk._risk_map_cache.clear()
    intelligence_risk._globe_cache.clear()
