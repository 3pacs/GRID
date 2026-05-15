"""Regression tests for the thread-safe TTLCache wiring on the unified-thesis
endpoint.

The thesis router previously held a plain dict (``_thesis_cache``) for the
cached payload + timestamp. Under the multi-threaded uvicorn worker model a
concurrent reader could observe a half-updated ``data``/``ts`` pair and serve
stale-flagged-as-fresh content. Swapping in ``utils.ttl_cache.TTLCache`` gives
us atomic get/set under an internal lock with proper expiry.

These tests are pure / async-free: they exercise the module-level cache object
directly and never call into the real ``score_thesis``/DB-backed builder.
"""

from __future__ import annotations

import threading
import time

import pytest

from api.routers import intelligence_thesis
from utils.ttl_cache import TTLCache


def test_thesis_cache_is_ttlcache_instance():
    assert isinstance(intelligence_thesis._thesis_cache, TTLCache)


def test_thesis_cache_ttl_matches_constant():
    assert intelligence_thesis._thesis_cache._ttl == intelligence_thesis._THESIS_CACHE_TTL


def test_thesis_cache_set_then_get_roundtrip():
    cache = intelligence_thesis._thesis_cache
    cache.clear()
    payload = {"direction": "BULLISH", "score": 42}
    cache.set(intelligence_thesis._THESIS_CACHE_KEY, payload)
    assert cache.get(intelligence_thesis._THESIS_CACHE_KEY) == payload


def test_thesis_cache_returns_none_when_missing():
    cache = intelligence_thesis._thesis_cache
    cache.clear()
    assert cache.get(intelligence_thesis._THESIS_CACHE_KEY) is None


def test_thesis_cache_expires_after_ttl():
    short_cache = TTLCache(ttl=0.05, max_size=1)
    short_cache.set("k", {"v": 1})
    assert short_cache.get("k") == {"v": 1}
    time.sleep(0.1)
    assert short_cache.get("k") is None


def test_thesis_cache_concurrent_set_and_get_is_safe():
    """The whole point of the change — concurrent writers + readers must not
    raise and must end in a self-consistent state."""
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
                # Either None (window before first write) or a dict — never a
                # half-mutated tuple/exception.
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
    intelligence_thesis._thesis_cache.clear()
