"""Fail-closed TTL caching for current SEC-dependent financial results.

Generation checks are intentionally uncached. A cached value is usable only
for the same engine and a source generation verified on this request. Work
that overlaps a head/rollup change is retried once, never published under the
new generation. Unrelated market/news caches keep their existing policies.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, TypeVar

from store.financial_generation import get_financial_generation
from utils.ttl_cache import TTLCache

T = TypeVar("T")


class FinancialCacheUnavailable(RuntimeError):
    """Current financial source identity could not be established."""


@dataclass(frozen=True)
class _Entry:
    engine: Any
    value: Any


def read_financial_generation(engine: Any) -> str:
    """Read a fresh generation; never map errors to an old/default token."""
    try:
        with engine.connect() as conn:
            generation = get_financial_generation(conn)
        if not isinstance(generation, str) or not generation:
            raise ValueError("Missing financial generation")
        return generation
    except Exception as exc:
        raise FinancialCacheUnavailable("Financial source version unavailable") from exc


def get_cached_financial(
    cache: TTLCache,
    key: str,
    engine: Any,
    build: Callable[[], T],
    *,
    cache_if: Callable[[T], bool] | None = None,
) -> T:
    """Get/build one result verified against a stable financial generation.

    Store engine identity with the entry rather than using id(engine), which
    can be reused after collection. This also isolates identical generation
    hashes from different databases without exposing connection credentials.
    """
    for _attempt in range(2):
        generation = read_financial_generation(engine)
        cache_key = f"{generation}|{key}"
        entry = cache.get(cache_key)
        hit = isinstance(entry, _Entry) and entry.engine is engine
        value = entry.value if hit else build()
        if read_financial_generation(engine) != generation:
            continue
        if not hit and (cache_if is None or cache_if(value)):
            cache.set(cache_key, _Entry(engine, value))
        return value
    raise FinancialCacheUnavailable("Financial source changed during computation")
