"""Thread-safe TTL cache for replacing unprotected global cache dicts.

Usage:
    from utils.ttl_cache import TTLCache

    _cache = TTLCache(ttl=300, max_size=200)
    _cache.set("key", value)
    hit = _cache.get("key")  # returns None if expired or missing
    _cache.clear()
"""

from __future__ import annotations

import threading
import time
from typing import Any


class TTLCache:
    """Thread-safe in-memory cache with per-entry TTL and bounded size.

    Parameters:
        ttl: Time-to-live in seconds for each entry (default 300).
        max_size: Maximum number of entries before oldest are evicted (default 1000).
    """

    __slots__ = ("_ttl", "_max_size", "_lock", "_store")

    def __init__(self, ttl: float = 300.0, max_size: int = 1000) -> None:
        if ttl <= 0:
            raise ValueError("ttl must be positive")
        if max_size <= 0:
            raise ValueError("max_size must be positive")
        self._ttl = ttl
        self._max_size = max_size
        self._lock = threading.Lock()
        # store: key -> (expires_at, value)
        self._store: dict[str, tuple[float, Any]] = {}

    # ── Public API ────────────────────────────────────────────────────

    def get(self, key: str) -> Any | None:
        """Return cached value if present and not expired, else None.

        Lazily evicts the entry if it has expired.
        """
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            expires_at, value = entry
            if time.monotonic() > expires_at:
                del self._store[key]
                return None
            return value

    def set(self, key: str, value: Any) -> None:
        """Store a value with the configured TTL.

        If the cache exceeds max_size after insertion, expired entries
        are purged first; if still over limit, the oldest entries are
        removed until size is within bounds.
        """
        now = time.monotonic()
        expires_at = now + self._ttl
        with self._lock:
            self._store[key] = (expires_at, value)
            if len(self._store) > self._max_size:
                self._evict_locked(now)

    def clear(self) -> None:
        """Remove all entries."""
        with self._lock:
            self._store.clear()

    def __len__(self) -> int:
        """Return the number of entries (including possibly expired ones)."""
        with self._lock:
            return len(self._store)

    def __contains__(self, key: str) -> bool:
        """Check if key is present and not expired."""
        return self.get(key) is not None

    # ── Internal ──────────────────────────────────────────────────────

    def _evict_locked(self, now: float) -> None:
        """Remove expired entries, then oldest if still over max_size.

        Must be called while holding self._lock.
        """
        # Phase 1: remove expired
        expired_keys = [k for k, (exp, _) in self._store.items() if now > exp]
        for k in expired_keys:
            del self._store[k]

        # Phase 2: if still over limit, drop oldest by expiry time
        if len(self._store) > self._max_size:
            sorted_keys = sorted(self._store, key=lambda k: self._store[k][0])
            to_remove = len(self._store) - self._max_size
            for k in sorted_keys[:to_remove]:
                del self._store[k]
