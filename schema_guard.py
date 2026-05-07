"""Process-level gate for runtime schema-ensure operations.

Many GRID classes call `self._ensure_tables()` or `self._ensure_columns()`
in `__init__`. Under concurrent construction (per-request handlers,
per-cycle workers, multi-thread API), each construction races for the
same `ACCESS EXCLUSIVE` table lock, queues, and exhausts the connection
pool. The fix is to run the schema check at most once per process.

This module provides a tiny helper so the pattern is consistent across
the codebase. Each call site picks a unique key (typically the class
name) and the runner only fires the function the first time that key is
seen in this process.

Usage:
    from db.schema_guard import ensure_once

    class MyEngine:
        def __init__(self, engine):
            self.engine = engine
            ensure_once("MyEngine.tables", self._ensure_tables)

The helper is thread-safe via a global lock with double-checked
visibility, so the first concurrent constructions on a fresh process
race once and then everyone else hits the fast path.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any

_done: set[str] = set()
_lock = threading.Lock()


def ensure_once(key: str, fn: Callable[[], Any]) -> None:
    """Run `fn` exactly once per process for the given key.

    Subsequent calls with the same key are no-ops and return immediately
    after a single set-membership check (the common case).
    """
    if key in _done:
        return
    with _lock:
        if key in _done:
            return
        fn()
        _done.add(key)


def reset_for_tests() -> None:
    """Forget all keys. Only for use in tests that need a fresh state."""
    with _lock:
        _done.clear()
