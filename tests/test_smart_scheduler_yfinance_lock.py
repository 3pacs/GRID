"""Tests for the GRID-YF-CLOSE-REPAIR-20260926 SmartScheduler changes:

- Registry ``kwargs`` values may be callables, resolved at call time (needed
  for the yfinance entry's rolling-window start_date — fix #2, "timeout
  honestly sized").
- A puller method that accepts ``should_continue`` gets one auto-wired to a
  deadline inside the call's own ``timeout_s``, so it can stop itself
  between items instead of being abandoned as an orphan.
- A puller reporting ``{"status": "SKIPPED", ...}`` (e.g.
  YFinancePuller.pull_all's single-flight lock) must not be recorded as a
  successful run, and must NOT advance last_pull_at.
"""

from __future__ import annotations

import sys
import threading
import types
from unittest.mock import MagicMock

from ingestion.smart_scheduler import SmartScheduler


def _install_fake_module(mod_name: str, cls: type) -> None:
    mod = types.ModuleType(mod_name)
    setattr(mod, cls.__name__, cls)
    sys.modules[mod_name] = mod


def _full_scheduler() -> SmartScheduler:
    """A SmartScheduler with the attributes _run_puller needs, without
    running __init__ (which would hit the database)."""
    sched = SmartScheduler.__new__(SmartScheduler)
    sched.engine = MagicMock()
    sched._thread_semaphore = threading.Semaphore(SmartScheduler.MAX_CONCURRENT_THREADS)
    sched._active_threads = set()
    sched._threads_lock = threading.Lock()
    sched._orphan_thread_count = 0
    sched._state = {}
    return sched


def test_should_continue_is_auto_wired_for_methods_that_accept_it() -> None:
    captured: dict = {}

    class _Puller:
        def __init__(self, db_engine):
            self.db_engine = db_engine

        def pull_all(self, ticker_list=None, start_date=None, should_continue=None):
            captured["should_continue"] = should_continue
            captured["initial_value"] = should_continue() if should_continue else None
            return ["ok"]

    _install_fake_module("grid_test_fake_yf_mod_should_continue", _Puller)
    sched = _full_scheduler()
    sched._update_last_pull = MagicMock()

    result = sched._run_puller({
        "name": "fake_yf",
        "mod": "grid_test_fake_yf_mod_should_continue",
        "cls": "_Puller",
        "method": "pull_all",
        "timeout_s": 60,
    })

    assert result["status"] == "SUCCESS"
    assert callable(captured["should_continue"])
    # Right after wiring, the deadline hasn't passed — must read True.
    assert captured["initial_value"] is True
    sched._update_last_pull.assert_called_once_with("fake_yf")


def test_should_continue_is_not_overridden_when_registry_supplies_one() -> None:
    """A registry entry that already sets should_continue in its own
    kwargs must not be clobbered by the auto-wiring."""
    captured: dict = {}
    sentinel = lambda: False  # noqa: E731

    class _Puller:
        def __init__(self, db_engine):
            pass

        def pull_all(self, should_continue=None):
            captured["should_continue"] = should_continue
            return "ok"

    _install_fake_module("grid_test_fake_yf_mod_explicit_sc", _Puller)
    sched = _full_scheduler()
    sched._update_last_pull = MagicMock()

    sched._run_puller({
        "name": "fake_yf2",
        "mod": "grid_test_fake_yf_mod_explicit_sc",
        "cls": "_Puller",
        "method": "pull_all",
        "timeout_s": 60,
        "kwargs": {"should_continue": sentinel},
    })

    assert captured["should_continue"] is sentinel


def test_callable_kwargs_are_resolved_at_call_time() -> None:
    captured: dict = {}

    class _Puller:
        def __init__(self, db_engine):
            pass

        def pull_all(self, start_date=None):
            captured["start_date"] = start_date
            return "done"

    _install_fake_module("grid_test_fake_yf_mod_callable_kwargs", _Puller)
    sched = _full_scheduler()
    sched._update_last_pull = MagicMock()

    result = sched._run_puller({
        "name": "fake_yf3",
        "mod": "grid_test_fake_yf_mod_callable_kwargs",
        "cls": "_Puller",
        "method": "pull_all",
        "timeout_s": 60,
        "kwargs": {"start_date": lambda: "2026-09-01"},
    })

    assert result["status"] == "SUCCESS"
    assert captured["start_date"] == "2026-09-01"


def test_skipped_puller_result_is_not_treated_as_success() -> None:
    """A puller's own {"status": "SKIPPED"} (e.g. yfinance's single-flight
    lock finding a previous run active) must surface as SKIPPED and must
    NOT advance source_catalog.last_pull_at."""

    class _Puller:
        def __init__(self, db_engine):
            pass

        def pull_all(self):
            return {"status": "SKIPPED", "skipped_reason": "already running"}

    _install_fake_module("grid_test_fake_yf_mod_skip", _Puller)
    sched = _full_scheduler()
    sched._update_last_pull = MagicMock()

    result = sched._run_puller({
        "name": "fake_yf4",
        "mod": "grid_test_fake_yf_mod_skip",
        "cls": "_Puller",
        "method": "pull_all",
        "timeout_s": 60,
    })

    assert result["status"] == "SKIPPED"
    assert result["reason"] == "already running"
    sched._update_last_pull.assert_not_called()


def test_yfinance_registry_entry_uses_incremental_start_and_honest_timeout() -> None:
    """Fix #2 ("timeout honestly sized"): the scheduled yfinance entry must
    not default to pull_all's own full-history start_date (1990-01-01) on
    every 4-hourly tick — that full 70-ticker download is what regularly
    overran the old 120s timeout_s. It must supply its own recent-window
    start_date and a larger, more realistic timeout_s."""
    from ingestion.smart_scheduler import PULLER_REGISTRY, _yfinance_incremental_start

    entry = next(p for p in PULLER_REGISTRY if p["name"] == "yfinance")
    assert entry["timeout_s"] > 120
    assert callable(entry["kwargs"]["start_date"])

    resolved = entry["kwargs"]["start_date"]()
    assert resolved != "1990-01-01"
    # Must look like an ISO date (YYYY-MM-DD), not a lambda repr.
    assert len(resolved) == 10 and resolved.count("-") == 2
    assert resolved == _yfinance_incremental_start()
