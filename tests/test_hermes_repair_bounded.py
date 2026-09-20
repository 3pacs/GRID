"""Tests for the bounded-repair fix (fable-hermes-repair-bound, 2026-09-19).

Background — see docs/handoffs/2026-09-19/fable-hermes-repair-bound.md for
the full trace. In short: a Hermes self-diagnostics REPULL command used to
call ``_retry_source(attempt=1)`` with no ``start_date`` override, which for
``pull_all(start_date=...)`` pullers (YFinancePuller) fell through to the
module default ``start_date="1990-01-01"`` — a full-history backfill
disguised as a routine repair. Production cycle 6300 stayed on that one
diagnostics step for 71 minutes (the source was not actually stale — every
ticker already had yesterday's data; the "fix" reprocessed 1.3-2.3M
existing rows per series to insert 0 new rows) and starved every step
scheduled after it. Because the call never returned, ``last_pull_at`` was
never advanced and the cooldown never engaged, so the next diagnostics
cycle repeated the same full-history repull, possibly while the previous
orphaned worker was still running.

This file covers the fix:
  1. Every repair attempt requests a bounded recent window, never full
     history (scripts/hermes_fixers.py::REPAIR_LOOKBACK_DAYS).
  2. A cooperative REPAIR_BUDGET_SECONDS deadline stops a slow/stuck pull
     between tickers, persists the unattempted remainder to
     ``state.repair_backlog``, and lets the next attempt resume from it.
  3. Repeated no-op (zero-insert) repairs still complete within budget,
     advance ``last_pull_at``, and leave the source cooled down.
  4. Two ``_retry_source`` calls for the same source never overlap; an
     abandoned worker's ``should_continue`` reflects being superseded.
  5. The self-diagnostics ``run_cycle`` step (``_run_diagnostics_step``) has
     its own timeout, so a hang anywhere inside it cannot starve due
     maintenance scheduled after it.
  6. The budget constants nest correctly inside the step timeouts that
     wrap them.

No network/DB — fake engines/pullers and monkeypatched
``_resolve_puller``/``run_self_diagnostics`` throughout.
"""
from __future__ import annotations

import threading
import time
from datetime import date, timedelta
from typing import Any
from unittest.mock import MagicMock

import pytest

from scripts import hermes_fixers as hf
from scripts import hermes_operator as ho
from scripts.hermes_health import OperatorState

# ─── shared fakes ────────────────────────────────────────────────────────


class _Result:
    def __init__(self, rows=None):
        self._rows = rows or []

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _Ctx:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self.conn

    def __exit__(self, *exc):
        return False


class _RecordingConn:
    """Fake connection that records every execute() call's SQL text."""

    def __init__(self):
        self.calls: list[str] = []

    def execute(self, stmt, params=None):
        sql = " ".join(str(stmt).split())
        self.calls.append(sql)
        return _Result()

    def commit(self):
        pass


def _fake_engine(conn: _RecordingConn) -> Any:
    return type("_Engine", (), {
        "begin": lambda self: _Ctx(conn),
        "connect": lambda self: _Ctx(conn),
    })()


@pytest.fixture(autouse=True)
def _clean_in_flight_registry():
    """_REPAIRS_IN_FLIGHT is module-level global state shared with
    production code — never leak an entry from one test into the next."""
    hf._REPAIRS_IN_FLIGHT.clear()
    yield
    hf._REPAIRS_IN_FLIGHT.clear()


def _patch_resolve_puller(monkeypatch, puller: Any, method: str = "pull_all", kwargs: dict | None = None) -> None:
    monkeypatch.setattr(
        hf, "_resolve_puller", lambda name, engine: (puller, method, dict(kwargs or {}))
    )


# ─── 1. Bounded window — never full history ────────────────────────────


class TestBoundedRepairWindow:
    def test_attempt_1_never_pulls_earlier_than_seven_days_back(self, monkeypatch) -> None:
        captured: dict[str, Any] = {}

        class FakePuller:
            def pull_all(self, ticker_list=None, start_date=None, should_continue=None):
                captured["start_date"] = start_date
                return {"status": "SUCCESS", "stopped_by_budget": False,
                        "results": [], "tickers_not_attempted": []}

        _patch_resolve_puller(monkeypatch, FakePuller())
        result = hf._retry_source("boundedsrc", MagicMock(), attempt=1)

        expected = (date.today() - timedelta(days=hf.REPAIR_LOOKBACK_DAYS)).isoformat()
        assert captured["start_date"] == expected
        assert result["status"] == "SUCCESS"

    def test_attempt_3_widens_to_twentyone_days_still_bounded(self, monkeypatch) -> None:
        captured: dict[str, Any] = {}

        class FakePuller:
            def pull_all(self, ticker_list=None, start_date=None, should_continue=None):
                captured["start_date"] = start_date
                return {"status": "SUCCESS", "stopped_by_budget": False,
                        "results": [], "tickers_not_attempted": []}

        _patch_resolve_puller(monkeypatch, FakePuller())
        hf._retry_source("boundedsrc", MagicMock(), attempt=3)

        expected = (date.today() - timedelta(days=hf.REPAIR_LOOKBACK_DAYS * 3)).isoformat()
        assert captured["start_date"] == expected
        # Never anywhere close to a full-history backfill.
        assert captured["start_date"] != "1990-01-01"

    def test_days_back_pullers_keep_pre_existing_widen_on_retry_behavior(self, monkeypatch) -> None:
        """Pullers keyed by days_back (not start_date) are untouched by
        this fix — only the full-history start_date defect is bounded."""
        captured: dict[str, Any] = {}

        class FakePuller:
            def run(self, days_back=None):
                captured["days_back"] = days_back
                return {"status": "ok"}

        _patch_resolve_puller(monkeypatch, FakePuller(), method="run", kwargs={"days_back": 10})
        hf._retry_source("daysbacksrc", MagicMock(), attempt=2)
        assert captured["days_back"] == 10 * (2 + 1)


# ─── 2. Cooperative budget + resumable backlog ─────────────────────────


class _SlowTickerPuller:
    """Mimics YFinancePuller.pull_all's should_continue contract without
    any network access: sleeps a bit per ticker and checks the budget
    between tickers."""

    def __init__(self, tickers: list[str], sleep_s: float) -> None:
        self.tickers = tickers
        self.sleep_s = sleep_s
        self.attempted: list[str] = []

    def pull_all(self, ticker_list=None, start_date=None, should_continue=None):
        tickers = ticker_list if ticker_list is not None else self.tickers
        attempted = []
        for t in tickers:
            if should_continue is not None and not should_continue():
                break
            time.sleep(self.sleep_s)
            attempted.append(t)
        self.attempted.extend(attempted)
        stopped = len(attempted) < len(tickers)
        return {
            "status": "PARTIAL" if stopped else "SUCCESS",
            "stopped_by_budget": stopped,
            "results": [{"ticker": t, "rows_inserted": 0, "status": "SUCCESS"} for t in attempted],
            "tickers_not_attempted": list(tickers[len(attempted):]),
        }


class TestCooperativeBudgetAndBacklog:
    def test_slow_provider_stops_at_budget_and_persists_backlog(self) -> None:
        tickers = [f"T{i}" for i in range(20)]
        puller = _SlowTickerPuller(tickers, sleep_s=0.3)
        state = OperatorState()
        engine = MagicMock()

        import scripts.hermes_fixers as hf_mod
        orig_resolve = hf_mod._resolve_puller
        hf_mod._resolve_puller = lambda name, engine: (puller, "pull_all", {})
        try:
            deadline = time.monotonic() + 1.0
            started = time.monotonic()
            result = hf._retry_source(
                "slowsrc", engine, attempt=1, state=state,
                should_continue=lambda: time.monotonic() < deadline,
            )
            elapsed = time.monotonic() - started
        finally:
            hf_mod._resolve_puller = orig_resolve

        assert elapsed < 1.5, f"took {elapsed:.2f}s — should stop near the 1s budget"
        assert result["stopped_by_budget"] is True
        assert state.repair_backlog.get("slowsrc"), "remainder must be persisted"
        assert set(state.repair_backlog["slowsrc"]) <= set(tickers)
        assert len(state.repair_backlog["slowsrc"]) > 0

    def test_next_attempt_resumes_from_backlog_instead_of_restarting(self) -> None:
        tickers = [f"T{i}" for i in range(6)]
        puller = _SlowTickerPuller(tickers, sleep_s=0.0)
        state = OperatorState()
        # Pretend a prior attempt left a backlog.
        state.repair_backlog["resumesrc"] = ["T4", "T5"]
        engine = MagicMock()

        import scripts.hermes_fixers as hf_mod
        orig_resolve = hf_mod._resolve_puller
        hf_mod._resolve_puller = lambda name, engine: (puller, "pull_all", {})
        try:
            hf._retry_source("resumesrc", engine, attempt=1, state=state)
        finally:
            hf_mod._resolve_puller = orig_resolve

        assert puller.attempted == ["T4", "T5"], (
            "resumed pull must use the persisted backlog, not the full ticker list"
        )
        # Completed cleanly (no remainder) — backlog cleared.
        assert "resumesrc" not in state.repair_backlog

    def test_repull_command_records_failed_attempt_and_engages_cooldown_on_budget_stop(self) -> None:
        tickers = [f"T{i}" for i in range(20)]
        puller = _SlowTickerPuller(tickers, sleep_s=0.3)
        state = OperatorState()
        engine = MagicMock()

        import scripts.hermes_fixers as hf_mod
        orig_resolve = hf_mod._resolve_puller
        hf_mod._resolve_puller = lambda name, engine: (puller, "pull_all", {})
        try:
            deadline = time.monotonic() + 1.0
            result = hf._execute_hermes_repair_command(
                "REPULL:slowsrc2", engine, {}, state,
                should_continue=lambda: time.monotonic() < deadline,
            )
        finally:
            hf_mod._resolve_puller = orig_resolve

        assert result["status"] == "partial"
        assert result["stopped_by_budget"] is True
        assert not state.cooldowns.can_retry("slowsrc2"), (
            "a budget-stopped attempt must be recorded as a failed attempt "
            "so the cooldown engages"
        )


# ─── 3. Repeated zero-insert results ───────────────────────────────────


class TestRepeatedZeroInsertResultsStayBounded:
    def test_three_consecutive_zero_insert_repairs_update_freshness_and_cool_down(self) -> None:
        tickers = ["A", "B", "C"]

        class ZeroInsertPuller:
            def pull_all(self, ticker_list=None, start_date=None, should_continue=None):
                use = ticker_list if ticker_list is not None else tickers
                return {
                    "status": "SUCCESS", "stopped_by_budget": False,
                    "results": [{"ticker": t, "rows_inserted": 0, "status": "SUCCESS"} for t in use],
                    "tickers_not_attempted": [],
                }

        conn = _RecordingConn()
        engine = _fake_engine(conn)
        state = OperatorState()
        puller = ZeroInsertPuller()

        import scripts.hermes_fixers as hf_mod
        orig_resolve = hf_mod._resolve_puller
        hf_mod._resolve_puller = lambda name, engine: (puller, "pull_all", {})
        try:
            for _ in range(3):
                result = hf._retry_source("zerosrc", engine, attempt=1, state=state)
                assert result["status"] == "SUCCESS"
                assert result["stopped_by_budget"] is False
                state.cooldowns.record_attempt("zerosrc", success=True)
                assert not state.cooldowns.can_retry("zerosrc"), (
                    "cooldown must engage immediately after every attempt, "
                    "success or not, so a zero-insert source is not "
                    "re-selected on the very next cycle"
                )
        finally:
            hf_mod._resolve_puller = orig_resolve

        update_calls = [c for c in conn.calls if "UPDATE source_catalog" in c and "last_pull_at" in c]
        assert len(update_calls) == 3, "last_pull_at must be advanced on every completed (non-budget-stopped) repair"
        assert "zerosrc" not in state.repair_backlog


# ─── 4. No overlapping repair workers ──────────────────────────────────


class TestNoOverlappingRepairWorkers:
    def test_second_call_is_skipped_while_first_is_in_flight_then_cleans_up(self) -> None:
        started_event = threading.Event()
        release_event = threading.Event()
        outcome: dict[str, Any] = {}

        class BlockingPuller:
            def pull_all(self, ticker_list=None, start_date=None, should_continue=None):
                started_event.set()
                release_event.wait(5)
                return {"status": "SUCCESS", "stopped_by_budget": False,
                        "results": [], "tickers_not_attempted": []}

        engine = MagicMock()
        state = OperatorState()
        puller = BlockingPuller()

        import scripts.hermes_fixers as hf_mod
        orig_resolve = hf_mod._resolve_puller
        hf_mod._resolve_puller = lambda name, engine: (puller, "pull_all", {})

        def _worker() -> None:
            outcome["first"] = hf._retry_source("blocksrc", engine, attempt=1, state=state)

        t = threading.Thread(target=_worker, daemon=True)
        try:
            t.start()
            assert started_event.wait(2), "worker never started"

            second = hf._retry_source("blocksrc", engine, attempt=1, state=state)
            assert second["status"] == "skipped"
            assert second["reason"] == "in_flight"

            release_event.set()
            t.join(5)

            assert outcome["first"]["status"] == "SUCCESS"
            assert "blocksrc" not in hf._REPAIRS_IN_FLIGHT, "entry must be released once the worker finishes"

            third = hf._retry_source("blocksrc", engine, attempt=1, state=state)
            assert third["status"] != "skipped", "a later call must not be blocked forever"
        finally:
            hf_mod._resolve_puller = orig_resolve

    def test_should_continue_reflects_a_superseded_token(self, monkeypatch) -> None:
        """Unit-level test of the token check inside _retry_source's
        combined should_continue closure — the mechanism that lets an
        abandoned worker (one _run_with_timeout could not kill) notice it
        has been superseded and stop at its next ticker boundary."""
        captured: dict[str, Any] = {}

        class RecordingPuller:
            def pull_all(self, ticker_list=None, start_date=None, should_continue=None):
                captured["should_continue"] = should_continue
                # Simulate this attempt's slot being taken over by a
                # fresher one (e.g. after the outer step timeout abandoned
                # this worker and a later cycle started a new attempt).
                with hf._REPAIRS_LOCK:
                    hf._REPAIRS_IN_FLIGHT["tokensrc"] = {
                        "started": time.monotonic(),
                        "token": hf._next_repair_token(),
                        "thread": threading.get_ident(),
                    }
                still_ok = should_continue() if should_continue else True
                return {"status": "PARTIAL", "stopped_by_budget": not still_ok,
                        "results": [], "tickers_not_attempted": []}

        monkeypatch.setattr(hf, "_resolve_puller", lambda name, engine: (RecordingPuller(), "pull_all", {}))
        try:
            hf._retry_source("tokensrc", MagicMock(), attempt=1)
            assert captured["should_continue"]() is False, (
                "should_continue must go False once this attempt's token "
                "is no longer the current in-flight entry for the source"
            )
        finally:
            hf._REPAIRS_IN_FLIGHT.pop("tokensrc", None)


# ─── 5. Diagnostics step has its own timeout — due maintenance not starved ──


class TestDiagnosticsStepHasItsOwnTimeout:
    def test_step_returns_quickly_when_run_self_diagnostics_blocks(self, monkeypatch) -> None:
        def _blocking_diagnostics(engine, hermes_ok, health, state, dry_run=False):
            threading.Event().wait(3)
            return {"skipped": "never reached — orphaned by the timeout"}

        monkeypatch.setattr(ho, "run_self_diagnostics", _blocking_diagnostics)
        monkeypatch.setattr(ho, "DIAGNOSTICS_TIMEOUT_SECONDS", 1)

        state = OperatorState()
        state.cycle_count = 6  # every-6th-cycle gate
        cycle_result: dict[str, Any] = {}

        started = time.monotonic()
        ho._run_diagnostics_step(MagicMock(), True, {"db": {"healthy": True}}, state, False, cycle_result)
        elapsed = time.monotonic() - started

        assert elapsed < 3.0, (
            f"_run_diagnostics_step took {elapsed:.1f}s — a hung "
            "run_self_diagnostics must not block the rest of run_cycle"
        )
        assert cycle_result["diagnostics"] == {"timeout": True}

    def test_step_is_a_noop_off_the_every_sixth_cycle_schedule(self) -> None:
        state = OperatorState()
        state.cycle_count = 7  # not divisible by 6
        cycle_result: dict[str, Any] = {}
        ho._run_diagnostics_step(MagicMock(), True, {"db": {"healthy": True}}, state, False, cycle_result)
        assert cycle_result == {}


# ─── 6. Budget constants pin correctly inside the timeouts wrapping them ──


class TestBudgetConstantsPinInsideStepTimeouts:
    def test_repair_budget_under_diagnose_pulls_timeout(self) -> None:
        assert hf.REPAIR_BUDGET_SECONDS < ho.DIAGNOSE_PULLS_TIMEOUT_SECONDS

    def test_repair_budget_under_diagnostics_step_timeout(self) -> None:
        assert hf.REPAIR_BUDGET_SECONDS < ho.DIAGNOSTICS_TIMEOUT_SECONDS

    def test_diagnostics_step_timeout_under_cycle_timeout(self) -> None:
        assert ho.DIAGNOSTICS_TIMEOUT_SECONDS < ho.CYCLE_TIMEOUT_SECONDS

    def test_repair_lookback_days_is_small_and_positive(self) -> None:
        assert 0 < hf.REPAIR_LOOKBACK_DAYS <= 30, (
            "a repair window anywhere near backfill scale defeats the fix"
        )
