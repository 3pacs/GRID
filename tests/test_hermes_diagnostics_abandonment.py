"""Tests for action-loop abandonment (fable-hermes-repair-bound follow-up,
controller-directed 2026-09-20).

Background — see docs/handoffs/2026-09-19/fable-hermes-repair-bound.md,
"Action-loop abandonment". Production cycle 6300's diagnostics step
(``run_self_diagnostics``) was abandoned by the cycle watchdog at
23:27:12Z ("Cycle 6300 TIMED OUT after 4500s (stuck on: diagnostics)"),
but the orphaned worker thread that ``_run_with_timeout`` hands control
back from (without killing it — see its own docstring) kept walking the
LLM's already-parsed ``ACTION:`` list: it logged "Hermes action:
re-pulling yfinance_options" at 00:10:04Z (28 minutes into the NEXT
cycle, 6301) and "re-pulling TradingView" at 00:38:00Z. The existing
REPAIR_BUDGET_SECONDS deadline and per-ticker ``should_continue``
plumbing bound a single REPULL once it has *started*, but nothing
previously stopped an abandoned worker from *starting the next* action in
its list — that gap is what this test covers.

The fix (scripts/hermes_fixers.py): a module-level ``_DIAGNOSTICS_TOKEN``
counter, bumped by ``_next_diagnostics_token()`` every time
``run_self_diagnostics`` or ``diagnose_and_fix_pulls`` starts.
Each call captures the token it saw at entry and, before starting every
remaining action/source, calls ``_diagnostics_abandoned(token, deadline)``
— true if the shared REPAIR_BUDGET_SECONDS deadline has passed OR a
newer diagnostics start has bumped the counter past this call's token.
On abandonment the whole remaining list is skipped in one log line and
the worker returns a partial result (``skipped_actions``) without
starting anything else.
"""
from __future__ import annotations

import io
import threading
import time
from typing import Any

import pytest
from loguru import logger as loguru_logger

from scripts import hermes_fixers as hf
from scripts import hermes_operator as ho
from scripts.hermes_health import OperatorState


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


class _FakeLLMClient:
    def __init__(self, response: str):
        self._response = response

    def chat(self, *args: Any, **kwargs: Any) -> str:
        return self._response


_HEALTH: dict[str, Any] = {
    "db": {
        "healthy": True,
        "stale_sources": [],
        "failed_pulls_24h": 0,
        "raw_series_count": 0,
        "latest_pull": None,
    },
}


@pytest.fixture(autouse=True)
def _clean_module_state():
    """_REPAIRS_IN_FLIGHT and _DIAGNOSTICS_TOKEN are module-level globals
    shared with production code — never leak state between tests."""
    hf._REPAIRS_IN_FLIGHT.clear()
    yield
    hf._REPAIRS_IN_FLIGHT.clear()


def _patch_llm(monkeypatch, response: str) -> None:
    monkeypatch.setattr(
        "llm.router.get_llm", lambda *a, **kw: _FakeLLMClient(response)
    )


class TestActionLoopAbandonmentBySupersession:
    def test_abandoned_worker_skips_remaining_actions_when_superseded(self, monkeypatch) -> None:
        """Reproduces the 6300→6301 shape: action 1's provider call is
        already running when this diagnostics worker is superseded; it
        must not start action 2 or 3, must publish nothing for action 1
        (already covered by _retry_source's own Check 2c, re-asserted
        here at the loop level), and must log a single combined skip
        line."""
        block_event = threading.Event()
        started_event = threading.Event()
        call2 = {"called": False}
        call3 = {"called": False}

        class BlockingSrc1Puller:
            def pull_all(self, ticker_list=None, start_date=None, should_continue=None):
                started_event.set()
                block_event.wait(5)
                return {
                    "status": "SUCCESS", "stopped_by_budget": False,
                    "results": [{"ticker": "T0", "rows_inserted": 1, "status": "SUCCESS",
                                  "outcome": "inserted", "errors": []}],
                    "tickers_not_attempted": [],
                    "counts": {"inserted": 1, "duplicate_only": 0, "no_data": 0, "error": 0, "unattempted": 0},
                }

        class NeverCalledSrc2Puller:
            def pull_all(self, ticker_list=None, start_date=None, should_continue=None):
                call2["called"] = True
                return {"status": "SUCCESS", "stopped_by_budget": False, "results": [], "tickers_not_attempted": []}

        class NeverCalledSrc3Puller:
            def pull_all(self, ticker_list=None, start_date=None, should_continue=None):
                call3["called"] = True
                return {"status": "SUCCESS", "stopped_by_budget": False, "results": [], "tickers_not_attempted": []}

        pullers = {
            "src1": BlockingSrc1Puller(),
            "src2": NeverCalledSrc2Puller(),
            "src3": NeverCalledSrc3Puller(),
        }

        def _resolve(name: str, engine: Any):
            return pullers[name], "pull_all", {}

        monkeypatch.setattr(hf, "_resolve_puller", _resolve)
        monkeypatch.setattr(hf, "REPAIR_BUDGET_SECONDS", 1)
        _patch_llm(
            monkeypatch,
            "SEVERITY: WARNING\n"
            "ACTION: REPULL:src1\n"
            "ACTION: REPULL:src2\n"
            "ACTION: REPULL:src3\n"
            "SUMMARY: repair three sources\n",
        )

        conn = _RecordingConn()
        engine = _fake_engine(conn)
        state = OperatorState()

        log_sink = io.StringIO()
        sink_id = loguru_logger.add(log_sink, level="WARNING")
        try:
            # Run the whole diagnostics step under a real, short outer
            # timeout — exactly how scripts/hermes_operator.py's
            # _run_diagnostics_step wraps run_self_diagnostics with
            # _run_with_timeout(..., DIAGNOSTICS_TIMEOUT_SECONDS). The
            # worker keeps running as an orphan after the outer call
            # gives up (see _run_with_timeout's docstring).
            result, ok = ho._run_with_timeout(
                "diagnostics_abandon_probe",
                lambda: hf.run_self_diagnostics(engine, True, _HEALTH, state),
                1,
                state,
            )
            assert ok is False
            assert result is None
            assert started_event.wait(2), "worker never reached the blocking action"

            # action 1's provider call is still in flight.
            assert hf._REPAIRS_IN_FLIGHT.get("src1") is not None, (
                "in-flight registry must still hold action 1's source while "
                "its provider call is running"
            )

            # Simulate the next diagnostics start elsewhere (e.g. the next
            # cycle's diagnostics step, or a fresh diagnose_and_fix_pulls
            # call) — this is what actually happened between 23:27:12Z and
            # 00:10:04Z in production.
            hf._next_diagnostics_token()

            # Now let action 1's blocked provider call return on its own.
            block_event.set()

            deadline = time.monotonic() + 5
            while (
                time.monotonic() < deadline
                and "remaining action(s) skipped" not in log_sink.getvalue()
            ):
                time.sleep(0.02)

            log_text = log_sink.getvalue()
            assert "diagnostics actions: 2 remaining action(s) skipped" in log_text
            assert "worker superseded" in log_text

            # action 2 and action 3 must never have been started.
            assert call2["called"] is False, "action 2 must not run once abandoned"
            assert call3["called"] is False, "action 3 must not run once abandoned"

            # action 1 itself was abandoned by _retry_source's own Check 2c
            # (its provider call finished, but the token had already been
            # superseded by the time control returned) — nothing published.
            update_calls = [c for c in conn.calls if "UPDATE source_catalog" in c]
            assert not update_calls, "an abandoned worker must not touch source_catalog"
            assert state.repair_backlog == {}
            assert state.repair_last_check == {}
            assert state.repair_uncovered == {}
            assert state.cooldowns.get_status("src1") is None
            assert state.cooldowns.get_status("src2") is None
            assert state.cooldowns.get_status("src3") is None

            # The in-flight entry for src1 is removed once the worker
            # actually exits (its own finally), same guarantee as
            # TestAbandonmentUnderRealOuterTimeout in
            # test_hermes_repair_bounded.py.
            deadline2 = time.monotonic() + 5
            while "src1" in hf._REPAIRS_IN_FLIGHT and time.monotonic() < deadline2:
                time.sleep(0.02)
            assert "src1" not in hf._REPAIRS_IN_FLIGHT
        finally:
            loguru_logger.remove(sink_id)
            hf._REPAIRS_IN_FLIGHT.pop("src1", None)


class TestActionLoopAbandonmentByDeadlineAlone:
    def test_deadline_alone_skips_remaining_actions_without_a_new_token(self, monkeypatch) -> None:
        """No competing diagnostics worker exists in this scenario — the
        shared REPAIR_BUDGET_SECONDS deadline alone (patched to expire
        almost immediately) is what causes the remaining actions to be
        skipped."""
        call2 = {"called": False}
        call3 = {"called": False}

        class SlowSrc1Puller:
            def pull_all(self, ticker_list=None, start_date=None, should_continue=None):
                # Long enough that the patched budget has certainly
                # expired by the time this call returns and the loop
                # re-checks abandonment before action 2.
                time.sleep(0.3)
                return {
                    "status": "SUCCESS", "stopped_by_budget": False,
                    "results": [{"ticker": "T0", "rows_inserted": 1, "status": "SUCCESS",
                                  "outcome": "inserted", "errors": []}],
                    "tickers_not_attempted": [],
                    "counts": {"inserted": 1, "duplicate_only": 0, "no_data": 0, "error": 0, "unattempted": 0},
                }

        class NeverCalledPuller:
            def __init__(self, flag: dict) -> None:
                self._flag = flag

            def pull_all(self, ticker_list=None, start_date=None, should_continue=None):
                self._flag["called"] = True
                return {"status": "SUCCESS", "stopped_by_budget": False, "results": [], "tickers_not_attempted": []}

        pullers = {
            "src1": SlowSrc1Puller(),
            "src2": NeverCalledPuller(call2),
            "src3": NeverCalledPuller(call3),
        }

        def _resolve(name: str, engine: Any):
            return pullers[name], "pull_all", {}

        monkeypatch.setattr(hf, "_resolve_puller", _resolve)
        # Budget expires almost immediately — shorter than src1's 0.3s
        # provider call, so by the time that call returns the deadline has
        # already passed, with no token supersession involved at all.
        monkeypatch.setattr(hf, "REPAIR_BUDGET_SECONDS", 0.05)
        _patch_llm(
            monkeypatch,
            "SEVERITY: WARNING\n"
            "ACTION: REPULL:src1\n"
            "ACTION: REPULL:src2\n"
            "ACTION: REPULL:src3\n"
            "SUMMARY: repair three sources\n",
        )

        conn = _RecordingConn()
        engine = _fake_engine(conn)
        state = OperatorState()

        log_sink = io.StringIO()
        sink_id = loguru_logger.add(log_sink, level="WARNING")
        try:
            result = hf.run_self_diagnostics(engine, True, _HEALTH, state)
        finally:
            loguru_logger.remove(sink_id)

        assert call2["called"] is False, "action 2 must not run once the deadline has passed"
        assert call3["called"] is False, "action 3 must not run once the deadline has passed"
        assert result.get("skipped_actions") == ["REPULL:src2", "REPULL:src3"]

        log_text = log_sink.getvalue()
        assert "diagnostics actions: 2 remaining action(s) skipped" in log_text
        assert "repair deadline passed" in log_text
        assert "worker superseded" not in log_text
