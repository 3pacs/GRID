"""GRID W4b — bounded work (timeout) and generation fencing at the
hermes_operator.py / hermes_fixers.py layer, plus the hypotheses_tested
counter's handling of a real failure vs. a real success.

Companion to tests/test_autoresearch_runstate.py, which covers the same
fencing/idempotency mechanisms from inside scripts/autoresearch.py. This
file covers the two pieces that live one layer up:

  - TestGenerationTracker: scripts.hermes_operator._AutoresearchGenerationTracker
    in isolation — the in-process counter _run_with_timeout's caller bumps
    on a timeout, which is what makes a subsequently-fenced write in
    scripts/autoresearch.py possible. Also proves the exact "timed-out
    worker cannot publish after the operator moved on" chain end-to-end
    with the REAL tracker and REAL run_autoresearch fencing check (no
    threads, no timing — the timeout is simulated by calling .next() the
    way the operator's timeout branch does).
  - TestOperatorTimeoutRecord: the "timeout" research_run record is written
    by _record_research_run (imported from scripts.autoresearch by
    scripts/hermes_operator.py's timeout branch) with status="timeout" and
    error_category="timeout" — the exact call hermes_operator.py's cycle-6
    gate makes when _run_with_timeout signals ok=False.
  - TestHypothesesTestedCounter: scripts.hermes_fixers.maybe_run_autoresearch
    increments state.hypotheses_tested by result["iterations"], which is 0
    on a real failure and >0 on a real success — so a failure never
    "zeroes" an already-accumulated counter (it is `+=`, not `=`), while a
    genuine success still advances it. Also confirms the one-line
    docs/handoffs/2026-09-18/fable-w4-hermes-operator.patch addition:
    log.error() fires exactly when result["status"] == "failed".

Run with:
    DB_PASSWORD=testpass PYTHONUTF8=1 python -m pytest tests/test_hermes_autoresearch_bounded_and_fenced.py -q
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import scripts.autoresearch as autoresearch  # noqa: E402
import scripts.hermes_fixers as hermes_fixers  # noqa: E402
import scripts.hermes_operator as hermes_operator  # noqa: E402
from scripts.hermes_health import OperatorState  # noqa: E402


class _RecordingSnapshotStore:
    events: list[dict[str, Any]] = []

    def __init__(self, db_engine: Any = None) -> None:
        self.db_engine = db_engine

    def save_snapshot(self, category, payload, as_of_date=None, subcategory=None, metrics=None, actor_name=None):
        _RecordingSnapshotStore.events.append(
            {"category": category, "subcategory": subcategory, "payload": dict(payload)}
        )
        return len(_RecordingSnapshotStore.events)


@pytest.fixture(autouse=True)
def _reset_recording_store(monkeypatch):
    _RecordingSnapshotStore.events = []
    import store.snapshots as snapshots_module

    monkeypatch.setattr(snapshots_module, "AnalyticalSnapshotStore", _RecordingSnapshotStore)
    yield


# ── Generation tracker, in isolation ──────────────────────────────────────

class TestGenerationTracker:
    def test_next_advances_and_is_current_tracks_latest_only(self):
        tracker = hermes_operator._AutoresearchGenerationTracker()

        gen_a = tracker.next()
        assert tracker.is_current(gen_a) is True

        gen_b = tracker.next()
        assert gen_b != gen_a
        # The operator moved on — the OLD generation is no longer current.
        assert tracker.is_current(gen_a) is False
        assert tracker.is_current(gen_b) is True

    def test_timed_out_worker_cannot_publish_after_operator_bumps_generation(self, monkeypatch):
        """End-to-end with the REAL tracker and the REAL fencing check in
        scripts/autoresearch.py (no threads involved — the timeout itself
        is simulated the way hermes_operator.py's timeout branch handles
        it: by calling tracker.next() immediately once _run_with_timeout
        signals ok=False, exactly as the cycle-6 gate does).
        """
        tracker = hermes_operator._AutoresearchGenerationTracker()

        # Step 1: operator assigns a generation for this invocation, as the
        # cycle-6 gate does before calling _run_with_timeout.
        worker_generation = tracker.next()

        # Step 2: simulate _run_with_timeout reporting ok=False (the
        # worker thread is still running — abandoned, not killed). The
        # operator's timeout branch bumps the generation right away.
        tracker.next()

        # Step 3: the orphaned worker (still holding worker_generation)
        # finally gets around to trying to write. It must be fenced before
        # any WRITE (context loading is a read and is allowed to proceed —
        # fencing guards writes, not the whole call).
        write_attempts: list[str] = []

        class _NullCursor:
            def execute(self, sql, params=None):
                if sql.strip().upper().startswith(("INSERT", "UPDATE", "DELETE")):
                    write_attempts.append(sql)

            def fetchone(self):
                return None

            def fetchall(self):
                return []

            def close(self):
                pass

        class _NullConnection:
            autocommit = False

            def cursor(self):
                return _NullCursor()

            def close(self):
                pass

        import psycopg2

        monkeypatch.setattr(autoresearch, "_ortho_cache", None)
        monkeypatch.setattr(autoresearch, "get_engine", lambda: object())
        monkeypatch.setattr(autoresearch, "PITStore", lambda engine: object())
        monkeypatch.setattr(autoresearch, "WalkForwardBacktest", lambda engine, pit: object())
        monkeypatch.setattr(autoresearch, "get_ollama", lambda: type("O", (), {"is_available": True})())
        monkeypatch.setattr(autoresearch, "OllamaReasoner", lambda ollama: object())
        monkeypatch.setattr(autoresearch, "get_feature_list", lambda cur: "(no features)")
        monkeypatch.setattr(autoresearch, "get_feature_name_map", lambda cur: {})
        monkeypatch.setattr(autoresearch, "get_market_snapshot", lambda cur: "(no data)")
        monkeypatch.setattr(autoresearch, "_select_orthogonal_features", lambda cur, **kw: [])
        monkeypatch.setattr(psycopg2, "connect", lambda **kwargs: _NullConnection())

        result = autoresearch.run_autoresearch(
            max_iterations=5,
            run_id="orphan-run",
            generation=worker_generation,
            is_current_generation=tracker.is_current,
        )

        assert result["status"] == "abandoned"
        assert result["fenced"] is True
        # The orphan reached context loading (a read) but never wrote —
        # this is the precise claim "timed-out workers cannot publish
        # after the operator moved on".
        assert write_attempts == []


# ── Operator-side timeout record ─────────────────────────────────────────

class TestOperatorTimeoutRecord:
    def test_record_research_run_timeout_matches_operator_timeout_branch(self):
        """This is the exact call scripts/hermes_operator.py's cycle-6 gate
        makes (via `from scripts.autoresearch import _record_research_run`)
        when _run_with_timeout returns ok=False for the autoresearch step.
        """
        engine = object()
        autoresearch._record_research_run(
            engine, "op-timeout-run-1", "timeout",
            phase="operator_timeout",
            error="exceeded 1800s",
            error_category="timeout",
            generation=3,
        )

        assert len(_RecordingSnapshotStore.events) == 1
        event = _RecordingSnapshotStore.events[0]
        assert event["category"] == "research_run"
        assert event["subcategory"] == "autoresearch"
        assert event["payload"]["status"] == "timeout"
        assert event["payload"]["error_category"] == "timeout"
        assert event["payload"]["run_id"] == "op-timeout-run-1"
        assert event["payload"]["generation"] == 3


# ── hypotheses_tested counter on failure vs success ───────────────────────

class TestHypothesesTestedCounter:
    @pytest.fixture(autouse=True)
    def _enable_autoresearch(self, monkeypatch):
        # These tests exercise maybe_run_autoresearch's post-gate behavior
        # (counter/log-level/plumbing), which is unreachable while the
        # 2026-09-19 AUTORESEARCH_ENABLED off-by-default gate (see
        # tests/test_autoresearch_gate.py) is at its default False.
        from config import settings

        monkeypatch.setattr(settings, "AUTORESEARCH_ENABLED", True)

    def test_real_failure_does_not_zero_an_already_accumulated_counter(self, monkeypatch, caplog):
        state = OperatorState()
        state.hypotheses_tested = 5  # accumulated from earlier, real successes
        state.last_autoresearch = None  # bypass the 12h cooldown gate

        failure_result = {
            "status": "failed",
            "phase": "feature_list",
            "error": "column does not exist (simulated)",
            "iterations": 0,
            "iterations_run": 0,
            "passed": False,
        }
        monkeypatch.setattr(autoresearch, "run_autoresearch", lambda **kwargs: failure_result)

        import loguru

        with caplog.at_level("ERROR"):
            result = hermes_fixers.maybe_run_autoresearch(state, dry_run=False)

        assert result["status"] == "failed"
        # += 0, never a bare reset to 0 — the accumulated total survives a
        # real failure.
        assert state.hypotheses_tested == 5

    def test_real_success_advances_the_counter(self, monkeypatch):
        state = OperatorState()
        state.hypotheses_tested = 5
        state.last_autoresearch = None

        success_result = {
            "status": "ok",
            "iterations": 3,
            "iterations_run": 3,
            "passed": True,
            "best_result": None,
            "best_sharpe": -999.0,
            "all_attempts": [],
        }
        monkeypatch.setattr(autoresearch, "run_autoresearch", lambda **kwargs: success_result)

        result = hermes_fixers.maybe_run_autoresearch(state, dry_run=False)

        assert result["status"] == "ok"
        assert state.hypotheses_tested == 8  # 5 + 3, not reset

    def test_failed_status_logs_at_error_per_the_w4_slice1_patch(self, monkeypatch, caplog):
        """docs/handoffs/2026-09-18/fable-w4-hermes-operator.patch's one-line
        addition: maybe_run_autoresearch must log.error() when
        result["status"] == "failed", naming the phase and error.
        """
        state = OperatorState()
        state.last_autoresearch = None

        failure_result = {
            "status": "failed",
            "phase": "ollama_availability",
            "error": "Ollama not available",
            "iterations": 0,
            "iterations_run": 0,
            "passed": False,
        }
        monkeypatch.setattr(autoresearch, "run_autoresearch", lambda **kwargs: failure_result)

        import loguru
        from loguru import logger as loguru_logger

        messages: list[str] = []
        handler_id = loguru_logger.add(lambda msg: messages.append(str(msg)), level="ERROR")
        try:
            hermes_fixers.maybe_run_autoresearch(state, dry_run=False)
        finally:
            loguru_logger.remove(handler_id)

        assert any("ollama_availability" in m and "Ollama not available" in m for m in messages)

    def test_run_id_and_generation_are_forwarded_to_run_autoresearch(self, monkeypatch):
        """maybe_run_autoresearch must forward run_id/generation/
        is_current_generation through to run_autoresearch() unchanged —
        this is the plumbing scripts/hermes_operator.py's cycle-6 gate
        depends on for both retry (run_id) and fencing (generation).
        """
        state = OperatorState()
        state.last_autoresearch = None
        captured: dict[str, Any] = {}

        def _fake_run_autoresearch(**kwargs):
            captured.update(kwargs)
            return {"status": "ok", "iterations": 0, "iterations_run": 0, "passed": False}

        monkeypatch.setattr(autoresearch, "run_autoresearch", _fake_run_autoresearch)

        is_current = lambda g: True
        hermes_fixers.maybe_run_autoresearch(
            state, dry_run=False, run_id="fwd-run-1", generation=9, is_current_generation=is_current,
        )

        assert captured["run_id"] == "fwd-run-1"
        assert captured["generation"] == 9
        assert captured["is_current_generation"] is is_current
