"""GRID W4b — research run-state persistence, generation fencing, and
idempotent retry for scripts/autoresearch.py::run_autoresearch().

Scope note: this task (W4b) makes the autoresearch loop OBSERVABLE and safe
to RESTART, without activating it on any schedule (no scheduler/systemd
change, no gate change). These tests use fakes only — no real Postgres, no
real Ollama, no real backtester — per that task's hard boundary. Run with:

    DB_PASSWORD=testpass PYTHONUTF8=1 python -m pytest tests/test_autoresearch_runstate.py -q

What each test class covers:
  - _RecordingSnapshotStore + TestRunRecordSequence: every research_run
    event run_autoresearch() writes (started / running checkpoints / the
    terminal ok|failed|abandoned record), with the right statuses, in order.
  - TestGenerationFencing: a worker holding a stale (superseded) generation
    is fenced at the very first checkpoint — it never reaches the
    hypothesis_registry INSERT — and the fenced reason is recorded both in
    the returned attempt and in the run-record's skip_reasons.
  - TestIdempotentRetry: retrying run_autoresearch() with the SAME run_id
    after a hypothesis already PASSED does not insert a second
    hypothesis_registry row, does not create a second model_registry
    candidate, and does not call the notification hook a second time (a
    recorder stands in for scripts.notify.notify_on_pass — dry-run, no real
    email is ever sent by this test).
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import scripts.autoresearch as autoresearch  # noqa: E402


# ── Shared fakes ─────────────────────────────────────────────────────────

class _RecordingSnapshotStore:
    """Stand-in for store.snapshots.AnalyticalSnapshotStore.

    Appends every save_snapshot() call's (category, subcategory, payload,
    metrics) to a class-shared list instead of touching any database, so
    tests can assert on exactly what run_autoresearch() tried to persist.
    """

    events: list[dict[str, Any]] = []

    def __init__(self, db_engine: Any = None) -> None:
        self.db_engine = db_engine

    def save_snapshot(self, category, payload, as_of_date=None, subcategory=None, metrics=None, actor_name=None):
        _RecordingSnapshotStore.events.append(
            {"category": category, "subcategory": subcategory, "payload": dict(payload)}
        )
        return len(_RecordingSnapshotStore.events)


class _FakeOllama:
    is_available = True

    def chat(self, *args: Any, **kwargs: Any):
        # No response -> run_autoresearch records an "Ollama no response"
        # attempt and moves on; with max_iterations=1 the loop ends there.
        return None


class _NullCursor:
    def execute(self, *a, **k):
        pass

    def fetchall(self):
        return []

    def fetchone(self):
        return None

    def close(self):
        pass


class _NullConnection:
    autocommit = False

    def __init__(self):
        self.closed = False

    def cursor(self):
        return _NullCursor()

    def close(self):
        self.closed = True


@pytest.fixture(autouse=True)
def _reset_recording_store(monkeypatch):
    """Every test gets a clean event log and the recorder wired in place of
    the real AnalyticalSnapshotStore, plus a clean orthogonal-feature cache
    (module global memoized across calls in the real module).
    """
    _RecordingSnapshotStore.events = []
    import store.snapshots as snapshots_module

    monkeypatch.setattr(snapshots_module, "AnalyticalSnapshotStore", _RecordingSnapshotStore)
    monkeypatch.setattr(autoresearch, "_ortho_cache", None)
    monkeypatch.setattr(autoresearch, "get_engine", lambda: object())
    monkeypatch.setattr(autoresearch, "PITStore", lambda engine: object())
    monkeypatch.setattr(autoresearch, "WalkForwardBacktest", lambda engine, pit: object())
    monkeypatch.setattr(autoresearch, "get_ollama", lambda: _FakeOllama())
    monkeypatch.setattr(autoresearch, "OllamaReasoner", lambda ollama: object())
    monkeypatch.setattr(autoresearch, "get_feature_list", lambda cur: "(no features)")
    monkeypatch.setattr(autoresearch, "get_feature_name_map", lambda cur: {})
    monkeypatch.setattr(autoresearch, "get_market_snapshot", lambda cur: "(no data)")
    monkeypatch.setattr(autoresearch, "_select_orthogonal_features", lambda cur, **kw: [])

    import psycopg2

    monkeypatch.setattr(psycopg2, "connect", lambda **kwargs: _NullConnection())
    yield


# ── started / checkpoint / end statuses ──────────────────────────────────

class TestRunRecordSequence:
    def test_success_run_writes_started_context_loaded_iteration_and_ok(self):
        result = autoresearch.run_autoresearch(max_iterations=1, run_id="run-seq-1")

        assert result["status"] == "ok"

        statuses = [e["payload"]["status"] for e in _RecordingSnapshotStore.events]
        phases = [e["payload"]["phase"] for e in _RecordingSnapshotStore.events]

        assert statuses == ["started", "running", "running", "ok"]
        assert phases == ["init", "context_loaded", "iteration", "complete"]

        # subcategory/category must be exactly what scripts/research_status.py
        # reads.
        for e in _RecordingSnapshotStore.events:
            assert e["category"] == "research_run"
            assert e["subcategory"] == "autoresearch"
            assert e["payload"]["run_id"] == "run-seq-1"

        # The context_loaded checkpoint carries the inputs manifest.
        ctx_event = _RecordingSnapshotStore.events[1]
        assert ctx_event["payload"]["inputs"] == {
            "feature_ids_count": 0,
            "market_snapshot_keys": [],
            "evaluation_version": None,
        }

        # The iteration checkpoint identifies iteration 1.
        iter_event = _RecordingSnapshotStore.events[2]
        assert iter_event["payload"]["iteration"] == 1

        # Terminal record carries the final iteration count.
        end_event = _RecordingSnapshotStore.events[-1]
        assert end_event["payload"]["iterations"] == 1

    def test_db_load_failure_writes_started_then_failed_with_zero_iterations(self, monkeypatch):
        original = RuntimeError('column "f.does_not_exist" does not exist (simulated)')

        def _raise(cur):
            raise original

        monkeypatch.setattr(autoresearch, "get_feature_list", _raise)

        result = autoresearch.run_autoresearch(max_iterations=1, run_id="run-seq-2")

        assert result["status"] == "failed"

        statuses = [e["payload"]["status"] for e in _RecordingSnapshotStore.events]
        assert statuses == ["started", "failed"]

        failed_event = _RecordingSnapshotStore.events[-1]
        assert failed_event["payload"]["phase"] == "feature_list"
        assert failed_event["payload"]["error_category"] == "db_load_failure"
        assert failed_event["payload"]["iterations"] == 0
        assert "does_not_exist" in failed_event["payload"]["error"]


# ── generation fencing ───────────────────────────────────────────────────

class TestGenerationFencing:
    def test_stale_generation_is_fenced_before_any_hypothesis_write(self):
        """A worker carrying a generation the operator has already moved
        past must not reach the hypothesis_registry INSERT at all — it is
        fenced at the top of the very first iteration.
        """
        insert_calls: list[Any] = []

        class _CountingCursor(_NullCursor):
            def execute(self, sql, params=None):
                if "INSERT INTO hypothesis_registry" in sql:
                    insert_calls.append(params)

        class _CountingConnection(_NullConnection):
            def cursor(self):
                return _CountingCursor()

        import psycopg2

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(psycopg2, "connect", lambda **kwargs: _CountingConnection())

            result = autoresearch.run_autoresearch(
                max_iterations=3,
                run_id="run-fenced-1",
                generation=1,
                is_current_generation=lambda g: False,  # operator has moved to gen 2+
            )

        assert result["status"] == "abandoned"
        assert result["fenced"] is True
        assert insert_calls == []  # no write of any kind was attempted

        fenced_attempt = result["all_attempts"][0]
        assert fenced_attempt["fenced"] is True
        assert "generation superseded" in fenced_attempt["error"]

        end_event = _RecordingSnapshotStore.events[-1]
        assert end_event["payload"]["status"] == "abandoned"
        assert end_event["payload"]["skip_reasons"] == ["fenced_before_iteration_1"]

    def test_current_generation_is_not_fenced(self):
        """The mirror case: a generation that IS still current must proceed
        normally (no fencing false-positive)."""
        result = autoresearch.run_autoresearch(
            max_iterations=1,
            run_id="run-not-fenced-1",
            generation=7,
            is_current_generation=lambda g: g == 7,
        )
        assert result["status"] == "ok"
        assert result["fenced"] is False


# ── idempotent retry ──────────────────────────────────────────────────────

class _FakeDB:
    """In-memory stand-in for the slice of Postgres run_autoresearch()
    touches: hypothesis_registry and model_registry. Shared across two
    run_autoresearch() calls with the same run_id to prove a retry does not
    duplicate either table's rows.
    """

    def __init__(self) -> None:
        self.hypotheses: dict[int, dict[str, Any]] = {}
        self.models: dict[int, int] = {}
        self.next_hyp_id = 1
        self.next_model_id = 1
        self.hyp_insert_count = 0
        self.model_insert_count = 0


class _RetryCursor:
    def __init__(self, db: _FakeDB):
        self.db = db
        self._result: Any = None

    def execute(self, sql: str, params: Any = None) -> None:
        s = " ".join(sql.split())
        if s.startswith("SELECT id, state FROM hypothesis_registry"):
            statement, layer = params
            match = None
            for hyp_id, row in self.db.hypotheses.items():
                if row["statement"] == statement and row["layer"] == layer:
                    match = (hyp_id, row["state"])
            self._result = match
        elif s.startswith("INSERT INTO hypothesis_registry"):
            self.db.hyp_insert_count += 1
            statement, layer = params[0], params[1]
            hyp_id = self.db.next_hyp_id
            self.db.next_hyp_id += 1
            self.db.hypotheses[hyp_id] = {"statement": statement, "layer": layer, "state": "TESTING"}
            self._result = (hyp_id,)
        elif s.startswith("UPDATE hypothesis_registry SET state"):
            state, _kill_reason, hyp_id = params
            self.db.hypotheses[hyp_id]["state"] = state
            self._result = None
        elif s.startswith("SELECT id FROM model_registry WHERE hypothesis_id"):
            (hyp_id,) = params
            self._result = (self.db.models[hyp_id],) if hyp_id in self.db.models else None
        elif s.startswith("SELECT id FROM validation_results"):
            self._result = None
        elif s.startswith("INSERT INTO model_registry"):
            self.db.model_insert_count += 1
            hyp_id = params[3]
            model_id = self.db.next_model_id
            self.db.next_model_id += 1
            self.db.models[hyp_id] = model_id
            self._result = (model_id,)
        else:
            self._result = None

    def fetchone(self):
        return self._result

    def fetchall(self):
        return []

    def close(self):
        pass


class _RetryConnection:
    autocommit = False

    def __init__(self, db: _FakeDB):
        self.db = db
        self.closed = False

    def cursor(self):
        return _RetryCursor(self.db)

    def close(self):
        self.closed = True


class _PassingBacktester:
    def run_validation(self, **kwargs):
        return {
            "overall_verdict": "PASS",
            "full_period_metrics": {"sharpe": 1.2, "return": 0.1, "max_drawdown": 0.05},
            "baseline_comparison": {"sharpe": 0.1},
            "era_results": [],
        }


class _ChattyOllama:
    """Returns non-None so parse_hypothesis_json (monkeypatched below) is
    reached; the actual text is irrelevant since parsing is stubbed."""

    is_available = True

    def chat(self, *a, **k):
        return "irrelevant — parse_hypothesis_json is stubbed"


class TestIdempotentRetry:
    def test_retry_with_same_run_id_does_not_duplicate_hypothesis_model_or_notification(self, monkeypatch):
        db = _FakeDB()
        notify_calls: list[Any] = []

        fixed_hyp = {
            "statement": "When VIX spikes, SP500 mean-reverts within 5 days",
            "feature_ids": [1, 2],
            "lag_structure": {"1": 0, "2": 5},
            "layer": "REGIME",
            "proposed_metric": "sharpe",
            "proposed_threshold": 0.5,
        }

        import psycopg2
        import scripts.notify as notify_module

        monkeypatch.setattr(psycopg2, "connect", lambda **kwargs: _RetryConnection(db))
        monkeypatch.setattr(autoresearch, "WalkForwardBacktest", lambda engine, pit: _PassingBacktester())
        monkeypatch.setattr(autoresearch, "get_ollama", lambda: _ChattyOllama())
        monkeypatch.setattr(autoresearch, "parse_hypothesis_json", lambda text: dict(fixed_hyp))
        monkeypatch.setattr(notify_module, "notify_on_pass", lambda attempt: notify_calls.append(attempt))

        # First attempt: genuinely new hypothesis, passes, notifies once.
        result_1 = autoresearch.run_autoresearch(max_iterations=1, run_id="retry-run-1")
        assert result_1["status"] == "ok"
        assert result_1["passed"] is True
        assert db.hyp_insert_count == 1
        assert db.model_insert_count == 1
        assert len(notify_calls) == 1

        # Retry with the SAME run_id (e.g. the operator abandoned the
        # first attempt on a timeout and is retrying it) — same statement
        # comes back from the LLM (stubbed deterministically above), and
        # the hypothesis is already PASSED in the fake DB from attempt 1.
        result_2 = autoresearch.run_autoresearch(max_iterations=1, run_id="retry-run-1")
        assert result_2["status"] == "ok"

        # No duplicate row in either table, and the notification hook was
        # NOT called a second time.
        assert db.hyp_insert_count == 1
        assert db.model_insert_count == 1
        assert len(notify_calls) == 1

        assert result_2["all_attempts"][0]["reused"] is True
        assert result_2["all_attempts"][0]["verdict"] == "PASS"
