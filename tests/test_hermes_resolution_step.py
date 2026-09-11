"""Tests for the Hermes cycle's conflict-resolution step.

Regression cover for the 2026-03-29 → 2026-09-11 outage: commit b0a02b4
replaced ``Resolver.resolve_pending()`` in cycle step 3b with an
``INSERT ... SELECT ... JOIN entity_map`` statement that could never execute
(no such table; no such columns) inside ``except Exception: log.debug(...)``.
resolved_series stopped advancing and nothing said so for five and a half
months.

These tests pin the two properties that would have caught it:

  * the step actually invokes the resolver and records what it returned;
  * a failure is loud — a warning log plus an entry in ``cycle_result`` —
    never a debug line.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from loguru import logger as log


# ---------------------------------------------------------------------------
# Doubles
# ---------------------------------------------------------------------------

class _FakeCooldowns:
    def __init__(self) -> None:
        self.blacklisted: list[str] = []
        self.attempts: list[tuple] = []

    def blacklist_for_timeout(self, name: str) -> None:
        self.blacklisted.append(name)

    def record_attempt(self, name: str, success: bool = True, error: str | None = None) -> None:
        self.attempts.append((name, success, error))


class _FakeState:
    """Minimal OperatorState stand-in carrying the resolution watermark."""

    def __init__(self, last_resolution: datetime | None = None) -> None:
        self.current_step: str | None = None
        self.last_resolution = last_resolution
        self.cooldowns = _FakeCooldowns()
        self.task_status: dict[str, dict] = {}

    def record_task(
        self, task_name, success, duration_s, error=None, transient=False,
    ) -> None:
        # Mirrors OperatorState.record_task in scripts/hermes_health.py.
        self.task_status[task_name] = {
            "success": success, "duration_s": duration_s, "error": error,
            "transient": transient,
        }


@pytest.fixture
def warnings_captured():
    """Capture WARNING-and-above loguru records emitted during the test."""
    records: list[str] = []
    sink_id = log.add(lambda msg: records.append(msg), level="WARNING")
    yield records
    log.remove(sink_id)


def _install_resolver(monkeypatch, *, summary=None, raises=None, calls=None):
    """Replace normalization.resolver.Resolver with a recording double."""
    class _FakeResolver:
        def __init__(self, db_engine=None):
            self.engine = db_engine

        def resolve_pending(self, **kwargs):
            if calls is not None:
                calls.append(kwargs)
            if raises is not None:
                raise raises
            return summary

    monkeypatch.setattr("normalization.resolver.Resolver", _FakeResolver)


# ---------------------------------------------------------------------------
# The step runs the resolver
# ---------------------------------------------------------------------------

class TestResolutionStepRunsResolver:

    def test_step_invokes_resolver_and_records_result(self, monkeypatch):
        import scripts.hermes_operator as hermes

        calls: list[dict] = []
        summary = {
            "resolved": 42, "conflicts_found": 3, "errors": 0,
            "series_scanned": 17, "duration_s": 4.2, "dry_run": False,
        }
        _install_resolver(monkeypatch, summary=summary, calls=calls)
        state = _FakeState()

        result = hermes._run_resolution_step(object(), state)

        assert result == summary
        assert len(calls) == 1, "resolver was not called exactly once"
        assert calls[0]["lookback_days"] == hermes.RESOLUTION_CYCLE_LOOKBACK_DAYS
        assert calls[0]["workers"] == hermes.RESOLUTION_CYCLE_WORKERS
        assert state.task_status["resolution"]["success"] is True
        assert state.current_step == "resolution"

    def test_cycle_step_3b_calls_the_resolver_helper(self):
        """run_cycle must delegate resolution to the resolver, not raw SQL."""
        import inspect

        import scripts.hermes_operator as hermes

        source = inspect.getsource(hermes.run_cycle)
        assert '_run_resolution_step(engine, state)' in source
        # The impossible fast path must be gone for good.
        assert "JOIN entity_map" not in source
        assert "resolved_at" not in source

    def test_no_sql_literal_references_the_phantom_entity_map_table(self):
        """`entity_map` is a Python dict, never a table — no SQL may join it.

        Scans string constants only, so prose about the regression in a
        comment or docstring does not trip the guard.
        """
        import ast
        from pathlib import Path

        for rel in ("scripts/hermes_operator.py", "normalization/resolver.py"):
            tree = ast.parse(Path(rel).read_text())
            for node in ast.walk(tree):
                if not (isinstance(node, ast.Constant) and isinstance(node.value, str)):
                    continue
                lowered = node.value.lower()
                assert "join entity_map" not in lowered, rel
                assert "from entity_map" not in lowered, rel

    def test_clean_run_advances_the_watermark(self, monkeypatch):
        import scripts.hermes_operator as hermes

        _install_resolver(monkeypatch, summary={
            "resolved": 1, "conflicts_found": 0, "errors": 0,
            "series_scanned": 1, "duration_s": 0.1, "dry_run": False,
        })
        state = _FakeState()
        before = datetime.now(timezone.utc)

        hermes._run_resolution_step(object(), state)

        assert state.last_resolution is not None
        assert state.last_resolution >= before

    def test_watermark_is_passed_as_since_with_overlap(self, monkeypatch):
        import scripts.hermes_operator as hermes

        calls: list[dict] = []
        _install_resolver(monkeypatch, summary={
            "resolved": 0, "conflicts_found": 0, "errors": 0,
            "series_scanned": 0, "duration_s": 0.0, "dry_run": False,
        }, calls=calls)
        watermark = datetime(2026, 9, 11, 12, 0, tzinfo=timezone.utc)
        state = _FakeState(last_resolution=watermark)

        hermes._run_resolution_step(object(), state)

        expected = watermark - timedelta(hours=hermes.RESOLUTION_WATERMARK_OVERLAP_HOURS)
        assert calls[0]["since"] == expected

    def test_first_run_has_no_watermark(self, monkeypatch):
        import scripts.hermes_operator as hermes

        calls: list[dict] = []
        _install_resolver(monkeypatch, summary={
            "resolved": 0, "conflicts_found": 0, "errors": 0,
            "series_scanned": 0, "duration_s": 0.0, "dry_run": False,
        }, calls=calls)

        hermes._run_resolution_step(object(), _FakeState())

        assert calls[0]["since"] is None
        assert calls[0]["lookback_days"] == hermes.RESOLUTION_CYCLE_LOOKBACK_DAYS


# ---------------------------------------------------------------------------
# Failures are loud
# ---------------------------------------------------------------------------

class TestResolutionStepFailsLoudly:

    def test_exception_yields_warning_and_cycle_error(
        self, monkeypatch, warnings_captured,
    ):
        import scripts.hermes_operator as hermes

        # The real _run_with_timeout runs here: it reports a raise and a
        # timeout identically, so the step must still tell them apart.
        _install_resolver(monkeypatch, raises=RuntimeError("relation does not exist"))
        state = _FakeState()

        result = hermes._run_resolution_step(object(), state)

        assert "error" in result
        assert "timeout" not in result, "a raise must not be reported as a timeout"
        assert "relation does not exist" in result["error"]
        assert any("relation does not exist" in str(r) for r in warnings_captured), (
            "resolution failure must be logged at WARNING, not debug"
        )
        assert state.task_status["resolution"]["success"] is False

    def test_failure_does_not_advance_the_watermark(self, monkeypatch):
        import scripts.hermes_operator as hermes

        _install_resolver(monkeypatch, raises=RuntimeError("boom"))
        watermark = datetime(2026, 9, 1, tzinfo=timezone.utc)
        state = _FakeState(last_resolution=watermark)

        hermes._run_resolution_step(object(), state)

        assert state.last_resolution == watermark

    def test_timeout_is_reported_not_swallowed(self, monkeypatch, warnings_captured):
        import scripts.hermes_operator as hermes

        _install_resolver(monkeypatch, summary=None)
        monkeypatch.setattr(
            hermes, "_run_with_timeout",
            lambda _name, _fn, _timeout, _state: (None, False),
        )
        watermark = datetime(2026, 9, 1, tzinfo=timezone.utc)
        state = _FakeState(last_resolution=watermark)

        result = hermes._run_resolution_step(object(), state)

        assert result["timeout"] is True
        assert state.last_resolution == watermark
        assert any("did not complete" in str(r) for r in warnings_captured)
        assert state.task_status["resolution"]["error"] == "timeout"

    def test_worker_errors_are_surfaced(self, monkeypatch, warnings_captured):
        import scripts.hermes_operator as hermes

        _install_resolver(monkeypatch, summary={
            "resolved": 5, "conflicts_found": 0, "errors": 2,
            "series_scanned": 9, "duration_s": 1.0, "dry_run": False,
        })
        state = _FakeState()

        result = hermes._run_resolution_step(object(), state)

        assert result["errors"] == 2
        assert state.last_resolution is None, "watermark advanced past a failed run"
        assert any("worker error" in str(r) for r in warnings_captured)

    def test_step_never_logs_resolution_failures_at_debug(self):
        """The outage was a `log.debug` on the failure path. Keep it gone."""
        import inspect

        import scripts.hermes_operator as hermes

        source = inspect.getsource(hermes._run_resolution_step)
        assert "log.debug" not in source
        assert source.count("log.warning") >= 3


# ---------------------------------------------------------------------------
# Transient vs. real: the health surfaces must be able to tell them apart
# ---------------------------------------------------------------------------

class TestTransientFailureMarker:
    """A timeout and a broken statement are both warnings, not the same event.

    Before this, every failure reported identically, so a health surface
    could not distinguish "the database was busy for 240s" from "this SQL
    names a column that does not exist" — which is the distinction the
    2026-03 regression destroyed by hiding both under ``log.debug``.
    """

    def test_operational_error_is_marked_transient(
        self, monkeypatch, warnings_captured,
    ):
        from sqlalchemy.exc import OperationalError

        import scripts.hermes_operator as hermes

        exc = OperationalError(
            "SELECT 1", {}, Exception("canceling statement due to statement timeout")
        )
        _install_resolver(monkeypatch, raises=exc)
        state = _FakeState()

        result = hermes._run_resolution_step(object(), state)

        assert result["transient"] is True
        assert result["error_class"] == "OperationalError"
        assert state.task_status["resolution"]["transient"] is True
        assert state.task_status["resolution"]["success"] is False
        # Still a warning, and the class is in the message.
        assert any("OperationalError" in str(r) for r in warnings_captured)

    def test_programming_error_is_not_transient(
        self, monkeypatch, warnings_captured,
    ):
        """The 2026-03 regression's own error must never read as transient."""
        from sqlalchemy.exc import ProgrammingError

        import scripts.hermes_operator as hermes

        exc = ProgrammingError(
            "INSERT INTO resolved_series", {},
            Exception('column "source_id" of relation "resolved_series" '
                      'does not exist'),
        )
        _install_resolver(monkeypatch, raises=exc)
        state = _FakeState()

        result = hermes._run_resolution_step(object(), state)

        assert result["transient"] is False
        assert result["error_class"] == "ProgrammingError"
        assert state.task_status["resolution"]["transient"] is False
        assert any("ProgrammingError" in str(r) for r in warnings_captured)

    def test_plain_exception_is_not_transient(self, monkeypatch):
        import scripts.hermes_operator as hermes

        _install_resolver(monkeypatch, raises=RuntimeError("boom"))
        state = _FakeState()

        result = hermes._run_resolution_step(object(), state)

        assert result["transient"] is False
        assert result["error_class"] == "RuntimeError"

    def test_timeout_is_transient(self, monkeypatch):
        """A step abandoned at its budget ran out of time, it did not break."""
        import scripts.hermes_operator as hermes

        _install_resolver(monkeypatch, summary=None)
        monkeypatch.setattr(
            hermes, "_run_with_timeout",
            lambda _name, _fn, _timeout, _state: (None, False),
        )
        state = _FakeState()

        result = hermes._run_resolution_step(object(), state)

        assert result == {"timeout": True, "transient": True}
        assert state.task_status["resolution"]["transient"] is True

    def test_recorded_detail_carries_the_exception_class(self, monkeypatch):
        import scripts.hermes_operator as hermes

        _install_resolver(monkeypatch, raises=ValueError("bad vintage"))
        state = _FakeState()

        hermes._run_resolution_step(object(), state)

        assert state.task_status["resolution"]["error"] == "ValueError: bad vintage"

    def test_classifier_is_exception_type_based_not_string_matching(self):
        """OperationalError is the class; message text must not decide."""
        from sqlalchemy.exc import OperationalError, ProgrammingError

        import scripts.hermes_operator as hermes

        # Same message, different class — only the class may matter.
        message = Exception("canceling statement due to statement timeout")
        assert hermes._is_transient_db_error(
            OperationalError("s", {}, message)
        ) is True
        assert hermes._is_transient_db_error(
            ProgrammingError("s", {}, message)
        ) is False
        assert hermes._is_transient_db_error(RuntimeError("timeout")) is False


def test_operator_state_record_task_persists_the_transient_flag():
    """The real OperatorState, not the double, must store the flag."""
    from scripts.hermes_health import OperatorState

    state = OperatorState()
    state.record_task("resolution", False, 1.5, "OperationalError: x", transient=True)
    assert state.task_status["resolution"]["transient"] is True
    assert state.task_status["resolution"]["error"] == "OperationalError: x"

    # Default stays False so existing callers are unchanged.
    state.record_task("other", True, 0.1)
    assert state.task_status["other"]["transient"] is False


def test_hermes_status_schema_carries_the_transient_flag():
    """The flag is pointless if the API layer drops it.

    HermesTaskStatus is constructed as ``HermesTaskStatus(**v)`` from the
    recorded dict in api/routers/system.py, and pydantic ignores unknown
    keys by default — an undeclared field would be silently discarded
    before any health surface could read it.
    """
    from api.schemas.system import HermesTaskStatus
    from scripts.hermes_health import OperatorState

    state = OperatorState()
    state.record_task("resolution", False, 2.0, "OperationalError: x", transient=True)
    model = HermesTaskStatus(**state.task_status["resolution"])
    assert model.transient is True

    # A snapshot written before the field existed must still validate.
    legacy = {"last_run": None, "success": False, "duration_s": 1.0, "error": "x"}
    assert HermesTaskStatus(**legacy).transient is False


def test_resolution_timeout_fits_in_the_cycle():
    """The step's budget must leave room inside the 5-minute cycle interval."""
    import scripts.hermes_operator as hermes

    assert hermes.RESOLUTION_TIMEOUT_SECONDS <= hermes.CYCLE_TIMEOUT_SECONDS
    assert hermes.RESOLUTION_CYCLE_LOOKBACK_DAYS >= 1
    assert hermes.RESOLUTION_WATERMARK_OVERLAP_HOURS >= 1
