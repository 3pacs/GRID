"""Pin the nesting invariants between Hermes per-step timeouts.

Background (2026-09-10 audit)
-----------------------------
``run_intelligence_tasks()`` is executed by the cycle under
``_run_with_timeout(..., INTELLIGENCE_TASKS_TIMEOUT_SECONDS)``. Inside it,
the 30-minute active-hypothesis scorer runs *first*, via ``_run_intel_task``
— which adds timing and error isolation but **no timeout of its own**
(``scripts/hermes_fixers.py::_run_intel_task``). The scorer's only bound is
its ``max_runtime_s`` argument, ``ACTIVE_HYPO_SCORING_MAX_RUNTIME_S``.

From 2026-05-15 the scorer budget was 600 s inside a 360 s step. Twice an
hour the step hit its cap before the scorer returned: ``_run_with_timeout``
orphaned the worker thread, recorded ``{"timeout": True}`` for the whole
intelligence step, and the daily 02:00 block — which is where
``HypothesisGenerator.auto_discover()`` lives — only ever ran inside an
orphaned thread whose results were never recorded. These tests make that
class of misconfiguration a CI failure instead of a silent stall.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.schema import CreateSchema, DropSchema

from scripts import hermes_operator as ho


def test_smart_ingestion_encloses_last_started_options_puller_and_cleanup() -> None:
    """The scheduling cutoff is not a deadline for the last in-flight job."""
    from ingestion.smart_scheduler import PULLER_REGISTRY, TICK_TIME_BUDGET_S

    options = next(p for p in PULLER_REGISTRY if p["name"] == "options")
    assert TICK_TIME_BUDGET_S + options["timeout_s"] + 60 <= ho.SMART_INGESTION_TIMEOUT_SECONDS
    assert ho.SMART_INGESTION_TIMEOUT_SECONDS < ho.CYCLE_TIMEOUT_SECONDS


@pytest.mark.parametrize("instant,expected", [
    ("2026-11-10T14:29:59+00:00", False),
    ("2026-11-10T14:30:00+00:00", True),
    ("2026-11-10T20:59:59+00:00", True),
    ("2026-11-10T21:00:00+00:00", False),
    ("2026-11-14T17:00:00+00:00", False),
    ("2026-11-27T17:59:59+00:00", True),
    ("2026-11-27T18:00:00+00:00", False),
])
def test_forward_cohort_uses_actual_regular_session(instant, expected):
    assert ho._oracle_forward_regular_session(datetime.fromisoformat(instant)) is expected


def _forward_state():
    return SimpleNamespace(last_oracle_cycle=None, current_step=None, cooldowns=MagicMock())


@pytest.mark.parametrize("condition,reason", [
    ("closed", "regular_session_closed"), ("hourly", "hourly_cadence"),
    ("cooldown", "timeout_or_failure_cooldown"), ("budget", "insufficient_cycle_budget"),
    ("dry_run", "dry_run"),
])
def test_forward_admission_defers_without_dispatch_or_attempt_marker(monkeypatch, condition, reason):
    state = _forward_state()
    state.cooldowns.can_retry.return_value = condition != "cooldown"
    monkeypatch.setattr(ho, "_oracle_forward_regular_session", lambda now: condition != "closed")
    monkeypatch.setattr(ho, "_run_with_timeout", lambda *a: pytest.fail("Ineligible pass dispatched"))
    if condition == "hourly":
        state.last_oracle_cycle = datetime.now(timezone.utc) - timedelta(minutes=59)
    previous = state.last_oracle_cycle
    budget = 599 if condition == "budget" else 1000
    result = {}
    ho._run_oracle_forward_step(object(), state, condition == "dry_run", result,
                                ho.time.monotonic() + budget)
    assert result["oracle"]["reason"] == reason
    assert state.last_oracle_cycle == previous


def test_forward_timeout_cancels_orphan_and_does_not_claim_completion(monkeypatch):
    state = _forward_state()
    state.cooldowns.can_retry.return_value = True
    monkeypatch.setattr(ho, "_oracle_forward_regular_session", lambda now: True)
    callbacks = []

    def pass_stub(engine, *, should_continue):
        callbacks.append(should_continue)
        assert should_continue()
        return {"status": "completed"}

    def timeout(name, fn, timeout_s, state):
        assert name == "oracle_cycle"
        assert timeout_s == 300
        fn()  # Capture the callback held by a worker that outlives its waiter.
        state.cooldowns.blacklist_for_timeout(name)
        return None, False

    monkeypatch.setattr(ho, "_run_oracle_forward_pass", pass_stub)
    monkeypatch.setattr(ho, "_run_with_timeout", timeout)
    result = {}
    ho._run_oracle_forward_step(object(), state, False, result, ho.time.monotonic() + 1000)
    assert result["oracle"]["status"] == "failed"
    assert not callbacks[0]()
    state.cooldowns.record_attempt.assert_not_called()
    state.cooldowns.blacklist_for_timeout.assert_called_once_with("oracle_cycle")


@pytest.fixture
def forward_engine(pg_engine):
    if not (pg_engine.url.database or "").endswith("_test"):
        pytest.skip("Forward cadence proof requires a disposable _test database")
    with pg_engine.connect() as conn:
        assert conn.execute(text("SELECT current_database()")).scalar_one().endswith("_test")
    schema = "forward_loop_" + uuid4().hex
    with pg_engine.begin() as conn:
        conn.execute(CreateSchema(schema))
    engine = create_engine(pg_engine.url, connect_args={
        "options": "-c search_path=" + schema + " -c statement_timeout=5000",
    })
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE oracle_iterations (
                    iteration_at TIMESTAMPTZ, models_updated INTEGER,
                    predictions_scored INTEGER, notes TEXT
                )
            """))
        yield engine
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(DropSchema(schema, cascade=True))


def _stub_forward_inputs(monkeypatch, calls, *, captured=True):
    from ingestion import options
    from oracle import engine as oracle_engine

    class Oracle:
        def __init__(self, db_engine):
            self._last_store_counts = {"qualified": 0, "inserted": 2, "abstentions": 2,
                                       "duplicates": 3, "rejected": 4}

        def score_expired_predictions(self):
            calls.append("score")
            return {"total": 0}

        def generate_predictions(self, tickers, *, horizon_days, should_continue):
            assert tickers == ["SPY", "QQQ"]
            assert horizon_days == 1 and should_continue()
            calls.append("generate")
            return []

        def _get_leaderboard(self):
            calls.append("leaderboard")
            return []

        def run_cycle(self, *a, **kw):
            pytest.fail("Measurement pass must not invoke evolution")

    class Capture:
        def __init__(self, db_engine):
            pass

        def pull_all(self, tickers, *, capture_source, should_continue):
            assert tickers == ["SPY", "QQQ"]
            assert capture_source == "oracle_forward" and should_continue()
            calls.append("capture")
            return [{"ticker": ticker, "status": "SUCCESS" if captured else "FAILED"}
                    for ticker in tickers]

    monkeypatch.setattr(oracle_engine, "OracleEngine", Oracle)
    monkeypatch.setattr(options, "OptionsPuller", Capture)
    monkeypatch.setattr(options, "automatic_options_capture_guard", lambda engine: None)
    monkeypatch.setattr(ho, "_oracle_forward_regular_session", lambda now: True)


def test_forward_pass_captures_before_inputs_and_durable_hourly_gate_survives_restart(
    forward_engine, monkeypatch,
):
    calls = []
    _stub_forward_inputs(monkeypatch, calls)
    result = ho._run_oracle_forward_pass(forward_engine, should_continue=lambda: True)
    assert calls == ["score", "capture", "generate", "leaderboard"]
    assert result["status"] == "completed"
    assert result["new_predictions"] == 0
    assert result["new_abstentions"] == 2
    assert result["new_record_rows"] == 2
    assert result["unchanged_forecasts"] == 3
    assert result["rejected_models"] == 4
    assert result["evolution"]["mode"] == "held"
    calls.clear()
    # No shared OperatorState: this is the path a restarted daemon takes.
    second = ho._run_oracle_forward_pass(forward_engine, should_continue=lambda: True)
    assert second["reason"] == "hourly_attempt_already_recorded"
    assert calls == []
    with forward_engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM oracle_iterations")).scalar_one() == 1


def test_forward_capture_failure_never_generates_and_attempt_remains_durable(forward_engine, monkeypatch):
    calls = []
    _stub_forward_inputs(monkeypatch, calls, captured=False)
    result = ho._run_oracle_forward_pass(forward_engine, should_continue=lambda: True)
    assert result["status"] == "abstained"
    assert calls == ["score", "capture"]
    assert ho._run_oracle_forward_pass(forward_engine, should_continue=lambda: True)["status"] == "deferred"


def test_forward_existing_worker_lock_defers_without_capture_or_attempt(forward_engine, monkeypatch):
    calls = []
    _stub_forward_inputs(monkeypatch, calls)
    with forward_engine.connect() as owner:
        owner.execute(text("SELECT pg_advisory_lock(hashtext('oracle_forward_v2'), hashtext('SPY_QQQ_1d'))"))
        try:
            result = ho._run_oracle_forward_pass(forward_engine, should_continue=lambda: True)
            assert result["reason"] == "forward_pass_in_flight"
            assert calls == []
        finally:
            owner.execute(text("SELECT pg_advisory_unlock(hashtext('oracle_forward_v2'), hashtext('SPY_QQQ_1d'))"))
            owner.commit()


def test_active_hypo_scorer_fits_inside_intelligence_step_with_daily_batch() -> None:
    """Scorer budget + the daily-intel per-cycle budget (+ headroom) must
    fit inside the step budget.

    The scorer runs first and the daily-intel block (DAILY_INTEL_TASKS,
    executed by _run_daily_intel_block) runs last inside the same step; if
    their combined worst case exceeds the step cap, the daily block is
    starved of any turn at all on the cycles that matter.

    fable-daily-intel-resumable (2026-09-20) replaced the single
    DAILY_INTEL_BATCH_OBSERVED_S measurement with a hard per-cycle cap,
    DAILY_INTEL_CYCLE_BUDGET_SECONDS, that _run_daily_intel_block enforces
    itself (checked before starting each task) — so this pin now compares
    against the cap the code actually enforces, not an observed value from
    the monolithic block that preceded it. The +60 covers the
    earnings-calendar-sync SQL call and active-hypo-scoring bookkeeping
    that run ahead of both inside the same step.
    """
    assert (
        ho.ACTIVE_HYPO_SCORING_MAX_RUNTIME_S + ho.DAILY_INTEL_CYCLE_BUDGET_SECONDS + 60
        <= ho.INTELLIGENCE_TASKS_TIMEOUT_SECONDS
    ), (
        "ACTIVE_HYPO_SCORING_MAX_RUNTIME_S + DAILY_INTEL_CYCLE_BUDGET_SECONDS + 60 "
        "must not exceed INTELLIGENCE_TASKS_TIMEOUT_SECONDS — otherwise the "
        "intelligence step times out before the daily-intel block ever gets a "
        "turn (regression of 2026-05-15, re-created one level down before this "
        "fix)."
    )


def test_active_hypo_scorer_alone_is_strictly_inside_step_budget() -> None:
    """Even on non-daily cycles the scorer must never be the thing that
    trips the step timeout."""
    assert ho.ACTIVE_HYPO_SCORING_MAX_RUNTIME_S < ho.INTELLIGENCE_TASKS_TIMEOUT_SECONDS


def test_intelligence_step_budget_is_inside_cycle_budget() -> None:
    """The step cap is a sub-budget of the cycle cap, not a replacement."""
    assert ho.INTELLIGENCE_TASKS_TIMEOUT_SECONDS < ho.CYCLE_TIMEOUT_SECONDS


def test_scorer_budget_leaves_headroom_for_batch_at_observed_throughput() -> None:
    """At the 2026-05-15 measured ~15 rows/sec, a full batch should need well
    under the runtime cap so the cap is a guard, not the normal exit path."""
    measured_rows_per_sec = 15.0
    expected_batch_seconds = ho.ACTIVE_HYPO_SCORING_BATCH_SIZE / measured_rows_per_sec
    assert expected_batch_seconds * 4 < ho.ACTIVE_HYPO_SCORING_MAX_RUNTIME_S


def test_sector_health_timeout_is_a_positive_int_independent_of_intelligence_budget() -> None:
    """SECTOR_HEALTH_TIMEOUT_SECONDS (2026-09-19 split — sector-health is
    now its own run_cycle step, dispatched ahead of intelligence_tasks; see
    _run_sector_and_intelligence_steps) must be a sane, positive timeout
    used by that step's dispatch, and must NOT be derived from or tied to
    INTELLIGENCE_TASKS_TIMEOUT_SECONDS — the whole point of the split is
    that the two budgets are independent, so sector-health is reachable
    and bounded regardless of whether intelligence_tasks times out."""
    assert isinstance(ho.SECTOR_HEALTH_TIMEOUT_SECONDS, int)
    assert ho.SECTOR_HEALTH_TIMEOUT_SECONDS > 0
    assert ho.SECTOR_HEALTH_TIMEOUT_SECONDS != ho.INTELLIGENCE_TASKS_TIMEOUT_SECONDS
    assert ho.SECTOR_HEALTH_TIMEOUT_SECONDS < ho.INTELLIGENCE_TASKS_TIMEOUT_SECONDS, (
        "sector-health is observed at 3-8s in production; it should stay a "
        "short step, not scale with the much larger intelligence_tasks budget"
    )
    assert ho.SECTOR_HEALTH_TIMEOUT_SECONDS < ho.CYCLE_TIMEOUT_SECONDS

    import inspect
    src = inspect.getsource(ho._run_sector_and_intelligence_steps)
    assert "SECTOR_HEALTH_TIMEOUT_SECONDS" in src, (
        "the sector-health dispatch must actually use the constant"
    )
