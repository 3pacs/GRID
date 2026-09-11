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

from scripts import hermes_operator as ho


def test_active_hypo_scorer_fits_inside_intelligence_step_with_daily_batch() -> None:
    """Scorer budget + observed daily batch must fit inside the step budget.

    The scorer runs first and the daily block runs last inside the same
    step; if their combined worst case exceeds the step cap, the daily
    block (and hypothesis discovery) is unreachable on the days it matters.
    """
    assert (
        ho.ACTIVE_HYPO_SCORING_MAX_RUNTIME_S + ho.DAILY_INTEL_BATCH_OBSERVED_S
        <= ho.INTELLIGENCE_TASKS_TIMEOUT_SECONDS
    ), (
        "ACTIVE_HYPO_SCORING_MAX_RUNTIME_S + DAILY_INTEL_BATCH_OBSERVED_S must not "
        "exceed INTELLIGENCE_TASKS_TIMEOUT_SECONDS — otherwise the intelligence "
        "step times out before auto_discover() runs (regression of 2026-05-15)."
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
