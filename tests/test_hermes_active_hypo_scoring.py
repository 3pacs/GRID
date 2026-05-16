"""Tests for the periodic active-hypothesis scoring tick wired into
``hermes_operator`` via ``intelligence.hypothesis_engine.score_due_active_hypotheses``.

Background
----------
Yesterday (2026-05-15) the boost_log direction-vocabulary bug was fixed
(PR #173) and a one-off ``/tmp/score_expiring.py`` re-scored ~9,340 backlog
hypos before today's expiry window. But the operator daemon itself never
iterates ``discovered_hypotheses WHERE status='active'`` on a schedule —
``hermes_operator.run_intelligence_tasks`` only calls
``HypothesisGenerator.auto_discover()`` to GENERATE new ones (every ~20h)
and ``analysis.backtest_scanner.review_existing_hypotheses`` (which is a
different, narrower review path).

The gap is recorded in the user-memory file
``project-active-hypo-scoring-gap`` and the
``handoff-pre-expiry-2026-05-16`` pre-expiry handoff. These tests pin the
batch/eval-window/timeout/counting behaviour so the wiring can't silently
regress.
"""
from __future__ import annotations

import time
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from intelligence.hypothesis_engine import score_due_active_hypotheses


def _stub_engine(due_ids: list[str]) -> MagicMock:
    """Build a SQLAlchemy-engine stub that yields the given hypothesis ids
    from the eval-window SELECT and accepts every other call as a no-op.

    The function under test runs exactly one SELECT (to fetch the batch)
    and then delegates scoring to ``HypothesisGenerator.score_hypothesis``
    which we patch separately.
    """
    fake_result = MagicMock()
    fake_result.fetchall.return_value = [(hid,) for hid in due_ids]

    fake_conn = MagicMock()
    fake_conn.execute.return_value = fake_result
    fake_conn.__enter__ = MagicMock(return_value=fake_conn)
    fake_conn.__exit__ = MagicMock(return_value=False)

    engine = MagicMock()
    engine.connect.return_value = fake_conn
    engine.begin.return_value = fake_conn
    return engine


def _score_result(outcome: str) -> dict[str, Any]:
    """Mimic ``HypothesisGenerator.score_hypothesis`` happy-path return."""
    return {
        "id": "hyp_x",
        "thesis": "stub",
        "outcome": outcome,
        "confidence": 0.5,
        "status": "active",
        "times_tested": 1,
        "times_correct": 1 if outcome == "confirmed" else 0,
        "kill_reason": None,
    }


class TestEvalWindowFilter:
    """The SELECT must filter to status='active' AND eval window closed."""

    def test_query_uses_active_status_and_window_days(self) -> None:
        engine = _stub_engine([])
        score_due_active_hypotheses(engine, batch_size=10, max_runtime_s=5)

        # First (and only) DB call must be the eval-window query with batch_size bound.
        conn = engine.connect.return_value
        assert conn.execute.call_count == 1
        sql_clause = str(conn.execute.call_args.args[0])
        params = conn.execute.call_args.args[1]

        assert "status = 'active'" in sql_clause
        assert "test_criteria->>'window_days'" in sql_clause
        assert "make_interval" in sql_clause  # parameterized interval — never f-string
        assert "ORDER BY last_tested ASC NULLS FIRST" in sql_clause
        assert "LIMIT :batch_size" in sql_clause
        assert params == {"batch_size": 10}

    def test_empty_due_returns_zero_counts(self) -> None:
        engine = _stub_engine([])
        out = score_due_active_hypotheses(engine, batch_size=50, max_runtime_s=10)
        assert out["scanned"] == 0
        assert out["scored"] == 0
        assert out["confirmed"] == 0
        assert out["invalidated"] == 0
        assert out["inconclusive"] == 0


class TestBatchAndTimeout:
    """Batch semantics: respect batch_size; stop early on max_runtime_s."""

    def test_scores_full_batch_when_under_time_budget(self) -> None:
        engine = _stub_engine(["h1", "h2", "h3"])
        with patch(
            "intelligence.hypothesis_engine.HypothesisGenerator.score_hypothesis",
            side_effect=[
                _score_result("confirmed"),
                _score_result("invalidated"),
                _score_result("inconclusive"),
            ],
        ) as mock_score:
            out = score_due_active_hypotheses(engine, batch_size=10, max_runtime_s=30)

        assert mock_score.call_count == 3
        assert out["scanned"] == 3
        assert out["scored"] == 3
        assert out["confirmed"] == 1
        assert out["invalidated"] == 1
        assert out["inconclusive"] == 1
        assert out["timed_out"] is False
        assert out["errors"] == 0

    def test_stops_on_max_runtime_breach(self) -> None:
        engine = _stub_engine(["h1", "h2", "h3", "h4"])

        # Force the timer to look like 1s passed per row so the 2nd row trips
        # max_runtime_s=2. We patch time.monotonic to give a deterministic
        # sequence: 0 (start), 1 (after h1), 2 (after h2 — trips break).
        seq = iter([0.0, 1.0, 2.0, 3.0, 4.0])
        with patch("intelligence.hypothesis_engine.time.monotonic", side_effect=lambda: next(seq)), \
             patch(
                "intelligence.hypothesis_engine.HypothesisGenerator.score_hypothesis",
                side_effect=lambda hid: _score_result("confirmed"),
             ) as mock_score:
            out = score_due_active_hypotheses(engine, batch_size=10, max_runtime_s=2)

        # The loop checks elapsed BEFORE scoring each row, so we expect
        # the 1st row scored at t=1 (elapsed=1<2), the 2nd loop check sees
        # elapsed=2>=2 and breaks. So at most 1 score call before timeout.
        assert mock_score.call_count <= 2
        assert out["timed_out"] is True

    def test_invalid_batch_size_raises(self) -> None:
        engine = _stub_engine([])
        with pytest.raises(ValueError):
            score_due_active_hypotheses(engine, batch_size=0, max_runtime_s=10)
        with pytest.raises(ValueError):
            score_due_active_hypotheses(engine, batch_size=10, max_runtime_s=0)


class TestStatusTransitionAccounting:
    """Counts must correctly bucket each outcome from score_hypothesis."""

    def test_counts_each_outcome(self) -> None:
        engine = _stub_engine(["h1", "h2", "h3", "h4", "h5"])
        with patch(
            "intelligence.hypothesis_engine.HypothesisGenerator.score_hypothesis",
            side_effect=[
                _score_result("confirmed"),
                _score_result("confirmed"),
                _score_result("invalidated"),
                _score_result("inconclusive"),
                _score_result("inconclusive"),
            ],
        ):
            out = score_due_active_hypotheses(engine, batch_size=10, max_runtime_s=60)

        assert out["confirmed"] == 2
        assert out["invalidated"] == 1
        assert out["inconclusive"] == 2
        assert out["scored"] == 5

    def test_skipped_non_active_rows_not_double_counted(self) -> None:
        """If score_hypothesis returns the 'not active, skipping' branch the
        row counts toward ``skipped_non_active`` and NOT toward outcomes.

        This happens when a row's status flips during the batch (e.g. a
        confirmed thesis killed its antithesis between the SELECT and the
        per-row score call)."""
        engine = _stub_engine(["h1", "h2"])
        with patch(
            "intelligence.hypothesis_engine.HypothesisGenerator.score_hypothesis",
            side_effect=[
                {"id": "h1", "status": "invalidated",
                 "message": "not active, skipping"},
                _score_result("confirmed"),
            ],
        ):
            out = score_due_active_hypotheses(engine, batch_size=10, max_runtime_s=60)

        assert out["skipped_non_active"] == 1
        assert out["confirmed"] == 1
        assert out["scored"] == 1  # only the real one counted as scored

    def test_per_row_exception_isolated(self) -> None:
        """A score_hypothesis() exception on one row must not abort the batch."""
        engine = _stub_engine(["h1", "h2", "h3"])
        with patch(
            "intelligence.hypothesis_engine.HypothesisGenerator.score_hypothesis",
            side_effect=[
                _score_result("confirmed"),
                RuntimeError("boom"),
                _score_result("invalidated"),
            ],
        ):
            out = score_due_active_hypotheses(engine, batch_size=10, max_runtime_s=60)

        assert out["scored"] == 2
        assert out["confirmed"] == 1
        assert out["invalidated"] == 1
        assert out["errors"] == 1

    def test_score_hypothesis_error_dict_counted(self) -> None:
        """``score_hypothesis`` returns ``{'error': '...'}`` if the id is
        missing — must bump errors and not score."""
        engine = _stub_engine(["missing1"])
        with patch(
            "intelligence.hypothesis_engine.HypothesisGenerator.score_hypothesis",
            return_value={"error": "hypothesis missing1 not found"},
        ):
            out = score_due_active_hypotheses(engine, batch_size=5, max_runtime_s=10)

        assert out["errors"] == 1
        assert out["scored"] == 0


class TestDueQueryFailureIsGraceful:
    """A DB error on the eval-window SELECT must be logged + counted, not
    raised. The operator already wraps the call in try/except via
    ``_run_intel_task`` but defence in depth matters here because this loop
    runs every 30 minutes against a 25k-row table."""

    def test_select_failure_counted_as_error(self) -> None:
        engine = MagicMock()
        conn = MagicMock()
        conn.__enter__ = MagicMock(return_value=conn)
        conn.__exit__ = MagicMock(return_value=False)
        conn.execute.side_effect = RuntimeError("DB exploded")
        engine.connect.return_value = conn

        out = score_due_active_hypotheses(engine, batch_size=10, max_runtime_s=5)
        assert out["errors"] >= 1
        assert out["scored"] == 0
        assert out["scanned"] == 0
