"""
Tests for the GRID decision journal.

Verifies journal entry creation, outcome immutability, and input validation.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

from journal.log import DecisionJournal


@pytest.fixture
def test_engine(pg_engine):
    """Set up journal test data using the shared pg_engine fixture.

    Creates a test hypothesis and model for use in journal tests.
    Cleans up after the test.
    """
    engine = pg_engine

    # Create test hypothesis and model
    with engine.begin() as conn:
        # Clean up any prior test data
        conn.execute(
            text("DELETE FROM decision_journal WHERE annotation = 'TEST_JOURNAL'")
        )
        conn.execute(
            text("DELETE FROM model_registry WHERE name = 'test_journal_model'")
        )
        conn.execute(
            text("DELETE FROM hypothesis_registry WHERE statement = 'Test journal hypothesis'")
        )

        # Create test hypothesis
        hyp_id = conn.execute(
            text(
                "INSERT INTO hypothesis_registry "
                "(statement, layer, feature_ids, lag_structure, "
                "proposed_metric, proposed_threshold, state) "
                "VALUES ('Test journal hypothesis', 'REGIME', '{1}', "
                "'{}'::jsonb, 'sharpe', 0.5, 'PASSED') "
                "RETURNING id"
            )
        ).fetchone()[0]

        # Create test model
        model_id = conn.execute(
            text(
                "INSERT INTO model_registry "
                "(name, layer, version, state, hypothesis_id, "
                "feature_set, parameter_snapshot) "
                "VALUES ('test_journal_model', 'REGIME', '1.0', "
                "'CANDIDATE', :hid, '{1}', '{}'::jsonb) "
                "RETURNING id"
            ),
            {"hid": hyp_id},
        ).fetchone()[0]

    yield engine, model_id

    # Clean up
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM decision_journal WHERE annotation = 'TEST_JOURNAL'")
        )
        conn.execute(
            text("DELETE FROM model_registry WHERE name = 'test_journal_model'")
        )
        conn.execute(
            text("DELETE FROM hypothesis_registry WHERE statement = 'Test journal hypothesis'")
        )


class TestLogDecision:
    """Tests for logging decisions."""

    def test_log_decision_returns_id(self, test_engine):
        """log_decision should return a positive integer ID."""
        engine, model_id = test_engine
        journal = DecisionJournal(engine)

        decision_id = journal.log_decision(
            model_version_id=model_id,
            inferred_state="EXPANSION",
            state_confidence=0.85,
            transition_probability=0.15,
            contradiction_flags={},
            grid_recommendation="INCREASE_EQUITY",
            baseline_recommendation="HOLD",
            action_taken="INCREASE_EQUITY",
            counterfactual="Would have held",
            operator_confidence="HIGH",
        )

        assert isinstance(decision_id, int)
        assert decision_id > 0

        # Clean up by marking with annotation
        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE decision_journal SET annotation = 'TEST_JOURNAL' "
                    "WHERE id = :id"
                ),
                {"id": decision_id},
            )


class TestRecordOutcome:
    """Tests for recording outcomes."""

    def test_record_outcome_immutable(self, test_engine):
        """Recording a second outcome should raise ValueError."""
        engine, model_id = test_engine
        journal = DecisionJournal(engine)

        # Log a decision
        decision_id = journal.log_decision(
            model_version_id=model_id,
            inferred_state="CONTRACTION",
            state_confidence=0.70,
            transition_probability=0.30,
            contradiction_flags={"vix_vs_credit": True},
            grid_recommendation="REDUCE_EQUITY",
            baseline_recommendation="HOLD",
            action_taken="REDUCE_EQUITY",
            counterfactual="Would have held through drawdown",
            operator_confidence="MEDIUM",
        )

        # Mark for cleanup
        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE decision_journal SET annotation = 'TEST_JOURNAL' "
                    "WHERE id = :id"
                ),
                {"id": decision_id},
            )

        # Record first outcome — should succeed
        result = journal.record_outcome(
            decision_id=decision_id,
            outcome_value=-0.02,
            verdict="HELPED",
        )
        assert result is True

        # Record second outcome — should raise
        with pytest.raises(ValueError, match="Outcome already recorded"):
            journal.record_outcome(
                decision_id=decision_id,
                outcome_value=0.01,
                verdict="HARMED",
            )

    def test_invalid_verdict_rejected(self, test_engine):
        """An invalid verdict should raise ValueError."""
        engine, model_id = test_engine
        journal = DecisionJournal(engine)

        decision_id = journal.log_decision(
            model_version_id=model_id,
            inferred_state="EXPANSION",
            state_confidence=0.90,
            transition_probability=0.10,
            contradiction_flags={},
            grid_recommendation="HOLD",
            baseline_recommendation="HOLD",
            action_taken="HOLD",
            counterfactual="Same action",
            operator_confidence="LOW",
        )

        # Mark for cleanup
        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE decision_journal SET annotation = 'TEST_JOURNAL' "
                    "WHERE id = :id"
                ),
                {"id": decision_id},
            )

        with pytest.raises(ValueError, match="Invalid verdict"):
            journal.record_outcome(
                decision_id=decision_id,
                outcome_value=0.01,
                verdict="WRONG",
            )
