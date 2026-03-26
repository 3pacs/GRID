"""
Tests for validation/gates.py GateChecker.

Verifies gate logic for each model lifecycle transition without
requiring a live database connection.
"""

from __future__ import annotations

from unittest.mock import MagicMock, call

from validation.gates import GateChecker


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_mock_conn(mock_engine):
    """Return the mock connection wired to mock_engine.connect()."""
    return mock_engine.connect.return_value.__enter__.return_value


# ---------------------------------------------------------------------------
# CANDIDATE -> SHADOW
# ---------------------------------------------------------------------------


class TestCandidateToShadow:
    """Gate: validation_run_id set AND hypothesis state == PASSED."""

    def test_candidate_to_shadow_passes(self, mock_engine):
        conn = _get_mock_conn(mock_engine)
        model_row = MagicMock()
        model_row.__getitem__ = lambda self, idx: {0: 1, 1: 1}[idx]  # validation_run_id=1, hypothesis_id=1

        hyp_row = MagicMock()
        hyp_row.__getitem__ = lambda self, idx: {0: "PASSED"}[idx]

        result_model = MagicMock()
        result_model.fetchone.return_value = model_row

        result_hyp = MagicMock()
        result_hyp.fetchone.return_value = hyp_row

        conn.execute.side_effect = [result_model, result_hyp]

        gc = GateChecker(db_engine=mock_engine)
        result = gc.check_candidate_to_shadow(42)

        assert result["passed"] is True
        assert len(result["details"]) == 2

    def test_candidate_to_shadow_no_validation_run(self, mock_engine):
        conn = _get_mock_conn(mock_engine)
        model_row = MagicMock()
        model_row.__getitem__ = lambda self, idx: {0: None, 1: 1}[idx]

        hyp_row = MagicMock()
        hyp_row.__getitem__ = lambda self, idx: {0: "PASSED"}[idx]

        result_model = MagicMock()
        result_model.fetchone.return_value = model_row

        result_hyp = MagicMock()
        result_hyp.fetchone.return_value = hyp_row

        conn.execute.side_effect = [result_model, result_hyp]

        gc = GateChecker(db_engine=mock_engine)
        result = gc.check_candidate_to_shadow(42)

        assert result["passed"] is False
        assert any("validation_run_id" in d for d in result["details"])

    def test_candidate_to_shadow_hypothesis_not_passed(self, mock_engine):
        conn = _get_mock_conn(mock_engine)
        model_row = MagicMock()
        model_row.__getitem__ = lambda self, idx: {0: 1, 1: 1}[idx]

        hyp_row = MagicMock()
        hyp_row.__getitem__ = lambda self, idx: {0: "TESTING"}[idx]

        result_model = MagicMock()
        result_model.fetchone.return_value = model_row

        result_hyp = MagicMock()
        result_hyp.fetchone.return_value = hyp_row

        conn.execute.side_effect = [result_model, result_hyp]

        gc = GateChecker(db_engine=mock_engine)
        result = gc.check_candidate_to_shadow(42)

        assert result["passed"] is False
        assert any("TESTING" in d for d in result["details"])

    def test_candidate_to_shadow_model_not_found(self, mock_engine):
        conn = _get_mock_conn(mock_engine)
        result_none = MagicMock()
        result_none.fetchone.return_value = None
        conn.execute.side_effect = [result_none]

        gc = GateChecker(db_engine=mock_engine)
        result = gc.check_candidate_to_shadow(99)

        assert result["passed"] is False
        assert any("not found" in d for d in result["details"])

    def test_candidate_to_shadow_hypothesis_not_found(self, mock_engine):
        conn = _get_mock_conn(mock_engine)
        model_row = MagicMock()
        model_row.__getitem__ = lambda self, idx: {0: 1, 1: 1}[idx]

        result_model = MagicMock()
        result_model.fetchone.return_value = model_row

        result_hyp = MagicMock()
        result_hyp.fetchone.return_value = None

        conn.execute.side_effect = [result_model, result_hyp]

        gc = GateChecker(db_engine=mock_engine)
        result = gc.check_candidate_to_shadow(42)

        assert result["passed"] is False
        assert any("hypothesis not found" in d.lower() for d in result["details"])


# ---------------------------------------------------------------------------
# SHADOW -> STAGING
# ---------------------------------------------------------------------------


class TestShadowToStaging:

    def test_shadow_to_staging_rule_based_passes(self, mock_engine):
        """Rule-based models pass with soft gates (no artifact, no validation, low shadow days)."""
        conn = _get_mock_conn(mock_engine)

        # Query 1: model_type = 'rule_based'
        model_row = MagicMock()
        model_row.__getitem__ = lambda self, idx: {0: "rule_based"}[idx]
        result_model = MagicMock()
        result_model.fetchone.return_value = model_row

        # Query 2: validation_results = None (ok for rule_based)
        result_val = MagicMock()
        result_val.fetchone.return_value = None

        # Query 3: shadow_scores count = 0 (ok for rule_based)
        count_row = MagicMock()
        count_row.__getitem__ = lambda self, idx: {0: 0}[idx]
        result_count = MagicMock()
        result_count.fetchone.return_value = count_row

        conn.execute.side_effect = [result_model, result_val, result_count]

        gc = GateChecker(db_engine=mock_engine)
        result = gc.check_shadow_to_staging(1)

        assert result["passed"] is True
        assert len(result["details"]) > 0


# ---------------------------------------------------------------------------
# STAGING -> PRODUCTION
# ---------------------------------------------------------------------------


class TestStagingToProduction:

    def test_staging_to_production_passes(self, mock_engine):
        conn = _get_mock_conn(mock_engine)

        # model row with layer
        model_row = MagicMock()
        model_row.__getitem__ = lambda self, idx: {0: "REGIME"}[idx]
        result_model = MagicMock()
        result_model.fetchone.return_value = model_row

        # journal count = 25
        count_row = MagicMock()
        count_row.__getitem__ = lambda self, idx: {0: 25}[idx]
        result_count = MagicMock()
        result_count.fetchone.return_value = count_row

        # no existing prod model
        result_no_prod = MagicMock()
        result_no_prod.fetchone.return_value = None

        conn.execute.side_effect = [result_model, result_count, result_no_prod]

        gc = GateChecker(db_engine=mock_engine)
        result = gc.check_staging_to_production(10)

        assert result["passed"] is True
        assert any("25" in d for d in result["details"])

    def test_staging_to_production_insufficient_journal(self, mock_engine):
        conn = _get_mock_conn(mock_engine)

        model_row = MagicMock()
        model_row.__getitem__ = lambda self, idx: {0: "REGIME"}[idx]
        result_model = MagicMock()
        result_model.fetchone.return_value = model_row

        count_row = MagicMock()
        count_row.__getitem__ = lambda self, idx: {0: 5}[idx]
        result_count = MagicMock()
        result_count.fetchone.return_value = count_row

        result_no_prod = MagicMock()
        result_no_prod.fetchone.return_value = None

        conn.execute.side_effect = [result_model, result_count, result_no_prod]

        gc = GateChecker(db_engine=mock_engine)
        result = gc.check_staging_to_production(10)

        assert result["passed"] is False
        assert any("5/20" in d for d in result["details"])

    def test_staging_to_production_existing_prod_model(self, mock_engine):
        conn = _get_mock_conn(mock_engine)

        model_row = MagicMock()
        model_row.__getitem__ = lambda self, idx: {0: "REGIME"}[idx]
        result_model = MagicMock()
        result_model.fetchone.return_value = model_row

        count_row = MagicMock()
        count_row.__getitem__ = lambda self, idx: {0: 25}[idx]
        result_count = MagicMock()
        result_count.fetchone.return_value = count_row

        # existing prod model
        existing_row = MagicMock()
        existing_row.__getitem__ = lambda self, idx: {0: 99, 1: "old-model"}[idx]
        result_existing = MagicMock()
        result_existing.fetchone.return_value = existing_row

        conn.execute.side_effect = [result_model, result_count, result_existing]

        gc = GateChecker(db_engine=mock_engine)
        result = gc.check_staging_to_production(10)

        # Still passes — just warns about demotion
        assert result["passed"] is True
        assert any("demoted" in d.lower() for d in result["details"])


# ---------------------------------------------------------------------------
# check_all_gates dispatch
# ---------------------------------------------------------------------------


class TestCheckAllGates:

    def test_check_all_gates_dispatches_correctly(self, mock_engine):
        gc = GateChecker(db_engine=mock_engine)

        # SHADOW dispatches to check_candidate_to_shadow
        # Set up conn to return model not found so we get a quick result
        conn = _get_mock_conn(mock_engine)
        result_none = MagicMock()
        result_none.fetchone.return_value = None
        conn.execute.side_effect = [result_none]

        result = gc.check_all_gates(1, "SHADOW")
        # Should have called check_candidate_to_shadow which queries model_registry
        assert result["passed"] is False

        # STAGING dispatches to check_shadow_to_staging — mock rule_based path
        model_row = MagicMock()
        model_row.__getitem__ = lambda self, idx: {0: "rule_based"}[idx]
        result_model = MagicMock()
        result_model.fetchone.return_value = model_row

        result_val = MagicMock()
        result_val.fetchone.return_value = None

        count_row = MagicMock()
        count_row.__getitem__ = lambda self, idx: {0: 0}[idx]
        result_count = MagicMock()
        result_count.fetchone.return_value = count_row

        conn.execute.side_effect = [result_model, result_val, result_count]

        result = gc.check_all_gates(1, "STAGING")
        assert result["passed"] is True

    def test_check_all_gates_unknown_state_passes(self, mock_engine):
        gc = GateChecker(db_engine=mock_engine)
        result = gc.check_all_gates(1, "RETIRED")

        assert result["passed"] is True
        assert any("No gates defined" in d for d in result["details"])
