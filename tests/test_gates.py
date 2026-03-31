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


def _make_row(*values):
    """Create a minimal mock row supporting __getitem__ access by index."""
    row = MagicMock()
    row.__getitem__ = lambda self, idx: {i: v for i, v in enumerate(values)}[idx]
    return row


def _make_result(row):
    """Wrap a row in a mock execute result."""
    result = MagicMock()
    result.fetchone.return_value = row
    return result


def _make_count_result(n):
    """Return a mock result whose fetchone()[0] is n."""
    return _make_result(_make_row(n))


# ---------------------------------------------------------------------------
# GateChecker init
# ---------------------------------------------------------------------------


class TestGateCheckerInit:

    def test_stores_engine(self):
        engine = MagicMock()
        gc = GateChecker(db_engine=engine)
        assert gc.engine is engine


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

    def test_candidate_to_shadow_hypothesis_failed_state(self, mock_engine):
        """A FAILED hypothesis must block the CANDIDATE -> SHADOW gate."""
        conn = _get_mock_conn(mock_engine)
        model_row = MagicMock()
        model_row.__getitem__ = lambda self, idx: {0: 5, 1: 10}[idx]

        hyp_row = MagicMock()
        hyp_row.__getitem__ = lambda self, idx: {0: "FAILED"}[idx]

        result_model = MagicMock()
        result_model.fetchone.return_value = model_row
        result_hyp = MagicMock()
        result_hyp.fetchone.return_value = hyp_row

        conn.execute.side_effect = [result_model, result_hyp]

        gc = GateChecker(db_engine=mock_engine)
        result = gc.check_candidate_to_shadow(7)

        assert result["passed"] is False
        assert any("FAILED" in d for d in result["details"])

    def test_candidate_to_shadow_hypothesis_draft_state(self, mock_engine):
        """A DRAFT hypothesis must block the gate (not promoted yet)."""
        conn = _get_mock_conn(mock_engine)
        model_row = MagicMock()
        model_row.__getitem__ = lambda self, idx: {0: 5, 1: 10}[idx]
        result_model = MagicMock()
        result_model.fetchone.return_value = model_row

        hyp_row = MagicMock()
        hyp_row.__getitem__ = lambda self, idx: {0: "DRAFT"}[idx]
        result_hyp = MagicMock()
        result_hyp.fetchone.return_value = hyp_row

        conn.execute.side_effect = [result_model, result_hyp]

        gc = GateChecker(db_engine=mock_engine)
        result = gc.check_candidate_to_shadow(7)

        assert result["passed"] is False

    def test_candidate_to_shadow_details_always_list(self, mock_engine):
        """The details field must always be a list of strings."""
        conn = _get_mock_conn(mock_engine)
        result_none = MagicMock()
        result_none.fetchone.return_value = None
        conn.execute.side_effect = [result_none]

        gc = GateChecker(db_engine=mock_engine)
        result = gc.check_candidate_to_shadow(1)

        assert isinstance(result["details"], list)
        for item in result["details"]:
            assert isinstance(item, str)


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

    def test_shadow_to_staging_trained_model_no_artifact_fails(self, mock_engine):
        """A trained model without an artifact must fail the artifact gate."""
        conn = _get_mock_conn(mock_engine)

        model_row = _make_row("xgboost")
        result_model = _make_result(model_row)

        # No artifact found
        result_artifact = _make_result(None)

        # Validation verdict = PASS
        val_row = _make_row("PASS")
        result_val = _make_result(val_row)

        # 20 days of shadow
        result_count = _make_count_result(20)

        conn.execute.side_effect = [result_model, result_artifact, result_val, result_count]

        gc = GateChecker(db_engine=mock_engine)
        result = gc.check_shadow_to_staging(5)

        assert result["passed"] is False
        assert any("artifact" in d.lower() for d in result["details"])

    def test_shadow_to_staging_trained_model_with_artifact_passes(self, mock_engine):
        """A trained model with artifact, passing verdict, and 14+ days must pass."""
        conn = _get_mock_conn(mock_engine)

        model_row = _make_row("random_forest")
        result_model = _make_result(model_row)

        # Artifact found
        artifact_row = _make_row(99)
        result_artifact = _make_result(artifact_row)

        # Validation verdict = PASS
        val_row = _make_row("PASS")
        result_val = _make_result(val_row)

        # 20 days of shadow
        result_count = _make_count_result(20)

        conn.execute.side_effect = [result_model, result_artifact, result_val, result_count]

        gc = GateChecker(db_engine=mock_engine)
        result = gc.check_shadow_to_staging(5)

        assert result["passed"] is True

    def test_shadow_to_staging_conditional_verdict_allowed(self, mock_engine):
        """CONDITIONAL validation verdict is acceptable (as well as PASS)."""
        conn = _get_mock_conn(mock_engine)

        model_row = _make_row("rule_based")
        result_model = _make_result(model_row)

        val_row = _make_row("CONDITIONAL")
        result_val = _make_result(val_row)

        result_count = _make_count_result(20)

        conn.execute.side_effect = [result_model, result_val, result_count]

        gc = GateChecker(db_engine=mock_engine)
        result = gc.check_shadow_to_staging(3)

        assert result["passed"] is True
        assert any("CONDITIONAL" in d for d in result["details"])

    def test_shadow_to_staging_fail_verdict_blocks_trained(self, mock_engine):
        """FAIL validation verdict must block a trained model."""
        conn = _get_mock_conn(mock_engine)

        model_row = _make_row("xgboost")
        result_model = _make_result(model_row)

        artifact_row = _make_row(10)
        result_artifact = _make_result(artifact_row)

        val_row = _make_row("FAIL")
        result_val = _make_result(val_row)

        result_count = _make_count_result(20)

        conn.execute.side_effect = [result_model, result_artifact, result_val, result_count]

        gc = GateChecker(db_engine=mock_engine)
        result = gc.check_shadow_to_staging(5)

        assert result["passed"] is False
        assert any("FAIL" in d for d in result["details"])

    def test_shadow_to_staging_insufficient_shadow_days_blocks_trained(self, mock_engine):
        """Fewer than 14 shadow days blocks a trained model."""
        conn = _get_mock_conn(mock_engine)

        model_row = _make_row("xgboost")
        result_model = _make_result(model_row)

        artifact_row = _make_row(10)
        result_artifact = _make_result(artifact_row)

        val_row = _make_row("PASS")
        result_val = _make_result(val_row)

        # Only 7 days
        result_count = _make_count_result(7)

        conn.execute.side_effect = [result_model, result_artifact, result_val, result_count]

        gc = GateChecker(db_engine=mock_engine)
        result = gc.check_shadow_to_staging(5)

        assert result["passed"] is False
        assert any("7/14" in d for d in result["details"])

    def test_shadow_to_staging_exactly_14_days_passes(self, mock_engine):
        """Exactly 14 days of shadow scoring meets the gate requirement."""
        conn = _get_mock_conn(mock_engine)

        model_row = _make_row("xgboost")
        result_model = _make_result(model_row)

        artifact_row = _make_row(10)
        result_artifact = _make_result(artifact_row)

        val_row = _make_row("PASS")
        result_val = _make_result(val_row)

        result_count = _make_count_result(14)

        conn.execute.side_effect = [result_model, result_artifact, result_val, result_count]

        gc = GateChecker(db_engine=mock_engine)
        result = gc.check_shadow_to_staging(5)

        assert result["passed"] is True

    def test_shadow_to_staging_model_not_found(self, mock_engine):
        """Missing model record must return passed=False immediately."""
        conn = _get_mock_conn(mock_engine)
        conn.execute.side_effect = [_make_result(None)]

        gc = GateChecker(db_engine=mock_engine)
        result = gc.check_shadow_to_staging(999)

        assert result["passed"] is False
        assert any("not found" in d for d in result["details"])

    def test_shadow_to_staging_always_appends_operator_approval(self, mock_engine):
        """The details list must always mention operator approval."""
        conn = _get_mock_conn(mock_engine)

        model_row = _make_row("rule_based")
        result_model = _make_result(model_row)
        result_val = _make_result(None)
        result_count = _make_count_result(0)

        conn.execute.side_effect = [result_model, result_val, result_count]

        gc = GateChecker(db_engine=mock_engine)
        result = gc.check_shadow_to_staging(1)

        assert any("operator" in d.lower() for d in result["details"])

    def test_shadow_to_staging_none_model_type_defaults_rule_based(self, mock_engine):
        """A NULL model_type in the DB should behave as rule_based."""
        conn = _get_mock_conn(mock_engine)

        # model_type returns None
        model_row = _make_row(None)
        result_model = _make_result(model_row)
        result_val = _make_result(None)
        result_count = _make_count_result(0)

        conn.execute.side_effect = [result_model, result_val, result_count]

        gc = GateChecker(db_engine=mock_engine)
        result = gc.check_shadow_to_staging(1)

        # Should pass (rule_based soft gates)
        assert result["passed"] is True


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

    def test_staging_to_production_exactly_20_journal_passes(self, mock_engine):
        """Exactly 20 journal entries meets the >= 20 requirement."""
        conn = _get_mock_conn(mock_engine)

        model_row = _make_row("ALPHA")
        result_model = _make_result(model_row)
        result_count = _make_count_result(20)
        result_no_prod = _make_result(None)

        conn.execute.side_effect = [result_model, result_count, result_no_prod]

        gc = GateChecker(db_engine=mock_engine)
        result = gc.check_staging_to_production(10)

        assert result["passed"] is True

    def test_staging_to_production_zero_journal_fails(self, mock_engine):
        """Zero journal entries must fail the gate."""
        conn = _get_mock_conn(mock_engine)

        model_row = _make_row("REGIME")
        result_model = _make_result(model_row)
        result_count = _make_count_result(0)
        result_no_prod = _make_result(None)

        conn.execute.side_effect = [result_model, result_count, result_no_prod]

        gc = GateChecker(db_engine=mock_engine)
        result = gc.check_staging_to_production(10)

        assert result["passed"] is False
        assert any("0/20" in d for d in result["details"])

    def test_staging_to_production_model_not_found(self, mock_engine):
        """Missing model returns passed=False immediately."""
        conn = _get_mock_conn(mock_engine)
        conn.execute.side_effect = [_make_result(None)]

        gc = GateChecker(db_engine=mock_engine)
        result = gc.check_staging_to_production(404)

        assert result["passed"] is False
        assert any("not found" in d for d in result["details"])

    def test_staging_to_production_layer_is_used_in_exclusion_query(self, mock_engine):
        """The existing-production check must use the correct layer."""
        conn = _get_mock_conn(mock_engine)

        model_row = _make_row("LIQUIDITY")
        result_model = _make_result(model_row)
        result_count = _make_count_result(25)
        result_no_prod = _make_result(None)

        conn.execute.side_effect = [result_model, result_count, result_no_prod]

        gc = GateChecker(db_engine=mock_engine)
        result = gc.check_staging_to_production(10)

        # Verify "LIQUIDITY" layer appears in one of the SQL calls
        call_args = [str(c) for c in conn.execute.call_args_list]
        assert any("LIQUIDITY" in str(args) for args in conn.execute.call_args_list)

    def test_staging_to_production_details_always_list(self, mock_engine):
        """The details field must always be a list."""
        conn = _get_mock_conn(mock_engine)
        conn.execute.side_effect = [_make_result(None)]

        gc = GateChecker(db_engine=mock_engine)
        result = gc.check_staging_to_production(1)

        assert isinstance(result["details"], list)


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

    def test_check_all_gates_production_dispatches_correctly(self, mock_engine):
        """check_all_gates(PRODUCTION) must delegate to check_staging_to_production."""
        conn = _get_mock_conn(mock_engine)

        model_row = _make_row("REGIME")
        result_model = _make_result(model_row)
        result_count = _make_count_result(25)
        result_no_prod = _make_result(None)

        conn.execute.side_effect = [result_model, result_count, result_no_prod]

        gc = GateChecker(db_engine=mock_engine)
        result = gc.check_all_gates(10, "PRODUCTION")

        assert result["passed"] is True

    def test_check_all_gates_flagged_state_passes_without_gates(self, mock_engine):
        """FLAGGED state has no defined gates and should pass unconditionally."""
        gc = GateChecker(db_engine=mock_engine)
        result = gc.check_all_gates(1, "FLAGGED")

        assert result["passed"] is True
        assert any("No gates defined" in d for d in result["details"])

    def test_check_all_gates_candidate_state_passes_without_gates(self, mock_engine):
        """CANDIDATE is the initial state and has no promotion gates."""
        gc = GateChecker(db_engine=mock_engine)
        result = gc.check_all_gates(1, "CANDIDATE")

        assert result["passed"] is True

    def test_check_all_gates_result_always_has_passed_and_details(self, mock_engine):
        """Every result from check_all_gates must contain 'passed' and 'details'."""
        gc = GateChecker(db_engine=mock_engine)
        for state in ("SHADOW", "STAGING", "PRODUCTION", "RETIRED", "FLAGGED"):
            conn = _get_mock_conn(mock_engine)
            # Reset side effects to prevent exhaustion across states
            conn.execute.side_effect = None
            conn.execute.return_value = _make_result(None)

            result = gc.check_all_gates(1, state)
            assert "passed" in result, f"'passed' missing for state {state}"
            assert "details" in result, f"'details' missing for state {state}"
