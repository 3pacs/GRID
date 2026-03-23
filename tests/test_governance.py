"""
Tests for the GRID governance registry and promotion gate modules.

Uses unittest.mock to avoid real database connections. Tests cover the
ModelRegistry lifecycle state machine and GateChecker promotion requirements.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch, call

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_engine() -> MagicMock:
    """Return a MagicMock that behaves like a SQLAlchemy Engine.

    Supports both ``engine.connect()`` and ``engine.begin()`` as context
    managers, returning the same mock connection for inspection.
    """
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    engine._mock_conn = conn  # stash for test assertions
    return engine


def _row(*values):
    """Create a mock database row that supports integer indexing and _mapping."""
    row = MagicMock()
    row.__getitem__ = lambda self, idx: values[idx]
    row._mapping = {f"col{i}": v for i, v in enumerate(values)}
    return row


# ===========================================================================
# GateChecker tests
# ===========================================================================


class TestGateCheckerCandidateToShadow:
    """Tests for the CANDIDATE -> SHADOW gate check."""

    def test_passes_when_validation_run_and_hypothesis_passed(self):
        """Gate passes when validation_run_id is set and hypothesis is PASSED."""
        engine = _mock_engine()
        conn = engine._mock_conn

        # First query: model row (validation_run_id=42, hypothesis_id=7)
        # Second query: hypothesis row (state='PASSED')
        conn.execute.return_value.fetchone.side_effect = [
            _row(42, 7),
            _row("PASSED"),
        ]

        from validation.gates import GateChecker

        gc = GateChecker(db_engine=engine)
        result = gc.check_candidate_to_shadow(model_id=1)

        assert result["passed"] is True
        assert any("validation_run_id is set" in d for d in result["details"])
        assert any("PASSED" in d for d in result["details"])

    def test_fails_when_validation_run_id_missing(self):
        """Gate fails when validation_run_id is None."""
        engine = _mock_engine()
        conn = engine._mock_conn

        # validation_run_id=None, hypothesis_id=7
        conn.execute.return_value.fetchone.side_effect = [
            _row(None, 7),
            _row("PASSED"),
        ]

        from validation.gates import GateChecker

        gc = GateChecker(db_engine=engine)
        result = gc.check_candidate_to_shadow(model_id=1)

        assert result["passed"] is False
        assert any("validation_run_id is not set" in d for d in result["details"])

    def test_fails_when_hypothesis_not_passed(self):
        """Gate fails when the associated hypothesis is not in PASSED state."""
        engine = _mock_engine()
        conn = engine._mock_conn

        conn.execute.return_value.fetchone.side_effect = [
            _row(42, 7),
            _row("FAILED"),
        ]

        from validation.gates import GateChecker

        gc = GateChecker(db_engine=engine)
        result = gc.check_candidate_to_shadow(model_id=1)

        assert result["passed"] is False
        assert any("FAILED" in d for d in result["details"])

    def test_fails_when_hypothesis_not_found(self):
        """Gate fails when hypothesis_registry row is missing."""
        engine = _mock_engine()
        conn = engine._mock_conn

        conn.execute.return_value.fetchone.side_effect = [
            _row(42, 7),
            None,
        ]

        from validation.gates import GateChecker

        gc = GateChecker(db_engine=engine)
        result = gc.check_candidate_to_shadow(model_id=1)

        assert result["passed"] is False
        assert any("hypothesis not found" in d for d in result["details"])

    def test_fails_when_model_not_found(self):
        """Gate fails when the model itself does not exist."""
        engine = _mock_engine()
        conn = engine._mock_conn

        conn.execute.return_value.fetchone.return_value = None

        from validation.gates import GateChecker

        gc = GateChecker(db_engine=engine)
        result = gc.check_candidate_to_shadow(model_id=999)

        assert result["passed"] is False
        assert any("not found" in d for d in result["details"])


class TestGateCheckerShadowToStaging:
    """Tests for the SHADOW -> STAGING gate check."""

    def test_always_passes(self):
        """SHADOW -> STAGING only requires operator approval (always passes)."""
        engine = _mock_engine()

        from validation.gates import GateChecker

        gc = GateChecker(db_engine=engine)
        result = gc.check_shadow_to_staging(model_id=1)

        assert result["passed"] is True
        assert any("Operator approval" in d for d in result["details"])


class TestGateCheckerStagingToProduction:
    """Tests for the STAGING -> PRODUCTION gate check."""

    def test_passes_with_sufficient_journal_entries(self):
        """Gate passes when journal has >= 20 entries and no existing PRODUCTION."""
        engine = _mock_engine()
        conn = engine._mock_conn

        # model lookup -> layer='REGIME'
        # journal count -> 25
        # existing production -> None
        conn.execute.return_value.fetchone.side_effect = [
            _row("REGIME"),
            _row(25),
            None,
        ]

        from validation.gates import GateChecker

        gc = GateChecker(db_engine=engine)
        result = gc.check_staging_to_production(model_id=1)

        assert result["passed"] is True
        assert any("25 journal entries" in d for d in result["details"])

    def test_fails_with_insufficient_journal_entries(self):
        """Gate fails when journal has fewer than 20 entries."""
        engine = _mock_engine()
        conn = engine._mock_conn

        conn.execute.return_value.fetchone.side_effect = [
            _row("REGIME"),
            _row(5),
            None,
        ]

        from validation.gates import GateChecker

        gc = GateChecker(db_engine=engine)
        result = gc.check_staging_to_production(model_id=1)

        assert result["passed"] is False
        assert any("5/20" in d for d in result["details"])

    def test_passes_with_exactly_20_journal_entries(self):
        """Gate passes at the boundary of exactly 20 journal entries."""
        engine = _mock_engine()
        conn = engine._mock_conn

        conn.execute.return_value.fetchone.side_effect = [
            _row("TACTICAL"),
            _row(20),
            None,
        ]

        from validation.gates import GateChecker

        gc = GateChecker(db_engine=engine)
        result = gc.check_staging_to_production(model_id=1)

        assert result["passed"] is True

    def test_warns_about_existing_production_model(self):
        """Gate notes the existing PRODUCTION model that will be demoted."""
        engine = _mock_engine()
        conn = engine._mock_conn

        conn.execute.return_value.fetchone.side_effect = [
            _row("REGIME"),
            _row(30),
            _row(99, "old-model-v1"),  # existing production
        ]

        from validation.gates import GateChecker

        gc = GateChecker(db_engine=engine)
        result = gc.check_staging_to_production(model_id=1)

        # Still passes -- the existing model is a warning, not a blocker
        assert result["passed"] is True
        assert any("demoted" in d for d in result["details"])

    def test_fails_when_model_not_found(self):
        """Gate fails when the model does not exist."""
        engine = _mock_engine()
        conn = engine._mock_conn

        conn.execute.return_value.fetchone.return_value = None

        from validation.gates import GateChecker

        gc = GateChecker(db_engine=engine)
        result = gc.check_staging_to_production(model_id=999)

        assert result["passed"] is False
        assert any("not found" in d for d in result["details"])


class TestGateCheckerAllGates:
    """Tests for the check_all_gates dispatcher."""

    def test_dispatches_to_candidate_to_shadow(self):
        """check_all_gates('SHADOW') delegates to check_candidate_to_shadow."""
        engine = _mock_engine()

        from validation.gates import GateChecker

        gc = GateChecker(db_engine=engine)
        gc.check_candidate_to_shadow = MagicMock(
            return_value={"passed": True, "details": ["mock"]}
        )

        result = gc.check_all_gates(model_id=5, target_state="SHADOW")

        gc.check_candidate_to_shadow.assert_called_once_with(5)
        assert result["passed"] is True

    def test_dispatches_to_shadow_to_staging(self):
        """check_all_gates('STAGING') delegates to check_shadow_to_staging."""
        engine = _mock_engine()

        from validation.gates import GateChecker

        gc = GateChecker(db_engine=engine)
        gc.check_shadow_to_staging = MagicMock(
            return_value={"passed": True, "details": ["mock"]}
        )

        result = gc.check_all_gates(model_id=5, target_state="STAGING")

        gc.check_shadow_to_staging.assert_called_once_with(5)
        assert result["passed"] is True

    def test_dispatches_to_staging_to_production(self):
        """check_all_gates('PRODUCTION') delegates to check_staging_to_production."""
        engine = _mock_engine()

        from validation.gates import GateChecker

        gc = GateChecker(db_engine=engine)
        gc.check_staging_to_production = MagicMock(
            return_value={"passed": True, "details": ["mock"]}
        )

        result = gc.check_all_gates(model_id=5, target_state="PRODUCTION")

        gc.check_staging_to_production.assert_called_once_with(5)
        assert result["passed"] is True

    def test_no_gates_for_retired(self):
        """States like RETIRED have no gates and pass automatically."""
        engine = _mock_engine()

        from validation.gates import GateChecker

        gc = GateChecker(db_engine=engine)
        result = gc.check_all_gates(model_id=5, target_state="RETIRED")

        assert result["passed"] is True
        assert any("No gates defined" in d for d in result["details"])

    def test_no_gates_for_flagged(self):
        """FLAGGED state has no promotion gates and passes automatically."""
        engine = _mock_engine()

        from validation.gates import GateChecker

        gc = GateChecker(db_engine=engine)
        result = gc.check_all_gates(model_id=5, target_state="FLAGGED")

        assert result["passed"] is True
        assert any("No gates defined" in d for d in result["details"])


# ===========================================================================
# ModelRegistry tests
# ===========================================================================


class TestModelRegistryTransitionValid:
    """Tests for valid state transitions through the ModelRegistry."""

    def test_candidate_to_shadow(self):
        """CANDIDATE -> SHADOW succeeds when gates pass."""
        engine = _mock_engine()
        conn = engine._mock_conn

        # Model lookup: state='CANDIDATE', layer='REGIME', name='m1', version='v1'
        conn.execute.return_value.fetchone.return_value = _row(
            "CANDIDATE", "REGIME", "m1", "v1"
        )

        from governance.registry import ModelRegistry

        with patch.object(
            ModelRegistry, "__init__", lambda self, db_engine: None
        ):
            reg = ModelRegistry.__new__(ModelRegistry)
            reg.engine = engine
            reg.gate_checker = MagicMock()
            reg.gate_checker.check_all_gates.return_value = {
                "passed": True,
                "details": ["all good"],
            }

        result = reg.transition(
            model_id=1, new_state="SHADOW", operator_id="ops"
        )

        assert result is True
        reg.gate_checker.check_all_gates.assert_called_once_with(1, "SHADOW")

    def test_shadow_to_staging(self):
        """SHADOW -> STAGING succeeds when gates pass."""
        engine = _mock_engine()
        conn = engine._mock_conn

        conn.execute.return_value.fetchone.return_value = _row(
            "SHADOW", "TACTICAL", "m2", "v2"
        )

        from governance.registry import ModelRegistry

        with patch.object(
            ModelRegistry, "__init__", lambda self, db_engine: None
        ):
            reg = ModelRegistry.__new__(ModelRegistry)
            reg.engine = engine
            reg.gate_checker = MagicMock()
            reg.gate_checker.check_all_gates.return_value = {
                "passed": True,
                "details": [],
            }

        result = reg.transition(
            model_id=2, new_state="STAGING", operator_id="ops"
        )

        assert result is True

    def test_staging_to_production_demotes_existing(self):
        """STAGING -> PRODUCTION demotes the existing PRODUCTION model."""
        engine = _mock_engine()
        conn = engine._mock_conn

        # First call (connect context): model lookup
        # Subsequent calls (begin context): existing prod lookup, then updates
        # We need separate side effects for connect vs begin
        model_row = _row("STAGING", "REGIME", "m3", "v3")
        existing_prod_row = _row(77)

        # The connect() context manager returns fetchone for model lookup
        connect_conn = MagicMock()
        connect_conn.execute.return_value.fetchone.return_value = model_row
        engine.connect.return_value.__enter__ = MagicMock(return_value=connect_conn)

        # The begin() context manager handles the writes
        begin_conn = MagicMock()
        begin_conn.execute.return_value.fetchone.return_value = existing_prod_row
        engine.begin.return_value.__enter__ = MagicMock(return_value=begin_conn)

        from governance.registry import ModelRegistry

        with patch.object(
            ModelRegistry, "__init__", lambda self, db_engine: None
        ):
            reg = ModelRegistry.__new__(ModelRegistry)
            reg.engine = engine
            reg.gate_checker = MagicMock()
            reg.gate_checker.check_all_gates.return_value = {
                "passed": True,
                "details": [],
            }

        result = reg.transition(
            model_id=3, new_state="PRODUCTION", operator_id="admin"
        )

        assert result is True
        # Should have executed multiple statements: existing prod query, demote, promote
        assert begin_conn.execute.call_count >= 2


class TestModelRegistryTransitionInvalid:
    """Tests for invalid state transitions."""

    def test_candidate_to_production_raises(self):
        """CANDIDATE -> PRODUCTION is not allowed and raises ValueError."""
        engine = _mock_engine()
        conn = engine._mock_conn

        conn.execute.return_value.fetchone.return_value = _row(
            "CANDIDATE", "REGIME", "m1", "v1"
        )

        from governance.registry import ModelRegistry

        with patch.object(
            ModelRegistry, "__init__", lambda self, db_engine: None
        ):
            reg = ModelRegistry.__new__(ModelRegistry)
            reg.engine = engine
            reg.gate_checker = MagicMock()

        with pytest.raises(ValueError, match="Invalid transition"):
            reg.transition(
                model_id=1, new_state="PRODUCTION", operator_id="ops"
            )

    def test_candidate_to_staging_raises(self):
        """CANDIDATE -> STAGING is not allowed (must go through SHADOW)."""
        engine = _mock_engine()
        conn = engine._mock_conn

        conn.execute.return_value.fetchone.return_value = _row(
            "CANDIDATE", "REGIME", "m1", "v1"
        )

        from governance.registry import ModelRegistry

        with patch.object(
            ModelRegistry, "__init__", lambda self, db_engine: None
        ):
            reg = ModelRegistry.__new__(ModelRegistry)
            reg.engine = engine
            reg.gate_checker = MagicMock()

        with pytest.raises(ValueError, match="Invalid transition"):
            reg.transition(
                model_id=1, new_state="STAGING", operator_id="ops"
            )

    def test_shadow_to_production_raises(self):
        """SHADOW -> PRODUCTION is not allowed (must go through STAGING)."""
        engine = _mock_engine()
        conn = engine._mock_conn

        conn.execute.return_value.fetchone.return_value = _row(
            "SHADOW", "REGIME", "m1", "v1"
        )

        from governance.registry import ModelRegistry

        with patch.object(
            ModelRegistry, "__init__", lambda self, db_engine: None
        ):
            reg = ModelRegistry.__new__(ModelRegistry)
            reg.engine = engine
            reg.gate_checker = MagicMock()

        with pytest.raises(ValueError, match="Invalid transition"):
            reg.transition(
                model_id=1, new_state="PRODUCTION", operator_id="ops"
            )

    def test_model_not_found_raises(self):
        """Transition on a nonexistent model raises ValueError."""
        engine = _mock_engine()
        conn = engine._mock_conn

        conn.execute.return_value.fetchone.return_value = None

        from governance.registry import ModelRegistry

        with patch.object(
            ModelRegistry, "__init__", lambda self, db_engine: None
        ):
            reg = ModelRegistry.__new__(ModelRegistry)
            reg.engine = engine
            reg.gate_checker = MagicMock()

        with pytest.raises(ValueError, match="not found"):
            reg.transition(
                model_id=999, new_state="SHADOW", operator_id="ops"
            )

    def test_gate_failure_raises(self):
        """Transition raises ValueError when gate check fails."""
        engine = _mock_engine()
        conn = engine._mock_conn

        conn.execute.return_value.fetchone.return_value = _row(
            "CANDIDATE", "REGIME", "m1", "v1"
        )

        from governance.registry import ModelRegistry

        with patch.object(
            ModelRegistry, "__init__", lambda self, db_engine: None
        ):
            reg = ModelRegistry.__new__(ModelRegistry)
            reg.engine = engine
            reg.gate_checker = MagicMock()
            reg.gate_checker.check_all_gates.return_value = {
                "passed": False,
                "details": ["validation_run_id is not set"],
            }

        with pytest.raises(ValueError, match="Gate check failed"):
            reg.transition(
                model_id=1, new_state="SHADOW", operator_id="ops"
            )


class TestModelRegistryGetProductionModel:
    """Tests for the get_production_model method."""

    def test_returns_none_when_no_production_model(self):
        """Returns None when no PRODUCTION model exists for the layer."""
        engine = _mock_engine()
        conn = engine._mock_conn

        conn.execute.return_value.fetchone.return_value = None

        from governance.registry import ModelRegistry

        with patch.object(
            ModelRegistry, "__init__", lambda self, db_engine: None
        ):
            reg = ModelRegistry.__new__(ModelRegistry)
            reg.engine = engine
            reg.gate_checker = MagicMock()

        result = reg.get_production_model("REGIME")

        assert result is None

    def test_returns_model_dict_when_found(self):
        """Returns a dict representation when a PRODUCTION model exists."""
        engine = _mock_engine()
        conn = engine._mock_conn

        mock_row = MagicMock()
        mock_row._mapping = {
            "id": 1,
            "name": "regime-v3",
            "version": "3.0",
            "state": "PRODUCTION",
            "layer": "REGIME",
        }
        conn.execute.return_value.fetchone.return_value = mock_row

        from governance.registry import ModelRegistry

        with patch.object(
            ModelRegistry, "__init__", lambda self, db_engine: None
        ):
            reg = ModelRegistry.__new__(ModelRegistry)
            reg.engine = engine
            reg.gate_checker = MagicMock()

        result = reg.get_production_model("REGIME")

        assert result is not None
        assert result["name"] == "regime-v3"
        assert result["state"] == "PRODUCTION"
        assert result["layer"] == "REGIME"


class TestModelRegistryFlagModel:
    """Tests for the flag_model method."""

    def test_flag_production_model_succeeds(self):
        """Flagging a PRODUCTION model succeeds."""
        engine = _mock_engine()
        conn = engine._mock_conn

        conn.execute.return_value.fetchone.return_value = _row("PRODUCTION")

        from governance.registry import ModelRegistry

        with patch.object(
            ModelRegistry, "__init__", lambda self, db_engine: None
        ):
            reg = ModelRegistry.__new__(ModelRegistry)
            reg.engine = engine
            reg.gate_checker = MagicMock()

        result = reg.flag_model(model_id=1, reason="Performance degradation")

        assert result is True

    def test_flag_non_production_model_raises(self):
        """Flagging a model that is not in PRODUCTION raises ValueError."""
        engine = _mock_engine()
        conn = engine._mock_conn

        conn.execute.return_value.fetchone.return_value = _row("SHADOW")

        from governance.registry import ModelRegistry

        with patch.object(
            ModelRegistry, "__init__", lambda self, db_engine: None
        ):
            reg = ModelRegistry.__new__(ModelRegistry)
            reg.engine = engine
            reg.gate_checker = MagicMock()

        with pytest.raises(ValueError, match="Can only flag PRODUCTION"):
            reg.flag_model(model_id=1, reason="Bad performance")

    def test_flag_candidate_model_raises(self):
        """Flagging a CANDIDATE model raises ValueError."""
        engine = _mock_engine()
        conn = engine._mock_conn

        conn.execute.return_value.fetchone.return_value = _row("CANDIDATE")

        from governance.registry import ModelRegistry

        with patch.object(
            ModelRegistry, "__init__", lambda self, db_engine: None
        ):
            reg = ModelRegistry.__new__(ModelRegistry)
            reg.engine = engine
            reg.gate_checker = MagicMock()

        with pytest.raises(ValueError, match="Can only flag PRODUCTION"):
            reg.flag_model(model_id=1, reason="Drift detected")

    def test_flag_nonexistent_model_raises(self):
        """Flagging a nonexistent model raises ValueError."""
        engine = _mock_engine()
        conn = engine._mock_conn

        conn.execute.return_value.fetchone.return_value = None

        from governance.registry import ModelRegistry

        with patch.object(
            ModelRegistry, "__init__", lambda self, db_engine: None
        ):
            reg = ModelRegistry.__new__(ModelRegistry)
            reg.engine = engine
            reg.gate_checker = MagicMock()

        with pytest.raises(ValueError, match="not found"):
            reg.flag_model(model_id=999, reason="Gone")


class TestModelRegistryRollback:
    """Tests for the rollback method."""

    def test_rollback_retires_current_and_promotes_predecessor(self):
        """Rollback retires the current model and promotes its predecessor."""
        engine = _mock_engine()
        conn = engine._mock_conn

        # rollback() reads predecessor_id, layer, state
        conn.execute.return_value.fetchone.return_value = _row(
            10, "REGIME", "FLAGGED"
        )

        from governance.registry import ModelRegistry

        with patch.object(
            ModelRegistry, "__init__", lambda self, db_engine: None
        ):
            reg = ModelRegistry.__new__(ModelRegistry)
            reg.engine = engine
            reg.gate_checker = MagicMock()

        # Mock transition to avoid recursive DB calls
        reg.transition = MagicMock(return_value=True)

        result = reg.rollback(model_id=5, operator_id="admin")

        assert result is True

        # First transition call: retire the current model
        retire_call = reg.transition.call_args_list[0]
        assert retire_call == call(5, "RETIRED", "admin", reason="Rollback")

        # Second transition call: promote the predecessor
        promote_call = reg.transition.call_args_list[1]
        assert promote_call == call(
            10, "PRODUCTION", "admin", reason="Rollback promotion"
        )

    def test_rollback_no_predecessor_raises(self):
        """Rollback raises ValueError when there is no predecessor."""
        engine = _mock_engine()
        conn = engine._mock_conn

        # predecessor_id=None
        conn.execute.return_value.fetchone.return_value = _row(
            None, "REGIME", "FLAGGED"
        )

        from governance.registry import ModelRegistry

        with patch.object(
            ModelRegistry, "__init__", lambda self, db_engine: None
        ):
            reg = ModelRegistry.__new__(ModelRegistry)
            reg.engine = engine
            reg.gate_checker = MagicMock()

        with pytest.raises(ValueError, match="no predecessor"):
            reg.rollback(model_id=5, operator_id="admin")

    def test_rollback_model_not_found_raises(self):
        """Rollback raises ValueError when the model does not exist."""
        engine = _mock_engine()
        conn = engine._mock_conn

        conn.execute.return_value.fetchone.return_value = None

        from governance.registry import ModelRegistry

        with patch.object(
            ModelRegistry, "__init__", lambda self, db_engine: None
        ):
            reg = ModelRegistry.__new__(ModelRegistry)
            reg.engine = engine
            reg.gate_checker = MagicMock()

        with pytest.raises(ValueError, match="not found"):
            reg.rollback(model_id=999, operator_id="admin")


# ===========================================================================
# Valid transition map coverage
# ===========================================================================


class TestValidTransitionMap:
    """Tests verifying the _VALID_TRANSITIONS map is correct."""

    def test_candidate_can_go_to_shadow_or_retired(self):
        """CANDIDATE can transition to SHADOW or RETIRED."""
        from governance.registry import _VALID_TRANSITIONS

        assert _VALID_TRANSITIONS["CANDIDATE"] == {"SHADOW", "RETIRED"}

    def test_shadow_can_go_to_staging_or_retired(self):
        """SHADOW can transition to STAGING or RETIRED."""
        from governance.registry import _VALID_TRANSITIONS

        assert _VALID_TRANSITIONS["SHADOW"] == {"STAGING", "RETIRED"}

    def test_staging_can_go_to_production_or_retired(self):
        """STAGING can transition to PRODUCTION or RETIRED."""
        from governance.registry import _VALID_TRANSITIONS

        assert _VALID_TRANSITIONS["STAGING"] == {"PRODUCTION", "RETIRED"}

    def test_production_can_go_to_flagged_or_retired(self):
        """PRODUCTION can transition to FLAGGED or RETIRED."""
        from governance.registry import _VALID_TRANSITIONS

        assert _VALID_TRANSITIONS["PRODUCTION"] == {"FLAGGED", "RETIRED"}

    def test_flagged_can_go_to_retired_or_production(self):
        """FLAGGED can transition to RETIRED or back to PRODUCTION."""
        from governance.registry import _VALID_TRANSITIONS

        assert _VALID_TRANSITIONS["FLAGGED"] == {"RETIRED", "PRODUCTION"}
