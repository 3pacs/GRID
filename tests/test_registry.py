"""
Tests for governance/registry.py ModelRegistry.

Verifies model lifecycle state transitions, gate enforcement, demotion
logic, flagging, and production model queries without a live database.
"""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch, call

import pytest

from governance.registry import ModelRegistry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_UNSET = object()

def _make_mock_engine(connect_fetchone=_UNSET, connect_fetchall=None,
                      begin_fetchone=None, begin_side_effect=None):
    """Create a mock engine with configurable return values."""
    engine = MagicMock()

    # connect() context manager
    conn_mock = MagicMock()
    conn_result = MagicMock()
    if connect_fetchone is not _UNSET:
        conn_result.fetchone.return_value = connect_fetchone
    if connect_fetchall is not None:
        conn_result.fetchall.return_value = connect_fetchall
    conn_mock.execute.return_value = conn_result

    @contextmanager
    def connect_ctx():
        yield conn_mock
    engine.connect = connect_ctx

    # begin() context manager
    begin_mock = MagicMock()
    if begin_side_effect is not None:
        begin_mock.execute.side_effect = begin_side_effect
    else:
        begin_result = MagicMock()
        if begin_fetchone is not None:
            begin_result.fetchone.return_value = begin_fetchone
        begin_mock.execute.return_value = begin_result

    @contextmanager
    def begin_ctx():
        yield begin_mock
    engine.begin = begin_ctx

    return engine, conn_mock, begin_mock


def _make_gate_result(passed: bool, details: list[str] | None = None):
    return {"passed": passed, "details": details or []}


def _make_registry(engine, mock_gc=None):
    """Create a ModelRegistry without calling __init__."""
    registry = ModelRegistry.__new__(ModelRegistry)
    registry.engine = engine
    registry.gate_checker = mock_gc or MagicMock()
    return registry


# ---------------------------------------------------------------------------
# ModelRegistry.transition
# ---------------------------------------------------------------------------


class TestTransition:

    def test_transition_candidate_to_shadow(self):
        engine, conn_mock, begin_mock = _make_mock_engine(
            connect_fetchone=("CANDIDATE", "REGIME", "test-model", "1.0"),
        )
        mock_gc = MagicMock()
        mock_gc.check_all_gates.return_value = _make_gate_result(True)

        registry = _make_registry(engine, mock_gc)
        result = registry.transition(1, "SHADOW", "op-1")

        assert result is True
        mock_gc.check_all_gates.assert_called_once_with(1, "SHADOW")
        begin_mock.execute.assert_called_once()

    def test_transition_invalid(self):
        engine, _, _ = _make_mock_engine(
            connect_fetchone=("CANDIDATE", "REGIME", "test-model", "1.0"),
        )
        registry = _make_registry(engine)

        with pytest.raises(ValueError, match="Invalid transition"):
            registry.transition(1, "PRODUCTION", "op-1")

    def test_transition_model_not_found(self):
        engine, _, _ = _make_mock_engine(connect_fetchone=None)
        registry = _make_registry(engine)

        with pytest.raises(ValueError, match="not found"):
            registry.transition(99, "SHADOW", "op-1")

    def test_transition_gate_failure(self):
        engine, _, _ = _make_mock_engine(
            connect_fetchone=("CANDIDATE", "REGIME", "test-model", "1.0"),
        )
        mock_gc = MagicMock()
        mock_gc.check_all_gates.return_value = _make_gate_result(
            False, ["validation_run_id is not set"]
        )
        registry = _make_registry(engine, mock_gc)

        with pytest.raises(ValueError, match="Gate check failed"):
            registry.transition(1, "SHADOW", "op-1")

    def test_transition_to_production_demotes_existing(self):
        engine, _, _ = _make_mock_engine(
            connect_fetchone=("STAGING", "REGIME", "new-model", "2.0"),
        )
        mock_gc = MagicMock()
        mock_gc.check_all_gates.return_value = _make_gate_result(True)

        # begin() needs 3 calls: find existing prod, demote, update
        existing_result = MagicMock()
        existing_result.fetchone.return_value = (50,)
        ok_result = MagicMock()

        _, _, begin_mock = _make_mock_engine()
        begin_mock.execute.side_effect = [existing_result, ok_result, ok_result]

        # Rebuild engine with custom begin
        @contextmanager
        def connect_ctx():
            c = MagicMock()
            r = MagicMock()
            r.fetchone.return_value = ("STAGING", "REGIME", "new-model", "2.0")
            c.execute.return_value = r
            yield c

        @contextmanager
        def begin_ctx():
            yield begin_mock

        engine = MagicMock()
        engine.connect = connect_ctx
        engine.begin = begin_ctx

        registry = _make_registry(engine, mock_gc)
        result = registry.transition(10, "PRODUCTION", "op-1")

        assert result is True
        assert begin_mock.execute.call_count == 3

    def test_transition_to_retired_sets_reason(self):
        engine, _, begin_mock = _make_mock_engine(
            connect_fetchone=("CANDIDATE", "REGIME", "old-model", "1.0"),
        )
        mock_gc = MagicMock()
        mock_gc.check_all_gates.return_value = _make_gate_result(True)

        registry = _make_registry(engine, mock_gc)
        result = registry.transition(1, "RETIRED", "op-1", reason="Outdated")

        assert result is True
        execute_call = begin_mock.execute.call_args
        params = execute_call[0][1]
        assert params["reason"] == "Outdated"
        assert params["state"] == "RETIRED"


# ---------------------------------------------------------------------------
# ModelRegistry.flag_model
# ---------------------------------------------------------------------------


class TestFlagModel:

    def test_flag_production_model(self):
        engine, _, begin_mock = _make_mock_engine(
            connect_fetchone=("PRODUCTION",),
        )
        registry = _make_registry(engine)
        result = registry.flag_model(1, "Drift detected")

        assert result is True
        begin_mock.execute.assert_called_once()

    def test_flag_non_production_raises(self):
        engine, _, _ = _make_mock_engine(connect_fetchone=("SHADOW",))
        registry = _make_registry(engine)

        with pytest.raises(ValueError, match="PRODUCTION"):
            registry.flag_model(1, "Drift detected")

    def test_flag_not_found_raises(self):
        engine, _, _ = _make_mock_engine(connect_fetchone=None)
        registry = _make_registry(engine)

        with pytest.raises(ValueError, match="not found"):
            registry.flag_model(99, "Drift detected")


# ---------------------------------------------------------------------------
# ModelRegistry.get_production_model
# ---------------------------------------------------------------------------


class TestGetProductionModel:

    def test_get_production_model_found(self):
        row = MagicMock()
        row._mapping = {"id": 1, "name": "regime-v3", "layer": "REGIME", "state": "PRODUCTION"}
        engine, _, _ = _make_mock_engine(connect_fetchone=row)

        registry = _make_registry(engine)
        model = registry.get_production_model("REGIME")

        assert model is not None
        assert model["name"] == "regime-v3"

    def test_get_production_model_none(self):
        engine, _, _ = _make_mock_engine(connect_fetchone=None)
        registry = _make_registry(engine)

        model = registry.get_production_model("TACTICAL")
        assert model is None
