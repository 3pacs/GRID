"""Tests for the strategy-level circuit breaker."""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from trading.circuit_breaker import BreakerState, StrategyCircuitBreaker


@pytest.fixture
def mock_engine():
    """Create a mock SQLAlchemy engine with in-memory state."""
    engine = MagicMock()
    return engine


@pytest.fixture
def breaker(mock_engine):
    """Create a breaker with threshold=3, cooldown=1h for fast tests."""
    with patch("trading.circuit_breaker.settings") as mock_settings:
        mock_settings.CIRCUIT_BREAKER_THRESHOLD = 3
        mock_settings.CIRCUIT_BREAKER_COOLDOWN_HOURS = 1
        cb = StrategyCircuitBreaker(mock_engine, threshold=3, cooldown_hours=1)
    return cb


class TestBreakerStateTransitions:
    """Test the CLOSED -> OPEN -> HALF_OPEN -> CLOSED state machine."""

    def test_new_strategy_defaults_to_closed(self, breaker):
        """Unknown strategies should return CLOSED state."""
        conn_mock = MagicMock()
        conn_mock.execute.return_value.fetchone.return_value = None
        breaker.engine.connect.return_value.__enter__ = MagicMock(return_value=conn_mock)
        breaker.engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        state = breaker.get_state("new-strat")
        assert state["state"] == BreakerState.CLOSED
        assert state["consecutive_failures"] == 0

    def test_should_execute_returns_true_when_closed(self, breaker):
        """CLOSED state allows execution."""
        with patch.object(breaker, "get_state", return_value={
            "state": BreakerState.CLOSED,
            "consecutive_failures": 0,
            "last_failure_at": None,
            "opened_at": None,
        }):
            assert breaker.should_execute("strat-1") is True

    def test_should_execute_returns_false_when_open(self, breaker):
        """OPEN state blocks execution (before cooldown)."""
        now = datetime.now(timezone.utc)
        with patch.object(breaker, "get_state", return_value={
            "state": BreakerState.OPEN,
            "consecutive_failures": 3,
            "last_failure_at": now,
            "opened_at": now,
        }):
            assert breaker.should_execute("strat-1") is False

    def test_open_transitions_to_half_open_after_cooldown(self, breaker):
        """OPEN should auto-transition to HALF_OPEN after cooldown."""
        past = datetime.now(timezone.utc) - timedelta(hours=2)
        with patch.object(breaker, "get_state", return_value={
            "state": BreakerState.OPEN,
            "consecutive_failures": 3,
            "last_failure_at": past,
            "opened_at": past,
        }), patch.object(breaker, "_set_state") as mock_set:
            result = breaker.should_execute("strat-1")
            assert result is True
            mock_set.assert_called_once_with("strat-1", BreakerState.HALF_OPEN)

    def test_half_open_allows_execution(self, breaker):
        """HALF_OPEN state allows one probe trade."""
        with patch.object(breaker, "get_state", return_value={
            "state": BreakerState.HALF_OPEN,
            "consecutive_failures": 3,
            "last_failure_at": None,
            "opened_at": None,
        }):
            assert breaker.should_execute("strat-1") is True


class TestRecordSuccess:
    """Test that success resets the breaker."""

    def test_success_resets_to_closed(self, breaker):
        with patch.object(breaker, "get_state", return_value={
            "state": BreakerState.HALF_OPEN,
            "consecutive_failures": 3,
            "last_failure_at": None,
            "opened_at": None,
        }), patch.object(breaker, "_upsert") as mock_upsert:
            breaker.record_success("strat-1")
            mock_upsert.assert_called_once_with("strat-1", BreakerState.CLOSED, consecutive_failures=0)


class TestRecordFailure:
    """Test failure accumulation and breaker tripping."""

    def test_failure_below_threshold_stays_closed(self, breaker):
        with patch.object(breaker, "get_state", return_value={
            "state": BreakerState.CLOSED,
            "consecutive_failures": 1,
            "last_failure_at": None,
            "opened_at": None,
        }), patch.object(breaker, "_upsert") as mock_upsert, \
             patch.object(breaker, "_send_alert"):
            breaker.record_failure("strat-1", "test error")
            call_args = mock_upsert.call_args
            assert call_args[0][1] == BreakerState.CLOSED
            assert call_args[1]["consecutive_failures"] == 2

    def test_failure_at_threshold_trips_to_open(self, breaker):
        with patch.object(breaker, "get_state", return_value={
            "state": BreakerState.CLOSED,
            "consecutive_failures": 2,
            "last_failure_at": None,
            "opened_at": None,
        }), patch.object(breaker, "_upsert") as mock_upsert, \
             patch.object(breaker, "_send_alert"):
            breaker.record_failure("strat-1", "third failure")
            call_args = mock_upsert.call_args
            assert call_args[0][1] == BreakerState.OPEN
            assert call_args[1]["consecutive_failures"] == 3

    def test_half_open_failure_reopens(self, breaker):
        with patch.object(breaker, "get_state", return_value={
            "state": BreakerState.HALF_OPEN,
            "consecutive_failures": 3,
            "last_failure_at": None,
            "opened_at": None,
        }), patch.object(breaker, "_upsert") as mock_upsert, \
             patch.object(breaker, "_send_alert"):
            breaker.record_failure("strat-1", "probation fail")
            call_args = mock_upsert.call_args
            assert call_args[0][1] == BreakerState.OPEN


class TestManualReset:
    def test_reset_forces_closed(self, breaker):
        with patch.object(breaker, "_upsert") as mock_upsert:
            breaker.reset("strat-1")
            mock_upsert.assert_called_once_with("strat-1", BreakerState.CLOSED, consecutive_failures=0)
