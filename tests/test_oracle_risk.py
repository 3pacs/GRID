"""
Tests for ``oracle.risk`` — the salvaged pre-trade circuit breaker gate.

Covers:
  - Singleton lifecycle (get/reset)
  - check_recommendation happy path + HOLD bypass
  - Kill-switch activation/reset
  - Daily loss / exposure / max-positions refusals
  - Warning list at 80% exposure
  - record_outcome triggering automatic halt
  - Cooldown behaviour after a halt
  - get_status / get_events / to_risk_config shapes
  - Defensive edge cases (NaN confidence, zero positions, etc.)
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone

import pytest

from oracle.risk import (
    CircuitBreaker,
    CircuitBreakerConfig,
    RiskCheckResult,
    RiskEvent,
    get_global_circuit_breaker,
    reset_global_circuit_breaker,
)


# ── Fixtures ──────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _reset_singleton():
    """Ensure every test starts with a clean global singleton."""
    reset_global_circuit_breaker()
    yield
    reset_global_circuit_breaker()


def _fresh_breaker(**overrides) -> CircuitBreaker:
    """Build a CircuitBreaker with overridable config."""
    cfg_defaults = dict(
        max_daily_loss=5000.0,
        kill_switch_threshold=-10000.0,
        max_total_exposure=50000.0,
        max_positions=10,
        cooldown_after_halt_minutes=60.0,
    )
    cfg_defaults.update(overrides)
    return CircuitBreaker(CircuitBreakerConfig(**cfg_defaults))


# ── Singleton lifecycle ───────────────────────────────────────────────


def test_global_singleton_returns_same_instance():
    a = get_global_circuit_breaker()
    b = get_global_circuit_breaker()
    assert a is b
    assert isinstance(a, CircuitBreaker)


def test_reset_global_circuit_breaker_returns_new_instance():
    a = get_global_circuit_breaker()
    reset_global_circuit_breaker()
    b = get_global_circuit_breaker()
    assert a is not b


def test_reset_clears_halt_state_between_tests():
    a = get_global_circuit_breaker()
    a.activate_kill_switch("manual")
    assert a.is_halted is True
    reset_global_circuit_breaker()
    b = get_global_circuit_breaker()
    assert b.is_halted is False


# ── check_recommendation happy / HOLD paths ───────────────────────────


def test_check_recommendation_happy_path():
    breaker = _fresh_breaker()
    result = breaker.check_recommendation(
        regime="GROWTH",
        confidence=0.8,
        recommended_action="BUY",
        position_size=100.0,
    )
    assert isinstance(result, RiskCheckResult)
    assert result.passed is True


def test_hold_always_passes_even_when_halted():
    breaker = _fresh_breaker()
    breaker.activate_kill_switch("manual halt")
    result = breaker.check_recommendation(
        regime="GROWTH",
        confidence=0.8,
        recommended_action="HOLD",
        position_size=1_000_000.0,
    )
    assert result.passed is True
    assert "HOLD" in result.reason


def test_hold_passes_even_with_overexposure():
    breaker = _fresh_breaker(max_total_exposure=1_000.0)
    breaker.record_outcome(regime="GROWTH", pnl=0.0, position_delta=5000.0, price=1.0)
    result = breaker.check_recommendation(
        regime="GROWTH",
        confidence=0.5,
        recommended_action="HOLD",
        position_size=50.0,
    )
    assert result.passed is True


# ── Kill switch activation / reset ────────────────────────────────────


def test_activate_kill_switch_halts_trading():
    breaker = _fresh_breaker()
    breaker.activate_kill_switch("manual halt")
    assert breaker.is_halted is True
    result = breaker.check_recommendation(
        regime="GROWTH", confidence=0.8,
        recommended_action="BUY", position_size=100.0,
    )
    # Cooldown kicks in before kill-switch check, but the answer is the same:
    # BLOCKED.
    assert result.passed is False


def test_kill_switch_blocks_with_cooldown_reason_after_manual_activation():
    breaker = _fresh_breaker()
    breaker.activate_kill_switch("manual halt")
    result = breaker.check_recommendation(
        regime="GROWTH", confidence=0.8,
        recommended_action="SELL", position_size=100.0,
    )
    # Either cooldown or kill switch reason is acceptable — both mean BLOCKED.
    assert result.passed is False
    assert ("cooldown" in result.reason.lower() or "kill" in result.reason.lower())


def test_reset_kill_switch_unhalts():
    breaker = _fresh_breaker()
    breaker.activate_kill_switch("manual")
    assert breaker.is_halted is True
    success = breaker.reset_kill_switch()
    assert success is True
    assert breaker.is_halted is False


def test_kill_switch_after_reset_but_cooldown_still_blocks():
    breaker = _fresh_breaker(cooldown_after_halt_minutes=60.0)
    breaker.activate_kill_switch("manual")
    breaker.reset_kill_switch()
    # The halt time was recorded at activation, so cooldown is still active.
    result = breaker.check_recommendation(
        regime="GROWTH", confidence=0.7,
        recommended_action="BUY", position_size=10.0,
    )
    assert result.passed is False
    assert "cooldown" in result.reason.lower()


def test_cooldown_elapsed_allows_trade():
    breaker = _fresh_breaker(cooldown_after_halt_minutes=60.0)
    breaker.activate_kill_switch("manual")
    breaker.reset_kill_switch()
    # Rewind the halt time to 2h ago so cooldown has elapsed.
    breaker._last_halt_time = datetime.now(timezone.utc) - timedelta(hours=2)
    result = breaker.check_recommendation(
        regime="GROWTH", confidence=0.7,
        recommended_action="BUY", position_size=10.0,
    )
    assert result.passed is True


# ── Loss / exposure / position limits ─────────────────────────────────


def test_daily_loss_limit_blocks():
    breaker = _fresh_breaker(max_daily_loss=1000.0, kill_switch_threshold=-1e9)
    # Accumulate a daily loss beyond the limit but NOT past the kill threshold.
    breaker.record_outcome(regime="GROWTH", pnl=-1500.0, position_delta=0.0, price=0.5)
    assert breaker.is_halted is False
    result = breaker.check_recommendation(
        regime="GROWTH", confidence=0.8,
        recommended_action="BUY", position_size=100.0,
    )
    assert result.passed is False
    assert "Daily loss limit exceeded" in result.reason


def test_max_exposure_blocks_trade():
    breaker = _fresh_breaker(max_total_exposure=100.0)
    # Build existing exposure to fill the limit.
    breaker.record_outcome(regime="GROWTH", pnl=0.0, position_delta=100.0, price=1.0)
    result = breaker.check_recommendation(
        regime="GROWTH", confidence=0.8,
        recommended_action="BUY", position_size=1000.0,
    )
    assert result.passed is False
    assert "exposure" in result.reason.lower()


def test_max_positions_blocks_when_new_regime():
    breaker = _fresh_breaker(max_positions=2)
    # Open two position slots under distinct regime market IDs.
    breaker.record_outcome(regime="GROWTH", pnl=0.0, position_delta=1.0, price=1.0)
    breaker.record_outcome(regime="FRAGILE", pnl=0.0, position_delta=1.0, price=1.0)
    result = breaker.check_recommendation(
        regime="CRISIS", confidence=0.5,
        recommended_action="BUY", position_size=1.0,
    )
    assert result.passed is False
    assert "Max positions" in result.reason


def test_warning_at_80_percent_exposure_but_still_passes():
    breaker = _fresh_breaker(max_total_exposure=100.0)
    # Build up 60 units of exposure; new 25-unit trade pushes to 85 (>80% but <100).
    breaker.record_outcome(regime="GROWTH", pnl=0.0, position_delta=60.0, price=1.0)
    result = breaker.check_recommendation(
        regime="GROWTH", confidence=1.0,
        recommended_action="BUY", position_size=25.0,
    )
    assert result.passed is True
    assert result.warnings, "expected an approaching-exposure warning"
    assert any("exposure" in w.lower() for w in result.warnings)


# ── record_outcome / automatic halt ───────────────────────────────────


def test_record_outcome_updates_daily_pnl():
    breaker = _fresh_breaker()
    breaker.record_outcome(regime="GROWTH", pnl=250.0)
    status = breaker.get_status()
    assert status["daily_pnl"] == 250.0
    assert status["total_pnl"] == 250.0


def test_record_outcome_triggers_automatic_halt_when_below_threshold():
    breaker = _fresh_breaker(kill_switch_threshold=-500.0)
    breaker.record_outcome(regime="GROWTH", pnl=-600.0)
    assert breaker.is_halted is True


# ── get_status / get_events / RiskEvent ──────────────────────────────


def test_get_status_returns_all_expected_keys():
    breaker = _fresh_breaker()
    status = breaker.get_status()
    expected = {
        "is_halted", "in_cooldown", "daily_pnl", "total_pnl",
        "total_exposure", "num_positions", "unrealized_pnl",
        "events_count", "last_halt",
    }
    assert expected.issubset(status.keys())
    assert status["is_halted"] is False
    assert status["last_halt"] is None


def test_get_events_truncates_to_last_n():
    breaker = _fresh_breaker()
    for i in range(5):
        breaker.activate_kill_switch(f"halt {i}")
    events = breaker.get_events(last_n=3)
    assert len(events) == 3
    assert events[-1]["reason"] == "halt 4"


def test_get_events_returns_dicts_with_timestamp():
    breaker = _fresh_breaker()
    breaker.activate_kill_switch("for audit")
    events = breaker.get_events(last_n=10)
    assert len(events) >= 1
    for e in events:
        assert "timestamp" in e and "type" in e and "reason" in e
        # Parseable ISO with timezone info
        dt = datetime.fromisoformat(e["timestamp"])
        assert dt.tzinfo is not None


def test_risk_event_timestamp_is_utc():
    breaker = _fresh_breaker()
    breaker.activate_kill_switch("audit")
    assert breaker._events, "activation should record an event"
    ev: RiskEvent = breaker._events[-1]
    assert ev.timestamp.tzinfo == timezone.utc


# ── Config helpers ────────────────────────────────────────────────────


def test_circuit_breaker_config_to_risk_config_has_expected_keys():
    cfg = CircuitBreakerConfig()
    rc = cfg.to_risk_config()
    assert set(rc.keys()) == {
        "max_position_per_market",
        "max_total_exposure",
        "max_daily_loss",
        "kill_switch_threshold",
        "max_positions",
        "position_timeout_hours",
        "enable_kill_switch",
    }
    assert rc["max_position_per_market"] > 0


def test_config_allows_unusual_values_without_crashing():
    # Negative max_daily_loss and a positive kill threshold should not
    # crash — we do not add validation; we just verify the code runs.
    cfg = CircuitBreakerConfig(
        max_daily_loss=-1000.0,
        kill_switch_threshold=500.0,
        max_total_exposure=1.0,
    )
    breaker = CircuitBreaker(cfg)
    result = breaker.check_recommendation(
        regime="GROWTH", confidence=0.5,
        recommended_action="BUY", position_size=1.0,
    )
    # Should not raise; return type shape is fixed.
    assert isinstance(result, RiskCheckResult)


def test_zero_max_positions_does_not_crash():
    # max_positions=0 is nonsensical but shouldn't crash the breaker.
    # The to_risk_config divides by max(max_positions, 1) so we're safe.
    cfg = CircuitBreakerConfig(max_positions=0)
    breaker = CircuitBreaker(cfg)
    result = breaker.check_recommendation(
        regime="GROWTH", confidence=0.5,
        recommended_action="BUY", position_size=1.0,
    )
    assert isinstance(result, RiskCheckResult)


def test_nan_confidence_does_not_crash_breaker():
    breaker = _fresh_breaker()
    # NaN confidence should flow through check_order (which uses it as
    # current_price) without raising.
    result = breaker.check_recommendation(
        regime="GROWTH",
        confidence=float("nan"),
        recommended_action="BUY",
        position_size=1.0,
    )
    assert isinstance(result, RiskCheckResult)


def test_zero_position_size_does_not_crash():
    breaker = _fresh_breaker()
    result = breaker.check_recommendation(
        regime="GROWTH",
        confidence=0.5,
        recommended_action="BUY",
        position_size=0.0,
    )
    assert isinstance(result, RiskCheckResult)
    assert result.passed is True


# ── Singleton + mutation interaction ──────────────────────────────────


def test_singleton_preserves_state_across_calls():
    breaker_a = get_global_circuit_breaker()
    breaker_a.record_outcome(regime="GROWTH", pnl=100.0)
    breaker_b = get_global_circuit_breaker()
    assert breaker_b.get_status()["daily_pnl"] == 100.0


def test_singleton_is_halted_after_activate():
    b = get_global_circuit_breaker()
    b.activate_kill_switch("test")
    assert get_global_circuit_breaker().is_halted is True


def test_reset_singleton_between_tests_clears_pnl():
    b = get_global_circuit_breaker()
    b.record_outcome(regime="GROWTH", pnl=50.0)
    reset_global_circuit_breaker()
    fresh = get_global_circuit_breaker()
    assert fresh.get_status()["daily_pnl"] == 0.0


# ── Direction routing & metadata ──────────────────────────────────────


def test_sell_action_is_routed_through_check_order():
    breaker = _fresh_breaker()
    result = breaker.check_recommendation(
        regime="CRISIS", confidence=0.6,
        recommended_action="SELL", position_size=10.0,
    )
    assert result.passed is True


def test_blocked_event_has_metadata():
    breaker = _fresh_breaker(max_total_exposure=1.0)
    breaker.record_outcome(regime="GROWTH", pnl=0.0, position_delta=5.0, price=1.0)
    breaker.check_recommendation(
        regime="GROWTH", confidence=0.8,
        recommended_action="BUY", position_size=100.0,
    )
    # Last recorded event should be a "blocked" with metadata
    events = [e for e in breaker._events if e.event_type == "blocked"]
    assert events, "expected a blocked event"
    meta = events[-1].metadata
    assert meta.get("regime") == "GROWTH"
    assert meta.get("action") == "BUY"
