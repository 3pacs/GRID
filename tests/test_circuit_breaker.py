"""Tests for grid.inference.circuit_breaker — autopredict risk kill switch bridge."""

from __future__ import annotations

import pytest

from inference.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    RiskEvent,
)


class TestCircuitBreakerConfig:
    def test_defaults(self):
        cfg = CircuitBreakerConfig()
        assert cfg.max_daily_loss == 5000.0
        assert cfg.enable_kill_switch is True

    def test_to_risk_config(self):
        cfg = CircuitBreakerConfig(max_daily_loss=1000.0, max_positions=5)
        rc = cfg.to_risk_config()
        assert rc.max_daily_loss == 1000.0
        assert rc.max_positions == 5
        assert rc.max_position_per_market == 50000.0 / 5


class TestCircuitBreaker:
    def test_allow_normal_recommendation(self):
        breaker = CircuitBreaker()
        result = breaker.check_recommendation(
            regime="GROWTH",
            confidence=0.80,
            recommended_action="BUY",
            position_size=1000.0,
        )
        assert result.passed is True

    def test_hold_always_passes(self):
        breaker = CircuitBreaker()
        # Even if halted setup
        result = breaker.check_recommendation(
            regime="NEUTRAL",
            confidence=0.60,
            recommended_action="HOLD",
        )
        assert result.passed is True

    def test_kill_switch_blocks(self):
        cfg = CircuitBreakerConfig(cooldown_after_halt_minutes=0)
        breaker = CircuitBreaker(cfg)
        breaker.activate_kill_switch("Test halt")
        assert breaker.is_halted is True

        result = breaker.check_recommendation(
            regime="GROWTH",
            confidence=0.90,
            recommended_action="BUY",
        )
        assert result.passed is False
        assert "kill switch" in result.reason.lower() or "Kill" in result.reason

    def test_kill_switch_reset(self):
        breaker = CircuitBreaker(
            CircuitBreakerConfig(cooldown_after_halt_minutes=0)
        )
        breaker.activate_kill_switch("Test")
        assert breaker.is_halted is True

        success = breaker.reset_kill_switch()
        assert success is True
        assert breaker.is_halted is False

    def test_daily_loss_limit_blocks(self):
        cfg = CircuitBreakerConfig(
            max_daily_loss=100.0,
            kill_switch_threshold=-500.0,
            cooldown_after_halt_minutes=0,
        )
        breaker = CircuitBreaker(cfg)

        # Record large loss
        breaker.record_outcome("GROWTH", pnl=-150.0)

        result = breaker.check_recommendation(
            regime="NEUTRAL",
            confidence=0.70,
            recommended_action="BUY",
            position_size=100.0,
        )
        assert result.passed is False

    def test_exposure_limit_blocks(self):
        cfg = CircuitBreakerConfig(
            max_total_exposure=500.0,
            max_positions=2,
        )
        breaker = CircuitBreaker(cfg)

        # Fill up exposure
        breaker._risk_mgr.update_position("regime_A", 600.0, 0.8)

        result = breaker.check_recommendation(
            regime="GROWTH",
            confidence=0.80,
            recommended_action="BUY",
            position_size=100.0,
        )
        assert result.passed is False

    def test_get_status(self):
        breaker = CircuitBreaker()
        status = breaker.get_status()
        assert "is_halted" in status
        assert "daily_pnl" in status
        assert "total_exposure" in status
        assert "num_positions" in status
        assert status["is_halted"] is False

    def test_get_events(self):
        breaker = CircuitBreaker()
        breaker.activate_kill_switch("Test")
        events = breaker.get_events()
        assert len(events) >= 1
        assert events[-1]["type"] == "kill_switch"

    def test_cooldown_blocks(self):
        cfg = CircuitBreakerConfig(cooldown_after_halt_minutes=60)
        breaker = CircuitBreaker(cfg)

        # Trigger and reset
        breaker.activate_kill_switch("Test")
        breaker.reset_kill_switch()

        # Should still be in cooldown
        result = breaker.check_recommendation(
            regime="GROWTH",
            confidence=0.80,
            recommended_action="BUY",
        )
        assert result.passed is False
        assert "cooldown" in result.reason.lower()


class TestRiskEvent:
    def test_creation(self):
        from datetime import datetime, timezone
        event = RiskEvent(
            timestamp=datetime.now(timezone.utc),
            event_type="blocked",
            reason="Test block",
        )
        assert event.event_type == "blocked"
        assert event.metadata == {}
