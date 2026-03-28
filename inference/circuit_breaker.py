"""
GRID circuit breaker / kill switch.

Provides automated trading halts when GRID's inference pipeline exceeds
loss or exposure thresholds.  Sits between the ensemble output and the
decision journal — blocks recommendations when risk limits are breached.

Fully self-contained — no external dependencies.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from loguru import logger as log


# ── Local types (replacing autopredict imports) ──────────────────────


@dataclass
class RiskCheckResult:
    """Result of a risk check."""
    passed: bool
    reason: str = ""
    warnings: list[str] = field(default_factory=list)


@dataclass
class _Position:
    """Internal position tracker."""
    market_id: str
    size: float = 0.0
    entry_price: float = 0.0
    current_price: float = 0.0
    unrealized_pnl: float = 0.0


class _RiskManager:
    """Self-contained risk manager for the circuit breaker."""

    def __init__(
        self,
        max_position_per_market: float,
        max_total_exposure: float,
        max_daily_loss: float,
        kill_switch_threshold: float,
        max_positions: int = 10,
        enable_kill_switch: bool = True,
    ) -> None:
        self._max_position = max_position_per_market
        self._max_exposure = max_total_exposure
        self._max_daily_loss = max_daily_loss
        self._kill_threshold = kill_switch_threshold
        self._max_positions = max_positions
        self._enable_kill_switch = enable_kill_switch
        self._positions: dict[str, _Position] = {}
        self._daily_pnl: float = 0.0
        self._total_pnl: float = 0.0
        self._halted: bool = False
        self._halt_reason: str = ""

    def check_order(self, market_id: str, side: str, size: float,
                    current_price: float) -> RiskCheckResult:
        """Check whether an order passes risk limits."""
        if self._halted:
            return RiskCheckResult(passed=False, reason=f"Kill switch active: {self._halt_reason}")

        if self._daily_pnl < -self._max_daily_loss:
            return RiskCheckResult(passed=False, reason="Daily loss limit exceeded")

        total_exposure = sum(abs(p.size * p.current_price) for p in self._positions.values())
        new_exposure = size * current_price
        if total_exposure + new_exposure > self._max_exposure:
            return RiskCheckResult(
                passed=False,
                reason=f"Total exposure would exceed limit ({total_exposure + new_exposure:.0f} > {self._max_exposure:.0f})",
            )

        if len(self._positions) >= self._max_positions and market_id not in self._positions:
            return RiskCheckResult(passed=False, reason="Max positions reached")

        warnings = []
        if total_exposure + new_exposure > self._max_exposure * 0.8:
            warnings.append("Approaching total exposure limit")

        return RiskCheckResult(passed=True, warnings=warnings)

    def update_position(self, market_id: str, size_delta: float,
                        price: float, pnl_delta: float = 0.0) -> None:
        """Update a position and track P&L."""
        if market_id not in self._positions:
            self._positions[market_id] = _Position(market_id=market_id, entry_price=price)

        pos = self._positions[market_id]
        pos.size += size_delta
        pos.current_price = price
        self._daily_pnl += pnl_delta
        self._total_pnl += pnl_delta

        if self._enable_kill_switch and self._daily_pnl < self._kill_threshold:
            self._halted = True
            self._halt_reason = f"P&L ({self._daily_pnl:.2f}) below kill threshold ({self._kill_threshold:.2f})"

    def is_kill_switch_active(self) -> bool:
        return self._halted

    def manual_kill_switch(self, reason: str) -> None:
        self._halted = True
        self._halt_reason = reason

    def reset_kill_switch(self, confirmation: str) -> bool:
        if confirmation == "RESET KILL SWITCH":
            self._halted = False
            self._halt_reason = ""
            return True
        return False

    def get_daily_pnl(self) -> float:
        return self._daily_pnl

    @property
    def total_pnl(self) -> float:
        return self._total_pnl

    def get_positions_summary(self) -> dict[str, Any]:
        total_exposure = sum(abs(p.size * p.current_price) for p in self._positions.values())
        unrealized = sum(p.unrealized_pnl for p in self._positions.values())
        return {
            "total_exposure": total_exposure,
            "num_positions": len(self._positions),
            "unrealized_pnl": unrealized,
        }


# ── Public types ─────────────────────────────────────────────────────


@dataclass
class CircuitBreakerConfig:
    """Configuration for GRID's circuit breaker.

    Attributes:
        max_daily_loss: Maximum allowed daily loss before halting (USD).
        kill_switch_threshold: Loss that triggers immediate full halt (USD, negative).
        max_total_exposure: Maximum concurrent exposure (USD).
        max_positions: Maximum number of simultaneous regime positions.
        position_timeout_hours: Auto-close positions after this many hours.
        enable_kill_switch: Whether the kill switch is armed.
        cooldown_after_halt_minutes: Minutes to wait after a halt before resuming.
    """

    max_daily_loss: float = 5000.0
    kill_switch_threshold: float = -10000.0
    max_total_exposure: float = 50000.0
    max_positions: int = 10
    position_timeout_hours: float = 168.0
    enable_kill_switch: bool = True
    cooldown_after_halt_minutes: float = 60.0

    def to_risk_config(self) -> dict[str, Any]:
        """Convert to a risk config dict."""
        return {
            "max_position_per_market": self.max_total_exposure / max(self.max_positions, 1),
            "max_total_exposure": self.max_total_exposure,
            "max_daily_loss": self.max_daily_loss,
            "kill_switch_threshold": self.kill_switch_threshold,
            "max_positions": self.max_positions,
            "position_timeout_hours": self.position_timeout_hours,
            "enable_kill_switch": self.enable_kill_switch,
        }


@dataclass
class RiskEvent:
    """Recorded risk event for audit trail."""

    timestamp: datetime
    event_type: str  # "blocked", "warning", "kill_switch", "cooldown"
    reason: str
    metadata: dict[str, Any] = field(default_factory=dict)


class CircuitBreaker:
    """Automated circuit breaker for GRID's inference pipeline.

    Sits between the ensemble classifier and decision journal.  Before
    any recommendation is logged, ``check_recommendation()`` validates
    that risk limits are not breached.

    The circuit breaker tracks:
    - Daily P&L from recorded outcomes
    - Total exposure across active regime positions
    - Number of open positions
    - Kill switch state (manual or automatic)

    Usage::

        breaker = CircuitBreaker()

        # Before logging a recommendation
        check = breaker.check_recommendation(
            regime="GROWTH",
            confidence=0.82,
            recommended_action="BUY",
            position_size=10000.0,
        )

        if check.passed:
            journal.log_decision(...)
        else:
            log.warning(f"Blocked: {check.reason}")

        # After outcome is known
        breaker.record_outcome(regime="GROWTH", pnl=250.0)
    """

    def __init__(self, config: CircuitBreakerConfig | None = None) -> None:
        self.config = config or CircuitBreakerConfig()
        rc = self.config.to_risk_config()
        self._risk_mgr = _RiskManager(
            max_position_per_market=rc["max_position_per_market"],
            max_total_exposure=rc["max_total_exposure"],
            max_daily_loss=rc["max_daily_loss"],
            kill_switch_threshold=rc["kill_switch_threshold"],
            max_positions=rc.get("max_positions", 10),
            enable_kill_switch=rc.get("enable_kill_switch", True),
        )
        self._events: list[RiskEvent] = []
        self._last_halt_time: datetime | None = None
        log.info(
            "CircuitBreaker initialised — max_daily_loss={d}, kill_threshold={k}",
            d=self.config.max_daily_loss, k=self.config.kill_switch_threshold,
        )

    # ── Pre-trade check ───────────────────────────────────────────────

    def check_recommendation(
        self,
        regime: str,
        confidence: float,
        recommended_action: str,
        position_size: float = 1.0,
    ) -> RiskCheckResult:
        """Check if a recommendation should be allowed through.

        Parameters:
            regime: Predicted regime (e.g. "GROWTH").
            confidence: Model confidence (0-1).
            recommended_action: GRID action (BUY/SELL/HOLD/REDUCE).
            position_size: Notional position size.

        Returns:
            RiskCheckResult — check .passed before logging.
        """
        # HOLD doesn't add exposure, always allow
        if recommended_action in ("HOLD",):
            return RiskCheckResult(passed=True, reason="HOLD — no new exposure")

        # Check cooldown
        if self._in_cooldown():
            result = RiskCheckResult(
                passed=False,
                reason=f"Cooldown active — {self.config.cooldown_after_halt_minutes}min after last halt",
            )
            self._record_event("cooldown", result.reason)
            return result

        # Check kill switch
        if self._risk_mgr.is_kill_switch_active():
            result = RiskCheckResult(
                passed=False,
                reason="Kill switch is active",
            )
            self._record_event("blocked", result.reason)
            return result

        side = "buy" if recommended_action in ("BUY",) else "sell"

        result = self._risk_mgr.check_order(
            market_id=f"regime_{regime}",
            side=side,
            size=position_size,
            current_price=confidence,
        )

        if not result.passed:
            self._record_event("blocked", result.reason, {
                "regime": regime,
                "action": recommended_action,
                "position_size": position_size,
            })
            log.warning(
                "CircuitBreaker BLOCKED — {r} | regime={reg}, action={a}",
                r=result.reason, reg=regime, a=recommended_action,
            )
        elif result.warnings:
            for w in result.warnings:
                self._record_event("warning", w, {"regime": regime})
                log.warning("CircuitBreaker warning: {w}", w=w)

        return result

    # ── Post-trade updates ────────────────────────────────────────────

    def record_outcome(
        self,
        regime: str,
        pnl: float,
        position_delta: float = 0.0,
        price: float = 0.5,
    ) -> None:
        """Record a trade outcome to update P&L tracking.

        Parameters:
            regime: Regime market ID.
            pnl: Realized P&L from this outcome.
            position_delta: Change in position size.
            price: Price at which outcome was recorded.
        """
        self._risk_mgr.update_position(
            market_id=f"regime_{regime}",
            size_delta=position_delta,
            price=price,
            pnl_delta=pnl,
        )

        if self._risk_mgr.is_kill_switch_active():
            self._record_event("kill_switch", f"Kill switch triggered by P&L update (pnl={pnl})")
            self._last_halt_time = datetime.now(timezone.utc)

    # ── Kill switch controls ──────────────────────────────────────────

    def activate_kill_switch(self, reason: str = "Manual activation") -> None:
        """Manually halt all trading."""
        self._risk_mgr.manual_kill_switch(reason)
        self._last_halt_time = datetime.now(timezone.utc)
        self._record_event("kill_switch", reason)
        log.warning("Kill switch ACTIVATED — {r}", r=reason)

    def reset_kill_switch(self) -> bool:
        """Reset kill switch."""
        success = self._risk_mgr.reset_kill_switch("RESET KILL SWITCH")
        if success:
            self._record_event("kill_switch", "Kill switch reset")
            log.info("Kill switch RESET")
        return success

    @property
    def is_halted(self) -> bool:
        """Check if trading is currently halted."""
        return self._risk_mgr.is_kill_switch_active()

    # ── Status / audit ────────────────────────────────────────────────

    def get_status(self) -> dict[str, Any]:
        """Get current circuit breaker status."""
        summary = self._risk_mgr.get_positions_summary()
        return {
            "is_halted": self.is_halted,
            "in_cooldown": self._in_cooldown(),
            "daily_pnl": round(self._risk_mgr.get_daily_pnl(), 2),
            "total_pnl": round(self._risk_mgr.total_pnl, 2),
            "total_exposure": round(summary["total_exposure"], 2),
            "num_positions": summary["num_positions"],
            "unrealized_pnl": round(summary["unrealized_pnl"], 2),
            "events_count": len(self._events),
            "last_halt": self._last_halt_time.isoformat() if self._last_halt_time else None,
        }

    def get_events(self, last_n: int = 50) -> list[dict[str, Any]]:
        """Get recent risk events for audit."""
        return [
            {
                "timestamp": e.timestamp.isoformat(),
                "type": e.event_type,
                "reason": e.reason,
                "metadata": e.metadata,
            }
            for e in self._events[-last_n:]
        ]

    # ── Internal ──────────────────────────────────────────────────────

    def _in_cooldown(self) -> bool:
        if self._last_halt_time is None:
            return False
        elapsed = (datetime.now(timezone.utc) - self._last_halt_time).total_seconds()
        return elapsed < self.config.cooldown_after_halt_minutes * 60

    def _record_event(
        self,
        event_type: str,
        reason: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self._events.append(RiskEvent(
            timestamp=datetime.now(timezone.utc),
            event_type=event_type,
            reason=reason,
            metadata=metadata or {},
        ))
