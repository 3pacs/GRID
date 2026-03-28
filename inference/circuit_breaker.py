"""
Circuit breaker / kill switch powered by autopredict.

Wraps autopredict's RiskManager to provide automated trading halts
when GRID's inference pipeline exceeds loss or exposure thresholds.
Designed to sit between the ensemble output and the decision journal —
blocks recommendations when risk limits are breached.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from loguru import logger as log

from autopredict.config.schema import RiskConfig
from autopredict.live.risk import Position, RiskCheckResult, RiskManager


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

    def to_risk_config(self) -> RiskConfig:
        """Convert to autopredict RiskConfig."""
        return RiskConfig(
            max_position_per_market=self.max_total_exposure / max(self.max_positions, 1),
            max_total_exposure=self.max_total_exposure,
            max_daily_loss=self.max_daily_loss,
            kill_switch_threshold=self.kill_switch_threshold,
            max_positions=self.max_positions,
            position_timeout_hours=self.position_timeout_hours,
            enable_kill_switch=self.enable_kill_switch,
        )


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
        self._risk_mgr = RiskManager(self.config.to_risk_config())
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
        # HOLD/REDUCE don't add exposure, always allow
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

        # Build a synthetic order for autopredict's risk manager
        from autopredict.live.trader import Order
        order = Order(
            market_id=f"regime_{regime}",
            side="buy" if recommended_action in ("BUY",) else "sell",
            size=position_size,
            order_type="market",
        )

        result = self._risk_mgr.check_order(order, current_price=confidence)

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
        """Reset kill switch (requires explicit confirmation string)."""
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
