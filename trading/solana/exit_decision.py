"""
Pure exit-decision function.

This module does **no I/O** — it takes an :class:`ExitState` snapshot and
an :class:`ExitPolicy`, and returns an :class:`ExitAction`. All state
mutation and persistence happens in :mod:`trading.solana.exit_manager`,
which calls this function once per open position per tick.

Keeping the decision logic pure means:

  * every branch is a trivial unit test
  * the same function runs identically in paper mode, live mode, and
    historical replays / learner training sets
  * no hidden dependency on wall-clock time (``now`` is an argument)

Decision order of precedence — *highest severity wins*:

  1. Hard stop loss (``stop_loss_pct`` hit)
  2. Max-hold timer expired
  3. Trailing stop (only if armed)
  4. Take-profit rung
  5. Arm trailing stop (mutation, not an exit)
  6. Hold
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from trading.solana.exit_policy import ExitPolicy


# ----------------------------------------------------------------------
# Action kinds
# ----------------------------------------------------------------------
ACTION_HOLD = "hold"
ACTION_TP_RUNG = "tp_rung"
ACTION_STOP_LOSS = "stop_loss"
ACTION_TRAILING_STOP = "trailing_stop"
ACTION_MAX_HOLD = "max_hold"
ACTION_ARM_TRAILING = "arm_trailing"

TERMINAL_ACTIONS: frozenset[str] = frozenset(
    {ACTION_STOP_LOSS, ACTION_TRAILING_STOP, ACTION_MAX_HOLD}
)


# ----------------------------------------------------------------------
# Dataclasses
# ----------------------------------------------------------------------
@dataclass(frozen=True)
class ExitState:
    """Snapshot of a single open position at decision time."""

    trade_id: int
    direction: str           # 'LONG' or 'SHORT'
    entry_price: float
    current_price: float
    entry_time: datetime
    now: datetime
    peak_pnl_pct: float
    remaining_fraction: float
    tp_rungs_hit: int
    trailing_armed: bool


@dataclass(frozen=True)
class ExitAction:
    """What the decision engine wants to do.

    ``close_fraction`` is always expressed as a fraction of the **original**
    position (not the remaining fraction) so the caller can directly
    compare it to ``state.remaining_fraction`` to detect the final slice.
    """

    kind: str
    close_fraction: float
    rung_index: int | None
    reason: str
    arm_trailing: bool = False

    @property
    def is_terminal(self) -> bool:
        return self.kind in TERMINAL_ACTIONS

    @property
    def closes_position(self) -> bool:
        return self.close_fraction > 0


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def compute_pnl_pct(direction: str, entry_price: float, current_price: float) -> float:
    """Return pnl as a fraction of entry, honouring trade direction.

    LONG: (current - entry) / entry
    SHORT: (entry - current) / entry
    """
    if entry_price <= 0:
        return 0.0
    raw = (current_price - entry_price) / entry_price
    return raw if direction == "LONG" else -raw


def _held_seconds(state: ExitState) -> float:
    return (state.now - state.entry_time).total_seconds()


# ----------------------------------------------------------------------
# Core decision
# ----------------------------------------------------------------------
def decide_exit(state: ExitState, policy: ExitPolicy) -> ExitAction:
    """Map ``(state, policy)`` to a single :class:`ExitAction`.

    The function is total — it always returns an action, never raises.
    """
    # Nothing to do if the position is already fully filled on the exit side.
    if state.remaining_fraction <= 0:
        return ExitAction(
            kind=ACTION_HOLD,
            close_fraction=0.0,
            rung_index=None,
            reason="remaining_fraction already zero",
        )

    current_pnl = compute_pnl_pct(
        state.direction, state.entry_price, state.current_price
    )

    # ---- 1. Hard stop --------------------------------------------------
    if current_pnl <= policy.stop_loss_pct:
        return ExitAction(
            kind=ACTION_STOP_LOSS,
            close_fraction=state.remaining_fraction,
            rung_index=None,
            reason=(
                f"stop_loss {current_pnl:.3f} ≤ {policy.stop_loss_pct:.3f}"
            ),
        )

    # ---- 2. Max hold ---------------------------------------------------
    if _held_seconds(state) >= policy.max_hold_seconds:
        return ExitAction(
            kind=ACTION_MAX_HOLD,
            close_fraction=state.remaining_fraction,
            rung_index=None,
            reason=(
                f"max_hold {_held_seconds(state):.0f}s "
                f"≥ {policy.max_hold_seconds}s"
            ),
        )

    # Peak must be at least the current pnl (manager usually updates
    # peak before calling us, but defend against stale state).
    effective_peak = max(state.peak_pnl_pct, current_pnl)

    # ---- 3. Trailing stop ----------------------------------------------
    if state.trailing_armed and policy.trailing_stop_pct is not None:
        drawdown_from_peak = current_pnl - effective_peak  # ≤ 0
        if drawdown_from_peak <= policy.trailing_stop_pct:
            return ExitAction(
                kind=ACTION_TRAILING_STOP,
                close_fraction=state.remaining_fraction,
                rung_index=None,
                reason=(
                    f"trailing: peak {effective_peak:.3f} − current "
                    f"{current_pnl:.3f} = {drawdown_from_peak:.3f} ≤ "
                    f"{policy.trailing_stop_pct:.3f}"
                ),
            )

    # ---- 4. Take-profit rung ------------------------------------------
    next_rung_index = state.tp_rungs_hit
    if next_rung_index < len(policy.take_profit_rungs):
        rung = policy.take_profit_rungs[next_rung_index]
        if current_pnl >= rung.trigger_pnl_pct:
            # Never close more than we have left — clamp to remaining.
            close_fraction = min(rung.close_fraction, state.remaining_fraction)
            return ExitAction(
                kind=ACTION_TP_RUNG,
                close_fraction=close_fraction,
                rung_index=next_rung_index,
                reason=(
                    f"tp rung #{next_rung_index} @ "
                    f"trigger {rung.trigger_pnl_pct:.3f}, "
                    f"current {current_pnl:.3f}"
                ),
                # Take-profit rungs may also arm trailing if we crossed
                # the activation threshold on this tick.
                arm_trailing=(
                    policy.trailing_stop_pct is not None
                    and not state.trailing_armed
                    and current_pnl >= policy.activate_trailing_after_pct
                ),
            )

    # ---- 5. Arm trailing stop -----------------------------------------
    if (
        not state.trailing_armed
        and policy.trailing_stop_pct is not None
        and current_pnl >= policy.activate_trailing_after_pct
    ):
        return ExitAction(
            kind=ACTION_ARM_TRAILING,
            close_fraction=0.0,
            rung_index=None,
            reason=(
                f"arm trailing: current {current_pnl:.3f} ≥ "
                f"{policy.activate_trailing_after_pct:.3f}"
            ),
            arm_trailing=True,
        )

    # ---- 6. Hold -------------------------------------------------------
    return ExitAction(
        kind=ACTION_HOLD,
        close_fraction=0.0,
        rung_index=None,
        reason=f"hold at {current_pnl:.3f}",
    )
