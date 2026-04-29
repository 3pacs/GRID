"""
Tests for trading/solana/exit_decision.py.

Pure-function tests — no DB, no HTTP, no mocks beyond dataclasses.
Every branch of the decision precedence is covered.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from trading.solana.exit_decision import (
    ACTION_ARM_TRAILING,
    ACTION_HOLD,
    ACTION_MAX_HOLD,
    ACTION_STOP_LOSS,
    ACTION_TP_RUNG,
    ACTION_TRAILING_STOP,
    ExitState,
    compute_pnl_pct,
    decide_exit,
)
from trading.solana.exit_policy import BALANCED, CONSERVATIVE, ExitPolicy, ExitRung


# ----------------------------------------------------------------------
# compute_pnl_pct
# ----------------------------------------------------------------------
def test_compute_pnl_pct_long_positive():
    assert compute_pnl_pct("LONG", 100.0, 150.0) == pytest.approx(0.5)


def test_compute_pnl_pct_long_negative():
    assert compute_pnl_pct("LONG", 100.0, 80.0) == pytest.approx(-0.2)


def test_compute_pnl_pct_short_inverts():
    assert compute_pnl_pct("SHORT", 100.0, 80.0) == pytest.approx(0.2)
    assert compute_pnl_pct("SHORT", 100.0, 120.0) == pytest.approx(-0.2)


def test_compute_pnl_pct_zero_entry_returns_zero():
    assert compute_pnl_pct("LONG", 0.0, 100.0) == 0.0


# ----------------------------------------------------------------------
# Fixtures
# ----------------------------------------------------------------------
EPOCH = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _state(
    current_price: float,
    *,
    entry_price: float = 100.0,
    direction: str = "LONG",
    held_seconds: int = 60,
    peak_pnl_pct: float | None = None,
    remaining_fraction: float = 1.0,
    tp_rungs_hit: int = 0,
    trailing_armed: bool = False,
) -> ExitState:
    if peak_pnl_pct is None:
        peak_pnl_pct = compute_pnl_pct(direction, entry_price, current_price)
    return ExitState(
        trade_id=1,
        direction=direction,
        entry_price=entry_price,
        current_price=current_price,
        entry_time=EPOCH,
        now=EPOCH + timedelta(seconds=held_seconds),
        peak_pnl_pct=peak_pnl_pct,
        remaining_fraction=remaining_fraction,
        tp_rungs_hit=tp_rungs_hit,
        trailing_armed=trailing_armed,
    )


# ----------------------------------------------------------------------
# Stop loss takes precedence over everything
# ----------------------------------------------------------------------
def test_stop_loss_fires_on_hard_breach():
    # Balanced stop is -20%; a -25% drop blows through it.
    state = _state(current_price=75.0, remaining_fraction=0.67, tp_rungs_hit=1)
    action = decide_exit(state, BALANCED)
    assert action.kind == ACTION_STOP_LOSS
    assert action.close_fraction == pytest.approx(0.67)
    assert action.is_terminal


def test_stop_loss_beats_max_hold_when_both_fire():
    # Held for 1 day — way past max hold — AND price hit the stop.
    state = _state(
        current_price=75.0,
        held_seconds=86_400,
        remaining_fraction=1.0,
    )
    action = decide_exit(state, BALANCED)
    assert action.kind == ACTION_STOP_LOSS


# ----------------------------------------------------------------------
# Max hold
# ----------------------------------------------------------------------
def test_max_hold_fires_when_stale_and_flat():
    # No pnl, but held past the 60-min cap.
    state = _state(current_price=100.5, held_seconds=BALANCED.max_hold_seconds + 1)
    action = decide_exit(state, BALANCED)
    assert action.kind == ACTION_MAX_HOLD
    assert action.close_fraction == pytest.approx(1.0)


def test_max_hold_fires_before_tp_rung_if_both_would_fire():
    # Held long AND at +50% (first rung trigger). Precedence: max_hold first.
    state = _state(
        current_price=150.0,
        held_seconds=BALANCED.max_hold_seconds + 1,
    )
    action = decide_exit(state, BALANCED)
    assert action.kind == ACTION_MAX_HOLD


# ----------------------------------------------------------------------
# Trailing stop
# ----------------------------------------------------------------------
def test_trailing_stop_fires_when_armed_and_drawdown_exceeds():
    # Peak was +100%, now at +70% = 30% drawdown from peak.
    # Balanced trailing distance is -15%.
    state = _state(
        current_price=170.0,
        peak_pnl_pct=1.0,
        trailing_armed=True,
        remaining_fraction=0.5,
    )
    action = decide_exit(state, BALANCED)
    assert action.kind == ACTION_TRAILING_STOP
    assert action.close_fraction == pytest.approx(0.5)
    assert action.is_terminal


def test_trailing_stop_does_not_fire_if_not_armed():
    state = _state(
        current_price=170.0,
        peak_pnl_pct=1.0,
        trailing_armed=False,
    )
    action = decide_exit(state, BALANCED)
    assert action.kind != ACTION_TRAILING_STOP


def test_trailing_stop_does_not_fire_within_distance():
    # Peak +100%, now +90% = 10% drawdown, under 15% threshold.
    state = _state(
        current_price=190.0,
        peak_pnl_pct=1.0,
        trailing_armed=True,
    )
    action = decide_exit(state, BALANCED)
    assert action.kind != ACTION_TRAILING_STOP


# ----------------------------------------------------------------------
# Take-profit rungs
# ----------------------------------------------------------------------
def test_tp_rung_fires_at_trigger():
    # Balanced rung 0: trigger +50%, fraction 0.33
    state = _state(current_price=150.0)
    action = decide_exit(state, BALANCED)
    assert action.kind == ACTION_TP_RUNG
    assert action.rung_index == 0
    assert action.close_fraction == pytest.approx(0.33)
    assert not action.is_terminal


def test_second_rung_ignores_first():
    state = _state(current_price=200.0, tp_rungs_hit=1)
    action = decide_exit(state, BALANCED)
    assert action.kind == ACTION_TP_RUNG
    assert action.rung_index == 1


def test_tp_rung_clamps_to_remaining_fraction():
    # Rung would fire at 33% but only 0.10 remains.
    state = _state(current_price=150.0, remaining_fraction=0.10, tp_rungs_hit=0)
    action = decide_exit(state, BALANCED)
    assert action.close_fraction == pytest.approx(0.10)


def test_tp_rung_arms_trailing_when_threshold_crossed():
    # Balanced arms trailing at +50%, rung 0 triggers exactly at +50%.
    state = _state(current_price=150.0, trailing_armed=False)
    action = decide_exit(state, BALANCED)
    assert action.arm_trailing is True


def test_no_more_rungs_after_all_hit():
    # Conservative has 3 rungs; pretend they're all done at +150%.
    state = _state(
        current_price=250.0,
        tp_rungs_hit=3,
        trailing_armed=True,
        peak_pnl_pct=1.5,
        remaining_fraction=0.01,
    )
    action = decide_exit(state, CONSERVATIVE)
    # With only 0.01 remaining and not on a rung, manager would hold.
    assert action.kind == ACTION_HOLD


# ----------------------------------------------------------------------
# Arm trailing stop as a standalone action
# ----------------------------------------------------------------------
def test_arm_trailing_without_tp_rung():
    # Policy with activation below the first rung trigger, and we've
    # crossed activation but not the rung yet.
    policy = ExitPolicy(
        variant_id="custom",
        description="custom",
        take_profit_rungs=(ExitRung(0.80, 0.5),),
        stop_loss_pct=-0.3,
        trailing_stop_pct=-0.1,
        activate_trailing_after_pct=0.20,
        max_hold_seconds=3600,
    )
    state = _state(current_price=130.0, trailing_armed=False)
    action = decide_exit(state, policy)
    assert action.kind == ACTION_ARM_TRAILING
    assert action.arm_trailing is True
    assert action.close_fraction == 0.0


def test_no_arm_trailing_if_policy_has_no_trailing():
    policy = ExitPolicy(
        variant_id="no_trail",
        description="no trail",
        take_profit_rungs=(ExitRung(2.0, 1.0),),
        stop_loss_pct=-0.5,
        trailing_stop_pct=None,
        activate_trailing_after_pct=0.10,
        max_hold_seconds=3600,
    )
    state = _state(current_price=150.0)
    action = decide_exit(state, policy)
    assert action.kind == ACTION_HOLD


# ----------------------------------------------------------------------
# Hold
# ----------------------------------------------------------------------
def test_hold_when_nothing_interesting():
    state = _state(current_price=105.0, held_seconds=30)
    action = decide_exit(state, BALANCED)
    assert action.kind == ACTION_HOLD
    assert action.close_fraction == 0.0


def test_hold_when_already_drained():
    state = _state(current_price=150.0, remaining_fraction=0.0)
    action = decide_exit(state, BALANCED)
    assert action.kind == ACTION_HOLD
