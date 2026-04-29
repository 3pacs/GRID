"""
Tests for trading/solana/exit_manager.py.

End-to-end tick behaviour with mocked store, jupiter, learner, and
paper engine. Verifies the wiring, not the pure decision logic (that's
exercised in test_solana_exit_decision.py).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from trading.solana.exit_decision import (
    ACTION_ARM_TRAILING,
    ACTION_HOLD,
    ACTION_STOP_LOSS,
    ACTION_TP_RUNG,
)
from trading.solana.exit_manager import ExitManager
from trading.solana.exit_policy import BALANCED
from trading.solana.exit_state import PositionStateRow


EPOCH = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _pos(
    trade_id: int = 1,
    entry_price: float = 100.0,
    direction: str = "LONG",
    peak_pnl_pct: float = 0.0,
    remaining_fraction: float = 1.0,
    tp_rungs_hit: int = 0,
    trailing_armed: bool = False,
    filled_exit_value: float = 0.0,
    filled_exit_fraction: float = 0.0,
    policy_variant: str = "balanced",
    source_type: str = "unknown",
) -> PositionStateRow:
    return PositionStateRow(
        trade_id=trade_id,
        ticker="MINTXYZ",
        direction=direction,
        entry_price=entry_price,
        entry_time=EPOCH,
        peak_pnl_pct=peak_pnl_pct,
        trough_pnl_pct=0.0,
        remaining_fraction=remaining_fraction,
        tp_rungs_hit=tp_rungs_hit,
        trailing_armed=trailing_armed,
        filled_exit_value=filled_exit_value,
        filled_exit_fraction=filled_exit_fraction,
        policy_variant=policy_variant,
        source_type=source_type,
    )


@pytest.fixture()
def mock_store():
    store = MagicMock()
    store.list_open_positions.return_value = []
    return store


@pytest.fixture()
def mock_jupiter():
    j = MagicMock()
    j.get_token_price.return_value = {"MINTXYZ": {"usdPrice": 100.0}}
    return j


@pytest.fixture()
def mock_paper():
    p = MagicMock()
    p.close_trade.return_value = {"pnl": 10.0, "pnl_pct": 0.1}
    return p


@pytest.fixture()
def mock_learner():
    l = MagicMock()
    l.record_outcome.return_value = 0.5
    return l


@pytest.fixture()
def manager(mock_store, mock_jupiter, mock_paper, mock_learner):
    engine = MagicMock()
    clock = lambda: EPOCH + timedelta(seconds=60)
    return ExitManager(
        engine=engine,
        jupiter=mock_jupiter,
        store=mock_store,
        learner=mock_learner,
        paper=mock_paper,
        clock=clock,
    )


# ----------------------------------------------------------------------
# Hold branch
# ----------------------------------------------------------------------
def test_tick_holds_when_flat(mock_store, manager):
    mock_store.list_open_positions.return_value = [_pos()]
    # Price unchanged, held 60s → below every threshold.
    summary = manager.tick()
    assert summary.positions_checked == 1
    assert summary.holds == 1
    assert summary.tp_rungs == 0
    assert summary.closes == 0
    # The peak/last-tick update always runs, even on HOLD.
    mock_store.update_position.assert_called()


def test_tick_skips_position_when_price_missing(mock_store, mock_jupiter, manager):
    mock_store.list_open_positions.return_value = [_pos()]
    mock_jupiter.get_token_price.return_value = {"MINTXYZ": {}}
    summary = manager.tick()
    assert summary.holds == 0
    assert summary.closes == 0
    assert summary.errors == 0
    assert summary.details[0]["reason"] == "no price"


# ----------------------------------------------------------------------
# Arm trailing
# ----------------------------------------------------------------------
def test_tick_arms_trailing_stop(mock_store, mock_jupiter, manager):
    # Use a custom policy scenario via a position with rungs already done
    # so the decision falls through to ACTION_ARM_TRAILING.
    from trading.solana.exit_policy import ExitPolicy, ExitRung, policy_by_id

    # Trick: make a position using 'balanced' but at a price between
    # the activation threshold (+50%) and the next rung. We need all
    # three rungs already "done" to skip the rung branch.
    mock_store.list_open_positions.return_value = [
        _pos(tp_rungs_hit=3, trailing_armed=False)
    ]
    # +60% — above activation, no rung triggers because tp_rungs_hit==3.
    mock_jupiter.get_token_price.return_value = {"MINTXYZ": {"usdPrice": 160.0}}

    summary = manager.tick()
    assert summary.arm_trailings == 1
    # Second call was the state update to set trailing_armed=True
    update_calls = [
        c for c in mock_store.update_position.call_args_list
        if c.kwargs.get("trailing_armed") is True
    ]
    assert update_calls, "expected a trailing_armed=True update"


# ----------------------------------------------------------------------
# TP rung partial close (not final)
# ----------------------------------------------------------------------
def test_tick_fires_tp_rung_partial(mock_store, mock_jupiter, mock_paper, manager):
    mock_store.list_open_positions.return_value = [_pos()]
    # +50% → rung 0 fires, close 0.33
    mock_jupiter.get_token_price.return_value = {"MINTXYZ": {"usdPrice": 150.0}}

    summary = manager.tick()
    assert summary.tp_rungs == 1
    assert summary.closes == 0
    # Event recorded
    mock_store.record_event.assert_called_once()
    event_kwargs = mock_store.record_event.call_args.kwargs
    assert event_kwargs["event_type"] == ACTION_TP_RUNG
    assert event_kwargs["rung_index"] == 0
    # Paper trade NOT closed — still has remainder
    mock_paper.close_trade.assert_not_called()


def test_tick_tp_rung_finalises_when_last_slice(mock_store, mock_jupiter, mock_paper, mock_learner, manager):
    # Only 0.33 remaining and on the last rung — clamps to 0.33, remaining → 0.
    mock_store.list_open_positions.return_value = [
        _pos(
            remaining_fraction=0.33,
            tp_rungs_hit=2,
            peak_pnl_pct=2.0,
            filled_exit_value=0.67 * 130.0,  # already sold 0.67 at some price
            filled_exit_fraction=0.67,
        )
    ]
    # +200% — rung 2 fires at +200%
    mock_jupiter.get_token_price.return_value = {"MINTXYZ": {"usdPrice": 300.0}}

    summary = manager.tick()
    assert summary.closes == 1
    mock_paper.close_trade.assert_called_once()
    # Learner was fed a realised pnl
    mock_learner.record_outcome.assert_called_once()
    call = mock_learner.record_outcome.call_args.kwargs
    assert call["variant_id"] == "balanced"
    assert call["source_type"] == "unknown"
    assert call["pnl_pct"] > 0  # we made money


# ----------------------------------------------------------------------
# Stop loss — terminal
# ----------------------------------------------------------------------
def test_tick_stop_loss_closes_and_feeds_learner(mock_store, mock_jupiter, mock_paper, mock_learner, manager):
    mock_store.list_open_positions.return_value = [_pos()]
    # -25% → past balanced stop of -20%
    mock_jupiter.get_token_price.return_value = {"MINTXYZ": {"usdPrice": 75.0}}

    summary = manager.tick()
    assert summary.closes == 1
    assert summary.tp_rungs == 0
    mock_paper.close_trade.assert_called_once()
    mock_learner.record_outcome.assert_called_once()
    call = mock_learner.record_outcome.call_args.kwargs
    assert call["pnl_pct"] < 0


def test_tick_stop_loss_records_immutable_event_before_close(mock_store, mock_jupiter, mock_paper, manager):
    mock_store.list_open_positions.return_value = [_pos()]
    mock_jupiter.get_token_price.return_value = {"MINTXYZ": {"usdPrice": 70.0}}

    manager.tick()
    # Event was recorded with the stop event type
    event_call = mock_store.record_event.call_args.kwargs
    assert event_call["event_type"] == ACTION_STOP_LOSS
    assert event_call["fraction"] == pytest.approx(1.0)


# ----------------------------------------------------------------------
# Blended exit price
# ----------------------------------------------------------------------
def test_tick_close_uses_blended_exit_price(mock_store, mock_jupiter, mock_paper, manager):
    # 0.67 already sold at $130 average; now sell remaining 0.33 at $300.
    # Expected blended avg = (0.67 * 130 + 0.33 * 300) / 1 = 87.1 + 99 = 186.1
    mock_store.list_open_positions.return_value = [
        _pos(
            remaining_fraction=0.33,
            tp_rungs_hit=2,
            peak_pnl_pct=2.0,
            filled_exit_value=0.67 * 130.0,
            filled_exit_fraction=0.67,
        )
    ]
    mock_jupiter.get_token_price.return_value = {"MINTXYZ": {"usdPrice": 300.0}}

    manager.tick()
    close_call = mock_paper.close_trade.call_args.kwargs
    assert close_call["exit_price"] == pytest.approx(186.1, abs=0.01)


# ----------------------------------------------------------------------
# Error isolation
# ----------------------------------------------------------------------
def test_tick_isolates_errors_per_position(mock_store, mock_jupiter, mock_paper, manager):
    # Two positions — the first one blows up in record_event, the second
    # should still be processed.
    mock_store.list_open_positions.return_value = [
        _pos(trade_id=1),
        _pos(trade_id=2),
    ]
    mock_jupiter.get_token_price.return_value = {"MINTXYZ": {"usdPrice": 150.0}}
    # Blow up on the first record_event but let the second through.
    call_count = {"n": 0}

    def maybe_raise(**kwargs):
        call_count["n"] += 1
        if kwargs["trade_id"] == 1:
            raise RuntimeError("db down")
        return 99

    mock_store.record_event.side_effect = maybe_raise

    summary = manager.tick()
    assert summary.positions_checked == 2
    assert summary.errors == 1
    assert summary.tp_rungs == 1


# ----------------------------------------------------------------------
# register_position
# ----------------------------------------------------------------------
def test_register_position_delegates_to_learner(mock_store, manager, mock_learner):
    mock_learner.select_policy.return_value = BALANCED
    policy = manager.register_position(trade_id=42)
    assert policy.variant_id == "balanced"
    mock_store.ensure_position.assert_called_once_with(
        trade_id=42, policy_variant="balanced", source_type="unknown"
    )


def test_register_position_accepts_explicit_policy(mock_store, manager):
    policy = manager.register_position(trade_id=42, policy=BALANCED)
    assert policy is BALANCED
    mock_store.ensure_position.assert_called_once()


def test_register_position_requires_learner_when_no_explicit(mock_store, mock_jupiter, mock_paper):
    engine = MagicMock()
    mgr = ExitManager(
        engine=engine,
        jupiter=mock_jupiter,
        store=mock_store,
        learner=None,
        paper=mock_paper,
    )
    with pytest.raises(ValueError):
        mgr.register_position(trade_id=1)


# ----------------------------------------------------------------------
# No learner path (backtests / fixed policy)
# ----------------------------------------------------------------------
def test_tick_runs_without_learner(mock_store, mock_jupiter, mock_paper):
    engine = MagicMock()
    mgr = ExitManager(
        engine=engine,
        jupiter=mock_jupiter,
        store=mock_store,
        learner=None,
        paper=mock_paper,
        clock=lambda: EPOCH + timedelta(seconds=60),
    )
    mock_store.list_open_positions.return_value = [_pos()]
    mock_jupiter.get_token_price.return_value = {"MINTXYZ": {"usdPrice": 70.0}}

    summary = mgr.tick()
    assert summary.closes == 1
    mock_paper.close_trade.assert_called_once()
