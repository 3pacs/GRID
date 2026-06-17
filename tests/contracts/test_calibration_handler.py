"""Tests for contracts.handlers.calibration."""
from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from contracts.handlers import calibration
from contracts.schemas import OptionsTradeOutcome


def _options_trade_outcome(
    pnl: float,
    strategy: str = "contagion_long",
) -> OptionsTradeOutcome:
    return OptionsTradeOutcome(
        producer_module="trading.contagion_to_ticket",
        correlation_id=uuid4(),
        trade_id=99,
        ticker="XOM",
        strategy=strategy,
        pnl=Decimal(str(pnl)),
        signal_mix={"contagion_ranked_impact": 1.0},
        hit_levels={"closed": True},
        duration_s=3600,
    )


@pytest.mark.parametrize(
    ("pnl", "expected_actual"),
    [
        (125.0, 1.0),
        (0.0, 0.0),
        (-50.0, 0.0),
    ],
)
def test_options_trade_outcome_updates_strategy_calibration(
    pnl,
    expected_actual,
):
    engine = MagicMock()

    with patch("oracle.calibration.update_running_metrics") as update:
        calibration.on_options_trade_outcome(
            _options_trade_outcome(pnl),
            engine=engine,
        )

    update.assert_called_once_with(
        engine,
        model_id="strategy:contagion_long",
        prediction=0.5,
        actual=expected_actual,
    )


def test_options_trade_outcome_missing_strategy_is_noop():
    engine = MagicMock()

    with patch("oracle.calibration.update_running_metrics") as update:
        calibration.on_options_trade_outcome(
            _options_trade_outcome(125.0, strategy=""),
            engine=engine,
        )

    update.assert_not_called()
