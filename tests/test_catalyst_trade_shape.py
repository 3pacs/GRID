"""Trade-shape gating on the catalyst board.

The board's two headline numbers answer different questions and pointed in
opposite directions on the 2026-09-11 board: every ranked name carried a
POSITIVE edge over the option chain while every one had an expected value
BELOW 1.0. Read as a buy-the-stock list it was uniformly negative EV; read
as a defined-risk option list it was the opposite. These pin that a reader
cannot get that backwards again.
"""
from __future__ import annotations

from intelligence.catalyst_ev import (
    TRADE_SHAPE_EQUITY,
    TRADE_SHAPE_NONE,
    TRADE_SHAPE_OPTION_TAIL,
    catalyst_ev,
    classify_trade_shape,
)


class TestClassifyTradeShape:
    def test_negative_edge_is_never_a_trade(self):
        # No disagreement with the chain means nothing to harvest, however
        # pretty the EV looks.
        assert classify_trade_shape(expected_value_multiple=3.0, edge_vs_market=-0.01) == TRADE_SHAPE_NONE
        assert classify_trade_shape(expected_value_multiple=3.0, edge_vs_market=0.0) == TRADE_SHAPE_NONE

    def test_positive_edge_with_weak_ev_is_defined_risk_only(self):
        # The real 2026-09-11 IDYA row: p 0.247 vs a 15% tail, EV 0.44.
        assert (
            classify_trade_shape(expected_value_multiple=0.44, edge_vs_market=0.097)
            == TRADE_SHAPE_OPTION_TAIL
        )

    def test_positive_edge_with_ev_above_one_allows_equity(self):
        assert (
            classify_trade_shape(expected_value_multiple=1.8, edge_vs_market=0.10)
            == TRADE_SHAPE_EQUITY
        )

    def test_ev_exactly_one_is_not_equity_grade(self):
        # Fair at today's price is not an edge on the equity leg.
        assert (
            classify_trade_shape(expected_value_multiple=1.0, edge_vs_market=0.05)
            == TRADE_SHAPE_OPTION_TAIL
        )

    def test_missing_inputs_degrade_to_no_trade(self):
        assert classify_trade_shape(expected_value_multiple=None, edge_vs_market=None) == TRADE_SHAPE_NONE
        assert classify_trade_shape(expected_value_multiple=2.0, edge_vs_market=None) == TRADE_SHAPE_NONE

    def test_nan_and_inf_do_not_leak_through(self):
        assert classify_trade_shape(expected_value_multiple=float("nan"), edge_vs_market=float("nan")) == TRADE_SHAPE_NONE
        assert classify_trade_shape(expected_value_multiple=float("inf"), edge_vs_market=float("inf")) == TRADE_SHAPE_NONE


class TestBoardRowsMatchTheLiveBoard:
    def test_live_idya_row_reproduces_and_is_option_only(self):
        # Verbatim from the 2026-09-11 board.
        ev = catalyst_ev(
            p_success=0.247,
            upside_multiple=1.76,
            downside_multiple=0.007,
            market_implied_p=0.15,
        )
        assert ev is not None
        assert round(ev["expected_value_multiple"], 2) == 0.44
        assert ev["edge_vs_market"] > 0
        # Needs ~57% to break even on the equity; GRID's prior is 24.7%.
        assert ev["breakeven_probability"] > 0.55
        assert (
            classify_trade_shape(
                expected_value_multiple=ev["expected_value_multiple"],
                edge_vs_market=ev["edge_vs_market"],
            )
            == TRADE_SHAPE_OPTION_TAIL
        )

    def test_partial_legs_return_none_rather_than_a_misleading_ev(self):
        assert catalyst_ev(p_success=0.3, upside_multiple=None, downside_multiple=0.1) is None
        assert catalyst_ev(p_success=None, upside_multiple=1.5, downside_multiple=0.1) is None
