from __future__ import annotations

import math
from datetime import datetime, timezone

import pytest

from paper_log.gex_levels.definitions import (
    STATUS_GAP_THROUGH,
    STATUS_NONE,
    STATUS_REACHED,
    apply_costs,
    compute_h3_trade,
    evaluate_reach,
    is_held,
    range_ln,
)
from paper_log.gex_levels.market_data import Bar

T0 = datetime(2026, 9, 24, 9, 30, tzinfo=timezone.utc)


def _bar(i: int, o: float, h: float, l: float, c: float) -> Bar:  # noqa: E741
    return Bar(time=T0.replace(minute=(30 + 5 * i) % 60, hour=9 + (30 + 5 * i) // 60), open=o, high=h, low=l, close=c)


# ── range_ln ───────────────────────────────────────────────────────────


def test_range_ln() -> None:
    assert range_ln(110.0, 100.0) == pytest.approx(math.log(1.1))


# ── is_held ────────────────────────────────────────────────────────────


def test_is_held_true_when_close_same_side_as_open() -> None:
    # A resistance level above the open (110); close stays below it (held).
    assert is_held(level=110.0, session_open=100.0, session_close=105.0) is True


def test_is_held_false_when_close_crosses_to_other_side() -> None:
    assert is_held(level=110.0, session_open=100.0, session_close=115.0) is False


# ── evaluate_reach ─────────────────────────────────────────────────────


def test_reach_above_level_touched_by_high() -> None:
    bars = [
        _bar(0, 100, 101, 99, 100.5),
        _bar(1, 100.5, 106, 100, 105),  # high 106 >= level 105
    ]
    outcome = evaluate_reach(level=105.0, p0=100.0, session_open=100.0, session_close=104.0, bars=bars)
    assert outcome.status == STATUS_REACHED
    assert outcome.side == "above"
    assert outcome.bar_time == bars[1].time
    assert outcome.held is True  # close 104 < 105, same side as open (100 < 105)


def test_reach_above_level_broken_at_close() -> None:
    bars = [_bar(0, 100, 106, 99, 105)]
    outcome = evaluate_reach(level=105.0, p0=100.0, session_open=100.0, session_close=108.0, bars=bars)
    assert outcome.status == STATUS_REACHED
    assert outcome.held is False  # close 108 > 105, open 100 < 105 -> broke


def test_reach_below_level_touched_by_low() -> None:
    bars = [_bar(0, 100, 101, 94, 95)]  # low 94 <= level 95
    outcome = evaluate_reach(level=95.0, p0=100.0, session_open=100.0, session_close=96.0, bars=bars)
    assert outcome.status == STATUS_REACHED
    assert outcome.side == "below"
    assert outcome.held is True  # close 96 > 95, same side as open (100 > 95)


def test_gap_through_when_realized_open_crosses_level_relative_to_p0() -> None:
    # Level (105) was above P0 (100) when set, but the session gapped up
    # and opened at 110 — already past the level before the bell.
    bars = [_bar(0, 110, 112, 109, 111)]
    outcome = evaluate_reach(level=105.0, p0=100.0, session_open=110.0, session_close=111.0, bars=bars)
    assert outcome.status == STATUS_GAP_THROUGH
    assert outcome.bar_time is None
    assert outcome.held is None


def test_gap_through_downside() -> None:
    # Level (95) was below P0 (100), but session gapped down and opened at 90.
    bars = [_bar(0, 90, 91, 88, 89)]
    outcome = evaluate_reach(level=95.0, p0=100.0, session_open=90.0, session_close=89.0, bars=bars)
    assert outcome.status == STATUS_GAP_THROUGH


def test_no_reach_when_level_never_touched() -> None:
    bars = [_bar(0, 100, 102, 99, 101), _bar(1, 101, 103, 100, 102)]
    outcome = evaluate_reach(level=110.0, p0=100.0, session_open=100.0, session_close=102.0, bars=bars)
    assert outcome.status == STATUS_NONE
    assert outcome.held is None


def test_reach_uses_first_touching_bar_not_a_later_one() -> None:
    bars = [
        _bar(0, 100, 101, 99, 100),   # doesn't touch 105
        _bar(1, 100, 105, 100, 104),  # first touch
        _bar(2, 104, 107, 103, 106),  # also touches, but later
    ]
    outcome = evaluate_reach(level=105.0, p0=100.0, session_open=100.0, session_close=106.0, bars=bars)
    assert outcome.bar_time == bars[1].time


# ── apply_costs ────────────────────────────────────────────────────────


def test_apply_costs_long_is_adverse_both_sides() -> None:
    entry_adj, exit_adj, ret, pnl = apply_costs("long", entry_price=100.0, exit_price=110.0)
    assert entry_adj == pytest.approx(100.0 * 1.0001)
    assert exit_adj == pytest.approx(110.0 * 0.9999)
    assert ret == pytest.approx((exit_adj - entry_adj) / entry_adj)
    assert pnl == pytest.approx(ret * 1000.0)


def test_apply_costs_short_is_adverse_both_sides() -> None:
    entry_adj, exit_adj, ret, pnl = apply_costs("short", entry_price=100.0, exit_price=90.0)
    assert entry_adj == pytest.approx(100.0 * 0.9999)
    assert exit_adj == pytest.approx(90.0 * 1.0001)
    assert ret == pytest.approx((entry_adj - exit_adj) / entry_adj)
    assert pnl == pytest.approx(ret * 1000.0)


def test_apply_costs_rejects_unknown_direction() -> None:
    with pytest.raises(ValueError):
        apply_costs("sideways", 100.0, 110.0)


# ── compute_h3_trade ───────────────────────────────────────────────────


def test_neutral_regime_never_trades() -> None:
    bars = [_bar(0, 100, 130, 70, 100)]  # would trigger everything if regime mattered
    trade = compute_h3_trade("NEUTRAL", {"put_wall": 90.0, "call_wall": 110.0}, 100.0, 100.0, 100.0, bars)
    assert trade.triggered is False
    assert trade.regime_rule == "none"


def test_unrecognized_regime_fails_safe_to_no_trade() -> None:
    bars = [_bar(0, 100, 130, 70, 100)]
    trade = compute_h3_trade("SOMETHING_ELSE", {"put_wall": 90.0, "call_wall": 110.0}, 100.0, 100.0, 100.0, bars)
    assert trade.triggered is False
    assert trade.regime_rule == "none"


def test_long_gamma_fade_shorts_call_wall_reach() -> None:
    bars = [_bar(0, 100, 111, 99, 108)]  # touches call_wall 110
    trade = compute_h3_trade(
        "LONG_GAMMA", {"put_wall": 90.0, "call_wall": 110.0},
        p0=100.0, session_open=100.0, session_close=108.0, bars=bars,
    )
    assert trade.triggered is True
    assert trade.regime_rule == "fade"
    assert trade.direction == "short"
    assert trade.trigger_level_name == "call_wall"
    assert trade.raw_entry_price == 110.0  # "pretend short AT THE WALL"
    assert trade.raw_exit_price == 108.0  # session close


def test_long_gamma_fade_longs_put_wall_reach() -> None:
    bars = [_bar(0, 100, 101, 89, 92)]  # touches put_wall 90
    trade = compute_h3_trade(
        "LONG_GAMMA", {"put_wall": 90.0, "call_wall": 110.0},
        p0=100.0, session_open=100.0, session_close=95.0, bars=bars,
    )
    assert trade.triggered is True
    assert trade.direction == "long"
    assert trade.trigger_level_name == "put_wall"
    assert trade.raw_entry_price == 90.0


def test_long_gamma_fade_first_trigger_in_time_wins() -> None:
    # Put wall (90) is reached at bar 0; call wall (110) at bar 1. Even
    # though both eventually trigger, only the FIRST (put, long) trades.
    bars = [
        _bar(0, 100, 101, 89, 95),   # touches put_wall 90 first
        _bar(1, 95, 111, 95, 108),   # touches call_wall 110 second
    ]
    trade = compute_h3_trade(
        "LONG_GAMMA", {"put_wall": 90.0, "call_wall": 110.0},
        p0=100.0, session_open=100.0, session_close=108.0, bars=bars,
    )
    assert trade.triggered is True
    assert trade.trigger_level_name == "put_wall"
    assert trade.direction == "long"


def test_long_gamma_fade_gap_through_does_not_trigger() -> None:
    # Session opens already beyond the call wall (gap-through) — per the
    # Definitions section this is not a "reach", so no fade trade fires
    # off it even though price is on the far side of the wall all day.
    bars = [_bar(0, 112, 115, 111, 113)]
    trade = compute_h3_trade(
        "LONG_GAMMA", {"put_wall": 90.0, "call_wall": 110.0},
        p0=100.0, session_open=112.0, session_close=113.0, bars=bars,
    )
    assert trade.triggered is False


def test_short_gamma_follow_shorts_first_close_below_put_wall() -> None:
    bars = [
        _bar(0, 100, 100, 92, 95),   # close 95, not below 90
        _bar(1, 95, 96, 88, 89),     # close 89 < put_wall 90 -> trigger
        _bar(2, 89, 90, 85, 86),     # also below, but later
    ]
    trade = compute_h3_trade(
        "SHORT_GAMMA", {"put_wall": 90.0, "call_wall": 110.0},
        p0=100.0, session_open=100.0, session_close=86.0, bars=bars,
    )
    assert trade.triggered is True
    assert trade.regime_rule == "follow"
    assert trade.direction == "short"
    assert trade.trigger_level_name == "put_wall"
    assert trade.raw_entry_price == 89.0  # "at that bar's close"
    assert trade.trigger_time == bars[1].time


def test_short_gamma_follow_longs_first_close_above_call_wall() -> None:
    bars = [_bar(0, 100, 112, 99, 111)]  # close 111 > call_wall 110
    trade = compute_h3_trade(
        "SHORT_GAMMA", {"put_wall": 90.0, "call_wall": 110.0},
        p0=100.0, session_open=100.0, session_close=111.0, bars=bars,
    )
    assert trade.triggered is True
    assert trade.direction == "long"
    assert trade.trigger_level_name == "call_wall"
    assert trade.raw_entry_price == 111.0


def test_short_gamma_follow_ignores_reach_gap_through_carveout() -> None:
    # Follow-mode triggers purely on bar CLOSE vs wall — even a session
    # that gapped through the put wall at the open should still trigger
    # the follow rule the moment a bar closes below it (unlike fade mode).
    bars = [_bar(0, 85, 86, 83, 84)]  # opened already below put_wall(90); close 84 < 90
    trade = compute_h3_trade(
        "SHORT_GAMMA", {"put_wall": 90.0, "call_wall": 110.0},
        p0=100.0, session_open=85.0, session_close=84.0, bars=bars,
    )
    assert trade.triggered is True
    assert trade.direction == "short"


def test_no_trade_when_no_trigger_fires() -> None:
    bars = [_bar(0, 100, 101, 99, 100)]
    trade = compute_h3_trade(
        "SHORT_GAMMA", {"put_wall": 90.0, "call_wall": 110.0},
        p0=100.0, session_open=100.0, session_close=100.0, bars=bars,
    )
    assert trade.triggered is False
    assert trade.regime_rule == "follow"


def test_wall_absent_from_levels_dict_cannot_trigger() -> None:
    # Simulates a dropped placebo wall — only put_wall is passed in.
    bars = [_bar(0, 100, 112, 99, 111)]  # would trigger call_wall if present
    trade = compute_h3_trade(
        "SHORT_GAMMA", {"put_wall": 90.0},
        p0=100.0, session_open=100.0, session_close=111.0, bars=bars,
    )
    assert trade.triggered is False
