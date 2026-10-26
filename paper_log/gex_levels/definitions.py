"""Reach / held / range / H3-trade — the pre-registration's "Definitions"
and "Hypotheses" H3 section, translated to code. Shared by ``postclose.py``
(computes these once per session, writes them to the record) and
``evaluate.py`` (re-reads the written outcomes; does not recompute them).

Side and gap-through (Amendment 1 made this literal and unambiguous —
history for anyone diffing against the pre-amendment version of this
file): "Side: a level above P0 is an 'above' level; a level below P0 is a
'below' level." / "If the session opens at or beyond the level (open >= L
for an above level, open <= L for a below level), it is a gap-through."
Side is fixed by P0, not by the realized open; gap-through is then a
direct, boundary-inclusive comparison of the realized open against L.
(The original pre-registration text was ambiguous here — "a level above
the open" vs. "the open is already beyond L" read as contradictory taken
completely literally — and this module's first version resolved that
ambiguity by inferring the P0-anchored reading via a side-vs-P0-vs-
side-vs-open comparison. Amendment 1 confirms that inferred reading was
right in substance, but is stricter at the exact boundary open == L than
that inference was — this version implements the amendment's literal
`open >= L` / `open <= L` directly rather than keeping the old derivation.)
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Sequence

from paper_log.gex_levels.config import (
    NOTIONAL_USD,
    REGIME_LONG_GAMMA,
    REGIME_NEUTRAL,
    REGIME_SHORT_GAMMA,
    SLIPPAGE_BP_PER_SIDE,
)
from paper_log.gex_levels.market_data import Bar

STATUS_REACHED = "reached"
STATUS_GAP_THROUGH = "gap_through"
STATUS_NONE = "none"

SIDE_ABOVE = "above"
SIDE_BELOW = "below"


@dataclass(frozen=True)
class ReachOutcome:
    status: str  # "reached" | "gap_through" | "none"
    side: str  # "above" | "below" — level's side relative to P0
    bar_time: datetime | None
    held: bool | None  # only meaningful when status == "reached"


def _level_side(level: float, p0: float) -> str:
    """"a level above P0 is an 'above' level; a level below P0 is a 'below'
    level" (Amendment 1). ``level == p0`` exactly is not addressed by the
    text either; treated as "above" for a single, documented, harmless
    tie-break — real P0/level floats coinciding exactly is not a case that
    occurs in practice."""
    return SIDE_ABOVE if level >= p0 else SIDE_BELOW


def is_held(level: float, session_open: float, session_close: float) -> bool:
    """"the session closes on the same side of L as the open."""
    open_above = session_open >= level
    close_above = session_close >= level
    return open_above == close_above


def evaluate_reach(
    level: float,
    p0: float,
    session_open: float,
    session_close: float,
    bars: Sequence[Bar],
) -> ReachOutcome:
    """Reached / gap-through / none, per the pre-registration's Definitions
    (Amendment 1's literal side/gap-through wording)."""
    side = _level_side(level, p0)

    gap_through = session_open >= level if side == SIDE_ABOVE else session_open <= level
    if gap_through:
        return ReachOutcome(status=STATUS_GAP_THROUGH, side=side, bar_time=None, held=None)

    for bar in bars:
        touched = bar.high >= level if side == SIDE_ABOVE else bar.low <= level
        if touched:
            return ReachOutcome(
                status=STATUS_REACHED,
                side=side,
                bar_time=bar.time,
                held=is_held(level, session_open, session_close),
            )

    return ReachOutcome(status=STATUS_NONE, side=side, bar_time=None, held=None)


def range_ln(session_high: float, session_low: float) -> float:
    """ln(session high / session low)."""
    return math.log(session_high / session_low)


# ── H3: the level-rule trade ────────────────────────────────────────────

DIRECTION_LONG = "long"
DIRECTION_SHORT = "short"

RULE_FADE = "fade"
RULE_FOLLOW = "follow"
RULE_NONE = "none"


@dataclass(frozen=True)
class TradeOutcome:
    triggered: bool
    regime_rule: str  # "fade" | "follow" | "none"
    direction: str | None
    trigger_level_name: str | None  # "put_wall" | "call_wall"
    trigger_time: datetime | None
    raw_entry_price: float | None
    raw_exit_price: float | None
    entry_price_after_cost: float | None
    exit_price_after_cost: float | None
    return_pct: float | None
    pnl_usd: float | None


def _no_trade(regime_rule: str) -> TradeOutcome:
    return TradeOutcome(
        triggered=False, regime_rule=regime_rule, direction=None,
        trigger_level_name=None, trigger_time=None, raw_entry_price=None,
        raw_exit_price=None, entry_price_after_cost=None,
        exit_price_after_cost=None, return_pct=None, pnl_usd=None,
    )


def apply_costs(direction: str, entry_price: float, exit_price: float) -> tuple[float, float, float, float]:
    """1bp adverse slippage per side, $1,000 notional. Returns
    (entry_after_cost, exit_after_cost, return_pct, pnl_usd)."""
    bp = SLIPPAGE_BP_PER_SIDE / 10_000.0
    if direction == DIRECTION_LONG:
        entry_adj = entry_price * (1 + bp)
        exit_adj = exit_price * (1 - bp)
        return_pct = (exit_adj - entry_adj) / entry_adj
    elif direction == DIRECTION_SHORT:
        entry_adj = entry_price * (1 - bp)
        exit_adj = exit_price * (1 + bp)
        return_pct = (entry_adj - exit_adj) / entry_adj
    else:
        raise ValueError(f"unknown direction: {direction!r}")

    return entry_adj, exit_adj, return_pct, return_pct * NOTIONAL_USD


def _first_close_beyond(level: float, bars: Sequence[Bar], *, above: bool) -> Bar | None:
    for bar in bars:
        if (bar.close > level) if above else (bar.close < level):
            return bar
    return None


def compute_h3_trade(
    regime: str | None,
    levels: dict[str, float],
    p0: float,
    session_open: float,
    session_close: float,
    bars: Sequence[Bar],
) -> TradeOutcome:
    """The level-rule trade for one regime, real or placebo walls.

    ``levels`` maps a subset of {"put_wall", "call_wall"} to the wall value
    to trade against (real values, or placebo mirror values — the caller
    decides; a wall absent from the dict, e.g. because its placebo was
    dropped by the 0.10% collision rule, simply cannot trigger).
    """
    if regime == REGIME_NEUTRAL or regime is None:
        return _no_trade(RULE_NONE)

    candidates: list[tuple[datetime, str, str, float]] = []

    if regime == REGIME_LONG_GAMMA:
        # Fade: first reach of the call wall -> short at the wall;
        # first reach of the put wall -> long at the wall.
        for level_name, direction in (("call_wall", DIRECTION_SHORT), ("put_wall", DIRECTION_LONG)):
            level = levels.get(level_name)
            if level is None:
                continue
            outcome = evaluate_reach(level, p0, session_open, session_close, bars)
            if outcome.status == STATUS_REACHED:
                candidates.append((outcome.bar_time, direction, level_name, level))
        regime_rule = RULE_FADE

    elif regime == REGIME_SHORT_GAMMA:
        # Follow: first bar closing below the put wall -> short at that
        # close; first bar closing above the call wall -> long at that close.
        put_wall = levels.get("put_wall")
        if put_wall is not None:
            bar = _first_close_beyond(put_wall, bars, above=False)
            if bar is not None:
                candidates.append((bar.time, DIRECTION_SHORT, "put_wall", bar.close))
        call_wall = levels.get("call_wall")
        if call_wall is not None:
            bar = _first_close_beyond(call_wall, bars, above=True)
            if bar is not None:
                candidates.append((bar.time, DIRECTION_LONG, "call_wall", bar.close))
        regime_rule = RULE_FOLLOW

    else:
        # Unrecognized regime string from the engine — fail safe to no-trade
        # rather than guessing a rule; recorded regime is still whatever the
        # engine returned, so this is visible in the record, not swallowed.
        return _no_trade(RULE_NONE)

    if not candidates:
        return _no_trade(regime_rule)

    # At most one trade per session: the first trigger in time.
    candidates.sort(key=lambda c: c[0])
    trigger_time, direction, level_name, entry_price = candidates[0]
    entry_adj, exit_adj, return_pct, pnl_usd = apply_costs(direction, entry_price, session_close)

    return TradeOutcome(
        triggered=True,
        regime_rule=regime_rule,
        direction=direction,
        trigger_level_name=level_name,
        trigger_time=trigger_time,
        raw_entry_price=entry_price,
        raw_exit_price=session_close,
        entry_price_after_cost=entry_adj,
        exit_price_after_cost=exit_adj,
        return_pct=return_pct,
        pnl_usd=pnl_usd,
    )
