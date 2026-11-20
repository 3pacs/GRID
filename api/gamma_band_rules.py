"""Prospective SPY $1-strike 0DTE band flags.

Call step once per *completed* regular-session bar, with the immediately prior
minute's observed SPY GEX frame. The bar minute is its opening UTC epoch minute.
The caller starts a new state each session. Only subsequent bars can grade an
emitted flag. Duplicate or out-of-order minutes are ignored on replay.

An upward close across a negative red with a positive adjacent lower strike
qualifies once per red per session. Scan contiguous negative strikes above red
to the first positive strike; missing or zero exposure breaks that scan. Freeze
both green targets. A loss requires a close strictly below red. A later close
strictly above red arms a reclaim; a subsequent close below red emits the
downward flag only if the upper green has not been touched during that attempt.
A successful reclaim can arm again only after a fresh loss. Touching the lower
green during a loss before reclaim ends the setup. Unknown intra-bar ordering
cannot qualify a failed reclaim after an upper touch on the same bar.
"""

from __future__ import annotations

import math


def _finite(value):
    return type(value) in (int, float) and math.isfinite(value)


def _upper(levels, red):
    if levels.get(red, 0) >= 0 or levels.get(red - 1, 0) <= 0:
        return None
    strike = red + 1
    while levels.get(strike, 0) < 0:
        strike += 1
    return strike if levels.get(strike, 0) > 0 else None


def _event(kind, red, upper, close):
    lower = red - 1
    return dict(
        kind=kind,
        red=red,
        lower=lower,
        upper=upper,
        target=upper if kind == "first_cross" else lower,
        close=close,
    )


def step(state, *, minute: int, bar: dict, levels: dict | None):
    """Advance without modifying the supplied JSON state; return (state, flags)."""
    if type(minute) is not int:
        raise ValueError("Expected a UTC epoch minute")
    old = state or {}
    previous = old.get("minute")
    if previous is not None and minute <= previous:
        return state, []
    seen = set(old.get("seen", []))
    active = {k: dict(v) for k, v in old.get("active", {}).items()}
    events = []

    close, opening, low, high = (
        (bar.get(k) for k in ("close", "open", "low", "high"))
        if isinstance(bar, dict)
        else (None,) * 4
    )
    if not all(_finite(v) and v > 0 for v in (opening, high, low, close)) or not (
        low <= min(close, opening) <= max(close, opening) <= high
    ):
        close = None
    # Only observed, integral SPY strikes can participate in this rule.
    levels = {
        int(k): v
        for k, v in (levels or {}).items()
        if _finite(k) and k > 0 and int(k) == k and _finite(v)
    }
    prior_close = old.get("close")
    if not levels or close is None or prior_close is None or previous != minute - 1:
        return dict(minute=minute, close=close, seen=sorted(seen), active={}), []

    for key, setup in list(active.items()):
        red, upper = int(key), setup["upper"]
        lower = red - 1
        phase = setup["phase"]
        if phase == "above" and close < red:
            if low <= lower or _upper(levels, red) != upper:
                del active[key]
            else:
                setup["phase"] = "lost"
        elif phase == "lost":
            if low <= lower:
                del active[key]
            elif close > red:
                if _upper(levels, red) != upper:
                    del active[key]
                else:
                    setup["phase"] = "above" if high >= upper else "reclaimed"
        elif phase == "reclaimed":
            if high >= upper:
                if close < red and (low <= lower or _upper(levels, red) != upper):
                    del active[key]
                else:
                    setup["phase"] = "lost" if close < red else "above"
            elif close < red:
                if close > lower and _upper(levels, red) == upper:
                    events.append(_event("failed_reclaim", red, upper, close))
                del active[key]

    for red in sorted(k for k in levels if prior_close <= k < close and k not in seen):
        upper = _upper(levels, red)
        if upper is None:
            continue
        seen.add(red)
        if close >= upper:
            continue  # The first crossing already reached its target at decision.
        events.append(_event("first_cross", red, upper, close))
        active[str(red)] = {"upper": upper, "phase": "above"}
    return dict(minute=minute, close=close, seen=sorted(seen), active=active), events
