"""The "reasonably sane" gate a proposal passes before it may be registered.

Every check runs on the proposal and the ledger state only; none reads market data,
so a rejected proposal spends nothing. The report (``{G1: "ok" | reason, ...}``) is
written into the registration (all "ok") or into ``rejections.jsonl``.

G1 mechanism + citation · G2 direction depends on market state, not the clock ·
G3 directional rules trade and are judged on both sides · G4 costs at least the
policy floor · G5 the judged exit is one the account can take (the edge6 lesson: a
SPY fly that pays only held to settlement is not a rule for an account that must
close at 15:45) · G6 the window can supply n · G7 the data is fresh · G8 budget ·
G9 decision before entry, lookahead-guarded harness · G10 instruments on the liquid list ·
G11 the trade can pay its costs.

G11 (session families; an event family has no session trade to price) is the one check that
reads bars, and only bars from **before** the window: on those
sessions it takes the family's entry and exit prices for every instrument and compares the
policy cost floor (round trip) with the mean unsigned move between them. A directional rule
that is right a fraction p of the time earns about (2p - 1) x mean |move| before costs, so
it breaks even at p* = (1 + cost / mean |move|) / 2. The trial is pooled, so p* is taken
over all instruments' sessions together, and the proposal is refused when it exceeds
:data:`MAX_BREAKEVEN_HIT` (per-instrument figures are reported beside it). No direction, side or outcome is read, and
no window session is touched, so the look still sees its window for the first time.
"""

from __future__ import annotations

from datetime import date, timedelta
from itertools import pairwise

from . import policy as P
from .data import RETENTION_DAYS, SourceError
from .families import FAMILIES, _plus1
from .ledger import StreamState, conflicts
from .stats import bootstrap_b

MIN_MECHANISM_CHARS = 80

#: G11. Calendar days before the window start whose sessions are measured (clamped to
#: Polygon's retention; the first fetched session only supplies a prior close). A history
#: window starts 700 days back, so it gets ~13 sessions; a forward window ~16.
PREWINDOW_DAYS = 24
PREWINDOW_MIN_TRADES = 10
#: The pooled hit rate a directional rule would need to break even. Set after the first
#: three stream looks (IM1 metals, energy, intl_equity, all FAIL): on their windows the
#: pooled break-even was 0.69 / 1.18 / 0.95, and the best gross edge any of their nine
#: instruments showed was 0.21 x mean |move| (GLD), an effective hit rate of ~0.60. The
#: gate only refuses; it can never pass a rule, so setting it from spent looks costs no
#: false positive.
MAX_BREAKEVEN_HIT = 0.60

_feasibility_source = None


def set_feasibility_source(source) -> None:
    """The bar source G11 reads; the CLI sets it for a cycle and clears it after.

    Not an argument of :func:`check` on purpose: the runner is a hashed evaluation module
    (``families.EVALUATION_MODULES``), so threading a source through it would change the
    code hash of every open registration and abandon it on deploy. With no source set
    (unit tests of the runner) G11 is not run.
    """
    global _feasibility_source
    _feasibility_source = source


def breakeven_hits(proposal: dict, source, today: date) -> dict[str, dict]:
    """From the sessions before the window, per instrument and ``"pooled"``: trades, mean
    |move|, mean round-trip cost and the break-even hit rate. Unsigned sizes only."""
    family = FAMILIES[proposal["family"]]
    start = date.fromisoformat(proposal["window"]["start"])
    first = max(start - timedelta(days=PREWINDOW_DAYS), today - timedelta(days=RETENTION_DAYS - 1))
    per, all_moves, all_costs = {}, [], []
    for t in proposal["instruments"]:
        bars = source.sessions(t, first, start)  # end exclusive: no window session
        days = [d for d in sorted(bars) if d < start.isoformat()]
        moves, costs = [], []
        for prev, day in pairwise(days):
            tr = family.session_trade(bars[prev], bars[day], t, day)
            if tr is None:
                continue
            moves.append(abs(tr["exit"] / tr["entry"] - 1.0))
            costs.append(tr["cost_rt"])
        per[t] = _summary(moves, costs, days[1] if len(days) > 1 else None, days[-1] if days else None)
        all_moves += moves
        all_costs += costs
    froms = [v["from"] for v in per.values() if v["from"]]
    tos = [v["to"] for v in per.values() if v["to"]]
    per["pooled"] = _summary(all_moves, all_costs, min(froms, default=None), max(tos, default=None))
    return per


def _summary(moves: list[float], costs: list[float], first: str | None, last: str | None) -> dict:
    mean_move = sum(moves) / len(moves) if moves else 0.0
    mean_cost = sum(costs) / len(costs) if costs else 0.0
    hit = (1.0 + mean_cost / mean_move) / 2.0 if mean_move > 0 else float("inf")
    return {"n": len(moves), "mean_abs_move_bp": round(mean_move * 1e4, 2), "cost_rt_bp": round(mean_cost * 1e4, 2),
            "breakeven_hit": round(hit, 3), "from": first, "to": last}


def _g11(proposal: dict, source, today: date) -> str:
    try:
        m = breakeven_hits(proposal, source, today)
    except SourceError as exc:
        return f"bar source: {exc}"
    pooled = m["pooled"]
    if pooled["n"] < PREWINDOW_MIN_TRADES:
        return f"fewer than {PREWINDOW_MIN_TRADES} pre-window trades ({pooled['n']})"
    if pooled["breakeven_hit"] <= MAX_BREAKEVEN_HIT:
        return "ok"
    return (f"pooled break-even hit rate {pooled['breakeven_hit']} above {MAX_BREAKEVEN_HIT} at the policy cost floor "
            f"(cost {pooled['cost_rt_bp']} bp vs mean |move| {pooled['mean_abs_move_bp']} bp, "
            f"{pooled['from']}..{pooled['to']}): " + "; ".join(
                f"{t} {v['breakeven_hit']}" for t, v in m.items() if t != "pooled"))


def _weekdays(start: str, end: str) -> int:
    d, stop, n = date.fromisoformat(start), date.fromisoformat(end), 0
    while d < stop:
        n += d.weekday() < 5
        d += timedelta(days=1)
    return n


def check(proposal: dict, state: StreamState, today: date) -> dict[str, str]:
    r: dict[str, str] = {}
    family = FAMILIES.get(proposal.get("family"))
    instruments = list(proposal.get("instruments") or [])
    shape = []
    if proposal.get("lane") not in ("history", "forward"):
        shape.append(f"lane {proposal.get('lane')!r}")
    if proposal.get("horizon") not in P.HORIZONS:
        shape.append(f"horizon {proposal.get('horizon')!r}")
    if not all(isinstance(t, str) for t in instruments) or len(set(instruments)) != len(instruments):
        shape.append("instruments must be distinct ticker strings")
    if proposal.get("stage") not in (1, 2):
        shape.append("stage")
    r["G0_wellformed"] = "ok" if not shape else "malformed: " + ", ".join(shape)
    if shape:
        return r

    # G1 mechanism
    mech, cite = proposal.get("mechanism") or "", proposal.get("citation") or ""
    r["G1_mechanism"] = "ok" if family and len(mech) >= MIN_MECHANISM_CHARS and len(cite) >= 10 else \
        "no implemented family, or mechanism / citation missing"

    # G2 direction from state, not clock
    if family is None:
        r["G2_direction"] = "no family"
    else:
        inputs = set(proposal["direction_rule"]["inputs"])
        a, b = family.direction_probe()
        if not inputs or inputs <= P.CLOCK_INPUTS:
            r["G2_direction"] = "direction uses only clock/calendar inputs"
        elif a.get("time_of_day") != b.get("time_of_day") or family.direction(a) == family.direction(b):
            r["G2_direction"] = "direction does not change with market state at a fixed clock"
        else:
            r["G2_direction"] = "ok"

    # G3 both sides
    if proposal.get("directional"):
        r["G3_both_sides"] = "ok" if proposal.get("sides") == "both" else "a directional rule must trade both sides"
    else:
        r["G3_both_sides"] = "ok" if proposal.get("non_directional_reason") else \
            "non-directional rules must state why"

    # G4 costs
    bad = [t for t in instruments
           if P.LIQUIDITY.get(t) is None
           or (proposal["costs"].get(t) or {}).get("usd", 0) < P.COST_FLOORS[P.LIQUIDITY[t]]["usd"]
           or (proposal["costs"].get(t) or {}).get("bp", 0) < P.COST_FLOORS[P.LIQUIDITY[t]]["bp"]]
    hedge = proposal.get("hedge")
    if hedge is not None:
        tier = P.LIQUIDITY.get(hedge.get("instrument"))
        if hedge.get("instrument") != P.HEDGE_INSTRUMENT or tier is None \
                or hedge.get("usd", 0) < P.COST_FLOORS[tier]["usd"] or hedge.get("bp", 0) < P.COST_FLOORS[tier]["bp"] \
                or proposal.get("short_carry_annual", 0) < P.SHORT_CARRY_ANNUAL:
            bad.append("hedge leg / short carry")
    r["G4_costs"] = "ok" if instruments and not bad else f"costs below the policy floor for {bad}"

    # G5 executable exit
    acct = P.ACCOUNTS.get(proposal.get("account"))
    if acct is None:
        r["G5_exit"] = f"unknown account {proposal.get('account')!r}"
    elif proposal.get("exit_kind") not in acct["exits"]:
        r["G5_exit"] = f"exit {proposal.get('exit_kind')!r} not available in {proposal['account']}"
    elif proposal.get("instrument_kind") == "shares" and proposal.get("sides") in ("both", "short") \
            and not acct["shares_short"]:
        r["G5_exit"] = f"{proposal['account']} cannot short shares"
    elif proposal.get("instrument_kind") == "spread" and not acct["spreads"]:
        r["G5_exit"] = f"{proposal['account']} cannot hold spreads"
    else:
        r["G5_exit"] = "ok"

    # G6 power
    w = proposal["window"]
    if proposal["lane"] == "history":
        cap = _weekdays(w["start"], w["end"]) * len(instruments) * proposal["max_fires_per_session"]
    else:
        cap = w.get("max_sessions", 0) * len(instruments) * proposal["max_fires_per_session"]
    r["G6_power"] = "ok" if cap >= 1.2 * P.N_MIN else f"window can supply at most {cap} trades"

    # G7 fresh data
    groups = {P.group_of(t) for t in instruments}
    if groups != {proposal.get("group")}:
        r["G7_fresh"] = f"instruments span groups {sorted(map(str, groups))}, not just {proposal.get('group')}"
    elif proposal["lane"] == "history" and not (w.get("end") and w["start"] < w["end"] <= today.isoformat()):
        r["G7_fresh"] = "history window must end by today"
    elif proposal["lane"] == "forward" and not w["start"] > today.isoformat():
        r["G7_fresh"] = "forward window must start after today"
    else:
        hits = conflicts(state, proposal["group"], proposal["horizon"], w["start"], w.get("end"), proposal["lane"],
                         proposal.get("family"))
        r["G7_fresh"] = "ok" if not hits else "reuses consumed data: " + "; ".join(
            f"{h['start']}..{h['end']} ({h['by']})" for h in hits)

    # G8 budget
    epoch = proposal.get("epoch", today.year)
    closed = state.closed_families().get((proposal.get("family"), proposal.get("group")))
    if proposal["stage"] == 1 and state.stage1_count(epoch) >= P.MAX_REGISTRATIONS_PER_YEAR:
        r["G8_budget"] = f"{epoch} budget of {P.MAX_REGISTRATIONS_PER_YEAR} looks spent"
    elif proposal["stage"] == 1 and closed:
        r["G8_budget"] = f"family closed in this group: its confirmation {closed} did not pass"
    else:
        try:
            bootstrap_b(proposal["alpha_trial"])
            r["G8_budget"] = "ok" if proposal.get("k", 0) <= P.MAX_TRIALS_PER_LOOK else "too many trials"
        except ValueError as exc:
            r["G8_budget"] = str(exc)

    # G9 decision before entry, guarded harness
    if family is None:
        r["G9_no_lookahead"] = "no family"
    elif getattr(family, "kind", "session") == "event":
        # decided at the reaction day's close, entered at the next session's open; the family's
        # decide() refuses any bar after the reaction day (LookaheadError)
        ok = (family.decision, family.entry) == ("reaction-day close", "next session open") \
            and hasattr(family, "decide") and hasattr(family, "event_trade")
        r["G9_no_lookahead"] = "ok" if ok else "entry is not after the decision"
    else:
        # the entry price is the close of the entry bar: it must not be earlier than the decision
        ok = _plus1(family.entry_hm) >= family.decision_hm and hasattr(family, "session_trade")
        r["G9_no_lookahead"] = "ok" if ok else "entry is priced before the decision"

    # G10 liquid list
    missing = [t for t in instruments if t not in P.LIQUIDITY]
    r["G10_liquid"] = "ok" if instruments and not missing else f"not on the liquid list: {missing}"

    # G11 cost feasibility (stage 1 only: a confirmation follows a look judged net of costs).
    # Session families only: it prices the family's own session_trade on minute bars. An event
    # family (EC1: daily bars, a multi-day hedged hold) has no session trade to price; its look
    # is judged net of its four cost floors and the short-leg carry.
    if (_feasibility_source is not None and proposal["stage"] == 1 and family is not None
            and getattr(family, "kind", "session") == "session"
            and all(v == "ok" for v in r.values())):
        r["G11_cost_feasible"] = _g11(proposal, _feasibility_source, today)
    return r


def passed(report: dict[str, str]) -> bool:
    return bool(report) and all(v == "ok" for v in report.values())
