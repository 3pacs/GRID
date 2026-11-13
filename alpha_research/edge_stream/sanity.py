"""The "reasonably sane" gate a proposal passes before it may be registered.

Every check runs on the proposal and the ledger state only; none reads market data,
so a rejected proposal spends nothing. The report (``{G1: "ok" | reason, ...}``) is
written into the registration (all "ok") or into ``rejections.jsonl``.

G1 mechanism + citation · G2 direction depends on market state, not the clock ·
G3 directional rules trade and are judged on both sides · G4 costs at least the
policy floor · G5 the judged exit is one the account can take (the edge6 lesson: a
SPY fly that pays only held to settlement is not a rule for an account that must
close at 15:45) · G6 the window can supply n · G7 the data is fresh · G8 budget ·
G9 decision before entry, lookahead-guarded harness · G10 instruments on the liquid list.
"""

from __future__ import annotations

from datetime import date, timedelta

from . import policy as P
from .families import FAMILIES, _plus1
from .ledger import StreamState, conflicts
from .stats import bootstrap_b

MIN_MECHANISM_CHARS = 80


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
    if proposal["stage"] == 1 and state.stage1_count(epoch) >= P.MAX_REGISTRATIONS_PER_YEAR:
        r["G8_budget"] = f"{epoch} budget of {P.MAX_REGISTRATIONS_PER_YEAR} looks spent"
    else:
        try:
            bootstrap_b(proposal["alpha_trial"])
            r["G8_budget"] = "ok" if proposal.get("k", 0) <= P.MAX_TRIALS_PER_LOOK else "too many trials"
        except ValueError as exc:
            r["G8_budget"] = str(exc)

    # G9 decision before entry, guarded harness
    if family is None:
        r["G9_no_lookahead"] = "no family"
    else:
        # the entry price is the close of the entry bar: it must not be earlier than the decision
        ok = _plus1(family.entry_hm) >= family.decision_hm and hasattr(family, "session_trade")
        r["G9_no_lookahead"] = "ok" if ok else "entry is priced before the decision"

    # G10 liquid list
    missing = [t for t in instruments if t not in P.LIQUIDITY]
    r["G10_liquid"] = "ok" if instruments and not missing else f"not on the liquid list: {missing}"
    return r


def passed(report: dict[str, str]) -> bool:
    return bool(report) and all(v == "ok" for v in report.values())
