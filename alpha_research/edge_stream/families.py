"""Rule families the stream may register, and the queue it registers them from.

A family is code: its decision, direction rule, entry and exit are fixed here and its
source is hashed into every registration (``code_sha256``); a look whose family code
changed after registration is abandoned, never run on different code. A new family is
a reviewed PR. Proposals from other agents (``inbox/*.json``) may only parameterise an
existing family (instruments, group, lane), and pass the same gate.

Every family hands its rule bars that end before the decision minute
(:func:`decision_bars`), and :func:`assert_no_lookahead` checks it: Polygon bars are
not GRID DB rows, so ``store/pit.py`` cannot guard them; this does.
"""

from __future__ import annotations

import ast
import hashlib
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from . import policy as P

Bar = tuple


class LookaheadError(AssertionError):
    pass


def decision_bars(bars: list[Bar], decision_hm: str) -> list[Bar]:
    """Bars that have closed by ``decision_hm`` (a bar starting at m closes at m + 1 min)."""
    return [b for b in bars if _plus1(b[0]) <= decision_hm]


def assert_no_lookahead(bars: list[Bar], decision_hm: str) -> None:
    late = [b[0] for b in bars if _plus1(b[0]) > decision_hm]
    if late:
        raise LookaheadError(f"bars after the {decision_hm} decision: {late[:3]}")


def _plus1(hm: str) -> str:
    h, m = int(hm[:2]), int(hm[3:]) + 1
    return f"{h + m // 60:02d}:{m % 60:02d}"


def close_at(bars: list[Bar], hm: str, max_age_min: int) -> float | None:
    """Close of the last bar starting at or before ``hm`` and no more than ``max_age_min`` earlier."""
    h, m = int(hm[:2]), int(hm[3:])
    floor_m = h * 60 + m - max_age_min
    floor = f"{floor_m // 60:02d}:{floor_m % 60:02d}"
    pick = None
    for b in bars:
        if floor <= b[0] <= hm:
            pick = b
    return None if pick is None else float(pick[4])


@dataclass(frozen=True)
class IntradayMomentum:
    """IM1: the first half hour's direction predicts the last half hour's."""

    id: str = "IM1"
    version: int = 1
    mechanism: str = (
        "Market intraday momentum: the return from the prior close to 10:00 ET predicts the return over the "
        "last half hour of the session. Channels: late-informed and infrequently rebalancing traders act at the "
        "close, and hedgers of short-gamma positions (leveraged ETFs, option dealers) trade in the direction of "
        "the day's move near the close."
    )
    citation: str = (
        "Gao, Han, Li & Zhou (2018), Market intraday momentum, JFE 129(2) 394-414; "
        "Baltussen, Da, Lammers & Martens (2021), Hedging demand and market intraday momentum, JFE 142(1) 377-403"
    )
    direction_rule: str = "long when prior-close -> 10:00 ET return > 0, short when < 0, no trade when 0"
    direction_inputs: tuple[str, ...] = ("ret_prior_close_to_1000",)
    horizon: str = "intraday"
    first_hm: str = "09:59"  # bar starting 09:59 closes at 10:00
    decision_hm: str = "15:30"
    entry_hm: str = "15:29"  # entry at the close of the 15:29 bar (15:30 price)
    exit_hm: str = "15:59"   # exit at the close of the 15:59 bar (16:00 price; MOC-executable)
    max_age_min: int = 4
    max_gap_move: float = 0.25  # |move| above this between bars = a basis jump (split), session skipped
    account: str = "schwab"
    instrument_kind: str = "shares"
    exit_kind: str = "rth_bar_close"
    sides: str = "both"
    directional: bool = True
    max_fires_per_session: int = 1

    def direction(self, state: dict) -> int:
        r = state["ret_prior_close_to_1000"]
        return 1 if r > 0 else (-1 if r < 0 else 0)

    def direction_probe(self) -> tuple[dict, dict]:
        """Two states at the same clock that must give different directions (gate G2)."""
        return ({"ret_prior_close_to_1000": 0.004, "time_of_day": "15:30"},
                {"ret_prior_close_to_1000": -0.004, "time_of_day": "15:30"})

    def spec(self) -> dict:
        return {
            "decision": f"at {self.decision_hm} ET, from bars closed by then",
            "entry": f"close of the {self.entry_hm} bar (age <= {self.max_age_min} min)",
            "exit": f"close of the {self.exit_hm} bar (age <= {self.max_age_min} min)",
            "skip": "no prior full-session close, a missing 10:00/15:30/16:00 price, zero first-half-hour return",
            "net": "side x (exit/entry - 1) - (cost_per_side(entry) + cost_per_side(exit)) as a fraction",
            "always_long_net": "(exit/entry - 1) - the same costs",
        }

    def session_trade(self, prev_bars: list[Bar], bars: list[Bar], instrument: str, session: str) -> dict | None:
        prior_close = close_at(prev_bars, "15:59", self.max_age_min)
        exit_px = close_at(bars, self.exit_hm, self.max_age_min)
        if prior_close is None or exit_px is None:
            return None  # not a full pair of sessions (early close, thin instrument)
        seen = decision_bars(bars, self.decision_hm)
        assert_no_lookahead(seen, self.decision_hm)
        p1000 = close_at(seen, self.first_hm, self.max_age_min)
        entry = close_at(seen, self.entry_hm, self.max_age_min)
        if p1000 is None or entry is None:
            return None
        if abs(p1000 / prior_close - 1.0) > self.max_gap_move or abs(exit_px / entry - 1.0) > self.max_gap_move:
            return None  # a split/adjustment-basis jump between cached sessions, not a market move
        side = self.direction({"ret_prior_close_to_1000": p1000 / prior_close - 1.0})
        if side == 0:
            return None
        cost = P.cost_per_side(instrument, entry) + P.cost_per_side(instrument, exit_px)
        move = exit_px / entry - 1.0
        return {
            "session": session, "instrument": instrument, "side": side,
            "entry": entry, "exit": exit_px, "r_first": p1000 / prior_close - 1.0,
            "gross": side * move, "cost_rt": cost, "net": side * move - cost, "always_long_net": move - cost,
        }

    def needed(self, bars: list[Bar]) -> bool:
        """Presence-only check (no price compared): the session has the bars a trade needs."""
        return all(close_at(bars, hm, self.max_age_min) is not None
                   for hm in (self.first_hm, self.entry_hm, self.exit_hm))


FAMILIES: dict[str, object] = {"IM1": IntradayMomentum()}


#: Modules whose code decides a verdict. Their AST (docstrings dropped, so comments and
#: docs never abandon a registration) is hashed into every registration with numpy's
#: major.minor (the bootstrap's RNG stream). Any real change to the evaluation path —
#: rule, trade selection, readiness, data filter, costs, test — changes the hash, and an
#: open registration is then abandoned rather than judged on code it was not sealed with.
EVALUATION_MODULES = ("families", "stats", "runner", "data", "policy")


def _ast_digest(source: str) -> str:
    tree = ast.parse(source)
    for node in ast.walk(tree):
        body = getattr(node, "body", None)
        if isinstance(body, list) and body and isinstance(body[0], ast.Expr) \
                and isinstance(getattr(body[0], "value", None), ast.Constant) and isinstance(body[0].value.value, str):
            node.body = body[1:] or [ast.Pass()]
    return hashlib.sha256(ast.dump(tree, include_attributes=False).encode()).hexdigest()


def code_sha256(family) -> str:
    import numpy

    here = Path(__file__).resolve().parent
    parts = [f"family={family.id}/v{family.version}", f"numpy={'.'.join(numpy.__version__.split('.')[:2])}",
             f"policy={P.policy_sha256()}"]
    parts += [f"{m}={_ast_digest((here / f'{m}.py').read_text())}" for m in EVALUATION_MODULES]
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()


# --- the queue ----------------------------------------------------------------------

#: What the stream registers first, in order (one new registration per weekday at most).
QUEUE: tuple[dict, ...] = (
    {"family": "IM1", "group": "metals", "instruments": ("GLD", "SLV"), "lane": "history"},
    {"family": "IM1", "group": "energy", "instruments": ("USO", "UNG"), "lane": "history"},
    {"family": "IM1", "group": "intl_equity", "instruments": ("EFA", "EEM", "EWJ", "FXI", "EWZ"), "lane": "history"},
    {"family": "IM1", "group": "crypto_etf", "instruments": ("IBIT", "ETHA"), "lane": "history"},
    {"family": "IM1", "group": "us_single_stocks",
     "instruments": ("AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA"), "lane": "history"},
    {"family": "IM1", "group": "fx", "instruments": ("FXE", "FXY", "UUP"), "lane": "history"},
    {"family": "IM1", "group": "us_equity_index", "instruments": ("SPY", "QQQ", "IWM"), "lane": "forward"},
)

#: Polygon's rolling window is ~728 days; a history window starts 700 days back so its
#: 10-day warm-up and a look up to two weeks later stay inside it (data.RETENTION_DAYS).
HISTORY_START_DAYS_BACK = 700


def next_weekday(d: date) -> date:
    d += timedelta(days=1)
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d


def build_proposal(item: dict, today: date, proposer: str = "template", confirms: str | None = None,
                   stage: int = 1) -> dict:
    """A full registration body (before the gate fills ``sanity``)."""
    family = FAMILIES[item["family"]]
    instruments = tuple(item["instruments"])
    lane = item["lane"]
    if lane == "history":
        window = {"start": (today - timedelta(days=HISTORY_START_DAYS_BACK)).isoformat(),
                  "end": today.isoformat()}
    else:
        window = {"start": next_weekday(today).isoformat(), "end": None,
                  "n_target_sessions": P.FORWARD_N_TARGET_SESSIONS, "max_sessions": P.FORWARD_MAX_SESSIONS}
    alpha_look = P.ALPHA_PER_LOOK if stage == 1 else P.CONFIRM_ALPHA
    trial = {"trial_id": "pooled", "instruments": list(instruments),
             "pooling": "all instruments' trades, resampled by session"}
    pid = f"{family.id}-v{family.version}:{item['group']}:{lane}:{window['start']}"
    if confirms:
        pid += f":confirms-{confirms[:12]}"
    return {
        "proposal_id": pid,
        "proposer": proposer,
        "stage": stage,
        "confirms": confirms,
        "epoch": today.year,
        "family": family.id,
        "family_version": family.version,
        "code_sha256": code_sha256(family),
        "mechanism": family.mechanism,
        "citation": family.citation,
        "lane": lane,
        "group": item["group"],
        "instruments": list(instruments),
        "horizon": family.horizon,
        "window": window,
        "decision_rule": family.spec(),
        "direction_rule": {"rule": family.direction_rule, "inputs": list(family.direction_inputs)},
        "directional": family.directional,
        "sides": family.sides,
        "account": family.account,
        "instrument_kind": family.instrument_kind,
        "exit_kind": family.exit_kind,
        "costs": {t: {"tier": P.LIQUIDITY.get(t), **P.COST_FLOORS.get(P.LIQUIDITY.get(t, ""), {})}
                  for t in instruments},
        "trials": [trial],
        "k": 1,
        "alpha_look": alpha_look,
        "alpha_trial": alpha_look / 1,
        "n_min": P.N_MIN,
        "n_side_min": P.N_SIDE_MIN,
        "max_fires_per_session": family.max_fires_per_session,
        "pass_test": "policy.pass_test",
    }
