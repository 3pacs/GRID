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
from datetime import date, datetime, timedelta
from pathlib import Path

from . import policy as P
from .universe import SP500_20251231

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


def iso_week(day: str) -> str:
    y, w, _ = date.fromisoformat(day).isocalendar()
    return f"{y}-W{w:02d}"


def reaction_day(accepted_et: str, sessions: list[str]) -> str | None:
    """The first session whose close comes after the filing was accepted: the same day when
    accepted before 16:00 ET on a session day, else the next session. ``sessions`` ascending."""
    when = datetime.fromisoformat(accepted_et)
    day, before_close = when.date().isoformat(), when.strftime("%H:%M") < "16:00"
    for s in sessions:
        if s > day or (s == day and before_close):
            return s
    return None


@dataclass(frozen=True)
class EarningsContamination:
    """EC1: an earnings reaction the market's own move hid keeps going for three weeks."""

    id: str = "EC1"
    version: int = 1
    kind: str = "event"
    mechanism: str = (
        "Post-earnings-announcement drift is larger when investors are distracted. On a day the whole market "
        "moves hard, a company's own earnings news is under-weighted: its raw return looks small or even has "
        "the wrong sign because the market move sits on top of it. The return measured against the market on "
        "the reaction day carries the news, and the under-reaction is corrected over the following weeks. "
        "The rule trades that market-adjusted reaction only when the market moved the other way."
    )
    citation: str = (
        "Bernard & Thomas (1989), Post-earnings-announcement drift, J. Accounting Research 27 (Suppl.) 1-36; "
        "Hirshleifer, Lim & Teoh (2009), Driven to distraction: extraneous events and underreaction to earnings "
        "news, J. Finance 64(5) 2289-2325; Kottimukkalur (2019), Attention to market information and "
        "underreaction to earnings on market moving days, JFQA 54(6) 2493-2516; Brandt, Kishore, Santa-Clara & "
        "Venkatachalam (2008), Earnings announcements are full of surprises (announcement-return drift)"
    )
    direction_rule: str = (
        "long when the reaction day's stock return minus SPY's return is >= +2 % and SPY's return is <= -0.75 %; "
        "short when it is <= -2 % and SPY's return is >= +0.75 %; no trade otherwise"
    )
    direction_inputs: tuple[str, ...] = ("reaction_day_market_adjusted_return", "reaction_day_market_return")
    horizon: str = "multiday"
    residual_min: float = 0.02
    market_min: float = 0.0075
    hold_sessions: int = 15
    min_gap_days: int = 40          # one event per company per 40 days (a second 2.02 filing is the same news)
    events_lookback_days: int = 45  # filings read before the window, only to apply the 40-day rule
    warmup_days: int = 10           # daily bars read before the window, only for the prior close
    max_gap_move: float = 0.35      # a larger one-day move inside a trade = split basis jump; trade skipped
    account: str = "schwab"
    instrument_kind: str = "shares"
    exit_kind: str = "close_auction"
    sides: str = "both"
    directional: bool = True
    max_fires_per_session: int = 1
    history_start: str = "2026-01-02"        # the years before are reserved (policy prior consumption)
    forward_n_target_sessions: int = 250     # a forward look needs a year of events, not 100 sessions
    decision: str = "reaction-day close"
    entry: str = "next session open"

    @property
    def hedge(self) -> str:
        return P.HEDGE_INSTRUMENT

    def direction(self, state: dict) -> int:
        resid, market = state["reaction_day_market_adjusted_return"], state["reaction_day_market_return"]
        if resid >= self.residual_min and market <= -self.market_min:
            return 1
        if resid <= -self.residual_min and market >= self.market_min:
            return -1
        return 0

    def direction_probe(self) -> tuple[dict, dict]:
        """Two states at the same clock that must give different directions (gate G2)."""
        return ({"reaction_day_market_adjusted_return": 0.04, "reaction_day_market_return": -0.012,
                 "time_of_day": "16:00"},
                {"reaction_day_market_adjusted_return": -0.04, "reaction_day_market_return": 0.012,
                 "time_of_day": "16:00"})

    def spec(self) -> dict:
        return {
            "event": "an 8-K with Item 2.02 (SEC EDGAR submissions API), timed by its acceptance time read as "
                     f"Eastern; one event per company per {self.min_gap_days} days (the earliest)",
            "reaction_day": "the first session whose close is after acceptance (same day if accepted before 16:00 ET)",
            "decision": "at the reaction day's close, from that close and the prior session's close of the stock "
                        f"and {self.hedge}",
            "entry": f"open of the next session: the stock on the rule's side, {self.hedge} on the other side, "
                     "equal notional",
            "exit": f"close of the session {self.hold_sessions} sessions after the reaction day, both legs",
            "skip": "no signal; a missing bar on any session from the prior close to the exit; a one-day move "
                    f"above {self.max_gap_move:.0%} inside the trade (split basis); hold not finished inside the "
                    "window; filings of the company not readable whole",
            "net": "side x ((stock exit/entry - 1) - (hedge exit/entry - 1)) - (four per-side cost floors + "
                   "short carry on calendar days held)",
            "always_long_net": "(stock exit/entry - 1) - (hedge exit/entry - 1) - the same costs",
            "resampling_unit": "ISO week of entry (written in each trade's 'session' field)",
            "warm_up": f"reads {self.warmup_days} days of bars and {self.events_lookback_days} days of filings "
                       "before the window, as inputs only",
        }

    def event_trade(self, instrument: str, event: dict, sessions: list[str], stock: dict[str, tuple],
                    hedge: dict[str, tuple]) -> tuple[dict | None, str]:
        """(trade, "") or (None, reason). ``stock`` / ``hedge``: {day: (open, close)}; ``sessions`` ascending."""
        d0 = reaction_day(event["accepted_et"], sessions)
        if d0 is None:
            return None, "no_reaction_session"
        i = sessions.index(d0)
        if i == 0:
            return None, "no_prior_session"
        if i + self.hold_sessions >= len(sessions):
            return None, "hold_incomplete"
        prior, d1, dx = sessions[i - 1], sessions[i + 1], sessions[i + self.hold_sessions]
        # the decision is handed only the bars up to the reaction day, and checks it
        decided = self.decide({d: b for d, b in stock.items() if d <= d0},
                              {d: b for d, b in hedge.items() if d <= d0}, prior, d0)
        if isinstance(decided, str):
            return None, decided
        side, r_stock, r_market = decided
        path = sessions[i:i + self.hold_sessions + 1]
        if any(d not in stock or d not in hedge for d in path):
            return None, "missing_bars"
        closes = [stock[d][1] for d in path]
        if abs(stock[d1][0] / stock[d0][1] - 1.0) > self.max_gap_move \
                or any(abs(b / a - 1.0) > self.max_gap_move for a, b in zip(closes, closes[1:])):
            return None, "basis_jump"
        entry, exit_px = stock[d1][0], stock[dx][1]
        h_entry, h_exit = hedge[d1][0], hedge[dx][1]
        days_held = (date.fromisoformat(dx) - date.fromisoformat(d1)).days
        cost = (P.cost_per_side(instrument, entry) + P.cost_per_side(instrument, exit_px)
                + P.cost_per_side(self.hedge, h_entry) + P.cost_per_side(self.hedge, h_exit)
                + P.SHORT_CARRY_ANNUAL * days_held / 365.0)
        move = (exit_px / entry - 1.0) - (h_exit / h_entry - 1.0)
        return {
            "session": iso_week(d1), "instrument": instrument, "side": side,
            "accession": event.get("accession"), "accepted_et": event["accepted_et"],
            "reaction_day": d0, "entry_day": d1, "exit_day": dx,
            "r_stock": r_stock, "r_market": r_market, "residual": r_stock - r_market,
            "entry": entry, "exit": exit_px, "hedge_entry": h_entry, "hedge_exit": h_exit,
            "gross": side * move, "cost_rt": cost, "net": side * move - cost, "always_long_net": move - cost,
        }, ""

    def decide(self, seen_stock: dict[str, tuple], seen_hedge: dict[str, tuple], prior: str,
               d0: str) -> tuple[int, float, float] | str:
        """(side, stock return, market return) from closes up to the reaction day, or a skip reason.
        Raises :class:`LookaheadError` if handed any bar after the reaction day."""
        late = sorted(d for d in (*seen_stock, *seen_hedge) if d > d0)
        if late:
            raise LookaheadError(f"bars after the {d0} decision: {late[:3]}")
        if any(d not in seen_stock or d not in seen_hedge for d in (prior, d0)):
            return "missing_bars"
        r_stock = seen_stock[d0][1] / seen_stock[prior][1] - 1.0
        r_market = seen_hedge[d0][1] / seen_hedge[prior][1] - 1.0
        if abs(r_stock) > self.max_gap_move:
            return "basis_jump"
        side = self.direction({"reaction_day_market_adjusted_return": r_stock - r_market,
                               "reaction_day_market_return": r_market})
        return "no_signal" if side == 0 else (side, r_stock, r_market)

    def company_events(self, events: list[dict]) -> list[dict]:
        """One event per company per ``min_gap_days``: the earliest of each cluster. Ascending."""
        kept: list[dict] = []
        for e in sorted(events, key=lambda e: (e["accepted_et"], e.get("accession") or "")):
            if kept and (datetime.fromisoformat(e["accepted_et"]) - datetime.fromisoformat(kept[-1]["accepted_et"])
                         ).days < self.min_gap_days:
                continue
            kept.append(e)
        return kept

    def collect(self, instruments: list[str], start: date, end: date, daily, events) -> dict:
        """Every trade of the window ``[start, end)``: reaction day on or after ``start``, exit before
        ``end``. Returns trades, skip counts by reason, instruments left out, and the inputs read."""
        from_bars = start - timedelta(days=self.warmup_days)
        hedge_bars = daily.daily(self.hedge, from_bars, end, require_complete=True)
        hedge = {b[0]: (b[1], b[2]) for b in hedge_bars}
        sessions = sorted(hedge)
        trades, skipped, left_out, used = [], {}, {}, {"bars": {self.hedge: [list(b) for b in hedge_bars]},
                                                      "events": {}}
        for t in instruments:
            found = events.events(t, start - timedelta(days=self.events_lookback_days), end)
            if found is None:
                left_out[t] = "filings_unreadable"
                continue
            bars = daily.daily(t, from_bars, end)
            used["events"][t] = found
            used["bars"][t] = [list(b) for b in bars]
            stock = {b[0]: (b[1], b[2]) for b in bars}
            for e in self.company_events(found):
                d0 = reaction_day(e["accepted_et"], sessions)
                if d0 is None or d0 < start.isoformat():
                    continue  # before the window (read only for the 40-day rule) or not yet reacted
                trade, why = self.event_trade(t, e, sessions, stock, hedge)
                if trade is None:
                    skipped[why] = skipped.get(why, 0) + 1
                else:
                    trades.append(trade)
        trades.sort(key=lambda x: (x["entry_day"], x["instrument"]))
        return {"trades": trades, "skipped": skipped, "left_out": left_out, "used": used, "sessions": sessions}


FAMILIES: dict[str, object] = {"IM1": IntradayMomentum(), "EC1": EarningsContamination()}


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
    {"family": "EC1", "group": "sp500_events", "instruments": SP500_20251231, "lane": "history"},
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
    event = getattr(family, "kind", "session") == "event"
    if lane == "history":
        start = (today - timedelta(days=HISTORY_START_DAYS_BACK)).isoformat()
        window = {"start": max(start, getattr(family, "history_start", start)), "end": today.isoformat()}
    else:
        window = {"start": next_weekday(today).isoformat(), "end": None,
                  "n_target_sessions": min(P.FORWARD_MAX_SESSIONS,
                                           getattr(family, "forward_n_target_sessions", P.FORWARD_N_TARGET_SESSIONS)),
                  "max_sessions": P.FORWARD_MAX_SESSIONS}
    alpha_look = P.ALPHA_PER_LOOK if stage == 1 else P.CONFIRM_ALPHA
    trial = {"trial_id": "pooled", "instruments": list(instruments),
             "pooling": "all instruments' trades, resampled by ISO week of entry" if event
             else "all instruments' trades, resampled by session"}
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
        **({"kind": "event", "hedge": {"instrument": family.hedge, "tier": P.LIQUIDITY[family.hedge],
                                       **P.COST_FLOORS[P.LIQUIDITY[family.hedge]],
                                       "note": "equal-notional hedge leg and reaction-day market return; "
                                               "not the group under test"},
            "short_carry_annual": P.SHORT_CARRY_ANNUAL} if event else {}),
    }
