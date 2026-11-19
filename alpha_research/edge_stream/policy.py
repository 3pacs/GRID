"""Frozen policy of the edge stream (``edge-stream-policy/2``).

Everything a look is judged by lives here and is written, whole, into the
ledger's ``edge_policy`` record at genesis (its sha256 is pinned in every
registration). Changing a value means a new policy version recorded as an
amendment that may only tighten: a lower alpha, fewer looks a year, more
prior consumption, more groups or instruments. Nothing here is read from the
environment.

Version 2 (2026-11-13) adds, and loosens nothing: the ``sp500_events`` group and its
``large_cap`` cost tier for the EC1 earnings family, the carry charged on the short leg
of a multi-day trade, the SPY hedge leg, and a consumption entry that keeps the stream
off the years the D0 research contract reserves for its own earnings test.
:func:`tightening_violations` is the rule an amendment record is checked by.

Owner decisions behind the numbers (Anik, 2026-11-13, roast of the design):

* budget: q 0.05 per calendar year shared by at most 40 stage-1 registrations
  (0.00125 per look, Bonferroni over the look's trials); abandoned
  registrations count; a stage-1 PASS must confirm once on fresh forward data
  at 0.05/k before it is reported as a candidate;
* fresh data: consumption is keyed per (group, horizon class, period);
* execution: the Schwab account can short shares (easy to borrow); the
  Robinhood agentic account is long options / long shares only.
"""

from __future__ import annotations

import hashlib
import json

from .universe import SP500_20251231

POLICY_VERSION = "edge-stream-policy/2"
LEDGER_ID = "grid-edge-looks"

# --- budget -----------------------------------------------------------------------
Q_YEAR = 0.05
MAX_REGISTRATIONS_PER_YEAR = 40
ALPHA_PER_LOOK = Q_YEAR / MAX_REGISTRATIONS_PER_YEAR  # 0.00125
CONFIRM_ALPHA = 0.05  # per confirmation look, Bonferroni over its trials
MAX_TRIALS_PER_LOOK = 8
MAX_NEW_REGISTRATIONS_PER_DAY = 1

# --- pass test --------------------------------------------------------------------
N_MIN = 100
N_SIDE_MIN = 50
BOOT_SEED = 20261107
BOOT_B_MIN = 10_000
BOOT_B_MAX = 2_000_000
BOOT_TAIL_RESAMPLES = 100  # B >= this / tail probability
THIRDS_REQUIRED = 2  # mean net > 0 in at least 2 of 3 equal time thirds

# --- looks ------------------------------------------------------------------------
FETCH_ATTEMPTS_BEFORE_ABANDON = 5
FETCH_DAYS_BEFORE_ABANDON = 14
FORWARD_FAILURE_DAYS_BEFORE_ABANDON = 10  # distinct failure days once a forward look is due
FORWARD_N_TARGET_SESSIONS = 100
FORWARD_MAX_SESSIONS = 250

HORIZONS = ("intraday", "overnight", "multiday")
CLOCK_INPUTS = frozenset({"time_of_day", "day_of_week", "calendar", "minute", "session_index"})

# --- instruments ------------------------------------------------------------------
#: Correlated instruments share one group: consumption is per group, never per ticker.
GROUPS: dict[str, tuple[str, ...]] = {
    "us_equity_index": (
        "SPY", "QQQ", "IWM", "DIA", "RSP", "XSP", "SPX", "ES", "NQ",
        "XLE", "XLU", "XLV", "XLB", "XLI", "XLF", "XLK", "XLP", "XLY", "XLC", "XLRE",
    ),
    "rates": ("TLT", "IEF", "SHY", "ZN"),
    "credit": ("HYG", "LQD"),
    "vol": ("VIXY", "VXX", "VIX"),
    "metals": ("GLD", "SLV"),
    "energy": ("USO", "UNG"),
    "intl_equity": ("EFA", "EEM", "EWJ", "FXI", "EWZ"),
    "fx": ("FXE", "FXY", "UUP"),
    "crypto_etf": ("IBIT", "ETHA"),
    "us_single_stocks": ("AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA"),
    "commodity_broad": ("DBC",),
    # single stocks traded around their own earnings releases (EC1); disjoint from every name above
    "sp500_events": SP500_20251231,
}

#: Instruments a look may trade, with their cost tier (G10). Polygon bars carry no
#: bid/ask, so costs are a floor per side: max(usd, bp x price).
LIQUIDITY: dict[str, str] = {
    **{t: "liquid" for t in ("SPY", "QQQ", "IWM", "GLD", "SLV", "USO", "EFA", "EEM", "IBIT",
                             "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA",
                             "TLT", "HYG", "XLE", "XLF", "XLK")},
    **{t: "thin" for t in ("UNG", "EWJ", "FXI", "EWZ", "ETHA", "FXE", "FXY", "UUP", "DIA", "RSP")},
    **{t: "large_cap" for t in SP500_20251231},
}
COST_FLOORS: dict[str, dict[str, float]] = {
    "liquid": {"usd": 0.02, "bp": 1.5},
    "thin": {"usd": 0.03, "bp": 3.0},
    "large_cap": {"usd": 0.02, "bp": 3.0},  # an S&P 500 share at the opening / closing auction
}
#: Multi-day trades. One leg of a hedged trade is always short: it pays borrow and the
#: dividends that fall in the hold, charged at this annual rate on calendar days held
#: (the long leg's dividends are not credited). Daily bars are split-adjusted prices.
SHORT_CARRY_ANNUAL = 0.02
#: The only hedge leg a multi-day single-stock trade may carry, at equal notional. It is
#: costed at its own floor and is not the group under test (disclosed per registration).
HEDGE_INSTRUMENT = "SPY"
SHARE_FEES_PER_SIDE_USD = 0.0  # Schwab / Robinhood commission-free shares
OPTION_FEE_PER_CONTRACT_USD = 0.65

#: What each account can do (G5). A rule is judged only at an exit its account can take.
ACCOUNTS: dict[str, dict] = {
    "schwab": {
        "shares_long": True,
        "shares_short": True,
        "options_long": True,
        "spreads": True,
        "hold_to_expiry": True,
        "exits": ["rth_bar_close", "close_auction", "settlement"],
    },
    "robinhood_agentic": {
        "shares_long": True,
        "shares_short": False,
        "options_long": True,
        "spreads": False,
        "hold_to_expiry": False,  # SPY expiry-day closeout at 15:45 ET
        "exits": ["rth_bar_close"],
        "latest_exit_et_expiry_day": "15:45",
    },
}

# --- what earlier searches already used (never re-used) -----------------------------
PRIOR_CONSUMPTION: tuple[dict, ...] = (
    {"group": "us_equity_index", "horizon": "intraday", "start": "2018-05-01", "end": "2026-11-13",
     "by": "edge9 holdout+dev, edges 2/4/5/6/7/8, climax replay, panel and level trials, fly/bounce shadows"},
    {"group": "us_equity_index", "horizon": "overnight", "start": "2022-11-14", "end": "2026-11-13",
     "by": "edge2 N1 overnight shares"},
    {"group": "us_equity_index", "horizon": "multiday", "start": "1926-07-01", "end": "2026-11-13",
     "by": "edge3 SPY daily 1993-2026, high-Sharpe v1-v6 (French factors 1926-2026)"},
    {"group": "rates", "horizon": "intraday", "start": "2018-05-01", "end": "2022-11-12", "by": "edge9 dials (TLT)"},
    {"group": "rates", "horizon": "intraday", "start": "2024-11-11", "end": "2026-11-10", "by": "edge9 dev map (TLT)"},
    {"group": "credit", "horizon": "intraday", "start": "2018-05-01", "end": "2022-11-12", "by": "edge9 dials (HYG)"},
    {"group": "credit", "horizon": "intraday", "start": "2024-11-11", "end": "2026-11-10", "by": "edge9 dev map (HYG)"},
    {"group": "vol", "horizon": "intraday", "start": "2018-05-01", "end": "2022-11-12", "by": "edge9 dials (VIXY)"},
    {"group": "vol", "horizon": "intraday", "start": "2024-11-11", "end": "2026-11-10", "by": "edge9 dev map (VIXY)"},
    {"group": "vol", "horizon": "multiday", "start": "1990-01-01", "end": "2026-11-13", "by": "high-Sharpe v3/v6 VIX inputs"},
    *(
        {"group": g, "horizon": "multiday", "start": "2014-01-01", "end": "2026-11-01",
         "by": "high-Sharpe ensemble v1-v6 universe (weekly/daily closes)"}
        for g in ("rates", "credit", "metals", "energy", "intl_equity", "us_single_stocks", "commodity_broad")
    ),
    {"group": "rates", "horizon": "multiday", "start": "1926-07-01", "end": "2026-11-13",
     "by": "high-Sharpe v6 long sample (LTGOVTBD/GS10 bond index)"},
    {"group": "energy", "horizon": "multiday", "start": "1946-01-01", "end": "2026-11-13",
     "by": "high-Sharpe v6 long sample (WTISPLC)"},
    {"group": "sp500_events", "horizon": "multiday", "start": "2012-01-01", "end": "2026-01-01",
     "by": "reserved, not used: D0 research contract D1 (8-K Item 2.02 events, dev 2012-18, holdout 2019-25; "
           "GRID PR #1142) keeps these years for its own registered test"},
)

#: Looks run before the stream existed: disclosed, not re-controlled by its budget.
PRIOR_LOOKS_DISCLOSED: tuple[str, ...] = (
    "DAYSIDE (2026-11-06) FAIL", "range-offsides (2026-11-06) FAIL", "climax holdout (2026-11-06) FAIL",
    "edge2 R1-R4 (2026-11-11) FAIL", "edge3 calendar rules (2026-11-11) FAIL",
    "edge4 pin-range/gamma-break/em-exhaustion (2026-11-12) FAIL", "edge5 (2026-11-12)",
    "edge6 short iron fly 12:00/14:00/15:00 PASS held to the close; 15:45 exit, XSP, 16:00 buy-back FAIL",
    "edge7 P1-P4 (2026-11-12) FAIL", "edge8 V3/F3/F1 (2026-11-12) FAIL", "edge9 T1-T3 (2026-11-12) FAIL",
    "high-Sharpe ensemble v1-v6 (2026-11-08) FAIL, 492 trials",
)


def group_of(instrument: str) -> str | None:
    for group, members in GROUPS.items():
        if instrument in members:
            return group
    return None


def cost_per_side(instrument: str, price: float) -> float:
    """Per-side cost floor as a fraction of price (half-spread + slippage + fees)."""
    tier = COST_FLOORS[LIQUIDITY[instrument]]
    usd = max(tier["usd"], tier["bp"] * 1e-4 * price) + SHARE_FEES_PER_SIDE_USD
    return usd / price


def policy_payload() -> dict:
    return {
        "policy_version": POLICY_VERSION,
        "ledger_id": LEDGER_ID,
        "budget": {
            "rule": "per calendar year (UTC): q_year over at most max_registrations stage-1 registrations; "
                    "each look alpha_per_look, Bonferroni over its trials; abandoned registrations count; "
                    "confirmations (forward, after a stage-1 PASS) at confirm_alpha/k outside the yearly count",
            "q_year": Q_YEAR,
            "max_registrations_per_year": MAX_REGISTRATIONS_PER_YEAR,
            "alpha_per_look": ALPHA_PER_LOOK,
            "confirm_alpha": CONFIRM_ALPHA,
            "max_trials_per_look": MAX_TRIALS_PER_LOOK,
            "max_new_registrations_per_day": MAX_NEW_REGISTRATIONS_PER_DAY,
        },
        "pass_test": {
            "n_min": N_MIN,
            "n_side_min": N_SIDE_MIN,
            "mean_net": "> 0, long mean > 0 and short mean > 0 separately (directional rules)",
            "bound": "session-resampled percentile bootstrap lower bound at 1 - alpha_trial > 0 AND "
                     "session-clustered t lower bound (ratio estimator, S-1 df) at 1 - alpha_trial > 0",
            "boot_seed": BOOT_SEED,
            "boot_b": f"max({BOOT_B_MIN}, ceil({BOOT_TAIL_RESAMPLES}/alpha_trial)) <= {BOOT_B_MAX}",
            "baseline": "mean net above the always-long mean net on the same trades",
            "thirds_required": THIRDS_REQUIRED,
            "underpowered": "n < n_min or a side < n_side_min",
        },
        "looks": {
            "fetch_attempts_before_abandon": FETCH_ATTEMPTS_BEFORE_ABANDON,
            "fetch_days_before_abandon": FETCH_DAYS_BEFORE_ABANDON,
            "forward_failure_days_before_abandon": FORWARD_FAILURE_DAYS_BEFORE_ABANDON,
            "attempts": "failures counted per distinct UTC day; the per-cycle request cap is never counted",
            "forward_n_target_sessions": FORWARD_N_TARGET_SESSIONS,
            "forward_max_sessions": FORWARD_MAX_SESSIONS,
        },
        "consumption_key": "(group, horizon class, period)",
        "horizons": list(HORIZONS),
        "clock_inputs": sorted(CLOCK_INPUTS),
        "groups": {k: list(v) for k, v in GROUPS.items()},
        "liquidity": dict(sorted(LIQUIDITY.items())),
        "cost_floors": COST_FLOORS,
        "share_fees_per_side_usd": SHARE_FEES_PER_SIDE_USD,
        "multiday": {
            "short_carry_annual": SHORT_CARRY_ANNUAL,
            "short_carry": "charged on calendar days held, on the leg that is short (borrow + dividends paid); "
                           "dividends on the long leg are not credited",
            "hedge_instrument": HEDGE_INSTRUMENT,
            "hedge": "equal-notional, opposite side, costed at its own floor; not the group under test",
            "resampling_unit": "ISO week of entry (holds overlap, so trades are resampled by week, not by day)",
        },
        "option_fee_per_contract_usd": OPTION_FEE_PER_CONTRACT_USD,
        "accounts": ACCOUNTS,
        "prior_consumption": [dict(c) for c in PRIOR_CONSUMPTION],
        "prior_looks_disclosed": list(PRIOR_LOOKS_DISCLOSED),
        "promotion": "never: a confirmed PASS is a candidate report; trading needs Anik's explicit go",
        "orders": "the stream never places, reviews or cancels an order",
    }


def tightening_violations(old: dict, new: dict) -> list[str]:
    """Why ``new`` may not replace ``old`` (empty = allowed).

    An amendment may lower the budget numbers, raise the sample-size and cost floors,
    and add groups, instruments, cost tiers, horizons, clock inputs, prior consumption
    and new top-level sections. Anything else that differs is refused.
    """
    bad: list[str] = []
    if new.get("policy_version") == old.get("policy_version"):
        bad.append("policy_version unchanged")
    if new.get("ledger_id") != old.get("ledger_id"):
        bad.append("ledger_id changed")
    ob, nb = old["budget"], new.get("budget", {})
    for k in ("q_year", "max_registrations_per_year", "alpha_per_look", "confirm_alpha", "max_trials_per_look",
              "max_new_registrations_per_day"):
        if not (k in nb and nb[k] <= ob[k]):
            bad.append(f"budget.{k} raised or missing")
    if nb.get("rule") != ob.get("rule"):
        bad.append("budget.rule changed")
    op, np_ = old["pass_test"], new.get("pass_test", {})
    for k in ("n_min", "n_side_min"):
        if not (k in np_ and np_[k] >= op[k]):
            bad.append(f"pass_test.{k} lowered or missing")
    for k in sorted(set(op) - {"n_min", "n_side_min"}):
        if np_.get(k) != op[k]:
            bad.append(f"pass_test.{k} changed")
    for k in ("looks", "consumption_key", "accounts", "share_fees_per_side_usd", "option_fee_per_contract_usd",
              "promotion", "orders"):
        if new.get(k) != old[k]:
            bad.append(f"{k} changed")
    for k in ("horizons", "clock_inputs"):
        if not set(old[k]) <= set(new.get(k, [])):
            bad.append(f"{k} lost an entry")
    for g, members in old["groups"].items():
        if not set(members) <= set(new.get("groups", {}).get(g, [])):
            bad.append(f"group {g} lost an instrument")
    seen: dict[str, str] = {}
    for g, members in new.get("groups", {}).items():
        for t in members:
            if seen.setdefault(t, g) != g:
                bad.append(f"{t} is in groups {seen[t]} and {g}")
    for t, tier in old["liquidity"].items():
        if new.get("liquidity", {}).get(t) != tier:
            bad.append(f"liquidity of {t} changed")
    for tier, floor in old["cost_floors"].items():
        nf = new.get("cost_floors", {}).get(tier)
        if nf is None or any(nf.get(k, -1) < v for k, v in floor.items()):
            bad.append(f"cost floor {tier} lowered or removed")
    if any(tier not in new.get("cost_floors", {}) for tier in new.get("liquidity", {}).values()):
        bad.append("an instrument names a cost tier with no floor")
    have = {canonical_bytes(c) for c in new.get("prior_consumption", [])}
    if any(canonical_bytes(c) not in have for c in old["prior_consumption"]):
        bad.append("prior consumption entry removed or edited")
    if not set(old["prior_looks_disclosed"]) <= set(new.get("prior_looks_disclosed", [])):
        bad.append("a disclosed prior look was removed")
    known = {"policy_version", "ledger_id", "budget", "pass_test", "looks", "consumption_key", "horizons",
             "clock_inputs", "groups", "liquidity", "cost_floors", "share_fees_per_side_usd",
             "option_fee_per_contract_usd", "accounts", "prior_consumption", "prior_looks_disclosed", "promotion",
             "orders"}
    for k in sorted(set(old) - known):
        if new.get(k) != old[k]:
            bad.append(f"{k} changed")
    return bad


def canonical_bytes(payload) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def policy_sha256() -> str:
    return hashlib.sha256(canonical_bytes(policy_payload())).hexdigest()
