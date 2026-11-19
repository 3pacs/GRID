"""Frozen policy of the edge stream (``edge-stream-policy/1``).

Everything a look is judged by lives here and is written, whole, into the
ledger's ``edge_policy`` record at genesis (its sha256 is pinned in every
registration). Changing a value means a new policy version recorded as an
amendment that may only tighten: a lower alpha, fewer looks a year, more
prior consumption, more groups or instruments. Nothing here is read from the
environment.

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

POLICY_VERSION = "edge-stream-policy/1"
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
}

#: Instruments a look may trade, with their cost tier (G10). Polygon bars carry no
#: bid/ask, so costs are a floor per side: max(usd, bp x price).
LIQUIDITY: dict[str, str] = {
    **{t: "liquid" for t in ("SPY", "QQQ", "IWM", "GLD", "SLV", "USO", "EFA", "EEM", "IBIT",
                             "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA",
                             "TLT", "HYG", "XLE", "XLF", "XLK")},
    **{t: "thin" for t in ("UNG", "EWJ", "FXI", "EWZ", "ETHA", "FXE", "FXY", "UUP", "DIA", "RSP")},
}
COST_FLOORS: dict[str, dict[str, float]] = {
    "liquid": {"usd": 0.02, "bp": 1.5},
    "thin": {"usd": 0.03, "bp": 3.0},
}
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
        "option_fee_per_contract_usd": OPTION_FEE_PER_CONTRACT_USD,
        "accounts": ACCOUNTS,
        "prior_consumption": [dict(c) for c in PRIOR_CONSUMPTION],
        "prior_looks_disclosed": list(PRIOR_LOOKS_DISCLOSED),
        "promotion": "never: a confirmed PASS is a candidate report; trading needs Anik's explicit go",
        "orders": "the stream never places, reviews or cancels an order",
    }


def canonical_bytes(payload) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def policy_sha256() -> str:
    return hashlib.sha256(canonical_bytes(policy_payload())).hexdigest()
