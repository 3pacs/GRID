"""Frozen policy of the edge stream (``edge-stream-policy/5``).

Everything a look is judged by lives here and is written, whole, into the
ledger's ``edge_policy`` record at genesis (its sha256 is pinned in every
registration). Changing a value means a new policy version recorded as an
amendment that may only tighten: a lower alpha, fewer looks a year, more
prior consumption, more groups or instruments. The one exception is a recorded
owner decision (:data:`OWNER_DECISIONS`), which may change the few fields in
:data:`OWNER_WAIVABLE` and nothing else. Nothing here is read from the
environment.

Version 5 (2026-11-22) adds one owner decision that names one registration sealed before
read records existed: the single-stocks history registration abandoned on 2026-11-20. A
receipt reconciled from the ledger, cycle.log, the release that ran the cycle, the data
dir and every edge-stream bar cache on grid-svr (sha256 pinned below) shows no read of its
window except four sessions (2024-12-19..24) whose move sizes rehearsal copies read; those
stay consumed and the rest of its window is freed.

Version 4 (2026-11-21) adds one owner decision (:data:`UNREAD_ABANDONMENT`): a history
registration sealed under it, abandoned because its family's code changed, whose ledger
holds no read of its window, has that window freed, and its (family, group, lane) may
register again on a new window. A forward registration whose presence was counted
was read and stays closed. Its alpha stays spent and it still counts toward the yearly maximum. Every
other abandonment, and every registration sealed before it, keeps its window consumed.

Version 3 (2026-11-16) carries the owner's two-stage evidence bar (GRID issue 1213):

* stage 1 is **discovery** at :data:`DISCOVERY_ALPHA` a look. The single 0.00125 history
  bar is retired: on a two-year window it needed an annualised Sharpe near 2.6;
* a discovery is refitted with its largest calendar year left out, and one whose effect
  disappears is recorded ``EPISODE``, not ``PASS`` (a look with too few trades outside
  that year cannot be refitted: that is recorded, and its confirmation is its second period);
* stage 2 is **one confirmation look** at 0.05 one-sided on data the search never read;
  a miss closes that family in that group, with no rescue cuts;
* noise is measured per session or coarser, in the unit the registration declared;
* a confirmed rule stays a paper result until its cost is measured against real fills.

The yearly budget is unchanged and now covers both stages:
``max_registrations x discovery alpha x confirmation alpha <= q_year``.

Version 2 (2026-11-13) adds, and loosens nothing: the ``sp500_events`` group and its
``large_cap`` cost tier for the EC1 earnings family, the carry charged on the short leg
of a multi-day trade, the SPY hedge leg, and a consumption entry that keeps the stream
off the years the D0 research contract reserves for its own earnings test.
:func:`amendment_violations` is the rule an amendment record is checked by.

Version 4 decision: https://github.com/3pacs/GRID/issues/1336#issuecomment-7073862603 (posted 2026-11-22).

Owner decisions behind the numbers (Anik, 2026-11-13, roast of the design):

* budget: q 0.05 per calendar year shared by at most 40 stage-1 registrations
  (Bonferroni over the look's trials); abandoned registrations count; a stage-1
  PASS must confirm once on fresh forward data at 0.05/k before it is reported
  as a candidate (versions 1 and 2 judged stage 1 at 0.00125 a look);
* fresh data: consumption is keyed per (group, horizon class, period);
* execution: the Schwab account can short shares (easy to borrow); the
  Robinhood agentic account is long options / long shares only.
"""

from __future__ import annotations

import hashlib
import json
import re

from .universe import SP500_20251231

POLICY_VERSION = "edge-stream-policy/5"
LEDGER_ID = "grid-edge-looks"

# --- budget -----------------------------------------------------------------------
Q_YEAR = 0.05
MAX_REGISTRATIONS_PER_YEAR = 40
CONFIRM_ALPHA = 0.05  # the one confirmation look, one-sided, Bonferroni over its trials
#: Stage 1 (discovery). The level that keeps a year's expected false *confirmed* candidates
#: at q_year when every registration is a false discovery: 40 x 0.025 x 0.05 = 0.05.
DISCOVERY_ALPHA = Q_YEAR / (MAX_REGISTRATIONS_PER_YEAR * CONFIRM_ALPHA)  # 0.025
ALPHA_PER_LOOK = DISCOVERY_ALPHA
EVIDENCE_BAR = "two-stage/1"
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
#: The largest-year-out refit needs this many trades outside the largest year to mean anything.
YEAR_OUT_N_MIN = 50
#: A trade's ``session`` is the unit noise is measured in: a day or an ISO week, never finer.
NOISE_UNITS = {"session": r"\d{4}-\d{2}-\d{2}", "iso_week": r"\d{4}-W\d{2}"}

# --- owner decisions --------------------------------------------------------------
#: Decisions of the repository owner that change the policy against the tightening rule.
#: ``waives`` names the fields the decision changes; only :data:`OWNER_WAIVABLE` fields can
#: be named. ``text_sha256`` is the sha256 of the decision as posted (LF line ends, one
#: trailing newline). Entries are never edited or removed.
OWNER_WAIVABLE = frozenset({"budget.alpha_per_look", "budget.rule", "promotion", "evidence_bar",
                            "unread_abandonment"})
OWNER_DECISIONS: tuple[dict, ...] = (
    {
        "id": "GRID#1213",
        "url": "https://github.com/3pacs/GRID/issues/1213#issuecomment-6932309442",
        "decided_by": "Anik (repository owner)",
        "decided_on": "2026-11-16",
        "text_sha256": "a4284cd50bca8cfb0f01015c1b2a4d4a88db7240d0c192859f3a55f0868228f4",
        "summary": "adopt the two-stage evidence bar: discovery, then one confirmation look at 5 % one-sided "
                   "on unread data; noise per day; largest year left out; cost against real fills before acting; "
                   "the single 0.00125 history bar is retired for new work",
        "waives": ["budget.alpha_per_look", "budget.rule", "promotion"],
    },
    {
        "id": "GRID#1336",
        "url": "https://github.com/3pacs/GRID/issues/1336#issuecomment-7073862603",
        "decided_by": "Anik (repository owner)",
        "decided_on": "2026-11-21",
        "text_sha256": "491fa4b3b9f645566e0b1e4fc9c683fb791aaf8986e57a13acbb4cca07f905a3",
        "summary": "a registration abandoned because its family's code changed before its look had none of its "
                   "window read: the window stops counting as consumed and the (family, group, lane) may register "
                   "again on a new window; its alpha stays spent and it counts toward the yearly maximum",
        "waives": [],
    },
    {
        "id": "GRID#1379",
        "url": "https://github.com/3pacs/GRID/issues/1379#issuecomment-7085985405",
        "decided_by": "Anik (repository owner)",
        "decided_on": "2026-11-22",
        "text_sha256": "603ac1939d464ce6962a08e09b5df2806ee75d084ec0c4880770aff531642715",
        "summary": "the 2026-11-20 abandonment of IM1-v1:us_single_stocks:history:2024-12-19 is attested unread by "
                   "receipt 5e002ed6 except 2024-12-19..24, which stay consumed; the rest of its window is freed and "
                   "single stocks may register again; its alpha stays spent",
        "waives": ["unread_abandonment"],
    },
)

#: The rule the second owner decision adds (a new top-level policy section). The ledger's
#: consumption and the runner's queue read it from the policy *in force*, so it applies only
#: to records appended after the amendment that carries it.
UNREAD_ABANDONMENT: dict = {
    "decision": OWNER_DECISIONS[1]["id"],
    "cause": "code_changed",
    "rule": "unread status is a positive receipt in the ledger, never inferred. Before the first read of each "
            "kind of an open registration's window (progress: a forward presence count; look: a look) the runner "
            "appends an edge_read record to this hash-chained, anchored ledger; a dry run reads no open window. "
            "A registration on either lane sealed under this policy or a later one, with no edge_read record at "
            "all, abandoned at the code-hash check with no failed attempt and no staged report in the data dir, "
            "has its window freed and its (family, group, lane) may register again on a new window. A forward "
            "registration whose presence was counted was read: its window stays consumed and its (family, group, "
            "lane) stays closed. Any registration sealed before this policy, and any whose evidence is missing, "
            "partial or disputed, keeps its window and its (family, group, lane) closed, except a history "
            "registration an owner decision names below with a reconciled receipt (its sha256 pinned; its bytes, "
            "shipped with this code, must agree with this ledger on every load): its window up to "
            "consumed_through_exclusive stays consumed and the rest is freed, and only while the ledger still "
            "holds no look and no edge_read of it and its abandonment shows no read. The alpha of an abandoned "
            "registration stays spent and it counts toward max_registrations",
    "attested": [
        {
            "registration_sha256": "cfa2537678d7af7153e767810fda3d87a9316c45687d9cc2747e54a1fd0b224b",
            "lane": "history",
            "frees_window": True,
            "proposal_id": "IM1-v1:us_single_stocks:history:2024-12-19",
            "decision": "GRID#1379",
            "receipt": "unread-cfa25376-v2-20261122.json",
            "receipt_sha256": "5e002ed65f177e6a39a7b5a07d7bb1cb8b102c300b75d69564a95753a555ff0a",
            "receipt_verdict": "UNREAD_EXCEPT",
            "consumed_through_exclusive": "2024-12-25",
            "evidence": "receipt edge-unread-receipt/2 built read-only on grid-svr 2026-11-22 against the 14-record "
                        "ledger (head line 69a84820): the ledger holds only its abandonment (seq 11, no look, no "
                        "edge_read); cycle.log holds one cycle between its registration and abandonment, not a dry "
                        "run, with no look line for it; release 1c94dae9 rebuilds its registered code hash 799ed2d5 "
                        "and release 15975857 rebuilds a9f28c3a, the hash the forward registration 69a84820 sealed "
                        "in the abandoning cycle, and runs the code-hash check before any read; no staged report or "
                        "failed attempt; every SQLite file changed since its registration on the host's disk "
                        "filesystems was opened read-only: of ten bar caches, the live one holds nothing in its "
                        "window and only three rehearsal copies (2026-11-22) hold sessions in it, 2024-12-19..24. "
                        "Those four sessions stay consumed",
        },
    ],
}

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
            "rule": "two-stage evidence bar (owner decision GRID#1213). Per calendar year (UTC) at most "
                    "max_registrations stage-1 registrations, abandoned ones included. Stage 1 is discovery: "
                    "each look at alpha_per_look, Bonferroni over its registered trials. Stage 2 is one "
                    "confirmation look at confirm_alpha/k, outside the yearly count. The year's budget covers "
                    "both stages: max_registrations x alpha_per_look x confirm_alpha <= q_year",
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
            "largest_year_out": "stage 1 only: the calendar year with the largest summed net is left out; when the "
                                "mean net of the remaining trades is <= 0 the verdict is EPISODE and nothing is "
                                "confirmed. With fewer than year_out_n_min trades outside that year (one-year "
                                "windows included) the refit is recorded as not possible and the verdict stands",
            "year_out_n_min": YEAR_OUT_N_MIN,
            "noise_unit": "the session field of every trade, a day (YYYY-MM-DD) or an ISO week (YYYY-Www) as the "
                          "registration declared before its data was read; trades sharing a unit are resampled "
                          "together and never counted as independent",
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
        "promotion": "never: a confirmed PASS is a candidate report and a paper result until its cost has been "
                     "measured against real fills (GRID#1214); trading needs Anik's explicit go",
        "evidence_bar": {
            "version": EVIDENCE_BAR,
            "decision": "GRID#1213",
            "discovery": "stage 1, on history or forward data: the pass test at alpha_per_look, Bonferroni over the "
                         "look's registered trials. The trials are fixed before the data is read, so Bonferroni "
                         "bounds the best-of-shuffled bar from above; a family that searches cells before it "
                         "chooses one must bring that bar from its own discovery",
            "largest_year_out": "every discovery is refitted without its largest calendar year (pass_test)",
            "confirmation": "stage 2: one look at confirm_alpha one-sided, Bonferroni over its trials, on data the "
                            "search never read; registered before the data exists or is fetched; run once. A "
                            "confirmation look that does not PASS closes the family in that group: no later "
                            "registration of it there, on any lane. An abandoned confirmation has no look: its "
                            "window stays consumed and its discovery stays unconfirmed",
            "before_acting": "the cost of a confirmed rule is measured against real fills first (GRID#1214)",
            "retired": "the single 0.00125 stage-1 bar of versions 1 and 2, for registrations sealed from this "
                       "version on; earlier registrations keep the terms they were sealed with",
        },
        "owner_decisions": [dict(d) for d in OWNER_DECISIONS],
        "unread_abandonment": dict(UNREAD_ABANDONMENT),
        "orders": "the stream never places, reviews or cancels an order",
    }


def _violations(old: dict, new: dict) -> list[tuple[str, str]]:
    """(field, message) for everything in ``new`` that does not tighten ``old``."""
    bad: list[tuple[str, str]] = []

    def add(field: str, message: str) -> None:
        bad.append((field, message))

    if new.get("policy_version") == old.get("policy_version"):
        add("policy_version", "policy_version unchanged")
    if new.get("ledger_id") != old.get("ledger_id"):
        add("ledger_id", "ledger_id changed")
    ob, nb = old["budget"], new.get("budget", {})
    for k in ("q_year", "max_registrations_per_year", "alpha_per_look", "confirm_alpha", "max_trials_per_look",
              "max_new_registrations_per_day"):
        if not (k in nb and nb[k] <= ob[k]):
            add(f"budget.{k}", f"budget.{k} raised or missing")
    if nb.get("rule") != ob.get("rule"):
        add("budget.rule", "budget.rule changed")
    op, np_ = old["pass_test"], new.get("pass_test", {})
    for k in ("n_min", "n_side_min"):
        if not (k in np_ and np_[k] >= op[k]):
            add(f"pass_test.{k}", f"pass_test.{k} lowered or missing")
    for k in sorted(set(op) - {"n_min", "n_side_min"}):
        if np_.get(k) != op[k]:
            add(f"pass_test.{k}", f"pass_test.{k} changed")
    for k in ("looks", "consumption_key", "accounts", "share_fees_per_side_usd", "option_fee_per_contract_usd",
              "promotion", "orders"):
        if new.get(k) != old[k]:
            add(k, f"{k} changed")
    for k in ("horizons", "clock_inputs"):
        if not set(old[k]) <= set(new.get(k, [])):
            add(k, f"{k} lost an entry")
    for g, members in old["groups"].items():
        if not set(members) <= set(new.get("groups", {}).get(g, [])):
            add("groups", f"group {g} lost an instrument")
    seen: dict[str, str] = {}
    for g, members in new.get("groups", {}).items():
        for t in members:
            if seen.setdefault(t, g) != g:
                add("groups", f"{t} is in groups {seen[t]} and {g}")
    for t, tier in old["liquidity"].items():
        if new.get("liquidity", {}).get(t) != tier:
            add("liquidity", f"liquidity of {t} changed")
    for tier, floor in old["cost_floors"].items():
        nf = new.get("cost_floors", {}).get(tier)
        if nf is None or any(nf.get(k, -1) < v for k, v in floor.items()):
            add("cost_floors", f"cost floor {tier} lowered or removed")
    if any(tier not in new.get("cost_floors", {}) for tier in new.get("liquidity", {}).values()):
        add("cost_floors", "an instrument names a cost tier with no floor")
    have = {canonical_bytes(c) for c in new.get("prior_consumption", [])}
    if any(canonical_bytes(c) not in have for c in old["prior_consumption"]):
        add("prior_consumption", "prior consumption entry removed or edited")
    if not set(old["prior_looks_disclosed"]) <= set(new.get("prior_looks_disclosed", [])):
        add("prior_looks_disclosed", "a disclosed prior look was removed")
    kept = old.get("owner_decisions", [])
    if new.get("owner_decisions", [])[:len(kept)] != kept:
        add("owner_decisions", "an owner decision was removed or edited")
    known = {"policy_version", "ledger_id", "budget", "pass_test", "looks", "consumption_key", "horizons",
             "clock_inputs", "groups", "liquidity", "cost_floors", "share_fees_per_side_usd",
             "option_fee_per_contract_usd", "accounts", "prior_consumption", "prior_looks_disclosed", "promotion",
             "orders", "owner_decisions"}
    for k in sorted(set(old) - known):
        if new.get(k) != old[k]:
            add(k, f"{k} changed")
    return bad


def tightening_violations(old: dict, new: dict) -> list[str]:
    """Why ``new`` does not only tighten ``old`` (empty = it only tightens).

    An amendment may lower the budget numbers, raise the sample-size and cost floors,
    and add groups, instruments, cost tiers, horizons, clock inputs, prior consumption,
    pass-test conditions, owner decisions and new top-level sections. Anything else
    that differs is named here.
    """
    return [message for _, message in _violations(old, new)]


def amendment_violations(old: dict, new: dict) -> list[str]:
    """Why ``new`` may not replace ``old`` (empty = allowed): the rule an amendment record is checked by.

    The tightening rule, except for the fields a *new* owner decision in ``new`` waives.
    A decision must be complete, may waive only :data:`OWNER_WAIVABLE` fields, and never
    lifts the yearly budget: ``max_registrations x alpha_per_look x confirm_alpha`` must
    stay within ``q_year`` whatever is waived.
    """
    bad: list[str] = []
    waived: set[str] = set()
    for d in new.get("owner_decisions", [])[len(old.get("owner_decisions", [])):]:
        name = d.get("id") or "?"
        if not all(isinstance(d.get(k), str) and d[k] for k in ("id", "url", "decided_by", "decided_on", "summary")) \
                or not re.fullmatch(r"https://github\.com/3pacs/GRID/issues/\d+#issuecomment-\d+", str(d.get("url"))) \
                or not re.fullmatch(r"[0-9a-f]{64}", str(d.get("text_sha256", ""))) \
                or not isinstance(d.get("waives"), list):
            bad.append(f"owner decision {name} is incomplete")
            continue
        outside = sorted(set(d["waives"]) - OWNER_WAIVABLE)
        if outside:
            bad.append(f"owner decision {name} waives what no decision can change: {', '.join(outside)}")
            continue
        waived |= set(d["waives"])
    bad += [message for field, message in _violations(old, new) if field not in waived]
    b = new.get("budget", {})
    try:
        spent = b["max_registrations_per_year"] * b["alpha_per_look"] * b["confirm_alpha"]
        if not spent <= b["q_year"] * (1 + 1e-12):
            bad.append("budget: max_registrations x alpha_per_look x confirm_alpha exceeds q_year")
    except (KeyError, TypeError):
        pass  # a missing number is already named above
    return bad


def canonical_bytes(payload) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def policy_sha256() -> str:
    return hashlib.sha256(canonical_bytes(policy_payload())).hexdigest()
