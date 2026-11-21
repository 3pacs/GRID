"""Implied volatility and greeks for an archived end-of-day option chain (method ``eod-greeks/1``).

The Databento end-of-day archive (``ingestion/options_eod_databento.py``)
stores raw closing quotes; the source carries no implied volatility, greeks or
underlying price. This module derives them when a chain is read. Nothing here
is stored, so the archive stays raw provider data and a better model later
needs no refetch. Every result names the method that produced it.

Method (fixed here; a change is a new method name):

* **Mid.** A contract counts only with a two-sided closing quote: bid > 0 and
  ask >= bid. Its price is the mid. A no-bid or crossed quote gets no value.
* **Forward, per expiry, from the chain itself.** For strikes where both the
  call and the put have a mid, put-call parity gives
  ``F = K + e^(rT) * (C - P)``. The forward is the median over the (up to)
  :data:`PAIRS_FOR_FORWARD` strikes where ``|C - P|`` is smallest, i.e. nearest
  the money. No outside stock price or dividend estimate is used: the forward
  already carries both. The spread of those candidates is reported so a
  caller can see how well the strikes agree.
* **Implied volatility.** Black's model on that forward, solved with the
  repository's bisection solver (``analysis.factory_options.implied_vols``)
  at the dividend-adjusted spot ``S* = F * e^(-rT)`` with no further yield,
  which is the same price function.
* **Greeks.** The repository's closed forms (``physics.greeks.black_scholes``)
  at ``S*``: delta and gamma are per $1 of ``S*``, vega per one volatility
  point, theta per calendar day.
* **Clocks.** ``quotes_at`` is the session close the quotes describe.
  ``known_at`` is the latest fetch time of the chain's parts, i.e. when GRID
  could first have computed this; it is None when the chain carries no fetch
  log. A point-in-time reader must use ``known_at``.
* **Time.** ``T`` runs from the session's cash close (the minute the quotes
  were taken) to the cash close of the expiry date, in calendar years of 365
  days. A contract expiring at this close has no time left and gets no value.

Known limits, stated on every result:

* Equity and ETF options are American; the model is European. Near the money
  and at short expiries the difference is small. In-the-money contracts
  (``in_the_money`` is set) and anything around a dividend are approximate,
  and the parity forward itself leans low by the puts' early-exercise value.
* The rate is the caller's input and is echoed back; this module does not
  read one.
* A closing mid is a quote, not a trade. Wide markets give noisy values; the
  quoted spread is carried on each contract.
"""

from __future__ import annotations

import math
import statistics
from collections import defaultdict
from datetime import date, datetime, time, timezone
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np

from analysis.factory_options import implied_vols
from ingestion.market_calendar import is_market_open, market_close_time
from physics.greeks import black_scholes as bs

METHOD = "eod-greeks/1"
PAIRS_FOR_FORWARD = 5
YEAR_DAYS = 365.0
# Half a calendar day: next-day expiries count, contracts expiring at this close do not.
MIN_T_YEARS = 0.5 / YEAR_DAYS
ET = ZoneInfo("America/New_York")
LIMITS: tuple[str, ...] = (
    "European model on American options: in-the-money contracts and dates around a dividend are approximate",
    "forward from put-call parity at the strikes nearest the money; it leans low by the puts' early-exercise value",
    "closing mid of a two-sided quote, not a trade; the rate is the caller's input",
)


def _close_utc(day: date) -> datetime:
    close = market_close_time(day) if is_market_open(day) else time(16, 0)
    return datetime(day.year, day.month, day.day, close.hour, close.minute, tzinfo=ET).astimezone(timezone.utc)


def _mid(contract: dict[str, Any]) -> float | None:
    bid, ask = contract.get("bid"), contract.get("ask")
    if not isinstance(bid, (int, float)) or not isinstance(ask, (int, float)) or isinstance(bid, bool) or isinstance(ask, bool):
        return None
    if not (math.isfinite(bid) and math.isfinite(ask)) or bid <= 0 or ask < bid:
        return None
    return 0.5 * (bid + ask)


def implied_forward(pairs: list[tuple[float, float, float]], t_years: float, rate: float) -> dict[str, Any] | None:
    """Forward from (strike, call mid, put mid) triples by put-call parity, or None without a pair.

    Uses the :data:`PAIRS_FOR_FORWARD` strikes with the smallest ``|C - P|``; returns the median
    candidate, how many strikes were used and the range of their candidates.
    """
    usable = sorted(pairs, key=lambda p: (abs(p[1] - p[2]), p[0]))[:PAIRS_FOR_FORWARD]
    growth = math.exp(rate * t_years)
    candidates = [k + growth * (c - p) for k, c, p in usable]
    candidates = [f for f in candidates if math.isfinite(f) and f > 0]
    if not candidates:
        return None
    return {"forward": float(statistics.median(candidates)), "pairs": len(candidates),
            "forward_range": float(max(candidates) - min(candidates))}


def _none_if_nan(value: float) -> float | None:
    return float(value) if math.isfinite(value) else None


def chain_greeks(chain: dict[str, Any], rate: float) -> dict[str, Any]:
    """Add mid, implied volatility and greeks to a chain from ``options_eod_databento.read_chain``.

    ``rate`` is the annual risk-free rate as a decimal (0.04 for 4%). Returns a new mapping; the
    input is not changed. A contract without a value carries ``no_value`` saying why.
    """
    if isinstance(rate, bool) or not isinstance(rate, (int, float)) or not math.isfinite(rate) or not -0.05 <= rate <= 0.25:
        raise ValueError("rate must be an annual decimal between -0.05 and 0.25")
    day = date.fromisoformat(chain["day"])
    as_of = _close_utc(day)   # when the quotes were taken; not when GRID knew them
    known = [str(part["known_at"]) for part in (chain.get("parts") or {}).values()
             if isinstance(part, dict) and part.get("known_at")]

    by_expiry: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for contract in chain.get("contracts", []):
        by_expiry[contract["expiry"]].append(dict(contract))

    expiries: dict[str, dict[str, Any]] = {}
    out: list[dict[str, Any]] = []
    for expiry in sorted(by_expiry):
        rows = by_expiry[expiry]
        t_years = (_close_utc(date.fromisoformat(expiry)) - as_of).total_seconds() / (YEAR_DAYS * 86400.0)
        info: dict[str, Any] = {"t_years": t_years, "forward": None, "pairs": 0, "forward_range": None}
        expiries[expiry] = info
        for row in rows:
            row.update(mid=_mid(row), implied_vol=None, delta=None, gamma=None, vega=None, theta=None,
                       in_the_money=None, no_value=None)
        if t_years < MIN_T_YEARS:
            info["no_value"] = "expires at this close"
        else:
            mids: dict[float, dict[str, float]] = defaultdict(dict)
            for row in rows:
                if row["mid"] is not None:
                    mids[row["strike"]][row["opt_type"]] = row["mid"]
            forward = implied_forward(
                [(k, sides["call"], sides["put"]) for k, sides in mids.items() if len(sides) == 2], t_years, rate)
            if forward is None:
                info["no_value"] = "no strike with a two-sided call and put"
            else:
                info.update(forward)
        if info.get("no_value"):
            for row in rows:
                row["no_value"] = info["no_value"] if row["mid"] is not None else "no two-sided quote"
            out.extend(rows)
            continue

        fwd = info["forward"]
        spot_star = fwd * math.exp(-rate * t_years)
        quoted = [row for row in rows if row["mid"] is not None]
        strikes = np.array([row["strike"] for row in quoted], dtype=float)
        is_call = np.array([row["opt_type"] == "call" for row in quoted], dtype=bool)
        mid = np.array([row["mid"] for row in quoted], dtype=float)
        iv = implied_vols(spot_star, strikes, t_years, mid, is_call, rate, 0.0)
        solved = np.isfinite(iv)
        sigma = np.where(solved, iv, 1.0)
        delta = np.where(is_call, bs.delta(spot_star, strikes, t_years, rate, sigma, 0.0, is_call=True),
                         bs.delta(spot_star, strikes, t_years, rate, sigma, 0.0, is_call=False))
        gamma = bs.gamma(spot_star, strikes, t_years, rate, sigma, 0.0)
        vega = bs.vega(spot_star, strikes, t_years, rate, sigma, 0.0) / 100.0
        theta = bs.theta(spot_star, strikes, t_years, rate, sigma, 0.0, is_call=is_call) / YEAR_DAYS
        for i, row in enumerate(quoted):
            row["in_the_money"] = bool(row["strike"] < fwd) if is_call[i] else bool(row["strike"] > fwd)
            if not solved[i]:
                row["no_value"] = "mid outside the model's bounds"
                continue
            row.update(implied_vol=float(iv[i]), delta=_none_if_nan(delta[i]), gamma=_none_if_nan(gamma[i]),
                       vega=_none_if_nan(vega[i]), theta=_none_if_nan(theta[i]))
        for row in rows:
            if row["mid"] is None:
                row["no_value"] = "no two-sided quote"
        out.extend(rows)

    valued = sum(1 for row in out if row["implied_vol"] is not None)
    return {
        "ticker": chain.get("ticker"), "day": chain["day"], "provider": chain.get("provider"),
        "audience": chain.get("audience"), "parts": chain.get("parts"),
        "method": METHOD, "rate": float(rate),
        # Two clocks, never one: the minute the quotes describe, and the latest fetch of any part
        # (the archive's known-at clock). A point-in-time reader uses known_at.
        "quotes_at": as_of.strftime("%Y-%m-%dT%H:%M:%SZ"), "known_at": max(known) if known else None,
        "units": {"delta": "per $1 of the dividend-adjusted spot", "gamma": "per $1 of the dividend-adjusted spot",
                  "vega": "per one volatility point", "theta": "per calendar day", "implied_vol": "annual decimal"},
        "limits": list(LIMITS), "expiries": expiries, "contracts": out,
        "counts": {"contracts": len(out), "valued": valued,
                   "two_sided": sum(1 for row in out if row["mid"] is not None)},
    }
