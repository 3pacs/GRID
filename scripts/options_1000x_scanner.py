#!/usr/bin/env python3
"""
1000x Options Scanner.

Identifies options setups with asymmetric payoff potential using:
  1. Bottom detector candidates (deep drawdown, momentum turning)
  2. PSI oracle signals (timing)
  3. IV/skew analysis (cheap insurance becomes 1000x lottery)
  4. Historical pattern: what option strikes would have produced 1000x
     on the known mega-rallies?

The thesis: when you identify the bottom with the 3-ingredient formula
(fear + narrative + astro), buying deep OTM calls 6-12 months out
on a $10-50 stock that's down 80%+ can produce 100-1000x returns
if the asset recovers to prior ATH.

Method:
  1. Take bottom detector PILOT/WATCH candidates
  2. For each, compute theoretical option payoffs at various strikes
  3. Identify the "sweet spot" strike/expiry combinations
  4. Score by risk/reward ratio
  5. Show what WOULD have happened on historical mega-rallies
"""

from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger as log

_GRID_DIR = str(Path(__file__).resolve().parent.parent)
os.chdir(_GRID_DIR)
if _GRID_DIR not in sys.path:
    sys.path.insert(0, _GRID_DIR)

from db import get_engine
from sqlalchemy import text
from alpha_research.data.split_adjuster import compute_real_drawdown, get_post_split_series


def load_price_history(engine, ticker: str, start: str = "2021-01-01", adjust_splits: bool = False) -> pd.Series:
    feat = f"{ticker.lower()}_full"
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT rs.obs_date, rs.value FROM resolved_series rs
            JOIN feature_registry fr ON rs.feature_id = fr.id
            WHERE fr.name = :n AND rs.obs_date >= :s ORDER BY rs.obs_date
        """), {"n": feat, "s": start}).fetchall()
    if not rows:
        return pd.Series(dtype=float)
    s = pd.Series([r[1] for r in rows], index=pd.to_datetime([r[0] for r in rows]))
    s = s[~s.index.duplicated(keep="last")].sort_index()
    if adjust_splits:
        return get_post_split_series(s)
    return s


def compute_option_payoffs(
    entry_price: float,
    future_prices: pd.Series,
    strikes: list[float],
    premium_pct: float = 0.05,
) -> dict:
    """
    Compute call option payoffs for various strikes.

    Assumes buying calls at premium_pct of entry price.
    Returns multiplier (payoff / premium) at each future price.
    """
    premium = entry_price * premium_pct
    results = {}

    for strike in strikes:
        payoffs = []
        for dt, px in future_prices.items():
            intrinsic = max(0, px - strike)
            multiplier = intrinsic / premium if premium > 0 else 0
            payoffs.append({
                "date": dt,
                "price": px,
                "intrinsic": intrinsic,
                "multiplier": multiplier,
            })

        if payoffs:
            max_payoff = max(payoffs, key=lambda p: p["multiplier"])
            results[strike] = {
                "payoffs": payoffs,
                "max_multiplier": max_payoff["multiplier"],
                "max_date": max_payoff["date"],
                "max_price": max_payoff["price"],
                "premium": premium,
            }

    return results


def main() -> None:
    engine = get_engine()

    log.info("=" * 80)
    log.info("1000x OPTIONS SCANNER")
    log.info("=" * 80)

    # === PHASE 1: HISTORICAL — What options would have returned 1000x? ===
    log.info("\n" + "=" * 80)
    log.info("PHASE 1: HISTORICAL 1000x OPTIONS")
    log.info("If you bought calls at the trough, what would have happened?")
    log.info("=" * 80)

    # Build historical rallies from ACTUAL post-split data
    HISTORICAL_TICKERS = ["NVDA", "META", "SOL", "COIN", "MSTR", "BTC", "NFLX", "PYPL",
                          "TSLA", "NKE", "ADBE", "LLY", "AVGO", "DVN"]

    HISTORICAL_RALLIES = []
    for ticker in HISTORICAL_TICKERS:
        prices_raw = load_price_history(engine, ticker, "2020-01-01")
        if prices_raw.empty:
            continue
        clean = get_post_split_series(prices_raw)
        if len(clean) < 60:
            continue
        # Find biggest trough-to-peak rally in post-split data
        cum_min = clean.expanding().min()
        rally_pct = ((clean / cum_min) - 1)
        max_rally = rally_pct.max()
        if max_rally < 0.5:  # Need at least 50% rally
            continue
        peak_date = rally_pct.idxmax()
        trough_val = cum_min.loc[peak_date]
        trough_candidates = clean[clean == trough_val]
        trough_date = trough_candidates.index[0] if len(trough_candidates) > 0 else clean.index[0]
        HISTORICAL_RALLIES.append({
            "ticker": ticker,
            "trough_date": str(trough_date.date()),
            "trough_px": trough_val,
            "peak_px": clean.loc[peak_date],
            "rally_pct": max_rally * 100,
        })

    HISTORICAL_RALLIES.sort(key=lambda r: -r["rally_pct"])
    log.info("Found {n} historical rallies (split-adjusted, >50%)", n=len(HISTORICAL_RALLIES))

    for rally in HISTORICAL_RALLIES[:10]:
        ticker = rally["ticker"]
        trough_px = rally["trough_px"]
        trough_date = rally["trough_date"]

        prices = load_price_history(engine, ticker, trough_date, adjust_splits=True)
        if prices.empty:
            continue

        # Define strikes: ATM, 50% OTM, 100% OTM, 200% OTM
        strikes = [
            trough_px * 1.0,   # ATM
            trough_px * 1.5,   # 50% OTM
            trough_px * 2.0,   # 100% OTM (double)
            trough_px * 3.0,   # 200% OTM (triple)
            trough_px * 5.0,   # 400% OTM
            trough_px * 10.0,  # 900% OTM
        ]

        # Premium assumptions: 5% of stock price for 6-month call
        premium = trough_px * 0.05

        log.info(
            "\n  {t:5s} — Trough ${px:.2f} on {d} → Peak ${ppx:.2f} (+{rp:.0f}%) (premium ~${pr:.2f} per call)",
            t=ticker, px=trough_px, d=trough_date,
            ppx=rally.get("peak_px", 0), rp=rally.get("rally_pct", 0),
            pr=premium,
        )

        # Future prices at 30d, 90d, 180d, 365d
        horizons = {"30d": 30, "90d": 90, "180d": 180, "365d": 365, "peak": None}

        log.info(
            "    {'Strike':>10s} {'Type':>8s} {'30d':>8s} {'90d':>8s} {'180d':>8s} {'365d':>8s} {'Peak':>8s}"
        )

        for strike in strikes:
            otm_pct = (strike / trough_px - 1) * 100
            strike_type = "ATM" if otm_pct < 5 else f"+{otm_pct:.0f}%"

            multipliers = {}
            for label, days in horizons.items():
                if days is not None and len(prices) > days:
                    px = prices.iloc[days]
                    intrinsic = max(0, px - strike)
                    mult = intrinsic / premium
                    multipliers[label] = mult
                elif days is None:
                    # Peak
                    px = prices.max()
                    intrinsic = max(0, px - strike)
                    mult = intrinsic / premium
                    multipliers[label] = mult

            log.info(
                "    ${s:>9.2f} {t:>8s} {m30:>7.0f}x {m90:>7.0f}x {m180:>7.0f}x {m365:>7.0f}x {peak:>7.0f}x",
                s=strike, t=strike_type,
                m30=multipliers.get("30d", 0),
                m90=multipliers.get("90d", 0),
                m180=multipliers.get("180d", 0),
                m365=multipliers.get("365d", 0),
                peak=multipliers.get("peak", 0),
            )

    # === PHASE 2: CURRENT CANDIDATES — What options look attractive now? ===
    log.info("\n" + "=" * 80)
    log.info("PHASE 2: CURRENT 1000x OPTION CANDIDATES (split-adjusted)")
    log.info("Based on bottom detector PILOT/WATCH list")
    log.info("=" * 80)

    # Build candidates dynamically from ALL tickers with split adjustment
    with engine.connect() as conn:
        all_tickers = conn.execute(text("""
            SELECT fr.name FROM feature_registry fr
            JOIN resolved_series rs ON fr.id = rs.feature_id
            WHERE fr.name LIKE '%_full'
            GROUP BY fr.name HAVING COUNT(*) > 200
        """)).fetchall()

    CURRENT_CANDIDATES = []
    for (feat_name,) in all_tickers:
        ticker = feat_name.replace("_full", "").upper()
        prices = load_price_history(engine, ticker, "2020-01-01")
        if prices.empty or len(prices) < 60:
            continue
        metrics = compute_real_drawdown(prices)
        if "error" in metrics or metrics["drawdown_pct"] > -25:
            continue
        CURRENT_CANDIDATES.append({
            "ticker": ticker,
            "current": metrics["current"],
            "ath": metrics["ath"],
            "dd": metrics["drawdown_pct"],
            "mom_30d": metrics["mom_30d"],
            "mom_90d": metrics["mom_90d"],
            "score": 5 if (metrics["mom_30d"] > 0 and metrics["mom_90d"] < 0 and metrics["drawdown_pct"] < -70) else 4,
            "status": "PILOT" if (metrics["mom_30d"] > 0 and metrics["mom_90d"] < 0 and metrics["drawdown_pct"] < -70) else "WATCH",
            "has_split": metrics["has_split"],
        })

    CURRENT_CANDIDATES.sort(key=lambda c: c["dd"])

    log.info("Found {n} candidates with >25% drawdown (split-adjusted)", n=len(CURRENT_CANDIDATES))

    for cand in CURRENT_CANDIDATES:
        ticker = cand["ticker"]
        px = cand["current"]
        ath = cand["ath"]
        dd = cand["dd"]

        # Strike levels
        atm = round(px, 0)
        otm_50 = round(px * 1.5, 0)
        otm_100 = round(px * 2.0, 0)
        recovery_50 = round(ath * 0.5, 0)  # 50% recovery to ATH
        recovery_100 = round(ath, 0)

        # Premium estimate: 5% for 6-month, 8% for 12-month
        prem_6m = px * 0.05
        prem_12m = px * 0.08

        # Payoff scenarios
        scenarios = {
            "50% bounce": px * 1.5,
            "100% bounce": px * 2.0,
            "50% ATH recovery": ath * 0.5,
            "Full ATH recovery": ath,
        }

        log.info(
            "\n  {t:5s} [{s}] ${px:.2f} (DD {dd:.0f}%, ATH ${ath:.2f})",
            t=ticker, s=cand["status"], px=px, dd=dd, ath=ath,
        )

        log.info("    OPTION SCENARIOS (6-month calls, ~{pr:.0f}% premium):", pr=5)
        log.info("    {'Strike':>10s} {'OTM%':>6s}  {'50%bounce':>10s} {'100%bounce':>11s} {'50%ATH':>10s} {'FullATH':>10s}")

        for strike_label, strike in [
            ("ATM", atm),
            ("+50%", otm_50),
            ("+100%", otm_100),
            ("50%ATH", recovery_50),
        ]:
            otm = (strike / px - 1) * 100
            payoffs = {}
            for scenario_name, target_px in scenarios.items():
                intrinsic = max(0, target_px - strike)
                mult = intrinsic / prem_6m if prem_6m > 0 else 0
                payoffs[scenario_name] = mult

            log.info(
                "    ${s:>9.0f} {otm:>5.0f}%  {p1:>9.0f}x {p2:>10.0f}x {p3:>9.0f}x {p4:>9.0f}x",
                s=strike, otm=otm,
                p1=payoffs.get("50% bounce", 0),
                p2=payoffs.get("100% bounce", 0),
                p3=payoffs.get("50% ATH recovery", 0),
                p4=payoffs.get("Full ATH recovery", 0),
            )

        # Best asymmetric play
        recovery_mult = (ath - atm) / prem_6m if prem_6m > 0 else 0
        log.info(
            "    BEST ASYMMETRIC: ATM call at ${s:.0f}, full recovery = {m:.0f}x on {pr:.0f}% premium",
            s=atm, m=recovery_mult, pr=5,
        )

    # === PHASE 3: THE 1000x PLAYBOOK ===
    log.info("\n" + "=" * 80)
    log.info("THE 1000x OPTIONS PLAYBOOK")
    log.info("=" * 80)
    log.info("""
    ENTRY CRITERIA (all must be true):
      1. Bottom detector score >= 5 (PILOT level)
      2. Asset in 70%+ drawdown from ATH
      3. Momentum turning (30d positive, 90d negative)
      4. PSI in favorable range (0.5-4.0)
      5. VIX > 20 (fear premium makes calls cheaper)

    OPTION STRUCTURE:
      - Buy 6-12 month expiry CALL options
      - Strike: ATM or slightly OTM (up to +50%)
      - Size: 1-2% of portfolio per position (lottery ticket sizing)
      - Premium: target 5-8% of stock price

    PAYOFF MATH:
      - $100 stock at $5 premium (5%)
      - Stock goes to $300 (ATH recovery) = $200 intrinsic = 40x return
      - Stock goes to $500 (new ATH) = $400 intrinsic = 80x return
      - Stock goes to $1000 (mega-rally) = $900 intrinsic = 180x return

    RISK MANAGEMENT:
      - Max loss: premium paid (1-2% of portfolio)
      - Roll forward at 50% time decay if thesis intact
      - Take 50% off at 10x, let rest ride to 100x+
      - Add to position on dips if setup score improves

    INVALIDATION (close position):
      - VIX > 45 (systemic crisis — options will spike, sell into vol)
      - Stock makes new low below trough (thesis broken)
      - Narrative catalyst fails (e.g., AI spending cuts for NVDA)
      - Setup score drops below 3

    CURRENT TOP PICKS (by asymmetry):""")

    # Rank by recovery potential / premium ratio
    picks = []
    for cand in CURRENT_CANDIDATES:
        recovery_potential = cand["ath"] / cand["current"] - 1
        premium_cost = 0.05  # 5%
        asymmetry = recovery_potential / premium_cost
        picks.append({**cand, "asymmetry": asymmetry, "recovery_pct": recovery_potential * 100})

    picks.sort(key=lambda p: -p["asymmetry"])

    for i, p in enumerate(picks[:7], 1):
        log.info(
            "      #{i}  {t:5s}  ${px:>8.2f}  DD={dd:>5.0f}%  Recovery={r:>6.0f}%  Asymmetry={a:>5.0f}x  [{s}]",
            i=i, t=p["ticker"], px=p["current"], dd=p["dd"],
            r=p["recovery_pct"], a=p["asymmetry"], s=p["status"],
        )

    log.info("\nDone.")


if __name__ == "__main__":
    main()
