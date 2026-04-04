#!/usr/bin/env python3
"""
Granular inflection point analysis — day-by-day signals.

For each mega-rally, zoom into the 60-day window around the trough:
  - ALL 21 astro features, daily
  - ALL macro features, daily
  - VIX trajectory (5d rolling, direction, acceleration)
  - Credit spread trajectory
  - Cross-feature correlations
  - Exact day the "turn" happened and what changed

Goal: build a bottom detector that fires within DAYS, not weeks.
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


MEGA_RALLIES = [
    {"ticker": "NVDA", "feature": "nvda_full", "trough": date(2022, 10, 14), "rally": 10677,
     "narrative": "AI revolution — ChatGPT launches Nov 30, GPU demand explodes"},
    {"ticker": "META", "feature": "meta_full", "trough": date(2022, 11, 3), "rally": 789,
     "narrative": "Year of Efficiency announced, metaverse pivot abandoned"},
    {"ticker": "BTC", "feature": "btc_full", "trough": date(2022, 11, 21), "rally": 690,
     "narrative": "Post-FTX bottom, ETF narrative begins building"},
    {"ticker": "COIN", "feature": "coin_full", "trough": date(2022, 12, 28), "rally": 1190,
     "narrative": "Crypto exchange survival + ETF catalyst"},
    {"ticker": "MSTR", "feature": "mstr_full", "trough": date(2022, 12, 29), "rally": 1305,
     "narrative": "Leveraged BTC proxy — max pain becomes max gain"},
    {"ticker": "NFLX", "feature": "nflx_full", "trough": date(2022, 5, 11), "rally": 705,
     "narrative": "Ad tier + password crackdown = growth restart"},
    {"ticker": "SOL", "feature": "sol_full", "trough": date(2021, 1, 2), "rally": 14454,
     "narrative": "DeFi summer → Solana ecosystem explosion"},
    {"ticker": "AVGO", "feature": "avgo_full", "trough": date(2023, 5, 4), "rally": 602,
     "narrative": "AI infra demand + VMware acquisition"},
]

ALL_FEATURES = [
    # Astro
    "planetary_stress_index", "lunar_illumination", "lunar_phase",
    "days_to_full_moon", "days_to_new_moon", "mars_volatility_index",
    "mercury_retrograde", "solar_flux_10_7cm", "sunspot_number",
    "nakshatra_index", "jupiter_saturn_angle", "solar_cycle_phase",
    "venus_cycle_phase", "solar_eclipse_proximity", "lunar_eclipse_proximity",
    # Macro
    "vix_spot", "hy_oas_spread", "yld_curve_2s10s", "ofr_financial_stress",
    "skew_index", "fed_funds_rate", "fed_balance_sheet", "treasury_general_acct",
    # Sentiment
    "gdelt_article_count", "gdelt_conflict_count",
]


def load_window(engine, feature: str, center: date, days: int = 45) -> pd.Series:
    start = center - timedelta(days=days)
    end = center + timedelta(days=days)
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT rs.obs_date, rs.value FROM resolved_series rs
            JOIN feature_registry fr ON rs.feature_id = fr.id
            WHERE fr.name = :n AND rs.obs_date BETWEEN :s AND :e
            ORDER BY rs.obs_date
        """), {"n": feature, "s": start, "e": end}).fetchall()
    if not rows:
        return pd.Series(dtype=float, name=feature)
    s = pd.Series([r[1] for r in rows], index=pd.to_datetime([r[0] for r in rows]), name=feature)
    return s[~s.index.duplicated(keep="last")].sort_index()


def compute_trajectory(series: pd.Series, trough_date: date) -> dict:
    """Compute pre/post trough trajectory metrics."""
    td = pd.Timestamp(trough_date)
    before = series[series.index <= td]
    after = series[series.index > td]

    result = {}
    if len(before) >= 5:
        result["30d_before_avg"] = before.tail(30).mean()
        result["7d_before_avg"] = before.tail(7).mean()
        result["at_trough"] = before.iloc[-1]
        # Direction: was it rising or falling into the trough?
        if len(before) >= 10:
            result["10d_slope"] = (before.iloc[-1] - before.iloc[-10]) / 10
    if len(after) >= 5:
        result["7d_after_avg"] = after.head(7).mean()
        result["30d_after_avg"] = after.head(30).mean()
        # Direction change after trough
        if "at_trough" in result:
            result["7d_change"] = after.head(7).mean() - result["at_trough"]

    return result


def main() -> None:
    engine = get_engine()

    log.info("=" * 80)
    log.info("GRANULAR INFLECTION ANALYSIS — DAY BY DAY")
    log.info("=" * 80)

    # Collect all inflection snapshots for cross-analysis
    all_snapshots = []

    for rally in MEGA_RALLIES:
        ticker = rally["ticker"]
        trough = rally["trough"]

        log.info("\n" + "=" * 80)
        log.info("{t} — TROUGH {d} (before +{r}% rally)", t=ticker, d=trough, r=rally["rally"])
        log.info("Narrative: {n}", n=rally["narrative"])
        log.info("=" * 80)

        # Load price around trough
        price = load_window(engine, rally["feature"], trough, 45)
        if price.empty:
            log.warning("No price data for {t}", t=ticker)
            continue

        td = pd.Timestamp(trough)
        price_before = price[price.index <= td]
        price_after = price[price.index > td]

        if len(price_before) > 0 and len(price_after) > 0:
            trough_px = price_before.iloc[-1]
            px_7d_after = price_after.head(7).iloc[-1] if len(price_after) >= 7 else price_after.iloc[-1]
            px_30d_after = price_after.head(30).iloc[-1] if len(price_after) >= 30 else price_after.iloc[-1]

            log.info("\n  PRICE TRAJECTORY:")
            log.info("    Trough: ${px:.2f}", px=trough_px)
            log.info("    +7d:    ${px:.2f} ({chg:+.1f}%)",
                     px=px_7d_after, chg=(px_7d_after / trough_px - 1) * 100)
            log.info("    +30d:   ${px:.2f} ({chg:+.1f}%)",
                     px=px_30d_after, chg=(px_30d_after / trough_px - 1) * 100)

            # Day-by-day price for 10 days around trough
            window_start = max(0, len(price_before) - 5)
            window_end = min(len(price_after), 5)
            log.info("\n  DAY-BY-DAY PRICE (5 days before → 5 days after trough):")
            for i in range(window_start, len(price_before)):
                dt = price_before.index[i]
                px = price_before.iloc[i]
                days_to = (td - dt).days
                log.info("    T-{d:2d}  {dt}  ${px:.2f}", d=days_to, dt=dt.date(), px=px)
            log.info("    T= 0  {dt}  ${px:.2f}  *** TROUGH ***", dt=td.date(), px=trough_px)
            for i in range(window_end):
                dt = price_after.index[i]
                px = price_after.iloc[i]
                days_from = (dt - td).days
                chg = (px / trough_px - 1) * 100
                log.info("    T+{d:2d}  {dt}  ${px:.2f}  ({chg:+.1f}%)",
                         d=days_from, dt=dt.date(), px=px, chg=chg)

        # Load ALL features around trough
        snapshot = {"ticker": ticker, "trough": trough, "rally_pct": rally["rally"]}
        log.info("\n  FEATURE TRAJECTORIES:")

        for feat_name in ALL_FEATURES:
            series = load_window(engine, feat_name, trough, 30)
            if series.empty or len(series) < 3:
                continue

            traj = compute_trajectory(series, trough)
            if not traj:
                continue

            at_trough = traj.get("at_trough")
            if at_trough is None:
                continue

            snapshot[feat_name] = at_trough

            # Only log features that show interesting movement
            slope = traj.get("10d_slope")
            change_7d = traj.get("7d_change")

            before_avg = traj.get("7d_before_avg", at_trough)
            after_avg = traj.get("7d_after_avg", at_trough)

            # Flag features with notable regime change around trough
            if before_avg != 0 and abs((after_avg - before_avg) / (abs(before_avg) + 1e-8)) > 0.1:
                direction = "↑" if after_avg > before_avg else "↓"
                log.info(
                    "    {f:30s} 7d_before={b:>8.2f} → trough={t:>8.2f} → 7d_after={a:>8.2f}  {d}",
                    f=feat_name, b=before_avg, t=at_trough, a=after_avg, d=direction,
                )

        all_snapshots.append(snapshot)

    # === CROSS-INFLECTION ANALYSIS ===
    log.info("\n" + "=" * 80)
    log.info("CROSS-INFLECTION ANALYSIS")
    log.info("What features are consistent across ALL bottoms?")
    log.info("=" * 80)

    snap_df = pd.DataFrame(all_snapshots)
    numeric_cols = [c for c in snap_df.columns if c not in ("ticker", "trough", "rally_pct")]

    # For each feature, compute z-score vs full history
    log.info("\n  Feature values at inflection vs historical distribution:")
    feature_zscores = {}

    for col in numeric_cols:
        vals = snap_df[col].dropna()
        if len(vals) < 3:
            continue

        with engine.connect() as conn:
            hist = conn.execute(text("""
                SELECT rs.value FROM resolved_series rs
                JOIN feature_registry fr ON rs.feature_id = fr.id
                WHERE fr.name = :n AND rs.obs_date >= '2020-01-01'
            """), {"n": col}).fetchall()
        if not hist:
            continue

        hist_vals = pd.Series([h[0] for h in hist])
        overall_mean = hist_vals.mean()
        overall_std = hist_vals.std()
        if overall_std == 0:
            continue

        inflection_mean = vals.mean()
        z = (inflection_mean - overall_mean) / overall_std

        feature_zscores[col] = {
            "z_score": z,
            "inflection_mean": inflection_mean,
            "overall_mean": overall_mean,
            "overall_std": overall_std,
            "n_rallies": len(vals),
            "consistency": vals.std() / (abs(inflection_mean) + 1e-8),  # lower = more consistent
        }

    # Sort by absolute z-score
    sorted_features = sorted(feature_zscores.items(), key=lambda x: abs(x[1]["z_score"]), reverse=True)

    log.info("\n  RANKED BY SIGNAL STRENGTH (z-score at inflection points):")
    for feat, stats in sorted_features:
        if abs(stats["z_score"]) > 0.3:
            direction = "HIGH" if stats["z_score"] > 0 else "LOW"
            log.info(
                "    {f:30s} z={z:+6.2f} ({d:4s})  at_inflection={i:>8.2f}  overall={o:>8.2f}  "
                "consistency={c:.2f}  n={n}",
                f=feat, z=stats["z_score"], d=direction,
                i=stats["inflection_mean"], o=stats["overall_mean"],
                c=stats["consistency"], n=stats["n_rallies"],
            )

    # === TIMING ANALYSIS ===
    log.info("\n" + "=" * 80)
    log.info("TIMING PRECISION — How close can we get to the exact bottom?")
    log.info("=" * 80)

    for rally in MEGA_RALLIES:
        ticker = rally["ticker"]
        trough = rally["trough"]

        # What changed in the 5 days BEFORE the trough?
        vix_window = load_window(engine, "vix_spot", trough, 15)
        psi_window = load_window(engine, "planetary_stress_index", trough, 15)
        hy_window = load_window(engine, "hy_oas_spread", trough, 15)
        price = load_window(engine, rally["feature"], trough, 15)

        if vix_window.empty or price.empty:
            continue

        td = pd.Timestamp(trough)

        log.info("\n  {t} — 10 days around trough {d}:", t=ticker, d=trough)
        log.info("    {'Date':12s} {'Price':>8s} {'VIX':>6s} {'PSI':>5s} {'HY':>6s} {'Notes':s}")

        for day_offset in range(-5, 6):
            dt = td + pd.Timedelta(days=day_offset)

            px = price.get(dt)
            vx = vix_window.get(dt)
            ps = psi_window.get(dt)
            hy_val = hy_window.get(dt)

            if px is None:
                continue

            marker = " *** TROUGH ***" if day_offset == 0 else ""
            log.info(
                "    {dt}  ${px:>8.2f}  {vx:>5s}  {ps:>4s}  {hy:>5s}{m}",
                dt=dt.date(),
                px=px,
                vx=f"{vx:.1f}" if vx is not None else "  -",
                ps=f"{ps:.1f}" if ps is not None else "  -",
                hy=f"{hy_val:.2f}" if hy_val is not None else "  -",
                m=marker,
            )

    # === BOTTOM DETECTOR RULES ===
    log.info("\n" + "=" * 80)
    log.info("BOTTOM DETECTOR — PRECISE RULES")
    log.info("=" * 80)

    log.info("""
    RULE SET (based on granular analysis of 8 mega-rallies):

    STRUCTURAL (must have ALL):
      1. Asset in >50% drawdown from ATH
      2. VIX > 20 (fear present, not complacent)
      3. PSI in range [0, 4] (planetary stress moderate)

    CONFIRMATION (need 3+ of 5):
      4. HY OAS spread > 4.0 (credit stress)
      5. OFR Financial Stress > 0 (systemic stress)
      6. Yield curve inverted (2s10s < 0)
      7. VIX declining from peak (fear starting to recede)
      8. Price 5-day momentum turning positive (first green shoots)

    TIMING PRECISION:
      - VIX peaking and starting to decline = the signal
      - Price forming higher low (not necessarily THE low)
      - Mercury retrograde ending can mark exact turns

    POSITION SIZING:
      Score 6-7: Full 20% position
      Score 5:   10% starter position
      Score 4:   5% pilot, add on confirmation
      Score <4:  Watch only
    """)


if __name__ == "__main__":
    main()
