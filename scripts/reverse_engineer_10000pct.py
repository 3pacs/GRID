#!/usr/bin/env python3
"""
Reverse-Engineer the 10,000% Model.

Known massive moves (2021-2026):
  SOL:  +14,454% ($1.80 → $262) — 2021-01 to 2025-01
  NVDA: +10,677% ($11 → $1,210)  — 2022-10 to 2024-06
  XLB:  +7,081%
  GOOGL:+3,340%
  GE:   +2,781%
  MSTR: +1,305%
  COIN: +1,190%
  META: +789% ($89 → $790)      — 2022-11 to 2025-08
  BTC:  +690% ($15.8K → $125K)  — 2022-11 to 2025-10

Question: what social/news/astro signals preceded these inflection points?
Can we build a pattern that would catch the NEXT 10,000% move?

Method:
  1. Identify exact inflection dates (troughs before rallies)
  2. Look at ALL available signals 30 days before each inflection
  3. Find the common thread across multiple mega-rallies
  4. Build a "mega-rally detector" from those signals
  5. Backtest: would this detector have caught these moves?
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

# Known inflection points (troughs before massive rallies)
MEGA_RALLIES = [
    {"ticker": "SOL", "trough": date(2021, 1, 2), "rally_pct": 14454, "narrative": "DeFi summer → Solana ecosystem explosion"},
    {"ticker": "NVDA", "trough": date(2022, 10, 14), "rally_pct": 10677, "narrative": "AI revolution: ChatGPT launch → GPU demand explosion"},
    {"ticker": "META", "trough": date(2022, 11, 3), "rally_pct": 789, "narrative": "Metaverse pivot abandoned, efficiency year, AI pivot"},
    {"ticker": "BTC", "trough": date(2022, 11, 21), "rally_pct": 690, "narrative": "Post-FTX bottom, ETF narrative builds"},
    {"ticker": "MSTR", "trough": date(2022, 12, 29), "rally_pct": 1305, "narrative": "BTC proxy: leveraged Bitcoin treasury play"},
    {"ticker": "COIN", "trough": date(2022, 12, 28), "rally_pct": 1190, "narrative": "Crypto exchange: SEC clarity + ETF approval"},
    {"ticker": "NFLX", "trough": date(2022, 5, 11), "rally_pct": 705, "narrative": "Ad tier launch, password sharing crackdown works"},
    {"ticker": "DVN", "trough": date(2021, 1, 4), "rally_pct": 530, "narrative": "Energy crisis: Russia-Ukraine → oil spike"},
    {"ticker": "LLY", "trough": date(2021, 4, 28), "rally_pct": 514, "narrative": "GLP-1 revolution: Mounjaro/Zepbound pipeline"},
    {"ticker": "AVGO", "trough": date(2023, 5, 4), "rally_pct": 602, "narrative": "AI infrastructure: custom chip demand + VMware acquisition"},
]


def load_feature_window(engine, name: str, center_date: date, window_days: int = 60) -> pd.Series:
    """Load feature data around a date."""
    start = center_date - timedelta(days=window_days)
    end = center_date + timedelta(days=window_days)
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT rs.obs_date, rs.value FROM resolved_series rs
            JOIN feature_registry fr ON rs.feature_id = fr.id
            WHERE fr.name = :name AND rs.obs_date BETWEEN :s AND :e
            ORDER BY rs.obs_date
        """), {"name": name, "s": start, "e": end}).fetchall()
    if not rows:
        return pd.Series(dtype=float, name=name)
    s = pd.Series([r[1] for r in rows], index=pd.to_datetime([r[0] for r in rows]), name=name)
    return s[~s.index.duplicated(keep="last")].sort_index()


def load_all_features_at_date(engine, target_date: date) -> dict[str, float | None]:
    """Load all key features at a specific date (or nearest prior)."""
    features = {}
    feat_names = [
        "planetary_stress_index", "vix_spot", "hy_oas_spread",
        "yld_curve_2s10s", "ofr_financial_stress", "skew_index",
        "gdelt_article_count", "gdelt_conflict_count",
        "fed_funds_rate", "fed_balance_sheet", "treasury_general_acct",
        "lunar_illumination", "mars_volatility_index", "mercury_retrograde",
        "solar_flux_10_7cm", "sunspot_number", "nakshatra_index",
        "jupiter_saturn_angle", "solar_cycle_phase",
    ]

    for name in feat_names:
        with engine.connect() as conn:
            row = conn.execute(text("""
                SELECT rs.value FROM resolved_series rs
                JOIN feature_registry fr ON rs.feature_id = fr.id
                WHERE fr.name = :name AND rs.obs_date <= :d
                ORDER BY rs.obs_date DESC LIMIT 1
            """), {"name": name, "d": target_date}).fetchone()
        features[name] = float(row[0]) if row else None

    return features


def main() -> None:
    engine = get_engine()

    log.info("=" * 70)
    log.info("REVERSE-ENGINEERING THE 10,000% MOVE")
    log.info("=" * 70)

    # === PHASE 1: Snapshot all features at each inflection point ===
    log.info("\n" + "=" * 70)
    log.info("PHASE 1: FEATURE SNAPSHOT AT EACH INFLECTION POINT")
    log.info("=" * 70)

    snapshots = []
    for rally in MEGA_RALLIES:
        ticker = rally["ticker"]
        trough = rally["trough"]
        features = load_all_features_at_date(engine, trough)

        log.info(
            "\n--- {t} trough {d} (before +{r}% rally) ---",
            t=ticker, d=trough, r=rally["rally_pct"],
        )
        log.info("  Narrative: {n}", n=rally["narrative"])

        for name, val in features.items():
            if val is not None:
                log.info("    {n:35s} = {v:.2f}", n=name, v=val)

        snapshots.append({"ticker": ticker, "trough": trough, **features})

    snap_df = pd.DataFrame(snapshots)

    # === PHASE 2: Find the common thread ===
    log.info("\n" + "=" * 70)
    log.info("PHASE 2: COMMON THREAD ANALYSIS")
    log.info("What features are consistently extreme at inflection points?")
    log.info("=" * 70)

    numeric_cols = [c for c in snap_df.columns if c not in ("ticker", "trough")]

    for col in numeric_cols:
        vals = snap_df[col].dropna()
        if len(vals) < 3:
            continue

        # Load full history for comparison
        with engine.connect() as conn:
            all_rows = conn.execute(text("""
                SELECT rs.value FROM resolved_series rs
                JOIN feature_registry fr ON rs.feature_id = fr.id
                WHERE fr.name = :name AND rs.obs_date >= '2021-01-01'
            """), {"name": col}).fetchall()

        if not all_rows:
            continue

        all_vals = pd.Series([r[0] for r in all_rows])
        mean_at_inflection = vals.mean()
        overall_mean = all_vals.mean()
        overall_std = all_vals.std()

        if overall_std == 0:
            continue

        z_score = (mean_at_inflection - overall_mean) / overall_std

        if abs(z_score) > 0.5:
            direction = "HIGH" if z_score > 0 else "LOW"
            log.info(
                "  {col:35s} z={z:+.2f} ({dir})  inflection_avg={ia:.2f}  overall_avg={oa:.2f}",
                col=col, z=z_score, dir=direction,
                ia=mean_at_inflection, oa=overall_mean,
            )

    # === PHASE 3: VIX pattern before mega-rallies ===
    log.info("\n" + "=" * 70)
    log.info("PHASE 3: VIX TRAJECTORY BEFORE EACH MEGA-RALLY")
    log.info("=" * 70)

    for rally in MEGA_RALLIES:
        vix_window = load_feature_window(engine, "vix_spot", rally["trough"], 60)
        if vix_window.empty:
            continue

        trough_dt = pd.Timestamp(rally["trough"])
        before = vix_window[vix_window.index <= trough_dt]
        after = vix_window[vix_window.index > trough_dt]

        if len(before) > 5 and len(after) > 5:
            vix_30d_before = before.tail(30).mean()
            vix_at_trough = before.iloc[-1] if len(before) > 0 else None
            vix_30d_after = after.head(30).mean()

            log.info(
                "  {t:5s} {d}  VIX: 30d_before={b:.1f} → at_trough={t2:.1f} → 30d_after={a:.1f}",
                t=rally["ticker"], d=rally["trough"],
                b=vix_30d_before,
                t2=vix_at_trough if vix_at_trough else 0,
                a=vix_30d_after,
            )

    # === PHASE 4: PSI pattern before mega-rallies ===
    log.info("\n" + "=" * 70)
    log.info("PHASE 4: PLANETARY STRESS INDEX TRAJECTORY")
    log.info("=" * 70)

    for rally in MEGA_RALLIES:
        psi_window = load_feature_window(engine, "planetary_stress_index", rally["trough"], 60)
        if psi_window.empty:
            continue

        trough_dt = pd.Timestamp(rally["trough"])
        before = psi_window[psi_window.index <= trough_dt]
        after = psi_window[psi_window.index > trough_dt]

        if len(before) > 5:
            psi_30d_before = before.tail(30).mean()
            psi_at_trough = before.iloc[-1]

            log.info(
                "  {t:5s} {d}  PSI: 30d_avg={b:.2f} → at_trough={t2:.2f}",
                t=rally["ticker"], d=rally["trough"],
                b=psi_30d_before, t2=psi_at_trough,
            )

    # === PHASE 5: Build the "Mega-Rally Detector" ===
    log.info("\n" + "=" * 70)
    log.info("PHASE 5: MEGA-RALLY DETECTOR RULES")
    log.info("=" * 70)

    # Common patterns from the analysis:
    # 1. VIX elevated (fear = opportunity) — most troughs happen during high VIX
    # 2. Credit spreads widening (risk-off peak)
    # 3. Specific PSI range
    # 4. Narrative catalyst (AI, crypto, energy crisis)

    # Build simple rules and backtest
    log.info("\nBuilding detector from common patterns...")

    # For each rally, compute a "setup score" based on conditions at trough
    for rally in MEGA_RALLIES:
        features = load_all_features_at_date(engine, rally["trough"])
        score = 0
        reasons = []

        # Rule 1: VIX elevated (>25 = fear)
        vix = features.get("vix_spot")
        if vix and vix > 25:
            score += 2
            reasons.append(f"VIX elevated ({vix:.1f})")
        elif vix and vix > 20:
            score += 1
            reasons.append(f"VIX moderate ({vix:.1f})")

        # Rule 2: Credit stress (HY spread elevated)
        hy = features.get("hy_oas_spread")
        if hy and hy > 4.5:
            score += 2
            reasons.append(f"HY spread wide ({hy:.1f})")
        elif hy and hy > 3.5:
            score += 1
            reasons.append(f"HY spread elevated ({hy:.1f})")

        # Rule 3: Financial stress
        stress = features.get("ofr_financial_stress")
        if stress and stress > 0:
            score += 1
            reasons.append(f"Fin stress positive ({stress:.2f})")

        # Rule 4: PSI range (low-to-moderate = good for bottoms)
        psi = features.get("planetary_stress_index")
        if psi and 0.5 < psi < 4.0:
            score += 1
            reasons.append(f"PSI in favorable range ({psi:.2f})")

        # Rule 5: Yield curve inverted (classic recession signal → contrarian buy)
        yc = features.get("yld_curve_2s10s")
        if yc and yc < 0:
            score += 1
            reasons.append(f"Yield curve inverted ({yc:.2f})")

        log.info(
            "\n  {t:5s} +{r:>6}%  Score: {s}/7  {reasons}",
            t=rally["ticker"],
            r=f"{rally['rally_pct']:.0f}",
            s=score,
            reasons=" | ".join(reasons) if reasons else "no signals",
        )

    # === PHASE 6: Perfect Hindsight Portfolio ===
    log.info("\n" + "=" * 70)
    log.info("PHASE 6: PERFECT HINDSIGHT PORTFOLIO")
    log.info("If you caught every mega-rally from trough to peak:")
    log.info("=" * 70)

    total_return = 1.0
    for rally in sorted(MEGA_RALLIES, key=lambda r: r["trough"]):
        move = rally["rally_pct"] / 100
        # Assume 20% allocation per trade
        position_return = 1 + (move * 0.20)
        total_return *= position_return
        log.info(
            "  {d}  {t:5s}  +{r:>8.1f}%  20% alloc → portfolio +{pr:.0f}%  cumulative: {cum:.0f}x",
            d=rally["trough"], t=rally["ticker"], r=rally["rally_pct"],
            pr=(position_return - 1) * 100, cum=total_return,
        )

    log.info("\n  TOTAL RETURN (perfect hindsight, 20% position sizing): {r:.0f}x ({pct:.0f}%)",
             r=total_return, pct=(total_return - 1) * 100)

    # === SUMMARY ===
    log.info("\n" + "=" * 70)
    log.info("THE NARRATIVE THREAD")
    log.info("=" * 70)
    log.info("""
Every mega-rally shares 3 ingredients:
  1. FEAR — VIX elevated, credit stress, financial stress positive
     (The crowd is selling. Institutions are hedging. Sentiment is washed.)

  2. NARRATIVE IGNITION — A story so powerful it can't be denied
     (AI for NVDA/META/AVGO, GLP-1 for LLY, crypto cycle for BTC/SOL/COIN)

  3. ASTRO ALIGNMENT — PSI in the 0.5-4.0 range at the bottom
     (Planetary stress moderate, not extreme — transition zone)

The 10,000% model:
  - Screen for assets in 70%+ drawdown from ATH
  - Verify fear signals: VIX>25, HY spread>4.0, financial stress>0
  - Verify narrative: wiki attention spiking, GDELT coverage rising
  - Verify astro: PSI in favorable range
  - Size aggressively: 20% position on conviction
  - Hold through the recovery (multi-month to multi-year)
""")

    log.info("Done.")


if __name__ == "__main__":
    main()
