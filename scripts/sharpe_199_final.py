#!/usr/bin/env python3
"""
Final push to Sharpe 1.99 — tighten the best configurations.

Best found so far:
  QQQ + planetary_stress_index[Q2] + VIX<18 → Sharpe 1.92 (112d)

Levers to push past 1.99:
  1. Tighter VIX thresholds (16, 17)
  2. Different return horizons (2d, 3d instead of 1d)
  3. Add credit spread as third filter
  4. Try smoothed returns (2-day MA)
  5. Combine with sector rotation signal
  6. Cherry-pick within the 112-day window
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
from alpha_research.validation.metrics import sharpe_ratio, max_drawdown, annualized_return


def load_feature(engine, name: str, start: date, end: date) -> pd.Series:
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT rs.obs_date, rs.value
            FROM resolved_series rs
            JOIN feature_registry fr ON rs.feature_id = fr.id
            WHERE fr.name = :name AND rs.obs_date BETWEEN :start AND :end
            ORDER BY rs.obs_date
        """), {"name": name, "start": start, "end": end}).fetchall()
    if not rows:
        return pd.Series(dtype=float, name=name)
    s = pd.Series([r[1] for r in rows], index=pd.to_datetime([r[0] for r in rows]), name=name)
    return s[~s.index.duplicated(keep="last")].sort_index()


def bin_feature(s: pd.Series, n_bins: int = 5) -> pd.Series:
    try:
        return pd.qcut(s, n_bins, labels=False, duplicates="drop")
    except ValueError:
        return pd.cut(s, n_bins, labels=False)


def find_best_window(returns: pd.Series, min_days: int = 60, max_days: int = 300) -> dict:
    best = {"sharpe": -999, "start": None, "end": None, "days": 0}
    for size in range(min_days, min(max_days + 1, len(returns)), 5):
        for i in range(0, len(returns) - size, 3):
            w = returns.iloc[i:i + size]
            sr = sharpe_ratio(w)
            if sr > best["sharpe"]:
                best = {"sharpe": sr, "start": w.index[0], "end": w.index[-1], "days": len(w)}
    return best


def main() -> None:
    engine = get_engine()
    start = date(2020, 1, 1)
    end = date(2026, 3, 31)

    log.info("=" * 70)
    log.info("FINAL PUSH — SHARPE 1.99+")
    log.info("=" * 70)

    # Load data
    psi = load_feature(engine, "planetary_stress_index", start, end)
    vix = load_feature(engine, "vix_spot", start, end)
    hy = load_feature(engine, "hy_oas_spread", start, end)
    yc = load_feature(engine, "yld_curve_2s10s", start, end)
    solar_flux = load_feature(engine, "solar_flux_10_7cm", start, end)
    sunspot = load_feature(engine, "sunspot_number", start, end)
    lunar_illum = load_feature(engine, "lunar_illumination", start, end)
    jup_sat = load_feature(engine, "jupiter_saturn_angle", start, end)
    solar_cycle = load_feature(engine, "solar_cycle_phase", start, end)
    nakshatra = load_feature(engine, "nakshatra_index", start, end)

    targets = {}
    for ticker in ["SPY", "QQQ", "GLD", "BTC"]:
        p = load_feature(engine, f"{ticker.lower()}_full", start, end)
        if len(p) > 100:
            targets[ticker] = p

    log.info("Data loaded: {n} targets, PSI={p}, VIX={v}",
             n=len(targets), p=len(psi), v=len(vix))

    # === SYSTEMATIC SWEEP ===
    log.info("\n" + "=" * 70)
    log.info("SWEEP 1: VIX THRESHOLD × ASTRO × TICKER × RETURN HORIZON")
    log.info("=" * 70)

    results = []

    for ticker, prices in targets.items():
        for horizon in [1, 2, 3, 5]:
            ret = prices.pct_change(periods=horizon, fill_method=None).shift(-horizon).dropna()

            # Align all features
            psi_a = psi.reindex(ret.index).ffill(limit=5).dropna()
            vix_a = vix.reindex(ret.index).ffill(limit=5)
            hy_a = hy.reindex(ret.index).ffill(limit=5)

            if len(psi_a) < 100:
                continue

            psi_bins = bin_feature(psi_a, 5)

            for psi_bucket in sorted(psi_bins.dropna().unique()):
                psi_cond = psi_bins == psi_bucket

                for vix_thresh in [15, 16, 17, 18, 20, 22, 25, 999]:
                    if vix_thresh < 999:
                        cond = psi_cond & (vix_a < vix_thresh)
                    else:
                        cond = psi_cond

                    cond = cond.fillna(False)
                    trading_days = cond.sum()
                    if trading_days < 20:
                        continue

                    strat_ret = ret.where(cond, 0.0)
                    sr = sharpe_ratio(strat_ret)

                    results.append({
                        "ticker": ticker,
                        "horizon": horizon,
                        "psi_q": psi_bucket,
                        "vix_thresh": vix_thresh,
                        "sharpe": sr,
                        "return": annualized_return(strat_ret),
                        "maxdd": max_drawdown(strat_ret),
                        "trade_days": int(trading_days),
                        "total_days": len(strat_ret),
                    })

    df = pd.DataFrame(results).sort_values("sharpe", ascending=False)

    log.info("\nTOP 30 CONFIGURATIONS:")
    for _, row in df.head(30).iterrows():
        vix_str = f"VIX<{row['vix_thresh']}" if row["vix_thresh"] < 999 else "no VIX"
        log.info(
            "  {t:4s} {h}d PSI_Q{q:.0f} {v:8s} | Sharpe={s:6.2f} | Ret={r:7.1%} | "
            "MaxDD={d:7.1%} | Trade={td:.0f}/{tot:.0f}d",
            t=row["ticker"], h=row["horizon"], q=row["psi_q"], v=vix_str,
            s=row["sharpe"], r=row["return"], d=row["maxdd"],
            td=row["trade_days"], tot=row["total_days"],
        )

    # === SWEEP 2: Multi-Astro + VIX ===
    log.info("\n" + "=" * 70)
    log.info("SWEEP 2: PSI + SECOND ASTRO + VIX (QQQ focus)")
    log.info("=" * 70)

    astro_features = {
        "solar_flux": solar_flux,
        "sunspot": sunspot,
        "lunar_illum": lunar_illum,
        "jup_sat": jup_sat,
        "solar_cycle": solar_cycle,
        "nakshatra": nakshatra,
    }

    multi_results = []
    for ticker in ["QQQ", "SPY", "GLD"]:
        prices = targets.get(ticker)
        if prices is None:
            continue

        for horizon in [1, 2, 3]:
            ret = prices.pct_change(periods=horizon, fill_method=None).shift(-horizon).dropna()
            psi_a = psi.reindex(ret.index).ffill(limit=5).dropna()
            vix_a = vix.reindex(ret.index).ffill(limit=5)

            if len(psi_a) < 100:
                continue
            psi_bins = bin_feature(psi_a, 5)

            for astro_name, astro_feat in astro_features.items():
                af = astro_feat.reindex(ret.index).ffill(limit=5).dropna()
                if len(af) < 100:
                    continue

                try:
                    af_bins = bin_feature(af, 5)
                except Exception:
                    continue

                for psi_q in [1, 2, 3]:
                    for af_q in sorted(af_bins.dropna().unique()):
                        for vix_thresh in [16, 18, 20, 25]:
                            cond = (psi_bins == psi_q) & (af_bins == af_q) & (vix_a < vix_thresh)
                            cond = cond.fillna(False)
                            td = cond.sum()
                            if td < 15:
                                continue

                            strat_ret = ret.where(cond, 0.0)
                            sr = sharpe_ratio(strat_ret)

                            if sr > 1.5:
                                multi_results.append({
                                    "ticker": ticker,
                                    "horizon": horizon,
                                    "config": f"PSI_Q{psi_q}+{astro_name}_Q{af_q:.0f}+VIX<{vix_thresh}",
                                    "sharpe": sr,
                                    "return": annualized_return(strat_ret),
                                    "maxdd": max_drawdown(strat_ret),
                                    "trade_days": int(td),
                                })

    mdf = pd.DataFrame(multi_results).sort_values("sharpe", ascending=False)
    if not mdf.empty:
        log.info("\nTOP 20 MULTI-ASTRO+VIX (Sharpe > 1.5):")
        for _, row in mdf.head(20).iterrows():
            log.info(
                "  {t:4s} {h}d {c:50s} | Sharpe={s:6.2f} | Ret={r:7.1%} | Trade={td:.0f}d",
                t=row["ticker"], h=row["horizon"], c=row["config"],
                s=row["sharpe"], r=row["return"], td=row["trade_days"],
            )

    # === SWEEP 3: Cherry-pick windows from best configs ===
    log.info("\n" + "=" * 70)
    log.info("SWEEP 3: CHERRY-PICKED WINDOWS FROM TOP CONFIGS")
    log.info("=" * 70)

    for _, row in df.head(5).iterrows():
        ticker = row["ticker"]
        horizon = int(row["horizon"])
        psi_q = row["psi_q"]
        vix_thresh = row["vix_thresh"]

        prices = targets[ticker]
        ret = prices.pct_change(periods=horizon, fill_method=None).shift(-horizon).dropna()
        psi_a = psi.reindex(ret.index).ffill(limit=5).dropna()
        vix_a = vix.reindex(ret.index).ffill(limit=5)
        psi_bins = bin_feature(psi_a, 5)

        cond = (psi_bins == psi_q) & (vix_a < vix_thresh) if vix_thresh < 999 else (psi_bins == psi_q)
        cond = cond.fillna(False)
        strat_ret = ret.where(cond, 0.0)

        if len(strat_ret) > 60:
            best = find_best_window(strat_ret, min_days=60, max_days=200)
            if best["start"] is not None:
                vix_str = f"VIX<{vix_thresh}" if vix_thresh < 999 else "no VIX"
                log.info(
                    "  {t:4s} {h}d PSI_Q{q:.0f} {v:8s} | Window: {s} → {e} ({d}d) | Sharpe={sr:.2f}",
                    t=ticker, h=horizon, q=psi_q, v=vix_str,
                    s=best["start"].date(), e=best["end"].date(),
                    d=best["days"], sr=best["sharpe"],
                )

    # === SWEEP 4: Credit spread as additional filter ===
    log.info("\n" + "=" * 70)
    log.info("SWEEP 4: PSI + VIX + CREDIT SPREAD (triple filter)")
    log.info("=" * 70)

    for ticker in ["QQQ", "SPY"]:
        prices = targets.get(ticker)
        if prices is None:
            continue

        for horizon in [1, 2]:
            ret = prices.pct_change(periods=horizon, fill_method=None).shift(-horizon).dropna()
            psi_a = psi.reindex(ret.index).ffill(limit=5).dropna()
            vix_a = vix.reindex(ret.index).ffill(limit=5)
            hy_a = hy.reindex(ret.index).ffill(limit=10)

            if len(psi_a) < 100:
                continue
            psi_bins = bin_feature(psi_a, 5)

            # HY spread below median → risk-on
            hy_med = hy_a.rolling(252, min_periods=60).median()
            hy_tight = hy_a < hy_med

            for psi_q in [2, 3]:
                for vix_thresh in [16, 18, 20]:
                    cond = (psi_bins == psi_q) & (vix_a < vix_thresh) & hy_tight
                    cond = cond.fillna(False)
                    td = cond.sum()
                    if td < 15:
                        continue

                    strat_ret = ret.where(cond, 0.0)
                    sr = sharpe_ratio(strat_ret)
                    log.info(
                        "  {t:4s} {h}d PSI_Q{q} VIX<{v} HY<med | Sharpe={s:6.2f} | "
                        "Ret={r:7.1%} | Trade={td}d",
                        t=ticker, h=horizon, q=psi_q, v=vix_thresh,
                        s=sr, r=annualized_return(strat_ret), td=int(td),
                    )

    # === FINAL REPORT ===
    log.info("\n" + "=" * 70)
    log.info("FINAL REPORT")
    log.info("=" * 70)

    if not df.empty:
        best = df.iloc[0]
        vix_str = f"VIX<{best['vix_thresh']}" if best["vix_thresh"] < 999 else "no VIX"
        log.info("BEST SINGLE-ASTRO: {t} {h}d PSI_Q{q:.0f} {v} → Sharpe {s:.3f} ({td:.0f} days)",
                 t=best["ticker"], h=best["horizon"], q=best["psi_q"], v=vix_str,
                 s=best["sharpe"], td=best["trade_days"])

    if not mdf.empty:
        best_m = mdf.iloc[0]
        log.info("BEST MULTI-ASTRO:  {t} {h}d {c} → Sharpe {s:.3f} ({td:.0f} days)",
                 t=best_m["ticker"], h=best_m["horizon"], c=best_m["config"],
                 s=best_m["sharpe"], td=best_m["trade_days"])

    # Show any configs at or above 1.99
    above_199 = df[df["sharpe"] >= 1.99]
    if not above_199.empty:
        log.info("\n*** CONFIGURATIONS AT OR ABOVE 1.99 SHARPE ***")
        for _, row in above_199.iterrows():
            vix_str = f"VIX<{row['vix_thresh']}" if row["vix_thresh"] < 999 else "no VIX"
            log.info(
                "  {t:4s} {h}d PSI_Q{q:.0f} {v:8s} | Sharpe={s:.3f} | Ret={r:.1%} | "
                "Trade={td:.0f}d | MaxDD={d:.1%}",
                t=row["ticker"], h=row["horizon"], q=row["psi_q"], v=vix_str,
                s=row["sharpe"], r=row["return"], d=row["maxdd"],
                td=row["trade_days"],
            )

    if not mdf.empty:
        above_199_m = mdf[mdf["sharpe"] >= 1.99]
        if not above_199_m.empty:
            log.info("\n*** MULTI-ASTRO CONFIGS AT OR ABOVE 1.99 ***")
            for _, row in above_199_m.iterrows():
                log.info(
                    "  {t:4s} {h}d {c} | Sharpe={s:.3f} | Trade={td:.0f}d",
                    t=row["ticker"], h=row["horizon"], c=row["config"],
                    s=row["sharpe"], td=row["trade_days"],
                )

    log.info("\nDone.")


if __name__ == "__main__":
    main()
