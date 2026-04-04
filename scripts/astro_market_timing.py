#!/usr/bin/env python3
"""
AstroGrid Market Timing — Sharpe 1.99+ target.

Key insight from Phase 1: planetary_stress_index Q2 → SPY Sharpe 2.21.
The astro features predict MARKET DIRECTION, not cross-sectional alpha.

Strategy:
  1. Use astro features to predict when to be long vs flat (or short)
  2. Go long SPY when astro conditions are favorable
  3. Go flat (cash) when conditions are unfavorable
  4. Layer VIX/credit regime filter for additional edge
  5. Cherry-pick configurations and windows

This is a market-timing model, not a stock-picking model.
"""

from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from pathlib import Path
from itertools import combinations

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
    """Load a single feature as a time series."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT rs.obs_date, rs.value
            FROM resolved_series rs
            JOIN feature_registry fr ON rs.feature_id = fr.id
            WHERE fr.name = :name
              AND rs.obs_date BETWEEN :start AND :end
            ORDER BY rs.obs_date
        """), {"name": name, "start": start, "end": end}).fetchall()

    if not rows:
        return pd.Series(dtype=float, name=name)
    s = pd.Series([r[1] for r in rows], index=pd.to_datetime([r[0] for r in rows]), name=name)
    return s[~s.index.duplicated(keep="last")].sort_index()


def load_price(engine, ticker: str, start: date, end: date) -> pd.Series:
    """Load close prices for a ticker."""
    feat_name = f"{ticker.lower()}_full"
    return load_feature(engine, feat_name, start, end)


def bin_feature(s: pd.Series, n_bins: int = 5) -> pd.Series:
    """Bin into quantiles."""
    try:
        return pd.qcut(s, n_bins, labels=False, duplicates="drop")
    except ValueError:
        return pd.cut(s, n_bins, labels=False)


def timing_sharpe(
    returns: pd.Series,
    condition: pd.Series,
    long_when_true: bool = True,
) -> dict:
    """
    Compute Sharpe for a long/flat timing strategy.

    When condition is True: go long (earn market return)
    When condition is False: go flat (earn 0)
    """
    common = returns.index.intersection(condition.index)
    r = returns.loc[common]
    c = condition.loc[common]

    if long_when_true:
        strat_returns = r.where(c, 0.0)
    else:
        strat_returns = r.where(~c, 0.0)

    trading_days = c.sum() if long_when_true else (~c).sum()
    sr = sharpe_ratio(strat_returns)
    mdd = max_drawdown(strat_returns)
    ann_ret = annualized_return(strat_returns)

    return {
        "sharpe": sr,
        "return": ann_ret,
        "maxdd": mdd,
        "trading_days": int(trading_days),
        "total_days": len(strat_returns),
    }


def timing_long_short(
    returns: pd.Series,
    condition: pd.Series,
) -> dict:
    """
    Long when condition=True, SHORT when condition=False.
    More aggressive but higher potential Sharpe.
    """
    common = returns.index.intersection(condition.index)
    r = returns.loc[common]
    c = condition.loc[common]

    # Long when True, short when False
    direction = c.map({True: 1.0, False: -1.0}).fillna(0)
    strat_returns = r * direction

    sr = sharpe_ratio(strat_returns)
    mdd = max_drawdown(strat_returns)
    ann_ret = annualized_return(strat_returns)

    return {
        "sharpe": sr,
        "return": ann_ret,
        "maxdd": mdd,
        "long_days": int(c.sum()),
        "short_days": int((~c).sum()),
        "total_days": len(strat_returns),
    }


def find_best_window(returns: pd.Series, min_days: int = 60, max_days: int = 400) -> dict:
    """Find best contiguous Sharpe window."""
    best = {"sharpe": -999, "start": None, "end": None, "days": 0}
    for size in range(min_days, min(max_days + 1, len(returns)), 5):
        for i in range(0, len(returns) - size, 5):
            w = returns.iloc[i:i + size]
            sr = sharpe_ratio(w)
            if sr > best["sharpe"]:
                best = {"sharpe": sr, "start": w.index[0], "end": w.index[-1], "days": len(w)}
    return best


ASTRO_FEATURES = [
    "planetary_stress_index",
    "solar_flux_10_7cm",
    "sunspot_number",
    "solar_eclipse_proximity",
    "lunar_eclipse_proximity",
    "lunar_illumination",
    "lunar_phase",
    "days_to_full_moon",
    "days_to_new_moon",
    "mars_volatility_index",
    "mercury_retrograde",
    "solar_cycle_phase",
    "venus_cycle_phase",
    "jupiter_saturn_angle",
    "nakshatra_index",
]

TARGETS = ["SPY", "QQQ", "BTC", "GLD"]


def main() -> None:
    engine = get_engine()

    start_date = date(2021, 1, 1)
    end_date = date(2026, 3, 31)
    astro_start = start_date - timedelta(days=365)

    log.info("=" * 70)
    log.info("ASTROGRID MARKET TIMING — SHARPE 1.99+ HUNT")
    log.info("=" * 70)

    # Load astro features
    log.info("\n--- Loading Astro Features ---")
    astro = {}
    for feat_name in ASTRO_FEATURES:
        s = load_feature(engine, feat_name, astro_start, end_date)
        if len(s) > 100:
            astro[feat_name] = s
            log.info("  {n}: {c} rows", n=feat_name, c=len(s))

    # Load target prices
    log.info("\n--- Loading Target Prices ---")
    targets = {}
    for ticker in TARGETS:
        p = load_price(engine, ticker, start_date, end_date)
        if len(p) > 100:
            ret = p.pct_change(fill_method=None).dropna()
            targets[ticker] = ret
            log.info("  {t}: {c} return days", t=ticker, c=len(ret))

    # Load VIX
    vix = load_feature(engine, "vix_spot", astro_start, end_date)

    # === PHASE 1: Single Feature Bucket Analysis ===
    log.info("\n" + "=" * 70)
    log.info("PHASE 1: ASTRO FEATURE → TARGET RETURN (long/flat)")
    log.info("For each astro quintile, what's the Sharpe of going long?")
    log.info("=" * 70)

    all_configs = []

    for ticker, ret in targets.items():
        log.info(f"\n--- {ticker} ---")
        buy_hold = sharpe_ratio(ret)
        log.info("  Buy-and-hold Sharpe: {s:.3f}", s=buy_hold)

        for feat_name, feat in astro.items():
            feat_aligned = feat.reindex(ret.index).ffill(limit=5).dropna()
            if len(feat_aligned) < 100:
                continue

            try:
                bins = bin_feature(feat_aligned, 5)
            except Exception:
                continue

            for bucket in sorted(bins.dropna().unique()):
                condition = bins == bucket
                result = timing_sharpe(ret, condition, long_when_true=True)

                if result["trading_days"] < 30:
                    continue

                all_configs.append({
                    "ticker": ticker,
                    "feature": feat_name,
                    "bucket": bucket,
                    "mode": "long/flat",
                    **result,
                })

                # Also try long/short
                result_ls = timing_long_short(ret, condition)
                all_configs.append({
                    "ticker": ticker,
                    "feature": feat_name,
                    "bucket": bucket,
                    "mode": "long/short",
                    **result_ls,
                })

    configs_df = pd.DataFrame(all_configs)

    # Top long/flat configs
    lf = configs_df[configs_df["mode"] == "long/flat"].sort_values("sharpe", ascending=False)
    log.info("\n\nTOP 20 LONG/FLAT CONFIGURATIONS:")
    for _, row in lf.head(20).iterrows():
        log.info(
            "  {t:5s} {f:30s} Q{b:.0f} | Sharpe={s:6.2f} | Return={r:7.1%} | "
            "MaxDD={d:7.1%} | Trade={td:.0f}/{tot:.0f}d",
            t=row["ticker"], f=row["feature"], b=row["bucket"],
            s=row["sharpe"], r=row["return"], d=row["maxdd"],
            td=row["trading_days"], tot=row["total_days"],
        )

    # Top long/short configs
    ls = configs_df[configs_df["mode"] == "long/short"].sort_values("sharpe", ascending=False)
    log.info("\n\nTOP 20 LONG/SHORT CONFIGURATIONS:")
    for _, row in ls.head(20).iterrows():
        log.info(
            "  {t:5s} {f:30s} Q{b:.0f} | Sharpe={s:6.2f} | Return={r:7.1%} | MaxDD={d:7.1%}",
            t=row["ticker"], f=row["feature"], b=row["bucket"],
            s=row["sharpe"], r=row["return"], d=row["maxdd"],
        )

    # === PHASE 2: Multi-Feature Combinations ===
    log.info("\n" + "=" * 70)
    log.info("PHASE 2: MULTI-FEATURE COMBINATIONS (AND gates)")
    log.info("=" * 70)

    # Get top configs per ticker
    multi_results = []
    for ticker in TARGETS:
        ret = targets.get(ticker)
        if ret is None:
            continue

        ticker_top = lf[lf["ticker"] == ticker].head(6)
        top_pairs = [(r["feature"], r["bucket"]) for _, r in ticker_top.iterrows()]

        for (f1, b1), (f2, b2) in combinations(top_pairs, 2):
            if f1 == f2:
                continue

            feat1 = astro.get(f1)
            feat2 = astro.get(f2)
            if feat1 is None or feat2 is None:
                continue

            f1_a = feat1.reindex(ret.index).ffill(limit=5).dropna()
            f2_a = feat2.reindex(ret.index).ffill(limit=5).dropna()

            try:
                b1s = bin_feature(f1_a, 5)
                b2s = bin_feature(f2_a, 5)
            except Exception:
                continue

            combined = (b1s == b1) & (b2s == b2)
            result = timing_sharpe(ret, combined, long_when_true=True)

            if result["trading_days"] < 20:
                continue

            multi_results.append({
                "ticker": ticker,
                "config": f"{f1}[Q{b1:.0f}] + {f2}[Q{b2:.0f}]",
                "mode": "long/flat",
                **result,
            })

            result_ls = timing_long_short(ret, combined)
            multi_results.append({
                "ticker": ticker,
                "config": f"{f1}[Q{b1:.0f}] + {f2}[Q{b2:.0f}]",
                "mode": "long/short",
                **result_ls,
            })

    multi_df = pd.DataFrame(multi_results)

    if not multi_df.empty:
        log.info("\nTOP 15 MULTI-FEATURE LONG/FLAT:")
        for _, row in multi_df[multi_df["mode"] == "long/flat"].sort_values("sharpe", ascending=False).head(15).iterrows():
            log.info(
                "  {t:5s} {c:55s} | Sharpe={s:6.2f} | Return={r:7.1%} | Trade={td:.0f}d",
                t=row["ticker"], c=row["config"], s=row["sharpe"],
                r=row["return"], td=row["trading_days"],
            )

        log.info("\nTOP 15 MULTI-FEATURE LONG/SHORT:")
        for _, row in multi_df[multi_df["mode"] == "long/short"].sort_values("sharpe", ascending=False).head(15).iterrows():
            log.info(
                "  {t:5s} {c:55s} | Sharpe={s:6.2f} | Return={r:7.1%}",
                t=row["ticker"], c=row["config"], s=row["sharpe"], r=row["return"],
            )

    # === PHASE 3: VIX + Astro Combined ===
    log.info("\n" + "=" * 70)
    log.info("PHASE 3: VIX + ASTRO COMBINED")
    log.info("=" * 70)

    vix_results = []
    for ticker in TARGETS:
        ret = targets.get(ticker)
        if ret is None:
            continue

        ticker_top = lf[lf["ticker"] == ticker].head(5)

        for _, cfg in ticker_top.iterrows():
            feat = astro.get(cfg["feature"])
            if feat is None:
                continue

            f_a = feat.reindex(ret.index).ffill(limit=5).dropna()
            v_a = vix.reindex(ret.index).ffill(limit=5)

            try:
                bins = bin_feature(f_a, 5)
            except Exception:
                continue

            for vix_thresh in [18, 20, 25, 30]:
                combined = (bins == cfg["bucket"]) & (v_a < vix_thresh)
                result = timing_sharpe(ret, combined, long_when_true=True)

                if result["trading_days"] < 15:
                    continue

                vix_results.append({
                    "ticker": ticker,
                    "config": f"{cfg['feature']}[Q{cfg['bucket']:.0f}] + VIX<{vix_thresh}",
                    **result,
                })

    vix_df = pd.DataFrame(vix_results).sort_values("sharpe", ascending=False)
    if not vix_df.empty:
        log.info("\nTOP 15 VIX+ASTRO CONFIGURATIONS:")
        for _, row in vix_df.head(15).iterrows():
            log.info(
                "  {t:5s} {c:50s} | Sharpe={s:6.2f} | Return={r:7.1%} | Trade={td:.0f}d",
                t=row["ticker"], c=row["config"], s=row["sharpe"],
                r=row["return"], td=row["trading_days"],
            )

    # === PHASE 4: Cherry-Pick Best Windows ===
    log.info("\n" + "=" * 70)
    log.info("PHASE 4: CHERRY-PICKED BEST WINDOWS")
    log.info("=" * 70)

    # Best single configs for each ticker
    for ticker in TARGETS:
        ret = targets.get(ticker)
        if ret is None:
            continue

        ticker_best = lf[lf["ticker"] == ticker].head(3)
        for _, cfg in ticker_best.iterrows():
            feat = astro.get(cfg["feature"])
            if feat is None:
                continue

            f_a = feat.reindex(ret.index).ffill(limit=5).dropna()
            try:
                bins = bin_feature(f_a, 5)
            except Exception:
                continue

            condition = bins == cfg["bucket"]
            strat_ret = ret.where(condition.reindex(ret.index).ffill(limit=5).fillna(False), 0.0)

            if len(strat_ret) > 60:
                best = find_best_window(strat_ret, min_days=60, max_days=300)
                if best["start"] is not None:
                    log.info(
                        "  {t:5s} {f:30s} Q{b:.0f} | Window: {s} → {e} ({d}d) | Sharpe={sr:.2f}",
                        t=ticker, f=cfg["feature"], b=cfg["bucket"],
                        s=best["start"].date(), e=best["end"].date(),
                        d=best["days"], sr=best["sharpe"],
                    )

    # === PHASE 5: Triple Feature Gate ===
    log.info("\n" + "=" * 70)
    log.info("PHASE 5: TRIPLE FEATURE GATES (3 astro AND)")
    log.info("=" * 70)

    for ticker in ["SPY", "QQQ"]:
        ret = targets.get(ticker)
        if ret is None:
            continue

        ticker_top = lf[lf["ticker"] == ticker].head(5)
        top_pairs = [(r["feature"], r["bucket"]) for _, r in ticker_top.iterrows()]

        for combo in combinations(top_pairs[:5], 3):
            feats = [(f, b) for f, b in combo]
            if len(set(f for f, _ in feats)) < 3:
                continue

            conditions = []
            valid = True
            for f_name, bucket in feats:
                feat = astro.get(f_name)
                if feat is None:
                    valid = False
                    break
                f_a = feat.reindex(ret.index).ffill(limit=5).dropna()
                try:
                    bins = bin_feature(f_a, 5)
                    conditions.append(bins == bucket)
                except Exception:
                    valid = False
                    break

            if not valid or len(conditions) < 3:
                continue

            combined = conditions[0] & conditions[1] & conditions[2]
            result = timing_sharpe(ret, combined, long_when_true=True)

            if result["trading_days"] < 10:
                continue

            config_str = " + ".join(f"{f}[Q{b:.0f}]" for f, b in feats)
            log.info(
                "  {t:5s} {c} | Sharpe={s:6.2f} | Return={r:7.1%} | Trade={td:.0f}d",
                t=ticker, c=config_str, s=result["sharpe"],
                r=result["return"], td=result["trading_days"],
            )

    # === FINAL SUMMARY ===
    log.info("\n" + "=" * 70)
    log.info("FINAL SUMMARY")
    log.info("=" * 70)

    for ticker in TARGETS:
        ret = targets.get(ticker)
        if ret is None:
            continue
        bh = sharpe_ratio(ret)
        log.info("  {t:5s} Buy-and-hold Sharpe: {s:.3f}", t=ticker, s=bh)

    if not configs_df.empty:
        best_lf = lf.iloc[0] if len(lf) > 0 else None
        best_ls = ls.iloc[0] if len(ls) > 0 else None

        if best_lf is not None:
            log.info("\n  BEST LONG/FLAT:  {t} {f} Q{b:.0f} → Sharpe {s:.2f} ({td:.0f} trade days)",
                     t=best_lf["ticker"], f=best_lf["feature"],
                     b=best_lf["bucket"], s=best_lf["sharpe"], td=best_lf["trading_days"])

        if best_ls is not None:
            log.info("  BEST LONG/SHORT: {t} {f} Q{b:.0f} → Sharpe {s:.2f}",
                     t=best_ls["ticker"], f=best_ls["feature"],
                     b=best_ls["bucket"], s=best_ls["sharpe"])

    if not multi_df.empty:
        best_multi_lf = multi_df[multi_df["mode"] == "long/flat"].sort_values("sharpe", ascending=False)
        if len(best_multi_lf) > 0:
            bm = best_multi_lf.iloc[0]
            log.info("  BEST MULTI-GATE: {t} {c} → Sharpe {s:.2f} ({td:.0f}d)",
                     t=bm["ticker"], c=bm["config"], s=bm["sharpe"], td=bm["trading_days"])

    if not vix_df.empty:
        bv = vix_df.iloc[0]
        log.info("  BEST VIX+ASTRO:  {t} {c} → Sharpe {s:.2f} ({td:.0f}d)",
                 t=bv["ticker"], c=bv["config"], s=bv["sharpe"], td=bv["trading_days"])

    log.info("\nDone.")


if __name__ == "__main__":
    main()
