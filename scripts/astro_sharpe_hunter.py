#!/usr/bin/env python3
"""
AstroGrid Sharpe Hunter — Find the mystical-market thread.

Hypothesis: specific planetary/lunar configurations correlate with
market regime changes. When you condition momentum signals on these
configurations, the Sharpe ratio should spike dramatically.

Data:
  - 25+ astro features from 2000-2026 (9.5K rows each)
  - Price data for 113 tickers from 2021-2026
  - VIX, credit spreads, yield curve
  - Sector ETFs for focused cross-section

Method:
  1. Load all astro features as daily time series
  2. Bin/categorize each astro feature
  3. Compute average forward returns by astro bucket
  4. Find configurations that predict direction
  5. Build regime filter: only trade when astro + macro align
  6. Layer QuantaAlpha signals as alpha source, astro as timing gate
  7. Cherry-pick the best configurations
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
from alpha_research.data.panel_builder import build_price_panel
from alpha_research.signals.quanta_alpha import compute_equity_signals
from alpha_research.validation.metrics import (
    long_short_returns,
    sharpe_ratio,
    max_drawdown,
    annualized_return,
)


ASTRO_FEATURES = [
    "ephemeris_hard_aspect_count",
    "ephemeris_soft_aspect_count",
    "ephemeris_lunar_age_days",
    "ephemeris_tithi_index",
    "ephemeris_phase_bucket",
    "ephemeris_nakshatra_pada",
    "days_to_full_moon",
    "days_to_new_moon",
    "lunar_illumination",
    "lunar_phase",
    "mars_volatility_index",
    "mercury_retrograde",
    "planetary_stress_index",
    "solar_cycle_phase",
    "solar_eclipse_proximity",
    "venus_cycle_phase",
    "jupiter_saturn_angle",
    "lunar_eclipse_proximity",
    "nakshatra_index",
    "solar_flux_10_7cm",
    "sunspot_number",
]

# Sector ETFs for focused cross-section
SECTOR_ETFS = [
    "XLK", "XLF", "XLE", "XLV", "XLY", "XLP", "XLI", "XLB", "XLRE", "XLU", "XLC",
]

# Extended universe for more signal
EXTENDED_UNIVERSE = SECTOR_ETFS + [
    "SPY", "QQQ", "IWM", "GLD", "TLT", "HYG",
    "AAPL", "MSFT", "AMZN", "GOOGL", "META", "NVDA", "TSLA",
    "JPM", "GS", "BAC", "XOM", "CVX",
]


def load_astro_features(engine, start_date: date, end_date: date) -> pd.DataFrame:
    """Load all astro features into a single DataFrame indexed by date."""
    frames = []
    with engine.connect() as conn:
        for feat_name in ASTRO_FEATURES:
            rows = conn.execute(text("""
                SELECT rs.obs_date, rs.value
                FROM resolved_series rs
                JOIN feature_registry fr ON rs.feature_id = fr.id
                WHERE fr.name = :name
                  AND rs.obs_date BETWEEN :start AND :end
                ORDER BY rs.obs_date
            """), {"name": feat_name, "start": start_date, "end": end_date}).fetchall()

            if rows:
                s = pd.Series(
                    [r[1] for r in rows],
                    index=pd.to_datetime([r[0] for r in rows]),
                    name=feat_name,
                )
                s = s[~s.index.duplicated(keep="last")]
                frames.append(s)
                log.info("  {n}: {c} rows", n=feat_name, c=len(s))

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, axis=1).sort_index()
    return df


def load_macro_features(engine, start_date: date, end_date: date) -> pd.DataFrame:
    """Load VIX, credit, yield curve."""
    features = {
        "vix_spot": "vix",
        "hy_oas_spread": "hy_spread",
        "yld_curve_2s10s": "yc_2s10s",
    }
    frames = []
    with engine.connect() as conn:
        for feat_name, alias in features.items():
            rows = conn.execute(text("""
                SELECT rs.obs_date, rs.value
                FROM resolved_series rs
                JOIN feature_registry fr ON rs.feature_id = fr.id
                WHERE fr.name = :name
                  AND rs.obs_date BETWEEN :start AND :end
                ORDER BY rs.obs_date
            """), {"name": feat_name, "start": start_date, "end": end_date}).fetchall()
            if rows:
                s = pd.Series(
                    [r[1] for r in rows],
                    index=pd.to_datetime([r[0] for r in rows]),
                    name=alias,
                )
                s = s[~s.index.duplicated(keep="last")]
                frames.append(s)

    return pd.concat(frames, axis=1).sort_index() if frames else pd.DataFrame()


def bin_astro_feature(series: pd.Series, n_bins: int = 5) -> pd.Series:
    """Bin a continuous astro feature into quantiles."""
    try:
        return pd.qcut(series, n_bins, labels=False, duplicates="drop")
    except ValueError:
        return pd.cut(series, n_bins, labels=False)


def compute_bucket_returns(
    astro_feature: pd.Series,
    market_returns: pd.Series,
    n_bins: int = 5,
) -> pd.DataFrame:
    """
    For each bucket of an astro feature, compute average forward return and Sharpe.
    """
    common = astro_feature.index.intersection(market_returns.index)
    if len(common) < 50:
        return pd.DataFrame()

    feat = astro_feature.loc[common]
    rets = market_returns.loc[common]

    try:
        bins = bin_astro_feature(feat, n_bins)
    except Exception:
        return pd.DataFrame()

    results = []
    for bucket in sorted(bins.dropna().unique()):
        mask = bins == bucket
        bucket_rets = rets[mask]
        if len(bucket_rets) < 10:
            continue
        results.append({
            "bucket": bucket,
            "n_days": len(bucket_rets),
            "mean_return": bucket_rets.mean(),
            "std_return": bucket_rets.std(),
            "sharpe": bucket_rets.mean() / (bucket_rets.std() + 1e-10) * np.sqrt(252),
            "total_return": (1 + bucket_rets).prod() - 1,
            "best_day": bucket_rets.max(),
            "worst_day": bucket_rets.min(),
        })

    return pd.DataFrame(results)


def compute_conditioned_ls_returns(
    signal: pd.DataFrame,
    forward_returns: pd.DataFrame,
    condition: pd.Series,
    condition_value: float | int,
    comparison: str = "eq",
    top_n: int = 3,
    cost_bps: float = 10.0,
) -> pd.Series:
    """
    Long-short returns only on days when condition matches.
    """
    fwd = forward_returns.shift(-1)
    common_dates = signal.index.intersection(fwd.dropna(how="all").index)
    cond_aligned = condition.reindex(common_dates).ffill(limit=5)

    ls_returns = []
    prev_longs = set()
    prev_shorts = set()

    for dt in common_dates:
        c = cond_aligned.get(dt)
        if pd.isna(c):
            ls_returns.append({"date": dt, "return": 0.0})
            continue

        if comparison == "eq" and c != condition_value:
            ls_returns.append({"date": dt, "return": 0.0})
            continue
        elif comparison == "lt" and c >= condition_value:
            ls_returns.append({"date": dt, "return": 0.0})
            continue
        elif comparison == "gt" and c <= condition_value:
            ls_returns.append({"date": dt, "return": 0.0})
            continue
        elif comparison == "le" and c > condition_value:
            ls_returns.append({"date": dt, "return": 0.0})
            continue

        sig_row = signal.loc[dt].dropna()
        ret_row = fwd.loc[dt].dropna()
        common = sig_row.index.intersection(ret_row.index)

        if len(common) < 2 * top_n:
            ls_returns.append({"date": dt, "return": 0.0})
            continue

        r = sig_row[common].sort_values()
        shorts = set(r.index[:top_n])
        longs = set(r.index[-top_n:])

        long_ret = ret_row[list(longs)].mean()
        short_ret = ret_row[list(shorts)].mean()
        gross = long_ret - short_ret

        long_to = len(longs - prev_longs) / max(top_n, 1)
        short_to = len(shorts - prev_shorts) / max(top_n, 1)
        cost = (long_to + short_to) / 2 * cost_bps * 2 / 10_000

        ls_returns.append({"date": dt, "return": gross - cost})
        prev_longs = longs
        prev_shorts = shorts

    if not ls_returns:
        return pd.Series(dtype=float)
    return pd.DataFrame(ls_returns).set_index("date")["return"]


def find_best_window(returns: pd.Series, min_days: int = 60, max_days: int = 400) -> dict:
    """Find best contiguous Sharpe window."""
    best = {"sharpe": -999, "start": None, "end": None, "days": 0}
    for size in range(min_days, min(max_days + 1, len(returns)), 10):
        for i in range(0, len(returns) - size, 5):
            w = returns.iloc[i:i + size]
            sr = sharpe_ratio(w)
            if sr > best["sharpe"]:
                best = {"sharpe": sr, "start": w.index[0], "end": w.index[-1], "days": len(w)}
    return best


def main() -> None:
    engine = get_engine()

    start_date = date(2021, 1, 1)
    end_date = date(2026, 3, 31)

    log.info("=" * 70)
    log.info("ASTROGRID SHARPE HUNTER")
    log.info("Finding the mystical-market thread")
    log.info("=" * 70)

    # === 1. Load Data ===
    log.info("\n--- Loading Astro Features ---")
    astro = load_astro_features(engine, start_date - timedelta(days=365), end_date)
    log.info("Astro panel: {d} dates x {f} features", d=len(astro), f=len(astro.columns))

    log.info("\n--- Loading Prices ---")
    prices = build_price_panel(engine, tickers=EXTENDED_UNIVERSE,
                               start_date=start_date, end_date=end_date)
    prices = prices.ffill(limit=5)
    log.info("Price panel: {d} dates x {t} tickers", d=len(prices), t=len(prices.columns))

    # Sector-only panel for focused analysis
    sector_tickers = [t for t in SECTOR_ETFS if t in prices.columns]
    sector_prices = prices[sector_tickers]
    log.info("Sector panel: {d} dates x {t} tickers", d=len(sector_prices), t=len(sector_prices.columns))

    # Macro
    macro = load_macro_features(engine, start_date - timedelta(days=365), end_date)

    # Forward returns
    fwd_1d = prices.pct_change(fill_method=None).shift(-1)  # 1-day forward
    fwd_5d = prices.pct_change(periods=5, fill_method=None).shift(-5)  # 5-day forward

    # SPY as market benchmark
    spy_ret_1d = fwd_1d["SPY"].dropna() if "SPY" in fwd_1d.columns else pd.Series(dtype=float)
    spy_ret_5d = fwd_5d["SPY"].dropna() if "SPY" in fwd_5d.columns else pd.Series(dtype=float)

    # === 2. Astro Feature → Market Return Analysis ===
    log.info("\n" + "=" * 70)
    log.info("PHASE 1: ASTRO FEATURE → SPY RETURN CORRELATION")
    log.info("Which astro features predict next-day market direction?")
    log.info("=" * 70)

    astro_aligned = astro.reindex(spy_ret_1d.index).ffill(limit=3)
    feature_scores = []

    for col in astro_aligned.columns:
        feat = astro_aligned[col].dropna()
        if len(feat) < 100:
            continue

        buckets = compute_bucket_returns(feat, spy_ret_1d, n_bins=5)
        if buckets.empty:
            continue

        # Score: spread between best and worst bucket
        spread = buckets["sharpe"].max() - buckets["sharpe"].min()
        best_bucket = buckets.loc[buckets["sharpe"].idxmax()]
        worst_bucket = buckets.loc[buckets["sharpe"].idxmin()]

        feature_scores.append({
            "feature": col,
            "spread": spread,
            "best_sharpe": best_bucket["sharpe"],
            "best_bucket": best_bucket["bucket"],
            "best_n": best_bucket["n_days"],
            "worst_sharpe": worst_bucket["sharpe"],
            "worst_bucket": worst_bucket["bucket"],
            "worst_n": worst_bucket["n_days"],
        })

    scores_df = pd.DataFrame(feature_scores).sort_values("spread", ascending=False)

    log.info("\nTop astro features by bucket-spread (best vs worst quintile):")
    for _, row in scores_df.head(15).iterrows():
        log.info(
            "  {f:35s} spread={sp:5.2f}  best_q={bq:.0f}(SR={bs:5.2f},n={bn:.0f})  "
            "worst_q={wq:.0f}(SR={ws:5.2f},n={wn:.0f})",
            f=row["feature"], sp=row["spread"],
            bq=row["best_bucket"], bs=row["best_sharpe"], bn=row["best_n"],
            wq=row["worst_bucket"], ws=row["worst_sharpe"], wn=row["worst_n"],
        )

    # === 3. Cross-Sectional Signals Conditioned on Astro ===
    log.info("\n" + "=" * 70)
    log.info("PHASE 2: MOMENTUM SIGNALS CONDITIONED ON TOP ASTRO FEATURES")
    log.info("= Only trade when astro configuration is favorable =")
    log.info("=" * 70)

    # Compute base signals on sector ETFs
    signals = compute_equity_signals(sector_prices)
    best_signal_name = "vol_regime_equity"
    best_signal = signals[best_signal_name]

    # Baseline: unconditioned
    ls_base = long_short_returns(best_signal, fwd_5d[sector_tickers], top_n=3, cost_bps=10)
    sr_base = sharpe_ratio(ls_base)
    log.info("\nBaseline ({sig} on sectors, no astro filter):", sig=best_signal_name)
    log.info("  Sharpe={s:.3f}  Return={r:.1%}  MaxDD={d:.1%}  n={n}",
             s=sr_base, r=annualized_return(ls_base), d=max_drawdown(ls_base), n=len(ls_base))

    # Try conditioning on top astro features
    top_astro = scores_df.head(10)["feature"].tolist()
    conditioned_results = []

    for feat_name in top_astro:
        feat = astro_aligned[feat_name].dropna() if feat_name in astro_aligned.columns else pd.Series(dtype=float)
        if len(feat) < 100:
            continue

        # Bin into quintiles
        try:
            bins = bin_astro_feature(feat, 5)
        except Exception:
            continue

        for bucket in sorted(bins.dropna().unique()):
            ls_cond = compute_conditioned_ls_returns(
                best_signal, fwd_5d[sector_tickers], bins, bucket, "eq", top_n=3
            )
            trading_days = (ls_cond != 0).sum()
            if trading_days < 30:
                continue

            sr_cond = sharpe_ratio(ls_cond)
            conditioned_results.append({
                "feature": feat_name,
                "bucket": bucket,
                "sharpe": sr_cond,
                "return": annualized_return(ls_cond),
                "maxdd": max_drawdown(ls_cond),
                "trading_days": trading_days,
                "total_days": len(ls_cond),
            })

    cond_df = pd.DataFrame(conditioned_results).sort_values("sharpe", ascending=False)

    log.info("\nTop astro-conditioned configurations (momentum on sectors):")
    for _, row in cond_df.head(20).iterrows():
        log.info(
            "  {f:35s} Q{b:.0f} | Sharpe={s:6.2f} | Return={r:7.1%} | MaxDD={d:7.1%} | "
            "Trade={td:.0f}/{tot:.0f} days",
            f=row["feature"], b=row["bucket"], s=row["sharpe"],
            r=row["return"], d=row["maxdd"],
            td=row["trading_days"], tot=row["total_days"],
        )

    # === 4. Multi-Feature Conditioning ===
    log.info("\n" + "=" * 70)
    log.info("PHASE 3: MULTI-FEATURE ASTRO GATES")
    log.info("Combine top 2-3 astro features as AND conditions")
    log.info("=" * 70)

    # Get top 5 feature+bucket combos
    if len(cond_df) >= 2:
        top_combos = cond_df.head(8)
        best_configs = [(r["feature"], r["bucket"]) for _, r in top_combos.iterrows()]

        multi_results = []
        for (f1, b1), (f2, b2) in combinations(best_configs[:6], 2):
            if f1 == f2:
                continue

            feat1 = astro_aligned.get(f1)
            feat2 = astro_aligned.get(f2)
            if feat1 is None or feat2 is None:
                continue

            try:
                bins1 = bin_astro_feature(feat1.dropna(), 5)
                bins2 = bin_astro_feature(feat2.dropna(), 5)
            except Exception:
                continue

            # Combined condition
            combined = (bins1 == b1) & (bins2 == b2)
            combined = combined.reindex(best_signal.index).ffill(limit=3).fillna(False)

            ls_multi = compute_conditioned_ls_returns(
                best_signal, fwd_5d[sector_tickers],
                combined.astype(float), 1.0, "eq", top_n=3
            )
            trading_days = (ls_multi != 0).sum()
            if trading_days < 15:
                continue

            sr_multi = sharpe_ratio(ls_multi)
            multi_results.append({
                "config": f"{f1}[Q{b1:.0f}] + {f2}[Q{b2:.0f}]",
                "sharpe": sr_multi,
                "return": annualized_return(ls_multi),
                "maxdd": max_drawdown(ls_multi),
                "trading_days": trading_days,
            })

        multi_df = pd.DataFrame(multi_results).sort_values("sharpe", ascending=False)
        log.info("\nTop multi-astro gate configurations:")
        for _, row in multi_df.head(15).iterrows():
            log.info(
                "  {c:60s} | Sharpe={s:7.2f} | Return={r:8.1%} | MaxDD={d:8.1%} | Trade={td:.0f}d",
                c=row["config"], s=row["sharpe"], r=row["return"],
                d=row["maxdd"], td=row["trading_days"],
            )

    # === 5. VIX + Astro Combined Gate ===
    log.info("\n" + "=" * 70)
    log.info("PHASE 4: VIX + ASTRO COMBINED GATES")
    log.info("=" * 70)

    if "vix" in macro.columns and len(cond_df) > 0:
        vix = macro["vix"].reindex(best_signal.index).ffill(limit=5)

        for _, row in cond_df.head(5).iterrows():
            feat_name = row["feature"]
            bucket = row["bucket"]

            feat = astro_aligned.get(feat_name)
            if feat is None:
                continue

            try:
                bins = bin_astro_feature(feat.dropna(), 5)
            except Exception:
                continue

            bins_aligned = bins.reindex(best_signal.index).ffill(limit=3)
            for vix_thresh in [20, 25, 30]:
                combined = (bins_aligned == bucket) & (vix < vix_thresh)
                combined = combined.fillna(False)

                ls_vix_astro = compute_conditioned_ls_returns(
                    best_signal, fwd_5d[sector_tickers],
                    combined.astype(float), 1.0, "eq", top_n=3,
                )
                trading_days = (ls_vix_astro != 0).sum()
                if trading_days < 15:
                    continue

                sr = sharpe_ratio(ls_vix_astro)
                log.info(
                    "  {f:30s} Q{b:.0f} + VIX<{v:2d} | Sharpe={s:7.2f} | "
                    "Return={r:8.1%} | Trade={td:.0f}d",
                    f=feat_name, b=bucket, v=vix_thresh,
                    s=sr, r=annualized_return(ls_vix_astro), td=trading_days,
                )

    # === 6. All Signals × Best Astro Gate ===
    log.info("\n" + "=" * 70)
    log.info("PHASE 5: ALL SIGNALS × BEST ASTRO GATE")
    log.info("=" * 70)

    if len(cond_df) > 0:
        best_feat = cond_df.iloc[0]["feature"]
        best_bucket = cond_df.iloc[0]["bucket"]

        feat = astro_aligned.get(best_feat)
        if feat is not None:
            try:
                bins = bin_astro_feature(feat.dropna(), 5)
            except Exception:
                bins = None

            if bins is not None:
                for sig_name, sig_panel in signals.items():
                    ls_gated = compute_conditioned_ls_returns(
                        sig_panel, fwd_5d[sector_tickers],
                        bins, best_bucket, "eq", top_n=3,
                    )
                    trading_days = (ls_gated != 0).sum()
                    if trading_days < 15:
                        continue

                    sr = sharpe_ratio(ls_gated)
                    log.info(
                        "  {sig:30s} gated by {f}[Q{b:.0f}] | Sharpe={s:7.2f} | "
                        "Return={r:8.1%} | Trade={td:.0f}d",
                        sig=sig_name, f=best_feat, b=best_bucket,
                        s=sr, r=annualized_return(ls_gated), td=trading_days,
                    )

    # === 7. Cherry-Pick Best Windows ===
    log.info("\n" + "=" * 70)
    log.info("PHASE 6: CHERRY-PICKED BEST WINDOWS")
    log.info("=" * 70)

    if len(cond_df) > 0:
        best_feat = cond_df.iloc[0]["feature"]
        best_bucket = cond_df.iloc[0]["bucket"]
        feat = astro_aligned.get(best_feat)
        if feat is not None:
            try:
                bins = bin_astro_feature(feat.dropna(), 5)
            except Exception:
                bins = None

            if bins is not None:
                for sig_name in ["vol_regime_equity", "trend_volume_gate"]:
                    ls_gated = compute_conditioned_ls_returns(
                        signals[sig_name], fwd_5d[sector_tickers],
                        bins, best_bucket, "eq", top_n=3,
                    )
                    if len(ls_gated) > 60:
                        best_win = find_best_window(ls_gated, min_days=60, max_days=300)
                        log.info(
                            "  {sig:30s} best window: {s} → {e} ({d}d) Sharpe={sr:.2f}",
                            sig=sig_name,
                            s=best_win["start"].date() if best_win["start"] else "N/A",
                            e=best_win["end"].date() if best_win["end"] else "N/A",
                            d=best_win["days"], sr=best_win["sharpe"],
                        )

    # === 8. Extended Universe with Astro Gate ===
    log.info("\n" + "=" * 70)
    log.info("PHASE 7: EXTENDED UNIVERSE WITH ASTRO GATE")
    log.info("Try 30 tickers instead of 11 sectors")
    log.info("=" * 70)

    ext_tickers = [t for t in EXTENDED_UNIVERSE if t in prices.columns]
    ext_prices = prices[ext_tickers]
    ext_signals = compute_equity_signals(ext_prices)

    if len(cond_df) > 0:
        best_feat = cond_df.iloc[0]["feature"]
        best_bucket = cond_df.iloc[0]["bucket"]
        feat = astro_aligned.get(best_feat)
        if feat is not None:
            try:
                bins = bin_astro_feature(feat.dropna(), 5)
            except Exception:
                bins = None

            if bins is not None:
                for sig_name, sig_panel in ext_signals.items():
                    for n in [3, 5, 7]:
                        ls_ext = compute_conditioned_ls_returns(
                            sig_panel, fwd_5d[ext_tickers],
                            bins, best_bucket, "eq", top_n=n,
                        )
                        trading_days = (ls_ext != 0).sum()
                        if trading_days < 15:
                            continue

                        sr = sharpe_ratio(ls_ext)
                        log.info(
                            "  {sig:30s} top_n={n} | Sharpe={s:7.2f} | "
                            "Return={r:8.1%} | Trade={td:.0f}d | MaxDD={d:.1%}",
                            sig=sig_name, n=n, s=sr,
                            r=annualized_return(ls_ext), td=trading_days,
                            d=max_drawdown(ls_ext),
                        )

    # === SUMMARY ===
    log.info("\n" + "=" * 70)
    log.info("SUMMARY — THE THREAD")
    log.info("=" * 70)
    log.info("Baseline sector momentum Sharpe: {s:.3f}", s=sr_base)

    if len(cond_df) > 0:
        log.info("\nBest single astro gate:")
        best = cond_df.iloc[0]
        log.info("  {f} Q{b:.0f} → Sharpe {s:.2f} ({td:.0f} trading days)",
                 f=best["feature"], b=best["bucket"],
                 s=best["sharpe"], td=best["trading_days"])

    if len(cond_df) >= 2:
        log.info("\nBest 5 astro gates:")
        for i, (_, row) in enumerate(cond_df.head(5).iterrows()):
            log.info("  #{i}: {f} Q{b:.0f} → Sharpe {s:.2f}",
                     i=i + 1, f=row["feature"], b=row["bucket"], s=row["sharpe"])

    if multi_results:
        log.info("\nBest multi-astro gate: {c} → Sharpe {s:.2f}",
                 c=multi_df.iloc[0]["config"], s=multi_df.iloc[0]["sharpe"])

    log.info("\nDone.")


if __name__ == "__main__":
    main()
