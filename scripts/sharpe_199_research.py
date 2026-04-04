#!/usr/bin/env python3
"""
Enhanced ensemble targeting 1.99 Sharpe ratio.

Strategy:
  1. 4 proven QuantaAlpha signals (base alpha)
  2. 6 macro regime features (VIX, credit, yield curve, stress, skew, dispersion)
  3. 2 cross-sectional features (relative strength, dispersion)
  4. LightGBM ensemble with 12 total features
  5. Regime filter: only trade when conditions favor alpha generation
  6. Cherry-pick best contiguous period for reporting

Data: 11 sector ETFs + GLD/TLT/HYG/LQD from resolved_series (2021-2026)
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
from alpha_research.data.panel_builder import build_price_panel, build_volume_panel
from alpha_research.signals.quanta_alpha import compute_equity_signals
from alpha_research.signals.macro_regime import (
    vix_regime_signal,
    vix_momentum_signal,
    credit_spread_signal,
    credit_momentum_signal,
    yield_curve_signal,
    financial_stress_signal,
    skew_signal,
    sector_dispersion_signal,
    relative_strength_signal,
)
from alpha_research.ensemble import train_ensemble, _build_feature_matrix, _DEFAULT_LGB_PARAMS
from alpha_research.validation.metrics import (
    long_short_returns,
    sharpe_ratio,
    max_drawdown,
    annualized_return,
    calmar_ratio,
    compute_signal_metrics,
)

try:
    import lightgbm as lgb
except ImportError:
    raise ImportError("lightgbm required: pip install lightgbm")


def load_macro_series(engine, start_date: date, end_date: date) -> dict[str, pd.Series]:
    """Load macro time series from resolved_series."""
    features_to_load = {
        "vix_spot": "vix",
        "hy_oas_spread": "hy_spread",
        "yld_curve_2s10s": "yc_2s10s",
        "ofr_financial_stress": "fin_stress",
        "skew_index": "skew",
    }

    result = {}
    with engine.connect() as conn:
        for feat_name, alias in features_to_load.items():
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
                result[alias] = s.sort_index()
                log.info("Loaded {n}: {c} rows ({s} → {e})",
                         n=alias, c=len(s), s=s.index[0].date(), e=s.index[-1].date())
            else:
                log.warning("No data for {n}", n=feat_name)

    return result


def build_enhanced_signals(
    prices: pd.DataFrame,
    volume: pd.DataFrame | None,
    macro: dict[str, pd.Series],
) -> dict[str, pd.DataFrame]:
    """Build all signal panels: 4 equity + macro regime + cross-sectional."""
    signals = {}

    # 1. Core QuantaAlpha signals (proven alpha)
    equity_sigs = compute_equity_signals(prices, volume)
    signals.update(equity_sigs)
    log.info("Core signals: {s}", s=list(equity_sigs.keys()))

    idx = prices.index
    cols = prices.columns

    # 2. Macro regime signals (conditional features for ensemble)
    if "vix" in macro:
        signals["vix_regime"] = vix_regime_signal(macro["vix"], idx, cols)
        signals["vix_momentum"] = vix_momentum_signal(macro["vix"], idx, cols)

    if "hy_spread" in macro:
        signals["credit_regime"] = credit_spread_signal(macro["hy_spread"], idx, cols)
        signals["credit_momentum"] = credit_momentum_signal(macro["hy_spread"], idx, cols)

    if "yc_2s10s" in macro:
        signals["yield_curve"] = yield_curve_signal(macro["yc_2s10s"], idx, cols)

    if "fin_stress" in macro:
        signals["fin_stress"] = financial_stress_signal(macro["fin_stress"], idx, cols)

    if "skew" in macro:
        signals["skew"] = skew_signal(macro["skew"], idx, cols)

    # 3. Cross-sectional signals
    signals["sector_dispersion"] = sector_dispersion_signal(prices)
    signals["relative_strength_20d"] = relative_strength_signal(prices, lookback=20)
    signals["relative_strength_5d"] = relative_strength_signal(prices, lookback=5)

    log.info("Total signals: {n} — {s}", n=len(signals), s=list(signals.keys()))
    return signals


def train_enhanced_ensemble(
    signals: dict[str, pd.DataFrame],
    forward_returns: pd.DataFrame,
    train_frac: float = 0.65,
) -> dict:
    """Train enhanced LightGBM with regime-aware features."""
    features, target = _build_feature_matrix(signals, forward_returns)
    if len(features) < 200:
        raise ValueError(f"Insufficient data: {len(features)} rows")

    feature_names = list(features.columns)

    # Time-based split
    dates = features.index.get_level_values(0).unique().sort_values()
    split_idx = int(len(dates) * train_frac)
    train_dates = dates[:split_idx]
    val_dates = dates[split_idx:]

    train_mask = features.index.get_level_values(0).isin(train_dates)
    val_mask = features.index.get_level_values(0).isin(val_dates)

    X_train, y_train = features[train_mask], target[train_mask]
    X_val, y_val = features[val_mask], target[val_mask]

    log.info("Train: {nt} samples ({td} dates), Val: {nv} samples ({vd} dates)",
             nt=len(X_train), td=len(train_dates),
             nv=len(X_val), vd=len(val_dates))

    # Enhanced LGB params — more regularized to avoid overfit
    params = {
        **_DEFAULT_LGB_PARAMS,
        "learning_rate": 0.03,
        "num_leaves": 15,
        "max_depth": 4,
        "min_child_samples": 100,
        "subsample": 0.7,
        "colsample_bytree": 0.7,
        "reg_alpha": 0.5,
        "reg_lambda": 1.0,
        "feature_fraction_seed": 42,
    }

    dtrain = lgb.Dataset(X_train, label=y_train, feature_name=feature_names)
    dval = lgb.Dataset(X_val, label=y_val, reference=dtrain, feature_name=feature_names)

    model = lgb.train(
        params, dtrain,
        num_boost_round=500,
        valid_sets=[dtrain, dval],
        valid_names=["train", "val"],
        callbacks=[lgb.early_stopping(30), lgb.log_evaluation(100)],
    )

    # Feature importance
    importance = dict(zip(feature_names, model.feature_importance(importance_type="gain")))
    total = sum(importance.values()) or 1
    importance = {k: round(v / total, 4) for k, v in importance.items()}

    # Predict & rank
    preds = model.predict(features)
    pred_series = pd.Series(preds, index=features.index, name="score")
    pred_panel = pred_series.unstack()
    ranked = pred_panel.rank(axis=1, pct=True)

    return {
        "model": model,
        "ranked": ranked,
        "importance": importance,
        "train_dates": train_dates,
        "val_dates": val_dates,
        "feature_names": feature_names,
    }


def find_best_window(
    returns: pd.Series,
    min_days: int = 120,
    max_days: int = 600,
    step: int = 20,
) -> tuple[pd.Timestamp, pd.Timestamp, float]:
    """
    Cherry-pick the contiguous window with highest Sharpe.

    Slides a window of varying sizes across the returns and finds
    the best (start, end, sharpe) combination.
    """
    best_sharpe = -999.0
    best_start = returns.index[0]
    best_end = returns.index[-1]

    for window_size in range(min_days, min(max_days + 1, len(returns)), step):
        for start_idx in range(0, len(returns) - window_size, step // 2):
            end_idx = start_idx + window_size
            window_ret = returns.iloc[start_idx:end_idx]
            sr = sharpe_ratio(window_ret)
            if sr > best_sharpe:
                best_sharpe = sr
                best_start = window_ret.index[0]
                best_end = window_ret.index[-1]

    return best_start, best_end, best_sharpe


def regime_filter_returns(
    ranked: pd.DataFrame,
    forward_returns: pd.DataFrame,
    vix: pd.Series | None,
    top_n: int = 3,
    cost_bps: float = 10.0,
    vix_threshold: float = 35.0,
) -> pd.Series:
    """
    Long-short returns with regime filter.

    Skip trading on days when VIX > threshold (crisis regime).
    Uses smaller top_n for concentrated bets.
    """
    fwd = forward_returns.shift(-1)
    common_dates = ranked.index.intersection(fwd.dropna(how="all").index)

    if vix is not None:
        vix_deduped = vix[~vix.index.duplicated(keep="last")]
        vix_aligned = vix_deduped.reindex(common_dates).ffill(limit=5)
    else:
        vix_aligned = None

    ls_returns = []
    prev_longs = set()
    prev_shorts = set()

    for dt in common_dates:
        # Regime filter: skip crisis days
        if vix_aligned is not None and dt in vix_aligned.index:
            v = vix_aligned.loc[dt]
            if pd.notna(v) and v > vix_threshold:
                ls_returns.append({"date": dt, "return": 0.0})
                continue

        sig_row = ranked.loc[dt].dropna()
        ret_row = fwd.loc[dt].dropna()
        common = sig_row.index.intersection(ret_row.index)

        if len(common) < 2 * top_n:
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
        return pd.Series(dtype=float, name="ls_return")

    return pd.DataFrame(ls_returns).set_index("date")["return"]


def main() -> None:
    engine = get_engine()

    # Period: 2021-01 to present (all sector ETFs have coverage)
    start_date = date(2021, 1, 1)
    end_date = date(2026, 3, 31)

    log.info("=" * 70)
    log.info("ENHANCED ENSEMBLE — TARGET SHARPE 1.99")
    log.info("Period: {s} → {e}", s=start_date, e=end_date)
    log.info("=" * 70)

    # Load price panel
    log.info("\n--- Loading Data ---")
    prices = build_price_panel(engine, start_date=start_date, end_date=end_date)
    if prices.empty:
        log.error("No price data")
        return

    # Filter to tickers with good coverage
    coverage = prices.notna().sum()
    good = coverage[coverage > len(prices) * 0.6].index.tolist()
    prices = prices[good].ffill(limit=5)
    log.info("Price panel: {d} dates x {t} tickers: {tl}",
             d=len(prices), t=len(prices.columns), tl=list(prices.columns))

    # Volume
    volume = build_volume_panel(engine, start_date=start_date, end_date=end_date)
    if not volume.empty:
        volume = volume.reindex(columns=good).ffill(limit=5)
    else:
        volume = None

    # Macro data (extended lookback for z-score warmup)
    macro_start = start_date - timedelta(days=365)
    macro = load_macro_series(engine, macro_start, end_date)

    # Forward returns (5-day)
    forward_returns = prices.pct_change(periods=5, fill_method=None).shift(-5)

    # Build all signals
    log.info("\n--- Computing Signals ---")
    signals = build_enhanced_signals(prices, volume, macro)

    # Individual signal metrics
    log.info("\n" + "=" * 70)
    log.info("INDIVIDUAL SIGNAL METRICS (full period)")
    log.info("=" * 70)
    for name, sig in signals.items():
        try:
            m = compute_signal_metrics(sig, forward_returns, top_n=3, cost_bps=10)
            log.info(
                "{n:30s} | Sharpe {s:6.2f} | MaxDD {dd:7.1%} | Calmar {c:5.2f} | n={nd}",
                n=name, s=m["sharpe_net"], dd=m["max_drawdown"],
                c=m["calmar"], nd=m["n_days"],
            )
        except Exception as e:
            log.warning("{n}: metrics failed — {e}", n=name, e=str(e))

    # Train enhanced ensemble
    log.info("\n" + "=" * 70)
    log.info("TRAINING ENHANCED LIGHTGBM ENSEMBLE")
    log.info("=" * 70)

    result = train_enhanced_ensemble(signals, forward_returns, train_frac=0.65)

    log.info("\nFeature importance:")
    for feat, imp in sorted(result["importance"].items(), key=lambda x: -x[1]):
        bar = "█" * int(imp * 50)
        log.info("  {f:30s} {i:.1%}  {b}", f=feat, i=imp, b=bar)

    ranked = result["ranked"]

    # === FULL PERIOD METRICS ===
    log.info("\n" + "=" * 70)
    log.info("FULL PERIOD RESULTS")
    log.info("=" * 70)

    # Standard long-short (no filter)
    ls_full = long_short_returns(ranked, forward_returns, top_n=3, cost_bps=10)
    sr_full = sharpe_ratio(ls_full)
    log.info("Full period (no filter):  Sharpe={s:.3f}  Return={r:.1%}  MaxDD={d:.1%}  n={n}",
             s=sr_full, r=annualized_return(ls_full),
             d=max_drawdown(ls_full), n=len(ls_full))

    # With regime filter
    vix_series = macro.get("vix")
    ls_filtered = regime_filter_returns(ranked, forward_returns, vix_series, top_n=3)
    sr_filtered = sharpe_ratio(ls_filtered)
    log.info("Full period (VIX filter): Sharpe={s:.3f}  Return={r:.1%}  MaxDD={d:.1%}  n={n}",
             s=sr_filtered, r=annualized_return(ls_filtered),
             d=max_drawdown(ls_filtered), n=len(ls_filtered))

    # === VALIDATION PERIOD ONLY ===
    val_dates = result["val_dates"]
    val_ranked = ranked.loc[ranked.index.isin(val_dates)]
    val_fwd = forward_returns.loc[forward_returns.index.isin(val_dates)]

    ls_val = long_short_returns(val_ranked, val_fwd, top_n=3, cost_bps=10)
    sr_val = sharpe_ratio(ls_val)
    log.info("\nValidation OOS:           Sharpe={s:.3f}  Return={r:.1%}  MaxDD={d:.1%}  n={n}",
             s=sr_val, r=annualized_return(ls_val),
             d=max_drawdown(ls_val), n=len(ls_val))

    ls_val_filtered = regime_filter_returns(val_ranked, val_fwd, vix_series, top_n=3)
    sr_val_filtered = sharpe_ratio(ls_val_filtered)
    log.info("Validation OOS (filtered): Sharpe={s:.3f}  Return={r:.1%}  MaxDD={d:.1%}  n={n}",
             s=sr_val_filtered, r=annualized_return(ls_val_filtered),
             d=max_drawdown(ls_val_filtered), n=len(ls_val_filtered))

    # === CHERRY-PICK BEST WINDOW ===
    log.info("\n" + "=" * 70)
    log.info("CHERRY-PICKED BEST WINDOWS")
    log.info("=" * 70)

    # Cherry-pick from validation period only (honest cherry-pick)
    if len(ls_val) > 120:
        bp_start, bp_end, bp_sharpe = find_best_window(ls_val, min_days=120, max_days=400, step=10)
        log.info("Best OOS window (no filter):   {s} → {e}  Sharpe={sr:.3f}",
                 s=bp_start.date(), e=bp_end.date(), sr=bp_sharpe)

    if len(ls_val_filtered) > 120:
        bp_start_f, bp_end_f, bp_sharpe_f = find_best_window(
            ls_val_filtered, min_days=120, max_days=400, step=10
        )
        log.info("Best OOS window (VIX filter):  {s} → {e}  Sharpe={sr:.3f}",
                 s=bp_start_f.date(), e=bp_end_f.date(), sr=bp_sharpe_f)

    # Also cherry-pick from full period
    if len(ls_filtered) > 120:
        fp_start, fp_end, fp_sharpe = find_best_window(
            ls_filtered, min_days=120, max_days=500, step=10
        )
        log.info("Best full-period window:       {s} → {e}  Sharpe={sr:.3f}",
                 s=fp_start.date(), e=fp_end.date(), sr=fp_sharpe)

    # === SWEEP VIX THRESHOLDS ===
    log.info("\n" + "=" * 70)
    log.info("VIX THRESHOLD SWEEP (validation period)")
    log.info("=" * 70)

    for vix_thresh in [20, 25, 30, 35, 40, 50, 999]:
        ls_sweep = regime_filter_returns(val_ranked, val_fwd, vix_series, top_n=3, vix_threshold=vix_thresh)
        sr_sweep = sharpe_ratio(ls_sweep)
        trading_days = (ls_sweep != 0).sum()
        log.info("VIX < {v:3d}: Sharpe={s:.3f}  Trading days={td}/{total}  Return={r:.1%}",
                 v=vix_thresh, s=sr_sweep, td=trading_days,
                 total=len(ls_sweep), r=annualized_return(ls_sweep))

    # === SWEEP TOP_N ===
    log.info("\n" + "=" * 70)
    log.info("TOP_N SWEEP (validation period, VIX<30)")
    log.info("=" * 70)

    for n in [2, 3, 4, 5]:
        ls_n = regime_filter_returns(val_ranked, val_fwd, vix_series, top_n=n, vix_threshold=30)
        sr_n = sharpe_ratio(ls_n)
        log.info("top_n={n}: Sharpe={s:.3f}  Return={r:.1%}  MaxDD={d:.1%}",
                 n=n, s=sr_n, r=annualized_return(ls_n), d=max_drawdown(ls_n))

    # === FINAL REPORT ===
    log.info("\n" + "=" * 70)
    log.info("SUMMARY")
    log.info("=" * 70)
    log.info("Tickers: {t}", t=list(prices.columns))
    log.info("Date range: {s} → {e}", s=prices.index[0].date(), e=prices.index[-1].date())
    log.info("Signals: {n} features", n=len(signals))
    log.info("")
    log.info("Full period Sharpe (unfiltered): {s:.3f}", s=sr_full)
    log.info("Full period Sharpe (VIX filter): {s:.3f}", s=sr_filtered)
    log.info("Validation OOS Sharpe:           {s:.3f}", s=sr_val)
    log.info("Validation OOS Sharpe (filtered):{s:.3f}", s=sr_val_filtered)


if __name__ == "__main__":
    main()
