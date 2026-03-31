"""
LightGBM ensemble for combining alpha research factors.

Takes ranked signal panels from multiple factors and trains a LightGBM
model to predict forward returns from the factor combination.

Evidence: QuantaAlpha v2 showed LightGBM ensemble added +0.15 Sharpe
over best single factor (val net Sharpe 1.18 → 1.33).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log

try:
    import lightgbm as lgb
    _HAS_LGB = True
except ImportError:
    _HAS_LGB = False
    lgb = None


@dataclass(frozen=True)
class EnsembleResult:
    predictions: pd.DataFrame       # dates × tickers, ensemble score [0, 1]
    feature_importance: dict[str, float]
    train_sharpe: float
    val_sharpe: float
    n_features: int
    n_train_samples: int


_DEFAULT_LGB_PARAMS: dict[str, Any] = {
    "objective": "regression",
    "metric": "rmse",
    "learning_rate": 0.05,
    "num_leaves": 31,
    "max_depth": 5,
    "min_child_samples": 50,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "reg_alpha": 0.1,
    "reg_lambda": 0.1,
    "verbose": -1,
    "n_jobs": -1,
    "seed": 42,
}


def _build_feature_matrix(
    signal_panels: dict[str, pd.DataFrame],
    forward_returns: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.Series]:
    """Stack signal panels into (date × ticker) feature rows with target."""
    frames = []
    for name, panel in signal_panels.items():
        stacked = panel.stack()
        stacked.name = name
        frames.append(stacked)

    features = pd.concat(frames, axis=1).dropna()
    target = forward_returns.stack()
    target.name = "forward_return"

    common = features.index.intersection(target.index)
    return features.loc[common], target.loc[common]


def train_ensemble(
    signal_panels: dict[str, pd.DataFrame],
    forward_returns: pd.DataFrame,
    train_frac: float = 0.7,
    lgb_params: dict[str, Any] | None = None,
    n_rounds: int = 200,
    early_stopping: int = 20,
) -> EnsembleResult:
    """
    Train LightGBM ensemble on stacked factor signals.

    Uses expanding-window split: first train_frac for training, rest for validation.
    Returns predictions on the full dataset (in-sample + OOS).
    """
    if not _HAS_LGB:
        raise ImportError("lightgbm not installed. Run: pip install lightgbm")

    features, target = _build_feature_matrix(signal_panels, forward_returns)
    if len(features) < 100:
        raise ValueError(f"Insufficient data: {len(features)} samples (need >= 100)")

    params = {**_DEFAULT_LGB_PARAMS, **(lgb_params or {})}
    feature_names = list(features.columns)

    # Expanding window split by date
    dates = features.index.get_level_values(0).unique().sort_values()
    split_idx = int(len(dates) * train_frac)
    train_dates = dates[:split_idx]
    val_dates = dates[split_idx:]

    train_mask = features.index.get_level_values(0).isin(train_dates)
    val_mask = features.index.get_level_values(0).isin(val_dates)

    X_train, y_train = features[train_mask], target[train_mask]
    X_val, y_val = features[val_mask], target[val_mask]

    log.info(
        "Training LightGBM ensemble: {nf} features, {nt} train / {nv} val samples",
        nf=len(feature_names), nt=len(X_train), nv=len(X_val),
    )

    dtrain = lgb.Dataset(X_train, label=y_train, feature_name=feature_names)
    dval = lgb.Dataset(X_val, label=y_val, reference=dtrain, feature_name=feature_names)

    callbacks = [lgb.early_stopping(early_stopping), lgb.log_evaluation(50)]
    model = lgb.train(
        params, dtrain,
        num_boost_round=n_rounds,
        valid_sets=[dtrain, dval],
        valid_names=["train", "val"],
        callbacks=callbacks,
    )

    # Feature importance
    importance = dict(zip(feature_names, model.feature_importance(importance_type="gain")))
    total_imp = sum(importance.values()) or 1
    importance = {k: round(v / total_imp, 4) for k, v in importance.items()}

    # Predict on full dataset
    preds = model.predict(features)

    # Rank predictions cross-sectionally per date
    pred_series = pd.Series(preds, index=features.index, name="ensemble_score")
    pred_panel = pred_series.unstack()
    ranked = pred_panel.rank(axis=1, pct=True)

    # Compute train/val Sharpe from long-short
    from alpha_research.validation.metrics import long_short_returns, sharpe_ratio

    train_pred = ranked.loc[ranked.index.isin(train_dates)]
    val_pred = ranked.loc[ranked.index.isin(val_dates)]
    train_fwd = forward_returns.unstack() if isinstance(forward_returns, pd.Series) else forward_returns
    train_fwd_aligned = train_fwd.reindex(index=ranked.index, columns=ranked.columns)

    train_ls = long_short_returns(train_pred, train_fwd_aligned.loc[train_pred.index])
    val_ls = long_short_returns(val_pred, train_fwd_aligned.loc[val_pred.index])

    train_sr = sharpe_ratio(train_ls)
    val_sr = sharpe_ratio(val_ls)

    log.info("Ensemble trained: train Sharpe={ts:.3f}, val Sharpe={vs:.3f}", ts=train_sr, vs=val_sr)
    log.info("Feature importance: {fi}", fi=importance)

    return EnsembleResult(
        predictions=ranked,
        feature_importance=importance,
        train_sharpe=train_sr,
        val_sharpe=val_sr,
        n_features=len(feature_names),
        n_train_samples=len(X_train),
    )
