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
    val_outcome_coverage: float     # observed val book slots / val book slots
    # outcome_coverage() of the "train" and "val" books behind the Sharpes:
    # gross/net coverage labels, estimate label, cost-ledger totals
    book_coverage: dict[str, dict[str, Any]]


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
    """Stack signal panels into (date × ticker) feature rows with target.

    Rows are the decision-time universe: every (date, ticker) whose factor
    values are all known. A row is never dropped because its forward return
    is missing (delisted, halted, data gap); the target is NaN there, so
    prediction and ranking see what the decision saw and only training
    drops unlabeled rows.
    """
    frames = []
    for name, panel in signal_panels.items():
        stacked = panel.stack()
        stacked.name = name
        frames.append(stacked)

    features = pd.concat(frames, axis=1).dropna()
    target = forward_returns.stack().reindex(features.index)
    target.name = "forward_return"
    return features, target


def train_ensemble(
    signal_panels: dict[str, pd.DataFrame],
    forward_returns: pd.DataFrame,
    train_frac: float = 0.7,
    lgb_params: dict[str, Any] | None = None,
    n_rounds: int = 200,
    early_stopping: int = 20,
    label_horizon: int = 5,
    embargo_days: int | None = None,
    inner_val_frac: float = 0.2,
) -> EnsembleResult:
    """
    Train LightGBM ensemble on stacked factor signals.

    Uses an expanding-window split with an embargo gap of ``embargo_days``
    (defaulting to ``label_horizon`` and required to be ``>= label_horizon``)
    between training and validation so overlapping forward labels never span
    the split boundary. Early stopping monitors an inner holdout carved from
    the END of the training window (separated from the inner fit set by the
    same embargo), never the outer validation set.
    """
    if not _HAS_LGB:
        raise ImportError("lightgbm not installed. Run: pip install lightgbm")
    if int(label_horizon) != label_horizon or label_horizon < 1:
        raise ValueError(f"label_horizon must be a positive integer, got {label_horizon}")
    embargo = int(label_horizon) if embargo_days is None else int(embargo_days)
    if embargo < label_horizon:
        raise ValueError(
            f"embargo_days ({embargo}) must be >= label_horizon ({label_horizon})"
        )
    if not 0.0 < train_frac < 1.0:
        raise ValueError(f"train_frac must be in (0, 1), got {train_frac}")
    if not 0.0 < inner_val_frac < 1.0:
        raise ValueError(f"inner_val_frac must be in (0, 1), got {inner_val_frac}")

    features, target = _build_feature_matrix(signal_panels, forward_returns)
    labeled = target.notna().to_numpy()
    if int(labeled.sum()) < 100:
        raise ValueError(f"Insufficient data: {int(labeled.sum())} samples (need >= 100)")

    params = {**_DEFAULT_LGB_PARAMS, **(lgb_params or {})}
    feature_names = list(features.columns)

    # Expanding-window split by date with outer embargo >= label_horizon.
    # Outer train: dates[:split_idx]; dropped embargo: dates[split_idx : split_idx + embargo];
    # outer validation: dates[split_idx + embargo :].
    dates = features.index[labeled].get_level_values(0).unique().sort_values()
    split_idx = int(len(dates) * train_frac)
    train_dates = dates[:split_idx]
    val_dates = dates[split_idx + embargo :]

    # Carve inner early-stopping holdout from the END of train_dates with the
    # same embargo gap between fit_dates and es_dates.
    n_train_dates = len(train_dates)
    es_len = max(1, int(n_train_dates * inner_val_frac))
    es_start_idx = n_train_dates - es_len
    fit_end_idx = es_start_idx - embargo
    if fit_end_idx <= 0 or es_len <= 0 or len(val_dates) == 0:
        raise ValueError(
            f"Insufficient dates ({len(dates)}) for train/inner-holdout/val split "
            f"with embargo={embargo} and train_frac={train_frac}"
        )

    fit_dates = train_dates[:fit_end_idx]
    es_dates = train_dates[es_start_idx:]

    date_level = features.index.get_level_values(0)
    fit_mask = date_level.isin(fit_dates) & labeled
    es_mask = date_level.isin(es_dates) & labeled
    val_mask = date_level.isin(val_dates)

    X_fit, y_fit = features[fit_mask], target[fit_mask]
    X_es, y_es = features[es_mask], target[es_mask]
    X_val = features[val_mask]
    if X_fit.empty or X_es.empty or X_val.empty:
        raise ValueError("Empty split after applying embargo between train, early-stop, and val")

    log.info(
        "Training LightGBM ensemble: {nf} features, {nt} fit / {ne} early-stop / "
        "{nv} val samples (embargo={eb} dates)",
        nf=len(feature_names), nt=len(X_fit), ne=len(X_es), nv=len(X_val), eb=embargo,
    )

    dfit = lgb.Dataset(X_fit, label=y_fit, feature_name=feature_names)
    des = lgb.Dataset(X_es, label=y_es, reference=dfit, feature_name=feature_names)

    callbacks = [lgb.early_stopping(early_stopping), lgb.log_evaluation(50)]
    model = lgb.train(
        params,
        dfit,
        num_boost_round=n_rounds,
        valid_sets=[dfit, des],
        valid_names=["train", "early_stop"],
        callbacks=callbacks,
    )

    # Feature importance
    importance = dict(zip(feature_names, model.feature_importance(importance_type="gain")))
    total_imp = sum(importance.values()) or 1
    importance = {k: round(v / total_imp, 4) for k, v in importance.items()}

    # Predict on the full decision-time universe (labeled or not)
    preds = model.predict(features)

    # Rank predictions cross-sectionally per date
    pred_series = pd.Series(preds, index=features.index, name="ensemble_score")
    pred_panel = pred_series.unstack()
    ranked = pred_panel.rank(axis=1, pct=True)

    # Compute train/val Sharpe from long-short (val_dates untouched during training/ES)
    from alpha_research.validation.metrics import (
        long_short_book,
        outcome_coverage,
        sharpe_ratio,
    )

    train_pred = ranked.loc[ranked.index.isin(fit_dates)]
    val_pred = ranked.loc[ranked.index.isin(val_dates)]
    train_fwd = forward_returns.unstack() if isinstance(forward_returns, pd.Series) else forward_returns
    train_fwd_aligned = train_fwd.reindex(index=ranked.index, columns=ranked.columns)

    train_book = long_short_book(train_pred, train_fwd_aligned.loc[train_pred.index])
    val_book = long_short_book(val_pred, train_fwd_aligned.loc[val_pred.index])
    train_ls = train_book["return"].dropna().astype(float)
    val_ls = val_book["return"].dropna().astype(float)
    book_coverage = {"train": outcome_coverage(train_book), "val": outcome_coverage(val_book)}
    val_coverage = float(book_coverage["val"]["outcome_coverage"])

    train_sr = sharpe_ratio(train_ls)
    val_sr = sharpe_ratio(val_ls)

    log.info(
        "Ensemble trained: train Sharpe={ts:.3f}, val Sharpe={vs:.3f}, "
        "val outcome coverage={vc:.4f} (net {nc}, {est})",
        ts=train_sr, vs=val_sr, vc=val_coverage,
        nc=book_coverage["val"]["net_coverage"], est=book_coverage["val"]["estimate"],
    )
    log.info("Feature importance: {fi}", fi=importance)

    return EnsembleResult(
        predictions=ranked,
        feature_importance=importance,
        train_sharpe=train_sr,
        val_sharpe=val_sr,
        n_features=len(feature_names),
        n_train_samples=len(X_fit),
        val_outcome_coverage=val_coverage,
        book_coverage=book_coverage,
    )
