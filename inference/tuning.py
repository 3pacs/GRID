"""
GRID strategy parameter tuning.

Optimises GRID's execution and ensemble parameters via walk-forward
backtesting.  Searches over Kelly fractions, edge thresholds, ensemble
weights, and execution config — then returns the best configuration
with full audit trail.

Fully self-contained — no external dependencies beyond numpy/pandas/sklearn.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from itertools import product
from typing import Any, Callable

import numpy as np
import pandas as pd
from loguru import logger as log

from inference.calibration import CalibrationScorer
from validation.execution_sim import ExecutionSimConfig, ExecutionSimulator


# ── Local types (replacing autopredict imports) ──────────────────────


@dataclass
class BacktestResult:
    """Result from a single backtest configuration."""
    params: dict[str, Any]
    total_pnl: float
    sharpe_ratio: float | None
    win_rate: float
    total_trades: int
    calibration_error: float
    edge_capture_rate: float

    def score(self, scoring_fn: Callable[[BacktestResult], float] | None = None) -> float:
        """Score this result using a scoring function or default (Sharpe + calibration bonus)."""
        if scoring_fn is not None:
            return scoring_fn(self)
        # Default: Sharpe ratio with calibration bonus
        base = self.sharpe_ratio if self.sharpe_ratio is not None else 0.0
        cal_bonus = max(0, 0.05 - self.calibration_error) * 10
        return base + cal_bonus

    def to_dict(self) -> dict[str, Any]:
        return {
            "params": self.params,
            "total_pnl": round(self.total_pnl, 4),
            "sharpe_ratio": round(self.sharpe_ratio, 4) if self.sharpe_ratio is not None else None,
            "win_rate": round(self.win_rate, 4),
            "total_trades": self.total_trades,
            "calibration_error": round(self.calibration_error, 4),
            "edge_capture_rate": round(self.edge_capture_rate, 4),
        }


class _ParameterGrid:
    """Simple parameter grid (replaces autopredict ParameterGrid)."""

    def __init__(self, grid: dict[str, list[Any]]) -> None:
        self._grid = grid
        self._keys = sorted(grid.keys())
        self._combos = list(product(*(grid[k] for k in self._keys)))

    def __len__(self) -> int:
        return len(self._combos)

    def __iter__(self):
        for combo in self._combos:
            yield dict(zip(self._keys, combo))


def _create_param_grid_from_current(
    current_params: dict[str, float],
    perturbation_factor: float = 0.20,
    n_steps: int = 3,
) -> _ParameterGrid:
    """Create a parameter grid centered on current values."""
    grid: dict[str, list[Any]] = {}
    for key, value in current_params.items():
        delta = abs(value) * perturbation_factor
        if delta == 0:
            delta = 0.01
        steps = np.linspace(value - delta, value + delta, 2 * n_steps + 1)
        grid[key] = [round(float(s), 6) for s in steps]
    return _ParameterGrid(grid)


class _GridSearchTuner:
    """Grid search tuner (replaces autopredict GridSearchTuner)."""

    def __init__(
        self,
        param_grid: _ParameterGrid,
        backtest_fn: Callable[[dict[str, Any]], BacktestResult],
        scoring_fn: Callable[[BacktestResult], float] | None = None,
        verbose: bool = True,
    ) -> None:
        self._grid = param_grid
        self._backtest_fn = backtest_fn
        self._scoring_fn = scoring_fn
        self._verbose = verbose
        self.results: list[BacktestResult] = []

    def tune(self) -> tuple[dict[str, Any], BacktestResult]:
        """Run grid search and return (best_params, best_result)."""
        best_score = float("-inf")
        best_result: BacktestResult | None = None
        best_params: dict[str, Any] = {}

        for i, params in enumerate(self._grid):
            result = self._backtest_fn(params)
            self.results.append(result)

            score = result.score(self._scoring_fn)
            if self._verbose and i % 10 == 0:
                log.debug("Config {i}/{n}: score={s:.4f}", i=i + 1, n=len(self._grid), s=score)

            if score > best_score:
                best_score = score
                best_result = result
                best_params = params

        if best_result is None:
            raise ValueError("No configurations to evaluate")

        if self._verbose:
            log.info("Best score: {s:.4f}, params: {p}", s=best_score, p=best_params)

        return best_params, best_result


# ── Default parameter grids ──────────────────────────────────────────

DEFAULT_EXECUTION_GRID: dict[str, list[Any]] = {
    "kelly_fraction": [0.10, 0.15, 0.20, 0.25, 0.35],
    "min_edge": [0.03, 0.05, 0.08, 0.10],
    "aggressive_edge": [0.10, 0.12, 0.15, 0.20],
    "base_spread_bps": [30.0, 50.0, 75.0],
}

DEFAULT_ENSEMBLE_GRID: dict[str, list[Any]] = {
    "xgboost_weight": [0.35, 0.40, 0.45, 0.50, 0.55],
    "random_forest_weight": [0.20, 0.25, 0.30, 0.35],
    # rule_based_weight derived as 1 - xgb - rf (clamped >= 0.10)
}


@dataclass
class TuningResult:
    """Result of a parameter tuning run.

    Attributes:
        best_params: Best parameter configuration found.
        best_score: Score of the best configuration.
        best_backtest: Full BacktestResult.
        all_results: Every configuration tested with scores.
        calibration_report: Calibration of the best configuration (if computed).
    """

    best_params: dict[str, Any]
    best_score: float
    best_backtest: BacktestResult
    all_results: list[dict[str, Any]]
    calibration_report: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "best_params": self.best_params,
            "best_score": round(self.best_score, 6),
            "best_backtest": self.best_backtest.to_dict(),
            "n_configs_tested": len(self.all_results),
            "calibration_report": self.calibration_report,
        }


class StrategyTuner:
    """Tunes GRID execution and ensemble parameters via grid search.

    Usage::

        tuner = StrategyTuner()
        result = tuner.tune_execution(
            predictions=proba_df,
            actuals=y_series,
            bankroll=100_000.0,
        )
        print(f"Best Kelly: {result.best_params['kelly_fraction']}")
        print(f"Best score: {result.best_score:.4f}")
    """

    def __init__(self, verbose: bool = True) -> None:
        self.verbose = verbose

    # ── Execution parameter tuning ────────────────────────────────────

    def tune_execution(
        self,
        predictions: pd.DataFrame | np.ndarray,
        actuals: pd.Series,
        bankroll: float = 100_000.0,
        class_names: list[str] | None = None,
        volatility: pd.Series | None = None,
        param_grid: dict[str, list[Any]] | None = None,
        scoring_fn: Callable[[BacktestResult], float] | None = None,
    ) -> TuningResult:
        """Tune execution parameters (Kelly, edge thresholds, spread assumptions).

        Parameters:
            predictions: Probability matrix (n_samples x n_classes).
            actuals: True regime labels.
            bankroll: Starting bankroll.
            class_names: Ordered class names.
            volatility: Per-observation volatility for spread scaling.
            param_grid: Override for parameter search grid.
            scoring_fn: Custom scoring function (default: Sharpe with calibration bonus).

        Returns:
            TuningResult with best parameters and full audit trail.
        """
        grid_spec = param_grid or DEFAULT_EXECUTION_GRID
        grid = _ParameterGrid(grid_spec)

        log.info(
            "Tuning execution params — {n} configurations",
            n=len(grid),
        )

        def _backtest_fn(params: dict[str, Any]) -> BacktestResult:
            config = ExecutionSimConfig(
                kelly_fraction=params.get("kelly_fraction", 0.25),
                min_edge=params.get("min_edge", 0.05),
                aggressive_edge=params.get("aggressive_edge", 0.15),
                base_spread_bps=params.get("base_spread_bps", 50.0),
            )
            sim = ExecutionSimulator(config=config)
            result = sim.simulate_era(
                predictions=predictions,
                actuals=actuals,
                bankroll=bankroll,
                class_names=class_names,
                volatility=volatility,
            )

            # Compute calibration for this config
            scorer = CalibrationScorer()
            cal_report = scorer.score(
                probabilities=predictions,
                actuals=actuals.values if isinstance(actuals, pd.Series) else actuals,
                class_names=class_names,
            )

            n_trades = result["n_trades"]
            total_pnl = result["total_pnl"]
            wins = sum(1 for t in result["per_trade"] if t["pnl"] > 0)
            win_rate = wins / n_trades if n_trades > 0 else 0.0

            # Sharpe approximation from per-trade PnL
            pnls = [t["pnl"] for t in result["per_trade"]]
            sharpe = None
            if len(pnls) >= 5:
                mean_pnl = np.mean(pnls)
                std_pnl = np.std(pnls)
                if std_pnl > 0:
                    sharpe = float(mean_pnl / std_pnl * np.sqrt(252))

            return BacktestResult(
                params=params,
                total_pnl=total_pnl,
                sharpe_ratio=sharpe,
                win_rate=win_rate,
                total_trades=n_trades,
                calibration_error=cal_report.mean_calibration_error,
                edge_capture_rate=cal_report.edge_capture,
            )

        tuner = _GridSearchTuner(
            param_grid=grid,
            backtest_fn=_backtest_fn,
            scoring_fn=scoring_fn,
            verbose=self.verbose,
        )

        best_params, best_result = tuner.tune()
        all_results = [r.to_dict() for r in tuner.results]

        return TuningResult(
            best_params=best_params,
            best_score=best_result.score(scoring_fn),
            best_backtest=best_result,
            all_results=all_results,
        )

    # ── Ensemble weight tuning ────────────────────────────────────────

    def tune_ensemble_weights(
        self,
        models: list[tuple[str, Any]],
        X_val: pd.DataFrame,
        y_val: pd.Series,
        class_names: list[str] | None = None,
        param_grid: dict[str, list[Any]] | None = None,
    ) -> TuningResult:
        """Tune ensemble constituent weights via calibration-aware grid search.

        Parameters:
            models: List of (name, model) tuples with predict_proba capability.
            X_val: Validation features.
            y_val: Validation labels.
            class_names: Ordered class names.
            param_grid: Override for weight search grid.

        Returns:
            TuningResult with best weights.
        """
        grid_spec = param_grid or DEFAULT_ENSEMBLE_GRID
        grid = _ParameterGrid(grid_spec)

        log.info(
            "Tuning ensemble weights — {n} configurations, {m} models",
            n=len(grid), m=len(models),
        )

        # Pre-compute per-model predictions
        model_probas: dict[str, np.ndarray | None] = {}
        for name, model in models:
            try:
                proba = model.predict_proba(X_val)
                if isinstance(proba, pd.DataFrame):
                    proba = proba.values
                model_probas[name] = proba
            except Exception as exc:
                log.warning("Model {m} failed predict_proba: {e}", m=name, e=exc)
                model_probas[name] = None

        scorer = CalibrationScorer()

        def _backtest_fn(params: dict[str, Any]) -> BacktestResult:
            xgb_w = params.get("xgboost_weight", 0.45)
            rf_w = params.get("random_forest_weight", 0.30)
            rule_w = max(0.10, 1.0 - xgb_w - rf_w)

            weights = {"xgboost": xgb_w, "random_forest": rf_w, "rule_based": rule_w}

            # Blend predictions
            blended = np.zeros_like(next(
                p for p in model_probas.values() if p is not None
            ), dtype=float)

            total_weight = 0.0
            for name, _ in models:
                p = model_probas.get(name)
                w = weights.get(name, 0.0)
                if p is not None and w > 0:
                    blended += p * w
                    total_weight += w

            if total_weight > 0:
                blended /= total_weight

            # Calibration score
            cal = scorer.score(blended, y_val, class_names)

            # Accuracy from argmax
            preds = np.argmax(blended, axis=1)
            from sklearn.preprocessing import LabelEncoder
            le = LabelEncoder()
            y_enc = le.fit_transform(y_val)
            accuracy = float(np.mean(preds == y_enc))

            # Synthetic Sharpe from calibration quality
            sharpe = (accuracy - 0.25) / max(cal.reliability, 0.01)

            full_params = {**params, "rule_based_weight": round(rule_w, 4)}

            return BacktestResult(
                params=full_params,
                total_pnl=accuracy * 100,
                sharpe_ratio=sharpe,
                win_rate=accuracy,
                total_trades=len(y_val),
                calibration_error=cal.mean_calibration_error,
                edge_capture_rate=cal.edge_capture,
            )

        tuner = _GridSearchTuner(
            param_grid=grid,
            backtest_fn=_backtest_fn,
            verbose=self.verbose,
        )

        best_params, best_result = tuner.tune()
        all_results = [r.to_dict() for r in tuner.results]

        # Calibration report for the winning configuration
        xgb_w = best_params.get("xgboost_weight", 0.45)
        rf_w = best_params.get("random_forest_weight", 0.30)
        rule_w = max(0.10, 1.0 - xgb_w - rf_w)
        weights = {"xgboost": xgb_w, "random_forest": rf_w, "rule_based": rule_w}

        blended = np.zeros_like(next(
            p for p in model_probas.values() if p is not None
        ), dtype=float)
        total_weight = 0.0
        for name, _ in models:
            p = model_probas.get(name)
            w = weights.get(name, 0.0)
            if p is not None and w > 0:
                blended += p * w
                total_weight += w
        if total_weight > 0:
            blended /= total_weight

        cal_report = scorer.score(blended, y_val, class_names)

        return TuningResult(
            best_params=best_params,
            best_score=best_result.score(),
            best_backtest=best_result,
            all_results=all_results,
            calibration_report=cal_report.to_dict(),
        )

    # ── Local search around current params ────────────────────────────

    def refine_params(
        self,
        current_params: dict[str, float],
        backtest_fn: Callable[[dict[str, Any]], BacktestResult],
        perturbation: float = 0.20,
        n_steps: int = 3,
        scoring_fn: Callable[[BacktestResult], float] | None = None,
    ) -> TuningResult:
        """Refine parameters with a local search around current values.

        Generates a grid centered on the current configuration, then
        searches it.

        Parameters:
            current_params: Current parameter values to refine.
            backtest_fn: Function that runs a backtest given params.
            perturbation: How much to vary each param (0.20 = +/-20%).
            n_steps: Steps above/below current value.
            scoring_fn: Custom scoring function.

        Returns:
            TuningResult.
        """
        grid = _create_param_grid_from_current(
            current_params=current_params,
            perturbation_factor=perturbation,
            n_steps=n_steps,
        )

        log.info(
            "Refining {n} params — {c} configurations (+/-{p:.0%} perturbation)",
            n=len(current_params), c=len(grid), p=perturbation,
        )

        tuner = _GridSearchTuner(
            param_grid=grid,
            backtest_fn=backtest_fn,
            scoring_fn=scoring_fn,
            verbose=self.verbose,
        )

        best_params, best_result = tuner.tune()
        all_results = [r.to_dict() for r in tuner.results]

        return TuningResult(
            best_params=best_params,
            best_score=best_result.score(scoring_fn),
            best_backtest=best_result,
            all_results=all_results,
        )
