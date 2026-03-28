"""Tests for grid.inference.tuning — autopredict strategy tuning bridge."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from inference.tuning import BacktestResult

from inference.tuning import (
    DEFAULT_ENSEMBLE_GRID,
    DEFAULT_EXECUTION_GRID,
    StrategyTuner,
    TuningResult,
)


CLASS_NAMES = ["GROWTH", "NEUTRAL", "FRAGILE", "CRISIS"]


def _make_data(n: int = 60, seed: int = 42):
    rng = np.random.RandomState(seed)
    proba = rng.dirichlet(alpha=[2, 3, 1, 0.5], size=n)
    df = pd.DataFrame(proba, columns=CLASS_NAMES)
    actuals = pd.Series(
        [CLASS_NAMES[rng.choice(4, p=proba[i])] for i in range(n)],
    )
    return df, actuals


# ── TuningResult ─────────────────────────────────────────────────────

class TestTuningResult:
    def test_to_dict(self):
        bt = BacktestResult(
            params={"kelly_fraction": 0.25},
            total_pnl=1000.0,
            sharpe_ratio=1.5,
            win_rate=0.55,
            total_trades=50,
            calibration_error=0.03,
            edge_capture_rate=0.80,
        )
        tr = TuningResult(
            best_params=bt.params,
            best_score=1.5,
            best_backtest=bt,
            all_results=[bt.to_dict()],
        )
        d = tr.to_dict()
        assert d["best_params"] == {"kelly_fraction": 0.25}
        assert d["n_configs_tested"] == 1


# ── StrategyTuner.tune_execution ─────────────────────────────────────

class TestTuneExecution:
    def test_small_grid(self):
        """Smoke test with a tiny grid to keep it fast."""
        df, actuals = _make_data(40)
        tuner = StrategyTuner(verbose=False)
        result = tuner.tune_execution(
            predictions=df,
            actuals=actuals,
            bankroll=50_000.0,
            class_names=CLASS_NAMES,
            param_grid={
                "kelly_fraction": [0.15, 0.25],
                "min_edge": [0.05],
                "aggressive_edge": [0.15],
                "base_spread_bps": [50.0],
            },
        )
        assert isinstance(result, TuningResult)
        assert "kelly_fraction" in result.best_params
        assert len(result.all_results) == 2

    def test_custom_scoring(self):
        df, actuals = _make_data(40)
        tuner = StrategyTuner(verbose=False)

        def pnl_scorer(r: BacktestResult) -> float:
            return r.total_pnl

        result = tuner.tune_execution(
            predictions=df,
            actuals=actuals,
            bankroll=50_000.0,
            class_names=CLASS_NAMES,
            param_grid={
                "kelly_fraction": [0.20],
                "min_edge": [0.03, 0.08],
                "aggressive_edge": [0.15],
                "base_spread_bps": [50.0],
            },
            scoring_fn=pnl_scorer,
        )
        assert isinstance(result, TuningResult)


# ── StrategyTuner.refine_params ──────────────────────────────────────

class TestRefineParams:
    def test_local_search(self):
        call_count = {"n": 0}

        def _mock_backtest(params):
            call_count["n"] += 1
            return BacktestResult(
                params=params,
                total_pnl=100.0 - abs(params["min_edge"] - 0.05) * 1000,
                sharpe_ratio=1.0,
                win_rate=0.5,
                total_trades=20,
                calibration_error=0.03,
                edge_capture_rate=0.8,
            )

        tuner = StrategyTuner(verbose=False)
        result = tuner.refine_params(
            current_params={"min_edge": 0.05},
            backtest_fn=_mock_backtest,
            perturbation=0.30,
            n_steps=2,
        )

        assert isinstance(result, TuningResult)
        assert call_count["n"] > 1
        # Best should be near 0.05
        assert 0.03 <= result.best_params["min_edge"] <= 0.07


# ── Default grids ────────────────────────────────────────────────────

class TestDefaultGrids:
    def test_execution_grid_has_expected_keys(self):
        assert "kelly_fraction" in DEFAULT_EXECUTION_GRID
        assert "min_edge" in DEFAULT_EXECUTION_GRID
        assert "aggressive_edge" in DEFAULT_EXECUTION_GRID
        assert "base_spread_bps" in DEFAULT_EXECUTION_GRID

    def test_ensemble_grid_has_expected_keys(self):
        assert "xgboost_weight" in DEFAULT_ENSEMBLE_GRID
        assert "random_forest_weight" in DEFAULT_ENSEMBLE_GRID
