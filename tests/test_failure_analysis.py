"""Tests for grid.inference.failure_analysis — autopredict failure regime bridge."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from grid.inference.failure_analysis import FailureAnalyzer, FailureDiagnostic


CLASS_NAMES = ["GROWTH", "NEUTRAL", "FRAGILE", "CRISIS"]


def _make_data(n: int = 60, seed: int = 42):
    rng = np.random.RandomState(seed)
    proba = rng.dirichlet(alpha=[2, 3, 1, 0.5], size=n)
    df = pd.DataFrame(proba, columns=CLASS_NAMES)
    actuals = pd.Series(
        [CLASS_NAMES[rng.choice(4, p=proba[i])] for i in range(n)],
    )
    return df, actuals


def _make_exec_results(n: int = 30, seed: int = 42):
    rng = np.random.RandomState(seed)
    per_trade = []
    for i in range(n):
        per_trade.append({
            "observation": i,
            "side": "buy" if rng.random() > 0.5 else "sell",
            "edge": rng.uniform(0.01, 0.20),
            "confidence": rng.uniform(0.5, 0.95),
            "size": rng.uniform(100, 1000),
            "filled_size": rng.uniform(50, 1000),
            "fill_price": rng.uniform(0.3, 0.7),
            "slippage_bps": rng.uniform(0, 30),
            "fill_rate": rng.uniform(0.5, 1.0),
            "outcome": "correct" if rng.random() > 0.4 else "wrong",
            "pnl": rng.uniform(-50, 100),
        })
    return {
        "realised_return": 0.05,
        "gross_return": 0.08,
        "execution_cost_bps": 25.0,
        "avg_slippage_bps": 12.0,
        "avg_fill_rate": 0.85,
        "n_trades": n,
        "n_fills": n - 3,
        "total_pnl": sum(t["pnl"] for t in per_trade),
        "ending_bankroll": 105000.0,
        "risk_events": [],
        "per_trade": per_trade,
    }


class TestFailureAnalyzerFromPredictions:
    def test_basic_analysis(self):
        df, actuals = _make_data(50)
        analyzer = FailureAnalyzer()
        diag = analyzer.from_predictions(df, actuals, CLASS_NAMES)

        assert isinstance(diag, FailureDiagnostic)
        assert diag.report.total_trades > 0
        assert isinstance(diag.failure_regimes, list)
        assert isinstance(diag.recommendations, list)
        assert isinstance(diag.by_regime, dict)

    def test_empty_data(self):
        analyzer = FailureAnalyzer()
        diag = analyzer.from_predictions(
            np.zeros((0, 4)),
            pd.Series(dtype=str),
            CLASS_NAMES,
        )
        assert diag.report.total_trades == 0
        assert "No trades to analyse." in diag.recommendations

    def test_to_dict(self):
        df, actuals = _make_data(40)
        diag = FailureAnalyzer().from_predictions(df, actuals, CLASS_NAMES)
        d = diag.to_dict()
        assert "total_trades" in d
        assert "failure_regimes" in d
        assert "recommendations" in d
        assert "by_regime" in d

    def test_by_regime_breakdown(self):
        df, actuals = _make_data(100)
        diag = FailureAnalyzer().from_predictions(df, actuals, CLASS_NAMES)
        # Should have at least some regimes represented
        assert len(diag.by_regime) > 0
        for regime, stats in diag.by_regime.items():
            assert "trades" in stats
            assert "pnl" in stats
            assert "win_rate" in stats
            assert "calibration_error" in stats


class TestFailureAnalyzerFromExecution:
    def test_from_exec_results(self):
        df, actuals = _make_data(30)
        exec_results = _make_exec_results(30)
        analyzer = FailureAnalyzer()
        diag = analyzer.from_execution_results(
            predictions=df,
            actuals=actuals,
            exec_results=exec_results,
            class_names=CLASS_NAMES,
        )
        assert diag.report.total_trades > 0

    def test_with_volatility(self):
        df, actuals = _make_data(30)
        exec_results = _make_exec_results(30)
        vol = pd.Series(np.random.uniform(0.1, 0.4, 30))
        analyzer = FailureAnalyzer()
        diag = analyzer.from_execution_results(
            predictions=df,
            actuals=actuals,
            exec_results=exec_results,
            class_names=CLASS_NAMES,
            volatility=vol,
        )
        assert diag.report.total_trades > 0

    def test_empty_exec_results(self):
        df, actuals = _make_data(10)
        diag = FailureAnalyzer().from_execution_results(
            predictions=df,
            actuals=actuals,
            exec_results={"per_trade": []},
            class_names=CLASS_NAMES,
        )
        assert diag.report.total_trades == 0


class TestFailureAnalyzerFromJournal:
    def test_from_journal(self):
        entries = pd.DataFrame({
            "inferred_state": ["GROWTH", "NEUTRAL", "CRISIS", "GROWTH", "FRAGILE"],
            "state_confidence": [0.85, 0.60, 0.75, 0.90, 0.55],
            "grid_recommendation": ["BUY", "HOLD", "SELL", "BUY", "REDUCE"],
            "outcome_value": [100.0, 0.0, -50.0, 200.0, -30.0],
            "verdict": ["HELPED", "NEUTRAL", "HARMED", "HELPED", "HARMED"],
            "decision_timestamp": pd.Timestamp.now(),
        })
        analyzer = FailureAnalyzer()
        diag = analyzer.from_journal_entries(entries, CLASS_NAMES)
        assert diag.report.total_trades > 0

    def test_empty_journal(self):
        entries = pd.DataFrame(columns=[
            "inferred_state", "state_confidence", "grid_recommendation",
            "outcome_value", "verdict", "decision_timestamp",
        ])
        diag = FailureAnalyzer().from_journal_entries(entries)
        assert diag.report.total_trades == 0
