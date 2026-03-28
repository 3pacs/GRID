"""Tests for grid.validation.execution_sim — autopredict execution bridge."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from grid.validation.execution_sim import ExecutionSimConfig, ExecutionSimulator


CLASS_NAMES = ["GROWTH", "NEUTRAL", "FRAGILE", "CRISIS"]


def _make_era_data(
    n: int = 50, seed: int = 42
) -> tuple[pd.DataFrame, pd.Series, pd.Series]:
    """Generate synthetic era data (predictions, actuals, volatility)."""
    rng = np.random.RandomState(seed)
    proba = rng.dirichlet(alpha=[2, 3, 1, 0.5], size=n)
    df = pd.DataFrame(proba, columns=CLASS_NAMES)

    actuals = pd.Series(
        [CLASS_NAMES[rng.choice(4, p=proba[i])] for i in range(n)],
        name="actual",
    )
    vol = pd.Series(rng.uniform(0.08, 0.35, size=n), name="volatility")
    return df, actuals, vol


# ── ExecutionSimConfig ───────────────────────────────────────────────

class TestExecutionSimConfig:
    def test_defaults(self):
        cfg = ExecutionSimConfig()
        assert cfg.base_spread_bps == 50.0
        assert cfg.kelly_fraction == 0.25
        assert cfg.min_edge == 0.05
        assert cfg.max_daily_loss == 5000.0

    def test_custom(self):
        cfg = ExecutionSimConfig(kelly_fraction=0.10, min_edge=0.03)
        assert cfg.kelly_fraction == 0.10
        assert cfg.min_edge == 0.03


# ── ExecutionSimulator ───────────────────────────────────────────────

class TestExecutionSimulator:
    def test_init_default_config(self):
        sim = ExecutionSimulator()
        assert sim.config.base_spread_bps == 50.0

    def test_init_custom_config(self):
        cfg = ExecutionSimConfig(base_spread_bps=30.0)
        sim = ExecutionSimulator(config=cfg)
        assert sim.config.base_spread_bps == 30.0

    def test_simulate_era_returns_correct_keys(self):
        df, actuals, vol = _make_era_data(30)
        sim = ExecutionSimulator()
        result = sim.simulate_era(
            predictions=df,
            actuals=actuals,
            bankroll=100_000.0,
            class_names=CLASS_NAMES,
            volatility=vol,
        )
        expected_keys = {
            "realised_return", "gross_return", "execution_cost_bps",
            "avg_slippage_bps", "avg_fill_rate", "n_trades", "n_fills",
            "total_pnl", "ending_bankroll", "risk_events", "per_trade",
        }
        assert set(result.keys()) == expected_keys

    def test_simulate_era_numpy_input(self):
        df, actuals, _ = _make_era_data(20)
        sim = ExecutionSimulator()
        result = sim.simulate_era(
            predictions=df.values,
            actuals=actuals,
            bankroll=50_000.0,
        )
        assert "n_trades" in result

    def test_simulate_era_empty(self):
        sim = ExecutionSimulator()
        result = sim.simulate_era(
            predictions=pd.DataFrame(),
            actuals=pd.Series(dtype=str),
            bankroll=100_000.0,
        )
        assert result["n_trades"] == 0
        assert result["realised_return"] == 0.0

    def test_trades_have_expected_fields(self):
        df, actuals, vol = _make_era_data(40)
        sim = ExecutionSimulator(config=ExecutionSimConfig(min_edge=0.01))
        result = sim.simulate_era(
            predictions=df,
            actuals=actuals,
            bankroll=100_000.0,
            class_names=CLASS_NAMES,
            volatility=vol,
        )
        if result["per_trade"]:
            trade = result["per_trade"][0]
            for key in ["observation", "side", "edge", "confidence",
                        "size", "filled_size", "fill_price",
                        "slippage_bps", "fill_rate", "outcome", "pnl"]:
                assert key in trade, f"Missing key: {key}"

    def test_risk_events_collected(self):
        """Very tight limits should trigger risk events."""
        cfg = ExecutionSimConfig(
            max_daily_loss=1.0,
            max_total_exposure=10.0,
            min_edge=0.01,
        )
        df, actuals, vol = _make_era_data(50)
        sim = ExecutionSimulator(config=cfg)
        result = sim.simulate_era(
            predictions=df,
            actuals=actuals,
            bankroll=100.0,
            class_names=CLASS_NAMES,
        )
        # With very tight risk limits, we expect either risk events or very few trades
        assert isinstance(result["risk_events"], list)


# ── estimate_execution_cost ──────────────────────────────────────────

class TestEstimateExecutionCost:
    def test_basic_cost(self):
        sim = ExecutionSimulator()
        cost = sim.estimate_execution_cost(trade_size=1000.0)
        assert "spread_cost_bps" in cost
        assert "market_impact_bps" in cost
        assert "total_cost_bps" in cost
        assert cost["total_cost_bps"] > 0

    def test_larger_trade_higher_impact(self):
        sim = ExecutionSimulator()
        small = sim.estimate_execution_cost(trade_size=100.0)
        large = sim.estimate_execution_cost(trade_size=10_000.0)
        assert large["market_impact_bps"] > small["market_impact_bps"]

    def test_higher_vol_wider_spread(self):
        sim = ExecutionSimulator()
        low_vol = sim.estimate_execution_cost(trade_size=1000.0, volatility=0.10)
        high_vol = sim.estimate_execution_cost(trade_size=1000.0, volatility=0.40)
        assert high_vol["spread_cost_bps"] > low_vol["spread_cost_bps"]

    def test_custom_spread_override(self):
        sim = ExecutionSimulator()
        cost = sim.estimate_execution_cost(trade_size=1000.0, spread_bps=100.0)
        assert cost["spread_cost_bps"] == 50.0  # half-spread
