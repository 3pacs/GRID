"""
AutoAgent scoring suite for GRID EOG signal hypothesis.

Evaluates the agent's signal_generator.py output against actual forward returns.
Produces a composite score (0.0-1.0) written to /logs/verifier/reward.txt.

Scoring weights:
  - Sharpe ratio of signal-gated returns:  40%
  - Hit rate (% correct BUY signals):      25%
  - Max drawdown during BUY periods:       15%
  - Information coefficient (IC):           10%
  - Parsimony (fewer features = better):    10%
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
PREDICTIONS_PATH = Path("/app/predictions.csv")
REWARD_PATH = Path("/logs/verifier/reward.txt")
REWARD_DETAIL_PATH = Path("/logs/verifier/reward_detail.json")
MIN_PREDICTIONS = 200  # Minimum predictions across all walk-forward windows
FORWARD_DAYS = 78      # Calendar days for forward return
MIN_WALK_FORWARD_WINDOWS = 4
BUY_AND_HOLD_SHARPE_BASELINE = 0.3  # Rough long-term EOG Sharpe

# Ensure output directory exists
REWARD_PATH.parent.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _load_predictions() -> pd.DataFrame:
    """Load and validate the agent's prediction output."""
    assert PREDICTIONS_PATH.exists(), (
        f"predictions.csv not found at {PREDICTIONS_PATH}. "
        "Agent must write predictions to /app/predictions.csv"
    )

    df = pd.read_csv(PREDICTIONS_PATH, parse_dates=["obs_date"])

    required_cols = {"obs_date", "signal", "confidence", "predicted_return"}
    missing = required_cols - set(df.columns)
    assert not missing, f"Missing columns in predictions.csv: {missing}"

    # Validate types
    assert df["signal"].isin(["BUY", "NO_BUY"]).all(), (
        "signal column must contain only 'BUY' or 'NO_BUY'"
    )
    assert df["confidence"].between(0, 1).all(), (
        "confidence must be between 0.0 and 1.0"
    )
    return df.sort_values("obs_date").reset_index(drop=True)


def _load_actual_returns() -> pd.DataFrame:
    """Pull actual EOG prices from GRID DB and compute forward returns."""
    sys.path.insert(0, "/app/files")
    from grid_bridge import GridBridge

    bridge = GridBridge()
    prices = bridge.get_eog_prices()
    prices = prices.set_index("obs_date").sort_index()

    # Compute 78-calendar-day forward return
    prices["fwd_return"] = prices["close"].shift(-FORWARD_DAYS) / prices["close"] - 1
    return prices.dropna(subset=["fwd_return"])


def _compute_sharpe(returns: pd.Series, annualization: float = 252.0) -> float:
    """Annualized Sharpe ratio from a series of daily returns."""
    if returns.std() == 0 or len(returns) < 10:
        return 0.0
    return float(returns.mean() / returns.std() * np.sqrt(annualization))


def _compute_max_drawdown(cumulative: pd.Series) -> float:
    """Maximum peak-to-trough drawdown."""
    if cumulative.empty:
        return 0.0
    peak = cumulative.expanding().max()
    drawdown = (cumulative - peak) / peak
    return float(drawdown.min())


def _score_component(value: float, thresholds: list[tuple[float, float]]) -> float:
    """Map a metric value to 0.0-1.0 using linear interpolation between thresholds.

    thresholds is a list of (metric_value, score) pairs, sorted ascending by metric_value.
    """
    if value <= thresholds[0][0]:
        return thresholds[0][1]
    if value >= thresholds[-1][0]:
        return thresholds[-1][1]
    for i in range(len(thresholds) - 1):
        lo_val, lo_score = thresholds[i]
        hi_val, hi_score = thresholds[i + 1]
        if lo_val <= value <= hi_val:
            frac = (value - lo_val) / (hi_val - lo_val) if hi_val != lo_val else 0
            return lo_score + frac * (hi_score - lo_score)
    return 0.0


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestSignalOutput:
    """Validate that the agent produced well-formed output."""

    def test_predictions_file_exists(self):
        assert PREDICTIONS_PATH.exists(), "predictions.csv not found"

    def test_predictions_have_required_columns(self):
        df = _load_predictions()
        for col in ["obs_date", "signal", "confidence", "predicted_return"]:
            assert col in df.columns, f"Missing column: {col}"

    def test_minimum_prediction_count(self):
        df = _load_predictions()
        assert len(df) >= MIN_PREDICTIONS, (
            f"Need at least {MIN_PREDICTIONS} predictions, got {len(df)}"
        )

    def test_has_buy_signals(self):
        df = _load_predictions()
        n_buys = (df["signal"] == "BUY").sum()
        assert n_buys >= 50, f"Need at least 50 BUY signals, got {n_buys}"

    def test_signal_values_valid(self):
        df = _load_predictions()
        assert df["signal"].isin(["BUY", "NO_BUY"]).all()

    def test_confidence_range(self):
        df = _load_predictions()
        assert df["confidence"].between(0, 1).all()

    def test_no_duplicate_dates(self):
        df = _load_predictions()
        assert not df["obs_date"].duplicated().any(), "Duplicate dates found"


class TestWalkForwardIntegrity:
    """Verify the agent used proper walk-forward methodology."""

    def test_predictions_sorted_chronologically(self):
        df = _load_predictions()
        assert df["obs_date"].is_monotonic_increasing, "Predictions not sorted by date"

    def test_date_range_reasonable(self):
        df = _load_predictions()
        date_range = (df["obs_date"].max() - df["obs_date"].min()).days
        assert date_range >= 365, (
            f"Prediction span is only {date_range} days; need at least 1 year"
        )

    def test_no_future_dates(self):
        df = _load_predictions()
        today = pd.Timestamp.now()
        future = df[df["obs_date"] > today]
        assert future.empty, f"Found {len(future)} predictions with future dates"


class TestSignalQuality:
    """Score the signal quality and write composite reward."""

    def test_score_and_write_reward(self):
        preds = _load_predictions()
        actuals = _load_actual_returns()

        # Merge predictions with actual forward returns
        preds_indexed = preds.set_index("obs_date")
        merged = preds_indexed.join(actuals[["fwd_return"]], how="inner")

        assert len(merged) >= 100, (
            f"Only {len(merged)} predictions matched actual data; need >= 100"
        )

        # --- Metric 1: Sharpe ratio of BUY-gated daily returns ---
        buy_mask = merged["signal"] == "BUY"
        buy_returns = merged.loc[buy_mask, "fwd_return"]
        all_returns = merged["fwd_return"]

        sharpe_signal = _compute_sharpe(buy_returns) if len(buy_returns) > 10 else 0.0
        sharpe_baseline = _compute_sharpe(all_returns)

        # Score: 0.0 if Sharpe <= 0, 1.0 if Sharpe >= 1.5
        sharpe_score = _score_component(sharpe_signal, [
            (0.0, 0.0), (0.3, 0.2), (0.6, 0.5), (1.0, 0.8), (1.5, 1.0),
        ])

        # --- Metric 2: Hit rate ---
        if len(buy_returns) > 0:
            hit_rate = float((buy_returns > 0).mean())
        else:
            hit_rate = 0.0

        hit_score = _score_component(hit_rate, [
            (0.4, 0.0), (0.5, 0.2), (0.55, 0.5), (0.6, 0.8), (0.7, 1.0),
        ])

        # --- Metric 3: Max drawdown ---
        if buy_mask.any():
            buy_cumulative = (1 + merged.loc[buy_mask, "fwd_return"]).cumprod()
            max_dd = _compute_max_drawdown(buy_cumulative)
        else:
            max_dd = -1.0

        # Score: 1.0 if dd > -0.1, 0.0 if dd < -0.5
        dd_score = _score_component(max_dd, [
            (-0.5, 0.0), (-0.3, 0.3), (-0.2, 0.6), (-0.1, 0.9), (0.0, 1.0),
        ])

        # --- Metric 4: Information coefficient ---
        if len(merged) > 30 and merged["confidence"].std() > 0:
            ic = float(merged["confidence"].corr(merged["fwd_return"]))
            ic = max(ic, 0.0)  # Negative IC is worse than zero
        else:
            ic = 0.0

        ic_score = _score_component(ic, [
            (0.0, 0.0), (0.02, 0.2), (0.05, 0.5), (0.1, 0.8), (0.15, 1.0),
        ])

        # --- Metric 5: Parsimony ---
        # Check how many features the agent used (from signal_generator.py)
        n_features = _count_features_used()
        parsimony_score = _score_component(n_features, [
            (5, 1.0), (10, 0.8), (15, 0.6), (20, 0.4), (30, 0.2), (50, 0.0),
        ])

        # --- Composite score ---
        composite = (
            0.40 * sharpe_score
            + 0.25 * hit_score
            + 0.15 * dd_score
            + 0.10 * ic_score
            + 0.10 * parsimony_score
        )

        # Write reward
        REWARD_PATH.write_text(f"{composite:.4f}\n")

        # Write detailed breakdown for debugging
        detail = {
            "composite_score": round(composite, 4),
            "components": {
                "sharpe": {
                    "value": round(sharpe_signal, 4),
                    "baseline": round(sharpe_baseline, 4),
                    "score": round(sharpe_score, 4),
                    "weight": 0.40,
                },
                "hit_rate": {
                    "value": round(hit_rate, 4),
                    "score": round(hit_score, 4),
                    "weight": 0.25,
                },
                "max_drawdown": {
                    "value": round(max_dd, 4),
                    "score": round(dd_score, 4),
                    "weight": 0.15,
                },
                "information_coefficient": {
                    "value": round(ic, 4),
                    "score": round(ic_score, 4),
                    "weight": 0.10,
                },
                "parsimony": {
                    "n_features": n_features,
                    "score": round(parsimony_score, 4),
                    "weight": 0.10,
                },
            },
            "stats": {
                "total_predictions": len(preds),
                "matched_with_actuals": len(merged),
                "n_buy_signals": int(buy_mask.sum()),
                "buy_pct": round(float(buy_mask.mean()), 4),
                "date_range": f"{merged.index.min()} to {merged.index.max()}",
            },
        }
        REWARD_DETAIL_PATH.write_text(json.dumps(detail, indent=2, default=str))

        # Assert minimum viable score — agent must beat random
        assert composite > 0.05, (
            f"Composite score {composite:.4f} is below minimum threshold. "
            f"Detail: {json.dumps(detail['components'], indent=2)}"
        )


def _count_features_used() -> int:
    """Count features used by inspecting signal_generator.py source."""
    gen_path = Path("/app/signal_generator.py")
    if not gen_path.exists():
        return 30  # Penalize missing file

    source = gen_path.read_text()

    # Look for get_features() calls with list arguments
    import re
    matches = re.findall(r"get_features\s*\(\s*\[([^\]]+)\]", source)
    if matches:
        # Count unique quoted strings across all get_features calls
        all_features = set()
        for match in matches:
            features = re.findall(r'["\']([^"\']+)["\']', match)
            all_features.update(features)
        return max(len(all_features), 1)

    # Fallback: count unique feature-like string literals
    feature_candidates = re.findall(r'["\'](\w+_(?:full|spot|ratio|stress|close))["\']', source)
    return max(len(set(feature_candidates)), 5)
