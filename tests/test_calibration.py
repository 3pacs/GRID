"""Tests for grid.inference.calibration — autopredict calibration bridge."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from inference.calibration import CalibrationReport, CalibrationScorer, _ForecastProxy


# ── Fixtures ─────────────────────────────────────────────────────────

CLASS_NAMES = ["GROWTH", "NEUTRAL", "FRAGILE", "CRISIS"]


def _make_predictions(n: int = 100, seed: int = 42) -> tuple[np.ndarray, np.ndarray]:
    """Generate synthetic prediction probabilities and true labels."""
    rng = np.random.RandomState(seed)
    # Random probability matrix (n x 4), row-normalised
    raw = rng.dirichlet(alpha=[2, 3, 1, 0.5], size=n)
    # True labels: sample from class names weighted by probabilities
    actuals = np.array([
        CLASS_NAMES[rng.choice(4, p=raw[i])] for i in range(n)
    ])
    return raw, actuals


# ── CalibrationScorer.score ──────────────────────────────────────────

class TestCalibrationScorer:
    def test_basic_score(self):
        proba, actuals = _make_predictions(80)
        scorer = CalibrationScorer(num_buckets=5)
        report = scorer.score(proba, actuals, CLASS_NAMES)

        assert isinstance(report, CalibrationReport)
        assert report.n_predictions > 0
        assert 0.0 <= report.brier_score <= 1.0
        assert report.reliability >= 0.0
        assert report.resolution >= 0.0
        assert report.uncertainty >= 0.0

    def test_score_with_dataframe(self):
        proba, actuals = _make_predictions(60)
        df = pd.DataFrame(proba, columns=CLASS_NAMES)
        scorer = CalibrationScorer()
        report = scorer.score(df, pd.Series(actuals))

        assert isinstance(report, CalibrationReport)
        assert report.n_predictions > 0

    def test_perfect_calibration_has_low_reliability(self):
        """If predictions perfectly match outcomes, reliability should be near 0."""
        n = 200
        proba = np.zeros((n, 4))
        actuals = []
        for i in range(n):
            cls = i % 4
            proba[i, cls] = 0.90
            for j in range(4):
                if j != cls:
                    proba[i, j] = 0.10 / 3
            actuals.append(CLASS_NAMES[cls])

        scorer = CalibrationScorer(num_buckets=5)
        report = scorer.score(proba, np.array(actuals), CLASS_NAMES)
        # Should be well-calibrated when predictions match outcomes
        assert report.reliability < 0.10

    def test_empty_predictions(self):
        scorer = CalibrationScorer()
        # Force empty by providing mismatched labels
        proba = np.array([[0.5, 0.5]])
        actuals = np.array(["NONEXISTENT"])
        report = scorer.score(proba, actuals, CLASS_NAMES)
        assert report.n_predictions == 0
        assert report.recommendations == ["Insufficient data for calibration scoring."]

    def test_bucket_details_structure(self):
        proba, actuals = _make_predictions(100)
        scorer = CalibrationScorer(num_buckets=5)
        report = scorer.score(proba, actuals, CLASS_NAMES)

        for bucket in report.bucket_details:
            assert "range" in bucket
            assert "count" in bucket
            assert "avg_probability" in bucket
            assert "realized_rate" in bucket
            assert "calibration_error" in bucket

    def test_to_dict(self):
        proba, actuals = _make_predictions(50)
        report = CalibrationScorer().score(proba, actuals, CLASS_NAMES)
        d = report.to_dict()
        assert set(d.keys()) == {
            "brier_score", "reliability", "resolution", "uncertainty",
            "mean_calibration_error", "max_calibration_error",
            "n_predictions", "bucket_details", "edge_capture",
            "recommendations",
        }


# ── CalibrationScorer.score_shadow ───────────────────────────────────

class TestScoreShadow:
    def test_shadow_scoring(self):
        shadow_preds = [
            {"probabilities": {"GROWTH": 0.6, "NEUTRAL": 0.2, "FRAGILE": 0.1, "CRISIS": 0.1}},
            {"probabilities": {"GROWTH": 0.1, "NEUTRAL": 0.7, "FRAGILE": 0.1, "CRISIS": 0.1}},
            {"probabilities": {"GROWTH": 0.1, "NEUTRAL": 0.1, "FRAGILE": 0.7, "CRISIS": 0.1}},
        ]
        outcomes = ["GROWTH", "NEUTRAL", "CRISIS"]
        scorer = CalibrationScorer()
        report = scorer.score_shadow(shadow_preds, outcomes, CLASS_NAMES)

        assert isinstance(report, CalibrationReport)
        assert report.n_predictions > 0

    def test_empty_shadow(self):
        scorer = CalibrationScorer()
        report = scorer.score_shadow([], [], CLASS_NAMES)
        assert report.n_predictions == 0

    def test_mismatched_length(self):
        scorer = CalibrationScorer()
        report = scorer.score_shadow(
            [{"probabilities": {"GROWTH": 1.0}}],
            ["GROWTH", "NEUTRAL"],
            CLASS_NAMES,
        )
        assert report.n_predictions == 0


# ── CalibrationReport properties ─────────────────────────────────────

class TestCalibrationReport:
    def test_is_well_calibrated(self):
        report = CalibrationReport(
            brier_score=0.15, reliability=0.03, resolution=0.10,
            uncertainty=0.25, mean_calibration_error=0.02,
            max_calibration_error=0.05, n_predictions=100,
            bucket_details=[], edge_capture=0.9, recommendations=[],
        )
        assert report.is_well_calibrated is True

    def test_is_not_well_calibrated(self):
        report = CalibrationReport(
            brier_score=0.25, reliability=0.08, resolution=0.10,
            uncertainty=0.25, mean_calibration_error=0.06,
            max_calibration_error=0.15, n_predictions=100,
            bucket_details=[], edge_capture=0.9, recommendations=[],
        )
        assert report.is_well_calibrated is False

    def test_has_strong_resolution(self):
        report = CalibrationReport(
            brier_score=0.15, reliability=0.03, resolution=0.08,
            uncertainty=0.25, mean_calibration_error=0.02,
            max_calibration_error=0.05, n_predictions=100,
            bucket_details=[], edge_capture=0.9, recommendations=[],
        )
        assert report.has_strong_resolution is True

    def test_no_strong_resolution(self):
        report = CalibrationReport(
            brier_score=0.15, reliability=0.03, resolution=0.03,
            uncertainty=0.25, mean_calibration_error=0.02,
            max_calibration_error=0.05, n_predictions=100,
            bucket_details=[], edge_capture=0.9, recommendations=[],
        )
        assert report.has_strong_resolution is False


# ── _ForecastProxy ───────────────────────────────────────────────────

class TestForecastProxy:
    def test_attributes(self):
        fp = _ForecastProxy(probability=0.75, outcome=1)
        assert fp.probability == 0.75
        assert fp.outcome == 1
        assert fp.market_id == "grid"

    def test_custom_market_id(self):
        fp = _ForecastProxy(probability=0.5, outcome=0, market_id="custom")
        assert fp.market_id == "custom"
