"""
Probability calibration scoring powered by autopredict.

Provides Brier score decomposition (reliability, resolution, uncertainty),
calibration curve analysis, and edge capture metrics for GRID's ensemble
classifier. Plugs into the shadow scoring pipeline so every model gets
continuous calibration feedback.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log

from autopredict.backtest.metrics import (
    BrierDecomposition,
    CalibrationAnalysis,
    CalibrationBucket,
    PredictionMarketMetrics,
)
from autopredict.learning.analyzer import PerformanceAnalyzer as APAnalyzer


@dataclass
class CalibrationReport:
    """Calibration report for a GRID model or ensemble.

    Attributes:
        brier_score: Overall Brier score (lower is better).
        reliability: Calibration component — how well probabilities match reality.
        resolution: Discrimination — how well predictions separate outcomes.
        uncertainty: Inherent dataset uncertainty.
        mean_calibration_error: Average absolute error across probability buckets.
        max_calibration_error: Worst-case bucket error.
        n_predictions: Total number of scored predictions.
        bucket_details: Per-bucket calibration statistics.
        edge_capture: Fraction of theoretical edge realised.
        recommendations: Actionable suggestions from autopredict's analyzer.
    """

    brier_score: float
    reliability: float
    resolution: float
    uncertainty: float
    mean_calibration_error: float
    max_calibration_error: float
    n_predictions: int
    bucket_details: list[dict[str, Any]]
    edge_capture: float
    recommendations: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "brier_score": self.brier_score,
            "reliability": self.reliability,
            "resolution": self.resolution,
            "uncertainty": self.uncertainty,
            "mean_calibration_error": self.mean_calibration_error,
            "max_calibration_error": self.max_calibration_error,
            "n_predictions": self.n_predictions,
            "bucket_details": self.bucket_details,
            "edge_capture": self.edge_capture,
            "recommendations": self.recommendations,
        }

    @property
    def is_well_calibrated(self) -> bool:
        """True when reliability < 0.05 (autopredict's 'good' threshold)."""
        return self.reliability < 0.05

    @property
    def has_strong_resolution(self) -> bool:
        """True when resolution > 0.05 (predictions discriminate outcomes)."""
        return self.resolution > 0.05


class CalibrationScorer:
    """Scores GRID model predictions using autopredict's calibration metrics.

    Works with both binary (prediction market style) and multi-class
    (regime classification) predictions by converting regime probabilities
    into per-class binary calibration problems.

    Usage::

        scorer = CalibrationScorer()
        report = scorer.score(
            probabilities=ensemble.predict_proba(X),
            actuals=y_true,
            class_names=["GROWTH", "NEUTRAL", "FRAGILE", "CRISIS"],
        )
        print(f"Brier: {report.brier_score:.4f}")
        print(f"Calibrated: {report.is_well_calibrated}")
    """

    def __init__(self, num_buckets: int = 10) -> None:
        self.num_buckets = num_buckets

    def score(
        self,
        probabilities: np.ndarray | pd.DataFrame,
        actuals: np.ndarray | pd.Series,
        class_names: list[str] | None = None,
    ) -> CalibrationReport:
        """Score model calibration.

        Parameters:
            probabilities: (n_samples, n_classes) probability matrix.
            actuals: True class labels (string or integer).
            class_names: Ordered class names matching probability columns.

        Returns:
            CalibrationReport with Brier decomposition and recommendations.
        """
        if isinstance(probabilities, pd.DataFrame):
            if class_names is None:
                class_names = [str(c) for c in probabilities.columns]
            probabilities = probabilities.values

        if isinstance(actuals, pd.Series):
            actuals = actuals.values

        n_samples, n_classes = probabilities.shape
        if class_names is None:
            class_names = [str(i) for i in range(n_classes)]

        log.info(
            "Scoring calibration — {n} samples, {c} classes",
            n=n_samples, c=n_classes,
        )

        # Convert to per-class binary calibration problems and aggregate
        from autopredict.backtest.metrics import PredictionMarketMetrics

        # Build ForecastRecord-compatible objects for autopredict
        # We create one "forecast" per sample using the predicted prob for the true class
        forecasts = []
        for i in range(n_samples):
            true_label = str(actuals[i])
            if true_label in class_names:
                true_idx = class_names.index(true_label)
            else:
                # Try numeric index
                try:
                    true_idx = int(actuals[i])
                except (ValueError, TypeError):
                    continue

            if true_idx >= n_classes:
                continue

            predicted_prob = float(probabilities[i, true_idx])
            forecasts.append(_ForecastProxy(probability=predicted_prob, outcome=1))

            # Also add the "miss" forecasts for other classes
            for j in range(n_classes):
                if j != true_idx:
                    forecasts.append(_ForecastProxy(
                        probability=float(probabilities[i, j]),
                        outcome=0,
                    ))

        if not forecasts:
            log.warning("No valid forecasts to score")
            return self._empty_report()

        # Use autopredict's calibration calculator
        cal_analysis = PredictionMarketMetrics.calculate_calibration(
            forecasts, num_buckets=self.num_buckets
        )

        # Edge capture: compare max-prob predictions to outcomes
        edge_capture = self._compute_edge_capture(probabilities, actuals, class_names)

        # Generate recommendations
        recommendations = self._generate_recommendations(cal_analysis, edge_capture)

        bucket_details = [
            {
                "range": b.range_str,
                "count": b.count,
                "avg_probability": round(b.avg_probability, 4),
                "realized_rate": round(b.realized_rate, 4),
                "calibration_error": round(b.calibration_error, 4),
            }
            for b in cal_analysis.buckets
        ]

        return CalibrationReport(
            brier_score=round(cal_analysis.overall_brier, 4),
            reliability=round(cal_analysis.brier_decomposition.reliability, 4),
            resolution=round(cal_analysis.brier_decomposition.resolution, 4),
            uncertainty=round(cal_analysis.brier_decomposition.uncertainty, 4),
            mean_calibration_error=round(cal_analysis.mean_absolute_calibration_error, 4),
            max_calibration_error=round(cal_analysis.max_calibration_error, 4),
            n_predictions=len(forecasts),
            bucket_details=bucket_details,
            edge_capture=round(edge_capture, 4),
            recommendations=recommendations,
        )

    def score_shadow(
        self,
        shadow_predictions: list[dict[str, Any]],
        outcomes: list[str],
        class_names: list[str],
    ) -> CalibrationReport:
        """Score shadow model predictions from the decision journal.

        Parameters:
            shadow_predictions: List of dicts with 'probabilities' key
                                (dict mapping class_name → prob).
            outcomes: List of true class labels.
            class_names: Ordered class names.

        Returns:
            CalibrationReport.
        """
        n = len(shadow_predictions)
        if n == 0 or n != len(outcomes):
            return self._empty_report()

        proba_matrix = np.zeros((n, len(class_names)))
        for i, pred in enumerate(shadow_predictions):
            probs = pred.get("probabilities", {})
            for j, cls in enumerate(class_names):
                proba_matrix[i, j] = probs.get(cls, 0.0)

        return self.score(proba_matrix, np.array(outcomes), class_names)

    def _compute_edge_capture(
        self,
        probabilities: np.ndarray,
        actuals: np.ndarray,
        class_names: list[str],
    ) -> float:
        """Compute edge capture rate (actual accuracy / predicted confidence)."""
        n = len(probabilities)
        if n == 0:
            return 0.0

        max_probs = probabilities.max(axis=1)
        pred_classes = probabilities.argmax(axis=1)

        correct = 0
        total_conf = 0.0
        for i in range(n):
            pred_idx = pred_classes[i]
            pred_label = class_names[pred_idx] if pred_idx < len(class_names) else str(pred_idx)
            if str(actuals[i]) == pred_label:
                correct += 1
            total_conf += max_probs[i]

        accuracy = correct / n
        avg_conf = total_conf / n

        if avg_conf == 0:
            return 0.0
        return accuracy / avg_conf

    def _generate_recommendations(
        self,
        cal_analysis: CalibrationAnalysis,
        edge_capture: float,
    ) -> list[str]:
        """Generate actionable recommendations from calibration analysis."""
        recs = []
        decomp = cal_analysis.brier_decomposition

        if decomp.reliability > 0.05:
            recs.append(
                f"Reliability is {decomp.reliability:.3f} (>0.05) — model probabilities "
                f"don't match outcomes. Consider Platt scaling or isotonic regression."
            )

        if decomp.resolution < 0.05:
            recs.append(
                f"Resolution is {decomp.resolution:.3f} (<0.05) — predictions don't "
                f"discriminate well between outcomes. Ensemble diversity may be too low."
            )

        if edge_capture < 0.7:
            recs.append(
                f"Edge capture is {edge_capture:.1%} — execution costs or model "
                f"overconfidence may be eating into returns."
            )

        if cal_analysis.max_calibration_error > 0.15:
            # Find worst bucket
            worst = max(cal_analysis.buckets, key=lambda b: b.calibration_error)
            recs.append(
                f"Worst calibration bucket is {worst.range_str} with "
                f"{worst.calibration_error:.1%} error over {worst.count} predictions."
            )

        return recs

    def _empty_report(self) -> CalibrationReport:
        return CalibrationReport(
            brier_score=0.0,
            reliability=0.0,
            resolution=0.0,
            uncertainty=0.0,
            mean_calibration_error=0.0,
            max_calibration_error=0.0,
            n_predictions=0,
            bucket_details=[],
            edge_capture=0.0,
            recommendations=["Insufficient data for calibration scoring."],
        )


class _ForecastProxy:
    """Minimal duck-type of autopredict's ForecastRecord for metrics."""

    __slots__ = ("probability", "outcome", "market_id")

    def __init__(self, probability: float, outcome: int, market_id: str = "grid") -> None:
        self.probability = probability
        self.outcome = outcome
        self.market_id = market_id
