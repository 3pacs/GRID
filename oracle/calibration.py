"""
GRID Oracle Calibration — measures how well predicted probabilities
match actual outcomes.

A well-calibrated system that says "70% confident" should be right ~70%
of the time. This module computes calibration curves, Brier scores,
and reliability diagrams for the Oracle's predictions.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import numpy as np
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass
class CalibrationBucket:
    """One bucket in a calibration curve."""
    bin_start: float
    bin_end: float
    bin_midpoint: float
    predicted_mean: float      # Average predicted confidence in this bin
    actual_rate: float         # Fraction that actually hit
    count: int                 # Number of predictions in this bin


@dataclass
class CalibrationReport:
    """Full calibration report for the Oracle."""
    buckets: list[CalibrationBucket]
    brier_score: float         # 0 = perfect, 1 = worst
    calibration_error: float   # Mean absolute calibration error (ECE)
    sharpness: float           # Variance of predicted probabilities
    label: str                 # "well_calibrated" / "overconfident" / "underconfident"
    total_predictions: int
    overall_accuracy: float

    def to_dict(self) -> dict:
        return {
            "buckets": [
                {
                    "bin_start": b.bin_start,
                    "bin_end": b.bin_end,
                    "bin_midpoint": b.bin_midpoint,
                    "predicted_mean": round(b.predicted_mean, 4),
                    "actual_rate": round(b.actual_rate, 4),
                    "count": b.count,
                }
                for b in self.buckets
            ],
            "brier_score": round(self.brier_score, 4),
            "calibration_error": round(self.calibration_error, 4),
            "sharpness": round(self.sharpness, 4),
            "label": self.label,
            "total_predictions": self.total_predictions,
            "overall_accuracy": round(self.overall_accuracy, 4),
        }


def compute_calibration(
    engine: Engine,
    n_bins: int = 10,
    model_name: str | None = None,
    ticker: str | None = None,
) -> CalibrationReport:
    """Compute calibration curve from scored predictions.

    Args:
        engine: Database engine.
        n_bins: Number of bins for the calibration curve.
        model_name: Optional filter by model.
        ticker: Optional filter by ticker.

    Returns:
        CalibrationReport with buckets, Brier score, ECE, and label.
    """
    query = """
        SELECT confidence, verdict
        FROM oracle_predictions
        WHERE verdict IN ('hit', 'miss', 'partial')
    """
    params: dict[str, Any] = {}

    if model_name:
        query += " AND model_name = :model"
        params["model"] = model_name
    if ticker:
        query += " AND ticker = :ticker"
        params["ticker"] = ticker

    with engine.connect() as conn:
        rows = conn.execute(text(query), params).fetchall()

    if not rows:
        return CalibrationReport(
            buckets=[], brier_score=0.0, calibration_error=0.0,
            sharpness=0.0, label="insufficient_data",
            total_predictions=0, overall_accuracy=0.0,
        )

    confidences = []
    outcomes = []  # 1 = hit, 0.5 = partial, 0 = miss
    for r in rows:
        conf = float(r[0])
        verdict = r[1]
        outcome = 1.0 if verdict == "hit" else 0.5 if verdict == "partial" else 0.0
        confidences.append(conf)
        outcomes.append(outcome)

    confidences = np.array(confidences)
    outcomes = np.array(outcomes)

    # Brier score
    brier = float(np.mean((confidences - outcomes) ** 2))

    # Sharpness = variance of predicted probabilities
    sharpness = float(np.var(confidences))

    # Overall accuracy (hits + 0.5 * partials) / total
    overall_accuracy = float(np.mean(outcomes))

    # Bin predictions into calibration buckets
    bin_edges = np.linspace(0.0, 1.0, n_bins + 1)
    buckets: list[CalibrationBucket] = []
    weighted_errors = []

    for i in range(n_bins):
        lo, hi = bin_edges[i], bin_edges[i + 1]
        mask = (confidences >= lo) & (confidences < hi) if i < n_bins - 1 else (confidences >= lo) & (confidences <= hi)
        count = int(mask.sum())

        if count == 0:
            buckets.append(CalibrationBucket(
                bin_start=float(lo), bin_end=float(hi),
                bin_midpoint=float((lo + hi) / 2),
                predicted_mean=float((lo + hi) / 2),
                actual_rate=0.0, count=0,
            ))
            continue

        predicted_mean = float(np.mean(confidences[mask]))
        actual_rate = float(np.mean(outcomes[mask]))

        buckets.append(CalibrationBucket(
            bin_start=float(lo), bin_end=float(hi),
            bin_midpoint=float((lo + hi) / 2),
            predicted_mean=predicted_mean,
            actual_rate=actual_rate,
            count=count,
        ))

        weighted_errors.append(count * abs(predicted_mean - actual_rate))

    # Expected Calibration Error (ECE)
    total = len(confidences)
    ece = sum(weighted_errors) / total if total > 0 else 0.0

    # Determine label
    # Check if predictions are systematically too high or too low
    populated = [b for b in buckets if b.count > 0]
    if len(populated) < 3:
        label = "insufficient_data"
    else:
        # Compare predicted vs actual across populated bins
        over_count = sum(1 for b in populated if b.predicted_mean > b.actual_rate + 0.05)
        under_count = sum(1 for b in populated if b.predicted_mean < b.actual_rate - 0.05)
        if ece < 0.08:
            label = "well_calibrated"
        elif over_count > under_count:
            label = "overconfident"
        else:
            label = "underconfident"

    report = CalibrationReport(
        buckets=buckets,
        brier_score=brier,
        calibration_error=ece,
        sharpness=sharpness,
        label=label,
        total_predictions=total,
        overall_accuracy=overall_accuracy,
    )

    log.info(
        "Calibration: {n} predictions, ECE={ece:.3f}, Brier={b:.3f}, label={l}",
        n=total, ece=ece, b=brier, l=label,
    )

    return report
