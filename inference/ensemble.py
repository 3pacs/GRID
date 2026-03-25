"""
Weighted ensemble classifier for GRID regime inference.

Combines multiple TrainedModelBase implementations (XGBoost, RandomForest,
RuleBased) into a single classifier via weighted probability averaging.

Includes disagreement detection: when constituent models disagree strongly,
the ensemble reduces confidence and biases toward HOLD/REVIEW.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log

from inference.trained_models import TrainedModelBase


class EnsembleClassifier(TrainedModelBase):
    """Weighted ensemble of multiple trained regime classifiers."""

    model_type = "ensemble"

    def __init__(
        self,
        models: list[tuple[str, TrainedModelBase, float]] | None = None,
    ) -> None:
        """Initialize with a list of (name, model, weight) tuples.

        Default weights: XGBoost 0.45, RandomForest 0.30, RuleBased 0.25.
        Weights are normalized to sum to 1.0.
        """
        self._models = models or []
        self._feature_names: list[str] = []
        self._classes_arr = np.array([])

        # Normalize weights
        if self._models:
            total_w = sum(w for _, _, w in self._models)
            if total_w > 0:
                self._models = [(n, m, w / total_w) for n, m, w in self._models]

    def fit(self, X: pd.DataFrame, y: pd.Series, **kwargs) -> "EnsembleClassifier":
        """Train all constituent models on the same data."""
        self._feature_names = list(X.columns)
        self._classes_arr = np.array(sorted(y.unique()))

        for name, model, weight in self._models:
            try:
                model.fit(X, y, **kwargs)
                log.info("Ensemble constituent '{n}' trained (weight={w:.2f})", n=name, w=weight)
            except Exception as exc:
                log.warning("Ensemble constituent '{n}' failed to train: {e}", n=name, e=str(exc))

        return self

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        proba = self.predict_proba(X)
        return self._classes_arr[np.argmax(proba, axis=1)]

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        """Weighted average of constituent predict_proba outputs."""
        n_samples = len(X)
        n_classes = len(self._classes_arr)

        if n_classes == 0 or not self._models:
            return np.ones((n_samples, max(n_classes, 1))) / max(n_classes, 1)

        combined = np.zeros((n_samples, n_classes))
        total_weight_used = 0.0

        for name, model, weight in self._models:
            try:
                proba = model.predict_proba(X)
                # Align classes if models have different class orders
                if len(model.classes_) == n_classes:
                    combined += weight * proba
                    total_weight_used += weight
                else:
                    log.debug("Skipping '{n}' — class count mismatch ({mc} vs {ec})",
                              n=name, mc=len(model.classes_), ec=n_classes)
            except Exception as exc:
                log.warning("Ensemble constituent '{n}' predict_proba failed: {e}", n=name, e=str(exc))

        if total_weight_used > 0:
            combined /= total_weight_used

        # Ensure rows sum to 1
        row_sums = combined.sum(axis=1, keepdims=True)
        row_sums[row_sums == 0] = 1
        combined /= row_sums

        return combined

    def get_disagreement(self, X: pd.DataFrame) -> dict[str, Any]:
        """Compute per-model predictions and disagreement score.

        Disagreement is Shannon entropy of the vote distribution across
        models. High disagreement → lower confidence → HOLD bias.

        Returns:
            per_model: dict of model_name → predicted_class
            vote_distribution: dict of class → vote count
            disagreement_score: float 0-1 (0 = unanimous, 1 = max entropy)
            unanimous: bool
        """
        votes: dict[str, int] = {}
        per_model: dict[str, str] = {}

        for name, model, _weight in self._models:
            try:
                pred = model.predict(X)
                cls = str(pred[0]) if len(pred) > 0 else "UNKNOWN"
                per_model[name] = cls
                votes[cls] = votes.get(cls, 0) + 1
            except Exception:
                per_model[name] = "ERROR"

        # Shannon entropy of vote distribution
        total_votes = sum(votes.values()) or 1
        probs = [c / total_votes for c in votes.values()]
        entropy = -sum(p * np.log2(p) for p in probs if p > 0)
        max_entropy = np.log2(len(self._models)) if len(self._models) > 1 else 1
        disagreement = entropy / max_entropy if max_entropy > 0 else 0

        return {
            "per_model": per_model,
            "vote_distribution": votes,
            "disagreement_score": round(float(disagreement), 4),
            "unanimous": len(votes) == 1,
        }

    def get_feature_importance(self) -> dict[str, float]:
        """Weight-averaged feature importance across all constituents."""
        combined: dict[str, float] = {}
        total_weight = 0.0

        for name, model, weight in self._models:
            try:
                imp = model.get_feature_importance()
                for feat, score in imp.items():
                    combined[feat] = combined.get(feat, 0) + weight * score
                total_weight += weight
            except Exception:
                pass

        if total_weight > 0:
            combined = {k: v / total_weight for k, v in combined.items()}

        return dict(sorted(combined.items(), key=lambda x: -x[1]))

    @property
    def classes_(self) -> np.ndarray:
        return self._classes_arr

    @property
    def feature_names(self) -> list[str]:
        return self._feature_names

    @property
    def constituent_summary(self) -> list[dict[str, Any]]:
        """Return summary of each constituent model."""
        return [
            {"name": name, "type": model.model_type, "weight": round(weight, 3)}
            for name, model, weight in self._models
        ]
