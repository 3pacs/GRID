"""
Trained model abstractions for GRID inference.

Defines a base class and concrete implementations for regime classification
models that can be trained, serialized, and used for production inference.

Implementations:
- GradientBoostingRegimeClassifier (XGBoost)
- RandomForestRegimeClassifier (sklearn)
- RuleBasedClassifier (wraps existing weighted-threshold logic)
"""

from __future__ import annotations

import hashlib
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from loguru import logger as log


_ARTIFACTS_DIR = Path(__file__).parent.parent / "artifacts" / "models"
_ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)


class TrainedModelBase(ABC):
    """Base class for all trained GRID regime classification models."""

    model_type: str = "base"

    @abstractmethod
    def fit(self, X: pd.DataFrame, y: pd.Series, **kwargs) -> "TrainedModelBase":
        """Train the model on feature matrix X and labels y."""

    @abstractmethod
    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """Return predicted class labels."""

    @abstractmethod
    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        """Return class probability matrix (n_samples, n_classes)."""

    @abstractmethod
    def get_feature_importance(self) -> dict[str, float]:
        """Return feature name → importance score mapping."""

    @property
    @abstractmethod
    def classes_(self) -> np.ndarray:
        """Return the class labels known to the model."""

    @property
    def feature_names(self) -> list[str]:
        """Return ordered feature names used for training."""
        return getattr(self, "_feature_names", [])

    def save(self, name: str | None = None) -> Path:
        """Serialize model to disk. Returns the artifact path."""
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        fname = name or f"{self.model_type}_{ts}.joblib"
        path = _ARTIFACTS_DIR / fname
        joblib.dump(self, path)
        log.info("Model saved — {p} ({sz:.1f} KB)", p=path.name, sz=path.stat().st_size / 1024)
        return path

    @classmethod
    def load(cls, path: Path | str) -> "TrainedModelBase":
        """Load a serialized model from disk."""
        path = Path(path)
        model = joblib.load(path)
        if not isinstance(model, TrainedModelBase):
            raise TypeError(f"Loaded object is {type(model)}, not TrainedModelBase")
        return model

    @staticmethod
    def hash_artifact(path: Path | str) -> str:
        """Compute SHA-256 hash of a model artifact file."""
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()


class GradientBoostingRegimeClassifier(TrainedModelBase):
    """XGBoost-based regime classifier.

    Primary model in the ensemble — handles mixed feature types well
    and provides native feature importance via gain.
    """

    model_type = "xgboost"

    def __init__(
        self,
        max_depth: int = 4,
        n_estimators: int = 200,
        learning_rate: float = 0.05,
        early_stopping_rounds: int = 20,
        **kwargs,
    ) -> None:
        self.hyperparameters = {
            "max_depth": max_depth,
            "n_estimators": n_estimators,
            "learning_rate": learning_rate,
            "early_stopping_rounds": early_stopping_rounds,
            **kwargs,
        }
        self._model = None
        self._feature_names: list[str] = []

    def fit(self, X: pd.DataFrame, y: pd.Series, **kwargs) -> "GradientBoostingRegimeClassifier":
        from xgboost import XGBClassifier

        self._feature_names = list(X.columns)
        eval_set = kwargs.pop("eval_set", None)

        self._model = XGBClassifier(
            max_depth=self.hyperparameters["max_depth"],
            n_estimators=self.hyperparameters["n_estimators"],
            learning_rate=self.hyperparameters["learning_rate"],
            objective="multi:softprob",
            eval_metric="mlogloss",
            use_label_encoder=False,
            verbosity=0,
            random_state=42,
        )

        fit_kwargs = {}
        if eval_set is not None:
            fit_kwargs["eval_set"] = eval_set
            fit_kwargs["verbose"] = False
        self._model.fit(X, y, **fit_kwargs)
        log.info("XGBoost trained — {n} samples, {f} features, {c} classes",
                 n=len(X), f=len(self._feature_names), c=len(self._model.classes_))
        return self

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        return self._model.predict(X[self._feature_names])

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        return self._model.predict_proba(X[self._feature_names])

    def get_feature_importance(self) -> dict[str, float]:
        if self._model is None:
            return {}
        imp = self._model.feature_importances_
        return dict(zip(self._feature_names, imp.tolist()))

    @property
    def classes_(self) -> np.ndarray:
        return self._model.classes_ if self._model else np.array([])


class RandomForestRegimeClassifier(TrainedModelBase):
    """Random Forest regime classifier.

    Serves as the uncorrelated second opinion in the ensemble.
    """

    model_type = "random_forest"

    def __init__(
        self,
        n_estimators: int = 500,
        max_depth: int = 6,
        **kwargs,
    ) -> None:
        self.hyperparameters = {
            "n_estimators": n_estimators,
            "max_depth": max_depth,
            **kwargs,
        }
        self._model = None
        self._feature_names: list[str] = []

    def fit(self, X: pd.DataFrame, y: pd.Series, **kwargs) -> "RandomForestRegimeClassifier":
        from sklearn.ensemble import RandomForestClassifier

        self._feature_names = list(X.columns)
        self._model = RandomForestClassifier(
            n_estimators=self.hyperparameters["n_estimators"],
            max_depth=self.hyperparameters["max_depth"],
            random_state=42,
            n_jobs=-1,
        )
        self._model.fit(X, y)
        log.info("RandomForest trained — {n} samples, {f} features, {c} classes",
                 n=len(X), f=len(self._feature_names), c=len(self._model.classes_))
        return self

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        return self._model.predict(X[self._feature_names])

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        return self._model.predict_proba(X[self._feature_names])

    def get_feature_importance(self) -> dict[str, float]:
        if self._model is None:
            return {}
        imp = self._model.feature_importances_
        return dict(zip(self._feature_names, imp.tolist()))

    @property
    def classes_(self) -> np.ndarray:
        return self._model.classes_ if self._model else np.array([])


class RuleBasedClassifier(TrainedModelBase):
    """Wraps existing rule-based weighted-threshold logic as a TrainedModelBase.

    This allows the ensemble to include the current production model
    for backward compatibility.
    """

    model_type = "rule_based"

    def __init__(self, state_thresholds: dict[str, dict] | None = None) -> None:
        self.state_thresholds = state_thresholds or {}
        self._feature_names: list[str] = []
        self._classes = np.array([])

    def fit(self, X: pd.DataFrame, y: pd.Series, **kwargs) -> "RuleBasedClassifier":
        self._feature_names = list(X.columns)
        self._classes = np.array(sorted(y.unique()))
        return self

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        proba = self.predict_proba(X)
        return self._classes[np.argmax(proba, axis=1)]

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        n_classes = len(self._classes)
        n_samples = len(X)

        if not self.state_thresholds or n_classes == 0:
            return np.ones((n_samples, max(n_classes, 1))) / max(n_classes, 1)

        proba = np.zeros((n_samples, n_classes))
        class_to_idx = {c: i for i, c in enumerate(self._classes)}

        for i in range(n_samples):
            row = X.iloc[i]
            scores = {}
            for state, config in self.state_thresholds.items():
                weights = config.get("weights", {})
                score = sum(
                    weights.get(feat, 0) * (row.get(feat, 0) or 0)
                    for feat in weights
                )
                scores[state] = score

            # Softmax-like normalization
            if scores:
                max_score = max(abs(s) for s in scores.values()) or 1
                for state, score in scores.items():
                    idx = class_to_idx.get(state)
                    if idx is not None:
                        proba[i, idx] = np.exp(score / max_score)

            row_sum = proba[i].sum()
            if row_sum > 0:
                proba[i] /= row_sum
            else:
                proba[i] = 1.0 / n_classes

        return proba

    def get_feature_importance(self) -> dict[str, float]:
        importance: dict[str, float] = {}
        for config in self.state_thresholds.values():
            for feat, weight in config.get("weights", {}).items():
                importance[feat] = importance.get(feat, 0) + abs(weight)
        # Normalize
        total = sum(importance.values()) or 1
        return {k: v / total for k, v in importance.items()}

    @property
    def classes_(self) -> np.ndarray:
        return self._classes
