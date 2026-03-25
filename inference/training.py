"""
PIT-correct model training pipeline for GRID.

Builds training datasets from the PITStore and GMM cluster labels,
then trains and validates models using walk-forward splits to prevent
lookahead bias.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from inference.trained_models import TrainedModelBase


class ModelTrainer:
    """Handles PIT-correct training data construction and walk-forward validation."""

    def __init__(self, db_engine: Engine, pit_store: Any, feature_lab: Any = None) -> None:
        self.engine = db_engine
        self.pit_store = pit_store
        self.feature_lab = feature_lab

    def build_training_set(
        self,
        feature_ids: list[int],
        start_date: date | str = "2005-01-01",
        end_date: date | str | None = None,
        label_source: str = "cluster",
        forward_return_days: int = 21,
    ) -> tuple[pd.DataFrame, pd.Series]:
        """Build PIT-correct feature matrix X and label vector y.

        Parameters:
            feature_ids: Feature IDs to include as columns.
            start_date: Training window start.
            end_date: Training window end (default: today).
            label_source: 'cluster' (GMM assignments) or 'forward_return' (bucketed returns).
            forward_return_days: Days ahead for forward return labels.

        Returns:
            (X, y) where X is a DataFrame of features and y is a Series of regime labels.
        """
        if end_date is None:
            end_date = date.today()
        start_date = date.fromisoformat(str(start_date)) if isinstance(start_date, str) else start_date
        end_date = date.fromisoformat(str(end_date)) if isinstance(end_date, str) else end_date

        log.info(
            "Building training set — {n} features, {s} to {e}, labels={l}",
            n=len(feature_ids), s=start_date, e=end_date, l=label_source,
        )

        # Get feature matrix from PIT store
        X = self.pit_store.get_feature_matrix(
            feature_ids=feature_ids,
            start_date=start_date,
            end_date=end_date,
        )

        if X is None or X.empty:
            raise ValueError("Empty feature matrix — check feature_ids and date range")

        # Map column names to feature names for readability
        feature_names = self._get_feature_names(feature_ids)
        col_map = {}
        for col in X.columns:
            col_str = str(col)
            for fid, fname in feature_names.items():
                if col_str == str(fid) or col_str == fname:
                    col_map[col] = fname
                    break
        if col_map:
            X = X.rename(columns=col_map)

        # Forward-fill then drop remaining NaNs
        X = X.ffill().dropna()

        if len(X) < 50:
            raise ValueError(f"Insufficient data after cleaning: {len(X)} rows (need >= 50)")

        # Build labels
        if label_source == "cluster":
            y = self._labels_from_clusters(X.index)
        elif label_source == "forward_return":
            y = self._labels_from_forward_returns(X.index, forward_return_days)
        else:
            raise ValueError(f"Unknown label_source: {label_source}")

        # Align X and y
        common_idx = X.index.intersection(y.index)
        X = X.loc[common_idx]
        y = y.loc[common_idx]

        # Drop any remaining NaN labels
        mask = y.notna()
        X = X[mask]
        y = y[mask]

        log.info(
            "Training set built — {n} samples, {f} features, {c} classes: {dist}",
            n=len(X), f=len(X.columns), c=y.nunique(),
            dist=y.value_counts().to_dict(),
        )

        return X, y

    def train_and_validate(
        self,
        model_class: type[TrainedModelBase],
        feature_ids: list[int],
        n_splits: int = 5,
        start_date: date | str = "2005-01-01",
        end_date: date | str | None = None,
        label_source: str = "cluster",
        **model_kwargs,
    ) -> dict[str, Any]:
        """Walk-forward train and validate a model.

        Splits the data into n_splits chronological folds. For each fold,
        trains on all prior data and evaluates on the fold.

        Returns:
            summary dict with per-fold and aggregate metrics, artifact path.
        """
        X, y = self.build_training_set(
            feature_ids=feature_ids,
            start_date=start_date,
            end_date=end_date,
            label_source=label_source,
        )

        # Walk-forward splits
        fold_size = len(X) // n_splits
        if fold_size < 20:
            raise ValueError(f"Too few samples per fold: {fold_size} (need >= 20)")

        era_results = []
        best_model = None
        best_accuracy = 0.0

        for fold in range(1, n_splits):
            train_end = fold * fold_size
            test_start = train_end
            test_end = min(test_start + fold_size, len(X))

            X_train = X.iloc[:train_end]
            y_train = y.iloc[:train_end]
            X_test = X.iloc[test_start:test_end]
            y_test = y.iloc[test_start:test_end]

            if len(X_train) < 30 or len(X_test) < 10:
                continue

            model = model_class(**model_kwargs)
            model.fit(X_train, y_train)

            preds = model.predict(X_test)
            proba = model.predict_proba(X_test)

            accuracy = float(np.mean(preds == y_test.values))
            confidence = float(np.mean(np.max(proba, axis=1)))

            # Per-class metrics
            from sklearn.metrics import classification_report
            report = classification_report(y_test, preds, output_dict=True, zero_division=0)

            era_results.append({
                "fold": fold,
                "train_size": len(X_train),
                "test_size": len(X_test),
                "accuracy": round(accuracy, 4),
                "mean_confidence": round(confidence, 4),
                "classification_report": report,
                "test_start": str(X_test.index[0])[:10] if hasattr(X_test.index[0], 'isoformat') else str(X_test.index[0]),
                "test_end": str(X_test.index[-1])[:10] if hasattr(X_test.index[-1], 'isoformat') else str(X_test.index[-1]),
            })

            if accuracy > best_accuracy:
                best_accuracy = accuracy
                best_model = model

        if not era_results:
            raise ValueError("No valid folds produced during walk-forward validation")

        # Train final model on all data
        final_model = model_class(**model_kwargs)
        final_model.fit(X, y)

        # Save artifact
        artifact_path = final_model.save()
        artifact_hash = TrainedModelBase.hash_artifact(artifact_path)

        avg_accuracy = np.mean([e["accuracy"] for e in era_results])
        avg_confidence = np.mean([e["mean_confidence"] for e in era_results])

        summary = {
            "model_type": final_model.model_type,
            "n_splits": n_splits,
            "n_samples": len(X),
            "n_features": len(X.columns),
            "feature_names": list(X.columns),
            "classes": list(y.unique()),
            "class_distribution": y.value_counts().to_dict(),
            "era_results": era_results,
            "avg_accuracy": round(float(avg_accuracy), 4),
            "avg_confidence": round(float(avg_confidence), 4),
            "best_fold_accuracy": round(float(best_accuracy), 4),
            "artifact_path": str(artifact_path),
            "artifact_hash": artifact_hash,
            "hyperparameters": getattr(final_model, "hyperparameters", {}),
            "feature_importance": final_model.get_feature_importance(),
        }

        log.info(
            "Walk-forward validation complete — avg_acc={a:.3f}, best={b:.3f}, artifact={p}",
            a=avg_accuracy, b=best_accuracy, p=artifact_path.name,
        )

        return summary

    # ── Label generation ──────────────────────────────────────────────

    def _labels_from_clusters(self, dates: pd.Index) -> pd.Series:
        """Get regime labels from the latest GMM cluster assignments."""
        with self.engine.connect() as conn:
            # Try analytical_snapshots first
            row = conn.execute(text(
                "SELECT result_data FROM analytical_snapshots "
                "WHERE snapshot_type = 'clustering' "
                "ORDER BY created_at DESC LIMIT 1"
            )).fetchone()

        if not row:
            raise ValueError("No clustering snapshot found — run ClusterDiscovery first")

        snap = json.loads(row[0]) if isinstance(row[0], str) else row[0]
        assignments = snap.get("assignments", [])

        if not assignments:
            raise ValueError("Clustering snapshot has no assignments")

        # Build date → cluster mapping
        cluster_map = {}
        for a in assignments:
            d = str(a.get("date", a.get("obs_date", "")))[:10]
            cluster_map[d] = a.get("cluster_id", a.get("cluster", 0))

        # Map cluster IDs to state names if available
        state_map = snap.get("state_map", {})

        labels = {}
        for d in dates:
            d_str = str(d)[:10]
            cluster_id = cluster_map.get(d_str)
            if cluster_id is not None:
                labels[d] = state_map.get(str(cluster_id), f"regime_{cluster_id}")

        return pd.Series(labels, name="regime_label")

    def _labels_from_forward_returns(self, dates: pd.Index, days: int = 21) -> pd.Series:
        """Generate labels from bucketed forward returns (SPY proxy)."""
        with self.engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT obs_date, value FROM resolved_series rs "
                "JOIN feature_registry fr ON rs.feature_id = fr.id "
                "WHERE fr.name = 'spy_close' OR fr.name = 'sp500' "
                "ORDER BY obs_date"
            )).fetchall()

        if not rows:
            raise ValueError("No SPY/SP500 data found for forward return labels")

        prices = pd.Series(
            {r[0]: float(r[1]) for r in rows},
        ).sort_index()

        # Forward returns
        fwd = prices.shift(-days) / prices - 1

        # Bucket into regime categories
        def _bucket(ret):
            if pd.isna(ret):
                return None
            if ret > 0.03:
                return "GROWTH"
            elif ret > -0.01:
                return "NEUTRAL"
            elif ret > -0.05:
                return "FRAGILE"
            else:
                return "CRISIS"

        labels = fwd.map(_bucket)
        labels.name = "regime_label"

        # Align with requested dates
        return labels.reindex(dates)

    # ── Helpers ───────────────────────────────────────────────────────

    def _get_feature_names(self, feature_ids: list[int]) -> dict[int, str]:
        """Map feature IDs to names."""
        if not feature_ids:
            return {}
        with self.engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT id, name FROM feature_registry WHERE id = ANY(:ids)"
            ), {"ids": feature_ids}).fetchall()
        return {r[0]: r[1] for r in rows}
