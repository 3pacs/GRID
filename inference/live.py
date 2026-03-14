"""
GRID live inference module.

Runs live inference using the current production models and the latest
point-in-time data to generate trading recommendations.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from features.lab import FeatureLab
from store.pit import PITStore


class LiveInference:
    """Runs live inference using production models.

    Loads the latest PIT data, computes derived features, and generates
    a recommendation based on the active production model for each layer.

    Attributes:
        engine: SQLAlchemy engine for database access.
        pit_store: PITStore for latest data retrieval.
        feature_lab: FeatureLab for derived feature computation.
    """

    def __init__(self, db_engine: Engine, pit_store: PITStore) -> None:
        """Initialise live inference.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
            pit_store: PITStore instance for point-in-time data access.
        """
        self.engine = db_engine
        self.pit_store = pit_store
        self.feature_lab = FeatureLab(db_engine, pit_store)
        log.info("LiveInference initialised")

    def get_production_models(self) -> list[dict[str, Any]]:
        """Retrieve all production models, one per layer.

        Returns:
            list[dict]: Production model records with all fields.
        """
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT id, name, layer, version, feature_set, "
                    "parameter_snapshot, hypothesis_id "
                    "FROM model_registry WHERE state = 'PRODUCTION' "
                    "ORDER BY layer"
                )
            ).fetchall()

        models = []
        for row in rows:
            models.append({
                "id": row[0],
                "name": row[1],
                "layer": row[2],
                "version": row[3],
                "feature_set": row[4],
                "parameter_snapshot": row[5],
                "hypothesis_id": row[6],
            })

        log.info("Found {n} production models", n=len(models))
        return models

    def run_inference(self, as_of_date: date | None = None) -> dict[str, Any]:
        """Run live inference for all production models.

        Parameters:
            as_of_date: Decision date (default: today).

        Returns:
            dict: Inference results keyed by layer, each containing
                  the inferred state, confidence, recommendation, and
                  feature values used.
        """
        if as_of_date is None:
            as_of_date = date.today()

        log.info("Running live inference — as_of={d}", d=as_of_date)

        models = self.get_production_models()
        if not models:
            log.warning("No production models found — cannot run inference")
            return {"error": "No production models", "layers": {}}

        results: dict[str, Any] = {"as_of_date": as_of_date.isoformat(), "layers": {}}

        for model in models:
            layer = model["layer"]
            log.info("Inferring for layer={l}, model={m}", l=layer, m=model["name"])

            feature_ids = model["feature_set"]
            if not feature_ids:
                log.warning("Model {m} has empty feature set", m=model["name"])
                results["layers"][layer] = {
                    "model_id": model["id"],
                    "error": "Empty feature set",
                }
                continue

            # Get latest PIT values
            latest = self.pit_store.get_latest_values(feature_ids)
            if latest.empty:
                log.warning("No data available for model {m}", m=model["name"])
                results["layers"][layer] = {
                    "model_id": model["id"],
                    "error": "No data available",
                }
                continue

            # Compute derived features
            derived = self.feature_lab.compute_derived_features(as_of_date)

            # Build feature vector
            feature_vector: dict[str, float | None] = {}
            for _, row in latest.iterrows():
                feature_vector[f"feature_{int(row['feature_id'])}"] = float(row["value"])

            # Merge derived features
            feature_vector.update({
                k: v for k, v in derived.items() if v is not None
            })

            # Generate recommendation based on feature values
            recommendation = self._generate_recommendation(
                feature_vector, model["parameter_snapshot"]
            )

            results["layers"][layer] = {
                "model_id": model["id"],
                "model_name": model["name"],
                "model_version": model["version"],
                "feature_values": feature_vector,
                "n_features_available": len([v for v in feature_vector.values() if v is not None]),
                "n_features_expected": len(feature_ids),
                "recommendation": recommendation,
            }

        log.info("Live inference complete for {n} layers", n=len(results["layers"]))
        return results

    def _generate_recommendation(
        self,
        feature_vector: dict[str, float | None],
        parameter_snapshot: dict[str, Any],
    ) -> dict[str, Any]:
        """Generate a recommendation from feature values and model parameters.

        This is a framework method that produces a structured recommendation.
        The actual model logic depends on the specific model type stored in
        parameter_snapshot.

        Parameters:
            feature_vector: Current feature values.
            parameter_snapshot: Model parameters (thresholds, weights, etc.).

        Returns:
            dict: Recommendation with inferred_state, confidence, and
                  suggested_action.
        """
        # Count available features
        available = {k: v for k, v in feature_vector.items() if v is not None}
        total = len(feature_vector)
        coverage = len(available) / max(total, 1)

        # Default recommendation structure
        recommendation: dict[str, Any] = {
            "inferred_state": "UNKNOWN",
            "state_confidence": 0.0,
            "transition_probability": 0.0,
            "suggested_action": "HOLD",
            "feature_coverage": round(coverage, 4),
            "contradiction_flags": {},
        }

        if coverage < 0.5:
            recommendation["suggested_action"] = "HOLD — insufficient feature coverage"
            return recommendation

        # Apply thresholds from parameter_snapshot if available
        thresholds = parameter_snapshot.get("state_thresholds", {})
        if thresholds:
            scores: dict[str, float] = {}
            for state, state_config in thresholds.items():
                weights = state_config.get("weights", {})
                score = sum(
                    weights.get(k, 0) * v
                    for k, v in available.items()
                    if k in weights
                )
                scores[state] = score

            if scores:
                best_state = max(scores, key=scores.get)  # type: ignore[arg-type]
                total_score = sum(abs(s) for s in scores.values())
                confidence = abs(scores[best_state]) / max(total_score, 1e-10)

                recommendation["inferred_state"] = best_state
                recommendation["state_confidence"] = round(min(confidence, 1.0), 4)
                recommendation["suggested_action"] = state_config.get(
                    "action", "REVIEW"
                )

        return recommendation

    def get_feature_snapshot(self, as_of_date: date | None = None) -> pd.DataFrame:
        """Get a snapshot of all model-eligible features for reporting.

        Parameters:
            as_of_date: Decision date (default: today).

        Returns:
            pd.DataFrame: Feature snapshot with columns [name, family, value, obs_date].
        """
        if as_of_date is None:
            as_of_date = date.today()

        with self.engine.connect() as conn:
            features = conn.execute(
                text(
                    "SELECT id, name, family FROM feature_registry "
                    "WHERE model_eligible = TRUE ORDER BY family, name"
                )
            ).fetchall()

        feature_ids = [f[0] for f in features]
        feature_info = {f[0]: {"name": f[1], "family": f[2]} for f in features}

        latest = self.pit_store.get_latest_values(feature_ids)
        if latest.empty:
            return pd.DataFrame(columns=["name", "family", "value", "obs_date"])

        records: list[dict[str, Any]] = []
        for _, row in latest.iterrows():
            fid = int(row["feature_id"])
            info = feature_info.get(fid, {"name": f"feature_{fid}", "family": "unknown"})
            records.append({
                "name": info["name"],
                "family": info["family"],
                "value": row["value"],
                "obs_date": row["obs_date"],
            })

        return pd.DataFrame(records)


if __name__ == "__main__":
    from db import get_engine

    engine = get_engine()
    pit = PITStore(engine)
    li = LiveInference(engine, pit)

    result = li.run_inference()
    print(f"Inference result: {result}")

    snapshot = li.get_feature_snapshot()
    if not snapshot.empty:
        print(f"\nFeature snapshot ({len(snapshot)} features):")
        print(snapshot.to_string(index=False))
