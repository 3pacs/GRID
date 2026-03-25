#!/usr/bin/env python3
"""
GRID — Train regime classification model.

Uses the ModelTrainer to build a PIT-correct training set from GMM
cluster labels, trains an XGBoost + RandomForest ensemble, validates
with walk-forward CV, and registers the result as a CANDIDATE model.

Usage:
    python scripts/train_regime_model.py
    python scripts/train_regime_model.py --label-source forward_return
    python scripts/train_regime_model.py --splits 7 --start 2010-01-01
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date
from pathlib import Path

_GRID_DIR = str(Path(__file__).resolve().parent.parent)
os.chdir(_GRID_DIR)
if _GRID_DIR not in sys.path:
    sys.path.insert(0, _GRID_DIR)

from loguru import logger as log


def main():
    parser = argparse.ArgumentParser(description="Train GRID regime model")
    parser.add_argument("--label-source", default="cluster", choices=["cluster", "forward_return"])
    parser.add_argument("--splits", type=int, default=5)
    parser.add_argument("--start", default="2005-01-01")
    parser.add_argument("--end", default=None)
    parser.add_argument("--layer", default="REGIME")
    parser.add_argument("--skip-ensemble", action="store_true", help="Train XGBoost only, skip ensemble")
    args = parser.parse_args()

    from db import get_engine
    from store.pit import PITStore
    from features.lab import FeatureLab
    from sqlalchemy import text

    engine = get_engine()
    pit = PITStore(engine)
    lab = FeatureLab(db_engine=engine, pit_store=pit)

    # Get model-eligible feature IDs
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT id, name FROM feature_registry WHERE model_eligible = TRUE ORDER BY id"
        )).fetchall()

    if not rows:
        print("ERROR: No model-eligible features found in feature_registry")
        sys.exit(1)

    feature_ids = [r[0] for r in rows]
    feature_names = {r[0]: r[1] for r in rows}
    print(f"Found {len(feature_ids)} model-eligible features")

    # Use orthogonal features if available
    try:
        from discovery.orthogonality import OrthogonalityAudit
        ortho = OrthogonalityAudit(db_engine=engine, pit_store=pit)
        ortho_result = ortho.get_orthogonal_features(corr_threshold=0.8)
        if ortho_result["orthogonal_ids"]:
            feature_ids = ortho_result["orthogonal_ids"]
            print(f"Filtered to {len(feature_ids)} orthogonal features "
                  f"(removed {len(ortho_result['redundant_pairs'])} redundant pairs)")
    except Exception as exc:
        print(f"Orthogonal filtering skipped: {exc}")

    # ── Train XGBoost ────────────────────────────────────────────────
    from inference.training import ModelTrainer
    from inference.trained_models import (
        GradientBoostingRegimeClassifier,
        RandomForestRegimeClassifier,
        RuleBasedClassifier,
    )

    trainer = ModelTrainer(db_engine=engine, pit_store=pit, feature_lab=lab)

    print("\n" + "=" * 60)
    print("TRAINING XGBoost REGIME CLASSIFIER")
    print("=" * 60)
    print(f"  Label source: {args.label_source}")
    print(f"  Walk-forward splits: {args.splits}")
    print(f"  Date range: {args.start} to {args.end or 'today'}")
    print(f"  Features: {len(feature_ids)}")
    print()

    try:
        xgb_result = trainer.train_and_validate(
            model_class=GradientBoostingRegimeClassifier,
            feature_ids=feature_ids,
            n_splits=args.splits,
            start_date=args.start,
            end_date=args.end,
            label_source=args.label_source,
        )
    except Exception as exc:
        print(f"\nXGBoost training FAILED: {exc}")
        log.error("XGBoost training failed: {e}", e=str(exc))
        sys.exit(1)

    print(f"\nXGBoost Results:")
    print(f"  Avg accuracy:  {xgb_result['avg_accuracy']:.4f}")
    print(f"  Avg confidence: {xgb_result['avg_confidence']:.4f}")
    print(f"  Best fold:     {xgb_result['best_fold_accuracy']:.4f}")
    print(f"  Classes:       {xgb_result['classes']}")
    print(f"  Artifact:      {xgb_result['artifact_path']}")

    print("\n  Per-fold results:")
    for era in xgb_result["era_results"]:
        print(f"    Fold {era['fold']}: acc={era['accuracy']:.4f} "
              f"conf={era['mean_confidence']:.4f} "
              f"({era['test_start']} to {era['test_end']})")

    print("\n  Top 10 feature importance:")
    imp = sorted(xgb_result["feature_importance"].items(), key=lambda x: -x[1])
    for feat, score in imp[:10]:
        print(f"    {feat:30s} {score:.4f}")

    if args.skip_ensemble:
        best_result = xgb_result
        model_type = "xgboost"
    else:
        # ── Train RandomForest ───────────────────────────────────────
        print("\n" + "=" * 60)
        print("TRAINING RandomForest REGIME CLASSIFIER")
        print("=" * 60)

        try:
            rf_result = trainer.train_and_validate(
                model_class=RandomForestRegimeClassifier,
                feature_ids=feature_ids,
                n_splits=args.splits,
                start_date=args.start,
                end_date=args.end,
                label_source=args.label_source,
            )
            print(f"\nRandomForest Results:")
            print(f"  Avg accuracy:  {rf_result['avg_accuracy']:.4f}")
            print(f"  Avg confidence: {rf_result['avg_confidence']:.4f}")
        except Exception as exc:
            print(f"\nRandomForest training FAILED: {exc} — using XGBoost only")
            rf_result = None

        # ── Build Ensemble ───────────────────────────────────────────
        if rf_result:
            print("\n" + "=" * 60)
            print("BUILDING ENSEMBLE")
            print("=" * 60)

            from inference.ensemble import EnsembleClassifier
            from inference.trained_models import TrainedModelBase

            xgb_model = TrainedModelBase.load(xgb_result["artifact_path"])
            rf_model = TrainedModelBase.load(rf_result["artifact_path"])
            rule_model = RuleBasedClassifier()

            ensemble = EnsembleClassifier(models=[
                ("xgboost", xgb_model, 0.45),
                ("random_forest", rf_model, 0.30),
                ("rule_based", rule_model, 0.25),
            ])

            # Quick fit rule-based on same training data to get classes
            X, y = trainer.build_training_set(
                feature_ids=feature_ids,
                start_date=args.start,
                end_date=args.end,
                label_source=args.label_source,
            )
            rule_model.fit(X, y)
            ensemble._classes_arr = xgb_model.classes_
            ensemble._feature_names = list(X.columns)

            ensemble_path = ensemble.save("ensemble_regime_latest.joblib")
            ensemble_hash = TrainedModelBase.hash_artifact(ensemble_path)

            print(f"  Ensemble saved: {ensemble_path}")
            print(f"  Constituents: {ensemble.constituent_summary}")

            best_result = xgb_result  # Use XGBoost metrics for registration
            best_result["ensemble_artifact_path"] = str(ensemble_path)
            best_result["ensemble_artifact_hash"] = ensemble_hash
            model_type = "ensemble"
        else:
            best_result = xgb_result
            model_type = "xgboost"

    # ── Register as CANDIDATE model ──────────────────────────────────
    print("\n" + "=" * 60)
    print("REGISTERING MODEL")
    print("=" * 60)

    try:
        from datetime import datetime, timezone

        version = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        name = f"regime_{model_type}_{version}"

        state_map = {}
        action_map = {
            "GROWTH": "BUY", "NEUTRAL": "HOLD",
            "FRAGILE": "REDUCE", "CRISIS": "SELL",
        }
        for i, cls in enumerate(best_result.get("classes", [])):
            state_map[str(i)] = str(cls)

        param_snapshot = json.dumps({
            "state_map": state_map,
            "action_map": action_map,
            "hyperparameters": best_result.get("hyperparameters", {}),
            "feature_names": best_result.get("feature_names", []),
        })

        with engine.begin() as conn:
            # Create model_registry entry
            model_row = conn.execute(text(
                "INSERT INTO model_registry "
                "(name, layer, version, state, hypothesis_id, feature_set, "
                " parameter_snapshot, model_type) "
                "VALUES (:name, :layer, :version, 'CANDIDATE', "
                " (SELECT id FROM hypothesis_registry WHERE state='PASSED' ORDER BY updated_at DESC LIMIT 1), "
                " :fset, :params, :mtype) "
                "RETURNING id"
            ), {
                "name": name,
                "layer": args.layer,
                "version": version,
                "fset": feature_ids,
                "params": param_snapshot,
                "mtype": model_type,
            }).fetchone()

            model_id = model_row[0]

            # Create model_artifacts entry
            artifact_path = best_result.get("ensemble_artifact_path") or best_result["artifact_path"]
            artifact_hash = best_result.get("ensemble_artifact_hash") or best_result["artifact_hash"]

            conn.execute(text(
                "INSERT INTO model_artifacts "
                "(model_id, artifact_path, artifact_hash, model_type, "
                " feature_names, hyperparameters, training_metrics, "
                " training_start_date, training_end_date) "
                "VALUES (:mid, :path, :hash, :mtype, :fnames, :hparams, "
                " :metrics, :start, :end)"
            ), {
                "mid": model_id,
                "path": artifact_path,
                "hash": artifact_hash,
                "mtype": model_type,
                "fnames": best_result.get("feature_names", []),
                "hparams": json.dumps(best_result.get("hyperparameters", {})),
                "metrics": json.dumps({
                    "avg_accuracy": best_result["avg_accuracy"],
                    "avg_confidence": best_result["avg_confidence"],
                    "best_fold_accuracy": best_result["best_fold_accuracy"],
                    "n_splits": best_result["n_splits"],
                    "n_samples": best_result["n_samples"],
                }),
                "start": args.start,
                "end": args.end or date.today().isoformat(),
            })

        print(f"  Registered model_id={model_id}")
        print(f"  Name: {name}")
        print(f"  State: CANDIDATE")
        print(f"  Type: {model_type}")
        print(f"  Layer: {args.layer}")
        print(f"\n  Next: promote to SHADOW via API:")
        print(f"    POST /api/v1/models/{model_id}/transition")
        print(f'    {{"new_state": "SHADOW", "reason": "Initial trained model"}}')

    except Exception as exc:
        print(f"\nModel registration FAILED: {exc}")
        log.error("Model registration failed: {e}", e=str(exc))
        sys.exit(1)

    # ── Email notification ───────────────────────────────────────────
    try:
        from alerts.email import alert_on_discovery_insight
        alert_on_discovery_insight(
            f"New {model_type.upper()} Model Trained",
            f"Model <strong>{name}</strong> registered as CANDIDATE.<br><br>"
            f"Accuracy: {best_result['avg_accuracy']:.1%} | "
            f"Confidence: {best_result['avg_confidence']:.1%} | "
            f"Features: {len(feature_ids)}<br><br>"
            f"Promote to SHADOW to begin shadow scoring.",
        )
    except Exception:
        pass

    print("\n" + "=" * 60)
    print("TRAINING COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
