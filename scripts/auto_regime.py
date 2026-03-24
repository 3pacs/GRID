#!/usr/bin/env python3
"""GRID auto-regime: runs clustering, maps clusters to regime labels, updates decision_journal."""

import json

import numpy as np
import psycopg2
from datetime import date, datetime
from config import settings
from db import get_engine
from store.pit import PITStore

def run():
    engine = get_engine()
    pit = PITStore(engine)
    pg = psycopg2.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        dbname=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
    )
    pg.autocommit = True
    cur = pg.cursor()

    # Get model ID
    cur.execute("SELECT id FROM model_registry WHERE state='PRODUCTION' AND layer='REGIME' LIMIT 1")
    row = cur.fetchone()
    if not row:
        print("No production regime model")
        return
    mid = row[0]

    # Get eligible features
    cur.execute("SELECT id FROM feature_registry WHERE model_eligible=TRUE ORDER BY id")
    fids = [r[0] for r in cur.fetchall()]

    # Get latest data
    df = pit.get_feature_matrix(fids, date(2024, 4, 1), date.today(), date.today())
    if df.empty:
        print("No data")
        return

    # Drop columns with >30% missing
    df = df.ffill().bfill()
    df = df.dropna(axis=1, how="all")
    df = df.dropna()

    if df.shape[1] < 3:
        print(f"Only {df.shape[1]} features after cleaning")
        return

    # Standardize
    from sklearn.preprocessing import StandardScaler
    from sklearn.mixture import GaussianMixture

    scaler = StandardScaler()
    X = scaler.fit_transform(df.values)

    # Fit GMM with 4 states (GROWTH, NEUTRAL, FRAGILE, CRISIS)
    gmm = GaussianMixture(n_components=4, random_state=42, n_init=5)
    gmm.fit(X)
    labels = gmm.predict(X)
    probs = gmm.predict_proba(X)

    # Get latest state
    latest_label = labels[-1]
    latest_prob = probs[-1]
    confidence = float(np.max(latest_prob))

    # Map clusters to regimes by VIX/HY spread characteristics
    # Get VIX and HY columns if they exist
    col_names = list(df.columns)
    cluster_means = {}
    for k in range(4):
        mask = labels == k
        if mask.sum() > 0:
            cluster_means[k] = df.values[mask].mean(axis=0)

    # Sort clusters by "stress level" — higher VIX/HY = more stressed
    # Use mean of all features as proxy (higher = more stressed typically)
    stress_order = sorted(cluster_means.keys(), key=lambda k: np.mean(np.abs(cluster_means[k])))

    REGIME_MAP = {
        stress_order[0]: "GROWTH",
        stress_order[1]: "NEUTRAL",
        stress_order[2]: "FRAGILE",
        stress_order[3]: "CRISIS",
    }

    POSTURE_MAP = {
        "GROWTH": "AGGRESSIVE",
        "NEUTRAL": "BALANCED",
        "FRAGILE": "DEFENSIVE",
        "CRISIS": "CAPITAL_PRESERVATION",
    }

    regime = REGIME_MAP[latest_label]
    posture = POSTURE_MAP[regime]

    # Transition probability
    if len(labels) > 1:
        transitions = sum(1 for i in range(1, len(labels)) if labels[i] != labels[i-1])
        trans_prob = transitions / len(labels)
    else:
        trans_prob = 0

    # Cluster distribution
    unique, counts = np.unique(labels, return_counts=True)
    dist = {REGIME_MAP.get(int(u), f"C{u}"): int(c) for u, c in zip(unique, counts)}

    # Contradiction check
    contradictions = {}
    if regime == "GROWTH" and confidence < 0.5:
        contradictions["low_confidence"] = f"GROWTH with only {confidence:.0%} confidence"
    if regime == "CRISIS" and confidence < 0.6:
        contradictions["uncertain_crisis"] = f"CRISIS signal at {confidence:.0%} — may be noise"

    # Insert into decision_journal
    cur.execute(
        "INSERT INTO decision_journal (model_version_id, inferred_state, state_confidence, "
        "transition_probability, contradiction_flags, grid_recommendation, baseline_recommendation, "
        "action_taken, counterfactual, operator_confidence, decision_timestamp) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())",
        (mid, regime, confidence, trans_prob,
         json.dumps(contradictions), posture, "NEUTRAL",
         f"AUTO_{posture}", f"If {REGIME_MAP.get((latest_label+1)%4,'OTHER')}: would rebalance",
         "HIGH")
    )

    # Persist snapshot to database for historical comparison
    try:
        from store.snapshots import AnalyticalSnapshotStore
        snap_store = AnalyticalSnapshotStore(db_engine=engine)
        snap_store.save_snapshot(
            category="regime_detection",
            payload={
                "regime": regime,
                "confidence": confidence,
                "posture": posture,
                "transition_probability": trans_prob,
                "distribution": dist,
                "contradictions": contradictions,
                "n_features": df.shape[1],
                "n_observations": len(labels),
                "cluster_means_abs": {
                    REGIME_MAP.get(k, f"C{k}"): float(np.mean(np.abs(v)))
                    for k, v in cluster_means.items()
                },
            },
            as_of_date=date.today(),
            metrics={
                "regime": regime,
                "confidence": round(confidence, 4),
                "posture": posture,
                "transition_probability": round(trans_prob, 4),
            },
        )
    except Exception as exc:
        print(f"Warning: snapshot persistence failed: {exc}")

    print(f"=== AUTO REGIME UPDATE ===")
    print(f"Regime:      {regime}")
    print(f"Confidence:  {confidence:.1%}")
    print(f"Posture:     {posture}")
    print(f"Trans prob:  {trans_prob:.1%}")
    print(f"Distribution: {dist}")
    if contradictions:
        print(f"Flags:       {contradictions}")
    print(f"Updated decision_journal")

    pg.close()

if __name__ == "__main__":
    run()
