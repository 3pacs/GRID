"""Feature association discovery endpoints.

Exposes deep relationships between features: correlation matrices,
lag analysis, cluster assignments, regime fingerprints, and anomalies.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine, get_pit_store

router = APIRouter(prefix="/api/v1/associations", tags=["associations"])


def _get_feature_registry(engine) -> pd.DataFrame:
    """Return model-eligible features with id and name."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT id, name FROM feature_registry "
                "WHERE model_eligible = TRUE ORDER BY id"
            )
        ).fetchall()
    return pd.DataFrame(rows, columns=["id", "name"])


def _build_feature_matrix(
    pit_store,
    feature_ids: list[int],
    days: int = 252,
) -> pd.DataFrame:
    """Build a PIT-correct feature matrix for the last N days.

    Parameters:
        pit_store: PITStore instance.
        feature_ids: List of feature IDs.
        days: Lookback window in calendar days.

    Returns:
        Wide DataFrame indexed by obs_date with feature columns.
    """
    today = date.today()
    start = today - timedelta(days=days)
    matrix = pit_store.get_feature_matrix(
        feature_ids=feature_ids,
        start_date=start,
        end_date=today,
        as_of_date=today,
        vintage_policy="LATEST_AS_OF",
    )
    if not matrix.empty:
        matrix = matrix.ffill(limit=5).dropna()
    return matrix


@router.get("/correlation-matrix")
async def get_correlation_matrix(
    days: int = Query(default=252, ge=30, le=1000),
    min_corr: float = Query(default=0.3, ge=0.0, le=1.0),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return feature correlation matrix and strong pairs.

    Parameters:
        days: Lookback period in calendar days.
        min_corr: Minimum |correlation| for strong_pairs.
    """
    engine = get_db_engine()
    pit_store = get_pit_store()

    registry = _get_feature_registry(engine)
    if registry.empty:
        return {"features": [], "matrix": [], "strong_pairs": []}

    feature_ids = registry["id"].tolist()
    id_to_name = dict(zip(registry["id"], registry["name"]))

    matrix = _build_feature_matrix(pit_store, feature_ids, days)
    if matrix.empty or matrix.shape[1] < 2:
        return {"features": [], "matrix": [], "strong_pairs": []}

    # Rename columns from feature_id to feature_name
    matrix.columns = [id_to_name.get(c, str(c)) for c in matrix.columns]
    features = list(matrix.columns)

    corr = matrix.corr()
    corr_values = corr.values.tolist()

    # Replace NaN with 0 for JSON serialisation
    corr_values = [
        [0.0 if (v != v) else round(v, 4) for v in row]
        for row in corr_values
    ]

    # Extract strong pairs
    strong_pairs: list[dict[str, Any]] = []
    for i in range(len(features)):
        for j in range(i + 1, len(features)):
            c = corr.iloc[i, j]
            if c != c:  # NaN check
                continue
            if abs(c) >= min_corr:
                strong_pairs.append({
                    "a": features[i],
                    "b": features[j],
                    "corr": round(float(c), 4),
                })
    strong_pairs.sort(key=lambda p: abs(p["corr"]), reverse=True)

    log.info(
        "Correlation matrix: {n} features, {p} strong pairs",
        n=len(features),
        p=len(strong_pairs),
    )

    return {
        "features": features,
        "matrix": corr_values,
        "strong_pairs": strong_pairs,
    }


@router.get("/lag-analysis")
async def get_lag_analysis(
    feature_a: str = Query(..., description="Name of first feature"),
    feature_b: str = Query(..., description="Name of second feature"),
    max_lag: int = Query(default=10, ge=1, le=60),
    days: int = Query(default=504, ge=60, le=2000),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Compute cross-correlation between two features at different lags.

    Identifies whether feature_a leads feature_b or vice versa.
    """
    engine = get_db_engine()
    pit_store = get_pit_store()

    # Look up feature IDs by name
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT id, name FROM feature_registry "
                "WHERE name IN (:name_a, :name_b) AND model_eligible = TRUE"
            ),
            {"name_a": feature_a, "name_b": feature_b},
        ).fetchall()

    name_to_id = {row[1]: row[0] for row in rows}
    if feature_a not in name_to_id or feature_b not in name_to_id:
        raise HTTPException(
            status_code=404,
            detail=f"Feature(s) not found. Available: {list(name_to_id.keys())}",
        )

    feature_ids = [name_to_id[feature_a], name_to_id[feature_b]]
    matrix = _build_feature_matrix(pit_store, feature_ids, days)

    if matrix.empty or matrix.shape[1] < 2:
        raise HTTPException(status_code=404, detail="Insufficient data for lag analysis")

    # Map columns to names
    id_to_name = {v: k for k, v in name_to_id.items()}
    matrix.columns = [id_to_name.get(c, str(c)) for c in matrix.columns]

    series_a = matrix[feature_a].dropna()
    series_b = matrix[feature_b].dropna()

    # Align on common dates
    common = series_a.index.intersection(series_b.index)
    if len(common) < max_lag + 10:
        raise HTTPException(status_code=400, detail="Not enough overlapping data points")

    series_a = series_a.loc[common]
    series_b = series_b.loc[common]

    # Standardise
    a_std = (series_a - series_a.mean()) / (series_a.std() + 1e-12)
    b_std = (series_b - series_b.mean()) / (series_b.std() + 1e-12)

    lags: list[dict[str, Any]] = []
    best_corr = 0.0
    best_lag = 0

    for lag in range(-max_lag, max_lag + 1):
        if lag > 0:
            cc = float(np.corrcoef(a_std.values[lag:], b_std.values[:-lag])[0, 1])
        elif lag < 0:
            cc = float(np.corrcoef(a_std.values[:lag], b_std.values[-lag:])[0, 1])
        else:
            cc = float(np.corrcoef(a_std.values, b_std.values)[0, 1])

        if cc != cc:  # NaN
            cc = 0.0

        lags.append({"lag": lag, "correlation": round(cc, 4)})
        if abs(cc) > abs(best_corr):
            best_corr = cc
            best_lag = lag

    if best_lag > 0:
        direction = f"{feature_a} leads {feature_b} by {best_lag} days"
    elif best_lag < 0:
        direction = f"{feature_b} leads {feature_a} by {-best_lag} days"
    else:
        direction = "Contemporaneous relationship"

    return {
        "feature_a": feature_a,
        "feature_b": feature_b,
        "lags": lags,
        "optimal_lag": best_lag,
        "direction": direction,
        "strength": round(abs(best_corr), 4),
    }


@router.get("/clusters")
async def get_clusters(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return latest clustering results with assignments and transitions.

    Fetches from the in-memory discovery job cache, or runs a
    lightweight computation if no cached result exists.
    """
    from api.routers.discovery import _jobs

    # Try to find most recent completed clustering job
    cluster_result = None
    for job in sorted(_jobs.values(), key=lambda j: j["started"], reverse=True):
        if job["type"] == "clustering" and job["status"] == "complete":
            cluster_result = job["result"]
            break

    if cluster_result is None or "error" in cluster_result:
        return {
            "clusters": [],
            "transition_matrix": [],
            "n_clusters": 0,
            "message": "No completed clustering run found. Trigger from Discovery page.",
        }

    best_k = cluster_result.get("best_k", 0)
    transition = cluster_result.get("transition_matrix", [])

    # Build cluster summary from metrics
    all_metrics = cluster_result.get("all_metrics", [])
    best_metrics = None
    for m in all_metrics:
        if m.get("k") == best_k:
            best_metrics = m
            break

    regime_labels = ["GROWTH", "NEUTRAL", "FRAGILE", "CRISIS", "RECOVERY", "UNKNOWN"]
    clusters: list[dict[str, Any]] = []
    for i in range(best_k):
        label = regime_labels[i] if i < len(regime_labels) else f"CLUSTER_{i}"
        clusters.append({
            "id": i,
            "label": label,
            "feature_count": cluster_result.get("pca_components_used", 0),
            "persistence": best_metrics.get("gmm_persistence", 0) if best_metrics else 0,
        })

    # Compute inter-cluster distances from transition matrix
    distances: list[dict[str, Any]] = []
    if transition:
        for i in range(len(transition)):
            for j in range(i + 1, len(transition)):
                # Use 1 - average transition probability as a distance proxy
                avg_prob = (
                    (transition[i][j] if j < len(transition[i]) else 0)
                    + (transition[j][i] if i < len(transition[j]) else 0)
                ) / 2
                distances.append({
                    "from": i,
                    "to": j,
                    "distance": round(1.0 - avg_prob, 4),
                    "transition_prob": round(avg_prob, 4),
                })

    return {
        "clusters": clusters,
        "n_clusters": best_k,
        "transition_matrix": transition,
        "inter_cluster_distances": distances,
        "variance_explained": cluster_result.get("variance_explained", 0),
        "n_observations": cluster_result.get("n_observations", 0),
    }


@router.get("/regime-features")
async def get_regime_features(
    days: int = Query(default=504, ge=60, le=2000),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """For each regime, return which features are most characteristic.

    Computes mean z-score of each feature during each regime period.
    """
    engine = get_db_engine()
    pit_store = get_pit_store()

    # Get regime history from decision_journal
    with engine.connect() as conn:
        regime_rows = conn.execute(
            text(
                "SELECT DATE(decision_timestamp) AS dt, inferred_state "
                "FROM decision_journal "
                "WHERE decision_timestamp >= NOW() - make_interval(days => :days) "
                "ORDER BY decision_timestamp"
            ),
            {"days": days},
        ).fetchall()

    if not regime_rows:
        return {"regimes": {}, "message": "No regime history found"}

    regime_df = pd.DataFrame(regime_rows, columns=["date", "state"])
    regime_df["date"] = pd.to_datetime(regime_df["date"])
    # Keep last assignment per date
    regime_df = regime_df.drop_duplicates(subset="date", keep="last")
    regime_df = regime_df.set_index("date")

    # Build feature matrix
    registry = _get_feature_registry(engine)
    if registry.empty:
        return {"regimes": {}, "message": "No eligible features"}

    feature_ids = registry["id"].tolist()
    id_to_name = dict(zip(registry["id"], registry["name"]))

    matrix = _build_feature_matrix(pit_store, feature_ids, days)
    if matrix.empty:
        return {"regimes": {}, "message": "Empty feature matrix"}

    matrix.columns = [id_to_name.get(c, str(c)) for c in matrix.columns]

    # Z-score the feature matrix
    z_matrix = (matrix - matrix.mean()) / (matrix.std() + 1e-12)

    # Align regime labels with feature dates
    z_matrix.index = pd.to_datetime(z_matrix.index)
    combined = z_matrix.join(regime_df[["state"]], how="inner")

    if combined.empty:
        return {"regimes": {}, "message": "No overlapping data between features and regimes"}

    features = [c for c in combined.columns if c != "state"]
    regimes: dict[str, list[dict[str, Any]]] = {}

    for state_name, group in combined.groupby("state"):
        if len(group) < 5:
            continue
        feature_stats: list[dict[str, Any]] = []
        for feat in features:
            avg_z = float(group[feat].mean())
            if avg_z != avg_z:  # NaN
                continue
            feature_stats.append({
                "feature": feat,
                "avg_zscore": round(avg_z, 3),
                "frequency": len(group),
            })
        # Sort by absolute z-score descending
        feature_stats.sort(key=lambda x: abs(x["avg_zscore"]), reverse=True)
        regimes[str(state_name)] = feature_stats[:20]  # Top 20 per regime

    return {"regimes": regimes}


@router.get("/anomalies")
async def get_anomalies(
    sigma_threshold: float = Query(default=2.5, ge=1.0, le=5.0),
    days: int = Query(default=252, ge=30, le=1000),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Detect features currently behaving anomalously.

    Flags features with current z-score > threshold or breaking
    historical correlation with partner features.
    """
    engine = get_db_engine()
    pit_store = get_pit_store()

    registry = _get_feature_registry(engine)
    if registry.empty:
        return {"anomalies": []}

    feature_ids = registry["id"].tolist()
    id_to_name = dict(zip(registry["id"], registry["name"]))

    matrix = _build_feature_matrix(pit_store, feature_ids, days)
    if matrix.empty or matrix.shape[0] < 30:
        return {"anomalies": [], "message": "Insufficient data"}

    matrix.columns = [id_to_name.get(c, str(c)) for c in matrix.columns]

    # Compute z-scores
    means = matrix.mean()
    stds = matrix.std()
    latest = matrix.iloc[-1]

    z_scores = (latest - means) / (stds + 1e-12)

    # Compute correlation matrix for breaking-correlation detection
    corr = matrix.corr()

    anomalies: list[dict[str, Any]] = []

    for feat in matrix.columns:
        z = float(z_scores[feat])
        if z != z:  # NaN
            continue

        if abs(z) < sigma_threshold:
            continue

        # Find correlations that might be breaking
        broken_correlations: list[dict[str, Any]] = []
        if matrix.shape[0] > 60:
            # Compare recent (30-day) correlation with historical
            recent = matrix.tail(30)
            historical = matrix.head(len(matrix) - 30)

            for other in matrix.columns:
                if other == feat:
                    continue
                hist_corr = float(historical[feat].corr(historical[other]))
                recent_corr = float(recent[feat].corr(recent[other]))

                if hist_corr != hist_corr or recent_corr != recent_corr:
                    continue

                # If correlation changed by more than 0.5, flag it
                if abs(hist_corr - recent_corr) > 0.5:
                    broken_correlations.append({
                        "partner": other,
                        "historical_corr": round(hist_corr, 3),
                        "recent_corr": round(recent_corr, 3),
                        "change": round(recent_corr - hist_corr, 3),
                    })

            broken_correlations.sort(
                key=lambda x: abs(x["change"]), reverse=True
            )

        anomalies.append({
            "feature": feat,
            "current_value": round(float(latest[feat]), 4),
            "zscore": round(z, 3),
            "historical_mean": round(float(means[feat]), 4),
            "historical_std": round(float(stds[feat]), 4),
            "severity": "extreme" if abs(z) > 3.0 else "moderate",
            "broken_correlations": broken_correlations[:5],
        })

    # Sort by absolute z-score
    anomalies.sort(key=lambda a: abs(a["zscore"]), reverse=True)

    log.info("Anomaly scan: {n} anomalous features detected", n=len(anomalies))
    return {"anomalies": anomalies, "threshold": sigma_threshold}
