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
    """Return model-eligible features with id, name, and family."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT id, name, family, description FROM feature_registry "
                "WHERE model_eligible = TRUE ORDER BY id"
            )
        ).fetchall()
    return pd.DataFrame(rows, columns=["id", "name", "family", "description"])


# ---------------------------------------------------------------------------
# Smart pair classification — separates trivial duplicates from real signals
# ---------------------------------------------------------------------------

# Known semantic groups: features within the same group are structurally related
# and high correlation between them is EXPECTED and BORING.
_SEMANTIC_GROUPS = {
    "treasury_curve": {
        "treasury_2y", "treasury_5y", "treasury_10y", "treasury_20y", "treasury_30y",
        "yield_curve_10y2y", "yield_curve_10y3m", "yield_curve_5y2y",
    },
    "overnight_rates": {
        "fed_funds_rate", "sofr", "repo_rate", "reverse_repo_rate", "iorb",
    },
    "inflation_expectations": {
        "breakeven_5y", "breakeven_10y", "breakeven_30y", "tips_5y", "tips_10y",
    },
    "equity_indices": {
        "sp500", "spy_close", "spx", "qqq_close", "nasdaq", "iwm_close", "russell_2000",
    },
    "vol_surface": {
        "vix", "vix_9d", "vix_3m", "vix_6m", "vvix", "skew",
    },
    "credit_spreads": {
        "hy_spread", "ig_spread", "bbb_spread", "hy_oas", "ig_oas",
    },
    "housing": {
        "mortgage_30y", "mortgage_15y", "case_shiller", "housing_starts",
    },
    "employment": {
        "unemployment", "nonfarm_payrolls", "initial_claims", "continuing_claims",
        "jolts_openings", "jolts_quits",
    },
    "fed_balance_sheet": {
        "fed_balance_sheet", "fed_total_assets", "reverse_repo", "repo_volume",
        "fed_reserves",
    },
    "dollar": {
        "dollar_index", "dxy", "trade_weighted_dollar", "broad_dollar",
    },
    "money_supply": {
        "m1_money_supply", "m2_money_supply",
    },
}

# Build reverse lookup: feature_name → group_name
_FEATURE_TO_GROUP: dict[str, str] = {}
for _grp, _members in _SEMANTIC_GROUPS.items():
    for _feat in _members:
        _FEATURE_TO_GROUP[_feat] = _grp


def _is_derivative_pair(name_a: str, name_b: str) -> bool:
    """Return True if one feature is clearly derived from the other.

    Catches patterns like:
    - X and X_3m_chg (lagged change of X)
    - X and X_slope (rolling slope of X)
    - X and X_zscore
    - X_ratio where X contains one of the pair
    """
    a_low, b_low = name_a.lower(), name_b.lower()

    # One is a suffix transformation of the other
    suffixes = ("_chg", "_slope", "_zscore", "_pct", "_diff", "_mom", "_yoy",
                "_3m", "_6m", "_1y", "_delta", "_rank", "_norm")
    for sfx in suffixes:
        base_a = a_low.replace(sfx, "")
        base_b = b_low.replace(sfx, "")
        if base_a == b_low or base_b == a_low:
            return True
        if base_a == base_b and a_low != b_low:
            return True

    # One name is a substring of the other (treasury_10y ⊂ yield_curve_10y2y)
    if len(a_low) > 5 and len(b_low) > 5:
        shorter, longer = sorted([a_low, b_low], key=len)
        if shorter in longer:
            return True

    return False


def _classify_pair(
    name_a: str, name_b: str, family_a: str, family_b: str, corr: float
) -> str:
    """Classify a correlated pair as 'trivial', 'expected', or 'interesting'.

    - trivial: same semantic group or one derived from the other (HIDE these)
    - expected: same family but different group (DIMMED)
    - interesting: different families or cross-group (HIGHLIGHT)
    """
    # Check if they're in the same semantic group
    group_a = _FEATURE_TO_GROUP.get(name_a, "")
    group_b = _FEATURE_TO_GROUP.get(name_b, "")
    if group_a and group_b and group_a == group_b:
        return "trivial"

    # Check if one is a derivative of the other
    if _is_derivative_pair(name_a, name_b):
        return "trivial"

    # Same family but different semantic group — expected
    if family_a == family_b:
        return "expected"

    # Cross-family — this is the interesting stuff
    return "interesting"


def _build_feature_matrix(
    pit_store,
    feature_ids: list[int],
    days: int = 252,
    max_missing_pct: float = 0.5,
) -> pd.DataFrame:
    """Build a PIT-correct feature matrix for the last N days.

    Drops columns with >max_missing_pct missing values before forward-filling,
    then drops remaining rows with NaN. This prevents a single sparse feature
    from eliminating all rows.

    Parameters:
        pit_store: PITStore instance.
        feature_ids: List of feature IDs.
        days: Lookback window in calendar days.
        max_missing_pct: Maximum fraction of missing values per column (0-1).

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
    if matrix.empty:
        return matrix

    # Drop columns with too much missing data (the main cause of empty matrices)
    missing_pct = matrix.isnull().mean()
    matrix = matrix.loc[:, missing_pct <= max_missing_pct]

    if matrix.empty:
        log.warning(
            "All features exceeded {pct:.0%} missing threshold — returning empty matrix",
            pct=max_missing_pct,
        )
        return matrix

    # Forward-fill gaps (up to 5 consecutive days), then drop remaining NaN rows
    matrix = matrix.ffill(limit=5).dropna()

    log.debug(
        "Feature matrix built — {r} rows x {c} columns (dropped {d} sparse columns)",
        r=matrix.shape[0],
        c=matrix.shape[1],
        d=int((missing_pct > max_missing_pct).sum()),
    )
    return matrix


@router.get("/correlation-matrix")
async def get_correlation_matrix(
    days: int = Query(default=252, ge=30, le=1000),
    min_corr: float = Query(default=0.3, ge=0.0, le=1.0),
    show_trivial: bool = Query(default=False, description="Include trivial/duplicate pairs"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return feature correlation matrix with intelligently classified pairs.

    Pairs are classified as:
    - **interesting**: Cross-family correlations (the valuable discoveries)
    - **expected**: Same family, different semantic group
    - **trivial**: Same semantic group or one derived from the other (hidden by default)

    Parameters:
        days: Lookback period in calendar days.
        min_corr: Minimum |correlation| for strong_pairs.
        show_trivial: Include trivial/duplicate pairs in the response.
    """
    engine = get_db_engine()
    pit_store = get_pit_store()

    registry = _get_feature_registry(engine)
    if registry.empty:
        return {"features": [], "matrix": [], "strong_pairs": []}

    feature_ids = registry["id"].tolist()
    id_to_name = dict(zip(registry["id"], registry["name"]))
    id_to_family = dict(zip(registry["id"], registry["family"]))
    name_to_family = dict(zip(registry["name"], registry["family"]))

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

    # Extract and classify pairs
    all_pairs: list[dict[str, Any]] = []
    classification_counts = {"interesting": 0, "expected": 0, "trivial": 0}

    for i in range(len(features)):
        for j in range(i + 1, len(features)):
            c = corr.iloc[i, j]
            if c != c:  # NaN
                continue
            if abs(c) < min_corr:
                continue

            fa = name_to_family.get(features[i], "")
            fb = name_to_family.get(features[j], "")
            kind = _classify_pair(features[i], features[j], fa, fb, float(c))
            classification_counts[kind] += 1

            all_pairs.append({
                "a": features[i],
                "b": features[j],
                "corr": round(float(c), 4),
                "kind": kind,
                "family_a": fa,
                "family_b": fb,
            })

    # Filter: show interesting first, then expected, then trivial only if requested
    strong_pairs = [p for p in all_pairs if p["kind"] == "interesting"]
    strong_pairs += [p for p in all_pairs if p["kind"] == "expected"]
    if show_trivial:
        strong_pairs += [p for p in all_pairs if p["kind"] == "trivial"]

    # Sort within each kind by absolute correlation
    strong_pairs.sort(key=lambda p: (
        {"interesting": 0, "expected": 1, "trivial": 2}[p["kind"]],
        -abs(p["corr"]),
    ))

    log.info(
        "Correlation matrix: {n} features, {t} total pairs "
        "({i} interesting, {e} expected, {d} trivial/duplicate)",
        n=len(features),
        t=len(all_pairs),
        i=classification_counts["interesting"],
        e=classification_counts["expected"],
        d=classification_counts["trivial"],
    )

    return {
        "features": features,
        "matrix": corr_values,
        "strong_pairs": strong_pairs,
        "pair_counts": classification_counts,
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
                "WHERE name = ANY(:names) AND model_eligible = TRUE"
            ),
            {"names": [feature_a, feature_b]},
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
    from api.routers.discovery import _jobs, _jobs_lock

    # Try to find most recent completed clustering job
    cluster_result = None
    with _jobs_lock:
        sorted_jobs = sorted(_jobs.values(), key=lambda j: j["started"], reverse=True)
    for job in sorted_jobs:
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
