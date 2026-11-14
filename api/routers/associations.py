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
from fastapi.responses import Response
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine, get_pit_store
from api.precompute_serving import serve_precomputed

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
        "yc_2y",  # live 2Y feature (FRED DGS2) since 2026-11-04
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


# A feature whose last PIT observation is older than this is dropped (and
# reported in ``matrix.attrs["excluded_stale"]``) before the row-wise dropna,
# so one feature that stopped updating cannot truncate every view to its
# last date while the response still claims to be current.
ASSOCIATIONS_MAX_STALENESS_DAYS = 10


def _build_feature_matrix(
    pit_store,
    feature_ids: list[int],
    days: int = 252,
    max_missing_pct: float = 0.5,
    max_staleness_days: int | None = ASSOCIATIONS_MAX_STALENESS_DAYS,
    raw: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build a PIT-correct feature matrix for the last N days.

    Drops columns with >max_missing_pct missing values before forward-filling,
    then drops stale columns (last observation older than
    ``max_staleness_days``), then drops remaining rows with NaN. This
    prevents a single sparse or dead feature from eliminating rows.
    Excluded stale columns are listed in ``matrix.attrs["excluded_stale"]``
    as ``{feature_id: last_obs_date_iso | None}``.

    Parameters:
        pit_store: PITStore instance.
        feature_ids: List of feature IDs.
        days: Lookback window in calendar days.
        max_missing_pct: Maximum fraction of missing values per column (0-1).
        raw: Optional wide PIT matrix from :func:`load_raw_feature_matrix`
            covering at least ``days``. The precompute job reads it once and
            builds every window from it instead of one PIT read per view;
            ``obs_date >= start`` slicing returns the same rows the bounded
            read would (the vintage pick is per obs_date).

    Returns:
        Wide DataFrame indexed by obs_date with feature columns.
    """
    today = date.today()
    start = today - timedelta(days=days)
    if raw is not None:
        cols = [c for c in raw.columns if c in set(feature_ids)]
        matrix = raw.loc[pd.to_datetime(raw.index) >= pd.Timestamp(start), cols]
        matrix = matrix.dropna(how="all")
    else:
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

    from discovery.matrix_guard import drop_stale_columns

    matrix, excluded_stale = drop_stale_columns(matrix, today, max_staleness_days)
    if excluded_stale:
        log.info(
            "Associations matrix: excluded {n} stale features (> {d}d old)",
            n=len(excluded_stale), d=max_staleness_days,
        )

    # Forward-fill gaps (up to 5 consecutive days), then drop remaining NaN rows
    matrix = matrix.ffill(limit=5).dropna()
    matrix.attrs["excluded_stale"] = excluded_stale

    log.debug(
        "Feature matrix built — {r} rows x {c} columns (dropped {d} sparse columns)",
        r=matrix.shape[0],
        c=matrix.shape[1],
        d=int((missing_pct > max_missing_pct).sum()),
    )
    return matrix


def load_raw_feature_matrix(pit_store, feature_ids: list[int], days: int) -> pd.DataFrame:
    """The unfiltered PIT read ``_build_feature_matrix`` would make for ``days``.

    LATEST_AS_OF, ``as_of_date = end_date = today``. Pass the result as
    ``raw=`` to build any window of at most ``days`` without another read.
    """
    today = date.today()
    return pit_store.get_feature_matrix(
        feature_ids=feature_ids,
        start_date=today - timedelta(days=days),
        end_date=today,
        as_of_date=today,
        vintage_policy="LATEST_AS_OF",
    )


@router.get("/correlation-matrix")
def get_correlation_matrix(
    days: int = Query(default=252, ge=30, le=1000),
    min_corr: float = Query(default=0.3, ge=0.0, le=1.0),
    show_trivial: bool = Query(default=False, description="Include trivial/duplicate pairs"),
    _token: str = Depends(require_auth),
) -> Response:
    """Return feature correlation matrix with intelligently classified pairs.

    Pairs are classified as:
    - **interesting**: Cross-family correlations (the valuable discoveries)
    - **expected**: Same family, different semantic group
    - **trivial**: Same semantic group or one derived from the other (hidden by default)

    Served from ``api_precomputed_results`` (job ``associations`` in
    ``intelligence/api_precompute.py``); the 15-37 s build never runs in the
    request. The response carries a ``precompute`` freshness block; a
    parameter set that was never computed returns 503 ``unavailable``.

    Parameters:
        days: Lookback period in calendar days.
        min_corr: Minimum |correlation| for strong_pairs.
        show_trivial: Include trivial/duplicate pairs in the response.
    """
    return serve_precomputed(
        "associations.correlation_matrix",
        {"days": days, "min_corr": min_corr, "show_trivial": show_trivial},
    )


def _compute_correlation_matrix(
    days: int, min_corr: float, show_trivial: bool,
    raw: pd.DataFrame | None = None,
    registry: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """Body of the correlation-matrix view (runs in the precompute job)."""
    engine = get_db_engine()
    pit_store = get_pit_store()

    if registry is None:
        registry = _get_feature_registry(engine)
    if registry.empty:
        return {"features": [], "matrix": [], "strong_pairs": []}

    feature_ids = registry["id"].tolist()
    id_to_name = dict(zip(registry["id"], registry["name"]))
    dict(zip(registry["id"], registry["family"]))
    name_to_family = dict(zip(registry["name"], registry["family"]))

    matrix = _build_feature_matrix(pit_store, feature_ids, days, raw=raw)
    excluded_stale = {
        id_to_name.get(fid, str(fid)): last
        for fid, last in (matrix.attrs.get("excluded_stale") or {}).items()
    }
    if matrix.empty or matrix.shape[1] < 2:
        return {"features": [], "matrix": [], "strong_pairs": [],
                "excluded_stale_features": excluded_stale}

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

    # Upper triangle, row-major (the order the nested loop visited), keeping
    # only finite pairs at or above min_corr. Indexing ``corr.iloc[i, j]``
    # once per pair was ~200K pandas lookups (6 s of a 15 s build).
    corr_np = corr.to_numpy()
    iu, ju = np.triu_indices(len(features), k=1)
    upper = corr_np[iu, ju]
    keep = np.isfinite(upper) & (np.abs(upper) >= min_corr)
    for i, j, c in zip(iu[keep].tolist(), ju[keep].tolist(), upper[keep].tolist()):
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
        "as_of_date": date.today().isoformat(),
        "window_start": matrix.index.min().date().isoformat(),
        "window_end": matrix.index.max().date().isoformat(),
        "excluded_stale_features": excluded_stale,
    }


@router.get("/lag-analysis")
def get_lag_analysis(
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
def get_clusters(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return latest clustering results with assignments and transitions.

    Uses the most recent clustering run triggered in this API process, else
    the latest persisted global ``clustering`` analytical snapshot (written
    by the scheduled analytics job). Cluster ids are unsupervised GMM
    components; they are labelled ``CLUSTER_<i>``, never mapped onto regime
    names they were not fitted to.
    """
    from api.routers.discovery import _latest_job_result, latest_snapshot_result

    cluster_result = _latest_job_result("clustering")
    source = "in_process_job"
    as_of_date = None
    if cluster_result is None:
        try:
            snap = latest_snapshot_result("clustering", None)
        except Exception as exc:
            log.warning("clustering snapshot read failed: {e}", e=str(exc))
            snap = None
        if snap is not None:
            cluster_result = snap["result"]
            source = "analytical_snapshots"
            as_of_date = snap["as_of_date"]

    if not isinstance(cluster_result, dict) or "error" in cluster_result:
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

    clusters: list[dict[str, Any]] = []
    for i in range(best_k):
        clusters.append({
            "id": i,
            "label": f"CLUSTER_{i}",
            "feature_count": cluster_result.get("pca_components_used"),
            "persistence": best_metrics.get("gmm_persistence") if best_metrics else None,
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
        "variance_explained": cluster_result.get("variance_explained"),
        "n_observations": cluster_result.get("n_observations"),
        "current_cluster": cluster_result.get("current_cluster"),
        "as_of_date": as_of_date or cluster_result.get("as_of_date"),
        "source": source,
    }


@router.get("/regime-features")
def get_regime_features(
    days: int = Query(default=504, ge=60, le=2000),
    _token: str = Depends(require_auth),
) -> Response:
    """For each regime, return which features are most characteristic.

    Computes mean z-score of each feature during each regime period.
    Served from the ``associations`` precompute job; never computed inline.
    """
    return serve_precomputed("associations.regime_features", {"days": days})


def _compute_regime_features(
    days: int,
    raw: pd.DataFrame | None = None,
    registry: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """Body of the regime-features view (runs in the precompute job)."""
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
    if registry is None:
        registry = _get_feature_registry(engine)
    if registry.empty:
        return {"regimes": {}, "message": "No eligible features"}

    feature_ids = registry["id"].tolist()
    id_to_name = dict(zip(registry["id"], registry["name"]))

    matrix = _build_feature_matrix(pit_store, feature_ids, days, raw=raw)
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
def get_anomalies(
    sigma_threshold: float = Query(default=2.5, ge=1.0, le=5.0),
    days: int = Query(default=252, ge=30, le=1000),
    _token: str = Depends(require_auth),
) -> Response:
    """Detect features currently behaving anomalously.

    Flags features with current z-score > threshold or breaking
    historical correlation with partner features. Served from the
    ``associations`` precompute job; never computed inline.
    """
    return serve_precomputed(
        "associations.anomalies", {"sigma_threshold": sigma_threshold, "days": days},
    )


def _compute_anomalies(
    sigma_threshold: float,
    days: int,
    raw: pd.DataFrame | None = None,
    registry: pd.DataFrame | None = None,
    alert: bool = True,
) -> dict[str, Any]:
    """Body of the anomalies view (runs in the precompute job).

    ``alert=False`` skips the extreme-anomaly e-mail; the precompute job
    sends it itself, only when the extreme set changes between runs.
    """
    engine = get_db_engine()
    pit_store = get_pit_store()

    if registry is None:
        registry = _get_feature_registry(engine)
    if registry.empty:
        return {"anomalies": []}

    feature_ids = registry["id"].tolist()
    id_to_name = dict(zip(registry["id"], registry["name"]))

    matrix = _build_feature_matrix(pit_store, feature_ids, days, raw=raw)
    if matrix.empty or matrix.shape[0] < 30:
        return {"anomalies": [], "message": "Insufficient data"}

    matrix.columns = [id_to_name.get(c, str(c)) for c in matrix.columns]

    # Compute z-scores
    means = matrix.mean()
    stds = matrix.std()
    latest = matrix.iloc[-1]

    z_scores = (latest - means) / (stds + 1e-12)

    anomalies: list[dict[str, Any]] = []

    # Recent (30-day) vs historical correlation matrices, computed once and
    # only if some feature is anomalous. Per-pair ``Series.corr`` calls were
    # ~24K pandas aligns per scan (13 s of 27 s on grid-svr, 2026-11-06);
    # ``DataFrame.corr`` gives the same pairwise Pearson values (the matrix
    # has no NaN after ``dropna``; a constant column is NaN both ways).
    hist_corr_m: pd.DataFrame | None = None
    recent_corr_m: pd.DataFrame | None = None

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
            if hist_corr_m is None or recent_corr_m is None:
                hist_corr_m = matrix.head(len(matrix) - 30).corr()
                recent_corr_m = matrix.tail(30).corr()
            hist_row = hist_corr_m[feat]
            recent_row = recent_corr_m[feat]

            for other in matrix.columns:
                if other == feat:
                    continue
                hist_corr = float(hist_row[other])
                recent_corr = float(recent_row[other])

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

    if alert:
        send_extreme_anomaly_alert(extreme_anomalies(anomalies))

    return {"anomalies": anomalies, "threshold": sigma_threshold}


EXTREME_ANOMALY_Z = 3.5


def extreme_anomalies(anomalies: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The anomalies that warrant an e-mail (|z| > 3.5)."""
    return [a for a in anomalies if abs(a["zscore"]) > EXTREME_ANOMALY_Z]


def send_extreme_anomaly_alert(extreme: list[dict[str, Any]]) -> None:
    """E-mail the extreme anomalies (no-op when there are none).

    Only the inline ``alert=True`` path uses this. The scheduled scan sends the full report instead
    (``intelligence/anomaly_report.py`` via ``intelligence.api_precompute._report_and_alert``).
    """
    if not extreme:
        return
    try:
        from alerts.email import alert_on_discovery_insight
        # Plain text: alert_on_discovery_insight escapes its body, so markup arrives as literal tags.
        lines = "\n".join(
            f"{a['feature']}: {a.get('current_value')} (z={a['zscore']:+.1f}, "
            f"1-yr mean {a.get('historical_mean')})"
            for a in extreme
        )
        alert_on_discovery_insight(
            f"Extreme Anomalies Detected ({len(extreme)})",
            f"Features more than {EXTREME_ANOMALY_Z} standard deviations from their one-year mean:\n\n{lines}",
        )
    except Exception as e:
        log.warning("Associations: anomaly alert dispatch failed: {e}", e=str(e))
