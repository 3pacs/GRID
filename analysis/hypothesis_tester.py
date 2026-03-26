"""Hypothesis backtesting orchestrator.

Tests all CANDIDATE hypotheses in hypothesis_registry by computing
lagged cross-correlations between leader and follower feature series.

Each hypothesis's lag_structure JSONB is expected to contain:
    - leader_features: list[str]   — feature names for the leader actor
    - follower_features: list[str] — feature names for the follower actor
    - expected_lag: int            — expected lag in days

The tester pulls 504 days of historical data from resolved_series,
computes cross-correlations at lags 0–20, and updates the hypothesis
state based on correlation strength at the expected lag range.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Data retrieval ──────────────────────────────────────────────

def _get_feature_series(
    engine: Engine,
    feature_name: str,
    days: int = 504,
) -> pd.Series | None:
    """Pull a time series for a single feature from resolved_series.

    Returns a pandas Series indexed by obs_date, or None if no data.
    Uses the latest vintage for each obs_date (point-in-time correct).
    """
    today = date.today()
    start = today - timedelta(days=days)

    with engine.connect() as conn:
        # Look up feature_id
        fid_row = conn.execute(
            text(
                "SELECT id FROM feature_registry "
                "WHERE name = :name AND model_eligible = TRUE"
            ),
            {"name": feature_name},
        ).fetchone()

        if not fid_row:
            return None

        fid = fid_row[0]

        # Pull data — use DISTINCT ON to get latest vintage per obs_date
        rows = conn.execute(
            text(
                "SELECT DISTINCT ON (obs_date) obs_date, value "
                "FROM resolved_series "
                "WHERE feature_id = :fid "
                "  AND obs_date >= :start "
                "  AND obs_date <= :end "
                "ORDER BY obs_date, vintage_date DESC"
            ),
            {"fid": fid, "start": start, "end": today},
        ).fetchall()

    if not rows:
        return None

    dates = [r[0] for r in rows]
    values = [float(r[1]) for r in rows]
    series = pd.Series(values, index=pd.DatetimeIndex(dates), name=feature_name)
    series = series.sort_index()

    # Drop duplicates (keep last)
    series = series[~series.index.duplicated(keep="last")]
    return series


def _get_best_series_for_features(
    engine: Engine,
    feature_names: list[str],
    days: int = 504,
) -> pd.Series | None:
    """Try each feature name and return the first one with sufficient data.

    If multiple features have data, return the one with the most observations.
    """
    best: pd.Series | None = None
    best_count = 0

    for name in feature_names:
        series = _get_feature_series(engine, name, days)
        if series is not None and len(series) > best_count:
            best = series
            best_count = len(series)

    return best


# ── Cross-correlation computation ──────────────────────────────

def compute_lagged_correlation(
    leader: pd.Series,
    follower: pd.Series,
    max_lag: int = 20,
) -> dict[str, Any]:
    """Compute cross-correlation between leader and follower at lags 0..max_lag.

    Positive lag means leader leads follower by that many days.

    Returns:
        dict with keys: lags (list of {lag, correlation}), optimal_lag,
        optimal_correlation, n_observations.
    """
    # Align on common dates
    common_idx = leader.index.intersection(follower.index)
    if len(common_idx) < max_lag + 20:
        return {
            "lags": [],
            "optimal_lag": 0,
            "optimal_correlation": 0.0,
            "n_observations": len(common_idx),
            "error": "insufficient_overlap",
        }

    leader_aligned = leader.loc[common_idx].sort_index()
    follower_aligned = follower.loc[common_idx].sort_index()

    # Standardize
    l_std = leader_aligned.std()
    f_std = follower_aligned.std()
    if l_std < 1e-12 or f_std < 1e-12:
        return {
            "lags": [],
            "optimal_lag": 0,
            "optimal_correlation": 0.0,
            "n_observations": len(common_idx),
            "error": "zero_variance",
        }

    l_norm = (leader_aligned - leader_aligned.mean()) / l_std
    f_norm = (follower_aligned - follower_aligned.mean()) / f_std

    l_vals = l_norm.values
    f_vals = f_norm.values
    n = len(l_vals)

    lags_result: list[dict[str, Any]] = []
    best_corr = 0.0
    best_lag = 0

    for lag in range(0, max_lag + 1):
        if lag == 0:
            cc = float(np.corrcoef(l_vals, f_vals)[0, 1])
        elif lag < n:
            # Leader shifted back by `lag` days relative to follower
            # i.e., leader[:-lag] vs follower[lag:]
            cc = float(np.corrcoef(l_vals[:-lag], f_vals[lag:])[0, 1])
        else:
            continue

        if np.isnan(cc):
            cc = 0.0

        lags_result.append({"lag": lag, "correlation": round(cc, 4)})

        if abs(cc) > abs(best_corr):
            best_corr = cc
            best_lag = lag

    return {
        "lags": lags_result,
        "optimal_lag": best_lag,
        "optimal_correlation": round(best_corr, 4),
        "n_observations": n,
    }


# ── Single hypothesis test ─────────────────────────────────────

def test_hypothesis(
    engine: Engine,
    hypothesis_id: int,
    leader_features: list[str],
    follower_features: list[str],
    expected_lag: int = 3,
    days: int = 504,
    max_lag: int = 20,
) -> dict[str, Any]:
    """Test a single hypothesis by computing lagged cross-correlation.

    Returns a result dict with the test outcome and details.
    """
    result: dict[str, Any] = {
        "hypothesis_id": hypothesis_id,
        "leader_features": leader_features,
        "follower_features": follower_features,
        "expected_lag": expected_lag,
    }

    # Get series data
    leader_series = _get_best_series_for_features(engine, leader_features, days)
    if leader_series is None:
        result["status"] = "SKIPPED"
        result["reason"] = f"No data for leader features: {leader_features}"
        return result

    follower_series = _get_best_series_for_features(engine, follower_features, days)
    if follower_series is None:
        result["status"] = "SKIPPED"
        result["reason"] = f"No data for follower features: {follower_features}"
        return result

    result["leader_feature_used"] = leader_series.name
    result["follower_feature_used"] = follower_series.name
    result["leader_obs"] = len(leader_series)
    result["follower_obs"] = len(follower_series)

    # Compute lagged correlation
    corr_result = compute_lagged_correlation(leader_series, follower_series, max_lag)

    if corr_result.get("error"):
        result["status"] = "SKIPPED"
        result["reason"] = corr_result["error"]
        result["n_observations"] = corr_result["n_observations"]
        return result

    result["lags"] = corr_result["lags"]
    result["optimal_lag"] = corr_result["optimal_lag"]
    result["optimal_correlation"] = corr_result["optimal_correlation"]
    result["n_observations"] = corr_result["n_observations"]

    # Determine if hypothesis passes
    opt_corr = abs(corr_result["optimal_correlation"])
    opt_lag = corr_result["optimal_lag"]

    # Check correlation at/near expected lag range
    # For expected_lag=3, check lags 1-5; for expected_lag=10, check lags 5-20
    lag_margin = max(2, expected_lag // 2)
    lag_low = max(0, expected_lag - lag_margin)
    lag_high = min(max_lag, expected_lag + lag_margin)

    # Find best correlation within expected lag range
    range_corrs = [
        abs(l["correlation"])
        for l in corr_result["lags"]
        if lag_low <= l["lag"] <= lag_high
    ]
    best_in_range = max(range_corrs) if range_corrs else 0.0

    result["expected_range"] = [lag_low, lag_high]
    result["best_corr_in_range"] = round(best_in_range, 4)

    # Decision thresholds
    if best_in_range > 0.3:
        result["verdict"] = "PASSED"
        result["verdict_reason"] = (
            f"Correlation {best_in_range:.3f} at lag range [{lag_low}-{lag_high}] "
            f"exceeds threshold 0.3. Optimal lag={opt_lag}, r={opt_corr:.3f}"
        )
    elif opt_corr < 0.15:
        result["verdict"] = "FAILED"
        result["verdict_reason"] = (
            f"Max correlation {opt_corr:.3f} at any lag is below 0.15. "
            f"No meaningful lead/lag relationship detected."
        )
    else:
        result["verdict"] = "TESTING"
        result["verdict_reason"] = (
            f"Inconclusive: best_in_range={best_in_range:.3f} (threshold 0.3), "
            f"optimal={opt_corr:.3f} at lag {opt_lag}. "
            f"Relationship exists but weak or at unexpected lag."
        )

    return result


# ── Orchestrator ────────────────────────────────────────────────

def run_all_tests(engine: Engine) -> dict[str, Any]:
    """Test all CANDIDATE hypotheses and update their state in the DB.

    Loads hypotheses from hypothesis_registry where state='CANDIDATE',
    extracts leader/follower features from lag_structure JSONB, runs
    cross-correlation analysis, and updates state + kill_reason.

    Returns:
        Summary dict with counts and per-hypothesis details.
    """
    log.info("Starting hypothesis backtesting run")

    # Load CANDIDATE hypotheses
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT id, statement, lag_structure, feature_ids "
                "FROM hypothesis_registry "
                "WHERE state = 'CANDIDATE' "
                "ORDER BY id"
            )
        ).fetchall()

    if not rows:
        log.info("No CANDIDATE hypotheses to test")
        return {
            "tested": 0, "passed": 0, "failed": 0,
            "inconclusive": 0, "skipped": 0, "results": [],
        }

    log.info("Found {n} CANDIDATE hypotheses to test", n=len(rows))

    # Build feature ID → name lookup for hypotheses that use feature_ids
    with engine.connect() as conn:
        feat_rows = conn.execute(
            text(
                "SELECT id, name FROM feature_registry "
                "WHERE model_eligible = TRUE"
            )
        ).fetchall()
    id_to_name: dict[int, str] = {r[0]: r[1] for r in feat_rows}

    results: list[dict[str, Any]] = []
    counts = {"tested": 0, "passed": 0, "failed": 0, "inconclusive": 0, "skipped": 0}

    for row in rows:
        hyp_id = row[0]
        statement = row[1]
        lag_structure = row[2]
        feature_ids = row[3]

        # Parse lag_structure JSONB to extract test parameters
        leader_features: list[str] = []
        follower_features: list[str] = []
        expected_lag: int = 3

        if isinstance(lag_structure, str):
            try:
                lag_structure = json.loads(lag_structure)
            except (json.JSONDecodeError, TypeError):
                lag_structure = {}
        elif lag_structure is None:
            lag_structure = {}

        if isinstance(lag_structure, dict):
            # Format from research_agent: {leader_features, follower_features, expected_lag}
            # Or from autoresearch: {test_lags, notes, ...}
            leader_features = lag_structure.get("leader_features", [])
            follower_features = lag_structure.get("follower_features", [])
            expected_lag = lag_structure.get("expected_lag", 3)

            # If lag_structure has a "test" sub-dict (migrate_and_load format)
            if not leader_features and "test" in lag_structure:
                test_data = lag_structure["test"]
                if isinstance(test_data, str):
                    try:
                        test_data = json.loads(test_data)
                    except (json.JSONDecodeError, TypeError):
                        test_data = {}
                if isinstance(test_data, dict):
                    leader_features = test_data.get("leader_features", [])
                    follower_features = test_data.get("follower_features", [])
                    expected_lag = test_data.get("expected_lag", 3)

        # Fallback: use feature_ids array to derive feature names
        if not leader_features and not follower_features and feature_ids:
            feat_names = [id_to_name.get(fid) for fid in feature_ids if fid in id_to_name]
            feat_names = [f for f in feat_names if f is not None]
            if len(feat_names) >= 2:
                # Split features: first half leader, second half follower
                mid = len(feat_names) // 2
                leader_features = feat_names[:mid]
                follower_features = feat_names[mid:]
            elif len(feat_names) == 1:
                # Single feature — can't do cross-correlation
                pass

        # If we still have no features to test, skip
        if not leader_features or not follower_features:
            log.debug(
                "Skipping hypothesis {id}: no leader/follower features extractable",
                id=hyp_id,
            )
            result = {
                "hypothesis_id": hyp_id,
                "statement": statement[:120] if statement else "",
                "status": "SKIPPED",
                "reason": "No leader/follower features in lag_structure",
            }
            results.append(result)
            counts["skipped"] += 1
            continue

        # Run the test
        log.info(
            "Testing hypothesis {id}: {s}",
            id=hyp_id,
            s=(statement or "")[:80],
        )

        test_result = test_hypothesis(
            engine=engine,
            hypothesis_id=hyp_id,
            leader_features=leader_features,
            follower_features=follower_features,
            expected_lag=expected_lag,
        )

        test_result["statement"] = statement[:200] if statement else ""
        results.append(test_result)
        counts["tested"] += 1

        verdict = test_result.get("verdict", test_result.get("status", "SKIPPED"))

        if verdict == "SKIPPED":
            counts["skipped"] += 1
            counts["tested"] -= 1  # don't count skips as tested
            continue

        # Update the hypothesis in the database
        new_state = verdict  # PASSED, FAILED, or TESTING
        # Build a results summary for kill_reason field
        reason_text = test_result.get("verdict_reason", "")
        lag_info = (
            f"optimal_lag={test_result.get('optimal_lag')}, "
            f"r={test_result.get('optimal_correlation')}, "
            f"n={test_result.get('n_observations')}, "
            f"leader={test_result.get('leader_feature_used')}, "
            f"follower={test_result.get('follower_feature_used')}"
        )
        full_reason = f"{reason_text} | {lag_info}"

        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "UPDATE hypothesis_registry "
                        "SET state = :state, "
                        "    kill_reason = :reason, "
                        "    updated_at = NOW() "
                        "WHERE id = :id"
                    ),
                    {"state": new_state, "reason": full_reason, "id": hyp_id},
                )
        except Exception as exc:
            log.warning(
                "Failed to update hypothesis {id}: {e}", id=hyp_id, e=str(exc)
            )

        if verdict == "PASSED":
            counts["passed"] += 1
        elif verdict == "FAILED":
            counts["failed"] += 1
        elif verdict == "TESTING":
            counts["inconclusive"] += 1

    log.info(
        "Hypothesis testing complete: {t} tested, {p} passed, {f} failed, "
        "{i} inconclusive, {s} skipped",
        t=counts["tested"],
        p=counts["passed"],
        f=counts["failed"],
        i=counts["inconclusive"],
        s=counts["skipped"],
    )

    return {
        "tested": counts["tested"],
        "passed": counts["passed"],
        "failed": counts["failed"],
        "inconclusive": counts["inconclusive"],
        "skipped": counts["skipped"],
        "results": results,
    }


if __name__ == "__main__":
    from db import get_engine

    engine = get_engine()
    summary = run_all_tests(engine)
    print(f"\nTested: {summary['tested']}, Passed: {summary['passed']}, "
          f"Failed: {summary['failed']}, Inconclusive: {summary['inconclusive']}, "
          f"Skipped: {summary['skipped']}")
    print(f"\nDetails:")
    for r in summary["results"]:
        status = r.get("verdict", r.get("status", "?"))
        stmt = r.get("statement", "")[:60]
        print(f"  [{status:8s}] #{r['hypothesis_id']}: {stmt}")
