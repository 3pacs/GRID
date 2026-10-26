"""Discovery engine endpoints."""

from __future__ import annotations

import asyncio
import threading
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine, get_pit_store

# Pre-import sklearn and discovery modules at load time to prevent circular
# import errors when multiple background threads try to first-import sklearn
# concurrently.  See: sklearn.utils._param_validation race condition.
import sklearn.cluster  # noqa: F401
import sklearn.decomposition  # noqa: F401
import sklearn.preprocessing  # noqa: F401
from discovery.clustering import ClusterDiscovery  # noqa: F401
from discovery.orthogonality import OrthogonalityAudit  # noqa: F401

router = APIRouter(prefix="/api/v1/discovery", tags=["discovery"])

# A feature whose last PIT observation is older than this is excluded from
# on-demand orthogonality/clustering runs (and listed in the result) instead
# of truncating the whole matrix to its last date. Same value the scheduled
# job (scripts/run_analytics_snapshots.py) uses.
DISCOVERY_MAX_STALENESS_DAYS = 10

# Wave 3 triage report item #20. hypothesis_registry/validation_results are
# noise-generator output (autoresearch + Hermes hypothesis_discovery/
# hypothesis_review, both paused/held — see intelligence.hypothesis_engine.
# ACTIVE_HYPOTHESIS_SCORING_HELD and scripts.hermes_operator.
# DAILY_INTEL_HOLD_REASONS): 41,521 rows, 2,040 "PASSED" with no trial
# ledger, FDR correction or holdout. A PASSED verdict here is NOT a
# validated finding — it is an unfiltered hypothesis scan result. Every
# response that surfaces hypothesis_registry rows carries this note so a
# caller (or the PWA) never has to infer that from silence. This does NOT
# apply to /results/orthogonality or /results/clustering (PR #691's
# on-demand, staleness-guarded jobs), which are a different, still-in-scope
# feature.
RESEARCH_LANE_OFF_NOTE = (
    "research lane off — hypothesis_registry/validation_results are "
    "noise-generator output (autoresearch paused, Hermes hypothesis "
    "scoring/discovery/review held) with no trial ledger, FDR correction "
    "or out-of-sample holdout. A PASSED verdict or a correlation/Sharpe "
    "figure here is not a validated finding."
)

# In-memory job tracking (guarded by lock for thread safety)
_jobs: dict[str, dict[str, Any]] = {}
_jobs_lock = threading.Lock()


def _run_orthogonality(job_id: str) -> None:
    """Run orthogonality audit in background."""
    with _jobs_lock:
        _jobs[job_id]["status"] = "running"
    try:
        engine = get_db_engine()
        pit = get_pit_store()
        audit = OrthogonalityAudit(engine, pit)
        result = audit.run_full_audit(max_staleness_days=DISCOVERY_MAX_STALENESS_DAYS)
        with _jobs_lock:
            _jobs[job_id]["status"] = "complete"
            _jobs[job_id]["result"] = result
    except Exception as exc:
        log.error("Orthogonality job failed: {e}", e=str(exc))
        with _jobs_lock:
            _jobs[job_id]["status"] = "failed"
            _jobs[job_id]["result"] = {"error": str(exc)}
    with _jobs_lock:
        _jobs[job_id]["finished"] = datetime.now(timezone.utc).isoformat()


def _run_clustering(job_id: str, n_components: int) -> None:
    """Run cluster discovery in background."""
    with _jobs_lock:
        _jobs[job_id]["status"] = "running"
    try:
        engine = get_db_engine()
        pit = get_pit_store()
        cd = ClusterDiscovery(engine, pit)
        result = cd.run_cluster_discovery(
            n_components=n_components,
            max_staleness_days=DISCOVERY_MAX_STALENESS_DAYS,
        )
        with _jobs_lock:
            _jobs[job_id]["status"] = "complete"
            _jobs[job_id]["result"] = result
    except Exception as exc:
        log.error("Clustering job failed: {e}", e=str(exc))
        with _jobs_lock:
            _jobs[job_id]["status"] = "failed"
            _jobs[job_id]["result"] = {"error": str(exc)}
    with _jobs_lock:
        _jobs[job_id]["finished"] = datetime.now(timezone.utc).isoformat()


@router.post("/orthogonality")
async def trigger_orthogonality(
    _token: str = Depends(require_auth),
) -> dict:
    """Trigger orthogonality audit as background task."""
    job_id = str(uuid.uuid4())[:8]
    with _jobs_lock:
        _jobs[job_id] = {
            "id": job_id,
            "type": "orthogonality",
            "status": "queued",
            "started": datetime.now(timezone.utc).isoformat(),
            "finished": None,
            "result": None,
        }
    loop = asyncio.get_event_loop()
    loop.run_in_executor(None, _run_orthogonality, job_id)
    return {"job_id": job_id, "status": "queued"}


@router.post("/clustering")
async def trigger_clustering(
    n_components: int = Query(default=3, ge=1, le=20),
    _token: str = Depends(require_auth),
) -> dict:
    """Trigger cluster discovery as background task."""
    job_id = str(uuid.uuid4())[:8]
    with _jobs_lock:
        _jobs[job_id] = {
            "id": job_id,
            "type": "clustering",
            "status": "queued",
            "started": datetime.now(timezone.utc).isoformat(),
            "finished": None,
            "result": None,
        }
    loop = asyncio.get_event_loop()
    loop.run_in_executor(None, _run_clustering, job_id, n_components)
    return {"job_id": job_id, "status": "queued"}


@router.get("/jobs")
async def get_jobs(
    _token: str = Depends(require_auth),
) -> dict:
    """Return list of recent jobs."""
    with _jobs_lock:
        jobs_list = sorted(
            _jobs.values(), key=lambda j: j["started"], reverse=True
        )[:20]
    return {"jobs": jobs_list}


def _latest_job_result(job_type: str) -> dict | None:
    """Most recent completed in-process job result of ``job_type``."""
    with _jobs_lock:
        sorted_jobs = sorted(_jobs.values(), key=lambda j: j["started"], reverse=True)
    for job in sorted_jobs:
        if job["type"] == job_type and job["status"] == "complete":
            return job["result"]
    return None


def latest_snapshot_result(category: str, subcategory: str | None = None) -> dict | None:
    """Latest persisted ``analytical_snapshots`` row for a category.

    ``subcategory=None`` selects the unpartitioned (global) row only, so a
    per-sector row can never be served as the global result. Returns
    ``{"result", "source", "as_of_date", "created_at", "snapshot_id"}`` or
    None. Read-only: a single SELECT, no table creation.
    """
    engine = get_db_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT id, as_of_date, created_at, payload "
                "FROM analytical_snapshots "
                "WHERE category = :cat "
                "  AND subcategory IS NOT DISTINCT FROM :sub "
                "ORDER BY as_of_date DESC, created_at DESC "
                "LIMIT 1"
            ),
            {"cat": category, "sub": subcategory},
        ).fetchone()
    if row is None:
        return None
    return {
        "result": row[3],
        "source": "analytical_snapshots",
        "snapshot_id": row[0],
        "as_of_date": row[1].isoformat() if row[1] is not None else None,
        "created_at": row[2].isoformat() if row[2] is not None else None,
    }


def _result_with_snapshot_fallback(
    job_type: str, category: str, subcategory: str | None, missing_message: str,
) -> dict:
    """In-process job result first, else the latest persisted snapshot."""
    if subcategory is None:
        result = _latest_job_result(job_type)
        if result is not None:
            return {"result": result, "source": "in_process_job"}
    try:
        snap = latest_snapshot_result(category, subcategory)
    except Exception as exc:
        log.warning("{c} snapshot read failed: {e}", c=category, e=str(exc))
        return {"result": None, "message": f"{category} snapshot unavailable"}
    if snap is not None:
        return snap
    return {"result": None, "message": missing_message}


@router.get("/results/orthogonality")
def get_orthogonality_results(
    _token: str = Depends(require_auth),
) -> dict:
    """Return the most recent orthogonality result.

    A run triggered in this API process wins; otherwise the latest
    ``orthogonality`` analytical snapshot (written by the scheduled
    analytics job) is served with its ``as_of_date``.
    """
    return _result_with_snapshot_fallback(
        "orthogonality", "orthogonality", None,
        "No completed orthogonality audit found",
    )


@router.get("/results/clustering")
def get_clustering_results(
    sector: str | None = Query(
        default=None,
        description="Sector name (analysis/sector_map.py) for a per-sector result",
    ),
    _token: str = Depends(require_auth),
) -> dict:
    """Return the most recent clustering result (global or one sector).

    Global: a run triggered in this API process wins, else the latest
    ``clustering`` snapshot. ``sector=...``: the latest
    ``clustering_sector`` snapshot for that sector.
    """
    if sector:
        return _result_with_snapshot_fallback(
            "clustering", "clustering_sector", sector,
            f"No clustering result for sector {sector!r}",
        )
    return _result_with_snapshot_fallback(
        "clustering", "clustering", None, "No completed clustering run found",
    )


@router.get("/results/clustering/sectors")
def list_sector_clustering_results(
    _token: str = Depends(require_auth),
) -> dict:
    """List the sectors that have a persisted per-sector clustering result."""
    engine = get_db_engine()
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT subcategory, MAX(as_of_date) AS as_of_date, COUNT(*) AS n "
                    "FROM analytical_snapshots "
                    "WHERE category = 'clustering_sector' AND subcategory IS NOT NULL "
                    "GROUP BY subcategory ORDER BY subcategory"
                )
            ).fetchall()
    except Exception as exc:
        log.warning("clustering_sector listing failed: {e}", e=str(exc))
        return {"sectors": [], "message": "clustering_sector snapshots unavailable"}
    return {
        "sectors": [
            {
                "sector": r[0],
                "as_of_date": r[1].isoformat() if r[1] is not None else None,
                "snapshots": int(r[2]),
            }
            for r in rows
        ]
    }


@router.get("/hypotheses/results")
def get_hypothesis_results(
    verdict: str | None = Query(default=None, description="Filter: PASSED, FAILED, TESTING"),
    sector: str | None = Query(default=None, description="Filter by sector/family"),
    min_correlation: float = Query(default=0.0, ge=0.0, le=1.0),
    limit: int = Query(default=50, ge=1, le=200),
    _token: str = Depends(require_auth),
) -> dict:
    """Return tested hypothesis results with correlation stats and lag info.

    Shows hypotheses that have been tested (not CANDIDATE/KILLED) along with
    their validation metrics: correlation strength, optimal lag, R², verdict.
    """
    engine = get_db_engine()

    # Build query for hypotheses with validation results
    query_parts = [
        "SELECT h.id, h.statement, h.state, h.layer, h.feature_ids, h.lag_structure,",
        "  h.created_at, h.updated_at,",
        "  v.full_period_metrics, v.overall_verdict, v.run_timestamp",
        "FROM hypothesis_registry h",
        "LEFT JOIN LATERAL (",
        "  SELECT full_period_metrics, overall_verdict, run_timestamp",
        "  FROM validation_results vr",
        "  WHERE vr.hypothesis_id = h.id",
        "  ORDER BY vr.run_timestamp DESC LIMIT 1",
        ") v ON TRUE",
        "WHERE h.state IN ('PASSED', 'FAILED', 'TESTING')",
    ]
    params: dict[str, Any] = {"lim": limit}

    if verdict:
        query_parts.append("AND h.state = :verdict")
        params["verdict"] = verdict

    if sector:
        query_parts.append("AND (h.statement ILIKE :sector_pattern OR h.layer = :sector)")
        params["sector_pattern"] = f"%{sector}%"
        params["sector"] = sector

    query_parts.append("ORDER BY h.updated_at DESC NULLS LAST LIMIT :lim")

    query = "\n".join(query_parts)

    try:
        with engine.connect() as conn:
            rows = conn.execute(text(query), params).fetchall()
    except Exception as exc:
        log.warning("Hypothesis results query failed: {e}", e=str(exc))
        # Fallback: simpler query without validation join
        try:
            with engine.connect() as conn:
                fallback_q = "SELECT * FROM hypothesis_registry WHERE state IN ('PASSED', 'FAILED', 'TESTING')"
                if verdict:
                    fallback_q += " AND state = :verdict"
                fallback_q += " ORDER BY updated_at DESC NULLS LAST LIMIT :lim"
                rows = conn.execute(text(fallback_q), params).fetchall()
        except Exception:
            return {"results": [], "count": 0, "error": str(exc)}

    results = []
    for row in rows:
        d = dict(row._mapping) if hasattr(row, "_mapping") else dict(row)

        # Parse metrics if available
        metrics = d.get("full_period_metrics")
        correlation = None
        optimal_lag = None
        r_squared = None
        if metrics:
            if isinstance(metrics, str):
                import json as _json
                try:
                    metrics = _json.loads(metrics)
                except Exception:
                    metrics = {}
            correlation = metrics.get("correlation") or metrics.get("corr")
            optimal_lag = metrics.get("optimal_lag") or metrics.get("lag")
            r_squared = metrics.get("r_squared") or metrics.get("r2")

        # Apply correlation filter
        if min_correlation > 0 and (correlation is None or abs(correlation) < min_correlation):
            continue

        for key in ("created_at", "updated_at", "run_timestamp"):
            if d.get(key) is not None:
                d[key] = str(d[key])

        results.append({
            "id": d.get("id"),
            "statement": d.get("statement"),
            "state": d.get("state") or d.get("overall_verdict"),
            "layer": d.get("layer"),
            "correlation": round(correlation, 4) if correlation is not None else None,
            "optimal_lag": optimal_lag,
            "r_squared": round(r_squared, 4) if r_squared is not None else None,
            "feature_ids": d.get("feature_ids"),
            "lag_structure": d.get("lag_structure"),
            "created_at": d.get("created_at"),
            "updated_at": d.get("updated_at"),
            "tested_at": d.get("run_timestamp"),
        })

    return {
        "results": results,
        "count": len(results),
        "research_lane_off": True,
        "note": RESEARCH_LANE_OFF_NOTE,
    }


@router.get("/hypotheses")
def get_hypotheses(
    state: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    _token: str = Depends(require_auth),
) -> dict:
    """Return all hypotheses from hypothesis_registry."""
    engine = get_db_engine()

    query = "SELECT * FROM hypothesis_registry"
    params: dict[str, Any] = {"lim": limit, "off": offset}
    if state:
        query += " WHERE state = :state"
        params["state"] = state
    query += " ORDER BY created_at DESC LIMIT :lim OFFSET :off"

    with engine.connect() as conn:
        rows = conn.execute(text(query), params).fetchall()

    hypotheses = []
    for row in rows:
        d = dict(row._mapping) if hasattr(row, "_mapping") else dict(row)
        for key in ("created_at", "updated_at"):
            if d.get(key) is not None:
                d[key] = str(d[key])
        hypotheses.append(d)

    return {
        "hypotheses": hypotheses,
        "research_lane_off": True,
        "note": RESEARCH_LANE_OFF_NOTE,
    }


@router.get("/backtest-results")
async def get_backtest_results(
    min_sharpe: float = Query(default=1.0, ge=0.0),
    min_win_rate: float = Query(default=0.55, ge=0.0, le=1.0),
    family: str | None = Query(default=None, description="Filter by leader family"),
    limit: int = Query(default=50, ge=1, le=200),
    _token: str = Depends(require_auth),
) -> dict:
    """Return latest backtest scan results (winners) from the scanner.

    Runs a lightweight scan if no cached results are available. Supports
    filtering by family, min Sharpe, and min win rate.
    """
    from analysis.backtest_scanner import scan_all_pairs, _display_name

    engine = get_db_engine()
    try:
        winners = scan_all_pairs(
            engine,
            min_sharpe=min_sharpe,
            min_win_rate=min_win_rate,
        )
    except Exception as exc:
        log.warning("Backtest scan failed: {e}", e=str(exc))
        return {"results": [], "count": 0, "error": str(exc)}

    # Filter by family if specified
    if family:
        winners = [w for w in winners if w.get("leader_family") == family or w.get("follower_family") == family]

    # Enrich with display names
    for w in winners:
        w["leader_display"] = _display_name(w.get("leader", ""))
        w["follower_display"] = _display_name(w.get("follower", ""))

    results = winners[:limit]
    return {"results": results, "count": len(results), "total_scanned": len(winners)}


@router.post("/backtest-scan")
async def run_backtest_scan(
    min_sharpe: float = Query(default=1.0, ge=0.5),
    min_win_rate: float = Query(default=0.55, ge=0.4, le=1.0),
    _token: str = Depends(require_auth),
) -> dict:
    """Run automated cross-asset backtest scan and generate hypotheses."""
    from analysis.backtest_scanner import run_full_scan
    engine = get_db_engine()
    return run_full_scan(engine)


@router.post("/hypotheses/review")
async def run_hypothesis_review(
    _token: str = Depends(require_auth),
) -> dict:
    """Run LLM-based review of all PASSED/TESTING hypotheses.

    Flags hypotheses that may have stale data, circular logic,
    survivorship bias, or unrealistic assumptions.  Flagged hypotheses
    are moved to TESTING state with a note.
    """
    from analysis.backtest_scanner import review_existing_hypotheses
    engine = get_db_engine()
    return review_existing_hypotheses(engine)


@router.post("/hypotheses/{hypothesis_id}/promote")
def promote_hypothesis_to_feature(
    hypothesis_id: int,
    _token: str = Depends(require_auth),
) -> dict:
    """Promote a PASSED hypothesis to a derived feature.

    Creates a new feature in feature_registry based on the hypothesis's
    leader/follower relationship. The feature computes the lagged signal
    strength between leader and follower.
    """
    engine = get_db_engine()
    import json

    with engine.connect() as conn:
        hypo = conn.execute(text(
            "SELECT id, statement, state, lag_structure FROM hypothesis_registry WHERE id = :id"
        ), {"id": hypothesis_id}).fetchone()

    if not hypo:
        raise HTTPException(status_code=404, detail="Hypothesis not found")

    if hypo[2] != 'PASSED':
        raise HTTPException(status_code=400, detail=f"Only PASSED hypotheses can be promoted (current: {hypo[2]})")

    lag = json.loads(hypo[3]) if isinstance(hypo[3], str) else (hypo[3] or {})
    leader = (lag.get("leader_features") or [None])[0]
    follower = (lag.get("follower_features") or [None])[0]
    expected_lag = lag.get("expected_lag", 1)

    if not leader or not follower:
        raise HTTPException(status_code=400, detail="Hypothesis has no leader/follower features defined")

    # Create derived feature name
    l_short = leader.replace("_full", "")
    f_short = follower.replace("_full", "")
    feature_name = f"hypo_{l_short}_leads_{f_short}"

    with engine.begin() as conn:
        # Check if already exists
        existing = conn.execute(text(
            "SELECT id FROM feature_registry WHERE name = :n"
        ), {"n": feature_name}).fetchone()

        if existing:
            return {"status": "already_exists", "feature_name": feature_name, "feature_id": existing[0]}

        # Create the feature
        conn.execute(text(
            "INSERT INTO feature_registry "
            "(name, family, description, transformation, normalization, "
            "missing_data_policy, model_eligible, eligible_from_date) "
            "VALUES (:name, 'derived', :desc, 'LAGGED_SIGNAL', 'ZSCORE', "
            "'FORWARD_FILL', TRUE, '2020-01-01')"
        ), {
            "name": feature_name,
            "desc": f"Derived: {leader} leads {follower} by {expected_lag}d. Source: hypothesis #{hypothesis_id}",
        })

        feat = conn.execute(text(
            "SELECT id FROM feature_registry WHERE name = :n"
        ), {"n": feature_name}).fetchone()

        # Update hypothesis state to indicate promotion
        conn.execute(text(
            "UPDATE hypothesis_registry SET state = 'PROMOTED' WHERE id = :id"
        ), {"id": hypothesis_id})

    return {
        "status": "promoted",
        "feature_name": feature_name,
        "feature_id": feat[0] if feat else None,
        "leader": leader,
        "follower": follower,
        "lag": expected_lag,
        "hypothesis_id": hypothesis_id,
    }


# Maintained cross-asset set for the correlation-matrix view: (feature_registry
# name, display name). Chosen 2026-09-27 from feature_registry rows that were
# model_eligible, not deprecated and observed within the last 10 days on
# grid-svr. It replaces 13 legacy names (spy_close, qqq_close, iwm_close,
# treasury_10y, treasury_2y, yield_curve_10y2y, gold_price, crude_oil,
# btc_price, dollar_index, vix, hy_spread, ig_spread): six of them no longer
# exist in the registry and the other seven stopped updating in March/April
# 2026. The names below are resolved to ids at request time and every
# feature is re-checked (eligible, not deprecated, fresh) on every call.
# IWM, the 10y-2y curve and VIX itself have no model-eligible maintained
# series today (iwm_full / yld_curve_2s10s / vix_spot are model_eligible =
# FALSE), so they are not substituted with something else under their name.
CROSS_ASSET_FEATURES: tuple[tuple[str, str], ...] = (
    ("spy_full", "SPY"),
    ("qqq_full", "QQQ"),
    ("tlt_full", "TLT"),
    ("shy_full", "SHY (1-3Y UST)"),
    ("gld_full", "GLD"),
    ("cl_close", "WTI crude"),
    ("btc_usd_full", "BTC"),
    ("uup_etf_close", "UUP (USD)"),
    ("spy_iv_atm", "SPY ATM IV"),
    ("vvix", "VVIX"),
    ("hy_oas_spread", "HY OAS"),
    ("ig_oas_spread", "IG OAS"),
)


def resolve_cross_asset_features(engine) -> tuple[list[int], dict[int, str], list[dict]]:
    """Resolve ``CROSS_ASSET_FEATURES`` to usable feature ids, failing closed.

    Returns (usable_ids, id_to_name, excluded) where ``excluded`` lists every
    configured feature that cannot be used, with a reason:
    ``not_in_registry``, ``deprecated`` or ``not_model_eligible``. Freshness
    is checked later against the PIT matrix itself.
    """
    names = [n for n, _ in CROSS_ASSET_FEATURES]
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT id, name, model_eligible, deprecated_at "
                "FROM feature_registry WHERE name = ANY(:names)"
            ),
            {"names": names},
        ).fetchall()
    by_name = {r[1]: r for r in rows}
    usable: list[int] = []
    id_to_name: dict[int, str] = {}
    excluded: list[dict] = []
    for name in names:
        row = by_name.get(name)
        if row is None:
            excluded.append({"feature": name, "reason": "not_in_registry"})
        elif row[3] is not None:
            excluded.append({"feature": name, "reason": "deprecated"})
        elif not row[2]:
            excluded.append({"feature": name, "reason": "not_model_eligible"})
        else:
            usable.append(int(row[0]))
            id_to_name[int(row[0])] = name
    return usable, id_to_name, excluded


@router.get("/correlation-matrix")
def get_correlation_matrix(
    period: int = Query(default=90, ge=30, le=1000, description="Lookback days"),
    regime: str = Query(default="all", description="Filter: all, GROWTH, FRAGILE, CRISIS"),
    _token: str = Depends(require_auth),
) -> dict:
    """Return cross-asset correlation matrix with regime breakdowns and PCA summary.

    Computes correlations from PIT ``resolved_series`` for the maintained
    cross-asset set in ``CROSS_ASSET_FEATURES``. Each feature fails closed
    on its own: one that is missing from the registry, deprecated, not
    model-eligible, or whose last observation is older than
    ``DISCOVERY_MAX_STALENESS_DAYS`` is left out and listed in
    ``excluded_features`` with the reason; the rest are still returned.
    Optionally breaks down by market regime detected from decision_journal.
    """
    import numpy as np
    import pandas as pd
    from sklearn.decomposition import PCA
    from sklearn.preprocessing import StandardScaler

    from discovery.matrix_guard import drop_stale_columns

    engine = get_db_engine()
    pit_store = get_pit_store()

    empty = {"features": [], "matrix": [], "regime_matrices": {},
             "breakdowns": [], "current_regime": "UNKNOWN",
             "pca": {"components": [], "total_variance": 0}}

    feature_ids, id_to_name, excluded = resolve_cross_asset_features(engine)
    empty["excluded_features"] = excluded
    if not feature_ids:
        return empty
    DISPLAY_NAMES = dict(CROSS_ASSET_FEATURES)

    # Build feature matrix using PIT store
    from datetime import date as _date, timedelta as _td
    today = _date.today()
    start = today - _td(days=int(period * 1.5))  # extra buffer for regime alignment

    matrix = pit_store.get_feature_matrix(
        feature_ids=feature_ids,
        start_date=start,
        end_date=today,
        as_of_date=today,
        vintage_policy="LATEST_AS_OF",
    )

    if matrix is None or matrix.empty:
        for fid in feature_ids:
            excluded.append({"feature": id_to_name[fid], "reason": "no_observations"})
        return empty

    # Features with no PIT rows in the window never become columns.
    for fid in feature_ids:
        if fid not in matrix.columns:
            excluded.append({"feature": id_to_name[fid], "reason": "no_observations"})

    # Fail closed per feature: a series that stopped updating would
    # otherwise truncate every row after its last date via dropna().
    matrix, stale = drop_stale_columns(matrix, today, DISCOVERY_MAX_STALENESS_DAYS)
    for fid, last in stale.items():
        excluded.append({"feature": id_to_name.get(fid, str(fid)),
                         "reason": "stale", "last_obs_date": last})

    # Rename columns to display names (keep display -> id for provenance)
    display_to_id = {DISPLAY_NAMES.get(id_to_name.get(c, ""), str(c)): int(c)
                     for c in matrix.columns}
    matrix.columns = list(display_to_id)

    # Clean: drop >50% missing, ffill, dropna
    missing_pct = matrix.isnull().mean()
    for col in missing_pct[missing_pct > 0.5].index:
        excluded.append({"feature": col, "reason": "sparse"})
    matrix = matrix.loc[:, missing_pct <= 0.5]
    matrix = matrix.ffill(limit=5).dropna()

    if matrix.empty or matrix.shape[1] < 2:
        return empty

    features = list(matrix.columns)

    # Trim to requested period
    cutoff = today - _td(days=period)
    matrix_period = matrix[matrix.index >= pd.Timestamp(cutoff)]
    if len(matrix_period) < 10:
        matrix_period = matrix.tail(max(10, len(matrix)))

    # ── Overall correlation matrix ──
    corr = matrix_period.corr()
    corr_list = [[round(float(corr.iloc[i, j]), 4)
                   for j in range(len(features))]
                  for i in range(len(features))]

    # ── Regime detection from decision_journal ──
    regime_matrices = {}
    current_regime = "UNKNOWN"
    try:
        with engine.connect() as conn:
            regime_rows = conn.execute(
                text(
                    "SELECT DATE(decision_timestamp) AS dt, inferred_state "
                    "FROM decision_journal "
                    "WHERE decision_timestamp >= NOW() - make_interval(days => :days) "
                    "ORDER BY decision_timestamp"
                ),
                {"days": int(period * 1.5)},
            ).fetchall()

        if regime_rows:
            regime_df = pd.DataFrame(regime_rows, columns=["date", "state"])
            regime_df["date"] = pd.to_datetime(regime_df["date"])
            regime_df = regime_df.drop_duplicates(subset="date", keep="last")
            regime_df = regime_df.set_index("date")

            # Current regime = most recent
            current_regime = str(regime_df["state"].iloc[-1]) if len(regime_df) > 0 else "UNKNOWN"

            # Align regimes with feature matrix
            matrix_period.index = pd.to_datetime(matrix_period.index)
            combined = matrix_period.join(regime_df[["state"]], how="inner")

            for regime_name in ["GROWTH", "FRAGILE", "CRISIS"]:
                regime_slice = combined[combined["state"] == regime_name]
                feat_cols = [c for c in regime_slice.columns if c != "state"]
                if len(regime_slice) >= 10:
                    rc = regime_slice[feat_cols].corr()
                    regime_matrices[regime_name] = [
                        [round(float(rc.iloc[i, j]), 4)
                         for j in range(len(feat_cols))]
                        for i in range(len(feat_cols))
                    ]
    except Exception as exc:
        log.warning("Regime breakdown failed (non-fatal): {e}", e=str(exc))

    # ── Breakdown alerts: pairs where current vs historical correlation diverges ──
    breakdowns = []
    try:
        half = len(matrix_period) // 2
        if half >= 10:
            recent = matrix_period.tail(half).corr()
            historical = matrix_period.head(half).corr()
            for i in range(len(features)):
                for j in range(i + 1, len(features)):
                    curr_c = float(recent.iloc[i, j])
                    hist_c = float(historical.iloc[i, j])
                    delta = abs(curr_c - hist_c)
                    if delta > 0.25:
                        breakdowns.append({
                            "pair": [features[i], features[j]],
                            "current_corr": round(curr_c, 3),
                            "historical_corr": round(hist_c, 3),
                            "diverging": delta > 0.3,
                        })
            breakdowns.sort(key=lambda x: abs(x["current_corr"] - x["historical_corr"]),
                            reverse=True)
            breakdowns = breakdowns[:10]
    except Exception as exc:
        log.warning("Breakdown analysis failed (non-fatal): {e}", e=str(exc))

    # ── PCA summary ──
    pca_result = {"components": [], "total_variance": 0}
    try:
        clean = matrix_period.dropna()
        if len(clean) > 10 and len(clean.columns) > 2:
            scaler = StandardScaler()
            scaled = scaler.fit_transform(clean)
            n_comp = min(3, clean.shape[1])
            pca = PCA(n_components=n_comp)
            pca.fit(scaled)

            components = []
            for idx in range(n_comp):
                loadings = pca.components_[idx]
                var_pct = float(pca.explained_variance_ratio_[idx])
                # Top 3 contributing features
                top_idx = np.argsort(np.abs(loadings))[::-1][:3]
                top_features = [
                    {"feature": features[k], "loading": round(float(loadings[k]), 3)}
                    for k in top_idx if k < len(features)
                ]
                # Describe the component by its measured loadings only; the
                # components are unsupervised, so no factor name is implied.
                interp = (
                    f"{var_pct:.0%} of variance; largest loadings: "
                    + ", ".join(t["feature"] for t in top_features)
                )

                components.append({
                    "id": f"PC{idx + 1}",
                    "variance_pct": round(var_pct, 4),
                    "top_features": top_features,
                    "interpretation": interp,
                })

            pca_result = {
                "components": components,
                "total_variance": round(float(sum(pca.explained_variance_ratio_)), 4),
            }
    except Exception as exc:
        log.warning("PCA summary failed (non-fatal): {e}", e=str(exc))

    return {
        "features": features,
        "matrix": corr_list,
        "regime_matrices": regime_matrices,
        "breakdowns": breakdowns,
        "current_regime": current_regime,
        "pca": pca_result,
        "period": period,
        "n_observations": len(matrix_period),
        "as_of_date": today.isoformat(),
        "window_start": matrix_period.index.min().date().isoformat(),
        "window_end": matrix_period.index.max().date().isoformat(),
        "feature_ids": {name: display_to_id[name] for name in features},
        "excluded_features": excluded,
    }


@router.get("/smart-heatmap")
def smart_heatmap(
    family: str | None = Query(default=None, description="Filter by feature family (rates, macro, credit, etc.)"),
    orthogonal_only: bool = Query(default=True, description="Filter to orthogonal features only"),
    corr_threshold: float = Query(default=0.8, ge=0.5, le=1.0),
    _token: str = Depends(require_auth),
) -> dict:
    """Return feature heatmap data filtered by orthogonality and optionally by family.

    Returns the correlation submatrix and z-scores for the selected feature set.
    This is the "smart" version that first removes redundant features via
    orthogonality analysis, then optionally filters by taxonomy family.
    """

    engine = get_db_engine()
    pit_store = get_pit_store()

    # Load all model-eligible features
    with engine.connect() as conn:
        feat_rows = conn.execute(text(
            "SELECT id, name, family FROM feature_registry "
            "WHERE model_eligible = TRUE ORDER BY id"
        )).fetchall()

    if not feat_rows:
        return {"features": [], "matrix": [], "z_scores": [], "families": []}

    all_ids = [r[0] for r in feat_rows]
    id_to_name = {r[0]: r[1] for r in feat_rows}
    id_to_family = {r[0]: r[2] for r in feat_rows}

    # Filter by orthogonality if requested
    selected_ids = all_ids
    redundant_pairs = []
    if orthogonal_only:
        try:
            from discovery.orthogonality import OrthogonalityAudit
            audit = OrthogonalityAudit(db_engine=engine, pit_store=pit_store)
            ortho = audit.get_orthogonal_features(corr_threshold=corr_threshold)
            selected_ids = ortho["orthogonal_ids"]
            redundant_pairs = ortho["redundant_pairs"]
        except Exception as exc:
            log.warning("Orthogonal filter failed, using all features: {e}", e=str(exc))

    # Filter by family if specified
    if family:
        selected_ids = [fid for fid in selected_ids if id_to_family.get(fid, "") == family]

    if not selected_ids:
        return {"features": [], "matrix": [], "z_scores": [], "families": [],
                "filtered_count": 0, "total_count": len(all_ids)}

    # Get feature names for selected IDs
    features = [id_to_name[fid] for fid in selected_ids if fid in id_to_name]

    # Build correlation matrix for selected features
    try:
        df = pit_store.get_feature_matrix(feature_ids=selected_ids, as_of_date=None)
        if df is not None and not df.empty:
            corr = df.corr()
            matrix = corr.values.tolist()

            # Get latest z-scores
            z_scores = []
            if len(df) > 0:
                last_row = df.iloc[-1]
                mean = df.mean()
                std = df.std().replace(0, 1)
                z = ((last_row - mean) / std).fillna(0)
                z_scores = [{"feature": features[i], "zscore": float(z.iloc[i]),
                             "family": id_to_family.get(selected_ids[i], "other")}
                            for i in range(min(len(features), len(z)))]
        else:
            matrix = []
            z_scores = []
    except Exception as exc:
        log.warning("Smart heatmap matrix failed: {e}", e=str(exc))
        matrix = []
        z_scores = []

    # Available families
    all_families = sorted(set(id_to_family.values()))

    return {
        "features": features,
        "matrix": matrix,
        "z_scores": z_scores,
        "families": all_families,
        "filtered_count": len(selected_ids),
        "total_count": len(all_ids),
        "redundant_pairs": redundant_pairs[:10],
        "orthogonal_only": orthogonal_only,
        "family_filter": family,
    }
