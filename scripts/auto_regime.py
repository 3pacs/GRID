#!/usr/bin/env python3
"""GRID auto-regime: continuous stress index with derivative-based transition detection.

Instead of GMM clustering (which suffers from label switching and has no
economic model), we compute a composite **Stress Index** from z-scores of
regime-relevant features, weighted by economic meaning.

The stress index S(t) is a continuous signal from ~-3 (extreme calm) to +3
(extreme stress). Regimes are derived from S(t) and its first derivative
dS/dt (momentum):

    GROWTH:  S < -0.3  and  dS/dt <= 0   (low stress, improving or stable)
    NEUTRAL: -0.3 <= S <= 0.6             (mixed signals)
    FRAGILE: S > 0.6   or  dS/dt > 0.15  (elevated stress or deteriorating fast)
    CRISIS:  S > 1.5   and  dS/dt > 0    (high stress, still worsening)

The derivative catches transitions early — a low-stress market that's
deteriorating quickly gets flagged as FRAGILE before S itself crosses
the threshold.

Confidence is derived from how cleanly the reading falls into one regime
vs straddling boundaries.
"""

import json
import os
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
from loguru import logger as log
from sqlalchemy import text

from db import get_engine
from store.pit import PITStore

# Path for user weight overrides (written by API, read here)
WEIGHTS_OVERRIDE_PATH = Path(__file__).resolve().parent.parent / "outputs" / "regime_weights.json"

# ── Feature weights ─────────────────────────────────────────────
#
# Positive weight = feature INCREASES with stress (VIX, spreads)
# Negative weight = feature DECREASES with stress (equities, copper)
# Magnitude = how much this feature matters for regime classification
#
# These are the knobs the user will eventually control via sliders.

DEFAULT_FEATURE_WEIGHTS: dict[str, float] = {
    # Volatility (most direct stress measures)
    "vix":               +0.20,
    "move_index":        +0.08,
    "vxn":               +0.05,

    # Credit (transmission mechanism: stress → wider spreads)
    "hy_spread":         +0.15,
    "chicago_fed":       +0.06,

    # Rates (curve inversion = recession signal)
    "yield_curve_10y2y": -0.10,   # inverted curve = stress
    "treasury_10y":      +0.03,   # higher rates = tighter conditions
    "breakeven_10y":     -0.03,   # falling breakevens = deflation fear

    # Risk assets (fall during stress)
    "sp500":             -0.10,
    "copper":            -0.05,   # Dr. Copper — growth proxy
    "crude_oil":         -0.03,

    # Safe havens (rise during stress)
    "gold":              +0.05,
    "dollar_index":      +0.05,

    # Sentiment
    "spy_rsi":           -0.04,   # low RSI = oversold = stress
    "put_call_ratio":    +0.03,   # high PCR = fear
}


def _load_effective_weights(overrides: dict[str, float] | None = None) -> dict[str, float]:
    """Return effective weights: defaults merged with file overrides and/or explicit overrides."""
    weights = dict(DEFAULT_FEATURE_WEIGHTS)
    # Load from persisted override file
    if WEIGHTS_OVERRIDE_PATH.exists():
        try:
            with open(WEIGHTS_OVERRIDE_PATH) as f:
                file_overrides = json.load(f)
            weights.update(file_overrides)
            log.info("Loaded {n} weight overrides from {p}", n=len(file_overrides), p=WEIGHTS_OVERRIDE_PATH)
        except Exception as exc:
            log.warning("Failed to load weight overrides: {e}", e=str(exc))
    # Apply explicit overrides (from API call)
    if overrides:
        weights.update(overrides)
    return weights


# Active weights (loaded at import, refreshed per-run)
FEATURE_WEIGHTS: dict[str, float] = _load_effective_weights()

# Regime thresholds on the stress index
REGIME_THRESHOLDS = {
    "CRISIS":  {"s_min": 1.5,  "ds_min": 0.0},
    "FRAGILE": {"s_min": 0.6,  "ds_min": None},
    "NEUTRAL": {"s_min": -0.3, "s_max": 0.6},
    "GROWTH":  {"s_max": -0.3},
}

POSTURE_MAP = {
    "GROWTH": "AGGRESSIVE",
    "NEUTRAL": "BALANCED",
    "FRAGILE": "DEFENSIVE",
    "CRISIS": "CAPITAL_PRESERVATION",
}

# How many days to use for the derivative (smoothing window)
DERIVATIVE_WINDOW = 5


def _compute_stress_index(
    feature_matrix: "pd.DataFrame",
    feature_names: dict[int, str],
    weights: dict[str, float] | None = None,
) -> "np.ndarray":
    """Compute the weighted stress index for each row in the feature matrix.

    For each feature:
      1. Compute rolling z-score (252-day window)
      2. Multiply by the feature's weight
      3. Sum across all features

    Returns:
        1D array of stress index values, one per row.
    """
    import pandas as pd

    active_weights = weights if weights is not None else FEATURE_WEIGHTS
    n_rows = len(feature_matrix)
    stress = np.zeros(n_rows)
    total_weight = 0.0
    contributions = {}

    for col in feature_matrix.columns:
        name = feature_names.get(col, "")
        weight = active_weights.get(name)
        if weight is None:
            continue

        series = feature_matrix[col].astype(float)
        # Rolling z-score: (value - rolling_mean) / rolling_std
        roll_mean = series.rolling(window=252, min_periods=30).mean()
        roll_std = series.rolling(window=252, min_periods=30).std().replace(0, 1)
        z = ((series - roll_mean) / roll_std).fillna(0).values

        stress += z * weight
        total_weight += abs(weight)
        # Track latest contribution for debugging
        if len(z) > 0:
            contributions[name] = {
                "z": round(float(z[-1]), 3),
                "weight": weight,
                "contribution": round(float(z[-1] * weight), 4),
            }

    # Normalize so the index is in z-score-like units
    if total_weight > 0:
        stress /= total_weight

    return stress, contributions


def _classify_regime(s: float, ds: float) -> tuple[str, float]:
    """Classify regime from stress index S and its derivative dS/dt.

    Returns (regime_name, confidence).
    """
    # CRISIS: high stress AND still worsening
    if s > 1.5 and ds >= 0:
        # Confidence: how far above 1.5
        conf = min(1.0, 0.7 + (s - 1.5) * 0.3)
        return "CRISIS", conf

    # FRAGILE: elevated stress OR deteriorating fast
    if s > 0.6 or ds > 0.15:
        # If both conditions met, higher confidence
        conf = 0.5
        if s > 0.6:
            conf += min(0.25, (s - 0.6) * 0.25)
        if ds > 0.15:
            conf += min(0.25, (ds - 0.15) * 1.0)
        return "FRAGILE", min(1.0, conf)

    # GROWTH: low stress and stable/improving
    if s < -0.3 and ds <= 0.05:
        conf = min(1.0, 0.6 + abs(s + 0.3) * 0.3)
        return "GROWTH", conf

    # NEUTRAL: everything else
    # Confidence is higher when squarely in the middle
    dist_to_edge = min(abs(s - 0.6), abs(s + 0.3))
    conf = min(1.0, 0.4 + dist_to_edge * 0.5)
    return "NEUTRAL", conf


def run_with_weights(engine, weights: dict[str, float], save: bool = False) -> dict[str, Any]:
    """Run regime classification with custom weights, optionally saving them.

    Args:
        engine: SQLAlchemy engine.
        weights: Full weight dict (merged with defaults by caller).
        save: If True, persist weights to the override file.

    Returns:
        Dict with regime, confidence, stress_index, etc.
    """
    if save:
        WEIGHTS_OVERRIDE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(WEIGHTS_OVERRIDE_PATH, "w") as f:
            # Only save overrides that differ from defaults
            overrides = {k: v for k, v in weights.items() if v != DEFAULT_FEATURE_WEIGHTS.get(k)}
            json.dump(overrides, f, indent=2)
        log.info("Saved {n} weight overrides to {p}", n=len(overrides), p=WEIGHTS_OVERRIDE_PATH)
        # Refresh module-level weights
        global FEATURE_WEIGHTS
        FEATURE_WEIGHTS = _load_effective_weights()

    pit = PITStore(engine)

    # Resolve feature IDs
    feature_names_list = list(weights.keys())
    with engine.connect() as conn:
        feat_rows = conn.execute(
            text(
                "SELECT id, name FROM feature_registry "
                "WHERE model_eligible = TRUE AND name = ANY(:names)"
            ),
            {"names": feature_names_list},
        ).fetchall()

    if len(feat_rows) < 3:
        return {"regime": "UNKNOWN", "confidence": 0.0, "error": "insufficient features"}

    fids = [r[0] for r in feat_rows]
    fid_to_name = {r[0]: r[1] for r in feat_rows}

    today = date.today()
    df = pit.get_feature_matrix(fids, today - timedelta(days=756), today, today)
    if df.empty or len(df) < 50:
        return {"regime": "UNKNOWN", "confidence": 0.0, "error": f"insufficient data: {df.shape}"}

    df = df.ffill().bfill().dropna(axis=1, how="all")

    stress_series, contributions = _compute_stress_index(df, fid_to_name, weights)

    s_current = float(stress_series[-1]) if len(stress_series) > 0 else 0.0
    if len(stress_series) > DERIVATIVE_WINDOW:
        ds = float(stress_series[-1] - stress_series[-1 - DERIVATIVE_WINDOW]) / DERIVATIVE_WINDOW
    else:
        ds = 0.0

    regime, confidence = _classify_regime(s_current, ds)
    posture = POSTURE_MAP.get(regime, "BALANCED")

    sorted_contribs = sorted(contributions.items(), key=lambda x: abs(x[1]["contribution"]), reverse=True)

    return {
        "regime": regime,
        "confidence": round(confidence, 4),
        "posture": posture,
        "stress_index": round(s_current, 4),
        "stress_derivative": round(ds, 4),
        "contributions": {k: v for k, v in sorted_contribs[:10]},
        "n_features": len(fid_to_name),
        "weights": weights,
    }


def run() -> dict[str, Any]:
    """Run regime detection and update decision_journal."""
    # Refresh weights from override file each run
    global FEATURE_WEIGHTS
    FEATURE_WEIGHTS = _load_effective_weights()

    engine = get_engine()
    pit = PITStore(engine)

    # Get production model ID
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT id FROM model_registry WHERE state='PRODUCTION' AND layer='REGIME' LIMIT 1")
        ).fetchone()
    if not row:
        log.warning("No production regime model found")
        return {"regime": "UNKNOWN", "confidence": 0.0, "error": "no production model"}
    model_id = row[0]

    # Resolve feature IDs for all weighted features
    feature_names_list = list(FEATURE_WEIGHTS.keys())
    with engine.connect() as conn:
        feat_rows = conn.execute(
            text(
                "SELECT id, name FROM feature_registry "
                "WHERE model_eligible = TRUE AND name = ANY(:names)"
            ),
            {"names": feature_names_list},
        ).fetchall()

    if len(feat_rows) < 3:
        log.warning("Only {n} regime features found", n=len(feat_rows))
        return {"regime": "UNKNOWN", "confidence": 0.0, "error": "insufficient features"}

    fids = [r[0] for r in feat_rows]
    fid_to_name = {r[0]: r[1] for r in feat_rows}
    found_names = set(fid_to_name.values())
    missing = [n for n in feature_names_list if n not in found_names]
    if missing:
        log.info("Regime features not in DB: {m}", m=missing)

    log.info("Regime detection using {n} features", n=len(fids))

    # Build feature matrix (2+ years for rolling z-score computation)
    today = date.today()
    df = pit.get_feature_matrix(fids, today - timedelta(days=756), today, today)
    if df.empty or len(df) < 50:
        return {"regime": "UNKNOWN", "confidence": 0.0, "error": f"insufficient data: {df.shape}"}

    df = df.ffill().bfill().dropna(axis=1, how="all")

    # Compute stress index
    stress_series, contributions = _compute_stress_index(df, fid_to_name)

    # Current stress value (last valid)
    s_current = float(stress_series[-1]) if len(stress_series) > 0 else 0.0

    # First derivative: rate of change over DERIVATIVE_WINDOW days
    if len(stress_series) > DERIVATIVE_WINDOW:
        ds = float(stress_series[-1] - stress_series[-1 - DERIVATIVE_WINDOW]) / DERIVATIVE_WINDOW
    else:
        ds = 0.0

    # Classify
    regime, confidence = _classify_regime(s_current, ds)
    posture = POSTURE_MAP.get(regime, "BALANCED")

    # Historical regime distribution (last 504 days)
    lookback = stress_series[-504:] if len(stress_series) > 504 else stress_series
    dist = {"GROWTH": 0, "NEUTRAL": 0, "FRAGILE": 0, "CRISIS": 0}
    for i, s_val in enumerate(lookback):
        if i > DERIVATIVE_WINDOW:
            d = (lookback[i] - lookback[i - DERIVATIVE_WINDOW]) / DERIVATIVE_WINDOW
        else:
            d = 0.0
        r, _ = _classify_regime(float(s_val), float(d))
        dist[r] = dist.get(r, 0) + 1

    # Transition probability (how often regime changed in last 252 days)
    recent = lookback[-252:] if len(lookback) > 252 else lookback
    regimes_recent = []
    for i, s_val in enumerate(recent):
        if i > DERIVATIVE_WINDOW:
            d = (recent[i] - recent[i - DERIVATIVE_WINDOW]) / DERIVATIVE_WINDOW
        else:
            d = 0.0
        r, _ = _classify_regime(float(s_val), float(d))
        regimes_recent.append(r)
    transitions = sum(1 for i in range(1, len(regimes_recent)) if regimes_recent[i] != regimes_recent[i - 1])
    trans_prob = transitions / max(len(regimes_recent), 1)

    # Contradiction flags
    contradictions = {}
    # Sort contributions by absolute contribution
    sorted_contribs = sorted(contributions.items(), key=lambda x: abs(x[1]["contribution"]), reverse=True)
    top_stress = [c for c in sorted_contribs if c[1]["contribution"] > 0][:3]
    top_calm = [c for c in sorted_contribs if c[1]["contribution"] < 0][:3]

    if regime == "GROWTH" and top_stress:
        biggest_stress = top_stress[0]
        if biggest_stress[1]["contribution"] > 0.05:
            contradictions["stress_in_growth"] = (
                f"{biggest_stress[0]} z={biggest_stress[1]['z']:.1f} is elevated despite GROWTH regime"
            )
    if regime in ("FRAGILE", "CRISIS") and top_calm:
        biggest_calm = top_calm[0]
        if biggest_calm[1]["contribution"] < -0.05:
            contradictions["calm_in_stress"] = (
                f"{biggest_calm[0]} z={biggest_calm[1]['z']:.1f} is calm despite {regime} regime"
            )

    # Insert into decision_journal
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO decision_journal "
                "(model_version_id, inferred_state, state_confidence, "
                "transition_probability, contradiction_flags, grid_recommendation, "
                "baseline_recommendation, action_taken, counterfactual, "
                "operator_confidence, decision_timestamp) "
                "VALUES (:mid, :state, :conf, :tp, :flags, :rec, 'NEUTRAL', "
                ":action, :cf, 'HIGH', NOW())"
            ),
            {
                "mid": model_id,
                "state": regime,
                "conf": confidence,
                "tp": trans_prob,
                "flags": json.dumps(contradictions),
                "rec": posture,
                "action": f"AUTO_{posture}",
                "cf": f"S={s_current:.2f}, dS/dt={ds:.3f}",
            },
        )

    # Persist snapshot
    try:
        from store.snapshots import AnalyticalSnapshotStore

        snap_store = AnalyticalSnapshotStore(db_engine=engine)
        snap_store.save_snapshot(
            category="regime_detection",
            payload={
                "regime": regime,
                "confidence": confidence,
                "posture": posture,
                "stress_index": round(s_current, 4),
                "stress_derivative": round(ds, 4),
                "transition_probability": trans_prob,
                "distribution": dist,
                "contradictions": contradictions,
                "contributions": {k: v for k, v in sorted_contribs[:10]},
                "n_features": len(fid_to_name),
                "n_observations": len(df),
                "features_used": list(found_names),
                "features_missing": missing,
            },
            as_of_date=today,
            metrics={
                "regime": regime,
                "confidence": round(confidence, 4),
                "stress_index": round(s_current, 4),
                "stress_derivative": round(ds, 4),
            },
        )
    except Exception as exc:
        log.warning("Snapshot persistence failed: {e}", e=str(exc))

    result = {
        "regime": regime,
        "confidence": confidence,
        "posture": posture,
        "stress_index": round(s_current, 4),
        "stress_derivative": round(ds, 4),
        "transition_probability": trans_prob,
        "distribution": dist,
        "contradictions": contradictions,
        "top_stress_drivers": [(k, v) for k, v in top_stress],
        "top_calm_drivers": [(k, v) for k, v in top_calm],
        "n_features": len(fid_to_name),
    }

    log.info("=== AUTO REGIME UPDATE ===")
    log.info("Regime:      {}", regime)
    log.info("Confidence:  {:.1%}", confidence)
    log.info("Posture:     {}", posture)
    log.info("Stress(S):   {:.3f}", s_current)
    log.info("dS/dt:       {:.4f}", ds)
    log.info("Trans prob:  {:.1%}", trans_prob)
    log.info("Distribution: {}", dist)
    stress_str = ', '.join(f"{k}({v['contribution']:+.3f})" for k, v in top_stress)
    calm_str = ', '.join(f"{k}({v['contribution']:+.3f})" for k, v in top_calm)
    log.info("Top stress:  {}", stress_str)
    log.info("Top calm:    {}", calm_str)
    if contradictions:
        log.info("Flags:       {}", contradictions)
    log.info("Features:    {} used, {} missing", len(fid_to_name), len(missing))
    log.info("Updated decision_journal")

    # Broadcast regime change to WebSocket clients if regime shifted
    try:
        from api.main import broadcast_event
        # Check previous regime from journal
        with engine.connect() as conn:
            prev_row = conn.execute(
                text(
                    "SELECT inferred_state FROM decision_journal "
                    "ORDER BY decision_timestamp DESC OFFSET 1 LIMIT 1"
                )
            ).fetchone()
        prev_regime = prev_row[0] if prev_row else None
        if prev_regime and prev_regime != regime:
            broadcast_event("regime_change", {
                "from": prev_regime,
                "to": regime,
                "confidence": round(confidence, 4),
                "stress_index": round(s_current, 4),
                "posture": posture,
            })
    except Exception:
        pass  # graceful degradation

    return result


if __name__ == "__main__":
    run()
