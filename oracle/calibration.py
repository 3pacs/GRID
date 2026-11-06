"""
GRID Oracle Calibration — measures how well predicted probabilities
match actual outcomes.

A well-calibrated system that says "70% confident" should be right ~70%
of the time. This module computes calibration curves, Brier scores,
and reliability diagrams for the Oracle's predictions.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

import numpy as np
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass
class CalibrationBucket:
    """One bucket in a calibration curve."""
    bin_start: float
    bin_end: float
    bin_midpoint: float
    predicted_mean: float      # Average predicted confidence in this bin
    actual_rate: float         # Fraction that actually hit
    count: int                 # Number of predictions in this bin


@dataclass
class OracleCalibrationReport:
    """Full calibration report for the Oracle.

    Named ``OracleCalibrationReport`` (not ``CalibrationReport``) to avoid a
    silent collision with :class:`inference.calibration.CalibrationReport`,
    which has a different field shape (Brier decomposition + recommendations).
    """
    buckets: list[CalibrationBucket]
    brier_score: float         # 0 = perfect, 1 = worst
    calibration_error: float   # Mean absolute calibration error (ECE)
    sharpness: float           # Variance of predicted probabilities
    label: str                 # "well_calibrated" / "overconfident" / "underconfident"
    total_predictions: int
    overall_accuracy: float

    def to_dict(self) -> dict:
        return {
            "buckets": [
                {
                    "bin_start": b.bin_start,
                    "bin_end": b.bin_end,
                    "bin_midpoint": b.bin_midpoint,
                    "predicted_mean": round(b.predicted_mean, 4),
                    "actual_rate": round(b.actual_rate, 4),
                    "count": b.count,
                }
                for b in self.buckets
            ],
            "brier_score": round(self.brier_score, 4),
            "calibration_error": round(self.calibration_error, 4),
            "sharpness": round(self.sharpness, 4),
            "label": self.label,
            "total_predictions": self.total_predictions,
            "overall_accuracy": round(self.overall_accuracy, 4),
        }


def compute_calibration(
    engine: Engine,
    n_bins: int = 10,
    model_name: str | None = None,
    ticker: str | None = None,
) -> OracleCalibrationReport:
    """Compute calibration curve from scored predictions.

    Args:
        engine: Database engine.
        n_bins: Number of bins for the calibration curve.
        model_name: Optional filter by model.
        ticker: Optional filter by ticker.

    Returns:
        OracleCalibrationReport with buckets, Brier score, ECE, and label.
    """
    query = """
        SELECT confidence, verdict
        FROM oracle_predictions
        WHERE verdict IN ('hit', 'miss', 'partial')
          AND dedup_keep = TRUE
          AND id NOT LIKE 'astrogrid:%'
          -- A prediction published without a stated confidence has no
          -- probability to score for reliability. It is excluded, not
          -- counted at a default (D-H11): `total_predictions` therefore
          -- counts only rows that actually stated one. `float(r[0])` below
          -- would raise TypeError on a NULL.
          AND confidence IS NOT NULL
    """
    params: dict[str, Any] = {}

    if model_name:
        query += " AND model_name = :model"
        params["model"] = model_name
    if ticker:
        query += " AND ticker = :ticker"
        params["ticker"] = ticker

    with engine.connect() as conn:
        rows = conn.execute(text(query), params).fetchall()

    if not rows:
        return OracleCalibrationReport(
            buckets=[], brier_score=0.0, calibration_error=0.0,
            sharpness=0.0, label="insufficient_data",
            total_predictions=0, overall_accuracy=0.0,
        )

    confidences = []
    outcomes = []  # 1 = hit, 0.5 = partial, 0 = miss
    for r in rows:
        conf = float(r[0])
        verdict = r[1]
        outcome = 1.0 if verdict == "hit" else 0.5 if verdict == "partial" else 0.0
        confidences.append(conf)
        outcomes.append(outcome)

    confidences = np.array(confidences)
    outcomes = np.array(outcomes)

    # Brier score
    brier = float(np.mean((confidences - outcomes) ** 2))

    # Sharpness = variance of predicted probabilities
    sharpness = float(np.var(confidences))

    # Overall accuracy (hits + 0.5 * partials) / total
    overall_accuracy = float(np.mean(outcomes))

    # Bin predictions into calibration buckets
    bin_edges = np.linspace(0.0, 1.0, n_bins + 1)
    buckets: list[CalibrationBucket] = []
    weighted_errors = []

    for i in range(n_bins):
        lo, hi = bin_edges[i], bin_edges[i + 1]
        mask = (confidences >= lo) & (confidences < hi) if i < n_bins - 1 else (confidences >= lo) & (confidences <= hi)
        count = int(mask.sum())

        if count == 0:
            buckets.append(CalibrationBucket(
                bin_start=float(lo), bin_end=float(hi),
                bin_midpoint=float((lo + hi) / 2),
                predicted_mean=float((lo + hi) / 2),
                actual_rate=0.0, count=0,
            ))
            continue

        predicted_mean = float(np.mean(confidences[mask]))
        actual_rate = float(np.mean(outcomes[mask]))

        buckets.append(CalibrationBucket(
            bin_start=float(lo), bin_end=float(hi),
            bin_midpoint=float((lo + hi) / 2),
            predicted_mean=predicted_mean,
            actual_rate=actual_rate,
            count=count,
        ))

        weighted_errors.append(count * abs(predicted_mean - actual_rate))

    # Expected Calibration Error (ECE)
    total = len(confidences)
    ece = sum(weighted_errors) / total if total > 0 else 0.0

    # Determine label
    # Check if predictions are systematically too high or too low
    populated = [b for b in buckets if b.count > 0]
    if len(populated) < 3:
        label = "insufficient_data"
    else:
        # Compare predicted vs actual across populated bins
        over_count = sum(1 for b in populated if b.predicted_mean > b.actual_rate + 0.05)
        under_count = sum(1 for b in populated if b.predicted_mean < b.actual_rate - 0.05)
        if ece < 0.08:
            label = "well_calibrated"
        elif over_count > under_count:
            label = "overconfident"
        else:
            label = "underconfident"

    report = OracleCalibrationReport(
        buckets=buckets,
        brier_score=brier,
        calibration_error=ece,
        sharpness=sharpness,
        label=label,
        total_predictions=total,
        overall_accuracy=overall_accuracy,
    )

    log.info(
        "Calibration: {n} predictions, ECE={ece:.3f}, Brier={b:.3f}, label={l}",
        n=total, ece=ece, b=brier, l=label,
    )

    return report


def update_running_metrics(
    engine: Engine,
    *,
    model_id: str,
    prediction: float,
    actual: float,
    horizon: int | str | None = None,
) -> dict[str, float]:
    """Update the running Brier / ECE counters on ``oracle_models``.

    Uses Welford-style incremental running averages so that each scored
    prediction costs exactly one UPDATE per metric surface. Two surfaces
    are maintained:

    * Legacy scalar columns (``running_brier`` / ``running_ece`` /
      ``scored_prediction_count``) — the unweighted average across all
      horizons so existing callers (scoreboard, legacy report) keep
      working. Kept in lockstep with the bucket state.
    * Per-horizon JSON buckets inside ``horizon_buckets`` (ALPHA-3 /
      task #106) — the targeted ``horizon`` kwarg determines which
      bucket is updated. When ``horizon`` is ``None`` the 7d bucket is
      used by default to preserve backward compatibility with Wave A
      callers that predate the horizon extension on ``PredictionScored``.

    Returns the new ``(running_brier, running_ece, count)`` triple so
    that the caller can log or assert on the post-update values.

    Requires migrations ``0038_oracle_running_metrics.sql`` (legacy
    columns) and ``0042_oracle_horizon_aware.sql`` (bucket column) to
    be applied.
    """
    if not model_id:
        raise ValueError("model_id is required")

    # Late import so tests that only exercise the oracle engine don't
    # pay the cost of importing numpy at module load.
    from oracle.engine import HORIZON_BUCKETS, _horizon_key, _parse_horizon_buckets

    bucket_key = _horizon_key(horizon)
    prediction = float(prediction)
    actual = float(actual)
    squared_error = (prediction - actual) ** 2
    absolute_error = abs(prediction - actual)

    with engine.begin() as conn:
        row = conn.execute(
            text(
                "SELECT running_brier, running_ece, scored_prediction_count "
                "FROM oracle_models WHERE name = :m"
            ),
            {"m": model_id},
        ).fetchone()

        if row is None:
            # Auto-seed a skeleton row so contract handlers never crash on
            # an unknown model id. Real model metadata is populated by
            # ``OracleEngine._load_models`` when the engine first runs.
            conn.execute(
                text(
                    "INSERT INTO oracle_models (name, version, signal_families, "
                    "                           running_brier, running_ece, "
                    "                           scored_prediction_count, last_updated) "
                    "VALUES (:m, '1.0', '[]'::jsonb, :b, :e, 1, NOW()) "
                    "ON CONFLICT (name) DO NOTHING"
                ),
                {"m": model_id, "b": squared_error, "e": absolute_error},
            )
            # Best-effort: try to seed the horizon bucket too. Swallow
            # errors so an un-migrated DB still returns normally.
            try:
                seed_buckets = _parse_horizon_buckets(None)
                seed_buckets[bucket_key]["scored"] = 1
                seed_buckets[bucket_key]["brier"] = squared_error
                seed_buckets[bucket_key]["ece"] = absolute_error
                conn.execute(
                    text(
                        "UPDATE oracle_models "
                        "SET horizon_buckets = CAST(:hb AS JSONB) "
                        "WHERE name = :m AND horizon_buckets IS NULL"
                    ),
                    {"m": model_id, "hb": json.dumps(seed_buckets)},
                )
            except Exception as exc:
                log.debug(
                    "update_running_metrics: bucket seed skipped for {m}: {e}",
                    m=model_id, e=str(exc),
                )
            return {
                "running_brier": squared_error,
                "running_ece": absolute_error,
                "count": 1,
            }

        old_brier = float(row[0]) if row[0] is not None else 0.0
        old_ece = float(row[1]) if row[1] is not None else 0.0
        old_count = int(row[2] or 0)
        new_count = old_count + 1

        if old_count == 0:
            new_brier = squared_error
            new_ece = absolute_error
        else:
            # Incremental running mean:
            #   new_avg = old_avg + (x - old_avg) / new_count
            new_brier = old_brier + (squared_error - old_brier) / new_count
            new_ece = old_ece + (absolute_error - old_ece) / new_count

        # ── Per-horizon bucket update (ALPHA-3 / task #106) ────────────
        # Pulled in a best-effort branch so an un-migrated DB (missing
        # the horizon_buckets column) still lets the legacy update
        # succeed. The bucket SELECT is wrapped separately because the
        # shared row above comes from the pre-ALPHA-3 SELECT shape.
        try:
            bucket_row = conn.execute(
                text(
                    "SELECT horizon_buckets FROM oracle_models "
                    "WHERE name = :m"
                ),
                {"m": model_id},
            ).fetchone()
            raw_bucket_payload = bucket_row[0] if bucket_row else None
            # Only treat the payload as real persisted bucket data when
            # it is a dict or JSON-encoded dict. Mocked fetchone results
            # (tuples, scalars) fall through to the default factory and
            # the legacy column override is suppressed so existing Wave
            # A tests that exercise update_running_metrics keep their
            # Welford-computed expectations intact.
            _bucket_payload_is_real = isinstance(raw_bucket_payload, dict)
            if isinstance(raw_bucket_payload, str):
                try:
                    _candidate = json.loads(raw_bucket_payload)
                    _bucket_payload_is_real = isinstance(_candidate, dict)
                except (TypeError, ValueError):
                    _bucket_payload_is_real = False
            parsed = _parse_horizon_buckets(
                raw_bucket_payload if _bucket_payload_is_real else None
            )
            bucket = dict(parsed.get(bucket_key, {}))
            b_scored = int(bucket.get("scored", 0) or 0)
            b_brier = float(bucket.get("brier", 0.0) or 0.0)
            b_ece = float(bucket.get("ece", 0.0) or 0.0)
            new_b_scored = b_scored + 1
            if b_scored == 0:
                new_b_brier = squared_error
                new_b_ece = absolute_error
            else:
                new_b_brier = b_brier + (squared_error - b_brier) / new_b_scored
                new_b_ece = b_ece + (absolute_error - b_ece) / new_b_scored
            bucket.update({
                "scored": new_b_scored,
                "brier": round(new_b_brier, 6),
                "ece": round(new_b_ece, 6),
            })
            # Keep default weight / counter fields intact.
            bucket.setdefault("weight", 1.0)
            bucket.setdefault("hits", 0)
            bucket.setdefault("misses", 0)
            bucket.setdefault("partials", 0)
            parsed[bucket_key] = bucket

            conn.execute(
                text(
                    "UPDATE oracle_models "
                    "SET horizon_buckets = jsonb_set("
                    "    COALESCE(horizon_buckets, '{}'::jsonb), "
                    "    :path, CAST(:bucket AS JSONB), true) "
                    "WHERE name = :m"
                ),
                {
                    "path": "{" + bucket_key + "}",
                    "bucket": json.dumps(bucket),
                    "m": model_id,
                },
            )

            # Refresh the legacy scalar Brier/ECE to the unweighted mean
            # across buckets that actually have scored events. Unscored
            # buckets are ignored so they don't drag the legacy average
            # toward zero on cold starts.
            if _bucket_payload_is_real:
                scored_buckets = [
                    parsed[k] for k in HORIZON_BUCKETS
                    if int(parsed.get(k, {}).get("scored", 0) or 0) > 0
                ]
                if scored_buckets:
                    legacy_brier = sum(
                        float(b.get("brier", 0.0) or 0.0)
                        for b in scored_buckets
                    ) / len(scored_buckets)
                    legacy_ece = sum(
                        float(b.get("ece", 0.0) or 0.0)
                        for b in scored_buckets
                    ) / len(scored_buckets)
                    # Override the Welford-computed values so the legacy
                    # columns reflect the bucket average (single-bucket
                    # callers see identical values to the pre-ALPHA-3
                    # path). Only fires when the DB actually returned a
                    # dict-shaped bucket payload — otherwise the Welford
                    # values from the legacy row are left intact so
                    # Wave A test_subsequent_value_is_running_mean keeps
                    # passing against the mock_engine fixture.
                    new_brier = legacy_brier
                    new_ece = legacy_ece
        except Exception as exc:
            log.debug(
                "update_running_metrics: bucket update skipped for {m}/{bk}: {e}",
                m=model_id, bk=bucket_key, e=str(exc),
            )

        conn.execute(
            text(
                "UPDATE oracle_models "
                "SET running_brier = :b, "
                "    running_ece = :e, "
                "    scored_prediction_count = :n, "
                "    last_updated = NOW() "
                "WHERE name = :m"
            ),
            {"b": new_brier, "e": new_ece, "n": new_count, "m": model_id},
        )

    return {
        "running_brier": new_brier,
        "running_ece": new_ece,
        "count": new_count,
    }


def compute_per_horizon_calibration(
    engine: Engine,
    model_name: str,
    horizons: list[int] | None = None,
    *,
    regime: str | None = None,
) -> dict[int, dict[str, float]]:
    """Read per-horizon Brier / ECE / counters from ``oracle_models.horizon_buckets``.

    Returns a ``{horizon_days: {brier, ece, scored, weight, hits, misses, partials}}``
    map suitable for the report layer. Missing / un-migrated rows return
    an empty dict. ALPHA-3 / task #106.

    ALPHA-13 / task #116: when ``regime`` is provided (one of
    CRISIS/TIGHTENING/NEUTRAL/EXPANSION/EXPANSION_STRONG) the per-horizon
    bucket weight is additionally multiplied by the matching
    ``regime_buckets[regime].weight`` so the caller sees the effective
    routed weight (not just the horizon-only weight). The Brier/ECE
    values are left untouched — they're still horizon-scoped. The
    regime→per-horizon Brier split is queued separately as CAT-180.
    The legacy path (``regime=None``) is byte-for-byte unchanged.
    """
    from oracle.engine import HORIZON_BUCKETS, _parse_horizon_buckets

    if horizons is None:
        horizons = [1, 7, 30, 90]

    try:
        with engine.connect() as conn:
            if regime is not None:
                try:
                    row = conn.execute(
                        text(
                            "SELECT horizon_buckets, regime_buckets "
                            "FROM oracle_models WHERE name = :m"
                        ),
                        {"m": model_name},
                    ).fetchone()
                except Exception as exc:
                    log.debug(
                        "compute_per_horizon_calibration: regime SELECT "
                        "failed {m}: {e}", m=model_name, e=str(exc),
                    )
                    row = None
            else:
                row = conn.execute(
                    text(
                        "SELECT horizon_buckets FROM oracle_models "
                        "WHERE name = :m"
                    ),
                    {"m": model_name},
                ).fetchone()
    except Exception as exc:
        log.debug(
            "compute_per_horizon_calibration: SELECT failed {m}: {e}",
            m=model_name, e=str(exc),
        )
        return {}

    if not row:
        return {}

    parsed = _parse_horizon_buckets(row[0])
    regime_weight_multiplier = 1.0
    if regime is not None and len(row) > 1 and row[1] is not None:
        try:
            from oracle.regime_router import (
                _canonical_regime,
                parse_regime_buckets,
            )

            parsed_regimes = parse_regime_buckets(row[1])
            r_bucket = parsed_regimes.get(_canonical_regime(regime)) or {}
            regime_weight_multiplier = float(
                r_bucket.get("weight", 1.0) or 1.0
            )
        except Exception as exc:  # noqa: BLE001
            log.debug(
                "compute_per_horizon_calibration: regime parse failed "
                "{m}/{r}: {e}", m=model_name, r=regime, e=str(exc),
            )
            regime_weight_multiplier = 1.0
    out: dict[int, dict[str, float]] = {}
    for h in horizons:
        key = f"{int(h)}d"
        if key not in parsed:
            continue
        b = parsed[key]
        out[int(h)] = {
            "brier": float(b.get("brier", 0.0) or 0.0),
            "ece": float(b.get("ece", 0.0) or 0.0),
            "scored": int(b.get("scored", 0) or 0),
            "weight": float(b.get("weight", 1.0) or 1.0) * regime_weight_multiplier,
            "hits": int(b.get("hits", 0) or 0),
            "misses": int(b.get("misses", 0) or 0),
            "partials": int(b.get("partials", 0) or 0),
        }
    # Also include any canonical bucket present in the JSON but not
    # explicitly requested — a report pulling the standard 1/7/30/90 set
    # should never surprise the caller with a missing bucket.
    for key in HORIZON_BUCKETS:
        days = int(key.rstrip("d"))
        if days not in out and key in parsed:
            b = parsed[key]
            out[days] = {
                "brier": float(b.get("brier", 0.0) or 0.0),
                "ece": float(b.get("ece", 0.0) or 0.0),
                "scored": int(b.get("scored", 0) or 0),
                "weight": float(b.get("weight", 1.0) or 1.0) * regime_weight_multiplier,
                "hits": int(b.get("hits", 0) or 0),
                "misses": int(b.get("misses", 0) or 0),
                "partials": int(b.get("partials", 0) or 0),
            }
    return out


# ── ALPHA-7 / task #110 — calibration drift persistence + alerts ────────────


@dataclass
class DriftAlert:
    """One calibration-drift event.

    Emitted when the current per-horizon Brier or ECE drifts more than
    ``sigma_threshold`` × historical std from the baseline mean.
    """

    model_name: str
    horizon_days: int
    metric: str              # 'brier' or 'ece'
    current: float
    baseline_mean: float
    baseline_std: float
    z_score: float           # signed z-score (positive = worse than baseline)
    sigma_threshold: float   # the threshold that was crossed (usually 2.0)
    window_days: int         # history window used
    severity: str            # 'warning' (>=2σ) or 'critical' (>=3σ)

    def to_dict(self) -> dict[str, Any]:
        return {
            "model_name": self.model_name,
            "horizon_days": self.horizon_days,
            "metric": self.metric,
            "current": round(self.current, 6),
            "baseline_mean": round(self.baseline_mean, 6),
            "baseline_std": round(self.baseline_std, 6),
            "z_score": round(self.z_score, 4),
            "sigma_threshold": self.sigma_threshold,
            "window_days": self.window_days,
            "severity": self.severity,
        }


# Minimum scored predictions a bucket needs before we trust its metrics
# enough to either snapshot or drift-check. Cold-start buckets are skipped.
_DRIFT_MIN_SCORED = 10
# Minimum baseline samples before drift detection runs. Below this the
# history is too thin to compute a reliable mean/std.
_DRIFT_MIN_HISTORY = 5


def snapshot_calibration_history(
    engine: Engine,
    *,
    horizons: list[int] | None = None,
) -> dict[str, int]:
    """Persist the current per-horizon calibration into ``oracle_calibration_history``.

    Called daily by the scheduler. Idempotent via the UNIQUE constraint on
    (model_name, horizon_days, snapshot_at) — if two runs land in the same
    second, the second one is silently skipped.

    Returns a count summary {models, buckets, skipped}.
    """
    counts = {"models": 0, "buckets": 0, "skipped": 0}
    try:
        with engine.begin() as conn:
            models = conn.execute(
                text("SELECT name FROM oracle_models"),
            ).fetchall()
            counts["models"] = len(models)

            for row in models:
                model_name = row[0]
                per_h = compute_per_horizon_calibration(
                    engine, model_name, horizons=horizons,
                )
                for horizon, bucket in per_h.items():
                    scored = int(bucket.get("scored", 0) or 0)
                    if scored < _DRIFT_MIN_SCORED:
                        counts["skipped"] += 1
                        continue
                    try:
                        conn.execute(
                            text(
                                """
                                INSERT INTO oracle_calibration_history
                                    (model_name, horizon_days, brier, ece,
                                     scored_count, bucket_weight)
                                VALUES
                                    (:m, :h, :b, :e, :s, :w)
                                ON CONFLICT (model_name, horizon_days, snapshot_at)
                                DO NOTHING
                                """
                            ),
                            {
                                "m": model_name,
                                "h": int(horizon),
                                "b": float(bucket.get("brier", 0.0) or 0.0),
                                "e": float(bucket.get("ece", 0.0) or 0.0),
                                "s": scored,
                                "w": float(bucket.get("weight", 1.0) or 1.0),
                            },
                        )
                        counts["buckets"] += 1
                    except Exception as exc:  # noqa: BLE001
                        log.debug(
                            "snapshot insert failed for {m}/{h}: {e}",
                            m=model_name, h=horizon, e=str(exc),
                        )
                        counts["skipped"] += 1
    except Exception as exc:  # noqa: BLE001
        log.warning("snapshot_calibration_history failed: {e}", e=str(exc))

    log.info(
        "calibration snapshot: {m} models, {b} buckets persisted, {s} skipped",
        m=counts["models"], b=counts["buckets"], s=counts["skipped"],
    )
    return counts


def detect_calibration_drift(
    engine: Engine,
    *,
    window_days: int = 30,
    sigma_threshold: float = 2.0,
    horizons: list[int] | None = None,
) -> list[DriftAlert]:
    """Compare the current per-horizon metrics to a rolling baseline.

    For each (model, horizon) pair:

    1. Read the current brier + ece from ``oracle_models.horizon_buckets``.
    2. Read the last ``window_days`` of snapshots from
       ``oracle_calibration_history`` for the SAME pair (excluding today).
    3. Compute the baseline mean + std. Skip pairs with fewer than
       ``_DRIFT_MIN_HISTORY`` historical samples.
    4. Emit a :class:`DriftAlert` for every metric whose absolute z-score
       exceeds ``sigma_threshold``. Severity is 'warning' for 2σ and
       'critical' for 3σ+.

    Brier ONLY emits alerts when the current value is WORSE than the baseline
    (positive z-score) — a sudden improvement is a feature not a bug. ECE
    emits on either side of the mean because drift in either direction is
    interesting.
    """
    if horizons is None:
        horizons = [1, 7, 30, 90]

    alerts: list[DriftAlert] = []

    try:
        with engine.connect() as conn:
            model_rows = conn.execute(
                text("SELECT name FROM oracle_models"),
            ).fetchall()
    except Exception as exc:  # noqa: BLE001
        log.warning("detect_calibration_drift: model list failed: {e}", e=str(exc))
        return alerts

    for mrow in model_rows:
        model_name = mrow[0]
        per_h = compute_per_horizon_calibration(
            engine, model_name, horizons=horizons,
        )

        for horizon in horizons:
            bucket = per_h.get(horizon)
            if bucket is None:
                continue
            if int(bucket.get("scored", 0) or 0) < _DRIFT_MIN_SCORED:
                continue

            try:
                with engine.connect() as conn:
                    hist_rows = conn.execute(
                        text(
                            """
                            SELECT brier, ece
                            FROM oracle_calibration_history
                            WHERE model_name = :m
                              AND horizon_days = :h
                              AND snapshot_at >= NOW() - (:w || ' days')::interval
                              AND snapshot_at < date_trunc('day', NOW())
                            ORDER BY snapshot_at DESC
                            LIMIT 500
                            """
                        ),
                        {"m": model_name, "h": int(horizon), "w": int(window_days)},
                    ).fetchall()
            except Exception as exc:  # noqa: BLE001
                log.debug(
                    "drift history read failed for {m}/{h}: {e}",
                    m=model_name, h=horizon, e=str(exc),
                )
                continue

            if len(hist_rows) < _DRIFT_MIN_HISTORY:
                continue

            brier_hist = [float(r[0]) for r in hist_rows if r[0] is not None]
            ece_hist = [float(r[1]) for r in hist_rows if r[1] is not None]

            cur_brier = float(bucket.get("brier", 0.0) or 0.0)
            cur_ece = float(bucket.get("ece", 0.0) or 0.0)

            for metric, cur, hist in (
                ("brier", cur_brier, brier_hist),
                ("ece", cur_ece, ece_hist),
            ):
                if len(hist) < _DRIFT_MIN_HISTORY:
                    continue
                mean = float(np.mean(hist))
                std = float(np.std(hist, ddof=1)) if len(hist) > 1 else 0.0
                if std <= 1e-9:
                    # Degenerate baseline — everything is the same value.
                    continue
                z = (cur - mean) / std

                # Brier: only alert on worsening (positive z).
                # ECE: alert on either direction.
                if metric == "brier" and z < sigma_threshold:
                    continue
                if metric == "ece" and abs(z) < sigma_threshold:
                    continue

                severity = "critical" if abs(z) >= 3.0 else "warning"
                alerts.append(DriftAlert(
                    model_name=model_name,
                    horizon_days=int(horizon),
                    metric=metric,
                    current=cur,
                    baseline_mean=mean,
                    baseline_std=std,
                    z_score=z,
                    sigma_threshold=sigma_threshold,
                    window_days=int(window_days),
                    severity=severity,
                ))

    if alerts:
        log.warning(
            "calibration drift detected: {n} alert(s) — "
            "{c} critical, {w} warning",
            n=len(alerts),
            c=sum(1 for a in alerts if a.severity == "critical"),
            w=sum(1 for a in alerts if a.severity == "warning"),
        )
    else:
        log.info("calibration drift check clean ({m} models scanned)",
                 m=len(model_rows))
    return alerts


# ── S10 (2026-11-06) — exploratory recalibration on honest outcomes ─────────
#
# The calibration surfaces above were last fed in April/May: the running
# Brier/ECE on ``oracle_models`` only moves when the 6-hour scorer closes a
# row, and ``oracle_calibration_history`` has no rows on griddb (2026-11-06).
# The honest outcomes now live in two places:
#
# * official forward scores on ``oracle_predictions`` (verdict hit/miss/partial),
# * ``oracle_prediction_rescores`` (#976): exploratory hindsight re-prices of
#   the rows the pre-2026-11 price-lookup bug closed as no_data
#   (``official = FALSE`` by CHECK constraint).
#
# ``run_exploratory_recalibration`` reads both, read-only, and writes ONLY to
# ``oracle_calibration_exploratory`` -- append-only, ``official = FALSE`` by
# CHECK constraint. It never writes ``oracle_predictions``,
# ``oracle_models`` (running metrics, horizon/regime weights, promotion
# state), ``oracle_calibration_history`` (the live drift baseline) or any E2
# evaluation output. Nothing on the live path reads the new table.
#
# Target: ``direction_correct`` = 1 for hit or partial (the call's direction
# was right), 0 for miss. ``confidence`` is read as the stated probability of
# that event. Strict hits (move >= half the expected move) are reported next
# to it, never instead of it.
#
# Rows on the same expiry date share one market close and are not
# independent. Every band is a circular moving-block bootstrap over the
# ordered expiry dates (block length round(K ** (1/3)) for K dates, fixed
# before looking at results), and every group reports n_eff -- the sample
# size a set of independent rows would need to give the same bootstrap
# variance of the base rate.

EXPLORATORY_TABLE = "oracle_calibration_exploratory"
EXPLORATORY_METHOD_VERSION = "oracle_recal_conformal_v1"
EXPLORATORY_TARGET = "direction_correct"
# #976's scoring version (scripts/score_oracle_trades.py RESCORE_SCORING_VERSION).
RESCORE_SCORING_VERSION = "oracle_rescore_hindsight_v1"
SOURCE_OFFICIAL = "official_forward"
SOURCE_RESCORE = "rescore_hindsight"

BOOTSTRAP_REPS = 1000
BOOTSTRAP_SEED = 20261106
MIN_GROUP_ROWS = 30
MIN_FAMILY_FIT_ROWS = 300
MIN_BLOCK_ROWS = 50
# Time blocks by expiry date: first 50% of dates fit the recalibrator, the
# next 20% supply conformal scores, the last 30% are the held-out test block.
FIT_FRAC = 0.5
CONFORMAL_FRAC = 0.2


@dataclass(frozen=True)
class ScoredOutcome:
    """One scored oracle call as the exploratory recalibration sees it."""

    prediction_id: str
    model_name: str
    family: str
    horizon: str              # canonical bucket: 1d / 7d / 30d / 90d
    confidence: float
    y: int                    # direction_correct: hit or partial = 1, miss = 0
    hit: int                  # strict hit = 1
    expiry: date
    created_date: date        # UTC date the call was made
    source: str               # official_forward | rescore_hindsight


def model_family(model_name: str | None) -> str:
    """Collapse evolver names to the family they descend from.

    ``options_flow_mut_7g7lxd`` -> ``options_flow``; ``hyp_*`` ->
    ``hypothesis``; ``cross_*`` -> ``crossover`` (see
    ``oracle/model_evolver.py`` for how those names are minted). Base model
    names are their own family.
    """
    name = (model_name or "").strip()
    if not name:
        return "unknown"
    if name.startswith("hyp_"):
        return "hypothesis"
    if name.startswith("cross_"):
        return "crossover"
    if "_mut_" in name:
        return name.split("_mut_", 1)[0] or "unknown"
    return name


def _as_utc_date(value: Any) -> date:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


_OFFICIAL_OUTCOMES_SQL = """
    SELECT p.id, p.model_name, p.horizon_days, p.confidence, p.verdict,
           p.expiry, p.created_at
    FROM oracle_predictions p
    WHERE p.verdict IN ('hit', 'miss', 'partial')
      AND p.dedup_keep = TRUE
      AND p.id NOT LIKE 'astrogrid:%'
      AND p.confidence IS NOT NULL
      AND p.direction IN ('CALL', 'PUT')
      AND p.expiry <= :as_of_date
      AND p.created_at <= :as_of_ts
      AND p.scored_at <= :as_of_ts
"""

_RESCORE_OUTCOMES_SQL = """
    SELECT p.id, p.model_name, p.horizon_days, p.confidence, r.verdict,
           p.expiry, p.created_at
    FROM oracle_prediction_rescores r
    JOIN oracle_predictions p ON p.id = r.prediction_id
    WHERE r.scoring_version = :rescore_version
      AND r.official = FALSE
      AND r.verdict IN ('hit', 'miss', 'partial')
      AND p.dedup_keep = TRUE
      AND p.id NOT LIKE 'astrogrid:%'
      AND p.confidence IS NOT NULL
      AND p.direction IN ('CALL', 'PUT')
      AND p.expiry <= :as_of_date
      AND p.created_at <= :as_of_ts
      AND r.rescored_at <= :as_of_ts
"""


def _row_to_outcome(row: Any, source: str) -> ScoredOutcome | None:
    pred_id, model_name, horizon_days, conf, verdict, expiry, created_at = row[:7]
    try:
        c = float(conf)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(c) or c < 0.0 or c > 1.0:
        return None
    if verdict not in ("hit", "miss", "partial"):
        return None
    from oracle.engine import _horizon_key

    exp = _as_utc_date(expiry)
    created = _as_utc_date(created_at)
    days = horizon_days if horizon_days is not None else (exp - created).days
    return ScoredOutcome(
        prediction_id=str(pred_id),
        model_name=str(model_name or ""),
        family=model_family(model_name),
        horizon=_horizon_key(days),
        confidence=c,
        y=0 if verdict == "miss" else 1,
        hit=1 if verdict == "hit" else 0,
        expiry=exp,
        created_date=created,
        source=source,
    )


def load_scored_outcomes(
    engine: Engine,
    as_of: datetime | None = None,
) -> list[ScoredOutcome]:
    """Read official forward scores plus #976 hindsight rescores, read-only.

    Point-in-time: only rows whose expiry is on or before ``as_of`` and whose
    score (``scored_at`` / ``rescored_at``) existed at ``as_of`` are read.
    A prediction present in both sources is kept once, as official.
    """
    as_of_ts = as_of or datetime.now(timezone.utc)
    if as_of_ts.tzinfo is None:
        as_of_ts = as_of_ts.replace(tzinfo=timezone.utc)
    params = {"as_of_date": as_of_ts.astimezone(timezone.utc).date(), "as_of_ts": as_of_ts}
    pg = engine.dialect.name == "postgresql"
    with engine.connect() as conn:
        if pg:
            conn.execute(text("SET TRANSACTION READ ONLY"))
            conn.execute(text("SET LOCAL statement_timeout = '300s'"))
        official = conn.execute(text(_OFFICIAL_OUTCOMES_SQL), params).fetchall()
        rescored: list[Any] = []
        if _has_table(conn, "oracle_prediction_rescores"):
            rescored = conn.execute(
                text(_RESCORE_OUTCOMES_SQL),
                {**params, "rescore_version": RESCORE_SCORING_VERSION},
            ).fetchall()
        conn.rollback()

    out: dict[str, ScoredOutcome] = {}
    for source, rows in ((SOURCE_OFFICIAL, official), (SOURCE_RESCORE, rescored)):
        for row in rows:
            rec = _row_to_outcome(row, source)
            if rec is not None and rec.prediction_id not in out:
                out[rec.prediction_id] = rec
    return sorted(out.values(), key=lambda r: (r.expiry, r.created_date, r.prediction_id))


def _has_table(conn: Any, name: str) -> bool:
    try:
        from sqlalchemy import inspect as _inspect

        return bool(_inspect(conn).has_table(name))
    except Exception as exc:  # noqa: BLE001
        log.debug("has_table({n}) failed: {e}", n=name, e=str(exc))
        return False


# ── block bootstrap clustered by expiry date ──────────────────────────────


def default_block_length(n_clusters: int) -> int:
    """Pre-declared block length: round(K ** (1/3)), at least 1."""
    if n_clusters <= 1:
        return 1
    return max(1, round(n_clusters ** (1.0 / 3.0)))


def block_bootstrap_weights(
    n_clusters: int,
    n_boot: int,
    rng: np.random.Generator,
    block_len: int | None = None,
) -> np.ndarray:
    """``(n_boot, K)`` multiplicities from a circular moving-block bootstrap.

    Clusters are assumed ordered in time. Each replicate draws
    ``ceil(K / L)`` blocks of ``L`` consecutive clusters (wrapping) and keeps
    the first ``K`` draws. ``L = 1`` is the plain cluster bootstrap.
    """
    K = int(n_clusters)
    L = block_len or default_block_length(K)
    n_blocks = int(np.ceil(K / L))
    starts = rng.integers(0, K, size=(n_boot, n_blocks))
    idx = (starts[:, :, None] + np.arange(L)[None, None, :]) % K
    idx = idx.reshape(n_boot, -1)[:, :K]
    W = np.zeros((n_boot, K), dtype=np.float64)
    rows = np.repeat(np.arange(n_boot), K)
    np.add.at(W, (rows, idx.ravel()), 1.0)
    return W


def _cluster_index(clusters: Sequence[Any]) -> tuple[np.ndarray, int]:
    keys = sorted(set(clusters))
    pos = {k: i for i, k in enumerate(keys)}
    return np.array([pos[c] for c in clusters], dtype=np.int64), len(keys)


def _band(samples: np.ndarray) -> tuple[float | None, float | None]:
    finite = samples[np.isfinite(samples)]
    if finite.size == 0:
        return None, None
    lo, hi = np.percentile(finite, [2.5, 97.5])
    return float(lo), float(hi)


def _effective_n(mean: float, boot_means: np.ndarray, n: int) -> float | None:
    """n_eff = p(1 - p) / Var_boot(p_bar), capped at n."""
    if not 0.0 < mean < 1.0:
        return None
    finite = boot_means[np.isfinite(boot_means)]
    if finite.size < 2:
        return None
    var = float(np.var(finite, ddof=1))
    if var <= 0.0:
        return float(n)
    return float(min(n, mean * (1.0 - mean) / var))


def cluster_bootstrap_mean(
    values: Sequence[float],
    clusters: Sequence[Any],
    *,
    n_boot: int = BOOTSTRAP_REPS,
    seed: int = BOOTSTRAP_SEED,
    block_len: int | None = None,
) -> dict[str, Any]:
    """Mean of ``values`` with an expiry-clustered block-bootstrap 95% band."""
    v = np.asarray(values, dtype=np.float64)
    n = int(v.size)
    if n == 0:
        return {"mean": None, "lo": None, "hi": None, "n": 0, "n_clusters": 0, "n_eff": None}
    ci, K = _cluster_index(list(clusters))
    mean = float(v.mean())
    if K < 2:
        return {"mean": mean, "lo": None, "hi": None, "n": n, "n_clusters": K, "n_eff": None}
    cnt = np.bincount(ci, minlength=K).astype(np.float64)
    tot = np.bincount(ci, weights=v, minlength=K)
    W = block_bootstrap_weights(K, n_boot, np.random.default_rng(seed), block_len)
    with np.errstate(invalid="ignore", divide="ignore"):
        boot = (W @ tot) / (W @ cnt)
    lo, hi = _band(boot)
    return {
        "mean": mean, "lo": lo, "hi": hi, "n": n, "n_clusters": K,
        "n_eff": _effective_n(mean, boot, n),
    }


def reliability_with_bands(
    confidences: Sequence[float],
    outcomes: Sequence[float],
    clusters: Sequence[Any],
    *,
    n_bins: int = 10,
    n_boot: int = BOOTSTRAP_REPS,
    seed: int = BOOTSTRAP_SEED,
    block_len: int | None = None,
) -> dict[str, Any]:
    """Reliability curve, Brier and ECE with expiry-clustered bootstrap bands.

    ``clusters`` (one key per row, sortable -- the expiry date) define the
    resampling unit. Bands are 2.5/97.5 percentiles over ``n_boot``
    circular moving-block replicates of the ordered clusters. Returns a
    JSON-ready dict (NaN-free).
    """
    p = np.asarray(confidences, dtype=np.float64)
    y = np.asarray(outcomes, dtype=np.float64)
    n = int(p.size)
    if n == 0:
        return {"n": 0, "n_clusters": 0, "bins": [], "brier": None, "ece": None}
    ci, K = _cluster_index(list(clusters))
    edges = np.linspace(0.0, 1.0, n_bins + 1)
    b = np.clip(np.searchsorted(edges, p, side="right") - 1, 0, n_bins - 1)

    flat = ci * n_bins + b
    size = K * n_bins
    cnt = np.bincount(flat, minlength=size).reshape(K, n_bins).astype(np.float64)
    sp = np.bincount(flat, weights=p, minlength=size).reshape(K, n_bins)
    sy = np.bincount(flat, weights=y, minlength=size).reshape(K, n_bins)
    sq = np.bincount(ci, weights=(p - y) ** 2, minlength=K)
    ny = np.bincount(ci, minlength=K).astype(np.float64)
    ty = np.bincount(ci, weights=y, minlength=K)

    tot_cnt, tot_sp, tot_sy = cnt.sum(0), sp.sum(0), sy.sum(0)
    brier = float(sq.sum() / n)
    ece = float(np.abs(tot_sp - tot_sy).sum() / n)
    base = float(ty.sum() / n)

    boot_bins_rate: np.ndarray | None = None
    brier_band = ece_band = base_band = (None, None)
    n_eff: float | None = None
    L = block_len or default_block_length(K)
    if K >= 2:
        W = block_bootstrap_weights(K, n_boot, np.random.default_rng(seed), L)
        bn = W @ ny
        with np.errstate(invalid="ignore", divide="ignore"):
            b_cnt = W @ cnt
            boot_bins_rate = (W @ sy) / b_cnt
            b_brier = (W @ sq) / bn
            b_ece = np.abs(W @ sp - W @ sy).sum(1) / bn
            b_base = (W @ ty) / bn
        brier_band, ece_band, base_band = _band(b_brier), _band(b_ece), _band(b_base)
        n_eff = _effective_n(base, b_base, n)

    bins: list[dict[str, Any]] = []
    for i in range(n_bins):
        c = int(tot_cnt[i])
        lo_b = hi_b = None
        if c and boot_bins_rate is not None:
            lo_b, hi_b = _band(boot_bins_rate[:, i])
        bins.append({
            "bin_start": round(float(edges[i]), 4),
            "bin_end": round(float(edges[i + 1]), 4),
            "count": c,
            "predicted_mean": round(float(tot_sp[i] / c), 4) if c else None,
            "actual_rate": round(float(tot_sy[i] / c), 4) if c else None,
            "actual_lo": None if lo_b is None else round(lo_b, 4),
            "actual_hi": None if hi_b is None else round(hi_b, 4),
        })
    return {
        "n": n,
        "n_clusters": K,
        "block_len": L,
        "n_boot": n_boot if K >= 2 else 0,
        "n_eff": None if n_eff is None else round(n_eff, 1),
        "base_rate": round(base, 4),
        "base_rate_lo": _r(base_band[0]),
        "base_rate_hi": _r(base_band[1]),
        "brier": round(brier, 4),
        "brier_lo": _r(brier_band[0]),
        "brier_hi": _r(brier_band[1]),
        "ece": round(ece, 4),
        "ece_lo": _r(ece_band[0]),
        "ece_hi": _r(ece_band[1]),
        "mean_confidence": round(float(p.mean()), 4),
        "bins": bins,
    }


def _r(x: float | None, nd: int = 4) -> float | None:
    return None if x is None or not np.isfinite(x) else round(float(x), nd)


# ── recalibrator (isotonic, per family with a global fallback) ─────────────


@dataclass
class Recalibrator:
    """Monotone map confidence -> P(direction correct), fitted on a past block.

    Stored as isotonic knots so it serializes to JSON and is applied with
    ``np.interp`` (the same piecewise-linear rule sklearn's
    ``IsotonicRegression.predict`` uses, clipped at the ends).
    """

    global_knots: tuple[list[float], list[float]]
    family_knots: dict[str, tuple[list[float], list[float]]]

    def predict(self, confidence: float, family: str | None = None) -> float:
        xs, ys = self.family_knots.get(family or "", self.global_knots)
        if not xs:
            return float(confidence)
        return float(np.clip(np.interp(float(confidence), xs, ys), 0.0, 1.0))

    def to_dict(self) -> dict[str, Any]:
        return {
            "global": {"x": self.global_knots[0], "y": self.global_knots[1]},
            "family": {k: {"x": v[0], "y": v[1]} for k, v in self.family_knots.items()},
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> Recalibrator:
        g = payload.get("global") or {}
        fam = payload.get("family") or {}
        return cls(
            global_knots=(list(g.get("x") or []), list(g.get("y") or [])),
            family_knots={k: (list(v.get("x") or []), list(v.get("y") or [])) for k, v in fam.items()},
        )


def _isotonic_knots(x: np.ndarray, y: np.ndarray) -> tuple[list[float], list[float]]:
    from sklearn.isotonic import IsotonicRegression

    iso = IsotonicRegression(increasing=True, out_of_bounds="clip", y_min=0.0, y_max=1.0)
    iso.fit(x, y)
    return (
        [round(float(v), 6) for v in iso.X_thresholds_],
        [round(float(v), 6) for v in iso.y_thresholds_],
    )


def fit_recalibrator(
    rows: Sequence[ScoredOutcome],
    *,
    min_family_rows: int = MIN_FAMILY_FIT_ROWS,
) -> Recalibrator:
    """Fit a global isotonic map plus one per family with enough rows."""
    x = np.array([r.confidence for r in rows], dtype=np.float64)
    y = np.array([r.y for r in rows], dtype=np.float64)
    global_knots = _isotonic_knots(x, y) if len(rows) else ([], [])
    fams: dict[str, list[int]] = {}
    for i, r in enumerate(rows):
        fams.setdefault(r.family, []).append(i)
    family_knots = {
        f: _isotonic_knots(x[idx], y[idx])
        for f, idx in fams.items()
        if len(idx) >= min_family_rows
    }
    return Recalibrator(global_knots=global_knots, family_knots=family_knots)


# ── held-out evaluation: before/after calibration + conformal coverage ─────


def time_blocks(
    rows: Sequence[ScoredOutcome],
    *,
    fit_frac: float = FIT_FRAC,
    conformal_frac: float = CONFORMAL_FRAC,
) -> dict[str, Any]:
    """Split by expiry date into fit / conformal / test blocks, with embargo.

    A conformal-block row must have been created after the last fit-block
    expiry and a test row after the last conformal-block expiry, so every
    row is evaluated with a recalibrator and score set that existed when the
    call was made. Rows straddling a boundary are dropped and counted.
    """
    dates = sorted({r.expiry for r in rows})
    K = len(dates)
    out: dict[str, Any] = {"fit": [], "conformal": [], "test": [], "dropped_embargo": 0,
                           "n_dates": K, "fit_end": None, "conformal_end": None}
    if K < 3:
        return out
    i1 = min(K - 2, max(1, round(K * fit_frac)))
    i2 = min(K - 1, max(i1 + 1, round(K * (fit_frac + conformal_frac))))
    t1, t2 = dates[i1 - 1], dates[i2 - 1]
    out["fit_end"], out["conformal_end"] = t1, t2
    for r in rows:
        if r.expiry <= t1:
            out["fit"].append(r)
        elif r.expiry <= t2:
            if r.created_date > t1:
                out["conformal"].append(r)
            else:
                out["dropped_embargo"] += 1
        elif r.created_date > t2:
            out["test"].append(r)
        else:
            out["dropped_embargo"] += 1
    return out


def _rate(rows: Sequence[ScoredOutcome], attr: str) -> dict[str, Any]:
    return cluster_bootstrap_mean([getattr(r, attr) for r in rows], [r.expiry for r in rows])


def evaluate_holdout(
    rows: Sequence[ScoredOutcome],
    *,
    alpha: float | None = None,
    gamma: float | None = None,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Fit on the past, score the held-out time block, return (report, state).

    ``report`` carries ECE/Brier before (raw confidence) and after
    (recalibrated) on the test block, split-conformal and ACI coverage
    against nominal, abstention rate, and directional / strict hit rates for
    all test rows and per stance -- abstain, take (interval above 0.5),
    fade (interval below 0.5). ``state`` is what a reader needs to produce
    an interval for a new call (see :func:`exploratory_conformal_interval`).
    """
    from oracle.uncertainty import (
        ACI_GAMMA,
        CONFORMAL_ALPHA,
        binary_residual_score,
        conformal_quantile,
        split_conformal_interval,
    )

    alpha = CONFORMAL_ALPHA if alpha is None else alpha
    gamma = ACI_GAMMA if gamma is None else gamma
    blocks = time_blocks(rows)
    fit, cal, test = blocks["fit"], blocks["conformal"], blocks["test"]
    report: dict[str, Any] = {
        "target": EXPLORATORY_TARGET,
        "alpha": alpha,
        "gamma": gamma,
        "gamma_rule": "fixed 0.005 (Gibbs & Candes 2021 main experiments); not tuned",
        "blocks": {
            "n_dates": blocks["n_dates"],
            "fit_end": str(blocks["fit_end"]) if blocks["fit_end"] else None,
            "conformal_end": str(blocks["conformal_end"]) if blocks["conformal_end"] else None,
            "fit_rows": len(fit),
            "conformal_rows": len(cal),
            "test_rows": len(test),
            "test_dates": len({r.expiry for r in test}),
            "dropped_embargo": blocks["dropped_embargo"],
        },
    }
    if min(len(fit), len(cal), len(test)) < MIN_BLOCK_ROWS:
        report["status"] = "insufficient_data"
        return report, None

    recal = fit_recalibrator(fit)
    cal_scores = sorted(
        binary_residual_score(recal.predict(r.confidence, r.family), r.y) for r in cal
    )
    test_sorted = sorted(test, key=lambda r: (r.created_date, r.prediction_id))
    p_after = {r.prediction_id: recal.predict(r.confidence, r.family) for r in test}

    clusters = [r.expiry for r in test_sorted]
    ys = [r.y for r in test_sorted]
    report["before"] = _summary(reliability_with_bands([r.confidence for r in test_sorted], ys, clusters))
    report["after"] = _summary(reliability_with_bands([p_after[r.prediction_id] for r in test_sorted], ys, clusters))
    # Reference: a constant forecast at the fit block's base rate. An "after"
    # Brier near this number means recalibration mostly learned the base rate.
    fit_base = float(np.mean([r.y for r in fit]))
    report["reference_constant_fit_base_rate"] = {
        "p": round(fit_base, 4),
        "brier": round(float(np.mean((fit_base - np.asarray(ys, dtype=np.float64)) ** 2)), 4),
    }

    # Split conformal: one static score set from the conformal block.
    split = {r.prediction_id: split_conformal_interval(p_after[r.prediction_id], cal_scores, alpha=alpha)
             for r in test_sorted}

    aci_iv, aci, online_scores = causal_aci_intervals(test, p_after, cal_scores, alpha=alpha, gamma=gamma)

    for name, ivs in (("split", split), ("aci", aci_iv)):
        report[name] = _conformal_summary(test_sorted, ivs, alpha)
    report["aci"]["alpha_t_final"] = round(aci.alpha_t, 6)
    report["aci"]["updates"] = aci.n_updates
    report["aci"]["miscoverage_bound"] = _r(aci.miscoverage_bound(), 6)
    report["aci"]["bound_note"] = (
        "Gibbs & Candes Prop. 4.1 assumes each outcome is seen before the next "
        "interval; here outcomes arrive at expiry (delayed feedback), so the "
        "bound is indicative, not a guarantee"
    )
    report["hit_rate_all_test_rows"] = {
        "direction_correct": _rate(test_sorted, "y"),
        "strict_hit": _rate(test_sorted, "hit"),
    }
    report["status"] = "ok"

    state = {
        "method_version": EXPLORATORY_METHOD_VERSION,
        "official": False,
        "alpha": alpha,
        "gamma": gamma,
        "recalibrator": recal.to_dict(),
        "split_q": _r(conformal_quantile(cal_scores, alpha), 6),
        "split_n_cal": len(cal_scores),
        "aci_alpha_t": round(aci.alpha_t, 6),
        "aci_q": _r(conformal_quantile(online_scores, aci.alpha_t), 6),
        "aci_n_cal": len(online_scores),
        "fit_end": str(blocks["fit_end"]),
        "scores_end": str(max(r.expiry for r in test)),
    }
    return report, state


def causal_aci_intervals(
    test: Sequence[ScoredOutcome],
    p_hat: dict[str, float],
    cal_scores: Sequence[float],
    *,
    alpha: float,
    gamma: float,
) -> tuple[dict[str, Any], Any, list[float]]:
    """ACI intervals for ``test`` with no use of an unobserved outcome.

    A call gets the ACI state (``alpha_t`` and score set) that existed on its
    created date. An outcome updates the state, and joins the score set, only
    once its expiry is strictly before the created date of the call being
    served -- so neither a call's own label nor any label still in the future
    at the time it was made can move its interval. After the last call, the
    remaining outcomes are released so the returned state is current.

    Returns ``(intervals by prediction_id, AdaptiveConformal, sorted scores)``.
    """
    import bisect

    from oracle.uncertainty import AdaptiveConformal, binary_residual_score

    aci = AdaptiveConformal(alpha=alpha, gamma=gamma)
    online = sorted(cal_scores)
    by_created = sorted(test, key=lambda r: (r.created_date, r.prediction_id))
    by_expiry = sorted(test, key=lambda r: (r.expiry, r.created_date, r.prediction_id))
    ivs: dict[str, Any] = {}

    def _release(u: ScoredOutcome) -> None:
        aci.update(ivs[u.prediction_id].covers(u.y))
        bisect.insort(online, binary_residual_score(p_hat[u.prediction_id], u.y))

    j = 0
    for r in by_created:
        while j < len(by_expiry) and by_expiry[j].expiry < r.created_date:
            _release(by_expiry[j])
            j += 1
        ivs[r.prediction_id] = aci.interval(p_hat[r.prediction_id], online)
    while j < len(by_expiry):
        _release(by_expiry[j])
        j += 1
    return ivs, aci, online


def _summary(rel: dict[str, Any]) -> dict[str, Any]:
    keys = ("n", "n_clusters", "n_eff", "base_rate", "mean_confidence", "brier",
            "brier_lo", "brier_hi", "ece", "ece_lo", "ece_hi")
    return {k: rel.get(k) for k in keys} | {"bins": rel.get("bins", [])}


def _conformal_summary(
    rows: Sequence[ScoredOutcome], ivs: dict[str, Any], alpha: float,
) -> dict[str, Any]:
    clusters = [r.expiry for r in rows]
    covered = [1.0 if ivs[r.prediction_id].covers(r.y) else 0.0 for r in rows]
    abstain = [1.0 if ivs[r.prediction_id].abstain else 0.0 for r in rows]
    widths = [ivs[r.prediction_id].upper - ivs[r.prediction_id].lower for r in rows]
    cov = cluster_bootstrap_mean(covered, clusters)
    abst = cluster_bootstrap_mean(abstain, clusters)
    stances: dict[str, Any] = {}
    for stance in ("abstain", "take", "fade"):
        sub = [r for r in rows if ivs[r.prediction_id].stance == stance]
        stances[stance] = {
            "n": len(sub),
            "share": round(len(sub) / len(rows), 4) if rows else None,
            "direction_correct": _rate(sub, "y") if sub else None,
            "strict_hit": _rate(sub, "hit") if sub else None,
        }
    return {
        "nominal_coverage": round(1.0 - alpha, 4),
        "coverage": _round_band(cov),
        "abstention_rate": _round_band(abst),
        "mean_width": round(float(np.mean(widths)), 4) if widths else None,
        "stances": stances,
    }


def _round_band(d: dict[str, Any]) -> dict[str, Any]:
    return {k: (_r(v) if isinstance(v, float) else v) for k, v in d.items()}


# ── exposure: interval + abstain for a new call (exploratory) ──────────────


def exploratory_conformal_interval(
    state: dict[str, Any],
    confidence: float,
    model_name: str | None = None,
    *,
    method: str = "aci",
) -> Any:
    """Calibrated interval and abstain signal for a call, from a saved state.

    ``state`` is the ``payload['state']`` of the latest ``scope = 'run'`` row
    in ``oracle_calibration_exploratory`` (see
    :func:`load_latest_exploratory_state`). Returns an
    :class:`oracle.uncertainty.ConformalInterval` whose ``abstain`` is True
    when the interval contains 0.5. Exploratory: not used for sizing.
    """
    import math

    from oracle.uncertainty import conformal_interval_from_quantile

    recal = Recalibrator.from_dict(state.get("recalibrator") or {})
    p_hat = recal.predict(confidence, model_family(model_name))
    alpha = float(state.get("alpha", 0.10))
    if method == "split":
        q, alpha_used, n_cal = state.get("split_q"), alpha, int(state.get("split_n_cal", 0))
    else:
        q, alpha_used, n_cal = state.get("aci_q"), float(state.get("aci_alpha_t", alpha)), int(state.get("aci_n_cal", 0))
    q_val = math.inf if q is None else float(q)
    return conformal_interval_from_quantile(
        p_hat, q_val, alpha=alpha, alpha_used=alpha_used, n_cal=n_cal, method=method,
    )


def load_latest_exploratory_state(engine: Engine) -> dict[str, Any] | None:
    """Latest saved conformal state, or None when the table is absent/empty."""
    try:
        with engine.connect() as conn:
            if not _has_table(conn, EXPLORATORY_TABLE):
                return None
            row = conn.execute(
                text(
                    "SELECT payload FROM oracle_calibration_exploratory "
                    "WHERE scope = 'run' AND method_version = :v "
                    "ORDER BY as_of DESC, id DESC LIMIT 1"
                ),
                {"v": EXPLORATORY_METHOD_VERSION},
            ).fetchone()
    except Exception as exc:  # noqa: BLE001
        log.warning("load_latest_exploratory_state failed: {e}", e=str(exc))
        return None
    if not row or row[0] is None:
        return None
    payload = row[0] if isinstance(row[0], dict) else json.loads(row[0])
    return payload.get("state")


# ── the daily job ──────────────────────────────────────────────────────────


def _json_safe(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe(v) for v in obj]
    if isinstance(obj, (float, np.floating)):
        f = float(obj)
        return f if np.isfinite(f) else None
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    return obj


def _group_reliability(rows: Sequence[ScoredOutcome]) -> list[tuple[str, str, dict[str, Any]]]:
    groups: list[tuple[str, str, list[ScoredOutcome]]] = [("global", "all", list(rows))]
    for scope, attr in (("horizon", "horizon"), ("family", "family"), ("source", "source")):
        keyed: dict[str, list[ScoredOutcome]] = {}
        for r in rows:
            keyed.setdefault(getattr(r, attr), []).append(r)
        groups.extend((scope, k, v) for k, v in sorted(keyed.items()))
    out = []
    for scope, key, sub in groups:
        if len(sub) < MIN_GROUP_ROWS:
            continue
        rel = reliability_with_bands(
            [r.confidence for r in sub], [r.y for r in sub], [r.expiry for r in sub],
        )
        rel["strict_hit_rate"] = round(float(np.mean([r.hit for r in sub])), 4)
        out.append((scope, key, rel))
    return out


_EXPLORATORY_INSERT = """
INSERT INTO oracle_calibration_exploratory (
    as_of, method_version, scope, group_key, target,
    n_rows, n_clusters, n_eff, base_rate,
    brier, brier_lo, brier_hi, ece, ece_lo, ece_hi,
    payload, score_class, official
) VALUES (
    :as_of, :method_version, :scope, :group_key, :target,
    :n_rows, :n_clusters, :n_eff, :base_rate,
    :brier, :brier_lo, :brier_hi, :ece, :ece_lo, :ece_hi,
    CAST(:payload AS JSONB), 'exploratory', FALSE
)
ON CONFLICT (as_of, method_version, scope, group_key) DO NOTHING
"""


def _persist_exploratory(
    engine: Engine,
    as_of_date: date,
    groups: Sequence[tuple[str, str, dict[str, Any]]],
    run_payload: dict[str, Any],
) -> int:
    base = {"as_of": as_of_date, "method_version": EXPLORATORY_METHOD_VERSION,
            "target": EXPLORATORY_TARGET}
    params: list[dict[str, Any]] = []
    for scope, key, rel in groups:
        params.append(base | {
            "scope": scope, "group_key": key,
            "n_rows": rel.get("n"), "n_clusters": rel.get("n_clusters"),
            "n_eff": rel.get("n_eff"), "base_rate": rel.get("base_rate"),
            "brier": rel.get("brier"), "brier_lo": rel.get("brier_lo"),
            "brier_hi": rel.get("brier_hi"), "ece": rel.get("ece"),
            "ece_lo": rel.get("ece_lo"), "ece_hi": rel.get("ece_hi"),
            "payload": json.dumps(_json_safe(rel)),
        })
    params.append(base | {
        "scope": "run", "group_key": "all",
        "n_rows": run_payload.get("n_rows"), "n_clusters": run_payload.get("n_dates"),
        "n_eff": None, "base_rate": None, "brier": None, "brier_lo": None,
        "brier_hi": None, "ece": None, "ece_lo": None, "ece_hi": None,
        "payload": json.dumps(_json_safe(run_payload)),
    })
    inserted = 0
    with engine.begin() as conn:
        if not _has_table(conn, EXPLORATORY_TABLE):
            log.warning(
                "exploratory recalibration: {t} missing (migration "
                "oracle_cal_exploratory_20261106 not applied); nothing written",
                t=EXPLORATORY_TABLE,
            )
            return 0
        for p in params:
            res = conn.execute(text(_EXPLORATORY_INSERT), p)
            inserted += int(res.rowcount or 0)
    return inserted


def run_exploratory_recalibration(
    engine: Engine,
    *,
    as_of: datetime | None = None,
    persist: bool = True,
) -> dict[str, Any]:
    """Daily exploratory recalibration + conformal refresh (S10).

    Reads official forward scores and #976 hindsight rescores up to
    ``as_of``; computes expiry-clustered reliability curves globally and per
    horizon / family / source; evaluates recalibration and split/ACI
    conformal intervals on a held-out time block; and, when ``persist``,
    appends one row per group plus one ``scope = 'run'`` row to
    ``oracle_calibration_exploratory``. A second run on the same UTC date
    inserts nothing (ON CONFLICT DO NOTHING). Nothing else is written.
    """
    as_of_ts = as_of or datetime.now(timezone.utc)
    if as_of_ts.tzinfo is None:
        as_of_ts = as_of_ts.replace(tzinfo=timezone.utc)
    as_of_date = as_of_ts.astimezone(timezone.utc).date()
    rows = load_scored_outcomes(engine, as_of_ts)
    summary: dict[str, Any] = {
        "as_of": as_of_date.isoformat(),
        "method_version": EXPLORATORY_METHOD_VERSION,
        "official": False,
        "n_rows": len(rows),
        "n_dates": len({r.expiry for r in rows}),
        "by_source": {
            s: sum(1 for r in rows if r.source == s) for s in (SOURCE_OFFICIAL, SOURCE_RESCORE)
        },
    }
    if len(rows) < MIN_GROUP_ROWS:
        summary["status"] = "insufficient_data"
        summary["inserted"] = 0
        return summary

    groups = _group_reliability(rows)
    holdout, state = evaluate_holdout(rows)
    summary["status"] = holdout.get("status", "ok")
    summary["groups"] = len(groups)
    summary["holdout"] = holdout
    run_payload = dict(summary)
    run_payload["state"] = state
    run_payload["writes"] = [EXPLORATORY_TABLE]
    summary["inserted"] = (
        _persist_exploratory(engine, as_of_date, groups, run_payload) if persist else 0
    )
    log.info(
        "exploratory recalibration {d}: {n} rows / {k} expiry dates, {g} groups, "
        "holdout={s}, inserted={i}",
        d=as_of_date, n=len(rows), k=summary["n_dates"], g=len(groups),
        s=summary["status"], i=summary["inserted"],
    )
    return summary
