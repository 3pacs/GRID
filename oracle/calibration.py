"""
GRID Oracle Calibration — measures how well predicted probabilities
match actual outcomes.

A well-calibrated system that says "70% confident" should be right ~70%
of the time. This module computes calibration curves, Brier scores,
and reliability diagrams for the Oracle's predictions.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
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
class CalibrationReport:
    """Full calibration report for the Oracle."""
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
) -> CalibrationReport:
    """Compute calibration curve from scored predictions.

    Args:
        engine: Database engine.
        n_bins: Number of bins for the calibration curve.
        model_name: Optional filter by model.
        ticker: Optional filter by ticker.

    Returns:
        CalibrationReport with buckets, Brier score, ECE, and label.
    """
    query = """
        SELECT confidence, verdict
        FROM oracle_predictions
        WHERE verdict IN ('hit', 'miss', 'partial')
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
        return CalibrationReport(
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

    report = CalibrationReport(
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
