"""Per-model confidence reliability curves.

Background
==========

The audit chain in PRs #187 → #213 surfaced a saturated cluster of 612
trades pinned at exactly ``confidence = 0.950`` (and another 65 at
0.945). These come from six oracle paths that clamp raw confidence
with ``min(0.95, ...)``:

  * ``oracle/engine.py``                — raw / 5.0
  * ``intelligence/news_impact.py``     — total_explained / move_bps
  * ``oracle/contrast_distillation.py`` — 0.3 + 0.65 * divergence
  * ``oracle/psi_model.py``             — (sharpe - 1.0) / 3.0 + 0.3
  * ``oracle/forecaster_adapter.py``    — 1.0 - interval_pct / 10.0
  * ``store/astrogrid.py``              — opaque mid-pipeline ceiling

None of these caps use per-model reliability. Models that confidently
overstate edge (e.g. ``options_flow_mut_g34098`` with raw mean 0.62
and realized 9% hit-rate) all pin at 0.95 and dominate the saturated
cluster.

This module replaces the dumb ceiling with an empirical calibration:

  * ``confidence_reliability_curves`` table stores per-(model_name,
    raw_bucket) realized hit rate from historical ``oracle_predictions``.
  * ``calibrate_confidence(raw, model_name)`` looks up the bucket
    containing the raw value and returns the realized hit rate.
  * When no data is available (cold-start model), returns the raw
    value unchanged so behaviour matches the pre-calibration path.

The calibration is intentionally piecewise-constant per bucket rather
than a smoothed curve. Piecewise-constant is robust to thin data and
plays nicely with the small sample sizes most models have today.

Schema
======

``confidence_reliability_curves``::

    model_name              TEXT NOT NULL,
    bucket_lower            DOUBLE PRECISION NOT NULL,
    bucket_upper            DOUBLE PRECISION NOT NULL,
    n_predictions           INTEGER NOT NULL,
    n_hits                  INTEGER NOT NULL,
    realized_hit_rate       DOUBLE PRECISION NOT NULL,
    last_updated            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (model_name, bucket_lower)

Bucket boundaries are fixed: ``[0.0, 0.10, 0.20, 0.30, 0.40, 0.50,
0.60, 0.70, 0.80, 0.90, 1.0]`` — 10 buckets of width 0.10. Fine
enough to capture the cap saturation around 0.9; coarse enough to
keep sample counts meaningful per bucket.

Cold-start policy
=================

If a model has fewer than ``MIN_PREDICTIONS_PER_BUCKET`` predictions
in the bucket containing a raw confidence, the calibration returns
the raw value unchanged. Below ``MIN_PREDICTIONS_TOTAL`` across all
buckets, the entire model is cold-start and ``calibrate_confidence``
is identity.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# Bucket boundaries (lower-inclusive, upper-exclusive except the last).
BUCKET_BOUNDARIES: tuple[float, ...] = (
    0.0, 0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90, 1.0001,
)

MIN_PREDICTIONS_PER_BUCKET: int = 5
MIN_PREDICTIONS_TOTAL: int = 30


_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS confidence_reliability_curves (
    model_name        TEXT             NOT NULL,
    bucket_lower      DOUBLE PRECISION NOT NULL,
    bucket_upper      DOUBLE PRECISION NOT NULL,
    n_predictions     INTEGER          NOT NULL,
    n_hits            INTEGER          NOT NULL,
    realized_hit_rate DOUBLE PRECISION NOT NULL,
    last_updated      TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    PRIMARY KEY (model_name, bucket_lower)
);

CREATE INDEX IF NOT EXISTS idx_crc_model
    ON confidence_reliability_curves (model_name);
"""


def ensure_tables(engine: Engine) -> None:
    """Create ``confidence_reliability_curves`` if missing. Idempotent."""
    try:
        with engine.begin() as conn:
            for stmt in _TABLE_SQL.strip().split(";"):
                s = stmt.strip()
                if s:
                    conn.execute(text(s))
    except Exception as exc:  # noqa: BLE001
        log.warning("confidence_calibration.ensure_tables failed: {e}", e=str(exc))


def _bucket_for(raw: float) -> tuple[float, float]:
    """Return the (lower, upper) bucket boundaries containing ``raw``.

    Bucket lower is inclusive, upper is exclusive (except the last,
    which is closed). ``raw`` is clamped to [0.0, 1.0] before bucketing
    so out-of-range inputs land safely in the edge bucket.
    """
    r = max(0.0, min(1.0, float(raw)))
    for i in range(len(BUCKET_BOUNDARIES) - 1):
        lower = BUCKET_BOUNDARIES[i]
        upper = BUCKET_BOUNDARIES[i + 1]
        if lower <= r < upper:
            return (lower, upper)
    # r exactly at 1.0
    return (BUCKET_BOUNDARIES[-2], BUCKET_BOUNDARIES[-1])


@dataclass(frozen=True)
class CalibrationCurve:
    """One model's reliability curve, organised for fast lookup."""

    model_name: str
    total_predictions: int
    # bucket_lower → (n_predictions, n_hits, realized_hit_rate)
    buckets: dict[float, tuple[int, int, float]]


def _load_curve(engine: Engine, model_name: str) -> CalibrationCurve | None:
    """Read all buckets for ``model_name`` from
    ``confidence_reliability_curves``. Returns None if the model has
    no rows in the table (cold-start case).
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT bucket_lower, n_predictions, n_hits, realized_hit_rate
                      FROM confidence_reliability_curves
                     WHERE model_name = :m
                    """
                ),
                {"m": model_name},
            ).fetchall()
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "confidence_calibration._load_curve failed for {m}: {e}",
            m=model_name, e=str(exc),
        )
        return None

    if not rows:
        return None

    buckets: dict[float, tuple[int, int, float]] = {}
    total = 0
    for r in rows:
        lower = float(r[0])
        n = int(r[1] or 0)
        hits = int(r[2] or 0)
        rate = float(r[3] or 0.0)
        buckets[lower] = (n, hits, rate)
        total += n

    return CalibrationCurve(
        model_name=model_name,
        total_predictions=total,
        buckets=buckets,
    )


# In-process cache so the hot path (per-prediction confidence
# calibration) does not hit the DB on every call. Invalidated by the
# nightly build_confidence_reliability rebuild via ``invalidate_cache``.
_CURVE_CACHE: dict[str, CalibrationCurve | None] = {}


def invalidate_cache() -> None:
    """Drop the in-process curve cache. Call from build script after
    rebuilding the table."""
    _CURVE_CACHE.clear()


def calibrate_confidence(
    raw_confidence: float,
    model_name: str | None,
    engine: Engine,
) -> float:
    """Translate a model's raw confidence into the empirical hit rate
    for the bucket containing ``raw_confidence``.

    Returns the raw value unchanged when:
      * ``model_name`` is None or empty
      * The model has no rows in ``confidence_reliability_curves``
      * The model's total prediction count is below
        ``MIN_PREDICTIONS_TOTAL`` (cold-start model)
      * The specific bucket containing ``raw_confidence`` has fewer
        than ``MIN_PREDICTIONS_PER_BUCKET`` predictions (thin slice)

    The result is always clamped to [0.0, 1.0]. Safe to call on the
    hot per-prediction path; failures are logged at debug level and
    fall back to the raw value.
    """
    if not model_name:
        return float(raw_confidence)

    if model_name not in _CURVE_CACHE:
        _CURVE_CACHE[model_name] = _load_curve(engine, model_name)

    curve = _CURVE_CACHE[model_name]
    if curve is None or curve.total_predictions < MIN_PREDICTIONS_TOTAL:
        return float(raw_confidence)

    bucket_lower, _ = _bucket_for(raw_confidence)
    entry = curve.buckets.get(bucket_lower)
    if entry is None or entry[0] < MIN_PREDICTIONS_PER_BUCKET:
        return float(raw_confidence)

    return max(0.0, min(1.0, entry[2]))


def calibrate_confidence_default(
    raw_confidence: float,
    model_name: str | None,
) -> float:
    """Engine-less wrapper for hot call sites that don't already have an
    Engine in scope.

    Pulls the global engine lazily via ``db.get_engine`` and delegates.
    Falls back to the raw value on any failure (engine unavailable,
    table missing, import error). Designed as a drop-in replacement for
    a bare ``min(0.95, raw)`` cap.
    """
    if not model_name:
        return float(raw_confidence)
    try:
        from db import get_engine  # local import to avoid circular deps at module load
        return calibrate_confidence(float(raw_confidence), model_name, get_engine())
    except Exception:
        return float(raw_confidence)


def build_reliability_curves(
    engine: Engine,
    days: int = 365,
    min_total: int = MIN_PREDICTIONS_TOTAL,
) -> dict[str, Any]:
    """Walk ``oracle_predictions`` and rebuild ``confidence_reliability_curves``
    in one pass. Truncates the existing table on each run so it
    reflects only the lookback window. Idempotent.

    Returns a summary dict with per-model bucket counts.
    """
    ensure_tables(engine)
    summary: dict[str, Any] = {
        "models_processed": 0,
        "models_skipped_low_n": 0,
        "rows_written": 0,
    }

    bucket_pairs = [
        (BUCKET_BOUNDARIES[i], BUCKET_BOUNDARIES[i + 1])
        for i in range(len(BUCKET_BOUNDARIES) - 1)
    ]

    try:
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE confidence_reliability_curves"))
            model_rows = conn.execute(
                text(
                    """
                    SELECT model_name,
                           confidence,
                           CASE WHEN verdict = 'hit' THEN 1 ELSE 0 END AS hit
                      FROM oracle_predictions
                     WHERE created_at >= NOW() - (:days || ' days')::interval
                       AND verdict IN ('hit', 'miss', 'partial')
                       AND dedup_keep = TRUE
                       AND model_name IS NOT NULL
                       AND confidence IS NOT NULL
                    """
                ),
                {"days": str(int(days))},
            ).fetchall()

            # Group by model
            by_model: dict[str, list[tuple[float, int]]] = {}
            for row in model_rows:
                by_model.setdefault(row[0], []).append((float(row[1]), int(row[2])))

            for model_name, preds in by_model.items():
                if len(preds) < min_total:
                    summary["models_skipped_low_n"] += 1
                    continue

                # Bucket the predictions
                for lower, upper in bucket_pairs:
                    in_bucket = [
                        p for p in preds
                        if lower <= p[0] < upper
                        or (upper >= 1.0001 and p[0] == 1.0)
                    ]
                    n = len(in_bucket)
                    if n == 0:
                        continue
                    n_hits = sum(p[1] for p in in_bucket)
                    rate = n_hits / n if n > 0 else 0.0

                    conn.execute(
                        text(
                            """
                            INSERT INTO confidence_reliability_curves
                                (model_name, bucket_lower, bucket_upper,
                                 n_predictions, n_hits, realized_hit_rate,
                                 last_updated)
                            VALUES (:m, :lo, :hi, :n, :h, :r, NOW())
                            """
                        ),
                        {
                            "m": model_name,
                            "lo": float(lower),
                            "hi": float(upper),
                            "n": n,
                            "h": n_hits,
                            "r": float(rate),
                        },
                    )
                    summary["rows_written"] += 1

                summary["models_processed"] += 1

    except Exception as exc:  # noqa: BLE001
        log.warning(
            "confidence_calibration.build_reliability_curves failed: {e}",
            e=str(exc),
        )

    invalidate_cache()
    log.info(
        "confidence_calibration: rebuilt {n} models ({rows} bucket rows), "
        "skipped {s} low-n models",
        n=summary["models_processed"],
        rows=summary["rows_written"],
        s=summary["models_skipped_low_n"],
    )
    return summary


def main(argv: list[str] | None = None) -> int:
    """CLI entry: rebuild ``confidence_reliability_curves`` from
    oracle_predictions over the last --days lookback window.
    """
    import argparse
    import sys

    p = argparse.ArgumentParser(prog="confidence_calibration")
    p.add_argument(
        "--days", type=int, default=365,
        help="Lookback window in days (default 365)",
    )
    p.add_argument(
        "--min-total", type=int, default=MIN_PREDICTIONS_TOTAL,
        help=f"Minimum total predictions per model (default {MIN_PREDICTIONS_TOTAL})",
    )
    args = p.parse_args(argv)

    from db import get_engine
    engine = get_engine()
    summary = build_reliability_curves(
        engine,
        days=int(args.days),
        min_total=int(args.min_total),
    )
    print(summary)
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
