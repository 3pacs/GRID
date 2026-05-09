"""
Per-horizon, per-confidence-bucket calibration tracker (CAT-180).

Answers the question ``features/per_signal_brier`` does not: "when the
oracle says 0.75, does it actually hit 75% of the time?" A sharper
diagnostic than the mean-square Brier score because it detects
over/under-confidence *within specific probability bins* — a model can
have a low overall Brier while still being systematically miscalibrated
in one bucket.

Relationship to existing modules
--------------------------------

* ``features/per_signal_brier`` tracks per-signal running Brier. This
  module tracks per-**bucket** running hit-rate and Brier. We reuse its
  canonical horizon snapping (``_canonical_horizon``), its calibrated
  sample threshold (``MIN_CALIBRATED_SAMPLES``), and its horizon key set
  (``CANONICAL_HORIZONS``) rather than redefining them.
* ``oracle/calibration.py`` already computes a one-shot reliability
  curve on demand. It does not persist per-bucket history — that's what
  this module adds. We intentionally do not wrap its
  ``compute_calibration`` function because the reliability curve it
  produces is full-scan; ours is an incremental running counter that
  survives across process restarts.
* ``scripts/bootstrap_per_signal_brier`` contains the canonical
  verdict → outcome mapping (``verdict_to_outcome``) and the row-loader
  that walks ``oracle_predictions`` in chronological order. We import
  both rather than reimplementing.

Conviction multiplier
---------------------

The public lookup ``conviction_multiplier_for_bucket`` is the consumer
of this data. For any new oracle prediction, callers pass its
confidence and horizon in and receive back a multiplicative adjuster:

* gap > 0.20 (severe over-confidence)        → 0.60
* gap > 0.10                                 → 0.80
* gap > 0.05                                 → 0.92
* |gap| <= 0.05 (well-calibrated)             → 1.00
* gap < -0.05 (slight under-confidence)       → 1.05
* gap < -0.10                                 → 1.08

where ``gap = confidence - empirical_hit_rate``. A bucket with no
history returns the neutral 1.0 multiplier. Any DB access failure also
returns 1.0 — never raise through to the caller.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# Reuse canonical constants & helpers — never reimplement them.
from features.per_signal_brier import (
    MIN_CALIBRATED_SAMPLES,
    _canonical_horizon,
)

# Reuse verdict mapping from the bootstrap script — same math keeps the
# two bootstrap paths in lock-step.
from scripts.bootstrap_per_signal_brier import verdict_to_outcome


# ── Bucket definitions ────────────────────────────────────────────────────

# Ten fixed buckets covering [0.50, 1.01). The upper edge of the final
# bucket is 1.01 so a perfect confidence of 1.0 falls inside it. Below
# 0.50 is explicitly *not* bucketed — the oracle never issues a
# "confident bearish" as <0.50 today; directional flips are handled
# upstream before we ever record to this tracker.
CONFIDENCE_BUCKETS: tuple[tuple[float, float], ...] = (
    (0.50, 0.55),
    (0.55, 0.60),
    (0.60, 0.65),
    (0.65, 0.70),
    (0.70, 0.75),
    (0.75, 0.80),
    (0.80, 0.85),
    (0.85, 0.90),
    (0.90, 0.95),
    (0.95, 1.01),  # inclusive of perfect 1.0 via the 1.01 ceiling
)


# ── Conviction multiplier thresholds ──────────────────────────────────────

# gap = confidence - empirical_hit_rate
#
# Severe over-confidence: the oracle's quoted probability is more than
# 20 points higher than the bucket's historical hit rate. A quoted 0.80
# that only lands 0.55 of the time must be heavily discounted — 0.60 is
# enough to drop a high-conviction signal out of the top tier without
# silencing it completely.
GAP_THRESHOLD_SEVERE: float = 0.20
MULT_SEVERE_OVERCONF: float = 0.60

# Significant over-confidence: 10-20 point gap. 0.80 discount leaves
# the signal usable for sizing but cuts its Kelly allocation in half.
GAP_THRESHOLD_HIGH: float = 0.10
MULT_HIGH_OVERCONF: float = 0.80

# Mild over-confidence: 5-10 point gap. 0.92 is a soft haircut —
# enough to tilt aggregation away from the bucket, not enough to veto.
GAP_THRESHOLD_MILD: float = 0.05
MULT_MILD_OVERCONF: float = 0.92

# Well-calibrated zone: |gap| <= 0.05. Neutral multiplier.
MULT_NEUTRAL: float = 1.00

# Slight under-confidence: gap < -0.05 (bucket hits more than claimed).
# Mild boost because the oracle is being too humble.
MULT_MILD_UNDERCONF: float = 1.05

# Strong under-confidence: gap < -0.10. Larger boost — the bucket has
# historically outperformed its quoted confidence. Capped at 1.08 to
# avoid snowballing (we trust the bucket, but not enough to re-lever
# it by >10% on a single gap observation).
GAP_THRESHOLD_UNDERCONF_STRONG: float = 0.10
MULT_STRONG_UNDERCONF: float = 1.08


# ── Schema bootstrap ──────────────────────────────────────────────────────

_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS confidence_bucket_history (
    id SERIAL PRIMARY KEY,
    horizon_days INT NOT NULL,
    bucket_low NUMERIC(4,2) NOT NULL,
    bucket_high NUMERIC(4,2) NOT NULL,
    n_predictions INT NOT NULL DEFAULT 0,
    n_hits NUMERIC(10, 2) NOT NULL DEFAULT 0,
    running_brier NUMERIC(8,6),
    last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(horizon_days, bucket_low, bucket_high)
);

CREATE INDEX IF NOT EXISTS idx_confidence_bucket_last_updated
    ON confidence_bucket_history(last_updated);
"""


# Engines we've already DDL'd this process. Keyed by ``id(engine)`` so
# tests can instantiate new fake engines freely; production pool reuse
# means this is effectively a one-shot per process.
_initialized_engines: set[int] = set()


def _ensure_schema(engine: Engine) -> None:
    """Create ``confidence_bucket_history`` if absent. Idempotent per
    engine — the DDL only runs the first time a given engine object is
    seen, matching the pattern from ``features/per_signal_brier``.

    Never raises — a missing permission or a locked table falls back to
    a warning log. The read/write callers are all try/except wrapped
    too, so a failed DDL just means subsequent ops return ``None`` or
    the neutral 1.0 multiplier.
    """
    key = id(engine)
    if key in _initialized_engines:
        return
    try:
        with engine.begin() as conn:
            for stmt in _TABLE_SQL.strip().split(";"):
                s = stmt.strip()
                if s:
                    conn.execute(text(s))
        _initialized_engines.add(key)
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "confidence_bucket_tracker: ensure_schema failed: {e}",
            e=str(exc),
        )


def _reset_initialized_engines() -> None:
    """Test-only hook — lets tests that instantiate multiple FakeEngines
    within one test module force DDL re-run without relying on Python's
    id-reuse semantics for garbage-collected objects.
    """
    _initialized_engines.clear()


# ── Pure helpers ──────────────────────────────────────────────────────────


def _bucket_for(confidence: float) -> tuple[float, float] | None:
    """Return the ``(low, high)`` bucket containing ``confidence``.

    Returns ``None`` when the input is below 0.50 (we do not bucket
    sub-50% confidences — see CONFIDENCE_BUCKETS rationale). Matching
    is ``[low, high)`` — a value exactly on ``low`` falls *into* the
    bucket; a value exactly on ``high`` falls into the next bucket.
    The final bucket's ``high`` is 1.01 so 1.0 maps cleanly inside.
    """
    try:
        c = float(confidence)
    except (TypeError, ValueError):
        return None
    if c != c:  # NaN
        return None
    if c < 0.50:
        return None
    for low, high in CONFIDENCE_BUCKETS:
        if low <= c < high:
            return (low, high)
    return None


def _compute_gap(confidence: float, empirical_hit_rate: float) -> float:
    """gap = confidence - empirical_hit_rate. Positive gap means the
    oracle is claiming more than the bucket historically delivers
    (over-confidence). Negative gap means under-confidence."""
    return float(confidence) - float(empirical_hit_rate)


def _multiplier_from_gap(gap: float) -> float:
    """Map a calibration gap onto the conviction multiplier constants.

    Ordered from severe over-confidence down to strong under-confidence
    so the first matching band wins. |gap| <= 0.05 is the neutral
    zone and returns 1.0 exactly.
    """
    if gap > GAP_THRESHOLD_SEVERE:
        return MULT_SEVERE_OVERCONF
    if gap > GAP_THRESHOLD_HIGH:
        return MULT_HIGH_OVERCONF
    if gap > GAP_THRESHOLD_MILD:
        return MULT_MILD_OVERCONF
    if gap < -GAP_THRESHOLD_UNDERCONF_STRONG:
        return MULT_STRONG_UNDERCONF
    if gap < -GAP_THRESHOLD_MILD:
        return MULT_MILD_UNDERCONF
    return MULT_NEUTRAL


# ── Data class ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class BucketCalibration:
    """Immutable snapshot of one (horizon, bucket) calibration row.

    ``n_hits`` is a float because partial verdicts contribute 0.5.
    ``empirical_hit_rate`` is derived from ``n_hits / max(n, 1)``.
    ``is_calibrated`` mirrors the ``MIN_CALIBRATED_SAMPLES`` gate used
    across the broader conviction stack — below the threshold, downstream
    consumers should treat the bucket as cold-start and fall back to
    the neutral multiplier regardless of the gap value.
    """

    horizon_days: int
    bucket_low: float
    bucket_high: float
    n_predictions: int
    n_hits: float
    empirical_hit_rate: float
    running_brier: float
    is_calibrated: bool
    last_updated: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "horizon_days": int(self.horizon_days),
            "bucket_low": round(float(self.bucket_low), 2),
            "bucket_high": round(float(self.bucket_high), 2),
            "n_predictions": int(self.n_predictions),
            "n_hits": round(float(self.n_hits), 2),
            "empirical_hit_rate": round(float(self.empirical_hit_rate), 4),
            "running_brier": round(float(self.running_brier), 6),
            "is_calibrated": bool(self.is_calibrated),
            "last_updated": self.last_updated,
        }


def _row_to_bucket(row: Any) -> BucketCalibration:
    """Translate a raw DB row into a ``BucketCalibration`` record.

    Row shape (matching every SELECT in this module):
        (horizon_days, bucket_low, bucket_high, n_predictions,
         n_hits, running_brier, last_updated)
    """
    horizon = int(row[0])
    low = float(row[1])
    high = float(row[2])
    n = int(row[3] or 0)
    hits = float(row[4] or 0.0)
    brier = float(row[5] or 0.0)
    last_updated = row[6]
    hit_rate = (hits / n) if n > 0 else 0.0
    is_calibrated = n >= MIN_CALIBRATED_SAMPLES
    ts_str = (
        last_updated.isoformat()
        if hasattr(last_updated, "isoformat")
        else str(last_updated) if last_updated is not None else ""
    )
    return BucketCalibration(
        horizon_days=horizon,
        bucket_low=low,
        bucket_high=high,
        n_predictions=n,
        n_hits=hits,
        empirical_hit_rate=hit_rate,
        running_brier=brier,
        is_calibrated=is_calibrated,
        last_updated=ts_str,
    )


# ── Write path ────────────────────────────────────────────────────────────


def record_scored_prediction(
    engine: Engine,
    *,
    confidence: float,
    outcome: float,
    horizon_days: int,
) -> None:
    """Record one scored prediction into the bucket history.

    * ``confidence`` is clamped to [0, 1]; values below 0.50 are silently
      ignored (their bucket is ``None`` by design).
    * ``outcome`` is 0.0 (miss) / 0.5 (partial) / 1.0 (hit). Values
      outside this set are clamped into [0, 1] for the Brier calc but
      still recorded — the tracker is tolerant of edge verdicts.
    * ``horizon_days`` is snapped to the canonical {1, 7, 30, 90} set
      via ``_canonical_horizon``.

    Never raises. Uses a single transaction: SELECT-then-INSERT/UPDATE
    with arithmetic-mean (Welford) update, mirroring
    ``features/per_signal_brier.record_scored_prediction`` exactly:

        new_brier = old_brier + (sq_err - old_brier) / new_count
    """
    try:
        conf = float(confidence)
        out = float(outcome)
    except (TypeError, ValueError):
        return
    if conf != conf or out != out:  # NaN guard
        return
    conf = max(0.0, min(1.0, conf))
    out = max(0.0, min(1.0, out))

    bucket = _bucket_for(conf)
    if bucket is None:
        # Sub-0.50 confidence — no bucket to record into.
        return

    horizon = _canonical_horizon(horizon_days)
    sq_err = (conf - out) ** 2
    low, high = bucket

    _ensure_schema(engine)

    try:
        with engine.begin() as conn:
            row = conn.execute(
                text(
                    "SELECT n_predictions, n_hits, running_brier "
                    "FROM confidence_bucket_history "
                    "WHERE horizon_days = :h "
                    "  AND bucket_low = :bl "
                    "  AND bucket_high = :bh"
                ),
                {"h": horizon, "bl": low, "bh": high},
            ).fetchone()

            if row is None:
                conn.execute(
                    text(
                        """
                        INSERT INTO confidence_bucket_history
                            (horizon_days, bucket_low, bucket_high,
                             n_predictions, n_hits, running_brier,
                             last_updated)
                        VALUES (:h, :bl, :bh, 1, :hits, :brier, NOW())
                        ON CONFLICT (horizon_days, bucket_low, bucket_high)
                        DO NOTHING
                        """
                    ),
                    {
                        "h": horizon,
                        "bl": low,
                        "bh": high,
                        "hits": out,
                        "brier": sq_err,
                    },
                )
                return

            old_n = int(row[0] or 0)
            old_hits = float(row[1] or 0.0)
            old_brier = float(row[2] or 0.0)
            new_n = old_n + 1
            new_hits = old_hits + out
            # Welford incremental mean — same one-liner as
            # features/per_signal_brier.
            new_brier = old_brier + (sq_err - old_brier) / new_n

            conn.execute(
                text(
                    """
                    UPDATE confidence_bucket_history
                    SET n_predictions = :n,
                        n_hits = :hits,
                        running_brier = :brier,
                        last_updated = NOW()
                    WHERE horizon_days = :h
                      AND bucket_low = :bl
                      AND bucket_high = :bh
                    """
                ),
                {
                    "n": new_n,
                    "hits": new_hits,
                    "brier": new_brier,
                    "h": horizon,
                    "bl": low,
                    "bh": high,
                },
            )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "confidence_bucket_tracker.record_scored_prediction failed: "
            "{e}",
            e=str(exc),
        )


# ── Read path ─────────────────────────────────────────────────────────────


def get_bucket_calibration(
    engine: Engine,
    *,
    confidence: float,
    horizon_days: int,
) -> BucketCalibration | None:
    """Look up the calibration row for the bucket containing
    ``confidence`` at the canonical horizon matching ``horizon_days``.

    Returns ``None`` when:
      * confidence is below 0.50 (no bucket)
      * the bucket has no recorded history yet
      * the DB read fails (swallow + debug log)
    """
    bucket = _bucket_for(confidence)
    if bucket is None:
        return None
    horizon = _canonical_horizon(horizon_days)
    low, high = bucket

    _ensure_schema(engine)

    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT horizon_days, bucket_low, bucket_high, "
                    "       n_predictions, n_hits, running_brier, "
                    "       last_updated "
                    "FROM confidence_bucket_history "
                    "WHERE horizon_days = :h "
                    "  AND bucket_low = :bl "
                    "  AND bucket_high = :bh"
                ),
                {"h": horizon, "bl": low, "bh": high},
            ).fetchone()
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "confidence_bucket_tracker.get_bucket_calibration failed: {e}",
            e=str(exc),
        )
        return None

    if row is None:
        return None
    return _row_to_bucket(row)


def conviction_multiplier_for_bucket(
    engine: Engine,
    *,
    confidence: float,
    horizon_days: int,
) -> float:
    """Return the conviction multiplier for ``confidence`` at ``horizon_days``.

    Pipeline:
      1. Resolve the containing bucket. No bucket → 1.0.
      2. Load its calibration row. No row → 1.0.
      3. Require is_calibrated (>= MIN_CALIBRATED_SAMPLES). Below it → 1.0.
      4. Compute gap = confidence - empirical_hit_rate.
      5. Map to multiplier via ``_multiplier_from_gap``.

    Never raises. Every failure mode returns the neutral 1.0 so callers
    can multiply without guarding.
    """
    try:
        cal = get_bucket_calibration(
            engine, confidence=confidence, horizon_days=horizon_days,
        )
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "conviction_multiplier_for_bucket: lookup failed: {e}",
            e=str(exc),
        )
        return MULT_NEUTRAL

    if cal is None or not cal.is_calibrated:
        return MULT_NEUTRAL

    gap = _compute_gap(float(confidence), cal.empirical_hit_rate)
    return _multiplier_from_gap(gap)


def rank_buckets_by_calibration(
    engine: Engine,
    *,
    horizon_days: int,
) -> list[BucketCalibration]:
    """Return every bucket at the canonical horizon matching
    ``horizon_days`` sorted worst→best calibrated.

    Sort key is ``|empirical_hit_rate - bucket_midpoint|`` descending,
    so the top of the list is the most miscalibrated bucket (the one
    most worthy of a dashboard red flag). Buckets with zero history
    are excluded — there's nothing to be miscalibrated about. DB
    failure → empty list.
    """
    horizon = _canonical_horizon(horizon_days)
    _ensure_schema(engine)

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT horizon_days, bucket_low, bucket_high, "
                    "       n_predictions, n_hits, running_brier, "
                    "       last_updated "
                    "FROM confidence_bucket_history "
                    "WHERE horizon_days = :h AND n_predictions > 0 "
                    "ORDER BY bucket_low ASC"
                ),
                {"h": horizon},
            ).fetchall()
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "rank_buckets_by_calibration: read failed: {e}",
            e=str(exc),
        )
        return []

    calibrations = [_row_to_bucket(r) for r in rows or []]

    def _miscalibration(c: BucketCalibration) -> float:
        midpoint = (c.bucket_low + c.bucket_high) / 2.0
        return abs(c.empirical_hit_rate - midpoint)

    calibrations.sort(key=_miscalibration, reverse=True)
    return calibrations


# ── Bootstrap replay ──────────────────────────────────────────────────────


# Pulled from oracle_predictions with the same time-ordered shape as
# ``scripts/bootstrap_per_signal_brier._fetch_scored_predictions``. We
# intentionally do NOT import that function because we need a narrower
# column set (confidence + verdict + horizon only); pulling the full
# cascade would waste work.
_BOOTSTRAP_QUERY = text(
    """
    SELECT confidence, verdict, created_at, expiry
    FROM oracle_predictions
    WHERE verdict IN ('hit', 'miss', 'partial')
      AND created_at >= NOW() - (:days || ' days')::interval
      AND dedup_keep = TRUE
    ORDER BY created_at ASC
    """
)


def _coerce_horizon_days_from_row(created: Any, expiry: Any) -> int:
    """Mirror of ``scripts/bootstrap_per_signal_brier._coerce_horizon_days``
    but without the dict indirection — direct tuple inputs. Defaults to
    7 when either timestamp is missing or the subtraction fails."""
    if created is None or expiry is None:
        return 7
    try:
        from datetime import datetime
        if isinstance(created, datetime) and isinstance(expiry, datetime):
            delta = (expiry - created).days
        else:
            created_d = (
                created.date() if hasattr(created, "date") else created
            )
            expiry_d = (
                expiry.date() if hasattr(expiry, "date") else expiry
            )
            delta = (expiry_d - created_d).days
    except Exception:  # noqa: BLE001
        return 7
    if delta <= 0:
        return 1
    return int(delta)


def bootstrap_from_oracle_predictions(
    engine: Engine,
    *,
    days: int = 365,
) -> int:
    """Replay scored ``oracle_predictions`` into bucket history.

    Mirrors the pattern in ``scripts/bootstrap_per_signal_brier`` — we
    re-use its ``verdict_to_outcome`` function (imported at module top)
    rather than rewriting the mapping. Returns the number of rows that
    were successfully written into a bucket.

    Single transaction for the replay loop. Never raises: on any top-level
    failure returns 0. Empty result set returns 0 cleanly.
    """
    _ensure_schema(engine)

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                _BOOTSTRAP_QUERY, {"days": int(days)},
            ).fetchall()
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "bootstrap_from_oracle_predictions: fetch failed: {e}",
            e=str(exc),
        )
        return 0

    if not rows:
        return 0

    replayed = 0
    for row in rows:
        try:
            confidence = row[0]
            verdict = row[1]
            created = row[2] if len(row) > 2 else None
            expiry = row[3] if len(row) > 3 else None

            if confidence is None or verdict is None:
                continue
            if verdict not in ("hit", "miss", "partial"):
                continue

            try:
                conf_f = float(confidence)
            except (TypeError, ValueError):
                continue
            if conf_f != conf_f:
                continue

            if _bucket_for(conf_f) is None:
                # Sub-0.50 confidences are out-of-scope — skip cleanly.
                continue

            outcome = verdict_to_outcome(verdict)
            horizon = _coerce_horizon_days_from_row(created, expiry)

            record_scored_prediction(
                engine,
                confidence=conf_f,
                outcome=outcome,
                horizon_days=horizon,
            )
            replayed += 1
        except Exception as exc:  # noqa: BLE001
            log.debug(
                "bootstrap_from_oracle_predictions: row replay failed: "
                "{e}",
                e=str(exc),
            )
            continue

    log.info(
        "confidence_bucket_tracker: bootstrap replayed {n} predictions",
        n=replayed,
    )
    return replayed
