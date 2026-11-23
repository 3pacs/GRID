"""
Per-signal per-horizon Brier tracker (ALPHA-15 / #118).

The calibration loop that closes the gap between "we have the data" and
"we know the data is predictive." Every time a prediction is scored,
the confidence is decomposed into signal contributions via the existing
Shapley attribution (ALPHA-9 / intelligence/shapley_attribution.py) and
each contributing signal source gets its running Brier / ECE / hit-rate
counter updated in the ``per_signal_brier_history`` table.

The conviction dial
-------------------

After 30 days of scored predictions this becomes the operator's
conviction weighter:

    "JODI oil 7d Brier = 0.18" → weight-up on energy predictions
    "reddit_options_pulse 1d Brier = 0.42" → contrarian-only, never
                                              trust the direction outright

Functions
---------

- ``record_scored_prediction`` — called once per scored prediction to
  decompose confidence → signal contributions and update every
  contributing signal's bucket.
- ``get_signal_scorecard`` — read back one (signal_source, horizon)
  scorecard with running Brier / ECE / sample count.
- ``rank_signals_by_horizon`` — sorted scorecards across all signals
  for a given horizon, used by the conviction weighter.
- ``get_full_scorecard_table`` — full matrix for the dashboard.

This module uses incremental (Welford) running averages so persisting
each scored prediction is O(k) writes where k is the number of
contributing signals — typically 3-8, never the full adapter fleet.

Schema
------

``per_signal_brier_history`` table (created on first insert):

    signal_source     TEXT     — the adapter source_module (e.g. 'jodi_oil')
    horizon_days      INTEGER  — 1 / 7 / 30 / 90
    scored_count      INTEGER  — number of predictions this signal contributed to
    running_brier     DOUBLE   — Welford mean of (pred - outcome)^2
    running_ece       DOUBLE   — Welford mean of |pred - outcome|
    hit_count         INTEGER  — predictions this signal got correct (directional)
    last_updated      TIMESTAMPTZ
    PRIMARY KEY (signal_source, horizon_days)
"""

from __future__ import annotations

import math
import threading
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Constants ─────────────────────────────────────────────────────────────

# Horizon key set — matches oracle.engine HORIZON_BUCKETS ordering.
CANONICAL_HORIZONS: tuple[int, ...] = (1, 7, 30, 90)

# Minimum sample count before a scorecard is treated as "calibrated" —
# below this, downstream consumers should treat the signal as cold-start
# and fall back to equal weighting.
MIN_CALIBRATED_SAMPLES: int = 20

# Minimum effective sample weight Σw before the conviction multiplier moves
# off neutral (1.0). Prevents tiny-weight noise signals from earning a boost
# off high raw row counts with negligible total attribution share.
MIN_EFFECTIVE_WEIGHT_SUM: float = 5.0

# Brier ceiling beyond which a signal is considered anti-predictive
# (worse than a 50/50 coin flip on the confidence scale).
ANTI_PREDICTIVE_BRIER_THRESHOLD: float = 0.25


# ── Data classes ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SignalScorecard:
    """Per-(signal_source, horizon_days) calibration scorecard.

    Immutable snapshot returned by ``get_signal_scorecard``.
    """

    signal_source: str
    horizon_days: int
    scored_count: int
    running_brier: float
    running_ece: float
    hit_rate: float
    last_updated: datetime | None
    is_calibrated: bool
    conviction_weight: float  # 0.0 (useless) → 1.5 (best-in-class)
    weight_sum: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "signal_source": self.signal_source,
            "horizon_days": self.horizon_days,
            "scored_count": self.scored_count,
            "running_brier": round(self.running_brier, 6),
            "running_ece": round(self.running_ece, 6),
            "hit_rate": round(self.hit_rate, 4),
            "last_updated": (
                self.last_updated.isoformat() if self.last_updated else None
            ),
            "is_calibrated": self.is_calibrated,
            "conviction_weight": round(self.conviction_weight, 4),
            "weight_sum": (
                round(self.weight_sum, 6) if self.weight_sum is not None else None
            ),
        }


# ── Table setup ───────────────────────────────────────────────────────────


_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS per_signal_brier_history (
    signal_source    TEXT        NOT NULL,
    horizon_days     INTEGER     NOT NULL,
    scored_count     INTEGER     NOT NULL DEFAULT 0,
    running_brier    DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    running_ece      DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    hit_count        INTEGER     NOT NULL DEFAULT 0,
    last_updated     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (signal_source, horizon_days)
);

CREATE INDEX IF NOT EXISTS idx_psbh_horizon
    ON per_signal_brier_history (horizon_days, running_brier ASC);
CREATE INDEX IF NOT EXISTS idx_psbh_source
    ON per_signal_brier_history (signal_source, horizon_days);
"""


def ensure_tables(engine: Engine) -> None:
    """Create ``per_signal_brier_history`` if it doesn't exist. Idempotent."""
    try:
        with engine.begin() as conn:
            for stmt in _TABLE_SQL.strip().split(";"):
                s = stmt.strip()
                if s:
                    conn.execute(text(s))
    except Exception as exc:  # noqa: BLE001
        log.warning("per_signal_brier: ensure_tables failed: {e}", e=str(exc))


# ── Live-skip tracking (no weight_sum column on current schema) ───────────

_SKIPPED_LIVE_UPDATES_LOCK = threading.Lock()
_SKIPPED_LIVE_UPDATE_COUNTS: dict[tuple[str, int], int] = {}


def _record_live_skip(source: str, horizon: int) -> bool:
    """Increment the skipped-live-update count for ``(source, horizon)``.

    Returns True on the first skip per process for that key so callers can
    log once at INFO instead of spamming per prediction.
    """
    key = (source, int(horizon))
    with _SKIPPED_LIVE_UPDATES_LOCK:
        prev = _SKIPPED_LIVE_UPDATE_COUNTS.get(key, 0)
        _SKIPPED_LIVE_UPDATE_COUNTS[key] = prev + 1
        return prev == 0


def get_skipped_live_update_counts() -> dict[tuple[str, int], int]:
    """Return a snapshot of skipped live update counts keyed by ``(source, horizon)``."""
    with _SKIPPED_LIVE_UPDATES_LOCK:
        return dict(_SKIPPED_LIVE_UPDATE_COUNTS)


def _reset_skipped_live_update_counts() -> None:
    """Clear process-local skip counts and log deduplication state (for tests)."""
    with _SKIPPED_LIVE_UPDATES_LOCK:
        _SKIPPED_LIVE_UPDATE_COUNTS.clear()


# ── Core update path ──────────────────────────────────────────────────────


def _canonical_horizon(horizon_days: int | str) -> int:
    """Snap an arbitrary horizon to the nearest canonical bucket."""
    try:
        h = int(horizon_days)
    except (TypeError, ValueError):
        return 7
    if h <= 2:
        return 1
    if h <= 14:
        return 7
    if h <= 60:
        return 30
    return 90


def record_scored_prediction(
    engine: Engine,
    *,
    horizon_days: int,
    confidence: float,
    outcome: float,
    signal_contributions: dict[str, float],
    scored_at: datetime | None = None,
) -> dict[str, Any]:
    """Live incremental updater hook for per-signal Brier tracking.

    Proper incremental weighted averaging ``Σ(w·se) / Σw`` requires a
    persisted ``weight_sum`` (Σw) column. Because the current
    ``per_signal_brier_history`` schema has no ``weight_sum`` column (and no
    schema change is assumed here), the live path refuses both incremental
    UPDATEs (which would drift) and brand-new ``count=1`` INSERTs (which
    could never advance on subsequent calls). Every valid contributor is
    skipped, counted in ``get_skipped_live_update_counts()``, and logged once
    per process per ``(source, horizon)`` at INFO. Recompute exact weighted
    means via ``scripts/bootstrap_per_signal_brier.py``.

    Returns an empty dict when skipped.
    """
    del engine, confidence, outcome, scored_at  # unused while schema lacks Σw
    if not signal_contributions:
        return {}

    horizon = _canonical_horizon(horizon_days)
    for source, weight in signal_contributions.items():
        try:
            w = float(weight)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(w) or w <= 0.0:
            continue
        if _record_live_skip(source, horizon):
            log.info(
                "per_signal_brier.record_scored_prediction: skipping live "
                "incremental write for ({s}, {h}d) — table has no persisted "
                "weight_sum (Σw) column; recompute via bootstrap",
                s=source,
                h=horizon,
            )

    return {}


# ── Conviction weighter ───────────────────────────────────────────────────


def compute_conviction_weight(
    running_brier: float,
    scored_count: int,
    *,
    weight_sum: float | None = None,
) -> float:
    """Translate a running Brier into a conviction weight in [0.0, 1.5].

    Rules (hard-coded, deterministic):

    - Below MIN_CALIBRATED_SAMPLES → 1.0 (neutral — insufficient history)
    - When ``weight_sum`` (Σw) is provided and below MIN_EFFECTIVE_WEIGHT_SUM
      (5.0) → 1.0 (neutral — insufficient effective sample weight)
    - Brier >= ANTI_PREDICTIVE_BRIER_THRESHOLD (0.25) → 0.0 (useless)
    - Brier <= 0.05 (very well-calibrated) → 1.5 (max weight-up)
    - Linear interpolation in between

    Pure function — no engine required.
    """
    if scored_count < MIN_CALIBRATED_SAMPLES:
        return 1.0
    if weight_sum is not None and (
        not math.isfinite(weight_sum)
        or weight_sum + 1e-12 < MIN_EFFECTIVE_WEIGHT_SUM
    ):
        return 1.0
    if running_brier >= ANTI_PREDICTIVE_BRIER_THRESHOLD:
        return 0.0
    if running_brier <= 0.05:
        return 1.5
    # Linear: Brier 0.05 → 1.5, Brier 0.25 → 0.0
    span = ANTI_PREDICTIVE_BRIER_THRESHOLD - 0.05
    progress = (running_brier - 0.05) / span  # 0.0 best → 1.0 worst
    weight = 1.5 - 1.5 * progress
    return max(0.0, min(1.5, weight))


# ── Read path ─────────────────────────────────────────────────────────────


def _row_to_scorecard(row: Any) -> SignalScorecard:
    """Reconstruct a SignalScorecard from a 7-column ``per_signal_brier_history`` row.

    Conservative read-time floor: ``per_signal_brier_history`` has no
    ``weight_sum`` (Σw) or JSON/metadata column on the current schema, so
    read-time enforces ``scored_count < MIN_CALIBRATED_SAMPLES (20)`` →
    ``is_calibrated=False`` and neutral ``conviction_weight=1.0``. The
    bootstrap writer enforces the ``Σw >= MIN_EFFECTIVE_WEIGHT_SUM (5.0)``
    floor fail-closed before persisting any row with ``scored_count >= 20``.

    A row cannot be checked for Σw or metric version here, so trust rests on
    the only writer that creates rows: ``scripts/bootstrap_per_signal_brier``
    replaces the whole table in one transaction and retires every row it
    rejects or no longer produces. Rows from before that cutover stay readable
    until the first replacement run. The live updater writes nothing, and
    ``intelligence.forensic_journal`` can only raise an existing row's Brier.
    """
    source = row[0]
    horizon = int(row[1])
    count = int(row[2] or 0)
    brier = float(row[3] or 0.0)
    ece = float(row[4] or 0.0)
    hits = int(row[5] or 0)
    last_updated = row[6]
    hit_rate = (hits / count) if count > 0 else 0.0
    is_calibrated = count >= MIN_CALIBRATED_SAMPLES
    conviction = (
        compute_conviction_weight(brier, count)
        if is_calibrated
        else 1.0
    )
    return SignalScorecard(
        signal_source=source,
        horizon_days=horizon,
        scored_count=count,
        running_brier=brier,
        running_ece=ece,
        hit_rate=hit_rate,
        last_updated=last_updated,
        is_calibrated=is_calibrated,
        conviction_weight=conviction,
        weight_sum=None,
    )


def get_signal_scorecard(
    engine: Engine,
    signal_source: str,
    horizon_days: int,
) -> SignalScorecard | None:
    """Read one (signal_source, horizon_days) scorecard. Returns None if
    no history has been recorded yet."""
    horizon = _canonical_horizon(horizon_days)
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT signal_source, horizon_days, scored_count, "
                    "running_brier, running_ece, hit_count, last_updated "
                    "FROM per_signal_brier_history "
                    "WHERE signal_source = :s AND horizon_days = :h"
                ),
                {"s": signal_source, "h": horizon},
            ).fetchone()
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "per_signal_brier.get_signal_scorecard read failed: {e}",
            e=str(exc),
        )
        return None
    if row is None:
        return None
    return _row_to_scorecard(row)


def rank_signals_by_horizon(
    engine: Engine,
    horizon_days: int,
    *,
    min_samples: int | None = None,
) -> list[SignalScorecard]:
    """Return all scorecards for ``horizon_days`` sorted best→worst by Brier.

    ``min_samples`` defaults to ``MIN_CALIBRATED_SAMPLES`` — callers that
    want to see cold-start signals too can pass ``0``.
    """
    horizon = _canonical_horizon(horizon_days)
    threshold = MIN_CALIBRATED_SAMPLES if min_samples is None else int(min_samples)
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT signal_source, horizon_days, scored_count, "
                    "running_brier, running_ece, hit_count, last_updated "
                    "FROM per_signal_brier_history "
                    "WHERE horizon_days = :h AND scored_count >= :n "
                    "ORDER BY running_brier ASC"
                ),
                {"h": horizon, "n": threshold},
            ).fetchall()
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "per_signal_brier.rank_signals_by_horizon read failed: {e}",
            e=str(exc),
        )
        return []
    return [_row_to_scorecard(r) for r in rows]


def get_full_scorecard_table(engine: Engine) -> list[SignalScorecard]:
    """Return every scorecard for every (signal_source, horizon) pair.

    Unordered — caller sorts. Used by the dashboard to render the full
    calibration matrix.
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT signal_source, horizon_days, scored_count, "
                    "running_brier, running_ece, hit_count, last_updated "
                    "FROM per_signal_brier_history "
                    "ORDER BY signal_source, horizon_days"
                )
            ).fetchall()
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "per_signal_brier.get_full_scorecard_table read failed: {e}",
            e=str(exc),
        )
        return []
    return [_row_to_scorecard(r) for r in rows]
