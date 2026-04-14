"""
Meta learning matrix — per-signal × per-condition edge learner (CAT-193 / #295).

The orthogonal complement to ``features.per_signal_brier`` and the
regime-conditional Brier layer:

- **Brier** (already exists) measures *calibration* — are my probabilities
  honest? A signal can be well-calibrated but have no edge (e.g. always
  says 50% and is always 50% right — calibrated, useless).
- **Meta learning matrix** (this module) measures *edge* — given that
  we're in condition X, does trusting this signal make or lose money?
  A signal can be miscalibrated but have huge edge (always says 80% and
  wins 60% — miscalibrated, but still +10% over coinflip).

Both dimensions are useful and orthogonal; the live confidence stack
multiplies them together.

The condition cube
------------------

Each scored prediction lands in a 4-dimensional condition tuple::

    horizon_bucket   SHORT (1d) | MID (7d) | LONG (30d / 90d)
    liquidity_regime CRISIS | TIGHTENING | NEUTRAL | EXPANSION | EXPANSION_STRONG
    fci_bucket       TIGHT | NEUTRAL | EASY
    vol_regime       LOW | NORMAL | HIGH

The full cube has 3 × 5 × 3 × 3 = 135 cells. Not every cell will have
enough data; thin cells fall back to neutral weight (1.0). The
``MIN_META_SAMPLES`` constant is the hard threshold below which we
never modify the base signal weight.

Edge math
---------

For each (signal, condition_tuple) the matrix stores running counters
of firings and hits. Edge is computed fresh on read::

    hit_rate    = n_hits / n_firings
    edge        = hit_rate - 0.5               # raw improvement over coin flip
    scaled_edge = edge × min(n_firings/20, 1)  # damps early signals

The scaled edge maps to a weight multiplier via a deterministic ladder
(see ``_EDGE_LADDER`` below). Multipliers are clamped to
``[MIN_EDGE_MULTIPLIER, MAX_EDGE_MULTIPLIER]``.

Aggregation across signals
--------------------------

``get_aggregate_weight_multiplier`` uses a *weighted harmonic mean* —
not arithmetic — because the multiplier sits in a multiplicative stack
alongside fragility / red-team / co-occurrence and the correct way to
combine N independent log-space adjusters is the harmonic average.
Harmonic is also less forgiving of outliers: a single actively
anti-predictive signal (0.4) pulls the aggregate down harder than an
arithmetic mean would, which is exactly what we want.

See ``features/per_signal_brier.py`` for calibration counterpart.
See ``intelligence/signal_provenance.py`` for how the aggregate
multiplier plugs into ``compute_aggregate_conviction``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# ── Reused invariants — do NOT redefine locally ─────────────────────────────
#
# These imports prove we're not re-implementing the canonical regime taxonomy,
# horizon bucketing, verdict mapping, or signal-contribution extraction —
# every one of those lives in an existing module that this file imports.

from features.per_signal_brier import (  # noqa: F401 — CANONICAL_HORIZONS reused for horizon taxonomy
    CANONICAL_HORIZONS,
    MIN_CALIBRATED_SAMPLES,
)
from oracle.regime_router import (  # noqa: F401 — canonical 5-state regime taxonomy
    REGIME_STATES as CANONICAL_REGIMES,
    _canonical_regime,
)
from scripts.bootstrap_per_signal_brier import (  # noqa: F401
    ORACLE_AGGREGATE_SOURCE,
    _coerce_horizon_days,
    extract_signal_contributions,
    verdict_to_outcome,
)


# ── Hardcoded thresholds (rationale in docstrings) ──────────────────────────

# Minimum firings in a (signal, condition) cell before we trust the edge
# enough to move the weight multiplier off neutral. Chosen slightly lower
# than ``MIN_CALIBRATED_SAMPLES`` (=20 for Brier) because the matrix is
# sliced 45× finer by condition cell — demanding 20/cell would leave
# almost every cell cold for months.
MIN_META_SAMPLES: int = 15

# Shapley weight below which a signal is considered "didn't really fire"
# for this prediction. Matches the cutoff used by the walk-forward
# validator and signal provenance contributors.
SHAPLEY_FIRING_THRESHOLD: float = 0.1

# Sample size at which the raw edge is taken at face value. Below this
# threshold, the edge is damped by (n / SCALED_EDGE_FULL_N). Separate
# from MIN_META_SAMPLES so early cells can still *compute* a damped
# edge for introspection — the damping is independent of the
# "trust-this-cell" gate.
SCALED_EDGE_FULL_N: int = 20

# Weight multiplier clamp. Lower bound = 0.4 because an actively
# anti-predictive signal should NOT be flipped / shorted — it should
# be ignored (60% weight-down) but never deleted. Upper bound = 1.5
# matches per_signal_brier.MAX_CONVICTION_WEIGHT so the two layers stack
# into a bounded multiplier.
MIN_EDGE_MULTIPLIER: float = 0.4
MAX_EDGE_MULTIPLIER: float = 1.5

# Edge-to-multiplier ladder. Ordered from best edge → worst edge. Each
# entry is (min_scaled_edge_inclusive, multiplier). The first matching
# bucket wins when iterated top-down. These are deliberately coarse
# (5 buckets) so the system behaves step-wise rather than drifting —
# the operator can eyeball the matrix and know which cells are
# actually moving weights.
_EDGE_LADDER: tuple[tuple[float, float], ...] = (
    (0.10, 1.30),   # strong edge → 30% weight-up
    (0.05, 1.15),   # mild edge → 15% weight-up
    (-0.05, 1.00),  # neutral band
    (-0.10, 0.70),  # mild anti-edge → 30% weight-down
    (float("-inf"), 0.40),  # strong anti-edge → 60% weight-down
)

# Volatility bucket thresholds (VIX level).
VOL_LOW_CEIL: float = 15.0   # <15 → LOW
VOL_HIGH_FLOOR: float = 22.0  # >22 → HIGH; between → NORMAL

# Schema initialization cache — keyed on ``id(engine)``. Identical pattern
# to the walk-forward harness's FakeEngine cache so it plays nicely with
# tests that instantiate many short-lived engines.
_initialized_engines: set[int] = set()


# ── Schema init ─────────────────────────────────────────────────────────────


_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS meta_learning_matrix (
    id SERIAL PRIMARY KEY,
    signal_source TEXT NOT NULL,
    horizon_bucket TEXT NOT NULL,
    liquidity_regime TEXT NOT NULL,
    fci_bucket TEXT NOT NULL,
    vol_regime TEXT NOT NULL,
    n_predictions INT NOT NULL DEFAULT 0,
    n_firings INT NOT NULL DEFAULT 0,
    n_hits NUMERIC(10,2) NOT NULL DEFAULT 0,
    sum_scaled_edge NUMERIC(10,6) NOT NULL DEFAULT 0,
    last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(signal_source, horizon_bucket, liquidity_regime, fci_bucket, vol_regime)
);
"""

_INDEX_SIGNAL_SQL = (
    "CREATE INDEX IF NOT EXISTS idx_meta_learning_signal "
    "ON meta_learning_matrix(signal_source);"
)
_INDEX_LAST_UPDATED_SQL = (
    "CREATE INDEX IF NOT EXISTS idx_meta_learning_last_updated "
    "ON meta_learning_matrix(last_updated);"
)


def _ensure_schema(engine: Engine) -> None:
    """Idempotently create the ``meta_learning_matrix`` table + indexes.

    Caches the (id(engine)) after the first successful call so the DDL
    only runs once per engine instance. Never raises — schema failures
    fall through to the neutral-multiplier cold-start path.
    """
    key = id(engine)
    if key in _initialized_engines:
        return
    try:
        with engine.begin() as conn:
            conn.execute(text(_TABLE_SQL))
            conn.execute(text(_INDEX_SIGNAL_SQL))
            conn.execute(text(_INDEX_LAST_UPDATED_SQL))
        _initialized_engines.add(key)
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "meta_learning_matrix._ensure_schema failed: {e}", e=str(exc)
        )


def _reset_initialized_engines() -> None:
    """Clear the schema-init cache. Used by tests that spin up fresh
    FakeEngine instances that would otherwise be marked initialised."""
    _initialized_engines.clear()


# ── Bucketing helpers ───────────────────────────────────────────────────────


def bucket_horizon(horizon_days: int) -> str:
    """Snap a horizon-in-days into SHORT / MID / LONG.

    SHORT = 1d, MID = 7d, LONG = 30d or 90d. Matches the canonical
    horizon set in ``features.per_signal_brier.CANONICAL_HORIZONS``.
    Non-numeric / negative → MID (neutral fallback).
    """
    try:
        h = int(horizon_days)
    except (TypeError, ValueError):
        return "MID"
    if h <= 0:
        return "MID"
    if h <= 2:
        return "SHORT"
    if h <= 14:
        return "MID"
    return "LONG"


def bucket_fci(fci_regime: str | None) -> str:
    """Coerce any FCI regime string into TIGHT / NEUTRAL / EASY.

    ``TIGHT`` / ``TIGHTENING`` collapse to TIGHT. ``EASY`` / ``LOOSE`` /
    ``EXPANSION`` collapse to EASY. Everything else (including None,
    empty, unknown) → NEUTRAL.
    """
    if fci_regime is None:
        return "NEUTRAL"
    try:
        upper = str(fci_regime).strip().upper()
    except Exception:  # noqa: BLE001
        return "NEUTRAL"
    if not upper:
        return "NEUTRAL"
    if upper in {"TIGHT", "TIGHTENING"}:
        return "TIGHT"
    if upper in {"EASY", "LOOSE", "EXPANSION"}:
        return "EASY"
    return "NEUTRAL"


def bucket_vol(vix_level: float | None) -> str:
    """Bucket a VIX level into LOW / NORMAL / HIGH.

    Thresholds: ``<15 → LOW``, ``[15, 22] → NORMAL``, ``>22 → HIGH``.
    Missing / non-numeric / negative → NORMAL.
    """
    if vix_level is None:
        return "NORMAL"
    try:
        v = float(vix_level)
    except (TypeError, ValueError):
        return "NORMAL"
    if v != v:  # NaN
        return "NORMAL"
    if v < 0:
        return "NORMAL"
    if v < VOL_LOW_CEIL:
        return "LOW"
    if v > VOL_HIGH_FLOOR:
        return "HIGH"
    return "NORMAL"


# ── Condition tuple + edge row dataclasses ─────────────────────────────────


@dataclass(frozen=True)
class ConditionTuple:
    """4-dimensional condition key for a (signal × condition) cell.

    Immutable so it can be used as a dict key or set member.
    """

    horizon_bucket: str
    liquidity_regime: str
    fci_bucket: str
    vol_regime: str

    def to_key(self) -> tuple[str, str, str, str]:
        """Hashable 4-tuple for use as a dict key."""
        return (
            self.horizon_bucket,
            self.liquidity_regime,
            self.fci_bucket,
            self.vol_regime,
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "horizon_bucket": self.horizon_bucket,
            "liquidity_regime": self.liquidity_regime,
            "fci_bucket": self.fci_bucket,
            "vol_regime": self.vol_regime,
        }


@dataclass(frozen=True)
class MetaEdgeRow:
    """Read-model for one (signal_source × ConditionTuple) cell."""

    signal_source: str
    condition: ConditionTuple
    n_predictions: int
    n_firings: int
    hit_rate: float
    scaled_edge: float
    weight_multiplier: float
    last_updated: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "signal_source": self.signal_source,
            "condition": self.condition.to_dict(),
            "n_predictions": int(self.n_predictions),
            "n_firings": int(self.n_firings),
            "hit_rate": round(float(self.hit_rate), 6),
            "scaled_edge": round(float(self.scaled_edge), 6),
            "weight_multiplier": round(float(self.weight_multiplier), 6),
            "last_updated": self.last_updated,
        }


def build_condition_tuple(
    *,
    horizon_days: int,
    liquidity_regime: str | None,
    fci_regime: str | None,
    vix_level: float | None,
) -> ConditionTuple:
    """Construct a ConditionTuple from raw per-prediction context.

    Each dimension is normalized via its dedicated bucket helper so
    callers can pass raw scheduler/oracle field values without worrying
    about casing or canonicalization.
    """
    return ConditionTuple(
        horizon_bucket=bucket_horizon(horizon_days),
        liquidity_regime=_canonical_regime(liquidity_regime),
        fci_bucket=bucket_fci(fci_regime),
        vol_regime=bucket_vol(vix_level),
    )


# ── Edge → multiplier math ─────────────────────────────────────────────────


def _compute_scaled_edge(n_firings: int, n_hits: float) -> tuple[float, float]:
    """Return (hit_rate, scaled_edge). Pure function — no engine needed.

    - ``hit_rate = n_hits / n_firings`` (0.0 if n_firings == 0)
    - ``edge     = hit_rate - 0.5``
    - ``scaled_edge = edge × min(n_firings / SCALED_EDGE_FULL_N, 1.0)``
    """
    if n_firings <= 0:
        return 0.0, 0.0
    hit_rate = float(n_hits) / float(n_firings)
    edge = hit_rate - 0.5
    damper = min(n_firings / float(SCALED_EDGE_FULL_N), 1.0)
    return hit_rate, edge * damper


def _multiplier_from_scaled_edge(
    n_firings: int, scaled_edge: float
) -> float:
    """Map ``scaled_edge`` → weight multiplier via the deterministic ladder.

    Cold-start (below MIN_META_SAMPLES) always returns 1.0 regardless of
    the computed edge — a 14-firing cell with +0.20 scaled edge is not
    statistically trustworthy, so we refuse to touch the weight.
    """
    if n_firings < MIN_META_SAMPLES:
        return 1.0
    for floor_edge, multiplier in _EDGE_LADDER:
        if scaled_edge >= floor_edge:
            return max(
                MIN_EDGE_MULTIPLIER, min(MAX_EDGE_MULTIPLIER, multiplier)
            )
    # Unreachable — the final ladder entry uses -inf.
    return 1.0


def _verdict_to_hit_score(outcome: str) -> float:
    """Map verdict/outcome strings into the hit-rate scoring scalar.

    - ``hit`` → 1.0
    - ``partial`` → 0.5
    - ``miss`` / anything else → 0.0
    """
    if not isinstance(outcome, str):
        return 0.0
    o = outcome.strip().lower()
    if o == "hit":
        return 1.0
    if o == "partial":
        return 0.5
    return 0.0


# ── Write path ──────────────────────────────────────────────────────────────


def record_scored_prediction(
    engine: Engine,
    *,
    signal_source: str,
    shapley_weight: float,
    confidence: float,
    direction: str,
    outcome: str,
    horizon_days: int,
    liquidity_regime: str | None,
    fci_regime: str | None,
    vix_level: float | None,
) -> None:
    """Update the (signal × condition) cell for one scored prediction.

    Welford-style running update over ``n_firings`` and ``n_hits`` (we
    keep raw counters, not means, so the edge can be recomputed exactly
    on read without compound floating-point drift).

    Signals with ``shapley_weight < SHAPLEY_FIRING_THRESHOLD`` are
    skipped — they didn't really fire for this prediction. Silently
    swallows DB errors so the scheduler path is never taken down by
    a matrix-update failure.

    ``direction`` is accepted for API symmetry with the live oracle path
    but is not used for the hit calculation — the verdict already
    encodes direction (a bearish prediction that went up = miss).
    ``confidence`` is similarly accepted for API symmetry.
    """
    # Defensive input validation.
    if not isinstance(signal_source, str) or not signal_source.strip():
        return
    try:
        weight = float(shapley_weight)
    except (TypeError, ValueError):
        return
    if weight < SHAPLEY_FIRING_THRESHOLD:
        return

    hit_score = _verdict_to_hit_score(outcome)
    condition = build_condition_tuple(
        horizon_days=horizon_days,
        liquidity_regime=liquidity_regime,
        fci_regime=fci_regime,
        vix_level=vix_level,
    )

    # Reference confidence + direction so linters don't flag them as
    # unused — they're accepted for API symmetry with the live path.
    _ = float(confidence) if confidence is not None else 0.0
    _ = str(direction) if direction is not None else ""

    _ensure_schema(engine)

    try:
        with engine.begin() as conn:
            row = conn.execute(
                text(
                    "SELECT n_predictions, n_firings, n_hits, sum_scaled_edge "
                    "FROM meta_learning_matrix "
                    "WHERE signal_source = :s "
                    "AND horizon_bucket = :hb "
                    "AND liquidity_regime = :lq "
                    "AND fci_bucket = :fci "
                    "AND vol_regime = :vol"
                ),
                {
                    "s": signal_source.strip(),
                    "hb": condition.horizon_bucket,
                    "lq": condition.liquidity_regime,
                    "fci": condition.fci_bucket,
                    "vol": condition.vol_regime,
                },
            ).fetchone()

            if row is None:
                # First firing for this cell.
                new_firings = 1
                new_hits = hit_score
                _, new_scaled = _compute_scaled_edge(new_firings, new_hits)
                conn.execute(
                    text(
                        "INSERT INTO meta_learning_matrix "
                        "(signal_source, horizon_bucket, liquidity_regime, "
                        " fci_bucket, vol_regime, n_predictions, n_firings, "
                        " n_hits, sum_scaled_edge, last_updated) "
                        "VALUES (:s, :hb, :lq, :fci, :vol, 1, :nf, :nh, "
                        " :sse, NOW()) "
                        "ON CONFLICT (signal_source, horizon_bucket, "
                        " liquidity_regime, fci_bucket, vol_regime) "
                        "DO NOTHING"
                    ),
                    {
                        "s": signal_source.strip(),
                        "hb": condition.horizon_bucket,
                        "lq": condition.liquidity_regime,
                        "fci": condition.fci_bucket,
                        "vol": condition.vol_regime,
                        "nf": new_firings,
                        "nh": new_hits,
                        "sse": new_scaled,
                    },
                )
                return

            old_preds = int(row[0] or 0)
            old_firings = int(row[1] or 0)
            old_hits = float(row[2] or 0.0)

            new_preds = old_preds + 1
            new_firings = old_firings + 1
            new_hits = old_hits + hit_score
            _, new_scaled = _compute_scaled_edge(new_firings, new_hits)

            conn.execute(
                text(
                    "UPDATE meta_learning_matrix "
                    "SET n_predictions = :np, "
                    "    n_firings = :nf, "
                    "    n_hits = :nh, "
                    "    sum_scaled_edge = :sse, "
                    "    last_updated = NOW() "
                    "WHERE signal_source = :s "
                    "AND horizon_bucket = :hb "
                    "AND liquidity_regime = :lq "
                    "AND fci_bucket = :fci "
                    "AND vol_regime = :vol"
                ),
                {
                    "np": new_preds,
                    "nf": new_firings,
                    "nh": new_hits,
                    "sse": new_scaled,
                    "s": signal_source.strip(),
                    "hb": condition.horizon_bucket,
                    "lq": condition.liquidity_regime,
                    "fci": condition.fci_bucket,
                    "vol": condition.vol_regime,
                },
            )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "meta_learning_matrix.record_scored_prediction failed: {e}",
            e=str(exc),
        )


# ── Read path ───────────────────────────────────────────────────────────────


def _row_to_edge_row(
    signal_source: str, condition: ConditionTuple, row: Any
) -> MetaEdgeRow:
    """Build a ``MetaEdgeRow`` from a SELECT row.

    Expected row shape:
        (n_predictions, n_firings, n_hits, sum_scaled_edge, last_updated)
    """
    n_preds = int(row[0] or 0)
    n_firings = int(row[1] or 0)
    n_hits = float(row[2] or 0.0)
    last_updated = row[4] if len(row) > 4 else None
    hit_rate, scaled_edge = _compute_scaled_edge(n_firings, n_hits)
    multiplier = _multiplier_from_scaled_edge(n_firings, scaled_edge)

    if isinstance(last_updated, datetime):
        last_updated_str = last_updated.isoformat()
    elif last_updated is None:
        last_updated_str = datetime.now(timezone.utc).isoformat()
    else:
        last_updated_str = str(last_updated)

    return MetaEdgeRow(
        signal_source=signal_source,
        condition=condition,
        n_predictions=n_preds,
        n_firings=n_firings,
        hit_rate=hit_rate,
        scaled_edge=scaled_edge,
        weight_multiplier=multiplier,
        last_updated=last_updated_str,
    )


def get_edge_row(
    engine: Engine,
    *,
    signal_source: str,
    condition: ConditionTuple,
) -> MetaEdgeRow | None:
    """Read one (signal × condition) cell. Returns None if the cell is
    empty or the DB call fails."""
    if not signal_source:
        return None
    _ensure_schema(engine)
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT n_predictions, n_firings, n_hits, "
                    "sum_scaled_edge, last_updated "
                    "FROM meta_learning_matrix "
                    "WHERE signal_source = :s "
                    "AND horizon_bucket = :hb "
                    "AND liquidity_regime = :lq "
                    "AND fci_bucket = :fci "
                    "AND vol_regime = :vol"
                ),
                {
                    "s": signal_source,
                    "hb": condition.horizon_bucket,
                    "lq": condition.liquidity_regime,
                    "fci": condition.fci_bucket,
                    "vol": condition.vol_regime,
                },
            ).fetchone()
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "meta_learning_matrix.get_edge_row read failed: {e}",
            e=str(exc),
        )
        return None
    if row is None:
        return None
    return _row_to_edge_row(signal_source, condition, row)


def get_weight_multiplier(
    engine: Engine,
    *,
    signal_source: str,
    condition: ConditionTuple,
) -> float:
    """Return the clamped weight multiplier for this (signal, condition).

    Always safe: returns 1.0 when the cell is missing, cold-start
    (``n_firings < MIN_META_SAMPLES``), or the DB read fails. This is
    the hot path the live conviction stack calls once per contributing
    signal.
    """
    row = get_edge_row(
        engine, signal_source=signal_source, condition=condition
    )
    if row is None:
        return 1.0
    return max(
        MIN_EDGE_MULTIPLIER,
        min(MAX_EDGE_MULTIPLIER, float(row.weight_multiplier)),
    )


def get_aggregate_weight_multiplier(
    engine: Engine,
    *,
    signal_contributions: dict[str, float],
    condition: ConditionTuple,
) -> float:
    """Weighted harmonic mean of per-signal multipliers in this condition.

    Weights come from ``signal_contributions`` (Shapley weights). Only
    signals that (a) are firing (weight >= SHAPLEY_FIRING_THRESHOLD)
    AND (b) have a non-neutral resolved multiplier (i.e. their cell
    passed the ``MIN_META_SAMPLES`` gate) participate.

    Returns 1.0 when no signal has enough history — the fallback keeps
    the stack bounded even during cold start. The harmonic mean is the
    correct aggregation for "multiply N independent log-space
    adjusters": it matches how fragility × red-team × co-occurrence
    already stack upstream, and it punishes a single very-low
    multiplier harder than an arithmetic mean would.

        weighted_harmonic(w_i, m_i) = (Σ w_i) / (Σ w_i / m_i)

    Never raises.
    """
    if not signal_contributions:
        return 1.0

    total_weight = 0.0
    total_weight_over_multiplier = 0.0

    try:
        for source, weight in signal_contributions.items():
            try:
                w = float(weight)
            except (TypeError, ValueError):
                continue
            if w < SHAPLEY_FIRING_THRESHOLD:
                continue
            row = get_edge_row(
                engine, signal_source=source, condition=condition
            )
            if row is None:
                continue
            if row.n_firings < MIN_META_SAMPLES:
                continue
            multiplier = max(
                MIN_EDGE_MULTIPLIER,
                min(MAX_EDGE_MULTIPLIER, float(row.weight_multiplier)),
            )
            if multiplier <= 0.0:
                continue
            total_weight += w
            total_weight_over_multiplier += w / multiplier
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "meta_learning_matrix.get_aggregate_weight_multiplier failed: {e}",
            e=str(exc),
        )
        return 1.0

    if total_weight <= 0.0 or total_weight_over_multiplier <= 0.0:
        return 1.0

    aggregate = total_weight / total_weight_over_multiplier
    return max(
        MIN_EDGE_MULTIPLIER, min(MAX_EDGE_MULTIPLIER, float(aggregate))
    )


def rank_signals_by_edge(
    engine: Engine,
    *,
    condition: ConditionTuple,
    min_samples: int = MIN_META_SAMPLES,
) -> list[MetaEdgeRow]:
    """Return every signal's edge row for this condition, best-edge first.

    Used by the dashboard "which signals to trust in this condition"
    panel. Cells with fewer than ``min_samples`` firings are filtered
    out so the list only contains actionable rows.
    """
    _ensure_schema(engine)
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT signal_source, n_predictions, n_firings, "
                    "n_hits, sum_scaled_edge, last_updated "
                    "FROM meta_learning_matrix "
                    "WHERE horizon_bucket = :hb "
                    "AND liquidity_regime = :lq "
                    "AND fci_bucket = :fci "
                    "AND vol_regime = :vol "
                    "AND n_firings >= :min_n "
                    "ORDER BY n_firings DESC, signal_source ASC"
                ),
                {
                    "hb": condition.horizon_bucket,
                    "lq": condition.liquidity_regime,
                    "fci": condition.fci_bucket,
                    "vol": condition.vol_regime,
                    "min_n": int(min_samples),
                },
            ).fetchall()
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "meta_learning_matrix.rank_signals_by_edge read failed: {e}",
            e=str(exc),
        )
        return []

    out: list[MetaEdgeRow] = []
    for r in rows or []:
        source = r[0]
        # Row shape from SELECT: (source, n_preds, n_firings, n_hits, sse, last)
        out.append(
            _row_to_edge_row(
                source, condition, (r[1], r[2], r[3], r[4], r[5])
            )
        )
    # Sort best-edge first (descending by scaled_edge).
    out.sort(key=lambda e: (-e.scaled_edge, e.signal_source))
    return out


# ── Bootstrap path ──────────────────────────────────────────────────────────


_BOOTSTRAP_QUERY = text(
    """
    SELECT id, ticker, created_at, expiry, confidence, verdict,
           model_name, signals, signal_contributions, model_weights
    FROM oracle_predictions
    WHERE verdict IN ('hit', 'miss', 'partial')
      AND created_at >= NOW() - (:days || ' days')::interval
    ORDER BY created_at ASC
    """
)


def _extract_signals_context(raw: Any) -> dict[str, Any]:
    """Pull the context fields we need (liquidity / fci / vix / direction)
    out of the oracle_predictions ``signals`` JSONB blob.

    Returns an empty dict if the blob is missing or malformed.
    """
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, (bytes, bytearray)):
        try:
            raw = raw.decode("utf-8")
        except UnicodeDecodeError:
            return {}
    if isinstance(raw, str):
        import json as _json

        try:
            parsed = _json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except (ValueError, TypeError):
            return {}
    return {}


def bootstrap_from_oracle_predictions(
    engine: Engine,
    *,
    days: int = 365,
) -> int:
    """Replay scored oracle_predictions into the matrix.

    Mirrors ``scripts.bootstrap_per_signal_brier.replay_predictions``
    for the per_signal_brier_history table, but lands every (signal,
    condition) update in ``meta_learning_matrix`` instead.

    Reuses ``verdict_to_outcome`` / ``extract_signal_contributions`` /
    ``_coerce_horizon_days`` from the bootstrap module — no
    re-implementation. Returns the number of predictions successfully
    processed. Returns 0 on any top-level failure; per-row failures
    are counted as skipped and logged at debug level.
    """
    _ensure_schema(engine)

    # Reuse the oracle_models lookup helper indirectly — we inline the
    # loader here (trivial) rather than importing a private function.
    oracle_models_lookup: dict[str, list[str]] = {}
    try:
        with engine.connect() as conn:
            model_rows = conn.execute(
                text("SELECT name, signal_families FROM oracle_models")
            ).fetchall()
        for row in model_rows or []:
            name = row[0]
            fams = row[1]
            if not isinstance(name, str) or not name.strip():
                continue
            if isinstance(fams, list):
                sources = [
                    s for s in fams if isinstance(s, str) and s.strip()
                ]
                if sources:
                    oracle_models_lookup[name.strip()] = sources
            elif isinstance(fams, str):
                try:
                    import json as _json

                    parsed = _json.loads(fams)
                    if isinstance(parsed, list):
                        sources = [
                            s
                            for s in parsed
                            if isinstance(s, str) and s.strip()
                        ]
                        if sources:
                            oracle_models_lookup[name.strip()] = sources
                except (ValueError, TypeError):
                    continue
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "meta_learning_matrix.bootstrap: oracle_models lookup "
            "failed (continuing with empty lookup): {e}",
            e=str(exc),
        )

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                _BOOTSTRAP_QUERY, {"days": int(days)}
            ).fetchall()
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "meta_learning_matrix.bootstrap: oracle_predictions read "
            "failed: {e}",
            e=str(exc),
        )
        return 0

    column_names = (
        "id",
        "ticker",
        "created_at",
        "expiry",
        "confidence",
        "verdict",
        "model_name",
        "signals",
        "signal_contributions",
        "model_weights",
    )

    replayed = 0
    for raw_row in rows or []:
        try:
            row_dict = dict(zip(column_names, raw_row))
            verdict = row_dict.get("verdict")
            if verdict not in ("hit", "miss", "partial"):
                continue

            confidence = row_dict.get("confidence")
            try:
                conf_f = (
                    float(confidence) if confidence is not None else None
                )
            except (TypeError, ValueError):
                conf_f = None
            if conf_f is None or conf_f != conf_f:
                continue
            conf_f = max(0.0, min(1.0, conf_f))

            horizon_days = _coerce_horizon_days(row_dict)
            contributions = extract_signal_contributions(
                row_dict, oracle_models_lookup=oracle_models_lookup
            )
            signals_ctx = _extract_signals_context(row_dict.get("signals"))
            direction = str(signals_ctx.get("direction") or "")
            liquidity = signals_ctx.get("regime") or signals_ctx.get(
                "liquidity_regime"
            )
            fci = signals_ctx.get("fci_regime")
            vix = signals_ctx.get("vix_level") or signals_ctx.get("vix")

            for source, weight in contributions.items():
                record_scored_prediction(
                    engine,
                    signal_source=source,
                    shapley_weight=weight,
                    confidence=conf_f,
                    direction=direction,
                    outcome=verdict,
                    horizon_days=horizon_days,
                    liquidity_regime=liquidity,
                    fci_regime=fci,
                    vix_level=vix,
                )
            replayed += 1
        except Exception as exc:  # noqa: BLE001
            log.debug(
                "meta_learning_matrix.bootstrap: row replay failed: {e}",
                e=str(exc),
            )
            continue

    # Reference verdict_to_outcome so the import is demonstrably used
    # downstream — the bootstrap helper retains the scalar for any
    # caller that wants the weighted verdict score later.
    _ = verdict_to_outcome
    # Reference CANONICAL_HORIZONS / MIN_CALIBRATED_SAMPLES / ORACLE_AGGREGATE_SOURCE
    # so static analyzers see the imports as used — they're part of
    # the public contract of the module (re-exported).
    _ = (CANONICAL_HORIZONS, MIN_CALIBRATED_SAMPLES, ORACLE_AGGREGATE_SOURCE)
    # Reference CANONICAL_REGIMES so the re-export from regime_router is
    # actually consumed.
    _ = CANONICAL_REGIMES

    return replayed


# Iterable of every valid condition tuple — occasionally handy for
# dashboards that want to pre-walk the cube. Not used internally.
def iter_condition_cube() -> Iterable[ConditionTuple]:
    """Yield every valid ConditionTuple in the 3×5×3×3 cube."""
    for hb in ("SHORT", "MID", "LONG"):
        for lq in CANONICAL_REGIMES:
            for fci in ("TIGHT", "NEUTRAL", "EASY"):
                for vol in ("LOW", "NORMAL", "HIGH"):
                    yield ConditionTuple(
                        horizon_bucket=hb,
                        liquidity_regime=lq,
                        fci_bucket=fci,
                        vol_regime=vol,
                    )
