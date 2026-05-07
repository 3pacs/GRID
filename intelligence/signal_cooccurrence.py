"""Pairwise signal co-occurrence tracker.

Measures whether two signals firing together produces more-accurate
(compounding) or less-accurate (redundant) predictions than the
independence baseline. The aggregate conviction formula implicitly
assumes independence; this module measures the deviation empirically
and exposes a lift multiplier the decision gateway can consume.

Design notes
------------
The existing per-signal Brier tracker calibrates each signal in
isolation. When `compute_aggregate_conviction` combines multiple
signals, it implicitly assumes they are independent sources of
evidence. They are not:

    - A pair of signals measuring the same underlying cycle from two
      angles (e.g. ``semi_book_to_bill`` and ``taiwan_exports``) is
      *redundant*; their joint hit rate is LOWER than the product of
      the marginals would predict, so the aggregate should be
      dampened.
    - A pair of signals that are genuinely orthogonal (e.g. a credit
      lever and an options-flow signal) has a joint hit rate HIGHER
      than the product of marginals; the aggregate should be boosted.

The lift metric used here is

    lift = joint_hit_rate / (marginal_a * marginal_b)

which is the classical independence baseline ratio. A lift of 1.0 means
the signals are empirically independent. Lift > 1.0 means the pair
compounds; lift < 1.0 means the pair is redundant.

Clamps
------
Lift is clamped to ``[MIN_LIFT_MULTIPLIER, MAX_LIFT_MULTIPLIER]`` to
prevent runaway amplification from noisy small-sample pairs. Pairs with
fewer than ``MIN_COOCCURRENCE_SAMPLES`` joint observations are flagged
``is_calibrated=False`` and contribute a neutral lift of 1.0.

Canonical pair ordering
-----------------------
Every pair is stored with ``signal_a < signal_b`` (lexicographic) so
the row ``("taiwan_exports", "semi_book_to_bill")`` collapses to
``("semi_book_to_bill", "taiwan_exports")``. This makes the primary
key unique and cuts storage in half.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from itertools import combinations
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Constants ──────────────────────────────────────────────────────────────

MIN_COOCCURRENCE_SAMPLES: int = 15
"""Minimum joint-firing samples before a pair's lift is trusted."""

MAX_LIFT_MULTIPLIER: float = 1.25
"""Hard ceiling on compounding lift to prevent runaway amplification."""

MIN_LIFT_MULTIPLIER: float = 0.75
"""Hard floor on redundancy penalty."""

COOCCURRENCE_MIN_SHAPLEY: float = 0.1
"""A signal must contribute at least 10% of the Shapley weight to
count as 'firing' for co-occurrence tracking."""

_LIFT_ZERO_EPSILON: float = 1e-9
"""Guard against divide-by-zero when the independence baseline is 0."""


# ── Dataclasses ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SignalPair:
    """Canonical (alphabetically ordered) signal pair.

    Construct via :func:`canonical_pair` to guarantee ordering. The
    frozen dataclass hash makes this usable as a dict key.
    """

    signal_a: str
    signal_b: str

    def to_tuple(self) -> tuple[str, str]:
        return (self.signal_a, self.signal_b)


@dataclass(frozen=True)
class CooccurrenceStats:
    """Full statistics for a single signal pair."""

    pair: SignalPair
    sample_count: int
    joint_hits: int
    joint_misses: int
    joint_hit_rate: float
    marginal_hit_rate_a: float
    marginal_hit_rate_b: float
    independence_baseline: float
    lift: float
    is_calibrated: bool
    last_updated: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["pair"] = {
            "signal_a": self.pair.signal_a,
            "signal_b": self.pair.signal_b,
        }
        d["last_updated"] = (
            self.last_updated.isoformat() if self.last_updated else None
        )
        return d


# ── DDL ────────────────────────────────────────────────────────────────────

_COOCCURRENCE_DDL: str = """
CREATE TABLE IF NOT EXISTS signal_cooccurrence_history (
    signal_a                TEXT NOT NULL,
    signal_b                TEXT NOT NULL,
    sample_count            INTEGER NOT NULL DEFAULT 0,
    joint_hits              INTEGER NOT NULL DEFAULT 0,
    joint_misses            INTEGER NOT NULL DEFAULT 0,
    marginal_hits_a         INTEGER NOT NULL DEFAULT 0,
    marginal_hits_b         INTEGER NOT NULL DEFAULT 0,
    marginal_fires_a        INTEGER NOT NULL DEFAULT 0,
    marginal_fires_b        INTEGER NOT NULL DEFAULT 0,
    last_updated            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (signal_a, signal_b)
);
"""

_COOCCURRENCE_INDEX_A: str = (
    "CREATE INDEX IF NOT EXISTS idx_sch_pair_a "
    "ON signal_cooccurrence_history (signal_a);"
)

_COOCCURRENCE_INDEX_B: str = (
    "CREATE INDEX IF NOT EXISTS idx_sch_pair_b "
    "ON signal_cooccurrence_history (signal_b);"
)


def ensure_cooccurrence_table(engine: Engine) -> None:
    """Create ``signal_cooccurrence_history`` + indices if missing.

    Idempotent. Swallows errors so first-run failures do not crash the
    caller (mirrors the per_signal_brier ensure pattern).
    """

    try:
        with engine.begin() as conn:
            conn.execute(text(_COOCCURRENCE_DDL))
            conn.execute(text(_COOCCURRENCE_INDEX_A))
            conn.execute(text(_COOCCURRENCE_INDEX_B))
    except Exception as exc:  # pragma: no cover - defensive
        log.warning("ensure_cooccurrence_table failed: {e}", e=exc)


# ── Pure helpers ───────────────────────────────────────────────────────────


def canonical_pair(a: str, b: str) -> SignalPair:
    """Return an alphabetically ordered :class:`SignalPair`.

    Ensures ``("b", "a")`` and ``("a", "b")`` both collapse to the same
    canonical row. Self-pairs (``a == b``) are returned as-is but
    callers typically filter them out before insertion.
    """

    if a <= b:
        return SignalPair(signal_a=a, signal_b=b)
    return SignalPair(signal_a=b, signal_b=a)


def compute_independence_baseline(
    marginal_a: float, marginal_b: float
) -> float:
    """Independence baseline = product of marginal hit rates."""

    return max(0.0, float(marginal_a)) * max(0.0, float(marginal_b))


def compute_lift(
    joint_hit_rate: float, independence_baseline: float
) -> float:
    """Lift = joint / baseline, clamped to the lift range.

    If the baseline is effectively zero, we default to
    ``MAX_LIFT_MULTIPLIER`` — the joint rate cannot be compared to an
    undefined baseline, so we optimistically expose the ceiling and let
    the ``is_calibrated`` flag decide whether the number is trusted.
    """

    if independence_baseline <= _LIFT_ZERO_EPSILON:
        return MAX_LIFT_MULTIPLIER

    raw = float(joint_hit_rate) / max(
        float(independence_baseline), _LIFT_ZERO_EPSILON
    )
    return max(MIN_LIFT_MULTIPLIER, min(MAX_LIFT_MULTIPLIER, raw))


def get_firing_signals(
    signal_contributions: dict[str, float],
) -> set[str]:
    """Filter to signals whose Shapley weight >= COOCCURRENCE_MIN_SHAPLEY.

    Signals below the threshold are considered background noise and do
    not participate in co-occurrence tracking.
    """

    if not signal_contributions:
        return set()
    return {
        name
        for name, weight in signal_contributions.items()
        if weight is not None and float(weight) >= COOCCURRENCE_MIN_SHAPLEY
    }


def compute_pair_lift_multiplier(
    firing_signals: set[str],
    stats_lookup: dict[SignalPair, CooccurrenceStats],
) -> float:
    """Average lift across every calibrated pair among firing signals.

    For N firing signals there are ``N*(N-1)/2`` pairs. We walk all of
    them, skip uncalibrated ones, and return the mean lift. When no
    calibrated pair exists we return a neutral ``1.0``.
    """

    if len(firing_signals) < 2:
        return 1.0

    lifts: list[float] = []
    for a, b in combinations(sorted(firing_signals), 2):
        pair = canonical_pair(a, b)
        stats = stats_lookup.get(pair)
        if stats is None or not stats.is_calibrated:
            continue
        lifts.append(stats.lift)

    if not lifts:
        return 1.0

    avg = sum(lifts) / len(lifts)
    return max(MIN_LIFT_MULTIPLIER, min(MAX_LIFT_MULTIPLIER, avg))


# ── Stats construction helper ──────────────────────────────────────────────


def _row_to_mapping(row: Any) -> dict[str, Any]:
    """Normalize a row to a plain dict."""

    if hasattr(row, "_mapping"):
        return dict(row._mapping)
    if isinstance(row, dict):
        return dict(row)
    keys = [
        "signal_a",
        "signal_b",
        "sample_count",
        "joint_hits",
        "joint_misses",
        "marginal_hits_a",
        "marginal_hits_b",
        "marginal_fires_a",
        "marginal_fires_b",
        "last_updated",
    ]
    return {k: row[i] for i, k in enumerate(keys)}


def _build_stats(
    row: Any,
    marginal_a_override: float | None = None,
    marginal_b_override: float | None = None,
) -> CooccurrenceStats:
    """Build :class:`CooccurrenceStats` from a pair row.

    ``marginal_*_override`` lets callers inject globally-aggregated
    marginal hit rates (computed across every pair containing that
    signal) rather than the per-row fallback. Without the override we
    fall back to the per-row marginal, which is only well-defined when
    the signal appears in many pairs.
    """

    m = _row_to_mapping(row)

    sample_count = int(m.get("sample_count", 0) or 0)
    joint_hits = int(m.get("joint_hits", 0) or 0)
    joint_misses = int(m.get("joint_misses", 0) or 0)
    fires_a = int(m.get("marginal_fires_a", 0) or 0)
    fires_b = int(m.get("marginal_fires_b", 0) or 0)
    hits_a = int(m.get("marginal_hits_a", 0) or 0)
    hits_b = int(m.get("marginal_hits_b", 0) or 0)

    joint_total = max(1, joint_hits + joint_misses)
    joint_hit_rate = joint_hits / joint_total

    marginal_a = (
        marginal_a_override
        if marginal_a_override is not None
        else hits_a / max(1, fires_a)
    )
    marginal_b = (
        marginal_b_override
        if marginal_b_override is not None
        else hits_b / max(1, fires_b)
    )

    baseline = compute_independence_baseline(marginal_a, marginal_b)
    lift_val = compute_lift(joint_hit_rate, baseline)

    is_calibrated = sample_count >= MIN_COOCCURRENCE_SAMPLES

    return CooccurrenceStats(
        pair=SignalPair(
            signal_a=str(m["signal_a"]),
            signal_b=str(m["signal_b"]),
        ),
        sample_count=sample_count,
        joint_hits=joint_hits,
        joint_misses=joint_misses,
        joint_hit_rate=joint_hit_rate,
        marginal_hit_rate_a=marginal_a,
        marginal_hit_rate_b=marginal_b,
        independence_baseline=baseline,
        lift=lift_val,
        is_calibrated=is_calibrated,
        last_updated=m.get("last_updated"),
    )


# ── DB write path ──────────────────────────────────────────────────────────


_UPSERT_PAIR_SQL: str = """
INSERT INTO signal_cooccurrence_history (
    signal_a, signal_b,
    sample_count, joint_hits, joint_misses,
    marginal_hits_a, marginal_hits_b,
    marginal_fires_a, marginal_fires_b,
    last_updated
) VALUES (
    :signal_a, :signal_b,
    1, :hit, :miss,
    :hit, :hit, 1, 1,
    NOW()
)
ON CONFLICT (signal_a, signal_b) DO UPDATE SET
    sample_count     = signal_cooccurrence_history.sample_count + 1,
    joint_hits       = signal_cooccurrence_history.joint_hits + :hit,
    joint_misses     = signal_cooccurrence_history.joint_misses + :miss,
    marginal_hits_a  = signal_cooccurrence_history.marginal_hits_a + :hit,
    marginal_hits_b  = signal_cooccurrence_history.marginal_hits_b + :hit,
    marginal_fires_a = signal_cooccurrence_history.marginal_fires_a + 1,
    marginal_fires_b = signal_cooccurrence_history.marginal_fires_b + 1,
    last_updated     = NOW();
"""


def record_joint_prediction(
    engine: Engine,
    *,
    outcome: float,
    signal_contributions: dict[str, float],
) -> int:
    """Record one scored prediction against the co-occurrence matrix.

    For every pair of firing signals (Shapley >= COOCCURRENCE_MIN_SHAPLEY)
    we upsert a row incrementing the joint counters. ``outcome >= 0.5``
    counts as a hit; anything else is a miss (matches the
    ``verdict → outcome`` mapping in :mod:`oracle.calibration`).

    Returns the number of pair rows touched. Returns ``0`` on any DB
    failure — the caller should never crash because calibration failed.
    """

    firing = get_firing_signals(signal_contributions)
    if len(firing) < 2:
        return 0

    hit = 1 if float(outcome) >= 0.5 else 0
    miss = 1 - hit

    pairs = [canonical_pair(a, b) for a, b in combinations(sorted(firing), 2)]

    touched = 0
    try:
        with engine.begin() as conn:
            for pair in pairs:
                if pair.signal_a == pair.signal_b:
                    # Self-pair: skip — meaningless for co-occurrence.
                    continue
                conn.execute(
                    text(_UPSERT_PAIR_SQL),
                    {
                        "signal_a": pair.signal_a,
                        "signal_b": pair.signal_b,
                        "hit": hit,
                        "miss": miss,
                    },
                )
                touched += 1
    except Exception as exc:
        log.warning(
            "record_joint_prediction failed ({n} pairs): {e}",
            n=len(pairs),
            e=exc,
        )
        return 0

    return touched


# ── DB read path ───────────────────────────────────────────────────────────


_SELECT_PAIR_SQL: str = """
SELECT signal_a, signal_b,
       sample_count, joint_hits, joint_misses,
       marginal_hits_a, marginal_hits_b,
       marginal_fires_a, marginal_fires_b,
       last_updated
FROM signal_cooccurrence_history
WHERE signal_a = :a AND signal_b = :b
"""

_SELECT_BY_SIGNAL_SQL: str = """
SELECT signal_a, signal_b,
       sample_count, joint_hits, joint_misses,
       marginal_hits_a, marginal_hits_b,
       marginal_fires_a, marginal_fires_b,
       last_updated
FROM signal_cooccurrence_history
WHERE signal_a = :s OR signal_b = :s
"""


def _compute_global_marginal(engine: Engine, signal: str) -> float | None:
    """Aggregate ``joint_hits`` / ``sample_count`` across every pair
    containing ``signal``.

    This is the *empirical* marginal hit rate for the signal — the rate
    at which predictions *it participated in* turned out right, taken
    across the full pair catalogue. Returns ``None`` when no rows
    contain the signal.
    """

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(_SELECT_BY_SIGNAL_SQL), {"s": signal}
            ).fetchall()
    except Exception as exc:
        log.warning("_compute_global_marginal failed: {e}", e=exc)
        return None

    total_hits = 0
    total_samples = 0
    for r in rows:
        m = _row_to_mapping(r)
        total_hits += int(m.get("joint_hits", 0) or 0)
        total_samples += int(m.get("sample_count", 0) or 0)

    if total_samples <= 0:
        return None
    return total_hits / total_samples


def get_cooccurrence_stats(
    engine: Engine, signal_a: str, signal_b: str
) -> CooccurrenceStats | None:
    """Read a single canonicalized pair. Returns ``None`` if absent.

    Marginal hit rates are computed by aggregating every pair row
    containing each signal so the independence baseline reflects the
    signal's broader calibration, not just this pair's counters.
    """

    pair = canonical_pair(signal_a, signal_b)
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(_SELECT_PAIR_SQL),
                {"a": pair.signal_a, "b": pair.signal_b},
            ).fetchone()
    except Exception as exc:
        log.warning("get_cooccurrence_stats failed: {e}", e=exc)
        return None

    if row is None:
        return None

    marginal_a = _compute_global_marginal(engine, pair.signal_a)
    marginal_b = _compute_global_marginal(engine, pair.signal_b)
    return _build_stats(
        row,
        marginal_a_override=marginal_a,
        marginal_b_override=marginal_b,
    )


def get_stats_for_signal(
    engine: Engine, signal: str
) -> list[CooccurrenceStats]:
    """All pairs containing ``signal`` in either position.

    Relies on the ``idx_sch_pair_a`` and ``idx_sch_pair_b`` indices so
    the ``OR`` scan stays O(log n). Uses the globally-aggregated
    marginal for ``signal`` in every returned stats object.
    """

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(_SELECT_BY_SIGNAL_SQL), {"s": signal}
            ).fetchall()
    except Exception as exc:
        log.warning("get_stats_for_signal failed: {e}", e=exc)
        return []

    # Cache marginals so we don't re-query per row.
    marginal_cache: dict[str, float | None] = {
        signal: _compute_global_marginal(engine, signal)
    }
    out: list[CooccurrenceStats] = []
    for r in rows:
        m = _row_to_mapping(r)
        other = (
            m["signal_b"] if m["signal_a"] == signal else m["signal_a"]
        )
        if other not in marginal_cache:
            marginal_cache[other] = _compute_global_marginal(engine, other)
        if m["signal_a"] == signal:
            ma = marginal_cache[signal]
            mb = marginal_cache[other]
        else:
            ma = marginal_cache[other]
            mb = marginal_cache[signal]
        out.append(
            _build_stats(
                r, marginal_a_override=ma, marginal_b_override=mb
            )
        )
    return out


# ── Main consumer API ──────────────────────────────────────────────────────


def get_lift_multiplier(
    engine: Engine, signal_contributions: dict[str, float]
) -> float:
    """Return the aggregated lift multiplier for the firing signals.

    This is the function the decision gateway and
    ``compute_aggregate_conviction`` path should call. It:

    1. Filters ``signal_contributions`` to the firing set via
       :func:`get_firing_signals`.
    2. Looks up every pair's calibrated stats from the DB.
    3. Averages lift across calibrated pairs using
       :func:`compute_pair_lift_multiplier`.

    Returns ``1.0`` on any failure so the caller's conviction math is
    left untouched when calibration data is missing.
    """

    try:
        firing = get_firing_signals(signal_contributions)
        if len(firing) < 2:
            return 1.0

        stats_lookup: dict[SignalPair, CooccurrenceStats] = {}
        for a, b in combinations(sorted(firing), 2):
            if a == b:
                continue
            stats = get_cooccurrence_stats(engine, a, b)
            if stats is not None:
                stats_lookup[stats.pair] = stats

        return compute_pair_lift_multiplier(firing, stats_lookup)
    except Exception as exc:
        log.warning("get_lift_multiplier failed: {e}", e=exc)
        return 1.0


# ── Bootstrap replay ───────────────────────────────────────────────────────


_ORACLE_REPLAY_SQL: str = """
SELECT id, verdict, signals, signal_strength, confidence, model_weights
FROM oracle_predictions
WHERE verdict IN ('hit', 'miss', 'partial')
  AND created_at >= NOW() - (:days || ' days')::INTERVAL
ORDER BY created_at ASC
"""


def _verdict_to_outcome(verdict: str | None) -> float:
    """Mirror :mod:`oracle.calibration` verdict → outcome mapping."""

    if verdict == "hit":
        return 1.0
    if verdict == "partial":
        return 0.5
    return 0.0


def _extract_signal_contributions(row: Any) -> dict[str, float]:
    """3-layer fallback to pull signal contributions from an oracle row.

    Mirrors ``scripts/bootstrap_per_signal_brier.py``:

    1. Prefer a top-level ``signal_contributions`` (if present on the
       row or nested in ``signals``).
    2. Fall back to ``signals`` JSONB — if it's a dict of
       ``{name: weight}`` use it directly; if it's a list of strings,
       distribute the ``signal_strength`` equally across them.
    3. Final fallback: ``model_weights`` (if the oracle stored the
       per-model Shapley weights instead of per-signal).
    """

    mapping = (
        dict(row._mapping) if hasattr(row, "_mapping") else dict(row)
    )
    signals_field = mapping.get("signals")
    signal_strength = float(mapping.get("signal_strength") or 0.0)

    # Layer 1: explicit signal_contributions (nested in signals dict)
    if isinstance(signals_field, dict):
        nested = signals_field.get("signal_contributions")
        if isinstance(nested, dict) and nested:
            return {str(k): float(v) for k, v in nested.items()}

        # Layer 2a: signals is already a {name: weight} dict
        numeric = {
            str(k): float(v)
            for k, v in signals_field.items()
            if isinstance(v, (int, float))
        }
        if numeric:
            return numeric

    # Layer 2b: signals is a list of names — distribute strength evenly
    if isinstance(signals_field, list) and signals_field:
        names = [str(s) for s in signals_field if s]
        if names:
            w = (
                max(signal_strength, COOCCURRENCE_MIN_SHAPLEY)
                / len(names)
            )
            # Ensure each distributed weight crosses the firing floor
            # for short signal lists, otherwise fall back to a flat
            # assignment at the Shapley floor so pairs can be tracked.
            w = max(w, COOCCURRENCE_MIN_SHAPLEY)
            return {name: w for name in names}

    # Layer 3: model_weights
    model_weights = mapping.get("model_weights")
    if isinstance(model_weights, dict) and model_weights:
        return {str(k): float(v) for k, v in model_weights.items()}

    return {}


def bootstrap_from_oracle_predictions(
    engine: Engine, days: int = 365, limit: int | None = None
) -> dict[str, Any]:
    """One-shot replay of historical oracle_predictions.

    Walks every scored (``hit``/``miss``/``partial``) prediction in the
    last ``days`` days, extracts signal contributions via the 3-layer
    cascade in :func:`_extract_signal_contributions`, and calls
    :func:`record_joint_prediction` for each row.

    Returns a summary dict with counters:

    ``rows_scanned``    — total oracle_predictions rows walked
    ``rows_used``       — rows with >= 2 firing signals
    ``pairs_updated``   — total pair-row upserts performed
    ``errors``          — count of rows that hit an exception
    """

    summary: dict[str, Any] = {
        "rows_scanned": 0,
        "rows_used": 0,
        "pairs_updated": 0,
        "errors": 0,
    }

    ensure_cooccurrence_table(engine)

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(_ORACLE_REPLAY_SQL), {"days": str(int(days))}
            ).fetchall()
    except Exception as exc:
        log.warning("bootstrap_from_oracle_predictions query failed: {e}", e=exc)
        return summary

    if limit is not None:
        rows = rows[: int(limit)]

    for row in rows:
        summary["rows_scanned"] += 1
        try:
            mapping = (
                dict(row._mapping) if hasattr(row, "_mapping") else dict(row)
            )
            verdict = mapping.get("verdict")
            contributions = _extract_signal_contributions(row)
            if len(get_firing_signals(contributions)) < 2:
                continue

            outcome = _verdict_to_outcome(verdict)
            pairs = record_joint_prediction(
                engine,
                outcome=outcome,
                signal_contributions=contributions,
            )
            if pairs > 0:
                summary["rows_used"] += 1
                summary["pairs_updated"] += pairs
        except Exception as exc:
            summary["errors"] += 1
            log.debug("bootstrap row failed: {e}", e=exc)

    log.info(
        "signal_cooccurrence bootstrap: scanned={s} used={u} pairs={p} errors={e}",
        s=summary["rows_scanned"],
        u=summary["rows_used"],
        p=summary["pairs_updated"],
        e=summary["errors"],
    )
    return summary


__all__ = [
    "MIN_COOCCURRENCE_SAMPLES",
    "MAX_LIFT_MULTIPLIER",
    "MIN_LIFT_MULTIPLIER",
    "COOCCURRENCE_MIN_SHAPLEY",
    "SignalPair",
    "CooccurrenceStats",
    "canonical_pair",
    "compute_independence_baseline",
    "compute_lift",
    "get_firing_signals",
    "compute_pair_lift_multiplier",
    "ensure_cooccurrence_table",
    "record_joint_prediction",
    "get_cooccurrence_stats",
    "get_stats_for_signal",
    "get_lift_multiplier",
    "bootstrap_from_oracle_predictions",
]
