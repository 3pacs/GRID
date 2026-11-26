"""
Regime-conditional calibration sibling to features/per_signal_brier.py.

Tracks Brier per (signal_source, horizon_days, regime) because signals
historically calibrated in one regime (EXPANSION) may be miscalibrated in
another (CRISIS). Graceful fallback to the non-regime scorecard when
regime history is thin so callers never get None in production.

The motivating example
----------------------

A signal like ``flow_momentum`` might score ``0.08 Brier`` in EXPANSION
(well calibrated, near-optimal edge) but ``0.24 Brier`` in CRISIS
(basically coin-flip noise). ``features/per_signal_brier.py`` averages
those two histories into a single ``0.14`` bucket → conviction weight
collapses to ≈ 0.8 in *both* regimes — which is **wrong in both**:

  - EXPANSION: dampens a genuinely edgy signal (should be 1.5×)
  - CRISIS:    over-trusts a noise signal (should be 0.0×)

This module adds the missing axis. The oracle already tags every
prediction with the ``regime`` field populated by the ALPHA-5 liquidity
classifier (CRISIS / TIGHTENING / NEUTRAL / EXPANSION /
EXPANSION_STRONG, see ``oracle/engine.py`` EnsemblePrediction). We reuse
that axis as a DB partition key and reuse the Welford update path from
``record_scored_prediction`` so the persistence story stays identical.

Sibling semantics
-----------------

This module is deliberately NOT a modification of
``features/per_signal_brier.py``. Instead:

- ``SignalScorecard``, ``compute_conviction_weight``, ``CANONICAL_HORIZONS``,
  ``MIN_CALIBRATED_SAMPLES``, and ``_canonical_horizon`` are IMPORTED from
  per_signal_brier. Zero duplication of the scorecard shape or the
  conviction-weight curve.
- The table ``regime_conditional_brier_history`` lives alongside
  ``per_signal_brier_history``, not in place of it. Both are written on
  every scored prediction via the provenance consumer.
- The composite fallback API
  ``get_scorecard_with_regime_fallback`` delegates to
  ``features.per_signal_brier.get_signal_scorecard`` when the
  regime-conditional row is missing or thin (below ``MIN_REGIME_SAMPLES``).
  This guarantees the signal_provenance consumer never has to branch on
  "regime history or not" — it just calls one function.

Schema
------

``regime_conditional_brier_history`` (created on first write):

    signal_source    TEXT     — adapter source_module (e.g. 'jodi_oil')
    horizon_days     INTEGER  — 1 / 7 / 30 / 90 (canonical horizon)
    regime           TEXT     — CRISIS / TIGHTENING / NEUTRAL / EXPANSION /
                                EXPANSION_STRONG
    scored_count     INTEGER  — predictions this (source, horizon, regime)
                                bucket has contributed to
    running_brier    DOUBLE   — Welford mean of (pred - outcome)^2 weighted
                                by Shapley share
    running_ece      DOUBLE   — Welford mean of |pred - outcome| weighted
                                by Shapley share
    hit_count        INTEGER  — predictions this bucket got directionally correct
    last_updated     TIMESTAMPTZ
    PRIMARY KEY (signal_source, horizon_days, regime)

Functions
---------

- ``ensure_regime_brier_table`` — create the sibling table. Idempotent.
- ``record_scored_prediction`` — update the per-regime running buckets.
  Mirrors the signature of per_signal_brier.record_scored_prediction,
  with an added ``regime`` kwarg.
- ``get_regime_conditional_scorecard`` — read one (source, horizon, regime)
  scorecard. Returns None when the row is missing OR when the sample
  count is below MIN_REGIME_SAMPLES (forces caller to fall back).
- ``get_scorecard_with_regime_fallback`` — production entry point.
  Always returns a SignalScorecard (or None if the non-regime scorecard
  is also missing). Callers never have to care about the fallback
  semantics — that's the whole point of the module.
- ``rank_signals_by_regime`` — best-first list of calibrated signals for
  a given regime.
- ``bootstrap_from_oracle_predictions`` — one-shot replay helper that
  walks ``oracle_predictions`` and seeds the regime table from the
  ``regime`` column already populated by the oracle.
"""

from __future__ import annotations

import math
import threading
from datetime import datetime, timezone
from typing import Any, Sequence

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# IMPORT from the sibling module — never reimplement the scorecard shape
# or the conviction curve. If either evolves in per_signal_brier, this
# module inherits the change for free.
from features.per_signal_brier import (  # noqa: F401
    CANONICAL_HORIZONS,
    MIN_CALIBRATED_SAMPLES,
    MIN_EFFECTIVE_WEIGHT_SUM,
    SignalScorecard,
    _canonical_horizon,
    compute_conviction_weight,
    get_signal_scorecard,
)
from store.options_provenance import VISIBLE_ORACLE_PREDICTIONS


# ── Constants ─────────────────────────────────────────────────────────────

# The 5-state liquidity regime output from the ALPHA-5 classifier. The
# oracle writes exactly these values into EnsemblePrediction.regime (see
# oracle/engine.py around line 2545). Anything else is normalized to
# "NEUTRAL" by ``_canonical_regime`` so the DB key space stays bounded.
CANONICAL_REGIMES: tuple[str, ...] = (
    "CRISIS",
    "TIGHTENING",
    "NEUTRAL",
    "EXPANSION",
    "EXPANSION_STRONG",
)

# Minimum per-(source, horizon, regime) sample count before the regime
# scorecard is trusted. Below this, ``get_regime_conditional_scorecard``
# returns None and the composite fallback API routes the caller down to
# the non-regime per_signal_brier scorecard.
#
# 10 chosen deliberately: per_signal_brier.MIN_CALIBRATED_SAMPLES is 20;
# splitting across 5 regimes at the same threshold would push per-regime
# warm-up to ~25 weeks of 6h oracle cycles which defeats the point. 10 is
# the smallest size where binomial hit-rate variance drops below ~0.16 —
# the "minimum stratified-calibration sample" rule of thumb.
MIN_REGIME_SAMPLES: int = 10

# Which table answered a ``get_scorecard_with_regime_source`` lookup.
# "regime": the regime-conditional row, warm enough to trust.
# "flat_fallback": a regime was given, but its row was missing or thin, so
#     the flat per-signal scorecard answered.
# "flat": no regime was given, so only the flat scorecard was read.
CALIBRATION_SOURCE_REGIME: str = "regime"
CALIBRATION_SOURCE_FLAT_FALLBACK: str = "flat_fallback"
CALIBRATION_SOURCE_FLAT: str = "flat"


# ── Table DDL ─────────────────────────────────────────────────────────────


_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS regime_conditional_brier_history (
    signal_source    TEXT        NOT NULL,
    horizon_days     INTEGER     NOT NULL,
    regime           TEXT        NOT NULL,
    scored_count     INTEGER     NOT NULL DEFAULT 0,
    running_brier    DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    running_ece      DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    hit_count        INTEGER     NOT NULL DEFAULT 0,
    last_updated     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (signal_source, horizon_days, regime)
);

CREATE INDEX IF NOT EXISTS idx_rcbh_lookup
    ON regime_conditional_brier_history (signal_source, horizon_days, regime);
CREATE INDEX IF NOT EXISTS idx_rcbh_regime_scan
    ON regime_conditional_brier_history (regime, running_brier ASC);
"""


def ensure_regime_brier_table(engine: Engine) -> None:
    """Create ``regime_conditional_brier_history`` if it doesn't exist.

    Idempotent — safe to call on every write. Mirrors the pattern used
    by ``features.per_signal_brier.ensure_tables``. Never raises.
    """
    try:
        with engine.begin() as conn:
            for stmt in _TABLE_SQL.strip().split(";"):
                s = stmt.strip()
                if s:
                    conn.execute(text(s))
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "regime_conditional_brier: ensure_regime_brier_table failed: {e}",
            e=str(exc),
        )


# ── Pure helpers ──────────────────────────────────────────────────────────


def _canonical_regime(regime: str | None) -> str:
    """Normalize an arbitrary regime string to one of ``CANONICAL_REGIMES``.

    Rules:
      - ``None`` or empty → ``"NEUTRAL"``
      - Case-insensitive match against canonical names after
        ``strip().upper()`` → return the canonical form
      - Alias mapping for a few historical spellings emitted by early
        versions of the oracle (``EXPANSIONSTRONG`` without underscore,
        ``TIGHT``, etc.) → closest canonical bucket
      - Anything else → ``"NEUTRAL"`` (safe default matching the oracle's
        cold-start behaviour)

    Pure function — no DB, fully deterministic.
    """
    if regime is None:
        return "NEUTRAL"
    try:
        cleaned = str(regime).strip().upper().replace("-", "_").replace(" ", "_")
    except Exception:  # noqa: BLE001
        return "NEUTRAL"
    if not cleaned:
        return "NEUTRAL"
    if cleaned in CANONICAL_REGIMES:
        return cleaned
    # Historical aliases / common abbreviations the oracle has emitted.
    aliases: dict[str, str] = {
        "EXPANSIONSTRONG": "EXPANSION_STRONG",
        "STRONG_EXPANSION": "EXPANSION_STRONG",
        "EXPANSION_PLUS": "EXPANSION_STRONG",
        "TIGHT": "TIGHTENING",
        "TIGHTEN": "TIGHTENING",
        "CONTRACT": "CRISIS",
        "CONTRACTION": "CRISIS",
        "STRESS": "CRISIS",
        "NORMAL": "NEUTRAL",
        "MID": "NEUTRAL",
        "MIDDLE": "NEUTRAL",
    }
    return aliases.get(cleaned, "NEUTRAL")


def _row_to_scorecard(
    row: Any,
    signal_source: str,
    horizon_days: int,
) -> SignalScorecard:
    """Reconstruct a SignalScorecard from a DB row.

    The row tuple is ``(scored_count, running_brier, running_ece,
    hit_count, last_updated)`` — the caller already knows the
    signal_source and horizon_days from the query parameters, which is
    why they're passed in explicitly rather than selected from the row.

    Conservative read-time floor: ``regime_conditional_brier_history`` has
    no ``weight_sum`` (Σw) or JSON/metadata column on the current schema, so
    read-time enforces ``scored_count < MIN_CALIBRATED_SAMPLES (20)`` →
    ``is_calibrated=False`` and neutral ``conviction_weight=1.0`` (even when
    ``MIN_REGIME_SAMPLES (10) <= scored_count < 20``). The bootstrap writer
    enforces ``Σw >= MIN_EFFECTIVE_WEIGHT_SUM (5.0)`` fail-closed at write
    time, and replaces the whole table in one transaction, so a row it rejects
    or no longer produces is retired. Rows from before that cutover stay
    readable until the first replacement run.
    """
    count = int(row[0] or 0)
    brier = float(row[1] or 0.0)
    ece = float(row[2] or 0.0)
    hits = int(row[3] or 0)
    last_updated = row[4] if len(row) > 4 else None
    hit_rate = (hits / count) if count > 0 else 0.0
    is_calibrated = count >= MIN_CALIBRATED_SAMPLES
    conviction = (
        compute_conviction_weight(brier, count)
        if is_calibrated
        else 1.0
    )
    return SignalScorecard(
        signal_source=signal_source,
        horizon_days=horizon_days,
        scored_count=count,
        running_brier=brier,
        running_ece=ece,
        hit_rate=hit_rate,
        last_updated=last_updated,
        is_calibrated=is_calibrated,
        conviction_weight=conviction,
        weight_sum=None,
    )


# ── Live-skip tracking (no weight_sum column on current schema) ───────────

_SKIPPED_LIVE_UPDATES_LOCK = threading.Lock()
_SKIPPED_LIVE_UPDATE_COUNTS: dict[tuple[str, int], int] = {}


def _record_live_skip(source: str, horizon: int) -> bool:
    """Increment skipped live update count for ``(source, horizon)``.

    Returns True on the first skip per process for that key so callers log
    once at INFO instead of spamming per prediction.
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


def record_scored_prediction(
    engine: Engine,
    *,
    horizon_days: int,
    confidence: float,
    outcome: float,
    signal_contributions: dict[str, float],
    regime: str,
) -> dict[str, Any]:
    """Live incremental updater hook for regime-conditional Brier tracking.

    Proper incremental weighted averaging ``Σ(w·se) / Σw`` requires a
    persisted ``weight_sum`` (Σw) column. Because the current
    ``regime_conditional_brier_history`` schema has no ``weight_sum`` column,
    the live path refuses both incremental UPDATEs and brand-new ``count=1``
    INSERTs (which could never advance). Every valid contributor is skipped,
    counted in ``get_skipped_live_update_counts()``, and logged once per
    process per ``(source, horizon)`` at INFO. Recompute exact weighted means
    via ``bootstrap_from_oracle_predictions``.
    """
    del engine, confidence, outcome  # unused while schema lacks Σw
    if not signal_contributions:
        return {}

    horizon = _canonical_horizon(horizon_days)
    canonical = _canonical_regime(regime)
    for source, weight in signal_contributions.items():
        try:
            w = float(weight)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(w) or w <= 0.0:
            continue
        if _record_live_skip(source, horizon):
            log.info(
                "regime_conditional_brier.record_scored_prediction: skipping "
                "live incremental write for ({s}, {h}d) (first seen in "
                "regime={r}) — table has no persisted weight_sum (Σw) column; "
                "recompute via bootstrap_from_oracle_predictions",
                s=source,
                h=horizon,
                r=canonical,
            )

    return {}


# ── Read path ─────────────────────────────────────────────────────────────


def get_regime_conditional_scorecard(
    engine: Engine,
    signal_source: str,
    horizon_days: int,
    regime: str,
) -> SignalScorecard | None:
    """Read one (signal_source, horizon_days, regime) scorecard.

    Returns ``None`` in two cases:

    1. The row does not exist yet.
    2. The row exists but ``scored_count < MIN_REGIME_SAMPLES`` — we
       deliberately hide thin rows so the composite fallback API routes
       the caller down to the non-regime scorecard instead.

    Callers who want the raw row (even when thin) should read the table
    directly; this API is the production guardrail.
    """
    horizon = _canonical_horizon(horizon_days)
    canonical = _canonical_regime(regime)
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT scored_count, running_brier, running_ece, "
                    "hit_count, last_updated "
                    "FROM regime_conditional_brier_history "
                    "WHERE signal_source = :s "
                    "  AND horizon_days = :h "
                    "  AND regime = :r"
                ),
                {"s": signal_source, "h": horizon, "r": canonical},
            ).fetchone()
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "regime_conditional_brier.get_regime_conditional_scorecard "
            "read failed: {e}",
            e=str(exc),
        )
        return None

    if row is None:
        return None
    count = int(row[0] or 0)
    if count < MIN_REGIME_SAMPLES:
        return None
    return _row_to_scorecard(row, signal_source, horizon)


def get_scorecard_with_regime_fallback(
    engine: Engine,
    signal_source: str,
    horizon_days: int,
    regime: str | None,
) -> SignalScorecard | None:
    """Composite production entry point: regime-conditional first, with
    graceful fallback to the non-regime scorecard.

    Resolution order:

    1. If ``regime`` is ``None`` → go directly to
       ``features.per_signal_brier.get_signal_scorecard``. Callers that
       genuinely don't know the regime (e.g. backtest harnesses that
       pre-date ALPHA-5) get the existing behaviour.
    2. If ``regime`` is provided and the regime-conditional row has at
       least ``MIN_REGIME_SAMPLES`` observations → return it.
    3. Otherwise → fall back to ``get_signal_scorecard``.

    Returns ``None`` only when both the regime-specific AND the
    non-regime scorecards are missing — i.e. a genuinely unknown signal.

    Callers that must tell a regime-specific hit from a flat fallback
    (signal_provenance counts only regime hits) call
    ``get_scorecard_with_regime_source`` instead.
    """
    return get_scorecard_with_regime_source(
        engine, signal_source, horizon_days, regime
    )[0]


def get_scorecard_with_regime_source(
    engine: Engine,
    signal_source: str,
    horizon_days: int,
    regime: str | None,
    *,
    aliases: Sequence[str] = (),
) -> tuple[SignalScorecard | None, str | None]:
    """``get_scorecard_with_regime_fallback`` plus which table answered.

    Same resolution order. Returns ``(card, source)``, where ``source`` is
    ``CALIBRATION_SOURCE_REGIME`` for a warm regime-conditional row,
    ``CALIBRATION_SOURCE_FLAT_FALLBACK`` when a regime was given but its row
    was missing or thin, ``CALIBRATION_SOURCE_FLAT`` when no regime was
    given, and ``None`` when no scorecard exists (``card`` is then ``None``).

    Key policy. ``aliases`` are other spellings of the same signal (the
    bootstrap paths write both ``<family>`` and ``feature:<family>``).
    ``signal_source`` is tried first, then each alias in order, and the
    regime tier is searched across every key before the flat tier is read
    at all. So a flat card on an earlier key never hides a warm regime card
    on a later one, and within a tier the earliest key wins.
    """
    keys = list(dict.fromkeys([signal_source, *aliases]))
    if regime is not None:
        for key in keys:
            regime_card = get_regime_conditional_scorecard(
                engine, key, horizon_days, regime
            )
            if regime_card is not None:
                return regime_card, CALIBRATION_SOURCE_REGIME
    flat_source = (
        CALIBRATION_SOURCE_FLAT if regime is None else CALIBRATION_SOURCE_FLAT_FALLBACK
    )
    for key in keys:
        card = get_signal_scorecard(engine, key, horizon_days)
        if card is not None:
            return card, flat_source
    return None, None


def rank_signals_by_regime(
    engine: Engine,
    regime: str,
    horizon_days: int,
    *,
    min_samples: int | None = None,
) -> list[SignalScorecard]:
    """Return all scorecards for a given ``(regime, horizon_days)``
    pair, sorted best-first by running Brier.

    ``min_samples`` defaults to ``MIN_REGIME_SAMPLES`` so thin rows are
    hidden by default. Pass ``0`` to see every bucket regardless of
    sample count (useful for the dashboard "cold-start signals" list).
    """
    horizon = _canonical_horizon(horizon_days)
    canonical = _canonical_regime(regime)
    threshold = MIN_REGIME_SAMPLES if min_samples is None else int(min_samples)
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT signal_source, horizon_days, scored_count, "
                    "running_brier, running_ece, hit_count, last_updated "
                    "FROM regime_conditional_brier_history "
                    "WHERE regime = :r "
                    "  AND horizon_days = :h "
                    "  AND scored_count >= :n "
                    "ORDER BY running_brier ASC"
                ),
                {"r": canonical, "h": horizon, "n": threshold},
            ).fetchall()
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "regime_conditional_brier.rank_signals_by_regime read failed: {e}",
            e=str(exc),
        )
        return []

    cards: list[SignalScorecard] = []
    for r in rows:
        source = r[0]
        h = int(r[1])
        cards.append(
            _row_to_scorecard(
                (r[2], r[3], r[4], r[5], r[6]),
                signal_source=source,
                horizon_days=h,
            )
        )
    return cards


# ── Bootstrap helper ──────────────────────────────────────────────────────


# The column list pulled from oracle_predictions for the bootstrap
# replay. Lists ``regime`` explicitly because that's the whole point of
# this module — if the oracle populates it, we seed it; otherwise we
# fall back to "NEUTRAL".
_BOOTSTRAP_COLUMNS: tuple[str, ...] = (
    "id",
    "created_at",
    "expiry",
    "confidence",
    "verdict",
    "model_name",
    "signals",
    "signal_contributions",
    "regime",
)

_BOOTSTRAP_QUERY = text(
    """
    SELECT id, created_at, expiry, confidence, verdict, model_name,
           signals,
           signals->'signal_contributions' AS signal_contributions,
           signals->>'regime'               AS regime
    FROM """ + VISIBLE_ORACLE_PREDICTIONS + """ oracle_predictions
    WHERE verdict IN ('hit', 'miss', 'partial')
      AND created_at >= NOW() - (:days || ' days')::interval
      AND dedup_keep = TRUE
      -- A prediction that stated no confidence has no probability to score
      -- for reliability; it is excluded, never replayed at a default.
      AND confidence IS NOT NULL
    ORDER BY created_at ASC
    """
)


def _verdict_to_outcome(verdict: str) -> float:
    """Map an oracle verdict string to the outcome scalar used by the
    Brier tracker. Mirrors the helper in ``bootstrap_per_signal_brier``."""
    if verdict == "hit":
        return 1.0
    if verdict == "partial":
        return 0.5
    return 0.0


def _coerce_horizon_days(created: Any, expiry: Any) -> int:
    """Compute horizon_days from a prediction row's created_at / expiry.

    Defensive against NULLs and datetime/date mixing. Defaults to 7 so
    downstream callers always get a canonical bucket.
    """
    if created is None or expiry is None:
        return 7
    try:
        if isinstance(created, datetime) and isinstance(expiry, datetime):
            delta = (expiry - created).days
        else:
            created_d = created.date() if isinstance(created, datetime) else created
            expiry_d = expiry.date() if isinstance(expiry, datetime) else expiry
            delta = (expiry_d - created_d).days
    except Exception:  # noqa: BLE001
        return 7
    if delta <= 0:
        return 1
    return int(delta)


def _coerce_contributions(raw: Any) -> dict[str, float]:
    """Best-effort parser for the oracle_predictions.signal_contributions
    cell. Returns an empty dict on failure.
    """
    import json

    if raw is None:
        return {}
    if isinstance(raw, dict):
        parsed: Any = raw
    elif isinstance(raw, (bytes, bytearray)):
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except Exception:  # noqa: BLE001
            return {}
    elif isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except Exception:  # noqa: BLE001
            return {}
    else:
        return {}

    if not isinstance(parsed, dict) or not parsed:
        return {}
    cleaned: dict[str, float] = {}
    for k, v in parsed.items():
        if not isinstance(k, str) or not k.strip():
            continue
        try:
            w = float(v)
        except (TypeError, ValueError):
            continue
        if w <= 0:
            continue
        cleaned[k.strip()] = w
    if not cleaned:
        return {}
    total = sum(cleaned.values())
    if total <= 0:
        return {}
    return {k: w / total for k, w in cleaned.items()}


def bootstrap_from_oracle_predictions(
    engine: Engine,
    days: int = 365,
    limit: int | None = None,
) -> dict[str, Any]:
    """One-shot historical replay that seeds
    ``regime_conditional_brier_history`` from ``oracle_predictions``.

    Walks scored predictions in chronological order, pulls the
    ``regime`` column directly off each row (defaults to ``"NEUTRAL"``
    when the column is missing — early predictions pre-date ALPHA-5),
    and accumulates ``Σ(w·se)`` / ``Σw`` per bucket. After a successful read the
    whole table is replaced in one transaction (delete, then insert the eligible
    buckets), so rows the replay rejects or no longer produces are retired.

    Parameters
    ----------
    engine
        SQLAlchemy engine. Tests pass a FakeEngine with ``.connect`` /
        ``.begin`` context managers.
    days
        Lookback window in days (default 365).
    limit
        If set, stop after this many predictions (testing aid).

    Returns
    -------
    Summary dict with ``replayed_count``, ``skipped_count``,
    ``seeded_buckets`` (list of ``{source, regime, count}``),
    ``regime_histogram`` (regime → replayed count), ``replaced``,
    ``retired_count`` and ``finished_at``. Empty shells and ``replaced=False``
    on DB error; the table is then unchanged.
    """
    ensure_regime_brier_table(engine)
    summary: dict[str, Any] = {
        "replayed_count": 0,
        "skipped_count": 0,
        "seeded_buckets": [],
        "regime_histogram": {},
        "replaced": False,
        "written_count": 0,
        "retired_count": 0,
        "skipped_low_effective_weight": 0,
        "finished_at": datetime.now(timezone.utc).isoformat(),
    }

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                _BOOTSTRAP_QUERY, {"days": int(days)}
            ).fetchall()
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "regime_conditional_brier.bootstrap: oracle_predictions read "
            "failed: {e}",
            e=str(exc),
        )
        return summary

    if limit is not None:
        rows = list(rows)[: int(limit)]

    replayed = 0
    skipped = 0
    bucket_counts: dict[tuple[str, str], int] = {}
    regime_histogram: dict[str, int] = {}
    # Aggregate weighted sums in memory across all raw rows so each persisted
    # bucket stores the exact weighted mean Σ(w·se)/Σw without requiring a
    # schema change to hold Σw per row.
    accumulators: dict[tuple[str, int, str], dict[str, Any]] = {}

    for row in rows:
        try:
            row_dict = dict(zip(_BOOTSTRAP_COLUMNS, row))
            verdict = row_dict.get("verdict")
            if verdict not in ("hit", "miss", "partial"):
                skipped += 1
                continue
            raw_confidence = row_dict.get("confidence")
            if raw_confidence is None:
                # Coercing an unstated confidence to zero turned it into a
                # stated 0% forecast and scored it. Not measurable, not
                # replayed.
                skipped += 1
                continue
            try:
                confidence = float(raw_confidence)
            except (TypeError, ValueError):
                skipped += 1
                continue
            if not math.isfinite(confidence):
                skipped += 1
                continue
            confidence = max(0.0, min(1.0, confidence))
            outcome = _verdict_to_outcome(verdict)
            horizon_days = _canonical_horizon(
                _coerce_horizon_days(
                    row_dict.get("created_at"), row_dict.get("expiry")
                )
            )
            contributions = _coerce_contributions(
                row_dict.get("signal_contributions")
            )
            if not contributions:
                # Degenerate fallback — attribute the whole prediction to
                # the synthetic ``oracle_aggregate`` source so the row
                # isn't dropped entirely. Mirrors the cascade in
                # bootstrap_per_signal_brier.
                contributions = {"oracle_aggregate": 1.0}
            canonical = _canonical_regime(row_dict.get("regime"))

            squared_error = (confidence - outcome) ** 2
            absolute_error = abs(confidence - outcome)
            directional_hit = 1 if (confidence >= 0.5) == (outcome >= 0.5) else 0

            contributed = False
            for source, weight in contributions.items():
                try:
                    w = float(weight)
                except (TypeError, ValueError):
                    continue
                if not math.isfinite(w) or w <= 0.0:
                    continue
                acc_key = (source, horizon_days, canonical)
                acc = accumulators.get(acc_key)
                if acc is None:
                    acc = {
                        "scored_count": 0,
                        "weight_sum": 0.0,
                        "weighted_brier_sum": 0.0,
                        "weighted_ece_sum": 0.0,
                        "hit_count": 0,
                    }
                    accumulators[acc_key] = acc
                acc["scored_count"] += 1
                acc["weight_sum"] += w
                acc["weighted_brier_sum"] += w * squared_error
                acc["weighted_ece_sum"] += w * absolute_error
                acc["hit_count"] += directional_hit
                contributed = True

            if contributed:
                replayed += 1
                regime_histogram[canonical] = (
                    regime_histogram.get(canonical, 0) + 1
                )
                for source in contributions:
                    key = (source, canonical)
                    bucket_counts[key] = bucket_counts.get(key, 0) + 1
            else:
                skipped += 1
        except Exception as exc:  # noqa: BLE001
            skipped += 1
            log.debug(
                "regime_conditional_brier.bootstrap: row failed: {e}",
                e=str(exc),
            )
            continue

    written_keys: set[tuple[str, int, str]] = set()
    skipped_low = 0
    try:
        with engine.begin() as conn:
            previous = conn.execute(
                text(
                    "DELETE FROM regime_conditional_brier_history "
                    "RETURNING signal_source, horizon_days, regime"
                )
            ).fetchall()
            previous_keys = {(str(r[0]), int(r[1]), str(r[2])) for r in previous or []}
            for (source, horizon_days, canonical), acc in accumulators.items():
                weight_sum = acc["weight_sum"]
                if weight_sum <= 0.0:
                    continue
                scored_count = int(acc["scored_count"])
                if (
                    scored_count >= MIN_REGIME_SAMPLES
                    and weight_sum + 1e-12 < MIN_EFFECTIVE_WEIGHT_SUM
                ):
                    # Fail closed on current schema (no weight_sum column):
                    # persisting scored_count >= threshold with Σw < 5
                    # would let readers bypass the effective-sample floor.
                    # The DELETE above has already retired any old row.
                    log.warning(
                        "regime_conditional_brier.bootstrap: skipping "
                        "({s}, {h}d, {r}) — scored_count={n} but effective "
                        "weight Σw={w:.3f} < {floor}; old row retired",
                        s=source,
                        h=horizon_days,
                        r=canonical,
                        n=scored_count,
                        w=weight_sum,
                        floor=MIN_EFFECTIVE_WEIGHT_SUM,
                    )
                    skipped_low += 1
                    continue
                running_brier = acc["weighted_brier_sum"] / weight_sum
                running_ece = acc["weighted_ece_sum"] / weight_sum
                # Plain INSERT: the table was emptied in this transaction, so
                # a key conflict means a concurrent writer and rolls back all.
                conn.execute(
                    text(
                        """
                        INSERT INTO regime_conditional_brier_history
                            (signal_source, horizon_days, regime,
                             scored_count, running_brier, running_ece,
                             hit_count, last_updated)
                        VALUES (:s, :h, :r, :n, :b, :e, :hit, NOW())
                        """
                    ),
                    {
                        "s": source,
                        "h": horizon_days,
                        "r": canonical,
                        "n": scored_count,
                        "b": running_brier,
                        "e": running_ece,
                        "hit": int(acc["hit_count"]),
                    },
                )
                written_keys.add((source, int(horizon_days), canonical))
        summary["replaced"] = True
        summary["written_count"] = len(written_keys)
        summary["retired_count"] = len(previous_keys - written_keys)
        summary["skipped_low_effective_weight"] = skipped_low
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "regime_conditional_brier.bootstrap: replacement rolled back, "
            "table unchanged: {e}",
            e=str(exc),
        )

    summary["replayed_count"] = replayed
    summary["skipped_count"] = skipped
    summary["regime_histogram"] = regime_histogram
    summary["seeded_buckets"] = [
        {"source": src, "regime": reg, "count": cnt}
        for (src, reg), cnt in sorted(bucket_counts.items())
    ]
    summary["finished_at"] = datetime.now(timezone.utc).isoformat()
    return summary
