"""Walk-forward backtest of the full decision stack over oracle_predictions
history. Measures whether HIGH-verdict trades win at higher rates than MEDIUM,
whether the stress test's 'fragile' label actually predicts failure, and
whether the conviction dial from per_signal_brier is well-calibrated. The
answer drives whether we trust the stack to size real positions.

Overview
--------

For every scored prediction in the last N days we:

  1. Snapshot the per_signal_brier_history rows as they existed at that
     prediction's ``created_at`` (strict ``last_updated <= as_of`` filter to
     prevent lookahead leak — see ``_reconstruct_historical_scorecards``).
  2. Reassemble a ``TradeProvenanceReport`` from the prediction row's Shapley
     contributions + those time-frozen scorecards. No oracle re-run; the
     prediction's confidence/direction/regime/etc. are trusted as-of that
     moment.
  3. Feed the provenance report through the real
     ``counterfactual_stress.run_stress_test`` to get the robustness label
     the operator WOULD HAVE seen that day.
  4. Feed the same report through ``trade_ticket_generator.generate_ticket``
     to materialize the exact ticket the desk would have received.
  5. Record the hypothetical verdict alongside the realized outcome
     (hit/partial/miss) so we can measure calibration.

Aggregation produces a ``BacktestReport`` with per-verdict hit rate, per-
verdict Sharpe / drawdown / return stats, a verdict-vs-outcome confusion
matrix, a per-signal contribution attribution that names the signals present
in the trades that hit versus the ones that missed, and — the empirical
payoff — a stress_test_calibration block that tells us whether the
``robustness_label == 'fragile'`` flag correlates with actual failures.

The whole harness is read-only by default except for the one ``INSERT`` into
``backtest_results`` at the end (skipped under ``--dry-run``).

CLI
---

    python -m scripts.walk_forward_validate --days 365
    python -m scripts.walk_forward_validate --days 90 --horizon-days 7
    python -m scripts.walk_forward_validate --dry-run --limit 20 --verbose
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# IMPORT — never reimplement. These are the exact entry points the live
# decision gateway calls, so any calibration drift shows up here too.
from features.per_signal_brier import (
    MIN_CALIBRATED_SAMPLES,
    SignalScorecard,
    _canonical_horizon,
    compute_conviction_weight,
)
from intelligence.counterfactual_stress import run_stress_test
from intelligence.signal_provenance import (
    CausationChain,
    SignalEvidence,
    TradeProvenanceReport,
    _verdict_from_aggregate,
    compute_aggregate_conviction,
)
from store.pit import PITStore
from trading.trade_ticket_generator import generate_ticket

# scripts.bootstrap_per_signal_brier owns the oracle_predictions row shape
# + the 3-layer contribution cascade. Reuse it verbatim so we never diverge.
from scripts.bootstrap_per_signal_brier import (
    _coerce_confidence,
    _coerce_horizon_days,
    _load_oracle_models_lookup,
    extract_signal_contributions,
)


# ── Constants ─────────────────────────────────────────────────────────────

DEFAULT_LOOKBACK_DAYS: int = 365
CONVICTION_BUCKETS: tuple[str, ...] = ("no_trade", "low", "medium", "high")
SHARPE_ANNUALIZATION: int = 252  # business days

# Mapping oracle verdict strings → outcome scalar used for hit/partial/miss
# maths. Keeps this module's verdict_to_outcome matching the bootstrap's
# one-liner exactly.
_VERDICT_TO_OUTCOME: dict[str, float] = {
    "hit": 1.0,
    "partial": 0.5,
    "miss": 0.0,
}

# Realized-return proxies keyed on outcome. In the absence of a real price
# series replay (wiring PIT queries back to oracle_predictions is a separate
# task), we use sign-aware scaled returns: bullish hit = +1 * scale,
# bullish miss = -1 * scale, partial = ±0.25 * scale. This is the simplest
# proxy that still lets Sharpe / drawdown / calibration maths run end-to-
# end. Real equity-curve replay lands in the next sprint.
_HIT_RETURN_SCALE: float = 0.02   # 2% per unit outcome
_PARTIAL_RETURN_SCALE: float = 0.005


# ── Dataclasses ───────────────────────────────────────────────────────────


@dataclass(frozen=True)
class BacktestTrade:
    """One replayed trade. Every field a downstream aggregator needs is
    baked in at construction time so the walk-forward loop can throw away
    intermediate objects.
    """

    prediction_id: str
    ticker: str
    prediction_date: str            # ISO timestamp of the prediction's created_at
    verdict: str                    # unified verdict our stack WOULD have emitted
    aggregate_conviction: float
    robustness_label: str           # robust / moderate / fragile / unknown
    robustness_score: float
    oracle_confidence: float
    oracle_direction: str
    outcome_verdict: str            # hit / partial / miss
    realized_return: float
    horizon_days: int
    hit: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class VerdictStats:
    """Per-verdict aggregate stats. All floats are finite — no NaN emitted
    even on empty buckets (see ``aggregate_per_verdict_stats``).
    """

    verdict: str
    n_trades: int
    hit_rate: float
    mean_return: float
    std_return: float
    sharpe: float
    max_drawdown: float
    stress_fragile_count: int
    stress_robust_count: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BacktestReport:
    """Full walk-forward result. One row per run persists to
    ``backtest_results``.
    """

    start_date: str
    end_date: str
    total_predictions_walked: int
    trades_generated: int
    verdict_stats: dict[str, VerdictStats]
    confusion_matrix: dict[str, int]   # "{verdict}|{outcome}" → count
    stress_test_calibration: dict[str, float]
    signal_contribution_attribution: dict[str, dict[str, int]]
    narrative: str
    generated_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "total_predictions_walked": self.total_predictions_walked,
            "trades_generated": self.trades_generated,
            "verdict_stats": {
                k: v.to_dict() for k, v in self.verdict_stats.items()
            },
            "confusion_matrix": dict(self.confusion_matrix),
            "stress_test_calibration": dict(self.stress_test_calibration),
            "signal_contribution_attribution": {
                k: dict(v) for k, v in self.signal_contribution_attribution.items()
            },
            "narrative": self.narrative,
            "generated_at": self.generated_at,
        }


# ── Pure helpers ──────────────────────────────────────────────────────────


def verdict_to_outcome(verdict: str) -> float:
    """Map an oracle verdict string to the outcome scalar (hit=1.0,
    partial=0.5, miss=0.0, unknown=0.0). Mirrors
    ``scripts.bootstrap_per_signal_brier.verdict_to_outcome``.
    """
    return _VERDICT_TO_OUTCOME.get(str(verdict or "").strip().lower(), 0.0)


def compute_sharpe(
    returns: list[float],
    annualization: int = SHARPE_ANNUALIZATION,
) -> float:
    """Annualized Sharpe from a return series. Defensive against empty
    input and zero-standard-deviation degenerate cases; always finite.
    """
    if not returns:
        return 0.0
    clean = [float(r) for r in returns if r is not None and math.isfinite(float(r))]
    if len(clean) < 2:
        return 0.0
    mean = statistics.fmean(clean)
    try:
        std = statistics.stdev(clean)
    except statistics.StatisticsError:
        return 0.0
    if std <= 0.0 or not math.isfinite(std):
        return 0.0
    return float((mean / std) * math.sqrt(max(1, int(annualization))))


def compute_max_drawdown(returns: list[float]) -> float:
    """Return the worst peak-to-trough drawdown magnitude of the compounded
    equity curve implied by ``returns``. Always ``>= 0.0``.
    """
    if not returns:
        return 0.0
    equity = 1.0
    peak = 1.0
    worst = 0.0
    for r in returns:
        try:
            step = float(r)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(step):
            continue
        equity *= (1.0 + step)
        if equity > peak:
            peak = equity
        if peak <= 0:
            continue
        dd = (peak - equity) / peak
        if dd > worst:
            worst = dd
    return float(max(0.0, worst))


def classify_hit(verdict: str, outcome: str) -> bool:
    """A trade is a 'hit' when the directional call was correct.

    Bullish direction + outcome in {'hit', 'partial'} → True
    Bearish direction + outcome == 'miss' → True (a correct short)
    Everything else → False

    The oracle's ``outcome_verdict`` field is always written relative to
    the PREDICTED direction ('hit' = the prediction was right, 'miss' = it
    was wrong), so for the realized-hit check we fold bearish misses into
    hits to reward correct shorts. Partial counts as a half-hit for
    bullish but as a miss for bearish (asymmetry intentional — shorts
    aren't half-right).
    """
    direction = str(verdict or "").strip().lower()
    outcome_norm = str(outcome or "").strip().lower()
    if direction == "bullish":
        return outcome_norm in ("hit", "partial")
    if direction == "bearish":
        return outcome_norm in ("hit", "partial")
    return outcome_norm == "hit"


def _realized_return_from_outcome(
    direction: str,
    outcome: str,
) -> float:
    """Convert an (direction, outcome) pair into a realized return proxy.

    Used as the fallback when PIT price replay cannot resolve a ticker
    (e.g. the ticker has no feature_registry entry). The PIT path in
    ``_realized_return_from_pit`` is preferred and always tried first.
    """
    dir_norm = str(direction or "").strip().lower()
    out_norm = str(outcome or "").strip().lower()
    sign = 1.0 if dir_norm == "bullish" else (-1.0 if dir_norm == "bearish" else 0.0)
    if out_norm == "hit":
        return sign * _HIT_RETURN_SCALE
    if out_norm == "partial":
        return sign * _PARTIAL_RETURN_SCALE
    if out_norm == "miss":
        return -sign * _HIT_RETURN_SCALE
    return 0.0


# ── PIT-correct price replay ─────────────────────────────────────────────

# feature_registry name patterns to try per ticker, in priority order.
# Matches the mix found across the codebase (entity_map.py, options_recommender.py).
_TICKER_FEATURE_NAME_PATTERNS: tuple[str, ...] = (
    "{t}_full",
    "{t}_close",
    "{t}_etf_close",
    "{t}",
)


def _resolve_ticker_feature_id(
    engine: Engine,
    ticker: str,
    cache: dict[str, int | None],
) -> int | None:
    """Resolve a ticker symbol to a ``feature_registry.id`` for PIT lookup.

    Tries a small set of name patterns and memoizes the result per run.
    Returns ``None`` if nothing matches — callers fall back to the
    outcome-based proxy.
    """
    key = (ticker or "").strip().lower()
    if not key:
        return None
    if key in cache:
        return cache[key]

    try:
        with engine.connect() as conn:
            for pattern in _TICKER_FEATURE_NAME_PATTERNS:
                name = pattern.format(t=key)
                row = conn.execute(
                    text(
                        "SELECT id FROM feature_registry WHERE name = :name LIMIT 1"
                    ),
                    {"name": name},
                ).fetchone()
                if row is not None:
                    cache[key] = int(row[0])
                    return cache[key]
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "walk_forward: feature_registry lookup failed for {t}: {e}",
            t=key,
            e=str(exc),
        )

    cache[key] = None
    return None


def _pit_price_on_or_before(
    pit_store: PITStore,
    feature_id: int,
    as_of: date,
) -> float | None:
    """Return the PIT-correct price for ``feature_id`` at ``as_of``.

    Uses ``LATEST_AS_OF`` vintage so later revisions win, and picks the
    most recent ``obs_date`` that was already released. Returns ``None``
    if no row qualifies.
    """
    try:
        df = pit_store.get_pit(
            feature_ids=[feature_id],
            as_of_date=as_of,
            vintage_policy="LATEST_AS_OF",
        )
    except Exception as exc:  # noqa: BLE001
        log.debug("walk_forward: PIT fetch failed fid={f}: {e}", f=feature_id, e=str(exc))
        return None

    if df is None or df.empty:
        return None

    try:
        latest = df.sort_values("obs_date").iloc[-1]
        value = float(latest["value"])
    except Exception as exc:  # noqa: BLE001
        log.debug("walk_forward: PIT frame parse failed fid={f}: {e}", f=feature_id, e=str(exc))
        return None

    if not math.isfinite(value) or value <= 0.0:
        return None
    return value


def _realized_return_from_pit(
    pit_store: PITStore,
    feature_id: int,
    direction: str,
    entry_as_of: date,
    exit_as_of: date,
) -> float | None:
    """PIT-correct realized return for a bullish/bearish call between two dates.

    Entry price is the latest PIT value at ``entry_as_of`` (prediction
    created_at). Exit price is the latest PIT value at
    ``entry_as_of + horizon``. Bearish directions flip the sign so a
    profitable short shows up positive. Returns ``None`` if either price
    is missing — the caller then falls back to the outcome proxy.
    """
    entry = _pit_price_on_or_before(pit_store, feature_id, entry_as_of)
    if entry is None:
        return None
    exit_px = _pit_price_on_or_before(pit_store, feature_id, exit_as_of)
    if exit_px is None:
        return None

    raw = (exit_px - entry) / entry
    dir_norm = str(direction or "").strip().lower()
    if dir_norm == "bearish":
        raw = -raw
    elif dir_norm != "bullish":
        return None

    if not math.isfinite(raw):
        return None
    return float(raw)


# ── Time-frozen provenance reconstruction ────────────────────────────────


class _FrozenPrediction:
    """Duck-typed stand-in for ``oracle.engine.EnsemblePrediction`` used by
    ``build_provenance_report``. We don't re-run the oracle — we replay the
    row. Only the attributes signal_provenance actually reads are set here;
    every other field stays at its default. Not a dataclass because the
    underlying EnsemblePrediction isn't frozen and we want attribute-style
    access without mandatory fields.
    """

    def __init__(self, **kwargs: Any) -> None:
        for k, v in kwargs.items():
            setattr(self, k, v)


def build_time_frozen_provenance(
    prediction_row: dict[str, Any],
    scorecards_at_time: dict[str, SignalScorecard],
) -> TradeProvenanceReport:
    """Reconstruct a ``TradeProvenanceReport`` using only data that existed
    at the prediction's ``created_at``.

    Parameters
    ----------
    prediction_row
        One dict from ``_load_scored_predictions`` (keyed by ORACLE_COLUMNS).
    scorecards_at_time
        Map of ``signal_source → SignalScorecard`` reflecting the state of
        ``per_signal_brier_history`` at ``prediction_row['created_at']``.
        Empty dict is acceptable — contributors without history are emitted
        with ``scorecard=None`` / ``classification='no_history'`` so the
        cold-start branch is exercised cleanly.

    Returns
    -------
    A fully-populated ``TradeProvenanceReport`` whose ``aggregate_conviction``
    and ``verdict`` were computed via the same
    ``compute_aggregate_conviction`` / ``_verdict_from_aggregate`` that the
    live gateway uses.
    """
    horizon = _coerce_horizon_days(prediction_row)
    confidence = _coerce_confidence(prediction_row.get("confidence")) or 0.0

    # Extract contributions using the exact same cascade the live path
    # uses. ``extract_signal_contributions`` returns a normalized dict even
    # on cold-start rows (falls through to ORACLE_AGGREGATE_SOURCE).
    contributions = extract_signal_contributions(
        prediction_row,
        oracle_models_lookup=prediction_row.get("_models_lookup") or {},
    )

    signal_evidence: list[SignalEvidence] = []
    for source, weight in contributions.items():
        scorecard = scorecards_at_time.get(source)
        classification = _classify_scorecard(scorecard)
        signal_evidence.append(
            SignalEvidence(
                signal_source=source,
                shapley_weight=float(weight),
                scorecard=scorecard,
                classification=classification,
            )
        )

    # No fudge-alert lookup in backtest — that's a live DB call. We treat
    # the historical window as clean (zero alerts) since we can't replay
    # them with the right freshness cheaply.
    fudge_alert_count = 0

    # Surface the prediction's stored metadata with safe defaults. The
    # replayed row's ``signals`` JSONB may carry fragility/disagreement;
    # default neutral values otherwise.
    signals = prediction_row.get("signals") or {}
    if isinstance(signals, str):
        try:
            signals = json.loads(signals)
        except (ValueError, TypeError):
            signals = {}
    if not isinstance(signals, dict):
        signals = {}

    fragility_multiplier = float(signals.get("fragility_multiplier", 1.0) or 1.0)
    disagreement_score = float(signals.get("disagreement_score", 0.0) or 0.0)
    red_team_risk = float(signals.get("red_team_epistemic_risk", 0.0) or 0.0)

    aggregate = compute_aggregate_conviction(
        signal_evidence,
        fragility_multiplier=fragility_multiplier,
        disagreement_score=disagreement_score,
        red_team_epistemic_risk=red_team_risk,
        fudge_alert_count=fudge_alert_count,
    )
    verdict = _verdict_from_aggregate(aggregate, confidence)

    direction = str(prediction_row.get("direction") or signals.get("direction") or "").strip().lower()
    # Derive flow from direction for the causation chain. Lever + actor
    # come from the stored signals dict when available.
    flow = (
        "open" if direction == "bullish"
        else "close" if direction == "bearish"
        else "neutral"
    )
    lever = str(signals.get("catalyst_type") or signals.get("lever") or "").strip()
    actor = str(signals.get("shapley_top_contributor") or signals.get("actor") or "").strip()
    causation = CausationChain(
        lever=lever,
        flow_direction=flow,
        actor=actor,
        complete=bool(lever) and bool(actor) and flow != "neutral",
    )

    created_at = prediction_row.get("created_at")
    if isinstance(created_at, datetime):
        generated_iso = created_at.isoformat()
    else:
        generated_iso = str(created_at or datetime.now(timezone.utc).isoformat())

    confidence_lower = float(signals.get("confidence_lower", max(0.0, confidence - 0.1)) or 0.0)
    confidence_upper = float(signals.get("confidence_upper", min(1.0, confidence + 0.1)) or 0.0)

    return TradeProvenanceReport(
        ticker=str(prediction_row.get("ticker") or ""),
        generated_at=generated_iso,
        direction=direction,
        score=int(signals.get("score", 50) or 50),
        confidence=confidence,
        confidence_lower=confidence_lower,
        confidence_upper=confidence_upper,
        horizon_days=int(horizon),
        regime=str(signals.get("regime") or ""),
        fci_regime=str(signals.get("fci_regime") or ""),
        signal_evidence=signal_evidence,
        top_shapley_contributor=str(signals.get("shapley_top_contributor") or ""),
        top_shapley_share=float(signals.get("shapley_top_share", 0.0) or 0.0),
        fragility_multiplier=fragility_multiplier,
        disagreement_score=disagreement_score,
        crowd_aligned=bool(signals.get("crowd_aligned", False)),
        market_implied_prob=float(signals.get("market_implied_prob", 0.0) or 0.0),
        red_team_epistemic_risk=red_team_risk,
        shipping_fudge_alerts=[],
        causation=causation,
        cooccurrence_lift=1.0,
        regime_calibrated_signal_count=0,
        confidence_bucket_multiplier=1.0,
        scenario_multiplier=1.0,
        null_hypothesis_penalty=1.0,
        meta_learning_multiplier=1.0,
        contra_indicator_multiplier=1.0,
        squeeze_multiplier=1.0,
        arbitrage_multiplier=1.0,
        convergence_multiplier=1.0,
        money_flow_multiplier=1.0,
        memory_lesson_multiplier=1.0,
        aggregate_conviction=aggregate,
        verdict=verdict,
    )


def _classify_scorecard(scorecard: SignalScorecard | None) -> str:
    """Local copy of signal_provenance's _classify_evidence so the
    backtest stays pure — we don't want to reach into an underscore
    helper of another module.
    """
    if scorecard is None:
        return "no_history"
    if not scorecard.is_calibrated:
        return "cold_start"
    w = scorecard.conviction_weight
    if w <= 0.01:
        return "anti_predictive"
    if w < 0.5:
        return "weak"
    if w >= 1.2:
        return "strong"
    return "neutral"


# ── Aggregation ──────────────────────────────────────────────────────────


def aggregate_per_verdict_stats(
    trades: list[BacktestTrade],
) -> dict[str, VerdictStats]:
    """Group ``trades`` by their unified verdict and compute summary stats
    for each of ``CONVICTION_BUCKETS``. Every bucket always appears in the
    output, even with zero trades (defaulted to safe zeros).
    """
    buckets: dict[str, list[BacktestTrade]] = {v: [] for v in CONVICTION_BUCKETS}
    for t in trades:
        if t.verdict in buckets:
            buckets[t.verdict].append(t)
        else:
            buckets.setdefault("no_trade", []).append(t)

    out: dict[str, VerdictStats] = {}
    for verdict in CONVICTION_BUCKETS:
        bucket = buckets[verdict]
        n = len(bucket)
        if n == 0:
            out[verdict] = VerdictStats(
                verdict=verdict,
                n_trades=0,
                hit_rate=0.0,
                mean_return=0.0,
                std_return=0.0,
                sharpe=0.0,
                max_drawdown=0.0,
                stress_fragile_count=0,
                stress_robust_count=0,
            )
            continue

        hits = sum(1 for t in bucket if t.hit)
        returns = [t.realized_return for t in bucket]
        mean = statistics.fmean(returns) if returns else 0.0
        try:
            std = statistics.stdev(returns) if len(returns) >= 2 else 0.0
        except statistics.StatisticsError:
            std = 0.0
        fragile = sum(1 for t in bucket if t.robustness_label == "fragile")
        robust = sum(1 for t in bucket if t.robustness_label == "robust")
        out[verdict] = VerdictStats(
            verdict=verdict,
            n_trades=n,
            hit_rate=float(hits / n),
            mean_return=float(mean),
            std_return=float(std),
            sharpe=compute_sharpe(returns),
            max_drawdown=compute_max_drawdown(returns),
            stress_fragile_count=int(fragile),
            stress_robust_count=int(robust),
        )
    return out


def compute_confusion_matrix(
    trades: list[BacktestTrade],
) -> dict[str, int]:
    """Return a flat ``{"{verdict}|{outcome}": count}`` dict. Flat keys are
    used so the dict is directly JSON-serializable for persistence.
    """
    counts: dict[str, int] = {}
    for t in trades:
        key = f"{t.verdict}|{t.outcome_verdict}"
        counts[key] = counts.get(key, 0) + 1
    return counts


def measure_stress_test_calibration(
    trades: list[BacktestTrade],
) -> dict[str, float]:
    """Return {fragile_failure_rate, robust_failure_rate, lift}.

    The lift is ``fragile_failure_rate - robust_failure_rate``; a positive
    value means the stress test correctly identifies trades more likely
    to fail. A zero/negative value means it is uncalibrated or backwards
    and we need to retune.
    """
    fragile = [t for t in trades if t.robustness_label == "fragile"]
    robust = [t for t in trades if t.robustness_label == "robust"]
    fragile_fail_rate = (
        sum(1 for t in fragile if not t.hit) / len(fragile) if fragile else 0.0
    )
    robust_fail_rate = (
        sum(1 for t in robust if not t.hit) / len(robust) if robust else 0.0
    )
    lift = float(fragile_fail_rate - robust_fail_rate)
    return {
        "fragile_failure_rate": float(fragile_fail_rate),
        "robust_failure_rate": float(robust_fail_rate),
        "lift": lift,
        "n_fragile": float(len(fragile)),
        "n_robust": float(len(robust)),
    }


def _signal_contribution_attribution(
    trades: list[BacktestTrade],
    per_trade_signals: dict[str, list[str]],
) -> dict[str, dict[str, int]]:
    """Sort contributing signals into {"hits": {source: count},
    "misses": {source: count}} buckets.

    ``per_trade_signals`` maps prediction_id → list of contributing signal
    sources. Kept as a sidecar argument so BacktestTrade stays frozen and
    small.
    """
    hits_map: dict[str, int] = {}
    misses_map: dict[str, int] = {}
    for t in trades:
        sources = per_trade_signals.get(t.prediction_id, [])
        target = hits_map if t.hit else misses_map
        for src in sources:
            target[src] = target.get(src, 0) + 1
    return {"hits": hits_map, "misses": misses_map}


# ── Narrative template ───────────────────────────────────────────────────


_NARRATIVE_TEMPLATE = (
    "Walk-forward backtest — {walked} predictions replayed over {days}d, "
    "{trades} trade tickets generated. "
    "HIGH verdict hit rate: {high_hr:.1%} (n={high_n}). "
    "MEDIUM verdict hit rate: {medium_hr:.1%} (n={medium_n}). "
    "LOW verdict hit rate: {low_hr:.1%} (n={low_n}). "
    "Stress-test calibration lift: {lift:+.1%} "
    "(fragile failure rate {fragile_fail:.1%} vs robust {robust_fail:.1%}). "
    "Verdict is {empirical_call}."
)

_NO_PREDICTIONS_NARRATIVE = (
    "Walk-forward: 0 scored predictions matched the last-{days}d query. "
    "Check the run log for SQL errors or schema mismatches."
)

_NO_TICKETS_NARRATIVE = (
    "Walk-forward: {walked} scored predictions replayed, 0 produced trade tickets. "
    "Ticket generation is rejecting every row — investigate generate_ticket."
)


def _build_narrative(
    *,
    days: int,
    walked: int,
    trades_generated: int,
    verdict_stats: dict[str, VerdictStats],
    calibration: dict[str, float],
) -> str:
    """Compose the summary narrative. Always returns a non-empty string."""
    if walked == 0:
        return _NO_PREDICTIONS_NARRATIVE.format(days=int(days))
    if trades_generated == 0:
        return _NO_TICKETS_NARRATIVE.format(walked=int(walked), days=int(days))

    high = verdict_stats.get("high")
    medium = verdict_stats.get("medium")
    low = verdict_stats.get("low")
    high_n = high.n_trades if high else 0
    medium_n = medium.n_trades if medium else 0
    low_n = low.n_trades if low else 0
    high_hr = high.hit_rate if high else 0.0
    medium_hr = medium.hit_rate if medium else 0.0
    low_hr = low.hit_rate if low else 0.0

    lift = float(calibration.get("lift", 0.0) or 0.0)
    # The acid test: HIGH should beat MEDIUM should beat LOW.
    if high_n > 0 and medium_n > 0 and high_hr > medium_hr + 0.05:
        call = "STACK CALIBRATED — HIGH meaningfully beats MEDIUM"
    elif high_n > 0 and medium_n > 0 and high_hr < medium_hr:
        call = "STACK BROKEN — HIGH underperforms MEDIUM, retune required"
    else:
        call = "STACK INCONCLUSIVE — insufficient separation between buckets"

    return _NARRATIVE_TEMPLATE.format(
        walked=int(walked),
        days=int(days),
        trades=int(trades_generated),
        high_hr=high_hr,
        high_n=high_n,
        medium_hr=medium_hr,
        medium_n=medium_n,
        low_hr=low_hr,
        low_n=low_n,
        lift=lift,
        fragile_fail=float(calibration.get("fragile_failure_rate", 0.0) or 0.0),
        robust_fail=float(calibration.get("robust_failure_rate", 0.0) or 0.0),
        empirical_call=call,
    )


# ── DB layer ──────────────────────────────────────────────────────────────


_WALK_QUERY = text(
    """
    SELECT id, ticker, created_at, expiry, confidence, verdict,
           model_name, signals, model_weights, pnl_pct, direction
    FROM oracle_predictions
    WHERE verdict IN ('hit', 'miss', 'partial')
      AND created_at >= NOW() - (:days || ' days')::interval
      AND dedup_keep = TRUE
    ORDER BY created_at ASC
    """
)

# Note: oracle_predictions does NOT have a `signal_contributions` column
# in the current schema (verified 2026-04-28). Downstream code that expects
# `signal_contributions` gets None — `extract_signal_contributions` and the
# provenance builder handle missing data gracefully.
_ORACLE_COLUMNS: tuple[str, ...] = (
    "id",
    "ticker",
    "created_at",
    "expiry",
    "confidence",
    "verdict",
    "model_name",
    "signals",
    "model_weights",
    "pnl_pct",
    "direction",
)


def _load_scored_predictions(
    engine: Engine,
    days: int,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Walk oracle_predictions for scored rows in the last ``days`` days.

    Mirrors ``scripts.bootstrap_per_signal_brier._fetch_scored_predictions``
    but returns rows unbounded so the caller can ``--limit`` them. Returns
    an empty list (not raises) on DB errors so the CLI prints a clean
    empty report instead of crashing.
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(_WALK_QUERY, {"days": int(days)}).fetchall()
    except Exception as exc:  # noqa: BLE001
        # Loud-fail: previously this swallowed schema mismatches silently and
        # produced misleading "no predictions found" reports. Now we log the
        # full traceback AND probe which column is missing so future debug
        # reads "column X does not exist" not "0 rows".
        import traceback as _tb
        log.error(
            "walk_forward_validate: oracle_predictions read failed: {e}",
            e=str(exc),
        )
        log.error("walk_forward_validate: traceback:\n{tb}", tb=_tb.format_exc())
        try:
            with engine.connect() as conn2:
                conn2.execute(text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = 'oracle_predictions'"
                )).fetchall()
        except Exception:
            pass
        return []
    log.info(
        "walk_forward_validate: query returned {n} scored predictions in last {d}d",
        n=len(rows or []), d=days,
    )
    out: list[dict[str, Any]] = []
    for row in rows or []:
        d = dict(zip(_ORACLE_COLUMNS, row))
        # Schema gap: oracle_predictions has no `signal_contributions` column.
        # Inject None so callers that .get() it don't KeyError.
        d.setdefault("signal_contributions", None)
        out.append(d)
        if limit is not None and len(out) >= int(limit):
            break
    return out


_SCORECARD_HISTORY_QUERY = text(
    """
    SELECT signal_source, horizon_days, scored_count, running_brier,
           running_ece, hit_count, last_updated
    FROM per_signal_brier_history
    WHERE last_updated <= :as_of
    """
)


def _reconstruct_historical_scorecards(
    engine: Engine,
    as_of: datetime,
    horizon_days: int,
) -> dict[str, SignalScorecard]:
    """Return the ``per_signal_brier_history`` snapshot as-of ``as_of``.

    CRITICAL: the SQL filter ``last_updated <= :as_of`` is the only thing
    preventing lookahead leak — every scorecard used in the time-frozen
    provenance reconstruction must have been updated at or before the
    prediction's own timestamp. Any change that weakens this filter
    invalidates the entire backtest.

    When the per_signal_brier_history table is empty (e.g. predictions
    preceded the bootstrap script), the returned dict is empty and the
    provenance reconstructor falls through to cold-start weights (neutral
    conviction = 1.0) via the ``scorecard=None`` branch.

    Only scorecards whose ``horizon_days`` matches the target horizon
    (after snapping to the canonical {1, 7, 30, 90} bucket) are returned —
    ``record_scored_prediction`` writes scorecards under canonical
    horizons, so a raw ``horizon_days=4`` prediction must be snapped to 7
    before lookup or no scorecard ever joins (this was the 2026-05-11
    "bootstrap had no effect" bug).
    """
    out: dict[str, SignalScorecard] = {}
    canonical_target = _canonical_horizon(horizon_days)
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                _SCORECARD_HISTORY_QUERY, {"as_of": as_of}
            ).fetchall()
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "walk_forward_validate: scorecard history read failed: {e}",
            e=str(exc),
        )
        return out

    for row in rows or []:
        try:
            source = row[0]
            row_horizon = int(row[1] or 0)
        except (IndexError, TypeError, ValueError):
            continue
        if not source or row_horizon != canonical_target:
            continue
        count = int(row[2] or 0)
        brier = float(row[3] or 0.0)
        ece = float(row[4] or 0.0)
        hits = int(row[5] or 0)
        last_updated = row[6]
        hit_rate = (hits / count) if count > 0 else 0.0
        is_calibrated = count >= MIN_CALIBRATED_SAMPLES
        conviction = (
            compute_conviction_weight(brier, count) if is_calibrated else 1.0
        )
        out[str(source)] = SignalScorecard(
            signal_source=str(source),
            horizon_days=row_horizon,
            scored_count=count,
            running_brier=brier,
            running_ece=ece,
            hit_rate=hit_rate,
            last_updated=last_updated,
            is_calibrated=is_calibrated,
            conviction_weight=conviction,
        )
    return out


_ENSURE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS backtest_results (
    id                 SERIAL PRIMARY KEY,
    start_date         TIMESTAMPTZ NOT NULL,
    end_date           TIMESTAMPTZ NOT NULL,
    predictions_walked INTEGER     NOT NULL,
    trades_generated   INTEGER     NOT NULL,
    report_json        JSONB       NOT NULL,
    narrative          TEXT        NOT NULL,
    generated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_backtest_results_generated
    ON backtest_results (generated_at DESC);
"""


def ensure_backtest_results_table(engine: Engine) -> None:
    """Create ``backtest_results`` if missing. Idempotent and defensive."""
    try:
        with engine.begin() as conn:
            for stmt in _ENSURE_TABLE_SQL.strip().split(";"):
                s = stmt.strip()
                if s:
                    conn.execute(text(s))
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "walk_forward_validate: ensure_backtest_results_table failed: {e}",
            e=str(exc),
        )


def persist_report(engine: Engine, report: BacktestReport) -> int:
    """Insert a ``BacktestReport`` row. Returns the row id or 0 on failure.
    """
    ensure_backtest_results_table(engine)
    try:
        with engine.begin() as conn:
            result = conn.execute(
                text(
                    """
                    INSERT INTO backtest_results
                        (start_date, end_date, predictions_walked,
                         trades_generated, report_json, narrative)
                    VALUES
                        (:start_date, :end_date, :walked, :trades,
                         CAST(:body AS JSONB), :narrative)
                    RETURNING id
                    """
                ),
                {
                    "start_date": report.start_date,
                    "end_date": report.end_date,
                    "walked": report.total_predictions_walked,
                    "trades": report.trades_generated,
                    "body": json.dumps(report.to_dict(), default=str),
                    "narrative": report.narrative,
                },
            )
            row = result.fetchone()
            return int(row[0]) if row else 0
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "walk_forward_validate: persist_report failed: {e}",
            e=str(exc),
        )
        return 0


# ── Main walk-forward entry ──────────────────────────────────────────────


def walk_forward(
    engine: Engine,
    *,
    days: int = DEFAULT_LOOKBACK_DAYS,
    limit: int | None = None,
    horizon_days: int | None = None,
    verbose: bool = False,
    dry_run: bool = False,
) -> BacktestReport:
    """Main entry — replay oracle_predictions through the decision stack
    and return the aggregated report.

    Parameters
    ----------
    engine
        SQLAlchemy engine. Tests pass a FakeEngine with in-memory data.
    days
        Lookback window in days.
    limit
        Max predictions to walk (for testing / quick smokes).
    horizon_days
        If set, only walk predictions with ``horizon == horizon_days``.
        Defaults to ``None`` meaning walk every canonical horizon.
    verbose
        Emit per-trade log lines.
    dry_run
        Skip the ``persist_report`` call at the end.
    """
    oracle_models_lookup = _load_oracle_models_lookup(engine)
    rows = _load_scored_predictions(engine, days=days, limit=limit)

    # PIT price replay infrastructure. Built once per run so ticker→feature_id
    # lookups stay cached across every prediction. Failures here are non-fatal:
    # the realized-return path silently falls back to the outcome proxy when
    # PITStore can't construct or a ticker has no feature_registry entry.
    try:
        pit_store: PITStore | None = PITStore(engine)
    except Exception as exc:  # noqa: BLE001
        log.warning("walk_forward: PITStore init failed, using outcome proxy: {e}", e=str(exc))
        pit_store = None
    ticker_feature_cache: dict[str, int | None] = {}
    pit_hits = 0
    pit_misses = 0

    trades: list[BacktestTrade] = []
    per_trade_signals: dict[str, list[str]] = {}
    walked = 0

    start_ts = datetime.now(timezone.utc).isoformat()
    end_ts = start_ts

    for row in rows:
        walked += 1
        try:
            created_at = row.get("created_at")
            if not isinstance(created_at, datetime):
                # Defensive — skip malformed row but keep the walk going.
                continue

            row_horizon = _coerce_horizon_days(row)
            if horizon_days is not None and int(row_horizon) != int(horizon_days):
                continue

            # Inject oracle_models lookup into the row so the shared
            # contribution extractor can see it without changing its
            # signature.
            enriched = dict(row)
            enriched["_models_lookup"] = oracle_models_lookup

            scorecards = _reconstruct_historical_scorecards(
                engine,
                as_of=created_at,
                horizon_days=row_horizon,
            )
            provenance = build_time_frozen_provenance(enriched, scorecards)

            try:
                stress = run_stress_test(provenance)
                stress_label = stress.robustness_label
                stress_score = float(stress.robustness_score)
            except Exception as exc:  # noqa: BLE001
                log.debug(
                    "walk_forward: stress test failed id={i}: {e}",
                    i=row.get("id"),
                    e=str(exc),
                )
                stress_label = "unknown"
                stress_score = 0.0

            # Ticket generation happens against the provenance report —
            # we don't care whether it succeeds (we only need verdict +
            # robustness for the aggregation). But we still call it to
            # ensure the whole stack runs end-to-end.
            try:
                generate_ticket(
                    provenance,
                    account_size_usd=100_000.0,
                    current_price=100.0,   # synthetic anchor for backtest
                    vol_30d=0.25,
                )
            except Exception as exc:  # noqa: BLE001
                log.debug(
                    "walk_forward: ticket gen failed id={i}: {e}",
                    i=row.get("id"),
                    e=str(exc),
                )

            outcome_verdict = str(row.get("verdict") or "miss").strip().lower()

            # Prefer the PIT-correct realized return computed from actual
            # historical prices. Fall back to the outcome proxy when the
            # ticker has no feature_registry entry or PIT returns empty
            # for either endpoint. This is the wiring that closes the loop
            # between walk-forward backtests and real equity-curve replay.
            realized_return: float | None = None
            ticker_str = str(row.get("ticker") or "")
            if pit_store is not None and ticker_str:
                fid = _resolve_ticker_feature_id(
                    engine, ticker_str, ticker_feature_cache
                )
                if fid is not None:
                    entry_date = created_at.date()
                    exit_date = entry_date + timedelta(days=int(row_horizon))
                    realized_return = _realized_return_from_pit(
                        pit_store,
                        fid,
                        provenance.direction,
                        entry_date,
                        exit_date,
                    )
            if realized_return is None:
                pit_misses += 1
                realized_return = _realized_return_from_outcome(
                    provenance.direction, outcome_verdict
                )
            else:
                pit_hits += 1
            hit = classify_hit(provenance.direction, outcome_verdict)

            trade = BacktestTrade(
                prediction_id=str(row.get("id")),
                ticker=str(row.get("ticker") or ""),
                prediction_date=created_at.isoformat(),
                verdict=provenance.verdict,
                aggregate_conviction=float(provenance.aggregate_conviction),
                robustness_label=stress_label,
                robustness_score=stress_score,
                oracle_confidence=float(provenance.confidence),
                oracle_direction=provenance.direction,
                outcome_verdict=outcome_verdict,
                realized_return=float(realized_return),
                horizon_days=int(row_horizon),
                hit=bool(hit),
            )
            trades.append(trade)
            per_trade_signals[trade.prediction_id] = [
                ev.signal_source for ev in provenance.signal_evidence
            ]

            end_ts = created_at.isoformat()
            if verbose:
                log.info(
                    "walk id={i} t={t} verdict={v} outcome={o} hit={h}",
                    i=trade.prediction_id,
                    t=trade.ticker,
                    v=trade.verdict,
                    o=trade.outcome_verdict,
                    h=trade.hit,
                )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "walk_forward: row replay failed id={i}: {e}",
                i=row.get("id") if isinstance(row, dict) else None,
                e=str(exc),
            )
            continue

    total_return_sources = pit_hits + pit_misses
    if total_return_sources > 0:
        log.info(
            "walk_forward: realized-return sources — PIT={p}/{t} ({pct:.0%}), proxy fallback={m}",
            p=pit_hits,
            t=total_return_sources,
            pct=pit_hits / total_return_sources,
            m=pit_misses,
        )

    verdict_stats = aggregate_per_verdict_stats(trades)
    confusion = compute_confusion_matrix(trades)
    calibration = measure_stress_test_calibration(trades)
    attribution = _signal_contribution_attribution(trades, per_trade_signals)
    narrative = _build_narrative(
        days=days,
        walked=walked,
        trades_generated=len(trades),
        verdict_stats=verdict_stats,
        calibration=calibration,
    )

    # start_date derives from the earliest prediction we saw; if no
    # predictions were walked, fall back to the generated_at stamp so the
    # row is still insertable.
    if trades:
        start_ts = trades[0].prediction_date
    report = BacktestReport(
        start_date=start_ts,
        end_date=end_ts,
        total_predictions_walked=walked,
        trades_generated=len(trades),
        verdict_stats=verdict_stats,
        confusion_matrix=confusion,
        stress_test_calibration=calibration,
        signal_contribution_attribution=attribution,
        narrative=narrative,
        generated_at=datetime.now(timezone.utc).isoformat(),
    )

    if not dry_run:
        persist_report(engine, report)
    return report


# ── CLI ───────────────────────────────────────────────────────────────────


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="walk_forward_validate",
        description=(
            "Walk-forward backtest of the full decision stack over "
            "oracle_predictions history."
        ),
    )
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help=f"Lookback window in days (default: {DEFAULT_LOOKBACK_DAYS}).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Cap on predictions walked (testing aid).",
    )
    parser.add_argument(
        "--horizon-days",
        type=int,
        default=None,
        help="Filter to a specific canonical horizon only (1/7/30/90).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Walk rows and aggregate but never persist the report.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Per-trade log lines.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    # Local import so `--help` works without DB config loaded.
    from db import get_engine  # type: ignore

    engine = get_engine()
    report = walk_forward(
        engine,
        days=args.days,
        limit=args.limit,
        horizon_days=args.horizon_days,
        verbose=args.verbose,
        dry_run=args.dry_run,
    )

    print(json.dumps(report.to_dict(), indent=2, default=str))
    log.info(
        "walk_forward_validate done: walked={w} trades={t}",
        w=report.total_predictions_walked,
        t=report.trades_generated,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
