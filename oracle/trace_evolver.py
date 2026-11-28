"""
GRID Oracle — Trace-Based Self-Evolution Engine.

Inspired by Nous Research's hermes-agent-self-evolution (GEPA pattern):
instead of random mutations, reads execution traces (postmortems, scoring
data, signal attribution) to understand WHY models fail, then proposes
TARGETED mutations that address specific failure modes.

Pipeline:
  1. TraceAnalyzer  — reads postmortems + oracle scoring → failure patterns
  2. TargetedMutator — proposes mutations addressing identified failure modes
  3. EvolutionGate  — constraint validation before candidates advance
  4. TraceEvolver   — orchestrates the full cycle

Integration:
  Called only from OracleEngine.run_cycle (oracle/engine.py), alongside
  ModelEvolver; that is reached by ``python oracle/run_cycle.py`` without
  ``--predict-only``. Hermes's Oracle slot (scripts/hermes_operator.py
  ``_run_oracle_forward_pass``) holds evolution and runs neither evolver.
  TraceEvolver is the "smart" layer; ModelEvolver remains the "random" layer.
  Together they implement explore (random) + exploit (trace-informed) evolution.

References:
  - GEPA: Genetic-Pareto Prompt Evolution (ICLR 2026 Oral)
  - hermes-agent-self-evolution: github.com/NousResearch/hermes-agent-self-evolution
"""

from __future__ import annotations

import json
import random
import re
import string
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from oracle.model_evolver import (
    SHADOW_TAG,
    _canonical_families,
    _tag_shadow_description,
)
from store.journal_qualification import DECISION_JOURNAL, non_provisional_journal_select


# ── Configuration ─────────────────────────────────────────────────────────

TRACE_LOOKBACK_DAYS = 30
MIN_POSTMORTEMS_FOR_ANALYSIS = 5
MIN_SCORED_PREDICTIONS = 10
MAX_MUTATIONS_PER_CYCLE = 3
SIGNAL_WRONG_THRESHOLD = 0.4      # signal wrong >40% of the time → demote
SIGNAL_RIGHT_THRESHOLD = 0.6      # signal right >60% of the time → promote
CONVERGENCE_BONUS = 0.15           # bonus weight for multi-source convergent signals
MIN_SIGNAL_SOURCES = 2
MAX_SIGNAL_SOURCES = 15
REGIME_AWARE = True                # adjust mutations per market regime
MIN_PATTERN1_FAIL_MENTIONS = 2
MIN_MISSED_TOKEN_LENGTH = 5

_PROSE_TOKEN_RE = re.compile(r"[A-Za-z0-9_]+(?:[:-][A-Za-z0-9_]+)*")
_MISSED_PROSE_STOPWORDS: frozenset[str] = frozenset({
    "about", "above", "after", "again", "against", "already", "always",
    "awareness", "because", "before", "being", "below", "between", "blocked",
    "calendar", "capture", "catalysts", "check", "checked", "consider",
    "could", "direction", "during", "earnings", "ensemble", "event", "every",
    "expiry", "first", "flagged", "found", "freshness", "gating", "going",
    "great", "having", "horizons", "however", "ignored", "included", "logic",
    "longer", "market", "meetings", "might", "missed", "model", "moved",
    "needed", "never", "other", "points", "position", "prediction", "present",
    "price", "recommendation", "rolled", "sanity", "short", "should", "signal",
    "signals", "since", "stale", "still", "their", "there", "these", "thing",
    "think", "those", "three", "through", "timing", "trade", "trailing",
    "under", "until", "using", "where", "which", "while", "would", "wrong",
})


def _extract_prose_tokens(prose: str) -> list[str]:
    """Extract whole tokens (including colon/underscore/hyphen registry names)."""
    return _PROSE_TOKEN_RE.findall(prose)


def _extract_prediction_signal_names(signals_raw: Any) -> set[str]:
    """Extract distinct signal names from an oracle_predictions.signals JSONB payload."""
    payload = signals_raw
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except (json.JSONDecodeError, TypeError):
            return set()
    if isinstance(payload, dict):
        items = payload.get("items", [])
    elif isinstance(payload, list):
        items = payload
    else:
        return set()
    names: set[str] = set()
    if isinstance(items, list):
        for item in items:
            if isinstance(item, dict):
                name = item.get("name")
                if isinstance(name, str) and name.strip():
                    names.add(name.strip())
            elif isinstance(item, str) and item.strip():
                names.add(item.strip())
    return names


def _rand(n: int = 6) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=n))


@dataclass(frozen=True)
class ScoredPrediction:
    """One eligible Oracle prediction for Pattern 1.

    Eligible means: scored inside the lookback, verdict hit/miss/partial,
    ``dedup_keep = TRUE`` and not an AstroGrid row. Pattern 1's failures and
    its fire denominator both come from these prediction ids only.
    """
    prediction_id: str
    verdict: str
    model_name: str | None
    signals: frozenset[str]


def _signal_fire_counts(
    eligible: Mapping[str, ScoredPrediction],
) -> tuple[dict[str, int], int, int]:
    """Per-signal fires (one per prediction id and signal), winners, total."""
    fires: Counter[str] = Counter()
    winning = 0
    for pred in eligible.values():
        if pred.verdict in ("hit", "partial"):
            winning += 1
        fires.update(pred.signals)
    return dict(fires), winning, len(eligible)


def _eligible_failure_postmortems(
    postmortems: list[dict],
    eligible: Mapping[str, ScoredPrediction],
) -> list[dict]:
    """Postmortems of eligible failed (miss) predictions, one per prediction id.

    The newest postmortem wins (``_load_postmortems`` orders by generated_at
    DESC). Paper-trade postmortems (no prediction id) and postmortems of
    predictions outside the eligible set are dropped, so they cannot add to a
    failure count whose denominator never saw them (Codex review 6011905413).
    """
    kept: list[dict] = []
    seen: set[str] = set()
    for pm in postmortems:
        pid = pm.get("prediction_id")
        pred = eligible.get(pid) if isinstance(pid, str) else None
        if pred is None or pred.verdict != "miss" or pid in seen:
            continue
        seen.add(pid)
        kept.append({**pm, "model_name": pred.model_name})
    return kept


def _signal_failure_counts(
    postmortems: list[dict],
    key: str,
    eligible: Mapping[str, ScoredPrediction],
) -> dict[str, tuple[int, int]]:
    """``signal -> (failures, fires)`` over the same eligible prediction ids.

    A failure is one (prediction id, signal) pair: the prediction is eligible
    and a miss, the signal fired in it, and a postmortem of it lists the signal
    under ``key``. Fires count eligible predictions in which the signal fired.
    Failures are therefore a subset of fires.
    """
    fires, _winning, _total = _signal_fire_counts(eligible)
    failed: set[tuple[str, str]] = set()
    for pm in postmortems:
        pid = pm.get("prediction_id")
        pred = eligible.get(pid) if isinstance(pid, str) else None
        if pred is None or pred.verdict != "miss":
            continue
        signals = pm.get(key, [])
        if not isinstance(signals, list):
            continue
        for sig in signals:
            if isinstance(sig, str) and sig in pred.signals:
                failed.add((pid, sig))
    failures = Counter(sig for _pid, sig in failed)
    return {sig: (failures.get(sig, 0), n) for sig, n in fires.items()}


# ── Data Classes ──────────────────────────────────────────────────────────

@dataclass(frozen=True)
class FailurePattern:
    """A recurring failure mode identified from postmortem traces."""
    category: str                  # wrong_signal, right_signal_wrong_timing, etc.
    frequency: int                 # how many times this pattern appeared
    fraction: float                # fraction of total failures
    signals_commonly_wrong: list[str]
    signals_commonly_right: list[str]
    signals_commonly_missed: list[str]
    affected_models: list[str]
    regime_context: str | None     # market regime when failures clustered
    recommended_action: str        # human-readable mutation suggestion

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class MutationProposal:
    """A specific, trace-informed mutation to apply to a model."""
    parent_model: str
    mutation_type: str             # add_signal, remove_signal, adjust_horizon,
                                   # adjust_min_signals, regime_filter, crossover_targeted
    description: str
    rationale: str                 # WHY this mutation (linked to failure pattern)
    params: dict[str, Any]         # mutation-specific parameters

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class EvolutionCycleResult:
    """Result of one trace-based evolution cycle."""
    patterns_found: list[dict] = field(default_factory=list)
    mutations_proposed: list[dict] = field(default_factory=list)
    mutations_applied: list[str] = field(default_factory=list)
    mutations_rejected: list[dict] = field(default_factory=list)
    trace_summary: dict = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {k: getattr(self, k) for k in self.__dataclass_fields__}


# ── 1. Trace Analyzer ────────────────────────────────────────────────────

class TraceAnalyzer:
    """Reads postmortem traces + scoring data to identify failure patterns.

    This is the GEPA insight adapted for trading: instead of reading LLM
    execution traces to find prompt failures, we read trade postmortems
    to find signal failures and propose targeted model mutations.
    """

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def analyze(self, lookback_days: int = TRACE_LOOKBACK_DAYS) -> list[FailurePattern]:
        """Analyze recent postmortems and scoring data for recurring patterns."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        postmortems = self._load_postmortems(cutoff)
        scoring = self._load_scoring_data(cutoff)
        signal_accuracy = self._load_signal_accuracy(cutoff)
        eligible = self._load_eligible_scored(cutoff)
        overall_fires, winning_outcomes, total_outcomes = _signal_fire_counts(eligible)
        registry_names = set(self._available_sources())
        regime = self._get_current_regime()

        if len(postmortems) < MIN_POSTMORTEMS_FOR_ANALYSIS:
            log.info("TraceAnalyzer: only {n} postmortems (need {m}), skipping",
                     n=len(postmortems), m=MIN_POSTMORTEMS_FOR_ANALYSIS)
            return []

        patterns = []

        # Pattern 1: Rate-based failure category clustering (requires overall fire
        # counts across BOTH winning and losing outcomes; trade_postmortems alone
        # only records failures, so disable Pattern 1 when no winning+losing
        # denominator is available). Failures and fires come from the same
        # eligible prediction ids: postmortems of paper trades or of predictions
        # outside that set do not count (Codex review 6011905413).
        if winning_outcomes <= 0 or total_outcomes <= 0 or not overall_fires:
            log.warning(
                "TraceAnalyzer: Pattern 1 disabled — trade_postmortems contains "
                "failures only and no winning+losing signal fire denominator is "
                "available (winning_outcomes={w}, total_outcomes={t}, signals={s})",
                w=winning_outcomes, t=total_outcomes, s=len(overall_fires),
            )
        else:
            failure_pms = _eligible_failure_postmortems(postmortems, eligible)
            if len(failure_pms) < len(postmortems):
                log.info(
                    "TraceAnalyzer: Pattern 1 uses {k} of {n} postmortems (one per "
                    "eligible failed Oracle prediction; paper-trade, ineligible and "
                    "repeat postmortems dropped)",
                    k=len(failure_pms), n=len(postmortems),
                )
            cat_counts = Counter(pm["failure_category"] for pm in failure_pms)
            for cat, count in cat_counts.most_common():
                if count < 2:
                    continue
                cat_pms = [pm for pm in failure_pms if pm["failure_category"] == cat]
                wrong_signals = self._aggregate_signals(
                    cat_pms, "signals_wrong",
                    eligible=eligible,
                    min_rate=SIGNAL_WRONG_THRESHOLD,
                )
                right_signals = self._aggregate_signals(
                    cat_pms, "signals_right",
                    eligible=eligible,
                    min_rate=0.0,
                )
                missed_signals = self._aggregate_missed(
                    cat_pms, registry_names=registry_names,
                )
                if cat == "wrong_signal" and not wrong_signals and not missed_signals:
                    continue
                affected = sorted(
                    {pm["model_name"] for pm in cat_pms if pm.get("model_name")}
                ) or ["unknown"]

                action = self._recommend_action(cat, wrong_signals, right_signals, missed_signals)

                patterns.append(FailurePattern(
                    category=cat,
                    frequency=count,
                    fraction=round(min(1.0, count / total_outcomes), 3),
                    signals_commonly_wrong=wrong_signals[:5],
                    signals_commonly_right=right_signals[:5],
                    signals_commonly_missed=missed_signals[:3],
                    affected_models=affected,
                    regime_context=regime,
                    recommended_action=action,
                ))

        # Pattern 2: Signal-level accuracy from trust_scorer data
        for sig_name, accuracy in signal_accuracy.items():
            if accuracy["total"] < 5:
                continue
            hit_rate = accuracy["correct"] / accuracy["total"]
            if hit_rate < SIGNAL_WRONG_THRESHOLD:
                patterns.append(FailurePattern(
                    category="chronic_bad_signal",
                    frequency=accuracy["total"],
                    fraction=round(1.0 - hit_rate, 3),
                    signals_commonly_wrong=[sig_name],
                    signals_commonly_right=[],
                    signals_commonly_missed=[],
                    affected_models=accuracy.get("models_using", []),
                    regime_context=regime,
                    recommended_action=f"Remove or downweight '{sig_name}' (hit rate {hit_rate:.1%})",
                ))

        # Pattern 3: Model-level underperformance from scoring
        for model_name, stats in scoring.items():
            if stats["total"] < MIN_SCORED_PREDICTIONS:
                continue
            hit_rate = stats["hits"] / stats["total"]
            if hit_rate < 0.30:
                patterns.append(FailurePattern(
                    category="model_underperformance",
                    frequency=stats["total"],
                    fraction=round(1.0 - hit_rate, 3),
                    signals_commonly_wrong=[],
                    signals_commonly_right=[],
                    signals_commonly_missed=[],
                    affected_models=[model_name],
                    regime_context=regime,
                    recommended_action=f"Restructure '{model_name}' signal mix (hit rate {hit_rate:.1%})",
                ))

        log.info("TraceAnalyzer: found {n} failure patterns from {pm} postmortems",
                 n=len(patterns), pm=len(postmortems))
        return patterns

    def get_trace_summary(self, lookback_days: int = TRACE_LOOKBACK_DAYS) -> dict[str, Any]:
        """Return a high-level summary of trace data for logging."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        postmortems = self._load_postmortems(cutoff)
        scoring = self._load_scoring_data(cutoff)

        total_pms = len(postmortems)
        cat_dist = dict(Counter(pm["failure_category"] for pm in postmortems))
        total_scored = sum(s["total"] for s in scoring.values())
        total_hits = sum(s["hits"] for s in scoring.values())

        return {
            "postmortems_analyzed": total_pms,
            "failure_distribution": cat_dist,
            "predictions_scored": total_scored,
            "overall_hit_rate": round(total_hits / total_scored, 3) if total_scored else 0,
            "lookback_days": lookback_days,
        }

    def _load_postmortems(self, cutoff: datetime) -> list[dict]:
        try:
            with self.engine.connect() as conn:
                # prediction_id is NULL for paper-trade postmortems. Pattern 1
                # joins it to the eligible scored predictions in Python, which
                # also supplies model_name (_eligible_failure_postmortems).
                rows = conn.execute(text("""
                    SELECT tp.ticker, tp.outcome, tp.failure_category, tp.root_cause,
                           tp.signals_wrong, tp.signals_right, tp.what_we_missed,
                           tp.recommended_fix, tp.confidence, tp.generated_at,
                           tp.prediction_id
                    FROM trade_postmortems tp
                    WHERE tp.generated_at >= :cutoff
                    ORDER BY tp.generated_at DESC
                """), {"cutoff": cutoff}).fetchall()
            return [
                {
                    "ticker": r[0], "outcome": r[1], "failure_category": r[2] or "unknown",
                    "root_cause": r[3], "signals_wrong": _parse_json(r[4]),
                    "signals_right": _parse_json(r[5]), "what_we_missed": r[6],
                    "recommended_fix": r[7], "confidence": r[8],
                    "generated_at": r[9],
                    "prediction_id": str(r[10]) if r[10] is not None else None,
                }
                for r in rows
            ]
        except Exception as e:
            log.warning("TraceAnalyzer: failed to load postmortems: {e}", e=str(e))
            return []

    def _load_scoring_data(self, cutoff: datetime) -> dict[str, dict]:
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(text("""
                    SELECT model_name,
                           COUNT(*) FILTER (WHERE verdict = 'hit') AS hits,
                           COUNT(*) FILTER (WHERE verdict = 'miss') AS misses,
                           COUNT(*) FILTER (WHERE verdict = 'partial') AS partials,
                           COUNT(*) AS total,
                           AVG(CASE WHEN pnl_pct IS NOT NULL THEN pnl_pct ELSE 0 END) AS avg_pnl
                    FROM oracle_predictions
                    WHERE scored_at >= :cutoff
                      AND verdict IN ('hit', 'miss', 'partial')
                      AND dedup_keep = TRUE
                      AND id NOT LIKE 'astrogrid:%'
                    GROUP BY model_name
                """), {"cutoff": cutoff}).fetchall()
            return {
                r[0]: {"hits": r[1], "misses": r[2], "partials": r[3],
                       "total": r[4], "avg_pnl": float(r[5] or 0)}
                for r in rows
            }
        except Exception as e:
            log.warning("TraceAnalyzer: failed to load scoring data: {e}", e=str(e))
            return {}

    def _load_signal_accuracy(self, cutoff: datetime) -> dict[str, dict]:
        """Load signal-level accuracy from trust_scorer data."""
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(text("""
                    SELECT source_type,
                           COUNT(*) FILTER (WHERE outcome = 'CORRECT') AS correct,
                           COUNT(*) FILTER (WHERE outcome = 'WRONG') AS wrong,
                           COUNT(*) AS total
                    FROM signal_sources
                    WHERE signal_date >= :cutoff AND outcome IN ('CORRECT', 'WRONG')
                    GROUP BY source_type
                """), {"cutoff": cutoff}).fetchall()
            return {
                r[0]: {"correct": r[1], "wrong": r[2], "total": r[3]}
                for r in rows
            }
        except Exception as e:
            log.warning("TraceAnalyzer: signal accuracy query failed: {e}", e=str(e))
            return {}

    def _get_current_regime(self) -> str | None:
        """Get the current market regime from the latest decision journal entry."""
        try:
            with self.engine.connect() as conn:
                row = conn.execute(
                    non_provisional_journal_select(
                        "inferred_state",
                    )
                    .order_by(DECISION_JOURNAL.c.id.desc())
                    .limit(1)
                ).fetchone()
            return row[0] if row else None
        except Exception:
            return None

    def _load_eligible_scored(self, cutoff: datetime) -> dict[str, ScoredPrediction]:
        """Eligible scored Oracle predictions by id: both sides of Pattern 1.

        Returns {} on a query failure, which disables Pattern 1 (fail closed).
        """
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(text("""
                    SELECT id, verdict, model_name, signals
                    FROM oracle_predictions
                    WHERE scored_at >= :cutoff
                      AND verdict IN ('hit', 'miss', 'partial')
                      AND dedup_keep = TRUE
                      AND id NOT LIKE 'astrogrid:%'
                """), {"cutoff": cutoff}).fetchall()
        except Exception as e:
            log.warning("TraceAnalyzer: eligible scored predictions query failed: {e}", e=str(e))
            return {}

        eligible: dict[str, ScoredPrediction] = {}
        for pid, verdict, model_name, signals_raw in rows:
            if pid is None or verdict not in ("hit", "miss", "partial"):
                continue
            key = str(pid)
            eligible[key] = ScoredPrediction(
                prediction_id=key,
                verdict=verdict,
                model_name=model_name,
                signals=frozenset(_extract_prediction_signal_names(signals_raw)),
            )
        return eligible

    def _available_sources(self) -> list[str]:
        try:
            with self.engine.connect() as conn:
                return [
                    r[0] for r in conn.execute(text(
                        "SELECT DISTINCT source_module FROM signal_registry ORDER BY source_module"
                    )).fetchall()
                    if r and r[0]
                ]
        except Exception as e:
            log.warning("TraceAnalyzer: available sources query failed: {e}", e=str(e))
            return []

    def _aggregate_signals(
        self,
        postmortems: list[dict],
        key: str,
        *,
        eligible: Mapping[str, ScoredPrediction] | None = None,
        min_rate: float = SIGNAL_WRONG_THRESHOLD,
        min_mentions: int = MIN_PATTERN1_FAIL_MENTIONS,
    ) -> list[str]:
        """Rank signals by failure rate (failed fires / all fires, same ids).

        Both sides come from ``eligible`` (see ``_signal_failure_counts``).
        Fails closed (returns []) when no eligible predictions are supplied.
        """
        if not eligible:
            log.warning(
                "TraceAnalyzer._aggregate_signals: disabled — no eligible scored predictions"
            )
            return []
        counts = _signal_failure_counts(postmortems, key, eligible)
        scored_signals: list[tuple[float, int, str]] = []
        for sig, (fail_count, total_fires) in counts.items():
            if fail_count < min_mentions or total_fires <= 0:
                continue
            rate = fail_count / total_fires
            if rate > min_rate:
                scored_signals.append((rate, fail_count, sig))
        scored_signals.sort(key=lambda item: (-item[0], -item[1], item[2]))
        return [sig for _, _, sig in scored_signals]

    def _aggregate_missed(
        self,
        postmortems: list[dict],
        registry_names: set[str] | list[str] | None = None,
    ) -> list[str]:
        """Extract missed signals by exact whole-token match against signal_registry."""
        names = self._available_sources() if registry_names is None else list(registry_names)
        canonical_by_lower = {
            n.strip().lower(): n.strip()
            for n in names
            if isinstance(n, str) and n.strip()
        }
        if not canonical_by_lower:
            return []
        counter: Counter[str] = Counter()
        for pm in postmortems:
            missed = pm.get("what_we_missed", "")
            if not missed or not isinstance(missed, str):
                continue
            matched_in_pm: set[str] = set()
            for token in _extract_prose_tokens(missed):
                lower = token.lower()
                if len(lower) < MIN_MISSED_TOKEN_LENGTH or lower in _MISSED_PROSE_STOPWORDS:
                    continue
                canonical = canonical_by_lower.get(lower)
                if canonical is not None:
                    matched_in_pm.add(canonical)
            counter.update(matched_in_pm)
        return [w for w, c in counter.most_common(10) if c >= 2]

    def _recommend_action(self, category: str, wrong: list[str],
                          right: list[str], missed: list[str]) -> str:
        actions = {
            "wrong_signal": f"Remove/downweight wrong signals: {wrong[:3]}",
            "right_signal_wrong_timing": "Extend target horizon or use adaptive expiry",
            "external_shock": "Add event-risk filter (FOMC, earnings, geopolitical)",
            "bad_data": "Add data freshness gate to affected sources",
            "model_error": "Review model logic for affected models",
            "insufficient_data": "No-op (ignore): insufficient data to attribute failure",
        }
        if category == "insufficient_data":
            return actions["insufficient_data"]
        base = actions.get(category, f"Investigate {category}")
        if missed:
            base += f"; consider adding missed signals: {missed[:3]}"
        return base


# ── 2. Targeted Mutator ──────────────────────────────────────────────────

class TargetedMutator:
    """Proposes model mutations that address specific failure patterns.

    Instead of random add/remove signal (ModelEvolver), this reads the
    TraceAnalyzer output and proposes mutations with clear rationale
    linked to observed failure modes.
    """

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def propose(self, patterns: list[FailurePattern],
                max_proposals: int = MAX_MUTATIONS_PER_CYCLE) -> list[MutationProposal]:
        """Generate targeted mutation proposals from failure patterns."""
        proposals: list[MutationProposal] = []

        for pattern in sorted(patterns, key=lambda p: p.frequency, reverse=True):
            if len(proposals) >= max_proposals:
                break

            new_proposals = self._proposals_for_pattern(pattern)
            proposals.extend(new_proposals)

        # Deduplicate by (parent_model, mutation_type)
        seen: set[tuple[str, str]] = set()
        unique: list[MutationProposal] = []
        for p in proposals:
            key = (p.parent_model, p.mutation_type)
            if key not in seen:
                seen.add(key)
                unique.append(p)

        return unique[:max_proposals]

    def _proposals_for_pattern(self, pattern: FailurePattern) -> list[MutationProposal]:
        proposals: list[MutationProposal] = []

        if pattern.category == "insufficient_data":
            return proposals

        if pattern.category == "wrong_signal" and pattern.signals_commonly_wrong:
            for model in pattern.affected_models:
                model_sources = self._get_model_sources(model)
                removable = [s for s in pattern.signals_commonly_wrong if s in model_sources]
                if removable and len(model_sources) > MIN_SIGNAL_SOURCES:
                    proposals.append(MutationProposal(
                        parent_model=model,
                        mutation_type="remove_signal",
                        description=f"Remove chronically wrong signal(s) from {model}",
                        rationale=f"Signals {removable[:2]} wrong in {pattern.frequency} "
                                  f"postmortems ({pattern.fraction:.0%} of failures)",
                        params={"signals_to_remove": removable[:2]},
                    ))

        elif pattern.category == "right_signal_wrong_timing":
            for model in pattern.affected_models:
                proposals.append(MutationProposal(
                    parent_model=model,
                    mutation_type="adjust_horizon",
                    description=f"Extend prediction horizon for {model}",
                    rationale=f"Direction correct but timing wrong in {pattern.frequency} cases",
                    params={"horizon_multiplier": 1.5},
                ))

        elif pattern.category == "external_shock":
            for model in pattern.affected_models:
                proposals.append(MutationProposal(
                    parent_model=model,
                    mutation_type="add_signal",
                    description=f"Add event-risk signal to {model}",
                    rationale=f"External shocks caused {pattern.frequency} failures; "
                              "adding event calendar awareness",
                    params={"signals_to_add": ["feature:event_risk", "feature:vol"]},
                ))

        elif pattern.category == "chronic_bad_signal":
            bad_signal = pattern.signals_commonly_wrong[0] if pattern.signals_commonly_wrong else None
            if bad_signal:
                for model in (pattern.affected_models or self._models_using_signal(bad_signal)):
                    proposals.append(MutationProposal(
                        parent_model=model,
                        mutation_type="remove_signal",
                        description=f"Remove underperforming signal '{bad_signal}' from {model}",
                        rationale=f"Signal '{bad_signal}' has {pattern.fraction:.0%} error rate "
                                  f"over {pattern.frequency} evaluations",
                        params={"signals_to_remove": [bad_signal]},
                    ))

        elif pattern.category == "model_underperformance":
            for model in pattern.affected_models:
                # Find the top-performing model to crossover with
                top_model = self._get_top_performer()
                if top_model and top_model != model:
                    proposals.append(MutationProposal(
                        parent_model=model,
                        mutation_type="crossover_targeted",
                        description=f"Crossover {model} with top performer {top_model}",
                        rationale=f"Model {model} underperforming ({pattern.fraction:.0%} miss rate); "
                                  f"crossing with {top_model}",
                        params={"donor_model": top_model, "take_ratio": 0.5},
                    ))

        # Add missed signal proposals (exact case-insensitive whole-name match only)
        if pattern.signals_commonly_missed:
            available = self._available_sources()
            missed_exact = {
                m.strip().lower()
                for m in pattern.signals_commonly_missed
                if isinstance(m, str)
                and len(m.strip()) >= MIN_MISSED_TOKEN_LENGTH
                and m.strip().lower() not in _MISSED_PROSE_STOPWORDS
            }
            for model in pattern.affected_models:
                addable = [s for s in available if s.lower() in missed_exact]
                if addable:
                    model_sources = self._get_model_sources(model)
                    new_signals = [s for s in addable if s not in model_sources][:2]
                    if new_signals:
                        proposals.append(MutationProposal(
                            parent_model=model,
                            mutation_type="add_signal",
                            description=f"Add missed signals to {model}",
                            rationale=f"Signals matching '{pattern.signals_commonly_missed[:2]}' "
                                      f"were missed in {pattern.frequency} failures",
                            params={"signals_to_add": new_signals},
                        ))

        return proposals

    def apply(self, proposal: MutationProposal) -> str | None:
        """Apply a mutation proposal, creating a new shadow model variant. Returns model name or None."""
        parent = self._load_model(proposal.parent_model)
        if not parent:
            log.warning("TargetedMutator: parent model '{m}' not found", m=proposal.parent_model)
            return None

        sources = _parse_json(parent.get("signal_sources"))
        parent_families = _parse_json(parent.get("signal_families"))
        families = list(parent_families)
        horizon = parent.get("target_horizon_days", 7)

        if proposal.mutation_type == "remove_signal":
            to_remove = set(proposal.params.get("signals_to_remove", []))
            new_sources = [s for s in sources if s not in to_remove]
            if len(new_sources) < MIN_SIGNAL_SOURCES:
                return None
            sources = new_sources

        elif proposal.mutation_type == "add_signal":
            to_add = proposal.params.get("signals_to_add", [])
            sources = list(dict.fromkeys(sources + to_add))  # dedupe, preserve order
            if len(sources) > MAX_SIGNAL_SOURCES:
                sources = sources[:MAX_SIGNAL_SOURCES]

        elif proposal.mutation_type == "adjust_horizon":
            multiplier = proposal.params.get("horizon_multiplier", 1.5)
            horizon = min(30, max(1, int(horizon * multiplier)))

        elif proposal.mutation_type == "crossover_targeted":
            donor_name = proposal.params.get("donor_model")
            donor = self._load_model(donor_name) if donor_name else None
            if not donor:
                return None
            donor_sources = _parse_json(donor.get("signal_sources"))
            take_ratio = proposal.params.get("take_ratio", 0.5)
            n_take = max(1, int(len(donor_sources) * take_ratio))
            donor_picks = random.sample(donor_sources, min(n_take, len(donor_sources)))
            sources = list(dict.fromkeys(sources + donor_picks))
            if len(sources) > MAX_SIGNAL_SOURCES:
                sources = sources[:MAX_SIGNAL_SOURCES]
            families = sorted(set(families) | set(_parse_json(donor.get("signal_families"))))

        else:
            log.warning("TargetedMutator: unknown mutation type '{t}'", t=proposal.mutation_type)
            return None

        # Refuse functional clones: OracleEngine._load_models reads signal_families
        # (not signal_sources or target_horizon_days). The comparison uses
        # signal_families only because GRID_SIGNAL_REGISTRY defaults off
        # (oracle/engine.py ~L44); if registry mode is enabled, signal_sources
        # becomes vote-relevant.
        parent_fam_sig = _canonical_families(parent_families)
        new_fam_sig = _canonical_families(families)
        if not new_fam_sig:
            log.info(
                "TargetedMutator: refusing {t} on '{p}' — empty vote-determining signal_families",
                t=proposal.mutation_type, p=proposal.parent_model,
            )
            return None
        if new_fam_sig == parent_fam_sig:
            log.info(
                "TargetedMutator: refusing {t} on '{p}' — vote-determining "
                "signal_families {f} unchanged (mutation only modified unread fields)",
                t=proposal.mutation_type, p=proposal.parent_model, f=list(new_fam_sig),
            )
            return None
        existing_sigs = self._active_or_shadow_family_signatures()
        if existing_sigs is None:
            log.warning(
                "TargetedMutator: refusing {t} on '{p}' — unable to verify active/shadow signal_families (fail closed)",
                t=proposal.mutation_type, p=proposal.parent_model,
            )
            return None
        if new_fam_sig in existing_sigs:
            log.info(
                "TargetedMutator: refusing {t} on '{p}' — vote-determining "
                "signal_families {f} matches an existing active or shadow model",
                t=proposal.mutation_type, p=proposal.parent_model, f=list(new_fam_sig),
            )
            return None

        name = f"trace_{proposal.mutation_type[:8]}_{_rand()}"
        desc = _tag_shadow_description(f"{proposal.description} | {proposal.rationale[:100]}")[:500]

        try:
            with self.engine.begin() as conn:
                res = conn.execute(text("""
                    INSERT INTO oracle_models
                    (name, version, description, signal_families, signal_sources,
                     weight, active, parent_model, created_by, target_horizon_days, last_updated)
                    VALUES (:name, '1.0', :desc, :fam, :src,
                            0.0, FALSE, :parent, :cb, :horizon, NOW())
                    ON CONFLICT (name) DO NOTHING
                """), {
                    "name": name, "desc": desc,
                    "fam": json.dumps(list(new_fam_sig)), "src": json.dumps(sources),
                    "parent": proposal.parent_model, "cb": "trace_evolver:" + proposal.mutation_type,
                    "horizon": horizon,
                })
            if getattr(res, "rowcount", 1) == 0:
                return None
            log.info("TargetedMutator: staged shadow model '{name}' from {parent} ({type})",
                     name=name, parent=proposal.parent_model, type=proposal.mutation_type)
            return name
        except Exception as e:
            log.error("TargetedMutator: failed to create model: {e}", e=str(e))
            return None

    def _active_or_shadow_family_signatures(self) -> set[tuple[str, ...]] | None:
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT signal_families FROM oracle_models "
                        "WHERE active = TRUE OR description LIKE :shadow_prefix"
                    ),
                    {"shadow_prefix": f"{SHADOW_TAG}%"},
                ).fetchall()
            return {_canonical_families(r[0]) for r in rows}
        except Exception as e:
            log.warning(
                "TargetedMutator: active/shadow signal_families query failed: {e}",
                e=str(e),
            )
            return None

    def _get_model_sources(self, model_name: str) -> list[str]:
        model = self._load_model(model_name)
        return _parse_json(model.get("signal_sources")) if model else []

    def _load_model(self, name: str) -> dict | None:
        try:
            with self.engine.connect() as conn:
                r = conn.execute(text(
                    "SELECT name, signal_sources, signal_families, description, "
                    "parent_model, target_horizon_days "
                    "FROM oracle_models WHERE name = :n"
                ), {"n": name}).fetchone()
            if not r:
                return None
            return {"name": r[0], "signal_sources": r[1], "signal_families": r[2],
                    "description": r[3], "parent_model": r[4],
                    "target_horizon_days": r[5]}
        except Exception:
            return None

    def _models_using_signal(self, signal: str) -> list[str]:
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(text("""
                    SELECT name FROM oracle_models
                    WHERE active = TRUE AND signal_sources::text LIKE :pattern
                """), {"pattern": f"%{signal}%"}).fetchall()
            return [r[0] for r in rows]
        except Exception:
            return []

    def _get_top_performer(self) -> str | None:
        try:
            with self.engine.connect() as conn:
                row = conn.execute(text("""
                    SELECT model_name,
                           COUNT(*) FILTER (WHERE verdict = 'hit') * 1.0 / NULLIF(COUNT(*), 0) AS hr
                    FROM oracle_predictions
                    WHERE verdict IN ('hit', 'miss', 'partial')
                      AND dedup_keep = TRUE
                      AND id NOT LIKE 'astrogrid:%'
                    GROUP BY model_name
                    HAVING COUNT(*) >= :min_preds
                    ORDER BY hr DESC LIMIT 1
                """), {"min_preds": MIN_SCORED_PREDICTIONS}).fetchone()
            return row[0] if row else None
        except Exception:
            return None

    def _available_sources(self) -> list[str]:
        try:
            with self.engine.connect() as conn:
                return [r[0] for r in conn.execute(text(
                    "SELECT DISTINCT source_module FROM signal_registry ORDER BY source_module"
                )).fetchall()]
        except Exception:
            return []


# ── 3. Evolution Gate ─────────────────────────────────────────────────────

class EvolutionGate:
    """Constraint validation before evolved candidates enter the population.

    Mirrors the Hermes GEPA safety gates adapted for trading models:
    - Size constraints (signal count bounds)
    - Semantic preservation (must keep core signal identity)
    - No duplicate models
    - Parent must still exist
    """

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def check(self, proposal: MutationProposal, parent_sources: list[str]) -> tuple[bool, str]:
        """Validate a mutation proposal. Returns (passed, reason)."""

        # Gate 1: Parent model must exist and be active
        try:
            with self.engine.connect() as conn:
                parent = conn.execute(text(
                    "SELECT active, signal_families FROM oracle_models WHERE name = :n"
                ), {"n": proposal.parent_model}).fetchone()
        except Exception as e:
            return False, f"gate db error checking parent: {e}"
        if not parent:
            return False, f"Parent model '{proposal.parent_model}' not found"
        if not parent[0]:
            return False, f"Parent model '{proposal.parent_model}' is inactive"
        parent_fam_sig = _canonical_families(parent[1])

        # Gate 2: Signal count bounds
        if proposal.mutation_type == "remove_signal":
            to_remove = set(proposal.params.get("signals_to_remove", []))
            remaining = len([s for s in parent_sources if s not in to_remove])
            if remaining < MIN_SIGNAL_SOURCES:
                return False, f"Would leave only {remaining} signals (min {MIN_SIGNAL_SOURCES})"

        if proposal.mutation_type in ("add_signal", "crossover_targeted"):
            to_add = proposal.params.get("signals_to_add", [])
            projected = len(set(parent_sources) | set(to_add))
            if projected > MAX_SIGNAL_SOURCES:
                return False, f"Would create {projected} signals (max {MAX_SIGNAL_SOURCES})"

        # Gate 3: No functional clones on vote-determining fields (signal_families,
        # the only model-definition field OracleEngine._load_models reads because
        # GRID_SIGNAL_REGISTRY defaults off in oracle/engine.py ~L44; if registry
        # mode is enabled, signal_sources becomes vote-relevant). Applies to ALL
        # mutation types including adjust_horizon, remove_signal, add_signal.
        if proposal.mutation_type == "crossover_targeted":
            donor_name = proposal.params.get("donor_model")
            if not donor_name:
                return False, "Missing donor_model for crossover_targeted"
            try:
                with self.engine.connect() as conn:
                    donor_row = conn.execute(text(
                        "SELECT signal_families FROM oracle_models WHERE name = :n"
                    ), {"n": donor_name}).fetchone()
            except Exception as e:
                return False, f"gate db error checking donor: {e}"
            if not donor_row:
                return False, f"Donor model '{donor_name}' not found"
            new_fam_sig = _canonical_families(
                list(parent_fam_sig) + list(_canonical_families(donor_row[0]))
            )
        else:
            new_fam_sig = parent_fam_sig

        if not new_fam_sig:
            return False, "Candidate has no vote-determining signal_families"
        if new_fam_sig == parent_fam_sig:
            return (
                False,
                f"Functional clone: mutation '{proposal.mutation_type}' does not change "
                f"vote-determining signal_families {list(new_fam_sig)}",
            )

        exists_clone, err = self._families_config_exists(new_fam_sig)
        if err is not None:
            return False, f"gate db error checking active/shadow signal_families: {err}"
        if exists_clone:
            return False, "Functional clone: identical vote-determining signal_families already exists on an active or shadow model"

        # Gate 4: Population cap
        try:
            with self.engine.connect() as conn:
                count = conn.execute(text(
                    "SELECT COUNT(*) FROM oracle_models WHERE active = TRUE"
                )).fetchone()[0]
        except Exception as e:
            return False, f"gate db error checking population: {e}"
        if count >= 50:  # MAX_ACTIVE_MODELS from model_evolver
            return False, f"Population at cap ({count}/50)"

        return True, "passed"

    def _families_config_exists(self, target_sig: tuple[str, ...]) -> tuple[bool, str | None]:
        """Check whether any active or [shadow] model has identical vote-determining signal_families."""
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT signal_families FROM oracle_models "
                        "WHERE active = TRUE OR description LIKE :shadow_prefix"
                    ),
                    {"shadow_prefix": f"{SHADOW_TAG}%"},
                ).fetchall()
            for (fam_raw,) in rows:
                if _canonical_families(fam_raw) == target_sig:
                    return True, None
            return False, None
        except Exception as e:
            log.warning(
                "EvolutionGate: active/shadow signal_families check failed (failing closed): {e}",
                e=str(e),
            )
            return True, str(e)


# ── 4. Trace Evolver (Orchestrator) ──────────────────────────────────────

class TraceEvolver:
    """Orchestrates trace-based self-evolution.

    Called from OracleEngine.run_cycle alongside ModelEvolver (not by Hermes,
    whose Oracle slot holds evolution).
    TraceEvolver = "exploit" (targeted), ModelEvolver = "explore" (random).

    Pipeline:
      1. TraceAnalyzer reads postmortems → failure patterns
      2. TargetedMutator proposes mutations addressing those patterns
      3. EvolutionGate validates constraints
      4. Apply mutations, log results
    """

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self.analyzer = TraceAnalyzer(engine)
        self.mutator = TargetedMutator(engine)
        self.gate = EvolutionGate(engine)

    def evolve_cycle(self, lookback_days: int = TRACE_LOOKBACK_DAYS) -> dict[str, Any]:
        """Run one trace-based evolution cycle."""
        result = EvolutionCycleResult()
        log.info("═══ TraceEvolver Cycle Starting ═══")

        # 1. Analyze traces
        try:
            patterns = self.analyzer.analyze(lookback_days)
            result.patterns_found = [p.to_dict() for p in patterns]
            result.trace_summary = self.analyzer.get_trace_summary(lookback_days)
        except Exception as e:
            result.errors.append(f"trace analysis: {e}")
            log.error("TraceEvolver: analysis failed: {e}", e=str(e))
            return result.to_dict()

        if not patterns:
            log.info("TraceEvolver: no actionable patterns found")
            self._log_iteration(result)
            return result.to_dict()

        # 2. Propose mutations
        try:
            proposals = self.mutator.propose(patterns)
            result.mutations_proposed = [p.to_dict() for p in proposals]
        except Exception as e:
            result.errors.append(f"mutation proposal: {e}")
            log.error("TraceEvolver: proposal failed: {e}", e=str(e))
            self._log_iteration(result)
            return result.to_dict()

        # 3. Gate-check and apply
        for proposal in proposals:
            parent_sources = self.mutator._get_model_sources(proposal.parent_model)

            passed, reason = self.gate.check(proposal, parent_sources)
            if not passed:
                result.mutations_rejected.append({
                    "proposal": proposal.to_dict(), "reason": reason,
                })
                log.info("TraceEvolver: rejected {t} on {m}: {r}",
                         t=proposal.mutation_type, m=proposal.parent_model, r=reason)
                continue

            try:
                new_name = self.mutator.apply(proposal)
                if new_name:
                    result.mutations_applied.append(new_name)
                    log.info("TraceEvolver: applied {t} → '{n}'",
                             t=proposal.mutation_type, n=new_name)
                else:
                    result.mutations_rejected.append({
                        "proposal": proposal.to_dict(), "reason": "apply returned None",
                    })
            except Exception as e:
                result.errors.append(f"apply {proposal.mutation_type}: {e}")

        self._log_iteration(result)
        log.info("═══ TraceEvolver Cycle Complete: {a} applied, {r} rejected, {e} errors ═══",
                 a=len(result.mutations_applied), r=len(result.mutations_rejected),
                 e=len(result.errors))
        return result.to_dict()

    def _log_iteration(self, result: EvolutionCycleResult) -> None:
        """Log evolution cycle to oracle_iterations for audit trail."""
        try:
            notes = (
                f"trace_evolver: {len(result.patterns_found)} patterns, "
                f"{len(result.mutations_applied)} applied, "
                f"{len(result.mutations_rejected)} rejected"
            )
            with self.engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO oracle_iterations
                    (models_updated, predictions_scored, best_model, best_hit_rate,
                     worst_model, worst_hit_rate, weight_changes, notes)
                    VALUES (:mu, 0, NULL, NULL, NULL, NULL, :wc, :notes)
                """), {
                    "mu": len(result.mutations_applied),
                    "wc": json.dumps({
                        "trace_evolver": True,
                        "patterns": len(result.patterns_found),
                        "applied": result.mutations_applied,
                        "rejected": [r.get("reason", "") for r in result.mutations_rejected],
                    }),
                    "notes": notes,
                })
        except Exception as e:
            log.warning("TraceEvolver: failed to log iteration: {e}", e=str(e))


# ── Helpers ───────────────────────────────────────────────────────────────

def _parse_json(v: Any) -> list[str]:
    """Safely parse a JSONB value into a list of strings."""
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x) for x in v]
    if isinstance(v, str):
        try:
            parsed = json.loads(v)
            return [str(x) for x in parsed] if isinstance(parsed, list) else []
        except (json.JSONDecodeError, TypeError):
            return []
    return []
