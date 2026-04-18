"""
Counterfactual stress test engine (CAT-175).

Per the user memory rule "every value on screen must be defensible," a
raw confidence number is not enough. A model confidence of 0.82 could
be a rock-solid 82% (every contributing signal points the same way and
all are well calibrated) OR a knife-edge 82% (one dominant signal at
0.95 dragging the rest along). The trader needs to know which one
they're staring at before they size the trade.

This module is the robustness check that complements
``intelligence.signal_provenance`` (the per-trade evidence report) and
``features.per_signal_brier`` (the per-signal calibration scorecards).
Given a pre-computed ``TradeProvenanceReport``, we perturb each
contributing signal by ±σ in the *adverse* direction (the direction
that would weaken the trade) and recompute the aggregate conviction at
each step. The output ``StressTestReport`` tells the trader:

  * how many perturbations flipped the verdict
  * which single signal — if any — could flip the verdict on its own
    at -2σ ("fragility signal")
  * a single ``robustness_score`` in [0, 1] = fraction of perturbations
    that did NOT flip the verdict
  * a one-sentence advisory naming the breaking signal (if any) so the
    operator can hedge or shrink position size

The whole module is pure: no engine, no DB, no network. We import the
canonical aggregate-conviction formula from ``signal_provenance`` and
the canonical conviction-weight formula from ``features.per_signal_brier``
so we never silently drift from the live scoring math.

CAT-175 deliverable. See also CAT-181 (LLM red-team risk),
ALPHA-9 (Shapley fragility multiplier), ALPHA-15 (per-signal Brier).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from features.per_signal_brier import (
    MIN_CALIBRATED_SAMPLES,
    SignalScorecard,
    compute_conviction_weight,
)
from intelligence.signal_provenance import (
    SignalEvidence,
    TradeProvenanceReport,
    _verdict_from_aggregate,
    compute_aggregate_conviction,
)


# ── Constants ─────────────────────────────────────────────────────────────

# The adverse perturbations we apply to every contributing signal. We do
# not test "what if everything went better" — the stress test is about
# the downside scenario only.
STRESS_PERTURBATION_SIGMAS: tuple[float, ...] = (-2.0, -1.0, +1.0)

# How much we shift Brier per σ in the conviction-weight space. A higher
# Brier always weakens conviction (lower Brier = better calibrated), so
# the adverse direction is "Brier goes up" regardless of sigma sign.
BRIER_PERTURBATION_DELTA: float = 0.05

# Robustness classification thresholds.
FRAGILITY_THRESHOLD: float = 0.7   # below → fragile
ROBUST_THRESHOLD: float = 0.9      # above → robust


# ── Data classes ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SignalPerturbation:
    """One perturbation row: the result of shifting a single signal by σ."""

    signal_source: str
    sigma: float
    perturbed_brier: float
    perturbed_conviction_weight: float
    new_aggregate_conviction: float
    new_verdict: str
    verdict_changed: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "signal_source": self.signal_source,
            "sigma": round(self.sigma, 4),
            "perturbed_brier": round(self.perturbed_brier, 6),
            "perturbed_conviction_weight": round(
                self.perturbed_conviction_weight, 4
            ),
            "new_aggregate_conviction": round(self.new_aggregate_conviction, 4),
            "new_verdict": self.new_verdict,
            "verdict_changed": self.verdict_changed,
        }


@dataclass(frozen=True)
class FragilityFlag:
    """Per-signal fragility verdict: did any single perturbation flip the call?"""

    signal_source: str
    fragile: bool
    reason: str
    breaking_sigma: float | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "signal_source": self.signal_source,
            "fragile": self.fragile,
            "reason": self.reason,
            "breaking_sigma": (
                round(self.breaking_sigma, 4)
                if self.breaking_sigma is not None
                else None
            ),
        }


@dataclass(frozen=True)
class StressTestReport:
    """Full counterfactual stress test report for one trade."""

    ticker: str
    original_verdict: str
    original_conviction: float
    perturbations: list[SignalPerturbation]
    fragility_flags: list[FragilityFlag]
    robustness_score: float
    robustness_label: str  # 'robust' / 'moderate' / 'fragile'
    break_count: int
    generated_at: str
    advisory: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "original_verdict": self.original_verdict,
            "original_conviction": round(self.original_conviction, 4),
            "perturbations": [p.to_dict() for p in self.perturbations],
            "fragility_flags": [f.to_dict() for f in self.fragility_flags],
            "robustness_score": round(self.robustness_score, 4),
            "robustness_label": self.robustness_label,
            "break_count": self.break_count,
            "generated_at": self.generated_at,
            "advisory": self.advisory,
        }


# ── Pure helpers ──────────────────────────────────────────────────────────


def perturb_brier(
    current_brier: float,
    sigma: float,
    delta_per_sigma: float = BRIER_PERTURBATION_DELTA,
) -> float:
    """Shift a Brier score in the adverse direction (always upward).

    Brier going UP is always adverse since lower Brier = better
    calibrated. We use ``|sigma|`` so both -2σ and +2σ shift Brier the
    same amount upward — the sign on sigma is purely a label for the
    perturbation magnitude in the adverse direction. Result is clamped
    to ``[0, 1]``.
    """
    shift = abs(float(sigma)) * float(delta_per_sigma)
    return max(0.0, min(1.0, float(current_brier) + shift))


def perturbed_conviction_weight(
    perturbed_brier: float,
    scored_count: int,
) -> float:
    """Reuses ``features.per_signal_brier.compute_conviction_weight``.

    Imported, not reimplemented — this guarantees the stress test stays
    in lockstep with whatever the live scoring layer is doing.
    """
    return compute_conviction_weight(float(perturbed_brier), int(scored_count))


def compute_robustness_score(perturbations: list[SignalPerturbation]) -> float:
    """Fraction of perturbations whose verdict did NOT change.

    Empty perturbation grid → 1.0 (trivially robust — there's nothing
    to flip).
    """
    if not perturbations:
        return 1.0
    held = sum(1 for p in perturbations if not p.verdict_changed)
    return held / len(perturbations)


def classify_robustness(score: float) -> str:
    """Map a robustness score in [0, 1] to robust / moderate / fragile."""
    if score >= ROBUST_THRESHOLD:
        return "robust"
    if score < FRAGILITY_THRESHOLD:
        return "fragile"
    return "moderate"


def identify_fragility_flags(
    perturbations: list[SignalPerturbation],
) -> list[FragilityFlag]:
    """For each unique signal_source, find the smallest ``|sigma|``
    perturbation that flipped the verdict. If found, the signal is
    fragile and we record the breaking sigma. If never flipped, the
    signal is robust (``fragile=False``).

    Determinism: signals are returned in first-seen order across the
    perturbation list. Within a signal we use the smallest absolute
    sigma; ties are broken by encounter order (stable iteration).
    """
    by_source: dict[str, list[SignalPerturbation]] = {}
    order: list[str] = []
    for p in perturbations:
        if p.signal_source not in by_source:
            by_source[p.signal_source] = []
            order.append(p.signal_source)
        by_source[p.signal_source].append(p)

    flags: list[FragilityFlag] = []
    for source in order:
        flips = [p for p in by_source[source] if p.verdict_changed]
        if not flips:
            flags.append(
                FragilityFlag(
                    signal_source=source,
                    fragile=False,
                    reason="no perturbation flipped verdict",
                    breaking_sigma=None,
                )
            )
            continue
        # Smallest |sigma| flip is the most damning — a tiny push was
        # all it took to break the call.
        smallest = min(flips, key=lambda x: abs(x.sigma))
        flags.append(
            FragilityFlag(
                signal_source=source,
                fragile=True,
                reason=(
                    f"verdict flips at sigma={smallest.sigma:+.1f} "
                    f"(new verdict: {smallest.new_verdict})"
                ),
                breaking_sigma=smallest.sigma,
            )
        )
    return flags


# ── Advisory copy ─────────────────────────────────────────────────────────


def build_advisory(
    report: "StressTestReport",
    original_report: TradeProvenanceReport | None,
) -> str:
    """Compose the human-readable robustness advisory.

    Templates (order of precedence):

      1. Empty signal evidence → "no signals to test"
      2. Fragile + at least one breaking signal → name the worst one
      3. Robust → reassuring "would require N+ signals to flip"
      4. Moderate → cautious middle ground
    """
    if not report.perturbations:
        return (
            "Stress test skipped — no contributing signals to perturb. "
            "Treat conviction as untestable."
        )

    fragile_flags = [f for f in report.fragility_flags if f.fragile]
    label = report.robustness_label
    orig_verdict = report.original_verdict.upper()

    if label == "fragile" and fragile_flags:
        # Pick the most damning: smallest |breaking_sigma|.
        worst = min(
            fragile_flags,
            key=lambda f: abs(f.breaking_sigma or 0.0),
        )
        sigma_label = (
            f"{worst.breaking_sigma:+.1f}σ"
            if worst.breaking_sigma is not None
            else "an adverse shift"
        )
        return (
            f"Trade is FRAGILE — flipping {worst.signal_source} to "
            f"{sigma_label} alone would change verdict from {orig_verdict}. "
            f"Reduce position size or add hedge."
        )

    if label == "robust":
        return (
            f"Trade is ROBUST — would require 3+ signals to flip "
            f"simultaneously to break the {orig_verdict} call. "
            f"Size with confidence."
        )

    # Moderate: some perturbations flipped, no single dominant breaker.
    return (
        f"Trade is MODERATE — {report.break_count} of "
        f"{len(report.perturbations)} stress perturbations flipped the "
        f"{orig_verdict} verdict. Trim size and watch for invalidation."
    )


# ── Main entry point ──────────────────────────────────────────────────────


def _scorecard_brier_and_count(
    scorecard: SignalScorecard | None,
) -> tuple[float, int]:
    """Pull (brier, scored_count) out of a possibly-None scorecard.

    Defensive: when no history exists, treat as cold-start (0.1 Brier,
    0 scored — which means perturbed_conviction_weight will return the
    neutral 1.0). That keeps the stress test deterministic for cold
    signals without crashing.
    """
    if scorecard is None:
        return (0.1, 0)
    return (float(scorecard.running_brier), int(scorecard.scored_count))


def _swap_signal_evidence(
    evidence_list: list[SignalEvidence],
    target_index: int,
    new_weight: float,
) -> list[SignalEvidence]:
    """Return a copy of ``evidence_list`` with the target signal's
    scorecard conviction_weight overridden to ``new_weight``.

    We rebuild the SignalEvidence (and its inner SignalScorecard) since
    both are frozen dataclasses. Pure — never mutates the input.
    """
    new_list: list[SignalEvidence] = []
    for i, ev in enumerate(evidence_list):
        if i != target_index:
            new_list.append(ev)
            continue
        old_card = ev.scorecard
        if old_card is None:
            # Synthesize a minimal scorecard so the perturbed weight has
            # somewhere to live. Conviction-weight is the only field
            # compute_aggregate_conviction reads.
            new_card = SignalScorecard(
                signal_source=ev.signal_source,
                horizon_days=7,
                scored_count=MIN_CALIBRATED_SAMPLES,
                running_brier=0.1,
                running_ece=0.1,
                hit_rate=0.0,
                last_updated=None,
                is_calibrated=True,
                conviction_weight=new_weight,
            )
        else:
            new_card = SignalScorecard(
                signal_source=old_card.signal_source,
                horizon_days=old_card.horizon_days,
                scored_count=old_card.scored_count,
                running_brier=old_card.running_brier,
                running_ece=old_card.running_ece,
                hit_rate=old_card.hit_rate,
                last_updated=old_card.last_updated,
                is_calibrated=old_card.is_calibrated,
                conviction_weight=new_weight,
            )
        new_list.append(
            SignalEvidence(
                signal_source=ev.signal_source,
                shapley_weight=ev.shapley_weight,
                scorecard=new_card,
                classification=ev.classification,
            )
        )
    return new_list


def run_stress_test(report: TradeProvenanceReport) -> StressTestReport:
    """Run the counterfactual stress test against a provenance report.

    Pure function — no engine, no DB, no network. Iterates every
    contributing signal in ``report.signal_evidence``, perturbs each
    one in turn at every sigma in ``STRESS_PERTURBATION_SIGMAS``,
    recomputes ``compute_aggregate_conviction`` (imported), and
    records the resulting verdict.
    """
    perturbations: list[SignalPerturbation] = []

    if not report.signal_evidence:
        # Empty evidence → no perturbations, trivially robust.
        empty_score = compute_robustness_score(perturbations)
        empty_report = StressTestReport(
            ticker=report.ticker,
            original_verdict=report.verdict,
            original_conviction=report.aggregate_conviction,
            perturbations=perturbations,
            fragility_flags=[],
            robustness_score=empty_score,
            robustness_label=classify_robustness(empty_score),
            break_count=0,
            generated_at=datetime.now(timezone.utc).isoformat(),
            advisory="",
        )
        advisory = build_advisory(empty_report, report)
        return StressTestReport(
            ticker=empty_report.ticker,
            original_verdict=empty_report.original_verdict,
            original_conviction=empty_report.original_conviction,
            perturbations=empty_report.perturbations,
            fragility_flags=empty_report.fragility_flags,
            robustness_score=empty_report.robustness_score,
            robustness_label=empty_report.robustness_label,
            break_count=empty_report.break_count,
            generated_at=empty_report.generated_at,
            advisory=advisory,
        )

    for idx, ev in enumerate(report.signal_evidence):
        base_brier, base_count = _scorecard_brier_and_count(ev.scorecard)
        for sigma in STRESS_PERTURBATION_SIGMAS:
            new_brier = perturb_brier(base_brier, sigma)
            new_weight = perturbed_conviction_weight(new_brier, base_count)
            swapped = _swap_signal_evidence(
                report.signal_evidence, idx, new_weight
            )
            new_aggregate = compute_aggregate_conviction(
                swapped,
                fragility_multiplier=report.fragility_multiplier,
                disagreement_score=report.disagreement_score,
                red_team_epistemic_risk=report.red_team_epistemic_risk,
                fudge_alert_count=len(report.shipping_fudge_alerts),
            )
            new_verdict = _verdict_from_aggregate(new_aggregate, report.confidence)
            perturbations.append(
                SignalPerturbation(
                    signal_source=ev.signal_source,
                    sigma=float(sigma),
                    perturbed_brier=new_brier,
                    perturbed_conviction_weight=new_weight,
                    new_aggregate_conviction=new_aggregate,
                    new_verdict=new_verdict,
                    verdict_changed=(new_verdict != report.verdict),
                )
            )

    fragility_flags = identify_fragility_flags(perturbations)
    score = compute_robustness_score(perturbations)
    label = classify_robustness(score)
    break_count = sum(1 for p in perturbations if p.verdict_changed)

    # Build the report once, then rebuild with the advisory baked in.
    bare = StressTestReport(
        ticker=report.ticker,
        original_verdict=report.verdict,
        original_conviction=report.aggregate_conviction,
        perturbations=perturbations,
        fragility_flags=fragility_flags,
        robustness_score=score,
        robustness_label=label,
        break_count=break_count,
        generated_at=datetime.now(timezone.utc).isoformat(),
        advisory="",
    )
    advisory = build_advisory(bare, report)
    return StressTestReport(
        ticker=bare.ticker,
        original_verdict=bare.original_verdict,
        original_conviction=bare.original_conviction,
        perturbations=bare.perturbations,
        fragility_flags=bare.fragility_flags,
        robustness_score=bare.robustness_score,
        robustness_label=bare.robustness_label,
        break_count=bare.break_count,
        generated_at=bare.generated_at,
        advisory=advisory,
    )
