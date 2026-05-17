"""Contract routing table.

ROUTES maps each contract type to the list of handler paths that should be
invoked when the contract fires. Handler paths are dotted Python imports in
the ``contracts.handlers.*`` namespace.

**Phase 2 final seal (SYNTH-45):** the canonical §7.3 routing from
``docs/synthesis/SYNTHESIS_WIRING_PLAN.md`` is now fully populated.
Every contract that closes a feedback loop has at least one handler.

Out-of-scope (per §7.3 explicit V5/governance carve-out):
``LeverageRiskUpdate``, ``BacktestGateVerdict``, ``HypothesisGenerated``,
``ActorMaterialized``, ``ForensicsTrace``, ``InvestigationProgress`` —
these land in a later oracle-governance PR.

Operationally retained (out of synthesis §7.3 scope):
``PullLifecycle`` — emitted by ingestion pullers and consumed by the
ops dashboard handler.
"""
from __future__ import annotations

import importlib
from typing import Callable

from contracts.schemas import (
    BaseContract,
    CrossReferenceAnomaly,
    EdgeValidated,
    OptionsTradeOutcome,
    PostmortemCompleted,
    PredictionScored,
    PullLifecycle,
    RegimeTransition,
    SignalFired,
)


ROUTES: dict[type[BaseContract], list[str]] = {
    # SYNTH-19/20/21 (Wave A) + journal.on_prediction_scored (SYNTH-42b)
    # — close-the-loop on every scored prediction. Ordering matters:
    # trust must update before oracle_weights so the weight delta sees
    # the fresh source posteriors (see §8.2 ordering invariant).
    PredictionScored: [
        "contracts.handlers.trust.on_prediction_scored",
        "contracts.handlers.oracle_weights.on_prediction_scored",
        "contracts.handlers.calibration.on_prediction_scored",
        "contracts.handlers.journal.on_prediction_scored",
    ],
    # SYNTH-22 / SYNTH-23 — postmortem feedback into trust + oracle.
    PostmortemCompleted: [
        "contracts.handlers.trust.on_postmortem_completed",
        "contracts.handlers.oracle_weights.on_postmortem_completed",
    ],
    # SYNTH-40 — trade outcome fanout. ``trade_outcomes.on_options_trade_outcome``
    # carries the existing weight-evolver path (legacy name preserved for
    # backward compatibility with handoff_2026-04-11c). The calibration
    # entry is the §7.3 second handler.
    OptionsTradeOutcome: [
        "contracts.handlers.trade_outcomes.on_options_trade_outcome",
        "contracts.handlers.calibration.on_options_trade_outcome",
    ],
    # SYNTH-B/C wave — offensive alpha fanout. The oracle_signals handler
    # projects detector-emitted SignalFired contracts into signal_sources
    # so _gather_signals_from_registry can consume them on the next cycle.
    # The trust handler registers the signal into the Bayesian beta posterior
    # tracking table so its outcome can be deposited later.
    # The journal handler mirrors high-strength signals into
    # ``decision_journal`` as provisional rows.
    SignalFired: [
        "contracts.handlers.oracle_signals.on_signal_fired",
        "contracts.handlers.trust.on_signal_fired",
        "contracts.handlers.journal.on_signal_fired",
    ],
    # SYNTH-28 / SYNTH-32 closure — cross-reference anomalies feed the
    # oracle anti-signal scan and (severity >= HIGH) page the operator.
    CrossReferenceAnomaly: [
        "contracts.handlers.oracle_anti_signals.on_cross_reference_anomaly",
        "contracts.handlers.alerts.on_cross_reference_anomaly",
    ],
    # SYNTH-39 — edge validation fanout. The legacy ``edges`` handler
    # downgrades each cross_lens *signal row*; the new ``trust`` handler
    # downgrades the cross_lens *source's* Bayesian trust score.
    EdgeValidated: [
        "contracts.handlers.edges.on_edge_validated",
        "contracts.handlers.trust.on_edge_validated",
    ],
    # SYNTH-30 / SYNTH-31 closure — regime transitions persist for oracle
    # family-weight routing and page the operator.
    RegimeTransition: [
        "contracts.handlers.oracle_regime.on_regime_transition",
        "contracts.handlers.alerts.on_regime_transition",
    ],
    # Operational — out of §7.3 scope but kept for ingestion observability.
    PullLifecycle: [
        "contracts.handlers.pull_lifecycle.on_pull_lifecycle",
    ],
}


def resolve_handler(dotted_path: str) -> Callable:
    """Import a handler from a dotted path.

    Raises ModuleNotFoundError or AttributeError if the path is invalid.
    """
    module_path, _, attr = dotted_path.rpartition(".")
    if not module_path:
        raise ValueError(f"handler path must be dotted: {dotted_path!r}")
    module = importlib.import_module(module_path)
    return getattr(module, attr)
