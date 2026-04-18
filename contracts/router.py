"""Contract routing table.

ROUTES maps each contract type to the list of handler paths that should be
invoked when the contract fires. Handler paths are dotted Python imports in
the ``contracts.handlers.*`` namespace.

**Phase 2 Wave A (SYNTH-19..23):** closes the feedback loop from
``PredictionScored`` and ``PostmortemCompleted`` back into the trust
scorer, oracle weight evolver, and calibration subsystems.
"""
from __future__ import annotations

import importlib
from typing import Callable

from contracts.schemas import (
    BaseContract,
    EdgeValidated,
    OptionsTradeOutcome,
    PostmortemCompleted,
    PredictionScored,
    PullLifecycle,
    SignalFired,
)


ROUTES: dict[type[BaseContract], list[str]] = {
    PredictionScored: [
        "contracts.handlers.trust.on_prediction_scored",
        "contracts.handlers.oracle_weights.on_prediction_scored",
        "contracts.handlers.calibration.on_prediction_scored",
    ],
    PostmortemCompleted: [
        "contracts.handlers.trust.on_postmortem_completed",
        "contracts.handlers.oracle_weights.on_postmortem_completed",
    ],
    # SYNTH-B wave — offensive alpha fanout. The oracle_signals handler
    # projects detector-emitted SignalFired contracts into signal_sources
    # so _gather_signals_from_registry can consume them on the next cycle.
    # SYNTH-C wave extends this with a second handler that mirrors
    # high-strength signals into ``decision_journal`` as provisional rows.
    SignalFired: [
        "contracts.handlers.oracle_signals.on_signal_fired",
        "contracts.handlers.journal.on_signal_fired",
    ],
    # SYNTH-C wave — trade outcome fanout (SYNTH-40). Updates the contagion
    # model head's weight via the Bayesian evolver when a ticket closes.
    OptionsTradeOutcome: [
        "contracts.handlers.trade_outcomes.on_options_trade_outcome",
    ],
    # SYNTH-C wave — edge validation fanout (SYNTH-39). Downgrades
    # cross_lens trust for any signal citing an edge that just went weak.
    EdgeValidated: [
        "contracts.handlers.edges.on_edge_validated",
    ],
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
