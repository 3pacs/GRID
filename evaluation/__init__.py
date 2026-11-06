"""GRID evaluation package.

Houses versioned, honest outcome evaluators for signals and predictions.
``evaluation/signal_outcomes.py`` evaluates ``signal_sources`` rows
(``sig-eval-4-sessions``) and appends final outcomes to the additive
``signal_source_outcomes`` table -- offline via
``scripts/evaluate_signals.py replay`` (dry run unless ``--apply``) and,
behind ``GRID_ENABLE_SIGNAL_OUTCOME_FORWARD_SCORING``, from the trust cycle.
The legacy ``signal_sources.outcome`` column is never written here. No
submodule imports here so the package merges cleanly across lanes.
"""
