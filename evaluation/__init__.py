"""GRID evaluation package.

Houses versioned, honest outcome evaluators for signals and predictions.
This package is intentionally decoupled from ``intelligence/trust_scorer.py``
and ``intelligence/postmortem.py`` — nothing here is wired into Hermes, the
scheduler, or any production read/write path. See ``evaluation/signal_outcomes.py``
for the first evaluator (workstream W3b, "sig-eval-1").
"""
