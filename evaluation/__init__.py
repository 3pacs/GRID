"""GRID evaluation package.

Houses versioned, honest outcome evaluators for signals and predictions
(``evaluation/signal_outcomes.py``, workstream W3b, "sig-eval-1") and the
contamination classifier (``evaluation/contamination.py``, workstream W7).
This package is intentionally decoupled from ``intelligence/trust_scorer.py``
and ``intelligence/postmortem.py`` — nothing here is wired into Hermes, the
scheduler, or any production read/write path. No submodule imports here so
the package merges cleanly across lanes.
"""
