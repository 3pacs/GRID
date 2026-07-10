"""Phase 2 contract handlers.

Handler modules mapped to contract types by ``contracts/router.py``. Each module
exposes the routed entry points (e.g. ``on_prediction_scored``,
``on_cross_reference_anomaly``) invoked by the dispatcher when the matching
contract fires. See ``contracts/router.py::ROUTES`` for the canonical wiring.

Modules: ``alerts``, ``calibration``, ``edges``, ``journal``,
``oracle_anti_signals``, ``oracle_regime``, ``oracle_signals``,
``oracle_weights``, ``pull_lifecycle``, ``trade_outcomes``, ``trust``.
"""
