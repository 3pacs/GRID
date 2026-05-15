"""Regression test for the publish_astrogrid_prediction single-source-of-truth.

PR #151 punch-list P0: oracle/publish.py and oracle/publisher_gate.py both
defined publish_astrogrid_prediction with divergent bodies. publish.py
enriches signals with regime / fci_regime / vix_level / signal_contributions
via build_prediction_context + enrich_signals_payload; publisher_gate.py did
not. api/routers/astrogrid_helpers.py imported the un-enriched version,
silently stripping conviction-stack context from every astrogrid prediction.

This test guards against the duplicate re-appearing.
"""

from __future__ import annotations

import inspect


def test_publisher_gate_reexports_canonical_publish_astrogrid_prediction():
    from oracle.publish import publish_astrogrid_prediction as canonical
    from oracle.publisher_gate import publish_astrogrid_prediction as via_gate

    assert canonical is via_gate, (
        "oracle.publisher_gate.publish_astrogrid_prediction must be the same "
        "object as oracle.publish.publish_astrogrid_prediction so astrogrid "
        "predictions written through api.routers.astrogrid_helpers also get "
        "the enriched regime / fci_regime / vix_level / signal_contributions "
        "context that the conviction-stack calibrators need."
    )


def test_publisher_gate_does_not_redefine_publish_helpers():
    import oracle.publish as canonical_module
    import oracle.publisher_gate as gate_module

    for helper in ("_compact_text", "_prediction_direction", "_prediction_expiry"):
        gate_attr = getattr(gate_module, helper, None)
        if gate_attr is None:
            continue
        canonical_attr = getattr(canonical_module, helper)
        assert gate_attr is canonical_attr, (
            f"{helper} appears to be redefined in oracle.publisher_gate. "
            "The canonical implementation lives in oracle.publish; "
            "publisher_gate should re-export, not redefine."
        )


def test_canonical_publish_calls_build_prediction_context():
    from oracle import publish as publish_module

    source = inspect.getsource(publish_module.publish_astrogrid_prediction)
    assert "build_prediction_context" in source, (
        "publish_astrogrid_prediction must enrich the signals payload with "
        "build_prediction_context so downstream calibrators see "
        "regime / fci_regime / vix_level / signal_contributions."
    )
    assert "enrich_signals_payload" in source, (
        "publish_astrogrid_prediction must invoke enrich_signals_payload "
        "to attach the conviction-stack context to each emitted signal."
    )
