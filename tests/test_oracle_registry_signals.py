"""Regression tests for OracleEngine._gather_signals_from_registry.

Guards the Signal positional-arg mapping: the registry z-score must land in
the `z_score` field (not `value`), otherwise downstream direction scoring
(`s.z_score * s.weight` in engine.py) silently contributes zero and
neutralizes every registry-sourced signal when GRID_SIGNAL_REGISTRY=1.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from oracle.engine import OracleEngine


def _engine() -> OracleEngine:
    """Build an OracleEngine without running __init__ (no DB needed)."""
    eng = OracleEngine.__new__(OracleEngine)
    eng.engine = MagicMock()
    return eng


def test_registry_signal_preserves_z_score():
    raw = [
        {"source_module": "altdata:credit", "z_score": 2.5,
         "confidence": 0.8, "direction": "bullish"},
        {"source_module": "altdata:vol", "value": -1.7,
         "confidence": 0.6, "direction": "bearish"},
    ]
    factory = MagicMock()
    factory.get_signals_for_model.return_value = raw

    with patch("oracle.model_factory.ModelFactory", return_value=factory):
        sigs = _engine()._gather_signals_from_registry(
            "AAPL", SimpleNamespace(name="m1"))

    assert len(sigs) == 2
    # z_score must carry the registry z, not 0
    assert sigs[0].z_score == 2.5
    assert sigs[0].value == 2.5
    assert sigs[1].z_score == -1.7  # falls back to the "value" key
    # downstream direction scoring must produce a non-zero contribution
    assert all(s.z_score * s.weight != 0 for s in sigs)


def test_registry_empty_returns_empty():
    factory = MagicMock()
    factory.get_signals_for_model.return_value = []

    with patch("oracle.model_factory.ModelFactory", return_value=factory):
        sigs = _engine()._gather_signals_from_registry(
            "AAPL", SimpleNamespace(name="m1"))

    assert sigs == []
