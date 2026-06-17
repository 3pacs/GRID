"""Tests for ``intelligence.signal_weight_overrides``."""

from __future__ import annotations

import pytest

from intelligence import signal_weight_overrides


@pytest.fixture(autouse=True)
def _enable_overrides(monkeypatch):
    """Keep tests isolated from env-derived module state and toggles."""
    monkeypatch.setattr(signal_weight_overrides, "SIGNAL_OVERRIDES_ENABLED", True)


def test_get_override_returns_configured_multiplier_for_known_signals():
    assert signal_weight_overrides.get_override("equity") == pytest.approx(1.20)
    assert signal_weight_overrides.get_override("vol") == pytest.approx(0.30)
    assert signal_weight_overrides.get_override(" commodity ") == pytest.approx(1.10)


def test_get_override_returns_neutral_for_unknown_or_unusable_signals():
    assert signal_weight_overrides.get_override("unknown_signal") == 1.0
    assert signal_weight_overrides.get_override("feature:equity") == 1.0
    assert signal_weight_overrides.get_override("") == 1.0
    assert signal_weight_overrides.get_override(None) == 1.0


def test_set_enabled_toggle_short_circuits_known_signal_overrides():
    signal_weight_overrides.set_enabled(False)
    assert signal_weight_overrides.get_override("equity") == 1.0

    signal_weight_overrides.set_enabled(True)
    assert signal_weight_overrides.get_override("equity") == pytest.approx(1.20)
