"""Unit tests for ``oracle.prediction_context`` enrichment helpers.

These tests hit the context builder directly with a fake engine — the real
DB is never touched. Every failure mode (PITStore exception, missing regime,
empty model votes) must fall back to safe defaults without raising.
"""

from __future__ import annotations

from datetime import date
from typing import Any
from unittest.mock import MagicMock

import pytest

from oracle.prediction_context import (
    DEFAULT_FCI_REGIME,
    DEFAULT_REGIME,
    build_prediction_context,
    canonical_regime,
    enrich_signals_payload,
    extract_signal_contributions,
)


# ── canonical_regime ─────────────────────────────────────────────────────────

@pytest.mark.parametrize(
    "raw,expected",
    [
        ("CRISIS", "CRISIS"),
        ("crisis", "CRISIS"),
        ("panic", "CRISIS"),
        ("TIGHTENING", "TIGHTENING"),
        ("hiking", "TIGHTENING"),
        ("neutral", "NEUTRAL"),
        ("holding", "NEUTRAL"),
        ("EXPANSION", "EXPANSION"),
        ("risk_on", "EXPANSION"),
        ("growth", "EXPANSION"),
        ("EXPANSION_STRONG", "EXPANSION_STRONG"),
        ("strong_expansion", "EXPANSION_STRONG"),
        (None, "NEUTRAL"),
        ("", "NEUTRAL"),
        ("nonsense", "NEUTRAL"),
    ],
)
def test_canonical_regime(raw, expected):
    assert canonical_regime(raw) == expected


# ── extract_signal_contributions ─────────────────────────────────────────────

def test_extract_prefers_shapley():
    result = extract_signal_contributions(
        shapley_contributions={"m1": 0.7, "m2": 0.3},
        model_weights={"m1": 1.0, "m2": 1.0},
        model_votes={"m1": 0.5, "m2": 0.5},
    )
    assert result == {"m1": 0.7, "m2": 0.3}


def test_extract_falls_back_to_votes_when_shapley_missing():
    result = extract_signal_contributions(
        shapley_contributions=None,
        model_weights={"m1": 1.0, "m2": 1.0},
        model_votes={"vote_a": 0.8, "vote_b": 0.2},
    )
    assert result == {"vote_a": 0.8, "vote_b": 0.2}


def test_extract_falls_back_to_weights_as_last_resort():
    result = extract_signal_contributions(
        shapley_contributions=None,
        model_weights={"only": 1.0},
        model_votes=None,
    )
    assert result == {"only": 1.0}


def test_extract_empty_returns_empty_dict():
    assert extract_signal_contributions() == {}
    assert extract_signal_contributions(shapley_contributions={}, model_weights={}) == {}


def test_extract_drops_nan_and_inf():
    result = extract_signal_contributions(
        model_votes={"ok": 0.5, "bad_nan": float("nan"), "bad_inf": float("inf"), "": 0.1},
    )
    assert result == {"ok": 0.5}


# ── build_prediction_context — happy path ───────────────────────────────────

class _FakeConnection:
    def __init__(self, result_rows: dict[str, Any]) -> None:
        self._rows = result_rows
        self._last_query = ""

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, query, params=None):
        sql = str(query)
        self._last_query = sql
        result = MagicMock()
        # Very coarse routing: return row for regime_history / resolved_series
        if "regime_history" in sql:
            row = self._rows.get("regime_history")
            result.fetchone.return_value = (row,) if row else None
        elif "resolved_series" in sql:
            fname = None
            if params and "fname" in params:
                fname = params["fname"]
            value = self._rows.get(f"feature:{fname}")
            result.fetchone.return_value = (value,) if value is not None else None
        else:
            result.fetchone.return_value = None
        return result


class _FakeEngine:
    def __init__(self, result_rows: dict[str, Any]) -> None:
        self._rows = result_rows

    def connect(self):
        return _FakeConnection(self._rows)


def test_build_context_reads_regime_history_and_vix():
    engine = _FakeEngine(
        {
            "regime_history": "EXPANSION",
            "feature:vix_close": 16.5,
            "feature:fci_composite": -0.5,
        }
    )
    ctx = build_prediction_context(
        engine,
        as_of=date(2026, 4, 14),
        model_weights={"default": 1.0, "ensemble": 0.8},
    )
    assert ctx["regime"] == "EXPANSION"
    assert ctx["vix_level"] == 16.5
    assert ctx["fci_regime"] == "EXPANSION"
    assert ctx["signal_contributions"] == {"default": 1.0, "ensemble": 0.8}


def test_build_context_uses_vix_rule_when_regime_history_empty():
    engine = _FakeEngine(
        {
            "regime_history": None,
            "feature:vix_close": 42.0,
        }
    )
    ctx = build_prediction_context(engine, as_of=date(2026, 4, 14))
    # VIX >= 40 → CRISIS
    assert ctx["regime"] == "CRISIS"
    assert ctx["vix_level"] == 42.0


def test_build_context_defaults_when_engine_none():
    ctx = build_prediction_context(None, as_of=date(2026, 4, 14))
    assert ctx["regime"] == DEFAULT_REGIME
    assert ctx["fci_regime"] == DEFAULT_FCI_REGIME
    assert ctx["vix_level"] is None
    assert ctx["signal_contributions"] == {}


# ── build_prediction_context — failure paths ────────────────────────────────

class _ExplodingConnection:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, *a, **kw):
        raise RuntimeError("PITStore dead")


class _ExplodingEngine:
    def connect(self):
        return _ExplodingConnection()


def test_build_context_swallows_pit_failures_and_uses_defaults():
    engine = _ExplodingEngine()
    ctx = build_prediction_context(
        engine,
        as_of=date(2026, 4, 14),
        model_weights={"m1": 0.6},
    )
    assert ctx["regime"] == DEFAULT_REGIME
    assert ctx["fci_regime"] == DEFAULT_FCI_REGIME
    assert ctx["vix_level"] is None
    # Model weights still flow through (they don't need DB)
    assert ctx["signal_contributions"] == {"m1": 0.6}


def test_build_context_signal_contributions_fallback_to_votes():
    engine = _FakeEngine({})
    ctx = build_prediction_context(
        engine,
        as_of=date(2026, 4, 14),
        model_votes={"momentum_buy": 0.7, "flow_buy": 0.3},
    )
    assert ctx["signal_contributions"] == {"momentum_buy": 0.7, "flow_buy": 0.3}


# ── enrich_signals_payload ───────────────────────────────────────────────────

def test_enrich_wraps_list_payload_under_items():
    raw = [{"name": "a"}, {"name": "b"}]
    ctx = {
        "regime": "CRISIS",
        "fci_regime": "TIGHTENING",
        "vix_level": 35.0,
        "signal_contributions": {"m": 1.0},
    }
    merged = enrich_signals_payload(raw, ctx)
    assert merged["items"] == raw
    assert merged["regime"] == "CRISIS"
    assert merged["fci_regime"] == "TIGHTENING"
    assert merged["vix_level"] == 35.0
    assert merged["signal_contributions"] == {"m": 1.0}


def test_enrich_does_not_overwrite_existing_keys():
    existing = {
        "items": [{"name": "a"}],
        "regime": "EXPANSION_STRONG",  # pre-set by caller
        "custom_key": "keep_me",
    }
    ctx = {
        "regime": "CRISIS",
        "fci_regime": "NEUTRAL",
        "vix_level": 18.0,
        "signal_contributions": {},
    }
    merged = enrich_signals_payload(existing, ctx)
    assert merged["regime"] == "EXPANSION_STRONG"  # preserved
    assert merged["custom_key"] == "keep_me"
    assert merged["fci_regime"] == "NEUTRAL"  # filled in from ctx
    # Original input not mutated
    assert "fci_regime" not in existing


def test_enrich_handles_none_payload():
    ctx = {
        "regime": "NEUTRAL",
        "fci_regime": "NEUTRAL",
        "vix_level": None,
        "signal_contributions": {},
    }
    merged = enrich_signals_payload(None, ctx)
    assert merged["items"] == []
    assert merged["regime"] == "NEUTRAL"


def test_enrich_accepts_dataclass_signals():
    from dataclasses import dataclass

    @dataclass
    class Sig:
        name: str
        weight: float

    raw = [Sig("momentum", 0.5), Sig("flow", 0.25)]
    merged = enrich_signals_payload(
        raw,
        {
            "regime": "NEUTRAL",
            "fci_regime": "NEUTRAL",
            "vix_level": None,
            "signal_contributions": {},
        },
    )
    assert merged["items"] == [
        {"name": "momentum", "weight": 0.5},
        {"name": "flow", "weight": 0.25},
    ]
