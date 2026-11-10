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


# ── aggregate_signal_weights + engine write path (S9a, 2026-11-06) ───────────
#
# Production rows on 2026-11-02 showed ten ``sector_network`` items collapsed
# to ``{"sector_network": 1.0}`` (last weight wins) and unnormalized weights
# such as ``{"timesfm_direction_7d": 1.5, ...}``. The write path now sums
# duplicate names and normalizes to 1.0, the shape per_signal_brier expects.

from oracle.prediction_context import (
    SIGNAL_CONTRIBUTIONS_BASIS,
    aggregate_signal_weights,
)


def test_aggregate_signal_weights_sums_duplicates_and_normalizes():
    out = aggregate_signal_weights(
        [
            {"name": "sector_network", "weight": 0.5},
            {"name": "sector_network", "weight": 1.0},
            {"name": "feature:equity", "weight": 1.5},
        ]
    )
    assert out == pytest.approx({"sector_network": 0.5, "feature:equity": 0.5})
    assert sum(out.values()) == pytest.approx(1.0)


def test_aggregate_signal_weights_drops_unusable_entries():
    out = aggregate_signal_weights(
        [
            {"name": "", "weight": 1.0},
            {"name": "nan_sig", "weight": float("nan")},
            {"name": "inf_sig", "weight": float("inf")},
            {"name": "zero", "weight": 0.0},
            {"name": "neg", "weight": -2.0},
            {"name": "junk", "weight": "x"},
            {"name": "ok", "weight": 0.2},
        ]
    )
    assert out == {"ok": 1.0}
    assert aggregate_signal_weights([]) == {}
    assert aggregate_signal_weights(None) == {}  # type: ignore[arg-type]


class _RecordingConn:
    def __init__(self, sink: list[dict[str, Any]]) -> None:
        self.sink = sink

    def __enter__(self):
        return self

    def __exit__(self, *args: object) -> bool:
        return False

    def execute(self, query: Any, params: dict[str, Any] | None = None) -> Any:
        if params is not None:
            self.sink.append(dict(params))
        res = MagicMock()
        res.fetchone.return_value = None
        return res


class _RecordingEngine:
    def __init__(self) -> None:
        self.params: list[dict[str, Any]] = []

    def begin(self) -> _RecordingConn:
        return _RecordingConn(self.params)

    connect = begin


def _store_one_fixture_prediction(monkeypatch) -> dict[str, Any]:
    """Run OracleEngine._store_predictions on one fixture prediction and
    return the bound INSERT parameters (no DB)."""
    from datetime import datetime, timezone

    import oracle.prediction_context as pc
    from oracle.engine import OracleEngine, OraclePrediction, PredictionType, Signal
    from oracle.model_factory import ModelFactory
    import oracle.entry_price_policy as policy
    monkeypatch.setattr(ModelFactory, "active_model_names", lambda *a, **k: {"fixture_model"})
    monkeypatch.setattr(policy, "validate_quote_reference", lambda *a, **k: None)

    monkeypatch.setattr(pc, "fetch_liquidity_regime", lambda *a, **k: "NEUTRAL")
    monkeypatch.setattr(pc, "fetch_fci_regime", lambda *a, **k: "NEUTRAL")
    monkeypatch.setattr(pc, "fetch_vix_level", lambda *a, **k: 18.0)

    eng = object.__new__(OracleEngine)
    rec = _RecordingEngine()
    eng.engine = rec  # type: ignore[attr-defined]

    pred = OraclePrediction(
        id="fixture-1",
        timestamp=datetime(2026, 11, 6, 14, 0, tzinfo=timezone.utc),
        ticker="SPY",
        prediction_type=PredictionType.DIRECTION,
        direction="CALL",
        target_price=None,
        current_price=600.0,
        expiry=date(2026, 11, 13),
        confidence=0.7,
        expected_move_pct=1.0,
        signals=[
            Signal("sector_network", "network", 1.0, 0.5, "bullish", 0.5, 0),
            Signal("sector_network", "network", 1.0, 0.5, "bullish", 1.0, 0),
            Signal("timesfm_direction_7d", "ts", 1.0, 1.0, "bullish", 1.5, 0),
        ],
        model_name="fixture_model",
        model_version="v1",
        model_weights={"fixture_model": 1.2, "other_model": 0.9},
        horizon_days=7,
        flow_context={"regime": "NEUTRAL", "fci_regime": "NEUTRAL", "vix_level": 18.0,
                      "entry_price_basis": {"price": 600.0}},
    )
    eng._store_predictions([pred])
    inserts = [p for p in rec.params if "sig" in p]
    assert len(inserts) == 1
    return inserts[0]


def test_engine_store_writes_normalized_signal_contributions(monkeypatch):
    import json

    params = _store_one_fixture_prediction(monkeypatch)
    signals = json.loads(params["sig"])
    assert signals["signal_contributions"] == pytest.approx(
        {"sector_network": 0.5, "timesfm_direction_7d": 0.5}
    )
    assert signals["signal_contributions_basis"] == SIGNAL_CONTRIBUTIONS_BASIS
    assert len(signals["items"]) == 3
    assert signals["regime"] == "NEUTRAL"
    assert signals["vix_level"] == 18.0


def test_per_signal_brier_consumes_engine_written_contributions(monkeypatch):
    """The row the engine writes feeds the per-signal Brier path unchanged:
    the bootstrap's Layer 1 picks the nested dict (not the family/aggregate
    fallbacks) and record_scored_prediction attributes error by share."""
    from features import per_signal_brier
    from scripts import bootstrap_per_signal_brier as boot

    params = _store_one_fixture_prediction(monkeypatch)
    row = {
        "id": params["id"],
        "model_name": params["mn"],
        "signals": params["sig"],  # JSON text, as psycopg returns for ::text
        "signal_contributions": None,
    }
    contributions = boot.extract_signal_contributions(
        row, oracle_models_lookup={"fixture_model": ["family_a", "family_b"]}
    )
    assert contributions == pytest.approx(
        {"sector_network": 0.5, "timesfm_direction_7d": 0.5}
    )

    updates = per_signal_brier.record_scored_prediction(
        _RecordingEngine(),
        horizon_days=7,
        confidence=0.7,
        outcome=1.0,
        signal_contributions=contributions,
    )
    assert set(updates) == {"sector_network", "timesfm_direction_7d"}
    # (0.7 - 1.0)^2 = 0.09, attributed by a 0.5 share each.
    for source in updates:
        assert updates[source]["scored_count"] == 1
        assert updates[source]["running_brier"] == pytest.approx(0.045)
