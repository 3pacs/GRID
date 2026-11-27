"""A priced ticker whose model finds no direction leaves a stated abstention.

Before, `_oracle_one_ticker` skipped such a model with a bare `continue`, so a
captured, priced ticker (SPY on every pass) wrote no row at all and could not
be told apart from a ticker that was never run.
"""
from __future__ import annotations

import sys
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from oracle import engine as oe

NOW = datetime(2026, 11, 27, 15, 40, tzinfo=UTC)
SPOT = 731.175


def _oracle(signals: list[oe.Signal]) -> oe.OracleEngine:
    oracle = oe.OracleEngine.__new__(oe.OracleEngine)
    oracle.engine = MagicMock()
    oracle.models = [oe.OracleModel("test_model", "1.0", "test", ["equity", "vol"])]
    oracle._get_flow_context = lambda ticker: {}
    oracle._get_entry_price_reference = lambda ticker: {"status": "measured", "price": SPOT}
    oracle._get_journal_feedback = lambda ticker: {}
    oracle._get_convergence_for_ticker = lambda ticker: []
    oracle._read_options_signal = lambda ticker: oe.OptionsSignalRead(None, False)
    oracle._gather_signals = lambda ticker, families, options_read=None: list(signals)
    oracle._get_realized_daily_vol = lambda ticker, as_of, window=20: 0.01
    oracle._find_anti_signals = lambda sigs, direction, ticker=None, **kw: []
    return oracle


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    monkeypatch.setattr(oe, "_USE_SIGNAL_REGISTRY", False)
    monkeypatch.setitem(sys.modules, "intelligence.confidence_calibration",
                        SimpleNamespace(calibrate_confidence=lambda v, *a: v))
    import intelligence.signal_weight_overrides as swo
    monkeypatch.setattr(swo, "SIGNAL_OVERRIDES_ENABLED", False)


def _neutral(name: str) -> oe.Signal:
    return oe.Signal(name, "equity", 0.0, 0.4, "neutral", 1.0, 0)


def _assert_abstention(pred: oe.OraclePrediction, reason: str, n_signals: int) -> None:
    assert pred.direction == "NONE"
    assert pred.verdict == oe.Verdict.NO_DATA
    assert pred.confidence == 0.0
    assert pred.current_price == SPOT  # matches the frozen receipt
    assert pred.score_notes == "No directional evidence at decision time"
    assert pred.flow_context["abstention_kind"] == "no_direction"
    assert pred.flow_context["no_direction_reason"] == reason
    assert pred.flow_context["entry_price_basis"]["price"] == SPOT
    assert len(pred.signals) == n_signals
    assert pred.model_name == "test_model"
    assert pred.horizon_days is not None


def test_only_neutral_signals_write_a_no_direction_abstention() -> None:
    signals = [_neutral("alpha_research:vol_price_divergence") for _ in range(3)]

    preds = _oracle(signals)._oracle_one_ticker(1, "SPY", 1, NOW, {}, horizon_days=1)

    assert len(preds) == 1
    _assert_abstention(preds[0], "no_direction", 3)


def test_fewer_than_three_signals_write_a_no_direction_abstention() -> None:
    preds = _oracle([_neutral("a"), _neutral("b")])._oracle_one_ticker(
        1, "SPY", 1, NOW, {}, horizon_days=1)

    assert len(preds) == 1
    _assert_abstention(preds[0], "lt3_signals", 2)


def test_balanced_directional_signals_abstain_too() -> None:
    signals = [
        oe.Signal("up", "equity", 1.0, 1.0, "bullish", 1.0, 0),
        oe.Signal("down", "equity", -1.0, -1.0, "bearish", 1.0, 0),
        _neutral("flat"),
    ]

    preds = _oracle(signals)._oracle_one_ticker(1, "SPY", 1, NOW, {}, horizon_days=1)

    assert len(preds) == 1
    _assert_abstention(preds[0], "no_direction", 3)


def test_directional_evidence_still_yields_a_real_prediction() -> None:
    signals = [
        oe.Signal("alpha_research:dual_horizon_equity", "equity", 0.9, 0.9, "bullish", 1.0, 0),
        _neutral("b"),
        _neutral("c"),
    ]

    preds = _oracle(signals)._oracle_one_ticker(1, "QQQ", 1, NOW, {}, horizon_days=1)

    assert len(preds) == 1
    assert preds[0].direction == "CALL"
    assert preds[0].verdict == oe.Verdict.PENDING
    assert "abstention_kind" not in preds[0].flow_context
