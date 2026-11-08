"""Offline replay barrier checks; every side effect is mocked or forbidden."""
from __future__ import annotations

import builtins
import json
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from contracts.financial_signal_guard import is_financial_divergence_signal
from contracts.handlers import journal, oracle_signals, trust
from contracts.schemas import SignalFired


_BLOCKED_MARKERS = [
    pytest.param({"signal_type": "fundamental_divergence"}, id="type-only"),
    pytest.param({"source": "fundamental_divergence"}, id="canonical-source-only"),
    pytest.param({"source": "fundamental_divergence:Technology"}, id="sector-source-only"),
    pytest.param({"source": "fundamental_divergence:"}, id="empty-sector-source"),
    pytest.param(
        {"producer_module": "intelligence.fundamental_divergence"}, id="producer-only",
    ),
    pytest.param({"signal_type": "  FUNDAMENTAL_DIVERGENCE  "}, id="normalized-type"),
    pytest.param(
        {"source": "  FUNDAMENTAL_DIVERGENCE:Technology  "}, id="normalized-source",
    ),
    pytest.param(
        {"producer_module": "  INTELLIGENCE.FUNDAMENTAL_DIVERGENCE  "},
        id="normalized-producer",
    ),
    pytest.param(
        {"signal_type": "holder_overlap", "source": "fundamental_divergence:Technology"},
        id="conflicting-type-and-source",
    ),
]
_HANDLERS = [oracle_signals.on_signal_fired, trust.on_signal_fired, journal.on_signal_fired]
_HANDLER_IDS = ["oracle", "trust", "journal"]


@pytest.mark.parametrize("markers", _BLOCKED_MARKERS)
def test_classifier_recognizes_each_financial_marker(markers):
    assert is_financial_divergence_signal(SimpleNamespace(**markers))


@pytest.mark.parametrize("markers", [
    {},
    {"signal_type": "fundamental_divergence_v2"},
    {"signal_type": "fundamental_divergence:Technology"},
    {"source": "other:fundamental_divergence"},
    {"source": "fundamental_divergence_report:Technology"},
    {"source": "fundamental_divergence-notice"},
    {"source": "prefundamental_divergence"},
    {"producer_module": "other.intelligence.fundamental_divergence"},
    {"producer_module": "intelligence.fundamental_divergence_helpers"},
    {"producer_module": "intelligence.fundamental_divergence.extra"},
    {"source": None, "signal_type": 123, "producer_module": []},
])
def test_classifier_does_not_match_substrings_or_coerce_nonstrings(markers):
    assert not is_financial_divergence_signal(SimpleNamespace(**markers))


@pytest.mark.parametrize("handler", _HANDLERS, ids=_HANDLER_IDS)
@pytest.mark.parametrize("markers", _BLOCKED_MARKERS)
def test_financial_replays_stop_before_imports_strength_or_io(monkeypatch, handler, markers):
    class UnreadableStrength:
        def __float__(self):
            pytest.fail("Quarantined events must stop before strength conversion")

    event_fields = {
        "signal_type": "holder_overlap",
        "source": "holder_overlap:Vanguard",
        "producer_module": "intelligence.holder_deal_overlap",
        "ticker": "NVDA",
        "strength": UnreadableStrength(),
    }
    event_fields.update(markers)
    event = SimpleNamespace(**event_fields)
    engine = MagicMock()
    original_import = builtins.__import__

    def reject_effect_import(name, *args, **kwargs):
        if name in {"intelligence.trust_scorer", "api.dependencies", "contracts.emit", "db"}:
            pytest.fail(f"Quarantined events must not import side-effect path {name}")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", reject_effect_import)
    # Repeat delivery must remain a pure no-op, not create duplicate evidence.
    assert handler(event, engine=engine) is None
    assert handler(event, engine=engine) is None
    assert engine.mock_calls == []


@pytest.mark.parametrize("handler", _HANDLERS, ids=_HANDLER_IDS)
@pytest.mark.parametrize("strength", [0.95, -0.95])
@pytest.mark.parametrize("source, producer", [
    ("holder_overlap:Vanguard", "intelligence.holder_deal_overlap"),
    ("fundamental_divergence_report:Technology", "intelligence.fundamental_divergence_helpers"),
])
def test_unrelated_events_keep_existing_mocked_effects(
    monkeypatch, handler, strength, source, producer,
):
    event = SignalFired(
        producer_module=producer,
        correlation_id=uuid4(),
        signal_id=uuid4(),
        source=source,
        signal_type="holder_overlap",
        strength=strength,
        ticker="NVDA",
        raw_row_ids=[42],
    )
    engine = MagicMock()
    conn = engine.begin.return_value.__enter__.return_value
    conn.execute.return_value.fetchone.return_value = (17,)
    scorer = ModuleType("intelligence.trust_scorer")
    register = MagicMock(return_value=123)
    scorer.register_signal = register
    monkeypatch.setitem(sys.modules, "intelligence.trust_scorer", scorer)

    assert not is_financial_divergence_signal(event)
    handler(event, engine=engine)

    if handler is trust.on_signal_fired:
        register.assert_called_once()
        args, kwargs = register.call_args
        assert args == (engine,)
        assert kwargs["source_type"] == source
        assert kwargs["signal_type"] == ("BUY" if strength > 0 else "SELL")
        assert kwargs["signal_value"] is None
        assert kwargs["metadata"]["strength"] == strength
        assert engine.mock_calls == []
    elif handler is oracle_signals.on_signal_fired:
        register.assert_not_called()
        engine.begin.assert_called_once()
        conn.execute.assert_called_once()
        statement, params = conn.execute.call_args.args
        assert "INSERT INTO signal_sources" in str(statement)
        assert params["st"] == "holder_overlap"
        assert params["si"] == source
        assert params["d"] == ("BUY" if strength > 0 else "SELL")
    else:
        register.assert_not_called()
        engine.begin.assert_called_once()
        assert conn.execute.call_count == 2
        statement, params = conn.execute.call_args.args
        assert "INSERT INTO decision_journal" in str(statement)
        assert params["state"] == "PROVISIONAL_HOLDER_OVERLAP"
        assert json.loads(params["cf"])["source"] == source
        assert params["cid"] == str(event.event_id)
