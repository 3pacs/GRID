"""Tests for the ReasoningBank success-side hook in scripts/score_oracle_trades.py.

Covers:
  - `_parse_signals_blob` shape coercion (dict / json-string / None / garbage / non-string)
  - `_record_success_lesson_safe` happy path: forwards verdict / horizon / fingerprint
  - `_record_success_lesson_safe` is fully defensive — never raises, even when the
    underlying record_success_lesson throws or the import fails.

The scoring loop only fires this path on hit/partial verdicts. We never want a
failure here to block the verdict UPDATE — these tests guard that contract.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any
from unittest.mock import patch

import pytest

from scripts.score_oracle_trades import (
    _parse_signals_blob,
    _record_success_lesson_safe,
)


# ── _parse_signals_blob ──────────────────────────────────────────────────


def test_parse_signals_blob_dict_passthrough() -> None:
    assert _parse_signals_blob({"regime": "GROWTH"}) == {"regime": "GROWTH"}


def test_parse_signals_blob_json_string_decodes() -> None:
    assert _parse_signals_blob('{"vix_level": 18.4}') == {"vix_level": 18.4}


def test_parse_signals_blob_none_returns_none() -> None:
    assert _parse_signals_blob(None) is None


def test_parse_signals_blob_invalid_json_returns_none() -> None:
    assert _parse_signals_blob("not json at all") is None


def test_parse_signals_blob_non_dict_json_returns_none() -> None:
    # JSON-decodes to a list, not a dict — fingerprint builder needs dict.
    assert _parse_signals_blob("[1, 2, 3]") is None


def test_parse_signals_blob_unsupported_type_returns_none() -> None:
    assert _parse_signals_blob(42) is None
    assert _parse_signals_blob(object()) is None


# ── _record_success_lesson_safe — happy path ─────────────────────────────


def _base_kwargs(**overrides: Any) -> dict[str, Any]:
    base = dict(
        engine=object(),
        prediction_id="pred-abc-123",
        ticker="TSM",
        direction="CALL",
        verdict="hit",
        confidence=0.72,
        expected_move_pct=3.0,
        actual_move_pct=4.1,
        pnl_pct=4.1,
        signals_blob={"regime": "GROWTH", "fci_regime": "EASY"},
        created_at=datetime(2026, 4, 1, 12, 0, 0),
        expiry=date(2026, 4, 8),
        model="oracle_v3",
    )
    base.update(overrides)
    return base


def test_success_lesson_forwards_canonical_fields() -> None:
    captured: dict[str, Any] = {}

    def _fake_record(engine: Any, **kwargs: Any) -> int | None:
        captured["engine"] = engine
        captured.update(kwargs)
        return 99

    with patch(
        "intelligence.postmortem.record_success_lesson", side_effect=_fake_record
    ):
        _record_success_lesson_safe(**_base_kwargs())

    assert captured["trade_id_or_prediction_id"] == "pred-abc-123"
    assert captured["ticker"] == "TSM"
    assert captured["direction"] == "CALL"
    assert captured["outcome"] == "hit"
    # signals_blob reaches the helper as data_at_decision
    assert captured["data_at_decision"] == {
        "regime": "GROWTH", "fci_regime": "EASY",
    }
    # horizon derived from (expiry - created_at).days
    assert captured["horizon_days"] == 7
    # narrative fields populated, not blank
    assert "TSM" in captured["thesis_at_decision"]
    assert "+4.10" in captured["what_worked"]
    assert "TSM CALL" in captured["generalizable_takeaway"]


def test_success_lesson_handles_partial_verdict() -> None:
    captured: dict[str, Any] = {}

    def _fake_record(engine: Any, **kwargs: Any) -> int | None:
        captured.update(kwargs)
        return 7

    with patch(
        "intelligence.postmortem.record_success_lesson", side_effect=_fake_record
    ):
        _record_success_lesson_safe(**_base_kwargs(verdict="partial"))

    assert captured["outcome"] == "partial"
    assert "partial" in captured["generalizable_takeaway"]


def test_success_lesson_handles_string_signals_blob() -> None:
    captured: dict[str, Any] = {}

    def _fake_record(engine: Any, **kwargs: Any) -> int | None:
        captured.update(kwargs)
        return 1

    with patch(
        "intelligence.postmortem.record_success_lesson", side_effect=_fake_record
    ):
        _record_success_lesson_safe(
            **_base_kwargs(signals_blob='{"regime":"FRAGILE"}')
        )

    assert captured["data_at_decision"] == {"regime": "FRAGILE"}


def test_success_lesson_handles_unparseable_signals_blob() -> None:
    captured: dict[str, Any] = {}

    def _fake_record(engine: Any, **kwargs: Any) -> int | None:
        captured.update(kwargs)
        return 1

    with patch(
        "intelligence.postmortem.record_success_lesson", side_effect=_fake_record
    ):
        _record_success_lesson_safe(**_base_kwargs(signals_blob="garbage"))

    # Garbage in → None data_at_decision, but call still proceeds.
    assert captured["data_at_decision"] is None


def test_success_lesson_handles_missing_horizon_inputs() -> None:
    captured: dict[str, Any] = {}

    def _fake_record(engine: Any, **kwargs: Any) -> int | None:
        captured.update(kwargs)
        return 1

    with patch(
        "intelligence.postmortem.record_success_lesson", side_effect=_fake_record
    ):
        _record_success_lesson_safe(
            **_base_kwargs(created_at=None, expiry=None)
        )

    assert captured["horizon_days"] is None
    # narrative still produced even without horizon info
    assert captured["what_worked"]


# ── Defensive boundary — never raise ─────────────────────────────────────


def test_success_lesson_swallows_record_exception() -> None:
    def _boom(*_a: Any, **_k: Any) -> int | None:
        raise RuntimeError("DB exploded")

    with patch(
        "intelligence.postmortem.record_success_lesson", side_effect=_boom
    ):
        # Must NOT raise — scoring path depends on this contract.
        _record_success_lesson_safe(**_base_kwargs())


def test_success_lesson_swallows_import_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    # Simulate the postmortem module not being importable.
    import builtins

    real_import = builtins.__import__

    def _fake_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "intelligence.postmortem":
            raise ImportError("simulated missing module")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)
    # Must not raise.
    _record_success_lesson_safe(**_base_kwargs())


def test_success_lesson_swallows_horizon_arithmetic_failure() -> None:
    captured: dict[str, Any] = {}

    def _fake_record(engine: Any, **kwargs: Any) -> int | None:
        captured.update(kwargs)
        return 1

    # An object that can't subtract — exercises the inner try/except around
    # horizon arithmetic.
    class _Weird:
        pass

    with patch(
        "intelligence.postmortem.record_success_lesson", side_effect=_fake_record
    ):
        _record_success_lesson_safe(
            **_base_kwargs(created_at=_Weird(), expiry=_Weird())
        )

    assert captured["horizon_days"] is None
