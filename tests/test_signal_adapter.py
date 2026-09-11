"""Unit tests for alpha_research.adapters.signal_adapter publish helpers."""

from __future__ import annotations

import json
import sys
import types
from datetime import date, timedelta, timezone
from typing import Any

import pandas as pd
import pytest

from alpha_research.adapters import signal_adapter
from intelligence.signal_registry import make_signal_id


AS_OF = date(2026, 6, 15)

REGISTRY_PARAM_KEYS = {
    "signal_id",
    "source_module",
    "signal_type",
    "ticker",
    "direction",
    "value",
    "z_score",
    "confidence",
    "valid_from",
    "valid_until",
    "freshness_hours",
    "metadata",
    "provenance",
}


class _ExecuteResult:
    def __init__(self, rowcount: int = 1) -> None:
        self.rowcount = rowcount


class _RecordingConnection:
    def __init__(self, rowcount: int = 1) -> None:
        self.rowcount = rowcount
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def execute(self, stmt: Any, params: dict[str, Any] | None = None) -> _ExecuteResult:
        sql = getattr(stmt, "text", None) or str(stmt)
        self.calls.append((sql, dict(params or {})))
        return _ExecuteResult(self.rowcount)


class _BeginContext:
    def __init__(self, conn: _RecordingConnection) -> None:
        self.conn = conn

    def __enter__(self) -> _RecordingConnection:
        return self.conn

    def __exit__(self, *exc: Any) -> bool:
        return False


class _RecordingEngine:
    def __init__(self, rowcount: int = 1) -> None:
        self.conn = _RecordingConnection(rowcount=rowcount)
        self.begin_calls = 0

    def begin(self) -> _BeginContext:
        self.begin_calls += 1
        return _BeginContext(self.conn)


def _registry_rows(engine: _RecordingEngine) -> list[dict[str, Any]]:
    return [
        params
        for sql, params in engine.conn.calls
        if "INSERT INTO signal_registry" in sql
    ]


def _registry_rows_by_ticker(engine: _RecordingEngine) -> dict[str, dict[str, Any]]:
    return {row["ticker"]: row for row in _registry_rows(engine)}


def _factor_panel() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "AAPL": 0.10,
                "MSFT": 0.90,
                "NVDA": 0.50,
                "TSLA": 0.70,
                "META": 0.30,
            },
            {
                "AAPL": 0.95,
                "MSFT": 0.05,
                "NVDA": 0.50,
                "TSLA": 0.80,
                "META": 0.20,
            },
        ],
        index=pd.to_datetime(["2026-06-14", "2026-06-15"]),
    )


def _module(name: str, **attrs: Any) -> types.ModuleType:
    module = types.ModuleType(name)
    for attr_name, value in attrs.items():
        setattr(module, attr_name, value)
    return module


@pytest.mark.parametrize(
    ("signal_name", "expected"),
    [
        (
            "vol_price_divergence",
            {
                "AAPL": "bearish",
                "MSFT": "bullish",
                "NVDA": "neutral",
                "TSLA": "bearish",
                "META": "bullish",
            },
        ),
        (
            "dual_horizon_equity",
            {
                "AAPL": "bullish",
                "MSFT": "bearish",
                "NVDA": "neutral",
                "TSLA": "bullish",
                "META": "bearish",
            },
        ),
    ],
)
def test_publish_factor_signals_inverts_direction_by_signal_type(
    signal_name: str,
    expected: dict[str, str],
) -> None:
    engine = _RecordingEngine()

    count = signal_adapter.publish_factor_signals(
        engine,
        signal_name,
        _factor_panel(),
        as_of_date=AS_OF,
        top_pct=0.20,
        confidence=0.75,
        valid_hours=6,
    )

    assert count == 5
    rows = _registry_rows_by_ticker(engine)
    assert {ticker: row["direction"] for ticker, row in rows.items()} == expected


def test_publish_factor_signals_writes_registry_payload_shape() -> None:
    engine = _RecordingEngine()

    count = signal_adapter.publish_factor_signals(
        engine,
        "dual_horizon_equity",
        _factor_panel(),
        as_of_date=AS_OF,
        confidence=0.72,
        valid_hours=3,
    )

    assert count == 5
    assert engine.begin_calls == 1
    sql, _ = engine.conn.calls[0]
    assert "INSERT INTO signal_registry" in sql
    assert "ON CONFLICT (signal_id, valid_from) DO NOTHING" in sql

    row = _registry_rows_by_ticker(engine)["AAPL"]
    assert set(row) == REGISTRY_PARAM_KEYS
    assert row["signal_id"] == make_signal_id(
        "alpha_research:dual_horizon_equity",
        f"AAPL:{AS_OF}",
    )
    assert row["source_module"] == "alpha_research:dual_horizon_equity"
    assert row["signal_type"] == "DIRECTIONAL"
    assert row["ticker"] == "AAPL"
    assert row["direction"] == "bullish"
    assert row["value"] == pytest.approx(0.9)
    assert row["z_score"] == pytest.approx(0.9)
    assert row["confidence"] == 0.72
    assert row["freshness_hours"] == 3.0
    assert row["valid_from"].tzinfo is timezone.utc
    assert row["valid_until"] - row["valid_from"] == timedelta(hours=3)
    assert json.loads(row["metadata"]) == {
        "signal_name": "dual_horizon_equity",
        "rank": 0.95,
    }
    assert row["provenance"] == f"alpha_research/dual_horizon_equity as_of {AS_OF}"


def test_publish_factor_signals_returns_zero_without_registry_write_for_sparse_panel() -> None:
    engine = _RecordingEngine()
    panel = pd.DataFrame(
        [{"AAPL": 0.9, "MSFT": 0.1, "NVDA": 0.5, "TSLA": 0.8}],
        index=pd.to_datetime(["2026-06-15"]),
    )

    count = signal_adapter.publish_factor_signals(
        engine,
        "dual_horizon_equity",
        panel,
        as_of_date=AS_OF,
    )

    assert count == 0
    assert engine.begin_calls == 0
    assert engine.conn.calls == []


def test_publish_regime_signal_writes_neutral_regime_payload() -> None:
    engine = _RecordingEngine()

    count = signal_adapter.publish_regime_signal(
        engine,
        "vix_exposure",
        "stressed",
        confidence=0.66,
        metadata={"ratio": 1.45, "scalar": 0.55},
        valid_hours=12,
    )

    assert count == 1
    rows = _registry_rows(engine)
    assert len(rows) == 1
    row = rows[0]
    assert set(row) == REGISTRY_PARAM_KEYS
    assert row["signal_id"] == make_signal_id(
        "alpha_research:vix_exposure",
        f"regime:stressed:{row['valid_from'].date()}",
    )
    assert row["source_module"] == "alpha_research:vix_exposure"
    assert row["signal_type"] == "REGIME"
    assert row["ticker"] is None
    assert row["direction"] == "neutral"
    assert row["value"] == 0.66
    assert row["confidence"] == 0.66
    assert row["z_score"] is None
    assert row["freshness_hours"] == 12.0
    assert row["valid_from"].tzinfo is timezone.utc
    assert row["valid_until"] - row["valid_from"] == timedelta(hours=12)
    assert json.loads(row["metadata"]) == {
        "state": "stressed",
        "ratio": 1.45,
        "scalar": 0.55,
    }
    assert row["provenance"] == "alpha_research/vix_exposure"


def test_publish_all_alpha_signals_orchestrates_factor_and_regime_publish(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = _RecordingEngine()
    calls: list[tuple[str, Any]] = []
    prices = pd.DataFrame(
        {"AAPL": [100.0, 101.0], "MSFT": [50.0, 51.0]},
        index=pd.to_datetime(["2026-06-14", "2026-06-15"]),
    )

    def build_price_panel(engine_arg: Any, start_date: date, end_date: date) -> pd.DataFrame:
        calls.append(("build_price_panel", (engine_arg, start_date, end_date)))
        return prices

    def factor(name: str) -> Any:
        def compute(prices_arg: pd.DataFrame) -> pd.DataFrame:
            calls.append((name, prices_arg))
            return _factor_panel()

        return compute

    def compute_vix(engine_arg: Any, as_of_date: date) -> dict[str, Any]:
        calls.append(("compute_vix_exposure_scalar", (engine_arg, as_of_date)))
        return {"regime_hint": "elevated", "ratio": 1.35, "scalar": 0.65}

    def compute_credit(engine_arg: Any, as_of_date: date) -> dict[str, Any]:
        calls.append(("compute_credit_cycle", (engine_arg, as_of_date)))
        return {"state": "contraction", "confidence": 0.44}

    monkeypatch.setitem(
        sys.modules,
        "alpha_research.data.panel_builder",
        _module(
            "alpha_research.data.panel_builder",
            build_price_panel=build_price_panel,
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "alpha_research.signals.exposure_scaler",
        _module(
            "alpha_research.signals.exposure_scaler",
            compute_vix_exposure_scalar=compute_vix,
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "alpha_research.signals.credit_cycle",
        _module(
            "alpha_research.signals.credit_cycle",
            compute_credit_cycle=compute_credit,
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "alpha_research.signals.quanta_alpha",
        _module(
            "alpha_research.signals.quanta_alpha",
            vol_price_divergence=factor("vol_price_divergence"),
            vol_regime_adaptive_equity=factor("vol_regime_adaptive_equity"),
            dual_horizon_equity=factor("dual_horizon_equity"),
        ),
    )

    results = signal_adapter.publish_all_alpha_signals(engine, as_of_date=AS_OF)

    assert results == {
        "vol_price_divergence": 5,
        "vol_regime_equity": 5,
        "dual_horizon_equity": 5,
        "vix_exposure": 1,
        "credit_cycle": 1,
    }
    assert calls[0] == (
        "build_price_panel",
        (engine, AS_OF - timedelta(days=120), AS_OF),
    )
    assert [name for name, _ in calls] == [
        "build_price_panel",
        "vol_price_divergence",
        "vol_regime_adaptive_equity",
        "dual_horizon_equity",
        "compute_vix_exposure_scalar",
        "compute_credit_cycle",
    ]

    source_counts: dict[str, int] = {}
    for row in _registry_rows(engine):
        source_counts[row["source_module"]] = source_counts.get(row["source_module"], 0) + 1

    assert source_counts == {
        "alpha_research:vol_price_divergence": 5,
        "alpha_research:vol_regime_equity": 5,
        "alpha_research:dual_horizon_equity": 5,
        "alpha_research:vix_exposure": 1,
        "alpha_research:credit_cycle": 1,
    }

    vix_row = [
        row
        for row in _registry_rows(engine)
        if row["source_module"] == "alpha_research:vix_exposure"
    ][0]
    assert vix_row["confidence"] == pytest.approx(0.70)
    assert json.loads(vix_row["metadata"])["regime_hint"] == "elevated"


def test_publish_all_alpha_signals_skips_empty_prices_and_unknown_regimes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = _RecordingEngine()

    monkeypatch.setitem(
        sys.modules,
        "alpha_research.data.panel_builder",
        _module(
            "alpha_research.data.panel_builder",
            build_price_panel=lambda *_args, **_kwargs: pd.DataFrame(),
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "alpha_research.signals.exposure_scaler",
        _module(
            "alpha_research.signals.exposure_scaler",
            compute_vix_exposure_scalar=lambda *_args, **_kwargs: {
                "regime_hint": "unknown"
            },
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "alpha_research.signals.credit_cycle",
        _module(
            "alpha_research.signals.credit_cycle",
            compute_credit_cycle=lambda *_args, **_kwargs: {
                "state": "expansion",
                "confidence": 0,
            },
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "alpha_research.signals.quanta_alpha",
        _module(
            "alpha_research.signals.quanta_alpha",
            vol_price_divergence=lambda *_args, **_kwargs: _factor_panel(),
            vol_regime_adaptive_equity=lambda *_args, **_kwargs: _factor_panel(),
            dual_horizon_equity=lambda *_args, **_kwargs: _factor_panel(),
        ),
    )

    results = signal_adapter.publish_all_alpha_signals(engine, as_of_date=AS_OF)

    assert results == {}
    assert engine.begin_calls == 0
    assert engine.conn.calls == []


def test_publish_regime_signal_docstring_has_no_dangling_todo_doc_reference() -> None:
    """Regression: PUNCH-LIST-2026-05-13 line 117.

    The comment block inside ``publish_regime_signal`` used to point readers
    at ``docs/TODO-REGIME-SIGNAL-USAGE.md`` for the "correct architecture"
    follow-up plan, but that doc has never existed in the repo. Drop the
    dangling reference so future readers do not chase a 404.
    """
    import inspect

    source = inspect.getsource(signal_adapter.publish_regime_signal)
    assert "TODO-REGIME-SIGNAL-USAGE" not in source, (
        "publish_regime_signal must not reference the missing "
        "docs/TODO-REGIME-SIGNAL-USAGE.md file"
    )
