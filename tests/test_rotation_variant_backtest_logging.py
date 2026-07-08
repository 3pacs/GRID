"""Regression test for alpha_research/strategies/rotation_variant_backtest.py.

Covers PUNCH-LIST-2026-05-13.md line 116:
"Log skipped rebalances in backtest_rotation_variant".

Verifies that ``backtest_rotation_variant`` no longer silently swallows
``run_rotation`` exceptions — failed rebalances must surface as a
``log.warning`` so silent dropped rebalances are visible in backtest
postmortems instead of looking like "all clean".
"""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from alpha_research.strategies import rotation_variant_backtest as rvb


def _synthetic_price_panel() -> pd.DataFrame:
    dates = pd.bdate_range("2025-01-06", "2025-02-07")
    steps = np.arange(len(dates))
    return pd.DataFrame(
        {
            "AAPL": 100.0 * np.power(1.010, steps),
            "TLT": 100.0 * np.power(1.002, steps),
            "QQQ": 100.0 * np.power(1.005, steps),
            "SPY": 100.0 * np.power(1.004, steps),
        },
        index=dates,
    )


@pytest.fixture
def captured_warnings(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, dict[str, Any]]]:
    """Capture loguru ``log.warning`` calls without touching the global sink."""
    captured: list[tuple[str, dict[str, Any]]] = []

    def _capture(message: str, *_args: Any, **kwargs: Any) -> None:
        captured.append((message, kwargs))

    monkeypatch.setattr(rvb.log, "warning", _capture)
    return captured


def test_run_rotation_failure_logs_warning_and_continues(
    monkeypatch: pytest.MonkeyPatch,
    captured_warnings: list[tuple[str, dict[str, Any]]],
) -> None:
    """A raising run_rotation must be logged and skipped, not silenced.

    The first two rebalance dates raise; the rest succeed. The backtest
    should complete (graceful degradation) AND each failure should have
    surfaced via log.warning with the asof and the underlying error.
    """
    engine = MagicMock()
    price_panel = _synthetic_price_panel()
    window_start = date(2025, 1, 6)
    window_end = date(2025, 2, 7)
    config = rvb.RotationConfig(TREND_WEEKS=10, RANKING_WEEKS=4)

    monkeypatch.setattr(
        rvb,
        "build_price_panel",
        lambda *_args, **_kwargs: price_panel,
    )

    successful_dates = {date(2025, 1, 20), date(2025, 1, 27), date(2025, 2, 3)}
    rotation_calls: list[date] = []

    def fake_run_rotation(_engine: Any, as_of_date: date, positions: dict) -> Any:
        rotation_calls.append(as_of_date)
        if as_of_date not in successful_dates:
            raise RuntimeError(f"db hiccup on {as_of_date}")
        return SimpleNamespace(weights={"AAPL": 1.0})

    monkeypatch.setattr(rvb.ar, "run_rotation", fake_run_rotation)

    metrics = rvb.backtest_rotation_variant(
        engine=engine,
        config=config,
        window_start=window_start,
        window_end=window_end,
        persist=False,
    )

    # Backtest still completes despite the failures (graceful degradation preserved);
    # only the 3 successful rebalances contribute to the returned series.
    assert "error" not in metrics
    assert metrics["rebalance_count"] == 3
    assert "backtest_run_id" not in metrics  # persist=False

    # Both failing rebalances surfaced a warning, including the asof + underlying error.
    assert len(captured_warnings) == 2
    failing_asofs = {asof for asof in rotation_calls if asof not in successful_dates}
    assert len(failing_asofs) == 2
    for message, kwargs in captured_warnings:
        assert "rotation" in message.lower() and "failed" in message.lower()
        assert kwargs.get("asof") in failing_asofs
        assert "db hiccup" in str(kwargs.get("err", ""))


def test_clean_run_emits_no_warning(
    monkeypatch: pytest.MonkeyPatch,
    captured_warnings: list[tuple[str, dict[str, Any]]],
) -> None:
    """No exception path must not produce noise on errors.jsonl."""
    engine = MagicMock()
    price_panel = _synthetic_price_panel()
    config = rvb.RotationConfig(TREND_WEEKS=10, RANKING_WEEKS=4)

    monkeypatch.setattr(
        rvb,
        "build_price_panel",
        lambda *_args, **_kwargs: price_panel,
    )
    monkeypatch.setattr(
        rvb.ar,
        "run_rotation",
        lambda *_args, **_kwargs: SimpleNamespace(weights={"AAPL": 1.0}),
    )

    rvb.backtest_rotation_variant(
        engine=engine,
        config=config,
        window_start=date(2025, 1, 6),
        window_end=date(2025, 2, 7),
        persist=False,
    )

    assert captured_warnings == []
