from __future__ import annotations

import json
from datetime import date, timedelta
from types import SimpleNamespace
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


def _engine_with_persist_result(run_id: int = 1234) -> tuple[MagicMock, MagicMock]:
    engine = MagicMock()
    conn = MagicMock()
    run_insert_result = MagicMock()
    run_insert_result.scalar_one.return_value = run_id

    engine.begin.return_value.__enter__.return_value = conn
    engine.begin.return_value.__exit__.return_value = False
    conn.execute.side_effect = [run_insert_result, MagicMock()]

    return engine, conn


def test_backtest_rotation_variant_persists_summary_payload(monkeypatch):
    engine, conn = _engine_with_persist_result()
    price_panel = _synthetic_price_panel()
    window_start = date(2025, 1, 6)
    window_end = date(2025, 2, 7)
    config = rvb.RotationConfig(TREND_WEEKS=10, RANKING_WEEKS=4)

    panel_calls = []

    def fake_build_price_panel(engine_arg, tickers, start_date, end_date):
        panel_calls.append(
            {
                "engine": engine_arg,
                "tickers": set(tickers),
                "start_date": start_date,
                "end_date": end_date,
            }
        )
        return price_panel

    rebalance_weights = {
        date(2025, 1, 6): {"AAPL": 0.6, "TLT": 0.4},
        date(2025, 1, 13): {"AAPL": 0.6, "TLT": 0.4},
        date(2025, 1, 20): {"AAPL": 1.0},
        date(2025, 1, 27): {"AAPL": 1.0},
        date(2025, 2, 3): {"AAPL": 1.0},
    }
    rotation_calls = []

    def fake_run_rotation(engine_arg, as_of_date, positions):
        rotation_calls.append(
            {
                "engine": engine_arg,
                "as_of_date": as_of_date,
                "positions": positions,
            }
        )
        return SimpleNamespace(weights=rebalance_weights[as_of_date])

    monkeypatch.setattr(rvb, "build_price_panel", fake_build_price_panel)
    monkeypatch.setattr(rvb.ar, "run_rotation", fake_run_rotation)

    metrics = rvb.backtest_rotation_variant(
        engine=engine,
        config=config,
        window_start=window_start,
        window_end=window_end,
        persist=True,
    )

    expected_metric_keys = {
        "config_id",
        "config",
        "window_start",
        "window_end",
        "rebalance_count",
        "total_return",
        "annualized_return",
        "benchmark_return",
        "alpha_vs_benchmark",
        "sharpe",
        "sortino",
        "max_drawdown",
        "win_rate_per_rebalance",
        "avg_holding_days",
        "n_trades",
    }
    assert set(metrics) == expected_metric_keys | {"backtest_run_id"}
    assert metrics["backtest_run_id"] == 1234
    assert metrics["config_id"] == config.fingerprint()
    assert metrics["config"] == {
        "TREND_WEEKS": 10,
        "VIX_ZSCORE_THRESHOLD": 3.0,
        "DRAWDOWN_THRESHOLD": -0.03,
        "DRAWDOWN_WINDOW": 3,
        "FAST_RISK_OFF_DURATION": 10,
        "FAST_RISK_OFF_CASH_FLOOR": 0.5,
        "RANKING_WEEKS": 4,
        "ABSOLUTE_STOP": 0.05,
        "TRAILING_STOP": 0.1,
        "COOLDOWN_DAYS": 20,
        "MAX_ACTIVE_GROUPS": 2,
    }
    assert metrics["window_start"] == "2025-01-06"
    assert metrics["window_end"] == "2025-02-07"
    assert metrics["rebalance_count"] == 5
    assert metrics["n_trades"] == 4
    assert metrics["avg_holding_days"] == pytest.approx(24.5)
    assert metrics["alpha_vs_benchmark"] == pytest.approx(
        metrics["total_return"] - metrics["benchmark_return"]
    )

    assert len(panel_calls) == 1
    assert panel_calls[0]["engine"] is engine
    assert panel_calls[0]["start_date"] == window_start - timedelta(days=400)
    assert panel_calls[0]["end_date"] == window_end
    assert {"SPY", "QQQ", "AAPL", "TLT"} <= panel_calls[0]["tickers"]
    assert [call["as_of_date"] for call in rotation_calls] == list(rebalance_weights)
    assert all(call["engine"] is engine for call in rotation_calls)
    assert all(call["positions"] == {} for call in rotation_calls)

    engine.begin.assert_called_once_with()
    assert conn.execute.call_count == 2
    run_stmt, run_params = conn.execute.call_args_list[0].args
    result_stmt, result_params = conn.execute.call_args_list[1].args

    run_sql = str(run_stmt)
    assert "INSERT INTO astrogrid.backtest_run" in run_sql
    assert "params_payload" in run_sql
    assert "ON CONFLICT (run_key) DO UPDATE SET" in run_sql
    assert "RETURNING id" in run_sql
    assert set(run_params) == {"run_key", "ws", "we", "payload"}
    assert run_params["run_key"] == (
        f"rotation_variant_{config.fingerprint()}_{window_start}_{window_end}"
    )
    assert run_params["ws"] == window_start
    assert run_params["we"] == window_end

    run_payload = json.loads(run_params["payload"])
    assert set(run_payload) == {"config", "summary"}
    assert run_payload["config"] == metrics["config"]
    assert set(run_payload["summary"]) == expected_metric_keys - {"config"}
    assert "backtest_run_id" not in run_payload["summary"]
    assert run_payload["summary"]["alpha_vs_benchmark"] == pytest.approx(
        metrics["alpha_vs_benchmark"]
    )

    result_sql = str(result_stmt)
    assert "INSERT INTO astrogrid.backtest_result" in result_sql
    assert "metrics_payload" in result_sql
    assert "ON CONFLICT (backtest_run_id, result_key) DO UPDATE SET" in result_sql
    assert set(result_params) == {"rid", "we", "alpha", "metrics"}
    assert result_params["rid"] == 1234
    assert result_params["we"] == window_end
    assert result_params["alpha"] == pytest.approx(metrics["alpha_vs_benchmark"])

    result_payload = json.loads(result_params["metrics"])
    assert set(result_payload) == expected_metric_keys
    assert "backtest_run_id" not in result_payload
    assert result_payload["config"] == metrics["config"]
    assert result_payload["alpha_vs_benchmark"] == pytest.approx(
        metrics["alpha_vs_benchmark"]
    )
