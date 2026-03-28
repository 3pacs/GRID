"""Tests for inference.trade_logger — trade logging."""

from __future__ import annotations

import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from inference.trade_logger import GridTradeLogger


CLASS_NAMES = ["GROWTH", "NEUTRAL", "FRAGILE", "CRISIS"]


def _make_data(n: int = 20, seed: int = 42):
    rng = np.random.RandomState(seed)
    proba = rng.dirichlet(alpha=[2, 3, 1, 0.5], size=n)
    df = pd.DataFrame(proba, columns=CLASS_NAMES)
    actuals = pd.Series(
        [CLASS_NAMES[rng.choice(4, p=proba[i])] for i in range(n)],
    )
    return df, actuals


def _make_exec_results(n: int = 10, seed: int = 42):
    rng = np.random.RandomState(seed)
    per_trade = []
    for i in range(n):
        per_trade.append({
            "observation": i,
            "side": "buy",
            "edge": 0.10,
            "confidence": 0.75,
            "size": 100.0,
            "filled_size": 95.0,
            "fill_price": 0.55,
            "slippage_bps": 5.0,
            "fill_rate": 0.95,
            "outcome": "correct",
            "pnl": rng.uniform(-10, 20),
        })
    return {
        "per_trade": per_trade,
        "n_trades": n,
        "total_pnl": sum(t["pnl"] for t in per_trade),
    }


class TestGridTradeLogger:
    def test_init_creates_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_dir = Path(tmpdir) / "test_trades"
            logger = GridTradeLogger(log_dir)
            assert logger.log_dir == log_dir
            assert log_dir.exists()

    def test_log_execution_trades(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            df, actuals = _make_data(10)
            exec_results = _make_exec_results(10)
            logger = GridTradeLogger(Path(tmpdir) / "trades")

            count = logger.log_execution_trades(
                exec_results=exec_results,
                predictions=df,
                actuals=actuals,
                class_names=CLASS_NAMES,
                model_version="test_v1",
            )
            assert count == 10

            # Verify logs were written
            all_logs = logger.load_all()
            assert len(all_logs) == 10

    def test_log_execution_empty(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            df, actuals = _make_data(5)
            logger = GridTradeLogger(Path(tmpdir) / "trades")
            count = logger.log_execution_trades(
                exec_results={"per_trade": []},
                predictions=df,
                actuals=actuals,
            )
            assert count == 0

    def test_log_journal_decision(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = GridTradeLogger(Path(tmpdir) / "trades")
            logger.log_journal_decision(
                inferred_state="GROWTH",
                confidence=0.82,
                recommendation="BUY",
                model_version="ensemble_v2",
            )
            logs = logger.load_all()
            assert len(logs) == 1
            assert logs[0].rationale["predicted_regime"] == "GROWTH"
            assert logs[0].decision == "buy"

    def test_log_journal_hold(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = GridTradeLogger(Path(tmpdir) / "trades")
            logger.log_journal_decision(
                inferred_state="NEUTRAL",
                confidence=0.60,
                recommendation="HOLD",
            )
            logs = logger.load_all()
            assert len(logs) == 1
            assert logs[0].decision == "pass"

    def test_log_journal_with_outcome(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = GridTradeLogger(Path(tmpdir) / "trades")
            logger.log_journal_decision(
                inferred_state="CRISIS",
                confidence=0.90,
                recommendation="SELL",
                outcome=1,
                outcome_value=-50.0,
            )
            logs = logger.load_all()
            assert logs[0].outcome == 1
            assert logs[0].pnl == -50.0

    def test_load_recent(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            df, actuals = _make_data(10)
            exec_results = _make_exec_results(10)
            logger = GridTradeLogger(Path(tmpdir) / "trades")
            logger.log_execution_trades(
                exec_results=exec_results,
                predictions=df,
                actuals=actuals,
            )
            recent = logger.load_recent(days=1)
            assert len(recent) == 10

    def test_update_outcomes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = GridTradeLogger(Path(tmpdir) / "trades")
            logger.log_journal_decision(
                inferred_state="GROWTH",
                confidence=0.80,
                recommendation="BUY",
            )
            # Market ID is "decision_GROWTH"
            updated = logger.update_outcomes({"decision_GROWTH": 1})
            assert updated == 1
            logs = logger.load_all()
            assert logs[0].outcome == 1
