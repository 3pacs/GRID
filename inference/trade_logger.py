"""
Execution-granularity trade logging powered by autopredict.

Supplements GRID's immutable decision journal (coarse, per-decision)
with per-trade execution logs (fine-grained, JSONL).  Every simulated
or live trade gets a structured TradeLog entry with full rationale,
enabling post-hoc failure analysis via FailureAnalyzer.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log

from autopredict.learning.logger import TradeLog, TradeLogger


# Default log directory under grid's data path
_DEFAULT_LOG_DIR = Path("data/trade_logs")

# Map GRID actions to trade decisions
_ACTION_MAP = {
    "BUY": "buy",
    "SELL": "sell",
    "HOLD": "pass",
    "REDUCE": "sell",
}


class GridTradeLogger:
    """Per-trade execution logger for GRID.

    Bridges GRID's execution simulator output and decision journal entries
    into autopredict's structured JSONL trade logs.  Logs are stored as
    daily files (``trades_YYYYMMDD.jsonl``) for efficient streaming analysis.

    Usage::

        logger = GridTradeLogger()

        # Log from execution sim
        logger.log_execution_trades(
            exec_results=sim_output,
            predictions=proba_df,
            actuals=y_series,
            class_names=["GROWTH", "NEUTRAL", "FRAGILE", "CRISIS"],
        )

        # Log from journal entry
        logger.log_journal_decision(
            inferred_state="GROWTH",
            confidence=0.82,
            recommendation="BUY",
            model_version="regime_ensemble_20260328",
        )

        # Load for analysis
        recent = logger.load_recent(days=7)
        all_logs = logger.load_all()
    """

    def __init__(self, log_dir: Path | str | None = None) -> None:
        self._dir = Path(log_dir) if log_dir else _DEFAULT_LOG_DIR
        self._logger = TradeLogger(self._dir)
        log.info("GridTradeLogger initialised — dir={d}", d=self._dir)

    @property
    def log_dir(self) -> Path:
        return self._dir

    # ── Log from execution sim results ────────────────────────────────

    def log_execution_trades(
        self,
        exec_results: dict[str, Any],
        predictions: pd.DataFrame | np.ndarray,
        actuals: pd.Series | np.ndarray,
        class_names: list[str] | None = None,
        model_version: str | None = None,
        volatility: pd.Series | None = None,
    ) -> int:
        """Log per-trade entries from an execution simulation run.

        Parameters:
            exec_results: Output from ExecutionSimulator.simulate_era().
            predictions: Probability matrix (n_samples x n_classes).
            actuals: True regime labels.
            class_names: Ordered class names.
            model_version: Identifier for the model that produced predictions.
            volatility: Per-observation volatility (enriches log context).

        Returns:
            Number of trades logged.
        """
        if class_names is None:
            class_names = ["GROWTH", "NEUTRAL", "FRAGILE", "CRISIS"]

        if isinstance(predictions, pd.DataFrame):
            proba = predictions.values
        else:
            proba = np.asarray(predictions)

        if isinstance(actuals, pd.Series):
            actual_arr = actuals.values
        else:
            actual_arr = np.asarray(actuals)

        per_trade = exec_results.get("per_trade", [])
        if not per_trade:
            return 0

        now = datetime.now(timezone.utc)
        entries: list[TradeLog] = []

        for trade in per_trade:
            obs_idx = trade.get("observation", 0)
            if obs_idx >= len(actual_arr) or obs_idx >= len(proba):
                continue

            pred_idx = int(np.argmax(proba[obs_idx]))
            pred_class = class_names[pred_idx] if pred_idx < len(class_names) else "UNKNOWN"
            actual_class = str(actual_arr[obs_idx])

            model_prob = float(proba[obs_idx, pred_idx])
            correct = 1 if pred_class == actual_class else 0

            rationale: dict[str, Any] = {
                "predicted_regime": pred_class,
                "actual_regime": actual_class,
                "category": pred_class,
                "confidence": trade.get("confidence", model_prob),
                "slippage_bps": trade.get("slippage_bps", 0.0),
                "fill_rate": trade.get("fill_rate", 1.0),
                "fill_price": trade.get("fill_price"),
            }
            if model_version:
                rationale["model_version"] = model_version
            if volatility is not None and obs_idx < len(volatility):
                vol_val = float(volatility.iloc[obs_idx] if isinstance(volatility, pd.Series) else volatility[obs_idx])
                rationale["volatility"] = vol_val

            entries.append(TradeLog(
                timestamp=now,
                market_id=f"regime_{obs_idx}",
                market_prob=0.5,
                model_prob=model_prob,
                edge=abs(trade.get("edge", model_prob - 0.5)),
                decision=trade.get("side", "buy"),
                size=trade.get("size", 0.0),
                execution_price=trade.get("fill_price"),
                outcome=correct,
                pnl=trade.get("pnl", 0.0),
                rationale=rationale,
            ))

        if entries:
            self._logger.append_batch(entries)
            log.info("Logged {n} execution trades", n=len(entries))

        return len(entries)

    # ── Log from journal decision ─────────────────────────────────────

    def log_journal_decision(
        self,
        inferred_state: str,
        confidence: float,
        recommendation: str,
        outcome: int | None = None,
        outcome_value: float | None = None,
        model_version: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        """Log a single decision journal entry as a trade log.

        Parameters:
            inferred_state: Predicted regime (GROWTH/NEUTRAL/FRAGILE/CRISIS).
            confidence: Model confidence (0-1).
            recommendation: GRID recommendation (BUY/SELL/HOLD/REDUCE).
            outcome: Binary outcome (1=correct, 0=wrong, None=pending).
            outcome_value: Realized P&L if available.
            model_version: Model identifier.
            extra: Additional context to include in rationale.
        """
        decision = _ACTION_MAP.get(recommendation, "pass")

        rationale: dict[str, Any] = {
            "predicted_regime": inferred_state,
            "category": inferred_state,
            "recommendation": recommendation,
            "confidence": confidence,
        }
        if model_version:
            rationale["model_version"] = model_version
        if extra:
            rationale.update(extra)

        entry = TradeLog(
            timestamp=datetime.now(timezone.utc),
            market_id=f"decision_{inferred_state}",
            market_prob=0.5,
            model_prob=confidence,
            edge=abs(confidence - 0.5),
            decision=decision,
            size=1.0,
            execution_price=0.5 if decision != "pass" else None,
            outcome=outcome,
            pnl=outcome_value,
            rationale=rationale,
        )

        self._logger.append(entry)
        log.debug(
            "Logged journal decision — state={s}, rec={r}, decision={d}",
            s=inferred_state, r=recommendation, d=decision,
        )

    # ── Load/query ────────────────────────────────────────────────────

    def load_all(self) -> list[TradeLog]:
        """Load all trade logs, sorted by timestamp."""
        return self._logger.load_all()

    def load_recent(self, days: int = 7) -> list[TradeLog]:
        """Load trade logs from the last N days."""
        return self._logger.load_recent(days=days)

    def update_outcomes(self, outcomes: dict[str, int]) -> int:
        """Update outcomes for resolved markets.

        Parameters:
            outcomes: Map of market_id -> outcome (1=correct, 0=wrong).

        Returns:
            Number of entries updated.
        """
        return self._logger.update_outcomes(outcomes)
