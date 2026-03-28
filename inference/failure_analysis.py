"""
Failure regime analysis powered by autopredict.

Wraps autopredict's PerformanceAnalyzer to diagnose *when and why*
GRID's regime model breaks down.  Converts GRID's decision journal
entries and execution sim results into autopredict TradeLog format,
runs failure detection, and returns actionable diagnostics.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log

from autopredict.learning.analyzer import PerformanceAnalyzer, PerformanceReport
from autopredict.learning.logger import TradeLog


# ── Regime-to-market mapping ─────────────────────────────────────────

# Map GRID regime labels to binary market outcomes (1 = favourable)
_REGIME_OUTCOME: dict[str, int] = {
    "GROWTH": 1,
    "NEUTRAL": 1,
    "FRAGILE": 0,
    "CRISIS": 0,
}

# Map GRID action recommendations to trade decisions
_ACTION_DECISION: dict[str, str] = {
    "BUY": "buy",
    "HOLD": "pass",
    "REDUCE": "sell",
    "SELL": "sell",
}


@dataclass
class FailureDiagnostic:
    """Diagnostic report from failure regime analysis.

    Attributes:
        report: Full autopredict PerformanceReport.
        failure_regimes: Human-readable failure pattern descriptions.
        recommendations: Actionable parameter/model adjustment suggestions.
        by_regime: Performance broken down by predicted regime state.
        calibration_error: Mean absolute calibration error.
        edge_capture: Fraction of predicted edge actually captured.
    """

    report: PerformanceReport
    failure_regimes: list[str]
    recommendations: list[str]
    by_regime: dict[str, dict[str, Any]]
    calibration_error: float
    edge_capture: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_trades": self.report.total_trades,
            "total_pnl": round(self.report.total_pnl, 4),
            "win_rate": round(self.report.win_rate, 4),
            "sharpe_ratio": round(self.report.sharpe_ratio, 4) if self.report.sharpe_ratio else None,
            "failure_regimes": self.failure_regimes,
            "recommendations": self.recommendations,
            "by_regime": self.by_regime,
            "calibration_error": round(self.calibration_error, 6),
            "edge_capture": round(self.edge_capture, 4),
        }


class FailureAnalyzer:
    """Diagnoses when and why GRID's regime model fails.

    Converts GRID predictions into autopredict's TradeLog format and
    runs PerformanceAnalyzer to find systematic failure patterns like:
    - High-volatility periods where calibration degrades
    - Regime transitions where the model is consistently wrong
    - Confidence levels that don't match realised accuracy

    Usage::

        analyzer = FailureAnalyzer()

        # From execution sim results
        diagnostic = analyzer.from_execution_results(
            predictions=proba_df,
            actuals=y_series,
            exec_results=sim_output,
            class_names=["GROWTH", "NEUTRAL", "FRAGILE", "CRISIS"],
        )

        # From decision journal entries
        diagnostic = analyzer.from_journal_entries(entries_df)

        print(diagnostic.failure_regimes)
        print(diagnostic.recommendations)
    """

    def from_execution_results(
        self,
        predictions: pd.DataFrame | np.ndarray,
        actuals: pd.Series | np.ndarray,
        exec_results: dict[str, Any],
        class_names: list[str] | None = None,
        volatility: pd.Series | None = None,
    ) -> FailureDiagnostic:
        """Analyse failures from execution simulation output.

        Parameters:
            predictions: Probability matrix (n_samples x n_classes).
            actuals: True regime labels.
            exec_results: Output dict from ExecutionSimulator.simulate_era().
            class_names: Ordered regime names.
            volatility: Per-observation volatility (optional, enriches diagnosis).

        Returns:
            FailureDiagnostic with failure patterns and recommendations.
        """
        if class_names is None:
            class_names = list(_REGIME_OUTCOME.keys())

        if isinstance(predictions, pd.DataFrame):
            proba = predictions.values
        else:
            proba = np.asarray(predictions)

        if isinstance(actuals, pd.Series):
            actual_arr = actuals.values
        else:
            actual_arr = np.asarray(actuals)

        # Build TradeLog entries from per-trade execution results
        trades: list[TradeLog] = []
        per_trade = exec_results.get("per_trade", [])

        for i, trade in enumerate(per_trade):
            obs_idx = trade.get("observation", i)
            if obs_idx >= len(actual_arr):
                continue

            predicted_class_idx = int(np.argmax(proba[obs_idx]))
            predicted_class = class_names[predicted_class_idx] if predicted_class_idx < len(class_names) else "UNKNOWN"
            actual_class = str(actual_arr[obs_idx])

            # Model probability for the predicted outcome
            model_prob = float(proba[obs_idx, predicted_class_idx])
            # Market probability (use 0.5 as base — we're mapping to binary)
            market_prob = 0.5

            edge = trade.get("edge", model_prob - market_prob)
            pnl = trade.get("pnl", 0.0)
            size = trade.get("size", 0.0)
            outcome = 1 if predicted_class == actual_class else 0

            rationale: dict[str, Any] = {
                "predicted_regime": predicted_class,
                "actual_regime": actual_class,
                "category": predicted_class,
                "confidence": trade.get("confidence", model_prob),
                "slippage_bps": trade.get("slippage_bps", 0.0),
                "fill_rate": trade.get("fill_rate", 1.0),
            }

            if volatility is not None and obs_idx < len(volatility):
                vol = float(volatility.iloc[obs_idx] if isinstance(volatility, pd.Series) else volatility[obs_idx])
                rationale["volatility"] = vol
                # Map volatility to spread_pct for autopredict's failure detection
                rationale["spread_pct"] = vol * 0.5
                rationale["liquidity_depth"] = max(50.0, 200.0 * (1.0 - vol))

            side = trade.get("side", "buy")
            decision = side if side in ("buy", "sell") else "buy"

            trades.append(TradeLog(
                timestamp=datetime.now(timezone.utc),
                market_id=f"regime_{obs_idx}",
                market_prob=market_prob,
                model_prob=model_prob,
                edge=abs(edge),
                decision=decision,
                size=size,
                execution_price=trade.get("fill_price"),
                outcome=outcome,
                pnl=pnl,
                rationale=rationale,
            ))

        return self._build_diagnostic(trades, class_names)

    def from_journal_entries(
        self,
        entries: pd.DataFrame,
        class_names: list[str] | None = None,
    ) -> FailureDiagnostic:
        """Analyse failures from GRID's decision journal.

        Parameters:
            entries: DataFrame from DecisionJournal.get_recent() with columns:
                     inferred_state, state_confidence, grid_recommendation,
                     outcome_value, verdict.
            class_names: Ordered regime names.

        Returns:
            FailureDiagnostic.
        """
        if class_names is None:
            class_names = list(_REGIME_OUTCOME.keys())

        trades: list[TradeLog] = []
        for idx, row in entries.iterrows():
            state = row.get("inferred_state", "UNKNOWN")
            confidence = float(row.get("state_confidence", 0.5))
            recommendation = str(row.get("grid_recommendation", "HOLD"))
            verdict = row.get("verdict")
            outcome_val = row.get("outcome_value")

            decision = _ACTION_DECISION.get(recommendation, "pass")
            model_prob = confidence
            market_prob = 0.5
            edge = abs(model_prob - market_prob)

            # Map verdict to binary outcome
            if verdict == "HELPED":
                outcome = 1
            elif verdict == "HARMED":
                outcome = 0
            elif verdict == "NEUTRAL":
                outcome = 1  # neutral = didn't hurt
            else:
                outcome = None

            pnl = float(outcome_val) if outcome_val is not None and not pd.isna(outcome_val) else None

            ts = row.get("decision_timestamp")
            if ts is None or pd.isna(ts):
                ts = datetime.now(timezone.utc)
            elif isinstance(ts, str):
                ts = datetime.fromisoformat(ts)

            trades.append(TradeLog(
                timestamp=ts,
                market_id=f"journal_{idx}",
                market_prob=market_prob,
                model_prob=model_prob,
                edge=edge,
                decision=decision,
                size=1.0,
                execution_price=market_prob if decision != "pass" else None,
                outcome=outcome,
                pnl=pnl,
                rationale={
                    "category": state,
                    "predicted_regime": state,
                    "confidence": confidence,
                    "recommendation": recommendation,
                },
            ))

        return self._build_diagnostic(trades, class_names)

    def from_predictions(
        self,
        predictions: pd.DataFrame | np.ndarray,
        actuals: pd.Series | np.ndarray,
        class_names: list[str] | None = None,
    ) -> FailureDiagnostic:
        """Lightweight analysis from raw predictions (no execution sim needed).

        Parameters:
            predictions: Probability matrix (n_samples x n_classes).
            actuals: True regime labels.
            class_names: Ordered regime names.

        Returns:
            FailureDiagnostic.
        """
        if class_names is None:
            class_names = list(_REGIME_OUTCOME.keys())

        if isinstance(predictions, pd.DataFrame):
            proba = predictions.values
        else:
            proba = np.asarray(predictions)

        if isinstance(actuals, pd.Series):
            actual_arr = actuals.values
        else:
            actual_arr = np.asarray(actuals)

        trades: list[TradeLog] = []
        for i in range(len(actual_arr)):
            if i >= len(proba):
                break

            pred_idx = int(np.argmax(proba[i]))
            pred_class = class_names[pred_idx] if pred_idx < len(class_names) else "UNKNOWN"
            actual_class = str(actual_arr[i])

            model_prob = float(proba[i, pred_idx])
            market_prob = 0.5
            correct = 1 if pred_class == actual_class else 0
            pnl = model_prob - market_prob if correct else -(model_prob - market_prob)

            trades.append(TradeLog(
                timestamp=datetime.now(timezone.utc),
                market_id=f"pred_{i}",
                market_prob=market_prob,
                model_prob=model_prob,
                edge=abs(model_prob - market_prob),
                decision="buy",
                size=1.0,
                execution_price=market_prob,
                outcome=correct,
                pnl=pnl,
                rationale={
                    "category": pred_class,
                    "predicted_regime": pred_class,
                    "actual_regime": actual_class,
                    "confidence": model_prob,
                },
            ))

        return self._build_diagnostic(trades, class_names)

    # ── Internal ──────────────────────────────────────────────────────

    def _build_diagnostic(
        self,
        trades: list[TradeLog],
        class_names: list[str],
    ) -> FailureDiagnostic:
        """Run autopredict's PerformanceAnalyzer and build diagnostic."""
        if not trades:
            empty_report = PerformanceReport(
                total_trades=0, total_pnl=0.0, win_rate=0.0,
                avg_win=0.0, avg_loss=0.0, sharpe_ratio=None,
                by_market={}, by_category={}, by_decision={"buy": 0, "sell": 0, "pass": 0},
                failure_regimes=[], calibration_error=0.0,
                edge_capture_rate=0.0, recommendations=["No trades to analyse."],
            )
            return FailureDiagnostic(
                report=empty_report,
                failure_regimes=[],
                recommendations=["No trades to analyse."],
                by_regime={},
                calibration_error=0.0,
                edge_capture=0.0,
            )

        analyzer = PerformanceAnalyzer(trades)
        report = analyzer.generate_report()

        # Build per-regime breakdown (autopredict calls it "by_category")
        by_regime: dict[str, dict[str, Any]] = {}
        for regime in class_names:
            regime_trades = [
                t for t in trades
                if t.rationale.get("category") == regime or t.rationale.get("predicted_regime") == regime
            ]
            if not regime_trades:
                continue

            resolved = [t for t in regime_trades if t.outcome is not None]
            wins = sum(1 for t in resolved if t.pnl and t.pnl > 0)
            total_pnl = sum(t.pnl for t in resolved if t.pnl is not None)
            errors = [abs(t.model_prob - (t.outcome or 0)) for t in resolved]

            by_regime[regime] = {
                "trades": len(regime_trades),
                "pnl": round(total_pnl, 4),
                "win_rate": round(wins / len(resolved), 4) if resolved else 0.0,
                "calibration_error": round(
                    sum(errors) / len(errors), 4
                ) if errors else 0.0,
            }

        # Enrich recommendations with GRID-specific advice
        recommendations = list(report.recommendations)
        for regime, stats in by_regime.items():
            if stats["trades"] >= 5 and stats["win_rate"] < 0.40:
                recommendations.append(
                    f"Model underperforms in {regime} regime "
                    f"(win rate {stats['win_rate']:.0%}, {stats['trades']} trades). "
                    f"Consider adding {regime}-specific features or adjusting confidence threshold."
                )
            if stats["calibration_error"] > 0.20:
                recommendations.append(
                    f"{regime} regime poorly calibrated "
                    f"(error {stats['calibration_error']:.2f}). "
                    f"Consider ensemble weight adjustment for {regime} predictions."
                )

        log.info(
            "Failure analysis complete — {n} trades, {f} failure regimes, {r} recommendations",
            n=report.total_trades, f=len(report.failure_regimes), r=len(recommendations),
        )

        return FailureDiagnostic(
            report=report,
            failure_regimes=report.failure_regimes,
            recommendations=recommendations,
            by_regime=by_regime,
            calibration_error=report.calibration_error,
            edge_capture=report.edge_capture_rate,
        )
