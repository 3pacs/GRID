"""
GRID failure regime analysis.

Diagnoses *when and why* GRID's regime model breaks down.  Converts
GRID's decision journal entries and execution sim results into structured
trade records, runs failure detection, and returns actionable diagnostics.

Fully self-contained — no external dependencies beyond numpy/pandas.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log


# ── Local types (replacing autopredict imports) ──────────────────────


@dataclass
class TradeRecord:
    """Structured record for a single trade/prediction."""
    timestamp: datetime
    market_id: str
    market_prob: float
    model_prob: float
    edge: float
    decision: str  # "buy", "sell", "pass"
    size: float
    execution_price: float | None
    outcome: int | None  # 1=correct, 0=wrong, None=pending
    pnl: float | None
    rationale: dict[str, Any] = field(default_factory=dict)


@dataclass
class PerformanceReport:
    """Performance analysis report."""
    total_trades: int
    total_pnl: float
    win_rate: float
    avg_win: float
    avg_loss: float
    sharpe_ratio: float | None
    by_market: dict[str, Any]
    by_category: dict[str, Any]
    by_decision: dict[str, int]
    failure_regimes: list[str]
    calibration_error: float
    edge_capture_rate: float
    recommendations: list[str]


# ── Regime-to-market mapping ─────────────────────────────────────────

_REGIME_OUTCOME: dict[str, int] = {
    "GROWTH": 1,
    "NEUTRAL": 1,
    "FRAGILE": 0,
    "CRISIS": 0,
}

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
        report: Full performance report.
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

    Converts GRID predictions into structured trade records and
    runs performance analysis to find systematic failure patterns like:
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

        trades: list[TradeRecord] = []
        per_trade = exec_results.get("per_trade", [])

        for i, trade in enumerate(per_trade):
            obs_idx = trade.get("observation", i)
            if obs_idx >= len(actual_arr):
                continue

            predicted_class_idx = int(np.argmax(proba[obs_idx]))
            predicted_class = class_names[predicted_class_idx] if predicted_class_idx < len(class_names) else "UNKNOWN"
            actual_class = str(actual_arr[obs_idx])

            model_prob = float(proba[obs_idx, predicted_class_idx])
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
                rationale["spread_pct"] = vol * 0.5
                rationale["liquidity_depth"] = max(50.0, 200.0 * (1.0 - vol))

            side = trade.get("side", "buy")
            decision = side if side in ("buy", "sell") else "buy"

            trades.append(TradeRecord(
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

        trades: list[TradeRecord] = []
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

            if verdict == "HELPED":
                outcome = 1
            elif verdict == "HARMED":
                outcome = 0
            elif verdict == "NEUTRAL":
                outcome = 1
            else:
                outcome = None

            pnl = float(outcome_val) if outcome_val is not None and not pd.isna(outcome_val) else None

            ts = row.get("decision_timestamp")
            if ts is None or pd.isna(ts):
                ts = datetime.now(timezone.utc)
            elif isinstance(ts, str):
                ts = datetime.fromisoformat(ts)

            trades.append(TradeRecord(
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

        trades: list[TradeRecord] = []
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

            trades.append(TradeRecord(
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
        trades: list[TradeRecord],
        class_names: list[str],
    ) -> FailureDiagnostic:
        """Run performance analysis and build diagnostic."""
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

        report = self._generate_report(trades)

        # Build per-regime breakdown
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

    def _generate_report(self, trades: list[TradeRecord]) -> PerformanceReport:
        """Generate a performance report from trade records."""
        resolved = [t for t in trades if t.outcome is not None]
        pnls = [t.pnl for t in resolved if t.pnl is not None]

        total_pnl = sum(pnls) if pnls else 0.0
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p < 0]

        win_rate = len(wins) / len(resolved) if resolved else 0.0
        avg_win = float(np.mean(wins)) if wins else 0.0
        avg_loss = float(np.mean(losses)) if losses else 0.0

        # Sharpe ratio approximation
        sharpe = None
        if len(pnls) >= 5:
            mean_pnl = float(np.mean(pnls))
            std_pnl = float(np.std(pnls))
            if std_pnl > 0:
                sharpe = mean_pnl / std_pnl * np.sqrt(252)

        # By-decision counts
        by_decision = {"buy": 0, "sell": 0, "pass": 0}
        for t in trades:
            if t.decision in by_decision:
                by_decision[t.decision] += 1

        # By-category breakdown
        by_category: dict[str, dict[str, Any]] = {}
        for t in trades:
            cat = t.rationale.get("category", "unknown")
            if cat not in by_category:
                by_category[cat] = {"trades": 0, "pnl": 0.0}
            by_category[cat]["trades"] += 1
            if t.pnl is not None:
                by_category[cat]["pnl"] += t.pnl

        # Calibration error
        cal_errors = [abs(t.model_prob - (t.outcome or 0)) for t in resolved]
        calibration_error = float(np.mean(cal_errors)) if cal_errors else 0.0

        # Edge capture
        total_edge = sum(t.edge for t in resolved)
        edge_capture = total_pnl / total_edge if total_edge > 0 else 0.0

        # Failure regimes: categories with win rate < 40% and >= 5 trades
        failure_regimes = []
        for cat, stats in by_category.items():
            cat_trades = [t for t in resolved if t.rationale.get("category") == cat]
            if len(cat_trades) >= 5:
                cat_wins = sum(1 for t in cat_trades if t.pnl and t.pnl > 0)
                cat_wr = cat_wins / len(cat_trades)
                if cat_wr < 0.40:
                    failure_regimes.append(
                        f"{cat}: win rate {cat_wr:.0%} over {len(cat_trades)} trades"
                    )

        # Recommendations
        recommendations = []
        if win_rate < 0.45 and len(resolved) >= 10:
            recommendations.append(
                f"Overall win rate is {win_rate:.0%} — consider re-calibrating the model."
            )
        if calibration_error > 0.15:
            recommendations.append(
                f"Calibration error is {calibration_error:.3f} — predictions don't match outcomes."
            )
        if avg_loss != 0 and avg_win != 0 and abs(avg_loss) > 2 * avg_win:
            recommendations.append(
                "Average loss is more than 2x average win — consider tighter stop-losses."
            )

        return PerformanceReport(
            total_trades=len(trades),
            total_pnl=total_pnl,
            win_rate=win_rate,
            avg_win=avg_win,
            avg_loss=avg_loss,
            sharpe_ratio=sharpe,
            by_market={},
            by_category=by_category,
            by_decision=by_decision,
            failure_regimes=failure_regimes,
            calibration_error=calibration_error,
            edge_capture_rate=edge_capture,
            recommendations=recommendations,
        )
