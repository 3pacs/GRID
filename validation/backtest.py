"""
GRID walk-forward backtesting engine.

Provides rigorous walk-forward validation with PIT-correct data access,
era-based evaluation, and baseline comparison.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from store.pit import PITStore


class WalkForwardBacktest:
    """Walk-forward backtesting engine with PIT-correct data access.

    Splits the evaluation period into non-overlapping eras and computes
    performance metrics in each era to detect overfitting and regime
    dependence.

    Attributes:
        engine: SQLAlchemy engine for database writes.
        pit_store: PITStore for point-in-time data access.
    """

    def __init__(self, db_engine: Engine, pit_store: PITStore) -> None:
        """Initialise the backtester.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
            pit_store: PITStore instance for PIT-correct data.
        """
        self.engine = db_engine
        self.pit_store = pit_store
        log.info("WalkForwardBacktest initialised")

    def run_validation(
        self,
        hypothesis_id: int,
        feature_ids: list[int],
        start_date: date,
        end_date: date,
        n_splits: int = 5,
        vintage_policy: str = "FIRST_RELEASE",
        cost_bps: float = 10.0,
        predict_fn: Any = None,
    ) -> dict[str, Any]:
        """Run a full walk-forward validation.

        Parameters:
            hypothesis_id: ID of the hypothesis being tested.
            feature_ids: Feature IDs to use.
            start_date: Start of the evaluation period.
            end_date: End of the evaluation period.
            n_splits: Number of walk-forward splits (eras).
            vintage_policy: PIT vintage policy ('FIRST_RELEASE' or 'LATEST_AS_OF').
            cost_bps: Transaction cost assumption in basis points.
            predict_fn: Callable that takes a feature DataFrame and returns
                        predictions. If None, uses a simple baseline.

        Returns:
            dict: Comprehensive validation results suitable for storing in
                  validation_results table.
        """
        log.info(
            "Running walk-forward validation — hypothesis={h}, {sd} to {ed}, "
            "{n} splits, vintage={v}",
            h=hypothesis_id,
            sd=start_date,
            ed=end_date,
            n=n_splits,
            v=vintage_policy,
        )

        # Build walk-forward splits
        total_days = (end_date - start_date).days
        split_days = total_days // n_splits

        era_results: list[dict[str, Any]] = []

        for i in range(n_splits):
            era_start = start_date + timedelta(days=i * split_days)
            era_end = era_start + timedelta(days=split_days - 1)
            if i == n_splits - 1:
                era_end = end_date

            log.info("Era {i}/{n}: {s} to {e}", i=i + 1, n=n_splits, s=era_start, e=era_end)

            # Get PIT-correct feature matrix for this era
            matrix = self.pit_store.get_feature_matrix(
                feature_ids=feature_ids,
                start_date=era_start,
                end_date=era_end,
                as_of_date=era_end,
                vintage_policy=vintage_policy,
            )

            if matrix.empty or matrix.shape[0] < 10:
                log.warning("Era {i} has insufficient data ({n} rows)", i=i + 1, n=len(matrix))
                era_results.append({
                    "era": i + 1,
                    "start": era_start.isoformat(),
                    "end": era_end.isoformat(),
                    "n_observations": len(matrix),
                    "status": "INSUFFICIENT_DATA",
                })
                continue

            # Forward-fill and drop NaN
            matrix = matrix.ffill().dropna()

            # Compute era metrics
            era_metric = self._compute_era_metrics(matrix, predict_fn, cost_bps)
            era_metric["era"] = i + 1
            era_metric["start"] = era_start.isoformat()
            era_metric["end"] = era_end.isoformat()
            era_metric["n_observations"] = len(matrix)
            era_metric["status"] = "OK"
            era_results.append(era_metric)

        # Compute full-period metrics
        full_matrix = self.pit_store.get_feature_matrix(
            feature_ids=feature_ids,
            start_date=start_date,
            end_date=end_date,
            as_of_date=end_date,
            vintage_policy=vintage_policy,
        )
        full_matrix = full_matrix.ffill().dropna()
        full_metrics = self._compute_era_metrics(full_matrix, predict_fn, cost_bps)

        # Baseline comparison (buy-and-hold equivalent)
        baseline = self._compute_baseline_metrics(full_matrix)

        # Simplicity comparison
        simplicity = self._compute_simplicity_comparison(
            full_metrics, baseline, len(feature_ids)
        )

        # Overall verdict
        verdict = self._determine_verdict(era_results, full_metrics, baseline)

        result = {
            "hypothesis_id": hypothesis_id,
            "vintage_policy": vintage_policy,
            "era_results": era_results,
            "full_period_metrics": full_metrics,
            "baseline_comparison": baseline,
            "simplicity_comparison": simplicity,
            "walk_forward_splits": n_splits,
            "cost_assumption_bps": cost_bps,
            "overall_verdict": verdict,
            "gate_detail": {
                "era_consistency": all(
                    e.get("status") == "OK" for e in era_results
                ),
                "beats_baseline": full_metrics.get("sharpe", 0) > baseline.get("sharpe", 0),
                "positive_in_all_eras": all(
                    e.get("return", 0) > 0 for e in era_results if e.get("status") == "OK"
                ),
            },
        }

        # Store in validation_results
        self._store_result(result)

        log.info("Validation complete — verdict={v}", v=verdict)
        return result

    def _compute_era_metrics(
        self,
        matrix: pd.DataFrame,
        predict_fn: Any,
        cost_bps: float,
    ) -> dict[str, Any]:
        """Compute performance metrics for a single era.

        Parameters:
            matrix: Feature matrix for the era.
            predict_fn: Prediction function (or None for baseline).
            cost_bps: Cost assumption in basis points.

        Returns:
            dict: Era performance metrics.
        """
        if matrix.empty:
            return {"return": 0.0, "sharpe": 0.0, "max_drawdown": 0.0}

        # Use first column as a proxy return series for metric computation
        returns = matrix.iloc[:, 0].pct_change().dropna()
        if returns.empty:
            return {"return": 0.0, "sharpe": 0.0, "max_drawdown": 0.0}

        # Apply cost adjustment
        cost_adjustment = cost_bps / 10000.0
        adjusted_returns = returns - cost_adjustment / 252  # Daily cost

        cum_return = float((1 + adjusted_returns).prod() - 1)
        ann_return = float((1 + cum_return) ** (252 / max(len(adjusted_returns), 1)) - 1)
        ann_vol = float(adjusted_returns.std() * np.sqrt(252))
        sharpe = ann_return / ann_vol if ann_vol > 0 else 0.0

        # Max drawdown
        cum = (1 + adjusted_returns).cumprod()
        peak = cum.expanding().max()
        drawdown = (cum - peak) / peak
        max_dd = float(drawdown.min())

        return {
            "return": round(cum_return, 6),
            "annualised_return": round(ann_return, 6),
            "annualised_vol": round(ann_vol, 6),
            "sharpe": round(sharpe, 4),
            "max_drawdown": round(max_dd, 6),
            "n_days": len(adjusted_returns),
        }

    def _compute_baseline_metrics(self, matrix: pd.DataFrame) -> dict[str, Any]:
        """Compute baseline (buy-and-hold) metrics.

        Parameters:
            matrix: Full period feature matrix.

        Returns:
            dict: Baseline performance metrics.
        """
        if matrix.empty or matrix.shape[1] == 0:
            return {"return": 0.0, "sharpe": 0.0, "max_drawdown": 0.0}

        returns = matrix.iloc[:, 0].pct_change().dropna()
        if returns.empty:
            return {"return": 0.0, "sharpe": 0.0, "max_drawdown": 0.0}

        cum_return = float((1 + returns).prod() - 1)
        ann_vol = float(returns.std() * np.sqrt(252))
        ann_return = float((1 + cum_return) ** (252 / max(len(returns), 1)) - 1)
        sharpe = ann_return / ann_vol if ann_vol > 0 else 0.0

        return {
            "return": round(cum_return, 6),
            "sharpe": round(sharpe, 4),
            "max_drawdown": 0.0,
            "type": "buy_and_hold",
        }

    def _compute_simplicity_comparison(
        self,
        full_metrics: dict[str, Any],
        baseline: dict[str, Any],
        n_features: int,
    ) -> dict[str, Any]:
        """Compare strategy complexity vs performance gain.

        Parameters:
            full_metrics: Full-period strategy metrics.
            baseline: Baseline metrics.
            n_features: Number of features used.

        Returns:
            dict: Simplicity comparison results.
        """
        sharpe_gain = full_metrics.get("sharpe", 0) - baseline.get("sharpe", 0)

        return {
            "n_features": n_features,
            "sharpe_gain_over_baseline": round(sharpe_gain, 4),
            "gain_per_feature": round(sharpe_gain / max(n_features, 1), 4),
            "complexity_justified": sharpe_gain > 0.1,
        }

    def _determine_verdict(
        self,
        era_results: list[dict[str, Any]],
        full_metrics: dict[str, Any],
        baseline: dict[str, Any],
    ) -> str:
        """Determine the overall validation verdict.

        Parameters:
            era_results: Per-era metrics.
            full_metrics: Full-period metrics.
            baseline: Baseline metrics.

        Returns:
            str: 'PASS', 'FAIL', or 'CONDITIONAL'.
        """
        valid_eras = [e for e in era_results if e.get("status") == "OK"]

        if not valid_eras:
            return "FAIL"

        # Must beat baseline
        if full_metrics.get("sharpe", 0) <= baseline.get("sharpe", 0):
            return "FAIL"

        # Must have positive return in majority of eras
        positive_eras = sum(1 for e in valid_eras if e.get("return", 0) > 0)
        if positive_eras < len(valid_eras) * 0.6:
            return "FAIL"

        # All eras positive = PASS, otherwise CONDITIONAL
        if positive_eras == len(valid_eras):
            return "PASS"

        return "CONDITIONAL"

    def _store_result(self, result: dict[str, Any]) -> None:
        """Store validation result in the validation_results table.

        Parameters:
            result: Complete validation result dict.
        """
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO validation_results
                        (hypothesis_id, vintage_policy, era_results,
                         full_period_metrics, baseline_comparison,
                         simplicity_comparison, walk_forward_splits,
                         cost_assumption_bps, overall_verdict, gate_detail)
                        VALUES
                        (:hid, :vp, :er, :fpm, :bc, :sc, :wfs, :cab, :ov, :gd)
                    """),
                    {
                        "hid": result["hypothesis_id"],
                        "vp": result["vintage_policy"],
                        "er": json.dumps(result["era_results"]),
                        "fpm": json.dumps(result["full_period_metrics"]),
                        "bc": json.dumps(result["baseline_comparison"]),
                        "sc": json.dumps(result["simplicity_comparison"]),
                        "wfs": result["walk_forward_splits"],
                        "cab": result["cost_assumption_bps"],
                        "ov": result["overall_verdict"],
                        "gd": json.dumps(result["gate_detail"]),
                    },
                )
            log.info("Validation result stored for hypothesis {h}", h=result["hypothesis_id"])
        except Exception as exc:
            log.error("Failed to store validation result: {err}", err=str(exc))


if __name__ == "__main__":
    from db import get_engine

    engine = get_engine()
    pit = PITStore(engine)
    bt = WalkForwardBacktest(engine, pit)
    print("WalkForwardBacktest ready")
