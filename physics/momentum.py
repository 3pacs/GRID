"""
GRID news momentum analysis.

Computes physics-inspired momentum metrics from GDELT sentiment features:
  - Sentiment trend (direction and strength of tone shift)
  - Momentum direction (first derivative of sentiment)
  - Kinetic energy of sentiment (rate of change squared)
  - Cross-correlation with price features (sentiment-price coupling)

Uses PIT-correct data retrieval to prevent lookahead bias.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from store.pit import PITStore


@dataclass
class MomentumResult:
    """Result of news momentum analysis."""

    available: bool
    sentiment_trend: str  # "rising", "falling", "neutral", "unavailable"
    momentum_direction: str  # "accelerating", "decelerating", "stable", "unavailable"
    energy_state: str  # "high", "medium", "low", "unavailable"
    details: dict[str, Any]
    warnings: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "available": self.available,
            "sentiment_trend": self.sentiment_trend,
            "momentum_direction": self.momentum_direction,
            "energy_state": self.energy_state,
            "details": self.details,
            "warnings": self.warnings,
        }


# GDELT feature names expected in feature_registry
GDELT_FEATURES = ["gdelt_tone_usa", "gdelt_conflict_global"]
PRICE_FEATURES = ["sp500_close"]


class NewsMomentumAnalyzer:
    """Analyze news sentiment momentum using physics analogs.

    Attributes:
        engine: SQLAlchemy engine for database access.
        pit_store: PITStore for point-in-time data retrieval.
    """

    def __init__(self, db_engine: Engine, pit_store: PITStore) -> None:
        self.engine = db_engine
        self.pit_store = pit_store

    def analyze(
        self,
        as_of_date: date,
        lookback_days: int = 90,
    ) -> MomentumResult:
        """Run full news momentum analysis.

        Parameters:
            as_of_date: Decision date for PIT-correct queries.
            lookback_days: Number of days of history to analyze.

        Returns:
            MomentumResult with sentiment trend, momentum, and energy state.
        """
        log.info(
            "Running news momentum analysis as_of={d}, lookback={lb}",
            d=as_of_date,
            lb=lookback_days,
        )
        warnings: list[str] = []
        details: dict[str, Any] = {"as_of_date": as_of_date.isoformat()}

        # Resolve feature IDs for GDELT features
        gdelt_ids = self._resolve_feature_ids(GDELT_FEATURES)
        if not gdelt_ids:
            log.warning("No GDELT features found in feature_registry")
            return MomentumResult(
                available=False,
                sentiment_trend="unavailable",
                momentum_direction="unavailable",
                energy_state="unavailable",
                details={
                    "note": "GDELT features not found in feature_registry. "
                    "Ensure gdelt_tone_usa and/or gdelt_conflict_global are ingested.",
                    "as_of_date": as_of_date.isoformat(),
                },
                warnings=["No GDELT sentiment features registered"],
            )

        # Get PIT-correct sentiment data
        start_date = as_of_date - timedelta(days=lookback_days)
        feature_ids = list(gdelt_ids.values())

        matrix = self.pit_store.get_feature_matrix(
            feature_ids=feature_ids,
            start_date=start_date,
            end_date=as_of_date,
            as_of_date=as_of_date,
            vintage_policy="LATEST_AS_OF",
        )

        if matrix.empty or matrix.shape[0] < 5:
            return MomentumResult(
                available=False,
                sentiment_trend="unavailable",
                momentum_direction="unavailable",
                energy_state="unavailable",
                details={
                    "note": "Insufficient GDELT data for momentum analysis",
                    "rows_available": matrix.shape[0] if not matrix.empty else 0,
                    "as_of_date": as_of_date.isoformat(),
                },
                warnings=["Insufficient GDELT data (need at least 5 observations)"],
            )

        details["data_points"] = matrix.shape[0]
        details["features_available"] = list(gdelt_ids.keys())

        # Use the primary tone feature (gdelt_tone_usa preferred)
        tone_id = gdelt_ids.get("gdelt_tone_usa") or next(iter(gdelt_ids.values()))
        tone_col = tone_id
        if tone_col not in matrix.columns:
            # Fall back to first available column
            tone_col = matrix.columns[0]

        tone_series = matrix[tone_col].dropna()
        if len(tone_series) < 5:
            return MomentumResult(
                available=False,
                sentiment_trend="unavailable",
                momentum_direction="unavailable",
                energy_state="unavailable",
                details={
                    "note": "Insufficient non-null tone data",
                    "as_of_date": as_of_date.isoformat(),
                },
                warnings=["Too few non-null GDELT tone observations"],
            )

        # 1) Sentiment trend: linear regression slope over lookback
        trend_info = self._compute_trend(tone_series)
        details["trend"] = trend_info
        sentiment_trend = trend_info["direction"]

        # 2) Momentum direction: acceleration (second derivative)
        momentum_info = self._compute_momentum(tone_series)
        details["momentum"] = momentum_info
        momentum_direction = momentum_info["direction"]

        # 3) Kinetic energy of sentiment
        energy_info = self._compute_energy(tone_series)
        details["energy"] = energy_info
        energy_state = energy_info["state"]

        # 4) Cross-correlation with price features (optional)
        price_ids = self._resolve_feature_ids(PRICE_FEATURES)
        if price_ids:
            xcorr_info = self._cross_correlate(
                tone_series, price_ids, start_date, as_of_date
            )
            details["cross_correlation"] = xcorr_info
            if xcorr_info.get("warnings"):
                warnings.extend(xcorr_info["warnings"])
        else:
            details["cross_correlation"] = {"note": "No price features available"}

        # Conflict feature analysis (if available)
        conflict_id = gdelt_ids.get("gdelt_conflict_global")
        if conflict_id is not None and conflict_id in matrix.columns:
            conflict_series = matrix[conflict_id].dropna()
            if len(conflict_series) >= 5:
                conflict_energy = self._compute_energy(conflict_series)
                details["conflict_energy"] = conflict_energy

        return MomentumResult(
            available=True,
            sentiment_trend=sentiment_trend,
            momentum_direction=momentum_direction,
            energy_state=energy_state,
            details=details,
            warnings=warnings,
        )

    # ------------------------------------------------------------------
    # Internal computations
    # ------------------------------------------------------------------

    def _resolve_feature_ids(
        self, feature_names: list[str]
    ) -> dict[str, int]:
        """Look up feature_registry IDs for given feature names.

        Returns:
            dict mapping feature_name -> feature_id for features that exist.
        """
        if not feature_names:
            return {}

        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT id, name FROM feature_registry "
                        "WHERE name = ANY(:names)"
                    ),
                    {"names": feature_names},
                ).fetchall()
            return {row[1]: row[0] for row in rows}
        except Exception as exc:
            log.warning(
                "Failed to resolve feature IDs: {e}", e=str(exc)
            )
            return {}

    def _compute_trend(self, series: pd.Series) -> dict[str, Any]:
        """Compute linear trend of sentiment series.

        Returns slope, direction, and R-squared.
        """
        from scipy import stats

        y = series.values
        x = np.arange(len(y), dtype=float)

        slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)

        # Classify direction
        if p_value > 0.1:
            direction = "neutral"
        elif slope > 0:
            direction = "rising"
        else:
            direction = "falling"

        return {
            "slope": round(float(slope), 6),
            "r_squared": round(float(r_value ** 2), 4),
            "p_value": round(float(p_value), 4),
            "direction": direction,
            "latest_value": round(float(y[-1]), 4),
        }

    def _compute_momentum(self, series: pd.Series) -> dict[str, Any]:
        """Compute momentum (first derivative) and acceleration (second derivative).

        Momentum = rate of change (velocity analog).
        Acceleration = rate of change of momentum.
        """
        # First derivative: rolling rate of change over 5-day window
        window = min(5, len(series) // 2)
        if window < 2:
            return {
                "velocity": 0.0,
                "acceleration": 0.0,
                "direction": "stable",
            }

        velocity = series.diff(window) / window
        acceleration = velocity.diff(window) / window

        latest_vel = float(velocity.dropna().iloc[-1]) if not velocity.dropna().empty else 0.0
        latest_acc = float(acceleration.dropna().iloc[-1]) if not acceleration.dropna().empty else 0.0

        # Classify
        if abs(latest_acc) < 0.001:
            direction = "stable"
        elif latest_acc > 0:
            direction = "accelerating"
        else:
            direction = "decelerating"

        return {
            "velocity": round(latest_vel, 6),
            "acceleration": round(latest_acc, 6),
            "direction": direction,
        }

    def _compute_energy(self, series: pd.Series) -> dict[str, Any]:
        """Compute kinetic energy of sentiment: KE = 0.5 * v^2.

        v = rolling rate of change (log-difference for positive series,
        or simple difference for sentiment scores that can be negative).
        """
        # Use simple differences for sentiment (can be negative)
        diffs = series.diff().dropna()
        if diffs.empty:
            return {
                "kinetic_energy": 0.0,
                "state": "low",
                "rolling_ke": [],
            }

        # Rolling KE over a 10-day window
        window = min(10, len(diffs))
        rolling_ke = (0.5 * diffs ** 2).rolling(window=window, min_periods=1).mean()

        latest_ke = float(rolling_ke.iloc[-1]) if not rolling_ke.empty else 0.0

        # Classify energy state using percentiles of the rolling KE
        if len(rolling_ke) >= 10:
            p75 = float(rolling_ke.quantile(0.75))
            p25 = float(rolling_ke.quantile(0.25))
            if latest_ke > p75:
                state = "high"
            elif latest_ke < p25:
                state = "low"
            else:
                state = "medium"
        else:
            state = "medium" if latest_ke > 0.01 else "low"

        return {
            "kinetic_energy": round(latest_ke, 6),
            "state": state,
            "mean_ke": round(float(rolling_ke.mean()), 6) if not rolling_ke.empty else 0.0,
        }

    def _cross_correlate(
        self,
        tone_series: pd.Series,
        price_ids: dict[str, int],
        start_date: date,
        as_of_date: date,
    ) -> dict[str, Any]:
        """Cross-correlate sentiment with price features.

        Computes lag-0 and lag-1 through lag-5 correlations to detect
        if sentiment leads or lags price movements.
        """
        result: dict[str, Any] = {"warnings": []}
        feature_ids = list(price_ids.values())

        try:
            price_matrix = self.pit_store.get_feature_matrix(
                feature_ids=feature_ids,
                start_date=start_date,
                end_date=as_of_date,
                as_of_date=as_of_date,
                vintage_policy="LATEST_AS_OF",
            )
        except Exception as exc:
            result["warnings"].append(f"Could not fetch price data: {exc}")
            return result

        if price_matrix.empty:
            result["note"] = "No price data available for cross-correlation"
            return result

        # Use first available price feature
        price_col = price_matrix.columns[0]
        price_series = price_matrix[price_col].dropna()

        if len(price_series) < 10:
            result["note"] = "Insufficient price data for cross-correlation"
            return result

        # Compute returns for price (percentage change)
        price_returns = price_series.pct_change().dropna()

        # Align tone and price returns on common dates
        common_idx = tone_series.index.intersection(price_returns.index)
        if len(common_idx) < 10:
            result["note"] = "Insufficient overlapping dates for cross-correlation"
            return result

        tone_aligned = tone_series.reindex(common_idx).dropna()
        price_aligned = price_returns.reindex(common_idx).dropna()

        # Recompute common after dropna
        common_idx = tone_aligned.index.intersection(price_aligned.index)
        if len(common_idx) < 10:
            result["note"] = "Insufficient overlapping non-null data"
            return result

        tone_aligned = tone_aligned.reindex(common_idx)
        price_aligned = price_aligned.reindex(common_idx)

        # Lag correlations (sentiment leading price)
        correlations: dict[str, float | None] = {}
        for lag in range(0, 6):
            if lag == 0:
                corr = float(tone_aligned.corr(price_aligned))
            else:
                if len(tone_aligned) <= lag:
                    break
                shifted_tone = tone_aligned.iloc[:-lag]
                shifted_price = price_aligned.iloc[lag:]
                if len(shifted_tone) < 5:
                    break
                shifted_tone = shifted_tone.reset_index(drop=True)
                shifted_price = shifted_price.reset_index(drop=True)
                corr = float(shifted_tone.corr(shifted_price))

            if np.isnan(corr):
                correlations[f"lag_{lag}"] = None
            else:
                correlations[f"lag_{lag}"] = round(corr, 4)

        result["lag_correlations"] = correlations

        # Find strongest lag
        valid_corrs = {
            k: abs(v) for k, v in correlations.items() if v is not None
        }
        if valid_corrs:
            best_lag = max(valid_corrs, key=valid_corrs.get)  # type: ignore[arg-type]
            result["strongest_lag"] = best_lag
            result["strongest_correlation"] = correlations[best_lag]

        return result
