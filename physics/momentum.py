"""
GRID news momentum analysis.

Computes physics-inspired momentum metrics from GDELT sentiment features:
  - Sentiment trend (direction and strength of tone shift)
  - Momentum direction (first derivative of sentiment)
  - Kinetic energy of sentiment (rate of change squared)
  - Cross-correlation with price features (sentiment-price coupling)

The tone signal is a composite (row-mean) across the named-actor tone
features that ingestion/altdata/gdelt.py::GDELTPuller.pull_recent actually
writes every scheduled cycle (gdelt_actor_*_tone), and the "conflict" signal
is a composite across its country-tension features (gdelt_tension_*). A
single actor or country pair can go quiet on any given day (GDELT DOC API
queries fail independently), so averaging across the whole named set is
more robust than pinning the read to one series.

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
    direction: str  # "bullish", "bearish", "mixed", "unavailable" — plain-English tone
    summary: str
    details: dict[str, Any]
    warnings: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "available": self.available,
            "sentiment_trend": self.sentiment_trend,
            "momentum_direction": self.momentum_direction,
            "energy_state": self.energy_state,
            "direction": self.direction,
            "summary": self.summary,
            "details": self.details,
            "warnings": self.warnings,
        }


# GDELT actor-tone features: named heads of state / central bankers whose
# media tone GDELTPuller._pull_actor_tones() tracks. Composite tone signal.
GDELT_ACTOR_TONE_FEATURES = [
    "gdelt_actor_powell_tone",
    "gdelt_actor_lagarde_tone",
    "gdelt_actor_xi_tone",
    "gdelt_actor_putin_tone",
    "gdelt_actor_mbs_tone",
    "gdelt_actor_yellen_tone",
    "gdelt_actor_ueda_tone",
]

# GDELT country-pair tension features: GDELTPuller._pull_tension_scores().
# Composite "conflict" signal (higher = more negative-tone volume between
# the pair).
GDELT_TENSION_FEATURES = [
    "gdelt_tension_us_china",
    "gdelt_tension_us_russia",
    "gdelt_tension_us_iran",
    "gdelt_tension_china_taiwan",
    "gdelt_tension_russia_ukraine",
    "gdelt_tension_israel_iran",
    "gdelt_tension_india_china",
]
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

        # Resolve feature IDs for the GDELT actor-tone composite
        tone_ids = self._resolve_feature_ids(GDELT_ACTOR_TONE_FEATURES)
        if not tone_ids:
            log.warning("No GDELT actor-tone features found in feature_registry")
            return self._unavailable_result(
                as_of_date,
                note=(
                    "GDELT actor-tone features not found in feature_registry. "
                    "Ensure GDELTPuller.pull_recent is scheduled and its "
                    "gdelt_actor_*_tone features are registered."
                ),
                warning="No GDELT actor-tone features registered",
            )

        # Get PIT-correct sentiment data
        start_date = as_of_date - timedelta(days=lookback_days)
        tone_feature_ids = list(tone_ids.values())

        matrix = self.pit_store.get_feature_matrix(
            feature_ids=tone_feature_ids,
            start_date=start_date,
            end_date=as_of_date,
            as_of_date=as_of_date,
            vintage_policy="LATEST_AS_OF",
        )

        if matrix.empty or matrix.shape[0] < 5:
            return self._unavailable_result(
                as_of_date,
                note="Insufficient GDELT data for momentum analysis",
                warning="Insufficient GDELT data (need at least 5 observations)",
                extra_details={
                    "rows_available": matrix.shape[0] if not matrix.empty else 0,
                },
            )

        details["data_points"] = matrix.shape[0]
        details["features_available"] = list(tone_ids.keys())

        # Composite tone: average across whichever actor-tone columns have
        # data on a given date (any one actor query can come back empty).
        tone_series = matrix[list(tone_ids.values())].mean(axis=1, skipna=True).dropna()
        if len(tone_series) < 5:
            return self._unavailable_result(
                as_of_date,
                note="Insufficient non-null tone data",
                warning="Too few non-null GDELT tone observations",
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

        # 5) Conflict/tension composite (optional — degrades gracefully)
        tension_ids = self._resolve_feature_ids(GDELT_TENSION_FEATURES)
        conflict_energy: dict[str, Any] | None = None
        if tension_ids:
            tension_matrix = self._load_matrix(
                list(tension_ids.values()), start_date, as_of_date
            )
            if not tension_matrix.empty:
                tension_series = (
                    tension_matrix[list(tension_ids.values())]
                    .mean(axis=1, skipna=True)
                    .dropna()
                )
                if len(tension_series) >= 5:
                    conflict_energy = self._compute_energy(tension_series)
                    details["conflict_energy"] = conflict_energy
                    details["tension_features_available"] = list(tension_ids.keys())

        summary = self._build_summary(
            sentiment_trend, momentum_direction, energy_state, conflict_energy,
        )

        return MomentumResult(
            available=True,
            sentiment_trend=sentiment_trend,
            momentum_direction=momentum_direction,
            energy_state=energy_state,
            direction=self._trend_to_direction(sentiment_trend),
            summary=summary,
            details=details,
            warnings=warnings,
        )

    @staticmethod
    def _trend_to_direction(sentiment_trend: str) -> str:
        """Map the physics trend label to the plain bull/bear word the
        stepdad.finance home page's plainSentiment() helper recognizes
        (pwa/src/components/home/plain.js — matches /bull|.../ and
        /bear|.../ substrings), so the news card's mood badge reflects
        actual tone instead of always falling through to its neutral
        default."""
        return {"rising": "bullish", "falling": "bearish", "neutral": "mixed"}.get(
            sentiment_trend, "unavailable"
        )

    def _unavailable_result(
        self,
        as_of_date: date,
        note: str,
        warning: str,
        extra_details: dict[str, Any] | None = None,
    ) -> MomentumResult:
        """Build the standard 'no data' MomentumResult, DRYing up the early returns."""
        details = {"note": note, "as_of_date": as_of_date.isoformat()}
        if extra_details:
            details.update(extra_details)
        return MomentumResult(
            available=False,
            sentiment_trend="unavailable",
            momentum_direction="unavailable",
            energy_state="unavailable",
            direction="unavailable",
            summary="News momentum isn't available right now — check back shortly.",
            details=details,
            warnings=[warning],
        )

    def _load_matrix(
        self, feature_ids: list[int], start_date: date, as_of_date: date
    ) -> pd.DataFrame:
        """Fetch a PIT-correct feature matrix, swallowing lookup errors as empty."""
        if not feature_ids:
            return pd.DataFrame()
        try:
            return self.pit_store.get_feature_matrix(
                feature_ids=feature_ids,
                start_date=start_date,
                end_date=as_of_date,
                as_of_date=as_of_date,
                vintage_policy="LATEST_AS_OF",
            )
        except Exception as exc:
            log.warning("Failed to load feature matrix: {e}", e=str(exc))
            return pd.DataFrame()

    @staticmethod
    def _build_summary(
        sentiment_trend: str,
        momentum_direction: str,
        energy_state: str,
        conflict_energy: dict[str, Any] | None,
    ) -> str:
        """Plain-English one-liner describing the current news momentum state."""
        trend_words = {
            "rising": "News tone toward major economic and policy actors is improving",
            "falling": "News tone toward major economic and policy actors is worsening",
            "neutral": "News tone toward major economic and policy actors is steady",
        }
        parts = [trend_words.get(sentiment_trend, "News tone is mixed")]

        if momentum_direction == "accelerating":
            parts.append("and moving quickly")
        elif momentum_direction == "decelerating":
            parts.append("and losing steam")

        if energy_state == "high":
            parts.append("— headline activity is elevated")
        elif energy_state == "low":
            parts.append("— it's a quiet news cycle")

        sentence = " ".join(parts) + "."

        if conflict_energy and conflict_energy.get("state") == "high":
            sentence += " Geopolitical tension chatter is elevated too."

        return sentence

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
