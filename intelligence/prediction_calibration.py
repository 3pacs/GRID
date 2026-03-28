"""
GRID Prediction Market Calibration Checker.

Detects two kinds of divergence:

1. **Cross-platform divergence** — The same event is priced differently
   across prediction market platforms (e.g. Polymarket says 70% yes while
   Kalshi says 45% yes). This is an arbitrage signal.

2. **Fundamental divergence** — Prediction market odds disagree with
   GRID's own regime signals (e.g. markets price recession at 15% but
   GRID regime discovery puts contraction probability at 60%).

Returns CrossRefCheck dataclasses (from intelligence.cross_reference)
so results integrate seamlessly with the existing Lie Detector pipeline.

Z-score thresholds:
    MINOR    = 1.0 sigma  — worth monitoring
    MAJOR    = 2.0 sigma  — actionable divergence
    CONTRADICTION = 3.0 sigma — strong signal of mispricing
"""

from __future__ import annotations

import math
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from intelligence.cross_reference import (
    CrossRefCheck,
    MINOR_DIVERGENCE_THRESHOLD,
    MAJOR_DIVERGENCE_THRESHOLD,
    CONTRADICTION_THRESHOLD,
    MIN_OBSERVATIONS,
)


# ── Constants ────────────────────────────────────────────────────────────

# Minimum price difference (absolute, 0-1 scale) to flag cross-platform
# divergence before z-score calculation.
_MIN_PRICE_DIFF: float = 0.05

# Rolling window for z-score on prediction series (days)
_LOOKBACK_DAYS: int = 90

# Category used for all prediction calibration checks
_CATEGORY: str = "prediction_calibration"


# ── Helpers ──────────────────────────────────────────────────────────────

def _assess_divergence(z_score: float) -> str:
    """Map a z-score to a human-readable assessment label.

    Parameters:
        z_score: Absolute z-score of the divergence.

    Returns:
        One of 'consistent', 'minor_divergence', 'major_divergence',
        'contradiction'.
    """
    abs_z = abs(z_score)
    if abs_z >= CONTRADICTION_THRESHOLD:
        return "contradiction"
    if abs_z >= MAJOR_DIVERGENCE_THRESHOLD:
        return "major_divergence"
    if abs_z >= MINOR_DIVERGENCE_THRESHOLD:
        return "minor_divergence"
    return "consistent"


def _compute_z_score(
    value: float,
    mean: float,
    std: float,
) -> float:
    """Compute z-score with safe division.

    Parameters:
        value: Observed value.
        mean: Historical mean.
        std: Historical standard deviation.

    Returns:
        Z-score, or 0.0 if std is zero or inputs are invalid.
    """
    if std is None or std == 0:
        return 0.0
    if any(math.isnan(v) or math.isinf(v) for v in (value, mean, std)):
        return 0.0
    return (value - mean) / std


# ── Checker ──────────────────────────────────────────────────────────────


class PredictionCalibrationChecker:
    """Detects divergence in prediction market data.

    Queries the resolved_series table for pmxt data, groups by event
    slug across platforms, and computes divergence metrics.

    Attributes:
        engine: SQLAlchemy engine for database operations.
    """

    def __init__(self, engine: Engine) -> None:
        """Initialise the calibration checker.

        Parameters:
            engine: SQLAlchemy engine connected to the GRID database.
        """
        self.engine = engine

    # ------------------------------------------------------------------ #
    # Cross-platform divergence
    # ------------------------------------------------------------------ #

    def _fetch_pmxt_latest(self) -> list[dict[str, Any]]:
        """Fetch the latest pmxt series data from resolved_series.

        Returns rows for series matching 'pmxt.%' with the most recent
        obs_date.

        Returns:
            List of dicts with series_id, value, obs_date fields.
        """
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT series_id, value, obs_date "
                        "FROM resolved_series "
                        "WHERE series_id LIKE :prefix "
                        "AND obs_date >= :cutoff "
                        "ORDER BY obs_date DESC"
                    ),
                    {
                        "prefix": "pmxt.%",
                        "cutoff": date.today() - timedelta(days=7),
                    },
                ).fetchall()

            return [
                {
                    "series_id": r[0],
                    "value": r[1],
                    "obs_date": r[2],
                }
                for r in rows
            ]
        except Exception as exc:
            log.error(
                "PredictionCalibration: failed to fetch pmxt data: {e}",
                e=str(exc),
            )
            return []

    def _fetch_series_stats(
        self,
        series_id: str,
    ) -> dict[str, float | None]:
        """Fetch historical mean and std for a series.

        Parameters:
            series_id: The series identifier.

        Returns:
            Dict with 'mean', 'std', and 'count' keys.
        """
        try:
            with self.engine.connect() as conn:
                row = conn.execute(
                    text(
                        "SELECT AVG(value), STDDEV(value), COUNT(*) "
                        "FROM resolved_series "
                        "WHERE series_id = :sid "
                        "AND obs_date >= :cutoff"
                    ),
                    {
                        "sid": series_id,
                        "cutoff": date.today() - timedelta(days=_LOOKBACK_DAYS),
                    },
                ).fetchone()

            if row:
                return {
                    "mean": float(row[0]) if row[0] is not None else None,
                    "std": float(row[1]) if row[1] is not None else None,
                    "count": int(row[2]) if row[2] is not None else 0,
                }
            return {"mean": None, "std": None, "count": 0}

        except Exception as exc:
            log.debug(
                "PredictionCalibration: stats fetch failed for {s}: {e}",
                s=series_id,
                e=str(exc),
            )
            return {"mean": None, "std": None, "count": 0}

    def _parse_series_id(self, series_id: str) -> dict[str, str]:
        """Parse a pmxt series_id into components.

        Format: pmxt.{platform}.{event_slug}.{outcome}

        Parameters:
            series_id: The series identifier.

        Returns:
            Dict with 'platform', 'event_slug', 'outcome' keys.
        """
        parts = series_id.split(".", 3)
        if len(parts) < 4:
            return {"platform": "", "event_slug": "", "outcome": ""}
        return {
            "platform": parts[1],
            "event_slug": parts[2],
            "outcome": parts[3],
        }

    def check_cross_platform(self) -> list[CrossRefCheck]:
        """Detect cross-platform pricing divergences.

        Groups pmxt series by event_slug + outcome, compares prices
        across platforms, and flags significant divergences.

        Returns:
            List of CrossRefCheck instances for detected divergences.
        """
        rows = self._fetch_pmxt_latest()
        if not rows:
            return []

        # Group by event_slug.outcome -> {platform: price}
        event_prices: dict[str, dict[str, float]] = {}
        for row in rows:
            parsed = self._parse_series_id(row["series_id"])
            if not parsed["event_slug"]:
                continue

            key = f"{parsed['event_slug']}.{parsed['outcome']}"
            platform = parsed["platform"]
            value = row["value"]

            if value is None or math.isnan(value) or math.isinf(value):
                continue

            if key not in event_prices:
                event_prices[key] = {}

            # Keep the most recent price per platform
            if platform not in event_prices[key]:
                event_prices[key][platform] = value

        # Find divergences
        checks: list[CrossRefCheck] = []
        now_str = datetime.now(timezone.utc).isoformat()

        for event_key, platforms in event_prices.items():
            if len(platforms) < 2:
                continue

            platform_list = list(platforms.items())

            for i in range(len(platform_list)):
                for j in range(i + 1, len(platform_list)):
                    p1_name, p1_price = platform_list[i]
                    p2_name, p2_price = platform_list[j]

                    diff = abs(p1_price - p2_price)
                    if diff < _MIN_PRICE_DIFF:
                        continue

                    # Compute z-score based on historical spread volatility
                    # Use the raw difference as a pseudo z-score scaled by
                    # typical prediction market spread (~0.05 std)
                    spread_std = 0.05
                    z_score = diff / spread_std if spread_std > 0 else 0.0

                    assessment = _assess_divergence(z_score)
                    if assessment == "consistent":
                        continue

                    avg_price = (p1_price + p2_price) / 2
                    confidence = min(1.0, diff / 0.20)

                    checks.append(
                        CrossRefCheck(
                            name=f"cross_platform:{event_key}",
                            category=_CATEGORY,
                            official_source=f"pmxt.{p1_name}",
                            official_value=p1_price,
                            physical_source=f"pmxt.{p2_name}",
                            physical_value=p2_price,
                            expected_relationship="positive_correlation",
                            actual_divergence=z_score,
                            assessment=assessment,
                            implication=(
                                f"{event_key}: {p1_name} prices at "
                                f"{p1_price:.1%} vs {p2_name} at "
                                f"{p2_price:.1%} — "
                                f"{diff:.1%} gap suggests arbitrage "
                                f"or information asymmetry"
                            ),
                            confidence=confidence,
                            checked_at=now_str,
                        )
                    )

        log.info(
            "PredictionCalibration: {n} cross-platform divergences found",
            n=len(checks),
        )
        return checks

    # ------------------------------------------------------------------ #
    # Fundamental divergence (prediction odds vs GRID regime)
    # ------------------------------------------------------------------ #

    def _fetch_regime_signals(self) -> dict[str, float]:
        """Fetch latest GRID regime signals for comparison.

        Looks for series like 'REGIME:recession_prob', 'REGIME:inflation_prob'
        in resolved_series.

        Returns:
            Dict mapping regime signal name to probability value.
        """
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT series_id, value "
                        "FROM resolved_series "
                        "WHERE series_id LIKE :prefix "
                        "AND obs_date >= :cutoff "
                        "ORDER BY obs_date DESC"
                    ),
                    {
                        "prefix": "REGIME:%",
                        "cutoff": date.today() - timedelta(days=7),
                    },
                ).fetchall()

            signals: dict[str, float] = {}
            for r in rows:
                name = r[0].replace("REGIME:", "")
                if name not in signals:
                    val = r[1]
                    if val is not None and not math.isnan(val) and not math.isinf(val):
                        signals[name] = val
            return signals

        except Exception as exc:
            log.debug(
                "PredictionCalibration: regime signals fetch failed: {e}",
                e=str(exc),
            )
            return {}

    # Mapping from prediction market keywords to regime signal names
    _REGIME_MAPPINGS: list[dict[str, str]] = [
        {
            "keyword": "recession",
            "regime_signal": "recession_prob",
            "relationship": "positive_correlation",
        },
        {
            "keyword": "inflation",
            "regime_signal": "inflation_prob",
            "relationship": "positive_correlation",
        },
        {
            "keyword": "rate_cut",
            "regime_signal": "rate_cut_prob",
            "relationship": "positive_correlation",
        },
        {
            "keyword": "rate_hike",
            "regime_signal": "rate_hike_prob",
            "relationship": "positive_correlation",
        },
    ]

    def check_fundamental(self) -> list[CrossRefCheck]:
        """Detect divergence between prediction markets and GRID regime signals.

        Compares prediction market probabilities for macro events against
        GRID's own regime detection outputs.

        Returns:
            List of CrossRefCheck instances for detected divergences.
        """
        regime_signals = self._fetch_regime_signals()
        if not regime_signals:
            log.info("PredictionCalibration: no regime signals available")
            return []

        rows = self._fetch_pmxt_latest()
        if not rows:
            return []

        # Group prediction market data by event keyword
        keyword_prices: dict[str, list[float]] = {}
        for row in rows:
            series_id = row["series_id"].lower()
            value = row["value"]
            if value is None or math.isnan(value) or math.isinf(value):
                continue

            for mapping in self._REGIME_MAPPINGS:
                if mapping["keyword"] in series_id:
                    kw = mapping["keyword"]
                    if kw not in keyword_prices:
                        keyword_prices[kw] = []
                    keyword_prices[kw].append(value)

        checks: list[CrossRefCheck] = []
        now_str = datetime.now(timezone.utc).isoformat()

        for mapping in self._REGIME_MAPPINGS:
            keyword = mapping["keyword"]
            signal_name = mapping["regime_signal"]

            if signal_name not in regime_signals:
                continue
            if keyword not in keyword_prices:
                continue

            regime_prob = regime_signals[signal_name]
            market_prices = keyword_prices[keyword]
            if not market_prices:
                continue

            avg_market_prob = sum(market_prices) / len(market_prices)
            diff = abs(avg_market_prob - regime_prob)

            if diff < _MIN_PRICE_DIFF:
                continue

            # Z-score: how unusual is this divergence?
            # Use a typical fundamental spread std of ~0.10
            fundamental_std = 0.10
            z_score = diff / fundamental_std if fundamental_std > 0 else 0.0

            assessment = _assess_divergence(z_score)
            if assessment == "consistent":
                continue

            confidence = min(1.0, len(market_prices) / 5.0)

            checks.append(
                CrossRefCheck(
                    name=f"fundamental:{keyword}",
                    category=_CATEGORY,
                    official_source=f"prediction_markets:{keyword}",
                    official_value=avg_market_prob,
                    physical_source=f"grid_regime:{signal_name}",
                    physical_value=regime_prob,
                    expected_relationship=mapping["relationship"],
                    actual_divergence=z_score,
                    assessment=assessment,
                    implication=(
                        f"Prediction markets price {keyword} at "
                        f"{avg_market_prob:.1%} but GRID regime signals "
                        f"{regime_prob:.1%} — "
                        f"{diff:.1%} gap suggests market mispricing "
                        f"or model disagreement"
                    ),
                    confidence=confidence,
                    checked_at=now_str,
                )
            )

        log.info(
            "PredictionCalibration: {n} fundamental divergences found",
            n=len(checks),
        )
        return checks

    # ------------------------------------------------------------------ #
    # Main entry point
    # ------------------------------------------------------------------ #

    def run_checks(self, engine: Engine | None = None) -> list[CrossRefCheck]:
        """Run all prediction calibration checks.

        Parameters:
            engine: Optional engine override (unused, for interface compat).

        Returns:
            Combined list of CrossRefCheck instances.
        """
        checks: list[CrossRefCheck] = []

        try:
            checks.extend(self.check_cross_platform())
        except Exception as exc:
            log.error(
                "PredictionCalibration: cross-platform check failed: {e}",
                e=str(exc),
            )

        try:
            checks.extend(self.check_fundamental())
        except Exception as exc:
            log.error(
                "PredictionCalibration: fundamental check failed: {e}",
                e=str(exc),
            )

        log.info(
            "PredictionCalibration: {n} total divergences detected",
            n=len(checks),
        )
        return checks
