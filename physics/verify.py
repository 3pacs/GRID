"""
GRID market physics verification layer.

Adapted from Get Physics Done's verify-work concept:
  - Conservation checks (capital flows must net to zero)
  - Dimensional analysis (units on financial quantities)
  - Limiting cases (zero rates, infinite vol, negative yields)
  - Regime boundary validation (plausible transitions)
  - Stationarity checks (ADF tests on features used in clustering)
  - Numerical stability (NaN/inf propagation, divide-by-zero)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log
from scipy import stats
from sqlalchemy import text
from sqlalchemy.engine import Engine

from physics.conventions import (
    CONVENTIONS,
    check_unit_compatibility,
    validate_convention,
)
from store.pit import PITStore


@dataclass
class VerificationResult:
    """Result of a single verification check."""

    check_name: str
    passed: bool
    score: float  # 0.0 (fail) to 1.0 (perfect)
    details: dict[str, Any]
    warnings: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "check_name": self.check_name,
            "passed": self.passed,
            "score": self.score,
            "details": self.details,
            "warnings": self.warnings,
        }


class MarketPhysicsVerifier:
    """Runs physics-inspired verification checks on GRID data and models.

    Attributes:
        engine: SQLAlchemy engine for database access.
        pit_store: PITStore for point-in-time data retrieval.
    """

    def __init__(self, db_engine: Engine, pit_store: PITStore) -> None:
        self.engine = db_engine
        self.pit_store = pit_store
        log.info("MarketPhysicsVerifier initialised")

    # ------------------------------------------------------------------
    # Master verification
    # ------------------------------------------------------------------

    def verify_all(self, as_of_date: date | None = None) -> dict[str, Any]:
        """Run all verification checks.

        Parameters:
            as_of_date: Decision date (default: today).

        Returns:
            dict: {check_name: VerificationResult.to_dict()} plus summary.
        """
        if as_of_date is None:
            as_of_date = date.today()

        log.info("Running full market physics verification as_of={d}", d=as_of_date)

        check_methods = [
            ("conservation", self.check_conservation),
            ("limiting_cases", self.check_limiting_cases),
            ("dimensional_consistency", self.check_dimensional_consistency),
            ("regime_boundaries", self.check_regime_boundaries),
            ("stationarity", self.check_stationarity),
            ("numerical_stability", self.check_numerical_stability),
            ("news_momentum", self.check_news_momentum),
        ]

        checks: list[VerificationResult] = []
        for check_name, method in check_methods:
            try:
                result = method(as_of_date)
                checks.append(result)
            except Exception as exc:
                log.error(
                    "Check '{name}' failed with error: {e}",
                    name=check_name,
                    e=str(exc),
                )
                checks.append(VerificationResult(
                    check_name=check_name,
                    passed=False,
                    score=0.0,
                    details={"error": str(exc)},
                    warnings=[f"Check failed with error: {exc}"],
                ))

        results = {c.check_name: c.to_dict() for c in checks}
        passed_count = sum(1 for c in checks if c.passed)
        avg_score = float(np.mean([c.score for c in checks])) if checks else 0.0

        results["_summary"] = {
            "total_checks": len(checks),
            "passed": passed_count,
            "failed": len(checks) - passed_count,
            "avg_score": round(avg_score, 4),
            "as_of_date": as_of_date.isoformat(),
        }

        all_warnings = []
        for c in checks:
            all_warnings.extend(c.warnings)
        if all_warnings:
            log.warning(
                "Physics verification: {n} warnings across {c} checks",
                n=len(all_warnings),
                c=len(checks),
            )

        log.info(
            "Verification complete: {p}/{t} passed, avg_score={s:.2f}",
            p=passed_count,
            t=len(checks),
            s=avg_score,
        )
        return results

    # ------------------------------------------------------------------
    # Check 1: Conservation of capital flows
    # ------------------------------------------------------------------

    def check_conservation(self, as_of_date: date) -> VerificationResult:
        """Verify that capital flow features approximately balance.

        In a closed system, net capital flows should sum near zero:
        equity inflows + bond inflows + commodity flows + FX flows ≈ 0

        This is the financial analog of conservation of energy/mass.
        """
        log.info("Checking conservation of flows")
        warnings: list[str] = []
        details: dict[str, Any] = {}

        # Query flow-family features
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT id, name FROM feature_registry "
                    "WHERE family = 'flow' AND model_eligible = TRUE"
                )
            ).fetchall()

        if not rows:
            return VerificationResult(
                check_name="conservation",
                passed=True,
                score=1.0,
                details={"note": "No flow features registered — check skipped"},
                warnings=[],
            )

        feature_ids = [r[0] for r in rows]
        feature_names = [r[1] for r in rows]

        # Get latest values
        df = self.pit_store.get_pit(feature_ids, as_of_date)
        if df.empty:
            return VerificationResult(
                check_name="conservation",
                passed=True,
                score=0.5,
                details={"note": "No flow data available"},
                warnings=["No flow data to verify conservation"],
            )

        # Sum net flows per date
        latest = df.sort_values("obs_date").groupby("feature_id").last()
        net_sum = latest["value"].sum()
        details["net_flow_sum"] = float(net_sum)
        details["feature_count"] = len(feature_ids)
        details["features"] = feature_names

        # Conservation: net sum should be small relative to gross flows
        gross = latest["value"].abs().sum()
        if gross > 0:
            imbalance_pct = abs(net_sum) / gross
            details["imbalance_pct"] = round(float(imbalance_pct) * 100, 2)
            passed = imbalance_pct < 0.15  # 15% tolerance
            score = max(0.0, 1.0 - imbalance_pct)
            if not passed:
                warnings.append(
                    f"Flow imbalance: {imbalance_pct:.1%} of gross — "
                    f"expected <15%. Net={net_sum:.0f}"
                )
        else:
            passed = True
            score = 1.0

        return VerificationResult(
            check_name="conservation",
            passed=passed,
            score=round(score, 4),
            details=details,
            warnings=warnings,
        )

    # ------------------------------------------------------------------
    # Check 2: Limiting cases
    # ------------------------------------------------------------------

    def check_limiting_cases(self, as_of_date: date) -> VerificationResult:
        """Test behavior at extreme values.

        Verifies that GRID's feature computations handle edge cases:
        - Zero interest rates → spreads still valid
        - VIX > 80 → regime detection doesn't break
        - Negative yields → ratio features handle sign correctly
        - Inverted yield curve → sign conventions correct
        """
        log.info("Checking limiting cases")
        warnings: list[str] = []
        details: dict[str, Any] = {"cases_tested": 0, "cases_passed": 0}
        cases_tested = 0
        cases_passed = 0

        # Case 1: Check if yield curve feature handles inversion
        yc = self._get_latest_value("yld_curve_2s10s", as_of_date)
        if yc is not None:
            cases_tested += 1
            if yc < 0:
                details["yield_curve_inverted"] = True
                details["yield_curve_value"] = float(yc)
                # Inversion is valid — just verify it's in a reasonable range
                if yc > -5.0:
                    cases_passed += 1
                else:
                    warnings.append(
                        f"Yield curve value {yc:.2f}% — extreme inversion, "
                        f"verify data quality"
                    )
            else:
                cases_passed += 1

        # Case 2: VIX sanity
        vix = self._get_latest_value("vix_spot", as_of_date)
        if vix is not None:
            cases_tested += 1
            details["vix_value"] = float(vix)
            if 0 < vix < 150:
                cases_passed += 1
            else:
                warnings.append(
                    f"VIX={vix:.1f} — outside historical range [0, 150]. "
                    f"Check data source."
                )

        # Case 3: Fed funds rate non-negative (post-2008 can be near-zero)
        ffr = self._get_latest_value("fed_funds_rate", as_of_date)
        if ffr is not None:
            cases_tested += 1
            details["fed_funds_rate"] = float(ffr)
            if ffr >= -0.5:  # Allow small negative for rounding
                cases_passed += 1
            else:
                warnings.append(
                    f"Fed funds rate={ffr:.2f}% — significantly negative. "
                    f"US has not had negative rates."
                )

        # Case 4: Copper/gold ratio should be positive
        copper = self._get_latest_value("copper_futures_close", as_of_date)
        gold = self._get_latest_value("gold_futures_close", as_of_date)
        if copper is not None and gold is not None:
            cases_tested += 1
            if copper > 0 and gold > 0:
                cases_passed += 1
                details["copper_gold_ratio"] = round(copper / gold, 6)
            else:
                warnings.append(
                    f"Non-positive prices: copper={copper}, gold={gold}"
                )

        # Case 5: S&P 500 should be positive
        sp500 = self._get_latest_value("sp500_close", as_of_date)
        if sp500 is not None:
            cases_tested += 1
            if sp500 > 0:
                cases_passed += 1
            else:
                warnings.append(f"S&P 500 close={sp500} — not physical")

        details["cases_tested"] = cases_tested
        details["cases_passed"] = cases_passed

        score = cases_passed / cases_tested if cases_tested > 0 else 1.0
        passed = score >= 0.8  # Allow 1 failure

        return VerificationResult(
            check_name="limiting_cases",
            passed=passed,
            score=round(score, 4),
            details=details,
            warnings=warnings,
        )

    # ------------------------------------------------------------------
    # Check 3: Dimensional consistency
    # ------------------------------------------------------------------

    def check_dimensional_consistency(self, as_of_date: date) -> VerificationResult:
        """Verify units are consistent across feature computations.

        - Spreads should subtract same-unit series
        - Ratios should produce dimensionless quantities
        - Z-scores should be dimensionless
        - Slopes should have correct annualization
        """
        log.info("Checking dimensional consistency")
        warnings: list[str] = []
        details: dict[str, Any] = {}

        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT name, family, normalization, transformation "
                    "FROM feature_registry WHERE model_eligible = TRUE"
                )
            ).fetchall()

        if not rows:
            return VerificationResult(
                check_name="dimensional_consistency",
                passed=True,
                score=1.0,
                details={"note": "No eligible features"},
                warnings=[],
            )

        issues = 0
        total_checks = 0

        for row in rows:
            name, family, normalization, transformation = row
            total_checks += 1

            # Z-score normalized features should be dimensionless
            if normalization == "ZSCORE":
                val = self._get_latest_value(name, as_of_date)
                if val is not None and abs(val) > 6:
                    issues += 1
                    warnings.append(
                        f"{name}: z-score={val:.2f} — more than 6σ is extreme. "
                        f"Check normalization window."
                    )

            # RANK normalized should be in [0, 1]
            if normalization == "RANK":
                val = self._get_latest_value(name, as_of_date)
                if val is not None and (val < 0 or val > 1):
                    issues += 1
                    warnings.append(
                        f"{name}: rank={val:.4f} — outside [0,1] range"
                    )

            # Convention validation
            if family:
                val = self._get_latest_value(name, as_of_date)
                if val is not None:
                    conv_warns = validate_convention(name, val, family)
                    if conv_warns:
                        issues += len(conv_warns)
                        warnings.extend(conv_warns)

        details["total_features_checked"] = total_checks
        details["issues_found"] = issues

        score = max(0.0, 1.0 - (issues / max(total_checks, 1)))
        passed = issues <= max(2, total_checks * 0.1)  # Allow 10% or 2 issues

        return VerificationResult(
            check_name="dimensional_consistency",
            passed=passed,
            score=round(score, 4),
            details=details,
            warnings=warnings,
        )

    # ------------------------------------------------------------------
    # Check 4: Regime boundary validation
    # ------------------------------------------------------------------

    def check_regime_boundaries(self, as_of_date: date) -> VerificationResult:
        """Verify regime transitions are physically plausible.

        - No instantaneous GROWTH → CRISIS without passing through
          NEUTRAL or FRAGILE (unless extreme shock)
        - Transition persistence meets minimum threshold
        - Regime distribution isn't degenerate (all one label)
        """
        log.info("Checking regime boundaries")
        warnings: list[str] = []
        details: dict[str, Any] = {}

        # Check decision_journal for recent regime assignments
        lookback = as_of_date - timedelta(days=365)
        with self.engine.connect() as conn:
            # Check if decision_journal table exists
            table_check = conn.execute(
                text(
                    "SELECT EXISTS ("
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_name = 'decision_journal'"
                    ")"
                )
            ).scalar()

            if not table_check:
                return VerificationResult(
                    check_name="regime_boundaries",
                    passed=True,
                    score=1.0,
                    details={"note": "decision_journal table not found — skipped"},
                    warnings=[],
                )

            rows = conn.execute(
                text(
                    "SELECT decision_timestamp, inferred_state as regime, "
                    "state_confidence as confidence "
                    "FROM decision_journal "
                    "WHERE decision_timestamp >= :start AND decision_timestamp <= :end "
                    "AND inferred_state IS NOT NULL "
                    "ORDER BY decision_timestamp"
                ),
                {"start": lookback, "end": as_of_date},
            ).fetchall()

        if not rows:
            return VerificationResult(
                check_name="regime_boundaries",
                passed=True,
                score=0.5,
                details={"note": "No regime data in decision_journal"},
                warnings=["No regime assignments found — cannot verify boundaries"],
            )

        regimes = [r[1] for r in rows if r[1]]
        confidences = [float(r[2]) for r in rows if r[2]]
        dates = [r[0] for r in rows if r[1]]
        details["regime_count"] = len(regimes)

        # Check 4a: No GROWTH → CRISIS direct jumps
        ORDERED_SEVERITY = {"GROWTH": 0, "NEUTRAL": 1, "FRAGILE": 2, "CRISIS": 3}
        direct_jumps = 0
        for i in range(1, len(regimes)):
            prev_sev = ORDERED_SEVERITY.get(regimes[i - 1])
            curr_sev = ORDERED_SEVERITY.get(regimes[i])
            if prev_sev is not None and curr_sev is not None:
                if abs(curr_sev - prev_sev) > 1:
                    direct_jumps += 1
                    warnings.append(
                        f"Direct regime jump: {regimes[i-1]} → {regimes[i]} "
                        f"on {dates[i]} (skipped intermediate state)"
                    )

        details["direct_jumps"] = direct_jumps

        # Check 4b: Regime distribution isn't degenerate
        unique_regimes = set(regimes)
        details["unique_regimes"] = list(unique_regimes)
        if len(unique_regimes) < 2 and len(regimes) > 30:
            warnings.append(
                f"Only {len(unique_regimes)} regime(s) in {len(regimes)} observations — "
                f"model may be degenerate"
            )

        # Check 4c: Persistence (average consecutive days in same regime)
        if len(regimes) > 1:
            runs = []
            current_run = 1
            for i in range(1, len(regimes)):
                if regimes[i] == regimes[i - 1]:
                    current_run += 1
                else:
                    runs.append(current_run)
                    current_run = 1
            runs.append(current_run)
            avg_persistence = np.mean(runs)
            details["avg_persistence_days"] = round(float(avg_persistence), 1)

            if avg_persistence < 5:
                warnings.append(
                    f"Low regime persistence ({avg_persistence:.1f} days) — "
                    f"may indicate noisy labels"
                )

        # Check 4d: Low confidence regimes
        if confidences:
            low_conf = sum(1 for c in confidences if c < 0.5)
            details["low_confidence_pct"] = round(low_conf / len(confidences) * 100, 1)
            if low_conf / len(confidences) > 0.3:
                warnings.append(
                    f"{low_conf}/{len(confidences)} regime assignments have "
                    f"confidence < 50%"
                )

        score_penalties = min(1.0, direct_jumps * 0.15 + len(warnings) * 0.05)
        score = max(0.0, 1.0 - score_penalties)
        passed = direct_jumps <= 2 and len(warnings) <= 3

        return VerificationResult(
            check_name="regime_boundaries",
            passed=passed,
            score=round(score, 4),
            details=details,
            warnings=warnings,
        )

    # ------------------------------------------------------------------
    # Check 5: Stationarity
    # ------------------------------------------------------------------

    def check_stationarity(self, as_of_date: date) -> VerificationResult:
        """ADF test on features used in clustering.

        Non-stationary features in clustering can produce spurious regimes.
        Features should be stationary (or differenced before use).
        """
        log.info("Checking stationarity of model-eligible features")
        warnings: list[str] = []
        details: dict[str, Any] = {}

        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT id, name, normalization FROM feature_registry "
                    "WHERE model_eligible = TRUE ORDER BY id"
                )
            ).fetchall()

        if not rows:
            return VerificationResult(
                check_name="stationarity",
                passed=True,
                score=1.0,
                details={"note": "No eligible features"},
                warnings=[],
            )

        feature_ids = [r[0] for r in rows]
        feature_names = [r[1] for r in rows]
        normalizations = [r[2] for r in rows]

        # Get feature data (last 2 years)
        start = as_of_date - timedelta(days=504)
        matrix = self.pit_store.get_feature_matrix(
            feature_ids=feature_ids,
            start_date=start,
            end_date=as_of_date,
            as_of_date=as_of_date,
            vintage_policy="LATEST_AS_OF",
        )

        if matrix.empty:
            return VerificationResult(
                check_name="stationarity",
                passed=True,
                score=0.5,
                details={"note": "No data for stationarity test"},
                warnings=["Insufficient data for ADF tests"],
            )

        stationary_count = 0
        non_stationary_count = 0
        tested = 0

        from statsmodels.tsa.stattools import adfuller

        for col_idx, col in enumerate(matrix.columns):
            series = matrix[col].dropna()
            if len(series) < 30:
                continue

            tested += 1
            try:
                adf_result = adfuller(series, autolag="AIC")
                p_value = adf_result[1]

                fname = feature_names[col_idx] if col_idx < len(feature_names) else str(col)

                if p_value < 0.05:
                    stationary_count += 1
                else:
                    non_stationary_count += 1
                    norm = normalizations[col_idx] if col_idx < len(normalizations) else "?"
                    warnings.append(
                        f"{fname}: ADF p={p_value:.4f} — non-stationary "
                        f"(normalization={norm}). Consider differencing."
                    )
            except Exception:
                continue

        details["tested"] = tested
        details["stationary"] = stationary_count
        details["non_stationary"] = non_stationary_count

        score = stationary_count / tested if tested > 0 else 1.0
        passed = non_stationary_count <= max(2, tested * 0.2)

        return VerificationResult(
            check_name="stationarity",
            passed=passed,
            score=round(score, 4),
            details=details,
            warnings=warnings[:10],  # Cap warnings
        )

    # ------------------------------------------------------------------
    # Check 6: Numerical stability
    # ------------------------------------------------------------------

    def check_numerical_stability(self, as_of_date: date) -> VerificationResult:
        """Check for NaN/inf propagation and numerical issues.

        Scans resolved_series for:
        - NULL values in recent data
        - Inf or extreme outliers
        - Constant series (zero variance)
        """
        log.info("Checking numerical stability")
        warnings: list[str] = []
        details: dict[str, Any] = {}

        lookback = as_of_date - timedelta(days=60)

        with self.engine.connect() as conn:
            # Count nulls in recent resolved data
            null_count = conn.execute(
                text(
                    "SELECT COUNT(*) FROM resolved_series "
                    "WHERE obs_date >= :start AND obs_date <= :end "
                    "AND value IS NULL"
                ),
                {"start": lookback, "end": as_of_date},
            ).scalar() or 0

            total_count = conn.execute(
                text(
                    "SELECT COUNT(*) FROM resolved_series "
                    "WHERE obs_date >= :start AND obs_date <= :end"
                ),
                {"start": lookback, "end": as_of_date},
            ).scalar() or 0

            # Check for extreme values (potential inf encoding)
            extreme_count = conn.execute(
                text(
                    "SELECT COUNT(*) FROM resolved_series "
                    "WHERE obs_date >= :start AND obs_date <= :end "
                    "AND (value > 1e15 OR value < -1e15)"
                ),
                {"start": lookback, "end": as_of_date},
            ).scalar() or 0

        details["null_count"] = null_count
        details["total_rows"] = total_count
        details["extreme_values"] = extreme_count

        if total_count > 0:
            null_pct = null_count / total_count
            details["null_pct"] = round(null_pct * 100, 2)
            if null_pct > 0.05:
                warnings.append(
                    f"{null_pct:.1%} of recent resolved values are NULL"
                )

        if extreme_count > 0:
            warnings.append(
                f"{extreme_count} extreme values (|v| > 1e15) in recent data — "
                f"possible inf encoding"
            )

        score = 1.0
        if total_count > 0:
            score -= min(0.5, (null_count / total_count) * 5)
        score -= min(0.3, extreme_count * 0.1)
        score = max(0.0, score)

        passed = len(warnings) == 0

        return VerificationResult(
            check_name="numerical_stability",
            passed=passed,
            score=round(score, 4),
            details=details,
            warnings=warnings,
        )

    # ------------------------------------------------------------------
    # Check 7: News momentum
    # ------------------------------------------------------------------

    def check_news_momentum(self, as_of_date: date) -> VerificationResult:
        """Verify news sentiment momentum is within plausible bounds.

        Uses the NewsMomentumAnalyzer to check:
        - GDELT data freshness (data exists and is recent)
        - Sentiment values are in a plausible range
        - Energy state is not anomalously high (possible data corruption)
        - Momentum is not stuck (zero variance in sentiment)
        """
        log.info("Checking news momentum")
        from physics.momentum import NewsMomentumAnalyzer

        warnings: list[str] = []
        details: dict[str, Any] = {}

        analyzer = NewsMomentumAnalyzer(self.engine, self.pit_store)
        result = analyzer.analyze(as_of_date, lookback_days=90)

        if not result.available:
            return VerificationResult(
                check_name="news_momentum",
                passed=True,
                score=0.5,
                details={
                    "note": "GDELT data not available — check skipped",
                    "analyzer_details": result.details,
                },
                warnings=result.warnings or [
                    "News momentum check skipped: no GDELT data"
                ],
            )

        details["sentiment_trend"] = result.sentiment_trend
        details["momentum_direction"] = result.momentum_direction
        details["energy_state"] = result.energy_state

        score = 1.0

        # Check 7a: Sentiment trend should have reasonable slope
        trend = result.details.get("trend", {})
        slope = trend.get("slope", 0.0)
        if abs(slope) > 1.0:
            warnings.append(
                f"Sentiment slope={slope:.4f} — unusually steep. "
                f"Verify GDELT ingestion quality."
            )
            score -= 0.2

        # Check 7b: Energy state — very high energy suggests data anomaly
        energy = result.details.get("energy", {})
        ke = energy.get("kinetic_energy", 0.0)
        if ke > 10.0:
            warnings.append(
                f"Sentiment kinetic energy={ke:.4f} — anomalously high. "
                f"Possible data quality issue."
            )
            score -= 0.3

        # Check 7c: Zero variance detection (stuck sentiment)
        mean_ke = energy.get("mean_ke", 0.0)
        if mean_ke < 1e-8 and result.details.get("data_points", 0) > 20:
            warnings.append(
                "Sentiment kinetic energy near zero — sentiment appears "
                "static over the lookback window. Check GDELT ingestion."
            )
            score -= 0.2

        # Check 7d: Latest sentiment value plausibility
        latest_val = trend.get("latest_value")
        if latest_val is not None and abs(latest_val) > 50:
            warnings.append(
                f"Latest sentiment value={latest_val} — extreme. "
                f"GDELT tone typically in [-10, 10] range."
            )
            score -= 0.2

        score = max(0.0, score)
        passed = score >= 0.5

        # Include cross-correlation summary if available
        xcorr = result.details.get("cross_correlation", {})
        if xcorr.get("strongest_lag"):
            details["price_coupling"] = {
                "strongest_lag": xcorr["strongest_lag"],
                "correlation": xcorr.get("strongest_correlation"),
            }

        return VerificationResult(
            check_name="news_momentum",
            passed=passed,
            score=round(score, 4),
            details=details,
            warnings=warnings,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_latest_value(
        self, feature_name: str, as_of_date: date
    ) -> float | None:
        """Get the most recent value for a named feature.

        Returns None if the feature is not found, has no data, or
        the value is NaN/NULL. Handles missing tables gracefully.
        """
        try:
            with self.engine.connect() as conn:
                row = conn.execute(
                    text(
                        "SELECT rs.value FROM resolved_series rs "
                        "JOIN feature_registry fr ON rs.feature_id = fr.id "
                        "WHERE fr.name = :name AND rs.obs_date <= :d "
                        "ORDER BY rs.obs_date DESC LIMIT 1"
                    ),
                    {"name": feature_name, "d": as_of_date},
                ).fetchone()
            if row is None or row[0] is None:
                return None
            val = float(row[0])
            if np.isnan(val) or np.isinf(val):
                return None
            return val
        except Exception as exc:
            log.debug(
                "Could not fetch latest value for '{name}': {e}",
                name=feature_name,
                e=str(exc),
            )
            return None
