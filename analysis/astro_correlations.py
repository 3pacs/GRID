"""
Celestial-Market Correlation Engine.

Computes statistical relationships between celestial features and market features.
Uses PIT-correct data, lead/lag analysis, and bootstrap significance testing.

DB table:
    astro_correlations — cached correlation results, refreshed weekly.
"""

from __future__ import annotations

import math
import random
from datetime import date, datetime, timedelta, timezone
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── All 23 celestial features ─────────────────────────────────────────
ALL_CELESTIAL_FEATURES: list[str] = [
    # Lunar (6)
    "lunar_phase",
    "lunar_illumination",
    "days_to_new_moon",
    "days_to_full_moon",
    "lunar_eclipse_proximity",
    "solar_eclipse_proximity",
    # Planetary (5)
    "mercury_retrograde",
    "jupiter_saturn_angle",
    "mars_volatility_index",
    "planetary_stress_index",
    "venus_cycle_phase",
    # Solar (7)
    "sunspot_number",
    "solar_flux_10_7cm",
    "geomagnetic_kp_index",
    "geomagnetic_ap_index",
    "solar_wind_speed",
    "solar_storm_probability",
    "solar_cycle_phase",
    # Vedic (5)
    "nakshatra_index",
    "nakshatra_quality",
    "tithi",
    "rahu_ketu_axis",
    "dasha_cycle_phase",
]

DEFAULT_MARKET_FEATURES: list[str] = [
    "spy_full",
    "qqq_full",
    "btc-usd_full",
    "vix_full",
]

_ENSURE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS astro_correlations (
    id SERIAL PRIMARY KEY,
    celestial_feature TEXT NOT NULL,
    market_feature TEXT NOT NULL,
    correlation FLOAT,
    optimal_lag INTEGER,
    p_value FLOAT,
    n_observations INTEGER,
    confidence_low FLOAT,
    confidence_high FLOAT,
    computed_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_astro_corr_computed ON astro_correlations(computed_at DESC);
"""


# ── Helpers ────────────────────────────────────────────────────────────

def _get_feature_series(
    engine: Engine,
    feature_name: str,
    days: int = 504,
) -> pd.Series | None:
    """Pull a PIT-correct time series for *feature_name* from resolved_series."""
    today = date.today()
    start = today - timedelta(days=days)

    with engine.connect() as conn:
        fid_row = conn.execute(
            text(
                "SELECT id FROM feature_registry "
                "WHERE name = :name AND model_eligible = TRUE"
            ),
            {"name": feature_name},
        ).fetchone()

        if not fid_row:
            return None

        fid = fid_row[0]

        rows = conn.execute(
            text(
                "SELECT DISTINCT ON (obs_date) obs_date, value "
                "FROM resolved_series "
                "WHERE feature_id = :fid "
                "  AND obs_date >= :start "
                "  AND obs_date <= :end "
                "ORDER BY obs_date, vintage_date DESC"
            ),
            {"fid": fid, "start": start, "end": today},
        ).fetchall()

    if not rows:
        return None

    dates = [r[0] for r in rows]
    values = [float(r[1]) for r in rows]
    series = pd.Series(values, index=pd.DatetimeIndex(dates), name=feature_name)
    series = series.sort_index()
    series = series[~series.index.duplicated(keep="last")]
    return series


def _bootstrap_p_value(
    x: np.ndarray,
    y: np.ndarray,
    observed_corr: float,
    n_resamples: int = 1000,
) -> tuple[float, float, float]:
    """Bootstrap significance test for correlation.

    Returns (p_value, ci_low, ci_high) at the 95% level.
    Shuffles *y* to break temporal dependence, computes the null distribution
    of Pearson r, and measures how often the null exceeds the observed |r|.
    """
    rng = np.random.default_rng(42)
    null_corrs = np.empty(n_resamples)
    n = len(x)

    for i in range(n_resamples):
        idx = rng.permutation(n)
        null_corrs[i] = np.corrcoef(x, y[idx])[0, 1]

    # p-value: fraction of null correlations with |r| >= observed |r|
    p_value = float(np.mean(np.abs(null_corrs) >= abs(observed_corr)))

    # Bootstrap CI on the *observed* correlation via resampling with replacement
    boot_corrs = np.empty(n_resamples)
    for i in range(n_resamples):
        idx = rng.choice(n, size=n, replace=True)
        boot_corrs[i] = np.corrcoef(x[idx], y[idx])[0, 1]

    ci_low = float(np.nanpercentile(boot_corrs, 2.5))
    ci_high = float(np.nanpercentile(boot_corrs, 97.5))

    return p_value, ci_low, ci_high


# ── Main engine ────────────────────────────────────────────────────────

class AstroCorrelationEngine:
    """Computes and caches celestial-market correlations."""

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine
        self._ensure_table()

    def _ensure_table(self) -> None:
        """Create the astro_correlations table if it does not exist."""
        try:
            with self.engine.begin() as conn:
                for stmt in _ENSURE_TABLE_SQL.strip().split(";"):
                    stmt = stmt.strip()
                    if stmt:
                        conn.execute(text(stmt))
        except Exception as exc:
            log.warning("Could not ensure astro_correlations table: {e}", e=str(exc))

    # ── Core correlation computation ──────────────────────────────

    def compute_correlations(
        self,
        celestial_features: list[str] | None = None,
        market_features: list[str] | None = None,
        lookback_days: int = 504,
    ) -> list[dict]:
        """Compute correlations between celestial and market features.

        For each pair, computes Pearson r at lags -30..+30 days,
        finds the optimal lag, and runs a bootstrap significance test.
        Returns only significant pairs (p <= 0.05), sorted by |r| descending.
        """
        cel_features = celestial_features or ALL_CELESTIAL_FEATURES
        mkt_features = market_features or DEFAULT_MARKET_FEATURES

        results: list[dict] = []

        for mkt_name in mkt_features:
            mkt_series = _get_feature_series(self.engine, mkt_name, lookback_days)
            if mkt_series is None or len(mkt_series) < 60:
                continue

            for cel_name in cel_features:
                cel_series = _get_feature_series(self.engine, cel_name, lookback_days)
                if cel_series is None or len(cel_series) < 60:
                    continue

                result = self._correlate_pair(cel_series, mkt_series, cel_name, mkt_name)
                if result is not None:
                    results.append(result)

        # Sort by |correlation| descending
        results.sort(key=lambda r: abs(r["correlation"]), reverse=True)
        return results

    def _correlate_pair(
        self,
        cel: pd.Series,
        mkt: pd.Series,
        cel_name: str,
        mkt_name: str,
    ) -> dict | None:
        """Compute lag-optimised correlation for a single pair.

        Returns a result dict or None if insufficient data or not significant.
        """
        # Align on common dates
        common = cel.index.intersection(mkt.index)
        if len(common) < 70:
            return None

        c = cel.loc[common].sort_index().values.astype(float)
        m = mkt.loc[common].sort_index().values.astype(float)
        n = len(c)

        # Skip zero-variance series
        if np.std(c) < 1e-12 or np.std(m) < 1e-12:
            return None

        best_corr = 0.0
        best_lag = 0
        max_lag = 30

        for lag in range(-max_lag, max_lag + 1):
            if lag == 0:
                r = float(np.corrcoef(c, m)[0, 1])
            elif lag > 0:
                # Celestial leads market by `lag` days
                if lag >= n:
                    continue
                r = float(np.corrcoef(c[:-lag], m[lag:])[0, 1])
            else:
                # Market leads celestial by |lag| days
                alag = abs(lag)
                if alag >= n:
                    continue
                r = float(np.corrcoef(c[alag:], m[:-alag])[0, 1])

            if math.isnan(r):
                continue
            if abs(r) > abs(best_corr):
                best_corr = r
                best_lag = lag

        if abs(best_corr) < 0.05:
            return None

        # Bootstrap significance test at optimal lag
        if best_lag == 0:
            x, y = c, m
        elif best_lag > 0:
            x, y = c[:-best_lag], m[best_lag:]
        else:
            alag = abs(best_lag)
            x, y = c[alag:], m[:-alag]

        p_value, ci_low, ci_high = _bootstrap_p_value(x, y, best_corr, n_resamples=1000)

        if p_value > 0.05:
            return None

        return {
            "celestial_feature": cel_name,
            "market_feature": mkt_name,
            "correlation": round(best_corr, 4),
            "optimal_lag": best_lag,
            "p_value": round(p_value, 4),
            "n_observations": len(x),
            "confidence_low": round(ci_low, 4),
            "confidence_high": round(ci_high, 4),
        }

    # ── Event impact analysis ─────────────────────────────────────

    def compute_event_impact(
        self,
        event_type: str,
        market_feature: str = "spy_full",
        window_days: int = 5,
    ) -> dict:
        """Compute market return around historical celestial events.

        Supported event_type: mercury_retrograde, full_moon, new_moon,
        lunar_eclipse, solar_eclipse.
        """
        from ingestion.celestial.planetary import _MERCURY_RETROGRADES
        from ingestion.celestial.lunar import (
            _LUNAR_ECLIPSES,
            _SOLAR_ECLIPSES,
            _lunar_phase,
            SYNODIC_MONTH,
        )

        # Determine event dates
        event_dates: list[date] = []
        today = date.today()

        if event_type == "mercury_retrograde":
            event_dates = [s for s, e in _MERCURY_RETROGRADES if s <= today]
        elif event_type == "full_moon":
            # Compute full moons over last 5 years
            d = today - timedelta(days=5 * 365)
            while d <= today:
                phase = _lunar_phase(d)
                if abs(phase - 0.5) < 0.02:
                    event_dates.append(d)
                    d += timedelta(days=25)
                else:
                    d += timedelta(days=1)
        elif event_type == "new_moon":
            d = today - timedelta(days=5 * 365)
            while d <= today:
                phase = _lunar_phase(d)
                if phase < 0.02 or phase > 0.98:
                    event_dates.append(d)
                    d += timedelta(days=25)
                else:
                    d += timedelta(days=1)
        elif event_type == "lunar_eclipse":
            event_dates = [e for e in _LUNAR_ECLIPSES if e <= today]
        elif event_type == "solar_eclipse":
            event_dates = [e for e in _SOLAR_ECLIPSES if e <= today]
        else:
            return {"event_type": event_type, "error": f"Unknown event type: {event_type}"}

        if not event_dates:
            return {"event_type": event_type, "n_events": 0, "error": "No events found"}

        # Pull market data with enough lookback
        lookback = (today - min(event_dates)).days + window_days + 30
        mkt = _get_feature_series(self.engine, market_feature, days=lookback)
        if mkt is None or len(mkt) < 20:
            return {
                "event_type": event_type,
                "n_events": len(event_dates),
                "error": f"Insufficient market data for {market_feature}",
            }

        # Compute returns around each event
        returns: list[float] = []
        for evt_date in event_dates:
            evt_ts = pd.Timestamp(evt_date)
            # Find closest market date on or after event - window
            pre_start = evt_ts - pd.Timedelta(days=window_days + 5)
            post_end = evt_ts + pd.Timedelta(days=window_days + 5)
            window = mkt.loc[pre_start:post_end]
            if len(window) < 3:
                continue

            # Use first and last value in window to compute return
            pre_vals = mkt.loc[pre_start:evt_ts]
            post_vals = mkt.loc[evt_ts:post_end]
            if len(pre_vals) > 0 and len(post_vals) > 0:
                pre_price = float(pre_vals.iloc[0])
                post_price = float(post_vals.iloc[-1])
                if pre_price > 0:
                    ret = (post_price - pre_price) / pre_price * 100.0
                    returns.append(ret)

        if not returns:
            return {
                "event_type": event_type,
                "n_events": len(event_dates),
                "error": "No overlapping market data for events",
            }

        arr = np.array(returns)

        # Baseline: overall market return over same-length windows
        baseline_returns: list[float] = []
        n_samples = min(200, len(mkt) - 2 * window_days)
        rng = np.random.default_rng(42)
        sample_indices = rng.choice(
            len(mkt) - 2 * window_days, size=max(1, n_samples), replace=False
        )
        for idx in sample_indices:
            pre_price = float(mkt.iloc[idx])
            post_price = float(mkt.iloc[min(idx + 2 * window_days, len(mkt) - 1)])
            if pre_price > 0:
                baseline_returns.append((post_price - pre_price) / pre_price * 100.0)

        baseline_avg = float(np.mean(baseline_returns)) if baseline_returns else 0.0

        return {
            "event_type": event_type,
            "market_feature": market_feature,
            "window_days": window_days,
            "n_events": len(returns),
            "avg_return": round(float(np.mean(arr)), 4),
            "median_return": round(float(np.median(arr)), 4),
            "std_return": round(float(np.std(arr)), 4),
            "positive_pct": round(float(np.mean(arr > 0) * 100), 1),
            "comparison_baseline": round(baseline_avg, 4),
        }

    # ── Caching ───────────────────────────────────────────────────

    def get_cached_or_compute(self, force_refresh: bool = False) -> list[dict]:
        """Return cached correlations if fresh (< 24h), otherwise recompute.

        Results are stored in the astro_correlations table.
        """
        if not force_refresh:
            try:
                with self.engine.connect() as conn:
                    row = conn.execute(
                        text(
                            "SELECT computed_at FROM astro_correlations "
                            "ORDER BY computed_at DESC LIMIT 1"
                        )
                    ).fetchone()

                if row:
                    computed_at = row[0]
                    if isinstance(computed_at, datetime):
                        if computed_at.tzinfo is None:
                            computed_at = computed_at.replace(tzinfo=timezone.utc)
                        age = datetime.now(timezone.utc) - computed_at
                        if age < timedelta(hours=24):
                            return self._load_cached()
            except Exception as exc:
                log.debug("Cache check failed: {e}", e=str(exc))

        # Compute fresh
        log.info("Computing astro correlations (this may take a few minutes)...")
        results = self.compute_correlations()
        self._store_results(results)
        log.info("Astro correlations computed: {n} significant pairs", n=len(results))
        return results

    def _load_cached(self) -> list[dict]:
        """Load the most recent batch of cached correlations."""
        with self.engine.connect() as conn:
            # Get the latest computed_at timestamp
            ts_row = conn.execute(
                text(
                    "SELECT computed_at FROM astro_correlations "
                    "ORDER BY computed_at DESC LIMIT 1"
                )
            ).fetchone()

            if not ts_row:
                return []

            latest_ts = ts_row[0]

            rows = conn.execute(
                text(
                    "SELECT celestial_feature, market_feature, correlation, "
                    "       optimal_lag, p_value, n_observations, "
                    "       confidence_low, confidence_high "
                    "FROM astro_correlations "
                    "WHERE computed_at = :ts "
                    "ORDER BY ABS(correlation) DESC"
                ),
                {"ts": latest_ts},
            ).fetchall()

        return [
            {
                "celestial_feature": r[0],
                "market_feature": r[1],
                "correlation": r[2],
                "optimal_lag": r[3],
                "p_value": r[4],
                "n_observations": r[5],
                "confidence_low": r[6],
                "confidence_high": r[7],
            }
            for r in rows
        ]

    def _store_results(self, results: list[dict]) -> None:
        """Store correlation results in astro_correlations table."""
        if not results:
            return

        now = datetime.now(timezone.utc)
        try:
            with self.engine.begin() as conn:
                for r in results:
                    conn.execute(
                        text(
                            "INSERT INTO astro_correlations "
                            "(celestial_feature, market_feature, correlation, "
                            " optimal_lag, p_value, n_observations, "
                            " confidence_low, confidence_high, computed_at) "
                            "VALUES (:cf, :mf, :corr, :lag, :pv, :n, :cl, :ch, :ts)"
                        ),
                        {
                            "cf": r["celestial_feature"],
                            "mf": r["market_feature"],
                            "corr": r["correlation"],
                            "lag": r["optimal_lag"],
                            "pv": r["p_value"],
                            "n": r["n_observations"],
                            "cl": r["confidence_low"],
                            "ch": r["confidence_high"],
                            "ts": now,
                        },
                    )
        except Exception as exc:
            log.error("Failed to store astro correlations: {e}", e=str(exc))


if __name__ == "__main__":
    from db import get_engine

    engine = get_engine()
    ace = AstroCorrelationEngine(engine)
    results = ace.get_cached_or_compute(force_refresh=True)
    print(f"\n{len(results)} significant celestial-market correlations found:\n")
    for r in results[:20]:
        print(
            f"  {r['celestial_feature']:30s} <-> {r['market_feature']:15s}  "
            f"r={r['correlation']:+.4f}  lag={r['optimal_lag']:+3d}  "
            f"p={r['p_value']:.4f}  n={r['n_observations']}"
        )
