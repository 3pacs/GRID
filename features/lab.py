"""
GRID feature transformation engine.

Computes derived features from raw and resolved series using transformation
rules defined in the feature registry.  Provides a library of reusable
transformation functions (z-score, rolling slope, lagged change, ratio, spread).

Also houses the per-sector percentile-rank primitives used by the capital_flow
endpoint for ratio badges (SYNTH-12: relocated from intelligence/ratio_percentiles.py
on 2026-04-11 — percentile ranking is a feature-engineering transform, not an
intelligence signal).
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log
from scipy import stats
from sqlalchemy import text
from sqlalchemy.engine import Engine

from store.pit import PITStore
from utils.ttl_cache import TTLCache


# ---------------------------------------------------------------------------
# Transformation functions
# ---------------------------------------------------------------------------

def zscore_normalize(series: pd.Series, window: int = 252) -> pd.Series:
    """Compute a rolling z-score normalisation.

    Parameters:
        series: Input time series.
        window: Lookback window in trading days (default 252 = ~1 year).

    Returns:
        pd.Series: Rolling z-score values.  NaN where the window is
                   insufficient.
    """
    # Cap window to series length so short series still produce values
    effective_window = min(window, len(series))
    min_periods = max(1, effective_window // 2)
    rolling_mean = series.rolling(window=effective_window, min_periods=min_periods).mean()
    rolling_std = series.rolling(window=effective_window, min_periods=min_periods).std()

    # For constant windows, both the residual (series - mean) and the std
    # are zero. The mathematical z-score is 0 in that degenerate case — the
    # point is exactly at the mean. Returning NaN broke downstream inference
    # any time a feed went flat. Divide safely and fill those positions with 0.
    zero_std_mask = rolling_std == 0
    safe_std = rolling_std.mask(zero_std_mask)
    result = (series - rolling_mean) / safe_std
    result = result.where(~zero_std_mask, 0.0)
    return result


def rolling_slope(series: pd.Series, window: int = 63) -> pd.Series:
    """Compute annualised linear regression slope over a rolling window.

    Parameters:
        series: Input time series.
        window: Lookback window in trading days (default 63 = ~3 months).

    Returns:
        pd.Series: Annualised slope coefficients.
    """
    def _slope(arr: np.ndarray) -> float:
        if len(arr) < 2 or np.isnan(arr).all():
            return np.nan
        valid = ~np.isnan(arr)
        if valid.sum() < 2:
            return np.nan
        x = np.arange(len(arr))[valid]
        y = arr[valid]
        slope, _, _, _, _ = stats.linregress(x, y)
        # Annualise: multiply by 252 trading days / window
        return slope * (252.0 / window)

    return series.rolling(window=window, min_periods=max(2, window // 2)).apply(
        _slope, raw=True
    )


def pct_change_lagged(series: pd.Series, lag_days: int) -> pd.Series:
    """Compute percentage change from lag_days ago.

    Parameters:
        series: Input time series.
        lag_days: Number of trading days to look back.

    Returns:
        pd.Series: Percentage change values.
    """
    return series.pct_change(periods=lag_days)


def ratio(series_a: pd.Series, series_b: pd.Series) -> pd.Series:
    """Compute element-wise ratio, handling zeros and NaNs.

    Parameters:
        series_a: Numerator series.
        series_b: Denominator series.

    Returns:
        pd.Series: Ratio values.  NaN where the denominator is zero or NaN.
    """
    denominator = series_b.replace(0, np.nan)
    return series_a / denominator


def spread(series_a: pd.Series, series_b: pd.Series) -> pd.Series:
    """Compute element-wise difference (spread).

    Parameters:
        series_a: First series.
        series_b: Second series.

    Returns:
        pd.Series: Difference values (series_a - series_b).
    """
    return series_a - series_b


# ---------------------------------------------------------------------------
# Feature Lab class
# ---------------------------------------------------------------------------

class FeatureLab:
    """Feature transformation engine that computes derived features.

    Uses the PITStore for data access and applies transformation functions
    to produce the final feature values.

    Attributes:
        engine: SQLAlchemy engine for database access.
        pit_store: PITStore instance for point-in-time queries.
    """

    def __init__(self, db_engine: Engine, pit_store: PITStore) -> None:
        """Initialise the Feature Lab.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
            pit_store: PITStore instance for point-in-time data access.
        """
        self.engine = db_engine
        self.pit_store = pit_store
        log.info("FeatureLab initialised")

    def _get_feature_id_by_name(self, name: str) -> int | None:
        """Look up a feature_registry ID by name.

        Parameters:
            name: Feature name.

        Returns:
            int: Feature ID, or None if not found.
        """
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM feature_registry WHERE name = :name"),
                {"name": name},
            ).fetchone()
        return row[0] if row else None

    def _get_pit_series(
        self,
        feature_name: str,
        as_of_date: date,
        lookback_days: int = 504,
    ) -> pd.Series | None:
        """Retrieve a single feature's time series from the PIT store.

        Parameters:
            feature_name: Feature name in the registry.
            as_of_date: Decision date for PIT filtering.
            lookback_days: How many calendar days back to fetch.

        Returns:
            pd.Series: Time series indexed by obs_date, or None if not found.
        """
        fid = self._get_feature_id_by_name(feature_name)
        if fid is None:
            log.warning("Feature '{n}' not found in registry", n=feature_name)
            return None

        start = as_of_date - timedelta(days=lookback_days)
        df = self.pit_store.get_pit([fid], as_of_date)
        if df.empty:
            return None

        df = df[df["obs_date"] >= start].sort_values("obs_date")
        series = df.set_index("obs_date")["value"]
        series.name = feature_name
        return series

    def compute_feature(
        self,
        feature_name: str,
        as_of_date: date,
        lookback_days: int = 504,
    ) -> float | None:
        """Compute a single feature value as of the given date.

        Retrieves the feature's transformation rule from the registry and
        applies it using the PIT store data.

        Parameters:
            feature_name: Name of the feature to compute.
            as_of_date: Decision date.
            lookback_days: Number of calendar days of history to use.

        Returns:
            float: The computed feature value, or None if insufficient data.
        """
        log.debug("Computing feature '{f}' as_of={d}", f=feature_name, d=as_of_date)

        series = self._get_pit_series(feature_name, as_of_date, lookback_days)
        if series is None or series.empty:
            log.warning("Insufficient data for feature '{f}'", f=feature_name)
            return None

        # Look up normalization method
        with self.engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT normalization, lag_days FROM feature_registry "
                    "WHERE name = :name"
                ),
                {"name": feature_name},
            ).fetchone()

        if row is None:
            return series.iloc[-1] if len(series) > 0 else None

        normalization = row[0]
        lag_days = row[1]

        # Apply lag if specified
        if lag_days > 0:
            if len(series) <= lag_days:
                return None
            series = series.diff(lag_days)

        # Apply normalization
        if normalization == "ZSCORE":
            result = zscore_normalize(series)
        elif normalization == "RAW":
            result = series
        elif normalization == "RANK":
            result = series.rank(pct=True)
        elif normalization == "MINMAX":
            s_min = series.min()
            s_max = series.max()
            if s_max != s_min:
                result = (series - s_min) / (s_max - s_min)
            else:
                result = series * 0.0
        else:
            result = series

        last_val = result.dropna().iloc[-1] if not result.dropna().empty else None
        return float(last_val) if last_val is not None else None

    def compute_derived_features(self, as_of_date: date) -> dict[str, float | None]:
        """Compute all derived features that combine multiple raw series.

        Parameters:
            as_of_date: Decision date for all computations.

        Returns:
            dict: Mapping of feature_name -> computed value (or None).
        """
        log.info("Computing derived features as_of={d}", d=as_of_date)
        results: dict[str, float | None] = {}

        # yld_curve_2s10s: direct from FRED (no derived computation needed)
        results["yld_curve_2s10s"] = self.compute_feature("yld_curve_2s10s", as_of_date)

        # fed_funds_3m_chg: rolling 63-day change of DFF
        dff = self._get_pit_series("fed_funds_rate", as_of_date)
        if dff is not None and len(dff) > 63:
            chg = dff.diff(63)
            val = zscore_normalize(chg).dropna()
            results["fed_funds_3m_chg"] = float(val.iloc[-1]) if not val.empty else None
        else:
            results["fed_funds_3m_chg"] = None

        # hy_spread_3m_chg: rolling 63-day change of hy_spread_proxy
        hy = self._get_pit_series("hy_spread_proxy", as_of_date)
        if hy is not None and len(hy) > 63:
            chg = hy.diff(63)
            val = zscore_normalize(chg).dropna()
            results["hy_spread_3m_chg"] = float(val.iloc[-1]) if not val.empty else None
        else:
            results["hy_spread_3m_chg"] = None

        # copper_gold_ratio: HG=F close / GC=F close
        copper = self._get_pit_series("copper_futures_close", as_of_date)
        gold = self._get_pit_series("gold_futures_close", as_of_date)
        if copper is not None and gold is not None:
            cg_ratio = ratio(copper, gold)
            val = zscore_normalize(cg_ratio).dropna()
            results["copper_gold_ratio"] = float(val.iloc[-1]) if not val.empty else None

            # copper_gold_slope: rolling_slope(copper_gold_ratio, 63)
            if len(cg_ratio) > 63:
                slope_series = rolling_slope(cg_ratio, 63).dropna()
                results["copper_gold_slope"] = (
                    float(slope_series.iloc[-1]) if not slope_series.empty else None
                )
            else:
                results["copper_gold_slope"] = None
        else:
            results["copper_gold_ratio"] = None
            results["copper_gold_slope"] = None

        # sp500_mom_12_1: (close[t-21] / close[t-252]) - 1
        sp500 = self._get_pit_series("sp500_close", as_of_date, lookback_days=600)
        if sp500 is not None and len(sp500) > 252:
            mom = (sp500.shift(21) / sp500.shift(252)) - 1
            val = zscore_normalize(mom).dropna()
            results["sp500_mom_12_1"] = float(val.iloc[-1]) if not val.empty else None
        else:
            results["sp500_mom_12_1"] = None

        # sp500_mom_3m: (close[t] / close[t-63]) - 1
        if sp500 is not None and len(sp500) > 63:
            mom3 = (sp500 / sp500.shift(63)) - 1
            val = zscore_normalize(mom3).dropna()
            results["sp500_mom_3m"] = float(val.iloc[-1]) if not val.empty else None
        else:
            results["sp500_mom_3m"] = None

        # real_ffr: DFF minus CPI_YOY
        cpi = self._get_pit_series("cpi_yoy", as_of_date)
        if dff is not None and cpi is not None:
            # Align on common dates
            common = dff.index.intersection(cpi.index)
            if len(common) > 0:
                real = spread(dff.loc[common], cpi.loc[common])
                val = zscore_normalize(real).dropna()
                results["real_ffr"] = float(val.iloc[-1]) if not val.empty else None
            else:
                results["real_ffr"] = None
        else:
            results["real_ffr"] = None

        # vix_3m_ratio: VIX / VIX3M
        vix = self._get_pit_series("vix_spot", as_of_date)
        vix3m = self._get_pit_series("vix_3m_ratio", as_of_date)
        if vix is not None and vix3m is not None:
            common = vix.index.intersection(vix3m.index)
            if len(common) > 0:
                vr = ratio(vix.loc[common], vix3m.loc[common])
                val = zscore_normalize(vr).dropna()
                results["vix_3m_ratio"] = float(val.iloc[-1]) if not val.empty else None
            else:
                results["vix_3m_ratio"] = None
        else:
            results["vix_3m_ratio"] = None

        # ---------------------------------------------------------
        # Physics-derived features
        # ---------------------------------------------------------

        # sp500_kinetic_energy: momentum energy (½v²)
        if sp500 is not None and len(sp500) > 21:
            from physics.transforms import kinetic_energy
            ke = kinetic_energy(sp500, window=21).dropna()
            results["sp500_kinetic_energy"] = float(ke.iloc[-1]) if not ke.empty else None
        else:
            results["sp500_kinetic_energy"] = None

        # sp500_potential_energy: distance from equilibrium
        if sp500 is not None and len(sp500) > 252:
            from physics.transforms import potential_energy
            pe = potential_energy(sp500, window=252).dropna()
            results["sp500_potential_energy"] = float(pe.iloc[-1]) if not pe.empty else None
        else:
            results["sp500_potential_energy"] = None

        # market_temperature: realized variance as temperature
        if sp500 is not None and len(sp500) > 63:
            from physics.transforms import market_temperature
            log_ret = np.log(sp500 / sp500.shift(1)).dropna()
            if len(log_ret) > 63:
                temp = market_temperature(log_ret, window=63).dropna()
                results["market_temperature"] = float(temp.iloc[-1]) if not temp.empty else None
            else:
                results["market_temperature"] = None
        else:
            results["market_temperature"] = None

        # sp500_ou_theta: Ornstein-Uhlenbeck mean-reversion speed
        if sp500 is not None and len(sp500) > 252:
            from physics.transforms import estimate_ou_parameters
            ou_params = estimate_ou_parameters(sp500)
            results["sp500_ou_theta"] = ou_params["theta"]
            results["sp500_ou_half_life"] = ou_params["half_life_days"]
        else:
            results["sp500_ou_theta"] = None
            results["sp500_ou_half_life"] = None

        # sp500_hurst: persistence/anti-persistence measure
        if sp500 is not None and len(sp500) > 252:
            from physics.transforms import hurst_exponent
            log_ret = np.log(sp500 / sp500.shift(1)).dropna()
            h = hurst_exponent(log_ret)
            results["sp500_hurst"] = round(float(h), 4) if not np.isnan(h) else None
        else:
            results["sp500_hurst"] = None

        # ---------------------------------------------------------
        # Options-derived features
        # ---------------------------------------------------------

        # SPY aggregate put/call ratio z-score
        spy_pcr = self._get_pit_series("spy_pcr", as_of_date, lookback_days=252)
        if spy_pcr is not None and len(spy_pcr) > 20:
            val = zscore_normalize(spy_pcr, window=63).dropna()
            results["spy_pcr_zscore"] = float(val.iloc[-1]) if not val.empty else None
        else:
            results["spy_pcr_zscore"] = None

        # SPY IV skew (OTM/ATM) z-score — measures tail risk pricing
        spy_skew = self._get_pit_series("spy_iv_skew", as_of_date, lookback_days=252)
        if spy_skew is not None and len(spy_skew) > 20:
            val = zscore_normalize(spy_skew, window=63).dropna()
            results["spy_iv_skew_zscore"] = float(val.iloc[-1]) if not val.empty else None
        else:
            results["spy_iv_skew_zscore"] = None

        # SPY IV ATM level (raw for regime detection)
        spy_iv = self._get_pit_series("spy_iv_atm", as_of_date, lookback_days=252)
        if spy_iv is not None and len(spy_iv) > 20:
            val = zscore_normalize(spy_iv, window=63).dropna()
            results["spy_iv_atm_zscore"] = float(val.iloc[-1]) if not val.empty else None
            # IV term structure slope z-score
            ts_slope = self._get_pit_series("spy_term_slope", as_of_date, lookback_days=252)
            if ts_slope is not None and len(ts_slope) > 20:
                ts_val = zscore_normalize(ts_slope, window=63).dropna()
                results["spy_term_slope_zscore"] = float(ts_val.iloc[-1]) if not ts_val.empty else None
            else:
                results["spy_term_slope_zscore"] = None
        else:
            results["spy_iv_atm_zscore"] = None
            results["spy_term_slope_zscore"] = None

        # SPY max pain divergence from spot (% distance)
        spy_mp = self._get_pit_series("spy_max_pain", as_of_date, lookback_days=252)
        if sp500 is not None and spy_mp is not None:
            common = sp500.index.intersection(spy_mp.index)
            if len(common) > 0:
                div = (sp500.loc[common] - spy_mp.loc[common]) / sp500.loc[common] * 100
                val = zscore_normalize(div, window=63).dropna()
                results["spy_max_pain_div_zscore"] = float(val.iloc[-1]) if not val.empty else None
            else:
                results["spy_max_pain_div_zscore"] = None
        else:
            results["spy_max_pain_div_zscore"] = None

        log.info(
            "Derived features computed — {n}/{t} non-null",
            n=sum(1 for v in results.values() if v is not None),
            t=len(results),
        )
        return results


    # ------------------------------------------------------------------
    # tsfresh automated feature extraction
    # ------------------------------------------------------------------

    def run_tsfresh_extraction(
        self,
        series_id: str,
        as_of_date: date,
        lookback_days: int = 504,
        register: bool = True,
    ) -> dict[str, float]:
        """Run tsfresh feature extraction on a raw series.

        Extracts a comprehensive set of time-series features (mean, std,
        entropy, autocorrelation, etc.) using tsfresh's efficient defaults.
        Results are stored with prefix ``TSFRESH:`` in the feature registry.

        Parameters:
            series_id: Name of the raw series / feature to extract from.
            as_of_date: Decision date for PIT-correct data retrieval.
            lookback_days: Calendar days of history to feed into tsfresh.
            register: If True, auto-register extracted features in
                      ``feature_registry`` with family ``tsfresh``.

        Returns:
            dict: Mapping of ``TSFRESH:{series_id}:{feature_name}`` to
                  extracted float values.  Empty dict on failure.
        """
        try:
            from tsfresh import extract_features
            from tsfresh.utilities.dataframe_functions import impute
        except ImportError:
            log.warning(
                "tsfresh not installed — skipping extraction for {s}",
                s=series_id,
            )
            return {}

        log.info(
            "Running tsfresh extraction on {s} (lookback={d}d)",
            s=series_id, d=lookback_days,
        )

        # Retrieve the time series via PIT store
        pit_series = self._get_pit_series(series_id, as_of_date, lookback_days)
        if pit_series is None or len(pit_series) < 10:
            log.warning(
                "Insufficient data for tsfresh on {s} ({n} points)",
                s=series_id,
                n=0 if pit_series is None else len(pit_series),
            )
            return {}

        # tsfresh expects a DataFrame with columns: id, time, value
        ts_df = pd.DataFrame({
            "id": 1,
            "time": range(len(pit_series)),
            "value": pit_series.values,
        })

        try:
            extracted = extract_features(
                ts_df,
                column_id="id",
                column_sort="time",
                column_value="value",
                disable_progressbar=True,
                n_jobs=1,
            )
            # Impute NaN/inf values
            impute(extracted)
        except Exception as exc:
            log.error(
                "tsfresh extraction failed for {s}: {e}",
                s=series_id, e=str(exc),
            )
            return {}

        if extracted.empty:
            return {}

        # Flatten the single-row result into a dict with TSFRESH: prefix
        results: dict[str, float] = {}
        for col in extracted.columns:
            val = extracted[col].iloc[0]
            if pd.isna(val):
                continue
            feature_name = f"TSFRESH:{series_id}:{col}"
            results[feature_name] = float(val)

        log.info(
            "tsfresh extracted {n} features for {s}",
            n=len(results), s=series_id,
        )

        # Optionally register in feature_registry and store values
        if register and results:
            self._register_tsfresh_features(series_id, results, as_of_date)

        return results

    def _register_tsfresh_features(
        self,
        source_series: str,
        features: dict[str, float],
        as_of_date: date,
    ) -> None:
        """Register tsfresh features in the feature registry and store values.

        Parameters:
            source_series: Original series name the features were extracted from.
            features: Mapping of TSFRESH:* feature names to values.
            as_of_date: Date the features were computed for.
        """
        with self.engine.begin() as conn:
            for feature_name, value in features.items():
                # Check if already registered
                existing = conn.execute(
                    text("SELECT id FROM feature_registry WHERE name = :name"),
                    {"name": feature_name},
                ).fetchone()

                if existing is None:
                    conn.execute(
                        text(
                            "INSERT INTO feature_registry "
                            "(name, family, source_series_id, normalization, "
                            "lag_days, model_eligible) "
                            "VALUES (:name, :family, :ssid, :norm, :lag, :elig)"
                        ),
                        {
                            "name": feature_name,
                            "family": "tsfresh",
                            "ssid": source_series,
                            "norm": "RAW",
                            "lag": 0,
                            "elig": False,  # Default off; operator enables selectively
                        },
                    )

    def run_tsfresh_batch(
        self,
        as_of_date: date,
        series_names: list[str] | None = None,
        lookback_days: int = 504,
    ) -> dict[str, dict[str, float]]:
        """Run tsfresh extraction on multiple series.

        Parameters:
            as_of_date: Decision date for PIT-correct data retrieval.
            series_names: List of feature/series names to extract from.
                          If None, uses all model-eligible features.
            lookback_days: Calendar days of history per series.

        Returns:
            dict: Outer key is series name, inner dict is extracted features.
        """
        if series_names is None:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT name FROM feature_registry "
                        "WHERE model_eligible = TRUE AND family != 'tsfresh' "
                        "ORDER BY name"
                    )
                ).fetchall()
                series_names = [r[0] for r in rows]

        log.info(
            "tsfresh batch extraction — {n} series, as_of={d}",
            n=len(series_names), d=as_of_date,
        )

        all_results: dict[str, dict[str, float]] = {}
        for name in series_names:
            result = self.run_tsfresh_extraction(
                name, as_of_date, lookback_days, register=True
            )
            if result:
                all_results[name] = result

        total_features = sum(len(v) for v in all_results.values())
        log.info(
            "tsfresh batch complete — {n} features from {s} series",
            n=total_features, s=len(all_results),
        )
        return all_results


# ---------------------------------------------------------------------------
# Per-sector percentile ranking (relocated from intelligence/ratio_percentiles.py
# on 2026-04-11 — SYNTH-12 / Wave 3 dedupe).
#
# Percentile ranking is a feature-engineering primitive, not an intelligence
# signal. The capital_flow endpoint calls compute_all_percentiles / get_percentile
# to attach sector-relative percentile badges to each ratio in the response.
#
# Ratio semantics — higher-is-better vs lower-is-better:
# Some ratios are "the bigger the better" (gross_margin, shareholder_yield)
# and some are "the smaller the better" (opex_intensity, acquisition_intensity).
# _HIGHER_IS_BETTER flips the polarity so P100 is always "elite" and P0 is
# always "worst" regardless of direction. Delta ratios and buyback_yield are
# handled as higher-is-better (expanding margins + bigger buybacks rewarded).
# ---------------------------------------------------------------------------

# All ratio names produced by api/routers/capital_flow._compute_ratios.
# Keep this list in sync with that function.
RATIO_NAMES: tuple[str, ...] = (
    "gross_margin",
    "opex_intensity",
    "capex_intensity",
    "r_and_d_intensity",
    "dividend_payout",
    "buyback_yield",
    "fcf_conversion",
    "revenue_per_dollar_capex",
    "shareholder_yield",
    "reinvestment_ratio",
    "net_debt_issuance_intensity",
    "acquisition_intensity",
    "delta_gross_margin_bp",
    "delta_opex_intensity_bp",
)

# True = bigger value is "better" for ranking purposes.
# Inverted so that P100 always means "best in sector".
_HIGHER_IS_BETTER: dict[str, bool] = {
    "gross_margin": True,
    "opex_intensity": False,
    "capex_intensity": False,  # lower capex intensity = more asset-light
    "r_and_d_intensity": True,
    "dividend_payout": True,
    "buyback_yield": True,
    "fcf_conversion": True,
    "revenue_per_dollar_capex": True,
    "shareholder_yield": True,
    "reinvestment_ratio": True,
    "net_debt_issuance_intensity": False,
    "acquisition_intensity": False,
    "delta_gross_margin_bp": True,
    "delta_opex_intensity_bp": False,
}


# ─── Cache ─────────────────────────────────────────────────────────────

_PCT_CACHE: TTLCache = TTLCache(ttl=3600.0, max_size=16)
_ALL_PCT_KEY_PREFIX = "all_percentiles"


def clear_cache() -> None:
    """Drop all cached percentile snapshots. Tests and admin ops only."""
    _PCT_CACHE.clear()


# ─── Sector map helpers ────────────────────────────────────────────────


def _ticker_to_sector_subsector() -> dict[str, tuple[str, str]]:
    """Flatten ``SECTOR_MAP`` to ``{ticker_upper: (sector, subsector)}``.

    Non-ticker actors (ids without a ticker field) are skipped. Tickers are
    normalized to uppercase to match the canonical form used throughout the
    rest of the capital_flow pipeline.
    """
    try:
        from analysis.sector_map import SECTOR_MAP
    except Exception as exc:
        log.warning("features.lab: sector_map import failed: {e}", e=exc)
        return {}

    out: dict[str, tuple[str, str]] = {}
    for sector_name, sector in SECTOR_MAP.items():
        if not isinstance(sector, dict):
            continue
        for sub_name, sub in (sector.get("subsectors") or {}).items():
            if not isinstance(sub, dict):
                continue
            for actor in sub.get("actors") or []:
                tk = (actor.get("ticker") or "").strip().upper()
                if not tk:
                    continue
                out[tk] = (sector_name, sub_name)
    return out


# ─── Data loading ──────────────────────────────────────────────────────

# The per-period ratios are not persisted; we recompute them from the latest
# ``capital_flows`` rows for every ticker. Dedup rules match the ones in
# capital_flow.py so the numbers line up exactly.
_LATEST_PERIOD_SQL = """
WITH latest AS (
    SELECT actor_id, MAX(fiscal_period) AS fp
    FROM capital_flows
    WHERE period_type = :pt
      AND fiscal_period IS NOT NULL
    GROUP BY actor_id
),
ranked AS (
    SELECT
        cf.actor_id,
        cf.fiscal_period,
        cf.flow_type,
        cf.direction,
        cf.amount_usd,
        ROW_NUMBER() OVER (
            PARTITION BY cf.actor_id, cf.fiscal_period, cf.period_type,
                         cf.flow_type, cf.direction,
                         COALESCE(NULLIF(cf.counterparty_id, ''), '__none__')
            ORDER BY
                CASE
                    WHEN cf.source_filing LIKE '10-%' THEN 1
                    WHEN cf.source_filing LIKE '20-%' THEN 2
                    WHEN cf.source_filing LIKE '8-%'  THEN 3
                    WHEN cf.source_filing LIKE 'seed%' THEN 5
                    ELSE 4
                END,
                CASE cf.confidence
                    WHEN 'confirmed' THEN 1
                    WHEN 'derived'   THEN 2
                    WHEN 'estimated' THEN 3
                    WHEN 'rumored'   THEN 4
                    WHEN 'inferred'  THEN 5
                    ELSE 6
                END,
                cf.as_of DESC NULLS LAST
        ) AS rk
    FROM capital_flows cf
    JOIN latest l
      ON l.actor_id = cf.actor_id
     AND l.fp       = cf.fiscal_period
    WHERE cf.period_type = :pt
)
SELECT actor_id, flow_type, amount_usd
FROM ranked
WHERE rk = 1
"""


def _table_exists(engine: Any, name: str) -> bool:
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT to_regclass(:n)").bindparams(n=name)
            ).fetchone()
            return bool(row and row[0])
    except Exception:
        return False


def _load_latest_amounts(engine: Any, period_type: str) -> dict[str, dict[str, float]]:
    """Return ``{ticker_upper: {flow_type: summed_amount_usd}}`` for the
    latest fiscal_period per actor at the given ``period_type``.

    Uppercases actor_id so it joins cleanly with the sector_map flatten.
    """
    if not _table_exists(engine, "public.capital_flows"):
        return {}

    amounts: dict[str, dict[str, float]] = {}
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(_LATEST_PERIOD_SQL).bindparams(pt=period_type),
            ).fetchall()
    except Exception as exc:
        log.debug("features.lab: _load_latest_amounts failed: {e}", e=exc)
        return {}

    for actor_id, flow_type, amount in rows:
        if actor_id is None or flow_type is None or amount is None:
            continue
        tk = str(actor_id).strip().upper()
        if not tk:
            continue
        try:
            amt = float(amount)
        except (TypeError, ValueError):
            continue
        bucket = amounts.setdefault(tk, {})
        bucket[flow_type] = bucket.get(flow_type, 0.0) + amt
    return amounts


# ─── Ratio derivation (mirror of capital_flow._compute_ratios minus mcap) ──


def _safe_div(num: float | None, den: float | None) -> float | None:
    if num is None or den is None:
        return None
    try:
        d = float(den)
        return None if d == 0.0 else float(num) / d
    except (TypeError, ValueError):
        return None


def _compute_ratios_from_amounts(amounts: dict[str, float]) -> dict[str, float | None]:
    """Mirror of capital_flow._compute_ratios sans market_cap + delta fields.

    Delta fields require prior-period context which we don't reconstruct
    here; for percentile purposes we only use the single latest period.
    """
    rev = amounts.get("revenue")
    cogs = amounts.get("cogs")
    opex = amounts.get("opex")
    rnd = amounts.get("r_and_d")
    capex = amounts.get("capex")
    tax = amounts.get("tax")
    div = amounts.get("dividends")
    buy = amounts.get("buybacks")
    debt_iss = amounts.get("debt_issuance")
    acq = amounts.get("acquisitions")

    gross_margin = (
        _safe_div(rev - cogs, rev) if rev is not None and cogs is not None else None
    )

    dividend_payout: float | None = None
    if div is not None and rev is not None and cogs is not None:
        ni_proxy = rev - cogs - (opex or 0.0) - (tax or 0.0)
        dividend_payout = _safe_div(div, ni_proxy)

    fcf_conv: float | None = None
    if rev is not None and cogs is not None:
        fcf_conv = _safe_div(rev - cogs - (opex or 0.0) - (capex or 0.0), rev)

    rev_per_capex: float | None = None
    if rev is not None and capex is not None:
        rev_per_capex = _safe_div(rev, capex)

    shareholder_yield: float | None = None
    if rev is not None and (div is not None or buy is not None):
        shareholder_yield = _safe_div((div or 0.0) + (buy or 0.0), rev)

    reinvestment_ratio: float | None = None
    if rev is not None and (capex is not None or rnd is not None):
        reinvestment_ratio = _safe_div((capex or 0.0) + (rnd or 0.0), rev)

    # Percentile-time buyback_yield uses buybacks/revenue fallback only
    # (no market cap context here). capital_flow.py still prefers mcap at
    # request time — this is just a best-effort peer ranking.
    buyback_yield: float | None = None
    if buy is not None and rev is not None:
        buyback_yield = _safe_div(buy, rev)

    return {
        "gross_margin": gross_margin,
        "opex_intensity": _safe_div(opex, rev),
        "capex_intensity": _safe_div(capex, rev),
        "r_and_d_intensity": _safe_div(rnd, rev) if rnd is not None else None,
        "dividend_payout": dividend_payout,
        "buyback_yield": buyback_yield,
        "fcf_conversion": fcf_conv,
        "revenue_per_dollar_capex": rev_per_capex,
        "shareholder_yield": shareholder_yield,
        "reinvestment_ratio": reinvestment_ratio,
        "net_debt_issuance_intensity": (
            _safe_div(debt_iss, rev) if debt_iss is not None else None
        ),
        "acquisition_intensity": (
            _safe_div(acq, rev) if acq is not None else None
        ),
        # Delta ratios can't be derived from one period — always None here.
        "delta_gross_margin_bp": None,
        "delta_opex_intensity_bp": None,
    }


# ─── Percentile math ───────────────────────────────────────────────────


def _percentile_rank(
    values: list[tuple[str, float]],
    higher_is_better: bool,
) -> dict[str, float]:
    """Rank ticker values inside a single sector bucket on a 0-100 scale.

    Tickers are ordered by value (ascending for higher_is_better=True so
    the biggest number wins the top slot, descending otherwise), then
    mapped linearly to 0..100. With a single ticker we return 50 (no peers
    to compare with). Ties share the average rank to avoid arbitrary
    ordering drift between cache refreshes.
    """
    n = len(values)
    if n == 0:
        return {}
    if n == 1:
        return {values[0][0]: 50.0}

    # Sort so index 0 is "worst" — then percentile = index / (n-1) * 100.
    ordered = sorted(values, key=lambda kv: kv[1], reverse=not higher_is_better)

    # Resolve ties via average rank.
    percentiles: dict[str, float] = {}
    i = 0
    while i < n:
        j = i
        while j + 1 < n and ordered[j + 1][1] == ordered[i][1]:
            j += 1
        avg_idx = (i + j) / 2.0
        pct = round((avg_idx / (n - 1)) * 100.0, 1)
        for k in range(i, j + 1):
            percentiles[ordered[k][0]] = pct
        i = j + 1
    return percentiles


def _sector_stats(
    values: list[float],
    higher_is_better: bool,
) -> tuple[float | None, float | None]:
    """Return ``(median, top_decile_threshold)`` for a sector bucket.

    "Top decile" always means the elite side — so for lower-is-better
    ratios it's the 10th percentile of raw values, not the 90th.
    """
    if not values:
        return (None, None)
    s = sorted(values)
    n = len(s)
    med = s[n // 2] if n % 2 == 1 else (s[n // 2 - 1] + s[n // 2]) / 2.0
    if n == 1:
        return (med, s[0])
    if higher_is_better:
        idx = max(0, int(round(0.9 * (n - 1))))
    else:
        idx = max(0, int(round(0.1 * (n - 1))))
    return (med, s[idx])


# ─── Public API ────────────────────────────────────────────────────────


def compute_sector_percentiles(
    engine: Any,
    ratio_name: str,
    period_type: str = "annual",
) -> dict[str, dict[str, float]]:
    """Compute per-sector percentiles for a single ratio.

    Returns ``{sector_name: {ticker: percentile_0_to_100}}``.

    Unknown ratio names return ``{}``. Tickers with no sector mapping are
    excluded from ranking (they're not comparable to anything). Tickers
    whose latest capital_flow row yields ``None`` for this ratio are
    excluded as well — that's the contract: "missing ratios → no rank".
    """
    if ratio_name not in _HIGHER_IS_BETTER:
        log.warning("features.lab: unknown ratio {r}", r=ratio_name)
        return {}
    higher = _HIGHER_IS_BETTER[ratio_name]
    sector_map = _ticker_to_sector_subsector()
    if not sector_map:
        return {}

    amounts_by_ticker = _load_latest_amounts(engine, period_type)
    if not amounts_by_ticker:
        return {}

    # Bucket tickers by sector → list[(ticker, ratio_value)]
    buckets: dict[str, list[tuple[str, float]]] = {}
    for ticker, amounts in amounts_by_ticker.items():
        if ticker not in sector_map:
            continue
        sector_name, _sub = sector_map[ticker]
        ratios = _compute_ratios_from_amounts(amounts)
        v = ratios.get(ratio_name)
        if v is None:
            continue
        try:
            fv = float(v)
        except (TypeError, ValueError):
            continue
        buckets.setdefault(sector_name, []).append((ticker, fv))

    return {
        sector: _percentile_rank(vals, higher)
        for sector, vals in buckets.items()
    }


def compute_all_percentiles(
    engine: Any,
    period_type: str = "annual",
) -> dict[str, dict[str, dict[str, float]]]:
    """Compute percentiles for every ratio in ``RATIO_NAMES``.

    Returns ``{ratio_name: {sector_name: {ticker: percentile}}}``. Cached
    for 1 hour keyed on ``period_type`` — subsequent calls within the
    cache window are O(1).
    """
    cache_key = f"{_ALL_PCT_KEY_PREFIX}|{period_type}"
    cached = _PCT_CACHE.get(cache_key)
    if cached is not None:
        return cached

    # Single DB scan, reused for every ratio — this is the hot-path win.
    sector_map = _ticker_to_sector_subsector()
    amounts_by_ticker = _load_latest_amounts(engine, period_type)

    out: dict[str, dict[str, dict[str, float]]] = {}
    if not sector_map or not amounts_by_ticker:
        _PCT_CACHE.set(cache_key, out)
        return out

    # Pre-compute ratios once per ticker so we're not dividing 14 times.
    ratios_by_ticker: dict[str, dict[str, float | None]] = {
        tk: _compute_ratios_from_amounts(amts)
        for tk, amts in amounts_by_ticker.items()
    }

    for ratio_name in RATIO_NAMES:
        higher = _HIGHER_IS_BETTER[ratio_name]
        buckets: dict[str, list[tuple[str, float]]] = {}
        for ticker, ratios in ratios_by_ticker.items():
            if ticker not in sector_map:
                continue
            sector_name, _sub = sector_map[ticker]
            v = ratios.get(ratio_name)
            if v is None:
                continue
            try:
                fv = float(v)
            except (TypeError, ValueError):
                continue
            buckets.setdefault(sector_name, []).append((ticker, fv))
        out[ratio_name] = {
            sector: _percentile_rank(vals, higher)
            for sector, vals in buckets.items()
        }

    _PCT_CACHE.set(cache_key, out)
    return out


def get_percentile(
    engine: Any,
    ticker: str,
    ratio_name: str,
    period_type: str = "annual",
) -> dict[str, Any]:
    """Return a percentile + sector context block for one (ticker, ratio).

    Shape::

        {
            "percentile": 40.0,
            "sector": "Consumer Staples",
            "subsector": "Staples / Packaged Foods",
            "sector_median": 0.031,
            "sector_top_decile": 0.055,
        }

    Missing data → ``percentile`` is None and context fields may be None.
    """
    tk = (ticker or "").strip().upper()
    sector_map = _ticker_to_sector_subsector()
    sector_name: str | None = None
    sub_name: str | None = None
    if tk in sector_map:
        sector_name, sub_name = sector_map[tk]

    empty: dict[str, Any] = {
        "percentile": None,
        "sector": sector_name,
        "subsector": sub_name,
        "sector_median": None,
        "sector_top_decile": None,
    }
    if ratio_name not in _HIGHER_IS_BETTER or sector_name is None:
        return empty

    all_pct = compute_all_percentiles(engine, period_type)
    ratio_bucket = all_pct.get(ratio_name) or {}
    sector_bucket = ratio_bucket.get(sector_name) or {}
    pct = sector_bucket.get(tk)

    # For the sector_median + top_decile we need the raw values, not just
    # their ranks. Recompute from the cached amounts rather than hitting
    # the DB again. ``compute_all_percentiles`` warmed the cache already.
    amounts_by_ticker = _load_latest_amounts(engine, period_type)
    raw_values: list[float] = []
    for peer, amounts in amounts_by_ticker.items():
        if sector_map.get(peer, (None, None))[0] != sector_name:
            continue
        r = _compute_ratios_from_amounts(amounts)
        v = r.get(ratio_name)
        if v is None:
            continue
        try:
            raw_values.append(float(v))
        except (TypeError, ValueError):
            continue
    median, top_decile = _sector_stats(raw_values, _HIGHER_IS_BETTER[ratio_name])

    return {
        "percentile": pct,
        "sector": sector_name,
        "subsector": sub_name,
        "sector_median": median,
        "sector_top_decile": top_decile,
    }


if __name__ == "__main__":
    from db import get_engine

    engine = get_engine()
    pit = PITStore(engine)
    lab = FeatureLab(engine, pit)

    today = date.today()
    derived = lab.compute_derived_features(today)
    print("Derived features as of today:")
    for name, val in derived.items():
        print(f"  {name:25s}: {val}")
