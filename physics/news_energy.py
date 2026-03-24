"""
GRID physics — News Energy Decomposition Engine.

Connects live news/intelligence data from Crucix/GDELT sources to the physics
framework. Computes kinetic energy (rate-of-change), potential energy (deviation
from equilibrium), detects energy conservation violations (regime shifts), and
cross-correlates news energy with market feature energy.

Core outputs:
  - energy_by_source: KE, PE, total per news stream
  - total_news_energy: aggregate energy across all news sources
  - coherence: how aligned news sources are in direction (0..1)
  - force_vector: which sources inject the most energy into markets
  - regime_signal: energy conservation violation flag
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from store.pit import PITStore


# Key market features to cross-correlate with news energy
_MARKET_FEATURES = [
    "sp500",
    "vix",
    "treasury_10y",
    "treasury_2y",
    "dxy_index",
]

# Crucix/GDELT feature prefixes
_NEWS_PREFIXES = ("crucix_", "gdelt_")


class NewsEnergyEngine:
    """Decomposes news/intelligence streams into physics energy components.

    Reads all crucix_* and gdelt_* features from the PIT store, computes
    kinetic and potential energy per source, detects regime shifts via
    energy conservation violations, and builds a force vector showing which
    news streams inject the most energy into markets.

    Attributes:
        engine: SQLAlchemy engine for database access.
        pit_store: PITStore instance for point-in-time queries.
    """

    def __init__(self, db_engine: Engine, pit_store: PITStore) -> None:
        self.engine = db_engine
        self.pit_store = pit_store

    # ------------------------------------------------------------------
    # Feature discovery
    # ------------------------------------------------------------------

    def _discover_news_features(self) -> list[dict[str, Any]]:
        """Query feature_registry for all crucix_* and gdelt_* features.

        Returns:
            List of dicts with keys: id, name, source.
        """
        query = text(
            "SELECT id, name FROM feature_registry "
            "WHERE name LIKE :prefix_crucix OR name LIKE :prefix_gdelt "
            "ORDER BY name"
        )
        with self.engine.connect() as conn:
            rows = conn.execute(
                query, {"prefix_crucix": "crucix_%", "prefix_gdelt": "gdelt_%"}
            ).fetchall()

        features = []
        for row in rows:
            fid, name = row[0], row[1]
            source = "crucix" if name.startswith("crucix_") else "gdelt"
            features.append({"id": fid, "name": name, "source": source})

        log.info(
            "Discovered {n} news features (crucix + gdelt)", n=len(features)
        )
        return features

    def _discover_market_features(self) -> list[dict[str, Any]]:
        """Query feature_registry for key market features.

        Returns:
            List of dicts with keys: id, name.
        """
        if not _MARKET_FEATURES:
            return []

        query = text(
            "SELECT id, name FROM feature_registry WHERE name = ANY(:names)"
        )
        with self.engine.connect() as conn:
            rows = conn.execute(
                query, {"names": list(_MARKET_FEATURES)}
            ).fetchall()

        return [{"id": row[0], "name": row[1]} for row in rows]

    # ------------------------------------------------------------------
    # PIT data loading
    # ------------------------------------------------------------------

    def _load_series(
        self,
        feature_ids: list[int],
        as_of_date: date,
        lookback_days: int,
    ) -> pd.DataFrame:
        """Load PIT-correct feature matrix for the given IDs.

        Parameters:
            feature_ids: Feature registry IDs to load.
            as_of_date: Decision date for PIT filtering.
            lookback_days: Calendar days of history to fetch.

        Returns:
            Wide DataFrame indexed by obs_date, columns = feature_id.
        """
        if not feature_ids:
            return pd.DataFrame()

        start_date = as_of_date - timedelta(days=lookback_days)
        matrix = self.pit_store.get_feature_matrix(
            feature_ids, start_date, as_of_date, as_of_date,
            vintage_policy="LATEST_AS_OF",
        )
        return matrix

    # ------------------------------------------------------------------
    # Energy computations
    # ------------------------------------------------------------------

    @staticmethod
    def _kinetic_energy(series: pd.Series, window: int = 5) -> pd.Series:
        """Kinetic energy = 0.5 * (rate of change)^2.

        Uses a short window appropriate for news data (daily frequency,
        fast-moving). Window of 5 = ~1 week of lookback.

        Parameters:
            series: News feature time series.
            window: Rolling window for rate-of-change.

        Returns:
            pd.Series of kinetic energy values.
        """
        diff = series.diff(window)
        vol = series.rolling(window=max(window * 2, 10), min_periods=window).std()
        vol = vol.replace(0, np.nan)
        # Normalize by volatility to make comparable across sources
        normalized_diff = diff / vol
        return 0.5 * normalized_diff ** 2

    @staticmethod
    def _potential_energy(
        series: pd.Series, window: int = 21
    ) -> pd.Series:
        """Potential energy = 0.5 * ((x - mu) / sigma)^2.

        Measures deviation from rolling equilibrium, normalized by noise.

        Parameters:
            series: News feature time series.
            window: Rolling window for equilibrium estimate.

        Returns:
            pd.Series of potential energy values.
        """
        mu = series.rolling(window=window, min_periods=window // 2).mean()
        sigma = series.rolling(window=window, min_periods=window // 2).std()
        sigma = sigma.replace(0, np.nan)
        displacement = (series - mu) / sigma
        return 0.5 * displacement ** 2

    # ------------------------------------------------------------------
    # Cross-correlation: news energy vs market energy
    # ------------------------------------------------------------------

    @staticmethod
    def _cross_correlate(
        news_energy: pd.Series,
        market_energy: pd.Series,
        max_lag: int = 10,
    ) -> dict[str, Any]:
        """Compute cross-correlation between news and market energy series.

        Parameters:
            news_energy: News source energy time series.
            market_energy: Market feature energy time series.
            max_lag: Maximum lag in observations.

        Returns:
            Dict with peak_correlation, optimal_lag, direction.
        """
        # Align on common dates
        combined = pd.concat(
            [news_energy.rename("news"), market_energy.rename("market")],
            axis=1,
        ).dropna()

        if len(combined) < 20:
            return {
                "peak_correlation": 0.0,
                "optimal_lag": 0,
                "direction": "insufficient_data",
            }

        n = news_energy.rename("n")
        m = market_energy.rename("m")

        best_corr = 0.0
        best_lag = 0

        for lag in range(-max_lag, max_lag + 1):
            if lag > 0:
                shifted_news = combined["news"].iloc[:-lag].values
                shifted_market = combined["market"].iloc[lag:].values
            elif lag < 0:
                shifted_news = combined["news"].iloc[-lag:].values
                shifted_market = combined["market"].iloc[:lag].values
            else:
                shifted_news = combined["news"].values
                shifted_market = combined["market"].values

            if len(shifted_news) < 10:
                continue

            corr = np.corrcoef(shifted_news, shifted_market)[0, 1]
            if not np.isnan(corr) and abs(corr) > abs(best_corr):
                best_corr = float(corr)
                best_lag = lag

        direction = "news_leads" if best_lag > 0 else (
            "market_leads" if best_lag < 0 else "synchronous"
        )
        return {
            "peak_correlation": round(best_corr, 4),
            "optimal_lag": best_lag,
            "direction": direction,
        }

    # ------------------------------------------------------------------
    # Coherence: multi-source narrative alignment
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_coherence(
        changes: dict[str, pd.Series],
    ) -> dict[str, Any]:
        """Compute narrative coherence across news sources.

        Coherence measures how aligned different news streams are in their
        direction of change. High coherence = all sources moving together.

        Parameters:
            changes: Dict of source_name -> rate-of-change series.

        Returns:
            Dict with coherence (0..1), dominant_direction, aligned_sources.
        """
        if not changes:
            return {"coherence": 0.0, "dominant_direction": "neutral",
                    "aligned_sources": [], "n_sources": 0}

        # Get the latest direction of each source
        latest_directions: dict[str, float] = {}
        for name, series in changes.items():
            valid = series.dropna()
            if len(valid) > 0:
                latest_directions[name] = float(valid.iloc[-1])

        if not latest_directions:
            return {"coherence": 0.0, "dominant_direction": "neutral",
                    "aligned_sources": [], "n_sources": 0}

        values = np.array(list(latest_directions.values()))
        n_positive = np.sum(values > 0)
        n_negative = np.sum(values < 0)
        n_total = len(values)

        # Coherence = fraction of sources aligned with the majority direction
        majority = max(n_positive, n_negative)
        coherence = float(majority / n_total) if n_total > 0 else 0.0

        dominant = "increasing" if n_positive > n_negative else (
            "decreasing" if n_negative > n_positive else "mixed"
        )

        threshold = 0.0
        if dominant == "increasing":
            aligned = [n for n, v in latest_directions.items() if v > threshold]
        elif dominant == "decreasing":
            aligned = [n for n, v in latest_directions.items() if v < -threshold]
        else:
            aligned = list(latest_directions.keys())

        return {
            "coherence": round(coherence, 3),
            "dominant_direction": dominant,
            "aligned_sources": aligned,
            "n_sources": n_total,
        }

    # ------------------------------------------------------------------
    # Main analysis
    # ------------------------------------------------------------------

    def analyze(
        self,
        as_of_date: date | None = None,
        lookback_days: int = 30,
    ) -> dict[str, Any]:
        """Run full news energy decomposition analysis.

        Parameters:
            as_of_date: Decision date (default: today).
            lookback_days: Days of history for analysis.

        Returns:
            Dict with energy_by_source, total_news_energy, coherence,
            force_vector, regime_signal, and summary.
        """
        if as_of_date is None:
            as_of_date = date.today()

        log.info(
            "Running news energy analysis as_of={d}, lookback={lb}",
            d=as_of_date, lb=lookback_days,
        )

        # 1. Discover available features
        news_features = self._discover_news_features()
        market_features = self._discover_market_features()

        if not news_features:
            log.warning("No crucix/gdelt features found in registry")
            return self._empty_result("No news features available")

        news_ids = [f["id"] for f in news_features]
        market_ids = [f["id"] for f in market_features]
        id_to_name = {f["id"]: f["name"] for f in news_features + market_features}

        # 2. Load PIT data
        # Use extra lookback for energy window warmup
        data_lookback = lookback_days + 60
        news_matrix = self._load_series(news_ids, as_of_date, data_lookback)
        market_matrix = self._load_series(market_ids, as_of_date, data_lookback)

        if news_matrix.empty:
            log.warning("No PIT data returned for news features")
            return self._empty_result("No news data available for the given period")

        # 3. Compute energy per news source
        energy_by_source: list[dict[str, Any]] = []
        all_total_energy = pd.Series(dtype=float)
        rate_of_change_by_source: dict[str, pd.Series] = {}

        for fid in news_ids:
            if fid not in news_matrix.columns:
                continue

            series = news_matrix[fid].dropna()
            if len(series) < 10:
                continue

            name = id_to_name.get(fid, str(fid))
            ke = self._kinetic_energy(series)
            pe = self._potential_energy(series)
            total = ke.add(pe, fill_value=0)

            # Latest values
            ke_latest = float(ke.dropna().iloc[-1]) if not ke.dropna().empty else 0.0
            pe_latest = float(pe.dropna().iloc[-1]) if not pe.dropna().empty else 0.0
            total_latest = ke_latest + pe_latest

            # Rate of change for coherence computation
            roc = series.diff(5)
            rate_of_change_by_source[name] = roc

            # Energy conservation check: is total energy stable or spiking?
            if len(total.dropna()) >= 10:
                recent_mean = float(total.dropna().iloc[-10:].mean())
                historical_mean = float(total.dropna().mean())
                conservation_ratio = (
                    recent_mean / historical_mean if historical_mean > 0 else 1.0
                )
            else:
                conservation_ratio = 1.0

            # Cross-correlation with market features
            market_correlations = {}
            if not market_matrix.empty:
                for mid in market_ids:
                    if mid not in market_matrix.columns:
                        continue
                    m_series = market_matrix[mid].dropna()
                    if len(m_series) < 10:
                        continue
                    m_energy = self._kinetic_energy(m_series)
                    xcorr = self._cross_correlate(total, m_energy)
                    market_name = id_to_name.get(mid, str(mid))
                    market_correlations[market_name] = xcorr

            # Classify energy level
            if total_latest > 3.0:
                level = "high"
            elif total_latest > 1.0:
                level = "building"
            else:
                level = "low"

            entry = {
                "feature": name,
                "kinetic_energy": round(ke_latest, 4),
                "potential_energy": round(pe_latest, 4),
                "total_energy": round(total_latest, 4),
                "energy_level": level,
                "conservation_ratio": round(conservation_ratio, 3),
                "market_correlations": market_correlations,
            }
            energy_by_source.append(entry)

            # Accumulate total
            all_total_energy = all_total_energy.add(total, fill_value=0)

        # 4. Total news energy
        if not all_total_energy.dropna().empty:
            total_news_energy = float(all_total_energy.dropna().iloc[-1])
        else:
            total_news_energy = 0.0

        # 5. Coherence
        coherence = self._compute_coherence(rate_of_change_by_source)

        # 6. Force vector: rank sources by energy injection (total energy)
        sorted_sources = sorted(
            energy_by_source, key=lambda x: x["total_energy"], reverse=True
        )
        force_vector = []
        for s in sorted_sources:
            # Determine direction from rate of change
            roc_series = rate_of_change_by_source.get(s["feature"])
            if roc_series is not None and not roc_series.dropna().empty:
                direction = float(roc_series.dropna().iloc[-1])
            else:
                direction = 0.0

            force_vector.append({
                "feature": s["feature"],
                "energy": s["total_energy"],
                "direction": round(direction, 4),
                "direction_label": (
                    "increasing" if direction > 0 else
                    "decreasing" if direction < 0 else "flat"
                ),
                "energy_level": s["energy_level"],
            })

        # 7. Regime signal: check for energy conservation violations
        conservation_violations = [
            s for s in energy_by_source
            if s["conservation_ratio"] > 2.0 or s["conservation_ratio"] < 0.3
        ]
        regime_signal = {
            "equilibrium": len(conservation_violations) == 0,
            "violations": len(conservation_violations),
            "violating_sources": [v["feature"] for v in conservation_violations],
            "interpretation": (
                "Market in equilibrium - news energy stable"
                if len(conservation_violations) == 0
                else f"{len(conservation_violations)} sources showing energy regime shift"
            ),
        }

        # 8. Summary
        summary = self._build_summary(
            energy_by_source, total_news_energy, coherence,
            force_vector, regime_signal,
        )

        return {
            "as_of_date": as_of_date.isoformat(),
            "lookback_days": lookback_days,
            "n_news_sources": len(energy_by_source),
            "n_market_features": len(market_ids),
            "energy_by_source": energy_by_source,
            "total_news_energy": round(total_news_energy, 4),
            "coherence": coherence,
            "force_vector": force_vector,
            "regime_signal": regime_signal,
            "summary": summary,
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _empty_result(reason: str) -> dict[str, Any]:
        """Return a structured empty result when analysis cannot proceed."""
        return {
            "as_of_date": date.today().isoformat(),
            "lookback_days": 0,
            "n_news_sources": 0,
            "n_market_features": 0,
            "energy_by_source": [],
            "total_news_energy": 0.0,
            "coherence": {"coherence": 0.0, "dominant_direction": "neutral",
                          "aligned_sources": [], "n_sources": 0},
            "force_vector": [],
            "regime_signal": {"equilibrium": True, "violations": 0,
                              "violating_sources": [],
                              "interpretation": reason},
            "summary": reason,
        }

    @staticmethod
    def _build_summary(
        energy_by_source: list[dict],
        total_energy: float,
        coherence: dict,
        force_vector: list[dict],
        regime_signal: dict,
    ) -> str:
        """Build a plain-English summary of the news energy state."""
        parts = []

        # Overall energy level
        if total_energy > 10.0:
            parts.append(
                f"News energy is ELEVATED at {total_energy:.1f} "
                "- multiple intelligence sources showing high activity."
            )
        elif total_energy > 3.0:
            parts.append(
                f"News energy is BUILDING at {total_energy:.1f} "
                "- attention warranted."
            )
        else:
            parts.append(
                f"News energy is LOW at {total_energy:.1f} "
                "- intelligence streams are quiet."
            )

        # Coherence
        coh = coherence.get("coherence", 0)
        if coh > 0.8:
            parts.append(
                f"Source coherence is HIGH ({coh:.0%}) - "
                f"narratives are strongly aligned ({coherence.get('dominant_direction', 'n/a')})."
            )
        elif coh > 0.6:
            parts.append(
                f"Source coherence is MODERATE ({coh:.0%}) - "
                "some narrative alignment emerging."
            )
        else:
            parts.append(
                f"Source coherence is LOW ({coh:.0%}) - "
                "news sources are sending mixed signals."
            )

        # Top energy source
        if force_vector:
            top = force_vector[0]
            parts.append(
                f"Highest energy source: {top['feature']} "
                f"({top['energy']:.2f}, {top['direction_label']})."
            )

        # Regime signal
        if not regime_signal.get("equilibrium", True):
            parts.append(
                f"REGIME SHIFT DETECTED: {regime_signal['violations']} sources "
                f"violating energy conservation "
                f"({', '.join(regime_signal['violating_sources'][:3])})."
            )

        return " ".join(parts)
