"""
GRID AutoBNN — Interpretable Signal Decomposition.

Google's AutoBNN replaces Gaussian processes with Bayesian neural networks
while retaining compositional kernel structure. It decomposes time series
into interpretable components (trend, seasonality, changepoint) with proper
uncertainty quantification.

Where TimesFM gives raw forecasting power, AutoBNN gives interpretability:
it tells you *what structural pattern* a signal follows and *when* it changes.

Features:
  - Structural decomposition: trend + seasonality + changepoint
  - Bayesian uncertainty with posterior sampling
  - Linear scaling in data points (vs cubic for GPs)
  - Automatic changepoint detection for regime transitions
  - GPU/TPU accelerated via JAX

Installation:
  pip install autobnn   # Requires JAX
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log


@dataclass(frozen=True)
class DecompositionResult:
    """Result of an AutoBNN signal decomposition.

    Attributes:
        series_id: Identifier for the decomposed series.
        analysis_date: Date the analysis was performed.
        trend: Extracted trend component values.
        seasonality: Extracted seasonal component values.
        residual: Unexplained residual component.
        changepoints: Detected structural changepoint indices and dates.
        kernel_description: Human-readable kernel structure found.
        posterior_std: Posterior standard deviation (uncertainty).
        model_evidence: Log marginal likelihood (model quality).
    """

    series_id: str
    analysis_date: date
    trend: list[float]
    seasonality: list[float]
    residual: list[float]
    changepoints: list[dict[str, Any]]
    kernel_description: str
    posterior_std: list[float]
    model_evidence: float


@dataclass(frozen=True)
class RegimeChangeSignal:
    """A detected regime change from AutoBNN changepoint analysis.

    Attributes:
        series_id: Which series the change was detected in.
        change_index: Index in the time series where change occurs.
        change_date: Date of the detected change.
        pre_regime: Description of the regime before the change.
        post_regime: Description of the regime after the change.
        confidence: Confidence in the changepoint (0-1).
        magnitude: Size of the structural break.
    """

    series_id: str
    change_index: int
    change_date: date | None
    pre_regime: str
    post_regime: str
    confidence: float
    magnitude: float


class AutoBNNDecomposer:
    """Wrapper around Google AutoBNN for interpretable signal decomposition.

    Lazily loads AutoBNN on first use. Provides structural decomposition
    of GRID signals into trend, seasonality, and changepoint components.

    Parameters:
        num_samples: Number of posterior samples for uncertainty.
        num_chains: Number of MCMC chains.
        seed: Random seed for reproducibility.
    """

    def __init__(
        self,
        num_samples: int = 200,
        num_chains: int = 2,
        seed: int = 42,
    ) -> None:
        self.num_samples = num_samples
        self.num_chains = num_chains
        self.seed = seed
        self._available: bool | None = None

    @property
    def is_available(self) -> bool:
        """Whether AutoBNN (and JAX) are importable."""
        if self._available is None:
            try:
                import jax  # noqa: F401
                import autobnn  # noqa: F401
                self._available = True
            except ImportError:
                log.warning(
                    "autobnn/jax not installed — "
                    "install with: pip install autobnn jax"
                )
                self._available = False
        return self._available

    def decompose(
        self,
        series: np.ndarray | pd.Series,
        dates: list[date] | None = None,
        series_id: str = "unknown",
    ) -> DecompositionResult:
        """Decompose a time series into structural components.

        Uses AutoBNN's compositional kernel search to find the best
        decomposition (trend + seasonality + changepoints).

        Parameters:
            series: Historical values as 1-D array.
            dates: Corresponding dates (for changepoint labeling).
            series_id: Identifier for logging.

        Returns:
            DecompositionResult with components and changepoints.

        Raises:
            RuntimeError: If AutoBNN is not installed.
        """
        if not self.is_available:
            raise RuntimeError(
                "AutoBNN not available — install with: pip install autobnn jax"
            )

        if isinstance(series, pd.Series):
            series = series.values

        series = series.astype(np.float64)
        n = len(series)

        log.info(
            "AutoBNN decompose — series={s}, len={n}",
            s=series_id,
            n=n,
        )

        import jax
        import jax.numpy as jnp
        import autobnn

        # Normalise time axis to [0, 1]
        x = np.linspace(0.0, 1.0, n).reshape(-1, 1)

        # Run AutoBNN with compositional kernel search
        model = autobnn.operators.Add(
            bnns=(
                autobnn.kernels.LinearBNN(width=50),         # trend
                autobnn.kernels.PeriodicBNN(width=50),       # seasonality
                autobnn.kernels.MaternBNN(width=50),         # residual structure
            ),
        )

        key = jax.random.PRNGKey(self.seed)
        likelihood_model = autobnn.likelihoods.NormalLikelihood(model)

        params = autobnn.training.fit_bnn(
            likelihood_model=likelihood_model,
            x_train=jnp.array(x),
            y_train=jnp.array(series),
            key=key,
            num_samples=self.num_samples,
            num_chains=self.num_chains,
        )

        # Get component predictions
        predictions = autobnn.training.predict_bnn(
            likelihood_model=likelihood_model,
            params=params,
            x_test=jnp.array(x),
        )

        mean_pred = np.array(predictions["mean"])
        std_pred = np.array(predictions["std"])

        # Decompose into components
        # The Add operator gives us component-wise predictions
        component_preds = autobnn.training.predict_components(
            likelihood_model=likelihood_model,
            params=params,
            x_test=jnp.array(x),
        )

        trend = np.array(component_preds.get("component_0", np.zeros(n)))
        seasonality = np.array(component_preds.get("component_1", np.zeros(n)))
        residual = series - trend - seasonality

        # Detect changepoints from trend second derivative
        changepoints = self._detect_changepoints(trend, dates, series_id)

        # Compute model evidence (log marginal likelihood)
        model_evidence = float(params.get("log_marginal_likelihood", 0.0))

        kernel_desc = "LinearBNN(trend) + PeriodicBNN(seasonality) + MaternBNN(residual)"

        log.info(
            "AutoBNN decompose complete — {n_cp} changepoints detected",
            n_cp=len(changepoints),
        )

        return DecompositionResult(
            series_id=series_id,
            analysis_date=date.today(),
            trend=trend.tolist(),
            seasonality=seasonality.tolist(),
            residual=residual.tolist(),
            changepoints=[
                {
                    "index": cp.change_index,
                    "date": cp.change_date.isoformat() if cp.change_date else None,
                    "confidence": cp.confidence,
                    "magnitude": cp.magnitude,
                    "pre_regime": cp.pre_regime,
                    "post_regime": cp.post_regime,
                }
                for cp in changepoints
            ],
            kernel_description=kernel_desc,
            posterior_std=std_pred.tolist(),
            model_evidence=model_evidence,
        )

    def detect_regime_changes(
        self,
        series: np.ndarray | pd.Series,
        dates: list[date] | None = None,
        series_id: str = "unknown",
        min_confidence: float = 0.5,
    ) -> list[RegimeChangeSignal]:
        """Detect structural regime changes in a time series.

        Uses trend decomposition to find significant structural breaks
        without requiring the full AutoBNN model (falls back to
        second-derivative analysis if AutoBNN is unavailable).

        Parameters:
            series: Historical values.
            dates: Corresponding dates.
            series_id: Identifier.
            min_confidence: Minimum confidence threshold for changepoints.

        Returns:
            list[RegimeChangeSignal]: Detected regime changes.
        """
        if isinstance(series, pd.Series):
            series = series.values

        series = series.astype(np.float64)

        # Use simple trend extraction if AutoBNN unavailable
        if not self.is_available:
            log.debug("AutoBNN unavailable — using moving average trend extraction")
            window = min(30, len(series) // 4)
            if window < 3:
                return []
            trend = pd.Series(series).rolling(window, center=True).mean().values
            trend = np.nan_to_num(trend, nan=series.mean())
        else:
            result = self.decompose(series, dates, series_id)
            trend = np.array(result.trend)

        changepoints = self._detect_changepoints(trend, dates, series_id)
        return [cp for cp in changepoints if cp.confidence >= min_confidence]

    def _detect_changepoints(
        self,
        trend: np.ndarray,
        dates: list[date] | None,
        series_id: str,
    ) -> list[RegimeChangeSignal]:
        """Detect changepoints from trend curvature.

        Uses the second derivative of the trend to identify points
        where the structural regime changes significantly.

        Parameters:
            trend: Extracted trend component.
            dates: Corresponding dates.
            series_id: Series identifier.

        Returns:
            list[RegimeChangeSignal]: Detected regime changes.
        """
        if len(trend) < 10:
            return []

        # Compute second derivative (curvature)
        d2 = np.diff(trend, n=2)
        if len(d2) == 0:
            return []

        # Z-score the curvature to find significant changes
        d2_std = np.std(d2)
        if d2_std < 1e-10:
            return []

        d2_z = np.abs(d2) / d2_std

        # Find peaks above threshold (2 sigma)
        threshold = 2.0
        change_indices = np.where(d2_z > threshold)[0] + 1  # +1 for diff offset

        # Merge nearby changepoints (within 5 steps)
        if len(change_indices) == 0:
            return []

        merged = [change_indices[0]]
        for idx in change_indices[1:]:
            if idx - merged[-1] > 5:
                merged.append(idx)

        changepoints: list[RegimeChangeSignal] = []
        for idx in merged:
            idx = int(idx)
            if idx >= len(trend) - 1:
                continue

            # Characterise pre/post regimes
            pre_window = trend[max(0, idx - 10) : idx]
            post_window = trend[idx : min(len(trend), idx + 10)]

            pre_slope = np.mean(np.diff(pre_window)) if len(pre_window) > 1 else 0
            post_slope = np.mean(np.diff(post_window)) if len(post_window) > 1 else 0

            pre_regime = "rising" if pre_slope > 0 else "falling" if pre_slope < 0 else "flat"
            post_regime = "rising" if post_slope > 0 else "falling" if post_slope < 0 else "flat"

            confidence = min(1.0, float(d2_z[idx - 1]) / 4.0)  # Normalize to [0, 1]
            magnitude = abs(float(d2[idx - 1]))

            change_date = dates[idx] if dates and idx < len(dates) else None

            changepoints.append(
                RegimeChangeSignal(
                    series_id=series_id,
                    change_index=idx,
                    change_date=change_date,
                    pre_regime=pre_regime,
                    post_regime=post_regime,
                    confidence=confidence,
                    magnitude=magnitude,
                )
            )

        return changepoints

    def health_check(self) -> dict[str, Any]:
        """Return structured health-check result."""
        return {
            "available": self.is_available,
            "num_samples": self.num_samples,
            "num_chains": self.num_chains,
            "requires": ["jax", "autobnn"],
        }


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_decomposer_instance: AutoBNNDecomposer | None = None


def get_decomposer() -> AutoBNNDecomposer:
    """Return a cached AutoBNNDecomposer singleton."""
    global _decomposer_instance
    if _decomposer_instance is None:
        from config import settings

        _decomposer_instance = AutoBNNDecomposer(
            num_samples=settings.AUTOBNN_NUM_SAMPLES,
            num_chains=settings.AUTOBNN_NUM_CHAINS,
            seed=settings.AUTOBNN_SEED,
        )
    return _decomposer_instance
