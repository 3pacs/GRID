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

from dataclasses import dataclass
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
        confirm_index: Causal detector only: index of the last observation
            the decision used. The break cannot be known before that
            observation is. ``None`` for the curvature detector.
        statistic: Causal detector only: the signed two-sample statistic of
            the drift change (positive = the series' drift moved up).
    """

    series_id: str
    change_index: int
    change_date: date | None
    pre_regime: str
    post_regime: str
    confidence: float
    magnitude: float
    confirm_index: int | None = None
    statistic: float | None = None


# ---------------------------------------------------------------------------
# Causal drift-break test (the fallback when AutoBNN/JAX is absent)
# ---------------------------------------------------------------------------
#
# A break at observation i means the series' drift (mean increment) after i
# differs from its drift before i, judged against the noise of the increments
# before i:
#
#     t(i) = (mean(d[i:i+k]) - mean(d[i-w:i])) / (sd(d[i-w:i]) * sqrt(1/k + 1/w))
#
# with d the first differences: the change lies in the k observations after
# i. i is a break when |t(i)| >= CAUSAL_BREAK_T and |t(i)| is
#   * larger than every |t| over the k candidates before it (a tie goes to the
#     earliest),
#   * larger than every same-signed |t| over the w candidates before it (one
#     sustained shift is one break, not a plateau of them; a reversal is not
#     suppressed by the shift it reverses), and
#   * not beaten by any |t| over the m candidates after it.
# The decision therefore reads observations i-2w .. i+m+k only: it never
# depends on where the window starts or ends, on data after the confirming
# observation i+m+k, or on any filled value. Candidates without that full
# context on either side are never evaluated, which is what keeps the old
# edge artifacts out (a centred rolling mean with its NaN ends filled by the
# series mean manufactured full-confidence "breaks" at index ~w/2 and n-w/2
# of every window).
CAUSAL_PRE_WINDOW = 60
CAUSAL_POST_WINDOW = 10
CAUSAL_PEAK_WINDOW = 3
CAUSAL_BREAK_T = 3.0
# confidence = min(1, |t| / CAUSAL_FULL_CONFIDENCE_T): |t| = 3 reads 0.5.
CAUSAL_FULL_CONFIDENCE_T = 6.0
# A drift is "rising"/"falling" when its own t against the pre-window noise
# exceeds this; otherwise "flat".
CAUSAL_DRIFT_T = 2.0
# Relative floor on the noise scale, so a noiseless synthetic series does not
# divide by zero (real series never get near it).
_SCALE_FLOOR = 1e-9


def causal_context_start(change_index: int, pre_window: int = CAUSAL_PRE_WINDOW) -> int:
    """First observation index a causal break decision at ``change_index`` read."""
    return max(0, change_index - 2 * pre_window)


def _drift_label(mean_increment: float, scale: float, n: int) -> str:
    """'rising' / 'falling' / 'flat' for a mean increment over ``n`` steps."""
    z = mean_increment / (scale / np.sqrt(n))
    if z > CAUSAL_DRIFT_T:
        return "rising"
    if z < -CAUSAL_DRIFT_T:
        return "falling"
    return "flat"


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

        np.array(predictions["mean"])
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

        With AutoBNN installed this decomposes the whole series and reads
        breaks off the trend's curvature (``_detect_changepoints``). That fit
        uses every observation in the window, so its breaks are not
        point-in-time. Without AutoBNN it runs the causal drift-break test
        (``detect_causal_breaks``). The fallback used to smooth with a
        centred rolling mean and fill its NaN ends with the series mean,
        which invented full-confidence breaks near both ends of every window.

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

        series = np.asarray(series, dtype=np.float64)

        if not self.is_available:
            log.debug("AutoBNN unavailable — using the causal drift-break test")
            return self.detect_causal_breaks(
                series, dates, series_id, min_confidence=min_confidence,
            )

        result = self.decompose(series, dates, series_id)
        trend = np.array(result.trend)
        changepoints = self._detect_changepoints(trend, dates, series_id)
        return [cp for cp in changepoints if cp.confidence >= min_confidence]

    def detect_causal_breaks(
        self,
        series: np.ndarray | pd.Series,
        dates: list[date] | None = None,
        series_id: str = "unknown",
        min_confidence: float = 0.5,
        *,
        pre_window: int = CAUSAL_PRE_WINDOW,
        post_window: int = CAUSAL_POST_WINDOW,
        peak_window: int = CAUSAL_PEAK_WINDOW,
    ) -> list[RegimeChangeSignal]:
        """Point-in-time drift breaks (see the CAUSAL_* constants above).

        Every returned break carries ``confirm_index`` (= change_index +
        peak_window + post_window): the decision read no observation after
        it, and none before ``change_index - 2 * pre_window``
        (``causal_context_start``).
        Non-finite values are never filled; a window that touches one is not
        evaluated.

        Parameters:
            series: Historical values in time order.
            dates: Corresponding dates (same length), or None.
            series_id: Identifier copied onto each signal.
            min_confidence: Minimum confidence (0-1) of a returned break.
            pre_window: Increments before a candidate that set its baseline
                drift and noise (w).
            post_window: Increments after a candidate that set its new
                drift (k).
            peak_window: Candidates after a break that must not beat it (m).

        Returns:
            list[RegimeChangeSignal]: Breaks in time order.
        """
        from numpy.lib.stride_tricks import sliding_window_view

        if isinstance(series, pd.Series):
            series = series.values
        x = np.asarray(series, dtype=np.float64)
        w, k, m = int(pre_window), int(post_window), int(peak_window)
        if w < 2 or k < 1 or m < 0:
            raise ValueError("need pre_window >= 2, post_window >= 1, peak_window >= 0")
        n = len(x)
        # Candidate i needs d[i-w : i+k], so i runs from w to n-1-k; a break
        # also needs w candidates before it (k <= w) and m after it.
        n_candidates = n - k - w
        if k > w:
            raise ValueError("post_window must not exceed pre_window")
        if n_candidates < w + m + 1:
            return []

        d = np.diff(x)
        pre = sliding_window_view(d, w)[:n_candidates]        # row j: d[j : j+w], candidate i = j + w
        post = sliding_window_view(d, k)[w:w + n_candidates]  # row j: d[j+w : j+w+k]
        finite_x = x[np.isfinite(x)]
        level = float(np.median(np.abs(finite_x))) if len(finite_x) else 1.0
        with np.errstate(invalid="ignore", divide="ignore", over="ignore"):
            pre_mean = pre.mean(axis=1)
            pre_sd = pre.std(axis=1, ddof=1)
            post_mean = post.mean(axis=1)
            scale = np.maximum(pre_sd, _SCALE_FLOOR * max(1.0, level))
            t = (post_mean - pre_mean) / (scale * np.sqrt(1.0 / k + 1.0 / w))
        t = np.where(np.isfinite(t), t, np.nan)
        abs_t = np.abs(t)

        breaks: list[RegimeChangeSignal] = []
        for j in range(w, n_candidates - m):
            here = abs_t[j]
            if not np.isfinite(here) or here < CAUSAL_BREAK_T:
                continue
            near = abs_t[j - k:j]
            same_sign = t[j - w:j]
            same_sign = np.abs(same_sign[np.isfinite(same_sign) & (np.sign(same_sign) == np.sign(t[j]))])
            after = abs_t[j + 1:j + 1 + m]
            if np.any(near[np.isfinite(near)] >= here) or np.any(same_sign >= here):
                continue
            if np.any(after[np.isfinite(after)] > here):
                continue
            confidence = min(1.0, float(here) / CAUSAL_FULL_CONFIDENCE_T)
            if confidence < min_confidence:
                continue
            i = j + w
            change_date = dates[i] if dates is not None and i < len(dates) else None
            breaks.append(
                RegimeChangeSignal(
                    series_id=series_id,
                    change_index=i,
                    change_date=change_date,
                    pre_regime=_drift_label(float(pre_mean[j]), float(scale[j]), w),
                    post_regime=_drift_label(float(post_mean[j]), float(scale[j]), k),
                    confidence=confidence,
                    magnitude=abs(float(post_mean[j] - pre_mean[j])),
                    confirm_index=i + m + k,
                    statistic=float(t[j]),
                )
            )
        return breaks

    def _detect_changepoints(
        self,
        trend: np.ndarray,
        dates: list[date] | None,
        series_id: str,
        edge_guard: int = 10,
    ) -> list[RegimeChangeSignal]:
        """Detect changepoints from trend curvature.

        Uses the second derivative of the trend to identify points
        where the structural regime changes significantly.

        Non-finite trend values are never filled: curvature is taken only
        where all three neighbours are finite, and a break within
        ``edge_guard`` points of either end of the finite span is dropped
        (its pre/post characterisation window would run off the data, and a
        smoother's ends are where it is least reliable).

        Parameters:
            trend: Extracted trend component.
            dates: Corresponding dates.
            series_id: Series identifier.
            edge_guard: Points at either end of the finite span that cannot
                hold a break.

        Returns:
            list[RegimeChangeSignal]: Detected regime changes.
        """
        trend = np.asarray(trend, dtype=np.float64)
        if len(trend) < 10:
            return []
        finite_idx = np.flatnonzero(np.isfinite(trend))
        if len(finite_idx) < 10:
            return []
        first, last = int(finite_idx[0]), int(finite_idx[-1])

        # Compute second derivative (curvature); NaN wherever a neighbour is.
        d2 = np.diff(trend, n=2)
        valid = np.isfinite(d2)
        if not valid.any():
            return []

        # Z-score the curvature to find significant changes
        d2_std = float(np.std(d2[valid]))
        if d2_std < 1e-10:
            return []

        d2_z = np.where(valid, np.abs(d2) / d2_std, 0.0)

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
            if idx < first + edge_guard or idx > last - edge_guard:
                continue

            # Characterise pre/post regimes
            pre_steps = np.diff(trend[max(0, idx - 10) : idx])
            post_steps = np.diff(trend[idx : min(len(trend), idx + 10)])
            pre_steps = pre_steps[np.isfinite(pre_steps)]
            post_steps = post_steps[np.isfinite(post_steps)]

            pre_slope = float(np.mean(pre_steps)) if len(pre_steps) else 0.0
            post_slope = float(np.mean(post_steps)) if len(post_steps) else 0.0

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
