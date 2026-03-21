"""
GRID physics-inspired market transforms.

Core analogs:
  - Kinetic energy (momentum squared)
  - Potential energy (deviation from equilibrium)
  - Market temperature (realized volatility)
  - Entropy rate (regime disorder)
  - Phase velocity (speed through state space)

Deep stochastic dynamics:
  - Ornstein-Uhlenbeck mean reversion (θ, μ, σ estimation)
  - Langevin dynamics (drift + diffusion decomposition)
  - Fokker-Planck stationary density estimation
  - Hamiltonian energy (total conserved quantity)
  - Relaxation time (half-life to equilibrium)
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from loguru import logger as log
from scipy import stats
from scipy.optimize import minimize_scalar


# ===================================================================
# Core physics analogs
# ===================================================================


def kinetic_energy(price_series: pd.Series, window: int = 21) -> pd.Series:
    """½v² analog: half the squared rolling return.

    Captures the "momentum energy" in a price series.  High KE means
    the market is moving fast; low KE means consolidation.

    Parameters:
        price_series: Price or index level series.
        window: Rolling return window in trading days.

    Returns:
        pd.Series: Kinetic energy values (dimensionless).
    """
    log_returns = np.log(price_series / price_series.shift(1))
    rolling_return = log_returns.rolling(window=window).sum()
    return 0.5 * rolling_return ** 2


def potential_energy(
    series: pd.Series, window: int = 252
) -> pd.Series:
    """Deviation from long-run equilibrium.

    Analogous to a spring potential: PE = ½k(x - x₀)² where x₀ is
    the rolling mean.  High PE → far from equilibrium → mean-reversion
    force is strong.

    Parameters:
        series: Feature or price series.
        window: Lookback for equilibrium estimate.

    Returns:
        pd.Series: Potential energy values.
    """
    equilibrium = series.rolling(window=window, min_periods=window // 2).mean()
    displacement = series - equilibrium
    # Normalize by rolling std to make dimensionless
    vol = series.rolling(window=window, min_periods=window // 2).std()
    vol = vol.replace(0, np.nan)
    normalized_displacement = displacement / vol
    return 0.5 * normalized_displacement ** 2


def total_energy(
    price_series: pd.Series,
    short_window: int = 21,
    long_window: int = 252,
) -> pd.Series:
    """Hamiltonian analog: H = KE + PE.

    In conservative systems, total energy is conserved.  Deviations from
    the rolling mean of H indicate energy injection (news, policy) or
    dissipation (mean reversion completing).

    Parameters:
        price_series: Price or index level series.
        short_window: Window for kinetic energy.
        long_window: Window for potential energy.

    Returns:
        pd.Series: Total energy = KE + PE.
    """
    ke = kinetic_energy(price_series, short_window)
    pe = potential_energy(price_series, long_window)
    return ke + pe


def market_temperature(
    returns: pd.Series, window: int = 63
) -> pd.Series:
    """Realized volatility as temperature.

    In statistical mechanics, temperature ∝ mean kinetic energy.
    Here: T = σ² (realized variance) over a rolling window.
    Higher temperature → more chaotic market → wider distribution of
    outcomes.

    Parameters:
        returns: Log return series.
        window: Rolling window in trading days.

    Returns:
        pd.Series: Market temperature (annualized variance).
    """
    variance = returns.rolling(window=window, min_periods=window // 2).var()
    # Annualize
    return variance * 252


def entropy_rate(
    labels: np.ndarray | pd.Series, window: int = 63
) -> pd.Series:
    """Rolling Shannon entropy of discrete labels.

    Measures disorder in regime assignments over a window.
    Low entropy → stable regime.  High entropy → frequent transitions.

    H = -Σ p_i log₂(p_i)

    Parameters:
        labels: Discrete label array (e.g., regime IDs).
        window: Rolling window size.

    Returns:
        pd.Series: Rolling entropy values.
    """
    if isinstance(labels, pd.Series):
        labels_arr = labels.values
    else:
        labels_arr = np.asarray(labels)

    n = len(labels_arr)
    result = np.full(n, np.nan)

    for i in range(window, n):
        window_labels = labels_arr[i - window : i]
        unique, counts = np.unique(window_labels, return_counts=True)
        probs = counts / counts.sum()
        result[i] = -np.sum(probs * np.log2(np.clip(probs, 1e-10, 1.0)))

    return pd.Series(result, index=getattr(labels, "index", range(n)))


def phase_velocity(
    features: pd.DataFrame, n_components: int = 3
) -> pd.Series:
    """Speed of movement through PCA-reduced state space.

    Projects multi-dimensional feature space into principal components,
    then computes the Euclidean velocity (distance moved per time step).
    High phase velocity → rapid regime shift.

    Parameters:
        features: Feature matrix (dates × features).
        n_components: Number of PCA components.

    Returns:
        pd.Series: Phase space velocity at each time step.
    """
    from sklearn.decomposition import PCA
    from sklearn.preprocessing import StandardScaler

    # Standardize and project
    scaled = StandardScaler().fit_transform(features.fillna(method="ffill").dropna())
    n_comp = min(n_components, scaled.shape[1], scaled.shape[0])
    pca = PCA(n_components=n_comp)
    projected = pca.fit_transform(scaled)

    # Compute velocity as Euclidean distance between consecutive points
    diffs = np.diff(projected, axis=0)
    velocities = np.sqrt(np.sum(diffs ** 2, axis=1))

    # Prepend NaN for alignment
    result = np.concatenate([[np.nan], velocities])

    index = features.dropna().index if hasattr(features, "index") else range(len(result))
    # Handle index length mismatch after dropna
    if len(index) != len(result):
        index = range(len(result))

    return pd.Series(result, index=index, name="phase_velocity")


# ===================================================================
# Stochastic dynamics: Ornstein-Uhlenbeck process
# ===================================================================


def estimate_ou_parameters(
    series: pd.Series, dt: float = 1.0 / 252
) -> dict[str, float]:
    """Estimate Ornstein-Uhlenbeck process parameters.

    The OU process: dx = θ(μ - x)dt + σdW

    Parameters:
        θ (theta): Mean-reversion speed (higher = faster reversion)
        μ (mu): Long-run equilibrium level
        σ (sigma): Volatility of the noise term

    Uses OLS on the discrete approximation:
        x_{t+1} - x_t = θ(μ - x_t)Δt + σ√(Δt)ε_t

    Parameters:
        series: Time series to model.
        dt: Time step (default: 1/252 for daily data).

    Returns:
        dict: {theta, mu, sigma, half_life_days, r_squared}
    """
    s = series.dropna()
    if len(s) < 30:
        return {"theta": np.nan, "mu": np.nan, "sigma": np.nan,
                "half_life_days": np.nan, "r_squared": np.nan}

    x = s.values[:-1]
    dx = np.diff(s.values)

    # OLS: dx = a + b*x + noise  →  θ = -b/dt, μ = -a/b
    slope, intercept, r_value, _, _ = stats.linregress(x, dx)

    if slope >= 0:
        # No mean reversion detected
        return {
            "theta": 0.0,
            "mu": float(s.mean()),
            "sigma": float(s.std()),
            "half_life_days": np.inf,
            "r_squared": float(r_value ** 2),
            "mean_reverting": False,
        }

    theta = -slope / dt
    mu = -intercept / slope
    residuals = dx - (intercept + slope * x)
    sigma = float(np.std(residuals) / np.sqrt(dt))
    half_life = np.log(2) / theta  # In years, convert to days
    half_life_days = half_life / dt

    return {
        "theta": round(float(theta), 6),
        "mu": round(float(mu), 6),
        "sigma": round(float(sigma), 6),
        "half_life_days": round(float(half_life_days), 1),
        "r_squared": round(float(r_value ** 2), 4),
        "mean_reverting": True,
    }


def ou_mean_reversion_signal(
    series: pd.Series,
    window: int = 252,
    dt: float = 1.0 / 252,
) -> pd.Series:
    """Rolling OU-based mean reversion strength signal.

    At each point, estimates θ over the lookback window.
    High θ → strong mean reversion → expect convergence to μ.
    θ ≈ 0 → random walk → no reversion expected.

    Parameters:
        series: Time series.
        window: Lookback window for parameter estimation.
        dt: Time step.

    Returns:
        pd.Series: Rolling theta (mean reversion speed).
    """
    result = pd.Series(np.nan, index=series.index, name="ou_theta")

    for i in range(window, len(series)):
        subseries = series.iloc[i - window : i]
        params = estimate_ou_parameters(subseries, dt)
        result.iloc[i] = params["theta"]

    return result


def ou_displacement(series: pd.Series, window: int = 252) -> pd.Series:
    """Displacement from OU equilibrium: (x - μ̂) / σ̂.

    Measures how far the current value is from the estimated long-run
    mean, in units of estimated noise.  Large |displacement| → strong
    mean reversion expected.

    Parameters:
        series: Time series.
        window: Lookback for OU parameter estimation.

    Returns:
        pd.Series: Normalized displacement from equilibrium.
    """
    result = pd.Series(np.nan, index=series.index, name="ou_displacement")

    for i in range(window, len(series)):
        subseries = series.iloc[i - window : i]
        params = estimate_ou_parameters(subseries)
        mu = params["mu"]
        sigma = params["sigma"]
        if sigma > 0 and not np.isnan(mu):
            result.iloc[i] = (series.iloc[i] - mu) / sigma

    return result


# ===================================================================
# Stochastic dynamics: Langevin decomposition
# ===================================================================


def langevin_drift(
    series: pd.Series, window: int = 63, bins: int = 20
) -> pd.Series:
    """Estimate the deterministic drift function from the Langevin equation.

    dx = f(x)dt + g(x)dW

    Drift f(x) is estimated via conditional mean of increments:
        f(x) ≈ <Δx | x> / Δt

    Positive drift → upward tendency from current level.
    Negative drift → downward tendency.

    Parameters:
        series: Time series.
        window: Rolling window for estimation.
        bins: Number of bins for conditional averaging.

    Returns:
        pd.Series: Estimated drift at each time step.
    """
    s = series.dropna()
    if len(s) < window:
        return pd.Series(np.nan, index=series.index, name="langevin_drift")

    dx = s.diff()
    result = pd.Series(np.nan, index=series.index, name="langevin_drift")

    for i in range(window, len(s)):
        x_window = s.iloc[i - window : i].values
        dx_window = dx.iloc[i - window : i].values

        valid = ~np.isnan(dx_window)
        if valid.sum() < 10:
            continue

        x_val = x_window[valid]
        dx_val = dx_window[valid]

        # Current value — find conditional mean of dx near this x
        current_x = s.iloc[i]
        distances = np.abs(x_val - current_x)
        # Use nearest 30% of points for local average
        k = max(3, int(0.3 * len(x_val)))
        nearest_idx = np.argsort(distances)[:k]
        result.iloc[i] = float(np.mean(dx_val[nearest_idx]))

    return result


def langevin_diffusion(
    series: pd.Series, window: int = 63
) -> pd.Series:
    """Estimate the state-dependent diffusion coefficient.

    g(x) ≈ √(<(Δx)² | x> / Δt)

    Higher diffusion at current state → more uncertain outcomes.
    State-dependent diffusion reveals where the market is "noisier."

    Parameters:
        series: Time series.
        window: Rolling window.

    Returns:
        pd.Series: Estimated diffusion coefficient.
    """
    s = series.dropna()
    if len(s) < window:
        return pd.Series(np.nan, index=series.index, name="langevin_diffusion")

    dx = s.diff()
    result = pd.Series(np.nan, index=series.index, name="langevin_diffusion")

    for i in range(window, len(s)):
        x_window = s.iloc[i - window : i].values
        dx_window = dx.iloc[i - window : i].values

        valid = ~np.isnan(dx_window)
        if valid.sum() < 10:
            continue

        x_val = x_window[valid]
        dx_val = dx_window[valid]

        current_x = s.iloc[i]
        distances = np.abs(x_val - current_x)
        k = max(3, int(0.3 * len(x_val)))
        nearest_idx = np.argsort(distances)[:k]
        result.iloc[i] = float(np.sqrt(np.mean(dx_val[nearest_idx] ** 2)))

    return result


# ===================================================================
# Fokker-Planck stationary density
# ===================================================================


def fokker_planck_density(
    series: pd.Series,
    window: int = 504,
    n_points: int = 100,
) -> dict[str, np.ndarray]:
    """Estimate the stationary probability density from the Fokker-Planck equation.

    For a system dx = f(x)dt + g(x)dW, the stationary density is:

        P_s(x) ∝ (1/g²(x)) exp(2 ∫ f(x)/g²(x) dx)

    This is approximated numerically from estimated drift and diffusion.

    Parameters:
        series: Time series.
        window: Lookback for parameter estimation.
        n_points: Grid resolution for density estimation.

    Returns:
        dict: {x_grid, density, mode, entropy}
    """
    s = series.dropna()
    if len(s) < 100:
        return {"x_grid": np.array([]), "density": np.array([]),
                "mode": np.nan, "entropy": np.nan}

    # Use KDE as practical approximation of stationary density
    from scipy.stats import gaussian_kde

    try:
        kde = gaussian_kde(s.values[-window:])
        x_min, x_max = s.min(), s.max()
        margin = (x_max - x_min) * 0.1
        x_grid = np.linspace(x_min - margin, x_max + margin, n_points)
        density = kde(x_grid)

        # Normalize
        dx = x_grid[1] - x_grid[0]
        density = density / (density.sum() * dx)

        mode = x_grid[np.argmax(density)]
        # Shannon entropy of the density
        p = density * dx
        p = p[p > 0]
        entropy = -np.sum(p * np.log(p))

        return {
            "x_grid": x_grid,
            "density": density,
            "mode": float(mode),
            "entropy": float(entropy),
        }
    except Exception as exc:
        log.warning("Fokker-Planck density estimation failed: {e}", e=str(exc))
        return {"x_grid": np.array([]), "density": np.array([]),
                "mode": np.nan, "entropy": np.nan}


# ===================================================================
# Relaxation time and half-life
# ===================================================================


def relaxation_time(
    series: pd.Series,
    window: int = 252,
    dt: float = 1.0 / 252,
) -> pd.Series:
    """Rolling relaxation time (1/θ) from OU estimation.

    The relaxation time τ = 1/θ is the characteristic timescale for
    mean reversion.  It's the e-folding time: after τ, displacement
    from equilibrium decays to 1/e of its initial value.

    Parameters:
        series: Time series.
        window: Lookback for OU estimation.
        dt: Time step.

    Returns:
        pd.Series: Relaxation time in trading days.
    """
    result = pd.Series(np.nan, index=series.index, name="relaxation_time")

    for i in range(window, len(series)):
        subseries = series.iloc[i - window : i]
        params = estimate_ou_parameters(subseries, dt)
        theta = params["theta"]
        if theta > 0:
            result.iloc[i] = 1.0 / theta / dt  # Convert to trading days

    return result


def half_life(
    series: pd.Series,
    window: int = 252,
    dt: float = 1.0 / 252,
) -> pd.Series:
    """Rolling half-life of mean reversion.

    t½ = ln(2) / θ — time for displacement to decay by half.

    Parameters:
        series: Time series.
        window: Lookback.
        dt: Time step.

    Returns:
        pd.Series: Half-life in trading days.
    """
    result = pd.Series(np.nan, index=series.index, name="half_life")

    for i in range(window, len(series)):
        subseries = series.iloc[i - window : i]
        params = estimate_ou_parameters(subseries, dt)
        theta = params["theta"]
        if theta > 0:
            result.iloc[i] = np.log(2) / theta / dt

    return result


# ===================================================================
# Hurst exponent (persistence/anti-persistence)
# ===================================================================


def hurst_exponent(
    series: pd.Series, max_lag: int = 100
) -> float:
    """Estimate the Hurst exponent using rescaled range (R/S) analysis.

    H < 0.5: Anti-persistent (mean reverting)
    H = 0.5: Random walk (Brownian motion)
    H > 0.5: Persistent (trending)

    Parameters:
        series: Time series.
        max_lag: Maximum lag for R/S computation.

    Returns:
        float: Estimated Hurst exponent.
    """
    s = series.dropna().values
    if len(s) < 20:
        return np.nan

    lags = range(2, min(max_lag, len(s) // 2))
    rs_values = []

    for lag in lags:
        # Split into non-overlapping subseries
        n_chunks = len(s) // lag
        if n_chunks < 1:
            continue

        rs_chunk = []
        for i in range(n_chunks):
            chunk = s[i * lag : (i + 1) * lag]
            mean_chunk = chunk.mean()
            deviations = chunk - mean_chunk
            cumsum = np.cumsum(deviations)
            r = cumsum.max() - cumsum.min()
            std = chunk.std(ddof=1)
            if std > 0:
                rs_chunk.append(r / std)

        if rs_chunk:
            rs_values.append((np.log(lag), np.log(np.mean(rs_chunk))))

    if len(rs_values) < 3:
        return np.nan

    log_lags, log_rs = zip(*rs_values)
    slope, _, _, _, _ = stats.linregress(log_lags, log_rs)
    return float(slope)


def rolling_hurst(
    series: pd.Series, window: int = 252, max_lag: int = 50
) -> pd.Series:
    """Rolling Hurst exponent over a window.

    Parameters:
        series: Time series.
        window: Rolling window.
        max_lag: Max lag for R/S analysis.

    Returns:
        pd.Series: Rolling Hurst values.
    """
    result = pd.Series(np.nan, index=series.index, name="hurst")

    for i in range(window, len(series)):
        subseries = series.iloc[i - window : i]
        result.iloc[i] = hurst_exponent(subseries, max_lag)

    return result


# ===================================================================
# Information-theoretic measures
# ===================================================================


def transfer_entropy(
    source: pd.Series,
    target: pd.Series,
    lag: int = 1,
    bins: int = 10,
) -> float:
    """Estimate transfer entropy from source to target.

    TE(X→Y) measures the reduction in uncertainty of Y's future
    given the past of both X and Y, compared to the past of Y alone.

    Positive TE → X provides predictive information about Y.

    Parameters:
        source: Potential causal series.
        target: Target series.
        lag: Time lag.
        bins: Discretization bins.

    Returns:
        float: Transfer entropy in bits.
    """
    x = source.dropna().values
    y = target.dropna().values
    n = min(len(x), len(y)) - lag

    if n < 50:
        return np.nan

    x = x[:n]
    y_past = y[:n]
    y_future = y[lag : n + lag]

    # Discretize
    x_d = np.digitize(x, np.linspace(x.min(), x.max(), bins + 1)[1:-1])
    yp_d = np.digitize(y_past, np.linspace(y_past.min(), y_past.max(), bins + 1)[1:-1])
    yf_d = np.digitize(y_future, np.linspace(y_future.min(), y_future.max(), bins + 1)[1:-1])

    # Joint and marginal probabilities
    def _entropy(*arrays):
        combined = np.column_stack(arrays)
        _, counts = np.unique(combined, axis=0, return_counts=True)
        p = counts / counts.sum()
        return -np.sum(p * np.log2(np.clip(p, 1e-10, 1.0)))

    # TE = H(Y_future, Y_past) + H(Y_past, X) - H(Y_past) - H(Y_future, Y_past, X)
    h_yf_yp = _entropy(yf_d, yp_d)
    h_yp_x = _entropy(yp_d, x_d)
    h_yp = _entropy(yp_d)
    h_yf_yp_x = _entropy(yf_d, yp_d, x_d)

    te = h_yf_yp + h_yp_x - h_yp - h_yf_yp_x
    return float(max(0.0, te))  # TE is non-negative in theory
