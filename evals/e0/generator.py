"""Synthetic outcome generator: a daily factor model with a plantable rank IC.

For one simulated "world" on a structure's session calendar (every entity,
every session plus the longest horizon):

    r[d, i] = beta_i * m[d] + sum_k L[i, k] * f_k[d] + s_i * sqrt(h[d, i]) * z[d, i]

* ``m`` (market) and ``f_k`` (industry/style factors, AR(1)) each follow a
  GARCH(1,1) with standardized Student-t innovations (volatility clustering and
  fat tails); ``f_k`` gives the cross-section a correlation structure beyond the
  market (the market alone cancels out of a per-date rank IC);
* ``h[d, i]`` is each entity's own GARCH variance; ``s_i`` is lognormally
  dispersed across entities;
* ``exposure`` > 0 correlates the factor-1 loading with the entity's feature
  propensity (a style tilt: the per-date IC then inherits factor-1 noise,
  heteroskedasticity and serial dependence under the null).

A trial's label at decision position ``p`` is the forward sum of daily returns
over sessions ``p+1 .. p+h``, so the fwd5 and fwd20 trials share one world (the
real study's dependence between trials). A planted effect is added to one
trial's label only, as ``c * sqrt(idio_var_h) * z`` with ``z`` the standardized
feature rank; ``c`` is calibrated by a seeded pilot so the expected realized
mean rank IC equals the target. Non-planted trials are exact nulls by
construction (their label is independent of every feature). Nothing here
reads a price or a return.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Mapping

import numpy as np
from scipy.signal import lfilter

from evals.e0.structure import Structure, TrialStructure


@dataclass(frozen=True)
class World:
    cum_returns: np.ndarray  # (sessions + 1) x entities, cumulative sum with a zero first row
    cum_idio_var: np.ndarray  # same shape, cumulative idiosyncratic variance

    def forward(self, trial: TrialStructure) -> tuple[np.ndarray, np.ndarray]:
        """(label, idio_sd) for each decision: returns over sessions p+1 .. p+h."""
        start = trial.positions + 1
        end = trial.positions + trial.horizon + 1
        label = self.cum_returns[end] - self.cum_returns[start]
        var = self.cum_idio_var[end] - self.cum_idio_var[start]
        return label, np.sqrt(np.maximum(var, 0.0))


def _innovations(rng: np.random.Generator, shape, df: float | None) -> np.ndarray:
    if df is None:
        return rng.standard_normal(shape)
    if df <= 2:
        raise ValueError("tail_df must exceed 2 (finite variance)")
    return rng.standard_t(df, size=shape) / math.sqrt(df / (df - 2.0))


def _garch_scale(z: np.ndarray, garch: Mapping[str, float] | None) -> np.ndarray:
    """sqrt(h_d) path of a unit-variance GARCH(1,1) driven by ``z`` (axis 0 = time)."""
    if not garch:
        return np.ones_like(z)
    a, b = float(garch["alpha"]), float(garch["beta"])
    if a < 0 or b < 0 or a + b >= 1:
        raise ValueError("GARCH needs alpha, beta >= 0 and alpha + beta < 1")
    omega = 1.0 - a - b
    growth = a * z * z + b
    h = np.empty_like(z)
    current = np.ones(z.shape[1:])
    for d in range(z.shape[0]):
        h[d] = current
        current = omega + growth[d] * current
    return np.sqrt(h)


def simulate_world(structure: Structure, scenario: Mapping, rng: np.random.Generator) -> World:
    n_days = structure.n_sessions + structure.max_horizon + 1
    n_ent = structure.n_entities
    df = scenario.get("tail_df")
    garch = scenario.get("garch")
    k = int(scenario.get("n_factors", 0))
    market = bool(scenario.get("market"))

    # Draw order is fixed (part of the pinned benchmark): entity vols, betas,
    # loadings, then all innovations [idiosyncratic | market | factors].
    disp = float(scenario.get("idio_vol_dispersion", 0.0))
    idio_sd = float(scenario["idio_vol"]) * np.exp(disp * rng.standard_normal(n_ent) - disp * disp / 2.0)
    beta = (float(scenario.get("beta_mean", 1.0)) + float(scenario.get("beta_sd", 0.0)) * rng.standard_normal(n_ent)
            if market else None)
    loadings = None
    if k:
        loadings = rng.standard_normal((n_ent, k))
        rho = float(scenario.get("exposure", 0.0))
        if rho:
            loadings[:, 0] = rho * structure.propensity() + math.sqrt(1.0 - rho * rho) * loadings[:, 0]
    width = n_ent + (1 if market else 0) + k
    z = _innovations(rng, (n_days, width), df)
    scale = _garch_scale(z, garch)
    shocks = scale * z  # unit-variance GARCH shocks, every series at once

    idio = shocks[:, :n_ent] * idio_sd[None, :]
    idio_var = (scale[:, :n_ent] * idio_sd[None, :]) ** 2
    returns = idio
    if market:
        returns = returns + float(scenario["market_vol"]) * shocks[:, n_ent:n_ent + 1] * beta[None, :]
    if k:
        phi = float(scenario.get("factor_ar1", 0.0))
        f = shocks[:, width - k:]
        if phi:
            f = lfilter([math.sqrt(1.0 - phi * phi)], [1.0, -phi], f, axis=0)
        returns = returns + float(scenario["factor_vol"]) * f @ loadings.T

    zero = np.zeros((1, n_ent))
    return World(
        cum_returns=np.vstack([zero, np.cumsum(returns, axis=0)]),
        cum_idio_var=np.vstack([zero, np.cumsum(idio_var, axis=0)]),
    )


def base_label(world: World, trial: TrialStructure, scenario: Mapping, rng: np.random.Generator) -> tuple[np.ndarray, np.ndarray]:
    """The trial's null label (with delisting-like missingness) and its planting scale."""
    label, idio_sd = world.forward(trial)
    rate = float(scenario.get("label_missing_rate", 0.0))
    if rate > 0:
        label = np.where(rng.random(label.shape) < rate, np.nan, label)
    return label, idio_sd


def plant(label: np.ndarray, idio_sd: np.ndarray, z: np.ndarray, scale: float) -> np.ndarray:
    """Add ``scale * idio_sd * z`` (z = standardized feature rank; NaN z leaves the label as is)."""
    if scale == 0:
        return label
    return label + scale * idio_sd * np.nan_to_num(z)
