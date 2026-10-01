#!/usr/bin/env python
"""EVAL-E0C2: calibrate E0's realistic outcome-model parameters on pre-2007-11 non-Technology prices.

The e0-v1 ``factor_t_garch`` parameters (market_vol, beta_sd, factor_vol,
factor_ar1, tail_df, GARCH alpha/beta, idio_vol, idio_vol_dispersion,
label_missing_rate) were set by hand. This script estimates them by indirect
inference (simulated method of moments) from real prices VS1 cannot touch and
writes a write-once ``calibration_receipt.json``. It does NOT change E0: the
generator is imported from the frozen ``evals.e0`` package and never edited;
EVAL-E0C3 releases e0-v2 from the receipt.

Data (hard rules, enforced in :func:`load_panel` and tested):

* only the cached E0 replication panel (TIINGO adj_close, today's
  non-Technology sector-map companies, every 5th session), no DB and no
  network;
* only dates strictly before :data:`CUTOFF` (2007-11-01). VS1 v8 moves the
  Technology discovery start to 2008-01-01 with an admission probe from
  2007-11-02, and a sectors-v6 bound to v8 would make non-Technology 2008-2010
  a registered discovery window. Rows of the pinned panel on or after the
  cutoff are dropped at load, before any computation; any other panel must
  already end before it (else :class:`CalibrationGuardError`);
* no ticker on the pinned 908-ticker VS1 Technology deny-list (the v2
  782-candidate universe used by v6/v7/v8, every sector-map Technology
  company, XLK).

Method (on the 5-session grid; the generator is daily):

1. real moments m1..m13 of 5-session log returns of tickers with >= 80%
   coverage (see :data:`MOMENT_NAMES`); m14, the interior missingness rate,
   maps directly to ``label_missing_rate``;
2. simulated moments: a synthetic structure with the same N and 5*T daily
   sessions, the e0-v1 generator's own draws (``simulate_world`` draw order,
   ``generator._innovations`` / ``generator._garch_scale``; a test pins the
   fast path to ``simulate_world`` itself), aggregated to 5 sessions, the real
   panel's missingness mask imposed, the same moment code;
3. weighted distance with weights 1 / block-bootstrap variance of the real
   moments (52-step moving blocks), common random numbers across candidates,
   ``tail_df`` on a grid (Student-t draws are not smooth in df), a coarse
   GARCH grid, then Nelder-Mead in transformed coordinates;
4. uncertainty: >= 200 block-bootstrap resamples of the real moments mapped to
   parameters by a Gauss-Newton step at each grid df's optimum (the
   simulated-method-of-moments delta bootstrap), df re-selected per resample;
   profiles of the weakly identified parameters (tail_df, the GARCH
   alpha/beta ridge);
5. conservative pick: for the weakly identified parameters, the 90% CI
   corner (strong parameters refit at that corner) with the LOWEST synthetic
   E0 power at planted IC 0.01 (E0 v1 machinery, v7 Technology SEC feature
   geometry, synthetic outcomes only);
6. per-sector fits (sector-map groups with N >= 40), the worst-power sector
   as a sensitivity.

``exposure`` (style tilt) is not identifiable from prices; the receipt
recommends carrying it as an e0-v2 sensitivity grid {0, 0.15, 0.30, 0.45}.

Nothing here is a trading signal or an effect-size estimate, and nothing
reads a VS1 price, return or IC.

Usage::

    python scripts/e0_calibrate_outcome_model.py --out DIR --seed 20261001 --bootstrap 200 --jobs 3
"""

from __future__ import annotations

import os

# One BLAS thread per process: --jobs parallelises at the process level, a shared box is not
# oversubscribed, and BLAS reductions are reproducible. (No effect when numpy is already loaded.)
for _var in ("OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "OMP_NUM_THREADS"):
    os.environ.setdefault(_var, "1")

import argparse
import hashlib
import json
import math
import platform
import subprocess
import sys
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import numpy as np
import scipy
from scipy.linalg import eigh as scipy_eigh
from scipy.optimize import minimize
from scipy.signal import lfilter

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from evals.e0 import generator, replication
from evals.e0.structure import PACKAGE as E0_PACKAGE
from evals.e0.structure import Structure, TrialStructure

RECEIPT_VERSION = "e0c2-calibration-v1"
CUTOFF = date(2007, 11, 1)  # exclusive: only dates < 2007-11-01 are ever used
PINNED_PANEL = E0_PACKAGE / "data" / "replication_pre2011_nontech_grid5.npz"
PINNED_PANEL_SHA256 = "d0b6968c46aa69c75c8c72be90fda53ff1bd9677009371875712a4ed5aa1a399"
E0_CONFIG = E0_PACKAGE / "config.json"
SECTOR_MAP = REPO / "analysis" / "sector_map_data.yaml"
GRID_SESSIONS = 5
MIN_COVERAGE = 0.80
BLOCK_STEPS = 52
WINSOR_Z = 8.0
N_FACTORS = 3
MIN_SECTOR_N = 40
DF_GRID = (2.5, 3.0, 3.5, 4.0, 5.0, 6.0, 8.0, 12.0, 30.0)
EXPOSURE_GRID = (0.0, 0.15, 0.30, 0.45)
# A priori (from the brief): daily GARCH alpha vs beta, tail_df and the factor AR(1) are poorly
# identified after 5-day aggregation. Their CI corners are power-checked; the rest are point fits.
WEAK = ("tail_df", "garch_persistence", "garch_alpha_share")

MOMENT_NAMES = (
    "m1_mean_pairwise_corr",
    "m2_resid_eig1_share",
    "m3_resid_eig2_share",
    "m4_resid_eig3_share",
    "m5_median_vol",
    "m6_idio_vol_cv",
    "m7_beta_sd",
    "m8_excess_kurtosis_winsorized",
    "m9_sq_acf_lag1",
    "m10_sq_acf_lag2",
    "m11_sq_acf_lag3",
    "m12_sq_acf_lag4",
    "m13_factor1_acf_lag1",
)

#: Continuous parameters in their transformed (unbounded) coordinates; tail_df is on DF_GRID.
FREE = (
    "market_vol", "beta_sd", "factor_vol", "factor_ar1",
    "garch_persistence", "garch_alpha_share", "idio_vol", "idio_vol_dispersion",
)
#: The parameters that do not need new GARCH paths (cheap to vary with cached shocks).
LINEAR = ("market_vol", "beta_sd", "factor_vol", "factor_ar1", "idio_vol", "idio_vol_dispersion")
AR1_BOUND = 0.95


class CalibrationGuardError(replication.ReplicationGuardError):
    """The calibration input reaches the 2007-11-01 cutoff or the VS1 Technology universe."""


# --------------------------------------------------------------------------------------------
# Input: the guarded pre-cutoff, non-Technology panel
# --------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Panel:
    dates: tuple[date, ...]
    tickers: tuple[str, ...]
    closes: np.ndarray  # grid dates x tickers, NaN = no close
    source: Mapping


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def e0_config() -> dict:
    return json.loads(E0_CONFIG.read_text(encoding="utf-8"))


def load_denylist() -> frozenset[str]:
    return replication.load_denylist(e0_config())


def guard(dates: Sequence[date], tickers: Sequence[str], denylist: frozenset[str] | None = None) -> None:
    """E0's replication guard at the CALIBRATION cutoff (2007-11-01) plus the VS1 Technology deny-list."""
    deny = load_denylist() if denylist is None else denylist
    try:
        replication.check_guards(list(dates), list(tickers), deny, CUTOFF)
    except replication.ReplicationGuardError as exc:
        raise CalibrationGuardError(f"calibration input refused: {exc}") from exc
    if max(dates) >= CUTOFF:  # belt and braces: check_guards already refuses this
        raise CalibrationGuardError(f"calibration input reaches {max(dates)} (cutoff {CUTOFF})")


def load_panel(path: Path | None = None) -> Panel:
    """The calibration panel. Only dates < CUTOFF and no deny-listed ticker ever leave this function.

    ``path=None`` is the pinned cached E0 replication panel (sha256-checked): its rows on or after
    the cutoff are dropped here, from the date vector alone, before any close is used. Any other
    ``path`` must already end before the cutoff; it is refused (before its closes are read) if not.
    """
    pinned = path is None
    p = PINNED_PANEL if pinned else Path(path)
    sha = sha256_file(p)
    if pinned and sha != PINNED_PANEL_SHA256:
        raise CalibrationGuardError(f"the pinned replication panel changed: sha256 {sha}")
    with np.load(p, allow_pickle=False) as data:
        all_dates = [date.fromisoformat(str(d)) for d in data["dates"]]
        tickers = tuple(str(t) for t in data["tickers"])
        keep = np.array([d < CUTOFF for d in all_dates]) if pinned else np.ones(len(all_dates), dtype=bool)
        if pinned and keep.any() and not keep[: int(keep.sum())].all():
            raise CalibrationGuardError("pinned panel dates are not sorted")
        dates = tuple(d for d, k in zip(all_dates, keep) if k)
        if not dates:
            raise CalibrationGuardError("no calibration dates before the cutoff")
        guard(dates, tickers)  # raises before any close is read
        closes = np.asarray(data["closes"], dtype=float)[: len(dates)] if pinned else np.asarray(
            data["closes"], dtype=float)
    if closes.shape != (len(dates), len(tickers)):
        raise ValueError("closes must be grid dates x tickers")
    if np.any(closes[np.isfinite(closes)] <= 0):
        raise ValueError("non-positive adjusted close in the calibration panel")
    source = {
        "panel": "evals/e0/data/replication_pre2011_nontech_grid5.npz" if pinned else p.name,
        "sha256": sha,
        "pinned": pinned,
        "cutoff_exclusive": CUTOFF.isoformat(),
        "rows_in_file": len(all_dates),
        "rows_dropped_at_or_after_cutoff": int((~keep).sum()),
        "first_date": dates[0].isoformat(),
        "last_date": dates[-1].isoformat(),
        "tickers_in_file": len(tickers),
        "denylist_sha256": sha256_file(E0_PACKAGE / e0_config()["replication"]["denylist"]),
        "denylist_size": len(load_denylist()),
    }
    return Panel(dates=dates, tickers=tickers, closes=closes, source=source)


def returns_panel(panel: Panel, tickers: Iterable[str] | None = None) -> tuple[np.ndarray, tuple[str, ...]]:
    """5-session log returns of tickers with >= MIN_COVERAGE finite returns (optionally a subset)."""
    with np.errstate(divide="ignore", invalid="ignore"):
        r = np.diff(np.log(panel.closes), axis=0)
    r[~np.isfinite(r)] = np.nan
    cols = np.arange(len(panel.tickers))
    if tickers is not None:
        wanted = set(tickers)
        cols = np.array([i for i, t in enumerate(panel.tickers) if t in wanted], dtype=int)
    cov = np.isfinite(r[:, cols]).mean(axis=0)
    cols = cols[cov >= MIN_COVERAGE]
    return r[:, cols], tuple(panel.tickers[i] for i in cols)


def interior_missing_rate(R: np.ndarray) -> float:
    """m14: NaN share strictly inside each ticker's first..last finite return (delisting-like gaps)."""
    M = np.isfinite(R)
    gaps = total = 0
    for j in range(R.shape[1]):
        idx = np.flatnonzero(M[:, j])
        if len(idx) < 2:
            continue
        span = idx[-1] - idx[0] + 1
        total += span
        gaps += span - len(idx)
    return float(gaps / total) if total else 0.0


# --------------------------------------------------------------------------------------------
# Moments (identical code for the real and the simulated panels)
# --------------------------------------------------------------------------------------------


def _acf(x: np.ndarray, lag: int) -> float:
    x = x - x.mean()
    den = float(np.dot(x, x))
    return float(np.dot(x[lag:], x[:-lag]) / den) if den > 0 else 0.0


def pair_counts(M: np.ndarray) -> np.ndarray:
    """Pairwise-complete observation counts minus one (the correlation denominators)."""
    Mf = np.asarray(M, dtype=float)
    return np.maximum(Mf.T @ Mf - 1.0, 1.0)


def compute_moments(R: np.ndarray, pairs: np.ndarray | None = None) -> np.ndarray:
    """m1..m13 of a T x N panel of 5-session log returns (NaN = missing). See MOMENT_NAMES.

    ``pairs`` (from :func:`pair_counts` of the same mask) may be passed to skip recomputing it.
    """
    M = np.isfinite(R)
    Mf = M.astype(float)
    n = Mf.sum(axis=0)
    N = R.shape[1]
    X = np.where(M, R, 0.0)
    mu = X.sum(axis=0) / n
    Xc = np.where(M, R - mu, 0.0)
    sd = np.sqrt((Xc ** 2).sum(axis=0) / (n - 1))
    Z = Xc / sd
    if pairs is None:
        pairs = pair_counts(M)
    C = (Z.T @ Z) / pairs
    off = ~np.eye(N, dtype=bool)
    m1 = float(C[off].mean())
    # equal-weight market of the available returns, OLS betas, residual (idiosyncratic) vols
    cnt = np.maximum(Mf.sum(axis=1), 1.0)
    mkt = X.sum(axis=1) / cnt
    mkt_c = mkt - mkt.mean()
    sxx = (Mf * mkt_c[:, None] ** 2).sum(axis=0)
    beta = (Xc * mkt_c[:, None]).sum(axis=0) / sxx
    m7 = float(beta.std(ddof=1))
    resid = np.where(M, Xc - beta[None, :] * mkt_c[:, None], 0.0)
    ivol = np.sqrt((resid ** 2).sum(axis=0) / (n - 2))
    m6 = float(ivol.std(ddof=1) / ivol.mean())
    # factor structure beyond the market: eigen-shares of the residual correlation matrix
    Zr = resid / ivol
    Cr = (Zr.T @ Zr) / pairs
    np.fill_diagonal(Cr, 1.0)
    w, V = scipy_eigh(Cr, subset_by_index=[N - 3, N - 1])  # the top three, ascending
    shares = w[::-1] / N
    f1 = Zr @ V[:, -1]
    m13 = _acf(f1, 1)
    m5 = float(np.median(sd))
    # tails and volatility clustering on winsorized standardized returns (robust to data errors)
    Zw = np.clip(Z, -WINSOR_Z, WINSOR_Z)
    nobs = Mf.sum()
    m2_ = float((Zw ** 2).sum() / nobs)
    m4_ = float((Zw ** 4).sum() / nobs)
    m8 = m4_ / (m2_ * m2_) - 3.0
    sq = Zw ** 2
    s = np.where(M, sq - (sq.sum(axis=0) / n)[None, :], 0.0)
    den = (s ** 2).sum(axis=0)
    acfs = [float(np.mean((s[k:] * s[:-k]).sum(axis=0) / den)) for k in (1, 2, 3, 4)]
    return np.array([m1, shares[0], shares[1], shares[2], m5, m6, m7, m8, *acfs, m13], dtype=float)


def block_bootstrap_indices(T: int, block: int, rng: np.random.Generator) -> np.ndarray:
    """Circular moving-block bootstrap row indices (length T)."""
    n_blocks = math.ceil(T / block)
    starts = rng.integers(0, T, size=n_blocks)
    idx = (starts[:, None] + np.arange(block)[None, :]) % T
    return idx.ravel()[:T]


def bootstrap_moments(R: np.ndarray, resamples: int, seed: int, block: int = BLOCK_STEPS) -> np.ndarray:
    rng = np.random.default_rng([seed, 0xB007])
    out = np.empty((resamples, len(MOMENT_NAMES)))
    for b in range(resamples):
        out[b] = compute_moments(R[block_bootstrap_indices(R.shape[0], block, rng)])
    return out


# --------------------------------------------------------------------------------------------
# Simulated panels: the e0-v1 generator's draws, cached for common random numbers
# --------------------------------------------------------------------------------------------


def scenario_from(params: Mapping, *, exposure: float = 0.0, label_missing_rate: float = 0.0) -> dict:
    """An E0 scenario dict (the e0-v1 ``factor_t_garch`` schema) from natural parameters."""
    return {
        "market": True,
        "market_vol": float(params["market_vol"]),
        "beta_mean": 1.0,
        "beta_sd": float(params["beta_sd"]),
        "n_factors": N_FACTORS,
        "factor_vol": float(params["factor_vol"]),
        "factor_ar1": float(params["factor_ar1"]),
        "tail_df": float(params["tail_df"]),
        "garch": {"alpha": float(params["garch_alpha"]), "beta": float(params["garch_beta"])},
        "exposure": float(exposure),
        "idio_vol": float(params["idio_vol"]),
        "idio_vol_dispersion": float(params["idio_vol_dispersion"]),
        "label_missing_rate": float(label_missing_rate),
    }


def synthetic_structure(n_entities: int, n_steps: int) -> Structure:
    """A feature-free structure with 5 * n_steps daily sessions (only its calendar size is used)."""
    trial = TrialStructure(trial="calibration|fwd1", horizon=1, positions=np.zeros(1, dtype=np.int64),
                           feature=np.zeros((1, n_entities)))
    return Structure(name="e0c2_synthetic", n_sessions=GRID_SESSIONS * n_steps, trials={trial.trial: trial},
                     primary_trial=trial.trial, propensity_trial=trial.trial)


def world_returns(world: generator.World, n_steps: int) -> np.ndarray:
    """A generator World aggregated to n_steps 5-session returns (what the calibration compares)."""
    cum = world.cum_returns
    edges = np.arange(0, GRID_SESSIONS * n_steps + 1, GRID_SESSIONS)
    return cum[edges[1:]] - cum[edges[:-1]]


@dataclass
class _Rep:
    dn: np.ndarray  # idio-vol dispersion normals (N)
    bn: np.ndarray  # beta normals (N)
    loadings: np.ndarray  # N x K
    z: np.ndarray  # n_days x width innovations at the current df
    garch_key: tuple | None = None
    I5: np.ndarray | None = None  # 5-session sums of unit idiosyncratic GARCH shocks (T x N)
    m5: np.ndarray | None = None  # 5-session sums of the market shock (T)
    fraw: np.ndarray | None = None  # daily factor shocks before the AR(1) filter (n_days x K)
    ar1_key: float | None = None
    f5: np.ndarray | None = None  # 5-session sums of AR(1)-filtered factor shocks (T x K)


class SimPanel:
    """Simulated 5-session panels of the e0-v1 generator with the real panel's shape and mask.

    Replication ``r`` draws exactly what ``generator.simulate_world`` draws for a scenario with a
    market, ``N_FACTORS`` factors and no exposure, from ``np.random.default_rng([seed, r])``: the
    dispersion normals, the beta normals, the loadings, then ``generator._innovations`` over
    ``n_days x (N + 1 + K)``; the GARCH paths come from ``generator._garch_scale``. The draws are
    cached per tail_df (common random numbers across candidates), the GARCH aggregation per
    (alpha, beta), the factor filter per AR(1); the six "linear" parameters only rescale cached
    sums. ``tests/test_e0_calibration.py`` pins this to ``simulate_world`` itself.
    """

    def __init__(self, mask: np.ndarray, *, seed: int, reps: int, n_factors: int = N_FACTORS):
        self.mask = np.asarray(mask, dtype=bool)
        self.T, self.N = self.mask.shape
        self.K = n_factors
        self.seed = int(seed)
        self.reps = int(reps)
        st = synthetic_structure(self.N, self.T)
        self.n_days = st.n_sessions + st.max_horizon + 1  # simulate_world's calendar
        self.width = self.N + 1 + self.K
        self.pairs = pair_counts(self.mask)
        self._df: float | None = None
        self._reps: list[_Rep] = []
        self.evaluations = 0

    def _draw(self, df: float) -> None:
        if self._df == df:
            return
        self._reps = []
        for r in range(self.reps):
            rng = np.random.default_rng([self.seed, r])
            dn = rng.standard_normal(self.N)
            bn = rng.standard_normal(self.N)
            loadings = rng.standard_normal((self.N, self.K))
            z = generator._innovations(rng, (self.n_days, self.width), df)
            self._reps.append(_Rep(dn=dn, bn=bn, loadings=loadings, z=z))
        self._df = df

    def _garch(self, rep: _Rep, alpha: float, beta: float) -> None:
        key = (alpha, beta)
        if rep.garch_key == key:
            return
        shocks = generator._garch_scale(rep.z, {"alpha": alpha, "beta": beta}) * rep.z
        n5 = GRID_SESSIONS * self.T
        rep.I5 = shocks[:n5, : self.N].reshape(self.T, GRID_SESSIONS, self.N).sum(axis=1)
        rep.m5 = shocks[:n5, self.N].reshape(self.T, GRID_SESSIONS).sum(axis=1)
        rep.fraw = shocks[:, self.N + 1:]
        rep.garch_key = key
        rep.ar1_key = None

    def _factors(self, rep: _Rep, phi: float) -> None:
        if rep.ar1_key == phi:
            return
        f = rep.fraw
        if phi:
            f = lfilter([math.sqrt(1.0 - phi * phi)], [1.0, -phi], f, axis=0)
        n5 = GRID_SESSIONS * self.T
        rep.f5 = f[:n5].reshape(self.T, GRID_SESSIONS, self.K).sum(axis=1)
        rep.ar1_key = phi

    def returns(self, params: Mapping, rep_index: int = 0, *, apply_mask: bool = True) -> np.ndarray:
        """One replication's T x N 5-session returns under ``params`` (natural units)."""
        self._draw(float(params["tail_df"]))
        rep = self._reps[rep_index]
        self._garch(rep, float(params["garch_alpha"]), float(params["garch_beta"]))
        self._factors(rep, float(params["factor_ar1"]))
        disp = float(params["idio_vol_dispersion"])
        idio_sd = float(params["idio_vol"]) * np.exp(disp * rep.dn - disp * disp / 2.0)
        beta = 1.0 + float(params["beta_sd"]) * rep.bn
        R = (rep.I5 * idio_sd[None, :]
             + (float(params["market_vol"]) * rep.m5)[:, None] * beta[None, :]
             + float(params["factor_vol"]) * (rep.f5 @ rep.loadings.T))
        if apply_mask:
            R = np.where(self.mask, R, np.nan)
        return R

    def market_scale(self, tail_df: float, garch: Mapping, rep_index: int) -> float:
        """Realised / population sd of one replication's 5-session market shock (a single time series)."""
        self._draw(float(tail_df))
        rep = self._reps[rep_index]
        self._garch(rep, float(garch["alpha"]), float(garch["beta"]))
        return float(rep.m5.std() / math.sqrt(GRID_SESSIONS))

    def moments(self, params: Mapping) -> np.ndarray:
        self.evaluations += 1
        return np.mean([compute_moments(self.returns(params, r), self.pairs) for r in range(self.reps)], axis=0)


# --------------------------------------------------------------------------------------------
# Parameter transforms and the objective
# --------------------------------------------------------------------------------------------


def _logit(p: float) -> float:
    return math.log(p / (1.0 - p))


def _expit(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def natural(theta: Sequence[float], tail_df: float) -> dict:
    """Transformed vector (FREE order) -> natural parameter dict (with garch_alpha/garch_beta)."""
    t = dict(zip(FREE, (float(x) for x in theta)))
    pers = 0.999 * _expit(t["garch_persistence"])
    share = _expit(t["garch_alpha_share"])
    return {
        "market_vol": math.exp(t["market_vol"]),
        "beta_sd": math.exp(t["beta_sd"]),
        "factor_vol": math.exp(t["factor_vol"]),
        "factor_ar1": AR1_BOUND * math.tanh(t["factor_ar1"]),
        "tail_df": float(tail_df),
        "garch_alpha": pers * share,
        "garch_beta": pers * (1.0 - share),
        "idio_vol": math.exp(t["idio_vol"]),
        "idio_vol_dispersion": math.exp(t["idio_vol_dispersion"]),
    }


def transformed(params: Mapping) -> np.ndarray:
    pers = float(params["garch_alpha"]) + float(params["garch_beta"])
    share = float(params["garch_alpha"]) / pers
    t = {
        "market_vol": math.log(params["market_vol"]),
        "beta_sd": math.log(params["beta_sd"]),
        "factor_vol": math.log(params["factor_vol"]),
        "factor_ar1": math.atanh(float(params["factor_ar1"]) / AR1_BOUND),
        "garch_persistence": _logit(pers / 0.999),
        "garch_alpha_share": _logit(share),
        "idio_vol": math.log(params["idio_vol"]),
        "idio_vol_dispersion": math.log(params["idio_vol_dispersion"]),
    }
    return np.array([t[k] for k in FREE])


@dataclass
class Target:
    moments: np.ndarray
    weights: np.ndarray  # 1 / bootstrap variance

    def distance(self, sim: np.ndarray) -> float:
        d = self.moments - sim
        return float(np.sum(self.weights * d * d))


def start_values(R: np.ndarray, m: np.ndarray) -> dict:
    """Moment-matching starting values (rough closed forms on the 5-session scale)."""
    root5 = math.sqrt(GRID_SESSIONS)
    mkt = np.nanmean(R, axis=1)
    market_vol = float(np.nanstd(mkt) / root5)
    m5 = m[MOMENT_NAMES.index("m5_median_vol")]
    s1 = m[MOMENT_NAMES.index("m2_resid_eig1_share")]
    iv = float(0.85 * m5 / root5)
    fv = float(math.sqrt(max(s1, 1e-4) * iv * iv / max(1.0 - 3.0 * s1, 0.2)))
    return {
        "market_vol": max(market_vol, 1e-4),
        "beta_sd": float(max(m[MOMENT_NAMES.index("m7_beta_sd")], 0.05)),
        "factor_vol": max(fv, 1e-4),
        "factor_ar1": 0.02,
        "tail_df": 4.0,
        "garch_alpha": 0.08,
        "garch_beta": 0.90,
        "idio_vol": max(iv, 1e-4),
        "idio_vol_dispersion": float(min(max(m[MOMENT_NAMES.index("m6_idio_vol_cv")], 0.05), 1.5)),
    }


@dataclass
class FitSettings:
    reps: int = 4
    maxfev: int = 600
    garch_grid: tuple = ((0.90, 0.10), (0.95, 0.06), (0.97, 0.08), (0.985, 0.06), (0.99, 0.04))
    df_grid: tuple = DF_GRID


def _nm(fun: Callable[[np.ndarray], float], x0: np.ndarray, maxfev: int, step: float = 0.15) -> tuple[np.ndarray, float]:
    simplex = np.vstack([x0] + [x0 + step * np.eye(len(x0))[i] for i in range(len(x0))])
    res = minimize(fun, x0, method="Nelder-Mead",
                   options={"maxfev": maxfev, "initial_simplex": simplex, "xatol": 1e-3, "fatol": 1e-4})
    return np.asarray(res.x, dtype=float), float(res.fun)


def fit_at_df(sim: SimPanel, target: Target, tail_df: float, start: Mapping, settings: FitSettings) -> dict:
    """Coarse GARCH grid (others at ``start``), then Nelder-Mead over all FREE parameters."""

    def J(theta: np.ndarray) -> float:
        return target.distance(sim.moments(natural(theta, tail_df)))

    best = None
    for pers, share in settings.garch_grid:
        p = dict(start)
        p["garch_alpha"], p["garch_beta"] = pers * share, pers * (1.0 - share)
        th = transformed(p)
        val = J(th)
        if best is None or val < best[1]:
            best = (th, val)
    theta, val = _nm(J, best[0], settings.maxfev)
    theta, val = _nm(J, theta, max(settings.maxfev // 3, 10), step=0.05)  # restart: NM can stall
    return {"tail_df": float(tail_df), "theta": theta, "J": val, "params": natural(theta, tail_df),
            "sim_moments": sim.moments(natural(theta, tail_df))}


def fit_linear(sim: SimPanel, target: Target, shape: Mapping, start: Mapping, maxfev: int) -> dict:
    """Refit the six LINEAR parameters with tail_df and the GARCH pair held at ``shape``."""
    fixed = {k: float(shape[k]) for k in ("tail_df", "garch_alpha", "garch_beta")}
    base = transformed({**start, **fixed})
    idx = [FREE.index(k) for k in LINEAR]

    def full(x: np.ndarray) -> np.ndarray:
        th = base.copy()
        th[idx] = x
        return th

    def J(x: np.ndarray) -> float:
        p = natural(full(x), fixed["tail_df"])
        p.update(fixed)  # exact GARCH pair (no round-trip drift)
        return target.distance(sim.moments(p))

    x, val = _nm(J, base[idx], maxfev)
    params = natural(full(x), fixed["tail_df"])
    params.update(fixed)
    return {"params": params, "J": val}


def jacobian(sim: SimPanel, theta: np.ndarray, tail_df: float, h: float = 0.05) -> np.ndarray:
    """Central-difference d(simulated moments)/d(theta) under common random numbers."""
    G = np.empty((len(MOMENT_NAMES), len(theta)))
    for i in range(len(theta)):
        e = np.zeros(len(theta))
        e[i] = h
        G[:, i] = (sim.moments(natural(theta + e, tail_df)) - sim.moments(natural(theta - e, tail_df))) / (2 * h)
    return G


# --------------------------------------------------------------------------------------------
# Synthetic E0 power at planted IC 0.01 (E0 v1 machinery; synthetic outcomes only)
# --------------------------------------------------------------------------------------------


def e0_power(params: Mapping, *, exposure: float, sims: int, label_missing_rate: float) -> dict:
    """E0 v1 power at IC 0.01 for one parameter set: v7 Technology SEC feature geometry, E0 seeds.

    Every run uses the e0-v1 scenario name ``factor_t_garch`` for seeding, so all parameter sets
    share common random numbers (and the e0-v1 values reproduce E0's own first ``sims`` worlds).
    """
    from evals.e0 import runners, scorer  # lazy: pulls in the analysis machinery
    from evals.e0.structure import load_structure

    config = e0_config()
    structure = load_structure(config)
    sel = config["selection"]
    selection = runners.Selection(ledger_q=sel["ledger_q"], run_k=sel["run_k"], bh_q=sel["bh_q"],
                                  direction=sel["direction"])
    planted = config["planted"]["planted_trials"]
    scenario = scenario_from(params, exposure=exposure, label_missing_rate=label_missing_rate)
    seed_name = "factor_t_garch"
    scales = runners.calibrate_scales(structure, seed_name, scenario, [0.01], planted,
                                      pilot_sims=config["planted"]["pilot_sims"],
                                      pilot_scale=config["planted"]["pilot_scale"],
                                      pilot_seed=config["seeds"]["pilot"])
    rows = runners.run_scenario(structure, seed_name, scenario, ic_grid=[0.01], sims=sims, null_sims=0,
                                perms=config["machinery"]["perms"], selection=selection, planted_trials=planted,
                                scales=scales, base_seed=config["seeds"]["base"])
    scored = scorer.score_scenario(rows, primary=structure.primary_trial, planted=planted,
                                   threshold=runners.raw_threshold(selection, len(structure.trials)),
                                   scales=scales, direction=selection.direction)
    row = scored["power_curve"][0]
    return {
        "exposure": exposure,
        "sims": row["sims"],
        "perms": config["machinery"]["perms"],
        "power_holm_run_alpha": row["power_holm_run_alpha"]["rate"],
        "power_holm_se": row["power_holm_run_alpha"]["se"],
        "power_bh": row["power_bh"]["rate"],
        "realized_mean_ic": row["realized_mean_ic"],
        "planted_scale": row["planted_scale"],
    }


def _power_job(args: tuple) -> tuple[str, dict]:
    key, params, exposure, sims, lmr = args
    return key, e0_power(params, exposure=exposure, sims=sims, label_missing_rate=lmr)


# --------------------------------------------------------------------------------------------
# Sectors
# --------------------------------------------------------------------------------------------

#: Sector-map groups folded into GICS-like sectors (a ticker listed in several groups takes the
#: first GICS-like one in sector-map order).
GICS_LIKE = {
    "Energy": "Energy", "Financials": "Financials", "Healthcare": "Healthcare", "Industrials": "Industrials",
    "Consumer Discretionary": "Consumer Discretionary", "Consumer Staples": "Consumer Staples",
    "Real Estate": "Real Estate", "Utilities": "Utilities", "Communication Services": "Communication Services",
    "Materials": "Materials", "Insurance & Pensions": "Financials", "Private Markets": "Financials",
    "Defense & Aerospace": "Industrials", "Transportation & Logistics": "Industrials",
    "Agriculture & Food": "Consumer Staples", "Commodities": "Materials",
}


def sector_of(tickers: Iterable[str]) -> dict[str, str]:
    import yaml

    smap = yaml.safe_load(SECTOR_MAP.read_text(encoding="utf-8"))["SECTOR_MAP"]
    first_core: dict[str, str] = {}
    first_any: dict[str, str] = {}
    for sector, body in smap.items():
        for sub in (body.get("subsectors") or {}).values():
            for actor in sub.get("actors") or []:
                t = str(actor.get("ticker") or "").upper()
                if not t or actor.get("type") != "company":
                    continue
                first_any.setdefault(t, sector)
                if sector in GICS_LIKE and GICS_LIKE[sector] == sector:
                    first_core.setdefault(t, sector)
    out = {}
    for t in tickers:
        s = first_core.get(t) or GICS_LIKE.get(first_any.get(t, ""), "")
        if s:
            out[t] = s
    return out


# --------------------------------------------------------------------------------------------
# The calibration
# --------------------------------------------------------------------------------------------


@dataclass
class RunSettings:
    seed: int = 20261001
    bootstrap: int = 200
    reps: int = 4
    maxfev: int = 600
    df_grid: tuple = DF_GRID
    jobs: int = 1
    sectors: bool = True
    power_sims: int = 100
    ridge: bool = True
    extra: dict = field(default_factory=dict)


def _round(value):
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return float(f"{value:.10g}")
    if isinstance(value, np.floating):
        return _round(float(value))
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.ndarray):
        return [_round(v) for v in value.tolist()]
    if isinstance(value, dict):
        return {str(k): _round(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_round(v) for v in value]
    return value


def _df_job(args: tuple) -> dict:
    mask, target, df, start, settings, seed = args
    sim = SimPanel(mask, seed=seed, reps=settings.reps)
    fit = fit_at_df(sim, target, df, start, settings)
    fit["jacobian"] = jacobian(sim, fit["theta"], df)
    fit["evaluations"] = sim.evaluations
    return fit


def _pmap(fn, items: list, jobs: int) -> list:
    if jobs > 1 and len(items) > 1:
        with ProcessPoolExecutor(max_workers=min(jobs, len(items))) as pool:
            return list(pool.map(fn, items))
    return [fn(x) for x in items]


def fit_panel(R: np.ndarray, *, seed: int, bootstrap: int, reps: int, maxfev: int, df_grid: Sequence[float],
              jobs: int = 1, log=print) -> dict:
    """Indirect-inference fit of one returns panel: profile over df_grid, then the best df."""
    real = compute_moments(R)
    boot = bootstrap_moments(R, bootstrap, seed)
    var = boot.var(axis=0, ddof=1)
    if not np.all(var > 0):
        raise ValueError("a real moment has zero bootstrap variance")
    target = Target(moments=real, weights=1.0 / var)
    start = start_values(R, real)
    settings = FitSettings(reps=reps, maxfev=maxfev, df_grid=tuple(df_grid))
    log(f"[e0c2] fitting N={R.shape[1]} T={R.shape[0]} over df grid {list(df_grid)}")
    fits = _pmap(_df_job, [(np.isfinite(R), target, df, start, settings, seed) for df in df_grid], jobs)
    best = min(fits, key=lambda f: f["J"])
    log(f"[e0c2] best tail_df {best['tail_df']} J={best['J']:.2f}")
    return {"real": real, "boot": boot, "target": target, "fits": fits, "best": best, "start": start}


def linearized_bootstrap(fitted: dict) -> dict:
    """Map each bootstrap resample of the real moments to parameters (Gauss-Newton step per df)."""
    target, fits = fitted["target"], fitted["fits"]
    # Recentre the resamples on the real moments: block joins bias the bootstrap mean of the ACF and
    # eigen-share moments, and only the spread (not that artefact) should move the parameters.
    boot = fitted["boot"] - fitted["boot"].mean(axis=0, keepdims=True) + target.moments[None, :]
    W = target.weights
    sqw = np.sqrt(W)
    per_df = []
    for f in fits:
        G = f["jacobian"]
        A = sqw[:, None] * G
        resid0 = target.moments - f["sim_moments"]
        # Least squares in the weighted metric; lstsq is stable when the ridge makes A'A near-singular.
        steps, *_ = np.linalg.lstsq(A, (sqw[None, :] * (boot - target.moments[None, :] + resid0[None, :])).T,
                                    rcond=None)
        steps = steps.T  # B x P
        pred = boot - f["sim_moments"][None, :] - steps @ G.T
        J = (W[None, :] * pred * pred).sum(axis=1)
        per_df.append((f, steps, J))
    Jmat = np.column_stack([J for _, _, J in per_df])
    choice = Jmat.argmin(axis=1)
    draws = []
    for b, k in enumerate(choice):
        f, steps, _ = per_df[k]
        draws.append(natural(f["theta"] + steps[b], f["tail_df"]))
    best = fitted["best"]
    G = best["jacobian"]
    info = G.T @ (W[:, None] * G)
    evals, evecs = np.linalg.eigh(info)
    return {
        "draws": draws,
        "df_choice_freq": {f"{fits[k]['tail_df']:g}": float(np.mean(choice == k)) for k in range(len(fits))},
        "information_eigenvalues": evals.tolist(),
        "weakest_direction": dict(zip(FREE, evecs[:, 0].tolist())),
        "information_condition": float(evals[-1] / max(evals[0], 1e-300)),
    }


def percentile_ci(draws: Sequence[Mapping], key: str, level: float = 0.90) -> list[float]:
    vals = np.array([d[key] if key != "garch_persistence" else d["garch_alpha"] + d["garch_beta"] for d in draws])
    lo, hi = np.percentile(vals, [50 * (1 - level), 100 - 50 * (1 - level)])
    return [float(lo), float(hi)]


def ridge_profile(R: np.ndarray, target: Target, best: Mapping, best_J: float, seed: int, reps: int,
                  maxfev: int, jobs: int = 1) -> list[dict]:
    """J over a (persistence, alpha) grid at the best tail_df, LINEAR parameters refit at each point."""
    jobs_ = []
    for pers in (0.90, 0.95, 0.97, 0.98, 0.99, 0.995):
        for alpha in (0.03, 0.05, 0.08, 0.12):
            shape = {"tail_df": best["tail_df"], "garch_alpha": alpha, "garch_beta": pers - alpha}
            key = f"p{pers:g}_a{alpha:g}"
            jobs_.append((key, np.isfinite(R), target, shape, best, seed, reps, maxfev))
    out = []
    for key, fit in _pmap(_corner_job, jobs_, jobs):
        a, b = fit["params"]["garch_alpha"], fit["params"]["garch_beta"]
        out.append({"garch_persistence": a + b, "garch_alpha": a, "garch_beta": b, "J": fit["J"],
                    "delta_J": fit["J"] - best_J, "linear_params": {k: fit["params"][k] for k in LINEAR}})
    return out


def _sector_job(args: tuple) -> tuple[str, dict]:
    name, R, seed, bootstrap, reps, maxfev, df = args
    fitted = fit_panel(R, seed=seed, bootstrap=bootstrap, reps=reps, maxfev=maxfev, df_grid=[df], log=lambda _m: None)
    best = fitted["best"]
    return name, {"n_tickers": int(R.shape[1]), "tail_df_fixed": df, "params": best["params"], "J": best["J"],
                  "real_moments": dict(zip(MOMENT_NAMES, fitted["real"].tolist()))}


def _corner_job(args: tuple) -> tuple[str, dict]:
    key, mask, target, shape, start, seed, reps, maxfev = args
    sim = SimPanel(mask, seed=seed, reps=reps)
    return key, fit_linear(sim, target, shape, start, maxfev)


def calibrate(settings: RunSettings, *, panel_path: Path | None = None, log=print) -> dict:
    panel = load_panel(panel_path)
    R, used = returns_panel(panel)
    if R.shape[1] < 20:
        raise ValueError(f"only {R.shape[1]} tickers reach {MIN_COVERAGE:.0%} coverage")
    lmr = interior_missing_rate(R)
    fitted = fit_panel(R, seed=settings.seed, bootstrap=settings.bootstrap, reps=settings.reps,
                       maxfev=settings.maxfev, df_grid=settings.df_grid, jobs=settings.jobs, log=log)
    best = fitted["best"]
    target = fitted["target"]
    sd = np.sqrt(1.0 / target.weights)
    unc = linearized_bootstrap(fitted)
    ci_keys = ("market_vol", "beta_sd", "factor_vol", "factor_ar1", "tail_df", "garch_alpha", "garch_beta",
               "garch_persistence", "idio_vol", "idio_vol_dispersion")
    ci = {k: percentile_ci(unc["draws"], k) for k in ci_keys}

    receipt: dict = {
        "slice": "EVAL-E0C2",
        "version": RECEIPT_VERSION,
        "statement": ("Indirect-inference calibration of the E0 outcome model on pre-2007-11-01 non-Technology "
                      "prices outside every VS1 universe and window. Informs the E0 benchmark only; not a VS1 "
                      "design input, not a trading signal, not an effect-size estimate."),
        "input": {**panel.source, "tickers_used": len(used), "grid_steps": int(R.shape[0]),
                  "min_coverage": MIN_COVERAGE, "tickers_used_sha256": hashlib.sha256(
                      "\n".join(used).encode()).hexdigest()},
        "code": code_provenance(),
        "environment": {"python": platform.python_version(), "numpy": np.__version__, "scipy": scipy.__version__},
        "settings": {"seed": settings.seed, "bootstrap": settings.bootstrap, "reps": settings.reps,
                     "maxfev": settings.maxfev, "df_grid": list(settings.df_grid), "block_steps": BLOCK_STEPS,
                     "winsor_z": WINSOR_Z, "n_factors": N_FACTORS, "grid_sessions": GRID_SESSIONS,
                     "power_sims": settings.power_sims, "sectors": settings.sectors, "ridge": settings.ridge},
        "moments": {"names": list(MOMENT_NAMES), "real": fitted["real"], "bootstrap_sd": sd,
                    "m14_interior_missing_rate": lmr},
        "df_profile": [{"tail_df": f["tail_df"], "J": f["J"], "delta_J": f["J"] - best["J"], "params": f["params"]}
                       for f in fitted["fits"]],
        "fit": {
            "params": {**best["params"], "label_missing_rate": lmr},
            "J": best["J"],
            "moment_fit": [{"moment": name, "real": r, "simulated": s, "bootstrap_sd": d, "z": (r - s) / d}
                           for name, r, s, d in zip(MOMENT_NAMES, fitted["real"], best["sim_moments"], sd)],
            "start_values": fitted["start"],
        },
        "uncertainty": {
            "method": ("circular moving-block bootstrap of the real moments (blocks of 52 grid steps, resamples "
                       "recentred on the real moments), each resample mapped to parameters by one Gauss-Newton "
                       "step at every grid df's optimum with df re-selected per resample; simulated moments at "
                       "fixed common random numbers (simulation noise not included)"),
            "resamples": settings.bootstrap,
            "ci90": ci,
            "tail_df_choice_freq": unc["df_choice_freq"],
            "information_eigenvalues": unc["information_eigenvalues"],
            "information_condition": unc["information_condition"],
            "weakest_direction": unc["weakest_direction"],
        },
        "e0_v1": {k: v for k, v in e0_v1_params().items()},
        "recommendation": {
            "exposure_sensitivity_grid": list(EXPOSURE_GRID),
            "exposure_note": ("style exposure is not identifiable from prices; it needs pre-2008 non-Technology "
                              "Form 4 propensity vs fitted factor loadings (a separate optional study)"),
            "label_missing_rate_note": ("the panel is today's survivors, so delisting-like missingness is not "
                                        "observable here; the interior rate is a lower bound"),
        },
    }
    if settings.ridge:
        log("[e0c2] GARCH ridge profile")
        receipt["ridge_profile"] = ridge_profile(R, target, best["params"], best["J"], settings.seed, settings.reps,
                                                 max(settings.maxfev // 3, 60), settings.jobs)
    sectors = {}
    if settings.sectors:
        smap = sector_of(used)
        groups: dict[str, list[str]] = {}
        for t in used:
            if t in smap:
                groups.setdefault(smap[t], []).append(t)
        jobs = []
        for name in sorted(groups):
            Rs, ts = returns_panel(panel, groups[name])
            if len(ts) >= MIN_SECTOR_N:
                jobs.append((name, Rs, settings.seed, settings.bootstrap, settings.reps, settings.maxfev,
                             best["tail_df"]))
        log(f"[e0c2] sector fits: {[j[0] for j in jobs]}")
        sectors = dict(_pmap(_sector_job, jobs, settings.jobs))
        receipt["sectors"] = {
            "rule": (f"sector-map groups folded to GICS-like sectors, tickers with >= {MIN_COVERAGE:.0%} coverage, "
                     f"N >= {MIN_SECTOR_N}; tail_df fixed at the pooled fit"),
            "fits": sectors,
            "spread": {k: [min(s["params"][k] for s in sectors.values()), max(s["params"][k] for s in sectors.values())]
                       for k in ("market_vol", "beta_sd", "factor_vol", "factor_ar1", "garch_alpha", "garch_beta",
                                 "idio_vol", "idio_vol_dispersion")} if sectors else {},
        }
    if settings.power_sims > 0:
        receipt["conservative_pick"], receipt["power"] = conservative_pick(
            R, target, best["params"], best["J"], ci, sectors, lmr, settings, log)
    else:
        receipt["conservative_pick"] = {"status": "not_run (power_sims=0)", "params": best["params"]}
    return _round(receipt)


def e0_v1_params() -> dict:
    sc = e0_config()["scenarios"]["factor_t_garch"]
    return {"market_vol": sc["market_vol"], "beta_sd": sc["beta_sd"], "factor_vol": sc["factor_vol"],
            "factor_ar1": sc["factor_ar1"], "tail_df": float(sc["tail_df"]), "garch_alpha": sc["garch"]["alpha"],
            "garch_beta": sc["garch"]["beta"], "idio_vol": sc["idio_vol"],
            "idio_vol_dispersion": sc["idio_vol_dispersion"], "label_missing_rate": sc["label_missing_rate"],
            "n_factors": sc["n_factors"], "exposure_headline": e0_config()["scenarios"]["factor_t_garch_exposed"][
                "exposure"]}


def conservative_pick(R, target, best, best_J, ci, sectors, lmr, settings, log) -> tuple[dict, dict]:
    """Power at the weak-parameter CI corners (strong parameters refit), pick the lowest-power one."""
    corners = {}
    df_lo, df_hi = ci["tail_df"]
    p_lo, p_hi = ci["garch_persistence"]
    pers = best["garch_alpha"] + best["garch_beta"]
    a_share = best["garch_alpha"] / pers
    alpha_lo, alpha_hi = ci["garch_alpha"]
    share_lo = min(max(alpha_lo / pers, 0.01), 0.99)
    share_hi = min(max(alpha_hi / pers, 0.01), 0.99)
    df_choices = sorted({_snap_df(df_lo, settings.df_grid), _snap_df(df_hi, settings.df_grid)})
    jobs = []
    for d in df_choices:
        for p in sorted({min(p_lo, 0.998), min(p_hi, 0.998)}):
            for s in sorted({share_lo, share_hi}):
                key = f"df{d:g}_p{p:.4f}_a{p * s:.4f}"
                shape = {"tail_df": d, "garch_alpha": p * s, "garch_beta": p * (1 - s)}
                jobs.append((key, np.isfinite(R), target, shape, best, settings.seed, settings.reps,
                             max(settings.maxfev // 2, 60)))
    log(f"[e0c2] refitting {len(jobs)} weak-parameter corners")
    for key, fit in _pmap(_corner_job, jobs, settings.jobs):
        corners[key] = fit
    sets = {"fitted": best, **{f"corner:{k}": v["params"] for k, v in corners.items()},
            "e0_v1": {k: v for k, v in e0_v1_params().items() if k in best}}
    worst_sector = None
    for name, s in sectors.items():
        sets[f"sector:{name}"] = s["params"]
    # Survivor panel: delisting-like missingness is unobservable here (lmr is a lower bound), so the
    # power runs keep e0-v1's 0.002 as a floor; the same rate for every set keeps the comparison CRN.
    lmr_power = max(lmr, float(e0_v1_params()["label_missing_rate"]))
    power_jobs = [(k, p, 0.0, settings.power_sims, lmr_power) for k, p in sets.items()]
    power_jobs += [(f"{k}@exposure0.3", p, 0.3, settings.power_sims, lmr_power) for k, p in sets.items()
                   if k in ("fitted", "e0_v1")]
    log(f"[e0c2] E0 power: {len(power_jobs)} runs x {settings.power_sims} sims")
    power = dict(_pmap(_power_job, power_jobs, settings.jobs))
    candidates = ["fitted"] + [f"corner:{k}" for k in corners]
    pick_key = min(candidates, key=lambda k: (power[k]["power_holm_run_alpha"], k))
    if sectors:
        worst_sector = min((k for k in sets if k.startswith("sector:")), key=lambda k: power[k]["power_holm_run_alpha"])
    pick_params = dict(sets[pick_key])
    if pick_key != "fitted":
        pick_jobs = [(f"{pick_key}@exposure0.3", pick_params, 0.3, settings.power_sims, lmr_power)]
        power.update(dict(_pmap(_power_job, pick_jobs, 1)))
    pick = {
        "rule": ("among the fitted point and the 90%-CI corners of the weakly identified parameters "
                 f"{list(WEAK)} (tail_df snapped to the grid; strong parameters refit at each corner), the set "
                 "with the lowest synthetic E0 Holm power at planted IC 0.01, exposure 0"),
        "picked": pick_key,
        "params": {**pick_params, "label_missing_rate": lmr_power},
        "label_missing_rate_rule": "max(interior missing rate of the survivor panel, e0-v1 0.002)",
        "corner_fits": {k: {"J": v["J"], "delta_J": v["J"] - best_J, "params": v["params"]}
                        for k, v in corners.items()},
        "worst_power_sector": worst_sector,
        "a_share_at_fit": a_share,
        "persistence_at_fit": pers,
    }
    return pick, power


def _snap_df(x: float, grid: Sequence[float]) -> float:
    return float(min(grid, key=lambda g: (abs(g - x), g)))


def code_provenance() -> dict:
    script = Path(__file__).resolve()
    out = {"script": "scripts/e0_calibrate_outcome_model.py",
           "script_sha256": hashlib.sha256(script.read_bytes().replace(b"\r\n", b"\n")).hexdigest(),
           "generator_sha256": hashlib.sha256((E0_PACKAGE / "generator.py").read_bytes().replace(b"\r\n", b"\n")
                                              ).hexdigest(),
           "e0_manifest_sha256": hashlib.sha256((E0_PACKAGE / "MANIFEST.sha256").read_bytes().replace(b"\r\n", b"\n")
                                                ).hexdigest()}
    try:
        head = subprocess.run(["git", "-C", str(REPO), "rev-parse", "HEAD"], capture_output=True, text=True,
                              timeout=30, check=True).stdout.strip()
        dirty = subprocess.run(["git", "-C", str(REPO), "status", "--porcelain", "--", "scripts/e0_calibrate_outcome_model.py",
                                "evals/e0"], capture_output=True, text=True, timeout=30, check=True).stdout.strip()
        out["git_head"] = head
        out["git_dirty_calibration_paths"] = bool(dirty)
    except (OSError, subprocess.SubprocessError):
        out["git_head"] = None
    return out


def write_receipt(receipt: Mapping, out_dir: Path) -> Path:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "calibration_receipt.json"
    if path.exists():
        raise FileExistsError(f"{path} exists: calibration receipts are write-once")
    path.write_text(json.dumps(receipt, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8",
                    newline="\n")
    return path


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--out", required=True, type=Path, help="directory for calibration_receipt.json (write-once)")
    ap.add_argument("--seed", type=int, default=20261001)
    ap.add_argument("--bootstrap", type=int, default=200, help="block-bootstrap resamples (>= 200 for the receipt)")
    ap.add_argument("--reps", type=int, default=4, help="simulated panels per candidate (common random numbers)")
    ap.add_argument("--maxfev", type=int, default=600, help="Nelder-Mead evaluations per df")
    ap.add_argument("--jobs", type=int, default=1)
    ap.add_argument("--power-sims", type=int, default=100, help="E0 sims per power check (0 = skip the pick)")
    ap.add_argument("--no-sectors", action="store_true")
    ap.add_argument("--no-ridge", action="store_true")
    ap.add_argument("--df-grid", type=float, nargs="+", default=list(DF_GRID))
    ap.add_argument("--panel", type=Path, default=None, help="a pre-cutoff panel npz (default: the pinned E0 panel)")
    a = ap.parse_args(argv)
    out = Path(a.out) / "calibration_receipt.json"
    if out.exists():
        ap.error(f"{out} exists: calibration receipts are write-once")
    settings = RunSettings(seed=a.seed, bootstrap=a.bootstrap, reps=a.reps, maxfev=a.maxfev,
                           df_grid=tuple(a.df_grid), jobs=a.jobs, sectors=not a.no_sectors,
                           power_sims=a.power_sims, ridge=not a.no_ridge)
    receipt = calibrate(settings, panel_path=a.panel)
    path = write_receipt(receipt, a.out)
    print(f"[e0c2] wrote {path} sha256 {sha256_file(path)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
