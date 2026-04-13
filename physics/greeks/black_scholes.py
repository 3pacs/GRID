"""
GRID — Vectorized Black-Scholes Greek primitives (crypto + equity shared).

GEX-3 / Wave 1 of docs/planning/GEX-V2-BUILD-PLAN.md. This module is the
single authoritative source for closed-form option Greeks in GRID; both
`physics/dealer_gamma.py` (equity GEX engine) and the upcoming
`physics/dealer_flow/` crypto engine import from here.

Unit conventions (per GEX-V2-BUILD-PLAN §6.2):
    S      — spot (same units as strike)
    K      — strike
    T      — time to expiry in YEARS (annualized, ACT/365 for crypto,
             ACT/365 or business-day fraction for equity; caller's choice).
             Clipped to T_MIN = 1e-6 internally to avoid 0DTE div-by-zero.
    r      — annual risk-free rate (decimal, e.g. 0.05 for 5%)
    sigma  — annual implied volatility (decimal, e.g. 0.20 for 20%)
    q      — annual continuous dividend / carry yield (decimal); for crypto
             set q = r if perpetual-funding proxy is used, else 0.

All functions are stateless and accept either Python floats or numpy arrays
(broadcast-safe). When numpy is available inputs are normalized to ndarrays
and ops run vectorized; without numpy the scalar fallback uses `math`.
Edge cases are squashed to 0.0, never NaN:
    - T <= 0             → 0.0 for every Greek except delta (intrinsic)
    - sigma <= 0         → 0.0 for every time-decay Greek
    - T clipped to 1e-6  → Greeks stay finite at the 0DTE boundary

No scipy dependency. The standard normal CDF uses `math.erf` via the
identity N(x) = 0.5 * (1 + erf(x / sqrt(2))). The standard normal PDF is
n(x) = (1/sqrt(2*pi)) * exp(-x^2 / 2).

Greek definitions implemented:
    d1, d2       — Black-Scholes coefficients
    delta        — dPrice/dSpot (call or put, via is_call flag)
    gamma        — d^2Price/dSpot^2 (same for call and put)
    vanna        — dDelta/dVol  = dVega/dSpot (call = put)
    charm        — dDelta/dTime (call or put, via is_call flag)
    vomma        — dVega/dVol  (call = put)  [aka "volga"]
    speed        — dGamma/dSpot (call = put)
    color        — dGamma/dTime (call = put)
    zomma        — dGamma/dVol  (call = put)
"""

from __future__ import annotations

import math

try:  # numpy is optional — fall back to scalar math if unavailable
    import numpy as _np

    _HAS_NUMPY = True
except ImportError:  # pragma: no cover - numpy is a hard dep elsewhere
    _np = None  # type: ignore[assignment]
    _HAS_NUMPY = False


# Clip T to this floor to stay numerically stable at 0DTE.
T_MIN: float = 1e-6
_SQRT_2: float = math.sqrt(2.0)
_SQRT_2PI: float = math.sqrt(2.0 * math.pi)


# ── array/scalar abstraction ────────────────────────────────────────

def _is_array(x) -> bool:
    return _HAS_NUMPY and isinstance(x, _np.ndarray)


def _any_array(*args) -> bool:
    return any(_is_array(a) for a in args)


def _as_array(*args):
    """Broadcast all inputs to numpy arrays (float64). Requires numpy."""
    return tuple(_np.asarray(a, dtype=_np.float64) for a in args)


def _norm_cdf_scalar(x: float) -> float:
    """Standard normal CDF via math.erf."""
    return 0.5 * (1.0 + math.erf(x / _SQRT_2))


def _norm_pdf_scalar(x: float) -> float:
    """Standard normal PDF."""
    return math.exp(-0.5 * x * x) / _SQRT_2PI


def _norm_cdf(x):
    if _is_array(x):
        # numpy has no erf in top-level; use math.erf via ufunc-like vectorization
        # but numpy >= 1.0 has np.vectorize; prefer the direct formula via special.
        # We avoid scipy, so use the fact that numpy's math functions work on arrays
        # for exp/sqrt, and call math.erf via a frompyfunc for portability.
        return 0.5 * (1.0 + _erf_array(x / _SQRT_2))
    return _norm_cdf_scalar(float(x))


def _norm_pdf(x):
    if _is_array(x):
        return _np.exp(-0.5 * x * x) / _SQRT_2PI
    return _norm_pdf_scalar(float(x))


if _HAS_NUMPY:
    # frompyfunc wraps math.erf so it works elementwise on ndarrays without scipy.
    _erf_ufunc = _np.frompyfunc(math.erf, 1, 1)

    def _erf_array(x):
        # frompyfunc returns object dtype; cast back to float64.
        return _erf_ufunc(x).astype(_np.float64)
else:  # pragma: no cover
    def _erf_array(x):
        raise RuntimeError("numpy not available")


# ── shared safe-T helper ────────────────────────────────────────────

def _safe_T(T):
    """Clip T to T_MIN. Returns same dtype (array or scalar)."""
    if _is_array(T):
        return _np.maximum(T, T_MIN)
    return max(float(T), T_MIN)


def _sqrt(x):
    if _is_array(x):
        return _np.sqrt(x)
    return math.sqrt(float(x))


def _log(x):
    if _is_array(x):
        return _np.log(x)
    return math.log(float(x))


def _exp(x):
    if _is_array(x):
        return _np.exp(x)
    return math.exp(float(x))


def _zeros_like(ref):
    if _is_array(ref):
        return _np.zeros_like(ref, dtype=_np.float64)
    return 0.0


def _where(cond, a, b):
    if _HAS_NUMPY and (_is_array(cond) or _is_array(a) or _is_array(b)):
        return _np.where(cond, a, b)
    return a if cond else b


def _mask_bad(T, sigma):
    """Return boolean mask (or scalar bool) where inputs are invalid."""
    if _HAS_NUMPY and (_is_array(T) or _is_array(sigma)):
        T_arr = _np.asarray(T, dtype=_np.float64)
        sigma_arr = _np.asarray(sigma, dtype=_np.float64)
        return (T_arr <= 0) | (sigma_arr <= 0)
    return (float(T) <= 0) or (float(sigma) <= 0)


# ── d1 / d2 ─────────────────────────────────────────────────────────

def d1(S, K, T, r, sigma, q: float = 0.0):
    """Black-Scholes d1 coefficient.

    Returns 0.0 where T<=0 or sigma<=0.
    """
    if _any_array(S, K, T, r, sigma, q):
        S_, K_, T_, r_, sigma_, q_ = _as_array(S, K, T, r, sigma, q)
        T_safe = _np.maximum(T_, T_MIN)
        sigma_safe = _np.where(sigma_ > 0, sigma_, 1.0)  # avoid div/0; masked below
        sqrt_T = _np.sqrt(T_safe)
        raw = (_np.log(S_ / K_) + (r_ - q_ + 0.5 * sigma_safe ** 2) * T_safe) / (
            sigma_safe * sqrt_T
        )
        bad = (T_ <= 0) | (sigma_ <= 0)
        return _np.where(bad, 0.0, raw)

    T_f = float(T)
    sigma_f = float(sigma)
    if T_f <= 0 or sigma_f <= 0:
        return 0.0
    T_safe = max(T_f, T_MIN)
    return (math.log(float(S) / float(K)) + (float(r) - float(q) + 0.5 * sigma_f ** 2) * T_safe) / (
        sigma_f * math.sqrt(T_safe)
    )


def d2(S, K, T, r, sigma, q: float = 0.0):
    """Black-Scholes d2 = d1 - sigma * sqrt(T). Returns 0.0 on invalid inputs."""
    if _any_array(S, K, T, r, sigma, q):
        T_, sigma_ = _as_array(T, sigma)
        d1_val = d1(S, K, T, r, sigma, q)
        T_safe = _np.maximum(T_, T_MIN)
        raw = d1_val - sigma_ * _np.sqrt(T_safe)
        bad = (T_ <= 0) | (sigma_ <= 0)
        return _np.where(bad, 0.0, raw)

    T_f = float(T)
    sigma_f = float(sigma)
    if T_f <= 0 or sigma_f <= 0:
        return 0.0
    return d1(S, K, T, r, sigma, q) - sigma_f * math.sqrt(max(T_f, T_MIN))


# ── first-order Greeks ──────────────────────────────────────────────

def delta(S, K, T, r, sigma, q: float = 0.0, is_call: bool = True):
    """Option delta = dPrice/dSpot.

    Call:  e^(-qT) * N(d1)
    Put :  e^(-qT) * (N(d1) - 1)

    At T<=0 returns intrinsic delta (1 or 0 for calls; -1 or 0 for puts).
    At sigma<=0 also returns the intrinsic (degenerate) delta.
    """
    if _any_array(S, K, T, r, sigma, q):
        S_, K_, T_, r_, sigma_, q_ = _as_array(S, K, T, r, sigma, q)
        bad = (T_ <= 0) | (sigma_ <= 0)

        T_safe = _np.maximum(T_, T_MIN)
        sigma_safe = _np.where(sigma_ > 0, sigma_, 1.0)
        d1_val = (_np.log(S_ / K_) + (r_ - q_ + 0.5 * sigma_safe ** 2) * T_safe) / (
            sigma_safe * _np.sqrt(T_safe)
        )
        cdf_d1 = _norm_cdf(d1_val)
        exp_qT = _np.exp(-q_ * T_safe)

        if is_call:
            live = exp_qT * cdf_d1
            intrinsic = _np.where(S_ > K_, _np.exp(-q_ * _np.maximum(T_, 0.0)), 0.0)
        else:
            live = exp_qT * (cdf_d1 - 1.0)
            intrinsic = _np.where(
                S_ < K_, -_np.exp(-q_ * _np.maximum(T_, 0.0)), 0.0
            )
        return _np.where(bad, intrinsic, live)

    S_f, K_f = float(S), float(K)
    T_f, sigma_f, r_f, q_f = float(T), float(sigma), float(r), float(q)
    if T_f <= 0 or sigma_f <= 0:
        # Intrinsic delta at/after expiry
        if is_call:
            return math.exp(-q_f * max(T_f, 0.0)) if S_f > K_f else 0.0
        return -math.exp(-q_f * max(T_f, 0.0)) if S_f < K_f else 0.0

    T_safe = max(T_f, T_MIN)
    d1_val = (math.log(S_f / K_f) + (r_f - q_f + 0.5 * sigma_f ** 2) * T_safe) / (
        sigma_f * math.sqrt(T_safe)
    )
    exp_qT = math.exp(-q_f * T_safe)
    if is_call:
        return exp_qT * _norm_cdf_scalar(d1_val)
    return exp_qT * (_norm_cdf_scalar(d1_val) - 1.0)


def gamma(S, K, T, r, sigma, q: float = 0.0):
    """Option gamma = d^2Price/dSpot^2 (same for call and put).

    gamma = e^(-qT) * n(d1) / (S * sigma * sqrt(T))
    """
    if _any_array(S, K, T, r, sigma, q):
        S_, K_, T_, r_, sigma_, q_ = _as_array(S, K, T, r, sigma, q)
        bad = (T_ <= 0) | (sigma_ <= 0) | (S_ <= 0)
        T_safe = _np.maximum(T_, T_MIN)
        sigma_safe = _np.where(sigma_ > 0, sigma_, 1.0)
        S_safe = _np.where(S_ > 0, S_, 1.0)
        sqrt_T = _np.sqrt(T_safe)
        d1_val = (_np.log(S_safe / K_) + (r_ - q_ + 0.5 * sigma_safe ** 2) * T_safe) / (
            sigma_safe * sqrt_T
        )
        pdf = _np.exp(-0.5 * d1_val * d1_val) / _SQRT_2PI
        live = _np.exp(-q_ * T_safe) * pdf / (S_safe * sigma_safe * sqrt_T)
        return _np.where(bad, 0.0, live)

    S_f, K_f = float(S), float(K)
    T_f, sigma_f, r_f, q_f = float(T), float(sigma), float(r), float(q)
    if T_f <= 0 or sigma_f <= 0 or S_f <= 0:
        return 0.0
    T_safe = max(T_f, T_MIN)
    sqrt_T = math.sqrt(T_safe)
    d1_val = (math.log(S_f / K_f) + (r_f - q_f + 0.5 * sigma_f ** 2) * T_safe) / (
        sigma_f * sqrt_T
    )
    return math.exp(-q_f * T_safe) * _norm_pdf_scalar(d1_val) / (S_f * sigma_f * sqrt_T)


# ── second-order & cross Greeks ─────────────────────────────────────

def vanna(S, K, T, r, sigma, q: float = 0.0):
    """Vanna = dDelta/dVol = dVega/dSpot.

    vanna = -e^(-qT) * n(d1) * d2 / sigma

    Same value for calls and puts. Returns 0.0 on invalid inputs.
    """
    if _any_array(S, K, T, r, sigma, q):
        S_, K_, T_, r_, sigma_, q_ = _as_array(S, K, T, r, sigma, q)
        bad = (T_ <= 0) | (sigma_ <= 0) | (S_ <= 0)
        T_safe = _np.maximum(T_, T_MIN)
        sigma_safe = _np.where(sigma_ > 0, sigma_, 1.0)
        S_safe = _np.where(S_ > 0, S_, 1.0)
        sqrt_T = _np.sqrt(T_safe)
        d1_val = (_np.log(S_safe / K_) + (r_ - q_ + 0.5 * sigma_safe ** 2) * T_safe) / (
            sigma_safe * sqrt_T
        )
        d2_val = d1_val - sigma_safe * sqrt_T
        pdf = _np.exp(-0.5 * d1_val * d1_val) / _SQRT_2PI
        live = -_np.exp(-q_ * T_safe) * pdf * d2_val / sigma_safe
        return _np.where(bad, 0.0, live)

    S_f, K_f = float(S), float(K)
    T_f, sigma_f, r_f, q_f = float(T), float(sigma), float(r), float(q)
    if T_f <= 0 or sigma_f <= 0 or S_f <= 0:
        return 0.0
    T_safe = max(T_f, T_MIN)
    sqrt_T = math.sqrt(T_safe)
    d1_val = (math.log(S_f / K_f) + (r_f - q_f + 0.5 * sigma_f ** 2) * T_safe) / (
        sigma_f * sqrt_T
    )
    d2_val = d1_val - sigma_f * sqrt_T
    return -math.exp(-q_f * T_safe) * _norm_pdf_scalar(d1_val) * d2_val / sigma_f


def charm(S, K, T, r, sigma, q: float = 0.0, is_call: bool = True):
    """Charm = dDelta/dTime (delta decay), in units of per-year.

    Call:
        charm = q*e^(-qT)*N(d1)
              - e^(-qT)*n(d1) * (2*(r-q)*T - d2*sigma*sqrt(T)) / (2*T*sigma*sqrt(T))
    Put:
        charm = -q*e^(-qT)*N(-d1)
              - e^(-qT)*n(d1) * (2*(r-q)*T - d2*sigma*sqrt(T)) / (2*T*sigma*sqrt(T))

    Convention: positive charm means delta increases as calendar time moves
    forward (i.e. as T decreases). Some references flip the sign — we follow
    Haug / Wilmott (dDelta/dt where t is calendar time).

    Note on convention: we return dDelta/dT with T = time-to-expiry, which
    matches the Grok MD reference and is the convention used by the GEX
    engine. Callers who want dDelta/dCalendarTime should negate.
    """
    if _any_array(S, K, T, r, sigma, q):
        S_, K_, T_, r_, sigma_, q_ = _as_array(S, K, T, r, sigma, q)
        bad = (T_ <= 0) | (sigma_ <= 0) | (S_ <= 0)
        T_safe = _np.maximum(T_, T_MIN)
        sigma_safe = _np.where(sigma_ > 0, sigma_, 1.0)
        S_safe = _np.where(S_ > 0, S_, 1.0)
        sqrt_T = _np.sqrt(T_safe)
        d1_val = (_np.log(S_safe / K_) + (r_ - q_ + 0.5 * sigma_safe ** 2) * T_safe) / (
            sigma_safe * sqrt_T
        )
        d2_val = d1_val - sigma_safe * sqrt_T
        pdf = _np.exp(-0.5 * d1_val * d1_val) / _SQRT_2PI
        cdf_d1 = _norm_cdf(d1_val)
        exp_qT = _np.exp(-q_ * T_safe)

        common = exp_qT * pdf * (
            2.0 * (r_ - q_) * T_safe - d2_val * sigma_safe * sqrt_T
        ) / (2.0 * T_safe * sigma_safe * sqrt_T)

        if is_call:
            live = q_ * exp_qT * cdf_d1 - common
        else:
            cdf_neg_d1 = _norm_cdf(-d1_val)
            live = -q_ * exp_qT * cdf_neg_d1 - common
        return _np.where(bad, 0.0, live)

    S_f, K_f = float(S), float(K)
    T_f, sigma_f, r_f, q_f = float(T), float(sigma), float(r), float(q)
    if T_f <= 0 or sigma_f <= 0 or S_f <= 0:
        return 0.0
    T_safe = max(T_f, T_MIN)
    sqrt_T = math.sqrt(T_safe)
    d1_val = (math.log(S_f / K_f) + (r_f - q_f + 0.5 * sigma_f ** 2) * T_safe) / (
        sigma_f * sqrt_T
    )
    d2_val = d1_val - sigma_f * sqrt_T
    pdf = _norm_pdf_scalar(d1_val)
    exp_qT = math.exp(-q_f * T_safe)
    common = exp_qT * pdf * (
        2.0 * (r_f - q_f) * T_safe - d2_val * sigma_f * sqrt_T
    ) / (2.0 * T_safe * sigma_f * sqrt_T)
    if is_call:
        return q_f * exp_qT * _norm_cdf_scalar(d1_val) - common
    return -q_f * exp_qT * _norm_cdf_scalar(-d1_val) - common


def vomma(S, K, T, r, sigma, q: float = 0.0):
    """Vomma = dVega/dVol (a.k.a. "volga"). Call = Put.

    vomma = S * e^(-qT) * sqrt(T) * n(d1) * d1 * d2 / sigma
           = vega * d1 * d2 / sigma
    """
    if _any_array(S, K, T, r, sigma, q):
        S_, K_, T_, r_, sigma_, q_ = _as_array(S, K, T, r, sigma, q)
        bad = (T_ <= 0) | (sigma_ <= 0) | (S_ <= 0)
        T_safe = _np.maximum(T_, T_MIN)
        sigma_safe = _np.where(sigma_ > 0, sigma_, 1.0)
        S_safe = _np.where(S_ > 0, S_, 1.0)
        sqrt_T = _np.sqrt(T_safe)
        d1_val = (_np.log(S_safe / K_) + (r_ - q_ + 0.5 * sigma_safe ** 2) * T_safe) / (
            sigma_safe * sqrt_T
        )
        d2_val = d1_val - sigma_safe * sqrt_T
        pdf = _np.exp(-0.5 * d1_val * d1_val) / _SQRT_2PI
        live = (
            S_safe * _np.exp(-q_ * T_safe) * sqrt_T * pdf * d1_val * d2_val / sigma_safe
        )
        return _np.where(bad, 0.0, live)

    S_f, K_f = float(S), float(K)
    T_f, sigma_f, r_f, q_f = float(T), float(sigma), float(r), float(q)
    if T_f <= 0 or sigma_f <= 0 or S_f <= 0:
        return 0.0
    T_safe = max(T_f, T_MIN)
    sqrt_T = math.sqrt(T_safe)
    d1_val = (math.log(S_f / K_f) + (r_f - q_f + 0.5 * sigma_f ** 2) * T_safe) / (
        sigma_f * sqrt_T
    )
    d2_val = d1_val - sigma_f * sqrt_T
    return (
        S_f
        * math.exp(-q_f * T_safe)
        * sqrt_T
        * _norm_pdf_scalar(d1_val)
        * d1_val
        * d2_val
        / sigma_f
    )


def speed(S, K, T, r, sigma, q: float = 0.0):
    """Speed = dGamma/dSpot. Call = Put.

    speed = -(gamma / S) * (d1 / (sigma * sqrt(T)) + 1)
    """
    if _any_array(S, K, T, r, sigma, q):
        S_, K_, T_, r_, sigma_, q_ = _as_array(S, K, T, r, sigma, q)
        bad = (T_ <= 0) | (sigma_ <= 0) | (S_ <= 0)
        T_safe = _np.maximum(T_, T_MIN)
        sigma_safe = _np.where(sigma_ > 0, sigma_, 1.0)
        S_safe = _np.where(S_ > 0, S_, 1.0)
        sqrt_T = _np.sqrt(T_safe)
        d1_val = (_np.log(S_safe / K_) + (r_ - q_ + 0.5 * sigma_safe ** 2) * T_safe) / (
            sigma_safe * sqrt_T
        )
        pdf = _np.exp(-0.5 * d1_val * d1_val) / _SQRT_2PI
        gamma_val = _np.exp(-q_ * T_safe) * pdf / (S_safe * sigma_safe * sqrt_T)
        live = -(gamma_val / S_safe) * (d1_val / (sigma_safe * sqrt_T) + 1.0)
        return _np.where(bad, 0.0, live)

    S_f = float(S)
    T_f, sigma_f = float(T), float(sigma)
    if T_f <= 0 or sigma_f <= 0 or S_f <= 0:
        return 0.0
    g = gamma(S, K, T, r, sigma, q)
    T_safe = max(T_f, T_MIN)
    sqrt_T = math.sqrt(T_safe)
    d1_val = d1(S, K, T, r, sigma, q)
    return -(g / S_f) * (d1_val / (sigma_f * sqrt_T) + 1.0)


def color(S, K, T, r, sigma, q: float = 0.0):
    """Color = dGamma/dTime. Call = Put.

    color = -e^(-qT) * n(d1) / (2*S*T*sigma*sqrt(T)) *
            [ 2*q*T + 1 + (2*(r-q)*T - d2*sigma*sqrt(T)) * d1/(sigma*sqrt(T)) ]

    Same dT convention as charm (T = time-to-expiry).
    """
    if _any_array(S, K, T, r, sigma, q):
        S_, K_, T_, r_, sigma_, q_ = _as_array(S, K, T, r, sigma, q)
        bad = (T_ <= 0) | (sigma_ <= 0) | (S_ <= 0)
        T_safe = _np.maximum(T_, T_MIN)
        sigma_safe = _np.where(sigma_ > 0, sigma_, 1.0)
        S_safe = _np.where(S_ > 0, S_, 1.0)
        sqrt_T = _np.sqrt(T_safe)
        d1_val = (_np.log(S_safe / K_) + (r_ - q_ + 0.5 * sigma_safe ** 2) * T_safe) / (
            sigma_safe * sqrt_T
        )
        d2_val = d1_val - sigma_safe * sqrt_T
        pdf = _np.exp(-0.5 * d1_val * d1_val) / _SQRT_2PI
        prefix = -_np.exp(-q_ * T_safe) * pdf / (
            2.0 * S_safe * T_safe * sigma_safe * sqrt_T
        )
        bracket = (
            2.0 * q_ * T_safe
            + 1.0
            + (2.0 * (r_ - q_) * T_safe - d2_val * sigma_safe * sqrt_T)
            * d1_val
            / (sigma_safe * sqrt_T)
        )
        live = prefix * bracket
        return _np.where(bad, 0.0, live)

    S_f, K_f = float(S), float(K)
    T_f, sigma_f, r_f, q_f = float(T), float(sigma), float(r), float(q)
    if T_f <= 0 or sigma_f <= 0 or S_f <= 0:
        return 0.0
    T_safe = max(T_f, T_MIN)
    sqrt_T = math.sqrt(T_safe)
    d1_val = (math.log(S_f / K_f) + (r_f - q_f + 0.5 * sigma_f ** 2) * T_safe) / (
        sigma_f * sqrt_T
    )
    d2_val = d1_val - sigma_f * sqrt_T
    pdf = _norm_pdf_scalar(d1_val)
    prefix = -math.exp(-q_f * T_safe) * pdf / (2.0 * S_f * T_safe * sigma_f * sqrt_T)
    bracket = (
        2.0 * q_f * T_safe
        + 1.0
        + (2.0 * (r_f - q_f) * T_safe - d2_val * sigma_f * sqrt_T)
        * d1_val
        / (sigma_f * sqrt_T)
    )
    return prefix * bracket


def zomma(S, K, T, r, sigma, q: float = 0.0):
    """Zomma = dGamma/dVol. Call = Put.

    zomma = gamma * (d1*d2 - 1) / sigma
    """
    if _any_array(S, K, T, r, sigma, q):
        S_, K_, T_, r_, sigma_, q_ = _as_array(S, K, T, r, sigma, q)
        bad = (T_ <= 0) | (sigma_ <= 0) | (S_ <= 0)
        T_safe = _np.maximum(T_, T_MIN)
        sigma_safe = _np.where(sigma_ > 0, sigma_, 1.0)
        S_safe = _np.where(S_ > 0, S_, 1.0)
        sqrt_T = _np.sqrt(T_safe)
        d1_val = (_np.log(S_safe / K_) + (r_ - q_ + 0.5 * sigma_safe ** 2) * T_safe) / (
            sigma_safe * sqrt_T
        )
        d2_val = d1_val - sigma_safe * sqrt_T
        pdf = _np.exp(-0.5 * d1_val * d1_val) / _SQRT_2PI
        gamma_val = _np.exp(-q_ * T_safe) * pdf / (S_safe * sigma_safe * sqrt_T)
        live = gamma_val * (d1_val * d2_val - 1.0) / sigma_safe
        return _np.where(bad, 0.0, live)

    S_f = float(S)
    T_f, sigma_f = float(T), float(sigma)
    if T_f <= 0 or sigma_f <= 0 or S_f <= 0:
        return 0.0
    g = gamma(S, K, T, r, sigma, q)
    d1_val = d1(S, K, T, r, sigma, q)
    d2_val = d2(S, K, T, r, sigma, q)
    return g * (d1_val * d2_val - 1.0) / sigma_f


__all__ = [
    "T_MIN",
    "d1",
    "d2",
    "delta",
    "gamma",
    "vanna",
    "charm",
    "vomma",
    "speed",
    "color",
    "zomma",
]
