"""
GRID Vol Surface Engine.

Constructs, analyzes, and scores implied volatility surfaces from options data.
Implements SVI (Stochastic Volatility Inspired) parameterization for smooth
interpolation and arbitrage detection.

Key outputs:
- Smoothed IV surface (moneyness x DTE grid)
- Skew metrics by expiry
- Term structure curve
- Butterfly and calendar arbitrage flags
- Historical percentile ranking per surface point
- Vol-of-vol estimation
"""

from __future__ import annotations

import math
from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log
from scipy.optimize import minimize
from scipy.stats import norm
from sqlalchemy import text
from sqlalchemy.engine import Engine

from physics.dealer_gamma import (
    _d1,
    _d2,
    bs_charm,
    bs_delta_call,
    bs_delta_put,
    bs_gamma,
    bs_vanna,
)

# ── Constants ────────────────────────────────────────────────────────

RISK_FREE_RATE = 0.05  # 5% — update from FRED if available
MONEYNESS_GRID = [0.80, 0.85, 0.90, 0.95, 0.97, 1.00, 1.03, 1.05, 1.10, 1.15, 1.20]
DTE_GRID = [7, 14, 30, 45, 60, 90, 120, 180, 365]


# ── Black-Scholes helpers not in dealer_gamma ────────────────────────

def _bs_vega(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Vega: dPrice/dSigma (same for calls and puts)."""
    if T <= 0 or sigma <= 0 or S <= 0:
        return 0.0
    d1 = _d1(S, K, T, r, sigma)
    return S * norm.pdf(d1) * math.sqrt(T)


def _bs_theta_call(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Theta for a call option (per calendar day)."""
    if T <= 0 or sigma <= 0 or S <= 0:
        return 0.0
    d1 = _d1(S, K, T, r, sigma)
    d2 = d1 - sigma * math.sqrt(T)
    term1 = -(S * norm.pdf(d1) * sigma) / (2 * math.sqrt(T))
    term2 = -r * K * math.exp(-r * T) * norm.cdf(d2)
    return (term1 + term2) / 365.0


def _bs_theta_put(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Theta for a put option (per calendar day)."""
    if T <= 0 or sigma <= 0 or S <= 0:
        return 0.0
    d1 = _d1(S, K, T, r, sigma)
    d2 = d1 - sigma * math.sqrt(T)
    term1 = -(S * norm.pdf(d1) * sigma) / (2 * math.sqrt(T))
    term2 = r * K * math.exp(-r * T) * norm.cdf(-d2)
    return (term1 + term2) / 365.0


def _bs_volga(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Volga (vomma): d^2Price/dSigma^2 = Vega * d1 * d2 / sigma."""
    if T <= 0 or sigma <= 0 or S <= 0:
        return 0.0
    d1 = _d1(S, K, T, r, sigma)
    d2 = d1 - sigma * math.sqrt(T)
    vega = _bs_vega(S, K, T, r, sigma)
    return vega * d1 * d2 / sigma


def _bs_call_price(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Black-Scholes call price."""
    if T <= 0:
        return max(0.0, S - K)
    if sigma <= 0 or S <= 0:
        return max(0.0, S - K * math.exp(-r * T))
    d1 = _d1(S, K, T, r, sigma)
    d2 = d1 - sigma * math.sqrt(T)
    return S * norm.cdf(d1) - K * math.exp(-r * T) * norm.cdf(d2)


def _bs_put_price(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Black-Scholes put price."""
    if T <= 0:
        return max(0.0, K - S)
    if sigma <= 0 or S <= 0:
        return max(0.0, K * math.exp(-r * T) - S)
    d1 = _d1(S, K, T, r, sigma)
    d2 = d1 - sigma * math.sqrt(T)
    return K * math.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)


# ── Vol Surface Engine ───────────────────────────────────────────────

class VolSurfaceEngine:
    """Constructs and analyzes implied volatility surfaces from options data.

    Parameters:
        db_engine: SQLAlchemy engine for reading options_snapshots and signals.
    """

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine
        self.r = RISK_FREE_RATE

    # ── Core: build_surface ──────────────────────────────────────────

    def build_surface(
        self, ticker: str, as_of_date: date | None = None,
    ) -> dict[str, Any]:
        """Construct the full implied volatility surface for a ticker.

        Queries options_snapshots, filters for valid contracts, computes
        moneyness for each option, and fits SVI parameterization per expiry.

        Parameters:
            ticker: Underlying ticker symbol.
            as_of_date: Snapshot date (default: latest available).

        Returns:
            Dictionary with raw_points, fitted_surface, grid, and metadata.
        """
        if as_of_date is None:
            as_of_date = date.today()

        chain = self._get_options_data(ticker, as_of_date)
        if chain.empty:
            log.warning("Vol surface: no data for {t} on {d}", t=ticker, d=as_of_date)
            return {"error": f"No options data for {ticker}", "ticker": ticker}

        spot = self._get_spot_price(ticker, as_of_date)
        if spot <= 0:
            return {"error": f"No spot price for {ticker}", "ticker": ticker}

        # Filter: reasonable IV, sufficient OI, positive DTE
        mask = (
            (chain["iv"] > 0.01) & (chain["iv"] < 5.0)
            & (chain["oi"] > 10)
            & (chain["dte"] > 0)
        )
        chain = chain[mask].copy()
        if chain.empty:
            return {"error": f"No valid options after filtering for {ticker}", "ticker": ticker}

        # Compute moneyness: K / S
        chain["moneyness"] = chain["strike"] / spot

        # Build raw surface points
        raw_points = []
        for _, row in chain.iterrows():
            raw_points.append({
                "moneyness": round(float(row["moneyness"]), 4),
                "dte": int(row["dte"]),
                "iv": round(float(row["iv"]), 4),
                "strike": float(row["strike"]),
                "expiry": str(row["expiry"]),
                "type": row["opt_type"],
                "oi": int(row["oi"]),
            })

        # Fit SVI per expiry slice
        fitted_surface: dict[str, dict[str, Any]] = {}
        expiries = chain["expiry"].unique()
        for expiry in sorted(expiries):
            exp_slice = chain[chain["expiry"] == expiry]
            if len(exp_slice) < 5:
                continue  # need enough points for SVI fit

            dte_val = exp_slice["dte"].iloc[0]
            T = float(dte_val) / 365.0
            forward = self._compute_forward(spot, self.r, T)

            # SVI parameterization uses log-moneyness: k = log(K / F)
            k_vals = np.log(exp_slice["strike"].values / forward)
            iv_vals = exp_slice["iv"].values.astype(float)

            svi_params = self.fit_svi_slice(k_vals.tolist(), iv_vals.tolist(), T)
            fitted_surface[str(expiry)] = {
                "dte": int(dte_val),
                "n_points": len(exp_slice),
                "svi": svi_params,
            }

        # Build interpolated grid: moneyness x DTE
        grid = self._build_interpolated_grid(fitted_surface, spot)

        log.info(
            "Vol surface built for {t}: {n} raw pts, {e} expiries fitted, spot=${s:.2f}",
            t=ticker, n=len(raw_points), e=len(fitted_surface), s=spot,
        )

        return {
            "ticker": ticker,
            "as_of_date": str(as_of_date),
            "spot": round(spot, 2),
            "raw_points": raw_points,
            "fitted_surface": fitted_surface,
            "grid": grid,
            "metadata": {
                "total_raw_points": len(raw_points),
                "expiries_fitted": len(fitted_surface),
                "risk_free_rate": self.r,
            },
        }

    # ── Core: fit_svi_slice ──────────────────────────────────────────

    def fit_svi_slice(
        self,
        moneyness_vals: list[float],
        iv_vals: list[float],
        T: float = 1.0,
    ) -> dict[str, Any]:
        """Fit SVI parameterization to a single expiry slice.

        SVI total variance model:
            w(k) = a + b * (rho * (k - m) + sqrt((k - m)^2 + sigma^2))

        Where:
            k = log(K/F), the log-moneyness
            w = IV^2 * T, total implied variance
            a, b, rho, m, sigma are the five SVI parameters

        Parameters:
            moneyness_vals: List of log-moneyness values (log(K/F)).
            iv_vals: List of implied volatilities.
            T: Time to expiry in years (for total variance conversion).

        Returns:
            Dictionary with fitted parameters {a, b, rho, m, sigma, rmse}.
        """
        k = np.array(moneyness_vals, dtype=np.float64)
        iv = np.array(iv_vals, dtype=np.float64)

        # Total variance: w = IV^2 * T
        w_market = iv ** 2 * T

        def svi_w(params: np.ndarray, k_arr: np.ndarray) -> np.ndarray:
            a, b, rho, m, sigma = params
            return a + b * (rho * (k_arr - m) + np.sqrt((k_arr - m) ** 2 + sigma ** 2))

        def objective(params: np.ndarray) -> float:
            w_model = svi_w(params, k)
            return float(np.sum((w_model - w_market) ** 2))

        # Initial guess: a ~ mean variance, b ~ slope, rho ~ 0, m ~ 0, sigma ~ 0.1
        a0 = float(np.mean(w_market))
        b0 = 0.1
        rho0 = -0.3  # typical equity skew is negative
        m0 = 0.0
        sigma0 = 0.1

        # Bounds: a > 0, b > 0, -1 < rho < 1, sigma > 0
        bounds = [
            (1e-6, None),      # a: total variance must be positive
            (1e-6, 5.0),       # b: positive, bounded
            (-0.999, 0.999),   # rho: correlation
            (-1.0, 1.0),       # m: center of smile
            (1e-4, 2.0),       # sigma: smoothness
        ]

        try:
            result = minimize(
                objective,
                x0=[a0, b0, rho0, m0, sigma0],
                method="L-BFGS-B",
                bounds=bounds,
                options={"maxiter": 500, "ftol": 1e-12},
            )
            a, b, rho, m, sigma = result.x

            # Compute RMSE in IV space
            w_fitted = svi_w(result.x, k)
            iv_fitted = np.sqrt(np.maximum(w_fitted / T, 0))
            rmse = float(np.sqrt(np.mean((iv_fitted - iv) ** 2)))

            return {
                "a": round(float(a), 6),
                "b": round(float(b), 6),
                "rho": round(float(rho), 6),
                "m": round(float(m), 6),
                "sigma": round(float(sigma), 6),
                "rmse": round(rmse, 6),
                "converged": bool(result.success),
            }
        except Exception as exc:
            log.debug("SVI fit failed: {e}", e=str(exc))
            return {
                "a": float(a0), "b": float(b0), "rho": float(rho0),
                "m": float(m0), "sigma": float(sigma0),
                "rmse": float("inf"), "converged": False,
            }

    # ── Core: compute_skew ───────────────────────────────────────────

    def compute_skew(
        self, ticker: str, as_of_date: date | None = None,
    ) -> list[dict[str, Any]]:
        """Compute IV skew metrics for each expiry.

        For each expiry: 25-delta put IV, ATM IV, 25-delta call IV, and
        derived skew, risk reversal, and butterfly spread.

        Parameters:
            ticker: Underlying ticker symbol.
            as_of_date: Snapshot date.

        Returns:
            List of per-expiry skew dictionaries.
        """
        if as_of_date is None:
            as_of_date = date.today()

        chain = self._get_options_data(ticker, as_of_date)
        if chain.empty:
            return []

        spot = self._get_spot_price(ticker, as_of_date)
        if spot <= 0:
            return []

        results: list[dict[str, Any]] = []

        for expiry in sorted(chain["expiry"].unique()):
            exp_data = chain[chain["expiry"] == expiry]
            dte = int(exp_data["dte"].iloc[0])
            if dte <= 0:
                continue

            T = dte / 365.0
            forward = self._compute_forward(spot, self.r, T)

            # ATM IV: closest to forward price
            calls = exp_data[exp_data["opt_type"] == "call"].copy()
            puts = exp_data[exp_data["opt_type"] == "put"].copy()

            if calls.empty and puts.empty:
                continue

            # ATM IV: average of call and put IVs at strike closest to forward
            all_strikes = exp_data["strike"].values
            if len(all_strikes) == 0:
                continue
            atm_strike_idx = np.argmin(np.abs(all_strikes - forward))
            atm_strike = float(all_strikes[atm_strike_idx])

            atm_options = exp_data[
                (exp_data["strike"] >= atm_strike * 0.99)
                & (exp_data["strike"] <= atm_strike * 1.01)
            ]
            atm_iv = float(atm_options["iv"].mean()) if not atm_options.empty else None
            if atm_iv is None or atm_iv <= 0:
                continue

            # 25-delta put IV: find strike where put delta ~ -0.25
            put_25d_strike = self._delta_to_strike(-0.25, forward, atm_iv, T, is_call=False)
            put_25d_iv = self._interpolate_iv(puts, put_25d_strike)

            # 25-delta call IV: find strike where call delta ~ 0.25
            call_25d_strike = self._delta_to_strike(0.25, forward, atm_iv, T, is_call=True)
            call_25d_iv = self._interpolate_iv(calls, call_25d_strike)

            if put_25d_iv is None or call_25d_iv is None:
                # Fallback: use 90% and 110% moneyness
                put_wing = puts[(puts["strike"] >= spot * 0.88) & (puts["strike"] <= spot * 0.92)]
                call_wing = calls[(calls["strike"] >= spot * 1.08) & (calls["strike"] <= spot * 1.12)]
                put_25d_iv = float(put_wing["iv"].mean()) if not put_wing.empty else None
                call_25d_iv = float(call_wing["iv"].mean()) if not call_wing.empty else None

            if put_25d_iv is None or call_25d_iv is None:
                continue

            skew = put_25d_iv - call_25d_iv
            risk_reversal = call_25d_iv - put_25d_iv
            butterfly = (put_25d_iv + call_25d_iv) / 2.0 - atm_iv

            results.append({
                "expiry": str(expiry),
                "dte": dte,
                "atm_iv": round(atm_iv, 4),
                "put_25d_iv": round(put_25d_iv, 4),
                "call_25d_iv": round(call_25d_iv, 4),
                "skew": round(skew, 4),
                "risk_reversal": round(risk_reversal, 4),
                "butterfly": round(butterfly, 4),
            })

        log.info("Skew computed for {t}: {n} expiries", t=ticker, n=len(results))
        return results

    # ── Core: compute_term_structure ─────────────────────────────────

    def compute_term_structure(
        self, ticker: str, as_of_date: date | None = None,
    ) -> dict[str, Any]:
        """Compute ATM IV term structure across expiries.

        Parameters:
            ticker: Underlying ticker symbol.
            as_of_date: Snapshot date.

        Returns:
            Dictionary with term_structure list, slope, and is_inverted flag.
        """
        if as_of_date is None:
            as_of_date = date.today()

        chain = self._get_options_data(ticker, as_of_date)
        if chain.empty:
            return {"term_structure": [], "slope": None, "is_inverted": None}

        spot = self._get_spot_price(ticker, as_of_date)
        if spot <= 0:
            return {"term_structure": [], "slope": None, "is_inverted": None}

        term: list[dict[str, Any]] = []

        for expiry in sorted(chain["expiry"].unique()):
            exp_data = chain[chain["expiry"] == expiry]
            dte = int(exp_data["dte"].iloc[0])
            if dte <= 0:
                continue

            # ATM: strikes within 2% of spot
            atm_data = exp_data[
                (exp_data["strike"] >= spot * 0.98)
                & (exp_data["strike"] <= spot * 1.02)
            ]
            if atm_data.empty:
                continue

            atm_iv = float(atm_data["iv"].mean())
            if atm_iv <= 0:
                continue

            term.append({
                "expiry": str(expiry),
                "dte": dte,
                "atm_iv": round(atm_iv, 4),
            })

        # Compute slope and inversion
        slope = None
        is_inverted = None

        if len(term) >= 2:
            near = term[0]
            far = term[-1]
            dte_diff = far["dte"] - near["dte"]
            if dte_diff > 0:
                slope = round((far["atm_iv"] - near["atm_iv"]) / dte_diff, 6)
            is_inverted = near["atm_iv"] > far["atm_iv"]

        log.info(
            "Term structure for {t}: {n} expiries, slope={s}, inverted={inv}",
            t=ticker, n=len(term), s=slope, inv=is_inverted,
        )

        return {
            "ticker": ticker,
            "term_structure": term,
            "slope": slope,
            "is_inverted": is_inverted,
        }

    # ── Core: detect_arbitrage ───────────────────────────────────────

    def detect_arbitrage(
        self, surface: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Detect butterfly and calendar arbitrage violations in the vol surface.

        Butterfly arbitrage: total variance must be convex in strike (non-negative
        butterfly spreads in variance space).

        Calendar arbitrage: total variance must be non-decreasing in time for
        a fixed strike.

        Parameters:
            surface: Output from build_surface().

        Returns:
            List of violation dictionaries with type, strike, expiry details.
        """
        violations: list[dict[str, Any]] = []

        if "error" in surface or not surface.get("raw_points"):
            return violations

        raw = pd.DataFrame(surface["raw_points"])

        # ── Butterfly arbitrage (convexity in strike per expiry) ─────
        for expiry in raw["expiry"].unique():
            exp_data = raw[raw["expiry"] == expiry].sort_values("strike")
            if len(exp_data) < 3:
                continue

            dte = int(exp_data["dte"].iloc[0])
            T = dte / 365.0
            if T <= 0:
                continue

            strikes = exp_data["strike"].values
            ivs = exp_data["iv"].values

            # Total variance = IV^2 * T
            total_var = ivs ** 2 * T

            # Convexity check: for each interior point, check
            # w(K_i) <= (w(K_{i-1}) + w(K_{i+1})) / 2
            # (i.e., second difference must be non-negative)
            for i in range(1, len(total_var) - 1):
                butterfly_val = total_var[i - 1] - 2 * total_var[i] + total_var[i + 1]
                if butterfly_val < -1e-6:  # tolerance for numerical noise
                    violations.append({
                        "type": "butterfly",
                        "strike": float(strikes[i]),
                        "expiry1": str(expiry),
                        "expiry2": None,
                        "detail": (
                            f"Convexity violation at K={strikes[i]:.1f}: "
                            f"butterfly_spread={butterfly_val:.6f} < 0 "
                            f"(K-={strikes[i-1]:.1f}, K+={strikes[i+1]:.1f})"
                        ),
                    })

        # ── Calendar arbitrage (monotonicity in time per strike) ─────
        # Group by strike, sort by DTE, check total variance is non-decreasing
        for strike in raw["strike"].unique():
            strike_data = raw[raw["strike"] == strike].drop_duplicates("expiry")
            strike_data = strike_data.sort_values("dte")
            if len(strike_data) < 2:
                continue

            dtes = strike_data["dte"].values
            ivs = strike_data["iv"].values
            expiries = strike_data["expiry"].values

            for i in range(1, len(dtes)):
                T_near = float(dtes[i - 1]) / 365.0
                T_far = float(dtes[i]) / 365.0
                if T_near <= 0 or T_far <= 0:
                    continue

                w_near = float(ivs[i - 1]) ** 2 * T_near
                w_far = float(ivs[i]) ** 2 * T_far

                if w_far < w_near - 1e-6:  # tolerance
                    violations.append({
                        "type": "calendar",
                        "strike": float(strike),
                        "expiry1": str(expiries[i - 1]),
                        "expiry2": str(expiries[i]),
                        "detail": (
                            f"Calendar arb at K={strike:.1f}: "
                            f"w({expiries[i-1]})={w_near:.6f} > "
                            f"w({expiries[i]})={w_far:.6f}"
                        ),
                    })

        log.info(
            "Arbitrage detection: {n} violations ({b} butterfly, {c} calendar)",
            n=len(violations),
            b=sum(1 for v in violations if v["type"] == "butterfly"),
            c=sum(1 for v in violations if v["type"] == "calendar"),
        )

        return violations

    # ── Core: compute_greeks_grid ────────────────────────────────────

    def compute_greeks_grid(
        self, ticker: str, as_of_date: date | None = None,
    ) -> dict[str, Any]:
        """Compute full Greeks for every option in the chain.

        Uses Black-Scholes formulas from physics.dealer_gamma.

        Parameters:
            ticker: Underlying ticker symbol.
            as_of_date: Snapshot date.

        Returns:
            Dictionary with grid (list of per-option Greek dictionaries)
            and summary statistics.
        """
        if as_of_date is None:
            as_of_date = date.today()

        chain = self._get_options_data(ticker, as_of_date)
        if chain.empty:
            return {"error": f"No options data for {ticker}", "grid": []}

        spot = self._get_spot_price(ticker, as_of_date)
        if spot <= 0:
            return {"error": f"No spot price for {ticker}", "grid": []}

        grid: list[dict[str, Any]] = []

        for _, row in chain.iterrows():
            K = float(row["strike"])
            T = float(row["dte"]) / 365.0
            iv = float(row["iv"])
            opt_type = row["opt_type"]
            oi = int(row["oi"])

            if T <= 0 or iv <= 0.01:
                continue

            is_call = opt_type == "call"

            delta = bs_delta_call(spot, K, T, self.r, iv) if is_call else bs_delta_put(spot, K, T, self.r, iv)
            gamma = bs_gamma(spot, K, T, self.r, iv)
            vega = _bs_vega(spot, K, T, self.r, iv) / 100.0  # per 1% IV move
            theta = _bs_theta_call(spot, K, T, self.r, iv) if is_call else _bs_theta_put(spot, K, T, self.r, iv)
            vanna = bs_vanna(spot, K, T, self.r, iv)
            charm_val = bs_charm(spot, K, T, self.r, iv, is_call=is_call)
            volga = _bs_volga(spot, K, T, self.r, iv)

            grid.append({
                "strike": K,
                "expiry": str(row["expiry"]),
                "type": opt_type,
                "delta": round(delta, 4),
                "gamma": round(gamma, 6),
                "vega": round(vega, 4),
                "theta": round(theta, 4),
                "vanna": round(vanna, 6),
                "charm": round(charm_val, 6),
                "volga": round(volga, 4),
                "iv": round(iv, 4),
                "oi": oi,
                "dte": int(row["dte"]),
            })

        # Sort by expiry then strike
        grid.sort(key=lambda g: (g["expiry"], g["strike"], g["type"]))

        log.info("Greeks grid for {t}: {n} contracts", t=ticker, n=len(grid))

        return {
            "ticker": ticker,
            "spot": round(spot, 2),
            "as_of_date": str(as_of_date),
            "grid": grid,
            "summary": {
                "total_contracts": len(grid),
                "calls": sum(1 for g in grid if g["type"] == "call"),
                "puts": sum(1 for g in grid if g["type"] == "put"),
                "max_gamma_strike": max(grid, key=lambda g: g["gamma"])["strike"] if grid else None,
                "max_vega_strike": max(grid, key=lambda g: g["vega"])["strike"] if grid else None,
            },
        }

    # ── Core: historical_percentile ──────────────────────────────────

    def historical_percentile(
        self, ticker: str, lookback_days: int = 252, as_of_date: date | None = None,
    ) -> dict[str, Any]:
        """Compare current vol surface metrics to historical values.

        Computes percentile rank, IV rank, and z-score for ATM IV,
        skew, and term structure slope.

        Parameters:
            ticker: Underlying ticker symbol.
            lookback_days: Historical window in trading days (default: 252 = 1yr).
            as_of_date: Reference date.

        Returns:
            Dictionary with iv_percentile, iv_rank, skew_percentile,
            term_slope_percentile, and z_score.
        """
        if as_of_date is None:
            as_of_date = date.today()

        start_date = as_of_date - timedelta(days=lookback_days)

        with self.engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT signal_date, iv_atm, iv_25d_put, iv_25d_call,
                           term_structure_slope
                    FROM options_daily_signals
                    WHERE ticker = :ticker
                    AND signal_date >= :start AND signal_date <= :end
                    ORDER BY signal_date
                """),
                {"ticker": ticker, "start": start_date, "end": as_of_date},
            ).fetchall()

        if not rows:
            return {
                "ticker": ticker,
                "error": "Insufficient historical data",
                "iv_percentile": None, "iv_rank": None, "z_score": None,
                "skew_percentile": None, "term_slope_percentile": None,
            }

        df = pd.DataFrame(rows, columns=[
            "signal_date", "iv_atm", "iv_25d_put", "iv_25d_call",
            "term_structure_slope",
        ])

        # Current values (latest row)
        current = df.iloc[-1]
        history = df.iloc[:-1] if len(df) > 1 else df

        result: dict[str, Any] = {"ticker": ticker, "as_of_date": str(as_of_date)}

        # ATM IV percentile and rank
        iv_atm = current["iv_atm"]
        if pd.notna(iv_atm) and not history["iv_atm"].dropna().empty:
            hist_iv = history["iv_atm"].dropna()
            result["iv_current"] = round(float(iv_atm), 4)
            result["iv_percentile"] = round(float((hist_iv < iv_atm).mean() * 100), 1)

            iv_min = float(hist_iv.min())
            iv_max = float(hist_iv.max())
            if iv_max > iv_min:
                result["iv_rank"] = round((float(iv_atm) - iv_min) / (iv_max - iv_min) * 100, 1)
            else:
                result["iv_rank"] = 50.0

            iv_mean = float(hist_iv.mean())
            iv_std = float(hist_iv.std())
            if iv_std > 0:
                result["z_score"] = round((float(iv_atm) - iv_mean) / iv_std, 2)
            else:
                result["z_score"] = 0.0
        else:
            result["iv_percentile"] = None
            result["iv_rank"] = None
            result["z_score"] = None

        # Skew percentile (25d put - 25d call)
        put_iv = current["iv_25d_put"]
        call_iv = current["iv_25d_call"]
        if pd.notna(put_iv) and pd.notna(call_iv):
            current_skew = float(put_iv) - float(call_iv)
            hist_skew = (history["iv_25d_put"] - history["iv_25d_call"]).dropna()
            if not hist_skew.empty:
                result["skew_current"] = round(current_skew, 4)
                result["skew_percentile"] = round(float((hist_skew < current_skew).mean() * 100), 1)
            else:
                result["skew_percentile"] = None
        else:
            result["skew_percentile"] = None

        # Term structure slope percentile
        ts_slope = current["term_structure_slope"]
        if pd.notna(ts_slope):
            hist_slope = history["term_structure_slope"].dropna()
            if not hist_slope.empty:
                result["term_slope_current"] = round(float(ts_slope), 6)
                result["term_slope_percentile"] = round(
                    float((hist_slope < ts_slope).mean() * 100), 1,
                )
            else:
                result["term_slope_percentile"] = None
        else:
            result["term_slope_percentile"] = None

        log.info(
            "Historical percentile for {t}: IV_pct={p}, IV_rank={r}, z={z}",
            t=ticker,
            p=result.get("iv_percentile"),
            r=result.get("iv_rank"),
            z=result.get("z_score"),
        )

        return result

    # ── Core: variance_swap_level ────────────────────────────────────

    def variance_swap_level(
        self, ticker: str, as_of_date: date | None = None,
    ) -> float | None:
        """Estimate fair variance swap strike from the vol surface.

        Uses the Breeden-Litzenberger approach: the fair variance is the
        integral of OTM option prices weighted by 1/K^2, which gives
        model-free implied variance.

        Parameters:
            ticker: Underlying ticker symbol.
            as_of_date: Snapshot date.

        Returns:
            Fair variance swap volatility level (annualized), or None if
            insufficient data.
        """
        if as_of_date is None:
            as_of_date = date.today()

        chain = self._get_options_data(ticker, as_of_date)
        if chain.empty:
            return None

        spot = self._get_spot_price(ticker, as_of_date)
        if spot <= 0:
            return None

        # Use the nearest expiry with enough contracts
        nearest_expiry = None
        for expiry in sorted(chain["expiry"].unique()):
            exp_data = chain[chain["expiry"] == expiry]
            if len(exp_data) >= 10:
                nearest_expiry = expiry
                break

        if nearest_expiry is None:
            return None

        exp_data = chain[chain["expiry"] == nearest_expiry].copy()
        dte = int(exp_data["dte"].iloc[0])
        T = dte / 365.0
        if T <= 0:
            return None

        forward = self._compute_forward(spot, self.r, T)

        # Separate OTM options: puts below forward, calls above forward
        otm_puts = exp_data[
            (exp_data["opt_type"] == "put") & (exp_data["strike"] <= forward)
        ].sort_values("strike")
        otm_calls = exp_data[
            (exp_data["opt_type"] == "call") & (exp_data["strike"] >= forward)
        ].sort_values("strike")

        if otm_puts.empty and otm_calls.empty:
            return None

        # Variance swap formula (discrete approximation):
        # Var_swap = (2/T) * sum( dK/K_i^2 * price_i ) - (1/T) * (F/K0 - 1)^2
        # where price_i = BS price of OTM option at K_i
        total = 0.0

        # OTM puts contribution
        if not otm_puts.empty:
            strikes_p = otm_puts["strike"].values
            ivs_p = otm_puts["iv"].values
            for j in range(len(strikes_p)):
                K = float(strikes_p[j])
                iv = float(ivs_p[j])
                if iv <= 0 or K <= 0:
                    continue
                price = _bs_put_price(spot, K, T, self.r, iv)
                # dK: midpoint spacing
                if j == 0:
                    dK = float(strikes_p[1] - strikes_p[0]) if len(strikes_p) > 1 else 1.0
                elif j == len(strikes_p) - 1:
                    dK = float(strikes_p[j] - strikes_p[j - 1])
                else:
                    dK = float(strikes_p[j + 1] - strikes_p[j - 1]) / 2.0
                total += (dK / (K ** 2)) * price

        # OTM calls contribution
        if not otm_calls.empty:
            strikes_c = otm_calls["strike"].values
            ivs_c = otm_calls["iv"].values
            for j in range(len(strikes_c)):
                K = float(strikes_c[j])
                iv = float(ivs_c[j])
                if iv <= 0 or K <= 0:
                    continue
                price = _bs_call_price(spot, K, T, self.r, iv)
                if j == 0:
                    dK = float(strikes_c[1] - strikes_c[0]) if len(strikes_c) > 1 else 1.0
                elif j == len(strikes_c) - 1:
                    dK = float(strikes_c[j] - strikes_c[j - 1])
                else:
                    dK = float(strikes_c[j + 1] - strikes_c[j - 1]) / 2.0
                total += (dK / (K ** 2)) * price

        # Fair variance = (2 * e^(rT) / T) * total - (1/T) * (F/K0 - 1)^2
        # K0 = first strike below forward
        K0 = forward
        if not otm_puts.empty:
            K0 = float(otm_puts["strike"].values[-1])

        fair_var = (2.0 * math.exp(self.r * T) / T) * total
        correction = (1.0 / T) * ((forward / K0 - 1.0) ** 2)
        fair_var -= correction

        if fair_var <= 0:
            return None

        var_swap_vol = math.sqrt(fair_var)

        log.info(
            "Variance swap level for {t}: {v:.4f} ({v100:.1f}%), expiry={e}",
            t=ticker, v=var_swap_vol, v100=var_swap_vol * 100, e=str(nearest_expiry),
        )

        return round(var_swap_vol, 4)

    # ── Helper methods ───────────────────────────────────────────────

    def _get_options_data(
        self, ticker: str, as_of_date: date,
    ) -> pd.DataFrame:
        """Query options_snapshots for a ticker.

        Tries exact snap_date first, falls back to most recent available.

        Parameters:
            ticker: Underlying ticker symbol.
            as_of_date: Target snapshot date.

        Returns:
            DataFrame with columns: strike, opt_type, oi, iv, expiry, dte,
            last_price, bid, ask, volume.
        """
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT strike, opt_type, open_interest, implied_vol,
                           expiry, (expiry - :snap_date) AS dte,
                           last_price, bid, ask, volume
                    FROM options_snapshots
                    WHERE ticker = :ticker AND snap_date = :snap_date
                    AND open_interest > 0 AND implied_vol > 0
                    AND expiry > :snap_date
                    ORDER BY expiry, strike
                """),
                {"ticker": ticker, "snap_date": as_of_date},
            ).fetchall()

        if not rows:
            # Fall back to most recent snap_date
            with self.engine.connect() as conn:
                latest = conn.execute(
                    text(
                        "SELECT MAX(snap_date) FROM options_snapshots "
                        "WHERE ticker = :t"
                    ),
                    {"t": ticker},
                ).fetchone()
                if latest and latest[0] and latest[0] != as_of_date:
                    return self._get_options_data(ticker, latest[0])
            return pd.DataFrame()

        df = pd.DataFrame(rows, columns=[
            "strike", "opt_type", "oi", "iv", "expiry", "dte",
            "last_price", "bid", "ask", "volume",
        ])
        df["dte"] = df["dte"].apply(lambda x: x.days if hasattr(x, "days") else int(x))
        return df[df["dte"] > 0]

    def _get_spot_price(
        self, ticker: str, as_of_date: date | None = None,
    ) -> float:
        """Get spot price from options_daily_signals or resolved_series.

        Parameters:
            ticker: Underlying ticker symbol.
            as_of_date: Reference date.

        Returns:
            Spot price, or 0.0 if unavailable.
        """
        if as_of_date is None:
            as_of_date = date.today()

        with self.engine.connect() as conn:
            # Try options_daily_signals first (has spot_price)
            row = conn.execute(
                text("""
                    SELECT spot_price FROM options_daily_signals
                    WHERE ticker = :ticker AND signal_date <= :d
                    ORDER BY signal_date DESC LIMIT 1
                """),
                {"ticker": ticker, "d": as_of_date},
            ).fetchone()
            if row and row[0]:
                return float(row[0])

            # Try resolved_series (yfinance close)
            row = conn.execute(
                text("""
                    SELECT rs.value FROM resolved_series rs
                    JOIN feature_registry fr ON rs.feature_id = fr.id
                    WHERE (fr.name = :name1 OR fr.name = :name2)
                    AND rs.obs_date <= :d
                    ORDER BY rs.obs_date DESC LIMIT 1
                """),
                {
                    "name1": f"{ticker.lower()}_close",
                    "name2": ticker.lower(),
                    "d": as_of_date,
                },
            ).fetchone()
            if row:
                return float(row[0])

            # Fallback: ATM strike from options chain by max OI
            row = conn.execute(
                text("""
                    SELECT strike FROM options_snapshots
                    WHERE ticker = :t AND snap_date = :d AND opt_type = 'call'
                    ORDER BY open_interest DESC LIMIT 1
                """),
                {"t": ticker, "d": as_of_date},
            ).fetchone()
            return float(row[0]) if row else 0.0

    def _compute_forward(
        self, spot: float, rate: float, dte: float,
    ) -> float:
        """Compute forward price: F = S * exp(r * T).

        Parameters:
            spot: Current spot price.
            rate: Annual risk-free rate.
            dte: Time to expiry in years.

        Returns:
            Forward price.
        """
        return spot * math.exp(rate * dte)

    def _delta_to_strike(
        self,
        delta: float,
        forward: float,
        iv: float,
        T: float,
        is_call: bool = True,
    ) -> float:
        """Invert Black-Scholes delta to find the corresponding strike.

        For a call: delta = N(d1), so d1 = N_inv(delta)
        For a put: delta = N(d1) - 1, so d1 = N_inv(delta + 1)

        Then: K = F * exp(-d1 * sigma * sqrt(T) + 0.5 * sigma^2 * T)

        Parameters:
            delta: Target delta (positive for calls, negative for puts).
            forward: Forward price.
            iv: Implied volatility estimate (ATM IV used as proxy).
            T: Time to expiry in years.
            is_call: True for call delta, False for put delta.

        Returns:
            Strike price corresponding to the target delta.
        """
        if T <= 0 or iv <= 0 or forward <= 0:
            return forward

        if is_call:
            d1 = norm.ppf(max(0.001, min(0.999, delta)))
        else:
            # Put delta is negative; delta = N(d1) - 1 => d1 = N_inv(delta + 1)
            d1 = norm.ppf(max(0.001, min(0.999, delta + 1.0)))

        K = forward * math.exp(-d1 * iv * math.sqrt(T) + 0.5 * iv ** 2 * T)
        return K

    def _interpolate_iv(
        self, chain_slice: pd.DataFrame, target_strike: float,
    ) -> float | None:
        """Linearly interpolate IV at a target strike from the chain.

        Parameters:
            chain_slice: DataFrame with strike and iv columns.
            target_strike: Strike to interpolate to.

        Returns:
            Interpolated IV or None if insufficient data.
        """
        if chain_slice.empty or len(chain_slice) < 2:
            return None

        sorted_slice = chain_slice.sort_values("strike")
        strikes = sorted_slice["strike"].values
        ivs = sorted_slice["iv"].values

        # Clamp to bounds
        if target_strike <= strikes[0]:
            return float(ivs[0])
        if target_strike >= strikes[-1]:
            return float(ivs[-1])

        # Find bracketing strikes and interpolate
        idx = np.searchsorted(strikes, target_strike)
        K_lo = float(strikes[idx - 1])
        K_hi = float(strikes[idx])
        iv_lo = float(ivs[idx - 1])
        iv_hi = float(ivs[idx])

        if K_hi == K_lo:
            return iv_lo

        weight = (target_strike - K_lo) / (K_hi - K_lo)
        return iv_lo + weight * (iv_hi - iv_lo)

    def _build_interpolated_grid(
        self,
        fitted_surface: dict[str, dict[str, Any]],
        spot: float,
    ) -> list[dict[str, Any]]:
        """Build a regular moneyness x DTE grid from SVI fits.

        Parameters:
            fitted_surface: Per-expiry SVI parameters from build_surface.
            spot: Current spot price.

        Returns:
            List of grid points with moneyness, dte, and interpolated IV.
        """
        if not fitted_surface:
            return []

        grid_points: list[dict[str, Any]] = []

        # Collect available DTEs and their SVI params
        available = []
        for expiry_str, data in fitted_surface.items():
            svi = data.get("svi", {})
            if not svi.get("converged", False) and svi.get("rmse", float("inf")) > 0.1:
                continue
            available.append((data["dte"], svi))

        available.sort(key=lambda x: x[0])
        if not available:
            return []

        for m in MONEYNESS_GRID:
            for target_dte in DTE_GRID:
                # Find the closest available DTE(s) for interpolation
                T_target = target_dte / 365.0
                forward = self._compute_forward(spot, self.r, T_target)
                k = math.log(m * spot / forward)  # log-moneyness vs forward

                # Find bracketing DTEs
                below = [(d, s) for d, s in available if d <= target_dte]
                above = [(d, s) for d, s in available if d >= target_dte]

                iv_est = None

                if below and above:
                    d_lo, svi_lo = below[-1]
                    d_hi, svi_hi = above[0]

                    if d_lo == d_hi:
                        # Exact match
                        T = d_lo / 365.0
                        w = self._svi_w(svi_lo, k)
                        iv_est = math.sqrt(max(w / T, 0)) if T > 0 else None
                    else:
                        # Linear interpolation in total variance space
                        T_lo = d_lo / 365.0
                        T_hi = d_hi / 365.0
                        w_lo = self._svi_w(svi_lo, k)
                        w_hi = self._svi_w(svi_hi, k)

                        weight = (target_dte - d_lo) / (d_hi - d_lo)
                        w_interp = w_lo + weight * (w_hi - w_lo)
                        iv_est = math.sqrt(max(w_interp / T_target, 0)) if T_target > 0 else None
                elif below:
                    # Extrapolate from nearest
                    d, svi = below[-1]
                    T = d / 365.0
                    w = self._svi_w(svi, k)
                    iv_est = math.sqrt(max(w / T_target, 0)) if T_target > 0 else None
                elif above:
                    d, svi = above[0]
                    T = d / 365.0
                    w = self._svi_w(svi, k)
                    iv_est = math.sqrt(max(w / T_target, 0)) if T_target > 0 else None

                if iv_est is not None and 0.01 < iv_est < 5.0:
                    grid_points.append({
                        "moneyness": m,
                        "dte": target_dte,
                        "iv": round(iv_est, 4),
                    })

        return grid_points

    @staticmethod
    def _svi_w(params: dict[str, Any], k: float) -> float:
        """Evaluate SVI total variance at log-moneyness k.

        Parameters:
            params: SVI parameters {a, b, rho, m, sigma}.
            k: Log-moneyness value.

        Returns:
            Total implied variance w(k).
        """
        a = params["a"]
        b = params["b"]
        rho = params["rho"]
        m = params["m"]
        sigma = params["sigma"]
        return a + b * (rho * (k - m) + math.sqrt((k - m) ** 2 + sigma ** 2))


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    from db import get_engine

    engine = get_engine()
    vse = VolSurfaceEngine(engine)

    # Build surface for SPY
    surface = vse.build_surface("SPY")
    if "error" not in surface:
        print(f"SPY surface: {surface['metadata']}")
        print(f"  Raw points: {len(surface['raw_points'])}")
        print(f"  Grid points: {len(surface['grid'])}")

        # Arbitrage detection
        arbs = vse.detect_arbitrage(surface)
        print(f"  Arbitrage violations: {len(arbs)}")
        for arb in arbs[:5]:
            print(f"    {arb['type']}: {arb['detail']}")
    else:
        print(f"SPY surface error: {surface['error']}")

    # Skew
    skew = vse.compute_skew("SPY")
    for s in skew[:3]:
        print(
            f"  Skew {s['expiry']}: ATM={s['atm_iv']:.1%} "
            f"Skew={s['skew']:.4f} RR={s['risk_reversal']:.4f} "
            f"BF={s['butterfly']:.4f}"
        )

    # Term structure
    ts = vse.compute_term_structure("SPY")
    print(f"  Term structure: slope={ts['slope']}, inverted={ts['is_inverted']}")

    # Historical percentile
    pct = vse.historical_percentile("SPY")
    print(
        f"  IV percentile={pct.get('iv_percentile')}%, "
        f"rank={pct.get('iv_rank')}%, "
        f"z-score={pct.get('z_score')}"
    )

    # Variance swap
    var_swap = vse.variance_swap_level("SPY")
    if var_swap:
        print(f"  Variance swap level: {var_swap:.4f} ({var_swap*100:.1f}%)")

    # Greeks grid
    greeks = vse.compute_greeks_grid("SPY")
    if "error" not in greeks:
        print(f"  Greeks grid: {greeks['summary']}")
