"""
GRID — Dealer gamma exposure and hedging flow mechanics.

Implements Cem Karsan's core insight: when dealers are short gamma (negative GEX),
they must hedge by buying into rallies and selling into drops — amplifying moves.
When dealers are long gamma (positive GEX), they do the opposite — dampening moves.

Key outputs:
  - GEX (Gamma Exposure Index): net dealer gamma at each strike and aggregate
  - Gamma flip point: spot price where GEX crosses zero
  - Dealer delta: net delta dealers need to hedge
  - GEX profile: gamma exposure vs spot price curve
  - Gamma wall: strike with maximum absolute gamma exposure
  - Put wall: strike with maximum put gamma (support level)
  - Call wall: strike with maximum call gamma (resistance level)
  - Vanna exposure: sensitivity of dealer delta to IV changes
  - Charm exposure: sensitivity of dealer delta to time decay

Dealer position assumption: dealers are NET SHORT options (market-making).
Retail/institutional clients BUY options; dealers sell them and delta-hedge.
This means for calls: dealer is short call = short gamma at strike.
For puts: dealer is short put = long gamma at strike (put gamma is negative
for the buyer, so dealer who is short the put has positive gamma).

Wait — that's wrong. Let's be precise:
  - Dealer SHORT a call: gamma is NEGATIVE (they get shorter delta as spot rises)
  - Dealer SHORT a put: gamma is POSITIVE (they get longer delta as spot drops)
  - GEX = Σ(call_OI × call_gamma × 100 × spot) - Σ(put_OI × put_gamma × 100 × spot)
  - When GEX > 0: dealer is long gamma (stabilizing flows)
  - When GEX < 0: dealer is short gamma (amplifying flows)
"""

from __future__ import annotations

import math
from datetime import date, datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# GEX-6: route Black-Scholes Greeks through the canonical primitives in
# `physics/greeks/black_scholes.py` (landed in GEX-3, #79). The local _d1/_d2/
# bs_* helpers below used to be a hand-rolled duplicate; they now forward to
# the canonical module and stay only as thin shims for backward compatibility
# with any external caller still importing them by name.
from physics.greeks import black_scholes as _bs
from store.availability import unavailable

# ── Black-Scholes Greeks (shims over physics/greeks/black_scholes) ───────────


def _d1(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Black-Scholes d1 — shim over physics.greeks.black_scholes.d1."""
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return 0.0
    return float(_bs.d1(S, K, T, r, sigma))


def _d2(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Black-Scholes d2 — shim over physics.greeks.black_scholes.d2."""
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return 0.0
    return float(_bs.d2(S, K, T, r, sigma))


def bs_gamma(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Gamma of an option (same for calls and puts)."""
    if T <= 0 or sigma <= 0 or S <= 0:
        return 0.0
    return float(_bs.gamma(S, K, T, r, sigma))


def bs_delta_call(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Call delta. Preserves the local intrinsic-at-expiry fallback."""
    if T <= 0 or sigma <= 0:
        return 1.0 if S > K else 0.0
    return float(_bs.delta(S, K, T, r, sigma, is_call=True))


def bs_delta_put(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Put delta = call delta − 1. Preserves the local intrinsic-at-expiry fallback."""
    if T <= 0 or sigma <= 0:
        return 0.0 if S > K else -1.0
    return float(_bs.delta(S, K, T, r, sigma, is_call=False))


def bs_vanna(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Vanna: dDelta/dVol = dVega/dSpot. Same for calls and puts."""
    if T <= 0 or sigma <= 0 or S <= 0:
        return 0.0
    return float(_bs.vanna(S, K, T, r, sigma))


def bs_charm(S: float, K: float, T: float, r: float, sigma: float, is_call: bool = True) -> float:
    """Charm: dDelta/dTime (delta decay). Negative T means time passing."""
    if T <= 1e-6 or sigma <= 0 or S <= 0:
        return 0.0
    return float(_bs.charm(S, K, T, r, sigma, is_call=is_call))


# ── GEX Computation Engine ───────────────────────────────────────────

class DealerGammaEngine:
    """Computes aggregate dealer gamma exposure from options chain data.

    Parameters:
        db_engine: SQLAlchemy engine for reading options_snapshots.
        risk_free_rate: Annual risk-free rate (default 5%).
    """

    def __init__(self, db_engine: Engine, risk_free_rate: float = 0.05) -> None:
        self.engine = db_engine
        self.r = risk_free_rate

    def compute_gex_profile(
        self,
        ticker: str,
        snap_date: date | None = None,
        spot_range_pct: float = 0.15,
        n_points: int = 50,
    ) -> dict[str, Any]:
        """Compute the full GEX profile for a ticker.

        Returns:
            Dictionary with:
            - gex_aggregate: float (total GEX at current spot)
            - gamma_flip: float (spot price where GEX = 0)
            - gamma_wall: float (strike with max |GEX|)
            - put_wall: float (strike with max put gamma OI)
            - call_wall: float (strike with max call gamma OI)
            - dealer_delta: float (net delta dealers must hedge)
            - regime: str (LONG_GAMMA / SHORT_GAMMA / NEUTRAL)
            - profile: list of {spot, gex} for charting
            - per_strike: list of {strike, call_gex, put_gex, net_gex}
            - vanna_exposure: float (aggregate vanna)
            - charm_exposure: float (aggregate charm)
        """
        if snap_date is None:
            snap_date = date.today()

        chain = self._load_chain(ticker, snap_date)
        if chain.empty:
            return {"error": f"No options data for {ticker} on {snap_date}", "ticker": ticker}

        chain_completed_at = chain.attrs.get("capture_completed_at")
        spot_receipt = self._get_spot_receipt(ticker, chain_completed_at)
        if spot_receipt is None:
            # Explicitly unavailable (store/availability contract); `error`
            # stays because every consumer keys on it.
            result = unavailable(
                f"no verified prior close for {ticker} available at the "
                f"{snap_date} options snapshot",
                source="spy_close_receipt",
            )
            result.update({"ticker": ticker, "error": f"No spot price for {ticker}"})
            return result
        spot = spot_receipt["price"]

        # Compute per-strike Greeks and GEX
        per_strike = self._compute_per_strike(chain, spot)

        # Aggregate GEX at the prior verified close used as reference spot
        gex_agg = sum(s["net_gex"] for s in per_strike)

        # Find gamma flip (spot where GEX crosses zero)
        gamma_flip = self._find_gamma_flip(chain, spot, spot_range_pct, n_points)

        # Gamma/put/call walls
        gamma_wall = max(per_strike, key=lambda s: abs(s["net_gex"]), default={}).get("strike", spot)
        put_wall = max(
            [s for s in per_strike if s["put_gex"] > 0],
            key=lambda s: s["put_gex"], default={},
        ).get("strike", 0)
        call_wall = max(
            [s for s in per_strike if s["call_gex"] < 0],
            key=lambda s: abs(s["call_gex"]), default={},
        ).get("strike", 0)

        # Dealer delta
        dealer_delta = sum(s.get("dealer_delta", 0) for s in per_strike)

        # Aggregate vanna and charm
        vanna_agg = sum(s.get("vanna", 0) for s in per_strike)
        charm_agg = sum(s.get("charm", 0) for s in per_strike)

        # GEX profile curve
        profile = self._compute_profile_curve(chain, spot, spot_range_pct, n_points)

        # Regime classification
        gex_normalized = gex_agg / (spot * 1e6) if spot > 0 else 0
        if gex_normalized > 0.5:
            regime = "LONG_GAMMA"
        elif gex_normalized < -0.5:
            regime = "SHORT_GAMMA"
        else:
            regime = "NEUTRAL"

        return {
            "ticker": ticker,
            "snap_date": str(snap_date),
            "chain_snap_date": chain.attrs["snap_date"].isoformat(),
            "chain_batch_id": chain.attrs["batch_id"],
            "chain_capture_ordinal": chain.attrs["capture_ordinal"],
            "chain_capture_started_at": chain.attrs["capture_started_at"].isoformat(),
            "chain_capture_completed_at": chain_completed_at.isoformat(),
            "chain_created_at": chain.attrs["created_at_min"].isoformat(),
            "chain_created_at_max": chain.attrs["created_at_max"].isoformat(),
            "spot": round(spot, 2),
            "spot_source": "spy_close_receipt",
            "spot_basis": "prior_completed_unadjusted_close",
            "spot_obs_date": spot_receipt["obs_date"].isoformat(),
            "spot_available_at": spot_receipt["available_at"].isoformat(),
            "spot_receipt_created_at": spot_receipt["receipt_created_at"].isoformat(),
            "spot_release_date": spot_receipt["release_date"].isoformat(),
            "spot_vintage_date": spot_receipt["vintage_date"].isoformat(),
            "spot_receipt_id": spot_receipt["receipt_id"],
            "estimated": True,
            "basis": "options_open_interest_with_assumed_dealer_sign_and_black_scholes",
            "gex_aggregate": round(gex_agg, 0),
            "gex_normalized": round(gex_normalized, 4),
            "gamma_flip": round(gamma_flip, 2) if gamma_flip else None,
            "gamma_wall": round(gamma_wall, 2),
            "put_wall": round(put_wall, 2) if put_wall else None,
            "call_wall": round(call_wall, 2) if call_wall else None,
            "dealer_delta": round(dealer_delta, 0),
            "vanna_exposure": round(vanna_agg, 0),
            "charm_exposure": round(charm_agg, 0),
            "regime": regime,
            "profile": profile,
            "per_strike": [
                {k: round(v, 4) if isinstance(v, float) else v for k, v in s.items()}
                for s in per_strike[:30]
            ],
        }

    def _compute_per_strike(self, chain: pd.DataFrame, spot: float) -> list[dict]:
        """Compute GEX, delta, vanna, charm per strike."""
        results = []
        grouped = chain.groupby("strike")

        for strike, group in grouped:
            T = group["dte"].iloc[0] / 365.0
            if T <= 0:
                continue

            call_rows = group[group["opt_type"] == "call"]
            put_rows = group[group["opt_type"] == "put"]

            call_oi = float(call_rows["open_interest"].sum()) if not call_rows.empty else 0
            put_oi = float(put_rows["open_interest"].sum()) if not put_rows.empty else 0

            call_iv = float(call_rows["implied_volatility"].mean()) if not call_rows.empty and call_rows["implied_volatility"].mean() > 0 else 0.25
            put_iv = float(put_rows["implied_volatility"].mean()) if not put_rows.empty and put_rows["implied_volatility"].mean() > 0 else 0.25

            K = float(strike)

            # Gamma per option × OI × 100 shares × spot (dollar gamma)
            call_gamma = bs_gamma(spot, K, T, self.r, call_iv) * call_oi * 100 * spot
            put_gamma = bs_gamma(spot, K, T, self.r, put_iv) * put_oi * 100 * spot

            # Dealer is SHORT options → dealer call GEX is negative, put GEX is positive
            call_gex = -call_gamma  # dealer short calls = short gamma
            put_gex = put_gamma     # dealer short puts = long gamma (put gamma is positive from dealer side)

            # Dealer delta (short calls = negative delta, short puts = positive delta)
            call_delta = -bs_delta_call(spot, K, T, self.r, call_iv) * call_oi * 100
            put_delta = -bs_delta_put(spot, K, T, self.r, put_iv) * put_oi * 100

            # Vanna and charm (aggregate across OI)
            v = bs_vanna(spot, K, T, self.r, (call_iv + put_iv) / 2)
            c = bs_charm(spot, K, T, self.r, (call_iv + put_iv) / 2)
            total_oi = call_oi + put_oi
            vanna_val = -v * total_oi * 100  # dealer is short → negate
            charm_val = -c * total_oi * 100

            results.append({
                "strike": K,
                "call_oi": call_oi,
                "put_oi": put_oi,
                "call_gex": call_gex,
                "put_gex": put_gex,
                "net_gex": call_gex + put_gex,
                "dealer_delta": call_delta + put_delta,
                "vanna": vanna_val,
                "charm": charm_val,
                "dte": float(group["dte"].iloc[0]),
            })

        results.sort(key=lambda x: x["strike"])
        return results

    def _prepare_chain_arrays(self, chain: pd.DataFrame) -> tuple:
        """Pre-extract numpy arrays from chain for vectorized GEX computation.

        Returns (strikes, T_arr, iv_arr, oi_arr, sign_arr) where sign_arr
        is -1 for calls (dealer short gamma) and +1 for puts (dealer long gamma).
        Only rows with dte > 0 are included.
        """
        valid = chain[chain["dte"] > 0].copy()
        if valid.empty:
            return (np.array([]), np.array([]), np.array([]),
                    np.array([]), np.array([]))

        strikes = valid["strike"].to_numpy(dtype=np.float64)
        T_arr = valid["dte"].to_numpy(dtype=np.float64) / 365.0
        iv_arr = valid["implied_volatility"].to_numpy(dtype=np.float64)
        iv_arr = np.where(iv_arr > 0, iv_arr, 0.25)
        oi_arr = valid["open_interest"].to_numpy(dtype=np.float64)
        sign_arr = np.where(valid["opt_type"].to_numpy() == "call", -1.0, 1.0)
        return strikes, T_arr, iv_arr, oi_arr, sign_arr

    def _gex_at_spots_vectorized(
        self, strikes: np.ndarray, T_arr: np.ndarray,
        iv_arr: np.ndarray, oi_arr: np.ndarray,
        sign_arr: np.ndarray, spots: np.ndarray,
    ) -> np.ndarray:
        """Compute aggregate GEX at multiple spot prices using vectorized math.

        Parameters are pre-extracted chain arrays (from _prepare_chain_arrays)
        and a 1-D array of spot prices. Returns a 1-D array of GEX values,
        one per spot price.
        """
        if len(strikes) == 0:
            return np.zeros(len(spots))

        # Broadcast: spots (N,1) vs chain arrays (M,)
        S = spots[:, np.newaxis]          # (N, 1)
        K = strikes[np.newaxis, :]        # (1, M)
        T = T_arr[np.newaxis, :]          # (1, M)
        sigma = iv_arr[np.newaxis, :]     # (1, M)
        oi = oi_arr[np.newaxis, :]        # (1, M)
        sign = sign_arr[np.newaxis, :]    # (1, M)

        # Vectorized Black-Scholes gamma
        sqrt_T = np.sqrt(T)
        d1 = (np.log(S / K) + (self.r + 0.5 * sigma**2) * T) / (sigma * sqrt_T)
        pdf_d1 = np.exp(-0.5 * d1**2) / np.sqrt(2.0 * np.pi)
        gamma = pdf_d1 / (S * sigma * sqrt_T)

        # Dollar gamma per contract × OI × 100 shares × spot
        dollar_gamma = gamma * oi * 100.0 * S  # (N, M)

        # Apply dealer sign and sum across chain dimension
        gex = np.sum(sign * dollar_gamma, axis=1)  # (N,)
        return gex

    def _find_gamma_flip(
        self, chain: pd.DataFrame, spot: float,
        range_pct: float, n_points: int,
    ) -> float | None:
        """Find the spot price where aggregate GEX crosses zero."""
        lo = spot * (1 - range_pct)
        hi = spot * (1 + range_pct)
        prices = np.linspace(lo, hi, n_points)

        arrays = self._prepare_chain_arrays(chain)
        gex_values = self._gex_at_spots_vectorized(*arrays, prices)

        # Find first sign change
        for i in range(1, len(gex_values)):
            if gex_values[i - 1] * gex_values[i] < 0:
                prev_gex = gex_values[i - 1]
                curr_gex = gex_values[i]
                ratio = abs(prev_gex) / (abs(prev_gex) + abs(curr_gex) + 1e-12)
                return float(prices[i - 1] + ratio * (prices[i] - prices[i - 1]))

        return None

    def _gex_at_spot(self, chain: pd.DataFrame, spot: float) -> float:
        """Compute aggregate GEX at a hypothetical spot price."""
        arrays = self._prepare_chain_arrays(chain)
        spots = np.array([spot])
        result = self._gex_at_spots_vectorized(*arrays, spots)
        return float(result[0])

    def _compute_profile_curve(
        self, chain: pd.DataFrame, spot: float,
        range_pct: float, n_points: int,
    ) -> list[dict]:
        """GEX vs spot price curve for charting."""
        lo = spot * (1 - range_pct)
        hi = spot * (1 + range_pct)
        prices = np.linspace(lo, hi, n_points)

        arrays = self._prepare_chain_arrays(chain)
        gex_values = self._gex_at_spots_vectorized(*arrays, prices)

        return [
            {"spot": round(float(p), 2), "gex": round(float(g), 0)}
            for p, g in zip(prices, gex_values)
        ]

    def _load_chain(self, ticker: str, snap_date: date) -> pd.DataFrame:
        """Load only the requested day's chain; reject mixed or late captures.

        Only one fully completed capture is eligible. A legacy or partially
        published chain without ordinal, start, batch, and completion provenance fails
        closed, as do rows mixed with an older writer.
        """
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT strike, opt_type, open_interest, implied_vol AS implied_volatility,
                       expiry, (expiry - :snap_date) AS dte, created_at,
                       capture_batch_id, capture_ordinal,
                       capture_started_at, capture_completed_at
                FROM options_snapshots
                WHERE ticker = :ticker AND snap_date = :snap_date
                ORDER BY expiry, strike, opt_type
            """), {"ticker": ticker, "snap_date": snap_date}).fetchall()

        if not rows:
            return pd.DataFrame()

        created = [row[6] for row in rows]
        batches = {row[7] for row in rows}
        ordinals = {row[8] for row in rows}
        starts = {row[9] for row in rows}
        completions = {row[10] for row in rows}
        now = datetime.now(timezone.utc)
        if (any(not isinstance(ts, datetime) or ts.tzinfo is None for ts in created)
                or len(batches) != 1 or not next(iter(batches))
                or len(ordinals) != 1
                or len(starts) != 1
                or len(completions) != 1):
            return pd.DataFrame()
        batch_id = next(iter(batches))
        ordinal = next(iter(ordinals))
        started_at = next(iter(starts))
        completed_at = next(iter(completions))
        try:
            UUID(batch_id)
        except (TypeError, ValueError, AttributeError):
            return pd.DataFrame()
        if not isinstance(ordinal, int) or isinstance(ordinal, bool) or ordinal <= 0:
            return pd.DataFrame()
        if (not isinstance(started_at, datetime) or started_at.tzinfo is None
                or not isinstance(completed_at, datetime) or completed_at.tzinfo is None):
            return pd.DataFrame()
        first, last = min(created), max(created)
        if (first.astimezone(timezone.utc).date() != snap_date
                or last.astimezone(timezone.utc).date() != snap_date
                or started_at.astimezone(timezone.utc).date() != snap_date
                or completed_at.astimezone(timezone.utc).date() != snap_date
                or started_at > completed_at or completed_at > now):
            return pd.DataFrame()

        df = pd.DataFrame(rows, columns=["strike", "opt_type", "open_interest",
                                          "implied_volatility", "expiry", "dte",
                                          "created_at", "capture_batch_id", "capture_ordinal",
                                          "capture_started_at", "capture_completed_at"])
        df["dte"] = df["dte"].apply(lambda x: x.days if hasattr(x, 'days') else int(x))
        df = df[(df["dte"] > 0)
                & (df["open_interest"] > 0)
                & (df["implied_volatility"] > 0)].copy()
        df.attrs.update(snap_date=snap_date, batch_id=batch_id,
                        capture_ordinal=ordinal,
                        capture_started_at=started_at,
                        capture_completed_at=completed_at,
                        created_at_min=first, created_at_max=last)
        return df

    def _get_spot_receipt(self, ticker: str, chain_completed_at: datetime | None) -> dict | None:
        """Use only a verified SPY close known before pull completion.

        The shared receipt verifier checks raw/resolved identity, the unadjusted
        price basis, the post-observation-day marker, and a four-calendar-day
        maximum age. Other tickers have no equivalent receipt contract yet.
        """
        if ticker != "SPY" or chain_completed_at is None:
            return None
        from store.astrogrid import AstroGridStore

        with self.engine.connect() as conn:
            receipt = AstroGridStore(self.engine)._verified_spy_receipt(
                conn, cutoff=chain_completed_at, mode="entry")
        if receipt is None:
            return None
        created = receipt.get("receipt_created_at")
        available = receipt.get("available_at")
        observed = receipt.get("obs_date")
        price = receipt.get("price")
        chain_day = chain_completed_at.astimezone(timezone.utc).date()
        if (not isinstance(created, datetime) or created.tzinfo is None
                or not isinstance(available, datetime) or available.tzinfo is None
                or not isinstance(observed, date)
                or not 1 <= (chain_day - observed).days <= 4
                or available < datetime.combine(
                    observed + timedelta(days=1), datetime.min.time(), timezone.utc)
                or available > chain_completed_at
                or not available <= created <= chain_completed_at
                or not isinstance(price, (int, float)) or not math.isfinite(price)
                or price <= 0
                or receipt.get("conflict_flag") is not False
                or not isinstance(receipt.get("release_date"), date)
                or not isinstance(receipt.get("vintage_date"), date)
                or receipt["release_date"] > chain_day
                or receipt["vintage_date"] > chain_day):
            return None
        return receipt

    # ── Convenience methods ──────────────────────────────────────────

    def compute_all_tickers(self, snap_date: date | None = None) -> list[dict]:
        """Run GEX analysis for all tickers with options data."""
        from ingestion.options import EQUITY_TICKERS

        results = []
        for ticker in EQUITY_TICKERS:
            try:
                result = self.compute_gex_profile(ticker, snap_date)
                if "error" not in result:
                    results.append(result)
            except Exception as exc:
                log.debug("GEX for {t} failed: {e}", t=ticker, e=str(exc))

        # Sort by absolute GEX (most significant first)
        results.sort(key=lambda r: abs(r.get("gex_aggregate", 0)), reverse=True)
        log.info("GEX computed for {n} tickers", n=len(results))
        return results

    def get_market_gex_summary(self, snap_date: date | None = None) -> dict:
        """Aggregate GEX summary across all tickers.

        Returns the macro-level dealer positioning picture:
        - Is the market in aggregate long or short gamma?
        - Where are the key support/resistance gamma walls?
        - How much vanna/charm exposure is outstanding?
        """
        results = self.compute_all_tickers(snap_date)
        if not results:
            return {"error": "No GEX data available"}

        # SPY is the market proxy
        spy = next((r for r in results if r["ticker"] == "SPY"), None)

        total_gex = sum(r["gex_aggregate"] for r in results)
        total_vanna = sum(r["vanna_exposure"] for r in results)
        total_charm = sum(r["charm_exposure"] for r in results)

        long_gamma = [r for r in results if r["regime"] == "LONG_GAMMA"]
        short_gamma = [r for r in results if r["regime"] == "SHORT_GAMMA"]

        return {
            "snap_date": str(snap_date or date.today()),
            "total_tickers": len(results),
            "aggregate_gex": round(total_gex, 0),
            "aggregate_vanna": round(total_vanna, 0),
            "aggregate_charm": round(total_charm, 0),
            "long_gamma_count": len(long_gamma),
            "short_gamma_count": len(short_gamma),
            "market_regime": spy["regime"] if spy else "UNKNOWN",
            "spy_spot": spy["spot"] if spy else None,
            "spy_gamma_flip": spy["gamma_flip"] if spy else None,
            "spy_put_wall": spy["put_wall"] if spy else None,
            "spy_call_wall": spy["call_wall"] if spy else None,
            "spy_gex": spy["gex_aggregate"] if spy else None,
            "tickers": [
                {
                    "ticker": r["ticker"],
                    "spot": r["spot"],
                    "gex": r["gex_aggregate"],
                    "regime": r["regime"],
                    "gamma_flip": r["gamma_flip"],
                    "vanna": r["vanna_exposure"],
                    "charm": r["charm_exposure"],
                }
                for r in results
            ],
        }
