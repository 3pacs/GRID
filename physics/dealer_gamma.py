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
from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log
from scipy.stats import norm
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Black-Scholes Greeks ─────────────────────────────────────────────

def _d1(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Black-Scholes d1."""
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return 0.0
    return (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))


def _d2(S: float, K: float, T: float, r: float, sigma: float) -> float:
    return _d1(S, K, T, r, sigma) - sigma * math.sqrt(T)


def bs_gamma(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Gamma of an option (same for calls and puts)."""
    if T <= 0 or sigma <= 0 or S <= 0:
        return 0.0
    d1 = _d1(S, K, T, r, sigma)
    return norm.pdf(d1) / (S * sigma * math.sqrt(T))


def bs_delta_call(S: float, K: float, T: float, r: float, sigma: float) -> float:
    if T <= 0 or sigma <= 0:
        return 1.0 if S > K else 0.0
    return norm.cdf(_d1(S, K, T, r, sigma))


def bs_delta_put(S: float, K: float, T: float, r: float, sigma: float) -> float:
    return bs_delta_call(S, K, T, r, sigma) - 1.0


def bs_vanna(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Vanna: dDelta/dVol = dVega/dSpot. Same for calls and puts."""
    if T <= 0 or sigma <= 0 or S <= 0:
        return 0.0
    d1 = _d1(S, K, T, r, sigma)
    d2 = d1 - sigma * math.sqrt(T)
    return -norm.pdf(d1) * d2 / sigma


def bs_charm(S: float, K: float, T: float, r: float, sigma: float, is_call: bool = True) -> float:
    """Charm: dDelta/dTime (delta decay). Negative T means time passing."""
    if T <= 1e-6 or sigma <= 0 or S <= 0:
        return 0.0
    d1 = _d1(S, K, T, r, sigma)
    d2 = d1 - sigma * math.sqrt(T)
    pdf_d1 = norm.pdf(d1)

    charm_val = -pdf_d1 * (
        2 * r * T - d2 * sigma * math.sqrt(T)
    ) / (2 * T * sigma * math.sqrt(T))

    if not is_call:
        charm_val += r * math.exp(-r * T) * norm.cdf(-d2)
    else:
        charm_val -= r * math.exp(-r * T) * norm.cdf(d2)

    return charm_val


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

        spot = self._get_spot(ticker, snap_date)
        if spot <= 0:
            return {"error": f"No spot price for {ticker}", "ticker": ticker}

        # Compute per-strike Greeks and GEX
        per_strike = self._compute_per_strike(chain, spot)

        # Aggregate GEX at current spot
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
            "spot": round(spot, 2),
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

    def _find_gamma_flip(
        self, chain: pd.DataFrame, spot: float,
        range_pct: float, n_points: int,
    ) -> float | None:
        """Find the spot price where aggregate GEX crosses zero."""
        lo = spot * (1 - range_pct)
        hi = spot * (1 + range_pct)
        prices = np.linspace(lo, hi, n_points)
        prev_gex = None

        for price in prices:
            gex = self._gex_at_spot(chain, price)
            if prev_gex is not None and prev_gex * gex < 0:
                # Linear interpolation
                ratio = abs(prev_gex) / (abs(prev_gex) + abs(gex) + 1e-12)
                return float(prices[max(0, np.searchsorted(prices, price) - 1)] +
                             ratio * (price - prices[max(0, np.searchsorted(prices, price) - 1)]))
            prev_gex = gex

        return None

    def _gex_at_spot(self, chain: pd.DataFrame, spot: float) -> float:
        """Compute aggregate GEX at a hypothetical spot price."""
        total = 0.0
        for _, row in chain.iterrows():
            T = row["dte"] / 365.0
            if T <= 0:
                continue
            K = float(row["strike"])
            iv = float(row["implied_volatility"]) if row["implied_volatility"] > 0 else 0.25
            oi = float(row["open_interest"])
            g = bs_gamma(spot, K, T, self.r, iv) * oi * 100 * spot

            if row["opt_type"] == "call":
                total -= g  # dealer short call = short gamma
            else:
                total += g  # dealer short put = long gamma
        return total

    def _compute_profile_curve(
        self, chain: pd.DataFrame, spot: float,
        range_pct: float, n_points: int,
    ) -> list[dict]:
        """GEX vs spot price curve for charting."""
        lo = spot * (1 - range_pct)
        hi = spot * (1 + range_pct)
        prices = np.linspace(lo, hi, n_points)

        return [
            {"spot": round(float(p), 2), "gex": round(self._gex_at_spot(chain, p), 0)}
            for p in prices
        ]

    def _load_chain(self, ticker: str, snap_date: date) -> pd.DataFrame:
        """Load options chain from database."""
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT strike, opt_type, open_interest, implied_volatility,
                       expiry, (expiry - :snap_date) AS dte
                FROM options_snapshots
                WHERE ticker = :ticker AND snap_date = :snap_date
                AND open_interest > 0 AND implied_volatility > 0
                AND expiry > :snap_date
                ORDER BY expiry, strike
            """), {"ticker": ticker, "snap_date": snap_date}).fetchall()

        if not rows:
            # Try most recent snap_date
            with self.engine.connect() as conn:
                latest = conn.execute(text(
                    "SELECT MAX(snap_date) FROM options_snapshots WHERE ticker = :t"
                ), {"t": ticker}).fetchone()
                if latest and latest[0]:
                    return self._load_chain(ticker, latest[0])
            return pd.DataFrame()

        df = pd.DataFrame(rows, columns=["strike", "opt_type", "open_interest",
                                          "implied_volatility", "expiry", "dte"])
        df["dte"] = df["dte"].apply(lambda x: x.days if hasattr(x, 'days') else int(x))
        return df[df["dte"] > 0]

    def _get_spot(self, ticker: str, snap_date: date) -> float:
        """Get spot price from resolved_series or options ATM."""
        with self.engine.connect() as conn:
            # Try resolved_series (yfinance close)
            row = conn.execute(text("""
                SELECT rs.value FROM resolved_series rs
                JOIN feature_registry fr ON rs.feature_id = fr.id
                WHERE (fr.name = :name1 OR fr.name = :name2)
                AND rs.obs_date <= :d
                ORDER BY rs.obs_date DESC LIMIT 1
            """), {
                "name1": f"{ticker.lower()}_close",
                "name2": ticker.lower(),
                "d": snap_date,
            }).fetchone()

            if row:
                return float(row[0])

            # Fallback: use ATM strike from options chain
            row = conn.execute(text("""
                SELECT strike FROM options_snapshots
                WHERE ticker = :t AND snap_date = :d AND opt_type = 'call'
                ORDER BY open_interest DESC LIMIT 1
            """), {"t": ticker, "d": snap_date}).fetchone()

            return float(row[0]) if row else 0.0

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
            "spy_gamma_flip": spy["gamma_flip"] if spy else None,
            "spy_put_wall": spy["put_wall"] if spy else None,
            "spy_call_wall": spy["call_wall"] if spy else None,
            "spy_gex": spy["gex_aggregate"] if spy else 0,
            "tickers": [
                {
                    "ticker": r["ticker"],
                    "gex": r["gex_aggregate"],
                    "regime": r["regime"],
                    "gamma_flip": r["gamma_flip"],
                    "vanna": r["vanna_exposure"],
                    "charm": r["charm_exposure"],
                }
                for r in results
            ],
        }
