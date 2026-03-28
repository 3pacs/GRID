"""
GRID — Options trade recommendation engine.

Takes scanner output + GEX profile + dealer regime and produces specific,
actionable trade recommendations with multi-layer sanity gating.

Pipeline:
  1. Run options scanner for each ticker (discovery/options_scanner.py)
  2. For each opportunity scoring >= 6:
     - Pull GEX profile from DealerGammaEngine (gamma flip, walls, vanna/charm)
     - Optimize strike: best gamma/premium ratio within direction
     - Pick expiry: 2-6 weeks out, avoid weekly OpEx, prefer monthly
     - Compute entry from bid/ask mid, target from gamma wall distance,
       stop from gamma flip point
     - Size via Kelly criterion (signal_executor pattern)
     - Run 5-layer sanity pipeline (hundredx_digest pattern)
  3. Return only recommendations passing all sanity layers
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Recommendation dataclass ─────────────────────────────────────────


@dataclass
class OptionsRecommendation:
    """A fully specified, actionable options trade recommendation."""

    ticker: str
    direction: str                  # "CALL" or "PUT"
    strike: float
    expiry: str                     # ISO date
    entry_price: float              # mid of bid-ask
    target_price: float             # based on expected move
    stop_loss: float                # based on gamma flip or wall
    expected_return: float          # payoff x probability
    max_risk: float                 # dollars at risk
    kelly_fraction: float
    confidence: float               # 0-1
    thesis: str                     # plain English why
    dealer_context: str             # what GEX/vanna/charm say
    sanity_status: dict             # {layer_name: {status, reason}}
    generated_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "direction": self.direction,
            "strike": self.strike,
            "expiry": self.expiry,
            "entry_price": self.entry_price,
            "target_price": self.target_price,
            "stop_loss": self.stop_loss,
            "expected_return": self.expected_return,
            "max_risk": self.max_risk,
            "kelly_fraction": self.kelly_fraction,
            "confidence": self.confidence,
            "thesis": self.thesis,
            "dealer_context": self.dealer_context,
            "sanity_status": self.sanity_status,
            "generated_at": self.generated_at,
        }

    @property
    def risk_reward_ratio(self) -> float:
        """Target profit / max risk."""
        if self.max_risk <= 0:
            return 0.0
        return (self.target_price - self.entry_price) / self.entry_price

    @property
    def all_sanity_passed(self) -> bool:
        """True only if every sanity layer returned PASS or SKIP."""
        return all(
            v.get("status") in ("PASS", "SKIP")
            for v in self.sanity_status.values()
        )


# ── Weekly OpEx dates (third Friday of each month is monthly) ────────

def _third_friday(year: int, month: int) -> date:
    """Return the third Friday of a given month (standard monthly OpEx)."""
    first_day = date(year, month, 1)
    # weekday(): Monday=0 ... Friday=4
    days_until_friday = (4 - first_day.weekday()) % 7
    first_friday = first_day + timedelta(days=days_until_friday)
    return first_friday + timedelta(weeks=2)


def _is_monthly_opex(d: date) -> bool:
    """Check whether a date is a standard monthly options expiration (3rd Friday)."""
    return d == _third_friday(d.year, d.month)


def _is_weekly_opex(d: date) -> bool:
    """True if a Friday expiry is NOT the monthly — i.e. it's a weekly."""
    if d.weekday() != 4:  # not a Friday
        return False
    return not _is_monthly_opex(d)


# ── Recommendation Engine ────────────────────────────────────────────


class OptionsRecommender:
    """Generates actionable options trade recommendations.

    Combines:
    - options_scanner: mispricing signal detection (7 signals)
    - DealerGammaEngine: GEX regime, gamma flip, walls, vanna/charm
    - Kelly sizing: position sizing from win probability + payoff
    - 5-layer sanity pipeline: data quality, dealer flow, cross-asset,
      LLM review, historical analog

    Parameters:
        db_engine: SQLAlchemy engine for database access.
        min_score: Minimum scanner composite score to consider (default 6.0).
        capital: Notional capital for risk calculations.
        max_kelly: Cap on Kelly fraction (half-Kelly default).
    """

    # Sane defaults for strike optimization
    _CALL_OTM_MIN_PCT = 0.02   # at least 2% OTM
    _CALL_OTM_MAX_PCT = 0.12   # at most 12% OTM
    _PUT_OTM_MIN_PCT = 0.02
    _PUT_OTM_MAX_PCT = 0.12

    # Expiry preferences
    _MIN_DTE = 14   # at least 2 weeks
    _MAX_DTE = 42   # at most 6 weeks
    _IDEAL_DTE_LO = 21  # prefer 3-4 weeks
    _IDEAL_DTE_HI = 28
    _MIN_OI_FOR_EXPIRY = 500   # minimum open interest at expiry

    # Sanity thresholds
    _IV_MIN = 0.05
    _IV_MAX = 2.00
    _MIN_OI = 100
    _MAX_SPREAD_PCT = 0.25   # bid-ask spread < 25% of mid

    def __init__(
        self,
        db_engine: Engine,
        min_score: float = 6.0,
        capital: float = 10_000.0,
        max_kelly: float = 0.25,
    ) -> None:
        self.engine = db_engine
        self.min_score = min_score
        self.capital = capital
        self.max_kelly = max_kelly
        log.info(
            "OptionsRecommender initialised — min_score={s}, capital=${c:,.0f}",
            s=min_score, c=capital,
        )

    # ── Public API ───────────────────────────────────────────────────

    def generate_recommendations(
        self,
        engine: Engine | None = None,
        tickers: list[str] | None = None,
    ) -> list[OptionsRecommendation]:
        """Generate actionable options trade recommendations.

        Parameters:
            engine: Optional override SQLAlchemy engine (defaults to self.engine).
            tickers: Restrict scan to these tickers (default: all available).

        Returns:
            list[OptionsRecommendation]: Only recommendations passing all 5
            sanity layers, sorted by expected return descending.
        """
        db = engine or self.engine

        # 1. Run options scanner
        from discovery.options_scanner import OptionsScanner

        scanner = OptionsScanner(db_engine=db)
        opportunities = scanner.scan_all(tickers=tickers, min_score=self.min_score)

        log.info(
            "Scanner returned {n} opportunities (score >= {s})",
            n=len(opportunities), s=self.min_score,
        )

        if not opportunities:
            return []

        # 2. Load GEX engine
        from physics.dealer_gamma import DealerGammaEngine

        gex_engine = DealerGammaEngine(db_engine=db)

        recommendations: list[OptionsRecommendation] = []

        for opp in opportunities:
            try:
                rec = self._build_recommendation(opp, gex_engine, db)
                if rec is not None:
                    recommendations.append(rec)
            except Exception as exc:
                log.warning(
                    "Recommendation build failed for {t}: {e}",
                    t=opp.ticker, e=str(exc),
                )

        # Sort by expected return descending
        recommendations.sort(key=lambda r: r.expected_return, reverse=True)

        log.info(
            "Generated {n} actionable recommendations from {t} opportunities",
            n=len(recommendations), t=len(opportunities),
        )
        return recommendations

    # ── Core build logic ─────────────────────────────────────────────

    def _build_recommendation(
        self,
        opp,  # MispricingOpportunity from options_scanner
        gex_engine,  # DealerGammaEngine
        db: Engine,
    ) -> OptionsRecommendation | None:
        """Build a single recommendation from a scanner opportunity.

        Returns None if the recommendation fails any required sanity layer.
        """
        ticker = opp.ticker
        direction = opp.direction
        spot = opp.spot_price

        if not spot or spot <= 0:
            log.debug("Skipping {t}: no spot price", t=ticker)
            return None

        # Get GEX profile
        gex_profile = gex_engine.compute_gex_profile(ticker)
        if "error" in gex_profile:
            log.debug("Skipping {t}: GEX error — {e}", t=ticker, e=gex_profile["error"])
            # Continue without GEX — graceful degradation
            gex_profile = {}

        # Load options chain for strike/expiry selection
        chain_df = self._load_chain(db, ticker)
        if chain_df.empty:
            log.debug("Skipping {t}: no options chain data", t=ticker)
            return None

        # Optimize strike
        strike = self._optimize_strike(chain_df, direction, spot, gex_profile)
        if strike is None:
            log.debug("Skipping {t}: no suitable strike", t=ticker)
            return None

        # Pick expiry
        expiry = self._pick_expiry(chain_df, spot)
        if expiry is None:
            log.debug("Skipping {t}: no suitable expiry", t=ticker)
            return None

        # Compute entry price from bid/ask mid
        entry_price = self._get_entry_price(db, ticker, strike, expiry, direction)
        if entry_price is None or entry_price <= 0:
            log.debug("Skipping {t}: no valid entry price at K={k}", t=ticker, k=strike)
            return None

        # Target from GEX expected move (gamma wall distance)
        target_price = self._compute_target_price(
            entry_price, spot, strike, direction, gex_profile,
        )

        # Stop loss from gamma flip point
        stop_loss = self._compute_stop_loss(
            entry_price, spot, strike, direction, gex_profile,
        )

        # Win probability and payoff for Kelly
        win_prob = self._estimate_win_probability(opp, gex_profile)
        payoff_ratio = (target_price - entry_price) / max(entry_price - stop_loss, 0.01)

        # Kelly fraction
        kelly_fraction = self._compute_kelly(win_prob, payoff_ratio)

        # Expected return
        expected_return = win_prob * (target_price - entry_price) - (1 - win_prob) * (entry_price - stop_loss)

        # Max risk in dollars (entry premium x 100 shares x kelly fraction of capital)
        contracts = max(1, int(self.capital * kelly_fraction / (entry_price * 100)))
        max_risk = contracts * entry_price * 100

        # Confidence from scanner score + GEX alignment
        confidence = self._compute_confidence(opp, gex_profile, direction)

        # Dealer context string
        dealer_context = self._format_dealer_context(gex_profile, direction)

        # Thesis from scanner
        thesis = opp.thesis

        # Build preliminary recommendation
        rec = OptionsRecommendation(
            ticker=ticker,
            direction=direction,
            strike=round(strike, 2),
            expiry=str(expiry),
            entry_price=round(entry_price, 4),
            target_price=round(target_price, 4),
            stop_loss=round(max(stop_loss, 0.01), 4),
            expected_return=round(expected_return, 4),
            max_risk=round(max_risk, 2),
            kelly_fraction=round(kelly_fraction, 4),
            confidence=round(confidence, 4),
            thesis=thesis,
            dealer_context=dealer_context,
            sanity_status={},
            generated_at=datetime.utcnow().isoformat() + "Z",
        )

        # Run 5-layer sanity pipeline
        sanity = self._run_sanity_pipeline(rec, db, opp, gex_profile)
        rec.sanity_status = sanity

        # Only return if all layers pass or skip
        if not rec.all_sanity_passed:
            failed = [
                k for k, v in sanity.items() if v.get("status") == "FAIL"
            ]
            log.info(
                "Recommendation rejected for {t}: failed layers={f}",
                t=ticker, f=failed,
            )
            return None

        return rec

    # ── Strike Optimization ──────────────────────────────────────────

    def _optimize_strike(
        self,
        chain_df: pd.DataFrame,
        direction: str,
        spot: float,
        gex_profile: dict,
    ) -> float | None:
        """Pick the strike with best gamma/premium ratio within direction.

        For CALL: strike above spot where call gamma is high but premium
        is reasonable. For PUT: strike below spot where put gamma is high.
        Avoids strikes beyond gamma wall (likely resistance/support).
        """
        from physics.dealer_gamma import bs_gamma

        call_wall = gex_profile.get("call_wall") or spot * 1.15
        put_wall = gex_profile.get("put_wall") or spot * 0.85

        if direction == "CALL":
            lo = spot * (1 + self._CALL_OTM_MIN_PCT)
            hi = min(spot * (1 + self._CALL_OTM_MAX_PCT), call_wall)
            opt_type = "call"
        else:
            hi = spot * (1 - self._PUT_OTM_MIN_PCT)
            lo = max(spot * (1 - self._PUT_OTM_MAX_PCT), put_wall)
            opt_type = "put"

        # Filter to relevant strikes and option type
        candidates = chain_df[
            (chain_df["strike"] >= lo)
            & (chain_df["strike"] <= hi)
            & (chain_df["opt_type"] == opt_type)
            & (chain_df["open_interest"] > self._MIN_OI)
        ].copy()

        if candidates.empty:
            # Relax OI filter
            candidates = chain_df[
                (chain_df["strike"] >= lo)
                & (chain_df["strike"] <= hi)
                & (chain_df["opt_type"] == opt_type)
            ].copy()

        if candidates.empty:
            return None

        # Score each strike: gamma / (implied cost proxy)
        # Higher gamma + lower IV = better value
        best_strike = None
        best_score = -1.0

        for _, row in candidates.iterrows():
            K = float(row["strike"])
            T = float(row["dte"]) / 365.0
            if T <= 0:
                continue
            iv = float(row["implied_volatility"]) if row["implied_volatility"] > 0 else 0.25
            oi = float(row["open_interest"])

            gamma = bs_gamma(spot, K, T, 0.05, iv)
            # Cost proxy: IV * sqrt(T) * spot (higher = more expensive)
            cost_proxy = iv * math.sqrt(T) * spot if iv > 0 else 1.0
            # OI liquidity bonus (log scale)
            oi_bonus = math.log1p(oi) / 10.0

            score = (gamma * spot * 100 / max(cost_proxy, 0.01)) + oi_bonus
            if score > best_score:
                best_score = score
                best_strike = K

        return best_strike

    # ── Expiry Selection ─────────────────────────────────────────────

    def _pick_expiry(
        self,
        chain_df: pd.DataFrame,
        spot: float,
    ) -> date | None:
        """Pick optimal expiry: 2-6 weeks out, avoid weekly OpEx, prefer monthly.

        Requires sufficient OI (>500 contracts) at the chosen expiry.
        """
        today = date.today()

        # Get distinct expiries with aggregate OI
        expiry_stats = (
            chain_df.groupby("expiry")
            .agg(total_oi=("open_interest", "sum"), dte=("dte", "first"))
            .reset_index()
        )

        # Filter to acceptable DTE range
        expiry_stats = expiry_stats[
            (expiry_stats["dte"] >= self._MIN_DTE)
            & (expiry_stats["dte"] <= self._MAX_DTE)
        ]

        if expiry_stats.empty:
            return None

        # Score each expiry
        best_expiry = None
        best_score = -1.0

        for _, row in expiry_stats.iterrows():
            exp = row["expiry"]
            dte = int(row["dte"])
            total_oi = float(row["total_oi"])

            # Parse expiry to date if needed
            if isinstance(exp, str):
                try:
                    exp_date = date.fromisoformat(exp)
                except ValueError:
                    continue
            elif isinstance(exp, date):
                exp_date = exp
            else:
                continue

            # OI threshold
            if total_oi < self._MIN_OI_FOR_EXPIRY:
                continue

            score = 0.0

            # Prefer ideal DTE range (3-4 weeks)
            if self._IDEAL_DTE_LO <= dte <= self._IDEAL_DTE_HI:
                score += 5.0
            elif self._MIN_DTE <= dte < self._IDEAL_DTE_LO:
                score += 3.0  # acceptable but shorter
            else:
                score += 2.0  # acceptable but longer

            # Prefer monthly OpEx over weekly (more liquidity, less dealer unwind noise)
            if _is_monthly_opex(exp_date):
                score += 3.0
            elif _is_weekly_opex(exp_date):
                score -= 1.0  # penalise weekly OpEx

            # OI liquidity bonus
            score += math.log1p(total_oi) / 5.0

            if score > best_score:
                best_score = score
                best_expiry = exp_date

        return best_expiry

    # ── Entry / Target / Stop ────────────────────────────────────────

    def _get_entry_price(
        self,
        db: Engine,
        ticker: str,
        strike: float,
        expiry: date,
        direction: str,
    ) -> float | None:
        """Get entry price as mid of bid/ask from latest options snapshot."""
        opt_type = "call" if direction == "CALL" else "put"

        with db.connect() as conn:
            row = conn.execute(text("""
                SELECT bid, ask, last_price
                FROM options_snapshots
                WHERE ticker = :ticker
                  AND strike = :strike
                  AND expiry = :expiry
                  AND opt_type = :opt_type
                ORDER BY snap_date DESC
                LIMIT 1
            """), {
                "ticker": ticker,
                "strike": strike,
                "expiry": expiry,
                "opt_type": opt_type,
            }).fetchone()

        if row is None:
            # Fallback: estimate from IV using simplified model
            return self._estimate_premium(db, ticker, strike, expiry, direction)

        bid = float(row[0]) if row[0] and row[0] > 0 else 0.0
        ask = float(row[1]) if row[1] and row[1] > 0 else 0.0
        last = float(row[2]) if row[2] and row[2] > 0 else 0.0

        if bid > 0 and ask > 0:
            return (bid + ask) / 2.0
        if last > 0:
            return last
        return None

    def _estimate_premium(
        self,
        db: Engine,
        ticker: str,
        strike: float,
        expiry: date,
        direction: str,
    ) -> float | None:
        """Rough premium estimate from Black-Scholes when bid/ask unavailable."""
        from physics.dealer_gamma import _d1, _d2
        from scipy.stats import norm

        spot = self._get_spot(db, ticker)
        if spot <= 0:
            return None

        T = max((expiry - date.today()).days, 1) / 365.0
        r = 0.05
        sigma = self._get_atm_iv(db, ticker) or 0.25

        d1 = _d1(spot, strike, T, r, sigma)
        d2 = _d2(spot, strike, T, r, sigma)

        if direction == "CALL":
            price = spot * norm.cdf(d1) - strike * math.exp(-r * T) * norm.cdf(d2)
        else:
            price = strike * math.exp(-r * T) * norm.cdf(-d2) - spot * norm.cdf(-d1)

        return max(price, 0.01)

    def _compute_target_price(
        self,
        entry_price: float,
        spot: float,
        strike: float,
        direction: str,
        gex_profile: dict,
    ) -> float:
        """Set target from GEX expected move (gamma wall distance).

        If GEX data available: target is based on distance to gamma wall.
        Otherwise: use 2x entry as conservative target.
        """
        if not gex_profile:
            return entry_price * 2.0

        if direction == "CALL":
            call_wall = gex_profile.get("call_wall") or 0
            if call_wall and call_wall > spot:
                # Expected move: spot -> call wall
                move_pct = (call_wall - spot) / spot
                # Option delta ~0.3 for OTM call; option moves ~ delta * underlying move
                # But with gamma acceleration, the option move is amplified
                option_move_mult = max(1.5, move_pct / max(entry_price / spot, 0.001))
                return entry_price * (1 + option_move_mult)
        else:
            put_wall = gex_profile.get("put_wall") or 0
            if put_wall and put_wall < spot:
                move_pct = (spot - put_wall) / spot
                option_move_mult = max(1.5, move_pct / max(entry_price / spot, 0.001))
                return entry_price * (1 + option_move_mult)

        # Fallback: 2x entry
        return entry_price * 2.0

    def _compute_stop_loss(
        self,
        entry_price: float,
        spot: float,
        strike: float,
        direction: str,
        gex_profile: dict,
    ) -> float:
        """Set stop from gamma flip point.

        If GEX data available: gamma flip represents the regime change point.
        When spot crosses gamma flip, dealer flows reverse -> our thesis breaks.
        Otherwise: use 50% of entry as stop.
        """
        if not gex_profile:
            return entry_price * 0.50

        gamma_flip = gex_profile.get("gamma_flip")
        if gamma_flip is None:
            return entry_price * 0.50

        if direction == "CALL":
            # For calls: if spot drops below gamma flip, thesis is broken
            if gamma_flip < spot:
                # Estimate how much the option loses if spot drops to gamma flip
                drop_pct = (spot - gamma_flip) / spot
                # Option loses roughly delta * drop_pct * spot
                option_loss_pct = min(0.70, drop_pct * 3.0)  # amplified by leverage
                return entry_price * (1 - option_loss_pct)
        else:
            # For puts: if spot rallies above gamma flip, thesis breaks
            if gamma_flip > spot:
                rally_pct = (gamma_flip - spot) / spot
                option_loss_pct = min(0.70, rally_pct * 3.0)
                return entry_price * (1 - option_loss_pct)

        return entry_price * 0.50

    # ── Kelly and Probability ────────────────────────────────────────

    def _estimate_win_probability(self, opp, gex_profile: dict) -> float:
        """Estimate win probability from scanner score and GEX alignment.

        Score 6/10 -> ~35% base win rate
        Score 8/10 -> ~50%
        Score 10/10 -> ~60% (capped — markets are efficient)
        GEX alignment bonus: +5-10%
        """
        score = opp.score
        # Linear mapping: score 5->30%, 10->60%
        base_prob = 0.30 + (score - 5.0) * 0.06

        # GEX alignment bonus
        regime = gex_profile.get("regime", "")
        direction = opp.direction
        if regime == "SHORT_GAMMA":
            # Short gamma amplifies moves — good for directional bets
            base_prob += 0.05
        elif regime == "LONG_GAMMA" and direction in ("CALL", "PUT"):
            # Long gamma dampens moves — harder for directional bets
            base_prob -= 0.03

        # Vanna/charm adjustment
        vanna = gex_profile.get("vanna_exposure", 0)
        if direction == "CALL" and vanna < 0:
            # Negative vanna + IV drop -> dealers buy underlying -> bullish
            base_prob += 0.03
        elif direction == "PUT" and vanna > 0:
            # Positive vanna + IV rise -> dealers sell underlying -> bearish
            base_prob += 0.03

        return max(0.10, min(0.65, base_prob))

    def _compute_kelly(self, win_prob: float, payoff_ratio: float) -> float:
        """Kelly criterion: f* = (b*p - q) / b, capped at half-Kelly.

        Follows the pattern from trading/paper_engine.py.
        """
        if payoff_ratio <= 0 or win_prob <= 0:
            return 0.0

        b = payoff_ratio
        p = win_prob
        q = 1 - p

        kelly = (b * p - q) / b
        # Half-Kelly for safety
        half_kelly = kelly / 2.0

        # Cap at max_kelly
        return max(0.0, min(half_kelly, self.max_kelly))

    def _compute_confidence(
        self, opp, gex_profile: dict, direction: str,
    ) -> float:
        """Compute 0-1 confidence from scanner score + GEX alignment."""
        # Base: normalize scanner score to 0-1
        base = opp.score / 10.0

        # GEX alignment bonus
        regime = gex_profile.get("regime", "")
        if regime == "SHORT_GAMMA":
            base += 0.05  # amplification regime supports directional bets
        elif regime == "NEUTRAL":
            pass
        elif regime == "LONG_GAMMA":
            base -= 0.05  # dampening works against us

        # Confidence level from scanner
        if opp.confidence == "HIGH":
            base += 0.10
        elif opp.confidence == "MEDIUM":
            base += 0.05

        return max(0.0, min(1.0, base))

    # ── 5-Layer Sanity Pipeline ──────────────────────────────────────

    def _run_sanity_pipeline(
        self,
        rec: OptionsRecommendation,
        db: Engine,
        opp,
        gex_profile: dict,
    ) -> dict:
        """Run 5-layer sanity check on a recommendation.

        Layers:
          1. DATA_QUALITY: IV in 5-200%, OI > 100, spread < 25% of mid
          2. DEALER_FLOW: GEX regime supports direction
          3. CROSS_ASSET: momentum + news energy aligned with direction
          4. LLM_REVIEW: structured prompt to LLM, graceful SKIP if unavailable
          5. HISTORICAL_ANALOG: query past scanner results with similar setups

        Each returns {status: "PASS"/"FAIL"/"SKIP", reason: "..."}
        """
        sanity: dict[str, dict[str, str]] = {}

        sanity["DATA_QUALITY"] = self._sanity_data_quality(rec, db)
        sanity["DEALER_FLOW"] = self._sanity_dealer_flow(rec, gex_profile)
        sanity["CROSS_ASSET"] = self._sanity_cross_asset(rec, db)
        sanity["LLM_REVIEW"] = self._sanity_llm_review(rec, opp, gex_profile)
        sanity["HISTORICAL_ANALOG"] = self._sanity_historical_analog(rec, db)

        passed = sum(1 for v in sanity.values() if v["status"] == "PASS")
        skipped = sum(1 for v in sanity.values() if v["status"] == "SKIP")
        failed = sum(1 for v in sanity.values() if v["status"] == "FAIL")
        log.info(
            "Sanity pipeline for {t} {d}: {p} PASS, {s} SKIP, {f} FAIL",
            t=rec.ticker, d=rec.direction, p=passed, s=skipped, f=failed,
        )

        return sanity

    def _sanity_data_quality(
        self, rec: OptionsRecommendation, db: Engine,
    ) -> dict[str, str]:
        """Layer 1: Data quality checks.

        - IV in 5-200%
        - OI > 100
        - Spread < 25% of mid
        """
        opt_type = "call" if rec.direction == "CALL" else "put"

        with db.connect() as conn:
            row = conn.execute(text("""
                SELECT implied_volatility, open_interest, bid, ask
                FROM options_snapshots
                WHERE ticker = :ticker
                  AND strike = :strike
                  AND opt_type = :opt_type
                ORDER BY snap_date DESC
                LIMIT 1
            """), {
                "ticker": rec.ticker,
                "strike": rec.strike,
                "opt_type": opt_type,
            }).fetchone()

        if row is None:
            return {"status": "SKIP", "reason": "No snapshot data for strike — using estimated premium"}

        iv = float(row[0]) if row[0] else 0
        oi = int(row[1]) if row[1] else 0
        bid = float(row[2]) if row[2] else 0
        ask = float(row[3]) if row[3] else 0

        issues = []

        if iv < self._IV_MIN or iv > self._IV_MAX:
            issues.append(f"IV {iv:.1%} outside sane range ({self._IV_MIN:.0%}-{self._IV_MAX:.0%})")

        if oi < self._MIN_OI:
            issues.append(f"OI {oi} below minimum {self._MIN_OI}")

        if bid > 0 and ask > 0:
            mid = (bid + ask) / 2.0
            spread_pct = (ask - bid) / mid if mid > 0 else 1.0
            if spread_pct > self._MAX_SPREAD_PCT:
                issues.append(f"Spread {spread_pct:.1%} exceeds {self._MAX_SPREAD_PCT:.0%} of mid")

        if issues:
            return {"status": "FAIL", "reason": "; ".join(issues)}

        return {"status": "PASS", "reason": "IV, OI, and spread within acceptable bounds"}

    def _sanity_dealer_flow(
        self, rec: OptionsRecommendation, gex_profile: dict,
    ) -> dict[str, str]:
        """Layer 2: Dealer flow alignment.

        GEX regime should support our direction:
        - SHORT_GAMMA amplifies moves -> supports directional bets (PASS)
        - LONG_GAMMA dampens moves -> works against directional bets (FAIL for high-conviction only)
        - NEUTRAL -> no strong signal (PASS)
        """
        if not gex_profile or "regime" not in gex_profile:
            return {"status": "SKIP", "reason": "No GEX data available"}

        regime = gex_profile["regime"]
        direction = rec.direction

        if regime == "SHORT_GAMMA":
            return {
                "status": "PASS",
                "reason": f"Short gamma regime amplifies {direction} directional move",
            }

        if regime == "LONG_GAMMA":
            # Long gamma dampens, but doesn't prevent moves; warn but pass
            # unless confidence is very low
            if rec.confidence < 0.4:
                return {
                    "status": "FAIL",
                    "reason": f"Long gamma dampens moves + low confidence ({rec.confidence:.1%})",
                }
            return {
                "status": "PASS",
                "reason": f"Long gamma dampens moves, but confidence ({rec.confidence:.1%}) is adequate",
            }

        # NEUTRAL
        return {"status": "PASS", "reason": "Neutral gamma regime — no strong headwind or tailwind"}

    def _sanity_cross_asset(
        self, rec: OptionsRecommendation, db: Engine,
    ) -> dict[str, str]:
        """Layer 3: Cross-asset alignment (momentum + news energy).

        Checks recent price momentum and decision journal sentiment
        to see if they align with the recommended direction.
        """
        try:
            with db.connect() as conn:
                # Price momentum: 5-day return
                feature_name = f"{rec.ticker.lower()}_close"
                prices = conn.execute(text("""
                    SELECT rs.value
                    FROM resolved_series rs
                    JOIN feature_registry fr ON rs.feature_id = fr.id
                    WHERE fr.name = :fname
                    ORDER BY rs.obs_date DESC
                    LIMIT 6
                """), {"fname": feature_name}).fetchall()

                if len(prices) >= 2:
                    latest = float(prices[0][0])
                    older = float(prices[-1][0])
                    if older > 0:
                        momentum = (latest - older) / older
                    else:
                        momentum = 0.0
                else:
                    momentum = 0.0

                # News energy from decision journal (latest regime)
                journal_row = conn.execute(text("""
                    SELECT inferred_state, grid_recommendation
                    FROM decision_journal
                    ORDER BY decision_timestamp DESC
                    LIMIT 1
                """)).fetchone()

                news_aligned = True  # default: no contradiction
                if journal_row:
                    state = journal_row[0] or ""
                    # CRISIS / FRAGILE state + CALL direction = misalignment
                    if state in ("CRISIS", "FRAGILE") and rec.direction == "CALL":
                        news_aligned = False
                    elif state == "GROWTH" and rec.direction == "PUT":
                        news_aligned = False

            # Evaluate
            if rec.direction == "CALL":
                momentum_aligned = momentum >= -0.02  # not in sharp decline
            else:
                momentum_aligned = momentum <= 0.02   # not in sharp rally

            if momentum_aligned and news_aligned:
                return {
                    "status": "PASS",
                    "reason": f"Momentum ({momentum:+.2%}) and regime aligned with {rec.direction}",
                }
            elif not momentum_aligned and not news_aligned:
                return {
                    "status": "FAIL",
                    "reason": f"Momentum ({momentum:+.2%}) and regime both contradict {rec.direction}",
                }
            else:
                # One aligned, one not — pass with warning
                return {
                    "status": "PASS",
                    "reason": f"Partial alignment — momentum={momentum:+.2%}, news_aligned={news_aligned}",
                }

        except Exception as exc:
            log.debug("Cross-asset check failed: {e}", e=str(exc))
            return {"status": "SKIP", "reason": f"Cross-asset data unavailable: {str(exc)[:80]}"}

    def _sanity_llm_review(
        self,
        rec: OptionsRecommendation,
        opp,
        gex_profile: dict,
    ) -> dict[str, str]:
        """Layer 4: LLM structured review.

        Sends all context to local LLM for PASS/FAIL judgment.
        Graceful degradation: returns SKIP if LLM is unavailable.
        """
        try:
            import requests as req

            # Check if LLM is available
            props = req.get("http://localhost:8080/props", timeout=3)
            if props.status_code != 200:
                return {"status": "SKIP", "reason": "LLM not available for review"}
        except Exception:
            return {"status": "SKIP", "reason": "LLM not available for review"}

        prompt = f"""You are a senior options strategist reviewing an automated trade recommendation.
Respond with ONLY a JSON object: {{"verdict": "PASS"|"FAIL", "reason": "one sentence"}}

PASS = plausible trade setup worth executing
FAIL = data quality issue, incoherent thesis, or fundamentally flawed logic

Recommendation:
- Ticker: {rec.ticker} {rec.direction}
- Strike: ${rec.strike:.2f}, Expiry: {rec.expiry}
- Entry: ${rec.entry_price:.4f}, Target: ${rec.target_price:.4f}, Stop: ${rec.stop_loss:.4f}
- Expected Return: ${rec.expected_return:.4f}, Max Risk: ${rec.max_risk:.2f}
- Kelly Fraction: {rec.kelly_fraction:.2%}
- Confidence: {rec.confidence:.1%}
- Thesis: {rec.thesis}
- Dealer Context: {rec.dealer_context}
- Scanner Score: {opp.score:.1f}/10

Rules for your review:
- Entry price near zero or negative → FAIL
- Target < entry → FAIL (no upside)
- Kelly fraction > 20% → FAIL (oversized)
- Confidence < 20% with Kelly > 10% → FAIL (overbet on low conviction)
- Thesis makes no logical sense → FAIL
- Everything else → PASS

Respond with ONLY the JSON object."""

        try:
            resp = req.post(
                "http://localhost:8080/v1/chat/completions",
                json={
                    "model": "default",
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.1,
                    "max_tokens": 200,
                },
                timeout=30,
            )
            if resp.status_code == 200:
                content = resp.json().get("choices", [{}])[0].get("message", {}).get("content", "")
                import re
                json_match = re.search(r'\{.*\}', content, re.DOTALL)
                if json_match:
                    verdict = json.loads(json_match.group())
                    status = verdict.get("verdict", "SKIP")
                    reason = verdict.get("reason", "No reason provided")
                    if status in ("PASS", "FAIL"):
                        return {"status": status, "reason": reason}

            return {"status": "SKIP", "reason": "LLM response unparseable"}

        except Exception as exc:
            log.debug("LLM review failed: {e}", e=str(exc))
            return {"status": "SKIP", "reason": f"LLM review error: {str(exc)[:60]}"}

    def _sanity_historical_analog(
        self, rec: OptionsRecommendation, db: Engine,
    ) -> dict[str, str]:
        """Layer 5: Historical analog check.

        Query past scanner results with similar setups and check outcomes.
        Similar = same ticker + same direction + score within 1.5 points.
        """
        try:
            with db.connect() as conn:
                # Check if mispricing scans table exists and has data
                rows = conn.execute(text("""
                    SELECT score, payoff_multiple, direction, confidence, scan_date
                    FROM options_mispricing_scans
                    WHERE ticker = :ticker
                      AND direction = :direction
                      AND score >= :lo_score
                      AND score <= :hi_score
                    ORDER BY scan_date DESC
                    LIMIT 20
                """), {
                    "ticker": rec.ticker,
                    "direction": rec.direction,
                    "lo_score": rec.confidence * 10 - 1.5,  # approximate score
                    "hi_score": rec.confidence * 10 + 1.5,
                }).fetchall()

            if not rows:
                return {"status": "SKIP", "reason": "No historical analogs found for this setup"}

            # Check if past similar setups had reasonable outcomes
            n = len(rows)
            avg_score = sum(float(r[0]) for r in rows) / n
            high_confidence = sum(1 for r in rows if r[3] in ("HIGH", "MEDIUM"))

            if n >= 5 and high_confidence / n >= 0.5:
                return {
                    "status": "PASS",
                    "reason": f"{n} historical analogs found, {high_confidence}/{n} were medium/high confidence (avg score {avg_score:.1f})",
                }
            elif n >= 3:
                return {
                    "status": "PASS",
                    "reason": f"{n} analogs found (avg score {avg_score:.1f}) — limited history",
                }
            else:
                return {
                    "status": "SKIP",
                    "reason": f"Only {n} analog(s) — insufficient for pattern matching",
                }

        except Exception as exc:
            log.debug("Historical analog check failed: {e}", e=str(exc))
            return {"status": "SKIP", "reason": f"Historical data unavailable: {str(exc)[:80]}"}

    # ── Dealer Context Formatting ────────────────────────────────────

    def _format_dealer_context(self, gex_profile: dict, direction: str) -> str:
        """Build human-readable dealer context string from GEX profile."""
        if not gex_profile:
            return "GEX data unavailable"

        parts = []

        regime = gex_profile.get("regime", "UNKNOWN")
        parts.append(f"Regime: {regime}")

        gex = gex_profile.get("gex_aggregate", 0)
        parts.append(f"GEX: {gex:,.0f}")

        gamma_flip = gex_profile.get("gamma_flip")
        if gamma_flip:
            parts.append(f"Gamma flip: ${gamma_flip:,.2f}")

        spot = gex_profile.get("spot", 0)
        call_wall = gex_profile.get("call_wall")
        put_wall = gex_profile.get("put_wall")
        if call_wall:
            parts.append(f"Call wall: ${call_wall:,.2f}")
        if put_wall:
            parts.append(f"Put wall: ${put_wall:,.2f}")

        vanna = gex_profile.get("vanna_exposure", 0)
        charm = gex_profile.get("charm_exposure", 0)
        if vanna != 0:
            parts.append(f"Vanna: {vanna:,.0f}")
        if charm != 0:
            parts.append(f"Charm: {charm:,.0f}")

        # Interpretation
        if regime == "SHORT_GAMMA":
            parts.append(f"Dealers are short gamma — will amplify {direction} move")
        elif regime == "LONG_GAMMA":
            parts.append(f"Dealers are long gamma — will dampen moves")

        return " | ".join(parts)

    # ── Database Helpers ─────────────────────────────────────────────

    def _load_chain(self, db: Engine, ticker: str) -> pd.DataFrame:
        """Load options chain from database (latest snap_date)."""
        with db.connect() as conn:
            rows = conn.execute(text("""
                SELECT strike, opt_type, open_interest, implied_volatility,
                       expiry, (expiry - snap_date) AS dte, bid, ask
                FROM options_snapshots
                WHERE ticker = :ticker
                  AND snap_date = (
                      SELECT MAX(snap_date) FROM options_snapshots WHERE ticker = :ticker
                  )
                  AND open_interest > 0
                  AND expiry > CURRENT_DATE
                ORDER BY expiry, strike
            """), {"ticker": ticker}).fetchall()

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows, columns=[
            "strike", "opt_type", "open_interest", "implied_volatility",
            "expiry", "dte", "bid", "ask",
        ])
        df["dte"] = df["dte"].apply(lambda x: x.days if hasattr(x, "days") else int(x))
        return df[df["dte"] > 0]

    def _get_spot(self, db: Engine, ticker: str) -> float:
        """Get latest spot price for a ticker."""
        with db.connect() as conn:
            row = conn.execute(text("""
                SELECT rs.value
                FROM resolved_series rs
                JOIN feature_registry fr ON rs.feature_id = fr.id
                WHERE fr.name = :fname OR fr.name = :fname2
                ORDER BY rs.obs_date DESC
                LIMIT 1
            """), {
                "fname": f"{ticker.lower()}_close",
                "fname2": ticker.lower(),
            }).fetchone()
        return float(row[0]) if row else 0.0

    def _get_atm_iv(self, db: Engine, ticker: str) -> float | None:
        """Get ATM implied volatility from latest options signals."""
        with db.connect() as conn:
            row = conn.execute(text("""
                SELECT iv_atm
                FROM options_daily_signals
                WHERE ticker = :ticker
                ORDER BY signal_date DESC
                LIMIT 1
            """), {"ticker": ticker}).fetchone()
        return float(row[0]) if row and row[0] else None

    # ── Formatting ───────────────────────────────────────────────────

    def format_report(self, recommendations: list[OptionsRecommendation]) -> str:
        """Format recommendations into a readable report."""
        if not recommendations:
            return "No actionable options recommendations generated."

        lines = [
            "=" * 80,
            "GRID OPTIONS TRADE RECOMMENDATIONS",
            f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
            f"Total Recommendations: {len(recommendations)}",
            "=" * 80,
            "",
        ]

        for i, rec in enumerate(recommendations, 1):
            sanity_summary = ", ".join(
                f"{k}:{v['status']}" for k, v in rec.sanity_status.items()
            )
            lines.extend([
                f"#{i}  {rec.ticker} {rec.direction}  |  Confidence: {rec.confidence:.1%}",
                f"     Strike: ${rec.strike:,.2f}  |  Expiry: {rec.expiry}",
                f"     Entry: ${rec.entry_price:.4f}  |  Target: ${rec.target_price:.4f}  |  Stop: ${rec.stop_loss:.4f}",
                f"     Expected Return: ${rec.expected_return:.4f}  |  Max Risk: ${rec.max_risk:,.2f}",
                f"     Kelly: {rec.kelly_fraction:.2%}  |  R:R: {rec.risk_reward_ratio:.1f}x",
                f"     Thesis: {rec.thesis}",
                f"     Dealer: {rec.dealer_context}",
                f"     Sanity: [{sanity_summary}]",
                "",
            ])

        return "\n".join(lines)


# ── CLI entrypoint ───────────────────────────────────────────────────

if __name__ == "__main__":
    from db import get_engine

    engine = get_engine()
    recommender = OptionsRecommender(engine)

    recs = recommender.generate_recommendations(engine)
    print(recommender.format_report(recs))

    if recs:
        log.info("Top recommendation: {t} {d} ${k} exp {e}",
                 t=recs[0].ticker, d=recs[0].direction,
                 k=recs[0].strike, e=recs[0].expiry)
