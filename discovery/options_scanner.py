"""GRID — Options mispricing scanner.

Identifies potential 100x+ options opportunities by scanning for:
1. Extreme IV skew dislocations (vol surface kinks)
2. Put/call ratio extremes (crowd positioning)
3. Max pain divergence (spot far from max pain = gamma squeeze potential)
4. IV term structure inversions (backwardation = near-term event pricing)
5. OI concentration spikes (unusual activity at specific strikes)
6. IV percentile rank (cheap options relative to history)
7. Gamma exposure imbalance (dealer hedging flow potential)

Scoring system: each signal contributes to a composite "mispricing score".
Opportunities scoring >= 7/10 with potential payoff >= 100x are flagged.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass
class MispricingOpportunity:
    """A flagged options mispricing opportunity."""

    ticker: str
    scan_date: date
    score: float                    # 0-10 composite score
    estimated_payoff_multiple: float  # e.g. 150.0 = 150x
    direction: str                  # "CALL" or "PUT"
    thesis: str                     # Human-readable thesis
    signals: dict[str, Any] = field(default_factory=dict)
    strikes: list[float] = field(default_factory=list)
    expiry: str = ""
    spot_price: float = 0.0
    iv_atm: float = 0.0
    confidence: str = "LOW"         # LOW / MEDIUM / HIGH

    @property
    def is_100x(self) -> bool:
        return self.estimated_payoff_multiple >= 100.0


class OptionsScanner:
    """Scans options data for mispricing and 100x+ opportunities.

    Uses historical options_daily_signals and options_snapshots tables
    to compute relative value signals and flag extreme dislocations.

    Attributes:
        engine: SQLAlchemy engine for database access.
        lookback_days: Number of days for historical percentile calculation.
    """

    # Signal thresholds (calibrated for extremes)
    PCR_EXTREME_HIGH = 1.8       # Very high put buying = potential capitulation
    PCR_EXTREME_LOW = 0.4        # Very low put buying = complacency
    IV_SKEW_EXTREME = 1.6        # OTM put IV >> ATM = tail hedging panic
    IV_SKEW_COLLAPSED = 0.85     # OTM put IV < ATM = cheap protection
    MAX_PAIN_DIVERGENCE_PCT = 8  # Spot >8% from max pain
    OI_CONCENTRATION_HIGH = 0.15 # >15% of total OI at one strike
    TERM_INVERSION_THRESHOLD = -0.05  # Negative term slope = backwardation
    IV_PERCENTILE_CHEAP = 10     # Below 10th percentile = historically cheap
    IV_PERCENTILE_RICH = 95      # Above 95th percentile = historically rich

    def __init__(self, db_engine: Engine, lookback_days: int = 252) -> None:
        self.engine = db_engine
        self.lookback_days = lookback_days
        log.info(
            "OptionsScanner initialised — lookback={d}d", d=lookback_days
        )

    def scan_all(
        self,
        tickers: list[str] | None = None,
        scan_date: date | None = None,
        min_score: float = 5.0,
    ) -> list[MispricingOpportunity]:
        """Run the full mispricing scan across all tickers.

        Parameters:
            tickers: Override ticker list (default: all tickers with data).
            scan_date: Date to scan (default: today).
            min_score: Minimum composite score to include (default: 5.0).

        Returns:
            list[MispricingOpportunity]: Sorted by score descending.
        """
        if scan_date is None:
            scan_date = date.today()

        if tickers is None:
            tickers = self._get_available_tickers()

        log.info(
            "Scanning {n} tickers for mispricing — date={d}, min_score={s}",
            n=len(tickers), d=scan_date, s=min_score,
        )

        opportunities: list[MispricingOpportunity] = []

        for ticker in tickers:
            ticker_opps = self._scan_ticker(ticker, scan_date)
            opportunities.extend(ticker_opps)

        # Filter and sort
        filtered = [o for o in opportunities if o.score >= min_score]
        filtered.sort(key=lambda o: o.score, reverse=True)

        n_100x = sum(1 for o in filtered if o.is_100x)
        log.info(
            "Scan complete — {n} opportunities ({x} potential 100x+)",
            n=len(filtered), x=n_100x,
        )

        # Send email alerts for any 100x+ opportunities
        if n_100x > 0:
            try:
                from alerts.email import alert_on_100x_opportunity
                for opp in filtered:
                    if opp.is_100x:
                        alert_on_100x_opportunity(
                            ticker=opp.ticker,
                            score=opp.score,
                            direction=opp.direction,
                            thesis=opp.thesis,
                        )
            except Exception:
                pass

        return filtered

    def get_100x_opportunities(
        self,
        scan_date: date | None = None,
    ) -> list[MispricingOpportunity]:
        """Convenience method: scan and return only 100x+ flagged opportunities.

        Parameters:
            scan_date: Date to scan (default: today).

        Returns:
            list[MispricingOpportunity]: Only opportunities with >= 100x payoff.
        """
        all_opps = self.scan_all(scan_date=scan_date, min_score=6.0)
        return [o for o in all_opps if o.is_100x]

    def _get_available_tickers(self) -> list[str]:
        """Get tickers that have options_daily_signals data."""
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT DISTINCT ticker FROM options_daily_signals "
                    "ORDER BY ticker"
                )
            ).fetchall()
        return [r[0] for r in rows]

    def _scan_ticker(
        self, ticker: str, scan_date: date
    ) -> list[MispricingOpportunity]:
        """Run all signal checks for a single ticker."""
        # Get current signals
        current = self._get_current_signals(ticker, scan_date)
        if current is None:
            return []

        # Get historical signals for percentile ranking
        history = self._get_signal_history(ticker, scan_date)

        # Compute individual signal scores
        signals: dict[str, dict[str, Any]] = {}

        # 1. PCR extremes
        pcr_score, pcr_direction = self._score_pcr(current, history)
        signals["pcr"] = {
            "score": pcr_score, "direction": pcr_direction,
            "value": current.get("put_call_ratio"),
        }

        # 2. IV skew dislocation
        skew_score, skew_direction = self._score_iv_skew(current, history)
        signals["iv_skew"] = {
            "score": skew_score, "direction": skew_direction,
            "value": current.get("iv_skew"),
        }

        # 3. Max pain divergence
        pain_score, pain_direction = self._score_max_pain_divergence(current)
        signals["max_pain_div"] = {
            "score": pain_score, "direction": pain_direction,
            "value": current.get("max_pain"),
        }

        # 4. IV term structure
        term_score, term_direction = self._score_term_structure(current)
        signals["term_structure"] = {
            "score": term_score, "direction": term_direction,
            "value": current.get("term_structure_slope"),
        }

        # 5. OI concentration
        oi_score = self._score_oi_concentration(current)
        signals["oi_concentration"] = {
            "score": oi_score, "value": current.get("oi_concentration"),
        }

        # 6. IV percentile rank (cheap/expensive)
        iv_pct_score, iv_pct_direction = self._score_iv_percentile(current, history)
        signals["iv_percentile"] = {
            "score": iv_pct_score, "direction": iv_pct_direction,
            "value": current.get("iv_atm"),
        }

        # 7. Gamma squeeze potential (OI + max pain divergence combo)
        gamma_score = self._score_gamma_squeeze(current)
        signals["gamma_squeeze"] = {"score": gamma_score}

        # Composite score (weighted)
        weights = {
            "pcr": 1.5, "iv_skew": 2.0, "max_pain_div": 1.5,
            "term_structure": 1.5, "oi_concentration": 1.0,
            "iv_percentile": 1.5, "gamma_squeeze": 1.0,
        }
        raw_score = sum(
            signals[k]["score"] * weights[k] for k in weights
        )
        max_possible = sum(10 * w for w in weights.values())
        composite = (raw_score / max_possible) * 10

        # Determine dominant direction
        direction_votes: dict[str, float] = {"CALL": 0, "PUT": 0}
        for sig_name, sig in signals.items():
            d = sig.get("direction", "")
            w = weights.get(sig_name, 1.0)
            if d in direction_votes:
                direction_votes[d] += sig["score"] * w

        dominant_direction = max(direction_votes, key=direction_votes.get)  # type: ignore[arg-type]

        # Estimate payoff multiple
        payoff = self._estimate_payoff_multiple(current, composite, dominant_direction)

        # Build thesis
        thesis = self._build_thesis(ticker, signals, dominant_direction, current)

        # Confidence level
        if composite >= 8.0:
            confidence = "HIGH"
        elif composite >= 6.0:
            confidence = "MEDIUM"
        else:
            confidence = "LOW"

        # Get strike targets
        strikes = self._get_target_strikes(ticker, scan_date, dominant_direction, current)

        opp = MispricingOpportunity(
            ticker=ticker,
            scan_date=scan_date,
            score=round(composite, 2),
            estimated_payoff_multiple=round(payoff, 1),
            direction=dominant_direction,
            thesis=thesis,
            signals=signals,
            strikes=strikes,
            expiry=str(current.get("near_expiry", "")),
            spot_price=current.get("spot_price", 0),
            iv_atm=current.get("iv_atm", 0),
            confidence=confidence,
        )

        # Only return if score is meaningful
        if composite >= 3.0:
            return [opp]
        return []

    # ------------------------------------------------------------------
    # Signal scoring functions (each returns 0-10 score + direction)
    # ------------------------------------------------------------------

    def _score_pcr(
        self, current: dict, history: pd.DataFrame
    ) -> tuple[float, str]:
        """Score put/call ratio extremes."""
        pcr = current.get("put_call_ratio")
        if pcr is None:
            return (0, "")

        score = 0.0
        direction = ""

        if pcr >= self.PCR_EXTREME_HIGH:
            # Extreme put buying = potential capitulation bottom → buy calls
            score = min(10, (pcr - self.PCR_EXTREME_HIGH) / 0.5 * 10 + 5)
            direction = "CALL"
        elif pcr <= self.PCR_EXTREME_LOW:
            # Extreme complacency = potential top → buy puts
            score = min(10, (self.PCR_EXTREME_LOW - pcr) / 0.2 * 10 + 5)
            direction = "PUT"

        # Percentile boost
        if not history.empty and "put_call_ratio" in history.columns:
            pct = (history["put_call_ratio"] < pcr).mean() * 100
            if pct >= 95 or pct <= 5:
                score = min(10, score + 2)

        return (score, direction)

    def _score_iv_skew(
        self, current: dict, history: pd.DataFrame
    ) -> tuple[float, str]:
        """Score IV skew dislocations."""
        skew = current.get("iv_skew")
        if skew is None:
            return (0, "")

        score = 0.0
        direction = ""

        if skew >= self.IV_SKEW_EXTREME:
            # Panic hedging with OTM puts = puts are expensive, calls are cheap
            # Contrarian: buy calls (the fear is overdone)
            score = min(10, (skew - self.IV_SKEW_EXTREME) / 0.3 * 10 + 5)
            direction = "CALL"
        elif skew <= self.IV_SKEW_COLLAPSED:
            # OTM puts are cheap relative to ATM = tail risk is underpriced
            # Buy puts (protection is cheap)
            score = min(10, (self.IV_SKEW_COLLAPSED - skew) / 0.15 * 10 + 5)
            direction = "PUT"

        # Percentile boost
        if not history.empty and "iv_skew" in history.columns:
            pct = (history["iv_skew"] < skew).mean() * 100
            if pct >= 95 or pct <= 5:
                score = min(10, score + 2)

        return (score, direction)

    def _score_max_pain_divergence(self, current: dict) -> tuple[float, str]:
        """Score spot price distance from max pain."""
        spot = current.get("spot_price")
        max_pain = current.get("max_pain")
        if spot is None or max_pain is None or spot == 0:
            return (0, "")

        divergence_pct = abs(spot - max_pain) / spot * 100

        if divergence_pct < self.MAX_PAIN_DIVERGENCE_PCT:
            return (0, "")

        score = min(10, (divergence_pct - self.MAX_PAIN_DIVERGENCE_PCT) / 5 * 10 + 3)

        # Direction: expect mean-reversion toward max pain
        if spot > max_pain:
            direction = "PUT"   # Spot above max pain → expect pullback
        else:
            direction = "CALL"  # Spot below max pain → expect rally

        return (score, direction)

    def _score_term_structure(self, current: dict) -> tuple[float, str]:
        """Score IV term structure inversions."""
        slope = current.get("term_structure_slope")
        if slope is None:
            return (0, "")

        if slope >= self.TERM_INVERSION_THRESHOLD:
            return (0, "")

        # Inverted term structure = near-term event priced in
        score = min(10, abs(slope) / 0.15 * 10)

        # Strong inversion = big near-term move expected
        # Direction depends on other signals (default PUT for fear events)
        pcr = current.get("put_call_ratio", 1.0)
        direction = "PUT" if (pcr and pcr > 1.0) else "CALL"

        return (score, direction)

    def _score_oi_concentration(self, current: dict) -> float:
        """Score unusual OI concentration at single strikes."""
        conc = current.get("oi_concentration")
        if conc is None:
            return 0

        if conc < self.OI_CONCENTRATION_HIGH:
            return 0

        return min(10, (conc - self.OI_CONCENTRATION_HIGH) / 0.1 * 10 + 3)

    def _score_iv_percentile(
        self, current: dict, history: pd.DataFrame
    ) -> tuple[float, str]:
        """Score IV percentile rank (cheap or expensive vol)."""
        iv_atm = current.get("iv_atm")
        if iv_atm is None:
            return (0, "")

        if history.empty or "iv_atm" not in history.columns:
            return (0, "")

        hist_iv = history["iv_atm"].dropna()
        if len(hist_iv) < 20:
            return (0, "")

        percentile = (hist_iv < iv_atm).mean() * 100

        score = 0.0
        direction = ""

        if percentile <= self.IV_PERCENTILE_CHEAP:
            # IV at historical lows = options are cheap → buy straddles/strangles
            score = min(10, (self.IV_PERCENTILE_CHEAP - percentile) / 5 * 10 + 5)
            direction = "CALL"  # Long vol via calls (cheaper)
        elif percentile >= self.IV_PERCENTILE_RICH:
            # IV at historical highs = options are rich → sell premium
            # But for 100x screening, extreme IV + extreme skew = potential dislocation
            score = min(10, (percentile - self.IV_PERCENTILE_RICH) / 3 * 10 + 3)
            direction = "PUT"

        return (score, direction)

    def _score_gamma_squeeze(self, current: dict) -> float:
        """Score gamma squeeze potential (high OI concentration + max pain divergence)."""
        conc = current.get("oi_concentration", 0) or 0
        spot = current.get("spot_price", 0) or 0
        max_pain = current.get("max_pain", 0) or 0

        if spot == 0:
            return 0

        divergence = abs(spot - max_pain) / spot * 100
        total_oi = current.get("total_oi", 0) or 0

        # High OI + high divergence + meaningful OI volume
        if conc >= 0.10 and divergence >= 5 and total_oi >= 50000:
            return min(10, (conc * 100 * divergence / 10))

        return 0

    # ------------------------------------------------------------------
    # Payoff estimation
    # ------------------------------------------------------------------

    def _estimate_payoff_multiple(
        self, current: dict, composite_score: float, direction: str
    ) -> float:
        """Estimate potential payoff multiple for a deep OTM option.

        This is a rough estimate based on:
        - How cheap the option is (IV percentile)
        - How large the potential move is (max pain divergence + signals)
        - The leverage of deep OTM options

        100x payoffs come from buying cheap, deep OTM options before a large
        move. Low IV + extreme positioning + catalysts = the setup.
        """
        iv_atm = current.get("iv_atm")
        spot = current.get("spot_price", 0) or 0
        max_pain = current.get("max_pain", 0) or 0

        if iv_atm is None or spot == 0:
            return composite_score * 5  # Rough fallback

        # Base leverage: lower IV = cheaper options = higher payoff potential
        if iv_atm > 0:
            # ATM options at 15% IV cost ~1/3 of options at 45% IV
            iv_leverage = max(1, 0.40 / iv_atm)  # normalized to 40% IV baseline
        else:
            iv_leverage = 1.0

        # Move magnitude from max pain divergence
        divergence_pct = abs(spot - max_pain) / spot * 100

        # Deep OTM payoff is highly nonlinear:
        # A 10% move on a 5-delta option (20:1 odds) can produce 200x
        # A 5% move on a 20-delta option (5:1 odds) can produce 25x
        #
        # Rough model: payoff = (expected_move / option_cost) ^ leverage
        # For deep OTM: option_cost ~ 0.5-2% of spot
        # If expected move is 10% and option cost is 0.5%, payoff ~ 20x base

        expected_move_pct = max(divergence_pct, composite_score * 1.5)
        otm_cost_pct = iv_atm * 0.5  # Deep OTM ~50% of ATM IV as cost proxy

        if otm_cost_pct > 0:
            base_payoff = (expected_move_pct / (otm_cost_pct * 100)) * iv_leverage * 10
        else:
            base_payoff = composite_score * 10

        # Score multiplier: higher conviction = higher estimated payoff
        score_mult = composite_score / 5  # 1.0 at score=5, 2.0 at score=10

        payoff = base_payoff * score_mult

        # Cap at reasonable maximum (options can't go above ~1000x in practice)
        return min(payoff, 1000.0)

    # ------------------------------------------------------------------
    # Thesis generation
    # ------------------------------------------------------------------

    def _build_thesis(
        self,
        ticker: str,
        signals: dict[str, dict[str, Any]],
        direction: str,
        current: dict,
    ) -> str:
        """Build a human-readable thesis string."""
        parts: list[str] = [f"{ticker} {direction}S:"]

        spot = current.get("spot_price", 0)
        max_pain = current.get("max_pain", 0)

        active_signals = [
            (name, sig) for name, sig in signals.items() if sig.get("score", 0) >= 3
        ]

        for name, sig in sorted(active_signals, key=lambda x: x[1]["score"], reverse=True):
            val = sig.get("value", "N/A")
            if name == "pcr":
                parts.append(f"P/C ratio at {val:.2f} (extreme)" if val else "")
            elif name == "iv_skew":
                parts.append(f"IV skew at {val:.2f} (dislocated)" if val else "")
            elif name == "max_pain_div":
                if spot and max_pain:
                    div = abs(spot - max_pain) / spot * 100
                    parts.append(f"Spot ${spot:.0f} vs max pain ${max_pain:.0f} ({div:.1f}% divergence)")
            elif name == "term_structure":
                parts.append(f"IV term structure inverted (slope={val:.3f})" if val else "")
            elif name == "iv_percentile":
                parts.append(f"IV at ATM={val:.1%} (historically extreme)" if val else "")
            elif name == "oi_concentration":
                parts.append(f"OI concentration={val:.1%} (unusual activity)" if val else "")
            elif name == "gamma_squeeze":
                parts.append("Gamma squeeze setup detected")

        return " | ".join(p for p in parts if p)

    # ------------------------------------------------------------------
    # Helper methods
    # ------------------------------------------------------------------

    def _get_current_signals(self, ticker: str, scan_date: date) -> dict | None:
        """Get the most recent signals for a ticker on or before scan_date."""
        with self.engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT put_call_ratio, max_pain, iv_skew, total_oi, "
                    "total_volume, near_expiry, spot_price, iv_atm, "
                    "iv_25d_put, iv_25d_call, term_structure_slope, "
                    "oi_concentration "
                    "FROM options_daily_signals "
                    "WHERE ticker = :ticker AND signal_date <= :sd "
                    "ORDER BY signal_date DESC LIMIT 1"
                ),
                {"ticker": ticker, "sd": scan_date},
            ).fetchone()

        if row is None:
            return None

        return {
            "put_call_ratio": row[0],
            "max_pain": row[1],
            "iv_skew": row[2],
            "total_oi": row[3],
            "total_volume": row[4],
            "near_expiry": row[5],
            "spot_price": row[6],
            "iv_atm": row[7],
            "iv_25d_put": row[8],
            "iv_25d_call": row[9],
            "term_structure_slope": row[10],
            "oi_concentration": row[11],
        }

    def _get_signal_history(self, ticker: str, scan_date: date) -> pd.DataFrame:
        """Get historical signals for percentile ranking."""
        start = scan_date - timedelta(days=self.lookback_days)
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT signal_date, put_call_ratio, max_pain, iv_skew, "
                    "total_oi, iv_atm, iv_25d_put, iv_25d_call, "
                    "term_structure_slope, oi_concentration "
                    "FROM options_daily_signals "
                    "WHERE ticker = :ticker "
                    "AND signal_date >= :start AND signal_date < :end "
                    "ORDER BY signal_date"
                ),
                {"ticker": ticker, "start": start, "end": scan_date},
            ).fetchall()

        if not rows:
            return pd.DataFrame()

        return pd.DataFrame(rows, columns=[
            "signal_date", "put_call_ratio", "max_pain", "iv_skew",
            "total_oi", "iv_atm", "iv_25d_put", "iv_25d_call",
            "term_structure_slope", "oi_concentration",
        ])

    def _get_target_strikes(
        self,
        ticker: str,
        scan_date: date,
        direction: str,
        current: dict,
    ) -> list[float]:
        """Identify target strikes for the opportunity."""
        spot = current.get("spot_price", 0)
        if not spot:
            return []

        # For 100x payoffs, look at deep OTM strikes
        if direction == "CALL":
            # OTM calls: 10-20% above spot
            return [
                round(spot * 1.10, 2),
                round(spot * 1.15, 2),
                round(spot * 1.20, 2),
            ]
        else:
            # OTM puts: 10-20% below spot
            return [
                round(spot * 0.90, 2),
                round(spot * 0.85, 2),
                round(spot * 0.80, 2),
            ]

    def format_report(self, opportunities: list[MispricingOpportunity]) -> str:
        """Format opportunities into a readable report.

        Parameters:
            opportunities: List of scanned opportunities.

        Returns:
            str: Formatted report string.
        """
        if not opportunities:
            return "No mispricing opportunities found."

        lines: list[str] = [
            "=" * 80,
            "GRID OPTIONS MISPRICING SCANNER",
            f"Scan Date: {opportunities[0].scan_date}",
            f"Total Opportunities: {len(opportunities)}",
            f"100x+ Flagged: {sum(1 for o in opportunities if o.is_100x)}",
            "=" * 80,
            "",
        ]

        for i, opp in enumerate(opportunities, 1):
            flag = " *** 100x+ ***" if opp.is_100x else ""
            lines.extend([
                f"#{i} [{opp.confidence}] {opp.ticker} {opp.direction}{flag}",
                f"   Score: {opp.score}/10  |  Est. Payoff: {opp.estimated_payoff_multiple:.0f}x",
                f"   Spot: ${opp.spot_price:,.2f}  |  IV ATM: {opp.iv_atm:.1%}" if opp.iv_atm else f"   Spot: ${opp.spot_price:,.2f}",
                f"   Target Strikes: {', '.join(f'${s:,.0f}' for s in opp.strikes)}",
                f"   Expiry: {opp.expiry}",
                f"   Thesis: {opp.thesis}",
                "",
            ])

        return "\n".join(lines)

    def persist_scan(
        self,
        opportunities: list[MispricingOpportunity],
    ) -> int:
        """Persist scan results to the database.

        Parameters:
            opportunities: List of opportunities to persist.

        Returns:
            int: Number of rows inserted.
        """
        if not opportunities:
            return 0

        with self.engine.begin() as conn:
            # Ensure table exists
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS options_mispricing_scans (
                    id              BIGSERIAL PRIMARY KEY,
                    ticker          TEXT NOT NULL,
                    scan_date       DATE NOT NULL,
                    score           DOUBLE PRECISION NOT NULL,
                    payoff_multiple DOUBLE PRECISION NOT NULL,
                    direction       TEXT NOT NULL,
                    thesis          TEXT NOT NULL,
                    signals         JSONB,
                    strikes         DOUBLE PRECISION[],
                    expiry          DATE,
                    spot_price      DOUBLE PRECISION,
                    iv_atm          DOUBLE PRECISION,
                    confidence      TEXT NOT NULL,
                    is_100x         BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at      TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (ticker, scan_date, direction)
                )
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_mispricing_score
                ON options_mispricing_scans (score DESC)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_mispricing_100x
                ON options_mispricing_scans (is_100x) WHERE is_100x = TRUE
            """))

            count = 0
            for opp in opportunities:
                import json
                conn.execute(
                    text(
                        "INSERT INTO options_mispricing_scans "
                        "(ticker, scan_date, score, payoff_multiple, direction, "
                        "thesis, signals, strikes, expiry, spot_price, iv_atm, "
                        "confidence, is_100x) "
                        "VALUES (:ticker, :sd, :score, :payoff, :dir, :thesis, "
                        ":signals, :strikes, :expiry, :spot, :iv, :conf, :is100) "
                        "ON CONFLICT (ticker, scan_date, direction) DO UPDATE SET "
                        "score = EXCLUDED.score, "
                        "payoff_multiple = EXCLUDED.payoff_multiple, "
                        "thesis = EXCLUDED.thesis, "
                        "signals = EXCLUDED.signals, "
                        "strikes = EXCLUDED.strikes, "
                        "spot_price = EXCLUDED.spot_price, "
                        "iv_atm = EXCLUDED.iv_atm, "
                        "confidence = EXCLUDED.confidence, "
                        "is_100x = EXCLUDED.is_100x"
                    ),
                    {
                        "ticker": opp.ticker,
                        "sd": opp.scan_date,
                        "score": opp.score,
                        "payoff": opp.estimated_payoff_multiple,
                        "dir": opp.direction,
                        "thesis": opp.thesis,
                        "signals": json.dumps({
                            k: {"score": v["score"], "value": str(v.get("value", ""))}
                            for k, v in opp.signals.items()
                        }),
                        "strikes": opp.strikes,
                        "expiry": opp.expiry if opp.expiry else None,
                        "spot": opp.spot_price,
                        "iv": opp.iv_atm,
                        "conf": opp.confidence,
                        "is100": opp.is_100x,
                    },
                )
                count += 1

        log.info("Persisted {n} mispricing scan results", n=count)
        return count


if __name__ == "__main__":
    from db import get_engine

    engine = get_engine()
    scanner = OptionsScanner(engine)

    # Full scan
    opps = scanner.scan_all(min_score=4.0)
    print(scanner.format_report(opps))

    # Persist results
    if opps:
        scanner.persist_scan(opps)

    # 100x only
    big_opps = scanner.get_100x_opportunities()
    if big_opps:
        print("\n\n=== 100x+ OPPORTUNITIES ===")
        print(scanner.format_report(big_opps))
