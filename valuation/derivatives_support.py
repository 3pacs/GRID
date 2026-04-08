"""
GRID — Derivatives Support Gauge.

Computes a composite score measuring how much the current stock price
is being supported (or pressured) by derivatives positioning:

1. Short positioning: short float %, days to cover, borrow rate, changes
2. Dealer gamma: GEX regime, gamma walls, vanna/charm from DealerGammaEngine
3. Options sentiment: put/call ratio, IV skew, max pain distance, IV percentile

The composite produces a 0-100 "derivatives support score":
  - 80-100: Strong support — dealers providing bid, shorts squeezable, options bullish
  - 60-80:  Mild support
  - 40-60:  Neutral
  - 20-40:  Mild pressure — dealers amplifying sells, shorts building, options bearish
  - 0-20:   Strong pressure

This directly answers: "Is the price being held up by derivatives, or is
derivatives positioning working against it?"
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass
class ShortPositioning:
    """Short interest data for a ticker."""

    short_float_pct: float | None = None
    short_interest: float | None = None
    days_to_cover: float | None = None
    short_change_pct: float | None = None
    borrow_rate: float | None = None

    def score(self) -> float:
        """Score short positioning pressure (0 = max pressure, 100 = max support).

        High short interest = pressure on price (low score).
        BUT extremely high short interest = squeeze potential (higher score).
        """
        if self.short_float_pct is None:
            return 50.0  # Neutral if no data

        pct = self.short_float_pct

        # Scoring curve: moderate shorts = pressure, extreme shorts = squeeze potential
        if pct < 2:
            score = 80.0  # Very low short interest = supportive
        elif pct < 5:
            score = 65.0  # Normal range
        elif pct < 10:
            score = 45.0  # Elevated shorts = mild pressure
        elif pct < 20:
            score = 30.0  # High shorts = significant pressure
        elif pct < 30:
            score = 35.0  # Very high = pressure but squeeze building
        else:
            score = 45.0  # Extreme = squeeze imminent, somewhat supportive

        # Days to cover modifier: high DTC amplifies squeeze potential
        if self.days_to_cover is not None:
            if self.days_to_cover > 5 and pct > 15:
                score += 10  # Squeeze potential
            elif self.days_to_cover > 10 and pct > 10:
                score += 15

        # Short change modifier: increasing shorts = more pressure
        if self.short_change_pct is not None:
            if self.short_change_pct > 10:
                score -= 10  # Shorts piling in
            elif self.short_change_pct < -10:
                score += 10  # Shorts covering = supportive

        return max(0, min(100, score))


@dataclass
class DealerPositioning:
    """Dealer gamma / options flow data."""

    gex_aggregate: float | None = None
    gex_regime: str | None = None
    gamma_flip: float | None = None
    gamma_wall: float | None = None
    put_wall: float | None = None
    call_wall: float | None = None
    vanna_exposure: float | None = None
    charm_exposure: float | None = None
    spot_price: float | None = None

    def score(self) -> float:
        """Score dealer gamma support (0 = amplifying downside, 100 = stabilizing)."""
        if self.gex_regime is None:
            return 50.0

        # Base score from regime
        if self.gex_regime == "LONG_GAMMA":
            score = 75.0  # Dealers dampening moves = stabilizing
        elif self.gex_regime == "SHORT_GAMMA":
            score = 25.0  # Dealers amplifying moves = destabilizing
        else:
            score = 50.0  # Neutral

        # Gamma wall proximity: if spot is near put wall, support is strong
        if self.spot_price and self.put_wall and self.put_wall > 0:
            put_dist = (self.spot_price - self.put_wall) / self.spot_price
            if 0 < put_dist < 0.02:
                score += 15  # Very close to put wall support
            elif 0 < put_dist < 0.05:
                score += 8
            elif put_dist < 0:
                score -= 10  # Below put wall, broken support

        # Call wall as ceiling resistance
        if self.spot_price and self.call_wall and self.call_wall > 0:
            call_dist = (self.call_wall - self.spot_price) / self.spot_price
            if call_dist < 0:
                score -= 5  # Above call wall, could pull back
            elif call_dist < 0.02:
                score -= 3  # Near resistance

        # Vanna: positive vanna with falling VIX = supportive (dealers buy delta)
        if self.vanna_exposure is not None and self.vanna_exposure > 0:
            score += 5

        return max(0, min(100, score))


@dataclass
class OptionsSentiment:
    """Options market sentiment indicators."""

    put_call_ratio: float | None = None
    iv_skew: float | None = None
    iv_percentile: float | None = None
    max_pain: float | None = None
    max_pain_dist_pct: float | None = None
    total_oi: float | None = None
    spot_price: float | None = None

    def score(self) -> float:
        """Score options sentiment (0 = max bearish, 100 = max bullish)."""
        score = 50.0

        # Put/Call ratio: high = bearish (contrarian bullish at extremes)
        if self.put_call_ratio is not None:
            pcr = self.put_call_ratio
            if pcr < 0.5:
                score += 5   # Complacency (slightly bearish contrarian)
            elif pcr < 0.7:
                score += 10  # Normal-low, mildly bullish
            elif pcr < 1.0:
                score -= 5   # Elevated puts
            elif pcr < 1.3:
                score -= 10  # Heavy puts
            elif pcr > 1.5:
                score += 5   # Extreme fear = contrarian bullish

        # IV skew: high skew = fear in OTM puts (bearish)
        if self.iv_skew is not None:
            if self.iv_skew > 0.10:
                score -= 10  # High skew = hedging demand
            elif self.iv_skew > 0.05:
                score -= 5
            elif self.iv_skew < -0.05:
                score += 5   # Inverted skew = call demand

        # IV percentile: high = expensive options (often after selloff)
        if self.iv_percentile is not None:
            if self.iv_percentile > 80:
                score -= 8   # Very expensive options, fear
            elif self.iv_percentile > 60:
                score -= 3
            elif self.iv_percentile < 20:
                score += 5   # Cheap options, complacency

        # Max pain distance: spot above max pain = options dealers push down
        if self.max_pain_dist_pct is not None:
            dist = self.max_pain_dist_pct
            if dist > 5:
                score -= 8  # Far above max pain, gravity pulls down
            elif dist > 2:
                score -= 3
            elif dist < -5:
                score += 8  # Far below max pain, gravity pulls up
            elif dist < -2:
                score += 3

        return max(0, min(100, score))


@dataclass
class DerivativesSupportResult:
    """Complete derivatives support analysis for a ticker."""

    ticker: str
    snap_date: date
    spot_price: float | None
    intrinsic_mid: float | None
    premium_to_intrinsic: float | None

    short: ShortPositioning = field(default_factory=ShortPositioning)
    dealer: DealerPositioning = field(default_factory=DealerPositioning)
    options: OptionsSentiment = field(default_factory=OptionsSentiment)

    short_pressure_score: float = 50.0
    gamma_support_score: float = 50.0
    options_sentiment_score: float = 50.0
    derivatives_support_score: float = 50.0

    support_regime: str = "NEUTRAL"
    narrative: str = ""

    def compute_composite(self) -> None:
        """Compute all sub-scores and the composite."""
        self.short_pressure_score = self.short.score()
        self.gamma_support_score = self.dealer.score()
        self.options_sentiment_score = self.options.score()

        # Weighted composite: gamma has highest weight (most mechanistic)
        self.derivatives_support_score = (
            self.short_pressure_score * 0.25
            + self.gamma_support_score * 0.45
            + self.options_sentiment_score * 0.30
        )

        # Classify regime
        s = self.derivatives_support_score
        if s >= 75:
            self.support_regime = "STRONG_SUPPORT"
        elif s >= 60:
            self.support_regime = "MILD_SUPPORT"
        elif s >= 40:
            self.support_regime = "NEUTRAL"
        elif s >= 25:
            self.support_regime = "MILD_PRESSURE"
        else:
            self.support_regime = "STRONG_PRESSURE"

        self.narrative = self._build_narrative()

    def _build_narrative(self) -> str:
        """Build human-readable explanation of derivatives support."""
        parts = []

        # Short interest narrative
        if self.short.short_float_pct is not None:
            pct = self.short.short_float_pct
            if pct > 20:
                parts.append(
                    f"Extreme short interest ({pct:.1f}% float short, "
                    f"{self.short.days_to_cover or 0:.1f} days to cover) "
                    f"creates significant squeeze potential."
                )
            elif pct > 10:
                parts.append(
                    f"Elevated short interest ({pct:.1f}% float) pressuring price."
                )
            elif pct < 3:
                parts.append(f"Low short interest ({pct:.1f}%) — minimal short pressure.")

        # Dealer positioning narrative
        if self.dealer.gex_regime:
            if self.dealer.gex_regime == "LONG_GAMMA":
                parts.append(
                    "Dealers are LONG GAMMA — hedging flows dampen volatility, "
                    "providing mechanical support."
                )
            elif self.dealer.gex_regime == "SHORT_GAMMA":
                parts.append(
                    "Dealers are SHORT GAMMA — hedging flows amplify moves in "
                    "both directions, increasing fragility."
                )

        if self.dealer.put_wall and self.spot_price:
            dist = (self.spot_price - self.dealer.put_wall) / self.spot_price * 100
            parts.append(f"Put wall at ${self.dealer.put_wall:.0f} ({dist:.1f}% below spot).")

        if self.dealer.gamma_wall and self.spot_price:
            parts.append(f"Gamma wall (resistance) at ${self.dealer.gamma_wall:.0f}.")

        # Options sentiment
        if self.options.put_call_ratio is not None:
            pcr = self.options.put_call_ratio
            if pcr > 1.3:
                parts.append(f"Put/call ratio elevated at {pcr:.2f} — heavy hedging.")
            elif pcr < 0.6:
                parts.append(f"Put/call ratio low at {pcr:.2f} — complacency.")

        # Premium/discount to intrinsic
        if self.premium_to_intrinsic is not None:
            p = self.premium_to_intrinsic * 100
            if p > 20:
                parts.append(
                    f"Price trades at {p:.0f}% PREMIUM to intrinsic value — "
                    f"derivatives support critical for maintaining price."
                )
            elif p < -20:
                parts.append(
                    f"Price trades at {abs(p):.0f}% DISCOUNT to intrinsic value — "
                    f"potential value trap if derivatives pressure continues."
                )

        return " ".join(parts) if parts else "Insufficient data for derivatives narrative."

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "snap_date": str(self.snap_date),
            "spot_price": self.spot_price,
            "intrinsic_mid": self.intrinsic_mid,
            "premium_to_intrinsic": self.premium_to_intrinsic,
            "short_float_pct": self.short.short_float_pct,
            "short_interest": self.short.short_interest,
            "days_to_cover": self.short.days_to_cover,
            "short_change_pct": self.short.short_change_pct,
            "borrow_rate": self.short.borrow_rate,
            "gex_aggregate": self.dealer.gex_aggregate,
            "gex_regime": self.dealer.gex_regime,
            "gamma_flip": self.dealer.gamma_flip,
            "gamma_wall": self.dealer.gamma_wall,
            "put_wall": self.dealer.put_wall,
            "call_wall": self.dealer.call_wall,
            "vanna_exposure": self.dealer.vanna_exposure,
            "charm_exposure": self.dealer.charm_exposure,
            "put_call_ratio": self.options.put_call_ratio,
            "iv_skew": self.options.iv_skew,
            "iv_percentile": self.options.iv_percentile,
            "max_pain": self.options.max_pain,
            "max_pain_dist_pct": self.options.max_pain_dist_pct,
            "short_pressure_score": self.short_pressure_score,
            "gamma_support_score": self.gamma_support_score,
            "options_sentiment_score": self.options_sentiment_score,
            "derivatives_support_score": self.derivatives_support_score,
            "support_regime": self.support_regime,
            "narrative": self.narrative,
        }


class DerivativesSupportEngine:
    """Computes derivatives support gauge for a ticker.

    Pulls from:
      - FINRA short interest (finra.short_interest_total in raw_series)
      - DealerGammaEngine (physics/dealer_gamma.py)
      - options_daily_signals table
      - company_valuations table (for intrinsic value context)
    """

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine

    def analyze(
        self,
        ticker: str,
        snap_date: date | None = None,
        intrinsic_mid: float | None = None,
    ) -> DerivativesSupportResult:
        """Full derivatives support analysis for a ticker."""
        if snap_date is None:
            snap_date = date.today()

        # Gather all data
        spot = self._get_spot_price(ticker, snap_date)
        short = self._get_short_positioning(ticker, snap_date)
        dealer = self._get_dealer_positioning(ticker, snap_date, spot)
        options = self._get_options_sentiment(ticker, snap_date, spot)

        # Get intrinsic value from valuations table if not provided
        if intrinsic_mid is None:
            intrinsic_mid = self._get_intrinsic_mid(ticker, snap_date)

        premium = None
        if spot and intrinsic_mid and intrinsic_mid > 0:
            premium = (spot - intrinsic_mid) / intrinsic_mid

        result = DerivativesSupportResult(
            ticker=ticker.upper(),
            snap_date=snap_date,
            spot_price=spot,
            intrinsic_mid=intrinsic_mid,
            premium_to_intrinsic=premium,
            short=short,
            dealer=dealer,
            options=options,
        )
        result.compute_composite()
        return result

    def analyze_and_store(
        self,
        ticker: str,
        snap_date: date | None = None,
        intrinsic_mid: float | None = None,
    ) -> DerivativesSupportResult:
        """Analyze and persist to derivatives_support table."""
        result = self.analyze(ticker, snap_date, intrinsic_mid)
        self._store(result)
        return result

    def get_history(self, ticker: str, days: int = 90) -> list[dict[str, Any]]:
        """Retrieve derivatives support history."""
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT snap_date, spot_price, intrinsic_mid, premium_to_intrinsic,
                           short_float_pct, days_to_cover, gex_regime,
                           gamma_wall, put_wall, put_call_ratio, iv_skew,
                           derivatives_support_score, support_regime, narrative
                    FROM derivatives_support
                    WHERE ticker = :ticker AND snap_date >= CURRENT_DATE - :days
                    ORDER BY snap_date
                """),
                {"ticker": ticker.upper(), "days": days},
            ).fetchall()

        return [
            {
                "date": str(r[0]), "spot_price": r[1], "intrinsic_mid": r[2],
                "premium_to_intrinsic": r[3], "short_float_pct": r[4],
                "days_to_cover": r[5], "gex_regime": r[6], "gamma_wall": r[7],
                "put_wall": r[8], "put_call_ratio": r[9], "iv_skew": r[10],
                "derivatives_support_score": r[11], "support_regime": r[12],
                "narrative": r[13],
            }
            for r in rows
        ]

    def _get_spot_price(self, ticker: str, as_of: date) -> float | None:
        """Get latest spot price from FMP or Tiingo data."""
        with self.engine.connect() as conn:
            for prefix in [f"fmp:{ticker}:price", f"TIINGO_FUND:{ticker}:market_cap"]:
                row = conn.execute(
                    text("""
                        SELECT value FROM raw_series
                        WHERE series_id = :sid AND obs_date <= :as_of
                        ORDER BY obs_date DESC, pull_timestamp DESC LIMIT 1
                    """),
                    {"sid": prefix, "as_of": as_of},
                ).fetchone()
                if row is not None:
                    return row[0]
        return None

    def _get_short_positioning(self, ticker: str, as_of: date) -> ShortPositioning:
        """Pull short interest data from FINRA raw_series and options signals."""
        short = ShortPositioning()

        with self.engine.connect() as conn:
            # Aggregate FINRA short interest
            row = conn.execute(
                text("""
                    SELECT value FROM raw_series
                    WHERE series_id = :sid AND obs_date <= :as_of
                    ORDER BY obs_date DESC LIMIT 1
                """),
                {"sid": "finra.short_interest_total", "as_of": as_of},
            ).fetchone()
            if row:
                short.short_interest = row[0]

            # Try to get per-ticker short data if available
            row = conn.execute(
                text("""
                    SELECT value FROM raw_series
                    WHERE series_id = :sid AND obs_date <= :as_of
                    ORDER BY obs_date DESC LIMIT 1
                """),
                {"sid": f"finra.short_interest:{ticker}", "as_of": as_of},
            ).fetchone()
            if row:
                short.short_interest = row[0]

            # Short change: compare current vs previous period
            rows = conn.execute(
                text("""
                    SELECT value, obs_date FROM raw_series
                    WHERE series_id LIKE :pattern AND obs_date <= :as_of
                    ORDER BY obs_date DESC LIMIT 2
                """),
                {"pattern": f"finra.short_interest%{ticker}%", "as_of": as_of},
            ).fetchall()
            if len(rows) >= 2 and rows[1][0] > 0:
                short.short_change_pct = (rows[0][0] - rows[1][0]) / rows[1][0] * 100

        return short

    def _get_dealer_positioning(
        self, ticker: str, as_of: date, spot: float | None
    ) -> DealerPositioning:
        """Pull dealer gamma data from DealerGammaEngine."""
        dealer = DealerPositioning(spot_price=spot)

        try:
            from physics.dealer_gamma import DealerGammaEngine
            gex_engine = DealerGammaEngine(self.engine)
            profile = gex_engine.compute_gex_profile(ticker.upper(), snap_date=as_of)

            if profile and not profile.get("error"):
                dealer.gex_aggregate = profile.get("gex_aggregate")
                dealer.gex_regime = profile.get("regime")
                dealer.gamma_flip = profile.get("gamma_flip")
                dealer.gamma_wall = profile.get("gamma_wall")
                dealer.put_wall = profile.get("put_wall")
                dealer.call_wall = profile.get("call_wall")
                dealer.vanna_exposure = profile.get("vanna_exposure")
                dealer.charm_exposure = profile.get("charm_exposure")
        except Exception as exc:
            log.debug("DealerGammaEngine unavailable for {t}: {e}", t=ticker, e=str(exc))

        return dealer

    def _get_options_sentiment(
        self, ticker: str, as_of: date, spot: float | None
    ) -> OptionsSentiment:
        """Pull options sentiment from options_daily_signals table."""
        sentiment = OptionsSentiment(spot_price=spot)

        with self.engine.connect() as conn:
            row = conn.execute(
                text("""
                    SELECT put_call_ratio, iv_skew, iv_atm, max_pain,
                           total_oi, spot_price
                    FROM options_daily_signals
                    WHERE ticker = :ticker AND signal_date <= :as_of
                    ORDER BY signal_date DESC LIMIT 1
                """),
                {"ticker": ticker.upper(), "as_of": as_of},
            ).fetchone()

            if row:
                sentiment.put_call_ratio = row[0]
                sentiment.iv_skew = row[1]
                sentiment.max_pain = row[3]
                sentiment.total_oi = row[4]
                if spot and row[3] and spot > 0:
                    sentiment.max_pain_dist_pct = (spot - row[3]) / spot * 100

        return sentiment

    def _get_intrinsic_mid(self, ticker: str, as_of: date) -> float | None:
        """Get the most recent intrinsic mid value from company_valuations."""
        with self.engine.connect() as conn:
            row = conn.execute(
                text("""
                    SELECT intrinsic_mid FROM company_valuations
                    WHERE ticker = :ticker AND valuation_date <= :as_of
                    ORDER BY valuation_date DESC LIMIT 1
                """),
                {"ticker": ticker.upper(), "as_of": as_of},
            ).fetchone()
            return row[0] if row else None

    def _store(self, result: DerivativesSupportResult) -> None:
        """Persist derivatives support result to database."""
        d = result.to_dict()
        with self.engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO derivatives_support (
                        ticker, snap_date, spot_price, intrinsic_mid, premium_to_intrinsic,
                        short_float_pct, short_interest, days_to_cover, short_change_pct, borrow_rate,
                        gex_aggregate, gex_regime, gamma_flip, gamma_wall, put_wall, call_wall,
                        vanna_exposure, charm_exposure,
                        put_call_ratio, iv_skew, iv_percentile, max_pain, max_pain_dist_pct,
                        short_pressure_score, gamma_support_score, options_sentiment_score,
                        derivatives_support_score, support_regime, narrative
                    ) VALUES (
                        :ticker, :snap_date, :spot_price, :intrinsic_mid, :premium_to_intrinsic,
                        :short_float_pct, :short_interest, :days_to_cover, :short_change_pct, :borrow_rate,
                        :gex_aggregate, :gex_regime, :gamma_flip, :gamma_wall, :put_wall, :call_wall,
                        :vanna_exposure, :charm_exposure,
                        :put_call_ratio, :iv_skew, :iv_percentile, :max_pain, :max_pain_dist_pct,
                        :short_pressure_score, :gamma_support_score, :options_sentiment_score,
                        :derivatives_support_score, :support_regime, :narrative
                    )
                    ON CONFLICT (ticker, snap_date) DO UPDATE SET
                        spot_price = EXCLUDED.spot_price,
                        intrinsic_mid = EXCLUDED.intrinsic_mid,
                        premium_to_intrinsic = EXCLUDED.premium_to_intrinsic,
                        short_float_pct = EXCLUDED.short_float_pct,
                        short_interest = EXCLUDED.short_interest,
                        days_to_cover = EXCLUDED.days_to_cover,
                        short_change_pct = EXCLUDED.short_change_pct,
                        gex_aggregate = EXCLUDED.gex_aggregate,
                        gex_regime = EXCLUDED.gex_regime,
                        gamma_flip = EXCLUDED.gamma_flip,
                        gamma_wall = EXCLUDED.gamma_wall,
                        put_wall = EXCLUDED.put_wall,
                        call_wall = EXCLUDED.call_wall,
                        vanna_exposure = EXCLUDED.vanna_exposure,
                        charm_exposure = EXCLUDED.charm_exposure,
                        put_call_ratio = EXCLUDED.put_call_ratio,
                        iv_skew = EXCLUDED.iv_skew,
                        iv_percentile = EXCLUDED.iv_percentile,
                        max_pain = EXCLUDED.max_pain,
                        max_pain_dist_pct = EXCLUDED.max_pain_dist_pct,
                        short_pressure_score = EXCLUDED.short_pressure_score,
                        gamma_support_score = EXCLUDED.gamma_support_score,
                        options_sentiment_score = EXCLUDED.options_sentiment_score,
                        derivatives_support_score = EXCLUDED.derivatives_support_score,
                        support_regime = EXCLUDED.support_regime,
                        narrative = EXCLUDED.narrative
                """),
                d,
            )
        log.info("Stored derivatives support for {t} @ {d}: {r} ({s:.0f})",
                 t=result.ticker, d=result.snap_date,
                 r=result.support_regime, s=result.derivatives_support_score)
