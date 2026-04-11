"""
GRID Intelligence — News Momentum Signal Engine.

Tracks sentiment acceleration/deceleration per ticker and detects
divergences between price direction and news sentiment momentum.

Signals produced:
    - MOMENTUM: sentiment is accelerating (getting more bullish/bearish faster)
    - DECELERATION: sentiment rate of change is slowing
    - DIVERGENCE: price moving one way, sentiment moving the other
    - CONVERGENCE: price and sentiment aligning (confirmation signal)

Architecture:
    1. SentimentTimeSeries  — build per-ticker sentiment time series from news_articles
    2. MomentumCalculator   — compute first & second derivatives of sentiment
    3. DivergenceDetector   — compare sentiment trajectory vs price trajectory
    4. NewsMomentumEngine   — orchestrator that produces actionable signals

The engine runs every 6 hours (aligned with Oracle cycle) and stores
momentum snapshots in the news_momentum table for historical tracking.
"""

from __future__ import annotations

import hashlib
import json
import math
import statistics
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Constants ────────────────────────────────────────────────────────────

# Lookback windows for momentum calculation
SHORT_WINDOW_HOURS = 24      # 1 day — "what happened today"
MEDIUM_WINDOW_HOURS = 72     # 3 days — "recent trend"
LONG_WINDOW_HOURS = 168      # 7 days — "weekly baseline"

# Minimum articles to compute meaningful sentiment
MIN_ARTICLES_SHORT = 3
MIN_ARTICLES_MEDIUM = 5
MIN_ARTICLES_LONG = 8

# Divergence detection thresholds
DIVERGENCE_THRESHOLD = 0.4   # sentiment delta vs price delta mismatch
ACCELERATION_THRESHOLD = 0.15  # min delta between windows to count as accelerating
STRONG_MOMENTUM_THRESHOLD = 0.5  # absolute sentiment score for "strong" label

# Momentum signal decay (hours)
SIGNAL_VALID_HOURS = 12


# ── Data Classes ─────────────────────────────────────────────────────────

@dataclass
class SentimentSnapshot:
    """Sentiment measurement for a specific window."""
    ticker: str
    window_hours: int
    article_count: int
    bullish_count: int
    bearish_count: int
    neutral_count: int
    avg_confidence: float
    sentiment_score: float    # [-1.0, +1.0]: (bullish - bearish) / total
    weighted_score: float     # confidence-weighted sentiment
    computed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class MomentumSignal:
    """A computed momentum signal for a ticker."""
    signal_id: str
    ticker: str
    signal_type: str          # ACCELERATING, DECELERATING, DIVERGENCE, CONVERGENCE
    direction: str            # bullish, bearish, neutral
    magnitude: float          # 0.0 to 1.0 — how strong is the signal
    sentiment_velocity: float  # rate of change (short - long)
    sentiment_acceleration: float  # second derivative (velocity change)
    price_direction: str | None  # up, down, flat — from price data
    price_pct: float | None   # price change % over the period
    confidence: float         # 0.0 to 1.0
    short_score: float        # sentiment score over short window
    medium_score: float       # sentiment score over medium window
    long_score: float         # sentiment score over long window
    article_count: int        # total articles across all windows
    evidence: list[str] = field(default_factory=list)
    computed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["computed_at"] = self.computed_at.isoformat()
        return d


# ── Sentiment Time Series ────────────────────────────────────────────────

class SentimentTimeSeries:
    """Builds windowed sentiment snapshots from news_articles table."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def get_snapshot(
        self,
        ticker: str,
        window_hours: int,
        now: datetime | None = None,
    ) -> SentimentSnapshot | None:
        """Compute sentiment snapshot for a ticker over a time window.

        Args:
            ticker: Ticker symbol (e.g. "AAPL").
            window_hours: How many hours to look back.
            now: Reference time (default: UTC now).

        Returns:
            SentimentSnapshot or None if insufficient data.
        """
        now = now or datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=window_hours)

        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text("""
                        SELECT sentiment, confidence
                        FROM news_articles
                        WHERE :ticker = ANY(tickers)
                          AND created_at >= :cutoff
                          AND created_at <= :now
                    """),
                    {"ticker": ticker.upper(), "cutoff": cutoff, "now": now},
                ).fetchall()
        except Exception as exc:
            log.warning(
                "Sentiment snapshot query failed for {t}: {e}",
                t=ticker, e=str(exc),
            )
            return None

        if not rows:
            return None

        bullish = sum(1 for r in rows if r[0] == "BULLISH")
        bearish = sum(1 for r in rows if r[0] == "BEARISH")
        neutral = sum(1 for r in rows if r[0] not in ("BULLISH", "BEARISH"))
        total = len(rows)

        avg_conf = sum(float(r[1] or 0.5) for r in rows) / total

        # Raw score: proportion bullish minus proportion bearish
        sentiment_score = (bullish - bearish) / total

        # Weighted score: factor in confidence
        weighted_sum = sum(
            (1.0 if r[0] == "BULLISH" else -1.0 if r[0] == "BEARISH" else 0.0)
            * float(r[1] or 0.5)
            for r in rows
        )
        weighted_score = weighted_sum / total

        return SentimentSnapshot(
            ticker=ticker.upper(),
            window_hours=window_hours,
            article_count=total,
            bullish_count=bullish,
            bearish_count=bearish,
            neutral_count=neutral,
            avg_confidence=round(avg_conf, 4),
            sentiment_score=round(sentiment_score, 4),
            weighted_score=round(weighted_score, 4),
            computed_at=now,
        )

    def get_market_snapshot(
        self,
        window_hours: int,
        now: datetime | None = None,
    ) -> SentimentSnapshot | None:
        """Compute aggregate market sentiment (all tickers combined).

        Args:
            window_hours: How many hours to look back.
            now: Reference time.

        Returns:
            SentimentSnapshot for "MARKET" or None.
        """
        now = now or datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=window_hours)

        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text("""
                        SELECT sentiment, confidence
                        FROM news_articles
                        WHERE created_at >= :cutoff
                          AND created_at <= :now
                    """),
                    {"cutoff": cutoff, "now": now},
                ).fetchall()
        except Exception as exc:
            log.warning("Market sentiment query failed: {e}", e=str(exc))
            return None

        if not rows:
            return None

        bullish = sum(1 for r in rows if r[0] == "BULLISH")
        bearish = sum(1 for r in rows if r[0] == "BEARISH")
        neutral = len(rows) - bullish - bearish
        total = len(rows)
        avg_conf = sum(float(r[1] or 0.5) for r in rows) / total
        sentiment_score = (bullish - bearish) / total
        weighted_sum = sum(
            (1.0 if r[0] == "BULLISH" else -1.0 if r[0] == "BEARISH" else 0.0)
            * float(r[1] or 0.5)
            for r in rows
        )
        weighted_score = weighted_sum / total

        return SentimentSnapshot(
            ticker="MARKET",
            window_hours=window_hours,
            article_count=total,
            bullish_count=bullish,
            bearish_count=bearish,
            neutral_count=neutral,
            avg_confidence=round(avg_conf, 4),
            sentiment_score=round(sentiment_score, 4),
            weighted_score=round(weighted_score, 4),
            computed_at=now,
        )


# ── Momentum Calculator ─────────────────────────────────────────────────

class MomentumCalculator:
    """Computes first and second derivatives of sentiment over time."""

    def compute_velocity(
        self,
        short: SentimentSnapshot,
        long: SentimentSnapshot,
    ) -> float:
        """First derivative: rate of change from long to short window.

        Positive = sentiment becoming more bullish.
        Negative = sentiment becoming more bearish.

        Args:
            short: Recent sentiment snapshot.
            long: Longer-window baseline snapshot.

        Returns:
            Velocity in [-2.0, +2.0].
        """
        return round(short.weighted_score - long.weighted_score, 4)

    def compute_acceleration(
        self,
        short: SentimentSnapshot,
        medium: SentimentSnapshot,
        long: SentimentSnapshot,
    ) -> float:
        """Second derivative: is velocity itself speeding up or slowing down?

        Positive = sentiment momentum accelerating (bullish getting more bullish).
        Negative = sentiment momentum decelerating.

        Args:
            short: 24h snapshot.
            medium: 72h snapshot.
            long: 168h snapshot.

        Returns:
            Acceleration value.
        """
        velocity_recent = short.weighted_score - medium.weighted_score
        velocity_prior = medium.weighted_score - long.weighted_score
        return round(velocity_recent - velocity_prior, 4)

    def classify_momentum(
        self,
        velocity: float,
        acceleration: float,
        short_score: float,
    ) -> tuple[str, str]:
        """Classify momentum state and direction.

        Args:
            velocity: Sentiment velocity (first derivative).
            acceleration: Sentiment acceleration (second derivative).
            short_score: Current short-window sentiment score.

        Returns:
            Tuple of (signal_type, direction).
        """
        # Direction from current sentiment
        if short_score > 0.1:
            direction = "bullish"
        elif short_score < -0.1:
            direction = "bearish"
        else:
            direction = "neutral"

        # Signal type from derivatives
        if abs(acceleration) >= ACCELERATION_THRESHOLD:
            # Sentiment is changing velocity
            if acceleration > 0 and velocity > 0:
                return "ACCELERATING", direction
            elif acceleration < 0 and velocity < 0:
                return "ACCELERATING", direction
            else:
                return "DECELERATING", direction
        elif abs(velocity) >= ACCELERATION_THRESHOLD:
            return "ACCELERATING", direction
        else:
            return "STEADY", direction


# ── Divergence Detector ──────────────────────────────────────────────────

class DivergenceDetector:
    """Detects divergences between sentiment trajectory and price movement."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def get_price_direction(
        self,
        ticker: str,
        days: int = 5,
    ) -> tuple[str | None, float | None]:
        """Get recent price direction for a ticker from raw_series.

        Args:
            ticker: Ticker symbol.
            days: Days to look back.

        Returns:
            Tuple of (direction, pct_change) or (None, None) if unavailable.
        """
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text("""
                        SELECT obs_date, value
                        FROM raw_series
                        WHERE series_id = :sid
                          AND obs_date >= CURRENT_DATE - :days
                        ORDER BY obs_date ASC
                    """),
                    {"sid": f"PRICE:{ticker}", "days": days},
                ).fetchall()

                if len(rows) < 2:
                    # Try alternative series ID pattern
                    rows = conn.execute(
                        text("""
                            SELECT obs_date, value
                            FROM raw_series
                            WHERE series_id LIKE :pattern
                              AND obs_date >= CURRENT_DATE - :days
                            ORDER BY obs_date ASC
                            LIMIT 20
                        """),
                        {"pattern": f"%{ticker}%CLOSE%", "days": days},
                    ).fetchall()
        except Exception as exc:
            log.debug(
                "Price lookup failed for {t}: {e}", t=ticker, e=str(exc),
            )
            return None, None

        if len(rows) < 2:
            return None, None

        first_val = float(rows[0][1])
        last_val = float(rows[-1][1])

        if first_val == 0:
            return None, None

        pct_change = (last_val - first_val) / first_val * 100

        if pct_change > 0.5:
            return "up", round(pct_change, 2)
        elif pct_change < -0.5:
            return "down", round(pct_change, 2)
        else:
            return "flat", round(pct_change, 2)

    def detect_divergence(
        self,
        ticker: str,
        sentiment_velocity: float,
        sentiment_direction: str,
    ) -> tuple[bool, str | None, float | None]:
        """Check if price and sentiment are diverging.

        A divergence means:
        - Price going up + sentiment getting more bearish
        - Price going down + sentiment getting more bullish

        These are often precursors to reversals.

        Args:
            ticker: Ticker symbol.
            sentiment_velocity: Current sentiment velocity.
            sentiment_direction: Current sentiment direction.

        Returns:
            Tuple of (is_divergent, price_direction, price_pct).
        """
        price_dir, price_pct = self.get_price_direction(ticker)

        if price_dir is None:
            return False, None, None

        # Check for divergence
        is_divergent = False
        if price_dir == "up" and sentiment_velocity < -DIVERGENCE_THRESHOLD:
            is_divergent = True  # Price up, sentiment deteriorating
        elif price_dir == "down" and sentiment_velocity > DIVERGENCE_THRESHOLD:
            is_divergent = True  # Price down, sentiment improving

        return is_divergent, price_dir, price_pct


# ── Main Engine ──────────────────────────────────────────────────────────

class NewsMomentumEngine:
    """Orchestrates sentiment momentum analysis and signal generation.

    Combines sentiment time series, momentum calculation, and divergence
    detection to produce actionable signals for the trading pipeline.

    Attributes:
        engine: SQLAlchemy engine.
        ts: SentimentTimeSeries instance.
        momentum: MomentumCalculator instance.
        divergence: DivergenceDetector instance.
    """

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self.ts = SentimentTimeSeries(engine)
        self.momentum = MomentumCalculator()
        self.divergence = DivergenceDetector(engine)
        self._ensure_table()

    def _ensure_table(self) -> None:
        """Create news_momentum table if it doesn't exist."""
        try:
            with self.engine.begin() as conn:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS news_momentum (
                        id SERIAL PRIMARY KEY,
                        signal_id TEXT UNIQUE NOT NULL,
                        ticker TEXT NOT NULL,
                        signal_type TEXT NOT NULL,
                        direction TEXT NOT NULL,
                        magnitude DOUBLE PRECISION DEFAULT 0.0,
                        sentiment_velocity DOUBLE PRECISION DEFAULT 0.0,
                        sentiment_acceleration DOUBLE PRECISION DEFAULT 0.0,
                        price_direction TEXT,
                        price_pct DOUBLE PRECISION,
                        confidence DOUBLE PRECISION DEFAULT 0.5,
                        short_score DOUBLE PRECISION DEFAULT 0.0,
                        medium_score DOUBLE PRECISION DEFAULT 0.0,
                        long_score DOUBLE PRECISION DEFAULT 0.0,
                        article_count INTEGER DEFAULT 0,
                        evidence JSONB DEFAULT '[]',
                        computed_at TIMESTAMPTZ DEFAULT NOW()
                    )
                """))
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_news_momentum_ticker
                    ON news_momentum (ticker)
                """))
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_news_momentum_type
                    ON news_momentum (signal_type)
                """))
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_news_momentum_computed
                    ON news_momentum (computed_at DESC)
                """))
        except Exception as exc:
            log.warning("Failed to create news_momentum table: {e}", e=str(exc))

    def analyze_ticker(self, ticker: str) -> MomentumSignal | None:
        """Run full momentum analysis for a single ticker.

        Args:
            ticker: Ticker symbol.

        Returns:
            MomentumSignal or None if insufficient data.
        """
        now = datetime.now(timezone.utc)

        # Get sentiment snapshots for three windows
        short = self.ts.get_snapshot(ticker, SHORT_WINDOW_HOURS, now)
        medium = self.ts.get_snapshot(ticker, MEDIUM_WINDOW_HOURS, now)
        long = self.ts.get_snapshot(ticker, LONG_WINDOW_HOURS, now)

        if not short or not long:
            return None

        # If medium is missing, interpolate
        if not medium:
            medium = short  # fallback: treat medium as short

        # Check minimum article counts
        if short.article_count < MIN_ARTICLES_SHORT:
            return None
        if long.article_count < MIN_ARTICLES_LONG:
            return None

        # Compute momentum
        velocity = self.momentum.compute_velocity(short, long)
        acceleration = self.momentum.compute_acceleration(short, medium, long)
        signal_type, direction = self.momentum.classify_momentum(
            velocity, acceleration, short.weighted_score,
        )

        # Check for divergence
        is_divergent, price_dir, price_pct = self.divergence.detect_divergence(
            ticker, velocity, direction,
        )

        if is_divergent:
            signal_type = "DIVERGENCE"

        # Compute magnitude (how noteworthy is this signal?)
        magnitude = min(1.0, abs(velocity) + abs(acceleration) * 0.5)
        if is_divergent:
            magnitude = min(1.0, magnitude + 0.2)

        # Compute confidence from article counts and consistency
        count_confidence = min(1.0, (short.article_count + long.article_count) / 30)
        consistency = 1.0 - abs(short.avg_confidence - long.avg_confidence)
        confidence = round(count_confidence * 0.6 + consistency * 0.4, 3)

        # Build evidence strings
        evidence = []
        evidence.append(
            f"24h: {short.bullish_count}B/{short.bearish_count}b/{short.neutral_count}N "
            f"({short.article_count} articles, score={short.sentiment_score:+.2f})"
        )
        evidence.append(
            f"7d: {long.bullish_count}B/{long.bearish_count}b/{long.neutral_count}N "
            f"({long.article_count} articles, score={long.sentiment_score:+.2f})"
        )
        evidence.append(f"Velocity={velocity:+.3f}, Acceleration={acceleration:+.3f}")
        if price_dir:
            evidence.append(f"Price: {price_dir} ({price_pct:+.1f}%)")
        if is_divergent:
            evidence.append("DIVERGENCE: sentiment and price moving opposite directions")

        signal_id = hashlib.sha256(
            f"momentum:{ticker}:{now.date()}:{signal_type}".encode()
        ).hexdigest()[:20]

        return MomentumSignal(
            signal_id=signal_id,
            ticker=ticker.upper(),
            signal_type=signal_type,
            direction=direction,
            magnitude=round(magnitude, 3),
            sentiment_velocity=velocity,
            sentiment_acceleration=acceleration,
            price_direction=price_dir,
            price_pct=price_pct,
            confidence=confidence,
            short_score=short.weighted_score,
            medium_score=medium.weighted_score,
            long_score=long.weighted_score,
            article_count=short.article_count + long.article_count,
            evidence=evidence,
            computed_at=now,
        )

    def analyze_market(self) -> MomentumSignal | None:
        """Run momentum analysis for the overall market.

        Returns:
            MomentumSignal for "MARKET" or None.
        """
        now = datetime.now(timezone.utc)

        short = self.ts.get_market_snapshot(SHORT_WINDOW_HOURS, now)
        medium = self.ts.get_market_snapshot(MEDIUM_WINDOW_HOURS, now)
        long = self.ts.get_market_snapshot(LONG_WINDOW_HOURS, now)

        if not short or not long:
            return None
        if not medium:
            medium = short

        if short.article_count < MIN_ARTICLES_SHORT:
            return None

        velocity = self.momentum.compute_velocity(short, long)
        acceleration = self.momentum.compute_acceleration(short, medium, long)
        signal_type, direction = self.momentum.classify_momentum(
            velocity, acceleration, short.weighted_score,
        )

        # Market divergence check against SPY
        is_divergent, price_dir, price_pct = self.divergence.detect_divergence(
            "SPY", velocity, direction,
        )
        if is_divergent:
            signal_type = "DIVERGENCE"

        magnitude = min(1.0, abs(velocity) + abs(acceleration) * 0.5)
        if is_divergent:
            magnitude = min(1.0, magnitude + 0.2)

        count_confidence = min(1.0, short.article_count / 50)
        confidence = round(count_confidence * 0.7 + 0.3, 3)

        evidence = [
            f"Market 24h: {short.bullish_count}B/{short.bearish_count}b "
            f"({short.article_count} articles, score={short.sentiment_score:+.2f})",
            f"Market 7d: {long.bullish_count}B/{long.bearish_count}b "
            f"({long.article_count} articles, score={long.sentiment_score:+.2f})",
            f"Velocity={velocity:+.3f}, Acceleration={acceleration:+.3f}",
        ]
        if price_dir:
            evidence.append(f"SPY: {price_dir} ({price_pct:+.1f}%)")

        signal_id = hashlib.sha256(
            f"momentum:MARKET:{now.date()}:{signal_type}".encode()
        ).hexdigest()[:20]

        return MomentumSignal(
            signal_id=signal_id,
            ticker="MARKET",
            signal_type=signal_type,
            direction=direction,
            magnitude=round(magnitude, 3),
            sentiment_velocity=velocity,
            sentiment_acceleration=acceleration,
            price_direction=price_dir,
            price_pct=price_pct,
            confidence=confidence,
            short_score=short.weighted_score,
            medium_score=medium.weighted_score,
            long_score=long.weighted_score,
            article_count=short.article_count + long.article_count,
            evidence=evidence,
            computed_at=now,
        )

    def _store_signal(self, signal: MomentumSignal) -> bool:
        """Persist a momentum signal to the database.

        Args:
            signal: The MomentumSignal to store.

        Returns:
            True if stored successfully.
        """
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO news_momentum
                        (signal_id, ticker, signal_type, direction, magnitude,
                         sentiment_velocity, sentiment_acceleration,
                         price_direction, price_pct, confidence,
                         short_score, medium_score, long_score,
                         article_count, evidence, computed_at)
                        VALUES
                        (:sid, :ticker, :stype, :dir, :mag,
                         :vel, :acc, :pdir, :ppct, :conf,
                         :ss, :ms, :ls, :ac, :ev, :at)
                        ON CONFLICT (signal_id) DO UPDATE SET
                            magnitude = EXCLUDED.magnitude,
                            sentiment_velocity = EXCLUDED.sentiment_velocity,
                            sentiment_acceleration = EXCLUDED.sentiment_acceleration,
                            confidence = EXCLUDED.confidence,
                            computed_at = EXCLUDED.computed_at
                    """),
                    {
                        "sid": signal.signal_id,
                        "ticker": signal.ticker,
                        "stype": signal.signal_type,
                        "dir": signal.direction,
                        "mag": signal.magnitude,
                        "vel": signal.sentiment_velocity,
                        "acc": signal.sentiment_acceleration,
                        "pdir": signal.price_direction,
                        "ppct": signal.price_pct,
                        "conf": signal.confidence,
                        "ss": signal.short_score,
                        "ms": signal.medium_score,
                        "ls": signal.long_score,
                        "ac": signal.article_count,
                        "ev": json.dumps(signal.evidence),
                        "at": signal.computed_at,
                    },
                )
            return True
        except Exception as exc:
            log.warning(
                "Failed to store momentum signal for {t}: {e}",
                t=signal.ticker, e=str(exc),
            )
            return False

    def run_full_scan(
        self,
        tickers: list[str] | None = None,
    ) -> dict[str, Any]:
        """Run momentum analysis for all watched tickers + market.

        Args:
            tickers: Optional list of tickers to scan. If None, discovers
                     active tickers from recent news articles.

        Returns:
            Summary dict with per-ticker results.
        """
        if tickers is None:
            tickers = self._discover_active_tickers()

        results: dict[str, Any] = {
            "computed_at": datetime.now(timezone.utc).isoformat(),
            "tickers_scanned": 0,
            "signals_generated": 0,
            "divergences": 0,
            "accelerating": 0,
            "decelerating": 0,
            "signals": [],
        }

        # Market-level momentum
        market_signal = self.analyze_market()
        if market_signal:
            self._store_signal(market_signal)
            results["market"] = market_signal.to_dict()
            results["signals_generated"] += 1

        # Per-ticker momentum
        for ticker in tickers:
            signal = self.analyze_ticker(ticker)
            if signal is None:
                continue

            self._store_signal(signal)
            results["tickers_scanned"] += 1
            results["signals_generated"] += 1

            if signal.signal_type == "DIVERGENCE":
                results["divergences"] += 1
            elif signal.signal_type == "ACCELERATING":
                results["accelerating"] += 1
            elif signal.signal_type == "DECELERATING":
                results["decelerating"] += 1

            results["signals"].append(signal.to_dict())

        log.info(
            "News momentum scan: {n} tickers, {s} signals, {d} divergences",
            n=results["tickers_scanned"],
            s=results["signals_generated"],
            d=results["divergences"],
        )

        return results

    def _discover_active_tickers(self, min_articles: int = 5) -> list[str]:
        """Find tickers with enough recent news for momentum analysis.

        Args:
            min_articles: Minimum article count in the last 7 days.

        Returns:
            List of ticker symbols.
        """
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text("""
                        SELECT ticker, COUNT(*) AS cnt
                        FROM (
                            SELECT unnest(tickers) AS ticker
                            FROM news_articles
                            WHERE published_at >= NOW() - INTERVAL '30 days'
                              AND tickers IS NOT NULL
                              AND array_length(tickers, 1) > 0
                        ) t
                        WHERE LENGTH(ticker) BETWEEN 1 AND 5
                          AND ticker ~ '^[A-Z]'
                          AND ticker NOT IN ('US','UK','EU','UN','AI','CEO','IPO','SEC','FDA','GDP')
                        GROUP BY ticker
                        HAVING COUNT(*) >= :min_count
                        ORDER BY cnt DESC
                        LIMIT 100
                    """),
                    {"min_count": min_articles},
                ).fetchall()
            return [r[0] for r in rows]
        except Exception as exc:
            log.warning("Active ticker discovery failed: {e}", e=str(exc))
            return []

    def get_recent_signals(
        self,
        ticker: str | None = None,
        hours: int = 24,
        signal_type: str | None = None,
    ) -> list[dict[str, Any]]:
        """Retrieve recent momentum signals from the database.

        Args:
            ticker: Optional ticker filter.
            hours: Hours to look back.
            signal_type: Optional filter (ACCELERATING, DIVERGENCE, etc.).

        Returns:
            List of signal dicts.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

        conditions = ["computed_at >= :cutoff"]
        params: dict[str, Any] = {"cutoff": cutoff}

        if ticker:
            conditions.append("ticker = :ticker")
            params["ticker"] = ticker.upper()
        if signal_type:
            conditions.append("signal_type = :stype")
            params["stype"] = signal_type.upper()

        where_clause = " AND ".join(conditions)

        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text(f"""
                        SELECT signal_id, ticker, signal_type, direction,
                               magnitude, sentiment_velocity, sentiment_acceleration,
                               price_direction, price_pct, confidence,
                               short_score, medium_score, long_score,
                               article_count, evidence, computed_at
                        FROM news_momentum
                        WHERE {where_clause}
                        ORDER BY magnitude DESC, computed_at DESC
                        LIMIT 100
                    """),
                    params,
                ).fetchall()
        except Exception as exc:
            log.warning("Recent signals query failed: {e}", e=str(exc))
            return []

        return [
            {
                "signal_id": r[0],
                "ticker": r[1],
                "signal_type": r[2],
                "direction": r[3],
                "magnitude": r[4],
                "sentiment_velocity": r[5],
                "sentiment_acceleration": r[6],
                "price_direction": r[7],
                "price_pct": r[8],
                "confidence": r[9],
                "short_score": r[10],
                "medium_score": r[11],
                "long_score": r[12],
                "article_count": r[13],
                "evidence": r[14],
                "computed_at": r[15].isoformat() if r[15] else None,
            }
            for r in rows
        ]

    def get_divergences(self, hours: int = 48) -> list[dict[str, Any]]:
        """Get all recent divergence signals — these are the highest-value alerts.

        Args:
            hours: Hours to look back.

        Returns:
            List of divergence signal dicts, sorted by magnitude.
        """
        return self.get_recent_signals(signal_type="DIVERGENCE", hours=hours)
