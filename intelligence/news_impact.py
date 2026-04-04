"""
GRID News Impact Attribution Engine.

Deep forensic analysis of how news, events, and catalysts translate into
stock price movements. Classifies every catalyst by time horizon, quantifies
its estimated impact in bps/dollars, and tracks the gap between what's
"baked in" and what the market still expects.

Designed for Mag 7+ tickers with deep coverage, works for any watchlist ticker.

Architecture:
    1. CatalystClassifier  — categorize news/events by horizon & magnitude
    2. PriceDecomposer     — attribute each significant move to its catalysts
    3. ExpectationTracker  — running tally of "baked in" vs "still expected"
    4. DeepDive            — full forensic report combining all three

The engine works both historically (100% hindsight attribution) and
forward-looking (>50% foresight from pattern recognition).
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import statistics
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# ── Constants ────────────────────────────────────────────────────────────

# Mag 7 + key indices + sectors
MAG7_TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA"]
EXTENDED_UNIVERSE = MAG7_TICKERS + [
    "SPY", "QQQ", "LLY", "V", "MA", "UNH", "RTX", "PG", "TMO",
    "AVGO", "JPM", "BRK-B", "XOM", "JNJ", "WMT", "HD",
]

# Time horizon thresholds
SHORT_TERM_DAYS = 5          # < 1 week
MEDIUM_TERM_DAYS = 60        # 1 week to ~2 months
# > MEDIUM_TERM_DAYS = long term

# Catalyst categories
CATALYST_TYPES = {
    "earnings": {"default_horizon": "short", "typical_bps": 300},
    "guidance": {"default_horizon": "medium", "typical_bps": 200},
    "product_launch": {"default_horizon": "medium", "typical_bps": 150},
    "regulation": {"default_horizon": "long", "typical_bps": 100},
    "macro_data": {"default_horizon": "short", "typical_bps": 50},
    "analyst_rating": {"default_horizon": "short", "typical_bps": 80},
    "insider_trade": {"default_horizon": "medium", "typical_bps": 40},
    "gov_contract": {"default_horizon": "medium", "typical_bps": 120},
    "congressional_trade": {"default_horizon": "medium", "typical_bps": 60},
    "m_and_a": {"default_horizon": "long", "typical_bps": 500},
    "buyback": {"default_horizon": "medium", "typical_bps": 100},
    "dividend": {"default_horizon": "short", "typical_bps": 50},
    "legal": {"default_horizon": "long", "typical_bps": 200},
    "geopolitical": {"default_horizon": "medium", "typical_bps": 150},
    "sector_rotation": {"default_horizon": "medium", "typical_bps": 80},
    "options_flow": {"default_horizon": "short", "typical_bps": 60},
    "technical_breakout": {"default_horizon": "short", "typical_bps": 100},
    "unknown": {"default_horizon": "short", "typical_bps": 30},
}

# Keywords for catalyst classification
_EARNINGS_KW = {"earnings", "eps", "revenue", "beat", "miss", "quarter", "q1", "q2", "q3", "q4", "fiscal"}
_GUIDANCE_KW = {"guidance", "outlook", "forecast", "raised", "lowered", "reiterated", "full-year", "fy"}
_PRODUCT_KW = {"launch", "product", "release", "announce", "unveil", "introduce", "new model", "ai chip"}
_REGULATION_KW = {"regulation", "antitrust", "doj", "ftc", "sec", "compliance", "fine", "probe", "investigate"}
_MACRO_KW = {"fed", "fomc", "inflation", "cpi", "ppi", "jobs", "nonfarm", "gdp", "rates", "tariff", "trade war"}
_ANALYST_KW = {"upgrade", "downgrade", "price target", "rating", "overweight", "underweight", "buy", "sell"}
_MA_KW = {"acquire", "acquisition", "merger", "buyout", "takeover", "deal"}
_LEGAL_KW = {"lawsuit", "settlement", "ruling", "court", "patent", "litigation"}
_GEO_KW = {"china", "tariff", "sanction", "ban", "export", "geopolitical", "war", "conflict"}


# ── Data classes ─────────────────────────────────────────────────────────

@dataclass
class Catalyst:
    """A classified news event or catalyst affecting a stock."""
    id: str                       # SHA-256 hash of (ticker, date, title)
    ticker: str
    title: str
    source: str                   # news source, signal type, etc.
    event_date: date
    catalyst_type: str            # one of CATALYST_TYPES keys
    horizon: str                  # short, medium, long
    direction: str                # bullish, bearish, neutral
    estimated_bps: float          # estimated basis-point impact
    estimated_dollars: float      # estimated dollar impact (at current mktcap)
    confidence: float             # 0-1 confidence in classification
    is_new: bool                  # True = new catalyst, False = confirms existing expectation
    raw_data: dict = field(default_factory=dict)


@dataclass
class MoveAttribution:
    """Attribution of a significant price move to its catalysts."""
    ticker: str
    move_date: date
    move_pct: float               # daily % change
    move_direction: str           # up, down
    total_explained_bps: float    # sum of attributed catalyst bps
    unexplained_bps: float        # residual (alpha opportunity)
    catalysts: list[Catalyst] = field(default_factory=list)
    macro_contribution_bps: float = 0.0   # broad market component
    sector_contribution_bps: float = 0.0  # sector beta component
    narrative: str = ""
    confidence: float = 0.0


@dataclass
class Expectation:
    """A tracked market expectation for a ticker."""
    id: str
    ticker: str
    description: str              # "NVDA expected to beat Q4 earnings by 15%"
    catalyst_type: str
    horizon: str                  # short, medium, long
    expected_direction: str       # bullish, bearish
    expected_magnitude_bps: float # how much is this worth?
    baked_in_pct: float          # 0-100: how much is already in the price?
    deadline: date | None        # by when does this need to happen?
    status: str                   # active, fulfilled, expired, invalidated
    evidence: list[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    resolved_at: datetime | None = None


@dataclass
class DeepDiveReport:
    """Full forensic deep dive for a single ticker."""
    ticker: str
    name: str
    generated_at: datetime
    # Price attribution (last 90 days)
    significant_moves: list[MoveAttribution]
    total_moves_analyzed: int
    avg_explained_pct: float      # avg % of moves explained by catalysts
    # Expectations
    active_expectations: list[Expectation]
    total_baked_in_bps: float     # net bps already priced in
    total_pending_bps: float      # net bps the market still expects
    # Historical accuracy
    historical_hit_rate: float    # % of past attributions that were correct
    # Catalyst breakdown
    catalyst_breakdown: dict[str, int]  # type -> count
    top_catalysts: list[Catalyst]       # highest-impact catalysts
    # LLM narrative
    narrative: str = ""
    confidence: float = 0.0


# ── Catalyst Classifier ─────────────────────────────────────────────────

class CatalystClassifier:
    """Classifies news and events into typed, horizon-tagged catalysts."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def classify_news(self, ticker: str, title: str, summary: str,
                      sentiment: str, source: str, pub_date: date,
                      confidence: float = 0.5) -> Catalyst:
        """Classify a single news article into a Catalyst."""
        text_lower = f"{title} {summary}".lower()
        cat_type = self._detect_type(text_lower)
        horizon = self._detect_horizon(text_lower, cat_type)
        direction = self._sentiment_to_direction(sentiment)
        est_bps = self._estimate_bps(cat_type, confidence, text_lower)

        # Check if this confirms an existing expectation
        is_new = not self._confirms_existing(ticker, cat_type, direction)

        cat_id = hashlib.sha256(
            f"{ticker}:{pub_date}:{title[:80]}".encode()
        ).hexdigest()[:16]

        return Catalyst(
            id=cat_id,
            ticker=ticker,
            title=title,
            source=source,
            event_date=pub_date,
            catalyst_type=cat_type,
            horizon=horizon,
            direction=direction,
            estimated_bps=est_bps,
            estimated_dollars=0.0,  # filled later with market cap
            confidence=confidence,
            is_new=is_new,
        )

    def classify_signal(self, ticker: str, signal_type: str,
                        signal_value: Any, signal_date: date,
                        actor: str = "") -> Catalyst:
        """Classify a signal_sources record into a Catalyst."""
        cat_type = self._signal_type_to_catalyst(signal_type)
        horizon = CATALYST_TYPES.get(cat_type, {}).get("default_horizon", "short")
        direction = self._signal_direction(signal_type, signal_value)
        est_bps = CATALYST_TYPES.get(cat_type, {}).get("typical_bps", 30)

        title = f"{signal_type}: {actor or ticker}" if actor else f"{signal_type} on {ticker}"
        cat_id = hashlib.sha256(
            f"{ticker}:{signal_date}:{signal_type}:{actor}".encode()
        ).hexdigest()[:16]

        return Catalyst(
            id=cat_id,
            ticker=ticker,
            title=title,
            source=f"signal:{signal_type}",
            event_date=signal_date,
            catalyst_type=cat_type,
            horizon=horizon,
            direction=direction,
            estimated_bps=est_bps,
            estimated_dollars=0.0,
            confidence=0.6,
            is_new=True,
        )

    def _detect_type(self, text_lower: str) -> str:
        """Detect catalyst type from text keywords (word boundary matching)."""
        import re
        checks = [
            (_EARNINGS_KW, "earnings"),
            (_GUIDANCE_KW, "guidance"),
            (_MA_KW, "m_and_a"),
            (_REGULATION_KW, "regulation"),
            (_LEGAL_KW, "legal"),
            (_GEO_KW, "geopolitical"),
            (_MACRO_KW, "macro_data"),
            (_ANALYST_KW, "analyst_rating"),
            (_PRODUCT_KW, "product_launch"),
        ]
        for keywords, cat_type in checks:
            for kw in keywords:
                # Use word boundary for single words to avoid substring matches
                # (e.g. "production" should not match "product")
                if " " in kw:
                    if kw in text_lower:
                        return cat_type
                elif re.search(rf"\b{re.escape(kw)}\b", text_lower):
                    return cat_type
        return "unknown"

    def _detect_horizon(self, text_lower: str, cat_type: str) -> str:
        """Determine time horizon from content and type."""
        # Explicit horizon signals
        if any(w in text_lower for w in ("today", "this week", "immediate", "intraday")):
            return "short"
        if any(w in text_lower for w in ("this quarter", "next quarter", "pipeline")):
            return "medium"
        if any(w in text_lower for w in ("long-term", "multi-year", "decade", "structural")):
            return "long"
        return CATALYST_TYPES.get(cat_type, {}).get("default_horizon", "short")

    def _sentiment_to_direction(self, sentiment: str) -> str:
        """Convert news sentiment to direction."""
        s = (sentiment or "").upper()
        if s in ("BULLISH", "POSITIVE", "BUY"):
            return "bullish"
        if s in ("BEARISH", "NEGATIVE", "SELL"):
            return "bearish"
        return "neutral"

    def _estimate_bps(self, cat_type: str, confidence: float, text_lower: str) -> float:
        """Estimate basis-point impact from type and confidence."""
        base = CATALYST_TYPES.get(cat_type, {}).get("typical_bps", 30)
        # Scale by confidence
        scaled = base * max(confidence, 0.3)
        # Amplify for strong language
        if any(w in text_lower for w in ("massive", "surge", "plunge", "crash", "soar", "historic")):
            scaled *= 1.5
        if any(w in text_lower for w in ("slight", "modest", "minor", "small")):
            scaled *= 0.5
        return round(scaled, 1)

    def _signal_type_to_catalyst(self, signal_type: str) -> str:
        """Map signal_sources.signal_type to catalyst type."""
        mapping = {
            "BUY": "insider_trade",
            "SELL": "insider_trade",
            "CLUSTER_BUY": "insider_trade",
            "UNUSUAL_BUY": "options_flow",
            "UNUSUAL_SELL": "options_flow",
            "UNUSUAL_OPTIONS": "options_flow",
            "CONTRACT_AWARD": "gov_contract",
            "NEW_RULE": "regulation",
            "CONGRESSIONAL_BUY": "congressional_trade",
            "CONGRESSIONAL_SELL": "congressional_trade",
        }
        return mapping.get(signal_type, "unknown")

    def _signal_direction(self, signal_type: str, value: Any) -> str:
        """Determine direction from signal type."""
        bullish = {"BUY", "CLUSTER_BUY", "UNUSUAL_BUY", "CONTRACT_AWARD", "CONGRESSIONAL_BUY"}
        bearish = {"SELL", "UNUSUAL_SELL", "CONGRESSIONAL_SELL"}
        if signal_type in bullish:
            return "bullish"
        if signal_type in bearish:
            return "bearish"
        return "neutral"

    def _confirms_existing(self, ticker: str, cat_type: str, direction: str) -> bool:
        """Check if this catalyst confirms a recently seen expectation."""
        try:
            with self.engine.connect() as conn:
                row = conn.execute(
                    text(
                        "SELECT COUNT(*) FROM news_impact_catalysts "
                        "WHERE ticker = :t AND catalyst_type = :ct "
                        "AND direction = :d AND event_date > CURRENT_DATE - 14"
                    ),
                    {"t": ticker, "ct": cat_type, "d": direction},
                ).fetchone()
                return (row[0] or 0) > 0
        except Exception:
            return False


# ── Price Decomposer ─────────────────────────────────────────────────────

class PriceDecomposer:
    """Decomposes significant price moves into attributed catalysts."""

    def __init__(self, engine: Engine, classifier: CatalystClassifier) -> None:
        self.engine = engine
        self.classifier = classifier

    def decompose_move(self, ticker: str, move_date: date,
                       move_pct: float, lookback_days: int = 7) -> MoveAttribution:
        """Attribute a price move to its catalysts.

        Steps:
            1. Get market (SPY) and sector returns → macro/sector contribution
            2. Gather all news + signals in the lookback window
            3. Classify each as a catalyst
            4. Estimate each catalyst's bps contribution
            5. Compute unexplained residual
        """
        move_bps = abs(move_pct) * 100
        move_dir = "up" if move_pct > 0 else "down"

        # 1. Market and sector decomposition
        spy_ret = self._get_return(
            "SPY" if ticker != "SPY" else "QQQ", move_date
        )
        macro_bps = abs(spy_ret * 100) if spy_ret else 0.0

        # Sector beta (rough: tech stocks move ~1.2x SPY)
        sector_beta = 1.2 if ticker in MAG7_TICKERS else 1.0
        sector_bps = macro_bps * (sector_beta - 1.0)

        # 2. Gather catalysts
        catalysts = self._gather_catalysts(ticker, move_date, lookback_days)

        # 3. Score attribution — catalysts that align with move direction
        # get a bigger attribution slice
        aligned = []
        opposed = []
        for c in catalysts:
            if (c.direction == "bullish" and move_dir == "up") or \
               (c.direction == "bearish" and move_dir == "down"):
                aligned.append(c)
            elif c.direction != "neutral":
                opposed.append(c)

        # 4. Distribute remaining bps across aligned catalysts
        remaining_bps = max(0, move_bps - macro_bps - sector_bps)
        total_catalyst_weight = sum(c.estimated_bps * c.confidence for c in aligned) or 1
        for c in aligned:
            weight = (c.estimated_bps * c.confidence) / total_catalyst_weight
            c.estimated_bps = round(remaining_bps * weight, 1)

        total_explained = macro_bps + sector_bps + sum(c.estimated_bps for c in aligned)
        unexplained = max(0, move_bps - total_explained)

        return MoveAttribution(
            ticker=ticker,
            move_date=move_date,
            move_pct=move_pct,
            move_direction=move_dir,
            total_explained_bps=round(total_explained, 1),
            unexplained_bps=round(unexplained, 1),
            catalysts=aligned + opposed,
            macro_contribution_bps=round(macro_bps, 1),
            sector_contribution_bps=round(sector_bps, 1),
            confidence=min(0.95, total_explained / max(move_bps, 1)),
        )

    def decompose_history(self, ticker: str, days: int = 90,
                          threshold: float = 0.02) -> list[MoveAttribution]:
        """Decompose all significant moves in the last N days."""
        moves = self._find_significant_moves(ticker, days, threshold)
        results = []
        for move_date, move_pct in moves:
            try:
                attr = self.decompose_move(ticker, move_date, move_pct)
                results.append(attr)
            except Exception as exc:
                log.warning(
                    "Failed to decompose {t} {d}: {e}",
                    t=ticker, d=move_date, e=str(exc),
                )
        return results

    def _gather_catalysts(self, ticker: str, move_date: date,
                          lookback_days: int) -> list[Catalyst]:
        """Gather all news + signals for a ticker around a move."""
        catalysts = []
        start = move_date - timedelta(days=lookback_days)

        # News articles
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT title, source, summary, sentiment, confidence, "
                        "published_at::date "
                        "FROM news_articles "
                        "WHERE :ticker = ANY(tickers) "
                        "AND published_at::date BETWEEN :start AND :end "
                        "ORDER BY published_at DESC"
                    ),
                    {"ticker": ticker, "start": start, "end": move_date},
                ).fetchall()
                for r in rows:
                    cat = self.classifier.classify_news(
                        ticker=ticker,
                        title=r[0] or "",
                        summary=r[2] or "",
                        sentiment=r[3] or "NEUTRAL",
                        source=r[1] or "unknown",
                        pub_date=r[5],
                        confidence=r[4] or 0.5,
                    )
                    catalysts.append(cat)
        except Exception as exc:
            log.debug("News gather failed for {t}: {e}", t=ticker, e=str(exc))

        # Signal sources (insider, congressional, options, contracts)
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT signal_type, signal_value, ticker, actor, "
                        "reported_date "
                        "FROM signal_sources "
                        "WHERE ticker = :ticker "
                        "AND reported_date BETWEEN :start AND :end "
                        "ORDER BY reported_date DESC"
                    ),
                    {"ticker": ticker, "start": start, "end": move_date},
                ).fetchall()
                for r in rows:
                    cat = self.classifier.classify_signal(
                        ticker=ticker,
                        signal_type=r[0] or "",
                        signal_value=r[1],
                        signal_date=r[4],
                        actor=r[3] or "",
                    )
                    catalysts.append(cat)
        except Exception as exc:
            log.debug("Signal gather failed for {t}: {e}", t=ticker, e=str(exc))

        return catalysts

    def _find_significant_moves(self, ticker: str, days: int,
                                threshold: float) -> list[tuple[date, float]]:
        """Find daily moves exceeding threshold %."""
        try:
            with self.engine.connect() as conn:
                # Try options_daily_signals first
                rows = conn.execute(
                    text(
                        "SELECT obs_date, close_price FROM options_daily_signals "
                        "WHERE ticker = :t AND obs_date > CURRENT_DATE - :d "
                        "ORDER BY obs_date ASC"
                    ),
                    {"t": ticker, "d": days},
                ).fetchall()
                if not rows:
                    # Fall back to raw_series
                    rows = conn.execute(
                        text(
                            "SELECT obs_date, value FROM raw_series "
                            "WHERE series_id LIKE :pat "
                            "AND obs_date > CURRENT_DATE - :d "
                            "ORDER BY obs_date ASC"
                        ),
                        {"pat": f"YFINANCE:{ticker}_close", "d": days},
                    ).fetchall()

            if len(rows) < 2:
                return []

            moves = []
            for i in range(1, len(rows)):
                prev_price = float(rows[i - 1][1])
                curr_price = float(rows[i][1])
                if prev_price == 0:
                    continue
                pct = (curr_price - prev_price) / prev_price
                if abs(pct) >= threshold:
                    moves.append((rows[i][0], round(pct, 5)))
            return moves
        except Exception as exc:
            log.warning("Significant moves scan failed for {t}: {e}", t=ticker, e=str(exc))
            return []

    def _get_return(self, ticker: str, on_date: date) -> float | None:
        """Get single-day return for a ticker."""
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT obs_date, close_price FROM options_daily_signals "
                        "WHERE ticker = :t AND obs_date BETWEEN :s AND :d "
                        "ORDER BY obs_date ASC"
                    ),
                    {"t": ticker, "s": on_date - timedelta(days=5), "d": on_date},
                ).fetchall()
                if len(rows) < 2:
                    return None
                prev = float(rows[-2][1])
                curr = float(rows[-1][1])
                return (curr - prev) / prev if prev else None
        except Exception:
            return None


# ── Expectation Tracker ──────────────────────────────────────────────────

class ExpectationTracker:
    """Tracks what's baked into each stock and what the market still expects."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def get_active_expectations(self, ticker: str) -> list[Expectation]:
        """Load active expectations for a ticker from DB."""
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT id, ticker, description, catalyst_type, horizon, "
                        "expected_direction, expected_magnitude_bps, baked_in_pct, "
                        "deadline, status, evidence, created_at "
                        "FROM news_impact_expectations "
                        "WHERE ticker = :t AND status = 'active' "
                        "ORDER BY expected_magnitude_bps DESC"
                    ),
                    {"t": ticker},
                ).fetchall()
                return [
                    Expectation(
                        id=r[0], ticker=r[1], description=r[2],
                        catalyst_type=r[3], horizon=r[4],
                        expected_direction=r[5],
                        expected_magnitude_bps=float(r[6]),
                        baked_in_pct=float(r[7]),
                        deadline=r[8], status=r[9],
                        evidence=r[10] if isinstance(r[10], list) else [],
                        created_at=r[11],
                    )
                    for r in rows
                ]
        except Exception:
            return []

    def create_expectation(self, expectation: Expectation) -> str:
        """Store a new market expectation."""
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        "INSERT INTO news_impact_expectations "
                        "(id, ticker, description, catalyst_type, horizon, "
                        "expected_direction, expected_magnitude_bps, baked_in_pct, "
                        "deadline, status, evidence, created_at) "
                        "VALUES (:id, :t, :desc, :ct, :h, :dir, :mag, :baked, "
                        ":dl, :st, :ev, :ca) "
                        "ON CONFLICT (id) DO UPDATE SET "
                        "baked_in_pct = EXCLUDED.baked_in_pct, "
                        "status = EXCLUDED.status"
                    ),
                    {
                        "id": expectation.id,
                        "t": expectation.ticker,
                        "desc": expectation.description,
                        "ct": expectation.catalyst_type,
                        "h": expectation.horizon,
                        "dir": expectation.expected_direction,
                        "mag": expectation.expected_magnitude_bps,
                        "baked": expectation.baked_in_pct,
                        "dl": expectation.deadline,
                        "st": expectation.status,
                        "ev": json.dumps(expectation.evidence),
                        "ca": expectation.created_at.isoformat(),
                    },
                )
            return expectation.id
        except Exception as exc:
            log.warning("Failed to store expectation: {e}", e=str(exc))
            return ""

    def update_baked_in(self, expectation_id: str, new_pct: float) -> None:
        """Update how much of an expectation is baked into the price."""
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        "UPDATE news_impact_expectations "
                        "SET baked_in_pct = :pct "
                        "WHERE id = :id"
                    ),
                    {"pct": min(100, max(0, new_pct)), "id": expectation_id},
                )
        except Exception as exc:
            log.debug("Failed to update baked_in: {e}", e=str(exc))

    def resolve_expectation(self, expectation_id: str, status: str) -> None:
        """Mark expectation as fulfilled, expired, or invalidated."""
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        "UPDATE news_impact_expectations "
                        "SET status = :st, resolved_at = NOW() "
                        "WHERE id = :id"
                    ),
                    {"st": status, "id": expectation_id},
                )
        except Exception as exc:
            log.debug("Failed to resolve expectation: {e}", e=str(exc))

    def compute_net_expectations(self, ticker: str) -> dict[str, float]:
        """Compute net baked-in and pending expectations for a ticker."""
        expectations = self.get_active_expectations(ticker)
        total_baked = 0.0
        total_pending = 0.0
        by_horizon = {"short": 0.0, "medium": 0.0, "long": 0.0}

        for exp in expectations:
            sign = 1.0 if exp.expected_direction == "bullish" else -1.0
            baked = exp.expected_magnitude_bps * (exp.baked_in_pct / 100) * sign
            pending = exp.expected_magnitude_bps * ((100 - exp.baked_in_pct) / 100) * sign
            total_baked += baked
            total_pending += pending
            by_horizon[exp.horizon] = by_horizon.get(exp.horizon, 0) + pending

        return {
            "total_baked_in_bps": round(total_baked, 1),
            "total_pending_bps": round(total_pending, 1),
            "pending_by_horizon": {k: round(v, 1) for k, v in by_horizon.items()},
            "active_count": len(expectations),
        }


# ── Deep Dive Engine ─────────────────────────────────────────────────────

class DeepDiveEngine:
    """Generates full forensic deep dive reports for a ticker."""

    # Map ticker to company name
    TICKER_NAMES = {
        "AAPL": "Apple", "MSFT": "Microsoft", "GOOGL": "Alphabet",
        "AMZN": "Amazon", "NVDA": "NVIDIA", "META": "Meta Platforms",
        "TSLA": "Tesla", "SPY": "S&P 500 ETF", "QQQ": "NASDAQ 100 ETF",
        "LLY": "Eli Lilly", "V": "Visa", "MA": "Mastercard",
        "UNH": "UnitedHealth", "RTX": "RTX Corp", "PG": "Procter & Gamble",
        "TMO": "Thermo Fisher", "AVGO": "Broadcom", "JPM": "JPMorgan",
    }

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self.classifier = CatalystClassifier(engine)
        self.decomposer = PriceDecomposer(engine, self.classifier)
        self.tracker = ExpectationTracker(engine)

    def generate_deep_dive(self, ticker: str, days: int = 90) -> DeepDiveReport:
        """Full forensic analysis of a ticker.

        Steps:
            1. Find and decompose all significant moves
            2. Load and compute expectations
            3. Build catalyst breakdown
            4. Generate LLM narrative
            5. Store results
        """
        log.info("Deep dive: {t} ({d} days)", t=ticker, d=days)

        # 1. Decompose history
        attributions = self.decomposer.decompose_history(ticker, days)

        # 2. Expectations
        active_exp = self.tracker.get_active_expectations(ticker)
        net_exp = self.tracker.compute_net_expectations(ticker)

        # 3. Catalyst breakdown
        all_catalysts = []
        for attr in attributions:
            all_catalysts.extend(attr.catalysts)

        cat_breakdown: dict[str, int] = {}
        for c in all_catalysts:
            cat_breakdown[c.catalyst_type] = cat_breakdown.get(c.catalyst_type, 0) + 1

        # Sort by estimated impact
        top_catalysts = sorted(
            all_catalysts, key=lambda c: abs(c.estimated_bps), reverse=True
        )[:10]

        # Avg explanation rate
        explained_rates = [
            a.total_explained_bps / max(abs(a.move_pct) * 100, 1)
            for a in attributions
        ]
        avg_explained = (
            statistics.mean(explained_rates) if explained_rates else 0.0
        )

        report = DeepDiveReport(
            ticker=ticker,
            name=self.TICKER_NAMES.get(ticker, ticker),
            generated_at=datetime.now(timezone.utc),
            significant_moves=attributions,
            total_moves_analyzed=len(attributions),
            avg_explained_pct=round(min(avg_explained, 1.0) * 100, 1),
            active_expectations=active_exp,
            total_baked_in_bps=net_exp["total_baked_in_bps"],
            total_pending_bps=net_exp["total_pending_bps"],
            historical_hit_rate=0.0,  # computed in backtest
            catalyst_breakdown=cat_breakdown,
            top_catalysts=top_catalysts,
        )

        # 4. LLM narrative
        report.narrative = self._generate_narrative(report)
        report.confidence = min(0.9, avg_explained * 0.8 + 0.2)

        # 5. Store
        self._store_report(report)

        log.info(
            "Deep dive complete: {t} — {n} moves, {c}% explained, "
            "{e} active expectations",
            t=ticker, n=len(attributions),
            c=report.avg_explained_pct, e=len(active_exp),
        )
        return report

    def generate_all_mag7(self, days: int = 90) -> list[DeepDiveReport]:
        """Run deep dives for all Mag 7 tickers."""
        reports = []
        for ticker in MAG7_TICKERS:
            try:
                report = self.generate_deep_dive(ticker, days)
                reports.append(report)
            except Exception as exc:
                log.error("Deep dive failed for {t}: {e}", t=ticker, e=str(exc))
        return reports

    def _generate_narrative(self, report: DeepDiveReport) -> str:
        """Generate LLM narrative for the deep dive."""
        try:
            from llm.router import get_llm as _get_llm_router
            _llm_client = _get_llm_router()
            generate = _llm_client.generate

            # Build context
            move_summary = ""
            for attr in report.significant_moves[:5]:
                cats = ", ".join(c.title[:50] for c in attr.catalysts[:3])
                move_summary += (
                    f"  {attr.move_date}: {attr.move_pct:+.2%} "
                    f"({attr.total_explained_bps:.0f}bps explained, "
                    f"{attr.unexplained_bps:.0f}bps unexplained) "
                    f"— {cats}\n"
                )

            exp_summary = ""
            for exp in report.active_expectations[:5]:
                exp_summary += (
                    f"  {exp.description} ({exp.baked_in_pct:.0f}% baked in, "
                    f"deadline: {exp.deadline or 'none'})\n"
                )

            prompt = f"""Analyze {report.ticker} ({report.name}) forensic deep dive.

SIGNIFICANT MOVES (last 90 days):
{move_summary or '  No significant moves found'}

ACTIVE EXPECTATIONS:
{exp_summary or '  No tracked expectations'}

CATALYST BREAKDOWN: {json.dumps(report.catalyst_breakdown)}
AVG EXPLAINED: {report.avg_explained_pct:.0f}%
NET PENDING BPS: {report.total_pending_bps:.0f}

Write 3-5 sentences: What's driving this stock? What's already priced in?
What's the market still expecting and by when? Where is the alpha opportunity
(unexplained moves or mispriced expectations)?

Be specific with numbers. Use short/medium/long-term framing."""

            result = generate(prompt, num_predict=400, temperature=0.3)
            if result:
                return result.strip()
        except Exception as exc:
            log.debug("LLM narrative failed: {e}", e=str(exc))

        # Fallback: rule-based
        top_cat = max(report.catalyst_breakdown, key=report.catalyst_breakdown.get) \
            if report.catalyst_breakdown else "unknown"
        return (
            f"{report.ticker} had {report.total_moves_analyzed} significant moves "
            f"with {report.avg_explained_pct:.0f}% average attribution. "
            f"Primary driver: {top_cat}. "
            f"Net pending expectations: {report.total_pending_bps:+.0f} bps."
        )

    def _store_report(self, report: DeepDiveReport) -> None:
        """Store deep dive report to DB."""
        try:
            payload = {
                "ticker": report.ticker,
                "name": report.name,
                "total_moves_analyzed": report.total_moves_analyzed,
                "avg_explained_pct": report.avg_explained_pct,
                "total_baked_in_bps": report.total_baked_in_bps,
                "total_pending_bps": report.total_pending_bps,
                "historical_hit_rate": report.historical_hit_rate,
                "catalyst_breakdown": report.catalyst_breakdown,
                "top_catalysts": [
                    {"title": c.title, "type": c.catalyst_type,
                     "bps": c.estimated_bps, "direction": c.direction}
                    for c in report.top_catalysts
                ],
                "active_expectations": [
                    {"desc": e.description, "baked_in": e.baked_in_pct,
                     "magnitude": e.expected_magnitude_bps,
                     "deadline": e.deadline.isoformat() if e.deadline else None}
                    for e in report.active_expectations
                ],
                "moves": [
                    {"date": str(a.move_date), "pct": a.move_pct,
                     "explained_bps": a.total_explained_bps,
                     "unexplained_bps": a.unexplained_bps,
                     "macro_bps": a.macro_contribution_bps}
                    for a in report.significant_moves
                ],
                "narrative": report.narrative,
                "confidence": report.confidence,
            }

            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        "INSERT INTO news_impact_reports "
                        "(ticker, payload, narrative, confidence, created_at) "
                        "VALUES (:t, :p, :n, :c, NOW()) "
                        "ON CONFLICT (ticker) DO UPDATE SET "
                        "payload = EXCLUDED.payload, "
                        "narrative = EXCLUDED.narrative, "
                        "confidence = EXCLUDED.confidence, "
                        "created_at = NOW()"
                    ),
                    {
                        "t": report.ticker,
                        "p": json.dumps(payload, default=str),
                        "n": report.narrative,
                        "c": report.confidence,
                    },
                )
        except Exception as exc:
            log.warning("Failed to store deep dive: {e}", e=str(exc))


# ── LLM Task Queue Integration ──────────────────────────────────────────

def generate_deep_dive_tasks() -> list[dict]:
    """Generate LLM task queue entries for Mag 7 deep dives.

    Called by llm_taskqueue._maybe_refill_background() to keep
    Qwen continuously analyzing the top stocks.
    """
    tasks = []
    for ticker in MAG7_TICKERS:
        tasks.append({
            "task_type": "deep_dive_forensic",
            "prompt": f"Run full forensic deep dive for {ticker}",
            "context": {"ticker": ticker, "days": 90},
            "priority": 3,
        })
    return tasks


def run_deep_dive_task(engine: Engine, context: dict) -> str:
    """Execute a deep dive task from the LLM queue."""
    ticker = context.get("ticker", "AAPL")
    days = context.get("days", 90)
    dive = DeepDiveEngine(engine)
    report = dive.generate_deep_dive(ticker, days)
    return json.dumps({
        "ticker": report.ticker,
        "moves_analyzed": report.total_moves_analyzed,
        "avg_explained_pct": report.avg_explained_pct,
        "pending_bps": report.total_pending_bps,
        "narrative": report.narrative[:500],
    }, default=str)


# ── Schema creation ──────────────────────────────────────────────────────

def ensure_tables(engine: Engine) -> None:
    """Create news impact tables if they don't exist."""
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS news_impact_catalysts (
                id          TEXT PRIMARY KEY,
                ticker      TEXT NOT NULL,
                title       TEXT NOT NULL,
                source      TEXT,
                event_date  DATE NOT NULL,
                catalyst_type TEXT NOT NULL,
                horizon     TEXT NOT NULL CHECK (horizon IN ('short', 'medium', 'long')),
                direction   TEXT NOT NULL CHECK (direction IN ('bullish', 'bearish', 'neutral')),
                estimated_bps NUMERIC DEFAULT 0,
                estimated_dollars NUMERIC DEFAULT 0,
                confidence  NUMERIC DEFAULT 0.5,
                is_new      BOOLEAN DEFAULT TRUE,
                raw_data    JSONB DEFAULT '{}',
                created_at  TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_nic_ticker_date
                ON news_impact_catalysts (ticker, event_date);

            CREATE TABLE IF NOT EXISTS news_impact_expectations (
                id          TEXT PRIMARY KEY,
                ticker      TEXT NOT NULL,
                description TEXT NOT NULL,
                catalyst_type TEXT NOT NULL,
                horizon     TEXT NOT NULL,
                expected_direction TEXT NOT NULL,
                expected_magnitude_bps NUMERIC DEFAULT 0,
                baked_in_pct NUMERIC DEFAULT 0,
                deadline    DATE,
                status      TEXT DEFAULT 'active'
                            CHECK (status IN ('active', 'fulfilled', 'expired', 'invalidated')),
                evidence    JSONB DEFAULT '[]',
                created_at  TIMESTAMPTZ DEFAULT NOW(),
                resolved_at TIMESTAMPTZ
            );
            CREATE INDEX IF NOT EXISTS idx_nie_ticker_status
                ON news_impact_expectations (ticker, status);

            CREATE TABLE IF NOT EXISTS news_impact_reports (
                ticker      TEXT PRIMARY KEY,
                payload     JSONB NOT NULL,
                narrative   TEXT,
                confidence  NUMERIC DEFAULT 0,
                created_at  TIMESTAMPTZ DEFAULT NOW()
            );
        """))
    log.info("News impact tables ensured")
