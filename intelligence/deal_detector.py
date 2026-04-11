"""
GRID Intelligence — M&A / Deal Detection Engine.

Parses news flow to detect M&A activity, deal rumors, partnership announcements,
and corporate actions. Classifies deals by type, stage, and structure, then
tracks them through their lifecycle (rumor → confirmed → closed/failed).

Deal types detected:
    - MERGER: two companies combining
    - ACQUISITION: one company buying another
    - TAKEOVER_BID: hostile or unsolicited offer
    - PARTNERSHIP: strategic alliance or JV
    - SPINOFF: company divesting a division
    - IPO: initial public offering
    - BUYBACK: share repurchase program
    - DIVESTITURE: selling a business unit
    - LICENSING: technology or IP licensing deal
    - INVESTMENT: strategic minority investment

Pipeline stages:
    RUMOR → REPORTED → CONFIRMED → REGULATORY → CLOSED | FAILED | WITHDRAWN

Architecture:
    1. DealClassifier  — NLP extraction of deal signals from headlines/articles
    2. DealTracker     — lifecycle tracking with stage transitions
    3. DealScorer      — estimate market impact (bps) and probability
    4. DealDetector    — orchestrator that scans news and maintains deal pipeline

Source: news_articles table (populated by all news pullers)
Schedule: Every 6 hours (aligned with Oracle cycle)
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# ── Constants ────────────────────────────────────────────────────────────

DEAL_TYPES = {
    "MERGER", "ACQUISITION", "TAKEOVER_BID", "PARTNERSHIP", "SPINOFF",
    "IPO", "BUYBACK", "DIVESTITURE", "LICENSING", "INVESTMENT",
}

DEAL_STAGES = {
    "RUMOR": 0,
    "REPORTED": 1,
    "CONFIRMED": 2,
    "REGULATORY": 3,
    "CLOSED": 4,
    "FAILED": 5,
    "WITHDRAWN": 6,
}

# Typical basis-point impact by deal type (on the target)
TYPICAL_IMPACT_BPS: dict[str, int] = {
    "MERGER": 500,
    "ACQUISITION": 600,
    "TAKEOVER_BID": 800,
    "PARTNERSHIP": 100,
    "SPINOFF": 200,
    "IPO": 300,
    "BUYBACK": 80,
    "DIVESTITURE": 150,
    "LICENSING": 60,
    "INVESTMENT": 120,
}

# ── Keyword patterns ────────────────────────────────────────────────────

_MA_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\b(takeover|hostile\s+bid|unsolicited\s+offer|tender\s+offer)\b", re.I), "TAKEOVER_BID"),
    (re.compile(r"\b(acquir(?:e[sd]?|ing)|acquisition|bought?|purchas(?:e[sd]?|ing))\b", re.I), "ACQUISITION"),
    (re.compile(r"\b(merg(?:e[sd]?|ing|er)|combination|combine[sd]?)\b", re.I), "MERGER"),
    (re.compile(r"\b(partner(?:ship|ed|ing)|alliance|joint\s+venture|JV|collaborat(?:e[sd]?|ion|ing))\b", re.I), "PARTNERSHIP"),
    (re.compile(r"\b(spin[\s-]?off|spin[\s-]?out|carve[\s-]?out|separat(?:e[sd]?|ion|ing)\s+(?:its|the)\s+\w+\s+(?:business|unit|division))\b", re.I), "SPINOFF"),
    (re.compile(r"\b(IPO|initial\s+public\s+offering|go(?:ing|es)?\s+public|direct\s+listing|SPAC\s+merger)\b", re.I), "IPO"),
    (re.compile(r"\b(buyback|repurchas(?:e[sd]?|ing)|share\s+repurchase)\b", re.I), "BUYBACK"),
    (re.compile(r"\b(divestiture|divest(?:s|ed|ing)?|sell(?:s|ing)?\s+(?:its|the)\s+\w+\s+(?:business|unit|division|arm))\b", re.I), "DIVESTITURE"),
    (re.compile(r"\b(licens(?:e[sd]?|ing)\s+(?:deal|agreement|pact))\b", re.I), "LICENSING"),
    (re.compile(r"\b(invest(?:s|ed|ing|ment)?\s+(?:\$[\d.]+\s*(?:billion|million|B|M)|\d+\s*(?:billion|million)))\b", re.I), "INVESTMENT"),
]

# Stage keyword patterns
_STAGE_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\b(rumor(?:ed|s)?|consider(?:s|ed|ing)?|explor(?:e[sd]?|ing)|weigh(?:s|ed|ing)?|talk(?:s|ed|ing)?|in\s+discussions?)\b", re.I), "RUMOR"),
    (re.compile(r"\b(report(?:s|ed|edly)|according\s+to\s+sources?|people\s+familiar|said\s+to\s+be)\b", re.I), "REPORTED"),
    (re.compile(r"\b(confirm(?:s|ed)?|announc(?:e[sd]?|ing)|agree(?:s|d)?|definitive\s+agreement|signed)\b", re.I), "CONFIRMED"),
    (re.compile(r"\b(regulatory\s+approv|antitrust\s+review|FTC\s+review|DOJ\s+review|EU\s+approv|clear(?:s|ed)\s+by)\b", re.I), "REGULATORY"),
    (re.compile(r"\b(complet(?:e[sd]?|ion)|clos(?:e[sd]?|ing)|finaliz(?:e[sd]?|ing))\b", re.I), "CLOSED"),
    (re.compile(r"\b(fail(?:s|ed)?|collapse[sd]?|scrap(?:s|ped)?|abandon(?:s|ed)?|break(?:s)?\s+(?:off|down))\b", re.I), "FAILED"),
    (re.compile(r"\b(withdraw(?:s|n)?|pull(?:s|ed)?\s+(?:out|back)|walk(?:s|ed)?\s+away)\b", re.I), "WITHDRAWN"),
]

# Dollar value extraction
_DOLLAR_PATTERN = re.compile(
    r"\$\s*([\d,.]+)\s*(billion|million|B|M|bn|mn|trillion|T)\b", re.I,
)

# Ticker extraction (e.g. $AAPL or (NASDAQ: AAPL))
_TICKER_PATTERN = re.compile(
    r"(?:\$([A-Z]{1,5})|\((?:NYSE|NASDAQ|AMEX|OTC):\s*([A-Z]{1,5})\))",
)


# ── Data Classes ─────────────────────────────────────────────────────────

@dataclass
class DealSignal:
    """A detected deal or M&A signal from news."""
    deal_id: str                  # SHA hash of key fields
    deal_type: str                # one of DEAL_TYPES
    stage: str                    # one of DEAL_STAGES
    tickers: list[str]            # involved ticker symbols
    acquirer: str                 # acquirer company name (if applicable)
    target: str                   # target company name
    headline: str                 # originating headline
    source: str                   # news source
    deal_value_usd: float | None  # estimated deal value in USD
    estimated_impact_bps: int     # estimated basis-point impact
    probability: float            # 0-1 probability of deal closing
    direction: str                # bullish/bearish for the target
    confidence: float             # 0-1 confidence in detection
    detected_at: datetime
    article_url: str = ""
    evidence: list[str] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["detected_at"] = self.detected_at.isoformat()
        return d


# ── Deal Classifier ──────────────────────────────────────────────────────

class DealClassifier:
    """Extract deal signals from news article text via pattern matching."""

    def classify(
        self,
        title: str,
        summary: str,
        tickers: list[str],
        source: str,
        pub_date: datetime | None = None,
        url: str = "",
    ) -> DealSignal | None:
        """Classify a news article as a potential deal.

        Args:
            title: Article headline.
            summary: Article body or summary text.
            tickers: Extracted ticker symbols.
            source: News source name.
            pub_date: Publication datetime.
            url: Article URL.

        Returns:
            DealSignal if deal detected, None otherwise.
        """
        full_text = f"{title} {summary}"

        # Detect deal type
        deal_type = self._detect_deal_type(full_text)
        if not deal_type:
            return None

        # Detect stage
        stage = self._detect_stage(full_text)

        # Extract dollar value
        deal_value = self._extract_value(full_text)

        # Extract company names
        acquirer, target = self._extract_parties(full_text, deal_type)

        # Extract tickers from text if not provided
        text_tickers = self._extract_tickers(full_text)
        all_tickers = list(set(tickers + text_tickers))

        # Estimate impact
        impact_bps = TYPICAL_IMPACT_BPS.get(deal_type, 100)

        # Estimate probability based on stage
        probability = self._stage_probability(stage)

        # Confidence based on specificity of match
        confidence = self._compute_confidence(
            deal_type, stage, deal_value, all_tickers, full_text,
        )

        # Direction: most deal types are bullish for target
        direction = self._infer_direction(deal_type, stage)

        now = pub_date or datetime.now(timezone.utc)
        deal_id = hashlib.sha256(
            f"{deal_type}:{':'.join(sorted(all_tickers))}:{target}:{now.date()}".encode()
        ).hexdigest()[:20]

        evidence = [f"Headline: {title}"]
        if deal_value:
            evidence.append(f"Value: ${deal_value:,.0f}")
        if acquirer:
            evidence.append(f"Acquirer: {acquirer}")
        if target:
            evidence.append(f"Target: {target}")
        evidence.append(f"Stage: {stage}")

        return DealSignal(
            deal_id=deal_id,
            deal_type=deal_type,
            stage=stage,
            tickers=all_tickers,
            acquirer=acquirer,
            target=target,
            headline=title[:500],
            source=source,
            deal_value_usd=deal_value,
            estimated_impact_bps=impact_bps,
            probability=probability,
            direction=direction,
            confidence=confidence,
            detected_at=now,
            article_url=url,
            evidence=evidence,
        )

    def _detect_deal_type(self, text: str) -> str | None:
        """Match text against deal type patterns."""
        for pattern, deal_type in _MA_PATTERNS:
            if pattern.search(text):
                return deal_type
        return None

    def _detect_stage(self, text: str) -> str:
        """Detect deal lifecycle stage from text."""
        # Check in reverse priority order (more specific patterns first)
        for pattern, stage in reversed(_STAGE_PATTERNS):
            if pattern.search(text):
                return stage
        return "REPORTED"

    def _extract_value(self, text: str) -> float | None:
        """Extract dollar value from text."""
        match = _DOLLAR_PATTERN.search(text)
        if not match:
            return None

        raw_value = float(match.group(1).replace(",", ""))
        multiplier_text = match.group(2).lower()

        multipliers = {
            "trillion": 1e12, "t": 1e12,
            "billion": 1e9, "b": 1e9, "bn": 1e9,
            "million": 1e6, "m": 1e6, "mn": 1e6,
        }
        multiplier = multipliers.get(multiplier_text, 1.0)
        return raw_value * multiplier

    def _extract_parties(
        self, text: str, deal_type: str,
    ) -> tuple[str, str]:
        """Extract acquirer and target names from text.

        Uses simple heuristics: in "X acquires Y" patterns,
        X is the acquirer and Y is the target.
        """
        acquirer = ""
        target = ""

        # Pattern: "X to acquire/merge with Y"
        patterns = [
            re.compile(
                r"([A-Z][\w\s&.']+?)\s+(?:to\s+)?(?:acquir|buy|purchas|tak(?:e|ing)\s+over)\w*\s+([A-Z][\w\s&.']+?)(?:\s+(?:for|in|at)\b|$)",
                re.I,
            ),
            re.compile(
                r"([A-Z][\w\s&.']+?)\s+(?:and|,)\s+([A-Z][\w\s&.']+?)\s+(?:to\s+)?merg",
                re.I,
            ),
        ]

        for pat in patterns:
            m = pat.search(text)
            if m:
                acquirer = m.group(1).strip()[:100]
                target = m.group(2).strip()[:100]
                break

        return acquirer, target

    def _extract_tickers(self, text: str) -> list[str]:
        """Extract ticker symbols from text."""
        matches = _TICKER_PATTERN.findall(text)
        tickers = []
        for dollar_tick, paren_tick in matches:
            tick = dollar_tick or paren_tick
            if tick and len(tick) <= 5:
                tickers.append(tick.upper())
        return tickers

    def _stage_probability(self, stage: str) -> float:
        """Estimate deal close probability from stage."""
        probabilities = {
            "RUMOR": 0.15,
            "REPORTED": 0.30,
            "CONFIRMED": 0.70,
            "REGULATORY": 0.80,
            "CLOSED": 1.0,
            "FAILED": 0.0,
            "WITHDRAWN": 0.0,
        }
        return probabilities.get(stage, 0.25)

    def _compute_confidence(
        self,
        deal_type: str,
        stage: str,
        deal_value: float | None,
        tickers: list[str],
        text: str,
    ) -> float:
        """Compute confidence in the deal detection."""
        conf = 0.4  # base

        # More specific deal types get higher confidence
        if deal_type in ("ACQUISITION", "MERGER", "TAKEOVER_BID"):
            conf += 0.1

        # Having a dollar value is strong evidence
        if deal_value is not None:
            conf += 0.15

        # Having tickers is strong evidence
        if len(tickers) >= 2:
            conf += 0.1
        elif len(tickers) >= 1:
            conf += 0.05

        # Confirmed/closed stages get higher confidence
        if stage in ("CONFIRMED", "REGULATORY", "CLOSED"):
            conf += 0.15
        elif stage == "REPORTED":
            conf += 0.05

        # Longer, more detailed text = more confident
        if len(text) > 200:
            conf += 0.05

        return min(1.0, round(conf, 3))

    def _infer_direction(self, deal_type: str, stage: str) -> str:
        """Infer market direction for the target."""
        if stage in ("FAILED", "WITHDRAWN"):
            return "bearish"

        bullish_types = {
            "ACQUISITION", "MERGER", "TAKEOVER_BID", "BUYBACK",
            "PARTNERSHIP", "INVESTMENT", "IPO",
        }
        if deal_type in bullish_types:
            return "bullish"

        return "neutral"


# ── Deal Tracker ─────────────────────────────────────────────────────────

class DealTracker:
    """Track deals through their lifecycle with stage transitions."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def update_or_create(self, signal: DealSignal) -> bool:
        """Update existing deal or create new one.

        If a deal with matching tickers + type already exists, update its
        stage if the new stage is more advanced. Otherwise create a new entry.

        Args:
            signal: The detected DealSignal.

        Returns:
            True if stored/updated successfully.
        """
        try:
            with self.engine.begin() as conn:
                # Check for existing deal
                existing = conn.execute(
                    text("""
                        SELECT deal_id, stage, probability
                        FROM deal_pipeline
                        WHERE deal_type = :dtype
                          AND tickers && :tickers
                          AND stage NOT IN ('CLOSED', 'FAILED', 'WITHDRAWN')
                        ORDER BY detected_at DESC
                        LIMIT 1
                    """),
                    {
                        "dtype": signal.deal_type,
                        "tickers": signal.tickers,
                    },
                ).fetchone()

                if existing:
                    old_stage_rank = DEAL_STAGES.get(existing[1], 0)
                    new_stage_rank = DEAL_STAGES.get(signal.stage, 0)

                    # Only advance stage, never regress
                    if new_stage_rank > old_stage_rank:
                        conn.execute(
                            text("""
                                UPDATE deal_pipeline
                                SET stage = :stage,
                                    probability = :prob,
                                    updated_at = NOW(),
                                    evidence = evidence || :new_evidence
                                WHERE deal_id = :did
                            """),
                            {
                                "stage": signal.stage,
                                "prob": signal.probability,
                                "new_evidence": json.dumps(signal.evidence),
                                "did": existing[0],
                            },
                        )
                        log.info(
                            "Deal {d} advanced: {old} → {new}",
                            d=existing[0], old=existing[1], new=signal.stage,
                        )
                    return True

                # Insert new deal
                conn.execute(
                    text("""
                        INSERT INTO deal_pipeline
                        (deal_id, deal_type, stage, tickers, acquirer, target,
                         headline, source, deal_value_usd, estimated_impact_bps,
                         probability, direction, confidence, detected_at,
                         article_url, evidence, metadata)
                        VALUES
                        (:did, :dtype, :stage, :tickers, :acq, :tgt,
                         :headline, :src, :val, :impact,
                         :prob, :dir, :conf, :det_at,
                         :url, :ev, :meta)
                        ON CONFLICT (deal_id) DO NOTHING
                    """),
                    {
                        "did": signal.deal_id,
                        "dtype": signal.deal_type,
                        "stage": signal.stage,
                        "tickers": signal.tickers,
                        "acq": signal.acquirer,
                        "tgt": signal.target,
                        "headline": signal.headline,
                        "src": signal.source,
                        "val": signal.deal_value_usd,
                        "impact": signal.estimated_impact_bps,
                        "prob": signal.probability,
                        "dir": signal.direction,
                        "conf": signal.confidence,
                        "det_at": signal.detected_at,
                        "url": signal.article_url,
                        "ev": json.dumps(signal.evidence),
                        "meta": json.dumps(signal.metadata, default=str),
                    },
                )
                return True

        except Exception as exc:
            log.warning(
                "Deal tracker update failed: {e}", e=str(exc),
            )
            return False


# ── Main Orchestrator ────────────────────────────────────────────────────

class DealDetector:
    """Scans news flow for M&A and deal activity.

    Orchestrates the classifier, tracker, and scoring pipeline.

    Attributes:
        engine: SQLAlchemy engine.
        classifier: DealClassifier instance.
        tracker: DealTracker instance.
    """

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self.classifier = DealClassifier()
        self.tracker = DealTracker(engine)
        self._ensure_table()

    def _ensure_table(self) -> None:
        """Create deal_pipeline table if it doesn't exist."""
        try:
            with self.engine.begin() as conn:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS deal_pipeline (
                        id SERIAL PRIMARY KEY,
                        deal_id TEXT UNIQUE NOT NULL,
                        deal_type TEXT NOT NULL,
                        stage TEXT NOT NULL DEFAULT 'REPORTED',
                        tickers TEXT[] DEFAULT '{}',
                        acquirer TEXT DEFAULT '',
                        target TEXT DEFAULT '',
                        headline TEXT NOT NULL,
                        source TEXT DEFAULT '',
                        deal_value_usd DOUBLE PRECISION,
                        estimated_impact_bps INTEGER DEFAULT 100,
                        probability DOUBLE PRECISION DEFAULT 0.25,
                        direction TEXT DEFAULT 'neutral',
                        confidence DOUBLE PRECISION DEFAULT 0.5,
                        detected_at TIMESTAMPTZ DEFAULT NOW(),
                        updated_at TIMESTAMPTZ DEFAULT NOW(),
                        article_url TEXT DEFAULT '',
                        evidence JSONB DEFAULT '[]',
                        metadata JSONB DEFAULT '{}'
                    )
                """))
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_deal_pipeline_type
                    ON deal_pipeline (deal_type)
                """))
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_deal_pipeline_stage
                    ON deal_pipeline (stage)
                """))
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_deal_pipeline_tickers
                    ON deal_pipeline USING GIN (tickers)
                """))
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_deal_pipeline_detected
                    ON deal_pipeline (detected_at DESC)
                """))
        except Exception as exc:
            log.warning("Failed to create deal_pipeline table: {e}", e=str(exc))

    def scan_recent_news(self, hours: int = 12) -> dict[str, Any]:
        """Scan recent news articles for deal signals.

        Args:
            hours: Hours to look back.

        Returns:
            Summary dict with detected deals.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text("""
                        SELECT title, summary, tickers, source, published_at, url
                        FROM news_articles
                        WHERE created_at >= :cutoff
                        ORDER BY created_at DESC
                        LIMIT 10000
                    """),
                    {"cutoff": cutoff},
                ).fetchall()
        except Exception as exc:
            log.warning("Deal scan query failed: {e}", e=str(exc))
            return {"deals_detected": 0, "error": str(exc)}

        deals_detected: list[dict] = []
        deals_stored = 0

        for row in rows:
            title = row[0] or ""
            summary = row[1] or ""
            tickers = row[2] or []
            source = row[3] or ""
            pub_date = row[4]
            url = row[5] or ""

            signal = self.classifier.classify(
                title=title,
                summary=summary,
                tickers=tickers,
                source=source,
                pub_date=pub_date,
                url=url,
            )

            if signal and signal.confidence >= 0.4:
                stored = self.tracker.update_or_create(signal)
                if stored:
                    deals_stored += 1
                deals_detected.append(signal.to_dict())

        # Also scan trending_items for deal signals
        trending_deals = self._scan_trending(cutoff)
        deals_detected.extend(trending_deals)

        result = {
            "scanned_at": datetime.now(timezone.utc).isoformat(),
            "articles_scanned": len(rows),
            "deals_detected": len(deals_detected),
            "deals_stored": deals_stored,
            "deals": deals_detected,
            "by_type": self._count_by_field(deals_detected, "deal_type"),
            "by_stage": self._count_by_field(deals_detected, "stage"),
        }

        log.info(
            "Deal scan: {n} articles → {d} deals detected, {s} stored",
            n=len(rows), d=len(deals_detected), s=deals_stored,
        )

        return result

    def _scan_trending(self, cutoff: datetime) -> list[dict]:
        """Scan trending_items for deal-related content.

        Args:
            cutoff: Datetime cutoff.

        Returns:
            List of deal signal dicts from trending items.
        """
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text("""
                        SELECT title, content, platform, created_at, url
                        FROM trending_items
                        WHERE created_at >= :cutoff
                          AND engagement_total >= 20
                        ORDER BY engagement_total DESC
                        LIMIT 200
                    """),
                    {"cutoff": cutoff},
                ).fetchall()
        except Exception:
            return []

        results: list[dict] = []
        for row in rows:
            title = row[0] or ""
            content = row[1] or ""
            platform = row[2] or ""
            pub_date = row[3]
            url = row[4] or ""

            signal = self.classifier.classify(
                title=title,
                summary=content,
                tickers=[],
                source=f"trending_{platform}",
                pub_date=pub_date,
                url=url,
            )

            if signal and signal.confidence >= 0.45:
                self.tracker.update_or_create(signal)
                results.append(signal.to_dict())

        return results

    def _count_by_field(
        self, deals: list[dict], field_name: str,
    ) -> dict[str, int]:
        """Count deals grouped by a field."""
        counts: dict[str, int] = {}
        for d in deals:
            val = d.get(field_name, "unknown")
            counts[val] = counts.get(val, 0) + 1
        return counts

    def get_active_deals(
        self,
        deal_type: str | None = None,
        ticker: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get all active (non-terminal) deals in the pipeline.

        Args:
            deal_type: Optional filter by deal type.
            ticker: Optional filter by ticker.

        Returns:
            List of deal dicts.
        """
        conditions = ["stage NOT IN ('CLOSED', 'FAILED', 'WITHDRAWN')"]
        params: dict[str, Any] = {}

        if deal_type:
            conditions.append("deal_type = :dtype")
            params["dtype"] = deal_type.upper()
        if ticker:
            conditions.append(":ticker = ANY(tickers)")
            params["ticker"] = ticker.upper()

        where_clause = " AND ".join(conditions)

        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text(f"""
                        SELECT deal_id, deal_type, stage, tickers, acquirer, target,
                               headline, source, deal_value_usd, estimated_impact_bps,
                               probability, direction, confidence, detected_at,
                               updated_at, article_url, evidence
                        FROM deal_pipeline
                        WHERE {where_clause}
                        ORDER BY probability DESC, detected_at DESC
                        LIMIT 100
                    """),
                    params,
                ).fetchall()
        except Exception as exc:
            log.warning("Active deals query failed: {e}", e=str(exc))
            return []

        return [
            {
                "deal_id": r[0],
                "deal_type": r[1],
                "stage": r[2],
                "tickers": r[3] or [],
                "acquirer": r[4],
                "target": r[5],
                "headline": r[6],
                "source": r[7],
                "deal_value_usd": r[8],
                "estimated_impact_bps": r[9],
                "probability": r[10],
                "direction": r[11],
                "confidence": r[12],
                "detected_at": r[13].isoformat() if r[13] else None,
                "updated_at": r[14].isoformat() if r[14] else None,
                "article_url": r[15],
                "evidence": r[16],
            }
            for r in rows
        ]

    def get_deal_history(
        self,
        ticker: str,
        days: int = 90,
    ) -> list[dict[str, Any]]:
        """Get deal history for a ticker.

        Args:
            ticker: Ticker symbol.
            days: Days to look back.

        Returns:
            List of deal dicts, including closed/failed.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text("""
                        SELECT deal_id, deal_type, stage, tickers, acquirer, target,
                               headline, source, deal_value_usd, estimated_impact_bps,
                               probability, direction, confidence, detected_at,
                               updated_at, evidence
                        FROM deal_pipeline
                        WHERE :ticker = ANY(tickers)
                          AND detected_at >= :cutoff
                        ORDER BY detected_at DESC
                        LIMIT 50
                    """),
                    {"ticker": ticker.upper(), "cutoff": cutoff},
                ).fetchall()
        except Exception as exc:
            log.warning("Deal history query failed for {t}: {e}", t=ticker, e=str(exc))
            return []

        return [
            {
                "deal_id": r[0],
                "deal_type": r[1],
                "stage": r[2],
                "tickers": r[3] or [],
                "acquirer": r[4],
                "target": r[5],
                "headline": r[6],
                "source": r[7],
                "deal_value_usd": r[8],
                "estimated_impact_bps": r[9],
                "probability": r[10],
                "direction": r[11],
                "confidence": r[12],
                "detected_at": r[13].isoformat() if r[13] else None,
                "updated_at": r[14].isoformat() if r[14] else None,
                "evidence": r[15],
            }
            for r in rows
        ]

    def get_pipeline_summary(self) -> dict[str, Any]:
        """Get a summary of the entire deal pipeline.

        Returns:
            Summary dict with counts by type, stage, and recent activity.
        """
        try:
            with self.engine.connect() as conn:
                # Counts by stage
                stage_rows = conn.execute(text("""
                    SELECT stage, COUNT(*)
                    FROM deal_pipeline
                    GROUP BY stage
                    ORDER BY COUNT(*) DESC
                """)).fetchall()

                # Counts by type
                type_rows = conn.execute(text("""
                    SELECT deal_type, COUNT(*)
                    FROM deal_pipeline
                    WHERE stage NOT IN ('CLOSED', 'FAILED', 'WITHDRAWN')
                    GROUP BY deal_type
                    ORDER BY COUNT(*) DESC
                """)).fetchall()

                # Recent activity (last 48h)
                recent_count = conn.execute(text("""
                    SELECT COUNT(*)
                    FROM deal_pipeline
                    WHERE detected_at >= NOW() - INTERVAL '48 hours'
                """)).scalar() or 0

                # Total deal value (active deals)
                total_value = conn.execute(text("""
                    SELECT SUM(deal_value_usd)
                    FROM deal_pipeline
                    WHERE stage NOT IN ('CLOSED', 'FAILED', 'WITHDRAWN')
                      AND deal_value_usd IS NOT NULL
                """)).scalar() or 0

        except Exception as exc:
            log.warning("Pipeline summary query failed: {e}", e=str(exc))
            return {"error": str(exc)}

        return {
            "by_stage": {r[0]: r[1] for r in stage_rows},
            "by_type": {r[0]: r[1] for r in type_rows},
            "recent_48h": recent_count,
            "total_active_value_usd": float(total_value),
            "total_deals": sum(r[1] for r in stage_rows),
        }
