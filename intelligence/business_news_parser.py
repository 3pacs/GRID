"""
GRID Intelligence — Business News Parser.

Extracts structured business signals from raw news articles:
    - Corporate deals, partnerships, and joint ventures
    - Executive changes (CEO, CFO, board)
    - Product launches and expansions
    - Layoffs, restructurings, and cost-cutting
    - Regulatory actions and legal developments
    - Capital raises, debt issuances, credit events
    - Guidance changes and earnings surprises
    - Geographic expansion and market entry

Each extracted event is classified by:
    - Category (deal, executive, product, restructuring, etc.)
    - Impact direction (bullish/bearish/neutral)
    - Estimated magnitude (bps)
    - Time horizon (immediate/short/medium/long)
    - Confidence score

Sources: news_articles table + trending_items table
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

# ── Event Categories ─────────────────────────────────────────────────────

EVENT_CATEGORIES: dict[str, dict[str, Any]] = {
    "executive_change": {
        "typical_bps": 80,
        "horizon": "medium",
        "description": "CEO/CFO/board appointment or departure",
    },
    "product_launch": {
        "typical_bps": 120,
        "horizon": "medium",
        "description": "New product, service, or platform launch",
    },
    "expansion": {
        "typical_bps": 100,
        "horizon": "long",
        "description": "Geographic or market expansion",
    },
    "restructuring": {
        "typical_bps": 150,
        "horizon": "medium",
        "description": "Layoffs, cost-cutting, reorganization",
    },
    "regulatory_action": {
        "typical_bps": 200,
        "horizon": "long",
        "description": "Regulatory investigation, fine, or approval",
    },
    "legal_action": {
        "typical_bps": 180,
        "horizon": "long",
        "description": "Lawsuit, settlement, or court ruling",
    },
    "capital_raise": {
        "typical_bps": 60,
        "horizon": "short",
        "description": "Equity offering, debt issuance, convertible",
    },
    "credit_event": {
        "typical_bps": 300,
        "horizon": "short",
        "description": "Downgrade, default, covenant breach",
    },
    "guidance_change": {
        "typical_bps": 200,
        "horizon": "short",
        "description": "Raised/lowered/reaffirmed guidance",
    },
    "earnings_surprise": {
        "typical_bps": 300,
        "horizon": "short",
        "description": "Beat/miss on earnings or revenue",
    },
    "contract_win": {
        "typical_bps": 100,
        "horizon": "medium",
        "description": "Major contract or government award",
    },
    "analyst_action": {
        "typical_bps": 60,
        "horizon": "short",
        "description": "Upgrade, downgrade, or price target change",
    },
    "insider_activity": {
        "typical_bps": 40,
        "horizon": "medium",
        "description": "Insider buying/selling cluster",
    },
    "supply_chain": {
        "typical_bps": 80,
        "horizon": "medium",
        "description": "Supply disruption, new supplier, reshoring",
    },
    "competitive": {
        "typical_bps": 60,
        "horizon": "medium",
        "description": "Competitor action affecting the company",
    },
}

# ── Keyword Patterns ─────────────────────────────────────────────────────

_PATTERNS: list[tuple[re.Pattern, str, str]] = [
    # Executive changes
    (re.compile(r"\b(appoint|name[sd]?|hire[sd]?|promote[sd]?)\s+\w+(?:\s+\w+){0,4}\s*(?:as\s+)?(?:new\s+)?(?:CEO|chief executive|president)\b", re.I), "executive_change", "neutral"),
    (re.compile(r"\b(?:CEO|chief executive|president)\s+(?:step(?:s|ped)?\s+down|resign|retire|depart|leave)\b", re.I), "executive_change", "bearish"),
    (re.compile(r"\b(appoint|name[sd]?|hire[sd]?)\s+(?:as\s+)?(?:new\s+)?(?:CFO|chief financial|COO|CTO)\b", re.I), "executive_change", "neutral"),
    (re.compile(r"\b(?:board|director)\s+(?:member|seat)\s+(?:appoint|add|resign|depart)\b", re.I), "executive_change", "neutral"),

    # Product launches
    (re.compile(r"\b(?:launch|unveil|introduc|releas|announc)\w*\s+(?:new|its|a)\s+(?:product|platform|service|chip|model|device|drug|app|tool)\b", re.I), "product_launch", "bullish"),
    (re.compile(r"\b(?:FDA|regulatory)\s+(?:approv|clear|authoriz)\w*\b", re.I), "product_launch", "bullish"),
    (re.compile(r"\b(?:recall|discontinu|withdraw|suspend)\w*\s+(?:its|the)\s+(?:product|drug|device|model)\b", re.I), "product_launch", "bearish"),

    # Expansion
    (re.compile(r"\b(?:expand|enter|open)\w*\s+(?:into|in|new)\s+(?:market|region|countr|segment|stor|facilit)\b", re.I), "expansion", "bullish"),
    (re.compile(r"\b(?:build|construct|break\s+ground|new\s+plant|new\s+factory|new\s+facility|new\s+headquarter)\b", re.I), "expansion", "bullish"),

    # Restructuring
    (re.compile(r"\b(?:layoff|lay\s+off|cut|reduc|eliminat)\w*\s+(?:[\d,]+\s+)?(?:jobs?|positions?|employees?|workers?|staff|roles?|headcount)\b", re.I), "restructuring", "bearish"),
    (re.compile(r"\b(?:restructur|reorganiz|streamlin|consolidat|right[\s-]?siz)\w*\b", re.I), "restructuring", "neutral"),
    (re.compile(r"\b(?:cost[\s-]?cut|expense\s+reduc|austerity|efficiency\s+program)\b", re.I), "restructuring", "neutral"),

    # Regulatory
    (re.compile(r"\b(?:SEC|FTC|DOJ|EU|antitrust|regulator)\s+(?:investigat|prob|su|fine[sd]?|charg|rul|block)\w*\b", re.I), "regulatory_action", "bearish"),
    (re.compile(r"\b(?:regulatory\s+approv|license|permit|clear(?:ed|ance))\b", re.I), "regulatory_action", "bullish"),

    # Legal
    (re.compile(r"\b(?:lawsuit|su(?:e[sd]?|ing)|litigat|class[\s-]?action|patent\s+infring)\w*\b", re.I), "legal_action", "bearish"),
    (re.compile(r"\b(?:settl(?:e[sd]?|ment)|verdict|judgment|award)\w*\s+(?:for\s+)?\$[\d,.]+\b", re.I), "legal_action", "neutral"),

    # Capital markets
    (re.compile(r"\b(?:rais|offer|issu)\w*\s+(?:\$[\d,.]+\s*(?:billion|million|B|M)\s+)?(?:in\s+)?(?:equity|stock|share|debt|bond|convertible|note)\b", re.I), "capital_raise", "neutral"),
    (re.compile(r"\b(?:secondary\s+offering|shelf\s+registration|ATM\s+program|at[\s-]the[\s-]market)\b", re.I), "capital_raise", "bearish"),

    # Credit
    (re.compile(r"\b(?:downgrad|credit\s+rating|Moody|Fitch|S&P)\w*\s+(?:cut|lower|reduc|downgrad)\b", re.I), "credit_event", "bearish"),
    (re.compile(r"\b(?:upgrad|credit\s+rating)\w*\s+(?:rais|improv|upgrad)\b", re.I), "credit_event", "bullish"),
    (re.compile(r"\b(?:default|bankrupt|chapter\s+11|chapter\s+7|insolvenc|covenant\s+breach)\b", re.I), "credit_event", "bearish"),

    # Guidance
    (re.compile(r"\b(?:rais|increas|boost)\w*\s+(?:its\s+)?(?:guidance|outlook|forecast|target)\b", re.I), "guidance_change", "bullish"),
    (re.compile(r"\b(?:lower|cut|reduc|slash)\w*\s+(?:its\s+)?(?:guidance|outlook|forecast|target)\b", re.I), "guidance_change", "bearish"),
    (re.compile(r"\b(?:reaffirm|reiterat|maintain)\w*\s+(?:its\s+)?(?:guidance|outlook|forecast)\b", re.I), "guidance_change", "neutral"),

    # Earnings
    (re.compile(r"\b(?:beat|top|exceed|surpass)\w*\s+(?:earnings|revenue|EPS|estimates?|expectations?|consensus)\b", re.I), "earnings_surprise", "bullish"),
    (re.compile(r"\b(?:miss|fall\s+short|disappoint|below)\w*\s+(?:earnings|revenue|EPS|estimates?|expectations?|consensus)\b", re.I), "earnings_surprise", "bearish"),

    # Contracts
    (re.compile(r"\b(?:win|award|secur|land)\w*\s+(?:\$[\d,.]+\s*(?:billion|million|B|M)\s+)?(?:contract|deal|order|agreement)\b", re.I), "contract_win", "bullish"),
    (re.compile(r"\b(?:government|military|defense|Pentagon|DOD|NASA)\s+(?:contract|award|order)\b", re.I), "contract_win", "bullish"),

    # Analyst
    (re.compile(r"\b(?:upgrad|rais)\w*\s+(?:to\s+)?(?:buy|overweight|outperform|strong\s+buy)\b", re.I), "analyst_action", "bullish"),
    (re.compile(r"\b(?:downgrad|cut)\w*\s+(?:to\s+)?(?:sell|underweight|underperform|neutral|hold)\b", re.I), "analyst_action", "bearish"),
    (re.compile(r"\b(?:price\s+target)\s+(?:rais|increas|cut|lower|set)\w*\s+(?:to\s+)?\$[\d,.]+\b", re.I), "analyst_action", "neutral"),

    # Insider
    (re.compile(r"\b(?:insider|CEO|CFO|director)\s+(?:buy|bought|purchas)\w*\s+(?:\$[\d,.]+|[\d,.]+\s+shares?)\b", re.I), "insider_activity", "bullish"),
    (re.compile(r"\b(?:insider|CEO|CFO|director)\s+(?:sell|sold|dump)\w*\s+(?:\$[\d,.]+|[\d,.]+\s+shares?)\b", re.I), "insider_activity", "bearish"),

    # Supply chain
    (re.compile(r"\b(?:supply\s+chain|supplier|shortage|disruption|bottleneck|reshore|onshore|nearshore)\b", re.I), "supply_chain", "neutral"),

    # Competitive
    (re.compile(r"\b(?:market\s+share|competitive\s+threat|rival|competitor)\b", re.I), "competitive", "neutral"),
]

# Dollar value extraction
_DOLLAR_RE = re.compile(r"\$\s*([\d,.]+)\s*(billion|million|B|M|bn|mn|trillion|T)\b", re.I)

_MULTIPLIERS = {
    "trillion": 1e12, "t": 1e12,
    "billion": 1e9, "b": 1e9, "bn": 1e9,
    "million": 1e6, "m": 1e6, "mn": 1e6,
}


# ── Data Classes ─────────────────────────────────────────────────────────

@dataclass
class BusinessEvent:
    """A structured business event extracted from news."""
    event_id: str
    category: str                 # one of EVENT_CATEGORIES
    tickers: list[str]
    headline: str
    description: str
    source: str
    direction: str                # bullish/bearish/neutral
    estimated_bps: int
    horizon: str                  # immediate/short/medium/long
    dollar_value: float | None
    confidence: float
    published_at: datetime | None
    article_url: str = ""
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["published_at"] = self.published_at.isoformat() if self.published_at else None
        return d


# ── Business News Parser ─────────────────────────────────────────────────

_TICKER_IN_PARENS = re.compile(r"\(([A-Z]{1,5})\)")  # e.g., "Apple (AAPL)"
_TICKER_STANDALONE = re.compile(r"\b([A-Z]{2,5})\b")  # standalone tickers

# Well-known company name -> ticker for headline extraction
_HEADLINE_COMPANY_MAP: dict[str, str] = {
    "apple": "AAPL", "microsoft": "MSFT", "google": "GOOGL", "alphabet": "GOOGL",
    "amazon": "AMZN", "meta": "META", "meta platforms": "META", "facebook": "META",
    "nvidia": "NVDA", "tesla": "TSLA", "netflix": "NFLX", "adobe": "ADBE",
    "salesforce": "CRM", "intel": "INTC", "amd": "AMD", "broadcom": "AVGO",
    "qualcomm": "QCOM", "cisco": "CSCO", "oracle": "ORCL", "ibm": "IBM",
    "goldman sachs": "GS", "goldman": "GS", "jpmorgan": "JPM", "jp morgan": "JPM",
    "morgan stanley": "MS", "bank of america": "BAC", "citigroup": "C",
    "wells fargo": "WFC", "blackrock": "BLK", "berkshire": "BRK-B",
    "disney": "DIS", "walmart": "WMT", "costco": "COST", "target": "TGT",
    "coca-cola": "KO", "pepsi": "PEP", "pepsico": "PEP",
    "pfizer": "PFE", "eli lilly": "LLY", "lilly": "LLY", "moderna": "MRNA",
    "johnson & johnson": "JNJ", "unitedhealth": "UNH", "abbvie": "ABBV",
    "boeing": "BA", "lockheed": "LMT", "raytheon": "RTX", "northrop": "NOC",
    "exxon": "XOM", "chevron": "CVX", "conocophillips": "COP",
    "uber": "UBER", "airbnb": "ABNB", "spotify": "SPOT", "snowflake": "SNOW",
    "palantir": "PLTR", "coinbase": "COIN", "robinhood": "HOOD",
    "amdocs": "DOX", "coreweave": "CRWV", "sandisk": "SNDK",
    "blackstone": "BX", "kkr": "KKR", "apollo": "APO", "carlyle": "CG",
    "tiger global": "TIGER", "popup bagels": "POPUP",
    "simply good foods": "SMPL", "technip energies": "TE",
    "nio": "NIO", "rivian": "RIVN", "lucid": "LCID",
    "microstrategy": "MSTR", "marathon digital": "MARA", "riot": "RIOT",
    "crowdstrike": "CRWD", "palo alto": "PANW", "fortinet": "FTNT",
    "datadog": "DDOG", "cloudflare": "NET", "zscaler": "ZS",
}

# Words that look like tickers but aren't (false positive blocklist)
_NOT_TICKERS = {
    "CEO", "CFO", "CTO", "COO", "IPO", "SEC", "DOJ", "FTC", "FDA", "EPA",
    "EU", "UK", "US", "USA", "GDP", "CPI", "ETF", "ATM", "AI", "EPS",
    "PE", "YOY", "QOQ", "M&A", "EST", "PM", "AM", "OTC", "NYSE", "ICE",
    "DOD", "NASA", "UN", "WHO", "IMF", "ECB", "BOJ", "BOE", "RBI",
    "THE", "FOR", "AND", "NOT", "BUT", "ALL", "NEW", "NOW", "HOW",
    "WHY", "HAS", "HAD", "MAY", "CAN", "ITS", "SET", "TOP", "BIG",
    "LOW", "WAR", "OIL", "GAS", "TAX", "GDP", "BID",
}


class BusinessNewsParser:
    """Parse news articles into structured business events.

    Scans news_articles and trending_items for business-relevant content,
    classifies each match, and stores structured events for downstream
    consumption by the intelligence pipeline.

    Attributes:
        engine: SQLAlchemy engine.
    """

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self._known_tickers: set[str] | None = None
        self._ensure_table()

    def _ensure_table(self) -> None:
        """Create business_events table if it doesn't exist."""
        try:
            with self.engine.begin() as conn:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS business_events (
                        id SERIAL PRIMARY KEY,
                        event_id TEXT UNIQUE NOT NULL,
                        category TEXT NOT NULL,
                        tickers TEXT[] DEFAULT '{}',
                        headline TEXT NOT NULL,
                        description TEXT DEFAULT '',
                        source TEXT DEFAULT '',
                        direction TEXT DEFAULT 'neutral',
                        estimated_bps INTEGER DEFAULT 0,
                        horizon TEXT DEFAULT 'short',
                        dollar_value DOUBLE PRECISION,
                        confidence DOUBLE PRECISION DEFAULT 0.5,
                        published_at TIMESTAMPTZ,
                        article_url TEXT DEFAULT '',
                        metadata JSONB DEFAULT '{}',
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    )
                """))
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_business_events_category
                    ON business_events (category)
                """))
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_business_events_tickers
                    ON business_events USING GIN (tickers)
                """))
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_business_events_created
                    ON business_events (created_at DESC)
                """))
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_business_events_direction
                    ON business_events (direction)
                """))
        except Exception as exc:
            log.warning("Failed to create business_events table: {e}", e=str(exc))

    def _load_known_tickers(self) -> set[str]:
        """Load valid tickers from company_profiles + feature_registry."""
        if self._known_tickers is not None:
            return self._known_tickers
        tickers: set[str] = set()
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(text(
                    "SELECT UPPER(ticker) FROM company_profiles WHERE ticker IS NOT NULL"
                )).fetchall()
                for r in rows:
                    tickers.add(r[0])
        except Exception:
            pass
        # Add well-known tickers from aliases
        tickers.update(_HEADLINE_COMPANY_MAP.values())
        self._known_tickers = tickers
        return tickers

    def _extract_tickers(self, title: str, summary: str) -> list[str]:
        """Extract stock tickers and company names from headline text.

        Strategy (ordered by reliability):
        1. Parenthetical tickers: "Apple (AAPL)" -> AAPL
        2. Company name matching: "Goldman Sachs" -> GS
        3. Standalone uppercase words verified against known tickers
        """
        found: set[str] = set()
        full_text = f"{title} {summary}"
        lower_text = full_text.lower()

        # 1. Parenthetical tickers — highest confidence
        for m in _TICKER_IN_PARENS.finditer(title):
            candidate = m.group(1)
            if candidate not in _NOT_TICKERS:
                found.add(candidate)

        # 2. Company name resolution — scan for known company names
        # Names that are also common English words need word-boundary + capitalization
        _AMBIGUOUS = {"apple", "meta", "target", "oracle", "uber", "snap", "lilly",
                       "riot", "hood", "net", "block", "coke", "ford"}
        for name, ticker in _HEADLINE_COMPANY_MAP.items():
            if len(name) < 4:
                continue  # skip very short names
            if name in _AMBIGUOUS:
                # Require capitalized form in original text (not lowered)
                cap_name = name.capitalize()
                if cap_name in full_text or name.upper() in full_text:
                    found.add(ticker)
            elif name in lower_text:
                found.add(ticker)

        # 3. Standalone tickers (only in title — summary too noisy)
        known = self._load_known_tickers()
        for m in _TICKER_STANDALONE.finditer(title):
            candidate = m.group(1)
            if candidate in _NOT_TICKERS:
                continue
            if candidate in known:
                found.add(candidate)

        return sorted(found)

    def parse_article(
        self,
        title: str,
        summary: str,
        tickers: list[str],
        source: str,
        pub_date: datetime | None = None,
        url: str = "",
    ) -> list[BusinessEvent]:
        """Parse a single article for business events.

        A single article can contain multiple events (e.g., "AAPL beats
        earnings AND raises guidance AND announces $100B buyback").

        Args:
            title: Article headline.
            summary: Article body or summary.
            tickers: Associated ticker symbols (may contain geo names).
            source: News source name.
            pub_date: Publication datetime.
            url: Article URL.

        Returns:
            List of BusinessEvent objects detected.
        """
        # Extract real stock tickers from headline text
        extracted = self._extract_tickers(title, summary or "")

        # Filter input tickers — remove geo/political names, keep only valid stock tickers
        known = self._load_known_tickers()
        valid_input = [t for t in tickers if t.upper() in known and t.upper() not in _NOT_TICKERS]

        # Merge: extracted tickers + valid input tickers
        merged = list(dict.fromkeys(extracted + valid_input))  # dedup preserving order

        full_text = f"{title} {summary}"
        events: list[BusinessEvent] = []
        seen_categories: set[str] = set()

        for pattern, category, default_direction in _PATTERNS:
            if pattern.search(full_text) and category not in seen_categories:
                seen_categories.add(category)

                cat_info = EVENT_CATEGORIES.get(category, {})
                estimated_bps = cat_info.get("typical_bps", 50)
                horizon = cat_info.get("horizon", "short")

                # Extract dollar value if present
                dollar_value = self._extract_dollar_value(full_text)

                # Compute confidence
                confidence = self._compute_confidence(
                    category, merged, dollar_value, full_text,
                )

                event_id = hashlib.sha256(
                    f"{category}:{':'.join(sorted(merged))}:{title[:80]}".encode()
                ).hexdigest()[:20]

                events.append(BusinessEvent(
                    event_id=event_id,
                    category=category,
                    tickers=merged,
                    headline=title[:500],
                    description=summary[:1000] if summary else "",
                    source=source,
                    direction=default_direction,
                    estimated_bps=estimated_bps,
                    horizon=horizon,
                    dollar_value=dollar_value,
                    confidence=confidence,
                    published_at=pub_date,
                    article_url=url,
                ))

        return events

    def _extract_dollar_value(self, text: str) -> float | None:
        """Extract the first dollar value from text."""
        match = _DOLLAR_RE.search(text)
        if not match:
            return None
        raw = float(match.group(1).replace(",", ""))
        mult = _MULTIPLIERS.get(match.group(2).lower(), 1.0)
        return raw * mult

    def _compute_confidence(
        self,
        category: str,
        tickers: list[str],
        dollar_value: float | None,
        text: str,
    ) -> float:
        """Compute detection confidence."""
        conf = 0.4

        # Ticker specificity
        if len(tickers) >= 1:
            conf += 0.15
        if len(tickers) >= 2:
            conf += 0.05

        # Dollar value = strong evidence
        if dollar_value is not None:
            conf += 0.15

        # Text length (more detail = more confident)
        if len(text) > 300:
            conf += 0.1

        # Some categories have higher base confidence
        high_confidence_categories = {
            "earnings_surprise", "guidance_change", "credit_event",
        }
        if category in high_confidence_categories:
            conf += 0.1

        return min(1.0, round(conf, 3))

    def _store_events(self, events: list[BusinessEvent]) -> int:
        """Persist events to the database.

        Args:
            events: List of BusinessEvent objects.

        Returns:
            Number of events stored.
        """
        stored = 0
        with self.engine.begin() as conn:
            for event in events:
                try:
                    conn.execute(
                        text("""
                            INSERT INTO business_events
                            (event_id, category, tickers, headline, description,
                             source, direction, estimated_bps, horizon,
                             dollar_value, confidence, published_at, article_url,
                             metadata)
                            VALUES
                            (:eid, :cat, :tickers, :headline, :desc,
                             :src, :dir, :bps, :horizon,
                             :dval, :conf, :pub, :url, :meta)
                            ON CONFLICT (event_id) DO NOTHING
                        """),
                        {
                            "eid": event.event_id,
                            "cat": event.category,
                            "tickers": event.tickers,
                            "headline": event.headline,
                            "desc": event.description,
                            "src": event.source,
                            "dir": event.direction,
                            "bps": event.estimated_bps,
                            "horizon": event.horizon,
                            "dval": event.dollar_value,
                            "conf": event.confidence,
                            "pub": event.published_at,
                            "url": event.article_url,
                            "meta": json.dumps(event.metadata, default=str),
                        },
                    )
                    stored += 1
                except Exception as exc:
                    log.debug("Failed to store event {e}: {err}", e=event.event_id, err=str(exc))

        return stored

    def scan_recent_news(self, hours: int = 12) -> dict[str, Any]:
        """Scan recent news for business events.

        Args:
            hours: Hours to look back.

        Returns:
            Summary dict with event counts by category.
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
            log.warning("Business news scan query failed: {e}", e=str(exc))
            return {"events_detected": 0, "error": str(exc)}

        all_events: list[BusinessEvent] = []

        for row in rows:
            events = self.parse_article(
                title=row[0] or "",
                summary=row[1] or "",
                tickers=row[2] or [],
                source=row[3] or "",
                pub_date=row[4],
                url=row[5] or "",
            )
            all_events.extend(events)

        # Also scan trending items
        trending_events = self._scan_trending(cutoff)
        all_events.extend(trending_events)

        stored = self._store_events(all_events)

        # Build category summary
        by_category: dict[str, int] = {}
        by_direction: dict[str, int] = {}
        for e in all_events:
            by_category[e.category] = by_category.get(e.category, 0) + 1
            by_direction[e.direction] = by_direction.get(e.direction, 0) + 1

        result = {
            "scanned_at": datetime.now(timezone.utc).isoformat(),
            "articles_scanned": len(rows),
            "events_detected": len(all_events),
            "events_stored": stored,
            "by_category": by_category,
            "by_direction": by_direction,
            "events": [e.to_dict() for e in all_events[:50]],
        }

        log.info(
            "Business news scan: {n} articles → {e} events detected, {s} stored",
            n=len(rows), e=len(all_events), s=stored,
        )

        return result

    def _scan_trending(self, cutoff: datetime) -> list[BusinessEvent]:
        """Scan trending_items for business events.

        Args:
            cutoff: Datetime cutoff.

        Returns:
            List of BusinessEvent objects from trending content.
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

        events: list[BusinessEvent] = []
        for row in rows:
            parsed = self.parse_article(
                title=row[0] or "",
                summary=row[1] or "",
                tickers=[],
                source=f"trending_{row[2]}",
                pub_date=row[3],
                url=row[4] or "",
            )
            events.extend(parsed)

        return events

    def get_recent_events(
        self,
        category: str | None = None,
        ticker: str | None = None,
        direction: str | None = None,
        hours: int = 24,
    ) -> list[dict[str, Any]]:
        """Retrieve recent business events from the database.

        Args:
            category: Optional category filter.
            ticker: Optional ticker filter.
            direction: Optional direction filter (bullish/bearish/neutral).
            hours: Hours to look back.

        Returns:
            List of event dicts.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

        conditions = ["created_at >= :cutoff"]
        params: dict[str, Any] = {"cutoff": cutoff}

        if category:
            conditions.append("category = :cat")
            params["cat"] = category
        if ticker:
            conditions.append(":ticker = ANY(tickers)")
            params["ticker"] = ticker.upper()
        if direction:
            conditions.append("direction = :dir")
            params["dir"] = direction.lower()

        where_clause = " AND ".join(conditions)

        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text(f"""
                        SELECT event_id, category, tickers, headline, description,
                               source, direction, estimated_bps, horizon,
                               dollar_value, confidence, published_at,
                               article_url, metadata, created_at
                        FROM business_events
                        WHERE {where_clause}
                        ORDER BY created_at DESC
                        LIMIT 100
                    """),
                    params,
                ).fetchall()
        except Exception as exc:
            log.warning("Recent events query failed: {e}", e=str(exc))
            return []

        return [
            {
                "event_id": r[0],
                "category": r[1],
                "tickers": r[2] or [],
                "headline": r[3],
                "description": r[4],
                "source": r[5],
                "direction": r[6],
                "estimated_bps": r[7],
                "horizon": r[8],
                "dollar_value": r[9],
                "confidence": r[10],
                "published_at": r[11].isoformat() if r[11] else None,
                "article_url": r[12],
                "metadata": r[13],
                "created_at": r[14].isoformat() if r[14] else None,
            }
            for r in rows
        ]

    def get_event_summary(self, hours: int = 24) -> dict[str, Any]:
        """Get aggregate event statistics for the dashboard.

        Args:
            hours: Hours to look back.

        Returns:
            Summary dict with counts and breakdowns.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

        try:
            with self.engine.connect() as conn:
                category_rows = conn.execute(
                    text("""
                        SELECT category, COUNT(*), AVG(confidence)
                        FROM business_events
                        WHERE created_at >= :cutoff
                        GROUP BY category
                        ORDER BY COUNT(*) DESC
                    """),
                    {"cutoff": cutoff},
                ).fetchall()

                direction_rows = conn.execute(
                    text("""
                        SELECT direction, COUNT(*)
                        FROM business_events
                        WHERE created_at >= :cutoff
                        GROUP BY direction
                    """),
                    {"cutoff": cutoff},
                ).fetchall()

                ticker_rows = conn.execute(
                    text("""
                        SELECT unnest(tickers) AS ticker, COUNT(*) AS cnt
                        FROM business_events
                        WHERE created_at >= :cutoff AND tickers IS NOT NULL
                        GROUP BY ticker
                        ORDER BY cnt DESC
                        LIMIT 20
                    """),
                    {"cutoff": cutoff},
                ).fetchall()

        except Exception as exc:
            log.warning("Event summary query failed: {e}", e=str(exc))
            return {"error": str(exc)}

        return {
            "hours": hours,
            "total_events": sum(r[1] for r in category_rows),
            "by_category": {
                r[0]: {"count": r[1], "avg_confidence": round(r[2] or 0, 3)}
                for r in category_rows
            },
            "by_direction": {r[0]: r[1] for r in direction_rows},
            "top_tickers": [
                {"ticker": r[0], "event_count": r[1]} for r in ticker_rows
            ],
        }
