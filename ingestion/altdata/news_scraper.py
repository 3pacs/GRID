"""
GRID free news scraper — RSS-based financial news ingestion with LLM sentiment.

Scrapes financial news from free RSS feeds (no API keys required), extracts
ticker mentions, scores sentiment via LLM, and stores in raw_series for the
intelligence pipeline.

Sources:
    Reuters Business, CNBC, MarketWatch, Yahoo Finance, Bloomberg Markets,
    Federal Reserve press releases, SEC EDGAR 8-K filings.

Each article produces:
    - Extracted tickers (regex $AAPL + common entity patterns)
    - LLM sentiment: BULLISH / BEARISH / NEUTRAL + confidence + one-liner
    - Series ID: NEWS:{source}:{ticker}:{sentiment}
    - Signal emission for trust scoring

Deduplication: SHA-256 hash of (title + source) prevents storing the same
story twice. Rate limiting: max 1 request per source per 10 minutes.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from dataclasses import dataclass, asdict, field
from datetime import date, datetime, timedelta, timezone
from typing import Any
from xml.etree import ElementTree

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# ── RSS Feed Registry ────────────────────────────────────────────────────

RSS_FEEDS: dict[str, dict[str, str]] = {
    "reuters": {
        "url": "https://feeds.reuters.com/reuters/businessNews",
        "label": "Reuters Business",
    },
    "cnbc": {
        "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114",
        "label": "CNBC Top News",
    },
    "marketwatch": {
        "url": "https://feeds.marketwatch.com/marketwatch/topstories",
        "label": "MarketWatch Top Stories",
    },
    "yahoo_finance": {
        "url": "https://finance.yahoo.com/news/rssindex",
        "label": "Yahoo Finance",
    },
    "bloomberg": {
        "url": "https://feeds.bloomberg.com/markets/news.rss",
        "label": "Bloomberg Markets",
    },
    "fed": {
        "url": "https://www.federalreserve.gov/feeds/press_all.xml",
        "label": "Federal Reserve Press",
    },
    "sec_edgar": {
        "url": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&dateb=&owner=include&count=40&search_text=&action=getcurrent&output=atom",
        "label": "SEC EDGAR 8-K Filings",
    },
}

# ── Ticker Detection ─────────────────────────────────────────────────────

# $AAPL-style cashtag pattern
_CASHTAG_RE = re.compile(r"\$([A-Z]{1,5})\b")

# Common tickers that appear as plain words in headlines
_KNOWN_TICKERS: set[str] = {
    "AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "META", "TSLA", "NVDA",
    "AMD", "INTC", "AVGO", "NFLX", "CRM", "ORCL", "ADBE", "CSCO",
    "JPM", "BAC", "GS", "MS", "WFC", "C", "BRK", "V", "MA",
    "UNH", "JNJ", "PFE", "ABBV", "MRK", "LLY", "TMO",
    "XOM", "CVX", "COP", "OXY", "SLB",
    "DIS", "CMCSA", "T", "VZ", "TMUS",
    "HD", "WMT", "COST", "TGT", "AMZN",
    "BA", "CAT", "GE", "HON", "RTX", "LMT",
    "SPY", "QQQ", "IWM", "DIA", "VTI", "VOO",
    "BTC", "ETH", "SOL", "XRP",
}

# Words that look like tickers but aren't
_TICKER_BLACKLIST: set[str] = {
    "CEO", "CFO", "CTO", "COO", "IPO", "GDP", "CPI", "PPI",
    "ETF", "SEC", "FED", "FDA", "DOJ", "FBI", "CIA", "NSA",
    "NYSE", "AI", "EV", "US", "UK", "EU", "IMF", "WHO",
    "THE", "FOR", "AND", "BUT", "NOT", "NEW", "OLD", "ALL",
    "ARE", "HAS", "HAD", "WAS", "CAN", "MAY", "NOW", "TOP",
    "BIG", "LOW", "HIGH", "UP", "DOWN", "OUT", "OFF", "ON",
}

# ── Rate Limiting ────────────────────────────────────────────────────────

_RATE_LIMIT_SECONDS = 600  # 10 minutes between requests to same source
_REQUEST_TIMEOUT = 30
_last_fetch: dict[str, float] = {}

# ── Sentiment Prompt ─────────────────────────────────────────────────────

_SENTIMENT_SYSTEM = (
    "You are a financial news sentiment analyst. For each headline and summary, "
    "respond with EXACTLY one line in this format:\n"
    "SENTIMENT|CONFIDENCE|ONE_LINE_SUMMARY\n\n"
    "Where SENTIMENT is BULLISH, BEARISH, or NEUTRAL.\n"
    "CONFIDENCE is a float 0.0-1.0.\n"
    "ONE_LINE_SUMMARY is a brief market-relevant takeaway.\n\n"
    "Examples:\n"
    "BULLISH|0.85|NVDA beats Q4 estimates by 22%, guidance raised\n"
    "BEARISH|0.70|Fed signals more rate hikes, 10Y yield spikes\n"
    "NEUTRAL|0.50|Mixed jobs data, market waits for CPI\n"
)


# ── Data Classes ─────────────────────────────────────────────────────────

@dataclass
class NewsArticle:
    """A scraped news article with extracted metadata."""
    title: str
    source: str
    url: str
    published: datetime | None
    summary: str
    tickers: list[str] = field(default_factory=list)
    sentiment: str = "NEUTRAL"
    confidence: float = 0.5
    llm_summary: str = ""
    dedup_hash: str = ""

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        if d["published"]:
            d["published"] = d["published"].isoformat()
        return d


# ── Main Scraper ─────────────────────────────────────────────────────────

class NewsScraperPuller(BasePuller):
    """Scrapes financial news from free RSS feeds with LLM sentiment scoring.

    No API keys required. Uses RSS/Atom feeds from Reuters, CNBC, MarketWatch,
    Yahoo Finance, Bloomberg, Federal Reserve, and SEC EDGAR.

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id.
        llm: Optional LlamaCppClient for sentiment scoring.
    """

    SOURCE_NAME: str = "NewsScraperRSS"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://feeds.reuters.com",
        "cost_tier": "FREE",
        "latency_class": "INTRADAY",
        "pit_available": False,
        "revision_behavior": "NEVER",
        "trust_score": "MED",
        "priority_rank": 30,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "GRID/4.0 (research; stepdadfinance@gmail.com)",
            "Accept": "application/rss+xml, application/xml, text/xml, */*",
        })
        self._seen_hashes: set[str] = set()
        self._llm = None
        self._ensure_news_table()
        log.info("NewsScraperPuller initialised — source_id={sid}", sid=self.source_id)

    def _get_llm(self):
        """Lazy-load LLM client."""
        if self._llm is None:
            try:
                from llm.router import get_llm, Tier
                self._llm = get_llm(Tier.LOCAL)
            except Exception:
                log.debug("LLM client not available for news sentiment")
        return self._llm

    # ── Table Setup ──────────────────────────────────────────────────────

    def _ensure_news_table(self) -> None:
        """Create news_articles table if it doesn't exist."""
        with self.engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS news_articles (
                    id SERIAL PRIMARY KEY,
                    dedup_hash TEXT UNIQUE NOT NULL,
                    title TEXT NOT NULL,
                    source TEXT NOT NULL,
                    url TEXT,
                    published_at TIMESTAMPTZ,
                    summary TEXT,
                    tickers TEXT[],
                    sentiment TEXT DEFAULT 'NEUTRAL',
                    confidence DOUBLE PRECISION DEFAULT 0.5,
                    llm_summary TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_news_published
                ON news_articles (published_at DESC)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_news_source
                ON news_articles (source)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_news_sentiment
                ON news_articles (sentiment)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_news_tickers
                ON news_articles USING GIN (tickers)
            """))

    # ── Deduplication ────────────────────────────────────────────────────

    @staticmethod
    def _hash_article(title: str, source: str) -> str:
        """SHA-256 hash of title+source for deduplication."""
        content = f"{title.strip().lower()}|{source.strip().lower()}"
        return hashlib.sha256(content.encode("utf-8")).hexdigest()[:32]

    def _is_duplicate(self, dedup_hash: str) -> bool:
        """Check if article already stored (memory cache + DB)."""
        if dedup_hash in self._seen_hashes:
            return True
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT 1 FROM news_articles WHERE dedup_hash = :h LIMIT 1"),
                {"h": dedup_hash},
            ).fetchone()
        if row is not None:
            self._seen_hashes.add(dedup_hash)
            return True
        return False

    # ── Rate Limiting ────────────────────────────────────────────────────

    @staticmethod
    def _rate_limited(source_key: str) -> bool:
        """Return True if source was fetched less than 10 minutes ago."""
        last = _last_fetch.get(source_key)
        if last is None:
            return False
        return (time.time() - last) < _RATE_LIMIT_SECONDS

    @staticmethod
    def _mark_fetched(source_key: str) -> None:
        """Record fetch timestamp for rate limiting."""
        _last_fetch[source_key] = time.time()

    # ── Ticker Extraction ────────────────────────────────────────────────

    @staticmethod
    def _extract_tickers(text_content: str) -> list[str]:
        """Extract ticker symbols from article text.

        Uses cashtag regex ($AAPL) and known-ticker word matching.
        Filters out common false positives.

        Parameters:
            text_content: Combined title + summary text.

        Returns:
            Deduplicated list of ticker symbols.
        """
        found: set[str] = set()

        # Cashtag pattern: $AAPL, $MSFT, etc.
        for match in _CASHTAG_RE.finditer(text_content):
            ticker = match.group(1)
            if ticker not in _TICKER_BLACKLIST:
                found.add(ticker)

        # Known tickers as standalone words (case-sensitive)
        for ticker in _KNOWN_TICKERS:
            if len(ticker) >= 3:  # Skip 1-2 char tickers (too many false positives)
                pattern = r"\b" + re.escape(ticker) + r"\b"
                if re.search(pattern, text_content):
                    found.add(ticker)

        return sorted(found)

    # ── RSS Parsing ──────────────────────────────────────────────────────

    @retry_on_failure(
        max_attempts=2,
        backoff=5.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError, requests.RequestException),
    )
    def _fetch_feed(self, source_key: str) -> list[NewsArticle]:
        """Fetch and parse an RSS feed into NewsArticle objects.

        Parameters:
            source_key: Key into RSS_FEEDS dict.

        Returns:
            List of parsed articles (not yet sentiment-scored).
        """
        feed_config = RSS_FEEDS.get(source_key)
        if not feed_config:
            log.warning("Unknown feed source: {s}", s=source_key)
            return []

        if self._rate_limited(source_key):
            log.debug("Rate limited: {s}, skipping", s=source_key)
            return []

        url = feed_config["url"]
        label = feed_config["label"]

        try:
            resp = self._session.get(url, timeout=_REQUEST_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as exc:
            log.warning("Failed to fetch {s}: {e}", s=label, e=str(exc))
            self._mark_fetched(source_key)
            return []

        self._mark_fetched(source_key)

        articles: list[NewsArticle] = []
        try:
            root = ElementTree.fromstring(resp.content)
        except ElementTree.ParseError as exc:
            log.warning("RSS parse error for {s}: {e}", s=label, e=str(exc))
            return []

        # Handle both RSS 2.0 (<item>) and Atom (<entry>) feeds
        namespaces = {
            "atom": "http://www.w3.org/2005/Atom",
            "dc": "http://purl.org/dc/elements/1.1/",
            "content": "http://purl.org/rss/1.0/modules/content/",
        }

        # RSS 2.0 items
        items = root.findall(".//item")
        if not items:
            # Try Atom entries
            items = root.findall(".//atom:entry", namespaces)

        for item in items:
            title = self._xml_text(item, "title", namespaces)
            if not title:
                continue

            link = self._xml_text(item, "link", namespaces)
            if not link:
                # Atom uses <link href="..."/>
                link_el = item.find("atom:link", namespaces)
                if link_el is not None:
                    link = link_el.get("href", "")

            description = (
                self._xml_text(item, "description", namespaces)
                or self._xml_text(item, "atom:summary", namespaces)
                or ""
            )
            # Strip HTML tags from description
            description = re.sub(r"<[^>]+>", "", description).strip()[:500]

            pub_date = self._parse_pub_date(item, namespaces)

            dedup_hash = self._hash_article(title, source_key)
            if self._is_duplicate(dedup_hash):
                continue

            combined_text = f"{title} {description}"
            tickers = self._extract_tickers(combined_text)

            articles.append(NewsArticle(
                title=title,
                source=source_key,
                url=link or "",
                published=pub_date,
                summary=description,
                tickers=tickers,
                dedup_hash=dedup_hash,
            ))

        log.info(
            "Fetched {n} new articles from {s} ({total} items in feed)",
            n=len(articles), s=label, total=len(items),
        )
        return articles

    @staticmethod
    def _xml_text(
        element: ElementTree.Element,
        tag: str,
        namespaces: dict[str, str],
    ) -> str | None:
        """Extract text from an XML element, trying plain and namespaced tags."""
        el = element.find(tag)
        if el is not None and el.text:
            return el.text.strip()
        # Try with atom namespace
        for prefix, uri in namespaces.items():
            el = element.find(f"{{{uri}}}{tag.split(':')[-1]}")
            if el is not None and el.text:
                return el.text.strip()
        return None

    @staticmethod
    def _parse_pub_date(
        item: ElementTree.Element,
        namespaces: dict[str, str],
    ) -> datetime | None:
        """Parse publication date from various RSS/Atom formats."""
        for tag in ("pubDate", "published", "dc:date", "updated"):
            el = item.find(tag)
            if el is None:
                for prefix, uri in namespaces.items():
                    el = item.find(f"{{{uri}}}{tag.split(':')[-1]}")
                    if el is not None:
                        break
            if el is not None and el.text:
                raw = el.text.strip()
                # RFC 822: "Mon, 25 Mar 2026 14:30:00 GMT"
                for fmt in (
                    "%a, %d %b %Y %H:%M:%S %z",
                    "%a, %d %b %Y %H:%M:%S %Z",
                    "%Y-%m-%dT%H:%M:%S%z",
                    "%Y-%m-%dT%H:%M:%SZ",
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%d",
                ):
                    try:
                        dt = datetime.strptime(raw, fmt)
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        return dt
                    except ValueError:
                        continue
                # Last resort: ISO parse
                try:
                    return datetime.fromisoformat(raw.replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    pass
        return None

    # ── LLM Sentiment Scoring ────────────────────────────────────────────

    def _score_sentiment_batch(self, articles: list[NewsArticle]) -> None:
        """Score sentiment for a batch of articles via LLM.

        Mutates articles in place, setting sentiment, confidence, llm_summary.
        Falls back to keyword heuristic if LLM is unavailable.

        Parameters:
            articles: List of articles to score.
        """
        llm = self._get_llm()

        if llm is None or not llm.is_available:
            # Fallback: keyword heuristic
            for art in articles:
                art.sentiment, art.confidence = self._keyword_sentiment(
                    f"{art.title} {art.summary}"
                )
                art.llm_summary = art.title[:100]
            return

        # Score in small batches to fit context window
        batch_size = 10
        for i in range(0, len(articles), batch_size):
            batch = articles[i:i + batch_size]
            prompt_lines = []
            for idx, art in enumerate(batch, 1):
                prompt_lines.append(
                    f"{idx}. [{art.source}] {art.title}\n   {art.summary[:200]}"
                )

            prompt = (
                "Score the financial sentiment of each headline below. "
                "One line per headline in format: SENTIMENT|CONFIDENCE|SUMMARY\n\n"
                + "\n".join(prompt_lines)
            )

            try:
                response = llm.generate(
                    prompt=prompt,
                    system=_SENTIMENT_SYSTEM,
                    temperature=0.1,
                    num_predict=500,
                )
                if response:
                    self._parse_sentiment_response(response, batch)
                else:
                    for art in batch:
                        art.sentiment, art.confidence = self._keyword_sentiment(
                            f"{art.title} {art.summary}"
                        )
                        art.llm_summary = art.title[:100]
            except Exception as exc:
                log.debug("LLM sentiment scoring failed: {e}", e=str(exc))
                for art in batch:
                    art.sentiment, art.confidence = self._keyword_sentiment(
                        f"{art.title} {art.summary}"
                    )
                    art.llm_summary = art.title[:100]

    @staticmethod
    def _parse_sentiment_response(
        response: str,
        articles: list[NewsArticle],
    ) -> None:
        """Parse LLM sentiment response and apply to articles."""
        lines = [
            ln.strip() for ln in response.strip().split("\n")
            if "|" in ln and ln.strip()
        ]

        for idx, art in enumerate(articles):
            if idx < len(lines):
                parts = lines[idx].split("|", 2)
                # Strip leading number prefix like "1. " if present
                if parts:
                    parts[0] = re.sub(r"^\d+\.\s*", "", parts[0]).strip()
                if len(parts) >= 3:
                    sentiment = parts[0].strip().upper()
                    if sentiment in ("BULLISH", "BEARISH", "NEUTRAL"):
                        art.sentiment = sentiment
                    try:
                        art.confidence = max(0.0, min(1.0, float(parts[1].strip())))
                    except (ValueError, TypeError):
                        art.confidence = 0.5
                    art.llm_summary = parts[2].strip()[:200]
                elif len(parts) == 2:
                    sentiment = parts[0].strip().upper()
                    if sentiment in ("BULLISH", "BEARISH", "NEUTRAL"):
                        art.sentiment = sentiment
                    art.llm_summary = parts[1].strip()[:200]

    @staticmethod
    def _keyword_sentiment(text_content: str) -> tuple[str, float]:
        """Fallback keyword-based sentiment when LLM is unavailable.

        Returns:
            Tuple of (sentiment_label, confidence).
        """
        text_lower = text_content.lower()

        bullish_words = {
            "surge", "rally", "boom", "growth", "recovery", "gain", "beat",
            "record high", "expansion", "upgrade", "outperform", "strong",
            "soar", "bullish", "breakthrough", "approval", "stimulus",
            "buyback", "dividend", "profit",
        }
        bearish_words = {
            "crash", "crisis", "collapse", "fear", "panic", "recession",
            "default", "plunge", "slump", "disaster", "fail", "bankrupt",
            "layoff", "shutdown", "sanctions", "downgrade", "miss", "weak",
            "bearish", "investigation", "subpoena", "fraud",
        }

        bull = sum(1 for w in bullish_words if w in text_lower)
        bear = sum(1 for w in bearish_words if w in text_lower)
        total = bull + bear

        if total == 0:
            return "NEUTRAL", 0.5
        ratio = bull / total
        if ratio > 0.6:
            return "BULLISH", min(0.9, 0.5 + ratio * 0.4)
        elif ratio < 0.4:
            return "BEARISH", min(0.9, 0.5 + (1 - ratio) * 0.4)
        return "NEUTRAL", 0.5

    # ── Storage ──────────────────────────────────────────────────────────

    def _store_articles(self, articles: list[NewsArticle]) -> int:
        """Store scored articles in news_articles table and raw_series.

        Parameters:
            articles: Scored articles to store.

        Returns:
            Number of rows inserted.
        """
        if not articles:
            return 0

        inserted = 0
        today = date.today()

        with self.engine.begin() as conn:
            for art in articles:
                # Store in news_articles table
                try:
                    conn.execute(
                        text(
                            "INSERT INTO news_articles "
                            "(dedup_hash, title, source, url, published_at, summary, "
                            "tickers, sentiment, confidence, llm_summary) "
                            "VALUES (:hash, :title, :src, :url, :pub, :summary, "
                            ":tickers, :sentiment, :conf, :llm_sum) "
                            "ON CONFLICT (dedup_hash) DO NOTHING"
                        ),
                        {
                            "hash": art.dedup_hash,
                            "title": art.title,
                            "src": art.source,
                            "url": art.url,
                            "pub": art.published,
                            "summary": art.summary,
                            "tickers": art.tickers,
                            "sentiment": art.sentiment,
                            "conf": art.confidence,
                            "llm_sum": art.llm_summary,
                        },
                    )
                    inserted += 1
                    self._seen_hashes.add(art.dedup_hash)
                except Exception as exc:
                    log.debug("Failed to store article: {e}", e=str(exc))
                    continue

                # Store in raw_series per ticker
                sentiment_value = {
                    "BULLISH": 1.0,
                    "NEUTRAL": 0.0,
                    "BEARISH": -1.0,
                }.get(art.sentiment, 0.0)

                tickers_to_store = art.tickers if art.tickers else ["MARKET"]
                for ticker in tickers_to_store:
                    series_id = f"NEWS:{art.source}:{ticker}:{art.sentiment}"
                    try:
                        if not self._row_exists(series_id, today, conn):
                            self._insert_raw(
                                conn=conn,
                                series_id=series_id,
                                obs_date=today,
                                value=sentiment_value * art.confidence,
                                raw_payload={
                                    "title": art.title,
                                    "url": art.url,
                                    "sentiment": art.sentiment,
                                    "confidence": art.confidence,
                                    "llm_summary": art.llm_summary,
                                    "tickers": art.tickers,
                                },
                            )
                    except Exception as exc:
                        log.debug(
                            "Failed to store raw_series for {sid}: {e}",
                            sid=series_id, e=str(exc),
                        )

                # Emit to signal_sources for trust scoring
                self._emit_signal(conn, art, today)

        return inserted

    def _emit_signal(
        self,
        conn: Any,
        article: NewsArticle,
        obs_date: date,
    ) -> None:
        """Emit news sentiment as a signal for trust scoring.

        Parameters:
            conn: Active DB connection.
            article: The scored article.
            obs_date: Observation date.
        """
        if not article.tickers:
            return

        for ticker in article.tickers[:3]:  # Limit signals per article
            try:
                conn.execute(
                    text(
                        "INSERT INTO signal_sources "
                        "(source_type, source_name, ticker, direction, confidence, "
                        "signal_date, metadata) "
                        "VALUES ('news', :name, :ticker, :direction, :conf, "
                        ":sig_date, :meta) "
                        "ON CONFLICT DO NOTHING"
                    ),
                    {
                        "name": f"news_{article.source}",
                        "ticker": ticker,
                        "direction": article.sentiment.lower(),
                        "conf": article.confidence,
                        "sig_date": obs_date,
                        "meta": json.dumps({
                            "title": article.title[:200],
                            "url": article.url,
                            "llm_summary": article.llm_summary[:200],
                        }),
                    },
                )
            except Exception:
                # signal_sources table may not exist yet — not critical
                pass

    # ── Feature Registration ─────────────────────────────────────────────

    def _ensure_features(self) -> None:
        """Register news sentiment features in feature_registry."""
        features = [
            ("news_volume_daily", "sentiment", "News: total articles scraped per day"),
            ("news_bullish_ratio", "sentiment", "News: bullish / total articles ratio"),
            ("news_bearish_ratio", "sentiment", "News: bearish / total articles ratio"),
            ("news_avg_confidence", "sentiment", "News: average LLM sentiment confidence"),
        ]
        with self.engine.begin() as conn:
            for name, family, desc in features:
                conn.execute(
                    text(
                        "INSERT INTO feature_registry "
                        "(name, family, description, transformation, transformation_version, "
                        "lag_days, normalization, missing_data_policy, eligible_from_date, model_eligible) "
                        "VALUES (:name, :family, :desc, 'RAW', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2024-01-01', TRUE) "
                        "ON CONFLICT (name) DO NOTHING"
                    ),
                    {"name": name, "family": family, "desc": desc},
                )

    # ── Aggregate Feature Storage ────────────────────────────────────────

    def _store_daily_aggregates(
        self,
        articles: list[NewsArticle],
        target_date: date,
    ) -> None:
        """Compute and store daily aggregate features from scraped articles.

        Parameters:
            articles: All articles scraped in this pull.
            target_date: Observation date.
        """
        if not articles:
            return

        total = len(articles)
        bullish = sum(1 for a in articles if a.sentiment == "BULLISH")
        bearish = sum(1 for a in articles if a.sentiment == "BEARISH")
        avg_conf = sum(a.confidence for a in articles) / total

        with self.engine.begin() as conn:
            for series_id, value in [
                ("news_volume_daily", float(total)),
                ("news_bullish_ratio", round(bullish / total, 4) if total else 0.0),
                ("news_bearish_ratio", round(bearish / total, 4) if total else 0.0),
                ("news_avg_confidence", round(avg_conf, 4)),
            ]:
                if not self._row_exists(series_id, target_date, conn):
                    self._insert_raw(
                        conn=conn,
                        series_id=series_id,
                        obs_date=target_date,
                        value=value,
                    )

    # ── Public Pull Methods ──────────────────────────────────────────────

    def pull_source(self, source_key: str) -> list[NewsArticle]:
        """Pull and score articles from a single RSS source.

        Parameters:
            source_key: Key into RSS_FEEDS dict.

        Returns:
            List of scored articles.
        """
        articles = self._fetch_feed(source_key)
        if articles:
            self._score_sentiment_batch(articles)
        return articles

    def pull_all(self) -> dict[str, Any]:
        """Pull all RSS feeds, score sentiment, store results.

        Returns:
            Summary dict with per-source article counts and totals.
        """
        self._ensure_features()
        today = date.today()

        all_articles: list[NewsArticle] = []
        source_counts: dict[str, int] = {}

        for source_key in RSS_FEEDS:
            try:
                articles = self.pull_source(source_key)
                stored = self._store_articles(articles)
                source_counts[source_key] = stored
                all_articles.extend(articles)
                log.info(
                    "News {src}: {n} articles scored, {s} stored",
                    src=source_key, n=len(articles), s=stored,
                )
            except Exception as exc:
                log.warning("News pull failed for {src}: {e}", src=source_key, e=str(exc))
                source_counts[source_key] = 0

            time.sleep(2)  # Brief pause between sources

        # Store daily aggregates
        self._store_daily_aggregates(all_articles, today)

        total = len(all_articles)
        bullish = sum(1 for a in all_articles if a.sentiment == "BULLISH")
        bearish = sum(1 for a in all_articles if a.sentiment == "BEARISH")
        neutral = total - bullish - bearish

        summary = {
            "date": today.isoformat(),
            "total_articles": total,
            "sentiment_breakdown": {
                "bullish": bullish,
                "bearish": bearish,
                "neutral": neutral,
            },
            "source_counts": source_counts,
            "tickers_mentioned": sorted({
                t for a in all_articles for t in a.tickers
            }),
        }

        log.info(
            "News scraper complete — {n} articles: {b} bullish, {br} bearish, {ne} neutral",
            n=total, b=bullish, br=bearish, ne=neutral,
        )
        return summary

    def get_recent(
        self,
        ticker: str | None = None,
        hours: int = 24,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Retrieve recent news articles from the database.

        Parameters:
            ticker: Optional ticker filter.
            hours: Hours to look back.
            limit: Max articles to return.

        Returns:
            List of article dicts.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

        if ticker:
            query = text(
                "SELECT title, source, url, published_at, summary, tickers, "
                "sentiment, confidence, llm_summary "
                "FROM news_articles "
                "WHERE :ticker = ANY(tickers) AND created_at >= :cutoff "
                "ORDER BY published_at DESC NULLS LAST "
                "LIMIT :lim"
            )
            params = {"ticker": ticker.upper(), "cutoff": cutoff, "lim": limit}
        else:
            query = text(
                "SELECT title, source, url, published_at, summary, tickers, "
                "sentiment, confidence, llm_summary "
                "FROM news_articles "
                "WHERE created_at >= :cutoff "
                "ORDER BY published_at DESC NULLS LAST "
                "LIMIT :lim"
            )
            params = {"cutoff": cutoff, "lim": limit}

        with self.engine.connect() as conn:
            rows = conn.execute(query, params).fetchall()

        return [
            {
                "title": r[0],
                "source": r[1],
                "url": r[2],
                "published": r[3].isoformat() if r[3] else None,
                "summary": r[4],
                "tickers": r[5] or [],
                "sentiment": r[6],
                "confidence": r[7],
                "llm_summary": r[8],
            }
            for r in rows
        ]
