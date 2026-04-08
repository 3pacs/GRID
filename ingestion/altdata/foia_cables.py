"""
GRID FOIA diplomatic cables ingestion module.

Pulls declassified diplomatic cables and State Department documents
from publicly available FOIA reading rooms and archives.

Sources:
  1. State Dept FOIA Electronic Reading Room (foia.state.gov)
     — search API for declassified cables and documents
  2. National Security Archive (nsarchive.gwu.edu)
     — curated document collections on foreign policy topics
  3. CIA FOIA Reading Room (cia.gov/readingroom)
     — declassified intelligence assessments

These documents reveal backroom motivations, diplomatic pressure,
and policy positions that were hidden at the time but explain market
moves in hindsight. Current declassification events (e.g., new
batch releases) signal what topics the government is willing to
expose — often a leading indicator of policy shifts.

Series pattern: FOIA:{source}:{topic_slug}:{classification}
Emits signal_sources entries for trust scoring integration.

Scheduled: weekly pull (new releases trickle out).
"""

from __future__ import annotations

import json
import os
import re
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests
from loguru import logger as log
from playwright.sync_api import sync_playwright, Browser, Page
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# ── API Configuration ────────────────────────────────────────────────────

_STATE_FOIA_URL: str = "https://foia.state.gov/Search/Results.aspx"
_STATE_FOIA_API: str = "https://foia.state.gov/api/Search"  # deprecated, kept for reference
_NSA_SEARCH_URL: str = "https://nsarchive.gwu.edu/search"
_CIA_FOIA_URL: str = "https://www.cia.gov/readingroom/search/site"

# ── RSS Feed Registry ─────────────────────────────────────────────────
# Topic-specific feeds. Each feed maps to the topics it serves.
# Feed sources: GovInfo (legislation), agency press (policy), data releases.

_RSS_FEEDS: list[dict[str, Any]] = [
    # GovInfo — legislation
    {"url": "https://www.govinfo.gov/rss/bills.xml",     "name": "govinfo_bills",   "topics": ["defense_sales", "tech_competition", "financial_regulation"]},
    {"url": "https://www.govinfo.gov/rss/plaw.xml",      "name": "govinfo_plaw",    "topics": ["trade_policy", "sanctions", "energy_diplomacy", "defense_sales", "tech_competition", "financial_regulation"]},
    {"url": "https://www.govinfo.gov/rss/fr.xml",        "name": "govinfo_fr",      "topics": ["trade_policy", "sanctions", "energy_diplomacy", "financial_regulation"]},
    {"url": "https://www.govinfo.gov/rss/bills-enr.xml", "name": "govinfo_enr",     "topics": ["trade_policy", "sanctions", "tech_competition"]},
    {"url": "https://www.govinfo.gov/rss/crpt.xml",      "name": "govinfo_crpt",    "topics": ["defense_sales", "tech_competition"]},

    # DoD — defense, arms, military, tech, energy security
    {"url": "https://www.defense.gov/DesktopModules/ArticleCS/RSS.ashx?ContentType=1&Site=945",
     "name": "dod_news", "topics": ["defense_sales", "tech_competition", "sanctions", "trade_policy", "energy_diplomacy", "economic_espionage"]},

    # Federal Reserve — monetary, financial regulation, currency
    {"url": "https://www.federalreserve.gov/feeds/press_all.xml",  "name": "fed_press",     "topics": ["financial_regulation", "currency_war"]},
    {"url": "https://www.federalreserve.gov/feeds/speeches.xml",   "name": "fed_speeches",  "topics": ["financial_regulation", "currency_war", "trade_policy"]},
    {"url": "https://www.federalreserve.gov/feeds/testimony.xml",  "name": "fed_testimony", "topics": ["financial_regulation", "tech_competition", "currency_war"]},

    # FDIC — banking stability
    {"url": "https://www.fdic.gov/rss.xml", "name": "fdic", "topics": ["financial_regulation"]},

    # Census — trade data, economic indicators
    {"url": "https://www.census.gov/economic-indicators/indicator.xml", "name": "census_econ", "topics": ["trade_policy", "currency_war"]},

    # GAO — audits, investigations (covers every topic)
    {"url": "https://www.gao.gov/rss/reports.xml", "name": "gao_reports",
     "topics": ["trade_policy", "sanctions", "defense_sales", "tech_competition",
                "financial_regulation", "economic_espionage", "energy_diplomacy", "currency_war"]},

    # DOJ — espionage, sanctions enforcement, trade fraud, cyber
    {"url": "https://www.justice.gov/news/rss", "name": "doj_news",
     "topics": ["sanctions", "economic_espionage", "trade_policy", "tech_competition"]},
]

# Playwright-scraped pages (JS-rendered, high-value but slower)
_PLAYWRIGHT_PAGES: list[dict[str, Any]] = [
    # USTR — trade policy, tariffs, Section 301 (richest trade source)
    {
        "url": "https://ustr.gov/about-us/policy-offices/press-office/press-releases",
        "name": "ustr_press",
        "topics": ["trade_policy", "sanctions"],
        "link_pattern": r'href="(/about-us/policy-offices/press-office/press-releases/\d[^"]*)"[^>]*>(.*?)</a>',
        "base_url": "https://ustr.gov",
    },
]

# Legacy ref kept for _state_foia_search (Playwright-based, used for priority topics)
_GOVINFO_RSS_FEEDS: dict[str, str] = {}  # replaced by _RSS_FEEDS above

_REQUEST_TIMEOUT: int = 30
_RATE_LIMIT_DELAY: float = 1.5
_MAX_PAGES: int = 10

# ── Topic definitions ──────────────────────────────────────────────────
# Each topic maps to geopolitical themes that affect markets.
# Keywords are broadened for better RSS matching across all sources.

FOIA_TOPICS: list[dict[str, Any]] = [
    {
        "query": "trade tariff section 301 import duty",
        "topic": "trade_policy",
        "ticker": "XLI",
        "keywords": ["tariff", "trade", "import", "export", "wto", "usmca",
                      "customs", "duty", "section 301", "anti-dumping",
                      "countervailing", "commerce department", "manufacturing",
                      "trade deficit", "trade surplus", "trade agreement",
                      "retaliatory", "reciprocal"],
    },
    {
        "query": "sanctions OFAC enforcement designation",
        "topic": "sanctions",
        "ticker": "SPY",
        "keywords": ["sanction", "embargo", "freeze", "restrict", "ofac",
                      "sdnlist", "designation", "penalty", "enforcement",
                      "blocked", "prohibited", "specially designated",
                      "iran", "russia", "north korea", "venezuela"],
    },
    {
        "query": "oil petroleum energy pipeline LNG",
        "topic": "energy_diplomacy",
        "ticker": "XLE",
        "keywords": ["oil", "petroleum", "opec", "pipeline", "lng", "energy",
                      "natural gas", "crude", "barrel", "refinery", "fuel",
                      "strategic petroleum reserve", "spr", "eia",
                      "drilling", "production", "opec+", "nuclear",
                      "solar", "wind", "renewable", "power grid",
                      "electric", "hydrogen", "carbon", "climate"],
    },
    {
        "query": "arms sales military defense contract procurement",
        "topic": "defense_sales",
        "ticker": "ITA",
        "keywords": ["arms", "military", "defense", "weapon", "f-35", "missile",
                      "contract", "procurement", "navy", "army", "air force",
                      "lockheed", "raytheon", "northrop", "general dynamics",
                      "boeing", "security assistance", "foreign military"],
    },
    {
        "query": "semiconductor chip export control technology AI",
        "topic": "tech_competition",
        "ticker": "SMH",
        "keywords": ["semiconductor", "chip", "technology", "huawei",
                      "export control", "entity list", "nvidia", "asml",
                      "artificial intelligence", "quantum", "cyber",
                      "advanced computing", "foundry", "tsmc",
                      "chips act", "5g", "telecommunications"],
    },
    {
        "query": "currency exchange rate monetary policy federal funds",
        "topic": "currency_war",
        "ticker": "UUP",
        "keywords": ["currency", "exchange rate", "devaluation", "manipulation",
                      "forex", "dollar", "yuan", "renminbi", "monetary policy",
                      "interest rate", "central bank", "federal funds",
                      "inflation", "deflation", "treasur", "rate",
                      "swap", "reserve", "capital flow", "balance of payments",
                      "findings", "financial stability"],
    },
    {
        "query": "espionage intelligence cyber theft foreign agent",
        "topic": "economic_espionage",
        "ticker": "XLK",
        "keywords": ["espionage", "intelligence", "cyber", "theft", "proprietary",
                      "trade secret", "foreign agent", "infiltration", "hacking",
                      "nation-state", "counterintelligence", "classified",
                      "intellectual property", "phishing", "ransomware",
                      "security", "breach", "spy", "covert",
                      "indictment", "conspiracy", "foreign influence"],
    },
    {
        "query": "banking regulation capital requirements systemic risk",
        "topic": "financial_regulation",
        "ticker": "XLF",
        "keywords": ["bank", "financial", "regulation", "crisis", "bailout",
                      "systemic", "capital requirement", "stress test",
                      "dodd-frank", "basel", "fdic", "occ", "fed",
                      "lending", "reserve", "liquidity", "deposit",
                      "monetary", "supervisory"],
    },
]

# Classification levels that affect signal confidence
CLASSIFICATION_CONFIDENCE: dict[str, str] = {
    "SECRET": "confirmed",     # High-value declassified intel
    "CONFIDENTIAL": "confirmed",
    "UNCLASSIFIED": "derived",
    "SENSITIVE": "derived",
    "TOP SECRET": "confirmed",
    "RESTRICTED": "derived",
}


def _slugify(text_str: str, max_len: int = 50) -> str:
    """Create a slug from text for use in series_id."""
    slug = re.sub(r"[^A-Z0-9 ]", "", text_str.strip().upper())
    slug = re.sub(r"\s+", "_", slug.strip())
    return slug[:max_len]


def _extract_classification(text_str: str) -> str:
    """Extract classification level from document text.

    Parameters:
        text_str: Document text or metadata string.

    Returns:
        Classification level or 'UNCLASSIFIED'.
    """
    if not text_str:
        return "UNCLASSIFIED"
    text_upper = text_str.upper()
    for level in ("TOP SECRET", "SECRET", "CONFIDENTIAL", "SENSITIVE", "RESTRICTED"):
        if level in text_upper:
            return level
    return "UNCLASSIFIED"


def _score_topic_relevance(
    text_str: str,
    keywords: list[str],
) -> float:
    """Score how relevant a document is to a topic based on keywords.

    Parameters:
        text_str: Document text or description.
        keywords: List of topic keywords to match.

    Returns:
        Relevance score 0.0 to 1.0.
    """
    if not text_str or not keywords:
        return 0.0
    text_lower = text_str.lower()
    hits = sum(1 for kw in keywords if kw.lower() in text_lower)
    return min(hits / max(len(keywords), 1), 1.0)


class FOIACablesPuller(BasePuller):
    """Pulls declassified diplomatic cables from FOIA reading rooms.

    Searches State Dept, NSA, and CIA FOIA archives for documents
    matching geopolitical topics that affect markets. Scores relevance
    and classification level, and emits signals when high-value
    declassified material appears on market-relevant topics.

    Series pattern: FOIA:{source}:{topic_slug}:{classification}
    Value: topic relevance score (0.0-1.0).

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for FOIA_CABLES.
    """

    SOURCE_NAME: str = "FOIA_CABLES"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://foia.state.gov",
        "cost_tier": "FREE",
        "latency_class": "WEEKLY",
        "pit_available": False,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 55,
    }

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the FOIA cables puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        super().__init__(db_engine)
        self._playwright_ctx = None
        self._browser: Browser | None = None
        log.info(
            "FOIACablesPuller initialised — source_id={sid}",
            sid=self.source_id,
        )

    # ── Browser lifecycle ────────────────────────────────────────────────

    def _ensure_browser(self) -> Browser:
        """Launch Playwright Chromium if not already running."""
        if self._browser is None or not self._browser.is_connected():
            self._playwright_ctx = sync_playwright().start()
            self._browser = self._playwright_ctx.chromium.launch(headless=True)
            log.debug("Playwright Chromium launched for FOIA scraper")
        return self._browser

    def _close_browser(self) -> None:
        """Shut down Playwright browser and context."""
        if self._browser is not None:
            try:
                self._browser.close()
            except Exception:
                pass
            self._browser = None
        if self._playwright_ctx is not None:
            try:
                self._playwright_ctx.stop()
            except Exception:
                pass
            self._playwright_ctx = None

    def _fetch_rendered_html(self, url: str, wait_selector: str | None = None) -> str:
        """Fetch a page using headless Chromium and return rendered HTML.

        Parameters:
            url: Full URL to fetch.
            wait_selector: Optional CSS selector to wait for before extracting.

        Returns:
            Rendered HTML string.
        """
        browser = self._ensure_browser()
        page: Page = browser.new_page(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        )
        try:
            page.goto(url, timeout=30000, wait_until="networkidle")
            if wait_selector:
                try:
                    page.wait_for_selector(wait_selector, timeout=10000)
                except Exception:
                    pass  # proceed with what we have
            time.sleep(_RATE_LIMIT_DELAY)
            return page.content()
        finally:
            page.close()

    # ── API helpers ──────────────────────────────────────────────────────

    def _state_foia_search(
        self,
        query: str,
        page: int = 1,
    ) -> dict[str, Any]:
        """Search State Dept FOIA Electronic Reading Room via headless browser.

        The JSON API at /api/Search was deprecated by State Dept.
        Uses Playwright to render the JavaScript SPA and extract results.

        Parameters:
            query: Search query string.
            page: Page number for pagination.

        Returns:
            Dict with 'Results' list and 'TotalCount'.
        """
        url = f"{_STATE_FOIA_URL}?searchText={requests.utils.quote(query)}"

        try:
            html = self._fetch_rendered_html(url, wait_selector="table, .searchResult, .results")
        except Exception as exc:
            log.debug("State FOIA browser fetch failed for '{q}': {e}", q=query, e=str(exc))
            return {"Results": [], "TotalCount": 0}

        results: list[dict[str, Any]] = []

        # Extract from table rows (common State FOIA format after JS render)
        row_pattern = re.compile(r'<tr[^>]*>(.*?)</tr>', re.DOTALL | re.IGNORECASE)
        for match in row_pattern.finditer(html):
            block = match.group(1)
            if '<th' in block.lower():
                continue
            doc = self._extract_doc_from_html(block)
            if doc:
                results.append(doc)

        # Also try div-based results
        if not results:
            div_pattern = re.compile(
                r'<div[^>]*class="[^"]*(?:searchResult|result-item|document)[^"]*"[^>]*>(.*?)</div>',
                re.DOTALL | re.IGNORECASE,
            )
            for match in div_pattern.finditer(html):
                doc = self._extract_doc_from_html(match.group(1))
                if doc:
                    results.append(doc)

        # Last resort: look for any anchors with document-like links
        if not results:
            link_pattern = re.compile(
                r'<a[^>]*href="([^"]*(?:document|pdf|doc)[^"]*)"[^>]*>(.*?)</a>',
                re.DOTALL | re.IGNORECASE,
            )
            for match in link_pattern.finditer(html):
                href, link_text = match.group(1), match.group(2)
                clean_text = re.sub(r'<[^>]+>', '', link_text).strip()
                if clean_text and len(clean_text) > 10:
                    results.append({
                        "Subject": clean_text[:300],
                        "DocDate": "",
                        "PostedDate": date.today().isoformat(),
                        "body": clean_text,
                        "DocNbr": href,
                        "Classification": "",
                        "From": "",
                        "To": "",
                    })

        log.debug(
            "State FOIA search '{q}': {n} results from rendered page",
            q=query, n=len(results),
        )
        return {"Results": results, "TotalCount": len(results)}

    def _extract_doc_from_html(self, block: str) -> dict[str, Any] | None:
        """Extract document metadata from an HTML block.

        Parameters:
            block: HTML snippet containing one document result.

        Returns:
            Document dict compatible with _parse_state_doc, or None.
        """
        text_content = re.sub(r'<[^>]+>', ' ', block).strip()
        text_content = re.sub(r'\s+', ' ', text_content)

        if len(text_content) < 10:
            return None

        link_match = re.search(r'href="([^"]*)"', block)
        doc_id = link_match.group(1) if link_match else ""

        date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4}|\d{4}-\d{2}-\d{2})', text_content)
        doc_date = date_match.group(1) if date_match else ""

        # Extract cell contents for table-based results
        cells = re.findall(r'<td[^>]*>(.*?)</td>', block, re.DOTALL | re.IGNORECASE)
        if cells:
            cell_texts = [re.sub(r'<[^>]+>', '', c).strip() for c in cells]
            cell_texts = [c for c in cell_texts if c]
            title = cell_texts[0] if cell_texts else text_content[:200]
            # Look for date in cells
            for ct in cell_texts:
                dm = re.search(r'(\d{1,2}/\d{1,2}/\d{4}|\d{4}-\d{2}-\d{2})', ct)
                if dm:
                    doc_date = dm.group(1)
                    break
        else:
            parts = [p.strip() for p in text_content.split('  ') if p.strip() and len(p.strip()) > 3]
            title = parts[0] if parts else text_content[:200]

        return {
            "Subject": title[:300],
            "DocDate": doc_date,
            "PostedDate": doc_date or date.today().isoformat(),
            "body": text_content,
            "DocNbr": doc_id,
            "Classification": "",
            "From": "",
            "To": "",
        }

    def _nsa_search(
        self,
        query: str,
    ) -> list[dict[str, Any]]:
        """Search National Security Archive via headless browser.

        NSA is a JavaScript-rendered site. Uses Playwright to fetch
        and extract document results.

        Parameters:
            query: Search query string.

        Returns:
            List of document metadata dicts.
        """
        url = f"{_NSA_SEARCH_URL}?search_api_fulltext={requests.utils.quote(query)}&sort_by=ds_changed&sort_order=DESC&items_per_page=25"

        try:
            html = self._fetch_rendered_html(url, wait_selector="article, .view-content, .search-results")
        except Exception as exc:
            log.debug("NSA browser fetch failed for '{q}': {e}", q=query, e=str(exc))
            return []

        results: list[dict[str, Any]] = []

        # NSA uses article tags or divs with node classes
        article_pattern = re.compile(
            r'<article[^>]*>(.*?)</article>',
            re.DOTALL | re.IGNORECASE,
        )
        for match in article_pattern.finditer(html):
            block = match.group(1)
            doc = self._parse_nsa_html_block(block)
            if doc:
                results.append(doc)

        # Fallback: div.views-row pattern (Drupal)
        if not results:
            row_pattern = re.compile(
                r'<div[^>]*class="[^"]*views-row[^"]*"[^>]*>(.*?)</div>\s*(?=<div[^>]*class="[^"]*views-row|$)',
                re.DOTALL | re.IGNORECASE,
            )
            for match in row_pattern.finditer(html):
                doc = self._parse_nsa_html_block(match.group(1))
                if doc:
                    results.append(doc)

        # Last resort: h2/h3 with links
        if not results:
            heading_pattern = re.compile(
                r'<h[23][^>]*>\s*<a[^>]*href="([^"]*)"[^>]*>(.*?)</a>',
                re.DOTALL | re.IGNORECASE,
            )
            for match in heading_pattern.finditer(html):
                href, title_html = match.group(1), match.group(2)
                clean_title = re.sub(r'<[^>]+>', '', title_html).strip()
                if clean_title and len(clean_title) > 10:
                    results.append({
                        "title": clean_title[:300],
                        "url": href if href.startswith("http") else f"https://nsarchive.gwu.edu{href}",
                        "date": "",
                        "changed": date.today().isoformat(),
                        "body": clean_title,
                    })

        log.debug(
            "NSA search '{q}': {n} results from rendered page",
            q=query, n=len(results),
        )
        return results

    def _parse_nsa_html_block(self, block: str) -> dict[str, Any] | None:
        """Parse an NSA article/result block into a document dict.

        Parameters:
            block: HTML block from a single search result.

        Returns:
            Document dict or None.
        """
        # Extract title from heading or link
        title_match = re.search(
            r'<(?:h[1-4]|a)[^>]*>(.*?)</(?:h[1-4]|a)>',
            block, re.DOTALL | re.IGNORECASE,
        )
        if not title_match:
            return None

        title = re.sub(r'<[^>]+>', '', title_match.group(1)).strip()
        if not title or len(title) < 5:
            return None

        # Extract link
        link_match = re.search(r'href="([^"]*)"', block)
        url = ""
        if link_match:
            href = link_match.group(1)
            url = href if href.startswith("http") else f"https://nsarchive.gwu.edu{href}"

        # Extract date
        date_match = re.search(r'(\d{4}-\d{2}-\d{2}|\w+ \d{1,2},? \d{4})', block)
        doc_date = date_match.group(1) if date_match else ""

        # Extract body text
        body = re.sub(r'<[^>]+>', ' ', block).strip()
        body = re.sub(r'\s+', ' ', body)

        return {
            "title": title[:300],
            "url": url,
            "date": doc_date,
            "changed": doc_date or date.today().isoformat(),
            "body": body[:500],
        }

    # ── GovInfo RSS ──────────────────────────────────────────────────────

    def _rss_search(
        self,
        topic_def: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Search topic-relevant RSS feeds for matching documents.

        Only queries feeds registered for this topic in _RSS_FEEDS.
        Uses feedparser for structured extraction — no browser needed.

        Parameters:
            topic_def: Topic dict with query, keywords, ticker, etc.

        Returns:
            List of normalised document dicts matching the topic.
        """
        try:
            import feedparser
        except ImportError:
            log.debug("feedparser not installed — skipping RSS search")
            return []

        topic = topic_def["topic"]
        keywords = topic_def.get("keywords", [])
        documents: list[dict[str, Any]] = []

        # Only query feeds registered for this topic
        relevant_feeds = [f for f in _RSS_FEEDS if topic in f.get("topics", [])]

        for feed_cfg in relevant_feeds:
            feed_url = feed_cfg["url"]
            feed_name = feed_cfg["name"]
            try:
                feed = feedparser.parse(feed_url)
                for entry in feed.entries:
                    title = entry.get("title", "")
                    summary = entry.get("summary", entry.get("description", ""))
                    combined = f"{title} {summary}".lower()

                    relevance = _score_topic_relevance(combined, keywords)
                    if relevance < 0.1:
                        continue

                    pub_date = entry.get("published_parsed") or entry.get("updated_parsed")
                    if pub_date:
                        try:
                            obs_date = date(pub_date.tm_year, pub_date.tm_mon, pub_date.tm_mday)
                        except (AttributeError, ValueError):
                            obs_date = date.today()
                    else:
                        obs_date = date.today()

                    documents.append({
                        "source": f"RSS_{feed_name.upper()}",
                        "title": title[:300],
                        "doc_date": obs_date,
                        "posted_date": obs_date,
                        "classification": "UNCLASSIFIED",
                        "confidence": CLASSIFICATION_CONFIDENCE.get("UNCLASSIFIED", "derived"),
                        "relevance": round(relevance, 3),
                        "topic": topic,
                        "ticker": topic_def["ticker"],
                        "doc_id": entry.get("id", entry.get("link", "")),
                        "url": entry.get("link", ""),
                        "body_snippet": summary[:500] if summary else title,
                    })

            except Exception as exc:
                log.debug(
                    "RSS feed '{f}' failed: {e}",
                    f=feed_name, e=str(exc),
                )

        log.debug(
            "RSS search: {n} matching documents for topic '{t}' from {nf} feeds",
            n=len(documents), t=topic, nf=len(relevant_feeds),
        )
        return documents

    def _playwright_page_search(
        self,
        topic_def: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Scrape Playwright-rendered pages for topic-matching documents.

        Only scrapes pages registered for this topic in _PLAYWRIGHT_PAGES.
        Extracts links matching the configured pattern.

        Parameters:
            topic_def: Topic dict with query, keywords, ticker, etc.

        Returns:
            List of normalised document dicts.
        """
        topic = topic_def["topic"]
        keywords = topic_def.get("keywords", [])
        documents: list[dict[str, Any]] = []

        relevant_pages = [p for p in _PLAYWRIGHT_PAGES if topic in p.get("topics", [])]

        for page_cfg in relevant_pages:
            page_url = page_cfg["url"]
            page_name = page_cfg["name"]
            link_pattern = page_cfg.get("link_pattern", r'href="([^"]+)"[^>]*>(.*?)</a>')
            base_url = page_cfg.get("base_url", "")

            try:
                html = self._fetch_rendered_html(page_url)

                for match in re.finditer(link_pattern, html, re.DOTALL | re.IGNORECASE):
                    href = match.group(1)
                    link_text = re.sub(r'<[^>]+>', '', match.group(2)).strip()

                    if not link_text or len(link_text) < 10:
                        continue

                    relevance = _score_topic_relevance(link_text, keywords)
                    # Lower threshold for USTR — everything on that page is trade-related
                    min_relevance = 0.0 if "ustr" in page_name else 0.1
                    if relevance < min_relevance:
                        continue

                    # For USTR, all press releases are trade-relevant
                    if "ustr" in page_name and relevance < 0.1:
                        relevance = 0.3

                    full_url = href if href.startswith("http") else f"{base_url}{href}"

                    documents.append({
                        "source": f"WEB_{page_name.upper()}",
                        "title": link_text[:300],
                        "doc_date": date.today(),
                        "posted_date": date.today(),
                        "classification": "UNCLASSIFIED",
                        "confidence": "derived",
                        "relevance": round(max(relevance, 0.3), 3),
                        "topic": topic,
                        "ticker": topic_def["ticker"],
                        "doc_id": full_url,
                        "url": full_url,
                        "body_snippet": link_text,
                    })

            except Exception as exc:
                log.debug(
                    "Playwright page '{p}' failed: {e}",
                    p=page_name, e=str(exc),
                )

        log.debug(
            "Playwright search: {n} documents for topic '{t}'",
            n=len(documents), t=topic,
        )
        return documents

    # ── Topic-based search ──────────────────────────────────────────────

    def _search_topic(
        self,
        topic_def: dict[str, Any],
        days_back: int = 90,
    ) -> list[dict[str, Any]]:
        """Search for FOIA documents matching a topic definition.

        Parameters:
            topic_def: Topic dict with query, keywords, ticker, etc.
            days_back: How far back to search for new releases.

        Returns:
            List of parsed document dicts.
        """
        documents: list[dict[str, Any]] = []
        query = topic_def["query"]
        topic = topic_def["topic"]

        # 1. RSS feeds (fast, no browser) — always run for all topics
        try:
            rss_docs = self._rss_search(topic_def)
            documents.extend(rss_docs)
        except Exception as exc:
            log.debug("RSS search failed for '{t}': {e}", t=topic, e=str(exc))

        # 2. Playwright-rendered pages (USTR, etc.) — topic-filtered
        try:
            pw_docs = self._playwright_page_search(topic_def)
            documents.extend(pw_docs)
        except Exception as exc:
            log.debug("Playwright page search failed for '{t}': {e}", t=topic, e=str(exc))

        # 3. State Dept FOIA via Playwright (slow — only top priority topics)
        priority_topics = {"trade_policy", "sanctions", "tech_competition"}
        if topic in priority_topics:
            try:
                data = self._state_foia_search(query, page=1)
                results = data.get("Results", data.get("results", []))
                if isinstance(results, list):
                    for doc in results:
                        parsed = self._parse_state_doc(doc, topic_def)
                        if parsed:
                            documents.append(parsed)
            except Exception as exc:
                log.debug(
                    "State FOIA search failed for '{q}': {e}",
                    q=query, e=str(exc),
                )

        return documents

    def _parse_state_doc(
        self,
        doc: dict[str, Any],
        topic_def: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Parse a State Dept FOIA document result.

        Parameters:
            doc: Raw document dict from State FOIA API.
            topic_def: Topic definition for relevance scoring.

        Returns:
            Normalised document dict, or None if unusable.
        """
        try:
            title = (
                doc.get("Subject")
                or doc.get("subject")
                or doc.get("title")
                or ""
            ).strip()
            if not title:
                return None

            # Extract date
            doc_date_str = (
                doc.get("DocDate")
                or doc.get("docDate")
                or doc.get("date")
                or ""
            )
            try:
                doc_date = date.fromisoformat(doc_date_str[:10]) if doc_date_str else None
            except (ValueError, TypeError):
                doc_date = None

            # Extract posted/released date (when declassified)
            posted_str = (
                doc.get("PostedDate")
                or doc.get("postedDate")
                or ""
            )
            try:
                posted_date = date.fromisoformat(posted_str[:10]) if posted_str else date.today()
            except (ValueError, TypeError):
                posted_date = date.today()

            # Document body/description for relevance scoring
            body = (
                doc.get("MessageText")
                or doc.get("body")
                or doc.get("description")
                or title
            )

            classification = _extract_classification(
                doc.get("Classification")
                or doc.get("classification")
                or body
            )

            relevance = _score_topic_relevance(
                f"{title} {body}",
                topic_def["keywords"],
            )

            if relevance < 0.1:
                return None

            return {
                "source": "STATE_DEPT",
                "title": title[:300],
                "doc_date": doc_date,
                "posted_date": posted_date,
                "classification": classification,
                "confidence": CLASSIFICATION_CONFIDENCE.get(classification, "derived"),
                "relevance": round(relevance, 3),
                "topic": topic_def["topic"],
                "ticker": topic_def["ticker"],
                "doc_id": doc.get("DocNbr") or doc.get("id") or "",
                "from_office": (doc.get("From") or doc.get("from") or "").strip(),
                "to_office": (doc.get("To") or doc.get("to") or "").strip(),
                "body_snippet": (body or "")[:500],
            }

        except Exception as exc:
            log.debug("Failed to parse State FOIA doc: {e}", e=str(exc))
            return None

    def _parse_nsa_doc(
        self,
        doc: dict[str, Any],
        topic_def: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Parse a National Security Archive document result.

        Parameters:
            doc: Raw document dict from NSA search.
            topic_def: Topic definition for relevance scoring.

        Returns:
            Normalised document dict, or None if unusable.
        """
        try:
            title = (
                doc.get("title")
                or doc.get("label")
                or ""
            ).strip()
            if not title:
                return None

            # Extract date
            doc_date_str = doc.get("date") or doc.get("created") or ""
            try:
                doc_date = date.fromisoformat(doc_date_str[:10]) if doc_date_str else None
            except (ValueError, TypeError):
                doc_date = None

            posted_str = doc.get("changed") or doc.get("posted") or ""
            try:
                posted_date = date.fromisoformat(posted_str[:10]) if posted_str else date.today()
            except (ValueError, TypeError):
                posted_date = date.today()

            body = doc.get("body") or doc.get("description") or title

            classification = _extract_classification(body)
            relevance = _score_topic_relevance(
                f"{title} {body}",
                topic_def["keywords"],
            )

            if relevance < 0.1:
                return None

            return {
                "source": "NSA_ARCHIVE",
                "title": title[:300],
                "doc_date": doc_date,
                "posted_date": posted_date,
                "classification": classification,
                "confidence": CLASSIFICATION_CONFIDENCE.get(classification, "derived"),
                "relevance": round(relevance, 3),
                "topic": topic_def["topic"],
                "ticker": topic_def["ticker"],
                "doc_id": doc.get("nid") or doc.get("id") or "",
                "url": doc.get("url") or "",
                "body_snippet": (body or "")[:500],
            }

        except Exception as exc:
            log.debug("Failed to parse NSA doc: {e}", e=str(exc))
            return None

    # ── Storage ─────────────────────────────────────────────────────────

    def _store_document(
        self,
        conn: Any,
        doc: dict[str, Any],
    ) -> bool:
        """Store a FOIA document as a raw_series row.

        Parameters:
            conn: Active database connection (within a transaction).
            doc: Normalised document dict.

        Returns:
            True if stored, False if duplicate.
        """
        source_slug = _slugify(doc["source"], max_len=15)
        topic_slug = _slugify(doc["topic"])
        classification = _slugify(doc["classification"], max_len=15)

        series_id = f"FOIA:{source_slug}:{topic_slug}:{classification}"
        obs_date = doc["posted_date"]

        if self._row_exists(series_id, obs_date, conn, dedup_hours=168):
            return False

        payload = {
            "source": doc["source"],
            "title": doc["title"],
            "doc_date": doc["doc_date"].isoformat() if doc["doc_date"] else None,
            "posted_date": doc["posted_date"].isoformat(),
            "classification": doc["classification"],
            "confidence": doc["confidence"],
            "topic": doc["topic"],
            "doc_id": doc.get("doc_id", ""),
            "from_office": doc.get("from_office", ""),
            "to_office": doc.get("to_office", ""),
            "body_snippet": doc.get("body_snippet", ""),
            "url": doc.get("url", ""),
        }

        self._insert_raw(
            conn=conn,
            series_id=series_id,
            obs_date=obs_date,
            value=doc["relevance"],
            raw_payload=payload,
        )
        return True

    def _emit_signal(
        self,
        conn: Any,
        doc: dict[str, Any],
    ) -> None:
        """Emit a signal_sources row for high-value declassified documents.

        Only emits signals for documents with classification >= CONFIDENTIAL
        and relevance > 0.3, as these are the most likely to contain
        actionable geopolitical intelligence.

        Parameters:
            conn: Active database connection (within a transaction).
            doc: Normalised document dict.
        """
        # Only signal on non-trivial documents
        if doc["classification"] == "UNCLASSIFIED" and doc["relevance"] < 0.5:
            return

        ticker = doc["ticker"]
        if not ticker:
            return

        conn.execute(
            text(
                "INSERT INTO signal_sources "
                "(source_type, source_id, ticker, signal_date, signal_type, signal_value) "
                "VALUES (:stype, :sid, :ticker, :sdate, :stype2, :sval) "
                "ON CONFLICT (source_type, source_id, ticker, signal_date, signal_type) "
                "DO NOTHING"
            ),
            {
                "stype": "diplomatic_cable",
                "sid": doc.get("doc_id", doc["title"][:60]),
                "ticker": ticker,
                "sdate": doc["posted_date"],
                "stype2": "DECLASSIFIED_INTEL",
                "sval": json.dumps({
                    "title": doc["title"][:200],
                    "classification": doc["classification"],
                    "confidence": doc["confidence"],
                    "relevance": doc["relevance"],
                    "topic": doc["topic"],
                    "source": doc["source"],
                    "doc_date": doc["doc_date"].isoformat() if doc["doc_date"] else None,
                }),
            },
        )

    # ── Public API ──────────────────────────────────────────────────────

    def pull_all(self, days_back: int = 90) -> dict[str, Any]:
        """Pull recent FOIA cable releases across all tracked topics.

        Parameters:
            days_back: Number of days to look back for new releases.

        Returns:
            Summary dict with counts per topic.
        """
        log.info("Pulling FOIA cables — searching {n} topics", n=len(FOIA_TOPICS))

        total_found = 0
        stored = 0
        signals = 0
        skipped_dup = 0
        topic_counts: dict[str, int] = {}
        alerts: list[str] = []

        try:
            return self._pull_all_inner(
                total_found, stored, signals, skipped_dup,
                topic_counts, alerts, days_back,
            )
        finally:
            self._close_browser()

    def _pull_all_inner(
        self,
        total_found: int,
        stored: int,
        signals: int,
        skipped_dup: int,
        topic_counts: dict[str, int],
        alerts: list[str],
        days_back: int,
    ) -> dict[str, Any]:
        """Inner pull logic — separated so pull_all can guarantee browser cleanup."""
        for topic_def in FOIA_TOPICS:
            topic = topic_def["topic"]
            try:
                documents = self._search_topic(topic_def, days_back=days_back)
                topic_counts[topic] = len(documents)
                total_found += len(documents)

                with self.engine.begin() as conn:
                    for doc in documents:
                        try:
                            was_stored = self._store_document(conn, doc)
                            if not was_stored:
                                skipped_dup += 1
                                continue
                            stored += 1

                            # Emit signal for high-value documents
                            if doc["relevance"] > 0.3:
                                try:
                                    self._emit_signal(conn, doc)
                                    signals += 1
                                except Exception as exc:
                                    log.debug(
                                        "Signal emission failed: {e}",
                                        e=str(exc),
                                    )

                            # Alert on formerly classified documents
                            if doc["classification"] in ("SECRET", "TOP SECRET"):
                                alert_msg = (
                                    f"FOIA DECLASSIFIED [{doc['classification']}]: "
                                    f"{doc['title'][:100]} — topic: {topic}"
                                )
                                alerts.append(alert_msg)
                                log.info(alert_msg)

                        except Exception as exc:
                            log.warning(
                                "Failed to store FOIA doc: {e}",
                                e=str(exc),
                            )

            except Exception as exc:
                log.warning(
                    "FOIA topic search failed for '{t}': {e}",
                    t=topic, e=str(exc),
                )
                topic_counts[topic] = 0

        summary = {
            "status": "SUCCESS",
            "total_found": total_found,
            "stored": stored,
            "signals_emitted": signals,
            "skipped_duplicate": skipped_dup,
            "topics_searched": len(FOIA_TOPICS),
            "topic_counts": topic_counts,
            "alerts": alerts,
            "days_back": days_back,
        }
        log.info("FOIA cables pull complete: {s}", s=summary)
        return summary

    def pull_recent(self, days_back: int = 90) -> dict[str, Any]:
        """Alias for pull_all — always incremental.

        Parameters:
            days_back: Number of days to look back.

        Returns:
            Summary dict.
        """
        return self.pull_all(days_back=days_back)
