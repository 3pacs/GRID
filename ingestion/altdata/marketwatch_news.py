"""
GRID MarketWatch RSS news scraper.

Pulls headlines from MarketWatch public RSS feeds (topstories + marketpulse).
Extracts title, published date, link, summary per article.
"""

from __future__ import annotations

import hashlib
import time
import xml.etree.ElementTree as ET
from datetime import date
from email.utils import parsedate_to_datetime
from typing import Any

import httpx
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# RSS feed URLs
_FEEDS: dict[str, str] = {
    "topstories": "https://feeds.marketwatch.com/marketwatch/topstories",
    "marketpulse": "https://feeds.marketwatch.com/marketwatch/marketpulse",
}

_RATE_LIMIT_DELAY: float = 0.5
_REQUEST_TIMEOUT: int = 30
_SERIES_PREFIX: str = "mw_news"


def _parse_rss_items(xml_text: str) -> list[dict[str, Any]]:
    """Parse RSS 2.0 XML into article dicts (title, link, pub_date, summary, guid)."""
    items: list[dict[str, Any]] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        log.warning("MarketWatch RSS: XML parse error: {e}", e=str(exc))
        return items

    # Standard RSS 2.0 structure: rss/channel/item
    for item in root.iter("item"):
        title_el = item.find("title")
        link_el = item.find("link")
        pubdate_el = item.find("pubDate")
        desc_el = item.find("description")
        guid_el = item.find("guid")

        title = title_el.text.strip() if title_el is not None and title_el.text else ""
        link = link_el.text.strip() if link_el is not None and link_el.text else ""
        summary = desc_el.text.strip() if desc_el is not None and desc_el.text else ""
        guid = guid_el.text.strip() if guid_el is not None and guid_el.text else link

        # Parse publication date
        pub_date: date | None = None
        if pubdate_el is not None and pubdate_el.text:
            try:
                pub_dt = parsedate_to_datetime(pubdate_el.text.strip())
                pub_date = pub_dt.date()
            except (ValueError, TypeError) as exc:
                log.warning(
                    "MarketWatch RSS: bad pubDate '{d}': {e}",
                    d=pubdate_el.text, e=str(exc),
                )

        if not title:
            continue

        items.append({
            "title": title,
            "link": link,
            "pub_date": pub_date or date.today(),
            "summary": summary[:500],  # Truncate long descriptions
            "guid": guid,
        })

    return items


class MarketWatchNewsPuller(BasePuller):
    """Pulls MarketWatch RSS news headlines (topstories + marketpulse)."""

    SOURCE_NAME: str = "MarketWatch_News"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://feeds.marketwatch.com/marketwatch/topstories",
        "cost_tier": "FREE",
        "latency_class": "REALTIME",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "MED",
        "priority_rank": 35,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info(
            "MarketWatchNewsPuller initialised -- source_id={sid}",
            sid=self.source_id,
        )

    @retry_on_failure(
        max_attempts=3,
        backoff=3.0,
        retryable_exceptions=(
            ConnectionError, TimeoutError, OSError, httpx.HTTPError,
        ),
    )
    def _fetch_feed(self, url: str) -> str:
        """Fetch an RSS feed and return raw XML text."""
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (compatible; GRID-DataPuller/1.0; "
                "+https://github.com/grid-trading)"
            ),
            "Accept": "application/rss+xml, application/xml, text/xml",
        }
        with httpx.Client(timeout=_REQUEST_TIMEOUT, follow_redirects=True) as client:
            resp = client.get(url, headers=headers)
            resp.raise_for_status()
        return resp.text

    def _guid_hash(self, guid: str) -> str:
        """Create a short hash of a GUID for dedup series_id."""
        return hashlib.md5(guid.encode()).hexdigest()[:12]

    def pull_feed(self, feed_name: str) -> dict[str, Any]:
        """Pull a single RSS feed and store articles."""
        url = _FEEDS.get(feed_name)
        if not url:
            return {"status": "FAILED", "feed_name": feed_name,
                    "rows_inserted": 0, "error": f"Unknown feed: {feed_name}"}

        try:
            xml_text = self._fetch_feed(url)
        except Exception as exc:
            log.error(
                "MarketWatch {f} fetch failed: {e}", f=feed_name, e=str(exc),
            )
            return {"status": "FAILED", "feed_name": feed_name,
                    "rows_inserted": 0, "error": str(exc)}

        articles = _parse_rss_items(xml_text)
        if not articles:
            log.warning("MarketWatch {f}: no articles parsed", f=feed_name)
            return {"status": "SUCCESS", "feed_name": feed_name,
                    "rows_inserted": 0}

        inserted = 0
        sid = f"{_SERIES_PREFIX}.{feed_name}"

        with self.engine.begin() as conn:
            existing = self._get_existing_dates(sid, conn)

            for article in articles:
                obs_date = article["pub_date"]
                guid_hash = self._guid_hash(article["guid"])
                dedup_sid = f"{sid}.{guid_hash}"

                if self._row_exists(dedup_sid, obs_date, conn):
                    continue

                self._insert_raw(
                    conn=conn,
                    series_id=dedup_sid,
                    obs_date=obs_date,
                    value=1.0,  # Presence indicator
                    raw_payload={
                        "title": article["title"],
                        "link": article["link"],
                        "summary": article["summary"],
                        "pub_date": str(article["pub_date"]),
                        "guid": article["guid"],
                        "feed": feed_name,
                        "source_url": url,
                    },
                )
                inserted += 1

        log.info(
            "MarketWatch {f}: {n} articles inserted", f=feed_name, n=inserted,
        )
        return {"status": "SUCCESS", "feed_name": feed_name,
                "rows_inserted": inserted}

    def pull_all(self) -> list[dict[str, Any]]:
        """Pull all configured MarketWatch RSS feeds."""
        results: list[dict[str, Any]] = []

        for feed_name in _FEEDS:
            result = self.pull_feed(feed_name)
            results.append(result)
            time.sleep(_RATE_LIMIT_DELAY)

        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        total_rows = sum(r["rows_inserted"] for r in results)
        log.info(
            "MarketWatch pull_all -- {ok}/{total} feeds, {rows} articles",
            ok=succeeded, total=len(results), rows=total_rows,
        )
        return results

    def pull(self) -> dict[str, Any]:
        """Standard pull entry point for scheduler."""
        results = self.pull_all()
        total = sum(r["rows_inserted"] for r in results)
        return {"status": "SUCCESS", "rows_inserted": total}
