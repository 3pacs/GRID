"""
Wikipedia "This Day in History" and RSS news ingestor.

Pulls historical events for the current date from:
1. Wikipedia "On this day" API
2. Wikipedia featured article / current events
3. RSS feeds for economic and financial history

Provides narrative context for market analysis — historical
analogs, seasonal patterns, anniversary effects.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from datetime import date, datetime
from typing import Any

import requests
from loguru import logger as log

# RSS feeds for financial/economic news and history
RSS_FEEDS = {
    "reuters_markets": "https://www.reutersagency.com/feed/?taxonomy=best-sectors&post_type=best",
    "ft_markets": "https://www.ft.com/markets?format=rss",
    "fed_speeches": "https://www.federalreserve.gov/feeds/speeches.xml",
    "bls_releases": "https://www.bls.gov/feed/bls_latest.rss",
    "treasury_press": "https://home.treasury.gov/system/files/136/RSS-Feed-for-Press-Releases.xml",
}


class WikiHistoryPuller:
    """Pull 'This Day in History' data from Wikipedia and RSS feeds."""

    def __init__(self, db_engine: Any = None) -> None:
        self.engine = db_engine
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "GRID/4.0 (research; grid.stepdad.finance)"})

    def pull_today(self, target_date: date | None = None) -> dict:
        """Pull all historical context for a given date.

        Returns dict with keys: date, wiki_events, wiki_births,
        wiki_deaths, rss_news, on_this_day_summary.
        """
        d = target_date or date.today()
        result = {
            "date": d.isoformat(),
            "month": d.month,
            "day": d.day,
            "wiki_events": [],
            "wiki_selected": [],
            "rss_news": [],
            "on_this_day_summary": "",
        }

        # Wikipedia "On this day"
        try:
            events = self._wiki_on_this_day(d.month, d.day)
            result["wiki_events"] = events[:20]  # Top 20 events
        except Exception as exc:
            log.warning("Wikipedia on-this-day failed: {e}", e=str(exc))

        # Wikipedia featured/selected anniversaries
        try:
            selected = self._wiki_selected_anniversaries(d.month, d.day)
            result["wiki_selected"] = selected[:5]
        except Exception as exc:
            log.debug("Wiki selected anniversaries failed: {e}", e=str(exc))

        # RSS feeds
        for feed_name, feed_url in RSS_FEEDS.items():
            try:
                items = self._parse_rss(feed_url)
                for item in items[:3]:  # Max 3 per feed
                    item["source"] = feed_name
                    result["rss_news"].append(item)
            except Exception as exc:
                log.debug("RSS feed {f} failed: {e}", f=feed_name, e=str(exc))

        # Build summary
        if result["wiki_events"]:
            financial = [e for e in result["wiki_events"]
                        if any(kw in e.get("text", "").lower()
                              for kw in ["bank", "stock", "trade", "economy", "federal",
                                         "treasury", "gold", "oil", "crash", "recession",
                                         "inflation", "depress", "tariff", "sanction",
                                         "war", "treaty", "embargo"])]
            if financial:
                result["on_this_day_summary"] = (
                    f"On {d.strftime('%B %d')}: "
                    + "; ".join(e["text"][:100] for e in financial[:3])
                )
            else:
                result["on_this_day_summary"] = (
                    f"{len(result['wiki_events'])} historical events found for {d.strftime('%B %d')}"
                )

        log.info(
            "Wiki history: {n} events, {r} RSS items for {d}",
            n=len(result["wiki_events"]), r=len(result["rss_news"]), d=d,
        )
        return result

    def _wiki_on_this_day(self, month: int, day: int) -> list[dict]:
        """Fetch Wikipedia 'On this day' events via REST API."""
        url = f"https://api.wikimedia.org/feed/v1/wikipedia/en/onthisday/all/{month:02d}/{day:02d}"
        resp = self._session.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        events = []
        for item in data.get("events", []):
            text = item.get("text", "")
            year = item.get("year")
            if text:
                events.append({
                    "year": year,
                    "text": text,
                    "type": "event",
                })

        # Sort by year descending (most recent first)
        events.sort(key=lambda e: e.get("year") or 0, reverse=True)
        return events

    def _wiki_selected_anniversaries(self, month: int, day: int) -> list[dict]:
        """Fetch Wikipedia selected anniversaries."""
        url = f"https://api.wikimedia.org/feed/v1/wikipedia/en/onthisday/selected/{month:02d}/{day:02d}"
        resp = self._session.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        selected = []
        for item in data.get("selected", []):
            text = item.get("text", "")
            year = item.get("year")
            if text:
                selected.append({"year": year, "text": text, "type": "selected"})
        return selected

    def _parse_rss(self, url: str) -> list[dict]:
        """Parse an RSS feed and return items."""
        resp = self._session.get(url, timeout=10)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)

        items = []
        # Handle both RSS 2.0 and Atom feeds
        for item in root.iter("item"):
            title = item.findtext("title", "")
            link = item.findtext("link", "")
            desc = item.findtext("description", "")
            pub_date = item.findtext("pubDate", "")
            if title:
                items.append({
                    "title": title.strip(),
                    "link": link.strip(),
                    "description": re.sub(r"<[^>]+>", "", desc)[:200].strip(),
                    "pub_date": pub_date.strip(),
                })

        # Atom feed fallback
        if not items:
            ns = {"atom": "http://www.w3.org/2005/Atom"}
            for entry in root.iter("{http://www.w3.org/2005/Atom}entry"):
                title = entry.findtext("{http://www.w3.org/2005/Atom}title", "")
                link_el = entry.find("{http://www.w3.org/2005/Atom}link")
                link = link_el.get("href", "") if link_el is not None else ""
                summary = entry.findtext("{http://www.w3.org/2005/Atom}summary", "")
                if title:
                    items.append({
                        "title": title.strip(),
                        "link": link.strip(),
                        "description": re.sub(r"<[^>]+>", "", summary)[:200].strip(),
                        "pub_date": "",
                    })

        return items

    def save_to_db(self, data: dict) -> bool:
        """Save historical context to raw_series for LLM consumption."""
        if not self.engine:
            return False

        import json
        from sqlalchemy import text

        try:
            with self.engine.begin() as conn:
                # Ensure source exists
                conn.execute(text(
                    "INSERT INTO source_catalog (name, base_url, cost_tier, latency_class) "
                    "VALUES ('WikiHistory', 'https://api.wikimedia.org', 'free', 'batch') "
                    "ON CONFLICT (name) DO NOTHING"
                ))
                src = conn.execute(
                    text("SELECT id FROM source_catalog WHERE name = 'WikiHistory'")
                ).fetchone()
                if not src:
                    return False

                conn.execute(
                    text(
                        "INSERT INTO raw_series (series_id, source_id, obs_date, pull_timestamp, value, raw_payload) "
                        "VALUES (:sid, :src, :d, NOW(), :v, :payload) "
                        "ON CONFLICT DO NOTHING"
                    ),
                    {
                        "sid": f"wiki_today_{data['date']}",
                        "src": src[0],
                        "d": data["date"],
                        "v": len(data.get("wiki_events", [])),
                        "payload": json.dumps(data),
                    },
                )
            log.info("Saved wiki history for {d}", d=data["date"])
            return True
        except Exception as exc:
            log.warning("Failed to save wiki history: {e}", e=str(exc))
            return False
