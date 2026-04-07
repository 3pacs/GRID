"""
GRID Federal Reserve speeches/press releases RSS ingestion.

Feeds: speeches.xml + press_all.xml (no key). Stores series_id=fed.speech.{date}, value=1.
"""

from __future__ import annotations

import re
import time
import xml.etree.ElementTree as ET
from datetime import date, datetime
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

_SPEECHES_URL = "https://www.federalreserve.gov/feeds/speeches.xml"
_PRESS_URL = "https://www.federalreserve.gov/feeds/press_all.xml"
_RATE_LIMIT: float = 1.0
_TIMEOUT: int = 30
_HEADERS = {"User-Agent": "GRID-DataPuller/1.0", "Accept": "application/xml"}
_DATE_FMTS = ["%a, %d %b %Y %H:%M:%S %Z", "%a, %d %b %Y %H:%M:%S %z",
               "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d"]
_SPEAKER_PATS = [
    re.compile(r"(?:Speech|Testimony|Statement)\s+by\s+([\w.\s]+?)\s+(?:on|at|to|before|regarding)", re.I),
    re.compile(r"^((?:Chair|Vice Chair|Governor)\s+\w+)", re.I),
]


def _parse_date(raw: str) -> date | None:
    for fmt in _DATE_FMTS:
        try:
            return datetime.strptime(raw.strip(), fmt).date()
        except ValueError:
            continue
    return None


def _extract_speaker(title: str) -> str:
    for pat in _SPEAKER_PATS:
        m = pat.search(title)
        if m:
            return m.group(1).strip()
    return "Unknown"


class FedSpeechesPuller(BasePuller):
    """Pulls Federal Reserve speeches and press releases from RSS feeds."""

    SOURCE_NAME: str = "FedSpeeches"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://www.federalreserve.gov/feeds/speeches.xml",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 20,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info("FedSpeechesPuller initialised -- source_id={sid}", sid=self.source_id)

    @retry_on_failure(
        max_attempts=3, backoff=2.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError, requests.RequestException),
    )
    def _fetch_feed(self, url: str) -> str:
        """Fetch an RSS feed and return raw XML text."""
        resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
        resp.raise_for_status()
        return resp.text

    def _parse_feed(self, xml_text: str, prefix: str) -> list[dict[str, Any]]:
        """Parse RSS XML into row dicts."""
        rows: list[dict[str, Any]] = []
        for item in ET.fromstring(xml_text).iter("item"):
            pub_raw = (item.findtext("pubDate") or "").strip()
            if not pub_raw:
                continue
            obs_date = _parse_date(pub_raw)
            if obs_date is None:
                continue
            title = (item.findtext("title") or "").strip()
            summary = (item.findtext("description") or "").strip()
            rows.append({
                "series_id": f"{prefix}.{obs_date.isoformat()}",
                "obs_date": obs_date, "title": title,
                "speaker": _extract_speaker(title),
                "link": (item.findtext("link") or "").strip(),
                "summary": summary[:500] if summary else "",
            })
        return rows

    def _pull_feed(self, url: str, prefix: str) -> int:
        """Pull one RSS feed. Returns rows inserted."""
        inserted = 0
        xml_text = self._fetch_feed(url)
        parsed = self._parse_feed(xml_text, prefix)
        with self.engine.begin() as conn:
            for row in parsed:
                existing = self._get_existing_dates(row["series_id"], conn)
                if row["obs_date"] in existing:
                    continue
                self._insert_raw(
                    conn=conn, series_id=row["series_id"],
                    obs_date=row["obs_date"], value=1,
                    raw_payload={
                        "title": row["title"], "speaker": row["speaker"],
                        "link": row["link"], "summary": row["summary"],
                        "feed_url": url,
                    },
                )
                inserted += 1
        return inserted

    def pull(self) -> dict[str, Any]:
        """Pull speeches and press releases from both Fed RSS feeds."""
        total_inserted = 0
        per_feed: dict[str, int] = {}
        errors: list[str] = []

        for url, prefix in [(_SPEECHES_URL, "fed.speech"), (_PRESS_URL, "fed.press")]:
            try:
                count = self._pull_feed(url, prefix)
                per_feed[prefix] = count
                total_inserted += count
            except Exception as exc:
                log.error("FedSpeeches {p} failed: {e}", p=prefix, e=str(exc))
                errors.append(f"{prefix}: {exc}")
                per_feed[prefix] = 0
            time.sleep(_RATE_LIMIT)

        status = "SUCCESS" if not errors else ("PARTIAL" if total_inserted > 0 else "FAILED")
        log.info("FedSpeechesPuller: {n} rows, {e} errors", n=total_inserted, e=len(errors))
        return {
            "status": status,
            "rows_inserted": total_inserted,
            "per_feed": per_feed,
            "errors": errors or None,
        }
