"""
GRID Atlanta Fed GDPNow nowcast ingestion module.

Scrapes the Atlanta Fed GDPNow page for the latest real-time GDP
growth estimate. Uses regex to extract the number.

Data source: https://www.atlantafed.org/cqer/research/gdpnow

Series stored:
- nowcast.gdpnow: Atlanta Fed GDPNow real GDP growth estimate (% SAAR)
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

_GDPNOW_URL = "https://www.atlantafed.org/cqer/research/gdpnow"
_SERIES_PREFIX = "nowcast"
_REQUEST_TIMEOUT: int = 30


class NowcastPuller(BasePuller):
    """Pulls Atlanta Fed GDPNow real-time GDP estimate."""

    SOURCE_NAME: str = "nowcast"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": _GDPNOW_URL,
        "cost_tier": "FREE",
        "latency_class": "WEEKLY",
        "pit_available": True,
        "revision_behavior": "FREQUENT",
        "trust_score": "HIGH",
        "priority_rank": 10,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info("NowcastPuller initialised -- source_id={sid}", sid=self.source_id)

    @retry_on_failure(
        max_attempts=3, backoff=3.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError, requests.RequestException),
    )
    def _fetch_page(self) -> str:
        """Fetch the Atlanta Fed GDPNow HTML page."""
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; GRID-DataPuller/1.0)",
            "Accept": "text/html,*/*;q=0.8",
        }
        resp = requests.get(_GDPNOW_URL, headers=headers, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.text

    def _parse_estimate(self, html: str) -> dict[str, Any] | None:
        """Extract the latest GDPNow estimate using regex."""
        pat = r"[Ll]atest\s+estimate[:\s]+(-?\d+\.?\d*)\s*percent"
        match = re.search(pat, html)
        if not match:
            pat = r"GDPNow\s+model\s+estimate[^-\d]*(-?\d+\.?\d*)\s*percent"
            match = re.search(pat, html)
        if not match:
            return None
        try:
            value = float(match.group(1))
        except (ValueError, TypeError):
            return None

        # Try to extract date from nearby text
        dpat = r"(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}"
        dm = re.search(dpat, html[match.start():match.start() + 200])
        obs = date.today()
        if dm:
            try:
                obs = datetime.strptime(dm.group(0), "%B %d, %Y").date()
            except ValueError:
                pass
        return {"obs_date": obs, "value": value}

    def pull(self) -> dict[str, Any]:
        """Pull the latest GDPNow estimate."""
        try:
            html = self._fetch_page()
        except Exception as exc:
            log.error("GDPNow pull failed: {e}", e=str(exc))
            return {"status": "FAILED", "rows_inserted": 0, "error": str(exc)}

        parsed = self._parse_estimate(html)
        if not parsed:
            log.warning("GDPNow: no estimate parsed")
            return {"status": "SUCCESS", "rows_inserted": 0}

        sid = f"{_SERIES_PREFIX}.gdpnow"
        total = 0
        with self.engine.begin() as conn:
            existing = self._get_existing_dates(sid, conn)
            if parsed["obs_date"] not in existing:
                self._insert_raw(conn=conn, series_id=sid, obs_date=parsed["obs_date"],
                                 value=parsed["value"],
                                 raw_payload={"source": "atlanta_fed", "source_url": _GDPNOW_URL})
                total = 1
                log.info("GDPNow: stored {v}% for {d}", v=parsed["value"], d=parsed["obs_date"])

        return {"status": "SUCCESS", "rows_inserted": total}
