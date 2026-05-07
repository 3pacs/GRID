"""
GRID FINRA margin debt statistics ingestion module.

Scrapes monthly margin debt data from FINRA's public margin statistics
page using httpx and regex-based HTML table parsing.

Data source: https://www.finra.org/investors/learn-to-invest/advanced-investing/margin-statistics

Series stored:
- finra.margin_debit: Total margin debit balances (USD millions)
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

import httpx
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

_FINRA_URL = "https://www.finra.org/rules-guidance/key-topics/margin-accounts/margin-statistics"
_SERIES_PREFIX = "finra"
_REQUEST_TIMEOUT: int = 30


class FINRAMarginPuller(BasePuller):
    """Pulls FINRA monthly margin debt statistics via HTML scrape."""

    SOURCE_NAME: str = "FINRA_MARGIN"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": _FINRA_URL,
        "cost_tier": "FREE",
        "latency_class": "MONTHLY",
        "pit_available": True,
        "revision_behavior": "RARE",
        "trust_score": "HIGH",
        "priority_rank": 25,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info("FINRAMarginPuller initialised -- source_id={sid}", sid=self.source_id)

    @retry_on_failure(
        max_attempts=3, backoff=3.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError, httpx.HTTPError),
    )
    def _fetch_page(self) -> str:
        """Fetch the FINRA margin stats HTML page with httpx."""
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; GRID-DataPuller/1.0)",
            "Accept": "text/html,*/*;q=0.8",
        }
        resp = httpx.get(_FINRA_URL, headers=headers, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.text

    def _parse_rows(self, html: str) -> list[dict[str, Any]]:
        """Extract margin debit rows from HTML using regex."""
        rows: list[dict[str, Any]] = []
        # Match table rows: <tr>...<td>date</td><td>debit</td>...</tr>
        tr_pat = re.compile(r"<tr[^>]*>(.*?)</tr>", re.DOTALL)
        td_pat = re.compile(r"<td[^>]*>(.*?)</td>", re.DOTALL)
        for tr in tr_pat.findall(html):
            cells = [re.sub(r"<[^>]+>", "", c).strip() for c in td_pat.findall(tr)]
            if len(cells) < 2:
                continue
            try:
                obs_date = datetime.strptime(cells[0], "%b-%y").date()
            except ValueError:
                try:
                    obs_date = datetime.strptime(cells[0], "%B %Y").date()
                except ValueError:
                    continue
            cleaned = cells[1].replace("$", "").replace(",", "").strip()
            try:
                debit = float(cleaned)
            except (ValueError, TypeError):
                continue
            rows.append({"obs_date": obs_date, "debit": debit})
        return rows

    def pull(self) -> dict[str, Any]:
        """Pull FINRA margin statistics and store to raw_series."""
        try:
            html = self._fetch_page()
        except Exception as exc:
            log.error("FINRA margin pull failed: {e}", e=str(exc))
            return {"status": "FAILED", "rows_inserted": 0, "error": str(exc)}

        parsed = self._parse_rows(html)
        if not parsed:
            log.warning("FINRA margin: no data parsed")
            return {"status": "SUCCESS", "rows_inserted": 0}

        total = 0
        sid = f"{_SERIES_PREFIX}.margin_debit"
        with self.engine.begin() as conn:
            existing = self._get_existing_dates(sid, conn)
            for row in parsed:
                if row["obs_date"] in existing:
                    continue
                self._insert_raw(conn=conn, series_id=sid, obs_date=row["obs_date"],
                                 value=row["debit"], raw_payload={"source_url": _FINRA_URL})
                total += 1

        log.info("FINRA margin: {n} rows inserted", n=total)
        return {"status": "SUCCESS", "rows_inserted": total}
