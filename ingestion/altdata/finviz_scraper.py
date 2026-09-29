"""
GRID Finviz fundamentals scraper.

Reads the Finviz quote-page snapshot table for P/E, EPS, Sales, Market Cap,
ROE, Debt/Eq and Beta (free, no API key) for a fixed list of ~20 large caps.
Source: https://finviz.com/stock?t={TICKER}

2026-09-29 fix: from ~2026-04-07 every run inserted 0 rows but reported
SUCCESS. Finviz moved the page (``/quote.ashx`` -> ``/quote`` -> ``/stock``,
301s) and changed the snapshot markup: the label is now inside
``<div class="snapshot-td-label">`` and the value inside
``<div class="snapshot-td-content"><b>..</b>`` (sometimes wrapped in a
colour ``<span>``), so the old regex matched nothing. The page is
server-rendered, so a plain HTTP GET with an honest User-Agent is enough;
Playwright/Chromium is no longer needed. robots.txt allows /stock.

Failure contract: if no ticker parses, ``pull()`` raises, so the scheduler
group records FAILED in pull_log and does not bump source_catalog. Text
fields (Sector/Industry) are no longer stored as a fake 0.0 value.
"""

from __future__ import annotations

import html as html_lib
import re
import time
from datetime import date
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

DEFAULT_TICKERS: list[str] = [
    "AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "META", "TSLA", "BRK.B", "UNH",
    "XOM", "JPM", "JNJ", "V", "PG", "MA", "HD", "AVGO", "LLY", "MRK", "COST",
]

# Numeric snapshot fields only. (Sector/Industry used to be stored as a
# meaningless 0.0 value; they are no longer written.) "Dividend %" is kept
# for older layouts; when a label is absent the field is simply skipped.
FIELDS_OF_INTEREST: dict[str, str] = {
    "P/E": "pe_ratio",
    "EPS (ttm)": "eps_ttm",
    "Market Cap": "market_cap",
    "Sales": "revenue",
    "Dividend %": "dividend_pct",
    "ROE": "roe",
    "Debt/Eq": "debt_equity",
    "Beta": "beta",
}

_BASE_URL: str = "https://finviz.com/stock"
_USER_AGENT: str = "GRID/4.0 (research; stepdadfinance@gmail.com)"
_RATE_LIMIT_DELAY: float = 1.5
_REQUEST_TIMEOUT: int = 30
_SERIES_PREFIX: str = "finviz"

# Current layout (2026-09): label div + content div in adjacent cells.
_SNAPSHOT_RE = re.compile(
    r'<div class="snapshot-td-label">(?P<label>[^<]+)</div>\s*</td>\s*'
    r'<td[^>]*>\s*<div class="snapshot-td-content">(?P<value>.*?)</div>\s*</td>',
    re.S,
)
# Pre-2026 layout, kept as a fallback.
_LEGACY_SNAPSHOT_RE = re.compile(
    r'class="snapshot-td2[^"]*cursor-pointer[^"]*"[^>]*>([^<]+)</td>'
    r'<td[^>]*class="snapshot-td2[^"]*"[^>]*><b>([^<]*)</b>',
)
_TAG_RE = re.compile(r"<[^>]+>")


def parse_snapshot_table(html: str) -> dict[str, str]:
    """Extract label -> raw value text from a Finviz quote page."""
    pairs: dict[str, str] = {}
    for m in _SNAPSHOT_RE.finditer(html):
        label = html_lib.unescape(m.group("label")).strip()
        value = html_lib.unescape(_TAG_RE.sub(" ", m.group("value"))).strip()
        # Some cells hold "value <small>pct</small>"; keep the first token.
        value = value.split()[0] if value else value
        pairs.setdefault(label, value)
    if not pairs:
        for m in _LEGACY_SNAPSHOT_RE.finditer(html):
            pairs.setdefault(m.group(1).strip(), m.group(2).strip())
    return pairs


def _parse_finviz_value(raw: str) -> float | str | None:
    """Convert Finviz cell to numeric (handles B/M/K suffixes and %)."""
    if not raw or raw == "-":
        return None

    clean = raw.strip().replace(",", "")

    # Percentage values
    if clean.endswith("%"):
        try:
            return float(clean[:-1])
        except ValueError:
            return clean

    # Suffixed numeric values (1.5B, 300M, etc.)
    multipliers = {"B": 1e9, "M": 1e6, "K": 1e3, "T": 1e12}
    if clean and clean[-1] in multipliers:
        try:
            return float(clean[:-1]) * multipliers[clean[-1]]
        except ValueError:
            return clean

    # Plain numeric
    try:
        return float(clean)
    except ValueError:
        return clean


class FinvizScraperPuller(BasePuller):
    """Scrapes Finviz for company fundamentals (P/E, EPS, Market Cap, etc.)."""

    SOURCE_NAME: str = "finviz_fundamentals"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://finviz.com/quote.ashx",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": False,
        "revision_behavior": "NEVER",
        "trust_score": "MED",
        "priority_rank": 45,
    }

    def __init__(self, db_engine: Engine, session: requests.Session | None = None) -> None:
        super().__init__(db_engine)
        self._session = session or requests.Session()
        self._session.headers.update({"User-Agent": _USER_AGENT})
        log.info(
            "FinvizScraperPuller initialised -- source_id={sid}",
            sid=self.source_id,
        )

    @retry_on_failure(
        max_attempts=3,
        backoff=3.0,
        retryable_exceptions=(ConnectionError, TimeoutError, requests.RequestException),
    )
    def _fetch_page(self, ticker: str) -> str:
        """Fetch the Finviz quote page HTML."""
        resp = self._session.get(
            _BASE_URL, params={"t": ticker}, timeout=_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.text

    def _parse_snapshot_table(self, html: str) -> dict[str, str]:
        """Extract key-value pairs from the Finviz snapshot table."""
        return parse_snapshot_table(html)

    def pull_ticker(self, ticker: str) -> dict[str, Any]:
        """Pull fundamentals for a single ticker."""
        try:
            html = self._fetch_page(ticker)
        except Exception as exc:
            log.warning(
                "Finviz fetch failed for {t}: {e}", t=ticker, e=str(exc),
            )
            return {"status": "FAILED", "ticker": ticker, "rows_inserted": 0,
                    "error": str(exc)}

        raw_pairs = self._parse_snapshot_table(html)
        if not raw_pairs:
            log.warning("Finviz: no data parsed for {t}", t=ticker)
            return {"status": "FAILED", "ticker": ticker, "rows_inserted": 0,
                    "error": "no snapshot table found"}

        today = date.today()
        inserted = 0

        with self.engine.begin() as conn:
            existing = set()
            for finviz_label, field_name in FIELDS_OF_INTEREST.items():
                sid = f"{_SERIES_PREFIX}.{ticker}.{field_name}"
                existing_dates = self._get_existing_dates(sid, conn)
                if today in existing_dates:
                    existing.add(field_name)

            for finviz_label, field_name in FIELDS_OF_INTEREST.items():
                if field_name in existing:
                    continue

                raw_val = raw_pairs.get(finviz_label)
                if raw_val is None:
                    continue

                parsed = _parse_finviz_value(raw_val)
                if not isinstance(parsed, (int, float)):
                    # None, or text that did not parse as a number: never
                    # store a placeholder value.
                    continue

                sid = f"{_SERIES_PREFIX}.{ticker}.{field_name}"
                numeric_val = parsed

                self._insert_raw(
                    conn=conn,
                    series_id=sid,
                    obs_date=today,
                    value=float(numeric_val),
                    raw_payload={
                        "ticker": ticker,
                        "field": finviz_label,
                        "raw_value": raw_val,
                        "parsed": str(parsed),
                        "source_url": f"{_BASE_URL}?t={ticker}",
                    },
                )
                inserted += 1

        log.info("Finviz {t}: {n} fields inserted", t=ticker, n=inserted)
        return {"status": "SUCCESS", "ticker": ticker, "rows_inserted": inserted}

    def pull_all(self, tickers: list[str] | None = None) -> list[dict[str, Any]]:
        """Pull fundamentals for a list of tickers (defaults to top-20 SPY)."""
        tickers = tickers or DEFAULT_TICKERS
        results: list[dict[str, Any]] = []

        for n, ticker in enumerate(tickers):
            if n:
                time.sleep(_RATE_LIMIT_DELAY)
            results.append(self.pull_ticker(ticker))

        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        total_rows = sum(r["rows_inserted"] for r in results)
        log.info(
            "Finviz pull_all -- {ok}/{total} tickers, {rows} rows",
            ok=succeeded, total=len(results), rows=total_rows,
        )
        return results

    def pull(self) -> dict[str, Any]:
        """Standard pull entry point for the scheduler group.

        Raises when no ticker could be fetched and parsed, so the group
        runner (which only treats exceptions as failures) records FAILED
        instead of a 0-row SUCCESS. Returns PARTIAL when some tickers failed.
        """
        results = self.pull_all()
        ok = [r for r in results if r["status"] == "SUCCESS"]
        total = sum(r["rows_inserted"] for r in results)
        if not ok:
            errors = sorted({str(r.get("error", ""))[:80] for r in results})
            raise RuntimeError(
                f"Finviz: 0/{len(results)} tickers parsed ({'; '.join(errors)[:300]})"
            )
        status = "SUCCESS" if len(ok) == len(results) else "PARTIAL"
        return {
            "status": status,
            "rows_inserted": total,
            "tickers_ok": len(ok),
            "tickers_total": len(results),
        }
