"""
GRID Finviz fundamentals scraper.

Scrapes Finviz for P/E, EPS, Revenue, Market Cap, Sector (free, no API key).
Source: https://finviz.com/quote.ashx?t={TICKER}
"""

from __future__ import annotations

import re
import time
from datetime import date
from typing import Any

import httpx
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

DEFAULT_TICKERS: list[str] = [
    "AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "META", "TSLA", "BRK.B", "UNH",
    "XOM", "JPM", "JNJ", "V", "PG", "MA", "HD", "AVGO", "LLY", "MRK", "COST",
]

FIELDS_OF_INTEREST: dict[str, str] = {
    "P/E": "pe_ratio",
    "EPS (ttm)": "eps_ttm",
    "Market Cap": "market_cap",
    "Revenue": "revenue",
    "Sector": "sector",
    "Industry": "industry",
    "Dividend %": "dividend_pct",
    "ROE": "roe",
    "Debt/Eq": "debt_equity",
    "Beta": "beta",
}

_BASE_URL: str = "https://finviz.com/quote.ashx"
_RATE_LIMIT_DELAY: float = 0.5
_REQUEST_TIMEOUT: int = 30
_SERIES_PREFIX: str = "finviz"


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
        "revision_behavior": "OVERWRITE",
        "trust_score": "MED",
        "priority_rank": 45,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info(
            "FinvizScraperPuller initialised -- source_id={sid}",
            sid=self.source_id,
        )

    @retry_on_failure(
        max_attempts=3,
        backoff=3.0,
        retryable_exceptions=(
            ConnectionError, TimeoutError, OSError, httpx.HTTPError,
        ),
    )
    def _fetch_page(self, ticker: str) -> str:
        """Fetch the Finviz quote page HTML for a ticker."""
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (compatible; GRID-DataPuller/1.0; "
                "+https://github.com/grid-trading)"
            ),
            "Accept": "text/html",
        }
        with httpx.Client(timeout=_REQUEST_TIMEOUT) as client:
            resp = client.get(
                _BASE_URL, params={"t": ticker}, headers=headers,
            )
            resp.raise_for_status()
        return resp.text

    def _parse_snapshot_table(self, html: str) -> dict[str, str]:
        """Extract key-value pairs from the Finviz snapshot table."""
        pairs: dict[str, str] = {}
        # Match label-value pairs in the snapshot table
        pattern = re.compile(
            r'class="snapshot-td2-cp"[^>]*>([^<]+)</td>\s*'
            r'<td[^>]*class="snapshot-td2"[^>]*><b>([^<]*)</b>',
            re.DOTALL,
        )
        for match in pattern.finditer(html):
            label = match.group(1).strip()
            value = match.group(2).strip()
            pairs[label] = value

        return pairs

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
                if parsed is None:
                    continue

                sid = f"{_SERIES_PREFIX}.{ticker}.{field_name}"
                numeric_val = parsed if isinstance(parsed, (int, float)) else 0.0

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

        for ticker in tickers:
            result = self.pull_ticker(ticker)
            results.append(result)
            time.sleep(_RATE_LIMIT_DELAY)

        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        total_rows = sum(r["rows_inserted"] for r in results)
        log.info(
            "Finviz pull_all -- {ok}/{total} tickers, {rows} rows",
            ok=succeeded, total=len(results), rows=total_rows,
        )
        return results

    def pull(self) -> dict[str, Any]:
        """Standard pull entry point for scheduler."""
        results = self.pull_all()
        total = sum(r["rows_inserted"] for r in results)
        return {"status": "SUCCESS", "rows_inserted": total}
