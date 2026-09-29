"""
GRID SEC EDGAR company fundamentals scraper.

Pulls revenue, net income, assets, EPS from SEC EDGAR XBRL API (free).
Source: https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json
Rate limit: 1 req/sec (SEC fair access policy).
"""

from __future__ import annotations

import json
import math
import re
import time
from datetime import date, datetime, timezone
from typing import Any

import httpx
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

_COMPANY_TICKERS_URL: str = "https://www.sec.gov/files/company_tickers.json"
_COMPANY_FACTS_URL: str = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

XBRL_CONCEPTS: dict[str, str] = {
    "Revenues": "revenue",
    "RevenueFromContractWithCustomerExcludingAssessedTax": "revenue_contracts",
    "NetIncomeLoss": "net_income",
    "Assets": "total_assets",
    "EarningsPerShareBasic": "eps_basic",
    "EarningsPerShareDiluted": "eps_diluted",
    "StockholdersEquity": "stockholders_equity",
    "LongTermDebt": "long_term_debt",
}

DEFAULT_TICKERS: list[str] = [
    "AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "META", "TSLA", "JPM", "UNH",
    "XOM", "JNJ", "V", "PG", "MA", "HD", "AVGO", "LLY", "MRK", "COST", "BAC", "BRK.B",
]

_RATE_LIMIT_DELAY: float = 1.0
_REQUEST_TIMEOUT: int = 30
# New series preserve provenance without silently relabeling legacy rows.
_SERIES_PREFIX: str = "sec_filed_fundamentals"
_USER_AGENT: str = "GRID-DataPuller/1.0 aniksrobot@gmail.com"


class SECEdgarCompanyPuller(BasePuller):
    """Pulls company fundamentals from SEC EDGAR XBRL companyfacts API."""

    SOURCE_NAME: str = "SEC_EDGAR_Fundamentals"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://data.sec.gov/api/xbrl/companyfacts/",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "RARE",
        "trust_score": "HIGH",
        "priority_rank": 20,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        self._cik_cache: dict[str, str] = {}
        log.info(
            "SECEdgarCompanyPuller initialised -- source_id={sid}",
            sid=self.source_id,
        )

    @retry_on_failure(
        max_attempts=3,
        backoff=3.0,
        retryable_exceptions=(
            ConnectionError, TimeoutError, OSError, httpx.HTTPError,
        ),
    )
    def _fetch_json(self, url: str) -> dict[str, Any]:
        """Fetch JSON from SEC EDGAR with required headers."""
        headers = {
            "User-Agent": _USER_AGENT,
            "Accept": "application/json",
        }
        with httpx.Client(timeout=_REQUEST_TIMEOUT) as client:
            resp = client.get(url, headers=headers)
            resp.raise_for_status()
        return resp.json()

    def _resolve_cik(self, ticker: str) -> str | None:
        """Resolve ticker to CIK via SEC company_tickers.json (cached)."""
        if ticker in self._cik_cache:
            return self._cik_cache[ticker]

        if not self._cik_cache:
            try:
                data = self._fetch_json(_COMPANY_TICKERS_URL)
                for entry in data.values():
                    t = entry.get("ticker", "").upper()
                    cik = str(entry.get("cik_str", "")).zfill(10)
                    self._cik_cache[t] = cik
                time.sleep(_RATE_LIMIT_DELAY)
            except Exception as exc:
                log.error("SEC CIK lookup failed: {e}", e=str(exc))
                return None

        return self._cik_cache.get(ticker.upper().replace(".", "-"))

    def _extract_facts(
        self, facts_data: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Extract dated reported facts, preserving filing/accession and units.

        Period facts are not TTM EPS/revenue. For the existing one-value per
        field/end-date shape, choose the longest reported period, then latest
        filing of that period; the precise start/end stay visible to readers.
        """
        rows: list[dict[str, Any]] = []
        us_gaap = facts_data.get("facts", {}).get("us-gaap", {})

        for concept_name, field_name in XBRL_CONCEPTS.items():
            concept_data = us_gaap.get(concept_name, {})
            units = concept_data.get("units", {})

            unit = "USD/shares" if field_name.startswith("eps_") else "USD"
            unit_data = units.get(unit) or []

            for entry in unit_data:
                form = entry.get("form", "")
                if form not in ("10-K", "10-K/A", "10-Q", "10-Q/A"):
                    continue

                end_str = entry.get("end")
                val = entry.get("val")
                if end_str is None or val is None:
                    continue

                try:
                    obs_date = date.fromisoformat(end_str)
                    filed = date.fromisoformat(entry["filed"])
                    start = date.fromisoformat(entry["start"]) if entry.get("start") else obs_date
                    number = float(val)
                except (TypeError, ValueError, KeyError):
                    continue
                if (isinstance(val, bool) or not math.isfinite(number)
                    or not re.fullmatch(r"\d{10}-\d{2}-\d{6}", str(entry.get("accn", "")))
                    or (field_name not in {"total_assets", "stockholders_equity", "long_term_debt"}
                        and not entry.get("start"))
                    or not start <= obs_date <= filed <= datetime.now(timezone.utc).date()):
                    continue

                rows.append({
                    "field_name": field_name,
                    "obs_date": obs_date,
                    "value": number,
                    "form": form,
                    "period_start": entry.get("start"),
                    "period_end": end_str,
                    "fiscal_year": entry.get("fy"),
                    "fiscal_period": entry.get("fp"),
                    "filed": filed.isoformat(),
                    "accession": entry["accn"],
                    "unit": unit,
                    "duration_days": (obs_date - start).days,
                })

        ordered = sorted(rows, key=lambda row: (
            row["duration_days"], row["filed"], row["accession"]
        ), reverse=True)
        selected = {}
        for row in ordered:
            selected.setdefault((row["field_name"], row["obs_date"]), row)
        return list(selected.values())

    def pull_ticker(self, ticker: str) -> dict[str, Any]:
        """Pull EDGAR fundamentals for a single ticker."""
        ticker = ticker.strip().upper().replace("-", ".")
        cik = self._resolve_cik(ticker)
        if not cik:
            log.warning("SEC EDGAR: CIK not found for {t}", t=ticker)
            return {"status": "FAILED", "ticker": ticker, "rows_inserted": 0,
                    "error": f"CIK not found for {ticker}"}

        url = _COMPANY_FACTS_URL.format(cik=cik)
        try:
            facts_data = self._fetch_json(url)
        except Exception as exc:
            log.warning(
                "SEC EDGAR fetch failed for {t} (CIK {c}): {e}",
                t=ticker, c=cik, e=str(exc),
            )
            return {"status": "FAILED", "ticker": ticker, "rows_inserted": 0,
                    "error": str(exc)}

        rows = self._extract_facts(facts_data)
        if not rows:
            log.warning("SEC EDGAR: no facts extracted for {t}", t=ticker)
            return {"status": "FAILED", "ticker": ticker, "rows_inserted": 0,
                    "error": "no supported provenance-complete SEC facts"}
        try:
            identity_matches = int(facts_data.get("cik")) == int(cik)
        except (TypeError, ValueError):
            identity_matches = False
        if not identity_matches:
            return {"status": "FAILED", "ticker": ticker, "rows_inserted": 0,
                    "error": "SEC companyfacts CIK mismatch"}

        inserted = 0
        with self.engine.begin() as conn:
            existing_cache = set()
            for fn in set(r["field_name"] for r in rows):
                sid = f"{_SERIES_PREFIX}.{ticker}.{fn}"
                for (payload,) in conn.execute(text(
                    "SELECT raw_payload FROM raw_series WHERE source_id = :src "
                    "AND series_id = :sid AND pull_status = 'SUCCESS'"
                ), {"src": self.source_id, "sid": sid}).fetchall():
                    if isinstance(payload, str):
                        payload = json.loads(payload)
                    existing_cache.add((sid, payload.get("period_end"),
                                        payload.get("period_start"), payload.get("accession")))

            # EDGAR returns the same fact across multiple filings (10-K +
            # subsequent 10-K/A, or concepts reported under both us-gaap and
            # ifrs-full) — dedupe within-batch to avoid UniqueViolation on
            # (series_id, source_id, obs_date, pull_timestamp).
            seen_this_cycle: set[tuple[str, date]] = set()

            for row in rows:
                fn = row["field_name"]
                sid = f"{_SERIES_PREFIX}.{ticker}.{fn}"
                filing_key = (sid, row["period_end"], row.get("period_start"), row["accession"])
                if filing_key in existing_cache:
                    continue
                key = (sid, row["obs_date"])
                if key in seen_this_cycle:
                    continue
                seen_this_cycle.add(key)
                payload = {
                    "ticker": ticker, "cik": cik, "field": fn,
                    "form": row["form"], "period_start": row.get("period_start"),
                    "period_end": row["obs_date"].isoformat(),
                    "fiscal_year": row.get("fiscal_year"),
                    "fiscal_period": row.get("fiscal_period"), "source_url": url,
                    "filed": row["filed"], "accession": row["accession"],
                    "unit": row["unit"], "known_at_precision": "date",
                }
                self._insert_raw(conn=conn, series_id=sid, obs_date=row["obs_date"],
                                 value=row["value"], raw_payload=payload)
                inserted += 1
        log.info("SEC EDGAR {t}: {n} rows inserted", t=ticker, n=inserted)
        return {"status": "SUCCESS" if inserted else "UNCHANGED", "ticker": ticker,
                "rows_inserted": inserted}

    def pull_all(self, tickers: list[str] | None = None) -> list[dict[str, Any]]:
        """Pull EDGAR fundamentals for a list of tickers."""
        tickers = tickers or DEFAULT_TICKERS
        results: list[dict[str, Any]] = []

        for ticker in tickers:
            result = self.pull_ticker(ticker)
            results.append(result)
            time.sleep(_RATE_LIMIT_DELAY)

        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        total_rows = sum(r["rows_inserted"] for r in results)
        log.info(
            "SEC EDGAR pull_all -- {ok}/{total} tickers, {rows} rows",
            ok=succeeded, total=len(results), rows=total_rows,
        )
        return results

    def pull(self) -> dict[str, Any]:
        """Standard pull entry point for scheduler."""
        results = self.pull_all()
        total = sum(r["rows_inserted"] for r in results)
        failed = any(row["status"] == "FAILED" for row in results)
        status = "PARTIAL" if total and failed else "SUCCESS" if total else "FAILED" if failed else "UNCHANGED"
        return {"status": status, "rows_inserted": total, "results": results}
