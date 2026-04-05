"""
SEC EDGAR 8-K Transcript Puller — earnings call transcripts from SEC filings.

Companies file 8-K forms with earnings results and often attach
full earnings call transcripts. This puller:
1. Fetches recent 8-K filings for tracked tickers
2. Extracts text content from HTML/text attachments
3. Stores raw transcript text for LLM analysis
4. Extracts key guidance phrases for milestone tracking

API: https://efts.sec.gov/LATEST/search-index?q=...
No API key needed. Rate limit: 10 requests/sec.
"""

from __future__ import annotations

import re
import time
from datetime import date, datetime, timedelta
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

SEC_SEARCH = "https://efts.sec.gov/LATEST/search-index"
SEC_EDGAR = "https://www.sec.gov/cgi-bin/browse-edgar"
SEC_FULL_TEXT = "https://efts.sec.gov/LATEST/search-index"
EDGAR_FILING_API = "https://data.sec.gov/submissions"

HEADERS = {
    "User-Agent": "GRID Intelligence Platform stepdadfinance@gmail.com",
    "Accept": "application/json",
}

# CIK mapping for top tickers (SEC uses CIK not ticker)
# These are the most important ones — expand as needed
TICKER_CIK: dict[str, str] = {
    "AAPL": "0000320193", "MSFT": "0000789019", "GOOGL": "0001652044",
    "AMZN": "0001018724", "NVDA": "0001045810", "META": "0001326801",
    "TSLA": "0001318605", "JPM": "0000019617", "V": "0001403161",
    "MA": "0001141391", "UNH": "0000731766", "JNJ": "0000200406",
    "PG": "0000080424", "HD": "0000354950", "BAC": "0000070858",
    "XOM": "0000034088", "ABBV": "0001551152", "MRK": "0000310158",
    "KO": "0000021344", "PEP": "0000077476", "WMT": "0000104169",
    "COST": "0000909832", "AMD": "0000002488", "NFLX": "0001065280",
    "CRM": "0001108524", "INTC": "0000050863", "GS": "0000886982",
    "LLY": "0000059478", "AVGO": "0001649338", "QCOM": "0000804328",
}


class EdgarTranscriptPuller(BasePuller):
    """Pull earnings-related 8-K filings and extract transcript text."""

    SOURCE_NAME = "edgar_transcripts"
    SOURCE_CONFIG = {
        "base_url": "https://www.sec.gov/cgi-bin/browse-edgar",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 14,
    }

    @retry_on_failure(max_attempts=2, retryable_exceptions=(ConnectionError, TimeoutError, OSError))
    def _sec_get(self, url: str) -> requests.Response:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        time.sleep(0.15)  # SEC rate limit: 10/sec
        resp.raise_for_status()
        return resp

    def get_recent_8k(self, cik: str, days_back: int = 90) -> list[dict[str, Any]]:
        """Get recent 8-K filings for a company.

        Args:
            cik: SEC CIK number.
            days_back: How far back to look.

        Returns:
            List of filing metadata dicts.
        """
        url = f"{EDGAR_FILING_API}/CIK{cik}.json"
        resp = self._sec_get(url)
        data = resp.json()

        recent = data.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        accessions = recent.get("accessionNumber", [])
        descriptions = recent.get("primaryDocDescription", [])

        cutoff = (date.today() - timedelta(days=days_back)).isoformat()
        filings: list[dict[str, Any]] = []

        for i, form in enumerate(forms):
            if form == "8-K" and i < len(dates) and dates[i] >= cutoff:
                filings.append({
                    "form": form,
                    "date": dates[i],
                    "accession": accessions[i] if i < len(accessions) else "",
                    "description": descriptions[i] if i < len(descriptions) else "",
                })

        return filings

    def fetch_filing_text(self, cik: str, accession: str) -> str | None:
        """Fetch the full text of a filing.

        Args:
            cik: CIK number.
            accession: Accession number.

        Returns:
            Full text content, or None.
        """
        # Clean accession number
        acc_clean = accession.replace("-", "")
        url = f"https://www.sec.gov/Archives/edgar/data/{cik.lstrip('0')}/{acc_clean}/{accession}.txt"

        try:
            resp = self._sec_get(url)
            text_content = resp.text

            # Strip HTML tags for cleaner text
            clean = re.sub(r'<[^>]+>', ' ', text_content)
            clean = re.sub(r'\s+', ' ', clean).strip()

            # Limit to 50K chars (some filings are huge)
            if len(clean) > 50000:
                clean = clean[:50000]

            return clean
        except Exception as exc:
            log.debug("Filing fetch failed: {e}", e=str(exc))
            return None

    def extract_guidance(self, text: str, ticker: str = "") -> list[dict[str, Any]]:
        """Extract guidance/outlook phrases from filing text.

        Uses regex for fast extraction, then optionally uses LLM for
        deeper milestone extraction from the full text.

        Args:
            text: Filing text content.
            ticker: Stock ticker for context.

        Returns:
            List of extracted guidance dicts.
        """
        guidance: list[dict[str, Any]] = []

        # Phase 1: Regex extraction (fast, always runs)
        patterns = [
            (r'(?i)(?:revenue|sales)\s+(?:guidance|outlook|expect|forecast|project)[^\.\n]{5,150}', "revenue_guidance"),
            (r'(?i)(?:earnings|EPS|earnings per share)\s+(?:guidance|outlook|expect|forecast)[^\.\n]{5,150}', "eps_guidance"),
            (r'(?i)(?:margin|margins)\s+(?:guidance|outlook|expect|target|improve)[^\.\n]{5,150}', "margin_guidance"),
            (r'(?i)(?:full.?year|FY\d{2,4}|fiscal year)\s+(?:guidance|outlook|expect|revenue|earnings)[^\.\n]{5,150}', "annual_guidance"),
            (r'(?i)(?:raise|raised|increase|lower|cut|reduce|narrow|widen|reaffirm|maintain)\s+(?:guidance|outlook|forecast|target)[^\.\n]{5,150}', "guidance_change"),
            (r'(?i)(?:backlog|pipeline|bookings|orders)\s+(?:of|grew|increased|decreased|totaled)\s+\$[\d,\.]+[^\.\n]{5,100}', "backlog"),
            (r'(?i)(?:share repurchase|buyback|dividend)\s+(?:program|increase|of|totaling)\s+\$[\d,\.]+[^\.\n]{5,100}', "capital_return"),
        ]

        for pattern, category in patterns:
            matches = re.findall(pattern, text)
            for match in matches[:3]:
                guidance.append({
                    "category": category,
                    "text": match.strip()[:200],
                    "source": "regex",
                })

        # Phase 2: LLM extraction (deeper, extracts milestones)
        try:
            llm_milestones = self._llm_extract_milestones(text[:15000], ticker)
            guidance.extend(llm_milestones)
        except Exception as exc:
            log.debug("LLM milestone extraction failed: {e}", e=str(exc))

        return guidance

    def _llm_extract_milestones(self, text: str, ticker: str) -> list[dict[str, Any]]:
        """Use local LLM to extract milestones from filing text.

        Extracts product launches, revenue targets, expansion plans,
        hiring/layoff signals, M&A hints, and regulatory milestones.
        """
        from llm.router import get_llm, Tier

        client = get_llm(Tier.REASON)
        if not client or not getattr(client, "is_available", False):
            return []

        prompt = f"""Extract financial milestones from this SEC 8-K filing for {ticker}.

FILING TEXT (first 15K chars):
{text}

Return a JSON array of milestones. Each milestone has:
- "category": one of [revenue_target, product_launch, expansion, restructuring, acquisition, partnership, regulatory, guidance_change, capital_return, hiring]
- "description": one sentence describing the milestone
- "timeline": when this is expected (e.g. "Q2 2026", "second half 2026", "next 12 months")
- "magnitude": dollar amount or percentage if mentioned

ONLY extract milestones explicitly stated in the text. Do not infer or fabricate.
Return [] if no clear milestones found.
Reply with ONLY the JSON array."""

        response = client.chat(
            [{"role": "user", "content": prompt}],
            temperature=0.1,
            num_predict=1000,
        )

        if not response:
            return []

        try:
            # Extract JSON array from response
            start = response.find("[")
            end = response.rfind("]") + 1
            if start >= 0 and end > start:
                milestones = __import__("json").loads(response[start:end])
                if isinstance(milestones, list):
                    return [
                        {**m, "source": "llm"}
                        for m in milestones
                        if isinstance(m, dict) and m.get("category")
                    ][:10]  # Cap at 10
        except Exception:
            pass

        return []

    def pull(self, tickers: list[str] | None = None, days_back: int = 90) -> dict[str, Any]:
        """Pull 8-K filings and extract transcripts + guidance.

        Args:
            tickers: Override ticker list.
            days_back: How far back to look.

        Returns:
            Summary with filing and guidance counts.
        """
        if tickers is None:
            tickers = list(TICKER_CIK.keys())

        total_filings = 0
        total_guidance = 0

        for ticker in tickers:
            cik = TICKER_CIK.get(ticker)
            if not cik:
                continue

            try:
                filings = self.get_recent_8k(cik, days_back=days_back)
                if not filings:
                    continue

                for filing in filings[:5]:  # Max 5 per ticker
                    filing_date = filing["date"]
                    accession = filing["accession"]

                    # Fetch filing text
                    text_content = self.fetch_filing_text(cik, accession)
                    if not text_content or len(text_content) < 100:
                        continue

                    total_filings += 1

                    # Store raw transcript
                    try:
                        obs = datetime.strptime(filing_date, "%Y-%m-%d").date()
                    except (ValueError, TypeError):
                        continue

                    with self.engine.begin() as conn:
                        self._insert_raw(conn,
                            series_id=f"edgar:8k:{ticker}",
                            obs_date=obs,
                            value=float(len(text_content)),  # Store char count as value
                            raw_payload={
                                "ticker": ticker,
                                "cik": cik,
                                "accession": accession,
                                "description": filing.get("description", ""),
                                "text_preview": text_content[:500],
                            },
                        )

                    # Extract guidance
                    guidance = self.extract_guidance(text_content)
                    if guidance:
                        total_guidance += len(guidance)
                        with self.engine.begin() as conn:
                            for g in guidance:
                                self._insert_raw(conn,
                                    series_id=f"edgar:guidance:{ticker}:{g['category']}",
                                    obs_date=obs,
                                    value=1.0,
                                    raw_payload={
                                        "ticker": ticker,
                                        "category": g["category"],
                                        "text": g["text"],
                                        "filing_date": filing_date,
                                    },
                                )

                    log.debug("8-K {t} {d}: {n} guidance phrases",
                              t=ticker, d=filing_date, n=len(guidance))

            except Exception as exc:
                log.debug("EDGAR transcript failed for {t}: {e}", t=ticker, e=str(exc))

        log.info("EDGAR transcripts: {f} filings, {g} guidance phrases from {t} tickers",
                 f=total_filings, g=total_guidance, t=len(tickers))

        return {
            "filings_processed": total_filings,
            "guidance_extracted": total_guidance,
            "tickers": len(tickers),
        }
