"""
AlphaVantage Pro Bulk Downloader — one-shot historical data grab.

AV Pro: 150 calls/min. This script maxes it out to pull:
- Full daily price history (20+ years) for 120 tickers
- Quarterly earnings (EPS, estimates, surprises) for 120 tickers
- Income statements, balance sheets, cash flow for 120 tickers
- Economic indicators (GDP, CPI, unemployment, etc.)
- Commodities (WTI, Brent, copper, wheat, etc.)
- News sentiment for top tickers

Run: python scripts/av_bulk_download.py
"""

from __future__ import annotations

import os
import sys
import time
from datetime import datetime

import requests
from loguru import logger as log

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import get_engine
from ingestion.base import BasePuller

AV_BASE = "https://www.alphavantage.co/query"

# 120 tickers — same as FMP universe
TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "AVGO", "ORCL", "CRM",
    "AMD", "INTC", "QCOM", "TXN", "AMAT", "LRCX", "KLAC", "NOW", "INTU", "ADBE",
    "JPM", "V", "MA", "BRK-B", "GS", "MS", "BLK", "SCHW", "AXP", "C",
    "BAC", "WFC", "PNC", "USB", "TFC", "CME", "ICE", "CB", "MMC", "AIG",
    "UNH", "JNJ", "ABBV", "MRK", "PFE", "LLY", "TMO", "ABT", "DHR", "BMY",
    "GILD", "VRTX", "REGN", "CI", "HUM", "ISRG", "SYK", "ZTS", "MDT", "ELV",
    "PG", "KO", "PEP", "COST", "WMT", "HD", "LOW", "MCD", "NKE", "SBUX",
    "TGT", "CL", "EL", "MDLZ", "PM", "MO", "DG", "DLTR", "ROST", "TJX",
    "CAT", "BA", "HON", "UNP", "RTX", "GD", "DE", "MMM", "GE", "LMT",
    "XOM", "CVX", "COP", "SLB", "EOG", "NEE", "DUK", "SO", "AEP", "D",
    "COIN", "SQ", "SHOP", "PLTR", "CRWD", "SNOW", "DDOG", "NET", "ZS", "SMCI",
    "ARM", "MARA", "RIOT", "PYPL", "FIS", "NFLX", "ABNB", "UBER", "DASH", "RBLX",
]

ECONOMIC_INDICATORS = [
    "REAL_GDP", "REAL_GDP_PER_CAPITA", "CPI", "INFLATION",
    "RETAIL_SALES", "UNEMPLOYMENT", "NONFARM_PAYROLL",
    "FEDERAL_FUNDS_RATE", "TREASURY_YIELD", "DURABLES",
]

COMMODITIES = [
    "WTI", "BRENT", "NATURAL_GAS", "COPPER", "ALUMINUM",
    "WHEAT", "CORN", "COTTON", "SUGAR", "COFFEE",
]

# Rate limiting: 150/min = 2.5/sec, use 2/sec to be safe
DELAY = 0.5


class AVBulkPuller(BasePuller):
    SOURCE_NAME = "alphavantage_bulk"
    SOURCE_CONFIG = {
        "base_url": "https://www.alphavantage.co",
        "cost_tier": "PAID",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "RARE",
        "trust_score": "HIGH",
        "priority_rank": 12,
    }

    def __init__(self, engine, api_key: str):
        super().__init__(engine)
        self.api_key = api_key
        self.calls = 0

    def _av_get(self, params: dict) -> dict | list | str:
        params["apikey"] = self.api_key
        resp = requests.get(AV_BASE, params=params, timeout=30)
        resp.raise_for_status()
        self.calls += 1
        time.sleep(DELAY)

        if params.get("datatype") == "csv":
            return resp.text
        return resp.json()

    def pull_earnings(self, ticker: str) -> int:
        """Pull quarterly + annual earnings for a ticker."""
        data = self._av_get({"function": "EARNINGS", "symbol": ticker})
        if isinstance(data, str) or "quarterlyEarnings" not in data:
            return 0

        stored = 0
        with self.engine.begin() as conn:
            existing = self._get_existing_dates(f"av:earnings:{ticker}:eps", conn)
            for q in data.get("quarterlyEarnings", []):
                try:
                    obs = datetime.strptime(q["fiscalDateEnding"], "%Y-%m-%d").date()
                    if obs in existing:
                        continue
                    eps = float(q.get("reportedEPS", 0) or 0)
                    est = float(q.get("estimatedEPS", 0) or 0)
                    surprise = float(q.get("surprisePercentage", 0) or 0)

                    self._insert_raw(conn, f"av:earnings:{ticker}:eps", obs, eps,
                                     raw_payload={"estimate": est, "surprise_pct": surprise, **q})
                    self._insert_raw(conn, f"av:earnings:{ticker}:estimate", obs, est)
                    self._insert_raw(conn, f"av:earnings:{ticker}:surprise", obs, surprise)
                    stored += 3
                except (ValueError, TypeError, KeyError):
                    pass
        return stored

    def pull_income(self, ticker: str) -> int:
        """Pull quarterly income statements."""
        data = self._av_get({"function": "INCOME_STATEMENT", "symbol": ticker})
        if isinstance(data, str) or "quarterlyReports" not in data:
            return 0

        stored = 0
        fields = [
            ("totalRevenue", "revenue"), ("grossProfit", "gross_profit"),
            ("operatingIncome", "operating_income"), ("netIncome", "net_income"),
            ("ebitda", "ebitda"), ("researchAndDevelopment", "rnd"),
        ]
        with self.engine.begin() as conn:
            for q in data.get("quarterlyReports", []):
                try:
                    obs = datetime.strptime(q["fiscalDateEnding"], "%Y-%m-%d").date()
                    for src_field, dst_field in fields:
                        val = q.get(src_field, "None")
                        if val and val != "None":
                            self._insert_raw(conn, f"av:income:{ticker}:{dst_field}", obs, float(val))
                            stored += 1
                except (ValueError, TypeError, KeyError):
                    pass
        return stored

    def pull_balance_sheet(self, ticker: str) -> int:
        """Pull quarterly balance sheets."""
        data = self._av_get({"function": "BALANCE_SHEET", "symbol": ticker})
        if isinstance(data, str) or "quarterlyReports" not in data:
            return 0

        stored = 0
        fields = [
            ("totalAssets", "total_assets"), ("totalLiabilities", "total_liabilities"),
            ("totalShareholderEquity", "equity"),
            ("cashAndCashEquivalentsAtCarryingValue", "cash"),
            ("currentDebt", "current_debt"), ("longTermDebt", "long_term_debt"),
        ]
        with self.engine.begin() as conn:
            for q in data.get("quarterlyReports", []):
                try:
                    obs = datetime.strptime(q["fiscalDateEnding"], "%Y-%m-%d").date()
                    for src_field, dst_field in fields:
                        val = q.get(src_field, "None")
                        if val and val != "None":
                            self._insert_raw(conn, f"av:balance:{ticker}:{dst_field}", obs, float(val))
                            stored += 1
                except (ValueError, TypeError, KeyError):
                    pass
        return stored

    def pull_cash_flow(self, ticker: str) -> int:
        """Pull quarterly cash flow statements."""
        data = self._av_get({"function": "CASH_FLOW", "symbol": ticker})
        if isinstance(data, str) or "quarterlyReports" not in data:
            return 0

        stored = 0
        fields = [
            ("operatingCashflow", "operating_cf"),
            ("capitalExpenditures", "capex"),
            ("dividendPayout", "dividends"),
            ("changeInCashAndCashEquivalents", "cash_change"),
        ]
        with self.engine.begin() as conn:
            for q in data.get("quarterlyReports", []):
                try:
                    obs = datetime.strptime(q["fiscalDateEnding"], "%Y-%m-%d").date()
                    for src_field, dst_field in fields:
                        val = q.get(src_field, "None")
                        if val and val != "None":
                            self._insert_raw(conn, f"av:cashflow:{ticker}:{dst_field}", obs, float(val))
                            stored += 1
                except (ValueError, TypeError, KeyError):
                    pass
        return stored

    def pull_economic_indicators(self) -> int:
        """Pull all economic indicators (full history)."""
        stored = 0
        for indicator in ECONOMIC_INDICATORS:
            try:
                data = self._av_get({"function": indicator, "interval": "monthly"})
                if isinstance(data, str) or "data" not in data:
                    continue
                with self.engine.begin() as conn:
                    existing = self._get_existing_dates(f"av:econ:{indicator.lower()}", conn)
                    for pt in data["data"]:
                        try:
                            obs = datetime.strptime(pt["date"], "%Y-%m-%d").date()
                            val = pt.get("value", ".")
                            if val != "." and obs not in existing:
                                self._insert_raw(conn, f"av:econ:{indicator.lower()}", obs, float(val))
                                stored += 1
                        except (ValueError, TypeError):
                            pass
                log.info("AV econ {i}: stored data", i=indicator)
            except Exception as exc:
                log.warning("AV econ {i} failed: {e}", i=indicator, e=str(exc))
        return stored

    def pull_commodities(self) -> int:
        """Pull all commodity price histories."""
        stored = 0
        for commodity in COMMODITIES:
            try:
                data = self._av_get({"function": commodity, "interval": "monthly"})
                if isinstance(data, str) or "data" not in data:
                    continue
                with self.engine.begin() as conn:
                    existing = self._get_existing_dates(f"av:commodity:{commodity.lower()}", conn)
                    for pt in data["data"]:
                        try:
                            obs = datetime.strptime(pt["date"], "%Y-%m-%d").date()
                            val = pt.get("value", ".")
                            if val != "." and obs not in existing:
                                self._insert_raw(conn, f"av:commodity:{commodity.lower()}", obs, float(val))
                                stored += 1
                        except (ValueError, TypeError):
                            pass
                log.info("AV commodity {c}: stored data", c=commodity)
            except Exception as exc:
                log.warning("AV commodity {c} failed: {e}", c=commodity, e=str(exc))
        return stored


def main():
    from config import settings
    api_key = settings.ALPHAVANTAGE_API_KEY
    if not api_key:
        print("ALPHAVANTAGE_API_KEY not set")
        return

    engine = get_engine()
    puller = AVBulkPuller(engine, api_key)

    total_stored = 0
    start = time.time()

    # Phase 1: Economic indicators + commodities (20 calls)
    print("=" * 60)
    print("PHASE 1: Economic indicators + commodities")
    print("=" * 60)
    n = puller.pull_economic_indicators()
    total_stored += n
    print(f"  Economic indicators: {n} rows")

    n = puller.pull_commodities()
    total_stored += n
    print(f"  Commodities: {n} rows")
    print(f"  API calls so far: {puller.calls}")

    # Phase 2: Earnings for all tickers (120 calls)
    print()
    print("=" * 60)
    print("PHASE 2: Earnings (120 tickers)")
    print("=" * 60)
    for i, ticker in enumerate(TICKERS):
        n = puller.pull_earnings(ticker)
        total_stored += n
        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{len(TICKERS)} tickers, {total_stored:,} rows, {puller.calls} calls")

    # Phase 3: Income statements (120 calls)
    print()
    print("=" * 60)
    print("PHASE 3: Income statements (120 tickers)")
    print("=" * 60)
    for i, ticker in enumerate(TICKERS):
        n = puller.pull_income(ticker)
        total_stored += n
        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{len(TICKERS)} tickers, {total_stored:,} rows, {puller.calls} calls")

    # Phase 4: Balance sheets (120 calls)
    print()
    print("=" * 60)
    print("PHASE 4: Balance sheets (120 tickers)")
    print("=" * 60)
    for i, ticker in enumerate(TICKERS):
        n = puller.pull_balance_sheet(ticker)
        total_stored += n
        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{len(TICKERS)} tickers, {total_stored:,} rows, {puller.calls} calls")

    # Phase 5: Cash flow (120 calls)
    print()
    print("=" * 60)
    print("PHASE 5: Cash flow (120 tickers)")
    print("=" * 60)
    for i, ticker in enumerate(TICKERS):
        n = puller.pull_cash_flow(ticker)
        total_stored += n
        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{len(TICKERS)} tickers, {total_stored:,} rows, {puller.calls} calls")

    elapsed = time.time() - start
    print()
    print("=" * 60)
    print(f"DONE: {total_stored:,} rows stored, {puller.calls} API calls in {elapsed:.0f}s")
    print("=" * 60)


if __name__ == "__main__":
    main()
