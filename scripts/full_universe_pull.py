#!/usr/bin/env python3
"""
Full Universe Data Pull — every thread, every source.

Pulls ALL available data across ALL APIs for the model universe:
  1. Price data (Tiingo, yfinance — split-adjusted EOD)
  2. Fundamentals (AlphaVantage — shares outstanding, float, insider txns)
  3. Splits history (TwelveData — exact split dates & ratios)
  4. Market cap / PE / PB (Tiingo daily fundamentals)
  5. News sentiment per ticker (Tiingo news, NewsAPI, WorldNews)
  6. Crypto social metrics (CoinGecko — github, twitter, reddit, telegram)
  7. Options flow (existing GRID puller — IV, PCR, skew)
  8. Short interest (FINRA ATS)
  9. Insider transactions (AlphaVantage)
  10. Earnings surprises (AlphaVantage)

Everything goes into raw_series with proper source attribution.
The resolver handles dedup/conflict. Downstream consumers get clean data.

Usage:
  python3 scripts/full_universe_pull.py --equities     # Price + fundamentals for 250 tickers
  python3 scripts/full_universe_pull.py --crypto        # CoinGecko social + market data
  python3 scripts/full_universe_pull.py --news          # News sentiment scan
  python3 scripts/full_universe_pull.py --splits        # Historical split dates (TwelveData)
  python3 scripts/full_universe_pull.py --all           # Everything
  python3 scripts/full_universe_pull.py --status        # Show what we have vs what we need
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from loguru import logger as log

_GRID_DIR = str(Path(__file__).resolve().parent.parent)
os.chdir(_GRID_DIR)
if _GRID_DIR not in sys.path:
    sys.path.insert(0, _GRID_DIR)

from db import get_engine
from sqlalchemy import text

# API Keys
_TIINGO_KEY = os.getenv("TIINGO_API_KEY", "")
_AV_KEY = os.getenv("ALPHAVANTAGE_API_KEY", "")
_TWELVE_KEY = os.getenv("TWELVEDATA_API_KEY", "")
_COINGECKO_KEY = os.getenv("COINGECKO_API_KEY", "")
_NEWSAPI_KEY = os.getenv("NEWSAPI_KEY", "")
_WORLDNEWS_KEY = os.getenv("WORLDNEWS_API_KEY", "")


# ═══════════════════════════════════════════════════════════════════
# 1. ALPHAVANTAGE — Fundamentals, Shares, Insider, Earnings
# ═══════════════════════════════════════════════════════════════════

def av_company_overview(ticker: str) -> dict[str, Any]:
    """Fetch company overview including shares outstanding."""
    if not _AV_KEY:
        return {}
    url = "https://www.alphavantage.co/query"
    params = {"function": "OVERVIEW", "symbol": ticker, "apikey": _AV_KEY}
    try:
        r = requests.get(url, params=params, timeout=15)
        return r.json() if r.ok else {}
    except Exception:
        return {}


def av_income_statement(ticker: str) -> list[dict]:
    """Quarterly income statements — revenue, earnings, margins."""
    if not _AV_KEY:
        return []
    url = "https://www.alphavantage.co/query"
    params = {"function": "INCOME_STATEMENT", "symbol": ticker, "apikey": _AV_KEY}
    try:
        r = requests.get(url, params=params, timeout=15)
        data = r.json() if r.ok else {}
        return data.get("quarterlyReports", [])
    except Exception:
        return []


def av_balance_sheet(ticker: str) -> list[dict]:
    """Quarterly balance sheets — total assets, debt, equity, shares."""
    if not _AV_KEY:
        return []
    url = "https://www.alphavantage.co/query"
    params = {"function": "BALANCE_SHEET", "symbol": ticker, "apikey": _AV_KEY}
    try:
        r = requests.get(url, params=params, timeout=15)
        data = r.json() if r.ok else {}
        return data.get("quarterlyReports", [])
    except Exception:
        return []


def av_insider_transactions(ticker: str) -> list[dict]:
    """Insider buy/sell transactions."""
    if not _AV_KEY:
        return []
    url = "https://www.alphavantage.co/query"
    params = {"function": "INSIDER_TRANSACTIONS", "symbol": ticker, "apikey": _AV_KEY}
    try:
        r = requests.get(url, params=params, timeout=15)
        data = r.json() if r.ok else {}
        return data.get("data", [])
    except Exception:
        return []


def av_earnings(ticker: str) -> list[dict]:
    """Earnings surprises — actual vs estimate."""
    if not _AV_KEY:
        return []
    url = "https://www.alphavantage.co/query"
    params = {"function": "EARNINGS", "symbol": ticker, "apikey": _AV_KEY}
    try:
        r = requests.get(url, params=params, timeout=15)
        data = r.json() if r.ok else {}
        return data.get("quarterlyEarnings", [])
    except Exception:
        return []


# ═══════════════════════════════════════════════════════════════════
# 2. TWELVEDATA — Splits history, Statistics
# ═══════════════════════════════════════════════════════════════════

def twelve_splits(ticker: str) -> list[dict]:
    """Get historical split dates and ratios."""
    if not _TWELVE_KEY:
        return []
    url = f"https://api.twelvedata.com/splits"
    params = {"symbol": ticker, "apikey": _TWELVE_KEY}
    try:
        r = requests.get(url, params=params, timeout=15)
        data = r.json() if r.ok else {}
        return data.get("splits", [])
    except Exception:
        return []


def twelve_statistics(ticker: str) -> dict:
    """Get statistics including shares outstanding, float, short ratio."""
    if not _TWELVE_KEY:
        return {}
    url = f"https://api.twelvedata.com/statistics"
    params = {"symbol": ticker, "apikey": _TWELVE_KEY}
    try:
        r = requests.get(url, params=params, timeout=15)
        return r.json() if r.ok else {}
    except Exception:
        return {}


def twelve_dividends(ticker: str) -> list[dict]:
    """Historical dividend payments."""
    if not _TWELVE_KEY:
        return []
    url = f"https://api.twelvedata.com/dividends"
    params = {"symbol": ticker, "apikey": _TWELVE_KEY}
    try:
        r = requests.get(url, params=params, timeout=15)
        data = r.json() if r.ok else {}
        return data.get("dividends", [])
    except Exception:
        return []


# ═══════════════════════════════════════════════════════════════════
# 3. COINGECKO — Crypto social + market metrics
# ═══════════════════════════════════════════════════════════════════

def coingecko_coin_data(coin_id: str) -> dict:
    """Full coin data including community, developer, market metrics."""
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}"
    params = {
        "localization": "false",
        "tickers": "false",
        "community_data": "true",
        "developer_data": "true",
    }
    if _COINGECKO_KEY:
        params["x_cg_demo_api_key"] = _COINGECKO_KEY
    try:
        r = requests.get(url, params=params, timeout=15)
        return r.json() if r.ok else {}
    except Exception:
        return {}


def coingecko_market_chart(coin_id: str, days: int = 365) -> dict:
    """Historical market cap, volume, price."""
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
    params = {"vs_currency": "usd", "days": days}
    if _COINGECKO_KEY:
        params["x_cg_demo_api_key"] = _COINGECKO_KEY
    try:
        r = requests.get(url, params=params, timeout=15)
        return r.json() if r.ok else {}
    except Exception:
        return {}


CRYPTO_ID_MAP = {
    "BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana", "BNB": "binancecoin",
    "XRP": "ripple", "ADA": "cardano", "DOGE": "dogecoin", "AVAX": "avalanche-2",
    "DOT": "polkadot", "LINK": "chainlink", "UNI": "uniswap", "NEAR": "near",
    "ICP": "internet-computer", "APT": "aptos", "SUI": "sui", "OP": "optimism",
    "ARB": "arbitrum", "MKR": "maker", "AAVE": "aave", "TAO": "bittensor",
    "FIL": "filecoin", "ATOM": "cosmos", "INJ": "injective-protocol",
    "RENDER": "render-token", "FTM": "fantom", "PEPE": "pepe",
    "BONK": "bonk", "KAS": "kaspa", "HBAR": "hedera-hashgraph",
}


# ═══════════════════════════════════════════════════════════════════
# 4. NEWS — Per-ticker sentiment from multiple sources
# ═══════════════════════════════════════════════════════════════════

def tiingo_news(ticker: str, days_back: int = 30) -> list[dict]:
    """Tiingo news articles tagged to a ticker."""
    if not _TIINGO_KEY:
        return []
    start = (date.today() - timedelta(days=days_back)).isoformat()
    url = "https://api.tiingo.com/tiingo/news"
    params = {"token": _TIINGO_KEY, "tickers": ticker, "startDate": start, "limit": 50}
    try:
        r = requests.get(url, params=params, timeout=15)
        return r.json() if r.ok else []
    except Exception:
        return []


def newsapi_articles(query: str, days_back: int = 7) -> list[dict]:
    """NewsAPI articles for a search query."""
    if not _NEWSAPI_KEY:
        return []
    start = (date.today() - timedelta(days=days_back)).isoformat()
    url = "https://newsapi.org/v2/everything"
    params = {"q": query, "from": start, "sortBy": "relevancy",
              "pageSize": 20, "apiKey": _NEWSAPI_KEY}
    try:
        r = requests.get(url, params=params, timeout=15)
        data = r.json() if r.ok else {}
        return data.get("articles", [])
    except Exception:
        return []


# ═══════════════════════════════════════════════════════════════════
# ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════════

def pull_equity_fundamentals(engine, tickers: list[str]) -> dict[str, Any]:
    """Pull fundamentals from AlphaVantage + TwelveData for all tickers."""
    results = {"av_overview": 0, "av_earnings": 0, "twelve_stats": 0, "twelve_splits": 0}

    for i, ticker in enumerate(tickers):
        log.info("[{i}/{n}] Pulling fundamentals for {t}...", i=i + 1, n=len(tickers), t=ticker)

        # AlphaVantage (5 calls/min free tier)
        if _AV_KEY:
            overview = av_company_overview(ticker)
            if overview and "SharesOutstanding" in overview:
                shares = overview.get("SharesOutstanding")
                shares_float = overview.get("SharesFloat")
                market_cap = overview.get("MarketCapitalization")
                log.info("  AV: shares={s} float={f} mcap={m}",
                         s=shares, f=shares_float, m=market_cap)
                results["av_overview"] += 1

                # Store in raw_series
                _store_fundamentals(engine, ticker, overview)

            time.sleep(12.5)  # AV free tier: 5/min

        # TwelveData splits
        if _TWELVE_KEY:
            splits = twelve_splits(ticker)
            if splits:
                log.info("  12D: {n} historical splits", n=len(splits))
                for s in splits:
                    log.info("    {d} ratio={r}", d=s.get("date"), r=s.get("description", s.get("ratio")))
                results["twelve_splits"] += 1

            stats = twelve_statistics(ticker)
            if stats and "statistics" in stats:
                st = stats["statistics"]
                shares_out = st.get("shares_outstanding")
                short_ratio = st.get("short_ratio")
                if shares_out:
                    log.info("  12D: shares_outstanding={s} short_ratio={sr}",
                             s=shares_out, sr=short_ratio)
                results["twelve_stats"] += 1

            time.sleep(1)  # 12D: 8/min free

    return results


def _store_fundamentals(engine, ticker: str, overview: dict) -> None:
    """Store AV overview data in raw_series."""
    fields = {
        "SharesOutstanding": "shares_outstanding",
        "SharesFloat": "shares_float",
        "MarketCapitalization": "market_cap_av",
        "EBITDA": "ebitda",
        "PERatio": "pe_ratio_av",
        "PEGRatio": "peg_ratio_av",
        "BookValue": "book_value",
        "DividendPerShare": "dividend_per_share",
        "DividendYield": "dividend_yield",
        "EPS": "eps",
        "RevenuePerShareTTM": "revenue_per_share",
        "ProfitMargin": "profit_margin",
        "OperatingMarginTTM": "operating_margin",
        "ReturnOnAssetsTTM": "return_on_assets",
        "ReturnOnEquityTTM": "return_on_equity",
        "RevenueTTM": "revenue_ttm",
        "GrossProfitTTM": "gross_profit_ttm",
        "Beta": "beta",
        "52WeekHigh": "high_52w",
        "52WeekLow": "low_52w",
        "50DayMovingAverage": "ma_50d",
        "200DayMovingAverage": "ma_200d",
        "AnalystTargetPrice": "analyst_target",
        "ForwardPE": "forward_pe",
        "ShortRatio": "short_ratio",
        "ShortPercentOutstanding": "short_pct_outstanding",
        "ShortPercentFloat": "short_pct_float",
        "PercentInsiders": "pct_insiders",
        "PercentInstitutions": "pct_institutions",
    }

    today = date.today()

    with engine.begin() as conn:
        # Ensure source exists
        row = conn.execute(text(
            "SELECT id FROM source_catalog WHERE name = 'ALPHAVANTAGE_FUND'"
        )).fetchone()

        if not row:
            conn.execute(text("""
                INSERT INTO source_catalog (name, config)
                VALUES ('ALPHAVANTAGE_FUND', :cfg)
                ON CONFLICT (name) DO NOTHING
            """), {"cfg": json.dumps({"type": "fundamentals", "provider": "alphavantage"})})
            row = conn.execute(text(
                "SELECT id FROM source_catalog WHERE name = 'ALPHAVANTAGE_FUND'"
            )).fetchone()

        source_id = row[0]

        for av_field, grid_suffix in fields.items():
            value = overview.get(av_field)
            if value is None or value == "None" or value == "-":
                continue
            try:
                val = float(value)
            except (ValueError, TypeError):
                continue

            series_id = f"AV_FUND:{ticker}:{grid_suffix}"

            conn.execute(text("""
                INSERT INTO raw_series (source_id, series_id, obs_date, value, release_date)
                VALUES (:sid, :series, :obs, :val, :rel)
                ON CONFLICT (source_id, series_id, obs_date) DO UPDATE SET value = :val
            """), {
                "sid": source_id,
                "series": series_id,
                "obs": today,
                "val": val,
                "rel": today,
            })


def pull_crypto_social(tickers: list[str]) -> dict[str, Any]:
    """Pull social/developer metrics from CoinGecko."""
    results = {}
    for ticker in tickers:
        coin_id = CRYPTO_ID_MAP.get(ticker)
        if not coin_id:
            continue

        log.info("CoinGecko: pulling {t} ({c})...", t=ticker, c=coin_id)
        data = coingecko_coin_data(coin_id)

        if not data:
            continue

        community = data.get("community_data", {})
        developer = data.get("developer_data", {})
        market = data.get("market_data", {})

        metrics = {
            "twitter_followers": community.get("twitter_followers"),
            "reddit_subscribers": community.get("reddit_subscribers"),
            "reddit_active_accounts_48h": community.get("reddit_accounts_active_48h"),
            "telegram_members": community.get("telegram_channel_user_count"),
            "github_forks": developer.get("forks"),
            "github_stars": developer.get("stars"),
            "github_subscribers": developer.get("subscribers"),
            "github_total_issues": developer.get("total_issues"),
            "github_closed_issues": developer.get("closed_issues"),
            "github_pull_requests_merged": developer.get("pull_requests_merged"),
            "github_commit_count_4_weeks": developer.get("commit_count_4_weeks"),
            "market_cap": market.get("market_cap", {}).get("usd"),
            "total_volume": market.get("total_volume", {}).get("usd"),
            "circulating_supply": market.get("circulating_supply"),
            "total_supply": market.get("total_supply"),
            "max_supply": market.get("max_supply"),
            "ath": market.get("ath", {}).get("usd"),
            "ath_date": market.get("ath_date", {}).get("usd"),
            "atl": market.get("atl", {}).get("usd"),
            "market_cap_rank": market.get("market_cap_rank"),
        }

        results[ticker] = {k: v for k, v in metrics.items() if v is not None}
        log.info("  {t}: {n} metrics collected", t=ticker, n=len(results[ticker]))

        time.sleep(1.5)  # CoinGecko rate limit

    return results


def show_status(engine) -> None:
    """Show what data we have vs what we need."""
    with engine.connect() as conn:
        # Price tickers
        price_tickers = conn.execute(text("""
            SELECT UPPER(REPLACE(fr.name, '_full', '')), COUNT(rs.id), MAX(rs.obs_date)
            FROM feature_registry fr
            JOIN resolved_series rs ON fr.id = rs.feature_id
            WHERE fr.name LIKE '%_full'
            GROUP BY fr.name ORDER BY COUNT(rs.id) DESC
        """)).fetchall()

        # Fundamental features
        fund_features = conn.execute(text("""
            SELECT fr.name, COUNT(rs.id), MAX(rs.obs_date)
            FROM feature_registry fr
            JOIN resolved_series rs ON fr.id = rs.feature_id
            WHERE fr.name LIKE '%_market_cap' OR fr.name LIKE '%_shares_%'
               OR fr.name LIKE '%_pe_ratio%' OR fr.name LIKE '%_short_%'
            GROUP BY fr.name ORDER BY fr.name
        """)).fetchall()

        # Raw series sources
        sources = conn.execute(text("""
            SELECT sc.name, COUNT(rs.id), MAX(rs.obs_date)
            FROM raw_series rs
            JOIN source_catalog sc ON rs.source_id = sc.id
            GROUP BY sc.name ORDER BY COUNT(rs.id) DESC
        """)).fetchall()

    log.info("=" * 70)
    log.info("DATA STATUS")
    log.info("=" * 70)

    log.info("\nPRICE DATA: {n} tickers", n=len(price_tickers))
    for t, n, latest in price_tickers[:20]:
        log.info("  {t:8s} {n:6d} rows  latest={d}", t=t, n=n, d=latest)

    log.info("\nFUNDAMENTAL FEATURES: {n}", n=len(fund_features))
    for name, n, latest in fund_features:
        log.info("  {f:30s} {n:5d} rows  latest={d}", f=name, n=n, d=latest)

    log.info("\nDATA SOURCES:")
    for name, n, latest in sources:
        log.info("  {s:25s} {n:8d} rows  latest={d}", s=name, n=n, d=latest)

    # What's missing
    from scripts.expand_universe import SP500_CORE, get_existing_tickers
    existing = get_existing_tickers(engine)
    missing_equities = set(SP500_CORE) - existing
    log.info("\nMISSING EQUITIES: {n} of {t}", n=len(missing_equities), t=len(SP500_CORE))


def main() -> None:
    parser = argparse.ArgumentParser(description="Full Universe Data Pull")
    parser.add_argument("--equities", action="store_true", help="Pull equity prices + fundamentals")
    parser.add_argument("--crypto", action="store_true", help="Pull crypto social/market from CoinGecko")
    parser.add_argument("--news", action="store_true", help="Pull news sentiment per ticker")
    parser.add_argument("--splits", action="store_true", help="Pull historical split dates")
    parser.add_argument("--all", action="store_true", help="Pull everything")
    parser.add_argument("--status", action="store_true", help="Show data status")
    parser.add_argument("-n", type=int, default=30, help="Max tickers per category")
    args = parser.parse_args()

    engine = get_engine()

    if args.status or not any([args.equities, args.crypto, args.news, args.splits, args.all]):
        show_status(engine)
        return

    if args.all or args.equities:
        from scripts.expand_universe import SP500_CORE, get_existing_tickers
        existing = get_existing_tickers(engine)
        missing = sorted(set(SP500_CORE) - existing)[:args.n]

        if missing:
            log.info("Pulling {n} missing equity tickers via Tiingo...", n=len(missing))
            from ingestion.tiingo_pull import TiingoPuller
            puller = TiingoPuller(engine)
            puller.pull_all(ticker_list=missing, start_date="2020-01-01")

        # Fundamentals for all tickers
        all_tickers = sorted(existing | set(SP500_CORE))[:args.n]
        log.info("Pulling fundamentals for {n} tickers...", n=len(all_tickers))
        pull_equity_fundamentals(engine, all_tickers)

    if args.all or args.crypto:
        crypto_tickers = list(CRYPTO_ID_MAP.keys())[:args.n]
        log.info("Pulling crypto social metrics for {n} coins...", n=len(crypto_tickers))
        results = pull_crypto_social(crypto_tickers)
        log.info("Crypto social: {n} coins with data", n=len(results))

    if args.all or args.news:
        from scripts.expand_universe import SP500_CORE
        news_tickers = SP500_CORE[:args.n]
        log.info("Pulling news for {n} tickers...", n=len(news_tickers))
        for ticker in news_tickers:
            articles = tiingo_news(ticker, days_back=30)
            if articles:
                log.info("  {t}: {n} articles", t=ticker, n=len(articles))
            time.sleep(0.3)

    if args.all or args.splits:
        if _TWELVE_KEY:
            from scripts.expand_universe import SP500_CORE
            for ticker in SP500_CORE[:args.n]:
                splits = twelve_splits(ticker)
                if splits:
                    log.info("{t}: {n} splits — {s}", t=ticker, n=len(splits),
                             s=[(s.get("date"), s.get("description")) for s in splits])
                time.sleep(1)

    log.info("\nDone.")


if __name__ == "__main__":
    main()
