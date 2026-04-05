"""
GRID Web Scraper — multi-source data collection with cross-verification.

When no dedicated API puller exists for a feature, this module scrapes the web
from multiple sources, cross-checks values, assigns trust rankings, and inserts
into raw_series with full provenance logging.

Trust Rankings:
    1 (OFFICIAL)   — Government/central bank primary source
    2 (VERIFIED)   — Well-known data aggregator (FRED, Bloomberg, Trading Economics)
    3 (AGGREGATOR) — Financial data sites (macrotrends, CEIC, investing.com)
    4 (COMMUNITY)  — Wikis, forums, crowd-sourced (Wikipedia, Reddit)
    5 (UNVERIFIED) — Single-source scrape, not yet cross-checked

Cross-verification:
    A value is promoted from UNVERIFIED to VERIFIED when ≥2 independent sources
    agree within the family conflict threshold. Disagreements are logged in
    raw_payload with full provenance for human review.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote_plus

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# ── Trust Rankings ──────────────────────────────────────────────────────────

TRUST_OFFICIAL = 1
TRUST_VERIFIED = 2
TRUST_AGGREGATOR = 3
TRUST_COMMUNITY = 4
TRUST_UNVERIFIED = 5

TRUST_LABELS = {
    1: "OFFICIAL",
    2: "VERIFIED",
    3: "AGGREGATOR",
    4: "COMMUNITY",
    5: "UNVERIFIED",
}

# ── Source Definitions ──────────────────────────────────────────────────────
# Each source defines: how to scrape, trust level, and parsing strategy.
# Sources are tried in trust-rank order (best first).

SCRAPE_SOURCES: dict[str, dict[str, Any]] = {
    "trading_economics": {
        "trust": TRUST_AGGREGATOR,
        "base_url": "https://tradingeconomics.com",
        "type": "html",
    },
    "fred_web": {
        "trust": TRUST_OFFICIAL,
        "base_url": "https://fred.stlouisfed.org",
        "type": "html",
    },
    "investing_com": {
        "trust": TRUST_AGGREGATOR,
        "base_url": "https://www.investing.com",
        "type": "html",
    },
    "yahoo_finance": {
        "trust": TRUST_AGGREGATOR,
        "base_url": "https://finance.yahoo.com",
        "type": "html",
    },
    "macrotrends": {
        "trust": TRUST_AGGREGATOR,
        "base_url": "https://www.macrotrends.net",
        "type": "html",
    },
    "wikipedia": {
        "trust": TRUST_COMMUNITY,
        "base_url": "https://en.wikipedia.org",
        "type": "html",
    },
}

# ── Conflict thresholds by family (matches resolver.py) ────────────────────

FAMILY_THRESHOLDS: dict[str, float] = {
    "vol": 0.02,
    "commodity": 0.015,
    "crypto": 0.03,
    "equity": 0.01,
    "alternative": 0.05,
    "flows": 0.02,
    "systemic": 0.02,
    "trade": 0.02,
    "rates": 0.005,
    "credit": 0.005,
    "macro": 0.01,
    "sentiment": 0.05,
    "breadth": 0.01,
    "fx": 0.005,
}

DEFAULT_THRESHOLD = 0.005


# ── Feature scrape configs ──────────────────────────────────────────────────
# Maps feature_name -> list of {source, url_path, parse_strategy, ...}
# This is the master lookup for how to find each feature's data on the web.

def _build_feature_scrape_configs() -> dict[str, list[dict[str, Any]]]:
    """Build the master config mapping features to their web scrape sources."""
    configs: dict[str, list[dict[str, Any]]] = {}

    # ── EIA Energy Data ─────────────────────────────────────────────────
    eia_features = {
        "eia_crude_price": {"desc": "WTI crude oil spot price", "search": "WTI crude oil spot price historical"},
        "eia_crude_refinery_input": {"desc": "crude refinery inputs", "search": "US crude oil refinery inputs barrels"},
        "eia_distillate_production": {"desc": "distillate fuel production", "search": "US distillate fuel production EIA"},
        "eia_distillate_stocks": {"desc": "distillate fuel stocks", "search": "US distillate fuel oil stocks EIA"},
        "eia_gasoline_production": {"desc": "motor gasoline production", "search": "US motor gasoline production EIA"},
        "eia_jet_fuel_stocks": {"desc": "jet fuel stocks", "search": "US kerosene jet fuel stocks EIA"},
        "eia_natgas_futures_1m": {"desc": "natural gas futures front month", "search": "natural gas futures front month price"},
        "eia_natgas_futures_4m": {"desc": "natural gas futures 4-month", "search": "natural gas futures 4 month price"},
        "eia_natgas_henry_hub": {"desc": "Henry Hub natural gas spot", "search": "Henry Hub natural gas spot price"},
        "eia_electricity_coal": {"desc": "coal electricity generation", "search": "US coal electricity generation monthly TWh"},
        "eia_electricity_demand": {"desc": "US electricity monthly", "search": "US total electricity generation monthly"},
        "eia_electricity_natgas": {"desc": "natural gas electricity", "search": "US natural gas electricity generation monthly"},
        "eia_electricity_nuclear": {"desc": "nuclear electricity", "search": "US nuclear electricity generation monthly"},
        "eia_electricity_solar": {"desc": "solar electricity", "search": "US solar electricity generation monthly"},
        "eia_electricity_total": {"desc": "total electricity", "search": "US total net electricity generation monthly"},
        "eia_electricity_wind": {"desc": "wind electricity", "search": "US wind electricity generation monthly"},
    }
    for fname, meta in eia_features.items():
        configs[fname] = [
            {"source": "trading_economics", "search_query": meta["search"], "parse": "table_latest"},
            {"source": "fred_web", "search_query": meta["search"] + " FRED", "parse": "fred_series"},
            {"source": "macrotrends", "search_query": meta["search"] + " macrotrends", "parse": "table_latest"},
        ]

    # ── Weather / Degree Days ───────────────────────────────────────────
    weather_cities = {
        "nyc": "New York", "chicago": "Chicago", "houston": "Houston",
        "london": "London", "tokyo": "Tokyo",
    }
    for city_key, city_name in weather_cities.items():
        for dd_type in ["hdd", "cdd"]:
            fname = f"weather_{city_key}_{dd_type}"
            full_name = "Heating" if dd_type == "hdd" else "Cooling"
            configs[fname] = [
                {"source": "trading_economics", "search_query": f"{city_name} {full_name} degree days", "parse": "table_latest"},
                {"source": "yahoo_finance", "search_query": f"{city_name} {full_name} degree days weather data", "parse": "table_latest"},
            ]

    # ── Breadth Features ────────────────────────────────────────────────
    breadth_features = {
        "sp500_adline": {"search": "S&P 500 advance decline line cumulative"},
        "sp500_adline_slope": {"search": "S&P 500 advance decline line 20 day slope"},
        "sp500_mom_12_1": {"search": "S&P 500 12 month minus 1 month momentum"},
        "sp500_mom_3m": {"search": "S&P 500 3 month momentum price change"},
        "sp500_pct_above_200ma": {"search": "percent S&P 500 stocks above 200 day moving average"},
    }
    for fname, meta in breadth_features.items():
        configs[fname] = [
            {"source": "trading_economics", "search_query": meta["search"], "parse": "table_latest"},
            {"source": "yahoo_finance", "search_query": meta["search"], "parse": "table_latest"},
            {"source": "macrotrends", "search_query": meta["search"], "parse": "table_latest"},
        ]

    # ── Credit Features ─────────────────────────────────────────────────
    credit_features = {
        "hy_spread_proxy": {"search": "high yield credit spread HYG LQD ratio"},
        "hy_spread_3m_chg": {"search": "high yield spread 3 month change"},
        "bis_credit_gap_us": {"search": "BIS credit to GDP gap United States"},
        "bis_credit_gdp_gap_us": {"search": "BIS US credit to GDP gap quarterly"},
        "bis_credit_gdp_gap_cn": {"search": "BIS China credit to GDP gap quarterly"},
    }
    for fname, meta in credit_features.items():
        configs[fname] = [
            {"source": "trading_economics", "search_query": meta["search"], "parse": "table_latest"},
            {"source": "fred_web", "search_query": meta["search"] + " FRED", "parse": "fred_series"},
        ]

    # ── Commodity ───────────────────────────────────────────────────────
    commodity_extra = {
        "copper_gold_ratio": {"search": "copper gold price ratio historical"},
        "copper_gold_slope": {"search": "copper gold ratio 3 month slope"},
        "corn_yield_forecast": {"search": "US corn yield forecast bushels per acre USDA"},
        "wheat_planted_acres": {"search": "US wheat planted acres USDA"},
    }
    for fname, meta in commodity_extra.items():
        configs[fname] = [
            {"source": "trading_economics", "search_query": meta["search"], "parse": "table_latest"},
            {"source": "macrotrends", "search_query": meta["search"], "parse": "table_latest"},
        ]

    # ── FX ──────────────────────────────────────────────────────────────
    fx_features = {
        "dxy_index": {"search": "US Dollar Index DXY historical"},
        "dxy_3m_chg": {"search": "US Dollar Index 3 month change"},
    }
    for fname, meta in fx_features.items():
        configs[fname] = [
            {"source": "trading_economics", "search_query": meta["search"], "parse": "table_latest"},
            {"source": "investing_com", "search_query": meta["search"], "parse": "table_latest"},
            {"source": "yahoo_finance", "search_query": meta["search"], "parse": "table_latest"},
        ]

    # ── Rates ───────────────────────────────────────────────────────────
    rates_features = {
        "fed_funds_3m_chg": {"search": "federal funds rate 3 month change"},
        "real_ffr": {"search": "real federal funds rate adjusted inflation"},
        "repo_volume": {"search": "NY Fed reverse repo operations volume daily"},
        "euro_bund_10y": {"search": "Germany 10 year bund yield historical"},
        "singapore_sora": {"search": "Singapore SORA overnight rate MAS"},
    }
    for fname, meta in rates_features.items():
        configs[fname] = [
            {"source": "trading_economics", "search_query": meta["search"], "parse": "table_latest"},
            {"source": "fred_web", "search_query": meta["search"] + " FRED", "parse": "fred_series"},
        ]

    # ── International Macro ─────────────────────────────────────────────
    intl_macro = {
        "brazil_credit_growth": {"search": "Brazil credit growth annual BCB"},
        "brazil_ipca_yoy": {"search": "Brazil IPCA inflation year over year"},
        "brazil_selic_rate": {"search": "Brazil SELIC interest rate"},
        "china_indpro_yoy": {"search": "China industrial production year over year"},
        "china_m2_yoy": {"search": "China M2 money supply growth year over year"},
        "china_pmi_mfg": {"search": "China manufacturing PMI NBS official"},
        "china_tss_yoy": {"search": "China total social financing year over year"},
        "ecb_m3_yoy": {"search": "ECB M3 money supply growth eurozone year over year"},
        "eci_china": {"search": "economic complexity index China"},
        "eci_usa": {"search": "economic complexity index United States"},
        # ism_pmi_mfg / ism_pmi_new_orders removed — FRED NAPM discontinued
        "conf_board_lei_slope": {"search": "Conference Board Leading Economic Index slope"},
        "korea_exports_total": {"search": "South Korea total exports monthly"},
        "korea_semi_exports": {"search": "South Korea semiconductor exports monthly"},
        "oecd_cli_china": {"search": "OECD composite leading indicator China"},
        "oecd_cli_g7": {"search": "OECD composite leading indicator G7"},
        "oecd_cli_us": {"search": "OECD composite leading indicator United States"},
        "oi_consumer_spend": {"search": "Opportunity Insights consumer spending tracker"},
        "oi_employment_overall": {"search": "Opportunity Insights employment tracker"},
        "oi_spend_high_income": {"search": "Opportunity Insights spending high income quartile"},
        "oi_spend_low_income": {"search": "Opportunity Insights spending low income quartile"},
    }
    for fname, meta in intl_macro.items():
        configs[fname] = [
            {"source": "trading_economics", "search_query": meta["search"], "parse": "table_latest"},
            {"source": "fred_web", "search_query": meta["search"] + " FRED", "parse": "fred_series"},
            {"source": "macrotrends", "search_query": meta["search"], "parse": "table_latest"},
        ]

    # ── Crypto / DeFi ───────────────────────────────────────────────────
    crypto_features = {
        "eth_total_volume": {"search": "Ethereum ETH daily trading volume"},
        "sol_total_volume": {"search": "Solana SOL daily trading volume"},
        "btc_total_volume": {"search": "Bitcoin BTC daily trading volume USD"},
        "tao_chain_market_cap": {"search": "Bittensor TAO market cap"},
        "tao_chain_total_volume": {"search": "Bittensor TAO daily trading volume"},
        "usdc_supply": {"search": "USDC total circulating supply"},
        "usdt_supply": {"search": "USDT Tether total circulating supply"},
        "polymarket_btc": {"search": "Polymarket Bitcoin price prediction market"},
    }
    for fname, meta in crypto_features.items():
        configs[fname] = [
            {"source": "yahoo_finance", "search_query": meta["search"], "parse": "table_latest"},
            {"source": "macrotrends", "search_query": meta["search"], "parse": "table_latest"},
        ]

    # DexScreener / Pump.fun features — these need specialized scraping
    dex_features = {
        "dex_sol_volume_24h": {"search": "Solana DEX total 24h volume DeFiLlama"},
        "dex_sol_liquidity": {"search": "Solana DEX total liquidity USD"},
        "dex_sol_buy_sell_ratio": {"search": "Solana DEX buy sell ratio"},
        "dex_sol_momentum_24h": {"search": "Solana DEX average 24h price change"},
        "dex_sol_txn_count_24h": {"search": "Solana DEX daily transaction count"},
        "dex_sol_boosted_tokens": {"search": "DexScreener Solana boosted tokens count"},
        "pump_new_tokens_count": {"search": "Pump.fun new token launches daily"},
        "pump_koth_mcap": {"search": "Pump.fun king of the hill market cap"},
        "pump_graduated_count": {"search": "Pump.fun graduated tokens bonding curve daily"},
        "pump_graduated_avg_mcap": {"search": "Pump.fun graduated tokens average market cap"},
        "pump_latest_avg_mcap": {"search": "Pump.fun latest launches average market cap"},
    }
    for fname, meta in dex_features.items():
        configs[fname] = [
            {"source": "yahoo_finance", "search_query": meta["search"], "parse": "table_latest"},
        ]

    # ── Volatility ──────────────────────────────────────────────────────
    vol_features = {
        "vix_1m_chg": {"search": "VIX index 1 month change"},
        "vix_3m_ratio": {"search": "VIX VIX3M ratio term structure"},
        "spy_macd": {"search": "SPY MACD signal line value"},
    }
    for fname, meta in vol_features.items():
        configs[fname] = [
            {"source": "trading_economics", "search_query": meta["search"], "parse": "table_latest"},
            {"source": "yahoo_finance", "search_query": meta["search"], "parse": "table_latest"},
        ]

    # ── Systemic Risk ───────────────────────────────────────────────────
    # OFR FSM features removed — data source permanently dead.
    # Systemic risk now covered by derived features in compute_derived_features.py.

    # ── Trade ��──────────────────────────────────────────────────────────
    trade_features = {
        "trade_volume_yoy": {"search": "US total trade volume year over year change"},
        "us_china_trade_balance": {"search": "US China bilateral trade balance monthly"},
    }
    for fname, meta in trade_features.items():
        configs[fname] = [
            {"source": "trading_economics", "search_query": meta["search"], "parse": "table_latest"},
            {"source": "fred_web", "search_query": meta["search"] + " FRED", "parse": "fred_series"},
        ]

    # ── Alternative ─────────────────────────────────────────────────────
    alt_features = {
        "viirs_us_lights": {"search": "VIIRS nighttime lights United States index"},
        "viirs_china_lights": {"search": "VIIRS nighttime lights China index"},
        "patent_velocity_software": {"search": "USPTO software patent applications monthly G06"},
        "patent_velocity_cleanenergy": {"search": "USPTO clean energy patent applications Y02"},
    }
    for fname, meta in alt_features.items():
        configs[fname] = [
            {"source": "macrotrends", "search_query": meta["search"], "parse": "table_latest"},
            {"source": "trading_economics", "search_query": meta["search"], "parse": "table_latest"},
        ]

    # ── Equity ──────────────────────────────────────────────────────────
    equity_features = {
        "brk-b_full": {"search": "Berkshire Hathaway BRK-B historical stock price"},
    }
    for fname, meta in equity_features.items():
        configs[fname] = [
            {"source": "yahoo_finance", "search_query": meta["search"], "parse": "table_latest"},
            {"source": "macrotrends", "search_query": meta["search"], "parse": "table_latest"},
        ]

    return configs


FEATURE_SCRAPE_CONFIGS = _build_feature_scrape_configs()


class WebScraperPuller(BasePuller):
    """Multi-source web scraper with cross-verification and trust ranking."""

    SOURCE_NAME = "web_scraper"
    SOURCE_CONFIG = {
        "base_url": "https://web-scraper.grid.local",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": False,
        "revision_behavior": "RARE",
        "trust_score": "LOW",
        "priority_rank": 80,  # Lower priority than dedicated API pullers
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        self._session = self._make_session()

    def _make_session(self):
        """Create a requests session with browser-like headers."""
        import requests
        s = requests.Session()
        s.headers.update({
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
        return s

    @retry_on_failure(max_attempts=2, backoff=3.0)
    def _fetch_url(self, url: str, timeout: int = 15) -> str | None:
        """Fetch a URL and return text content."""
        try:
            resp = self._session.get(url, timeout=timeout, allow_redirects=True)
            if resp.status_code == 200:
                return resp.text
            log.warning("HTTP {s} for {u}", s=resp.status_code, u=url)
            return None
        except Exception as e:
            log.warning("Fetch failed for {u}: {e}", u=url, e=str(e))
            return None

    def _web_search(self, query: str, num_results: int = 5) -> list[dict[str, str]]:
        """Search the web and return URLs + snippets.

        Uses DuckDuckGo HTML search (no API key needed).
        """
        results = []
        try:
            url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
            html = self._fetch_url(url, timeout=10)
            if not html:
                return results

            # Parse result links from DDG HTML
            link_pattern = re.compile(
                r'<a[^>]*class="result__a"[^>]*href="([^"]*)"[^>]*>(.*?)</a>',
                re.DOTALL,
            )
            snippet_pattern = re.compile(
                r'<a[^>]*class="result__snippet"[^>]*>(.*?)</a>',
                re.DOTALL,
            )

            links = link_pattern.findall(html)
            snippets = snippet_pattern.findall(html)

            for i, (href, title) in enumerate(links[:num_results]):
                snippet = snippets[i] if i < len(snippets) else ""
                # Clean HTML tags from title/snippet
                title = re.sub(r'<[^>]+>', '', title).strip()
                snippet = re.sub(r'<[^>]+>', '', snippet).strip()
                # DDG wraps URLs in a redirect — extract the actual URL
                if "uddg=" in href:
                    from urllib.parse import unquote, parse_qs, urlparse
                    parsed = urlparse(href)
                    qs = parse_qs(parsed.query)
                    href = unquote(qs.get("uddg", [href])[0])
                results.append({"url": href, "title": title, "snippet": snippet})
        except Exception as e:
            log.warning("Web search failed for '{q}': {e}", q=query, e=str(e))
        return results

    def _extract_number_from_text(self, text: str) -> float | None:
        """Extract the most likely numeric value from a text snippet."""
        # Look for numbers with optional commas, decimals, negative signs, % signs
        patterns = [
            r'[-+]?\d{1,3}(?:,\d{3})*(?:\.\d+)?',  # e.g., 1,234.56
            r'[-+]?\d+(?:\.\d+)?',                     # e.g., 1234.56
        ]
        for pattern in patterns:
            matches = re.findall(pattern, text)
            if matches:
                # Take the first reasonable number
                for m in matches:
                    try:
                        val = float(m.replace(',', ''))
                        # Skip numbers that look like years
                        if 1900 < val < 2100 and '.' not in m:
                            continue
                        return val
                    except ValueError:
                        continue
        return None

    def _scrape_for_value(
        self,
        feature_name: str,
        search_query: str,
    ) -> list[dict[str, Any]]:
        """Search the web for a feature value. Returns list of {value, source, url, trust, snippet}."""
        findings: list[dict[str, Any]] = []

        results = self._web_search(search_query)
        if not results:
            log.warning("No search results for '{q}'", q=search_query)
            return findings

        for r in results[:5]:
            # Try to extract a number from the snippet first (cheaper than full page fetch)
            val = self._extract_number_from_text(r["snippet"])
            source_domain = re.sub(r'^https?://(www\.)?', '', r["url"]).split('/')[0]

            # Assign trust based on domain
            trust = TRUST_UNVERIFIED
            if any(d in source_domain for d in ["fred.stlouisfed.org", "bls.gov", "census.gov", "eia.gov", "treasury.gov"]):
                trust = TRUST_OFFICIAL
            elif any(d in source_domain for d in ["tradingeconomics.com", "investing.com", "finance.yahoo.com", "bloomberg.com"]):
                trust = TRUST_AGGREGATOR
            elif any(d in source_domain for d in ["macrotrends.net", "ceicdata.com", "worldbank.org", "imf.org"]):
                trust = TRUST_AGGREGATOR
            elif any(d in source_domain for d in ["wikipedia.org", "reddit.com"]):
                trust = TRUST_COMMUNITY

            if val is not None:
                findings.append({
                    "value": val,
                    "source": source_domain,
                    "url": r["url"],
                    "trust": trust,
                    "trust_label": TRUST_LABELS[trust],
                    "snippet": r["snippet"][:200],
                    "extraction": "snippet",
                })

            # If no value from snippet, try fetching the page
            if val is None and trust <= TRUST_AGGREGATOR:
                time.sleep(0.5)  # Rate limit
                page_html = self._fetch_url(r["url"])
                if page_html:
                    # Extract from page — look for the search terms near numbers
                    val = self._extract_value_from_page(page_html, feature_name)
                    if val is not None:
                        findings.append({
                            "value": val,
                            "source": source_domain,
                            "url": r["url"],
                            "trust": trust,
                            "trust_label": TRUST_LABELS[trust],
                            "snippet": r["snippet"][:200],
                            "extraction": "page_parse",
                        })

        return findings

    def _extract_value_from_page(self, html: str, feature_name: str) -> float | None:
        """Extract a numeric value from a full HTML page."""
        try:
            # Remove script/style tags
            clean = re.sub(r'<(script|style)[^>]*>.*?</\1>', '', html, flags=re.DOTALL)
            # Get visible text
            text_content = re.sub(r'<[^>]+>', ' ', clean)
            text_content = re.sub(r'\s+', ' ', text_content)

            # Look for patterns like "Latest: 1234.56" or "Value: 1234.56"
            value_patterns = [
                r'(?:latest|current|last|value|price|index|rate|level)[:\s]+([+-]?\d[\d,]*\.?\d*)',
                r'(?:actual)[:\s]+([+-]?\d[\d,]*\.?\d*)',
            ]
            for pattern in value_patterns:
                m = re.search(pattern, text_content, re.IGNORECASE)
                if m:
                    try:
                        return float(m.group(1).replace(',', ''))
                    except ValueError:
                        continue
        except Exception as exc:
            log.warning("Web scraper numeric extraction failed: {e}", e=exc)
        return None

    def cross_verify(
        self,
        findings: list[dict[str, Any]],
        family: str,
    ) -> dict[str, Any]:
        """Cross-verify findings from multiple sources.

        Returns the best value with verification metadata.
        """
        if not findings:
            return {"value": None, "trust": TRUST_UNVERIFIED, "verified": False, "sources": []}

        threshold = FAMILY_THRESHOLDS.get(family, DEFAULT_THRESHOLD)

        # Sort by trust (best first)
        findings.sort(key=lambda f: f["trust"])

        # Group values that agree within threshold
        clusters: list[list[dict]] = []
        for f in findings:
            placed = False
            for cluster in clusters:
                ref = cluster[0]["value"]
                if ref != 0 and abs(f["value"] - ref) / abs(ref) <= threshold:
                    cluster.append(f)
                    placed = True
                    break
                elif ref == 0 and abs(f["value"]) < 0.01:
                    cluster.append(f)
                    placed = True
                    break
            if not placed:
                clusters.append([f])

        # Pick the cluster with the most sources (tie-break: best trust)
        clusters.sort(key=lambda c: (-len(c), c[0]["trust"]))
        best_cluster = clusters[0]

        verified = len(best_cluster) >= 2
        best_value = best_cluster[0]["value"]  # From highest-trust source in cluster
        best_trust = best_cluster[0]["trust"]

        # If verified by multiple sources, promote trust
        if verified and best_trust > TRUST_VERIFIED:
            best_trust = TRUST_VERIFIED

        return {
            "value": best_value,
            "trust": best_trust,
            "trust_label": TRUST_LABELS[best_trust],
            "verified": verified,
            "agreement_count": len(best_cluster),
            "total_sources": len(findings),
            "sources": [
                {"source": f["source"], "value": f["value"], "trust": f["trust_label"], "url": f["url"]}
                for f in findings
            ],
            "disagreements": [
                {"source": f["source"], "value": f["value"], "trust": f["trust_label"]}
                for cluster in clusters[1:]
                for f in cluster
            ] if len(clusters) > 1 else [],
        }

    def pull_feature(
        self,
        feature_name: str,
        feature_id: int,
        family: str,
        search_override: str | None = None,
    ) -> dict[str, Any]:
        """Pull a single feature by scraping the web.

        Returns dict with results and verification metadata.
        """
        log.info("Scraping feature: {f} (family={fam})", f=feature_name, fam=family)

        # Get scrape config
        configs = FEATURE_SCRAPE_CONFIGS.get(feature_name, [])

        all_findings: list[dict[str, Any]] = []

        if configs:
            # Use configured sources
            for cfg in configs:
                time.sleep(1.0)  # Rate limit between sources
                findings = self._scrape_for_value(feature_name, cfg["search_query"])
                all_findings.extend(findings)
        elif search_override:
            # Use provided search query
            all_findings = self._scrape_for_value(feature_name, search_override)
        else:
            # Auto-generate search query from feature name + description
            search_q = feature_name.replace('_', ' ')
            all_findings = self._scrape_for_value(feature_name, search_q)

        # Cross-verify
        verification = self.cross_verify(all_findings, family)

        if verification["value"] is not None:
            today = date.today()
            provenance = {
                "scraper": "web_scraper",
                "trust_rank": verification["trust"],
                "trust_label": verification["trust_label"],
                "verified": verification["verified"],
                "agreement_count": verification["agreement_count"],
                "total_sources_checked": verification["total_sources"],
                "sources": verification["sources"],
                "disagreements": verification["disagreements"],
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            }

            # Insert into raw_series
            series_id = f"WEB:{feature_name}"
            with self.engine.begin() as conn:
                # Check if already exists for today
                existing = conn.execute(
                    text(
                        "SELECT 1 FROM raw_series WHERE series_id = :sid "
                        "AND source_id = :src AND obs_date = :od LIMIT 1"
                    ),
                    {"sid": series_id, "src": self.source_id, "od": today},
                ).fetchone()

                if existing:
                    log.info("Already have today's scrape for {f}", f=feature_name)
                else:
                    self._insert_raw(
                        conn, series_id, today,
                        verification["value"],
                        raw_payload=provenance,
                        pull_status="SUCCESS",
                    )
                    log.info(
                        "Inserted {f}={v} trust={t} verified={vf} sources={n}",
                        f=feature_name,
                        v=verification["value"],
                        t=verification["trust_label"],
                        vf=verification["verified"],
                        n=verification["agreement_count"],
                    )

            # Log to scrape audit trail
            self._log_audit(feature_name, feature_id, verification)

        return {
            "feature": feature_name,
            "feature_id": feature_id,
            "family": family,
            **verification,
        }

    def _log_audit(
        self,
        feature_name: str,
        feature_id: int,
        verification: dict[str, Any],
    ) -> None:
        """Log scrape results to the supervision audit table."""
        try:
            with self.engine.begin() as conn:
                # Create audit table if not exists
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS scrape_audit (
                        id SERIAL PRIMARY KEY,
                        feature_name TEXT NOT NULL,
                        feature_id INTEGER NOT NULL,
                        scraped_at TIMESTAMPTZ DEFAULT NOW(),
                        value DOUBLE PRECISION,
                        trust_rank INTEGER NOT NULL,
                        trust_label TEXT NOT NULL,
                        verified BOOLEAN NOT NULL,
                        agreement_count INTEGER,
                        total_sources INTEGER,
                        sources JSONB,
                        disagreements JSONB,
                        human_reviewed BOOLEAN DEFAULT FALSE,
                        review_notes TEXT
                    )
                """))
                conn.execute(
                    text("""
                        INSERT INTO scrape_audit
                        (feature_name, feature_id, value, trust_rank, trust_label,
                         verified, agreement_count, total_sources, sources, disagreements)
                        VALUES (:fn, :fid, :val, :tr, :tl, :v, :ac, :ts, :src, :dis)
                    """),
                    {
                        "fn": feature_name,
                        "fid": feature_id,
                        "val": verification["value"],
                        "tr": verification["trust"],
                        "tl": verification["trust_label"],
                        "v": verification["verified"],
                        "ac": verification["agreement_count"],
                        "ts": verification["total_sources"],
                        "src": json.dumps(verification["sources"]),
                        "dis": json.dumps(verification["disagreements"]),
                    },
                )
        except Exception as e:
            log.error("Audit log failed for {f}: {e}", f=feature_name, e=str(e))

    def pull_batch(
        self,
        features: list[dict[str, Any]],
        delay_between: float = 2.0,
    ) -> list[dict[str, Any]]:
        """Pull a batch of features with rate limiting.

        Parameters:
            features: List of {name, id, family, search_override?}
            delay_between: Seconds between features.

        Returns:
            List of result dicts.
        """
        results = []
        for i, feat in enumerate(features, 1):
            log.info(
                "Scraping {i}/{n}: {f}",
                i=i, n=len(features), f=feat["name"],
            )
            try:
                result = self.pull_feature(
                    feature_name=feat["name"],
                    feature_id=feat["id"],
                    family=feat["family"],
                    search_override=feat.get("search_override"),
                )
                results.append(result)
            except Exception as e:
                log.error("Failed to scrape {f}: {e}", f=feat["name"], e=str(e))
                results.append({
                    "feature": feat["name"],
                    "feature_id": feat["id"],
                    "family": feat["family"],
                    "value": None,
                    "error": str(e),
                })

            if i < len(features):
                time.sleep(delay_between)

        return results
