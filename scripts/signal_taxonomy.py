#!/usr/bin/env python3
"""GRID — Signal Taxonomy: maps all features to canonical domains and subtypes.

Defines 10 domains and 72 subtypes. Updates feature_registry with
signal_domain and signal_subtype columns.

Run: python3 signal_taxonomy.py
"""

import os
import sys
import re

import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings
from loguru import logger as log

# 10 signal domains
DOMAINS = {
    "RATES": "Interest rates, yield curves, central bank policy",
    "CREDIT": "Credit spreads, HY/IG, corporate bond signals",
    "EQUITY": "Equity indices, breadth, momentum, sector rotation",
    "VOLATILITY": "VIX, IV, realized vol, term structure",
    "FX": "Dollar index, currency pairs, EM FX",
    "COMMODITY": "Energy, metals, agriculture, shipping",
    "SENTIMENT": "Surveys, positioning, flow, social signals",
    "MACRO": "GDP, employment, inflation, leading indicators",
    "CRYPTO": "Bitcoin, altcoins, on-chain, DeFi metrics",
    "ALTERNATIVE": "Weather, patents, GDELT, FDA, congressional, EDGAR",
}

# 72 subtypes mapped to domains
SUBTYPES = {
    # RATES (8)
    "yield_curve": "RATES",
    "fed_policy": "RATES",
    "treasury_auction": "RATES",
    "real_rates": "RATES",
    "sofr_repo": "RATES",
    "term_premium": "RATES",
    "breakeven_inflation": "RATES",
    "swap_rates": "RATES",
    # CREDIT (6)
    "hy_spread": "CREDIT",
    "ig_spread": "CREDIT",
    "credit_default": "CREDIT",
    "leverage_loan": "CREDIT",
    "credit_flow": "CREDIT",
    "muni_spread": "CREDIT",
    # EQUITY (10)
    "index_price": "EQUITY",
    "sector_etf": "EQUITY",
    "breadth": "EQUITY",
    "momentum": "EQUITY",
    "value_factor": "EQUITY",
    "earnings": "EQUITY",
    "insider_activity": "EQUITY",
    "analyst_ratings": "EQUITY",
    "fund_flow": "EQUITY",
    "market_structure": "EQUITY",
    # VOLATILITY (6)
    "vix_level": "VOLATILITY",
    "vix_term_structure": "VOLATILITY",
    "realized_vol": "VOLATILITY",
    "implied_vol": "VOLATILITY",
    "iv_skew": "VOLATILITY",
    "vol_of_vol": "VOLATILITY",
    # FX (5)
    "dollar_index": "FX",
    "major_pairs": "FX",
    "em_fx": "FX",
    "fx_volatility": "FX",
    "fx_carry": "FX",
    # COMMODITY (9)
    "crude_oil": "COMMODITY",
    "natural_gas": "COMMODITY",
    "gold_silver": "COMMODITY",
    "copper_industrial": "COMMODITY",
    "agriculture": "COMMODITY",
    "shipping_freight": "COMMODITY",
    "energy_stocks": "COMMODITY",
    "refinery": "COMMODITY",
    "electricity": "COMMODITY",
    # SENTIMENT (8)
    "fear_greed": "SENTIMENT",
    "reddit_social": "SENTIMENT",
    "news_volume": "SENTIMENT",
    "wiki_attention": "SENTIMENT",
    "prediction_market": "SENTIMENT",
    "options_flow": "SENTIMENT",
    "cot_positioning": "SENTIMENT",
    "survey": "SENTIMENT",
    # MACRO (8)
    "gdp_output": "MACRO",
    "employment": "MACRO",
    "inflation_cpi": "MACRO",
    "housing": "MACRO",
    "manufacturing": "MACRO",
    "trade_balance": "MACRO",
    "fiscal_tga": "MACRO",
    "weather_energy": "MACRO",
    # CRYPTO (6)
    "btc_price": "CRYPTO",
    "altcoin_price": "CRYPTO",
    "onchain_mempool": "CRYPTO",
    "defi_tvl": "CRYPTO",
    "stablecoin_supply": "CRYPTO",
    "crypto_volume": "CRYPTO",
    # ALTERNATIVE (6)
    "edgar_fundamentals": "ALTERNATIVE",
    "fda_events": "ALTERNATIVE",
    "patent_activity": "ALTERNATIVE",
    "gdelt_geopolitics": "ALTERNATIVE",
    "congressional_trades": "ALTERNATIVE",
    "wikidata_corporate": "ALTERNATIVE",
}

# Pattern-based classification rules: (regex_pattern, domain, subtype)
CLASSIFICATION_RULES = [
    # RATES
    (r"yld_curve|2s10s|3m10y|10y2y", "RATES", "yield_curve"),
    (r"fed_funds|ffr", "RATES", "fed_policy"),
    (r"real_ffr", "RATES", "real_rates"),
    (r"treasury_auction|treasury_bid|treasury_yield", "RATES", "treasury_auction"),
    (r"sofr|repo_rate|soma", "RATES", "sofr_repo"),
    (r"tlt|ief|shy", "RATES", "yield_curve"),
    # CREDIT
    (r"hy_spread|hyg", "CREDIT", "hy_spread"),
    (r"ig_spread|lqd", "CREDIT", "ig_spread"),
    # EQUITY
    (r"sp500(?!.*intraday)|nasdaq|russell|^eem$|iwm", "EQUITY", "index_price"),
    (r"sp500_pct|sp500_ad|adline", "EQUITY", "breadth"),
    (r"sp500_mom|momentum", "EQUITY", "momentum"),
    (r"xle|xlf|xlk|xlv|xlre|xlu|xli|xlb|xlc|xly|xlp|ita|iyr|vnq", "EQUITY", "sector_etf"),
    (r"insider|form4", "EQUITY", "insider_activity"),
    (r"earnings|pe_ratio|eps", "EQUITY", "earnings"),
    (r"analyst_rating", "EQUITY", "analyst_ratings"),
    # VOLATILITY
    (r"^vix_spot$|vixcls", "VOLATILITY", "vix_level"),
    (r"vix_3m|vix_term", "VOLATILITY", "vix_term_structure"),
    (r"vix_1m_chg", "VOLATILITY", "realized_vol"),
    (r"iv_skew|implied_vol", "VOLATILITY", "implied_vol"),
    (r"rsi|macd", "VOLATILITY", "realized_vol"),
    (r"intraday_vol|intraday_range", "VOLATILITY", "realized_vol"),
    # FX
    (r"dxy|dollar", "FX", "dollar_index"),
    # COMMODITY
    (r"crude_oil|wti|brent|cl_f", "COMMODITY", "crude_oil"),
    (r"nat_?gas|henry_hub|ng_", "COMMODITY", "natural_gas"),
    (r"gold|silver|gc_f", "COMMODITY", "gold_silver"),
    (r"copper|hg_f|copper_gold", "COMMODITY", "copper_industrial"),
    (r"gasoline|diesel|refinery", "COMMODITY", "refinery"),
    (r"eia_elec|solar_gen|wind_gen|coal_gen|nuclear_gen|natgas_gen", "COMMODITY", "electricity"),
    (r"eia_crude_stock|eia_crude_prod|eia_crude_import", "COMMODITY", "crude_oil"),
    # SENTIMENT
    (r"fear_greed|crypto_fear", "SENTIMENT", "fear_greed"),
    (r"reddit_", "SENTIMENT", "reddit_social"),
    (r"news_", "SENTIMENT", "news_volume"),
    (r"wiki_", "SENTIMENT", "wiki_attention"),
    (r"polymarket", "SENTIMENT", "prediction_market"),
    (r"pcr|put_call|max_pain|opt_vol|total_oi", "SENTIMENT", "options_flow"),
    (r"congress_trade", "SENTIMENT", "congressional_trades"),
    # MACRO
    (r"gdp|imf_gdp", "MACRO", "gdp_output"),
    (r"cpi|pce_deflator|core_pce|inflation", "MACRO", "inflation_cpi"),
    (r"ism_pmi|manufacturing|pmi", "MACRO", "manufacturing"),
    (r"conf_board|lei", "MACRO", "manufacturing"),
    (r"weather_|temp|hdd|cdd", "MACRO", "weather_energy"),
    (r"tga|fiscal", "MACRO", "fiscal_tga"),
    (r"fda_", "ALTERNATIVE", "fda_events"),
    (r"patent", "ALTERNATIVE", "patent_activity"),
    (r"portwatch", "MACRO", "trade_balance"),
    (r"wikidata", "ALTERNATIVE", "wikidata_corporate"),
    # CRYPTO
    (r"^btc(?!_rsi_av)|bitcoin|btc_market|btc_total", "CRYPTO", "btc_price"),
    (r"^eth|^sol|^tao|altcoin", "CRYPTO", "altcoin_price"),
    (r"mempool|lightning", "CRYPTO", "onchain_mempool"),
    (r"defi|tvl_", "CRYPTO", "defi_tvl"),
    (r"usdt_supply|usdc_supply|stablecoin", "CRYPTO", "stablecoin_supply"),
    (r"crypto_total|btc_dominance|eth_dominance|active_cryptos", "CRYPTO", "crypto_volume"),
    # ALTERNATIVE
    (r"edgar_agg|edgar_", "ALTERNATIVE", "edgar_fundamentals"),
    (r"gdelt_", "ALTERNATIVE", "gdelt_geopolitics"),
    (r"sec_form4", "ALTERNATIVE", "edgar_fundamentals"),
    # INTRADAY features
    (r"vwap_dev|close_location|volume_skew", "EQUITY", "market_structure"),
]


def classify_feature(name):
    """Classify a feature name into (domain, subtype) using rules."""
    name_lower = name.lower()
    for pattern, domain, subtype in CLASSIFICATION_RULES:
        if re.search(pattern, name_lower):
            return domain, subtype
    return "ALTERNATIVE", "wikidata_corporate"  # fallback


def connect():
    return psycopg2.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        dbname=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
    )


def main():
    conn = connect()
    conn.autocommit = True
    cur = conn.cursor()

    # Add columns if they don't exist
    for col in ["signal_domain", "signal_subtype"]:
        cur.execute(f"""
            DO $$ BEGIN
                ALTER TABLE feature_registry ADD COLUMN {col} TEXT;
            EXCEPTION WHEN duplicate_column THEN NULL;
            END $$;
        """)

    # Fetch all features
    cur.execute("SELECT id, name FROM feature_registry ORDER BY id")
    features = cur.fetchall()
    log.info("Classifying {n} features", n=len(features))

    domain_counts = {}
    subtype_counts = {}

    for fid, name in features:
        domain, subtype = classify_feature(name)
        cur.execute(
            "UPDATE feature_registry SET signal_domain=%s, signal_subtype=%s WHERE id=%s",
            (domain, subtype, fid),
        )
        domain_counts[domain] = domain_counts.get(domain, 0) + 1
        subtype_counts[subtype] = subtype_counts.get(subtype, 0) + 1

    log.info("=== Domain Distribution ===")
    for domain in sorted(DOMAINS.keys()):
        count = domain_counts.get(domain, 0)
        log.info("  {d}: {n} features", d=domain, n=count)

    log.info("=== Top Subtypes ===")
    for subtype, count in sorted(subtype_counts.items(), key=lambda x: -x[1])[:20]:
        log.info("  {s}: {n}", s=subtype, n=count)

    # Create index
    cur.execute("CREATE INDEX IF NOT EXISTS idx_feature_domain ON feature_registry (signal_domain)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_feature_subtype ON feature_registry (signal_subtype)")

    log.info("Signal taxonomy applied to {n} features across {d} domains",
             n=len(features), d=len(domain_counts))

    conn.close()


if __name__ == "__main__":
    main()
