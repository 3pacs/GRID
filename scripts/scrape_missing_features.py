#!/usr/bin/env python3
"""
Scrape missing features — worker script for background agents.

Usage:
    python scrape_missing_features.py --batch <batch_name>
    python scrape_missing_features.py --feature <feature_name>
    python scrape_missing_features.py --all
    python scrape_missing_features.py --list  # just list missing features

Each run:
1. Identifies features with zero data in resolved_series
2. Scrapes web sources with cross-verification
3. Inserts into raw_series with trust rankings
4. Runs resolver to promote to resolved_series
5. Logs everything to scrape_audit table
"""

from __future__ import annotations

import argparse
import json
import sys
import os

# Add grid package to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.chdir(os.path.join(os.path.dirname(__file__), ".."))

from loguru import logger as log
from sqlalchemy import text

from config import settings
from db import get_engine
from ingestion.web_scraper import WebScraperPuller, FEATURE_SCRAPE_CONFIGS, TRUST_LABELS
from normalization.entity_map import SEED_MAPPINGS
from normalization.resolver import Resolver


def get_zero_data_features(engine) -> list[dict]:
    """Get all features with zero rows in resolved_series."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT fr.id, fr.name, fr.family, fr.description
            FROM feature_registry fr
            LEFT JOIN resolved_series rs ON fr.id = rs.feature_id
            WHERE fr.deprecated_at IS NULL
            GROUP BY fr.id, fr.name, fr.family, fr.description
            HAVING COUNT(rs.id) = 0
            ORDER BY fr.family, fr.name
        """)).fetchall()
    return [{"id": r[0], "name": r[1], "family": r[2], "description": r[3]} for r in rows]


def ensure_entity_mappings(engine, features: list[dict]) -> None:
    """Ensure WEB: series_id -> feature_name mappings exist for scraped features."""
    for feat in features:
        series_id = f"WEB:{feat['name']}"
        if series_id not in SEED_MAPPINGS:
            SEED_MAPPINGS[series_id] = feat["name"]


def resolve_scraped_data(engine) -> int:
    """Run the resolver on web-scraped data to populate resolved_series."""
    resolver = Resolver(db_engine=engine)
    result = resolver.resolve_pending()
    log.info(
        "Resolver: {r} resolved, {c} conflicts, {e} errors",
        r=result.get("resolved", 0),
        c=result.get("conflicts_found", 0),
        e=result.get("errors", 0),
    )
    return result.get("resolved", 0)


# ── Batch Definitions ───────────────────────────────────────────────────────
# Group features by logical source for efficient scraping

BATCH_DEFINITIONS = {
    "eia_energy": [
        "eia_crude_price", "eia_crude_refinery_input", "eia_distillate_production",
        "eia_distillate_stocks", "eia_gasoline_production", "eia_jet_fuel_stocks",
        "eia_natgas_futures_1m", "eia_natgas_futures_4m", "eia_natgas_henry_hub",
        "eia_electricity_coal", "eia_electricity_demand", "eia_electricity_natgas",
        "eia_electricity_nuclear", "eia_electricity_solar", "eia_electricity_total",
        "eia_electricity_wind",
    ],
    "weather": [
        "weather_nyc_hdd", "weather_nyc_cdd", "weather_chicago_hdd", "weather_chicago_cdd",
        "weather_houston_hdd", "weather_houston_cdd", "weather_london_hdd", "weather_london_cdd",
        "weather_tokyo_hdd", "weather_tokyo_cdd",
    ],
    "breadth": [
        "sp500_adline", "sp500_adline_slope", "sp500_mom_12_1", "sp500_mom_3m",
        "sp500_pct_above_200ma",
    ],
    "credit": [
        "hy_spread_proxy", "hy_spread_3m_chg", "bis_credit_gap_us",
        "bis_credit_gdp_gap_us", "bis_credit_gdp_gap_cn",
    ],
    "commodity": [
        "copper_gold_ratio", "copper_gold_slope", "corn_yield_forecast",
        "wheat_planted_acres",
    ],
    "fx": ["dxy_index", "dxy_3m_chg"],
    "rates": [
        "fed_funds_3m_chg", "real_ffr", "repo_volume",
        "euro_bund_10y", "singapore_sora",
    ],
    "macro_intl": [
        "brazil_credit_growth", "brazil_ipca_yoy", "brazil_selic_rate",
        "china_indpro_yoy", "china_m2_yoy", "china_pmi_mfg", "china_tss_yoy",
        "ecb_m3_yoy", "eci_china", "eci_usa",
        "korea_exports_total", "korea_semi_exports",
        "oecd_cli_china", "oecd_cli_g7", "oecd_cli_us",
    ],
    "macro_us": [
        "conf_board_lei_slope",  # ism_pmi_mfg/new_orders removed — FRED NAPM discontinued
        "oi_consumer_spend", "oi_employment_overall",
        "oi_spend_high_income", "oi_spend_low_income",
    ],
    "crypto_main": [
        "eth_total_volume", "sol_total_volume", "btc_total_volume",
        "tao_chain_market_cap", "tao_chain_total_volume",
        "usdc_supply", "usdt_supply", "polymarket_btc",
    ],
    "crypto_defi": [
        "dex_sol_volume_24h", "dex_sol_liquidity", "dex_sol_buy_sell_ratio",
        "dex_sol_momentum_24h", "dex_sol_txn_count_24h", "dex_sol_boosted_tokens",
        "pump_new_tokens_count", "pump_koth_mcap", "pump_graduated_count",
        "pump_graduated_avg_mcap", "pump_latest_avg_mcap",
    ],
    "volatility": ["vix_1m_chg", "vix_3m_ratio", "spy_macd"],
    "systemic": [],  # OFR FSM features removed — data source permanently dead
    "trade": ["trade_volume_yoy", "us_china_trade_balance"],
    "alternative": [
        "viirs_us_lights", "viirs_china_lights",
        "patent_velocity_software", "patent_velocity_cleanenergy",
    ],
    "equity": ["brk-b_full"],
    "analyst_ratings": [
        "ci_analyst_buy", "ci_analyst_sell", "ci_analyst_hold",
        "cmcsa_analyst_buy", "cmcsa_analyst_sell", "cmcsa_analyst_hold",
        "dvn_analyst_buy", "dvn_analyst_sell", "dvn_analyst_hold",
        "eog_analyst_buy", "eog_analyst_sell", "eog_analyst_hold",
        "gd_analyst_buy", "gd_analyst_sell", "gd_analyst_hold",
        "intc_analyst_buy", "intc_analyst_sell", "intc_analyst_hold",
        "pypl_analyst_buy", "pypl_analyst_sell", "pypl_analyst_hold",
        "rtx_analyst_buy", "rtx_analyst_sell", "rtx_analyst_hold",
    ],
    "gdelt": ["gdelt_avg_tone"],
    "worldnews": [
        "wn_climate_article_count", "wn_health_article_count",
        "wn_energy_article_count", "wn_financial_article_count",
        "wn_labor_article_count", "wn_breadth", "wn_total_volume", "wn_fear_index",
        "wn_climate_global_spread", "wn_climate_sentiment_avg",
        "wn_energy_global_spread", "wn_energy_sentiment_avg",
        "wn_financial_global_spread", "wn_financial_sentiment_avg",
        "wn_fiscal_article_count", "wn_fiscal_global_spread", "wn_fiscal_sentiment_avg",
        "wn_geopolitical_article_count", "wn_geopolitical_global_spread", "wn_geopolitical_sentiment_avg",
        "wn_health_global_spread", "wn_health_sentiment_avg",
        "wn_labor_global_spread", "wn_labor_sentiment_avg",
        "wn_monetary_article_count", "wn_monetary_global_spread", "wn_monetary_sentiment_avg",
        "wn_technology_article_count", "wn_technology_global_spread", "wn_technology_sentiment_avg",
        "wn_trade_global_spread", "wn_trade_sentiment_avg", "wn_trade_article_count",
    ],
}


def run_batch(batch_name: str, all_missing: list[dict], engine) -> None:
    """Run a named batch of features."""
    if batch_name not in BATCH_DEFINITIONS:
        log.error("Unknown batch: {b}. Available: {a}", b=batch_name, a=list(BATCH_DEFINITIONS.keys()))
        return

    batch_feature_names = BATCH_DEFINITIONS[batch_name]
    missing_map = {f["name"]: f for f in all_missing}

    features_to_scrape = []
    for fname in batch_feature_names:
        if fname in missing_map:
            features_to_scrape.append(missing_map[fname])
        else:
            log.info("Feature {f} already has data, skipping", f=fname)

    if not features_to_scrape:
        log.info("All features in batch '{b}' already have data!", b=batch_name)
        return

    log.info(
        "Batch '{b}': {n} features to scrape",
        b=batch_name, n=len(features_to_scrape),
    )

    # Special handling for WorldNews — use existing puller instead of web scraper
    if batch_name == "worldnews":
        _run_worldnews_puller(engine)
        return

    # Special handling for analyst ratings
    if batch_name == "analyst_ratings":
        _run_analyst_scrape(features_to_scrape, engine)
        return

    # Standard web scraping
    scraper = WebScraperPuller(db_engine=engine)
    ensure_entity_mappings(engine, features_to_scrape)

    results = scraper.pull_batch(
        [{"name": f["name"], "id": f["id"], "family": f["family"]} for f in features_to_scrape],
        delay_between=2.0,
    )

    # Summary
    found = sum(1 for r in results if r.get("value") is not None)
    verified = sum(1 for r in results if r.get("verified", False))
    log.info(
        "Batch '{b}' complete: {found}/{total} found, {v} verified",
        b=batch_name, found=found, total=len(results), v=verified,
    )

    # Run resolver
    resolve_scraped_data(engine)

    # Print results
    for r in results:
        status = "OK" if r.get("value") is not None else "MISS"
        trust = r.get("trust_label") or "N/A"
        verified_str = "VERIFIED" if r.get("verified") else "SINGLE"
        val_str = f"{r['value']:>12.4f}" if r.get("value") is not None else "         N/A"
        print(f"  [{status}] {r['feature']:40s} = {val_str} trust={trust:12s} {verified_str}")


def _run_worldnews_puller(engine) -> None:
    """Run the existing WorldNewsAPI puller for news features."""
    try:
        from ingestion.altdata.world_news import WorldNewsPuller
        puller = WorldNewsPuller(db_engine=engine)
        result = puller.pull_all()
        log.info("WorldNews puller result: {r}", r=result)
    except Exception as e:
        log.error("WorldNews puller failed: {e}. Falling back to web scraping.", e=str(e))
        # Could fall back to scraping here


def _run_analyst_scrape(features: list[dict], engine) -> None:
    """Scrape analyst ratings from financial sites."""
    scraper = WebScraperPuller(db_engine=engine)
    ensure_entity_mappings(engine, features)

    for feat in features:
        # Parse ticker and rating type from feature name
        parts = feat["name"].rsplit("_analyst_", 1)
        if len(parts) == 2:
            ticker = parts[0].upper()
            rating_type = parts[1]  # buy, sell, hold
            search_q = f"{ticker} stock analyst ratings {rating_type} consensus"
            feat["search_override"] = search_q

    results = scraper.pull_batch(
        [{"name": f["name"], "id": f["id"], "family": f["family"],
          "search_override": f.get("search_override")} for f in features],
        delay_between=2.0,
    )

    found = sum(1 for r in results if r.get("value") is not None)
    log.info("Analyst ratings: {f}/{t} found", f=found, t=len(results))
    resolve_scraped_data(engine)


def main():
    parser = argparse.ArgumentParser(description="Scrape missing GRID features")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--batch", type=str, help="Run a named batch")
    group.add_argument("--feature", type=str, help="Run a single feature")
    group.add_argument("--all", action="store_true", help="Run all batches")
    group.add_argument("--list", action="store_true", help="List missing features")
    args = parser.parse_args()

    engine = get_engine()
    all_missing = get_zero_data_features(engine)
    log.info("Found {n} features with zero data", n=len(all_missing))

    if args.list:
        for f in all_missing:
            in_config = "CONFIG" if f["name"] in FEATURE_SCRAPE_CONFIGS else "AUTO"
            print(f"  {f['id']:>5d} | {f['family']:12s} | {f['name']:45s} | {in_config} | {f['description']}")
        print(f"\nTotal: {len(all_missing)}")
        # Show batch coverage
        covered = set()
        for batch_name, features in BATCH_DEFINITIONS.items():
            covered.update(features)
        uncovered = [f for f in all_missing if f["name"] not in covered]
        print(f"Covered by batches: {len(covered)}")
        print(f"Uncovered: {len(uncovered)}")
        for f in uncovered:
            print(f"  UNCOVERED: {f['name']} ({f['family']})")
        return

    if args.feature:
        feat = next((f for f in all_missing if f["name"] == args.feature), None)
        if not feat:
            log.error("Feature '{f}' not found in missing list", f=args.feature)
            return
        scraper = WebScraperPuller(db_engine=engine)
        ensure_entity_mappings(engine, [feat])
        result = scraper.pull_feature(feat["name"], feat["id"], feat["family"])
        resolve_scraped_data(engine)
        print(json.dumps(result, indent=2, default=str))
        return

    if args.batch:
        run_batch(args.batch, all_missing, engine)
        return

    if args.all:
        for batch_name in BATCH_DEFINITIONS:
            log.info("=" * 60)
            log.info("Starting batch: {b}", b=batch_name)
            log.info("=" * 60)
            try:
                run_batch(batch_name, all_missing, engine)
            except Exception as e:
                log.error("Batch {b} failed: {e}", b=batch_name, e=str(e))
            time.sleep(5)  # Breathe between batches


if __name__ == "__main__":
    import time
    main()
