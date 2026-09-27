"""
GRID Smart Scheduler — runs only due/stale pullers per cycle.

Replaces the old pattern of running ALL 50+ pullers every pipeline cycle.
Each puller has an expected frequency. On each tick, we check which pullers
are overdue and run only those, capped at MAX_PULLERS_PER_TICK to keep
cycles short (< 5 minutes).

Pullers that fail or timeout get exponential backoff cooldowns. The old
full pipeline (run_full_pipeline.py) is still available for manual runs.

Usage from Hermes:
    from ingestion.smart_scheduler import SmartScheduler
    sched = SmartScheduler(engine)
    result = sched.tick()  # runs 3-5 due pullers, returns in < 5 min
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# GRID-YF-CLOSE-REPAIR-20260926 fix #2 ("timeout honestly sized"): without
# an explicit start_date, YFinancePuller.pull_all() defaults to "1990-01-01"
# — a full-history download for every ticker on every call. That default is
# meant for deliberate, explicitly authorised backfills only (see that
# method's own docstring); the routine scheduled call below was calling it
# with no kwargs at all, so every 4-hourly tick re-downloaded ~35 years of
# history for 70 tickers. That is what regularly overran the old 120s
# timeout_s and produced the orphaned daemon thread that let a later tick's
# concurrent call race it (the confirmed April 2026 contamination
# mechanism). A short rolling window keeps the routine scheduled download
# small and fast while still covering weekends/holidays with margin.
YFINANCE_SCHEDULED_LOOKBACK_DAYS = 10


def _yfinance_incremental_start() -> str:
    """Recent-window start_date for the routine scheduled yfinance pull.

    Evaluated at call time (see _run_puller's callable-kwargs resolution
    below), not once when PULLER_REGISTRY is built at import — otherwise
    this would freeze at whatever date the process happened to start.
    """
    return (
        datetime.now(timezone.utc).date()
        - timedelta(days=YFINANCE_SCHEDULED_LOOKBACK_DAYS)
    ).isoformat()


# GRID task A1 (owner-approved 2026-09-27): the plain freq_h>=168 cadence let
# cftc_cot fire on whatever hour the previous success happened to land on,
# which could be mid-week — well before that week's CFTC report exists, so
# the pull would silently re-store the prior week's report under a fresh
# pull_timestamp instead of catching the new one. CFTC publishes the COT
# report every Friday at 15:30 ET (19:30 UTC EDT / 20:30 UTC EST). This gate
# restricts cftc_cot to a Friday>=19:45 UTC / Saturday-retry window — a
# 15-minute margin past the latest EDT/EST publish time — and is fail-closed:
# outside that window it is never due, even if far more than freq_h hours
# have elapsed. freq_h stays on the registry entry for the overdue-priority
# sort in _get_due_pullers (harmless there — it only affects ordering among
# pullers that ARE due) but no longer drives cftc_cot's due/not-due decision.
CFTC_RELEASE_WEEKDAY_UTC = 4  # Friday (Monday=0 .. Sunday=6)
CFTC_RELEASE_MIN_HOUR_UTC = 19
CFTC_RELEASE_MIN_MINUTE_UTC = 45
CFTC_RETRY_WEEKDAY_UTC = 5  # Saturday


def _cftc_release_anchor(now: datetime) -> datetime:
    """The most recent Friday 19:45 UTC at or before ``now``."""
    days_since_friday = (now.weekday() - CFTC_RELEASE_WEEKDAY_UTC) % 7
    anchor = now.replace(
        hour=CFTC_RELEASE_MIN_HOUR_UTC,
        minute=CFTC_RELEASE_MIN_MINUTE_UTC,
        second=0,
        microsecond=0,
    ) - timedelta(days=days_since_friday)
    if anchor > now:
        anchor -= timedelta(days=7)
    return anchor


def _cftc_cot_is_due(last_success: datetime | None, now: datetime) -> bool:
    """Fail-closed weekly release gate for cftc_cot.

    Due only on Friday at/after 19:45 UTC, or on Saturday as a retry if
    Friday's run has not yet succeeded since this week's release anchor.
    Never due on any other day, regardless of how stale the last success
    is — a missed week waits for the next Friday rather than firing off-
    schedule mid-week and risking a duplicate pull of the prior report.
    """
    weekday = now.weekday()
    if weekday not in (CFTC_RELEASE_WEEKDAY_UTC, CFTC_RETRY_WEEKDAY_UTC):
        return False
    if weekday == CFTC_RELEASE_WEEKDAY_UTC:
        today_release = now.replace(
            hour=CFTC_RELEASE_MIN_HOUR_UTC,
            minute=CFTC_RELEASE_MIN_MINUTE_UTC,
            second=0,
            microsecond=0,
        )
        if now < today_release:
            return False
    if last_success is None:
        return True
    if last_success.tzinfo is None:
        last_success = last_success.replace(tzinfo=timezone.utc)
    anchor = _cftc_release_anchor(now)
    # Already succeeded at/after this week's anchor: the Friday run landed,
    # so a Saturday retry (or a second same-day check) is not due again.
    return last_success < anchor


# ── Puller Registry ──────────────────────────────────────────────────────
# Every puller with its import path, method, and expected update frequency.
# Frequency is in hours. "8" means run every 8 hours at most.

PULLER_REGISTRY: list[dict[str, Any]] = [
    # ── Fast domestic (run frequently) ──
    {"name": "yfinance",          "mod": "ingestion.yfinance_pull",       "cls": "YFinancePuller",           "method": "pull_all",  "freq_h": 4,  "timeout_s": 240, "kwargs": {"start_date": _yfinance_incremental_start}},
    {"name": "options",           "mod": "ingestion.options",             "cls": "OptionsPuller",            "method": "pull_all",  "freq_h": 6,  "timeout_s": 180},
    {"name": "coingecko",         "mod": "ingestion.coingecko",           "cls": "CoinGeckoPuller",          "method": "pull_all",  "freq_h": 4,  "timeout_s": 60},
    {"name": "fred",              "mod": "ingestion.fred",                "cls": "FREDPuller",               "method": "pull_all",  "freq_h": 12, "timeout_s": 120, "api_key": "FRED_API_KEY"},

    # ── Alt data (daily) ──
    {"name": "insider_filings",   "mod": "ingestion.altdata.insider_filings",   "cls": "InsiderFilingsPuller",     "method": "pull_all",      "freq_h": 12, "timeout_s": 120},
    {"name": "congressional",     "mod": "ingestion.altdata.congressional",     "cls": "CongressionalTradingPuller",      "method": "pull_all",      "freq_h": 24, "timeout_s": 60},
    {"name": "unusual_whales",    "mod": "ingestion.altdata.unusual_whales",    "cls": "UnusualWhalesPuller",      "method": "pull_all",      "freq_h": 12, "timeout_s": 60},
    {"name": "prediction_odds",   "mod": "ingestion.altdata.prediction_odds",   "cls": "PredictionOddsPuller",     "method": "pull_all",      "freq_h": 0.167, "timeout_s": 60},  # 10 min — critical for breaking events
    {"name": "kalshi",            "mod": "ingestion.altdata.kalshi",            "cls": "KalshiPuller",             "method": "pull_all",      "freq_h": 12, "timeout_s": 60},
    {"name": "prediction_pmxt",   "mod": "ingestion.altdata.prediction_pmxt",   "cls": "PmxtPredictionPuller",     "method": "pull",          "freq_h": 12, "timeout_s": 120},
    {"name": "smart_money",       "mod": "ingestion.altdata.smart_money",       "cls": "SmartMoneyPuller",         "method": "pull_all",      "freq_h": 0.167, "timeout_s": 60},  # 10 min — critical for breaking events
    {"name": "fed_liquidity",     "mod": "ingestion.altdata.fed_liquidity",     "cls": "FedLiquidityPuller",       "method": "pull_all",      "freq_h": 12, "timeout_s": 60, "api_key": "FRED_API_KEY"},
    {"name": "etf_flows",         "mod": "ingestion.altdata.institutional_flows","cls": "InstitutionalFlowsPuller", "method": "pull_all",      "freq_h": 24, "timeout_s": 120},
    {"name": "analyst_ratings",   "mod": "ingestion.altdata.analyst_ratings",   "cls": "AnalystRatingsPuller",     "method": "pull_all",      "freq_h": 24, "timeout_s": 60},
    {
        "name": "gdelt",
        "mod": "ingestion.altdata.gdelt",
        "cls": "GDELTPuller",
        "method": "pull_recent",
        "freq_h": 0.083,
        "timeout_s": 60,
        "kwargs": {
            "days_back": 1,
            "max_theme_queries": 4,
            "include_actor_tones": False,
            "include_tensions": False,
            "include_signals": False,
        },
    },  # 5 min — bounded breaking-events lane; full GDELT runs belong in catch-up/backfill
    {
        "name": "news_scraper",
        "mod": "ingestion.altdata.news_scraper",
        "cls": "NewsScraperPuller",
        "method": "pull_all",
        "freq_h": 6,
        "timeout_s": 60,
        "kwargs": {"use_llm": False, "pause_seconds": 0.5},
    },
    {"name": "opportunity",       "mod": "ingestion.altdata.opportunity",       "cls": "OppInsightsPuller","method": "pull_all",      "freq_h": 24, "timeout_s": 60},

    # ── International (daily, but slower) ──
    {"name": "ecb",               "mod": "ingestion.international.ecb",         "cls": "ECBPuller",                "method": "pull_all",      "freq_h": 24, "timeout_s": 180},
    {"name": "bcb",               "mod": "ingestion.international.bcb",         "cls": "BCBPuller",                "method": "pull_all",      "freq_h": 48, "timeout_s": 120},
    {"name": "mas",               "mod": "ingestion.international.mas",         "cls": "MASPuller",                "method": "pull_all",      "freq_h": 48, "timeout_s": 120},
    {"name": "akshare",           "mod": "ingestion.international.akshare_macro","cls": "AKShareMacroPuller",      "method": "pull_all",      "freq_h": 48, "timeout_s": 120},
    {"name": "rbi",               "mod": "ingestion.international.rbi",         "cls": "RBIPuller",                "method": "pull_all",      "freq_h": 168, "timeout_s": 120},

    # ── Weekly ──
    {"name": "oecd",              "mod": "ingestion.international.oecd",        "cls": "OECDPuller",               "method": "pull_all",      "freq_h": 168, "timeout_s": 180},
    {"name": "bis",               "mod": "ingestion.international.bis",         "cls": "BISPuller",                "method": "pull_all",      "freq_h": 168, "timeout_s": 120},
    {"name": "imf",               "mod": "ingestion.international.imf",         "cls": "IMFPuller",                "method": "pull_all",      "freq_h": 168, "timeout_s": 180},
    {"name": "dark_pool",         "mod": "ingestion.altdata.dark_pool",         "cls": "DarkPoolPuller",           "method": "pull_weekly",   "freq_h": 168, "timeout_s": 60},
    {"name": "gov_contracts",     "mod": "ingestion.altdata.gov_contracts",     "cls": "GovContractsPuller",       "method": "pull_all",      "freq_h": 168, "timeout_s": 120},
    {"name": "supply_chain",      "mod": "ingestion.altdata.supply_chain",      "cls": "SupplyChainPuller",        "method": "pull_all",      "freq_h": 168, "timeout_s": 60, "api_key": "FRED_API_KEY"},

    # ── Monthly / slow (run rarely) ──
    {"name": "campaign_finance",  "mod": "ingestion.altdata.campaign_finance",  "cls": "CampaignFinancePuller",    "method": "pull_all",      "freq_h": 720, "timeout_s": 300},
    {"name": "lobbying",          "mod": "ingestion.altdata.lobbying",          "cls": "LobbyingPuller",           "method": "pull_all",      "freq_h": 720, "timeout_s": 120},
    {"name": "export_controls",   "mod": "ingestion.altdata.export_controls",   "cls": "ExportControlsPuller",     "method": "pull_all",      "freq_h": 720, "timeout_s": 60},

    # ── New intelligence sources (from PR merge) ──
    {"name": "fara",              "mod": "ingestion.altdata.fara",              "cls": "FARAPuller",               "method": "pull_all",      "freq_h": 168, "timeout_s": 120},
    {"name": "foia_cables",       "mod": "ingestion.altdata.foia_cables",       "cls": "FOIACablesPuller",         "method": "pull_all",      "freq_h": 168, "timeout_s": 120},

    # ── Corporate registry / asset cross-reference ──
    {"name": "uk_companies",     "mod": "ingestion.altdata.uk_companies_house", "cls": "UKCompaniesHousePuller", "method": "pull_all",      "freq_h": 168, "timeout_s": 120, "api_key": "UK_COMPANIES_HOUSE_KEY"},
    {"name": "opencorporates",   "mod": "ingestion.altdata.opencorporates",     "cls": "OpenCorporatesPuller",   "method": "pull_all",      "freq_h": 168, "timeout_s": 120},
    {"name": "asset_registries", "mod": "ingestion.altdata.asset_registries",  "cls": "AssetRegistryPuller",    "method": "pull_all",      "freq_h": 168, "timeout_s": 120},

    # ── Solana / memecoin scanners (from PR merge) ──
    {"name": "telegram_scanner",  "mod": "ingestion.altdata.telegram_scanner",  "cls": "TelegramScanner",          "method": "pull_all",      "freq_h": 4,  "timeout_s": 60},
    {"name": "discord_scanner",   "mod": "ingestion.altdata.discord_scanner",   "cls": "DiscordScanner",           "method": "pull_all",      "freq_h": 4,  "timeout_s": 60},

    # ── Celestial ──
    {"name": "planetary",         "mod": "ingestion.celestial.planetary",       "cls": "PlanetaryAspectPuller",          "method": "pull_all",      "freq_h": 24, "timeout_s": 30},
    {"name": "lunar",             "mod": "ingestion.celestial.lunar",           "cls": "LunarCyclePuller",         "method": "pull_all",      "freq_h": 24, "timeout_s": 30},
    {"name": "solar",             "mod": "ingestion.celestial.solar",           "cls": "SolarActivityPuller",      "method": "pull_all",      "freq_h": 24, "timeout_s": 30},

    # ── Paid APIs (MUST RUN — user is paying for these) ──
    {"name": "tiingo",            "mod": "ingestion.tiingo_pull",              "cls": "TiingoPuller",             "method": "pull_all",      "freq_h": 4,  "timeout_s": 120, "api_key": "TIINGO_API_KEY", "api_key_mode": "env"},
    {"name": "tiingo_news",       "mod": "ingestion.tiingo_news_pull",         "cls": "TiingoNewsPuller",         "method": "pull_all",      "freq_h": 6,  "timeout_s": 120, "api_key": "TIINGO_API_KEY", "api_key_mode": "env"},
    {"name": "tiingo_fundamentals","mod": "ingestion.tiingo_fundamentals_pull","cls": "TiingoFundamentalsPuller", "method": "pull_all",      "freq_h": 24, "timeout_s": 120, "api_key": "TIINGO_API_KEY", "api_key_mode": "env"},
    {"name": "quiverquant",       "mod": "ingestion.altdata.quiverquant",      "cls": "QuiverQuantPuller",        "method": "pull_all",      "freq_h": 12, "timeout_s": 120, "api_key": "QUIVERQUANT_API_KEY", "api_key_mode": "env"},

    # ── Crypto (DexScreener, PumpFun) ──
    {"name": "dexscreener",       "mod": "ingestion.dexscreener",             "cls": "DexScreenerPuller",        "method": "pull_aggregate_signals", "freq_h": 4,  "timeout_s": 60},
    {"name": "pumpfun",           "mod": "ingestion.pumpfun",                 "cls": "PumpFunPuller",            "method": "pull_all",      "freq_h": 6,  "timeout_s": 60},

    # ── Government / regulatory ──
    {"name": "bls",               "mod": "ingestion.bls",                     "cls": "BLSPuller",                "method": "pull_all",      "freq_h": 168, "timeout_s": 120, "api_key": "BLS_API_KEY"},
    {"name": "edgar",             "mod": "ingestion.edgar",                   "cls": "EDGARPuller",              "method": "pull_all",      "freq_h": 24, "timeout_s": 180},
    {"name": "cftc_cot",          "mod": "ingestion.altdata.cftc_cot",        "cls": "CFTCCOTPuller",            "method": "pull_all",      "freq_h": 168, "timeout_s": 120},  # due/not-due decided by _cftc_cot_is_due (Friday>=19:45 UTC + Saturday retry), not freq_h — see the GRID task A1 note above _cftc_release_anchor

    # ── Sentiment / alt ──
    {"name": "world_news",        "mod": "ingestion.altdata.world_news",      "cls": "WorldNewsPuller",          "method": "pull_all",      "freq_h": 6,  "timeout_s": 60, "api_key": "WORLDNEWS_API_KEY", "api_key_mode": "env"},
    {"name": "fear_greed",        "mod": "ingestion.altdata.fear_greed",      "cls": "FearGreedPuller",          "method": "pull_all",      "freq_h": 12, "timeout_s": 30},
    {"name": "social_sentiment",  "mod": "ingestion.social_sentiment",        "cls": "SocialSentimentPuller",    "method": "pull_all",      "freq_h": 12, "timeout_s": 60},
    {"name": "polymarket",        "mod": "ingestion.altdata.polymarket",      "cls": "PolymarketPuller",         "method": "pull_all",      "freq_h": 12, "timeout_s": 60},
    {"name": "wiki_history",      "mod": "ingestion.wiki_history",            "cls": "WikiHistoryPuller",        "method": "pull_all",      "freq_h": 24, "timeout_s": 60},

    # ── International (missing) ──
    {"name": "eurostat",          "mod": "ingestion.international.eurostat",   "cls": "EurostatPuller",           "method": "pull_all",      "freq_h": 168, "timeout_s": 180},
    {"name": "kosis",             "mod": "ingestion.international.kosis",     "cls": "KOSISPuller",              "method": "pull_all",      "freq_h": 168, "timeout_s": 120, "api_key": "KOSIS_API_KEY", "api_key_mode": "keyword"},

    # ── Previously unscheduled sources (Phase 1 fix, 2026-04-07) ──
    {"name": "aaii_sentiment",        "mod": "ingestion.altdata.aaii_sentiment",          "cls": "AAIISentimentPuller",       "method": "pull_all",  "freq_h": 168, "timeout_s": 60},
    {"name": "cloudflare_radar",      "mod": "ingestion.altdata.cloudflare_radar_puller", "cls": "CloudflareRadarPuller",     "method": "pull",      "freq_h": 24,  "timeout_s": 120},
    {"name": "marketwatch_news",      "mod": "ingestion.altdata.marketwatch_news",        "cls": "MarketWatchNewsPuller",     "method": "pull_all",  "freq_h": 6,   "timeout_s": 60},
    {"name": "nowcast",               "mod": "ingestion.nowcast_puller",                  "cls": "NowcastPuller",             "method": "pull",      "freq_h": 168, "timeout_s": 60},
    {"name": "sec_edgar_fundamentals","mod": "ingestion.altdata.sec_edgar_company",       "cls": "SECEdgarCompanyPuller",     "method": "pull_all",  "freq_h": 168, "timeout_s": 180},
    {"name": "kalshi_markets",        "mod": "ingestion.altdata.kalshi_markets",           "cls": "KalshiMarketsPuller",      "method": "pull_all",  "freq_h": 12,  "timeout_s": 60},
    {"name": "fed_speeches",          "mod": "ingestion.altdata.fed_speeches",             "cls": "FedSpeechPuller",          "method": "pull_all",  "freq_h": 24,  "timeout_s": 60},
    {"name": "crucix_bridge",         "mod": "ingestion.crucix_bridge",                    "cls": "CrucixBridgePuller",       "method": "pull_all",  "freq_h": 1,   "timeout_s": 60},

    # ── ticker_metrics_daily writer (task #161 fix, 2026-05-17) ──
    # sec_xbrl_shares reads YF:{ticker}:close from raw_series and writes
    # market_cap to ticker_metrics_daily. Was registered in hermes_operator
    # PULLER_CONFIG but missing from this registry, so it never ran from
    # 2026-04-12 onward — leaving blue-chip rows stuck at 03-31/04-08.
    {"name": "sec_xbrl_shares",     "mod": "ingestion.altdata.sec_xbrl_shares",      "cls": "SECXBRLSharesPuller",   "method": "pull_all",  "freq_h": 24,  "timeout_s": 1800, "kwargs": {"limit": 400, "backfill_days": 90}},

    # ── Silent-orphan additions (task #170 fix, 2026-05-17) ──
    # Pullers that were registered in hermes_operator.py _SOURCE_REGISTRY but
    # never made it into this runtime registry. Same class of bug as #161
    # (sec_xbrl_shares). Audited via /tmp/audit_v2.py — all entries are
    # BasePuller-compatible (db_engine=engine ctor + pull/pull_all method).
    # Module-level fn entries (e.g. obsidian, apple_supplier_list, sec_13f_live)
    # are NOT added here because smart_scheduler._build_puller_instance
    # requires a class. Those need their own scheduler path or class wrappers.
    {"name": "cboe",                  "mod": "ingestion.altdata.cboe_indices",       "cls": "CBOEIndicesPuller",          "method": "pull_all",  "freq_h": 24,  "timeout_s": 120},
    {"name": "googletrends",          "mod": "ingestion.altdata.google_trends",      "cls": "GoogleTrendsPuller",         "method": "pull_all",  "freq_h": 24,  "timeout_s": 180, "kwargs": {"days_back": 30}},
    {"name": "hf_financial_news",     "mod": "ingestion.altdata.hf_financial_news",  "cls": "HFFinancialNewsPuller",      "method": "pull_all",  "freq_h": 24,  "timeout_s": 300},
    {"name": "ny_fed",                "mod": "ingestion.altdata.nyfed",              "cls": "NYFedPuller",                "method": "pull_all",  "freq_h": 24,  "timeout_s": 120},
    {"name": "nyfed_gscpi",           "mod": "ingestion.altdata.nyfed_gscpi",        "cls": "NYFedGSCPIPuller",           "method": "pull_all",  "freq_h": 24,  "timeout_s": 60},
    {"name": "stocktwits",            "mod": "ingestion.altdata.stocktwits",         "cls": "StockTwitsPuller",           "method": "pull_all",  "freq_h": 12,  "timeout_s": 60},
    {"name": "defillama",             "mod": "ingestion.altdata.defi_llama_puller",  "cls": "DefiLlamaPuller",            "method": "pull_all",  "freq_h": 24,  "timeout_s": 180},
    {"name": "dune",                  "mod": "ingestion.altdata.dune_puller",        "cls": "DunePuller",                 "method": "pull_all",  "freq_h": 6,   "timeout_s": 180, "api_key": "DUNE_API_KEY", "api_key_mode": "keyword"},
    {"name": "repo_market",           "mod": "ingestion.altdata.repo_market",        "cls": "RepoMarketPuller",           "method": "pull_all",  "freq_h": 168, "timeout_s": 120, "api_key": "FRED_API_KEY", "api_key_mode": "first"},
    {"name": "legislation",           "mod": "ingestion.altdata.legislation",        "cls": "LegislationPuller",          "method": "pull_all",  "freq_h": 24,  "timeout_s": 180},
    {"name": "earnings_calendar",     "mod": "ingestion.altdata.earnings_calendar",  "cls": "EarningsCalendarPuller",     "method": "pull_all",  "freq_h": 24,  "timeout_s": 120},
    {"name": "social_attention",      "mod": "ingestion.altdata.social_attention",   "cls": "WikipediaAttentionPuller",   "method": "pull_all",  "freq_h": 24,  "timeout_s": 1800,  "kwargs": {"max_tickers": 100}},
    {"name": "yield_curve_full",      "mod": "ingestion.altdata.yield_curve_full",   "cls": "FullYieldCurvePuller",       "method": "pull_all",  "freq_h": 24,  "timeout_s": 120, "api_key": "FRED_API_KEY", "api_key_mode": "first"},
    {"name": "sec_xbrl_financials",   "mod": "ingestion.altdata.sec_xbrl_financials","cls": "SECXBRLFinancialsPuller",    "method": "pull_all",  "freq_h": 168, "timeout_s": 1800, "kwargs": {"limit": 200}},
    {"name": "fx_rates",              "mod": "ingestion.altdata.fx_rates",           "cls": "FXRatesPuller",              "method": "pull",      "freq_h": 24,  "timeout_s": 120, "kwargs": {"days_back": 7}},
    {"name": "margin_debt",           "mod": "ingestion.altdata.margin_debt",        "cls": "MarginDebtPuller",           "method": "pull",      "freq_h": 168, "timeout_s": 60},
    {"name": "ag_commodity_futures",  "mod": "ingestion.altdata.ag_commodity_futures","cls": "AgCommodityFuturesPuller",  "method": "pull_all",  "freq_h": 24,  "timeout_s": 180},
    {"name": "alphavantage_sentiment","mod": "ingestion.altdata.alphavantage_sentiment","cls": "AlphaVantageSentimentPuller","method": "pull_all","freq_h": 24,  "timeout_s": 180},
    {"name": "ads_index",             "mod": "ingestion.altdata.ads_index",          "cls": "ADSIndexPuller",             "method": "pull_all",  "freq_h": 168, "timeout_s": 60},
    {"name": "baltic_exchange",       "mod": "ingestion.altdata.baltic_dry",         "cls": "BalticDryPuller",            "method": "pull_all",  "freq_h": 24,  "timeout_s": 60,  "api_key": "FRED_API_KEY", "api_key_mode": "first"},
    {"name": "finra_ats",             "mod": "ingestion.altdata.finra_ats",          "cls": "FINRAATSPuller",             "method": "pull_all",  "freq_h": 168, "timeout_s": 120},
    {"name": "offshore_leaks",        "mod": "ingestion.altdata.offshore_leaks",     "cls": "OffshoreLeaksPuller",        "method": "pull",      "freq_h": 720, "timeout_s": 600},
    {"name": "wikidata_persons",      "mod": "ingestion.altdata.wikidata_persons",   "cls": "WikidataPersonPuller",       "method": "pull_all",  "freq_h": 168, "timeout_s": 1800},
]

# How many pullers to run per tick (keeps cycles short)
MAX_PULLERS_PER_TICK = 8

# Per-tick time budget (seconds) — stop scheduling more if we're over this
TICK_TIME_BUDGET_S = 300  # 5 minutes


class MissingPullerApiKey(Exception):
    """Raised when a registry entry requires an unset API key."""

    def __init__(self, key_name: str) -> None:
        self.key_name = key_name
        super().__init__(f"Missing API key: {key_name}")


class SmartScheduler:
    """Runs only due/stale pullers each tick, with per-source cooldowns.

    Thread-leak caveat (DEV-NOTES H17): a puller that exceeds its
    ``timeout_s`` budget is daemon-detached and reported as ``TIMEOUT``,
    but the underlying ``threading.Thread`` keeps running in the
    background until the process exits. The semaphore is released on
    timeout so a single hung puller cannot starve the others, which
    means orphaned threads do NOT count against
    ``MAX_CONCURRENT_THREADS``. ``self._orphan_thread_count`` tracks the
    cumulative number of orphans for operator-side observability and is
    surfaced through :meth:`get_status` so persistent leaks become
    visible without needing to inspect ``ps``/``/proc``.
    """

    # Maximum number of concurrent puller threads
    MAX_CONCURRENT_THREADS = 10

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        # In-memory tracking: {name: {last_success, last_attempt, consecutive_fails, cooldown_until}}
        self._state: dict[str, dict[str, Any]] = {}
        # Thread concurrency control
        self._thread_semaphore = threading.Semaphore(self.MAX_CONCURRENT_THREADS)
        self._active_threads: set[str] = set()
        self._threads_lock = threading.Lock()
        # Cumulative count of daemon threads that exceeded their timeout
        # and were left running in the background. See class docstring.
        self._orphan_thread_count: int = 0
        self._load_state_from_db()
        self._warn_registry_divergence()

    def _warn_registry_divergence(self) -> None:
        """Detect & log orphan pullers across hermes_operator and this registry.

        Task #170 (2026-05-17) added this guard. Task #179 (2026-05-17)
        consolidated ``_SOURCE_REGISTRY`` to be DERIVED from PULLER_REGISTRY
        + a small explicit ``_SOURCE_EXTRAS`` overlay, so the only remaining
        divergence sources are intentional (fn-based pullers, audit-only
        skip_runtime stubs, and historical alias names). Those live in
        ``hermes_operator._SOURCE_EXTRAS`` and ``_SOURCE_ALIASES`` and are
        filtered out before warning — anything left over IS a regression.
        """
        try:
            from scripts.hermes_operator import (
                _SOURCE_REGISTRY as _CFG,
                _SOURCE_EXTRAS,
                _SOURCE_ALIASES,
            )
        except Exception as exc:
            log.debug("Registry-divergence check skipped: {e}", e=str(exc))
            return
        cfg = set(_CFG.keys())
        reg = {p["name"] for p in PULLER_REGISTRY}
        # Intentional cfg-only entries (fn-based, skip_runtime, not-yet-wired
        # class pullers) live in _SOURCE_EXTRAS, and alias names alias onto
        # a canonical PULLER_REGISTRY entry — neither indicates a bug.
        intentional_cfg_only = set(_SOURCE_EXTRAS.keys()) | set(_SOURCE_ALIASES.keys())
        only_cfg = (cfg - reg) - intentional_cfg_only
        only_reg = reg - cfg
        if only_cfg:
            log.warning(
                "SmartScheduler registry divergence — {n} pullers in "
                "hermes_operator._SOURCE_REGISTRY but NOT in PULLER_REGISTRY "
                "(silent orphans, won't run): {names}",
                n=len(only_cfg), names=sorted(only_cfg),
            )
        if only_reg:
            log.info(
                "SmartScheduler registry note — {n} pullers in PULLER_REGISTRY "
                "but not in hermes_operator._SOURCE_REGISTRY (running but "
                "undocumented in operator): {names}",
                n=len(only_reg), names=sorted(only_reg),
            )
        if not only_cfg and not only_reg:
            log.info(
                "SmartScheduler registry consolidated — _SOURCE_REGISTRY "
                "derived from PULLER_REGISTRY ({n} entries) + {ne} extras "
                "+ {na} aliases. No divergence.",
                n=len(reg), ne=len(_SOURCE_EXTRAS), na=len(_SOURCE_ALIASES),
            )

    def _load_state_from_db(self) -> None:
        """Bootstrap state from source_catalog.last_pull_at."""
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(text(
                    "SELECT name, last_pull_at FROM source_catalog "
                    "WHERE last_pull_at IS NOT NULL"
                )).fetchall()
                for r in rows:
                    self._state[r[0].lower()] = {
                        "last_success": r[1],
                        "last_attempt": r[1],
                        "consecutive_fails": 0,
                        "cooldown_until": None,
                    }
            log.debug("SmartScheduler loaded {n} source states from DB", n=len(self._state))
        except Exception as exc:
            log.warning("SmartScheduler DB state load failed: {e}", e=str(exc))

    def _is_due(self, puller: dict) -> bool:
        """Check if a puller needs to run based on its frequency."""
        name = puller["name"]
        state = self._state.get(name)

        # cftc_cot is gated to its weekly release window (see
        # _cftc_cot_is_due) instead of the plain freq_h cadence — CFTC only
        # publishes once a week, on Friday, so "due" must mean "in Friday's
        # release window or Saturday's retry", not "168h since last run" (or
        # "never run" — a never-run cftc_cot still waits for the window
        # instead of firing off-schedule the instant this process starts).
        if name == "cftc_cot":
            cooldown = state.get("cooldown_until") if state else None
            if cooldown and datetime.now(timezone.utc) < cooldown:
                return False
            last = state.get("last_success") if state else None
            if last is not None and hasattr(last, "tzinfo") and last.tzinfo is None:
                last = last.replace(tzinfo=timezone.utc)
            return _cftc_cot_is_due(last, datetime.now(timezone.utc))

        # Never run before → definitely due
        if state is None:
            return True

        # In cooldown after failure → not due
        cooldown = state.get("cooldown_until")
        if cooldown and datetime.now(timezone.utc) < cooldown:
            return False

        # Check if enough time has passed since last success
        last = state.get("last_success")
        if last is None:
            return True

        if hasattr(last, "tzinfo") and last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)

        age_hours = (datetime.now(timezone.utc) - last).total_seconds() / 3600
        return age_hours >= puller["freq_h"]

    def _get_due_pullers(self) -> list[dict]:
        """Return pullers that are due, sorted by priority (most overdue first)."""
        due = []
        for p in PULLER_REGISTRY:
            if self._is_due(p):
                # Calculate how overdue (for priority sorting)
                state = self._state.get(p["name"])
                if state and state.get("last_success"):
                    last = state["last_success"]
                    if hasattr(last, "tzinfo") and last.tzinfo is None:
                        last = last.replace(tzinfo=timezone.utc)
                    overdue_h = (datetime.now(timezone.utc) - last).total_seconds() / 3600 - p["freq_h"]
                else:
                    overdue_h = 9999  # never run = most overdue
                due.append({**p, "_overdue_h": overdue_h})

        # Most overdue first, but fast pullers (low timeout) get priority
        due.sort(key=lambda x: (-x["_overdue_h"], x["timeout_s"]))
        return due

    def _run_puller(self, puller: dict) -> dict[str, Any]:
        """Import, instantiate, and run a single puller with timeout.

        Uses a semaphore to cap concurrent threads at MAX_CONCURRENT_THREADS
        and tracks active threads for observability.

        GRID-YF-CLOSE-REPAIR-20260926 follow-up: a puller that overruns
        ``timeout_s`` is daemon-detached and left running (see the class
        docstring) — that orphaned thread was the confirmed mechanism behind
        the April 2026 yfinance wrong-instrument contamination, once a
        thread-unsafe download library shared state across the orphan and
        the NEXT tick's fresh call. Two independent, consistent mitigations
        now live alongside this hard join-timeout:
          1. Any puller method that accepts a ``should_continue`` kwarg
             (currently ``YFinancePuller.pull_all``) gets one wired
             automatically, bound to a deadline a little inside this call's
             own ``timeout_s``. That lets a slow multi-item pull stop itself
             cleanly BETWEEN items before the hard join timeout fires,
             instead of relying on abandonment.
          2. If the puller's own return value reports
             ``{"status": "SKIPPED", ...}`` (e.g. YFinancePuller.pull_all's
             single-flight lock finding a previous run still active), this
             call is recorded as SKIPPED rather than SUCCESS — critically,
             it must NOT advance ``source_catalog.last_pull_at``, or a
             lock-skipped run would be indistinguishable from a real one.
        """
        import importlib
        import inspect
        import os

        name = puller["name"]
        timeout_s = puller.get("timeout_s", 120)
        result: dict[str, Any] = {"name": name, "status": "UNKNOWN"}

        # Acquire semaphore (non-blocking) to enforce thread limit
        if not self._thread_semaphore.acquire(blocking=False):
            with self._threads_lock:
                active = list(self._active_threads)
            log.warning(
                "SmartScheduler: thread limit ({lim}) reached, skipping {n} — active: {a}",
                lim=self.MAX_CONCURRENT_THREADS, n=name, a=active,
            )
            result["status"] = "SKIPPED"
            result["reason"] = f"Thread limit ({self.MAX_CONCURRENT_THREADS}) reached"
            return result

        # Register this thread as active
        with self._threads_lock:
            self._active_threads.add(name)

        try:
            mod = importlib.import_module(puller["mod"])
            cls = getattr(mod, puller["cls"])

            try:
                instance = self._build_puller_instance(puller, cls, os.environ)
            except MissingPullerApiKey as exc:
                result["status"] = "SKIPPED"
                result["reason"] = str(exc)
                return result

            method = getattr(instance, puller["method"])
            method_kwargs = dict(puller.get("kwargs") or {})
            # Registry kwargs may be plain values or zero-arg callables
            # (e.g. a "recent N days" start_date that must be computed at
            # call time, not once when PULLER_REGISTRY is built at import).
            # `should_continue` is itself always a callable BY CONTRACT (a
            # cooperative-cancellation check, not a value to precompute) —
            # never resolve it here, or an explicit registry-supplied one
            # would be invoked once and replaced by its boolean result.
            for key, val in list(method_kwargs.items()):
                if key != "should_continue" and callable(val):
                    method_kwargs[key] = val()

            # Cooperative-cancellation wiring: if the puller's method
            # accepts should_continue and the registry didn't already
            # supply one, bind a deadline a bit inside this call's own
            # timeout_s so it can stop itself between items instead of
            # being abandoned by the hard join timeout below. See this
            # method's docstring, mitigation 1.
            if "should_continue" not in method_kwargs:
                try:
                    accepts_should_continue = (
                        "should_continue" in inspect.signature(method).parameters
                    )
                except (TypeError, ValueError):
                    accepts_should_continue = False
                if accepts_should_continue:
                    margin = min(15, max(timeout_s // 8, 1))
                    deadline = time.monotonic() + max(timeout_s - margin, 1)
                    method_kwargs["should_continue"] = (
                        lambda _deadline=deadline: time.monotonic() < _deadline
                    )

            # Run with timeout — don't let any puller block for minutes
            out_box: list[Any] = [None]
            err_box: list[Exception | None] = [None]

            def _target() -> None:
                try:
                    out_box[0] = method(**method_kwargs)
                except Exception as e:
                    err_box[0] = e

            t = threading.Thread(target=_target, daemon=True)
            t.start()
            t.join(timeout=timeout_s)

            if t.is_alive():
                # The daemon thread keeps running in the background — we
                # release the semaphore on the way out so other pullers
                # are not starved, but the orphan is not joinable. Track
                # the cumulative count so operators can detect a slow
                # leak via get_status() without resorting to /proc.
                with self._threads_lock:
                    self._orphan_thread_count += 1
                    orphan_total = self._orphan_thread_count
                result["status"] = "TIMEOUT"
                result["error"] = f"Exceeded {timeout_s}s timeout"
                log.warning(
                    "SmartScheduler: {n} TIMEOUT after {s}s — daemon "
                    "thread orphaned (cumulative orphans={c})",
                    n=name, s=timeout_s, c=orphan_total,
                )
                return result

            if err_box[0]:
                raise err_box[0]

            out = out_box[0]
            # Mitigation 2 (see docstring): a puller can report its own
            # SKIPPED outcome — e.g. YFinancePuller.pull_all's single-flight
            # lock finding a previous run still active. That is NOT a
            # successful check and must not advance last_pull_at.
            if isinstance(out, dict) and out.get("status") == "SKIPPED":
                result["status"] = "SKIPPED"
                result["reason"] = out.get("skipped_reason", "puller reported SKIPPED")
                result["detail"] = str(out)[:200]
                return result

            result["status"] = "SUCCESS"
            result["detail"] = str(out)[:200] if out else ""
            self._update_last_pull(name)

        except Exception as exc:
            result["status"] = "FAILED"
            result["error"] = str(exc)[:200]
            log.warning("SmartScheduler: {n} failed: {e}", n=name, e=str(exc))

        finally:
            # Always clean up: release semaphore and unregister thread
            with self._threads_lock:
                self._active_threads.discard(name)
            self._thread_semaphore.release()

        return result

    def _build_puller_instance(
        self,
        puller: dict[str, Any],
        cls: type,
        environ: dict[str, str],
    ) -> Any:
        """Instantiate a puller using the constructor shape declared in registry."""
        api_key_name = puller.get("api_key")
        if not api_key_name:
            return cls(db_engine=self.engine)

        key_val = environ.get(api_key_name, "")
        if not key_val:
            raise MissingPullerApiKey(api_key_name)

        mode = puller.get("api_key_mode", "first")
        if mode == "first":
            return cls(key_val, self.engine)
        if mode == "keyword":
            return cls(db_engine=self.engine, api_key=key_val)
        if mode == "env":
            return cls(db_engine=self.engine)

        raise ValueError(f"Unknown api_key_mode for {puller['name']}: {mode}")

    def _update_last_pull(self, name: str) -> None:
        """Update source_catalog.last_pull_at for a source."""
        try:
            with self.engine.begin() as conn:
                conn.execute(text(
                    "UPDATE source_catalog SET last_pull_at = NOW() "
                    "WHERE LOWER(name) = :n"
                ), {"n": name.lower()})
        except Exception:
            pass  # best effort

    def _record_result(self, name: str, success: bool, error: str | None = None) -> None:
        """Record puller result and manage cooldowns."""
        state = self._state.get(name, {"consecutive_fails": 0})
        state["last_attempt"] = datetime.now(timezone.utc)

        if success:
            state["last_success"] = datetime.now(timezone.utc)
            state["consecutive_fails"] = 0
            state["cooldown_until"] = None
        else:
            fails = state.get("consecutive_fails", 0) + 1
            state["consecutive_fails"] = fails
            # Exponential backoff: 30min, 1h, 2h, 4h, 8h, max 24h
            cooldown_min = min(30 * (2 ** (fails - 1)), 1440)
            state["cooldown_until"] = (
                datetime.now(timezone.utc) + timedelta(minutes=cooldown_min)
            )
            log.info(
                "SmartScheduler: {n} failed {f}x, cooldown {c}min",
                n=name, f=fails, c=cooldown_min,
            )

        self._state[name] = state

    def tick(self) -> dict[str, Any]:
        """Run one scheduler tick — execute up to MAX_PULLERS_PER_TICK due pullers.

        Returns summary of what ran, what succeeded, what failed.
        """
        tick_start = time.monotonic()
        due = self._get_due_pullers()

        summary: dict[str, Any] = {
            "total_due": len(due),
            "ran": 0,
            "succeeded": 0,
            "failed": 0,
            "skipped": 0,
            "results": [],
            "still_due": [],
        }

        if not due:
            log.info("SmartScheduler tick: nothing due")
            return summary

        log.info(
            "SmartScheduler tick: {n} pullers due, running up to {m}",
            n=len(due), m=MAX_PULLERS_PER_TICK,
        )

        for puller in due[:MAX_PULLERS_PER_TICK]:
            # Check time budget
            elapsed = time.monotonic() - tick_start
            if elapsed > TICK_TIME_BUDGET_S:
                log.info("SmartScheduler: time budget exhausted ({e:.0f}s), deferring rest", e=elapsed)
                break

            name = puller["name"]
            log.info("SmartScheduler: running {n} (overdue {h:.0f}h)", n=name, h=puller.get("_overdue_h", 0))

            result = self._run_puller(puller)
            success = result["status"] == "SUCCESS"
            self._record_result(name, success, result.get("error"))

            summary["results"].append(result)
            summary["ran"] += 1
            if success:
                summary["succeeded"] += 1
            elif result["status"] == "SKIPPED":
                summary["skipped"] += 1
            else:
                summary["failed"] += 1

        # Report what's still due for next tick
        summary["still_due"] = [p["name"] for p in due[MAX_PULLERS_PER_TICK:]]

        elapsed = time.monotonic() - tick_start
        log.info(
            "SmartScheduler tick complete in {e:.1f}s — "
            "{ok}/{ran} succeeded, {f} failed, {d} still due",
            e=elapsed, ok=summary["succeeded"], ran=summary["ran"],
            f=summary["failed"], d=len(summary["still_due"]),
        )
        return summary

    def get_status(self) -> dict[str, Any]:
        """Return current scheduler state for the API."""
        due = self._get_due_pullers()
        in_cooldown = [
            {
                "name": name,
                "fails": s.get("consecutive_fails", 0),
                "cooldown_until": s["cooldown_until"].isoformat() if s.get("cooldown_until") else None,
            }
            for name, s in self._state.items()
            if s.get("cooldown_until") and datetime.now(timezone.utc) < s["cooldown_until"]
        ]
        with self._threads_lock:
            orphan_total = self._orphan_thread_count
            active_thread_names = sorted(self._active_threads)
        return {
            "total_registered": len(PULLER_REGISTRY),
            "total_due": len(due),
            "due_names": [p["name"] for p in due[:20]],
            "in_cooldown": in_cooldown,
            "max_per_tick": MAX_PULLERS_PER_TICK,
            "tick_budget_s": TICK_TIME_BUDGET_S,
            "active_threads": active_thread_names,
            "max_concurrent_threads": self.MAX_CONCURRENT_THREADS,
            "orphan_thread_count_total": orphan_total,
        }
