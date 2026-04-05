#!/usr/bin/env python3
"""
GRID Bulk Data Downloader — Run via Hermes or standalone.

Downloads large data caches to /data/bulk_data/ for offline ingestion.
Resumable (wget -c), parallel where safe, logs progress.

Usage:
    python3 scripts/bulk_data_downloader.py          # Run all
    python3 scripts/bulk_data_downloader.py --tier 1  # Run tier 1 only
    python3 scripts/bulk_data_downloader.py --list     # List all jobs
"""

import os
import subprocess
import sys
import json
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, field
from loguru import logger as log

BASE_DIR = Path("/data/bulk_data")
LOG_FILE = BASE_DIR / "download_log.json"


@dataclass
class DownloadJob:
    name: str
    tier: int  # 1=critical, 2=important, 3=enrichment
    category: str  # financial, sentiment, social, geopolitical, reference
    description: str
    commands: list[str]
    dest_dir: str
    est_size_gb: float = 0.0
    done: bool = False


# ═══════════════════════════════════════════════════════════════════════
# DOWNLOAD MANIFEST
# ═══════════════════════════════════════════════════════════════════════

JOBS: list[DownloadJob] = [

    # ── TIER 1: Critical — fills our biggest data gaps ────────────────

    DownloadJob(
        name="fnspid_prices_news",
        tier=1, category="financial",
        description="FNSPID: 29.7M stock prices + 15.7M financial news (1999-2023, S&P500)",
        dest_dir="fnspid",
        est_size_gb=12.0,
        commands=[
            "wget -c -q https://huggingface.co/datasets/Zihan1004/FNSPID/resolve/main/Stock_price/full_history.zip -O full_history.zip",
            "wget -c -q https://huggingface.co/datasets/Zihan1004/FNSPID/resolve/main/News/benzinga.zip -O benzinga.zip 2>/dev/null || true",
            "wget -c -q https://huggingface.co/datasets/Zihan1004/FNSPID/resolve/main/News/guardian.zip -O guardian.zip 2>/dev/null || true",
            "wget -c -q https://huggingface.co/datasets/Zihan1004/FNSPID/resolve/main/News/reuters.zip -O reuters.zip 2>/dev/null || true",
        ],
    ),
    DownloadJob(
        name="gdelt_master_events",
        tier=1, category="geopolitical",
        description="GDELT: Every geopolitical event 1979-2013 + GKG tone/sentiment",
        dest_dir="gdelt",
        est_size_gb=2.0,
        commands=[
            "wget -c -q http://data.gdeltproject.org/events/GDELT.MASTERREDUCEDV2.1979-2013.zip -O master_1979_2013.zip",
            "wget -c -q http://data.gdeltproject.org/gkg/GDELT-GKG-MASTER.zip -O gkg_master.zip 2>/dev/null || true",
        ],
    ),
    DownloadJob(
        name="fear_greed_historical",
        tier=1, category="sentiment",
        description="CNN Fear & Greed Index: daily values back to 2018",
        dest_dir="fear_greed",
        est_size_gb=0.01,
        commands=[
            "git clone --depth 1 https://github.com/whit3rabbit/fear-greed-data.git . 2>/dev/null || git pull",
            "git clone --depth 1 https://github.com/hackingthemarkets/sentiment-fear-and-greed.git fg_backtest 2>/dev/null || true",
        ],
    ),
    DownloadJob(
        name="polymarket_trades",
        tier=1, category="sentiment",
        description="Polymarket + Kalshi: largest public prediction market trade dataset",
        dest_dir="polymarket",
        est_size_gb=3.0,
        commands=[
            "git clone --depth 1 https://github.com/jon-becker/prediction-market-analysis.git . 2>/dev/null || git pull",
        ],
    ),
    DownloadJob(
        name="arctic_shift_reddit",
        tier=1, category="social",
        description="Reddit dumps for finance subs: WSB, stocks, crypto, investing, options, economics",
        dest_dir="reddit",
        est_size_gb=150.0,
        commands=[
            # Arctic Shift per-subreddit downloads (submissions + comments)
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/submission?subreddit=wallstreetbets' -O wsb_submissions.zst 2>/dev/null || true",
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/comment?subreddit=wallstreetbets' -O wsb_comments.zst 2>/dev/null || true",
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/submission?subreddit=stocks' -O stocks_submissions.zst 2>/dev/null || true",
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/submission?subreddit=cryptocurrency' -O crypto_submissions.zst 2>/dev/null || true",
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/submission?subreddit=investing' -O investing_submissions.zst 2>/dev/null || true",
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/submission?subreddit=options' -O options_submissions.zst 2>/dev/null || true",
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/submission?subreddit=economics' -O economics_submissions.zst 2>/dev/null || true",
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/submission?subreddit=personalfinance' -O pf_submissions.zst 2>/dev/null || true",
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/submission?subreddit=CryptoCurrency' -O cc_submissions.zst 2>/dev/null || true",
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/submission?subreddit=Bitcoin' -O btc_submissions.zst 2>/dev/null || true",
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/submission?subreddit=news' -O news_submissions.zst 2>/dev/null || true",
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/submission?subreddit=worldnews' -O worldnews_submissions.zst 2>/dev/null || true",
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/submission?subreddit=politics' -O politics_submissions.zst 2>/dev/null || true",
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/submission?subreddit=technology' -O tech_submissions.zst 2>/dev/null || true",
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/submission?subreddit=science' -O science_submissions.zst 2>/dev/null || true",
        ],
    ),

    # ── TIER 2: Important — enriches signal coverage ──────────────────

    DownloadJob(
        name="sovai_investment_data",
        tier=2, category="financial",
        description="Sovai: 60+ insider trading features, congressional trades, 13F data",
        dest_dir="sovai",
        est_size_gb=0.5,
        commands=[
            "git clone --depth 1 https://github.com/sovai-research/open-investment-datasets.git . 2>/dev/null || git pull",
        ],
    ),
    DownloadJob(
        name="wikipedia_pageviews",
        tier=2, category="social",
        description="Wikipedia hourly pageviews: public attention tracker (2015-present)",
        dest_dir="wikipedia",
        est_size_gb=50.0,
        commands=[
            # Recent monthly pageview dumps — one per month, ~4GB each compressed
            "wget -c -q https://dumps.wikimedia.org/other/pageview_complete/2026/2026-03/pageviews-20260301-automated.bz2 2>/dev/null || true",
            "wget -c -q https://dumps.wikimedia.org/other/pageview_complete/2026/2026-02/pageviews-20260201-automated.bz2 2>/dev/null || true",
            "wget -c -q https://dumps.wikimedia.org/other/pageview_complete/2026/2026-01/pageviews-20260101-automated.bz2 2>/dev/null || true",
        ],
    ),
    DownloadJob(
        name="google_trends_official",
        tier=2, category="social",
        description="Google Trends: official curated trend datasets from Google",
        dest_dir="google_trends",
        est_size_gb=0.1,
        commands=[
            "git clone --depth 1 https://github.com/GoogleTrends/data.git . 2>/dev/null || git pull",
        ],
    ),
    DownloadJob(
        name="sec_edgar_tools",
        tier=2, category="financial",
        description="SEC EDGAR bulk downloader + 13F parser tools",
        dest_dir="sec_edgar",
        est_size_gb=0.1,
        commands=[
            "pip install sec-edgar-downloader 2>/dev/null || true",
            "git clone --depth 1 https://github.com/git-shogg/finsec.git finsec_13f 2>/dev/null || true",
            "git clone --depth 1 https://github.com/dgunning/edgartools.git edgartools 2>/dev/null || true",
        ],
    ),
    DownloadJob(
        name="hf_financial_news_multisource",
        tier=2, category="sentiment",
        description="HuggingFace financial-news-multisource: 57M+ rows, 1990-2025, 24 subsets",
        dest_dir="hf_news",
        est_size_gb=20.0,
        commands=[
            # Download via HF CLI or direct parquet files
            "pip install huggingface_hub 2>/dev/null || true",
            "python3 -c \"from huggingface_hub import snapshot_download; snapshot_download('Brianferrell787/financial-news-multisource', local_dir='.', repo_type='dataset')\" 2>/dev/null || echo 'HF download may need login'",
        ],
    ),

    # ── TIER 3: Enrichment — social/reaction/temporal granularity ─────

    DownloadJob(
        name="sentiment140_tweets",
        tier=3, category="social",
        description="Sentiment140: 1.6M timestamped tweets with polarity labels",
        dest_dir="sentiment_training/sentiment140",
        est_size_gb=0.3,
        commands=[
            "python3 -c \"from huggingface_hub import snapshot_download; snapshot_download('stanfordnlp/sentiment140', local_dir='.', repo_type='dataset')\" 2>/dev/null || true",
        ],
    ),
    DownloadJob(
        name="financial_phrasebank",
        tier=3, category="sentiment",
        description="Financial PhraseBank: 4,840 expert-annotated sentences for FinBERT training",
        dest_dir="sentiment_training/phrasebank",
        est_size_gb=0.01,
        commands=[
            "python3 -c \"from huggingface_hub import snapshot_download; snapshot_download('takala/financial_phrasebank', local_dir='.', repo_type='dataset')\" 2>/dev/null || true",
        ],
    ),
    DownloadJob(
        name="twitter_financial_sentiment",
        tier=3, category="sentiment",
        description="Twitter financial news sentiment: labeled financial tweets",
        dest_dir="sentiment_training/twitter_fin",
        est_size_gb=0.05,
        commands=[
            "python3 -c \"from huggingface_hub import snapshot_download; snapshot_download('zeroshot/twitter-financial-news-sentiment', local_dir='.', repo_type='dataset')\" 2>/dev/null || true",
        ],
    ),
    DownloadJob(
        name="stock_emotions",
        tier=3, category="sentiment",
        description="StockEmotions: investor emotion dataset (anger, fear, trust, surprise) — AAAI 2023",
        dest_dir="sentiment_training/stock_emotions",
        est_size_gb=0.05,
        commands=[
            "git clone --depth 1 https://github.com/adlnlp/StockEmotions.git . 2>/dev/null || git pull",
        ],
    ),
    DownloadJob(
        name="finance_database",
        tier=3, category="reference",
        description="FinanceDatabase: 300K+ symbols (equities, ETFs, funds, indices, crypto)",
        dest_dir="reference/finance_db",
        est_size_gb=0.2,
        commands=[
            "pip install financedatabase 2>/dev/null || true",
            "python3 -c \"import financedatabase as fd; eq=fd.Equities(); eq.select().to_parquet('equities.parquet')\" 2>/dev/null || true",
            "python3 -c \"import financedatabase as fd; et=fd.ETFs(); et.select().to_parquet('etfs.parquet')\" 2>/dev/null || true",
        ],
    ),
    DownloadJob(
        name="awesome_quant_index",
        tier=3, category="reference",
        description="awesome-quant: curated master list of quant data sources and tools",
        dest_dir="reference/awesome_quant",
        est_size_gb=0.01,
        commands=[
            "git clone --depth 1 https://github.com/wilsonfreitas/awesome-quant.git . 2>/dev/null || git pull",
        ],
    ),
    DownloadJob(
        name="tweeteval_benchmark",
        tier=3, category="social",
        description="TweetEval: 7 Twitter classification tasks (emotion, hate, irony, sentiment, stance)",
        dest_dir="sentiment_training/tweeteval",
        est_size_gb=0.1,
        commands=[
            "python3 -c \"from huggingface_hub import snapshot_download; snapshot_download('cardiffnlp/tweet_eval', local_dir='.', repo_type='dataset')\" 2>/dev/null || true",
        ],
    ),
    DownloadJob(
        name="gdelt_recent_daily",
        tier=2, category="geopolitical",
        description="GDELT: recent daily event files (last 90 days)",
        dest_dir="gdelt/daily",
        est_size_gb=5.0,
        commands=[
            # Download last 90 days of GDELT daily event CSVs
            "python3 -c \"\nimport urllib.request, datetime\nfor i in range(90):\n    d = (datetime.date.today() - datetime.timedelta(days=i)).strftime('%Y%m%d')\n    url = f'http://data.gdeltproject.org/events/{d}.export.CSV.zip'\n    try:\n        urllib.request.urlretrieve(url, f'{d}.export.CSV.zip')\n        print(f'Downloaded {d}')\n    except: pass\n\" 2>/dev/null || true",
        ],
    ),
    DownloadJob(
        name="poly_data_pipeline",
        tier=2, category="sentiment",
        description="poly_data: Polymarket data pipeline with trade snapshots",
        dest_dir="polymarket/poly_data",
        est_size_gb=1.0,
        commands=[
            "git clone --depth 1 https://github.com/warproxxx/poly_data.git . 2>/dev/null || git pull",
        ],
    ),

    # ── TIER 2: Macro & Economic ─────────────────────────────────────

    DownloadJob(
        name="fred_bulk_all",
        tier=2, category="financial",
        description="FRED: ALL series bulk download (700K+ series, macro/rates/employment/housing/trade)",
        dest_dir="fred_bulk",
        est_size_gb=5.0,
        commands=[
            # FRED bulk downloads by category
            "wget -c -q 'https://fred.stlouisfed.org/graph/fredgraph.csv?id=UMCSENT' -O umcsent.csv || true",
            "wget -c -q 'https://fred.stlouisfed.org/graph/fredgraph.csv?id=VIXCLS' -O vixcls.csv || true",
            "wget -c -q 'https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS10' -O dgs10.csv || true",
            "wget -c -q 'https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS2' -O dgs2.csv || true",
            "wget -c -q 'https://fred.stlouisfed.org/graph/fredgraph.csv?id=T10Y2Y' -O t10y2y.csv || true",
            "wget -c -q 'https://fred.stlouisfed.org/graph/fredgraph.csv?id=BAMLH0A0HYM2' -O hy_spread.csv || true",
            "wget -c -q 'https://fred.stlouisfed.org/graph/fredgraph.csv?id=WALCL' -O fed_balance.csv || true",
            "wget -c -q 'https://fred.stlouisfed.org/graph/fredgraph.csv?id=RRPONTSYD' -O rrp.csv || true",
            "wget -c -q 'https://fred.stlouisfed.org/graph/fredgraph.csv?id=WTREGEN' -O tga.csv || true",
            "wget -c -q 'https://fred.stlouisfed.org/graph/fredgraph.csv?id=M2SL' -O m2.csv || true",
            "wget -c -q 'https://fred.stlouisfed.org/graph/fredgraph.csv?id=CPIAUCSL' -O cpi.csv || true",
            "wget -c -q 'https://fred.stlouisfed.org/graph/fredgraph.csv?id=UNRATE' -O unemployment.csv || true",
            "wget -c -q 'https://fred.stlouisfed.org/graph/fredgraph.csv?id=PAYEMS' -O nfp.csv || true",
            "wget -c -q 'https://fred.stlouisfed.org/graph/fredgraph.csv?id=GDP' -O gdp.csv || true",
            "wget -c -q 'https://fred.stlouisfed.org/graph/fredgraph.csv?id=TOTALSA' -O auto_sales.csv || true",
            "wget -c -q 'https://fred.stlouisfed.org/graph/fredgraph.csv?id=HOUST' -O housing_starts.csv || true",
            "wget -c -q 'https://fred.stlouisfed.org/graph/fredgraph.csv?id=PERMIT' -O building_permits.csv || true",
            "wget -c -q 'https://fred.stlouisfed.org/graph/fredgraph.csv?id=INDPRO' -O industrial_prod.csv || true",
            "wget -c -q 'https://fred.stlouisfed.org/graph/fredgraph.csv?id=RSAFS' -O retail_sales.csv || true",
            "wget -c -q 'https://fred.stlouisfed.org/graph/fredgraph.csv?id=DCOILWTICO' -O wti_crude.csv || true",
            "wget -c -q 'https://fred.stlouisfed.org/graph/fredgraph.csv?id=GOLDAMGBD228NLBM' -O gold.csv || true",
        ],
    ),
    DownloadJob(
        name="world_bank_bulk",
        tier=2, category="financial",
        description="World Bank: Global development indicators bulk (GDP, trade, debt for 200+ countries)",
        dest_dir="world_bank",
        est_size_gb=2.0,
        commands=[
            "wget -c -q 'https://databank.worldbank.org/data/download/WDI_CSV.zip' -O wdi_csv.zip || true",
            "wget -c -q 'https://databank.worldbank.org/data/download/GEP_CSV.zip' -O gep_csv.zip || true",
        ],
    ),
    DownloadJob(
        name="imf_data_bulk",
        tier=2, category="financial",
        description="IMF: International Financial Statistics + World Economic Outlook bulk",
        dest_dir="imf",
        est_size_gb=1.0,
        commands=[
            "wget -c -q 'https://data.imf.org/api/v1/rest/data/IFS/M..?format=csv' -O ifs_monthly.csv 2>/dev/null || true",
            "wget -c -q 'https://www.imf.org/external/pubs/ft/weo/2025/02/weodata/WEOOct2025all.ashx' -O weo_all.csv 2>/dev/null || true",
        ],
    ),
    DownloadJob(
        name="bis_bulk_data",
        tier=2, category="financial",
        description="BIS: Credit to GDP, debt, property prices, effective exchange rates (bulk CSVs)",
        dest_dir="bis",
        est_size_gb=0.5,
        commands=[
            "wget -c -q 'https://www.bis.org/statistics/full_webstats_long_cpi_dataflow_csv.zip' -O cpi_data.zip || true",
            "wget -c -q 'https://www.bis.org/statistics/full_webstats_credit_gap_dataflow_csv.zip' -O credit_gap.zip || true",
            "wget -c -q 'https://www.bis.org/statistics/full_webstats_eer_dataflow_csv.zip' -O eer_data.zip || true",
            "wget -c -q 'https://www.bis.org/statistics/full_webstats_pp_dataflow_csv.zip' -O property_prices.zip || true",
            "wget -c -q 'https://www.bis.org/statistics/full_webstats_lbs_d_pub_dataflow_csv.zip' -O banking_stats.zip || true",
        ],
    ),
    DownloadJob(
        name="oecd_bulk",
        tier=2, category="financial",
        description="OECD: Composite Leading Indicators + Main Economic Indicators bulk",
        dest_dir="oecd",
        est_size_gb=1.0,
        commands=[
            "wget -c -q 'https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_KEI@DF_KEI,1.0/all?format=csv' -O kei_all.csv 2>/dev/null || true",
            "wget -c -q 'https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_STES@DF_CLI,1.0/all?format=csv' -O cli_all.csv 2>/dev/null || true",
        ],
    ),

    # ── TIER 2: Physical Reality Indicators ───────────────────────────

    DownloadJob(
        name="eia_bulk_electricity",
        tier=2, category="geopolitical",
        description="EIA: US electricity generation, consumption, fuel types (bulk)",
        dest_dir="eia",
        est_size_gb=2.0,
        commands=[
            "wget -c -q 'https://api.eia.gov/bulk/ELEC.zip' -O elec_bulk.zip 2>/dev/null || true",
            "wget -c -q 'https://api.eia.gov/bulk/PET.zip' -O petroleum_bulk.zip 2>/dev/null || true",
            "wget -c -q 'https://api.eia.gov/bulk/NG.zip' -O natgas_bulk.zip 2>/dev/null || true",
            "wget -c -q 'https://api.eia.gov/bulk/TOTAL.zip' -O total_energy_bulk.zip 2>/dev/null || true",
            "wget -c -q 'https://api.eia.gov/bulk/STEO.zip' -O short_term_outlook.zip 2>/dev/null || true",
        ],
    ),
    DownloadJob(
        name="noaa_weather_events",
        tier=3, category="geopolitical",
        description="NOAA: Storm events database (hurricanes, floods, droughts — economic impact)",
        dest_dir="noaa_storms",
        est_size_gb=1.0,
        commands=[
            # NOAA storm events bulk CSV by year
            "for y in $(seq 2010 2025); do wget -c -q \"https://www1.ncdc.noaa.gov/pub/data/swdi/stormevents/csvfiles/StormEvents_details-ftp_v1.0_d${y}_c20260101.csv.gz\" -O storm_${y}.csv.gz 2>/dev/null || true; done",
        ],
    ),
    DownloadJob(
        name="usda_crop_data",
        tier=3, category="geopolitical",
        description="USDA NASS: Crop production, prices, yields (bulk QuickStats)",
        dest_dir="usda",
        est_size_gb=3.0,
        commands=[
            "wget -c -q 'https://quickstats.nass.usda.gov/results/D26120F2-7E5C-36D3-A7BD-E70D0ADD53B5' -O crop_production.csv 2>/dev/null || true",
            "wget -c -q 'https://www.ers.usda.gov/webdocs/DataFiles/50048/table01.csv' -O commodity_costs.csv 2>/dev/null || true",
        ],
    ),
    DownloadJob(
        name="shipping_ais_data",
        tier=3, category="geopolitical",
        description="Marine vessel tracking data (AIS) — global shipping activity proxy",
        dest_dir="shipping",
        est_size_gb=10.0,
        commands=[
            # MarineCadastre AIS data (US waters, free)
            "wget -c -q 'https://marinecadastre.gov/ais/AIS_2024_01_01.zip' -O ais_2024_01.zip 2>/dev/null || true",
            "wget -c -q 'https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2024/index.html' -O ais_index.html 2>/dev/null || true",
        ],
    ),

    # ── TIER 2: Crypto Specific ──────────────────────────────────────

    DownloadJob(
        name="binance_historical_klines",
        tier=2, category="financial",
        description="Binance: Full historical klines (1m/1h/1d) for top 50 pairs",
        dest_dir="binance",
        est_size_gb=30.0,
        commands=[
            # Binance public data bulk
            "for pair in BTCUSDT ETHUSDT SOLUSDT BNBUSDT XRPUSDT DOGEUSDT ADAUSDT AVAXUSDT DOTUSDT LINKUSDT; do "
            "wget -c -q \"https://data.binance.vision/data/spot/daily/klines/${pair}/1d/${pair}-1d-2025-01-01.zip\" -O ${pair}_1d_2025.zip 2>/dev/null || true; "
            "done",
        ],
    ),
    DownloadJob(
        name="defi_llama_bulk",
        tier=2, category="financial",
        description="DeFiLlama: TVL, yields, stablecoins, bridges — all protocols historical",
        dest_dir="defi",
        est_size_gb=1.0,
        commands=[
            "wget -c -q 'https://api.llama.fi/protocols' -O protocols.json || true",
            "wget -c -q 'https://api.llama.fi/v2/historicalChainTvl' -O chain_tvl_history.json || true",
            "wget -c -q 'https://stablecoins.llama.fi/stablecoincharts/all?stablecoin=1' -O stablecoin_supply.json || true",
        ],
    ),
    DownloadJob(
        name="glassnode_free_metrics",
        tier=3, category="financial",
        description="On-chain metrics: addresses, hash rate, supply (free tier)",
        dest_dir="onchain",
        est_size_gb=0.5,
        commands=[
            # CoinMetrics community data
            "wget -c -q 'https://raw.githubusercontent.com/coinmetrics/data/master/csv/btc.csv' -O btc_onchain.csv || true",
            "wget -c -q 'https://raw.githubusercontent.com/coinmetrics/data/master/csv/eth.csv' -O eth_onchain.csv || true",
        ],
    ),

    # ── TIER 3: Deep Social/Reaction Signals ─────────────────────────

    DownloadJob(
        name="reddit_comments_finance",
        tier=2, category="social",
        description="Reddit comments for finance subs (deeper than submissions — reaction text)",
        dest_dir="reddit/comments",
        est_size_gb=200.0,
        commands=[
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/comment?subreddit=wallstreetbets' -O wsb_comments.zst 2>/dev/null || true",
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/comment?subreddit=stocks' -O stocks_comments.zst 2>/dev/null || true",
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/comment?subreddit=cryptocurrency' -O crypto_comments.zst 2>/dev/null || true",
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/comment?subreddit=investing' -O investing_comments.zst 2>/dev/null || true",
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/comment?subreddit=options' -O options_comments.zst 2>/dev/null || true",
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/comment?subreddit=news' -O news_comments.zst 2>/dev/null || true",
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/comment?subreddit=worldnews' -O worldnews_comments.zst 2>/dev/null || true",
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/comment?subreddit=politics' -O politics_comments.zst 2>/dev/null || true",
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/comment?subreddit=economics' -O econ_comments.zst 2>/dev/null || true",
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/comment?subreddit=conspiracy' -O conspiracy_comments.zst 2>/dev/null || true",
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/comment?subreddit=collapse' -O collapse_comments.zst 2>/dev/null || true",
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/comment?subreddit=preppers' -O preppers_comments.zst 2>/dev/null || true",
        ],
    ),
    DownloadJob(
        name="reddit_broader_social",
        tier=3, category="social",
        description="Reddit submissions for broader social pulse: antiwork, lostgeneration, layoffs, etc.",
        dest_dir="reddit/social",
        est_size_gb=50.0,
        commands=[
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/submission?subreddit=antiwork' -O antiwork.zst 2>/dev/null || true",
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/submission?subreddit=lostgeneration' -O lostgen.zst 2>/dev/null || true",
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/submission?subreddit=layoffs' -O layoffs.zst 2>/dev/null || true",
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/submission?subreddit=povertyfinance' -O poverty.zst 2>/dev/null || true",
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/submission?subreddit=realestate' -O realestate.zst 2>/dev/null || true",
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/submission?subreddit=REBubble' -O rebubble.zst 2>/dev/null || true",
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/submission?subreddit=Superstonk' -O superstonk.zst 2>/dev/null || true",
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/submission?subreddit=GME' -O gme.zst 2>/dev/null || true",
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/submission?subreddit=geopolitics' -O geopolitics.zst 2>/dev/null || true",
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/submission?subreddit=energy' -O energy.zst 2>/dev/null || true",
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/submission?subreddit=climate' -O climate.zst 2>/dev/null || true",
            "wget -c -q 'https://arctic-shift.photon-reddit.com/download/submission?subreddit=environment' -O environment.zst 2>/dev/null || true",
        ],
    ),
    DownloadJob(
        name="hackernews_archive",
        tier=3, category="social",
        description="Hacker News: full story + comment archive (tech/startup sentiment proxy)",
        dest_dir="hackernews",
        est_size_gb=5.0,
        commands=[
            # HN BigQuery dump via GitHub mirror
            "wget -c -q 'https://hacker-news.firebaseio.com/v0/maxitem.json' -O maxitem.json || true",
            "git clone --depth 1 https://github.com/minimaxir/hacker-news-undocumented.git hn_tools 2>/dev/null || true",
        ],
    ),

    # ── TIER 2: Government & Regulatory ──────────────────────────────

    DownloadJob(
        name="sec_form4_bulk",
        tier=2, category="financial",
        description="SEC EDGAR: Form 4 insider transactions bulk (all filers, last 5 years)",
        dest_dir="sec_edgar/form4",
        est_size_gb=10.0,
        commands=[
            "pip install sec-edgar-downloader 2>/dev/null || true",
            # Download full index files to parse Form 4s
            "for y in $(seq 2020 2026); do for q in 1 2 3 4; do "
            "wget -c -q \"https://www.sec.gov/Archives/edgar/full-index/${y}/QTR${q}/company.idx\" -O idx_${y}_q${q}.idx 2>/dev/null || true; "
            "done; done",
        ],
    ),
    DownloadJob(
        name="sec_13f_bulk",
        tier=2, category="financial",
        description="SEC EDGAR: 13F institutional holdings bulk (hedge fund/pension positions)",
        dest_dir="sec_edgar/13f",
        est_size_gb=5.0,
        commands=[
            "git clone --depth 1 https://github.com/git-shogg/finsec.git . 2>/dev/null || git pull",
        ],
    ),
    DownloadJob(
        name="us_spending_contracts",
        tier=2, category="geopolitical",
        description="USASpending.gov: All federal contract awards (billions in gov spending, by contractor)",
        dest_dir="usaspending",
        est_size_gb=20.0,
        commands=[
            # USASpending bulk download files
            "wget -c -q 'https://files.usaspending.gov/generated_downloads/PrimeAwardSummariesAndSubawards_2026.zip' -O awards_2026.zip 2>/dev/null || true",
            "wget -c -q 'https://files.usaspending.gov/generated_downloads/PrimeAwardSummariesAndSubawards_2025.zip' -O awards_2025.zip 2>/dev/null || true",
            "wget -c -q 'https://files.usaspending.gov/generated_downloads/PrimeAwardSummariesAndSubawards_2024.zip' -O awards_2024.zip 2>/dev/null || true",
        ],
    ),
    DownloadJob(
        name="congress_legislation_bulk",
        tier=2, category="geopolitical",
        description="Congress.gov: All bills, votes, hearings (bulk XML/JSON)",
        dest_dir="congress",
        est_size_gb=5.0,
        commands=[
            # GovInfo bulk data
            "wget -c -q 'https://www.govinfo.gov/bulkdata/BILLSTATUS/118/hr' -O bills_118_hr.xml 2>/dev/null || true",
            "wget -c -q 'https://www.govinfo.gov/bulkdata/BILLSTATUS/118/s' -O bills_118_s.xml 2>/dev/null || true",
            "wget -c -q 'https://api.congress.gov/v3/bill?limit=250&format=json' -O recent_bills.json 2>/dev/null || true",
        ],
    ),
    DownloadJob(
        name="lobbying_disclosure",
        tier=3, category="geopolitical",
        description="Senate Lobbying Disclosure: who pays who to influence what",
        dest_dir="lobbying",
        est_size_gb=1.0,
        commands=[
            "wget -c -q 'https://lda.senate.gov/filings/public/filing/search/csv/' -O lobbying_filings.csv 2>/dev/null || true",
            "wget -c -q 'https://disclosurespreview.house.gov/ld/ldxmlrelease/' -O house_ld.html 2>/dev/null || true",
        ],
    ),
    DownloadJob(
        name="fara_foreign_agents",
        tier=3, category="geopolitical",
        description="DOJ FARA: Foreign agents registered to lobby US government",
        dest_dir="fara",
        est_size_gb=0.5,
        commands=[
            "wget -c -q 'https://efile.fara.gov/ords/fara/production/fara_efile/api/v1/registrants/csv' -O fara_registrants.csv 2>/dev/null || true",
            "wget -c -q 'https://efile.fara.gov/ords/fara/production/fara_efile/api/v1/principals/csv' -O fara_principals.csv 2>/dev/null || true",
        ],
    ),

    # ── TIER 3: Satellite & Physical ─────────────────────────────────

    DownloadJob(
        name="viirs_night_lights",
        tier=3, category="geopolitical",
        description="NASA VIIRS: Night light intensity (economic activity proxy from space)",
        dest_dir="viirs",
        est_size_gb=20.0,
        commands=[
            # VIIRS monthly composites
            "wget -c -q 'https://eogdata.mines.edu/nighttime_light/monthly/v10/2025/202501/SVDNB_npp_20250101-20250131_75N180W_vcmcfg_v10_c202502141600.avg_rade9h.tif.gz' -O viirs_202501.tif.gz 2>/dev/null || true",
        ],
    ),
    DownloadJob(
        name="openstreetmap_poi",
        tier=3, category="geopolitical",
        description="OpenStreetMap: Points of interest (retail density, infrastructure proxy)",
        dest_dir="osm",
        est_size_gb=50.0,
        commands=[
            "wget -c -q 'https://download.geofabrik.de/north-america/us-latest.osm.pbf' -O us_latest.osm.pbf 2>/dev/null || true",
        ],
    ),

    # ── TIER 3: Academic & Research ──────────────────────────────────

    DownloadJob(
        name="sentfin_dataset",
        tier=3, category="sentiment",
        description="SEntFiN: 10,700+ headlines with entity-level sentiment annotation",
        dest_dir="sentiment_training/sentfin",
        est_size_gb=0.01,
        commands=[
            "git clone --depth 1 https://github.com/maxwellsarpong/NLP-financial-text-processing-dataset.git . 2>/dev/null || true",
        ],
    ),
    DownloadJob(
        name="event_registry_news",
        tier=3, category="social",
        description="Common Crawl news subset: massive web archive of news articles",
        dest_dir="common_crawl_news",
        est_size_gb=100.0,
        commands=[
            # CC-NEWS segment list
            "wget -c -q 'https://data.commoncrawl.org/crawl-data/CC-NEWS/2026/warc.paths.gz' -O cc_news_paths_2026.gz 2>/dev/null || true",
        ],
    ),
]


# ═══════════════════════════════════════════════════════════════════════
# RUNNER
# ═══════════════════════════════════════════════════════════════════════

def load_log() -> dict:
    if LOG_FILE.exists():
        return json.loads(LOG_FILE.read_text())
    return {}

def save_log(log: dict):
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    LOG_FILE.write_text(json.dumps(log, indent=2, default=str))

def run_job(job: DownloadJob) -> bool:
    dest = BASE_DIR / job.dest_dir
    dest.mkdir(parents=True, exist_ok=True)
    log.info("\n{}", '='*60)
    log.info("[{}] {} — {}", job.tier, job.name, job.description)
    log.info("    dest: {}  est: {}GB", dest, job.est_size_gb)
    log.info("{}", '='*60)

    success = True
    for cmd in job.commands:
        log.info("  $ {}...", cmd[:100])
        try:
            result = subprocess.run(
                cmd, shell=True, cwd=str(dest),
                capture_output=True, text=True, timeout=7200,  # 2h max per command
            )
            if result.returncode != 0 and "|| true" not in cmd:
                log.warning("  WARN: exit {}: {}", result.returncode, result.stderr[:200])
                success = False
            else:
                log.info("  OK")
        except subprocess.TimeoutExpired:
            log.info("  TIMEOUT (2h limit)")
            success = False
        except Exception as e:
            log.error("  ERROR: {}", e)
            success = False

    return success


def main():
    import argparse
    parser = argparse.ArgumentParser(description="GRID Bulk Data Downloader")
    parser.add_argument("--tier", type=int, help="Only run this tier (1/2/3)")
    parser.add_argument("--list", action="store_true", help="List all jobs")
    parser.add_argument("--name", type=str, help="Run specific job by name")
    args = parser.parse_args()

    if args.list:
        for j in JOBS:
            status = "DONE" if j.done else "TODO"
            log.info("  [{}] {:35s} {:6.1f}GB  {:12s}  {}", j.tier, j.name, j.est_size_gb, j.category, status)
        total = sum(j.est_size_gb for j in JOBS)
        log.info("\n  Total estimated: {:.1f}GB", total)
        return

    BASE_DIR.mkdir(parents=True, exist_ok=True)
    log = load_log()

    jobs = JOBS
    if args.tier:
        jobs = [j for j in jobs if j.tier == args.tier]
    if args.name:
        jobs = [j for j in jobs if j.name == args.name]

    log.info("GRID Bulk Data Downloader — {} jobs", len(jobs))
    log.info("Base dir: {}", BASE_DIR)

    # Check disk space
    stat = os.statvfs(str(BASE_DIR))
    free_gb = (stat.f_bavail * stat.f_frsize) / (1024**3)
    log.info("Disk free: {:.1f}GB", free_gb)

    for job in jobs:
        if log.get(job.name, {}).get("done"):
            log.info("\nSKIP {} — already done", job.name)
            continue

        ok = run_job(job)
        log[job.name] = {
            "done": ok,
            "timestamp": datetime.now().isoformat(),
            "dest": str(BASE_DIR / job.dest_dir),
        }
        save_log(log)

    log.info("\n{}", '='*60)
    log.info("COMPLETE — {}/{} jobs succeeded", sum(1 for v in log.values() if v.get('done')), len(jobs))
    log.info("Log: {}", LOG_FILE)


if __name__ == "__main__":
    main()
