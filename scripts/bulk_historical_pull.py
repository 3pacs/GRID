#!/usr/bin/env python3
"""
Bulk historical data pull — downloads large datasets from trusted sources.

Sources:
  - CBOE: VIX, VIX3M, VIX9D, SKEW full history (CSV, free)
  - Binance: BTC, ETH, SOL, TAO daily klines (ZIP, free)
  - CoinGecko: Crypto market data (API, 365 days/call)
  - yfinance: Options chain snapshots + equity history (bulk download)

Trust rankings follow the scrape_audit schema:
  1=OFFICIAL, 2=VERIFIED, 3=AGGREGATOR
"""

from __future__ import annotations

import csv
import io
import json
import os
import sys
import time
import zipfile
from datetime import date, datetime, timedelta, timezone
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.chdir(os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from config import settings
from db import get_engine
from normalization.resolver import Resolver

TRUST_OFFICIAL = 1
TRUST_AGGREGATOR = 3


def _ensure_source(engine, name: str, config: dict) -> int:
    with engine.connect() as conn:
        row = conn.execute(text("SELECT id FROM source_catalog WHERE name = :n"), {"n": name}).fetchone()
        if row:
            return row[0]
    with engine.begin() as conn:
        result = conn.execute(text(
            "INSERT INTO source_catalog (name, base_url, cost_tier, latency_class, pit_available, "
            "revision_behavior, trust_score, priority_rank, active) "
            "VALUES (:n, :url, :cost, :lat, :pit, :rev, :trust, :rank, TRUE) "
            "ON CONFLICT (name) DO NOTHING RETURNING id"
        ), {"n": name, "url": config["base_url"], "cost": config.get("cost_tier", "FREE"),
            "lat": config.get("latency_class", "EOD"), "pit": config.get("pit_available", False),
            "rev": config.get("revision_behavior", "NEVER"), "trust": config.get("trust_score", "HIGH"),
            "rank": config.get("priority_rank", 10)})
        row = result.fetchone()
        if row:
            return row[0]
    with engine.connect() as conn:
        return conn.execute(text("SELECT id FROM source_catalog WHERE name = :n"), {"n": name}).fetchone()[0]


def _bulk_insert(engine, source_id: int, series_id: str, data: list[tuple[date, float]]) -> int:
    """Bulk insert into raw_series with dedup. Returns count inserted."""
    if not data:
        return 0
    count = 0
    with engine.begin() as conn:
        existing = set()
        rows = conn.execute(text(
            "SELECT DISTINCT obs_date FROM raw_series WHERE series_id = :sid AND source_id = :src"
        ), {"sid": series_id, "src": source_id}).fetchall()
        existing = {r[0] for r in rows}

        batch = []
        for obs_date, value in data:
            if obs_date in existing or pd.isna(value):
                continue
            batch.append({"sid": series_id, "src": source_id, "od": obs_date, "val": float(value)})
            count += 1

        # Batch insert for speed
        if batch:
            for i in range(0, len(batch), 1000):
                chunk = batch[i:i+1000]
                for row in chunk:
                    conn.execute(text(
                        "INSERT INTO raw_series (series_id, source_id, obs_date, value, pull_status) "
                        "VALUES (:sid, :src, :od, :val, 'SUCCESS') "
                        "ON CONFLICT DO NOTHING"
                    ), row)

    return count


# ── CBOE Bulk Downloads ────────────────────────────────────────────────────

CBOE_DATASETS = {
    "VIX_History": {
        "url": "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv",
        "series_id": "CBOE:VIX",
        "feature": "vix_spot",
        "field": "CLOSE",
    },
    "VIX3M_History": {
        "url": "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX3M_History.csv",
        "series_id": "CBOE:VIX3M",
        "feature": "vix3m_spot",
        "field": "CLOSE",
    },
    "VIX9D_History": {
        "url": "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX9D_History.csv",
        "series_id": "CBOE:VIX9D",
        "feature": "vix9d_spot",
        "field": "CLOSE",
    },
    "SKEW_History": {
        "url": "https://cdn.cboe.com/api/global/us_indices/daily_prices/SKEW_History.csv",
        "series_id": "CBOE:SKEW",
        "feature": "skew_index",
        "field": "SKEW",
    },
}


def pull_cboe_bulk(engine) -> list[dict]:
    """Download full CBOE index history CSVs."""
    source_id = _ensure_source(engine, "CBOE", {
        "base_url": "https://cdn.cboe.com",
        "cost_tier": "FREE", "latency_class": "EOD",
        "pit_available": True, "revision_behavior": "NEVER",
        "trust_score": "HIGH", "priority_rank": 5,
    })

    results = []
    for name, cfg in CBOE_DATASETS.items():
        log.info("Downloading CBOE {n}...", n=name)
        try:
            resp = requests.get(cfg["url"], timeout=30)
            if resp.status_code != 200:
                results.append({"dataset": name, "rows": 0, "error": f"HTTP {resp.status_code}"})
                continue

            # Parse CSV
            reader = csv.DictReader(io.StringIO(resp.text))
            data = []
            for row in reader:
                try:
                    date_str = row.get("DATE", "").strip()
                    if not date_str:
                        continue
                    # CBOE uses MM/DD/YYYY format
                    obs = datetime.strptime(date_str, "%m/%d/%Y").date()
                    val_str = row.get(cfg["field"], "").strip()
                    if not val_str:
                        continue
                    val = float(val_str)
                    data.append((obs, val))
                except (ValueError, KeyError):
                    continue

            count = _bulk_insert(engine, source_id, cfg["series_id"], data)
            results.append({"dataset": name, "feature": cfg["feature"],
                            "total_rows": len(data), "inserted": count,
                            "date_range": f"{data[0][0]} → {data[-1][0]}" if data else "N/A"})
            log.info("  CBOE {n}: {t} total, {c} new rows ({d})",
                     n=name, t=len(data), c=count,
                     d=f"{data[0][0]}→{data[-1][0]}" if data else "empty")
        except Exception as e:
            log.error("CBOE {n} failed: {e}", n=name, e=str(e))
            results.append({"dataset": name, "rows": 0, "error": str(e)})

    return results


# ── Binance Historical Klines (ZIP) ────────────────────────────────────────

BINANCE_PAIRS = {
    "BTCUSDT": {"feature_close": "btc_close", "feature_volume": "btc_total_volume"},
    "ETHUSDT": {"feature_close": "eth_close", "feature_volume": "eth_total_volume"},
    "SOLUSDT": {"feature_close": "sol_close", "feature_volume": "sol_total_volume"},
    "TAOUSDT": {"feature_close": "tao_chain_market_cap", "feature_volume": "tao_chain_total_volume"},
}


def pull_binance_bulk(engine) -> list[dict]:
    """Download Binance monthly kline ZIPs for full history."""
    source_id = _ensure_source(engine, "binance", {
        "base_url": "https://data.binance.vision",
        "cost_tier": "FREE", "latency_class": "EOD",
        "pit_available": False, "revision_behavior": "NEVER",
        "trust_score": "HIGH", "priority_rank": 10,
    })

    results = []
    today = date.today()

    for pair, features in BINANCE_PAIRS.items():
        log.info("Downloading Binance {p} history...", p=pair)
        all_data_close = []
        all_data_volume = []

        # Download monthly ZIPs going back 5 years
        for months_ago in range(60, -1, -1):
            d = today - timedelta(days=months_ago * 30)
            year_month = d.strftime("%Y-%m")
            url = f"https://data.binance.vision/data/spot/monthly/klines/{pair}/1d/{pair}-1d-{year_month}.zip"

            try:
                resp = requests.get(url, timeout=15)
                if resp.status_code != 200:
                    continue

                # Parse ZIP in memory
                zf = zipfile.ZipFile(io.BytesIO(resp.content))
                for name in zf.namelist():
                    if name.endswith('.csv'):
                        with zf.open(name) as f:
                            content = f.read().decode('utf-8')
                            for line in content.strip().split('\n'):
                                parts = line.split(',')
                                if len(parts) < 11:
                                    continue
                                try:
                                    # Binance kline: open_time, open, high, low, close, volume, ...
                                    ts = int(parts[0]) / 1000
                                    obs = datetime.fromtimestamp(ts, tz=timezone.utc).date()
                                    close = float(parts[4])
                                    volume_usd = float(parts[7])  # quote asset volume (USD)
                                    all_data_close.append((obs, close))
                                    all_data_volume.append((obs, volume_usd))
                                except (ValueError, IndexError):
                                    continue
                time.sleep(0.1)  # Rate limit
            except Exception as e:
                continue  # Skip months with no data (e.g., TAO before listing)

        # Insert close prices
        count_close = _bulk_insert(engine, source_id, f"BINANCE:{pair}:close", all_data_close)
        count_vol = _bulk_insert(engine, source_id, f"BINANCE:{pair}:volume", all_data_volume)

        date_range = f"{all_data_close[0][0]}→{all_data_close[-1][0]}" if all_data_close else "N/A"
        results.append({
            "pair": pair, "close_rows": count_close, "volume_rows": count_vol,
            "total_points": len(all_data_close), "date_range": date_range,
        })
        log.info("  {p}: {t} days, {cc} close + {cv} volume new rows ({d})",
                 p=pair, t=len(all_data_close), cc=count_close, cv=count_vol, d=date_range)

    return results


# ── CoinGecko Extended (365 days per call) ──────────────────────────────────

COINGECKO_COINS = {
    "bitcoin": {"close": "btc_close_cg", "volume": "btc_total_volume"},
    "ethereum": {"close": "eth_close_cg", "volume": "eth_total_volume"},
    "solana": {"close": "sol_close_cg", "volume": "sol_total_volume"},
    "bittensor": {"close": "tao_close_cg", "volume": "tao_chain_total_volume"},
    "tether": {"supply": "usdt_supply"},
    "usd-coin": {"supply": "usdc_supply"},
}


def pull_coingecko_bulk(engine) -> list[dict]:
    """Pull CoinGecko market data — max 365 days per call, loop for more."""
    source_id = _ensure_source(engine, "coingecko", {
        "base_url": "https://api.coingecko.com",
        "cost_tier": "FREE", "latency_class": "EOD",
        "pit_available": False, "revision_behavior": "NEVER",
        "trust_score": "MED", "priority_rank": 25,
    })

    api_key = getattr(settings, "COINGECKO_API_KEY", "")
    headers = {"x-cg-demo-api-key": api_key} if api_key else {}
    results = []

    for coin_id, features in COINGECKO_COINS.items():
        log.info("CoinGecko: {c}", c=coin_id)

        if "supply" in features:
            # Get current supply only
            try:
                resp = requests.get(f"https://api.coingecko.com/api/v3/coins/{coin_id}",
                                    params={"localization": "false", "tickers": "false"},
                                    headers=headers, timeout=15)
                if resp.status_code == 200:
                    data = resp.json()
                    supply = data.get("market_data", {}).get("circulating_supply")
                    if supply:
                        _bulk_insert(engine, source_id, f"CG:{features['supply']}",
                                     [(date.today(), float(supply))])
                        results.append({"coin": coin_id, "feature": features["supply"],
                                        "value": supply, "status": "OK"})
                time.sleep(1.5)
            except Exception as e:
                results.append({"coin": coin_id, "error": str(e)})
            continue

        # Market chart — loop 365 days at a time for up to 5 years
        all_prices = []
        all_volumes = []

        for chunk in range(5):  # 5 chunks × 365 days = ~5 years
            days_end = chunk * 365
            days_start = (chunk + 1) * 365
            to_ts = int((datetime.now(timezone.utc) - timedelta(days=days_end)).timestamp())
            from_ts = int((datetime.now(timezone.utc) - timedelta(days=days_start)).timestamp())

            try:
                resp = requests.get(
                    f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart/range",
                    params={"vs_currency": "usd", "from": from_ts, "to": to_ts},
                    headers=headers, timeout=30,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    for p in data.get("prices", []):
                        obs = datetime.fromtimestamp(p[0] / 1000, tz=timezone.utc).date()
                        all_prices.append((obs, p[1]))
                    for v in data.get("total_volumes", []):
                        obs = datetime.fromtimestamp(v[0] / 1000, tz=timezone.utc).date()
                        all_volumes.append((obs, v[1]))
                elif resp.status_code == 429:
                    log.warning("CoinGecko rate limited, sleeping 60s...")
                    time.sleep(60)
                    continue
                time.sleep(2)  # CoinGecko free tier: 30 calls/min
            except Exception as e:
                log.warning("CoinGecko {c} chunk {ch} failed: {e}", c=coin_id, ch=chunk, e=str(e))

        count_close = _bulk_insert(engine, source_id, f"CG:{coin_id}:close", all_prices)
        count_vol = _bulk_insert(engine, source_id, f"CG:{coin_id}:volume", all_volumes)

        date_range = f"{all_prices[0][0]}→{all_prices[-1][0]}" if all_prices else "N/A"
        results.append({
            "coin": coin_id, "days": len(all_prices),
            "close_inserted": count_close, "volume_inserted": count_vol,
            "date_range": date_range,
        })
        log.info("  {c}: {d} days, {cc} close + {cv} vol new ({dr})",
                 c=coin_id, d=len(all_prices), cc=count_close, cv=count_vol, dr=date_range)

    return results


# ── Options: yfinance bulk history for all tickers ──────────────────────────

OPTIONS_TICKERS = [
    "SPY", "QQQ", "IWM",  # Index ETFs
    "AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "META", "TSLA", "AVGO",  # Mega caps
    "JPM", "BAC", "GS", "BLK",  # Financials
    "XOM", "CVX", "DVN", "EOG",  # Energy
    "UNH", "JNJ", "CI", "ABBV",  # Healthcare
    "LMT", "RTX", "GD",  # Defense
    "COST", "HD", "WMT",  # Consumer
    "CMCSA", "DIS",  # Media
    "INTC", "AMD", "TSM",  # Semis
    "PYPL", "V", "MA",  # Payments
    "BRK-B",  # Berkshire
]


def pull_options_history(engine) -> list[dict]:
    """Pull options metrics history for all tickers via yfinance.

    yfinance gives us current options chains — we extract PCR, IV, max pain, OI.
    For historical IV, we can use the VIX as a proxy or pull from CBOE.
    """
    import yfinance as yf

    source_id = _ensure_source(engine, "yfinance_options", {
        "base_url": "https://finance.yahoo.com",
        "cost_tier": "FREE", "latency_class": "EOD",
        "pit_available": False, "revision_behavior": "FREQUENT",
        "trust_score": "MED", "priority_rank": 30,
    })

    results = []
    today = date.today()

    for ticker_sym in OPTIONS_TICKERS:
        try:
            log.info("Options: {t}", t=ticker_sym)
            ticker = yf.Ticker(ticker_sym)

            # Get options chain for nearest expiry
            try:
                expirations = ticker.options
                if not expirations:
                    results.append({"ticker": ticker_sym, "status": "NO_OPTIONS"})
                    continue

                # Get first 3 expirations for term structure
                chains = []
                for exp in expirations[:3]:
                    try:
                        chain = ticker.option_chain(exp)
                        chains.append({"expiry": exp, "calls": chain.calls, "puts": chain.puts})
                    except Exception:
                        continue

                if not chains:
                    results.append({"ticker": ticker_sym, "status": "CHAIN_EMPTY"})
                    continue

                # Extract metrics from nearest expiry
                nearest = chains[0]
                calls = nearest["calls"]
                puts = nearest["puts"]

                # Put/Call ratio (by volume)
                call_vol = calls["volume"].sum() if "volume" in calls else 0
                put_vol = puts["volume"].sum() if "volume" in puts else 0
                pcr = put_vol / call_vol if call_vol > 0 else 0

                # Total open interest
                call_oi = calls["openInterest"].sum() if "openInterest" in calls else 0
                put_oi = puts["openInterest"].sum() if "openInterest" in puts else 0
                total_oi = call_oi + put_oi

                # Implied volatility (ATM average)
                all_iv = pd.concat([
                    calls["impliedVolatility"] if "impliedVolatility" in calls else pd.Series(dtype=float),
                    puts["impliedVolatility"] if "impliedVolatility" in puts else pd.Series(dtype=float),
                ])
                iv_atm = all_iv.median() if len(all_iv) > 0 else 0

                # IV skew (25d put IV - 25d call IV proxy: use OTM options)
                info = ticker.info or {}
                current_price = info.get("regularMarketPrice", info.get("previousClose", 0))
                if current_price > 0:
                    otm_puts = puts[puts["strike"] < current_price * 0.95]
                    otm_calls = calls[calls["strike"] > current_price * 1.05]
                    iv_25d_put = otm_puts["impliedVolatility"].median() if len(otm_puts) > 0 else iv_atm
                    iv_25d_call = otm_calls["impliedVolatility"].median() if len(otm_calls) > 0 else iv_atm
                    iv_skew = iv_25d_put - iv_25d_call
                else:
                    iv_25d_put = iv_atm
                    iv_25d_call = iv_atm
                    iv_skew = 0

                # Max pain (strike with max total OI)
                all_strikes = pd.concat([
                    calls[["strike", "openInterest"]].rename(columns={"openInterest": "call_oi"}),
                    puts[["strike", "openInterest"]].rename(columns={"openInterest": "put_oi"}),
                ], ignore_index=True).groupby("strike").sum()
                if len(all_strikes) > 0:
                    all_strikes["total"] = all_strikes.get("call_oi", 0) + all_strikes.get("put_oi", 0)
                    max_pain = all_strikes["total"].idxmax()
                else:
                    max_pain = 0

                # OI concentration (top 5 strikes % of total)
                if total_oi > 0 and len(all_strikes) > 0:
                    top5_oi = all_strikes.nlargest(5, "total")["total"].sum()
                    oi_conc = top5_oi / total_oi
                else:
                    oi_conc = 0

                # Total options volume
                opt_vol = call_vol + put_vol

                # Term slope (if 2+ expirations)
                term_slope = 0
                if len(chains) >= 2:
                    iv1 = chains[0]["calls"]["impliedVolatility"].median() if len(chains[0]["calls"]) > 0 else 0
                    iv2 = chains[1]["calls"]["impliedVolatility"].median() if len(chains[1]["calls"]) > 0 else 0
                    if iv1 > 0:
                        term_slope = (iv2 - iv1) / iv1

                # Insert all metrics
                prefix = ticker_sym.lower().replace("-", "_")
                metrics = {
                    f"{prefix}_pcr": pcr,
                    f"{prefix}_iv_atm": iv_atm,
                    f"{prefix}_iv_skew": iv_skew,
                    f"{prefix}_iv_25d_put": iv_25d_put,
                    f"{prefix}_iv_25d_call": iv_25d_call,
                    f"{prefix}_max_pain": max_pain,
                    f"{prefix}_total_oi": total_oi,
                    f"{prefix}_opt_vol": opt_vol,
                    f"{prefix}_oi_conc": oi_conc,
                    f"{prefix}_term_slope": term_slope,
                }

                count = 0
                with engine.begin() as conn:
                    for metric_name, value in metrics.items():
                        if value is None or pd.isna(value):
                            continue
                        sid = f"OPT:{ticker_sym}:{metric_name.split('_', 1)[1] if '_' in metric_name else metric_name}"
                        # Check if already exists
                        existing = conn.execute(text(
                            "SELECT 1 FROM raw_series WHERE series_id = :sid AND source_id = :src AND obs_date = :od LIMIT 1"
                        ), {"sid": sid, "src": source_id, "od": today}).fetchone()
                        if not existing:
                            conn.execute(text(
                                "INSERT INTO raw_series (series_id, source_id, obs_date, value, pull_status) "
                                "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                            ), {"sid": sid, "src": source_id, "od": today, "val": float(value)})
                            count += 1

                results.append({"ticker": ticker_sym, "metrics_inserted": count, "pcr": round(pcr, 3),
                                "iv_atm": round(iv_atm, 4), "max_pain": max_pain, "status": "OK"})
                log.info("  {t}: pcr={p:.3f} iv={iv:.4f} max_pain={mp} oi={oi:,} ({c} new)",
                         t=ticker_sym, p=pcr, iv=iv_atm, mp=max_pain, oi=total_oi, c=count)

            except Exception as e:
                results.append({"ticker": ticker_sym, "status": str(e)})
                log.error("  {t} options failed: {e}", t=ticker_sym, e=str(e))

            time.sleep(1.0)  # Rate limit

        except Exception as e:
            results.append({"ticker": ticker_sym, "status": str(e)})

    return results


# ── DexScreener + DeFiLlama (Solana DeFi) ──────────────────────────────────

def pull_defi_data(engine) -> list[dict]:
    """Pull Solana DeFi data from DeFiLlama and DexScreener."""
    source_id = _ensure_source(engine, "defillama", {
        "base_url": "https://api.llama.fi",
        "cost_tier": "FREE", "latency_class": "EOD",
        "pit_available": False, "revision_behavior": "NEVER",
        "trust_score": "MED", "priority_rank": 30,
    })

    results = []

    # DeFiLlama: Solana DEX volume history
    try:
        log.info("DeFiLlama: Solana DEX volumes...")
        resp = requests.get("https://api.llama.fi/overview/dexs/solana?excludeTotalDataChart=false",
                            timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            chart_data = data.get("totalDataChart", [])
            dex_data = []
            for point in chart_data:
                try:
                    ts = point[0]
                    vol = point[1]
                    obs = datetime.fromtimestamp(ts, tz=timezone.utc).date()
                    dex_data.append((obs, vol))
                except (ValueError, IndexError):
                    continue
            count = _bulk_insert(engine, source_id, "DEFILLAMA:solana_dex_volume", dex_data)
            results.append({"feature": "dex_sol_volume_24h", "rows": count,
                            "total_points": len(dex_data), "status": "OK"})
            log.info("  Solana DEX volume: {t} days, {c} new", t=len(dex_data), c=count)
    except Exception as e:
        log.error("DeFiLlama failed: {e}", e=str(e))
        results.append({"feature": "dex_sol_volume_24h", "error": str(e)})

    time.sleep(1)

    # DeFiLlama: Solana TVL (liquidity proxy)
    try:
        log.info("DeFiLlama: Solana TVL...")
        resp = requests.get("https://api.llama.fi/v2/historicalChainTvl/Solana", timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            tvl_data = []
            for point in data:
                try:
                    obs = datetime.fromtimestamp(point["date"], tz=timezone.utc).date()
                    tvl_data.append((obs, point["tvl"]))
                except (ValueError, KeyError):
                    continue
            count = _bulk_insert(engine, source_id, "DEFILLAMA:solana_tvl", tvl_data)
            results.append({"feature": "dex_sol_liquidity", "rows": count,
                            "total_points": len(tvl_data), "status": "OK"})
            log.info("  Solana TVL: {t} days, {c} new", t=len(tvl_data), c=count)
    except Exception as e:
        log.error("DeFiLlama TVL failed: {e}", e=str(e))

    time.sleep(1)

    # DexScreener: current Solana stats
    try:
        log.info("DexScreener: Solana pairs...")
        resp = requests.get("https://api.dexscreener.com/latest/dex/search?q=SOL", timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            pairs = data.get("pairs", [])
            sol_pairs = [p for p in pairs if p.get("chainId") == "solana"][:50]

            if sol_pairs:
                today_date = date.today()
                total_txns = sum(p.get("txns", {}).get("h24", {}).get("buys", 0) +
                                 p.get("txns", {}).get("h24", {}).get("sells", 0) for p in sol_pairs)
                total_buys = sum(p.get("txns", {}).get("h24", {}).get("buys", 0) for p in sol_pairs)
                total_sells = sum(p.get("txns", {}).get("h24", {}).get("sells", 0) for p in sol_pairs)
                buy_sell_ratio = total_buys / total_sells if total_sells > 0 else 1.0
                avg_change = sum(float(p.get("priceChange", {}).get("h24", 0) or 0) for p in sol_pairs) / len(sol_pairs)
                boosted = sum(1 for p in sol_pairs if p.get("boosts", {}).get("active", 0) > 0)

                dex_metrics = [
                    ("DEXSCR:sol_txn_count", total_txns),
                    ("DEXSCR:sol_buy_sell_ratio", buy_sell_ratio),
                    ("DEXSCR:sol_momentum_24h", avg_change),
                    ("DEXSCR:sol_boosted_tokens", boosted),
                ]

                with engine.begin() as conn:
                    for sid, val in dex_metrics:
                        conn.execute(text(
                            "INSERT INTO raw_series (series_id, source_id, obs_date, value, pull_status) "
                            "VALUES (:sid, :src, :od, :val, 'SUCCESS') ON CONFLICT DO NOTHING"
                        ), {"sid": sid, "src": source_id, "od": today_date, "val": float(val)})

                results.append({"feature": "dex_sol_metrics", "txns": total_txns,
                                "buy_sell": round(buy_sell_ratio, 3), "momentum": round(avg_change, 2),
                                "boosted": boosted, "status": "OK"})
                log.info("  DexScreener: txns={t} buysell={bs:.3f} mom={m:.2f}% boosted={b}",
                         t=total_txns, bs=buy_sell_ratio, m=avg_change, b=boosted)
    except Exception as e:
        log.error("DexScreener failed: {e}", e=str(e))

    return results


# ── Polymarket ──────────────────────────────────────────────────────────────

def pull_polymarket(engine) -> list[dict]:
    """Pull Polymarket BTC prediction from their API."""
    source_id = _ensure_source(engine, "polymarket", {
        "base_url": "https://gamma-api.polymarket.com",
        "cost_tier": "FREE", "latency_class": "REALTIME",
        "pit_available": False, "revision_behavior": "FREQUENT",
        "trust_score": "MED", "priority_rank": 40,
    })

    results = []
    try:
        # Search for Bitcoin price markets
        resp = requests.get("https://gamma-api.polymarket.com/markets?tag=crypto&limit=10",
                            timeout=10)
        if resp.status_code == 200:
            markets = resp.json()
            for m in markets:
                if "bitcoin" in m.get("question", "").lower() or "btc" in m.get("question", "").lower():
                    price = m.get("outcomePrices", [0])[0] if m.get("outcomePrices") else None
                    if price:
                        _bulk_insert(engine, source_id, "POLYMARKET:btc",
                                     [(date.today(), float(price))])
                        results.append({"feature": "polymarket_btc", "value": price, "status": "OK"})
                        log.info("  Polymarket BTC: {p}", p=price)
                    break
    except Exception as e:
        log.warning("Polymarket failed: {e}", e=str(e))
        results.append({"feature": "polymarket_btc", "error": str(e)})

    return results


# ── Main ────────────────────────────────────────────────────────────────────

BATCHES = {
    "cboe": pull_cboe_bulk,
    "binance": pull_binance_bulk,
    "coingecko": pull_coingecko_bulk,
    "options": pull_options_history,
    "defi": pull_defi_data,
    "polymarket": pull_polymarket,
}


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Bulk historical data pull")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--batch", type=str, choices=list(BATCHES.keys()))
    group.add_argument("--all", action="store_true")
    args = parser.parse_args()

    engine = get_engine()

    if args.batch:
        log.info("Running batch: {b}", b=args.batch)
        results = BATCHES[args.batch](engine)
        # Resolve
        resolver = Resolver(db_engine=engine)
        resolver.resolve_pending()
        print(json.dumps(results, indent=2, default=str))
        return

    if args.all:
        for name, fn in BATCHES.items():
            log.info("=" * 60)
            log.info("Batch: {b}", b=name)
            try:
                results = fn(engine)
                print(f"\n--- {name} ---")
                print(json.dumps(results, indent=2, default=str))
            except Exception as e:
                log.error("{b} failed: {e}", b=name, e=str(e))
            time.sleep(2)

        # Final resolve
        resolver = Resolver(db_engine=engine)
        r = resolver.resolve_pending()
        log.info("Final resolve: {r}", r=r)


if __name__ == "__main__":
    main()
