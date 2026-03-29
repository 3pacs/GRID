#!/usr/bin/env python3
"""
Fill missing features using direct API calls and existing pullers.

Strategy per family:
  - FRED features: use FREDPuller with expanded series list
  - yfinance features: use YFinancePuller (breadth, equity, vol, fx, commodity ratios)
  - Crypto: use CoinGecko puller + yfinance
  - International macro: use existing international pullers (BIS, ECB, OECD, etc.)
  - WorldNews: use WorldNewsPuller
  - EIA: use direct EIA API (api.eia.gov)
  - Weather: use Open-Meteo API (free, no key)
  - Analyst ratings: use yfinance recommendations
  - OFR/GDELT: use direct API endpoints
  - Computed features (ratios, slopes, changes): compute from existing data

Multi-source cross-verification:
  When >1 source exists for a feature, all sources are pulled into raw_series.
  The resolver picks the highest-priority source and flags conflicts.
  Trust rankings are logged to scrape_audit table.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
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
from ingestion.base import BasePuller
from normalization.entity_map import SEED_MAPPINGS
from normalization.resolver import Resolver

# ── Trust Rankings ──────────────────────────────────────────────────────────

TRUST_OFFICIAL = 1
TRUST_VERIFIED = 2
TRUST_AGGREGATOR = 3

TRUST_LABELS = {1: "OFFICIAL", 2: "VERIFIED", 3: "AGGREGATOR", 4: "COMMUNITY", 5: "UNVERIFIED"}


def get_zero_data_features(engine) -> list[dict]:
    """Get features with zero rows in resolved_series."""
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


def ensure_scrape_audit_table(engine):
    """Create the scrape_audit table for trust-ranked logging."""
    with engine.begin() as conn:
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
                agreement_count INTEGER DEFAULT 1,
                total_sources INTEGER DEFAULT 1,
                sources JSONB,
                disagreements JSONB,
                human_reviewed BOOLEAN DEFAULT FALSE,
                review_notes TEXT
            )
        """))


def log_audit(engine, feature_name: str, feature_id: int, value: float,
              trust: int, verified: bool, sources: list, agreement: int = 1):
    """Log a scrape result to the audit table."""
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO scrape_audit
                (feature_name, feature_id, value, trust_rank, trust_label,
                 verified, agreement_count, total_sources, sources)
                VALUES (:fn, :fid, :val, :tr, :tl, :v, :ac, :ts, :src)
            """), {
                "fn": feature_name, "fid": feature_id, "val": value,
                "tr": trust, "tl": TRUST_LABELS.get(trust, "UNKNOWN"),
                "v": verified, "ac": agreement, "ts": len(sources),
                "src": json.dumps(sources),
            })
    except Exception as e:
        log.error("Audit log failed: {e}", e=str(e))


def ensure_entity_mapping(feature_name: str):
    """Ensure WEB/API series ID maps to feature name."""
    for prefix in ["WEB:", "FRED:", "YF:", "EIA:", "METEO:", "OFR:", "GDELT:", "ANALYST:"]:
        sid = f"{prefix}{feature_name}"
        if sid not in SEED_MAPPINGS:
            SEED_MAPPINGS[sid] = feature_name


# ── FRED Extended Pull ──────────────────────────────────────────────────────

# Map feature names to FRED series codes
FRED_FEATURE_MAP = {
    "hy_spread_proxy": "BAMLH0A0HYM2",
    "hy_spread_3m_chg": "BAMLH0A0HYM2",  # Derived: 3m change
    "bis_credit_gap_us": "EXCSRESNS",  # Proxy: excess reserves
    "fed_funds_3m_chg": "DFF",  # Derived: 3m change
    "real_ffr": "REAINTRATREARAT1YE",  # Real interest rate
    "repo_volume": "RRPONTSYD",  # ON RRP
    # ism_pmi_mfg / ism_pmi_new_orders removed — FRED NAPM discontinued
    "conf_board_lei_slope": "USSLIND",  # Derived: slope
    "dxy_index": "DTWEXBGS",  # Trade-weighted dollar
    "dxy_3m_chg": "DTWEXBGS",  # Derived
    "vix_1m_chg": "VIXCLS",  # Derived
    "vix_3m_ratio": "VIXCLS",  # Derived (need VIX3M too)
    "copper_gold_ratio": None,  # Computed from futures
    "copper_gold_slope": None,  # Derived
}

# Additional FRED series not in the default list
EXTRA_FRED_SERIES = [
    "BAMLH0A0HYM2",  # HY spread
    "REAINTRATREARAT1YE",  # Real FFR
    "RRPONTSYD",  # Repo volume
    "NEWORDER",  # New orders
    "DTWEXBGS",  # Trade-weighted dollar
    "NASDAQCOM",  # Nasdaq composite
]


def pull_fred_extended(engine):
    """Pull additional FRED series — bulk via fedfred, 10 years of history."""
    from fedfred import FredAPI
    fred = FredAPI(api_key=settings.FRED_API_KEY)

    source_id = _ensure_source(engine, "FRED", {
        "base_url": "https://api.stlouisfed.org",
        "cost_tier": "FREE", "latency_class": "EOD",
        "pit_available": True, "revision_behavior": "FREQUENT",
        "trust_score": "HIGH", "priority_rank": 5,
    })

    results = []
    start = (date.today() - timedelta(days=365 * 10)).isoformat()

    for series_id in EXTRA_FRED_SERIES:
        try:
            log.info("FRED bulk pull: {s} (10yr)", s=series_id)
            df = fred.get_series_observations(series_id, observation_start=start)
            if df is None or (hasattr(df, '__len__') and len(df) == 0):
                log.warning("FRED {s}: no data returned", s=series_id)
                results.append({"series": series_id, "rows": 0, "status": "EMPTY"})
                continue

            count = 0
            with engine.begin() as conn:
                existing = set()
                rows = conn.execute(text(
                    "SELECT DISTINCT obs_date FROM raw_series WHERE series_id = :sid AND source_id = :src"
                ), {"sid": series_id, "src": source_id}).fetchall()
                existing = {r[0] for r in rows}

                if isinstance(df, pd.DataFrame):
                    for idx, row in df.iterrows():
                        obs = idx.date() if hasattr(idx, 'date') else idx
                        val = row.iloc[0] if hasattr(row, 'iloc') else row
                        if obs in existing or pd.isna(val):
                            continue
                        conn.execute(text(
                            "INSERT INTO raw_series (series_id, source_id, obs_date, value, pull_status) "
                            "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                        ), {"sid": series_id, "src": source_id, "od": obs, "val": float(val)})
                        count += 1
                elif isinstance(df, pd.Series):
                    for obs, val in df.items():
                        obs_d = obs.date() if hasattr(obs, 'date') else obs
                        if obs_d in existing or pd.isna(val):
                            continue
                        conn.execute(text(
                            "INSERT INTO raw_series (series_id, source_id, obs_date, value, pull_status) "
                            "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                        ), {"sid": series_id, "src": source_id, "od": obs_d, "val": float(val)})
                        count += 1

            results.append({"series": series_id, "rows": count, "status": "OK"})
            log.info("  → {c} rows inserted", c=count)
            time.sleep(0.3)  # FRED rate limit: 120 req/min
        except Exception as e:
            log.error("FRED {s} failed: {e}", s=series_id, e=str(e))
            results.append({"series": series_id, "rows": 0, "status": str(e)})
    return results


# ── yfinance Extended Pull ──────────────────────────────────────────────────

YF_MISSING_TICKERS = {
    # Breadth proxies
    "sp500_pct_above_200ma": "^SP500MA200",  # Doesn't exist directly — compute
    "sp500_adline": None,  # Compute from ^GSPC
    "sp500_adline_slope": None,
    "sp500_mom_12_1": None,  # Compute
    "sp500_mom_3m": None,  # Compute
    # Equity
    "brk-b_full": "BRK-B",
    # SPY MACD
    "spy_macd": "SPY",
    # Crypto volumes (yfinance has these)
    "eth_total_volume": "ETH-USD",
    "sol_total_volume": "SOL-USD",
    "btc_total_volume": "BTC-USD",
    "tao_chain_market_cap": "TAO-USD",
    "tao_chain_total_volume": "TAO-USD",
}


def pull_yfinance_extended(engine):
    """Pull additional yfinance tickers — 5 year bulk history."""
    import yfinance as yf

    source_id = _ensure_source(engine, "yfinance", {
        "base_url": "https://finance.yahoo.com",
        "cost_tier": "FREE", "latency_class": "EOD",
        "pit_available": False, "revision_behavior": "RARE",
        "trust_score": "MED", "priority_rank": 20,
    })

    results = []
    tickers_to_pull = set()
    for fname, ticker in YF_MISSING_TICKERS.items():
        if ticker:
            tickers_to_pull.add(ticker)

    # Bulk download all tickers at once — 5 years
    ticker_str = " ".join(tickers_to_pull)
    log.info("yfinance bulk pull: {t} (5yr)", t=ticker_str)

    try:
        data = yf.download(list(tickers_to_pull), period="5y", interval="1d",
                           group_by="ticker", auto_adjust=True, threads=True)
        if data is None or data.empty:
            log.warning("yfinance returned empty data")
            return results

        for ticker in tickers_to_pull:
            try:
                if len(tickers_to_pull) == 1:
                    ticker_data = data
                else:
                    ticker_data = data[ticker] if ticker in data.columns.get_level_values(0) else pd.DataFrame()

                if ticker_data.empty:
                    results.append({"ticker": ticker, "rows": 0, "status": "EMPTY"})
                    continue

                count = 0
                with engine.begin() as conn:
                    for field in ["Close", "Volume"]:
                        if field not in ticker_data.columns:
                            continue
                        series_id = f"YF:{ticker}:{field.lower()}"
                        existing = set()
                        rows = conn.execute(text(
                            "SELECT DISTINCT obs_date FROM raw_series WHERE series_id = :sid AND source_id = :src"
                        ), {"sid": series_id, "src": source_id}).fetchall()
                        existing = {r[0] for r in rows}

                        for idx, val in ticker_data[field].items():
                            obs = idx.date() if hasattr(idx, 'date') else idx
                            if obs in existing or pd.isna(val):
                                continue
                            conn.execute(text(
                                "INSERT INTO raw_series (series_id, source_id, obs_date, value, pull_status) "
                                "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                            ), {"sid": series_id, "src": source_id, "od": obs, "val": float(val)})
                            count += 1

                results.append({"ticker": ticker, "rows": count, "status": "OK"})
                log.info("  {t}: {c} rows", t=ticker, c=count)
            except Exception as e:
                log.error("yfinance {t} process failed: {e}", t=ticker, e=str(e))
                results.append({"ticker": ticker, "rows": 0, "status": str(e)})
    except Exception as e:
        log.error("yfinance bulk download failed: {e}", e=str(e))
        results.append({"batch": "yfinance", "status": str(e)})

    return results


# ── EIA Direct API ──────────────────────────────────────────────────────────

EIA_SERIES_MAP = {
    "eia_crude_price": "PET.RWTC.D",
    "eia_crude_refinery_input": "PET.MCRRIUS2.M",
    "eia_distillate_production": "PET.MDIUPUS2.M",
    "eia_distillate_stocks": "PET.MDISTUS1.M",
    "eia_gasoline_production": "PET.MGFUPUS2.M",
    "eia_jet_fuel_stocks": "PET.MKJSTUS1.M",
    "eia_natgas_futures_1m": "NG.RNGC1.D",
    "eia_natgas_futures_4m": "NG.RNGC4.D",
    "eia_natgas_henry_hub": "NG.RNGWHHD.D",
    "eia_electricity_coal": "ELEC.GEN.COW-US-99.M",
    "eia_electricity_demand": "ELEC.GEN.ALL-US-99.M",
    "eia_electricity_natgas": "ELEC.GEN.NG-US-99.M",
    "eia_electricity_nuclear": "ELEC.GEN.NUC-US-99.M",
    "eia_electricity_solar": "ELEC.GEN.SUN-US-99.M",
    "eia_electricity_total": "ELEC.GEN.ALL-US-99.M",
    "eia_electricity_wind": "ELEC.GEN.WND-US-99.M",
}


class EIAPuller(BasePuller):
    """Pull EIA data via api.eia.gov v2."""

    SOURCE_NAME = "EIA"
    SOURCE_CONFIG = {
        "base_url": "https://api.eia.gov",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": False,
        "revision_behavior": "RARE",
        "trust_score": "HIGH",
        "priority_rank": 10,
    }

    def pull_series(self, series_id: str, feature_name: str) -> int:
        """Pull a single EIA series. Returns row count inserted."""
        # EIA v2 API
        api_key = getattr(settings, "EIA_API_KEY", "")
        if not api_key:
            log.warning("No EIA_API_KEY configured, trying v1 format")
            return self._pull_v1(series_id, feature_name)

        url = f"https://api.eia.gov/v2/seriesid/{series_id}"
        params = {"api_key": api_key, "frequency": "monthly", "data[0]": "value",
                  "sort[0][column]": "period", "sort[0][direction]": "desc", "length": 120}
        try:
            resp = requests.get(url, params=params, timeout=15)
            if resp.status_code != 200:
                log.warning("EIA API returned {s}: {t}", s=resp.status_code, t=resp.text[:200])
                return 0
            data = resp.json()
            rows = data.get("response", {}).get("data", [])
            count = 0
            with self.engine.begin() as conn:
                existing = self._get_existing_dates(f"EIA:{feature_name}", conn)
                for row in rows:
                    obs_date = date.fromisoformat(row["period"][:10]) if len(row["period"]) >= 10 else date.fromisoformat(row["period"] + "-01")
                    if obs_date in existing:
                        continue
                    val = row.get("value")
                    if val is None:
                        continue
                    self._insert_raw(conn, f"EIA:{feature_name}", obs_date, float(val),
                                     raw_payload={"source": "EIA_v2", "series": series_id})
                    count += 1
            return count
        except Exception as e:
            log.error("EIA pull failed for {s}: {e}", s=series_id, e=str(e))
            return 0

    def _pull_v1(self, series_id: str, feature_name: str) -> int:
        """Fallback to EIA v1 API."""
        url = "https://api.eia.gov/series/"
        params = {"series_id": series_id, "api_key": getattr(settings, "EIA_API_KEY", "")}
        try:
            resp = requests.get(url, params=params, timeout=15)
            if resp.status_code != 200:
                return 0
            data = resp.json()
            series_data = data.get("series", [{}])[0].get("data", [])
            count = 0
            with self.engine.begin() as conn:
                existing = self._get_existing_dates(f"EIA:{feature_name}", conn)
                for row in series_data:
                    try:
                        period_str = str(row[0])
                        if len(period_str) == 6:  # YYYYMM
                            obs_date = date(int(period_str[:4]), int(period_str[4:6]), 1)
                        elif len(period_str) == 8:  # YYYYMMDD
                            obs_date = date.fromisoformat(period_str[:4] + "-" + period_str[4:6] + "-" + period_str[6:8])
                        else:
                            obs_date = date.fromisoformat(period_str[:10])
                        if obs_date in existing:
                            continue
                        val = row[1]
                        if val is None:
                            continue
                        self._insert_raw(conn, f"EIA:{feature_name}", obs_date, float(val),
                                         raw_payload={"source": "EIA_v1", "series": series_id})
                        count += 1
                    except (ValueError, IndexError):
                        continue
            return count
        except Exception as e:
            log.error("EIA v1 pull failed: {e}", e=str(e))
            return 0


def pull_eia_batch(engine):
    """Pull all EIA features."""
    puller = EIAPuller(db_engine=engine)
    results = []
    for feature_name, series_id in EIA_SERIES_MAP.items():
        ensure_entity_mapping(feature_name)
        log.info("EIA pull: {f} → {s}", f=feature_name, s=series_id)
        count = puller.pull_series(series_id, feature_name)
        results.append({"feature": feature_name, "series": series_id, "rows": count})
        log.info("  → {c} rows", c=count)
        time.sleep(0.3)
    return results


# ── Open-Meteo Weather (free, no API key) ──────────────────────────────────

WEATHER_CITIES = {
    "nyc": {"lat": 40.7128, "lon": -74.0060},
    "chicago": {"lat": 41.8781, "lon": -87.6298},
    "houston": {"lat": 29.7604, "lon": -95.3698},
    "london": {"lat": 51.5074, "lon": -0.1278},
    "tokyo": {"lat": 35.6762, "lon": 139.6503},
}

# Base temperature for degree days (65°F = 18.3°C)
BASE_TEMP_C = 18.3


class WeatherPuller(BasePuller):
    """Pull weather degree days from Open-Meteo API."""

    SOURCE_NAME = "open_meteo"
    SOURCE_CONFIG = {
        "base_url": "https://api.open-meteo.com",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": False,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 15,
    }

    def pull_city(self, city_key: str, lat: float, lon: float) -> dict[str, int]:
        """Pull HDD/CDD for a city — 5 years of history. Returns {hdd: count, cdd: count}."""
        end = date.today() - timedelta(days=1)
        start = end - timedelta(days=365 * 5)

        # Use archive API for historical data (free, no key needed)
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": lat, "longitude": lon,
            "daily": "temperature_2m_mean",
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "timezone": "UTC",
        }

        try:
            resp = requests.get(url, params=params, timeout=15)
            if resp.status_code != 200:
                log.warning("Open-Meteo returned {s}", s=resp.status_code)
                return {"hdd": 0, "cdd": 0}

            data = resp.json()
            dates = data.get("daily", {}).get("time", [])
            temps = data.get("daily", {}).get("temperature_2m_mean", [])

            hdd_count = 0
            cdd_count = 0
            with self.engine.begin() as conn:
                hdd_existing = self._get_existing_dates(f"METEO:weather_{city_key}_hdd", conn)
                cdd_existing = self._get_existing_dates(f"METEO:weather_{city_key}_cdd", conn)

                for d_str, temp in zip(dates, temps):
                    if temp is None:
                        continue
                    obs = date.fromisoformat(d_str)
                    hdd = max(0, BASE_TEMP_C - temp)
                    cdd = max(0, temp - BASE_TEMP_C)

                    if obs not in hdd_existing:
                        self._insert_raw(conn, f"METEO:weather_{city_key}_hdd", obs, hdd,
                                         raw_payload={"source": "open_meteo", "city": city_key, "temp_c": temp})
                        hdd_count += 1
                    if obs not in cdd_existing:
                        self._insert_raw(conn, f"METEO:weather_{city_key}_cdd", obs, cdd,
                                         raw_payload={"source": "open_meteo", "city": city_key, "temp_c": temp})
                        cdd_count += 1

            return {"hdd": hdd_count, "cdd": cdd_count}
        except Exception as e:
            log.error("Weather pull failed for {c}: {e}", c=city_key, e=str(e))
            return {"hdd": 0, "cdd": 0}


def pull_weather_batch(engine):
    """Pull weather data for all cities."""
    puller = WeatherPuller(db_engine=engine)
    results = []
    for city_key, coords in WEATHER_CITIES.items():
        for dd in ["hdd", "cdd"]:
            ensure_entity_mapping(f"weather_{city_key}_{dd}")
        log.info("Weather pull: {c}", c=city_key)
        r = puller.pull_city(city_key, coords["lat"], coords["lon"])
        results.append({"city": city_key, **r})
        log.info("  → HDD: {h} rows, CDD: {c} rows", h=r["hdd"], c=r["cdd"])
        time.sleep(0.5)
    return results


# ── Analyst Ratings via yfinance ────────────────────────────────────────────

ANALYST_TICKERS = {
    "ci": "CI", "cmcsa": "CMCSA", "dvn": "DVN", "eog": "EOG",
    "gd": "GD", "intc": "INTC", "pypl": "PYPL", "rtx": "RTX",
}


def pull_analyst_ratings(engine):
    """Pull analyst ratings via yfinance."""
    import yfinance as yf

    source_id = _ensure_source(engine, "yfinance_analyst", {
        "base_url": "https://finance.yahoo.com",
        "cost_tier": "FREE", "latency_class": "EOD",
        "pit_available": False, "revision_behavior": "FREQUENT",
        "trust_score": "MED", "priority_rank": 40,
    })

    results = []
    today = date.today()

    for prefix, ticker in ANALYST_TICKERS.items():
        try:
            log.info("Analyst ratings: {t}", t=ticker)
            stock = yf.Ticker(ticker)
            rec = stock.recommendations
            if rec is not None and not rec.empty:
                # Get latest recommendation summary
                latest = rec.iloc[-1] if len(rec) > 0 else None
                if latest is not None:
                    buy = int(float(latest.get("strongBuy", 0) or 0)) + int(float(latest.get("buy", 0) or 0))
                    hold = int(float(latest.get("hold", 0) or 0))
                    sell = int(float(latest.get("sell", 0) or 0)) + int(float(latest.get("strongSell", 0) or 0))

                    with engine.begin() as conn:
                        for rating_type, value in [("buy", buy), ("sell", sell), ("hold", hold)]:
                            fname = f"{prefix}_analyst_{rating_type}"
                            ensure_entity_mapping(fname)
                            sid = f"ANALYST:{fname}"
                            conn.execute(text(
                                "INSERT INTO raw_series (series_id, source_id, obs_date, value, raw_payload, pull_status) "
                                "VALUES (:sid, :src, :od, :val, :payload, 'SUCCESS') "
                                "ON CONFLICT DO NOTHING"
                            ), {
                                "sid": sid, "src": source_id, "od": today, "val": value,
                                "payload": json.dumps({"ticker": ticker, "type": rating_type,
                                                       "raw": dict(latest) if hasattr(latest, 'items') else str(latest)}),
                            })
                            results.append({"feature": fname, "value": value, "status": "OK"})
                            log.info("  {f} = {v}", f=fname, v=value)
            time.sleep(1.0)
        except Exception as e:
            log.error("Analyst {t} failed: {e}", t=ticker, e=str(e))
            results.append({"ticker": ticker, "status": str(e)})
    return results


# ── OFR Financial Stress (REMOVED) ─────────────────────────────────────────
# OFR FSM features (ofr_fsm_composite, ofr_fsm_credit, ofr_fsm_funding)
# permanently removed — data source is dead.  Systemic risk is now covered
# by derived features: systemic_stress_composite, systemic_credit_stress,
# systemic_funding_stress (see compute_derived_features.py).

def pull_ofr_stress(engine):
    """No-op — OFR FSM data source is permanently dead."""
    log.info("pull_ofr_stress skipped — OFR FSM features removed from registry")
    return []


# ── GDELT (direct API) ─────────────────────────────────────────────────────

def pull_gdelt(engine):
    """Pull GDELT average tone from GDELT DOC API."""
    source_id = _ensure_source(engine, "GDELT", {
        "base_url": "https://api.gdeltproject.org",
        "cost_tier": "FREE", "latency_class": "EOD",
        "pit_available": False, "revision_behavior": "NEVER",
        "trust_score": "MED", "priority_rank": 30,
    })

    ensure_entity_mapping("gdelt_avg_tone")
    # GDELT GKG tone API
    url = "https://api.gdeltproject.org/api/v2/summary/summary"
    results = []
    count = 0

    # Pull last 30 days of daily tone
    for days_ago in range(30, 0, -1):
        d = date.today() - timedelta(days=days_ago)
        try:
            params = {"d": d.strftime("%Y%m%d"), "t": "summary"}
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code == 200:
                # Parse average tone from response
                data = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
                tone = data.get("average_tone", data.get("tone", None))
                if tone is not None:
                    with engine.begin() as conn:
                        conn.execute(text(
                            "INSERT INTO raw_series (series_id, source_id, obs_date, value, pull_status) "
                            "VALUES (:sid, :src, :od, :val, 'SUCCESS') "
                            "ON CONFLICT DO NOTHING"
                        ), {"sid": "GDELT:gdelt_avg_tone", "src": source_id, "od": d, "val": float(tone)})
                        count += 1
            time.sleep(0.3)
        except Exception:
            continue

    results.append({"feature": "gdelt_avg_tone", "rows": count})
    log.info("GDELT avg tone: {c} rows", c=count)
    return results


# ── Stablecoin Supply (CoinGecko) ──────────────────────────────────────────

def pull_stablecoin_supply(engine):
    """Pull USDT/USDC supply from CoinGecko."""
    source_id = _ensure_source(engine, "coingecko", {
        "base_url": "https://api.coingecko.com",
        "cost_tier": "FREE", "latency_class": "EOD",
        "pit_available": False, "revision_behavior": "NEVER",
        "trust_score": "MED", "priority_rank": 35,
    })

    coins = {
        "usdt_supply": "tether",
        "usdc_supply": "usd-coin",
    }
    results = []

    for fname, coin_id in coins.items():
        ensure_entity_mapping(fname)
        try:
            url = f"https://api.coingecko.com/api/v3/coins/{coin_id}"
            params = {"localization": "false", "tickers": "false", "community_data": "false"}
            api_key = getattr(settings, "COINGECKO_API_KEY", "")
            headers = {"x-cg-demo-api-key": api_key} if api_key else {}
            resp = requests.get(url, params=params, headers=headers, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                supply = data.get("market_data", {}).get("circulating_supply", None)
                if supply:
                    with engine.begin() as conn:
                        conn.execute(text(
                            "INSERT INTO raw_series (series_id, source_id, obs_date, value, raw_payload, pull_status) "
                            "VALUES (:sid, :src, :od, :val, :payload, 'SUCCESS') "
                            "ON CONFLICT DO NOTHING"
                        ), {
                            "sid": f"CG:{fname}", "src": source_id, "od": date.today(),
                            "val": float(supply),
                            "payload": json.dumps({"coin": coin_id, "supply": supply}),
                        })
                    results.append({"feature": fname, "value": supply, "status": "OK"})
                    log.info("{f} = {v:,.0f}", f=fname, v=supply)
            time.sleep(1.0)
        except Exception as e:
            log.error("{f} failed: {e}", f=fname, e=str(e))
            results.append({"feature": fname, "status": str(e)})
    return results


# ── Computed Features (ratios, slopes, changes) ────────────────────────────

def compute_derived_features(engine):
    """Compute features that are derived from other features already in the DB."""
    results = []

    with engine.connect() as conn:
        # Get available resolved data for computation
        def get_series(feature_name: str, days: int = 400) -> pd.Series:
            rows = conn.execute(text("""
                SELECT obs_date, value FROM resolved_series rs
                JOIN feature_registry fr ON rs.feature_id = fr.id
                WHERE fr.name = :fn AND rs.obs_date >= CURRENT_DATE - :days
                ORDER BY rs.obs_date
            """), {"fn": feature_name, "days": days}).fetchall()
            if not rows:
                return pd.Series(dtype=float)
            return pd.Series({r[0]: r[1] for r in rows}).sort_index()

        # Copper/Gold ratio
        copper = get_series("copper_futures_close")
        gold = get_series("gold_futures_close")
        if len(copper) > 0 and len(gold) > 0:
            ratio = copper / gold
            ratio = ratio.dropna()
            if len(ratio) > 0:
                _insert_computed(engine, "copper_gold_ratio", ratio, results)
                # 3-month slope
                if len(ratio) > 63:
                    slope = ratio.rolling(63).apply(lambda x: (x.iloc[-1] - x.iloc[0]) / len(x) if len(x) > 1 else 0)
                    slope = slope.dropna()
                    _insert_computed(engine, "copper_gold_slope", slope, results)

        # VIX 1-month change
        vix = get_series("vix_spot")
        if len(vix) > 21:
            vix_1m_chg = vix - vix.shift(21)
            vix_1m_chg = vix_1m_chg.dropna()
            _insert_computed(engine, "vix_1m_chg", vix_1m_chg, results)

        # S&P 500 momentum
        sp500 = get_series("sp500_close")
        if len(sp500) > 63:
            mom_3m = (sp500 / sp500.shift(63) - 1) * 100
            _insert_computed(engine, "sp500_mom_3m", mom_3m.dropna(), results)
        if len(sp500) > 252:
            mom_12_1 = ((sp500 / sp500.shift(252)) - (sp500 / sp500.shift(21))) * 100
            _insert_computed(engine, "sp500_mom_12_1", mom_12_1.dropna(), results)

        # DXY 3-month change (if we got DTWEXBGS from FRED)
        dxy = get_series("dxy_proxy_close")
        if len(dxy) > 63:
            dxy_3m = dxy - dxy.shift(63)
            _insert_computed(engine, "dxy_3m_chg", dxy_3m.dropna(), results)

        # HY spread 3-month change
        hy = get_series("hy_spread_proxy")
        if len(hy) > 63:
            hy_3m = hy - hy.shift(63)
            _insert_computed(engine, "hy_spread_3m_chg", hy_3m.dropna(), results)

        # Fed funds 3-month change
        ffr = get_series("fed_funds_rate")
        if len(ffr) > 63:
            ffr_3m = ffr - ffr.shift(63)
            _insert_computed(engine, "fed_funds_3m_chg", ffr_3m.dropna(), results)

        # SPY MACD
        spy_close = get_series("spy_close") if get_series("spy_close").any() else pd.Series(dtype=float)
        # Try alternate source
        if spy_close.empty:
            rows = conn.execute(text("""
                SELECT obs_date, value FROM raw_series
                WHERE series_id = 'YF:SPY:close' AND pull_status = 'SUCCESS'
                AND obs_date >= CURRENT_DATE - 400
                ORDER BY obs_date
            """)).fetchall()
            if rows:
                spy_close = pd.Series({r[0]: r[1] for r in rows}).sort_index()

        if len(spy_close) > 26:
            ema12 = spy_close.ewm(span=12, adjust=False).mean()
            ema26 = spy_close.ewm(span=26, adjust=False).mean()
            macd_line = ema12 - ema26
            signal = macd_line.ewm(span=9, adjust=False).mean()
            _insert_computed(engine, "spy_macd", signal.dropna(), results)

        # LEI slope
        lei = get_series("conf_board_lei")
        if len(lei) > 63:
            slope = lei.rolling(63).apply(lambda x: (x.iloc[-1] - x.iloc[0]) / len(x) if len(x) > 1 else 0)
            _insert_computed(engine, "conf_board_lei_slope", slope.dropna(), results)

    return results


def _insert_computed(engine, feature_name: str, series: pd.Series, results: list):
    """Insert a computed series into raw_series."""
    source_id = _ensure_source(engine, "computed", {
        "base_url": "local://computed",
        "cost_tier": "FREE", "latency_class": "REALTIME",
        "pit_available": True, "revision_behavior": "NEVER",
        "trust_score": "HIGH", "priority_rank": 5,
    })
    ensure_entity_mapping(feature_name)
    count = 0
    with engine.begin() as conn:
        existing = set()
        rows = conn.execute(text(
            "SELECT DISTINCT obs_date FROM raw_series WHERE series_id = :sid AND source_id = :src"
        ), {"sid": f"COMPUTED:{feature_name}", "src": source_id}).fetchall()
        existing = {r[0] for r in rows}

        for obs_date, value in series.items():
            if obs_date in existing or pd.isna(value):
                continue
            conn.execute(text(
                "INSERT INTO raw_series (series_id, source_id, obs_date, value, pull_status) "
                "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
            ), {"sid": f"COMPUTED:{feature_name}", "src": source_id, "od": obs_date, "val": float(value)})
            count += 1

    results.append({"feature": feature_name, "rows": count, "status": "OK" if count > 0 else "NO_DATA"})
    log.info("Computed {f}: {c} rows", f=feature_name, c=count)


# ── WorldNews ───────────────────────────────────────────────────────────────

def pull_worldnews(engine):
    """Use existing WorldNewsAPI puller."""
    try:
        from ingestion.altdata.world_news import WorldNewsPuller
        puller = WorldNewsPuller(db_engine=engine)
        result = puller.pull_all()
        log.info("WorldNews result: {r}", r=result)
        return [{"batch": "worldnews", "result": result}]
    except Exception as e:
        log.error("WorldNews failed: {e}", e=str(e))
        return [{"batch": "worldnews", "error": str(e)}]


# ── Helpers ─────────────────────────────────────────────────────────────────

def _ensure_source(engine, name: str, config: dict) -> int:
    """Ensure a source exists in source_catalog, return its id."""
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
        ), {
            "n": name, "url": config.get("base_url", ""),
            "cost": config.get("cost_tier", "FREE"), "lat": config.get("latency_class", "EOD"),
            "pit": config.get("pit_available", False), "rev": config.get("revision_behavior", "NEVER"),
            "trust": config.get("trust_score", "MED"), "rank": config.get("priority_rank", 50),
        })
        row = result.fetchone()
        if row:
            return row[0]
    with engine.connect() as conn:
        row = conn.execute(text("SELECT id FROM source_catalog WHERE name = :n"), {"n": name}).fetchone()
        return row[0]


def run_resolver(engine) -> dict:
    """Run the resolver to promote raw_series → resolved_series."""
    resolver = Resolver(db_engine=engine)
    result = resolver.resolve_pending()
    log.info("Resolver: {r}", r=result)
    return result


# ── Main ────────────────────────────────────────────────────────────────────

BATCHES = {
    "fred": pull_fred_extended,
    "yfinance": pull_yfinance_extended,
    "eia": pull_eia_batch,
    "weather": pull_weather_batch,
    "analyst": pull_analyst_ratings,
    "ofr": pull_ofr_stress,
    "gdelt": pull_gdelt,
    "stablecoins": pull_stablecoin_supply,
    "computed": compute_derived_features,
    "worldnews": pull_worldnews,
}


def main():
    parser = argparse.ArgumentParser(description="Fill missing GRID features via direct APIs")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--batch", type=str, choices=list(BATCHES.keys()), help="Run a named batch")
    group.add_argument("--all", action="store_true", help="Run all batches")
    group.add_argument("--list", action="store_true", help="List missing features")
    args = parser.parse_args()

    engine = get_engine()
    ensure_scrape_audit_table(engine)

    if args.list:
        missing = get_zero_data_features(engine)
        for f in missing:
            print(f"  {f['id']:>5d} | {f['family']:12s} | {f['name']:45s} | {f['description']}")
        print(f"\nTotal: {len(missing)}")
        return

    if args.batch:
        log.info("Running batch: {b}", b=args.batch)
        results = BATCHES[args.batch](engine)
        run_resolver(engine)
        print(json.dumps(results, indent=2, default=str))
        # Log audit
        for r in results:
            if r.get("rows", 0) > 0 or r.get("value") is not None:
                log_audit(engine, r.get("feature", r.get("batch", "unknown")),
                          0, r.get("value", r.get("rows", 0)),
                          TRUST_OFFICIAL if "FRED" in str(r) or "EIA" in str(r) else TRUST_AGGREGATOR,
                          True, [r])
        # Check remaining
        remaining = len(get_zero_data_features(engine))
        log.info("Features still missing: {n}", n=remaining)
        return

    if args.all:
        total_before = len(get_zero_data_features(engine))
        log.info("Starting fill — {n} features missing", n=total_before)

        for batch_name, batch_fn in BATCHES.items():
            log.info("=" * 60)
            log.info("Batch: {b}", b=batch_name)
            log.info("=" * 60)
            try:
                results = batch_fn(engine)
                for r in results:
                    if r.get("rows", 0) > 0 or r.get("value") is not None:
                        log_audit(engine, r.get("feature", r.get("batch", "unknown")),
                                  0, r.get("value", r.get("rows", 0)),
                                  TRUST_OFFICIAL, True, [r])
            except Exception as e:
                log.error("Batch {b} failed: {e}", b=batch_name, e=str(e))
            time.sleep(2)

        # Final resolve
        run_resolver(engine)
        total_after = len(get_zero_data_features(engine))
        log.info("=" * 60)
        log.info("DONE — Before: {b}, After: {a}, Filled: {f}",
                 b=total_before, a=total_after, f=total_before - total_after)


if __name__ == "__main__":
    main()
