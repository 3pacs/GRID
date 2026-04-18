#!/usr/bin/env python3
"""Backfill Surfacer's high-volume universe from paid data sources.

This is the paid-source lane for Surfacer enrichment:
- Tiingo: adjusted OHLCV, fundamentals, and ticker news.
- Twelve Data: statistics, splits, and dividends.
- QuiverQuant: congressional, insider, lobbying, dark-pool, WSB, flights.
- HuggingFace: financial-news/sentiment datasets for offline text priors.

The script avoids printing secrets and loads repo-local ``.env`` values before
importing pullers that read API keys at module import time.
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

import requests
from loguru import logger as log
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db import get_engine


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pull paid-source data for Surfacer tickers.")
    parser.add_argument("--limit", type=int, default=250, help="Top-volume tickers to enrich.")
    parser.add_argument("--start-date", default="2020-01-01", help="Earliest Tiingo/TwelveData date.")
    parser.add_argument("--news-days", type=int, default=30, help="Tiingo news lookback.")
    parser.add_argument("--news-limit", type=int, default=50, help="Max Tiingo articles per ticker.")
    parser.add_argument("--sleep", type=float, default=0.2, help="Delay between ticker API calls.")
    parser.add_argument("--skip-tiingo", action="store_true")
    parser.add_argument("--skip-twelve", action="store_true")
    parser.add_argument("--skip-quiver", action="store_true")
    parser.add_argument("--skip-hf", action="store_true")
    parser.add_argument(
        "--hf-subsets",
        default="twitter_financial_sentiment,twitter_financial_sentiment_val",
        help="Comma-separated HuggingFace financial-news subsets to stream.",
    )
    parser.add_argument("--hf-start-date", default=None, help="Optional HF start date.")
    return parser.parse_args(argv)


def _load_env_file() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for raw in env_path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def _top_tickers(engine: Any, limit: int) -> list[str]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT ticker
                FROM surfacer_top_volume_universe
                WHERE ticker IS NOT NULL AND ticker <> ''
                ORDER BY volume_rank ASC NULLS LAST, dollar_volume DESC NULLS LAST
                LIMIT :limit
                """
            ),
            {"limit": limit},
        ).fetchall()
    tickers = [str(r[0]).upper().strip() for r in rows if r[0]]
    if tickers:
        return tickers

    csv_path = ROOT / "data" / "tiingo_universe_tiered.csv"
    if not csv_path.exists():
        return []
    import csv

    out: list[str] = []
    with csv_path.open() as fh:
        for row in csv.DictReader(fh):
            ticker = (row.get("ticker") or row.get("symbol") or "").upper().strip()
            if ticker:
                out.append(ticker)
            if len(out) >= limit:
                break
    return out


def _ensure_source(engine: Any, name: str, base_url: str, latency: str = "EOD") -> int:
    with engine.begin() as conn:
        row = conn.execute(text("SELECT id FROM source_catalog WHERE name=:name"), {"name": name}).fetchone()
        if row:
            return int(row[0])
        row = conn.execute(
            text(
                """
                INSERT INTO source_catalog (
                    name, base_url, cost_tier, latency_class, pit_available,
                    revision_behavior, trust_score, priority_rank, active
                )
                VALUES (:name, :url, 'PAID', :latency, TRUE, 'RARE', 'HIGH', 6, TRUE)
                RETURNING id
                """
            ),
            {"name": name, "url": base_url, "latency": latency},
        ).fetchone()
        return int(row[0])


def _latest_obs(engine: Any, source_id: int, series_id: str) -> date | None:
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT MAX(obs_date)
                FROM raw_series
                WHERE source_id=:src AND series_id=:sid AND pull_status='SUCCESS'
                """
            ),
            {"src": source_id, "sid": series_id},
        ).fetchone()
    return row[0] if row and row[0] else None


def _insert_raw_once(
    engine: Any,
    source_id: int,
    series_id: str,
    obs_date: date,
    value: float,
    payload: dict[str, Any] | None = None,
) -> bool:
    with engine.begin() as conn:
        exists = conn.execute(
            text(
                """
                SELECT 1 FROM raw_series
                WHERE source_id=:src AND series_id=:sid AND obs_date=:obs
                LIMIT 1
                """
            ),
            {"src": source_id, "sid": series_id, "obs": obs_date},
        ).fetchone()
        if exists:
            return False
        conn.execute(
            text(
                """
                INSERT INTO raw_series
                    (series_id, source_id, obs_date, value, raw_payload, pull_status)
                VALUES
                    (:sid, :src, :obs, :value, CAST(:payload AS jsonb), 'SUCCESS')
                """
            ),
            {
                "sid": series_id,
                "src": source_id,
                "obs": obs_date,
                "value": float(value),
                "payload": json.dumps(payload or {}, default=str),
            },
        )
        return True


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        if isinstance(value, str):
            value = value.replace(",", "").replace("%", "").strip()
            if not value or value.lower() in {"none", "null", "nan", "-"}:
                return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_obs_date(value: Any) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _flatten_numeric(obj: dict[str, Any], prefix: str = "") -> dict[str, float]:
    out: dict[str, float] = {}
    for key, value in obj.items():
        clean = f"{prefix}_{key}" if prefix else str(key)
        clean = clean.lower().replace(" ", "_").replace("/", "_")
        if isinstance(value, dict):
            out.update(_flatten_numeric(value, clean))
            continue
        num = _to_float(value)
        if num is not None:
            out[clean] = num
    return out


def _parse_split_ratio(record: dict[str, Any]) -> float | None:
    for key in ("ratio", "split_ratio", "factor"):
        value = _to_float(record.get(key))
        if value is not None:
            return value
    desc = str(record.get("description") or "")
    if ":" in desc:
        left, right = desc.split(":", 1)
        a = _to_float(left)
        b = _to_float(right)
        if a and b:
            return a / b
    return None


def _is_twelvedata_error(data: Any) -> bool:
    if not isinstance(data, dict):
        return False
    status = str(data.get("status") or "").lower()
    if status == "error":
        return True
    return "message" in data and "code" in data and "values" not in data


def run_tiingo(engine: Any, tickers: list[str], args: argparse.Namespace) -> dict[str, int]:
    from ingestion.tiingo_fundamentals_pull import TiingoFundamentalsPuller
    from ingestion.tiingo_news_pull import TiingoNewsPuller
    from ingestion.tiingo_pull import TiingoPuller

    prices = TiingoPuller(engine)
    fundamentals = TiingoFundamentalsPuller(engine)
    news = TiingoNewsPuller(engine)

    counts = {"price_rows": 0, "fundamental_rows": 0, "news_rows": 0}
    today = date.today()
    base_start = date.fromisoformat(args.start_date)
    news_start = (today - timedelta(days=args.news_days)).isoformat()

    for i, ticker in enumerate(tickers, 1):
        log.info("Tiingo {i}/{n}: {ticker}", i=i, n=len(tickers), ticker=ticker)

        latest_price = _latest_obs(engine, prices.source_id, f"YF:{ticker}:close")
        price_start = max(base_start, latest_price + timedelta(days=1)) if latest_price else base_start
        if price_start <= today:
            res = prices.pull_ticker(ticker, start_date=price_start)
            counts["price_rows"] += int(res.get("rows_inserted", 0) or 0)

        latest_fund = _latest_obs(engine, fundamentals.source_id, f"TIINGO_FUND:{ticker}:market_cap")
        fund_start = max(base_start, latest_fund + timedelta(days=1)) if latest_fund else base_start
        if fund_start <= today:
            res = fundamentals.pull_ticker(ticker, start_date=fund_start)
            counts["fundamental_rows"] += int(res.get("rows_inserted", 0) or 0)

        latest_news = _latest_obs(engine, news.source_id, f"TIINGO_NEWS:{ticker}:daily_count")
        if latest_news != today:
            res = news.pull_ticker_news(ticker, start_date=news_start, limit=args.news_limit)
            counts["news_rows"] += int(res.get("rows_inserted", 0) or 0)

        time.sleep(args.sleep)

    return counts


def run_twelve_data(engine: Any, tickers: list[str], args: argparse.Namespace) -> dict[str, int]:
    api_key = os.environ.get("TWELVEDATA_API_KEY", "")
    if not api_key:
        log.warning("TWELVEDATA_API_KEY not configured")
        return {"stats_rows": 0, "split_rows": 0, "dividend_rows": 0}

    stats_src = _ensure_source(engine, "TWELVEDATA_STATS", "https://api.twelvedata.com/statistics")
    split_src = _ensure_source(engine, "TWELVEDATA_SPLITS", "https://api.twelvedata.com/splits")
    div_src = _ensure_source(engine, "TWELVEDATA_DIVIDENDS", "https://api.twelvedata.com/dividends")
    session = requests.Session()
    counts = {"stats_rows": 0, "split_rows": 0, "dividend_rows": 0}
    today = date.today()

    for i, ticker in enumerate(tickers, 1):
        log.info("TwelveData {i}/{n}: {ticker}", i=i, n=len(tickers), ticker=ticker)

        for endpoint, source_id, key in (
            ("statistics", stats_src, "stats_rows"),
            ("splits", split_src, "split_rows"),
            ("dividends", div_src, "dividend_rows"),
        ):
            try:
                resp = session.get(
                    f"https://api.twelvedata.com/{endpoint}",
                    params={"symbol": ticker, "apikey": api_key},
                    timeout=20,
                )
                if not resp.ok:
                    continue
                data = resp.json()
            except Exception as exc:
                log.debug("TwelveData {endpoint} failed for {ticker}: {exc}", endpoint=endpoint, ticker=ticker, exc=exc)
                continue
            if _is_twelvedata_error(data):
                log.debug(
                    "TwelveData {endpoint} unavailable for {ticker}: {msg}",
                    endpoint=endpoint,
                    ticker=ticker,
                    msg=data.get("message"),
                )
                continue

            if endpoint == "statistics":
                stats = data.get("statistics") if isinstance(data, dict) else None
                if not isinstance(stats, dict):
                    stats = data if isinstance(data, dict) else {}
                for metric, value in _flatten_numeric(stats).items():
                    if _insert_raw_once(
                        engine,
                        source_id,
                        f"TWELVEDATA_STATS:{ticker}:{metric}",
                        today,
                        value,
                        {"ticker": ticker, "metric": metric, "source": "twelvedata"},
                    ):
                        counts[key] += 1

            elif endpoint == "splits":
                records = data.get("splits") if isinstance(data, dict) else []
                for rec in records or []:
                    if not isinstance(rec, dict):
                        continue
                    obs = _parse_obs_date(rec.get("date") or rec.get("ex_date"))
                    ratio = _parse_split_ratio(rec)
                    if obs and ratio is not None and _insert_raw_once(
                        engine,
                        source_id,
                        f"TWELVEDATA_SPLITS:{ticker}:ratio",
                        obs,
                        ratio,
                        rec,
                    ):
                        counts[key] += 1

            elif endpoint == "dividends":
                records = data.get("dividends") if isinstance(data, dict) else []
                for rec in records or []:
                    if not isinstance(rec, dict):
                        continue
                    obs = _parse_obs_date(rec.get("date") or rec.get("ex_date") or rec.get("payment_date"))
                    amount = _to_float(rec.get("amount") or rec.get("dividend") or rec.get("cash_amount"))
                    if obs and amount is not None and _insert_raw_once(
                        engine,
                        source_id,
                        f"TWELVEDATA_DIVIDENDS:{ticker}:amount",
                        obs,
                        amount,
                        rec,
                    ):
                        counts[key] += 1

            time.sleep(args.sleep)

    return counts


def run_quiver(engine: Any) -> dict[str, Any]:
    from ingestion.altdata.quiverquant import pull_all

    results = pull_all(engine)
    return {
        "endpoints": len(results),
        "ok": sum(1 for r in results if r.get("status") == "SUCCESS"),
        "stored": sum(int(r.get("stored", 0) or 0) for r in results),
    }


def run_hf(engine: Any, args: argparse.Namespace) -> dict[str, Any]:
    from ingestion.altdata.hf_financial_news import HFFinancialNewsPuller

    subsets = [s.strip() for s in args.hf_subsets.split(",") if s.strip()]
    if not subsets:
        return {"subsets": 0, "rows": 0}
    puller = HFFinancialNewsPuller(engine)
    results = puller.pull_all(subsets=subsets, start_date=args.hf_start_date)
    return {
        "subsets": len(results),
        "ok": sum(1 for r in results if r.get("status") == "SUCCESS"),
        "rows": sum(int(r.get("rows_inserted", 0) or 0) for r in results),
    }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    _load_env_file()
    engine = get_engine()
    tickers = _top_tickers(engine, args.limit)
    if not tickers:
        log.error("No Surfacer tickers found")
        return 1

    summary: dict[str, Any] = {"tickers": len(tickers)}
    if not args.skip_tiingo:
        summary["tiingo"] = run_tiingo(engine, tickers, args)
    if not args.skip_twelve:
        summary["twelve_data"] = run_twelve_data(engine, tickers, args)
    if not args.skip_quiver:
        summary["quiverquant"] = run_quiver(engine)
    if not args.skip_hf:
        summary["huggingface"] = run_hf(engine, args)

    log.info("Paid source backfill complete: {summary}", summary=summary)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
