#!/usr/bin/env python3
"""Backfill news + price coverage for small/mid-cap gem-watchlist tickers.

Why this exists
---------------
Gem-hunter cross-class checks (e.g. insider_cluster_check) require multiple
data classes (news, prices, congress, signals) over a 14-day window before a
candidate passes. Mid/small-caps like FLUT, GEHC, GLND, BHRB clear the
insider-cluster score gate (0.83-0.99) but get rejected because the broad
pullers (Polygon /news with no ticker filter, Twelve Data macro pulls)
under-cover them. This script targets them directly.

What it does
------------
For each ticker on the watchlist:
  1. News: GET Polygon /v2/reference/news?ticker=<TICKER>&limit=50&order=desc
     over the last ``--days`` window. Upsert into ``news_articles`` with
     normalized sentiment, dedup on sha256(url)[:32]. Re-uses the same
     parsing logic as ``ingestion/altdata/polygon_news.py``.
  2. Prices: GET Twelve Data /time_series?symbol=<TICKER>&interval=1day
     over the same window. Upsert into ``ticker_metrics_daily`` keyed on
     (ticker, obs_date) — only fills close_price + source.

Idempotent (ON CONFLICT DO NOTHING on news, ON CONFLICT (ticker, obs_date)
DO UPDATE on prices).

Usage
-----
    python3 scripts/backfill_smallcap_coverage.py \\
        --tickers FLUT,GEHC,GLND,BHRB,OPCH,SPGI,PSUS

    # Single ticker, custom window
    python3 scripts/backfill_smallcap_coverage.py --tickers FLUT --days 90

    # Watchlist file (one ticker per line, # comments OK)
    python3 scripts/backfill_smallcap_coverage.py --watchlist /etc/grid/gem-watchlist.txt
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text

# Ensure grid root is on sys.path
_GRID_DIR = str(Path(__file__).resolve().parent.parent)
os.chdir(_GRID_DIR)
if _GRID_DIR not in sys.path:
    sys.path.insert(0, _GRID_DIR)

from config import settings  # noqa: E402
from sqlalchemy import create_engine  # noqa: E402

_POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "")
_TWELVEDATA_API_KEY = os.getenv("TWELVEDATA_API_KEY", "")
_POLYGON_NEWS_URL = "https://api.polygon.io/v2/reference/news"
_TWELVEDATA_TS_URL = "https://api.twelvedata.com/time_series"
_REQUEST_TIMEOUT = 30
_RATE_LIMIT_DELAY = 0.3  # courtesy delay between requests
# Twelve Data free/cheap tier: 8 credits/minute. /time_series = 1 credit/call.
# Stay well under: 7.5 sec spacing = ~8/min hard ceiling.
_TWELVEDATA_MIN_INTERVAL_S = 8.0
_LAST_TWELVEDATA_CALL = [0.0]  # mutable singleton; updated in _throttle_twelvedata


def _throttle_twelvedata() -> None:
    """Sleep enough to respect Twelve Data's per-minute credit cap."""
    elapsed = time.monotonic() - _LAST_TWELVEDATA_CALL[0]
    if elapsed < _TWELVEDATA_MIN_INTERVAL_S:
        time.sleep(_TWELVEDATA_MIN_INTERVAL_S - elapsed)
    _LAST_TWELVEDATA_CALL[0] = time.monotonic()


def _polygon_get_with_retry(url: str, params: dict, max_retries: int = 4) -> dict | None:
    """GET against Polygon with exponential backoff on 429."""
    backoff = 2.0
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=params, timeout=_REQUEST_TIMEOUT)
            if resp.status_code == 429:
                log.info(
                    "Polygon 429 (attempt {a}); sleeping {s:.1f}s",
                    a=attempt + 1,
                    s=backoff,
                )
                time.sleep(backoff)
                backoff *= 2
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as exc:
            log.warning("Polygon HTTP error: {e}", e=str(exc))
            return None
        except Exception as exc:
            log.warning("Polygon error: {e}", e=str(exc))
            return None
    log.warning("Polygon: exhausted retries")
    return None


# --- Polygon news helpers (mirrors ingestion/altdata/polygon_news.py) ---

def _aggregate_insight_sentiment(insights: list[dict]) -> tuple[str, float]:
    """Collapse Polygon per-ticker insights into article-level sentiment."""
    if not insights:
        return "NEUTRAL", 0.5
    counts = {"positive": 0, "negative": 0, "neutral": 0}
    for ins in insights:
        s = (ins.get("sentiment") or "").lower()
        if s in counts:
            counts[s] += 1
    total = sum(counts.values())
    if total == 0:
        return "NEUTRAL", 0.5
    if counts["positive"] > counts["negative"] and counts["positive"] > counts["neutral"]:
        winner, label = counts["positive"], "BULLISH"
    elif counts["negative"] > counts["positive"] and counts["negative"] > counts["neutral"]:
        winner, label = counts["negative"], "BEARISH"
    else:
        winner, label = max(counts["neutral"], 1), "NEUTRAL"
    confidence = min(0.95, 0.5 + (winner / total) * 0.45)
    return label, confidence


def _filter_tickers(raw: list[Any]) -> list[str]:
    out: list[str] = []
    for t in raw or []:
        if not isinstance(t, str):
            continue
        t = t.strip().upper()
        base = t.split(".")[0]
        if 1 <= len(base) <= 5 and base.isalpha():
            out.append(t)
    return out[:20]


def _fetch_polygon_news(ticker: str, days: int) -> list[dict]:
    """Fetch up to 50 newest Polygon articles for one ticker within window."""
    if not _POLYGON_API_KEY:
        log.warning("POLYGON_API_KEY missing; skipping news for {t}", t=ticker)
        return []
    published_gte = (
        datetime.now(timezone.utc) - timedelta(days=days)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    params = {
        "ticker": ticker,
        "limit": 50,
        "order": "desc",
        "sort": "published_utc",
        "published_utc.gte": published_gte,
        "apiKey": _POLYGON_API_KEY,
    }
    payload = _polygon_get_with_retry(_POLYGON_NEWS_URL, params)
    if payload is None:
        return []
    return payload.get("results", []) or []


def _upsert_news(conn, art: dict, target_ticker: str) -> bool:
    url = (art.get("article_url") or "").strip()
    title = (art.get("title") or "").strip()
    if not url or not title:
        return False
    dedup_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()[:32]

    # Parse published_at
    published_at = None
    ts_str = art.get("published_utc")
    if ts_str:
        try:
            published_at = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        except (TypeError, ValueError):
            published_at = None
    if published_at is None:
        published_at = datetime.now(timezone.utc)

    tickers = _filter_tickers(art.get("tickers") or [])
    # Guarantee our target ticker is in the array (Polygon should already
    # include it for ticker-filtered queries, but belt + suspenders so the
    # backfill is provably attributable to this ticker).
    if target_ticker.upper() not in tickers:
        tickers.append(target_ticker.upper())

    sentiment, confidence = _aggregate_insight_sentiment(art.get("insights") or [])
    summary = (art.get("description") or "")[:2000]
    publisher = ((art.get("publisher") or {}).get("name") or "").strip()
    if publisher:
        summary = f"[{publisher}] {summary}".strip()

    res = conn.execute(
        text(
            "INSERT INTO news_articles "
            "(dedup_hash, title, source, url, published_at, summary, "
            "tickers, sentiment, confidence) "
            "VALUES (:h, :t, :s, :u, :p, :sum, :tk, :se, :c) "
            "ON CONFLICT (dedup_hash) DO NOTHING"
        ),
        {
            "h": dedup_hash,
            "t": title[:500],
            "s": "polygon_backfill",
            "u": url[:1000],
            "p": published_at,
            "sum": summary,
            "tk": tickers,
            "se": sentiment,
            "c": confidence,
        },
    )
    return res.rowcount == 1


# --- Twelve Data price helpers ---

def _fetch_twelvedata_eod(ticker: str, days: int) -> list[dict]:
    """Fetch daily OHLC from Twelve Data /time_series for last ``days`` days."""
    if not _TWELVEDATA_API_KEY:
        log.warning("TWELVEDATA_API_KEY missing; skipping prices for {t}", t=ticker)
        return []
    end_dt = datetime.now(timezone.utc).date()
    start_dt = end_dt - timedelta(days=days)
    params = {
        "symbol": ticker,
        "interval": "1day",
        "start_date": start_dt.isoformat(),
        "end_date": end_dt.isoformat(),
        "format": "JSON",
        "apikey": _TWELVEDATA_API_KEY,
    }
    # Twelve Data quota: 8 credits / min on our tier. Self-throttle, and on
    # explicit rate-limit error, sleep 60s and retry once.
    for attempt in range(2):
        _throttle_twelvedata()
        try:
            resp = requests.get(_TWELVEDATA_TS_URL, params=params, timeout=_REQUEST_TIMEOUT)
            resp.raise_for_status()
            body = resp.json()
            if body.get("status") == "error":
                msg = body.get("message", "")
                if "API credits" in msg or "limit" in msg.lower():
                    if attempt == 0:
                        log.info("Twelvedata quota hit for {t}; sleep 65s then retry", t=ticker)
                        time.sleep(65)
                        continue
                log.warning("Twelvedata error for {t}: {m}", t=ticker, m=msg)
                return []
            return body.get("values", []) or []
        except Exception as exc:
            log.warning("Twelvedata HTTP error for {t}: {e}", t=ticker, e=str(exc))
            return []
    return []


def _upsert_prices(conn, ticker: str, rows: list[dict]) -> int:
    inserted = 0
    for r in rows:
        obs_date_str = r.get("datetime")
        close_str = r.get("close")
        if not obs_date_str or not close_str:
            continue
        try:
            obs_date = datetime.fromisoformat(obs_date_str.split("T")[0]).date()
            close = float(close_str)
        except (ValueError, TypeError):
            continue
        if close <= 0:
            continue
        res = conn.execute(
            text(
                "INSERT INTO ticker_metrics_daily (ticker, obs_date, close_price, source) "
                "VALUES (:t, :d, :c, :s) "
                "ON CONFLICT (ticker, obs_date) DO UPDATE "
                "  SET close_price = COALESCE(ticker_metrics_daily.close_price, EXCLUDED.close_price), "
                "      source = COALESCE(ticker_metrics_daily.source, EXCLUDED.source), "
                "      as_of = now()"
            ),
            {"t": ticker.upper(), "d": obs_date, "c": close, "s": "twelvedata_backfill"},
        )
        if res.rowcount >= 1:
            inserted += 1
    return inserted


# --- Driver ---

def _load_watchlist(path: str) -> list[str]:
    out: list[str] = []
    p = Path(path)
    if not p.exists():
        log.warning("Watchlist file not found: {p}", p=path)
        return out
    for line in p.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        out.append(line.upper())
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--tickers",
        type=str,
        default="",
        help="Comma-separated tickers (e.g. FLUT,GEHC,GLND,BHRB)",
    )
    ap.add_argument(
        "--watchlist",
        type=str,
        default="",
        help="File with one ticker per line (overrides --tickers if both given combines unique)",
    )
    ap.add_argument(
        "--days",
        type=int,
        default=60,
        help="Look-back window in days (default 60)",
    )
    ap.add_argument(
        "--news-only",
        action="store_true",
        help="Skip price backfill",
    )
    ap.add_argument(
        "--prices-only",
        action="store_true",
        help="Skip news backfill",
    )
    args = ap.parse_args()

    tickers: list[str] = []
    if args.tickers:
        tickers += [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    if args.watchlist:
        tickers += _load_watchlist(args.watchlist)
    # Dedupe, preserve order
    seen = set()
    tickers = [t for t in tickers if not (t in seen or seen.add(t))]

    if not tickers:
        log.error("No tickers specified — use --tickers or --watchlist")
        return 2

    log.info(
        "Backfill starting — {n} tickers, {d}-day window, news={ne}, prices={pe}",
        n=len(tickers),
        d=args.days,
        ne=not args.prices_only,
        pe=not args.news_only,
    )

    engine = create_engine(settings.DB_URL)

    stats: dict[str, dict[str, int]] = {}
    for ticker in tickers:
        s = {"news_inserted": 0, "news_seen": 0, "prices_inserted": 0}

        if not args.prices_only:
            articles = _fetch_polygon_news(ticker, args.days)
            s["news_seen"] = len(articles)
            if articles:
                with engine.begin() as conn:
                    for art in articles:
                        if _upsert_news(conn, art, ticker):
                            s["news_inserted"] += 1
            time.sleep(_RATE_LIMIT_DELAY)

        if not args.news_only:
            rows = _fetch_twelvedata_eod(ticker, args.days)
            if rows:
                with engine.begin() as conn:
                    s["prices_inserted"] = _upsert_prices(conn, ticker, rows)
            time.sleep(_RATE_LIMIT_DELAY)

        stats[ticker] = s
        log.info(
            "  {t}: +{ni} news (of {ns} seen), +{pi} prices",
            t=ticker,
            ni=s["news_inserted"],
            ns=s["news_seen"],
            pi=s["prices_inserted"],
        )

    total_news = sum(v["news_inserted"] for v in stats.values())
    total_prices = sum(v["prices_inserted"] for v in stats.values())
    log.info(
        "DONE — {n} tickers, +{ni} news rows, +{pi} price rows",
        n=len(tickers),
        ni=total_news,
        pi=total_prices,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
