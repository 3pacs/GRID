#!/usr/bin/env python3
"""Twelve Data backfill for gem tickers with subject_id fallback (task#134).

Same as td_backfill_universe.py's gem-only predecessor, but uses subject_id
when backtest_ticker is NULL (true for gems 867-897 / OPCH/SPGI/FLUT/GEHC/
GLND/BHRB).

Deployment note (2026-09-27 recovery): this script ran on grid-svr's
grid-td-backfill.timer for weeks without ever being committed — untracked
(via .git/info/exclude), invisible to review. It's added to the repo here
for the first time. It was also hardcoding a live plaintext Postgres
password and reading its Twelve Data API key from a host-specific absolute
path (/data/grid_v4/astrogrid_dedup/.env) instead of the environment; both
are replaced below with env-var reads, matching the pattern already
established in scripts/td_backfill_universe.py's own credential-exposure
fix (PR merged as commit b978ee91).
"""
from __future__ import annotations

import os
import sys
import time
from datetime import date, timedelta

import psycopg2
import psycopg2.extras
import requests


def _connect_params_from_env() -> dict[str, str | int]:
    """Read DB connection params from the process env as keyword args.

    This script's systemd unit already loads
    /home/grid/grid_v4/grid_repo/.env via EnvironmentFile=, the same file
    every other GRID service reads its DB credentials from — no credential
    belongs hardcoded in a script. Passed to psycopg2.connect() as kwargs
    rather than interpolated into a conninfo string — manual interpolation
    breaks (or worse, silently misparses) on a password containing a
    space, quote, or backslash.
    """
    password = os.environ.get("DB_PASSWORD", "")
    if not password:
        sys.exit("DB_PASSWORD missing from environment")
    return {
        "host": os.environ.get("DB_HOST", "localhost"),
        "port": int(os.environ.get("DB_PORT", "5432")),
        "dbname": os.environ.get("DB_NAME", "griddb"),
        "user": os.environ.get("DB_USER", "grid"),
        "password": password,
    }


CONNECT_PARAMS = _connect_params_from_env()
TD_URL = "https://api.twelvedata.com/time_series"

API_KEY = os.environ.get("TWELVEDATA_API_KEY", "")
if not API_KEY:
    sys.exit("TWELVEDATA_API_KEY missing")


def _redact_api_key(text: str) -> str:
    return text.replace(API_KEY, "***REDACTED***") if API_KEY else text


def fetch_td(ticker, start, end):
    try:
        r = requests.get(
            TD_URL,
            params={
                "symbol": ticker,
                "interval": "1day",
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "apikey": API_KEY,
                "format": "JSON",
                "outputsize": 5000,
            },
            timeout=30,
        )
    except requests.RequestException as exc:
        # requests' exception __str__ can embed the failed request's full
        # URL, including the apikey query param — never log it verbatim.
        print(f"  {ticker}: network error: {_redact_api_key(str(exc))}", file=sys.stderr)
        return []
    body = r.json()
    if body.get("status") == "error":
        print(f'  {ticker}: TD error: {body.get("message")}', file=sys.stderr)
        return []
    out = []
    for v in body.get("values") or []:
        d = (v.get("datetime") or "")[:10]
        c = v.get("close")
        if d and c is not None:
            try:
                out.append({"date": d, "close": float(c)})
            except ValueError:
                pass
    return out


def main():
    conn = psycopg2.connect(**CONNECT_PARAMS)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    # Pull union of backtest_ticker + subject_id where subject_kind='ticker'
    cur.execute("""
        SELECT DISTINCT t FROM (
          SELECT backtest_ticker AS t FROM gem_alerts WHERE backtest_ticker IS NOT NULL
          UNION
          SELECT subject_id AS t FROM gem_alerts WHERE subject_kind='ticker' AND subject_id IS NOT NULL
        ) sub
        WHERE t NOT IN ('MACRO', 'NONE', '') AND t !~ '[[:space:]]'
        ORDER BY 1
    """)
    all_tickers = [r['t'] for r in cur.fetchall()]
    cur.execute("SELECT DISTINCT ticker FROM ticker_metrics_daily WHERE obs_date >= '2026-05-01' AND close_price IS NOT NULL")
    have = {r['ticker'] for r in cur.fetchall()}
    tickers = [t for t in all_tickers if t not in have]
    print(f'all={len(all_tickers)} fresh={len(have)} need={len(tickers)}')
    print('to-fetch:', tickers)

    end_d = date.today()
    start_d = end_d - timedelta(days=60)
    print(f'window: {start_d} → {end_d}')

    inserted = 0
    failed = []
    for i, t in enumerate(tickers, 1):
        bars = fetch_td(t, start_d, end_d)
        if not bars:
            failed.append(t); print(f'[{i}/{len(tickers)}] {t}: 0'); time.sleep(2); continue
        ins = 0
        for b in bars:
            cur.execute("""
                INSERT INTO ticker_metrics_daily (ticker, obs_date, close_price, source, as_of)
                VALUES (%s, %s, %s, 'twelvedata_backfill', now())
                ON CONFLICT (ticker, obs_date) DO UPDATE
                  SET close_price = EXCLUDED.close_price, as_of = now()
                  WHERE ticker_metrics_daily.close_price IS NULL
                     OR ticker_metrics_daily.obs_date >= CURRENT_DATE - 14
            """, (t, b['date'], b['close']))
            if cur.rowcount > 0: ins += 1
        conn.commit()
        inserted += ins
        print(f'[{i}/{len(tickers)}] {t}: {len(bars)} bars, {ins} ins')
        time.sleep(8)  # TD basic plan = 8 req/min
    print(f'inserted={inserted} failed={failed}')
    # Stamp source_catalog so freshness monitoring reflects actual pull cadence.
    try:
        cur.execute("""
            UPDATE source_catalog SET last_pull_at = now()
            WHERE name IN ('TWELVEDATA','TWELVEDATA_DIVIDENDS','TWELVEDATA_SPLITS','TWELVEDATA_STATS')
        """)
        conn.commit()
    except Exception as exc:
        print(f'source_catalog stamp failed: {exc}', file=sys.stderr)
    conn.close()
    return 0

if __name__ == '__main__':
    sys.exit(main())
