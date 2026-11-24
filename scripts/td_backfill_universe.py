#!/usr/bin/env python3
"""Twelve Data universe backfill for signal_registry tickers (task #153).

Pattern adapted from scripts/td_backfill_gem_tickers.py — extends from the
gem-only universe to the full lever_pullers / news_intel / vol_price_divergence
/ sector_network / earnings_intel ticker universe in signal_registry.

For each ticker in data_freshness_universe:
  - DEAD bucket  → fetch last 365 days
  - STALE_30+    → fetch last 180 days
  - STALE_7_30   → fetch last 60 days
  - FRESH        → skip

Idempotent upsert into ticker_metrics_daily(ticker, obs_date, close_price, source).
Rate-limited to 8 req/min per Twelve Data basic plan.

Source-precedence policy (task #171, decided 2026-05-17):
---------------------------------------------------------
This writer is SUBORDINATE to ingestion.altdata.sec_xbrl_shares.
SEC XBRL is the canonical writer for shares_outstanding and
market_cap_usd (regulator-filed facts). This script writes ONLY
close_price for dates outside XBRL's ~90-day rolling window.

The ON CONFLICT clause:
  - Touches ONLY close_price, source, as_of (never shares_outstanding
    or market_cap_usd — those stay XBRL-owned).
  - Guard ``WHERE ticker_metrics_daily.shares_outstanding IS NULL``
    blocks TD from overwriting any row where XBRL has set the canonical
    shares field. This guarantees that the trade-ticket extractor (#118)
    always reads internally-consistent close × shares = market_cap.
  - The original ``OR obs_date >= CURRENT_DATE - 14`` clause is
    REMOVED. It allowed TD to overwrite XBRL rows within the last 14
    days, which silently shifted the close column away from the value
    market_cap_usd was computed against. Forcing TD to defer to XBRL on
    overlapping dates eliminates that drift.

Deployment note (2026-09-17): this script ran on grid-svr for months
without ever being committed — untracked, invisible to review, wired
only into grid-td-universe-backfill.timer. It's added to the repo here
for the first time. It was also hardcoding a live plaintext Postgres
password until this commit; see the companion PR description for the
containment/rotation decision.
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

    This script's own systemd unit already loads
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
RATE_LIMIT_SEC = 8.0   # 8 req/min = 7.5s between calls; 8s for safety margin

WINDOW_BY_BUCKET = {
    "DEAD":       365,
    "STALE_30+":  180,
    "STALE_7_30": 60,
}


API_KEY = os.environ.get("TWELVEDATA_API_KEY", "")
if not API_KEY:
    sys.exit("TWELVEDATA_API_KEY missing")


def _redact_api_key(text: str) -> str:
    return text.replace(API_KEY, "***REDACTED***") if API_KEY else text


def fetch_td(ticker: str, start: date, end: date) -> list[dict]:
    try:
        r = requests.get(
            TD_URL,
            params={
                "symbol":    ticker,
                "interval":  "1day",
                "start_date": start.isoformat(),
                "end_date":   end.isoformat(),
                "apikey":    API_KEY,
                "format":    "JSON",
                "outputsize": 5000,
            },
            timeout=30,
        )
    except requests.RequestException as exc:
        # requests' exception __str__ can embed the failed request's full
        # URL, including the apikey query param — never log it verbatim.
        print(f"  {ticker}: network error: {_redact_api_key(str(exc))}", file=sys.stderr)
        return []
    if r.status_code != 200:
        print(f"  {ticker}: HTTP {r.status_code}", file=sys.stderr)
        return []
    try:
        body = r.json()
    except ValueError:
        print(f"  {ticker}: non-JSON response", file=sys.stderr)
        return []
    if body.get("status") == "error":
        print(f"  {ticker}: TD error: {body.get('message')}", file=sys.stderr)
        return []
    out: list[dict] = []
    for v in body.get("values") or []:
        d = (v.get("datetime") or "")[:10]
        c = v.get("close")
        if d and c is not None:
            try:
                out.append({"date": d, "close": float(c)})
            except ValueError:
                pass
    return out


def main() -> int:
    only_bucket = os.environ.get("ONLY_BUCKET")          # e.g. "DEAD"
    limit       = int(os.environ.get("LIMIT", "0"))      # 0 = no cap
    dry_run     = bool(os.environ.get("DRY_RUN"))

    conn = psycopg2.connect(**CONNECT_PARAMS)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    where_bucket = "bucket IN ('DEAD','STALE_30+','STALE_7_30')"
    if only_bucket:
        where_bucket = "bucket = %s"
        params = (only_bucket,)
    else:
        params = ()
    cur.execute(
        f"""
        SELECT ticker, bucket
        FROM data_freshness_audit
        WHERE {where_bucket}
          AND audited_at = (SELECT MAX(audited_at) FROM data_freshness_audit)
        ORDER BY
          CASE bucket WHEN 'DEAD' THEN 0 WHEN 'STALE_30+' THEN 1 ELSE 2 END,
          ticker
        """,
        params,
    )
    work = cur.fetchall()
    if limit:
        work = work[:limit]
    print(f"backfill target: {len(work)} tickers (dry_run={dry_run})")
    if not work:
        conn.close()
        return 0

    end_d = date.today()

    if dry_run:
        # No write path is reachable from here — not even a transaction is
        # opened. A dry run must be a pure preview: it exists specifically
        # so a live check can be re-run to verify freshness/missing-data
        # behavior without risk of mutating source_catalog or anything
        # else, no matter what the rest of this function does below.
        for i, row in enumerate(work, 1):
            ticker = row["ticker"]
            bucket = row["bucket"]
            days = WINDOW_BY_BUCKET.get(bucket, 60)
            start_d = end_d - timedelta(days=days)
            print(f"[{i}/{len(work)}] {ticker} ({bucket}) → would fetch {start_d} → {end_d}")
        conn.close()
        return 0

    # Tracked separately, per the distinction that matters for both the
    # exit code and the freshness stamp below: a fetch either failed
    # outright (no bars — a real problem) or succeeded (bars came back
    # from Twelve Data, i.e. the source itself is reachable and fresh).
    # A successful fetch can still write zero rows if every date is
    # SEC-XBRL-owned (the ON CONFLICT WHERE guard intentionally skips
    # those) — that's a correct, protective no-op, not a failure, and
    # must not be counted as one.
    fetched_ok = 0
    inserted_total = 0
    protected_total = 0
    failed: list[str] = []
    for i, row in enumerate(work, 1):
        ticker = row["ticker"]
        bucket = row["bucket"]
        days = WINDOW_BY_BUCKET.get(bucket, 60)
        start_d = end_d - timedelta(days=days)
        bars = fetch_td(ticker, start_d, end_d)
        if not bars:
            failed.append(ticker)
            print(f"[{i}/{len(work)}] {ticker} ({bucket}): 0 bars")
            time.sleep(RATE_LIMIT_SEC)
            continue
        fetched_ok += 1
        ins = 0
        protected = 0
        for b in bars:
            # task #171: defer to SEC XBRL on every (ticker, obs_date)
            # row it has already written. We detect XBRL ownership via
            # shares_outstanding IS NOT NULL — that column is only ever
            # populated by sec_xbrl_shares.py. TD remains responsible
            # for filling pure-gap rows (XBRL never ran OR window out
            # of range). On conflict where XBRL has written, the UPDATE
            # is a no-op via the WHERE clause — cur.rowcount is 0 for
            # that row, distinct from a failure: the fetch succeeded and
            # this script correctly deferred to the canonical writer.
            cur.execute(
                """
                INSERT INTO ticker_metrics_daily (ticker, obs_date, close_price, source, as_of)
                VALUES (%s, %s, %s, 'twelvedata_universe_backfill', now())
                ON CONFLICT (ticker, obs_date) DO UPDATE
                  SET close_price = EXCLUDED.close_price,
                      source      = EXCLUDED.source,
                      as_of       = now()
                  WHERE ticker_metrics_daily.shares_outstanding IS NULL
                """,
                (ticker, b["date"], b["close"]),
            )
            if cur.rowcount > 0:
                ins += 1
            else:
                protected += 1
        conn.commit()
        inserted_total += ins
        protected_total += protected
        print(f"[{i}/{len(work)}] {ticker} ({bucket}): {len(bars)} bars, "
              f"{ins} ins/upd, {protected} XBRL-protected")
        time.sleep(RATE_LIMIT_SEC)

    print(f"\nDONE fetched_ok={fetched_ok}/{len(work)} inserted/updated={inserted_total} "
          f"protected={protected_total} failed={len(failed)}")
    if failed:
        print(f"failed tickers: {failed[:40]}{'...' if len(failed)>40 else ''}")

    # Stamp source_catalog so freshness monitoring reflects actual pull
    # cadence from the source — separate from data coverage. A run where
    # every fetch succeeded but every row was XBRL-protected still means
    # TWELVEDATA itself was successfully queried just now; it's only
    # inserted_total that would be zero. Gating the stamp on fetched_ok
    # (not inserted_total) keeps "source was pulled" and "rows were
    # written" as the two different things they are. Also fixed:
    # previously stamped TWELVEDATA_DIVIDENDS/_SPLITS/_STATS
    # unconditionally on every successful run — this script only ever
    # calls the /time_series daily-close endpoint and never fetches any
    # of those three.
    if fetched_ok > 0:
        try:
            cur.execute("""
                UPDATE source_catalog SET last_pull_at = now()
                WHERE name = 'TWELVEDATA'
            """)
            conn.commit()
        except Exception as exc:
            print(f'source_catalog stamp failed: {exc}')

    conn.close()

    if work and fetched_ok == 0:
        # Every ticker in this run failed to fetch — a scheduler/monitor
        # watching this job's exit code must be able to tell total
        # failure from success (including the legitimate all-protected
        # case, which is not a failure), not just read it out of the
        # log text.
        print(f"total failure: 0/{len(work)} tickers had a successful fetch",
              file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
