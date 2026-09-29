"""
GRID Tiingo data ingestion module — fallback for yfinance.

Uses the Tiingo REST API for reliable OHLCV price data.
Free tier: 50 symbols/hour, 500 requests/day, 30+ years history.

Requires TIINGO_API_KEY in environment (free at https://www.tiingo.com).
"""

from __future__ import annotations

import hashlib
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time as dtime, timedelta, timezone
from typing import Any, Callable
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError

from ingestion.base import BasePuller

# Process-scoped circuit breaker for catastrophic DB index corruption.
# Once a single ticker raises psycopg2.errors.IndexCorrupted on
# uq_raw_series_composite, every subsequent ticker in the cycle hits the
# same wall — burning ~30 ERROR rows per cycle until an operator runs
# REINDEX. Flip this once and skip the rest of the cycle so the log
# stays focused on the one actionable line.
_INDEX_CORRUPTED_BREAKER: bool = False

_TIINGO_API_KEY = os.getenv("TIINGO_API_KEY", "")
_BASE_URL = "https://api.tiingo.com"
_RATE_LIMIT_DELAY = 0.2  # seconds between calls (Pro tier — generous limits)
_REQUEST_TIMEOUT = 30

# ── Routine incremental pull (stale-sources audit 2026-09-29, root cause 2) ──
# The scheduled daily pull used to call pull_all(start_date="incremental"),
# which grid-scheduler resolved with ONE query: MAX(obs_date) over every
# TIINGO SUCCESS row (~12M rows). That query has hit the 120s statement
# timeout on every weekday run since at least mid-July (postgres log), the
# failure was swallowed at DEBUG, and the fallback start was "1990-01-01":
# each of the 1328 tickers re-downloaded ~36 years of history and pushed
# ~50k rows through a per-row INSERT ... WHERE NOT EXISTS (~28s/ticker,
# 10-14h per run). pull_incremental() below instead:
#   * looks up each ticker's own latest TIINGO obs_date (one indexed
#     backward scan on uq_raw_series_composite, sub-millisecond),
#   * skips tickers already holding the latest published session,
#   * fetches only the last ROUTINE_OVERLAP_DAYS for the rest,
#   * writes each ticker in one set-based statement (_insert_rows),
#   * spreads the HTTP calls over a small bounded worker pool sharing one
#     rate limiter, and
#   * holds a cross-process advisory lock, so grid-scheduler and
#     grid-hermes can never write the same (series, obs_date) concurrently.
ROUTINE_OVERLAP_DAYS = 10          # re-check this many days behind the latest stored session
NEW_TICKER_START = "2020-01-01"    # routine first pull for a ticker with no TIINGO rows (pull_ticker's default)
EOD_READY_ET = dtime(18, 0)        # a session's EOD bar is treated as published from 18:00 America/New_York
DEFAULT_MAX_WORKERS = 3            # concurrent HTTP fetches
DEFAULT_MIN_INTERVAL_S = 0.25      # >= 0.25s between request starts across all workers (<= 4 req/s; Tiingo Pro guidance ~5 req/s)
_MAX_RATE_LIMIT_RETRIES = 3        # HTTP 429 retries per ticker
_INSERT_CHUNK_ROWS = 5000          # rows per set-based INSERT statement
_ET = ZoneInfo("America/New_York")

# In-process single-flight guard; a new TiingoPuller is built per call, so
# the lock must be module-level.
_INCREMENTAL_LOCK = threading.Lock()


def _stable_advisory_lock_key(name: str) -> int:
    """Deterministic signed 64-bit key (same scheme as yfinance_pull)."""
    value = int.from_bytes(hashlib.sha256(name.encode("utf-8")).digest()[:8], "big")
    return value - (1 << 64) if value >= (1 << 63) else value


# Cross-process single-flight guard shared by every routine Tiingo price
# writer: grid-scheduler's daily worker, grid-hermes' SmartScheduler
# "tiingo" entry and Hermes' overnight step. Session-level (not xact) so it
# spans the per-ticker transactions; Postgres drops it if the holder dies.
_ADVISORY_LOCK_KEY: int = _stable_advisory_lock_key("grid:tiingo:prices")


def expected_latest_session(now: datetime | None = None) -> date:
    """Most recent US session whose Tiingo EOD bar should already exist."""
    from ingestion.market_calendar import is_market_open, last_trading_day

    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    now_et = now.astimezone(_ET)
    today = now_et.date()
    if is_market_open(today) and now_et.time() >= EOD_READY_ET:
        return today
    return last_trading_day(today - timedelta(days=1))


class _RateLimiter:
    """Thread-safe minimum spacing between request starts."""

    def __init__(self, min_interval_s: float) -> None:
        self._min_interval_s = max(float(min_interval_s), 0.0)
        self._lock = threading.Lock()
        self._next_at = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            start_at = max(now, self._next_at)
            self._next_at = start_at + self._min_interval_s
        delay = start_at - now
        if delay > 0:
            time.sleep(delay)


# Map Tiingo fields → GRID series suffix (same as yfinance convention)
_FIELD_MAP: dict[str, str] = {
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "volume": "volume",
    "adjClose": "adj_close",
}


def _tiingo_headers() -> dict[str, str]:
    return {
        "Content-Type": "application/json",
        "Authorization": f"Token {_TIINGO_API_KEY}",
    }


class TiingoPuller(BasePuller):
    """Pulls OHLCV data from Tiingo as a fallback for yfinance."""

    SOURCE_NAME: str = "TIINGO"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://api.tiingo.com",
        "cost_tier": "PAID",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "RARE",
        "trust_score": "HIGH",
        "priority_rank": 6,
    }

    def __init__(self, db_engine: Engine) -> None:
        if not _TIINGO_API_KEY:
            raise ValueError("TIINGO_API_KEY not set — get a free key at https://www.tiingo.com")
        super().__init__(db_engine)
        log.info("TiingoPuller initialised — source_id={sid}", sid=self.source_id)

    def pull_ticker(
        self,
        ticker: str,
        start_date: str | date = "2020-01-01",
        end_date: str | date | None = None,
        limiter: _RateLimiter | None = None,
    ) -> dict[str, Any]:
        """Download OHLCV for a single ticker from Tiingo.

        ``limiter`` (shared across pull_incremental's workers) spaces request
        starts; HTTP 429 is retried with backoff up to _MAX_RATE_LIMIT_RETRIES.
        """
        result: dict[str, Any] = {
            "ticker": ticker,
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        # Clean ticker: Tiingo uses plain symbols, no ^ prefix
        clean_ticker = ticker.replace("^", "").replace("=F", "").replace("=X", "")

        if end_date is None:
            end_date = date.today()

        url = f"{_BASE_URL}/tiingo/daily/{clean_ticker}/prices"
        params = {
            "startDate": str(start_date),
            "endDate": str(end_date),
            "format": "json",
        }

        try:
            for attempt in range(_MAX_RATE_LIMIT_RETRIES + 1):
                if limiter is not None:
                    limiter.wait()
                resp = requests.get(
                    url, headers=_tiingo_headers(), params=params, timeout=_REQUEST_TIMEOUT
                )
                if resp.status_code != 429 or attempt == _MAX_RATE_LIMIT_RETRIES:
                    break
                try:
                    backoff = float(resp.headers.get("Retry-After", ""))
                except (TypeError, ValueError):
                    backoff = 5.0 * (attempt + 1)
                log.warning(
                    "Tiingo 429 for {t} -- backing off {s:.0f}s (attempt {a})",
                    t=ticker, s=backoff, a=attempt + 1,
                )
                time.sleep(min(max(backoff, 1.0), 60.0))

            if resp.status_code == 404:
                log.debug("Tiingo: ticker {t} not found", t=ticker)
                result["status"] = "PARTIAL"
                result["errors"].append(f"Ticker {ticker} not found on Tiingo")
                return result

            resp.raise_for_status()
            data = resp.json()

            if not data:
                result["status"] = "PARTIAL"
                result["errors"].append("No data returned")
                return result

            df = pd.DataFrame(data)
            df["date"] = pd.to_datetime(df["date"]).dt.date
            inserted = 0

            rows_batch: list[dict] = []
            for tiingo_field, grid_field in _FIELD_MAP.items():
                if tiingo_field not in df.columns:
                    continue

                series_id = f"YF:{ticker}:{grid_field}"  # Same naming as yfinance
                col_data = df[["date", tiingo_field]].dropna()

                for _, row in col_data.iterrows():
                    rows_batch.append({
                        "sid": series_id,
                        "src": self.source_id,
                        "od": row["date"],
                        "val": float(row[tiingo_field]),
                    })

            if rows_batch:
                inserted = self._insert_rows(rows_batch)

            result["rows_inserted"] = inserted
            log.info("Tiingo {t}: inserted {n} rows", t=ticker, n=inserted)

        except requests.exceptions.HTTPError as e:
            log.warning("Tiingo HTTP error for {t}: {e}", t=ticker, e=str(e))
            result["status"] = "FAILED"
            result["errors"].append(str(e))
        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
        ) as exc:
            # Transient network failures (read timeouts, DNS hiccups, TCP
            # resets). Next cycle retries — keep errors.jsonl focused on
            # actionable issues by logging at WARNING level.
            log.warning(
                "Tiingo transient network failure for {t}: {err}",
                t=ticker, err=str(exc),
            )
            result["status"] = "FAILED"
            result["errors"].append(str(exc))
            return result
        except Exception as exc:
            # Detect IndexCorrupted on uq_raw_series_composite: this is a
            # catastrophic Postgres-level failure that requires an operator
            # to run REINDEX. Every subsequent ticker would raise the same
            # error, so flip a process-scoped breaker and surface ONE
            # actionable line instead of N×30 noisy ERROR rows per cycle.
            global _INDEX_CORRUPTED_BREAKER
            err_str = str(exc)
            if "IndexCorrupted" in err_str or "uq_raw_series_composite" in err_str:
                if not _INDEX_CORRUPTED_BREAKER:
                    _INDEX_CORRUPTED_BREAKER = True
                    log.error(
                        "Tiingo: Postgres index uq_raw_series_composite is "
                        "CORRUPTED — operator must run "
                        "`REINDEX INDEX CONCURRENTLY uq_raw_series_composite;` "
                        "(see scripts/migrations/reindex_raw_series.sql). "
                        "Skipping the rest of this Tiingo cycle to avoid "
                        "log flood. Triggered by ticker {t}: {err}",
                        t=ticker, err=err_str[:200],
                    )
                else:
                    log.debug(
                        "Tiingo: skipping {t} — index corruption breaker tripped",
                        t=ticker,
                    )
                result["status"] = "FAILED"
                result["errors"].append("index corruption — see prior ERROR")
                return result
            log.error("Tiingo pull failed for {t}: {err}", t=ticker, err=err_str)
            result["status"] = "FAILED"
            result["errors"].append(err_str)

        return result

    def _insert_rows(self, rows_batch: list[dict[str, Any]]) -> int:
        """Insert (series, obs_date, value) rows in set-based statements.

        Same dedupe contract as the per-row statement this replaces: a row
        is written only when no SUCCESS row already exists for that
        (series_id, source_id, obs_date); duplicate (series_id, obs_date)
        pairs inside one batch collapse to one. One statement per
        _INSERT_CHUNK_ROWS rows, all in one transaction, instead of one
        round trip per row -- the per-row form was ~28s per ticker on a
        ~50k-row history. Concurrent routine writers are kept apart by the
        advisory lock in pull_incremental.
        """
        if not rows_batch:
            return 0
        sql = text(
            "INSERT INTO raw_series "
            "(series_id, source_id, obs_date, value, pull_status) "
            "SELECT v.sid, :src, v.od, v.val, 'SUCCESS' "
            "FROM ("
            "  SELECT DISTINCT ON (u.sid, u.od) u.sid, u.od, u.val "
            "  FROM unnest(CAST(:sids AS text[]), CAST(:ods AS date[]), "
            "              CAST(:vals AS double precision[])) AS u(sid, od, val) "
            "  ORDER BY u.sid, u.od"
            ") AS v "
            "WHERE NOT EXISTS ("
            "  SELECT 1 FROM raw_series r "
            "  WHERE r.series_id = v.sid "
            "    AND r.source_id = :src "
            "    AND r.obs_date = v.od "
            "    AND r.pull_status = 'SUCCESS'"
            ")"
        )
        inserted = 0
        with self.engine.begin() as conn:
            for i in range(0, len(rows_batch), _INSERT_CHUNK_ROWS):
                chunk = rows_batch[i:i + _INSERT_CHUNK_ROWS]
                res = conn.execute(sql, {
                    "src": self.source_id,
                    "sids": [r["sid"] for r in chunk],
                    "ods": [r["od"] for r in chunk],
                    "vals": [r["val"] for r in chunk],
                })
                inserted += max(0, int(res.rowcount or 0))
        return inserted

    def _latest_tiingo_obs(self, ticker: str) -> date | None:
        """Latest SUCCESS obs_date this source holds for ``ticker``'s close.

        Walks uq_raw_series_composite (series_id, source_id, obs_date, ...)
        backwards and stops at the first row: sub-millisecond on grid-svr,
        versus the >120s source-wide MAX() it replaces.
        """
        with self.engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT obs_date FROM raw_series "
                    "WHERE series_id = :sid AND source_id = :src "
                    "AND pull_status = 'SUCCESS' "
                    "ORDER BY obs_date DESC LIMIT 1"
                ),
                {"sid": f"YF:{ticker}:close", "src": self.source_id},
            ).fetchone()
        if not row or row[0] is None:
            return None
        value = row[0]
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, str):
            return date.fromisoformat(value[:10])
        return value

    def _try_acquire_advisory_lock(self) -> Any | None:
        """Open a dedicated connection holding the cross-process lock, or None.

        The connection must stay open (not returned to the pool) for the
        whole run: a session-level advisory lock lives on that backend.
        """
        conn = self.engine.connect()
        try:
            acquired = conn.execute(
                text("SELECT pg_try_advisory_lock(:key)"), {"key": _ADVISORY_LOCK_KEY}
            ).scalar()
            conn.commit()
        except Exception:
            conn.close()
            raise
        if not acquired:
            conn.close()
            return None
        return conn

    def _release_advisory_lock(self, conn: Any) -> None:
        try:
            conn.execute(
                text("SELECT pg_advisory_unlock(:key)"), {"key": _ADVISORY_LOCK_KEY}
            )
            conn.commit()
        except Exception as exc:
            log.warning(
                "Tiingo: advisory lock release failed (Postgres releases it "
                "when the connection closes): {e}", e=str(exc),
            )
        finally:
            conn.close()

    @staticmethod
    def default_tickers() -> list[str]:
        """YF_TICKER_LIST merged with every sector-map ETF/actor ticker."""
        from ingestion.yfinance_pull import YF_TICKER_LIST

        all_tickers = set(YF_TICKER_LIST)
        try:
            from analysis.sector_map import SECTOR_MAP
            for sector in SECTOR_MAP.values():
                etf = sector.get("etf")
                if etf:
                    all_tickers.add(etf)
                for sub in sector.get("subsectors", {}).values():
                    for actor in sub.get("actors", []):
                        t = actor.get("ticker")
                        if t:
                            all_tickers.add(t)
        except Exception:
            pass  # Graceful degradation if sector_map has an issue
        return sorted(all_tickers)

    def pull_incremental(
        self,
        ticker_list: list[str] | None = None,
        *,
        overlap_days: int = ROUTINE_OVERLAP_DAYS,
        max_workers: int = DEFAULT_MAX_WORKERS,
        min_interval_s: float = DEFAULT_MIN_INTERVAL_S,
        should_continue: Callable[[], bool] | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """Routine daily price update: bounded, incremental, single-flight.

        See the module comment above ROUTINE_OVERLAP_DAYS. Returns a summary
        dict whose ``status`` is SUCCESS, PARTIAL (stopped by
        ``should_continue`` or the index-corruption breaker), FAILED (most
        fetches failed) or SKIPPED (another run holds the lock -- callers
        must not treat that as a completed pull).
        """
        tickers = list(ticker_list) if ticker_list is not None else self.default_tickers()

        def _skip(reason: str) -> dict[str, Any]:
            log.info("Tiingo incremental pull skipped: {r}", r=reason)
            return {"status": "SKIPPED", "skipped_reason": reason,
                    "tickers": len(tickers), "rows_inserted": 0}

        if not _INCREMENTAL_LOCK.acquire(blocking=False):
            return _skip("already running in this process")
        lock_conn = None
        try:
            lock_conn = self._try_acquire_advisory_lock()
            if lock_conn is None:
                return _skip("another process holds the Tiingo prices advisory lock")

            expected = expected_latest_session(now)
            limiter = _RateLimiter(min_interval_s)
            stop = threading.Event()
            fallback_start = (expected - timedelta(days=3 * overlap_days)).isoformat()

            def _one(ticker: str) -> dict[str, Any]:
                if stop.is_set() or _INDEX_CORRUPTED_BREAKER or (
                    should_continue is not None and not should_continue()
                ):
                    stop.set()
                    return {"ticker": ticker, "status": "UNATTEMPTED",
                            "rows_inserted": 0, "errors": []}
                try:
                    last = self._latest_tiingo_obs(ticker)
                    start = (
                        (last - timedelta(days=overlap_days)).isoformat()
                        if last is not None else NEW_TICKER_START
                    )
                except Exception as exc:
                    # Fail cheap: never fall back to a full-history pull.
                    log.warning(
                        "Tiingo {t}: latest-obs lookup failed ({e}); using {s}",
                        t=ticker, e=str(exc), s=fallback_start,
                    )
                    last, start = None, fallback_start
                if last is not None and last >= expected:
                    return {"ticker": ticker, "status": "CURRENT",
                            "rows_inserted": 0, "errors": []}
                return self.pull_ticker(ticker, start_date=start, limiter=limiter)

            log.info(
                "Tiingo incremental pull: {n} tickers, expecting session {d}, "
                "{w} workers, >= {i}s between requests",
                n=len(tickers), d=expected, w=max_workers, i=min_interval_s,
            )
            with ThreadPoolExecutor(max_workers=max(1, int(max_workers)),
                                    thread_name_prefix="tiingo") as pool:
                results = list(pool.map(_one, tickers))

            by_status: dict[str, int] = {}
            for r in results:
                by_status[r["status"]] = by_status.get(r["status"], 0) + 1
            fetched = sum(by_status.get(k, 0) for k in ("SUCCESS", "PARTIAL", "FAILED"))
            failed = by_status.get("FAILED", 0)
            unattempted = by_status.get("UNATTEMPTED", 0)
            rows = sum(int(r.get("rows_inserted") or 0) for r in results)
            if fetched >= 10 and failed * 2 > fetched:
                status = "FAILED"
            elif unattempted or _INDEX_CORRUPTED_BREAKER:
                status = "PARTIAL"
            else:
                status = "SUCCESS"
            summary: dict[str, Any] = {
                "status": status,
                "expected_session": expected.isoformat(),
                "tickers": len(tickers),
                "current": by_status.get("CURRENT", 0),
                "fetched": fetched,
                "succeeded": by_status.get("SUCCESS", 0),
                "no_data": by_status.get("PARTIAL", 0),
                "failed": failed,
                "unattempted": unattempted,
                "rows_inserted": rows,
                "failed_tickers": [r["ticker"] for r in results if r["status"] == "FAILED"][:50],
            }
            if status == "FAILED":
                summary["error"] = f"{failed}/{fetched} Tiingo fetches failed"
            log.info(
                "Tiingo incremental pull {s}: {c} current, {f} fetched, {ok} ok, "
                "{nd} no data, {fl} failed, {u} unattempted, {r} rows",
                s=status, c=summary["current"], f=fetched, ok=summary["succeeded"],
                nd=summary["no_data"], fl=failed, u=unattempted, r=rows,
            )
            return summary
        finally:
            if lock_conn is not None:
                self._release_advisory_lock(lock_conn)
            _INCREMENTAL_LOCK.release()

    def pull_all(
        self,
        ticker_list: list[str] | None = None,
        start_date: str | date = "2020-01-01",
    ) -> list[dict[str, Any]]:
        """Pull multiple tickers from ``start_date`` with rate limiting.

        Explicit/manual path (backfills, scripts): no incremental logic and
        no single-flight lock. Routine scheduled pulls use pull_incremental.
        Merges YF_TICKER_LIST with all tickers from the sector map
        so that every actor with a ticker gets daily price data.
        """
        tickers = ticker_list if ticker_list is not None else self.default_tickers()
        results = []
        succeeded = 0

        for ticker in tickers:
            res = self.pull_ticker(ticker, start_date=start_date)
            results.append(res)
            if res["status"] == "SUCCESS":
                succeeded += 1
            # Short-circuit the rest of the cycle when the catastrophic
            # IndexCorrupted breaker has tripped — every remaining ticker
            # would re-raise the same error.
            if _INDEX_CORRUPTED_BREAKER:
                log.warning(
                    "Tiingo bulk pull aborted at ticker {t} — "
                    "index corruption breaker tripped (operator must REINDEX)",
                    t=ticker,
                )
                break
            time.sleep(_RATE_LIMIT_DELAY)

        log.info(
            "Tiingo bulk pull complete — {s}/{t} succeeded",
            s=succeeded, t=len(tickers),
        )
        return results
