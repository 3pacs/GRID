"""
GRID Tiingo data ingestion module — fallback for yfinance.

Uses the Tiingo REST API for reliable OHLCV price data.
Free tier: 50 symbols/hour, 500 requests/day, 30+ years history.

Requires TIINGO_API_KEY in environment (free at https://www.tiingo.com).
"""

from __future__ import annotations

import hashlib
import math
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
from tiingo_symbol_contract import _ORACLE_NON_TIINGO, is_tiingo_equity  # noqa: F401 -- re-exported; readers share the rules
from store.observations import split_series_id

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
#
# That run-level lock only covers pull_incremental. pull_all / pull_ticker
# (backfill and universe scripts) do not take it, and the check-then-insert
# in _insert_rows is not atomic: two transactions writing the same ticker
# can each miss the other's uncommitted rows in NOT EXISTS and both insert.
# That is how the 09-15..09-28 byte-identical duplicate SUCCESS pairs were
# made (GRID-TIINGO-DUPLICATES-20260929). _insert_rows therefore also takes
# a per-ticker pg_advisory_xact_lock before its INSERT, on every path.
ROUTINE_OVERLAP_DAYS = 10          # re-check this many days behind the latest stored session
LAPSED_WINDOW_DAYS = 10            # a close this recent before the expected session = the ticker was live
MAX_NAMED_TICKERS = 50             # tickers named per bucket in the pull summary and log
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


# Tickers where 2+ different insiders bought on the open market (Form 4 code
# P) in the last INSIDER_BUYER_DAYS. Counted per feed, so one person spelled
# two ways by SEC and QuiverQuant is not two buyers. QuiverQuant's
# insider_buy also covers grants and option exercises (any "A" code), so only
# its TransactionCode P rows count. 2026-11-08: 332 tickers, 287 of them with
# no routine close, so Sleuth's insider-only control was 65% untestable.
INSIDER_BUYER_DAYS = 90
INSIDER_MIN_BUYERS = 2
INSIDER_MAX_TICKERS = 1000
INSIDER_BUYER_TICKERS_SQL = """
    SELECT t FROM (
        SELECT upper(trim(ticker)) AS t, source_type,
               count(DISTINCT lower(coalesce(signal_value->>'Name', source_id))) AS n
        FROM signal_sources
        WHERE signal_date >= :since
          AND ((source_type = 'insider' AND signal_type IN ('BUY', 'UNUSUAL_BUY'))
            OR (source_type = 'quiverquant:insider'
                AND signal_value->>'TransactionCode' = 'P'))
        GROUP BY 1, 2
    ) per_feed
    WHERE n >= :min_buyers
    GROUP BY t
    ORDER BY max(n) DESC, t
    LIMIT :cap
"""


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


def is_mutual_fund_symbol(ticker: str) -> bool:
    """Five letters ending in X: a mutual fund (NASDAQ fund-symbol convention).

    Funds reach the daily list through insider filings (MOALX, TPYTX on
    2026-11-10). Their NAV posts after the price slot, so they always look
    lapsed to :func:`still_behind`. They stay readable (their TIINGO rows are
    real NAVs); they are just not in the routine stock-close list.
    """
    return len(ticker) == 5 and ticker.isalpha() and ticker.isupper() and ticker.endswith("X")


def still_behind(results: list[dict[str, Any]], expected: date) -> list[tuple[str, date | None]]:
    """(ticker, latest close before the run) for every fetched ticker that
    still has no close for ``expected``: no data, a failed fetch, or data
    that stops earlier. A ticker last seen inside ``LAPSED_WINDOW_DAYS`` was
    trading, so missing it now is a failure, not a delisting; the pull
    reports PARTIAL and names it. Long-dead and never-served symbols (renamed
    companies, FX, crypto) are counted but do not fail the run.
    """
    out: list[tuple[str, date | None]] = []
    for r in results:
        if r.get("status") not in ("SUCCESS", "PARTIAL", "FAILED"):
            continue
        before, got = r.get("last_before"), r.get("latest_obs")
        latest = max((d for d in (before, got) if d is not None), default=None)
        if latest is None or latest < expected:
            out.append((r["ticker"], before))
    return out


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
        *,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Download OHLCV for a single ticker from Tiingo.

        ``limiter`` (shared across pull_incremental's workers) spaces request
        starts; HTTP 429 is retried with backoff up to _MAX_RATE_LIMIT_RETRIES.

        ``dry_run=True`` fetches exactly as a real pull would but writes
        nothing: ``rows_would_insert`` counts the rows the insert would add
        (read-only check, same dedupe contract as _insert_rows) and
        ``close_obs`` lists the fetched ``(obs_date, close)`` pairs.
        """
        result: dict[str, Any] = {
            "ticker": ticker,
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        if not is_tiingo_equity(ticker):
            # Never fetch: Tiingo would answer for an unrelated equity.
            result["status"] = "PARTIAL"
            result["errors"].append(f"{ticker} is not a Tiingo equity symbol")
            return result

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

            rows_batch.extend(self._split_rows(ticker, df))
            if dry_run:
                result["rows_would_insert"] = self._count_new_rows(rows_batch)
                result["close_obs"] = [
                    (r["od"], r["val"]) for r in rows_batch
                    if r["sid"] == f"YF:{ticker}:close"
                ]
                log.info("Tiingo {t} (dry run): would insert {n} rows",
                         t=ticker, n=result["rows_would_insert"])
                return result

            closes = [r["od"] for r in rows_batch if r["sid"] == f"YF:{ticker}:close"]
            result["latest_obs"] = max(closes) if closes else None
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

    def _split_rows(self, ticker: str, df: pd.DataFrame) -> list[dict[str, Any]]:
        """``YF:{ticker}:split_factor`` rows for the non-unit ``splitFactor`` days in ``df``.

        Tiingo's EOD rows carry ``splitFactor`` (new shares per old share on
        the ex-date: 10.0 for NVDA on 2024-06-10, 0.1 for a 1-for-10 reverse
        split, 1.0 otherwise). Only the split days are stored -- one row per
        corporate action, read by ``store.observations.read_splits``.
        """
        if "splitFactor" not in df.columns:
            return []
        rows: list[dict[str, Any]] = []
        for _, row in df[["date", "splitFactor"]].dropna().iterrows():
            try:
                factor = float(row["splitFactor"])
            except (TypeError, ValueError):
                continue
            if not math.isfinite(factor) or factor <= 0 or abs(factor - 1.0) < 1e-9:
                continue
            rows.append({
                "sid": split_series_id(ticker),
                "src": self.source_id,
                "od": row["date"],
                "val": factor,
            })
            log.info("Tiingo {t}: split factor {f} effective {d}", t=ticker, f=factor, d=row["date"])
        return rows

    def _insert_rows(self, rows_batch: list[dict[str, Any]]) -> int:
        """Insert (series, obs_date, value) rows in set-based statements.

        Same dedupe contract as the per-row statement this replaces: a row
        is written only when no SUCCESS row already exists for that
        (series_id, source_id, obs_date); duplicate (series_id, obs_date)
        pairs inside one batch collapse to one. One statement per
        _INSERT_CHUNK_ROWS rows, all in one transaction, instead of one
        round trip per row -- the per-row form was ~28s per ticker on a
        ~50k-row history.

        Concurrency: before the first INSERT, the transaction takes a
        per-ticker ``pg_advisory_xact_lock`` (see _lock_tickers). A second
        writer for the same ticker -- an orphaned SmartScheduler thread, a
        backfill script's pull_all, a manual pull_ticker -- waits there
        until the first commits. Its INSERT then runs as a new READ
        COMMITTED statement, so NOT EXISTS sees the committed rows and it
        inserts nothing. Without the lock, both transactions miss each
        other's uncommitted rows and both insert (the 09-29 duplicate
        pairs). The lock is released at commit or rollback.
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
            self._lock_tickers(conn, [r["sid"] for r in rows_batch])
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

    def _count_new_rows(self, rows_batch: list[dict[str, Any]]) -> int:
        """Rows of ``rows_batch`` that _insert_rows would add. Read only.

        Same contract: a (series_id, obs_date) pair counts once, and only when
        this source holds no SUCCESS row for it. One bounded, indexed read per
        series (uq_raw_series_composite prefix), never a write.
        """
        wanted: dict[str, set[date]] = {}
        for r in rows_batch:
            wanted.setdefault(r["sid"], set()).add(r["od"])
        new = 0
        with self.engine.connect() as conn:
            for sid, dates in wanted.items():
                held = {
                    (row[0].date() if isinstance(row[0], datetime) else
                     date.fromisoformat(str(row[0])[:10]) if isinstance(row[0], str) else row[0])
                    for row in conn.execute(
                        text(
                            "SELECT obs_date FROM raw_series "
                            "WHERE series_id = :sid AND source_id = :src "
                            "AND pull_status = 'SUCCESS' "
                            "AND obs_date BETWEEN :lo AND :hi"
                        ),
                        {"sid": sid, "src": self.source_id, "lo": min(dates), "hi": max(dates)},
                    ).fetchall()
                }
                new += len(dates - held)
        return new

    def _lock_tickers(self, conn: Any, series_ids: list[str]) -> None:
        """Take the per-ticker write lock for every ticker in ``series_ids``.

        Must run inside the inserting transaction (``engine.begin()``),
        before the INSERT. A series id ``YF:<TICKER>:<field>`` locks on
        ``YF:<TICKER>`` (everything before the last ``:``), so all six
        fields of one ticker share one lock: a pull_ticker batch takes
        exactly one. The key is BasePuller._file_advisory_lock's
        sha256-derived 63-bit key, namespaced by SOURCE_NAME, the same
        helper the file-based pullers use for this orphaned-thread race.
        Several tickers are locked in sorted order, so two writers with
        overlapping batches cannot deadlock.
        """
        tickers = sorted({sid.rsplit(":", 1)[0] for sid in series_ids})
        for ticker_key in tickers:
            self._file_advisory_lock(conn, "prices", ticker_key)

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
    def oracle_tickers(db_engine: Engine) -> list[str]:
        """The oracle's close universe (active ∪ pending), equities only.

        Source of truth: ``oracle.engine.oracle_close_universe``. Crypto and
        index symbols are left out: Tiingo's daily equity endpoint does not
        serve them (crypto closes come from the crypto pullers). Never
        raises; an unreachable DB yields ``[]`` and a warning.
        """
        try:
            from oracle.engine import oracle_close_universe

            with db_engine.connect() as conn:
                tickers = oracle_close_universe(conn)
        except Exception as exc:  # noqa: BLE001 -- graceful degradation
            log.warning("Tiingo: oracle close universe unavailable: {e}", e=str(exc))
            return []
        return [t for t in tickers if is_tiingo_equity(t)]

    @staticmethod
    def hypothesis_tickers(db_engine: Engine) -> list[str]:
        """Tickers active ticker-move hypotheses will be scored on, equities only.

        Source of truth: ``intelligence.hypothesis_trials.ticker_move_close_universe``.
        Never raises; an unreachable DB yields ``[]`` and a warning.
        """
        try:
            from intelligence.hypothesis_trials import ticker_move_close_universe

            with db_engine.connect() as conn:
                tickers = ticker_move_close_universe(conn)
        except Exception as exc:  # noqa: BLE001 -- graceful degradation
            log.warning("Tiingo: hypothesis close universe unavailable: {e}", e=str(exc))
            return []
        return [t for t in tickers if is_tiingo_equity(t)]

    @staticmethod
    def insider_buyer_tickers(db_engine: Engine, today: date | None = None) -> list[str]:
        """Listed equities with 2+ open-market insider buyers (see INSIDER_BUYER_TICKERS_SQL).

        Never raises; an unreachable DB yields ``[]`` and a warning.
        """
        try:
            from intelligence.actor_identity import is_placeholder_ticker
            from intelligence.sleuth_intersections import is_testable_ticker

            since = (today or date.today()) - timedelta(days=INSIDER_BUYER_DAYS)  # noqa: DTZ011
            with db_engine.connect() as conn:
                rows = conn.execute(text(INSIDER_BUYER_TICKERS_SQL), {
                    "since": since, "min_buyers": INSIDER_MIN_BUYERS, "cap": INSIDER_MAX_TICKERS,
                }).fetchall()
        except Exception as exc:  # noqa: BLE001 -- graceful degradation
            log.warning("Tiingo: insider buyer universe unavailable: {e}", e=str(exc))
            return []
        return [t for (t,) in rows
                if t and not is_placeholder_ticker(t) and is_testable_ticker(t) and is_tiingo_equity(t)]

    @staticmethod
    def default_tickers(db_engine: Engine | None = None) -> list[str]:
        """YF_TICKER_LIST ∪ every sector-map ETF/actor ticker ∪ the oracle,
        active-hypothesis and insider-buyer universes.

        With ``db_engine`` the oracle's close universe
        (:meth:`oracle_tickers`) is added, so every ticker the oracle predicts
        on gets a routine close (S15, 2026-11-06: ~45 biotech tickers had no
        close since 2026-09-09 because neither static list held them), and so
        is every ticker an active hypothesis will be scored on
        (:meth:`hypothesis_tickers`; 2026-11-07: 36 of 115 Sleuth lead tickers
        had no routine close), and so is every ticker with 2+ open-market
        insider buyers (:meth:`insider_buyer_tickers`; 2026-11-08: 287 of 332
        had no routine close).

        Sector-map tickers go through ``analysis.sector_map.price_tickers``:
        renamed symbols are swapped for their successor and acquired,
        delisted or foreign-listed ones are skipped (2026-11-08: 103 such
        tickers were fetched for 0 rows). Everything is then filtered by
        :func:`is_tiingo_equity`.
        """
        from ingestion.yfinance_pull import YF_TICKER_LIST

        all_tickers = set(YF_TICKER_LIST)
        if db_engine is not None:
            all_tickers.update(TiingoPuller.oracle_tickers(db_engine))
            all_tickers.update(TiingoPuller.hypothesis_tickers(db_engine))
            all_tickers.update(TiingoPuller.insider_buyer_tickers(db_engine))
        try:
            from analysis.sector_map import price_ticker, price_tickers
            all_tickers.update(price_tickers())
            # The lifecycle sidecar applies to every source, not just the map:
            # a stopped symbol from the oracle/insider lists would still get
            # Tiingo's frozen rows and trip the lapse check.
            all_tickers = {p for t in all_tickers if (p := price_ticker(t))}
        except Exception as exc:  # noqa: BLE001 -- graceful degradation
            log.warning("Tiingo: sector map tickers unavailable: {e}", e=str(exc))
        return sorted(t for t in all_tickers if is_tiingo_equity(t) and not is_mutual_fund_symbol(t))

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
        ``should_continue`` or the index-corruption breaker, or a ticker that
        was trading inside ``LAPSED_WINDOW_DAYS`` got no close for the expected
        session: see :func:`still_behind`), FAILED (most
        fetches failed) or SKIPPED (another run holds the lock -- callers
        must not treat that as a completed pull).
        """
        tickers = list(ticker_list) if ticker_list is not None else self.default_tickers(self.engine)

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
                res = self.pull_ticker(ticker, start_date=start, limiter=limiter)
                res["last_before"] = last
                return res

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
            behind = still_behind(results, expected)
            lapsed = [t for t, before in behind
                      if before is not None
                      and before >= expected - timedelta(days=LAPSED_WINDOW_DAYS)]
            if fetched >= 10 and failed * 2 > fetched:
                status = "FAILED"
            elif unattempted or _INDEX_CORRUPTED_BREAKER or lapsed:
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
                "no_data_tickers": [r["ticker"] for r in results
                                    if r["status"] == "PARTIAL"][:MAX_NAMED_TICKERS],
                "behind": len(behind),
                "lapsed_tickers": lapsed[:MAX_NAMED_TICKERS],
            }
            if status == "FAILED":
                summary["error"] = f"{failed}/{fetched} Tiingo fetches failed"
            elif lapsed:
                summary["error"] = (
                    f"{len(lapsed)} ticker(s) with a close in the {LAPSED_WINDOW_DAYS} days "
                    f"before {expected.isoformat()} got none for it: "
                    + ", ".join(lapsed[:MAX_NAMED_TICKERS]))
            log.info(
                "Tiingo incremental pull {s}: {c} current, {f} fetched, {ok} ok, "
                "{nd} no data, {fl} failed, {u} unattempted, {r} rows",
                s=status, c=summary["current"], f=fetched, ok=summary["succeeded"],
                nd=summary["no_data"], fl=failed, u=unattempted, r=rows,
            )
            if summary["no_data_tickers"]:
                log.info("Tiingo returned no data for: {t}",
                         t=", ".join(summary["no_data_tickers"]))
            if lapsed:
                log.warning("Tiingo incremental pull: {e}", e=summary["error"])
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
        tickers = ticker_list if ticker_list is not None else self.default_tickers(self.engine)
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
