#!/usr/bin/env python3
"""Bounded, append-only historical daily-close backfill for SPY, BTC and ETH.

Operator-run tool. It is not an Alembic migration and is not wired into the
scheduler, Hermes, deploy.yml or any automatic path.

Why this exists (2026-09-24 reconciliation, R1/R1b read-only receipts):
  * ``binance.{BTC,ETH}USDT.close`` (the live puller identity) holds zero rows;
    the puller logged 79 zero-row SUCCESS runs between 2026-05-27 and 09-22.
  * ``BINANCE:{BTC,ETH}USDT:close`` (legacy ``scripts/bulk_historical_pull.py``)
    holds 2021-04-01..2024-12-31 only. Binance's spot archive switched to
    microsecond timestamps on 2025-01-01, and that script's
    ``except (ValueError, IndexError): continue`` silently dropped every later line.
  * ``YF:SPY:close`` (source yfinance) has 92 sessions between 2026-05-07 and
    2026-09-18 whose only captures were pulled before the observation's UTC
    day ended, i.e. no provably final unadjusted close.

Design rules:
  * Writes go ONLY to ``raw_series`` rows in a dedicated namespace
    (``backfill_v1:<shadowed series id>``). No static entity-map entry, no
    ``^YF:`` pattern match, no resolver branch, no price-close receipt and no
    identified reader consumes that namespace, so nothing existing can treat
    newly fetched history as if it had been known earlier. Promoting these rows
    into canonical features is a separate, reviewed decision.
  * Payloads deliberately carry neither ``close_contract`` nor
    ``price_contract_version`` markers, so ``is_completed_canonical_close`` and
    ``is_policy_capture`` reject them even if a mapping were added later.
  * ``pull_timestamp`` is the database transaction time of the insert, which is
    always at or after the provider acquisition time recorded in the payload.
    Rows are never back-dated to their observation date.
  * Observation dates at or after ``FORWARD_OWNED_FROM`` belong to the forward
    cadence (#639) and are refused, as is anything newer than run date minus
    ``MIN_RUN_LAG_DAYS``.
  * ``plan`` is read-only against the database. It fetches provider data,
    validates it, and emits a JSON plan whose digest binds the exact scope
    (series, dates, values). ``apply`` refuses to run without that digest.
  * ``apply`` is single-writer (session advisory lock), fetches each monthly
    chunk again, and stops on any drift from the plan. Each chunk commits in its
    own short transaction. Existing equal rows are skipped, so re-running
    resumes and is idempotent. A differing existing value is a revision and
    stops the run unless ``--record-revisions``, which appends and never updates.

Usage:
    python3 scripts/market_history_backfill.py plan --series BTCUSDT,ETHUSDT,SPY \
        --start 2025-01-01 --end 2026-09-22 --out plan.json
    python3 scripts/market_history_backfill.py apply --plan plan.json \
        --expect-plan-digest <sha256> --run-id <id> --max-rows <n> --ledger ledger.jsonl
    python3 scripts/market_history_backfill.py verify --plan plan.json --run-id <id>

Database URL: ``--db-url-env NAME`` names an environment variable holding the
URL. Otherwise it is built from DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD, the
same variables the GRID services read. Secrets are never taken from argv.

Exit status:
    0 ok; 2 usage or guard refusal; 3 another writer holds the lock;
    4 provider data drifted from the approved plan; 5 revision detected;
    6 incomplete (a chunk failed; committed chunks stay, rerun to resume);
    7 plan blocked by anomalies.
"""

from __future__ import annotations

import argparse
import calendar
import hashlib
import json
import math
import os
import socket
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Iterable, Protocol

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from sqlalchemy import create_engine, text  # noqa: E402
from sqlalchemy.engine import URL, Connection, Engine  # noqa: E402

from binance_close_contract import PUBLIC_DATA_HOST, completed_kline_payload  # noqa: E402
from ingestion.market_calendar import is_market_open  # noqa: E402

BACKFILL_CONTRACT = "market_history_backfill_v1"
NAMESPACE_PREFIX = "backfill_v1:"
# First observation date owned by the forward cadence released in #639: the
# 2026-09-24 20:00 UTC Binance run takes obs 2026-09-23, and SPY has
# spy_close_v1 receipts from 2026-09-21 on. History stops the day before.
FORWARD_OWNED_FROM = date(2026, 9, 23)
MIN_RUN_LAG_DAYS = 2
# market_calendar is rule-based and has no unscheduled closures.
KNOWN_UNSCHEDULED_NYSE_CLOSURES: dict[date, str] = {
    date(2025, 1, 9): "NYSE closed: National Day of Mourning for President Jimmy Carter",
}
SPY_SAME_VALUE_TOLERANCE = 1e-6
SPY_VALIDATION_TOLERANCE = 0.005
PLAN_MAX_AGE_HOURS_DEFAULT = 72.0
ADVISORY_LOCK_KEY = int.from_bytes(
    hashlib.sha256(b"grid:market_history_backfill_v1").digest()[:8], "big", signed=True
)

EXIT_OK = 0
EXIT_USAGE = 2
EXIT_LOCKED = 3
EXIT_PLAN_DRIFT = 4
EXIT_REVISION = 5
EXIT_INCOMPLETE = 6
EXIT_BLOCKED = 7

BLOCKING_ANOMALIES = frozenset({
    "malformed_provider_bar",
    "provider_duplicate_conflict",
    "missing_from_provider",
    "namespace_revision",
    "namespace_foreign_source",
    "validation_mismatch",
})


class BackfillRefused(Exception):
    """A guard refused the requested operation; nothing was written."""

    exit_code = EXIT_USAGE


class ProviderError(Exception):
    """The provider could not supply a chunk (transport, status or payload)."""


class ProviderAccessBlocked(ProviderError):
    """The provider denied access (e.g. HTTP 403/451); never retried."""


@dataclass(frozen=True)
class SeriesSpec:
    key: str
    asset: str
    series_id: str
    shadows: str
    coverage_source: str  # existing acquisition channel whose final captures count as coverage
    write_source: str  # dedicated one-shot acquisition channel the backfill rows are written under
    provider: str
    calendar: str  # "utc_daily" | "nyse_sessions"
    target_rule: str  # "coverage_gaps"
    coverage_series: tuple[str, ...]
    exact_values: bool
    validation_overlap: bool


# Backfill rows get their own source_catalog rows rather than reusing
# 'binance'/'yfinance'. api/routers/system.py derives each source's pipeline
# freshness from its newest raw_series.pull_timestamp, so rows under 'binance'
# would paint the (never-delivering) forward Binance puller green. The rows are
# active = FALSE (Hermes' stale-source re-pull only reads active sources) and
# pit_available = FALSE (historical values were not available at the time).
WRITE_SOURCE_SPECS: dict[str, dict[str, Any]] = {
    "binance_history_backfill_v1": {
        "base_url": "https://data-api.binance.vision/api/v3/klines",
        "cost_tier": "FREE", "latency_class": "EOD", "pit_available": False,
        "revision_behavior": "NEVER", "trust_score": "HIGH", "priority_rank": 99, "active": False,
    },
    "yfinance_history_backfill_v1": {
        "base_url": "yfinance.download (Yahoo chart API)",
        "cost_tier": "FREE", "latency_class": "EOD", "pit_available": False,
        "revision_behavior": "RARE", "trust_score": "HIGH", "priority_rank": 99, "active": False,
    },
}

SERIES: dict[str, SeriesSpec] = {
    "BTCUSDT": SeriesSpec(
        key="BTCUSDT", asset="BTC",
        series_id=NAMESPACE_PREFIX + "binance.BTCUSDT.close",
        shadows="binance.BTCUSDT.close", coverage_source="binance",
        write_source="binance_history_backfill_v1",
        provider="binance_spot_klines_rest", calendar="utc_daily",
        target_rule="coverage_gaps",
        coverage_series=("binance.BTCUSDT.close", "BINANCE:BTCUSDT:close"),
        exact_values=True, validation_overlap=True,
    ),
    "ETHUSDT": SeriesSpec(
        key="ETHUSDT", asset="ETH",
        series_id=NAMESPACE_PREFIX + "binance.ETHUSDT.close",
        shadows="binance.ETHUSDT.close", coverage_source="binance",
        write_source="binance_history_backfill_v1",
        provider="binance_spot_klines_rest", calendar="utc_daily",
        target_rule="coverage_gaps",
        coverage_series=("binance.ETHUSDT.close", "BINANCE:ETHUSDT:close"),
        exact_values=True, validation_overlap=True,
    ),
    "SPY": SeriesSpec(
        key="SPY", asset="SPY",
        series_id=NAMESPACE_PREFIX + "YF:SPY:close",
        shadows="YF:SPY:close", coverage_source="yfinance",
        write_source="yfinance_history_backfill_v1",
        provider="yahoo_chart_via_yfinance", calendar="nyse_sessions",
        target_rule="coverage_gaps",
        coverage_series=("YF:SPY:close",),
        exact_values=False, validation_overlap=False,
    ),
}


# --------------------------------------------------------------------------
# Calendars and windows
# --------------------------------------------------------------------------

def utc_day_end(obs_date: date) -> datetime:
    """Instant the UTC observation day ends (obs + 1 at 00:00 UTC)."""
    return datetime(obs_date.year, obs_date.month, obs_date.day, tzinfo=timezone.utc) + timedelta(days=1)


def is_nyse_session(d: date) -> bool:
    return is_market_open(d) and d not in KNOWN_UNSCHEDULED_NYSE_CLOSURES


def expected_dates(spec: SeriesSpec, start: date, end: date) -> list[date]:
    days = []
    d = start
    while d <= end:
        if spec.calendar == "utc_daily" or is_nyse_session(d):
            days.append(d)
        d += timedelta(days=1)
    return days


def month_chunks(start: date, end: date) -> list[tuple[date, date]]:
    """Inclusive calendar-month chunks covering [start, end]."""
    if end < start:
        return []
    chunks = []
    cur = start
    while cur <= end:
        last_day = calendar.monthrange(cur.year, cur.month)[1]
        chunk_end = min(end, date(cur.year, cur.month, last_day))
        chunks.append((cur, chunk_end))
        cur = chunk_end + timedelta(days=1)
    return chunks


def check_window(start: date, end: date, now_utc: datetime) -> None:
    if end < start:
        raise BackfillRefused(f"window end {end} precedes start {start}")
    if end >= FORWARD_OWNED_FROM:
        raise BackfillRefused(
            f"window end {end} reaches forward-owned dates (>= {FORWARD_OWNED_FROM}); "
            "the forward cadence owns them"
        )
    latest = now_utc.astimezone(timezone.utc).date() - timedelta(days=MIN_RUN_LAG_DAYS)
    if end > latest:
        raise BackfillRefused(
            f"window end {end} is newer than run date minus {MIN_RUN_LAG_DAYS} days ({latest})"
        )


# --------------------------------------------------------------------------
# Provider bars
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class ProviderBar:
    obs_date: date
    value: float
    value_text: str
    bar: dict[str, Any]
    evidence: dict[str, Any]


@dataclass
class ProviderChunk:
    provider: str
    request: dict[str, Any]
    request_started_at: datetime
    acquired_at: datetime
    response_sha256: str
    bars: list[ProviderBar]
    notes: list[dict[str, Any]] = field(default_factory=list)


class Provider(Protocol):
    def fetch(self, spec: SeriesSpec, start: date, end: date) -> ProviderChunk: ...


def _decimal(value: Any, label: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"non-numeric {label}: {value!r}") from exc
    if not parsed.is_finite():
        raise ValueError(f"non-finite {label}: {value!r}")
    return parsed


def parse_binance_klines(
    klines: list[Any], acquired_at: datetime, start: date, end: date,
) -> tuple[list[ProviderBar], list[dict[str, Any]]]:
    """Keep completed UTC day bars inside [start, end]; report everything else."""
    bars: list[ProviderBar] = []
    notes: list[dict[str, Any]] = []
    for kline in klines:
        try:
            closed = completed_kline_payload(kline, acquired_at)
        except (ValueError, TypeError) as exc:
            notes.append({"kind": "malformed_provider_bar", "detail": str(exc)[:200]})
            continue
        if closed is None:
            notes.append({"kind": "unfinished_bar_rejected", "open_time_ms": int(kline[0])})
            continue
        obs_date, contract_evidence = closed
        if not (start <= obs_date <= end):
            notes.append({"kind": "out_of_window_bar_ignored", "obs_date": obs_date.isoformat()})
            continue
        try:
            opening = _decimal(kline[1], "open")
            high = _decimal(kline[2], "high")
            low = _decimal(kline[3], "low")
            closing = _decimal(kline[4], "close")
            volume = _decimal(kline[5], "volume")
            quote_volume = _decimal(kline[7], "quote_volume") if len(kline) > 7 else None
            trades = int(kline[8]) if len(kline) > 8 else None
        except (ValueError, TypeError) as exc:
            notes.append({"kind": "malformed_provider_bar", "obs_date": obs_date.isoformat(),
                          "detail": str(exc)[:200]})
            continue
        if min(opening, high, low, closing) <= 0 or volume < 0 or high < max(opening, closing) \
                or low > min(opening, closing):
            notes.append({"kind": "malformed_provider_bar", "obs_date": obs_date.isoformat(),
                          "detail": "inconsistent OHLCV"})
            continue
        bars.append(ProviderBar(
            obs_date=obs_date,
            value=float(closing),
            value_text=str(kline[4]),
            bar={
                "open": str(kline[1]), "high": str(kline[2]), "low": str(kline[3]),
                "close": str(kline[4]), "volume": str(kline[5]),
                "quote_volume": None if quote_volume is None else str(kline[7]),
                "trades": trades,
            },
            evidence={
                "interval": "1d", "time_zone": "UTC",
                "open_time_ms": contract_evidence["open_time_ms"],
                "close_time_ms": contract_evidence["close_time_ms"],
            },
        ))
    return bars, notes


class BinanceKlinesProvider:
    """Binance spot public market data: the same host as the forward puller."""

    base_url = f"https://{PUBLIC_DATA_HOST}/api/v3/klines"

    def __init__(self, session: Any | None = None, *, max_attempts: int = 3,
                 sleep: Callable[[float], None] = time.sleep, pause_s: float = 0.25) -> None:
        if session is None:
            import requests
            session = requests.Session()
            session.headers.update({"User-Agent": "GRID-HistoryBackfill/1.0"})
        self.session = session
        self.max_attempts = max_attempts
        self.sleep = sleep
        self.pause_s = pause_s

    def fetch(self, spec: SeriesSpec, start: date, end: date) -> ProviderChunk:
        start_ms = int(datetime(start.year, start.month, start.day, tzinfo=timezone.utc).timestamp() * 1000)
        end_ms = int(datetime(end.year, end.month, end.day, tzinfo=timezone.utc).timestamp() * 1000)
        params = {"symbol": spec.key, "interval": "1d", "timeZone": "0",
                  "startTime": start_ms, "endTime": end_ms, "limit": 1000}
        last_error: Exception | None = None
        for attempt in range(1, self.max_attempts + 1):
            requested = datetime.now(timezone.utc)
            try:
                resp = self.session.get(self.base_url, params=params, timeout=30)
            except Exception as exc:  # transport failure: bounded retry
                last_error = ProviderError(f"transport: {type(exc).__name__}")
                self.sleep(2.0 * attempt)
                continue
            status = int(getattr(resp, "status_code", 0))
            if status in (403, 451):
                raise ProviderAccessBlocked(f"Binance access denied: HTTP {status}")
            if status in (418, 429):
                raise ProviderError(f"Binance rate limited: HTTP {status}")
            if status >= 500:
                last_error = ProviderError(f"Binance server error: HTTP {status}")
                self.sleep(2.0 * attempt)
                continue
            if status != 200:
                raise ProviderError(f"Binance returned HTTP {status}")
            body = resp.text
            acquired = datetime.now(timezone.utc)
            try:
                klines = json.loads(body)
            except ValueError as exc:
                raise ProviderError("Binance returned non-JSON body") from exc
            if not isinstance(klines, list):
                raise ProviderError("Binance returned a non-list body")
            bars, notes = parse_binance_klines(klines, acquired, start, end)
            self.sleep(self.pause_s)
            return ProviderChunk(
                provider=spec.provider,
                request={"url": self.base_url, "params": params},
                request_started_at=requested, acquired_at=acquired,
                response_sha256=hashlib.sha256(body.encode("utf-8")).hexdigest(),
                bars=bars, notes=notes,
            )
        raise last_error or ProviderError("Binance fetch failed")


def parse_yahoo_frame(frame: Any, acquired_at: datetime, start: date, end: date) -> tuple[
        list[ProviderBar], list[dict[str, Any]], str]:
    """Normalise a yfinance daily frame (auto_adjust=False) to session bars."""
    import pandas as pd

    notes: list[dict[str, Any]] = []
    if frame is None or len(frame) == 0:
        return [], notes, hashlib.sha256(b"[]").hexdigest()
    if isinstance(frame.columns, pd.MultiIndex):
        frame = frame.copy()
        frame.columns = frame.columns.get_level_values(0)
    rows = []
    for idx, row in frame.iterrows():
        stamp = pd.Timestamp(idx)
        obs_date = stamp.date()
        rows.append((obs_date, row))
    canonical = []
    bars: list[ProviderBar] = []
    for obs_date, row in rows:
        record = {}
        for col, key in (("Open", "open"), ("High", "high"), ("Low", "low"),
                         ("Close", "close"), ("Adj Close", "adj_close"), ("Volume", "volume")):
            raw = row.get(col) if hasattr(row, "get") else None
            record[key] = None if raw is None or (isinstance(raw, float) and math.isnan(raw)) else float(raw)
        canonical.append([obs_date.isoformat(), record])
        if not (start <= obs_date <= end):
            notes.append({"kind": "out_of_window_bar_ignored", "obs_date": obs_date.isoformat()})
            continue
        if not is_nyse_session(obs_date):
            notes.append({"kind": "unexpected_provider_date", "obs_date": obs_date.isoformat(),
                          "detail": KNOWN_UNSCHEDULED_NYSE_CLOSURES.get(obs_date, "not an NYSE session")})
            continue
        if acquired_at < utc_day_end(obs_date):
            notes.append({"kind": "unfinished_bar_rejected", "obs_date": obs_date.isoformat()})
            continue
        close = record["close"]
        if close is None or not math.isfinite(close) or close <= 0:
            notes.append({"kind": "malformed_provider_bar", "obs_date": obs_date.isoformat(),
                          "detail": "missing or non-positive close"})
            continue
        bars.append(ProviderBar(
            obs_date=obs_date, value=close, value_text=repr(close), bar=record,
            evidence={"interval": "1d", "session_date": obs_date.isoformat(),
                      "session_calendar": "ingestion.market_calendar + known unscheduled closures"},
        ))
    digest = hashlib.sha256(json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return bars, notes, digest


class YahooDailyProvider:
    """Yahoo daily bars via yfinance with the same basis as ``YF:SPY:close``."""

    def __init__(self, download: Callable[..., Any] | None = None, *,
                 sleep: Callable[[float], None] = time.sleep, pause_s: float = 1.0) -> None:
        if download is None:
            import yfinance as yf
            download = yf.download
            self.library_version = getattr(yf, "__version__", "unknown")
        else:
            self.library_version = "injected"
        self.download = download
        self.sleep = sleep
        self.pause_s = pause_s

    def fetch(self, spec: SeriesSpec, start: date, end: date) -> ProviderChunk:
        end_exclusive = end + timedelta(days=1)
        request = {"ticker": spec.key, "start": start.isoformat(), "end_exclusive": end_exclusive.isoformat(),
                   "interval": "1d", "auto_adjust": False, "actions": False, "repair": False,
                   "library_version": self.library_version}
        kwargs: dict[str, Any] = {"start": start.isoformat(), "end": end_exclusive.isoformat(),
                                  "interval": "1d", "actions": False, "repair": False, "progress": False,
                                  "threads": False, "rounding": False, "timeout": 30,
                                  "multi_level_index": False}
        try:
            import inspect
            accepted = set(inspect.signature(self.download).parameters)
            if not any(p.kind == p.VAR_KEYWORD for p in inspect.signature(self.download).parameters.values()):
                kwargs = {k: v for k, v in kwargs.items() if k in accepted}
        except (TypeError, ValueError):
            pass
        requested = datetime.now(timezone.utc)
        try:
            # auto_adjust is passed literally: yfinance 1.x defaults it to True,
            # which would silently switch Close to a dividend-adjusted basis.
            frame = self.download(spec.key, auto_adjust=False, **kwargs)
        except Exception as exc:
            raise ProviderError(f"yfinance download failed: {type(exc).__name__}") from exc
        acquired = datetime.now(timezone.utc)
        bars, notes, digest = parse_yahoo_frame(frame, acquired, start, end)
        self.sleep(self.pause_s)
        return ProviderChunk(provider=spec.provider, request=request, request_started_at=requested,
                             acquired_at=acquired, response_sha256=digest, bars=bars, notes=notes)


def default_provider(spec: SeriesSpec) -> Provider:
    if spec.provider == "binance_spot_klines_rest":
        return BinanceKlinesProvider()
    if spec.provider == "yahoo_chart_via_yfinance":
        return YahooDailyProvider()
    raise BackfillRefused(f"no provider for {spec.provider}")


# --------------------------------------------------------------------------
# Database reads
# --------------------------------------------------------------------------

def source_id_for(conn: Connection, name: str) -> int:
    row = conn.execute(text("SELECT id FROM source_catalog WHERE lower(name) = lower(:n)"),
                       {"n": name}).fetchone()
    if row is None:
        raise BackfillRefused(f"source_catalog has no {name!r} row")
    return int(row[0])


_WRITE_SOURCE_COLUMNS = ("base_url", "cost_tier", "latency_class", "pit_available", "revision_behavior",
                         "trust_score", "priority_rank", "active")


def write_source_row(conn: Connection, name: str) -> tuple[int, dict[str, Any]] | None:
    """Existing dedicated backfill source (id, attributes), or None."""
    row = conn.execute(text("""
        SELECT id, base_url, cost_tier, latency_class, pit_available, revision_behavior,
               trust_score, priority_rank, active
        FROM source_catalog WHERE name = :n
    """), {"n": name}).fetchone()
    if row is None:
        return None
    return int(row[0]), dict(zip(_WRITE_SOURCE_COLUMNS, row[1:]))


def _ensure_write_source(conn: Connection, name: str) -> tuple[int, bool]:
    """Create the dedicated source row with its exact approved attributes, or verify it."""
    spec = WRITE_SOURCE_SPECS[name]
    existing = write_source_row(conn, name)
    if existing is not None:
        if existing[1] != spec:
            raise BackfillRefused(f"source_catalog row {name!r} exists with different attributes")
        return existing[0], False
    new_id = conn.execute(text("""
        INSERT INTO source_catalog
            (name, base_url, cost_tier, latency_class, pit_available, revision_behavior,
             trust_score, priority_rank, active)
        VALUES (:name, :base_url, :cost_tier, :latency_class, :pit_available, :revision_behavior,
                :trust_score, :priority_rank, :active)
        RETURNING id
    """), {"name": name, **spec}).scalar()
    return int(new_id), True


def namespace_rows(conn: Connection, spec: SeriesSpec, start: date, end: date) -> dict[date, list[dict[str, Any]]]:
    rows = conn.execute(text("""
        SELECT id, obs_date, value, pull_timestamp, source_id, raw_payload
        FROM raw_series
        WHERE series_id = :sid AND pull_status = 'SUCCESS' AND obs_date BETWEEN :a AND :b
        ORDER BY obs_date, pull_timestamp, id
    """), {"sid": spec.series_id, "a": start, "b": end}).fetchall()
    out: dict[date, list[dict[str, Any]]] = {}
    for r in rows:
        payload = r[5] if isinstance(r[5], dict) else (json.loads(r[5]) if r[5] else {})
        out.setdefault(r[1], []).append({"id": int(r[0]), "value": float(r[2]), "pull_timestamp": r[3],
                                          "source_id": int(r[4]), "payload": payload})
    return out


def coverage_rows(conn: Connection, series_id: str, source_id: int, start: date, end: date) -> dict[date, dict[str, Any]]:
    """Per obs date, the distinct values captured after the observation's UTC day ended.

    Several writers share one series id and source (e.g. the yfinance puller
    with auto_adjust=False and scripts/fill_missing_features.py with
    auto_adjust=True both write ``YF:SPY:close`` under 'yfinance'), so a date
    counts as covered only if one of these values agrees with the provider's
    raw close. Which capture came first is not evidence of its basis.
    """
    rows = conn.execute(text("""
        SELECT obs_date, array_agg(DISTINCT value ORDER BY value), min(pull_timestamp), count(*), min(id)
        FROM raw_series
        WHERE series_id = :sid AND source_id = :src AND pull_status = 'SUCCESS'
          AND obs_date BETWEEN :a AND :b
          AND pull_timestamp >= (CAST(obs_date + 1 AS timestamp) AT TIME ZONE 'UTC')
        GROUP BY obs_date
    """), {"sid": series_id, "src": source_id, "a": start, "b": end}).fetchall()
    return {r[0]: {"series_id": series_id, "values": [float(v) for v in r[1]], "first_pull": r[2],
                   "captures": int(r[3]), "first_raw_id": int(r[4])} for r in rows}


def _set_local_timeouts(conn: Connection, statement_timeout: str, lock_timeout: str = "2s") -> None:
    """Transaction-scoped timeouts (set_config(..., true) == SET LOCAL), bound as parameters."""
    conn.execute(text("SELECT set_config('statement_timeout', :st, true), set_config('lock_timeout', :lt, true)"),
                 {"st": statement_timeout, "lt": lock_timeout})


def _read_only(conn: Connection, statement_timeout: str = "120s") -> None:
    conn.execute(text("SET TRANSACTION READ ONLY"))
    _set_local_timeouts(conn, statement_timeout)


# --------------------------------------------------------------------------
# Plan
# --------------------------------------------------------------------------

def _values_equal(spec: SeriesSpec, a: float, b: float, tolerance: float | None = None) -> bool:
    if spec.exact_values and tolerance is None:
        return a == b
    tol = SPY_SAME_VALUE_TOLERANCE if tolerance is None else tolerance
    return abs(a - b) <= tol


def plan_digest(plan: dict[str, Any]) -> str:
    """Digest of the approved write scope: series, source, dates and values."""
    scope = {
        "contract": plan["contract"],
        "forward_owned_from": plan["forward_owned_from"],
        "window": plan["window"],
        "database": plan.get("database", {}).get("name"),
        "total_targets": plan.get("total_targets"),
        "write_sources": plan.get("write_sources"),
        "allow_validation_mismatch": bool(plan.get("allow_validation_mismatch")),
        "fill_basis_conflicts": bool(plan.get("fill_basis_conflicts")),
        "series": {sid: {"key": s["key"], "coverage_source_id": s["coverage_source_id"],
                         "write_source": s["write_source"],
                         "targets": [[t["obs_date"], t["value_text"], t.get("reason")] for t in s["targets"]]}
                   for sid, s in sorted(plan["series"].items())},
    }
    blob = json.dumps(scope, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def _code_identity() -> dict[str, Any]:
    body = Path(__file__).read_bytes()
    ident: dict[str, Any] = {"file": "scripts/market_history_backfill.py",
                             "file_sha256": hashlib.sha256(body).hexdigest()}
    head = _REPO_ROOT / ".git"
    try:
        import subprocess
        ident["git_head"] = subprocess.run(
            ["git", "-C", str(_REPO_ROOT), "rev-parse", "HEAD"], capture_output=True, text=True,
            timeout=10, check=True).stdout.strip() if head.exists() else None
    except Exception:
        ident["git_head"] = None
    return ident


def build_plan(
    engine: Engine,
    series_keys: Iterable[str],
    start: date,
    end: date,
    *,
    providers: dict[str, Provider] | None = None,
    now_utc: datetime | None = None,
    validation_overlap_days: int = 31,
    allow_validation_mismatch: bool = False,
    fill_basis_conflicts: bool = False,
) -> dict[str, Any]:
    """Read-only plan: coverage gaps, provider bars, validation, exact targets.

    A date is covered when an existing post-day-end capture agrees with the
    provider. A date whose post-day-end captures all disagree is a basis
    conflict and blocks the plan. ``fill_basis_conflicts`` makes in-window
    conflicts targets (the disagreeing rows are left untouched);
    ``allow_validation_mismatch`` accepts any remaining conflict (including
    out-of-window overlap checks) as covered. Both flags are bound into the
    plan digest.
    """
    now_utc = now_utc or datetime.now(timezone.utc)
    check_window(start, end, now_utc)
    providers = providers or {}
    plan: dict[str, Any] = {
        "contract": BACKFILL_CONTRACT,
        "created_at_utc": now_utc.isoformat(),
        "host": socket.gethostname(),
        "code": _code_identity(),
        "forward_owned_from": FORWARD_OWNED_FROM.isoformat(),
        "min_run_lag_days": MIN_RUN_LAG_DAYS,
        "window": {"start": start.isoformat(), "end": end.isoformat()},
        "allow_validation_mismatch": allow_validation_mismatch,
        "fill_basis_conflicts": fill_basis_conflicts,
        "write_sources": {},
        "series": {},
    }
    source_problems: list[dict[str, Any]] = []
    with engine.connect() as conn:
        with conn.begin():
            _read_only(conn)
            db = conn.execute(text("SELECT current_database(), current_setting('server_version'), now()")).fetchone()
            plan["database"] = {"name": db[0], "server_version": db[1], "read_at": db[2].isoformat()}
            for key in series_keys:
                if key not in SERIES:
                    raise BackfillRefused(f"unknown series {key!r}; choose from {sorted(SERIES)}")
                spec = SERIES[key]
                src = source_id_for(conn, spec.coverage_source)
                existing_write = write_source_row(conn, spec.write_source)
                if existing_write is not None and existing_write[1] != WRITE_SOURCE_SPECS[spec.write_source]:
                    source_problems.append({"kind": "write_source_attribute_mismatch",
                                            "write_source": spec.write_source})
                plan["write_sources"][spec.write_source] = dict(WRITE_SOURCE_SPECS[spec.write_source])
                val_start = start - timedelta(days=validation_overlap_days) if spec.validation_overlap else start
                existing_ns = namespace_rows(conn, spec, start, end)
                coverage: dict[date, dict[str, Any]] = {}
                for sid in spec.coverage_series:
                    for d, row in coverage_rows(conn, sid, src, val_start, end).items():
                        if d in coverage:
                            merged = coverage[d]
                            merged["values"] = sorted(set(merged["values"]) | set(row["values"]))
                            merged["captures"] += row["captures"]
                            merged["series_id"] = f"{merged['series_id']}+{sid}"
                        else:
                            coverage[d] = row
                plan["series"][spec.series_id] = {
                    "key": spec.key, "shadows": spec.shadows,
                    "coverage_source": spec.coverage_source, "coverage_source_id": src,
                    "write_source": spec.write_source,
                    "write_source_id_at_plan": None if existing_write is None else existing_write[0],
                    "provider": spec.provider, "calendar": spec.calendar,
                    "_existing": existing_ns, "_coverage": coverage, "_val_start": val_start,
                }
    for sid, entry in plan["series"].items():
        spec = SERIES[entry["key"]]
        provider = providers.get(spec.key) or default_provider(spec)
        _plan_series(spec, entry, provider, start, end, allow_validation_mismatch, fill_basis_conflicts)
    plan["source_problems"] = source_problems
    blocking = sorted({a["kind"] for s in plan["series"].values() for a in s["anomalies"]
                       if a["kind"] in BLOCKING_ANOMALIES and not (
                           a["kind"] == "validation_mismatch" and allow_validation_mismatch)}
                      | {p["kind"] for p in source_problems})
    plan["status"] = "BLOCKED" if blocking else "READY"
    plan["blocking_anomalies"] = blocking
    plan["total_targets"] = sum(len(s["targets"]) for s in plan["series"].values())
    plan["plan_digest"] = plan_digest(plan)
    return plan


def _plan_series(spec: SeriesSpec, entry: dict[str, Any], provider: Provider, start: date, end: date,
                 allow_validation_mismatch: bool, fill_basis_conflicts: bool) -> None:
    existing_ns: dict[date, list[dict[str, Any]]] = entry.pop("_existing")
    coverage: dict[date, dict[str, Any]] = entry.pop("_coverage")
    val_start: date = entry.pop("_val_start")
    expected = expected_dates(spec, start, end)
    expected_set = set(expected)
    anomalies: list[dict[str, Any]] = []
    notes: list[dict[str, Any]] = []
    requests_log: list[dict[str, Any]] = []
    provider_bars: dict[date, ProviderBar] = {}
    for c_start, c_end in month_chunks(val_start, end):
        chunk = provider.fetch(spec, c_start, c_end)
        requests_log.append({"chunk": [c_start.isoformat(), c_end.isoformat()], "request": chunk.request,
                             "acquired_at_utc": chunk.acquired_at.isoformat(),
                             "response_sha256": chunk.response_sha256, "bars": len(chunk.bars)})
        for note in chunk.notes:
            (anomalies if note["kind"] in BLOCKING_ANOMALIES else notes).append({**note, "chunk": c_start.isoformat()})
        seen: dict[date, ProviderBar] = {}
        for bar in chunk.bars:
            prior = seen.get(bar.obs_date)
            if prior is not None:
                if not _values_equal(spec, prior.value, bar.value):
                    anomalies.append({"kind": "provider_duplicate_conflict", "obs_date": bar.obs_date.isoformat(),
                                      "values": [prior.value_text, bar.value_text]})
                continue
            seen[bar.obs_date] = bar
        provider_bars.update(seen)

    tolerance = 0.0 if spec.exact_values else SPY_VALIDATION_TOLERANCE
    validation = {"compared": 0, "agreeing": 0, "max_abs_diff_agreeing": 0.0, "basis_conflicts": 0,
                  "unvalidated_coverage": 0}
    covered: set[date] = set()
    basis_conflicts: set[date] = set()
    for d, cov in sorted(coverage.items()):
        if spec.calendar == "nyse_sessions" and not is_nyse_session(d):
            # e.g. griddb YF:SPY:close rows dated 2025-01-09 and 2025-07-04 (R1b):
            # not sessions, so neither coverage nor a validation input.
            notes.append({"kind": "coverage_on_non_session_date", "obs_date": d.isoformat(),
                          "coverage_series": cov["series_id"], "captures": cov["captures"]})
            continue
        bar = provider_bars.get(d)
        if bar is None:
            # Existing final capture the provider no longer returns: keep it as
            # coverage (nothing to write), but say it was not validated.
            covered.add(d)
            validation["unvalidated_coverage"] += 1
            notes.append({"kind": "coverage_unvalidated", "obs_date": d.isoformat()})
            continue
        validation["compared"] += 1
        diffs = [abs(bar.value - v) for v in cov["values"]]
        best = min(diffs)
        if best <= tolerance:
            covered.add(d)
            validation["agreeing"] += 1
            validation["max_abs_diff_agreeing"] = max(validation["max_abs_diff_agreeing"], best)
            continue
        validation["basis_conflicts"] += 1
        nearest = cov["values"][diffs.index(best)]
        detail = {"obs_date": d.isoformat(), "coverage_series": cov["series_id"],
                  "coverage_first_raw_id": cov["first_raw_id"], "coverage_distinct_values": len(cov["values"]),
                  "nearest_abs_diff": best, "nearest_ratio_to_provider": nearest / bar.value}
        if fill_basis_conflicts and d in expected_set:
            basis_conflicts.add(d)
            notes.append({"kind": "basis_conflict_filled", **detail})
        elif allow_validation_mismatch:
            covered.add(d)
            notes.append({"kind": "basis_conflict_accepted_as_covered", **detail})
        else:
            covered.add(d)  # never silently overwrite; the plan blocks instead
            anomalies.append({"kind": "validation_mismatch", **detail})

    for d in expected:
        if d not in provider_bars and d not in covered and not existing_ns.get(d):
            anomalies.append({"kind": "missing_from_provider", "obs_date": d.isoformat()})
    targets = []
    skipped_present = 0
    for d in expected:
        if d in covered:
            continue
        ns_rows = existing_ns.get(d, [])
        bar = provider_bars.get(d)
        if ns_rows:
            foreign = [r for r in ns_rows if r["source_id"] != entry["write_source_id_at_plan"]]
            if foreign:
                anomalies.append({"kind": "namespace_foreign_source", "obs_date": d.isoformat()})
            if bar is not None and not any(_values_equal(spec, r["value"], bar.value) for r in ns_rows):
                anomalies.append({"kind": "namespace_revision", "obs_date": d.isoformat(),
                                  "existing_raw_ids": [r["id"] for r in ns_rows]})
            skipped_present += 1
            continue
        if bar is None:
            continue
        targets.append({"obs_date": d.isoformat(), "value": bar.value, "value_text": bar.value_text,
                        "reason": "basis_conflict" if d in basis_conflicts else "no_final_capture"})
    for d, rows in existing_ns.items():
        if d not in expected_set:
            anomalies.append({"kind": "namespace_foreign_source", "obs_date": d.isoformat(),
                              "detail": "namespace row outside expected calendar"})
    entry.update({
        "expected_dates": len(expected),
        "covered_by_existing_final_capture": sum(1 for d in expected if d in covered),
        "already_in_namespace": skipped_present,
        "targets": targets,
        "target_reasons": {r: sum(1 for t in targets if t["reason"] == r)
                           for r in ("no_final_capture", "basis_conflict")},
        "first_target": targets[0]["obs_date"] if targets else None,
        "last_target": targets[-1]["obs_date"] if targets else None,
        "validation": validation,
        "anomalies": anomalies,
        "notes": notes,
        "provider_requests": requests_log,
    })


# --------------------------------------------------------------------------
# Apply
# --------------------------------------------------------------------------

@dataclass
class ApplyResult:
    exit_code: int
    inserted: int = 0
    skipped_present: int = 0
    revisions_recorded: int = 0
    chunks_committed: int = 0
    message: str = ""
    inserted_ids: list[int] = field(default_factory=list)


class Ledger:
    """Append-only JSONL ledger, flushed and fsynced per event."""

    def __init__(self, path: Path | None) -> None:
        self.path = path

    def write(self, event: str, **fields: Any) -> None:
        if self.path is None:
            return
        record = {"ts": datetime.now(timezone.utc).isoformat(), "event": event, **fields}
        with open(self.path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, sort_keys=True, default=str) + "\n")
            fh.flush()
            os.fsync(fh.fileno())


def load_plan(path: Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _check_plan(plan: dict[str, Any], expect_digest: str, now_utc: datetime, max_age_hours: float) -> None:
    if plan.get("contract") != BACKFILL_CONTRACT:
        raise BackfillRefused("plan contract mismatch")
    if plan.get("status") != "READY":
        raise BackfillRefused(f"plan status is {plan.get('status')!r}, not READY")
    if int(plan.get("total_targets", -1)) != sum(len(s["targets"]) for s in plan["series"].values()):
        raise BackfillRefused("plan total_targets does not match its target lists")
    computed = plan_digest(plan)
    if computed != plan.get("plan_digest") or computed != expect_digest:
        raise BackfillRefused("plan digest does not match the approved digest")
    if plan.get("forward_owned_from") != FORWARD_OWNED_FROM.isoformat():
        raise BackfillRefused("plan was built for a different forward boundary")
    for name, attrs in (plan.get("write_sources") or {}).items():
        if WRITE_SOURCE_SPECS.get(name) != attrs:
            raise BackfillRefused(f"plan write source {name!r} does not match this code's specification")
    for entry in plan["series"].values():
        spec = SERIES[entry["key"]]
        if entry["write_source"] != spec.write_source or entry["coverage_source"] != spec.coverage_source:
            raise BackfillRefused(f"plan sources for {spec.key} do not match this code")
    created = datetime.fromisoformat(plan["created_at_utc"])
    if now_utc - created > timedelta(hours=max_age_hours):
        raise BackfillRefused(f"plan is older than {max_age_hours} h; re-plan")
    window = plan["window"]
    check_window(date.fromisoformat(window["start"]), date.fromisoformat(window["end"]), now_utc)
    for entry in plan["series"].values():
        for t in entry["targets"]:
            d = date.fromisoformat(t["obs_date"])
            if not (date.fromisoformat(window["start"]) <= d <= date.fromisoformat(window["end"])):
                raise BackfillRefused(f"target {d} outside the plan window")


def _payload(spec: SeriesSpec, bar: ProviderBar, chunk: ProviderChunk, *, run_id: str, attempt_id: str,
             digest: str, code_sha: str, revision_of: list[int] | None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "backfill_contract": BACKFILL_CONTRACT,
        "acquisition_mode": "historical_backfill",
        "known_at_rule": "not_known_before_pull_timestamp",
        "canonical_promotion": "none",
        "shadows_series_id": spec.shadows,
        "write_source": spec.write_source,
        "coverage_source": spec.coverage_source,
        "run_id": run_id,
        "attempt_id": attempt_id,
        "plan_digest": digest,
        "code_sha256": code_sha,
        "provider": spec.provider,
        "request": chunk.request,
        "response_sha256": chunk.response_sha256,
        "provider_request_started_at_utc": chunk.request_started_at.isoformat(),
        "acquired_at_utc": chunk.acquired_at.isoformat(),
        "obs_date": bar.obs_date.isoformat(),
        "bar": bar.bar,
        **bar.evidence,
    }
    if spec.provider == "binance_spot_klines_rest":
        payload["price_basis"] = "binance_spot_last_trade_close_usdt_unadjusted"
        payload["finalized_rule"] = "completed_utc_day_bar_acquired_after_close"
    else:
        payload["price_basis"] = "yahoo_close_auto_adjust_false_split_adjusted_only"
        payload["finalized_rule"] = "nyse_session_acquired_after_obs_utc_day_end"
    if revision_of:
        payload["revision_of_raw_ids"] = revision_of
    return payload


class _ChunkAbort(Exception):
    """Abort the current chunk transaction with a specific exit code."""

    def __init__(self, exit_code: int, message: str) -> None:
        super().__init__(message)
        self.exit_code = exit_code


def apply_plan(
    engine: Engine,
    plan: dict[str, Any],
    *,
    expect_digest: str,
    run_id: str,
    max_rows: int,
    providers: dict[str, Provider] | None = None,
    ledger: Ledger | None = None,
    record_revisions: bool = False,
    now_utc: datetime | None = None,
    max_age_hours: float = PLAN_MAX_AGE_HOURS_DEFAULT,
    insert_hook: Callable[[str, date], None] | None = None,
) -> ApplyResult:
    """Write the approved plan's targets: resumable, idempotent, single-writer.

    Refusals (``BackfillRefused``) happen before any write, or roll back the
    open chunk. Any other failure rolls back the open chunk, leaves earlier
    committed chunks in place, is recorded in the ledger, and returns
    ``EXIT_INCOMPLETE`` so the same command can be rerun to resume.
    """
    ledger = ledger or Ledger(None)
    now_utc = now_utc or datetime.now(timezone.utc)
    _check_plan(plan, expect_digest, now_utc, max_age_hours)
    total_targets = int(plan["total_targets"])
    if max_rows < 0 or total_targets > max_rows:
        raise BackfillRefused(f"plan has {total_targets} targets but --max-rows is {max_rows}")
    providers = providers or {}
    attempt_id = f"{run_id}:{now_utc.strftime('%Y%m%dT%H%M%SZ')}:{os.getpid()}"
    code_sha = _code_identity()["file_sha256"]
    result = ApplyResult(exit_code=EXIT_OK)
    with engine.connect() as conn:
        with conn.begin():
            db_name = conn.execute(text("SELECT current_database()")).scalar()
            if db_name != plan.get("database", {}).get("name"):
                raise BackfillRefused(
                    f"connected to database {db_name!r} but the plan was built on "
                    f"{plan.get('database', {}).get('name')!r}")
            for entry in plan["series"].values():
                spec = SERIES[entry["key"]]
                if source_id_for(conn, spec.coverage_source) != int(entry["coverage_source_id"]):
                    raise BackfillRefused(f"source id for {spec.coverage_source!r} differs from the plan")
    # A dedicated session holds the advisory lock for the whole run. It is
    # invalidated (never returned to the pool) at the end, so the lock cannot
    # outlive this call on a pooled connection.
    lock_conn = engine.connect()
    try:
        got = lock_conn.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": ADVISORY_LOCK_KEY}).scalar()
        lock_conn.commit()
        if not got:
            ledger.write("apply_refused_locked", run_id=run_id, attempt_id=attempt_id)
            result.exit_code = EXIT_LOCKED
            result.message = "another backfill writer holds the advisory lock"
            return result
        ledger.write("apply_start", run_id=run_id, attempt_id=attempt_id, plan_digest=expect_digest,
                     total_targets=total_targets, max_rows=max_rows, record_revisions=record_revisions)
        needed = {SERIES[e["key"]].write_source for e in plan["series"].values() if e["targets"]}
        write_ids: dict[str, int] = {}
        with engine.connect() as conn:
            with conn.begin():
                _set_local_timeouts(conn, "30s")
                for name in sorted(needed):
                    write_ids[name], created = _ensure_write_source(conn, name)
                    ledger.write("write_source_ready", run_id=run_id, attempt_id=attempt_id, name=name,
                                 source_id=write_ids[name], created=created)
        for sid, entry in plan["series"].items():
            spec = SERIES[entry["key"]]
            if spec.series_id != sid:
                raise BackfillRefused(f"plan series {sid} does not match {spec.key}")
            targets = {date.fromisoformat(t["obs_date"]): t for t in entry["targets"]}
            if not targets:
                continue
            provider = providers.get(spec.key) or default_provider(spec)
            for c_start, c_end in month_chunks(min(targets), max(targets)):
                chunk_targets = {d: t for d, t in targets.items() if c_start <= d <= c_end}
                if not chunk_targets:
                    continue
                try:
                    _apply_chunk(engine, spec, write_ids[spec.write_source], provider, c_start, c_end,
                                 chunk_targets, plan, run_id, attempt_id, code_sha, max_rows,
                                 record_revisions, result, ledger, insert_hook)
                except _ChunkAbort as abort:
                    result.exit_code = abort.exit_code
                    result.message = str(abort)
                    return result
        # Honest channel heartbeat: the newest row actually written under each
        # dedicated source, never "now" and never touching 'binance'/'yfinance'.
        with engine.connect() as conn:
            with conn.begin():
                _set_local_timeouts(conn, "30s")
                for name, source_id in sorted(write_ids.items()):
                    sids = [SERIES[e["key"]].series_id for e in plan["series"].values()
                            if SERIES[e["key"]].write_source == name]
                    stamped = conn.execute(text("""
                        UPDATE source_catalog
                        SET last_pull_at = (SELECT max(pull_timestamp) FROM raw_series
                                            WHERE series_id = ANY(:sids) AND source_id = :src)
                        WHERE id = :src
                          AND EXISTS (SELECT 1 FROM raw_series WHERE series_id = ANY(:sids) AND source_id = :src)
                        RETURNING last_pull_at
                    """), {"sids": sids, "src": source_id}).scalar()
                    ledger.write("write_source_stamped", run_id=run_id, attempt_id=attempt_id, name=name,
                                 source_id=source_id, last_pull_at=stamped)
        ledger.write("apply_done", run_id=run_id, attempt_id=attempt_id, inserted=result.inserted,
                     skipped_present=result.skipped_present, revisions_recorded=result.revisions_recorded,
                     chunks_committed=result.chunks_committed)
        return result
    except BackfillRefused as exc:
        ledger.write("apply_refused", run_id=run_id, attempt_id=attempt_id, error=str(exc)[:300])
        raise
    except Exception as exc:  # provider or database failure; the open chunk rolled back
        ledger.write("chunk_failed", run_id=run_id, attempt_id=attempt_id,
                     error_class=type(exc).__name__, error=str(exc)[:300],
                     inserted_so_far=result.inserted, chunks_committed=result.chunks_committed)
        result.exit_code = EXIT_INCOMPLETE
        result.message = f"{type(exc).__name__}: {str(exc)[:200]}"
        return result
    finally:
        try:
            lock_conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": ADVISORY_LOCK_KEY})
            lock_conn.commit()
        except Exception:
            pass
        lock_conn.invalidate()
        lock_conn.close()


def _apply_chunk(engine: Engine, spec: SeriesSpec, source_id: int, provider: Provider, c_start: date,
                 c_end: date, chunk_targets: dict[date, dict[str, Any]], plan: dict[str, Any], run_id: str,
                 attempt_id: str, code_sha: str, max_rows: int, record_revisions: bool,
                 result: ApplyResult, ledger: Ledger,
                 insert_hook: Callable[[str, date], None] | None) -> None:
    """Fetch one chunk again, compare it with the plan, then write it in one transaction."""
    chunk = provider.fetch(spec, c_start, c_end)
    bars: dict[date, ProviderBar] = {}
    for bar in chunk.bars:
        prior = bars.get(bar.obs_date)
        if prior is not None and not _values_equal(spec, prior.value, bar.value):
            ledger.write("plan_drift", run_id=run_id, attempt_id=attempt_id, series_id=spec.series_id,
                         obs_date=bar.obs_date, detail="conflicting duplicate bars at apply time")
            raise _ChunkAbort(EXIT_PLAN_DRIFT, f"conflicting duplicate provider bars for {bar.obs_date}")
        bars[bar.obs_date] = bar
    for d, target in sorted(chunk_targets.items()):
        bar = bars.get(d)
        if bar is None or not _values_equal(spec, bar.value, float(target["value"])):
            ledger.write("plan_drift", run_id=run_id, attempt_id=attempt_id, series_id=spec.series_id,
                         obs_date=d, planned=target["value_text"],
                         fetched=None if bar is None else bar.value_text)
            raise _ChunkAbort(EXIT_PLAN_DRIFT, f"provider value for {spec.series_id} {d} drifted from the plan")
    inserted_ids: list[int] = []
    skipped = 0
    revisions = 0
    with engine.connect() as conn:
        with conn.begin():
            _set_local_timeouts(conn, "30s")
            existing = namespace_rows(conn, spec, c_start, c_end)
            tx_now = conn.execute(text("SELECT now()")).scalar()
            if tx_now < chunk.acquired_at:
                raise BackfillRefused(
                    "database transaction time precedes the provider acquisition time; "
                    "pull_timestamp would claim knowledge before acquisition")
            for d in sorted(chunk_targets):
                bar = bars[d]
                rows = existing.get(d, [])
                revision_of = None
                if rows:
                    if any(_values_equal(spec, r["value"], bar.value) for r in rows):
                        skipped += 1
                        continue
                    if not record_revisions:
                        ledger.write("revision_detected", run_id=run_id, attempt_id=attempt_id,
                                     series_id=spec.series_id, obs_date=d,
                                     existing_raw_ids=[r["id"] for r in rows], fetched=bar.value_text)
                        raise _ChunkAbort(EXIT_REVISION, f"revision detected for {spec.series_id} {d}")
                    revision_of = [r["id"] for r in rows]
                if result.inserted + len(inserted_ids) + 1 > max_rows:
                    raise BackfillRefused(f"--max-rows {max_rows} would be exceeded")
                if insert_hook is not None:
                    insert_hook(spec.series_id, d)
                payload = _payload(spec, bar, chunk, run_id=run_id, attempt_id=attempt_id,
                                   digest=plan["plan_digest"], code_sha=code_sha, revision_of=revision_of)
                new_id = conn.execute(text("""
                    INSERT INTO raw_series
                        (series_id, source_id, obs_date, pull_timestamp, value, raw_payload, pull_status)
                    VALUES (:sid, :src, :od, now(), :val, CAST(:payload AS jsonb), 'SUCCESS')
                    RETURNING id
                """), {"sid": spec.series_id, "src": source_id, "od": d, "val": bar.value,
                       "payload": json.dumps(payload, sort_keys=True)}).scalar()
                inserted_ids.append(int(new_id))
                if revision_of:
                    revisions += 1
    committed_at = datetime.now(timezone.utc)
    result.inserted += len(inserted_ids)
    result.inserted_ids.extend(inserted_ids)
    result.skipped_present += skipped
    result.revisions_recorded += revisions
    result.chunks_committed += 1
    ledger.write("chunk_committed", run_id=run_id, attempt_id=attempt_id, series_id=spec.series_id,
                 chunk=[c_start, c_end], inserted=len(inserted_ids), skipped_present=skipped,
                 revisions_recorded=revisions, acquired_at=chunk.acquired_at,
                 response_sha256=chunk.response_sha256, committed_after=committed_at,
                 first_id=inserted_ids[0] if inserted_ids else None,
                 last_id=inserted_ids[-1] if inserted_ids else None)


# --------------------------------------------------------------------------
# Verify (read-only)
# --------------------------------------------------------------------------

def verify_plan(engine: Engine, plan: dict[str, Any], run_id: str | None = None) -> dict[str, Any]:
    """Read-only post-check: every target present once, fields intact, nothing extra."""
    report: dict[str, Any] = {"ok": True, "series": {}}
    window = plan["window"]
    start, end = date.fromisoformat(window["start"]), date.fromisoformat(window["end"])
    with engine.connect() as conn:
        with conn.begin():
            _read_only(conn)
            for sid, entry in plan["series"].items():
                spec = SERIES[entry["key"]]
                write_row = write_source_row(conn, spec.write_source)
                write_id = None if write_row is None else write_row[0]
                rows = namespace_rows(conn, spec, start, end)
                wrong_source = sorted(r["id"] for rs in rows.values() for r in rs if r["source_id"] != write_id)
                if write_row is not None and write_row[1] != WRITE_SOURCE_SPECS[spec.write_source]:
                    wrong_source.append(-1)  # the dedicated source row itself was altered
                planned = {date.fromisoformat(t["obs_date"]): float(t["value"]) for t in entry["targets"]}
                missing = sorted(d.isoformat() for d in planned if d not in rows)
                wrong = sorted(d.isoformat() for d, v in planned.items()
                               if d in rows and not any(_values_equal(spec, r["value"], v) for r in rows[d]))
                multi = sorted(d.isoformat() for d, r in rows.items() if len(r) > 1)
                unplanned = sorted(d.isoformat() for d in rows if d not in planned)
                bad_payload = []
                early = []
                for d, rs in rows.items():
                    for r in rs:
                        p = r["payload"]
                        if (p.get("backfill_contract") != BACKFILL_CONTRACT
                                or "close_contract" in p or "price_contract_version" in p
                                or p.get("shadows_series_id") != spec.shadows
                                or p.get("write_source") != spec.write_source):
                            bad_payload.append(r["id"])
                        acquired = p.get("acquired_at_utc")
                        if acquired and r["pull_timestamp"] < datetime.fromisoformat(acquired):
                            early.append(r["id"])
                        if r["pull_timestamp"] < utc_day_end(d):
                            early.append(r["id"])
                run_rows = sum(1 for rs in rows.values() for r in rs if run_id and r["payload"].get("run_id") == run_id)
                s_ok = not (missing or wrong or bad_payload or early or wrong_source)
                report["ok"] = report["ok"] and s_ok
                report["series"][sid] = {
                    "write_source": spec.write_source, "write_source_id": write_id,
                    "write_source_last_pull_at": None if write_id is None else conn.execute(
                        text("SELECT last_pull_at FROM source_catalog WHERE id = :i"), {"i": write_id}).scalar(),
                    "wrong_source_ids": wrong_source,
                    "planned": len(planned), "present_dates": len(rows), "missing": missing,
                    "value_mismatch": wrong, "multiple_rows": multi, "unplanned_dates": unplanned,
                    "bad_payload_ids": bad_payload, "pull_before_acquisition_or_day_end_ids": early,
                    "rows_with_run_id": run_rows if run_id else None, "ok": s_ok,
                }
    return report


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def engine_from_env(url_env: str | None) -> Engine:
    if url_env:
        url = os.environ.get(url_env)
        if not url:
            raise BackfillRefused(f"environment variable {url_env} is not set")
        return create_engine(url, pool_pre_ping=True)
    password = os.environ.get("DB_PASSWORD")
    if not password:
        raise BackfillRefused("DB_PASSWORD is not set and no --db-url-env was given")
    return create_engine(URL.create(
        "postgresql+psycopg2",
        username=os.environ.get("DB_USER", "grid"), password=password,
        host=os.environ.get("DB_HOST", "localhost"), port=int(os.environ.get("DB_PORT", "5432")),
        database=os.environ.get("DB_NAME", "griddb"),
    ), pool_pre_ping=True)


def _summary(plan: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": plan["status"], "plan_digest": plan["plan_digest"], "total_targets": plan["total_targets"],
        "blocking_anomalies": plan["blocking_anomalies"], "window": plan["window"],
        "series": {sid: {k: s[k] for k in ("expected_dates", "covered_by_existing_final_capture",
                                           "already_in_namespace", "first_target", "last_target", "validation",
                                           "target_reasons")}
                   | {"targets": len(s["targets"]), "anomalies": len(s["anomalies"]), "notes": len(s["notes"])}
                   for sid, s in plan["series"].items()},
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_plan = sub.add_parser("plan", help="read-only plan with provider validation")
    p_plan.add_argument("--series", required=True, help="comma list of " + ",".join(SERIES))
    p_plan.add_argument("--start", required=True, type=date.fromisoformat)
    p_plan.add_argument("--end", required=True, type=date.fromisoformat)
    p_plan.add_argument("--out", required=True, type=Path)
    p_plan.add_argument("--validation-overlap-days", type=int, default=31)
    p_plan.add_argument("--allow-validation-mismatch", action="store_true",
                        help="treat dates whose existing final captures all disagree with the provider as covered")
    p_plan.add_argument("--fill-basis-conflicts", action="store_true",
                        help="make such dates targets instead (their existing captures are left untouched)")
    p_plan.add_argument("--db-url-env")
    p_apply = sub.add_parser("apply", help="bounded, resumable, idempotent write of an approved plan")
    p_apply.add_argument("--plan", required=True, type=Path)
    p_apply.add_argument("--expect-plan-digest", required=True)
    p_apply.add_argument("--run-id", required=True)
    p_apply.add_argument("--max-rows", required=True, type=int)
    p_apply.add_argument("--ledger", required=True, type=Path)
    p_apply.add_argument("--record-revisions", action="store_true")
    p_apply.add_argument("--plan-max-age-hours", type=float, default=PLAN_MAX_AGE_HOURS_DEFAULT)
    p_apply.add_argument("--db-url-env")
    p_verify = sub.add_parser("verify", help="read-only post-apply verification")
    p_verify.add_argument("--plan", required=True, type=Path)
    p_verify.add_argument("--run-id")
    p_verify.add_argument("--db-url-env")
    args = parser.parse_args(argv)
    try:
        engine = engine_from_env(args.db_url_env)
        if args.cmd == "plan":
            keys = [k.strip() for k in args.series.split(",") if k.strip()]
            plan = build_plan(engine, keys, args.start, args.end,
                              validation_overlap_days=args.validation_overlap_days,
                              allow_validation_mismatch=args.allow_validation_mismatch,
                              fill_basis_conflicts=args.fill_basis_conflicts)
            args.out.write_text(json.dumps(plan, indent=1, sort_keys=True, default=str), encoding="utf-8")
            print(json.dumps(_summary(plan), indent=1, sort_keys=True, default=str))
            return EXIT_OK if plan["status"] == "READY" else EXIT_BLOCKED
        if args.cmd == "apply":
            plan = load_plan(args.plan)
            result = apply_plan(engine, plan, expect_digest=args.expect_plan_digest, run_id=args.run_id,
                                max_rows=args.max_rows, ledger=Ledger(args.ledger),
                                record_revisions=args.record_revisions,
                                max_age_hours=args.plan_max_age_hours)
            print(json.dumps({"exit_code": result.exit_code, "inserted": result.inserted,
                              "skipped_present": result.skipped_present,
                              "revisions_recorded": result.revisions_recorded,
                              "chunks_committed": result.chunks_committed, "message": result.message},
                             indent=1, sort_keys=True))
            return result.exit_code
        plan = load_plan(args.plan)
        report = verify_plan(engine, plan, args.run_id)
        print(json.dumps(report, indent=1, sort_keys=True, default=str))
        return EXIT_OK if report["ok"] else EXIT_INCOMPLETE
    except BackfillRefused as exc:
        print(f"REFUSED: {exc}", file=sys.stderr)
        return exc.exit_code
    except ProviderError as exc:
        print(f"PROVIDER FAILURE (nothing written by plan): {exc}", file=sys.stderr)
        return EXIT_INCOMPLETE


if __name__ == "__main__":
    raise SystemExit(main())
