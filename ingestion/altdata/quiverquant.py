"""GRID — QuiverQuant Expanded Puller.

Pulls all available endpoints from QuiverQuant API (Trader plan $75/mo):
  - WSB Mentions (wallstreetbets sentiment)
  - Wikipedia Trends (attention proxy)
  - Government Contracts (fiscal flow signal)
  - Lobbying (corporate influence signal)
  - Senate/House Trading (congressional insider proxy)
  - Insider Trading (QQ cleaned version)
  - Patent Filings (innovation signal)
  - SPAC Deals
  - Political Beta (party correlation)

All data stored in signal_sources with source_type='quiverquant:{endpoint}'.

source_id identifies the act, not the feed: insider / house / senate / lobbying
rows are keyed ``qq_<endpoint>:<identity>`` (see ``quiverquant_identity``) so
that two acts on the same ticker and date no longer overwrite each other; the
aggregate endpoints keep the constant ``qq_<endpoint>``.
"""

from __future__ import annotations

import os
import time
from datetime import date, datetime
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.altdata.quiverquant_identity import (
    fiscal_quarter_end,
    parse_year_qtr,
    source_id_for,
    transition_guard_blocks,
    transition_marker_path,
)
from ingestion.base import BasePuller
from ingestion.altdata import quiverquant_transactions as tx

_BASE_URL = "https://api.quiverquant.com/beta"
_RATE_LIMIT = 1.0  # seconds between requests
_TIMEOUT = 120
_MAX_ATTEMPTS = 3
STORE_BATCH_ROWS = tx.MAX_WRITE_ROWS


class QuiverStoreAborted(RuntimeError):
    """Preserve acknowledged writes without claiming an uncertain transaction."""

    def __init__(self, message: str, *, stored: int, failed: int = 0, uncertain: bool = False):
        super().__init__(message)
        self.stored = stored
        self.failed = failed
        self.commit_uncertain = uncertain

# Endpoints to pull with their config
ENDPOINTS = {
    "wsb": {
        "path": "/live/wallstreetbets",
        "source_type": "quiverquant:wsb",
        "description": "WallStreetBets ticker mentions and sentiment",
    },
    "lobbying": {
        "path": "/live/lobbying",
        "source_type": "quiverquant:lobbying",
        "description": "Corporate lobbying expenditures",
    },
    "insider_trading": {
        "path": "/live/insiders",
        "source_type": "quiverquant:insider",
        "description": "Insider trading filings (QQ cleaned)",
    },
    "gov_contracts": {
        "path": "/live/govcontracts",
        "source_type": "quiverquant:gov_contracts",
        "description": "Federal government contracts by ticker/quarter",
    },
    "off_exchange": {
        "path": "/live/offexchange",
        "source_type": "quiverquant:offexchange",
        "description": "Dark pool / OTC short volume with DPI",
    },
    "flights": {
        "path": "/live/flights",
        "source_type": "quiverquant:flights",
        "description": "Corporate jet tracking (departure/arrival cities)",
    },
    "senate_trading": {
        "path": "/live/senatetrading",
        "source_type": "quiverquant:senate",
        "description": "Senate stock trading disclosures",
    },
    "house_trading": {
        "path": "/live/housetrading",
        "source_type": "quiverquant:house",
        "description": "House stock trading disclosures",
    },
    "twitter": {
        "path": "/live/twitter",
        "source_type": "quiverquant:twitter",
        "description": "Twitter follower changes for companies",
    },
    "political_beta": {
        "path": "/live/politicalbeta",
        "source_type": "quiverquant:political_beta",
        "description": "Stock correlation with political outcomes (Trump beta)",
    },
}


class QuiverQuantPuller(BasePuller):
    """Scheduler adapter for the module-level QuiverQuant puller functions."""

    SOURCE_NAME = "quiverquant"
    SOURCE_CONFIG = {
        "base_url": _BASE_URL,
        "cost_tier": "PAID",
        "latency_class": "REALTIME",
        "pit_available": False,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 12,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)

    def pull_all(self) -> list[dict[str, Any]]:
        return pull_all(self.engine)


def _get_api_key() -> str:
    """Get QuiverQuant API key from environment."""
    key = os.environ.get("QUIVERQUANT_API_KEY", "")
    if not key:
        raise ValueError("QUIVERQUANT_API_KEY not set in environment")
    return key


def _fetch_endpoint(path: str, api_key: str) -> list[dict]:
    """Fetch data from a QuiverQuant endpoint."""
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }
    url = f"{_BASE_URL}{path}"
    last_exc: Exception | None = None
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=_TIMEOUT)

            if resp.status_code == 429:
                delay = min(30, 5 * attempt)
                log.warning("QuiverQuant rate limited on {}; retrying in {}s", path, delay)
                time.sleep(delay)
                continue

            resp.raise_for_status()
            data = resp.json()
            return data if isinstance(data, list) else []
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_exc = exc
            if attempt < _MAX_ATTEMPTS:
                delay = min(30, 5 * attempt)
                log.warning(
                    "QuiverQuant {} attempt {}/{} timed out; retrying in {}s",
                    path,
                    attempt,
                    _MAX_ATTEMPTS,
                    delay,
                )
                time.sleep(delay)
                continue
            raise
    if last_exc:
        raise last_exc
    return []


def _insider_signal_type(rec: dict[str, Any]) -> str:
    """Classify a QuiverQuant /live/insiders record as a buy or a sell.

    The endpoint returns ``AcquiredDisposedCode`` ("A" acquired / "D"
    disposed) and the Form 4 ``TransactionCode``; it does not return a
    ``TransactionType`` field. Reading that missing field labelled every
    insider row "insider_sell", which made the feed's direction meaningless
    downstream (edge_signals reads insider_sell as bearish).

    Parameters:
        rec: One QuiverQuant insider record.

    Returns:
        "insider_buy" or "insider_sell".
    """
    acq_disp = str(rec.get("AcquiredDisposedCode") or "").strip().upper()[:1]
    if acq_disp == "A":
        return "insider_buy"
    if acq_disp == "D":
        return "insider_sell"

    code = str(rec.get("TransactionCode") or "").strip().upper()[:1]
    if code == "P":
        return "insider_buy"
    if code == "S":
        return "insider_sell"

    txn = str(rec.get("TransactionType") or "").lower()
    return "insider_buy" if "buy" in txn or "purchase" in txn else "insider_sell"


def _gov_contract_period_date(rec: dict) -> date | None:
    """Resolve a stable period-end date for a gov_contracts quarterly record.

    QuiverQuant's ``(Year, Qtr)`` is the US *federal fiscal* quarter: FY Y Q1
    is Oct-Dec of Y-1, Q2 Jan-Mar Y, Q3 Apr-Jun Y, Q4 Jul-Sep Y. GD-FIX (#694)
    read it as a calendar quarter, which dated every aggregate one quarter too
    late (the people-events PIT canary saw "2026 Q4" on 2026-09-11, before
    calendar Q4 began, and ``max(signal_date)`` was a future 2026-12-31).

    The date is a stable key, not a "known at" time: QuiverQuant publishes the
    aggregate while its quarter is still running and rewrites it in place on every
    pull after the quarter ends too (``DO UPDATE SET signal_value``), so a stored
    value was not known at ``signal_date``. Do not score it as known then.

    Parameters:
        rec: A raw QuiverQuant ``/live/govcontracts`` record.

    Returns:
        The fiscal-quarter end date for the record's (Year, Qtr), or ``None``
        when those fields aren't present/parseable.
    """
    year_qtr = parse_year_qtr(rec)
    if year_qtr is None:
        return None
    return fiscal_quarter_end(*year_qtr)


def _resolve_signal_date(rec: dict, endpoint_key: str, today: date) -> date:
    """Resolve the signal_date to store for one QuiverQuant record.

    GD-FIX: gov_contracts records have no per-event date — only a (Year,
    Qtr) pair describing the aggregate's federal fiscal quarter — so the old
    fallback of ``signal_date = today`` meant the same ~821 quarterly rows
    were re-inserted under a new date every single daily pull (the plan's
    evidence: the row count multiplying about 30x/month). Anchoring
    signal_date to the fiscal quarter's own end date instead means a re-pull of
    unchanged data hits the same (source_type, source_id, ticker,
    signal_date, signal_type) key and updates in place via the existing
    ON CONFLICT clause, rather than inserting a new row.

    Parameters:
        rec: The raw record from the endpoint.
        endpoint_key: The QuiverQuant endpoint key (e.g. ``"gov_contracts"``).
        today: Today's date (injected for testability).

    Returns:
        The date to store as ``signal_date``.
    """
    if endpoint_key == "gov_contracts":
        period_date = _gov_contract_period_date(rec)
        if period_date is not None:
            return period_date

    date_str = rec.get("Date") or rec.get("date") or rec.get("ReportDate") or ""
    if date_str:
        try:
            if isinstance(date_str, str):
                return datetime.fromisoformat(date_str.replace("Z", "+00:00")).date()
            if isinstance(date_str, (int, float)):
                return datetime.fromtimestamp(date_str / 1000).date()
            return date_str
        except (ValueError, TypeError):
            return today
    return today


def _store_signals(
    engine: Engine,
    records: list[dict],
    source_type: str,
    endpoint_key: str,
) -> int:
    """Store QuiverQuant records into signal_sources table.

    ``source_id`` is built from the act's identity (``source_id_for``), so two
    acts that share a ticker and a date land in two rows instead of the second
    overwriting the first. Records that still share a full key (an identical
    act reported twice, or acts QuiverQuant gives us no field to tell apart)
    upsert onto one row, and the count is logged so the residue is visible.
    """
    if not records:
        return 0

    import json as _json

    rows_inserted = failed = 0
    tx.validate_batch_size(STORE_BATCH_ROWS)
    prepared: list[dict[str, Any]] = []
    today = date.today()
    seen_keys: set[tuple[str, str, date, str]] = set()
    key_repeats = 0

    for rec in records:
        try:
            ticker = rec.get("Ticker") or rec.get("ticker") or ""
            if not ticker:
                continue

            # Parse date — many QQ endpoints return current-day data without
            # a date. gov_contracts is special-cased (see docstring on
            # _resolve_signal_date): it gets a stable quarter-end marker
            # instead of "today" so re-pulls dedupe instead of piling up.
            signal_date = _resolve_signal_date(rec, endpoint_key, today)

            # Build signal_value as proper JSON
            signal_value = {k: v for k, v in rec.items()
                           if k not in ("Ticker", "ticker", "Date", "date", "ReportDate")}

            # Determine signal_type from endpoint
            signal_type = endpoint_key
            if endpoint_key == "wsb":
                sentiment = rec.get("Sentiment", 0)
                signal_type = "wsb_bullish" if sentiment and sentiment > 0 else "wsb_bearish" if sentiment and sentiment < 0 else "wsb_neutral"
            elif endpoint_key == "insider_trading":
                signal_type = _insider_signal_type(rec)

            source_id = source_id_for(endpoint_key, rec)
            key = (source_id, ticker.upper(), signal_date, signal_type)
            if key in seen_keys:
                key_repeats += 1
            seen_keys.add(key)

            prepared.append({
                "source_type": source_type, "source_id": source_id,
                "signal_type": signal_type, "ticker": ticker.upper(),
                "signal_date": signal_date, "signal_value": _json.dumps(signal_value),
            })
        except (ValueError, TypeError, AttributeError):
            failed += 1
            log.warning("QuiverQuant {}: malformed record skipped before writing", endpoint_key)

    statement = text("""
                    INSERT INTO signal_sources
                        (source_type, source_id, signal_type, ticker, signal_date, signal_value, created_at)
                    VALUES
                        (:source_type, :source_id, :signal_type, :ticker, :signal_date, CAST(:signal_value AS jsonb), NOW())
                    ON CONFLICT (source_type, source_id, ticker, signal_date, signal_type)
                    DO UPDATE SET signal_value = EXCLUDED.signal_value
                """)

    def write(batch: list[dict[str, Any]]) -> None:
        with tx.write_transaction(engine) as (conn, check):
            for params in batch:
                check()
                conn.execute(statement, params)

    for start in range(0, len(prepared), STORE_BATCH_ROWS):
        batch = prepared[start:start + STORE_BATCH_ROWS]
        try:
            write(batch)
        except Exception as exc:
            if not tx.is_rolled_back_write_error(exc):
                raise QuiverStoreAborted(
                    "QuiverQuant writes stopped; inspect acknowledged count before any retry",
                    stored=rows_inserted, failed=failed,
                    uncertain=isinstance(exc, tx.CommitUncertain),
                ) from exc
            # The failed batch was rolled back successfully. Isolate bad rows
            # in NEW transactions; PostgreSQL's aborted transaction is never reused.
            for params in batch:
                try:
                    write([params])
                except Exception as row_exc:
                    if not tx.is_rolled_back_write_error(row_exc):
                        raise QuiverStoreAborted(
                            "QuiverQuant row writes stopped; no automatic replay",
                            stored=rows_inserted, failed=failed,
                            uncertain=isinstance(row_exc, tx.CommitUncertain),
                        ) from row_exc
                    failed += 1
                    log.warning("QuiverQuant {}: one rejected row skipped", endpoint_key)
                else:
                    rows_inserted += 1
        else:
            rows_inserted += len(batch)

    if key_repeats:
        log.info(
            "QuiverQuant {}: {} of {} records repeat a full signal_sources key "
            "(same act twice, or indistinguishable acts) and were upserted onto one row",
            endpoint_key, key_repeats, len(records),
        )

    if failed:
        raise QuiverStoreAborted("QuiverQuant stored valid rows with rejected records", stored=rows_inserted, failed=failed)
    return rows_inserted


def pull_endpoint(
    engine: Engine,
    endpoint_key: str,
) -> dict[str, Any]:
    """Pull a single QuiverQuant endpoint."""
    if endpoint_key not in ENDPOINTS:
        return {"endpoint": endpoint_key, "status": "UNKNOWN", "rows": 0}

    if transition_guard_blocks(endpoint_key):
        # Fail-closed until the coordinator has run the re-key / re-date scripts and
        # created the marker file: no API call (the API is paid) and no write. Returned
        # as SKIPPED, never SUCCESS. NB: only an all-skipped result is read by the
        # scheduler as SKIPPED (flat 30-minute retry, no backoff); a mixed one is PARTIAL
        # and feeds the exponential failure backoff. ``pull_all`` therefore holds the
        # whole job while any endpoint is held, so the scheduler sees all-skipped.
        reason = (
            f"QuiverQuant {endpoint_key} held: transition marker {transition_marker_path()} not found "
            "(create it after scripts/qq_rekey_signal_sources.py and scripts/qq_gov_contracts_redate.py have run)"
        )
        log.warning("QuiverQuant pull SKIPPED: {}", reason)
        return {"endpoint": endpoint_key, "status": "SKIPPED", "skipped_reason": reason, "stored": 0}

    cfg = ENDPOINTS[endpoint_key]
    api_key = _get_api_key()

    log.info("QuiverQuant pulling: {} ({})", endpoint_key, cfg["description"])

    try:
        records = _fetch_endpoint(cfg["path"], api_key)
        rows = _store_signals(engine, records, cfg["source_type"], endpoint_key)
        log.info("QuiverQuant {}: {} records fetched, {} stored", endpoint_key, len(records), rows)
        time.sleep(_RATE_LIMIT)
        return {"endpoint": endpoint_key, "status": "SUCCESS", "fetched": len(records), "stored": rows}
    except QuiverStoreAborted as exc:
        log.error("QuiverQuant {} stopped: {}", endpoint_key, exc)
        return {
            "endpoint": endpoint_key, "status": "FAILED", "error": str(exc),
            "stored": exc.stored, "failed_records": exc.failed,
            "stored_is_acknowledged_prefix": True,
            "commit_uncertain": exc.commit_uncertain,
        }
    except Exception as exc:
        log.error("QuiverQuant {} failed: {}", endpoint_key, exc)
        return {"endpoint": endpoint_key, "status": "FAILED", "error": str(exc)}


def pull_all(engine: Engine) -> list[dict[str, Any]]:
    """Pull all QuiverQuant endpoints.

    While the transition guard holds any endpoint, the whole job is held: every endpoint
    is reported SKIPPED and nothing is pulled. A mixed result (aggregates pulled, guarded
    endpoints skipped) would be classified PARTIAL by ``smart_scheduler``, which feeds its
    exponential failure backoff (30 min .. 24 h, rebuilt from ``pull_log`` on restart) and
    could delay the first real pull after the marker is created. An all-skipped result
    retries every 30 minutes with no backoff, makes no API call, and cannot hide an
    aggregate-endpoint failure because no aggregate endpoint runs while held.
    """
    held = [key for key in ENDPOINTS if transition_guard_blocks(key)]
    if held:
        reason = (
            f"QuiverQuant job held: transition marker {transition_marker_path()} not found "
            f"(guarded endpoints: {', '.join(sorted(held))}); create it after "
            "scripts/qq_rekey_signal_sources.py and scripts/qq_gov_contracts_redate.py have run"
        )
        log.warning("QuiverQuant pull SKIPPED: {}", reason)
        return [{"endpoint": key, "status": "SKIPPED", "skipped_reason": reason, "stored": 0} for key in ENDPOINTS]

    log.info("QuiverQuant: pulling all {} endpoints", len(ENDPOINTS))
    results = []
    for key in ENDPOINTS:
        result = pull_endpoint(engine, key)
        results.append(result)
    total = sum(r.get("stored", 0) for r in results)
    ok = sum(1 for r in results if r["status"] == "SUCCESS")
    log.info("QuiverQuant: {}/{} endpoints succeeded, {} total rows stored", ok, len(ENDPOINTS), total)
    return results
