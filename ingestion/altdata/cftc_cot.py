"""
GRID CFTC Commitments of Traders (COT) data ingestion module.

Pulls weekly COT reports from the CFTC Socrata API (legacy, futures-only) and
stores positioning data (commercial, noncommercial, open interest, net
speculative) as separate series in ``raw_series``.

Identity (2026-09-26): each series is keyed by the CFTC
``cftc_contract_market_code`` — ``cftc.<market_code>.<metric>``, e.g.
``cftc.13874A.net_speculative`` for the CME E-mini S&P 500. The puller asks
Socrata for exactly that code and never matches on a market name, so a
renamed, micro, dividend-index or other-exchange market cannot enter a
series. A configured code missing from the report is logged and skipped;
nothing is substituted. See ``ingestion/altdata/cftc_markets.py`` for the
market registry, the reasons, and the publication-time rule.

The legacy name-matched ids (``cftc.SP500.*``, ``cftc.GOLD.*`` ...) are no
longer written. Their rows stay in ``raw_series`` untouched as history.

Every stored row carries in ``raw_payload``: the market code, root and
label, the market name *as reported that week*, the Socrata row id, the
Tuesday ``report_date`` and the scheduled ``release_at`` (Friday 15:30 ET,
holiday-shifted; a floor, see cftc_markets). ``raw_series.pull_timestamp``
is the binding known-at.

Scheduled runs only fetch forward from the newest stored report (or the last
``_BOOTSTRAP_LOOKBACK_DAYS`` when a market has no rows yet). Full history
under the new ids is a separate, owner-approved backfill:
``scripts/backfill_cftc_market_codes.py`` (dry-run by default).

Data source: https://publicreporting.cftc.gov/resource/6dca-aqww.json
No API key required (public dataset).
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

import pandas as pd
import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.altdata.cftc_markets import (
    COT_METRICS,
    MARKET_CODE_FIELD,
    MARKETS,
    RAW_FIELD_MAP,
    SOCRATA_DATASET,
    compute_release,
    series_id,
)
from ingestion.base import BasePuller, log_pull_failure, retry_on_failure

# CFTC Socrata API endpoint — Futures-Only COT reports
_API_BASE: str = f"https://publicreporting.cftc.gov/resource/{SOCRATA_DATASET}.json"

# Minimum delay between CFTC API calls (seconds)
_RATE_LIMIT_DELAY: float = 1.0

# HTTP request timeout (seconds)
_REQUEST_TIMEOUT: int = 30

# Socrata API page size limit
_PAGE_LIMIT: int = 5000

# Re-fetch overlap behind the newest stored report (dedup makes it a no-op
# unless a week was missed).
_INCREMENTAL_OVERLAP_DAYS: int = 7

# A market with no rows under its code-keyed ids gets only this much history
# from a scheduled run; the full history is the explicit backfill script.
_BOOTSTRAP_LOOKBACK_DAYS: int = 56

# Earliest report date the backfill asks for by default.
BACKFILL_DEFAULT_START: date = date(2006, 1, 1)


@dataclass(frozen=True)
class ParsedReport:
    """One week's positions for one market code, all metrics present."""

    report_date: date
    metrics: dict[str, float]
    market_name: str
    contract_market_name: str
    socrata_id: str | None


@dataclass
class ParseOutcome:
    reports: list[ParsedReport] = field(default_factory=list)
    skipped: list[dict[str, Any]] = field(default_factory=list)


def _parse_report_date(raw: Any) -> date | None:
    if raw is None:
        return None
    try:
        return pd.Timestamp(raw).date()
    except Exception:
        log.warning("CFTC COT: could not parse report date: {v}", v=raw)
        return None


def _parse_metrics(record: dict[str, Any]) -> tuple[dict[str, float] | None, str | None]:
    """All five raw metrics as floats plus net_speculative, or (None, reason)."""
    out: dict[str, float] = {}
    for metric, fld in RAW_FIELD_MAP.items():
        raw_val = record.get(fld)
        if raw_val is None or str(raw_val).strip() == "":
            return None, f"missing field {fld}"
        try:
            out[metric] = float(raw_val)
        except (TypeError, ValueError):
            log.warning("CFTC COT: could not parse {f}={v} as float", f=fld, v=raw_val)
            return None, f"unparseable field {fld}"
    out["net_speculative"] = out["noncommercial_long"] - out["noncommercial_short"]
    return out, None


def parse_market_records(code: str, records: Iterable[dict[str, Any]]) -> ParseOutcome:
    """Turn Socrata rows into one ``ParsedReport`` per report date for ``code``.

    Fail-closed rules (each skipped row is returned with a reason):

    * a row whose ``cftc_contract_market_code`` is not exactly ``code`` is
      dropped — whatever its name says;
    * a row missing any of the five raw metrics is dropped (no zero fill);
    * two rows for the same report date with different numbers drop that
      date entirely (identical duplicates collapse to one).
    """
    outcome = ParseOutcome()
    by_date: dict[date, list[ParsedReport]] = {}
    for rec in records:
        rec_code = str(rec.get(MARKET_CODE_FIELD) or "").strip()
        if rec_code != code:
            outcome.skipped.append({
                "reason": "market_code_mismatch",
                "expected": code,
                "got": rec_code or None,
                "market_name": rec.get("market_and_exchange_names"),
            })
            continue
        rd = _parse_report_date(rec.get("report_date_as_yyyy_mm_dd"))
        if rd is None:
            outcome.skipped.append({"reason": "bad_report_date", "raw": rec.get("report_date_as_yyyy_mm_dd")})
            continue
        metrics, why = _parse_metrics(rec)
        if metrics is None:
            outcome.skipped.append({"reason": "incomplete_metrics", "report_date": rd.isoformat(), "detail": why})
            continue
        by_date.setdefault(rd, []).append(ParsedReport(
            report_date=rd,
            metrics=metrics,
            market_name=str(rec.get("market_and_exchange_names") or ""),
            contract_market_name=str(rec.get("contract_market_name") or ""),
            socrata_id=rec.get("id"),
        ))

    for rd in sorted(by_date):
        rows = by_date[rd]
        if any(r.metrics != rows[0].metrics for r in rows[1:]):
            outcome.skipped.append({
                "reason": "conflicting_rows_same_report_date",
                "report_date": rd.isoformat(),
                "n": len(rows),
            })
            continue
        outcome.reports.append(rows[0])
    return outcome


def build_payload(code: str, report: ParsedReport, metric: str) -> dict[str, Any]:
    """``raw_payload`` for one stored metric row."""
    market = MARKETS[code]
    return {
        "identity": MARKET_CODE_FIELD,
        "market_code": code,
        "root": market.root,
        "label": market.label,
        "market_name": report.market_name,
        "contract_market_name": report.contract_market_name,
        "metric": metric,
        "socrata_dataset": SOCRATA_DATASET,
        "socrata_id": report.socrata_id,
        **compute_release(report.report_date).to_payload(),
    }


class CFTCCOTPuller(BasePuller):
    """Pulls CFTC Commitments of Traders (futures-only) data by market code.

    Each metric is stored as ``cftc.<market_code>.<metric>``, e.g.
    ``cftc.13874A.net_speculative``, ``cftc.088691.commercial_long``.
    """

    SOURCE_NAME: str = "CFTC_COT"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://publicreporting.cftc.gov",
        "cost_tier": "FREE",
        "latency_class": "WEEKLY",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 30,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info("CFTCCOTPuller initialised -- source_id={sid}", sid=self.source_id)

    @retry_on_failure(
        max_attempts=3,
        backoff=3.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError, requests.RequestException),
    )
    def _fetch_market_page(
        self,
        code: str,
        start_date: date,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """Fetch one page of rows for exactly one market code.

        The code goes in as a Socrata equality filter parameter (URL-encoded
        by requests), never into a LIKE on the market name.
        """
        params: dict[str, Any] = {
            MARKET_CODE_FIELD: code,
            "$where": f"report_date_as_yyyy_mm_dd >= '{start_date.isoformat()}'",
            "$order": "report_date_as_yyyy_mm_dd ASC, id ASC",
            "$limit": _PAGE_LIMIT,
            "$offset": offset,
        }
        resp = requests.get(
            _API_BASE,
            params=params,
            headers={"User-Agent": "GRID-DataPuller/1.0", "Accept": "application/json"},
            timeout=_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()

    def _fetch_market(self, code: str, start_date: date) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        offset = 0
        while True:
            page = self._fetch_market_page(code, start_date, offset)
            if not page:
                break
            out.extend(page)
            if len(page) < _PAGE_LIMIT:
                break
            offset += _PAGE_LIMIT
            time.sleep(_RATE_LIMIT_DELAY)
        return out

    def _incremental_start(self, code: str) -> tuple[date, str]:
        """Oldest per-metric latest date minus overlap, or a bootstrap window."""
        latest: list[date] = []
        for metric in COT_METRICS:
            d = self._get_latest_date(series_id(code, metric))
            if d is None:
                start = date.today() - timedelta(days=_BOOTSTRAP_LOOKBACK_DAYS)
                return start, "bootstrap"
            latest.append(d)
        return min(latest) - timedelta(days=_INCREMENTAL_OVERLAP_DAYS), "incremental"

    def pull_market(
        self,
        code: str,
        start_date: str | date | None = None,
        *,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Pull one market code into ``cftc.<code>.<metric>`` series.

        Parameters:
            code: A key of ``cftc_markets.MARKETS`` (e.g. ``"13874A"``).
            start_date: Earliest report date to fetch. ``None`` = incremental
                from the newest stored report (bootstrap window if none).
            dry_run: Fetch and parse, count what would be inserted, write nothing.
        """
        result: dict[str, Any] = {
            "market_code": code,
            "root": MARKETS[code].root if code in MARKETS else None,
            "status": "SUCCESS",
            "rows_inserted": 0,
            "rows_would_insert": 0,
            "reports": 0,
            "skipped": [],
            "errors": [],
            "dry_run": dry_run,
        }
        if code not in MARKETS:
            result["status"] = "FAILED"
            result["errors"].append(f"Unknown market code: {code}")
            return result

        if isinstance(start_date, str):
            start_date = date.fromisoformat(start_date)
        if start_date is None:
            start, mode = self._incremental_start(code)
        else:
            start, mode = start_date, "explicit"
        result["start_date"] = start.isoformat()
        result["mode"] = mode

        try:
            records = self._fetch_market(code, start)
        except Exception as exc:
            log_pull_failure("CFTC_COT", code, exc)
            result["status"] = "FAILED"
            result["errors"].append(str(exc))
            return result

        parsed = parse_market_records(code, records)
        result["skipped"] = parsed.skipped
        for s in parsed.skipped:
            log.warning("CFTC {c}: skipped row ({r})", c=code, r=s)
        if not parsed.reports:
            # Fail closed: the configured code is absent from the report.
            # No other market is looked up in its place.
            log.warning(
                "CFTC COT: market code {c} ({l}) not present in report since {d}; nothing stored",
                c=code, l=MARKETS[code].label, d=start,
            )
            result["status"] = "SKIPPED"
            result["errors"].append(f"market code {code} not in report since {start.isoformat()}")
            return result

        result["reports"] = len(parsed.reports)
        result["first_report_date"] = parsed.reports[0].report_date.isoformat()
        result["last_report_date"] = parsed.reports[-1].report_date.isoformat()
        result["market_names_seen"] = sorted({r.market_name for r in parsed.reports})

        try:
            ctx = self.engine.connect() if dry_run else self.engine.begin()
            with ctx as conn:
                existing = {
                    m: self._get_existing_dates(series_id(code, m), conn, start_date=start)
                    for m in COT_METRICS
                }
                for report in parsed.reports:
                    for metric in COT_METRICS:
                        if report.report_date in existing[metric]:
                            continue
                        if dry_run:
                            result["rows_would_insert"] += 1
                            continue
                        conn.execute(
                            text(
                                "INSERT INTO raw_series "
                                "(series_id, source_id, obs_date, value, "
                                "raw_payload, pull_status) "
                                "VALUES (:sid, :src, :od, :val, :payload, 'SUCCESS')"
                            ),
                            {
                                "sid": series_id(code, metric),
                                "src": self.source_id,
                                "od": report.report_date,
                                "val": report.metrics[metric],
                                "payload": json.dumps(build_payload(code, report, metric)),
                            },
                        )
                        existing[metric].add(report.report_date)
                        result["rows_inserted"] += 1
        except Exception as exc:
            # The transaction rolls back; no FAILED/zero marker row is written
            # (a zero is an observation of nothing).
            log_pull_failure("CFTC_COT", code, exc)
            result["status"] = "FAILED"
            result["errors"].append(str(exc))
            result["rows_inserted"] = 0
            return result

        if parsed.skipped:
            result["status"] = "PARTIAL"
        log.info(
            "CFTC {c} ({r}): {n} reports, {i} rows {verb}",
            c=code, r=MARKETS[code].root, n=len(parsed.reports),
            i=result["rows_would_insert"] if dry_run else result["rows_inserted"],
            verb="would insert (dry run)" if dry_run else "inserted",
        )
        return result

    def pull_all(
        self,
        market_codes: list[str] | None = None,
        start_date: str | date | None = None,
        *,
        dry_run: bool = False,
    ) -> list[dict[str, Any]]:
        """Pull every tracked market code. Never stops on one market's failure.

        The scheduler calls this with no arguments: incremental per market,
        bootstrap window only for markets with no rows yet (no full backfill).
        """
        codes = list(market_codes) if market_codes is not None else list(MARKETS)
        log.info(
            "Starting CFTC COT pull -- {n} market codes, start={sd}, dry_run={dr}",
            n=len(codes), sd=start_date or "incremental", dr=dry_run,
        )
        results: list[dict[str, Any]] = []
        for code in codes:
            results.append(self.pull_market(code, start_date, dry_run=dry_run))
            time.sleep(_RATE_LIMIT_DELAY)

        log.info(
            "CFTC COT pull complete -- {ok}/{total} markets SUCCESS, {rows} rows inserted",
            ok=sum(1 for r in results if r["status"] == "SUCCESS"),
            total=len(results),
            rows=sum(r["rows_inserted"] for r in results),
        )
        return results


if __name__ == "__main__":
    print(
        "Run the scheduled path via SmartScheduler, or the owner-gated history "
        "backfill via scripts/backfill_cftc_market_codes.py (dry-run by default)."
    )
