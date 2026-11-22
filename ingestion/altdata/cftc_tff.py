"""GRID CFTC Traders in Financial Futures (TFF) ingestion module.

Pulls weekly CFTC TFF futures-only positioning from Socrata (dataset
``gpe5-46if``), separating **Leveraged Funds** (hedge funds, CTAs, Treasury
basis-trade books) from **Asset Managers** and **Dealers**. The legacy COT
puller (``ingestion/altdata/cftc_cot.py``, dataset ``6dca-aqww``) only has the
commercial / non-commercial split, which lumps those groups together.

Reuses ``ingestion/altdata/cftc_markets.py`` for market identity (one
``cftc_contract_market_code`` per series, never a name match) and for the
publication rule: TFF is released with the legacy COT report, Friday 15:30 ET
for the Tuesday position date, holiday-shifted. Every stored row carries
``compute_release(report_date).to_payload()`` (``release_at`` and its rule) in
``raw_payload``; that value is a floor. The binding known-at of a row is its
own ``raw_series.pull_timestamp``, as for ``cftc_cot``. A report date with no
computable release is not stored.

Series stored in ``raw_series``: ``cftc_tff.<market_code>.<metric>`` with
metric in ``lev_money_long``, ``lev_money_short``, ``lev_money_net``,
``asset_mgr_net``, ``dealer_net``, ``lev_money_pct_oi``, ``open_interest``.

Not scheduled anywhere yet: nothing calls :meth:`CFTCTFFPuller.pull_all`
except tests. The Socrata field names below follow the TFF dataset's column
names; they have not been re-checked against the live dataset in this change,
so the parser fails closed (skips the row) on any missing field.
"""

from __future__ import annotations

import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

import pandas as pd
import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.altdata.cftc_markets import (
    MARKET_CODE_FIELD,
    MARKETS,
    compute_release,
)
from ingestion.base import BasePuller, log_pull_failure, retry_on_failure

#: Socrata dataset: Traders in Financial Futures (TFF), futures only.
TFF_SOCRATA_DATASET: str = "gpe5-46if"
_TFF_API_BASE: str = f"https://publicreporting.cftc.gov/resource/{TFF_SOCRATA_DATASET}.json"

_RATE_LIMIT_DELAY: float = 1.0
_REQUEST_TIMEOUT: int = 30
_PAGE_LIMIT: int = 5000
_BOOTSTRAP_LOOKBACK_DAYS: int = 90
_INCREMENTAL_OVERLAP_DAYS: int = 7

#: Financial-futures subset of ``cftc_markets.MARKETS`` covered by TFF.
TFF_MARKET_CODES: tuple[str, ...] = (
    "13874A",  # ES -- E-mini S&P 500
    "209742",  # NQ -- E-mini Nasdaq-100
    "124603",  # YM -- E-mini Dow
    "1170E1",  # VX -- VIX futures
    "042601",  # ZT -- 2Y UST note (basis-trade core)
    "044601",  # ZF -- 5Y UST note (basis-trade core)
    "043602",  # ZN -- 10Y UST note (basis-trade core)
    "020601",  # ZB -- UST bond
)

TFF_RAW_FIELDS: dict[str, str] = {
    "dealer_long": "dealer_positions_long_all",
    "dealer_short": "dealer_positions_short_all",
    "asset_mgr_long": "asset_mgr_positions_long",
    "asset_mgr_short": "asset_mgr_positions_short",
    "lev_money_long": "lev_money_positions_long",
    "lev_money_short": "lev_money_positions_short",
    "open_interest": "open_interest_all",
}

TFF_METRICS: tuple[str, ...] = (
    "lev_money_long",
    "lev_money_short",
    "lev_money_net",
    "asset_mgr_net",
    "dealer_net",
    "lev_money_pct_oi",
    "open_interest",
)


def tff_series_id(code: str, metric: str) -> str:
    """Build ``cftc_tff.<market_code>.<metric>`` with strict code/metric validation."""
    if code not in TFF_MARKET_CODES:
        raise ValueError(f"not a tracked CFTC TFF market code: {code!r}")
    if metric not in TFF_METRICS:
        raise ValueError(f"not a tracked CFTC TFF metric: {metric!r}")
    return f"cftc_tff.{code}.{metric}"


@dataclass(frozen=True)
class ParsedTFFReport:
    report_date: date
    metrics: dict[str, float]
    market_name: str
    socrata_id: str | None


@dataclass
class TFFParseOutcome:
    reports: list[ParsedTFFReport] = field(default_factory=list)
    skipped: list[dict[str, Any]] = field(default_factory=list)


def _parse_tff_metrics(rec: dict[str, Any]) -> tuple[dict[str, float] | None, str | None]:
    raw: dict[str, float] = {}
    for k, fld in TFF_RAW_FIELDS.items():
        val = rec.get(fld)
        if val is None or str(val).strip() == "":
            return None, f"missing field {fld}"
        try:
            raw[k] = float(val)
        except (TypeError, ValueError):
            return None, f"unparseable field {fld}"

    oi = raw["open_interest"]
    if oi <= 0:
        return None, "non_positive_open_interest"

    lev_net = raw["lev_money_long"] - raw["lev_money_short"]
    return {
        "lev_money_long": raw["lev_money_long"],
        "lev_money_short": raw["lev_money_short"],
        "lev_money_net": lev_net,
        "asset_mgr_net": raw["asset_mgr_long"] - raw["asset_mgr_short"],
        "dealer_net": raw["dealer_long"] - raw["dealer_short"],
        "lev_money_pct_oi": 100.0 * lev_net / oi,
        "open_interest": oi,
    }, None


def parse_tff_records(code: str, records: Iterable[dict[str, Any]]) -> TFFParseOutcome:
    """Fail-closed parser from Socrata TFF rows to ``ParsedTFFReport`` records.

    A row whose market code differs from ``code``, whose report date does not
    parse, or that lacks any raw field is skipped with a reason. Two rows for
    one report date with different numbers drop that date entirely.
    """
    outcome = TFFParseOutcome()
    by_date: dict[date, list[ParsedTFFReport]] = {}
    for rec in records:
        rec_code = str(rec.get(MARKET_CODE_FIELD) or "").strip()
        if rec_code != code:
            outcome.skipped.append({"reason": "market_code_mismatch", "expected": code, "got": rec_code or None})
            continue
        raw_rd = rec.get("report_date_as_yyyy_mm_dd")
        if raw_rd is None:
            outcome.skipped.append({"reason": "bad_report_date", "raw": None})
            continue
        try:
            rd = pd.Timestamp(raw_rd).date()
        except (TypeError, ValueError):
            outcome.skipped.append({"reason": "bad_report_date", "raw": raw_rd})
            continue
        metrics, why = _parse_tff_metrics(rec)
        if metrics is None:
            outcome.skipped.append({"reason": "incomplete_metrics", "report_date": rd.isoformat(), "detail": why})
            continue
        by_date.setdefault(rd, []).append(
            ParsedTFFReport(
                report_date=rd,
                metrics=metrics,
                market_name=str(rec.get("market_and_exchange_names") or ""),
                socrata_id=rec.get("id"),
            )
        )

    for rd in sorted(by_date):
        rows = by_date[rd]
        if any(r.metrics != rows[0].metrics for r in rows[1:]):
            outcome.skipped.append({"reason": "conflicting_rows_same_report_date", "report_date": rd.isoformat()})
            continue
        outcome.reports.append(rows[0])
    return outcome


def build_tff_payload(code: str, report: ParsedTFFReport, metric: str) -> dict[str, Any]:
    """``raw_payload`` for one stored metric row (identity + scheduled release)."""
    market = MARKETS[code]
    return {
        "identity": MARKET_CODE_FIELD,
        "market_code": code,
        "root": market.root,
        "label": market.label,
        "market_name": report.market_name,
        "metric": metric,
        "socrata_dataset": TFF_SOCRATA_DATASET,
        "socrata_id": report.socrata_id,
        **compute_release(report.report_date).to_payload(),
    }


class CFTCTFFPuller(BasePuller):
    """Pulls CFTC Traders in Financial Futures (Leveraged Funds vs. Asset Managers)."""

    SOURCE_NAME: str = "CFTC_TFF"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://publicreporting.cftc.gov",
        "cost_tier": "FREE",
        "latency_class": "WEEKLY",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 29,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)

    @retry_on_failure(
        max_attempts=3,
        backoff=3.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError, requests.RequestException),
    )
    def _fetch_market_page(self, code: str, start_date: date, offset: int = 0) -> list[dict[str, Any]]:
        """One Socrata page for exactly one market code (code as an equality filter).

        ``$where`` is a Socrata SoQL filter sent as a URL parameter (encoded by
        requests), not SQL against GRID's database; its only value is a date.
        """
        params: dict[str, Any] = {
            MARKET_CODE_FIELD: code,
            "$where": f"report_date_as_yyyy_mm_dd >= '{start_date.isoformat()}'",
            "$order": "report_date_as_yyyy_mm_dd ASC, id ASC",
            "$limit": _PAGE_LIMIT,
            "$offset": offset,
        }
        resp = requests.get(
            _TFF_API_BASE,
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

    def _start_date(self, code: str) -> date:
        """Latest stored report minus an overlap, or a bootstrap window."""
        last_obs = self._get_latest_date(tff_series_id(code, "lev_money_net"))
        if last_obs is None:
            return date.today() - timedelta(days=_BOOTSTRAP_LOOKBACK_DAYS)
        return last_obs - timedelta(days=_INCREMENTAL_OVERLAP_DAYS)

    def pull_all(self, *, codes: Iterable[str] | None = None) -> dict[str, Any]:
        """Pull every tracked TFF market code; one market's failure never stops the rest."""
        requested = list(codes) if codes is not None else list(TFF_MARKET_CODES)
        unknown = [c for c in requested if c not in TFF_MARKET_CODES]
        target_codes = [c for c in requested if c in TFF_MARKET_CODES]
        inserted = 0
        failed: list[str] = []
        errors: dict[str, str] = {}
        skipped_rows = 0
        no_release = 0

        for code in target_codes:
            # One guard around the market's whole lifecycle: its latest-date lookup, fetch, parse and write. A
            # failure at any stage is recorded for this market only, with the stage, and the next market runs.
            try:
                market = self._pull_market(code)
            except _MarketFailure as failure:
                log_pull_failure("CFTC_TFF", code, failure.cause)
                failed.append(code)
                errors[code] = f"{failure.stage}: {type(failure.cause).__name__}: {failure.cause}"[:300]
                continue
            inserted += market["inserted"]
            skipped_rows += market["skipped_rows"]
            no_release += market["no_release_rows"]
            time.sleep(_RATE_LIMIT_DELAY)

        if not target_codes or len(failed) == len(target_codes):
            status = "FAILED"
        elif failed or unknown or skipped_rows or no_release:
            status = "PARTIAL"
        else:
            status = "SUCCESS"
        return {
            "status": status,
            "rows_inserted": inserted,
            "failed": failed,
            "errors": errors,
            "unknown_codes": unknown,
            "skipped_rows": skipped_rows,
            "no_release_rows": no_release,
        }

    def _pull_market(self, code: str) -> dict[str, int]:
        """One market's latest-date lookup, fetch, parse and write; any failure is raised as ``_MarketFailure``."""
        stage = "latest_date"
        try:
            start_d = self._start_date(code)
            stage = "fetch"
            raw_rows = self._fetch_market(code, start_d)
            stage = "parse"
            outcome = parse_tff_records(code, raw_rows)
            for s in outcome.skipped:
                log.warning("CFTC TFF {c}: skipped row ({r})", c=code, r=s)
            stage = "write"
            code_inserted = 0
            no_release = 0
            # The transaction rolls back on any error; no FAILED/zero marker row is written.
            with self.engine.begin() as conn:
                for metric in TFF_METRICS:
                    sid = tff_series_id(code, metric)
                    existing = self._get_existing_dates(sid, conn, start_date=start_d)
                    for rep in outcome.reports:
                        if rep.report_date in existing or rep.report_date > date.today():
                            continue
                        payload = build_tff_payload(code, rep, metric)
                        if payload["release_at"] is None:
                            no_release += 1
                            continue
                        self._insert_raw(
                            conn=conn,
                            series_id=sid,
                            obs_date=rep.report_date,
                            value=rep.metrics[metric],
                            raw_payload=payload,
                        )
                        existing.add(rep.report_date)
                        code_inserted += 1
        except Exception as exc:  # noqa: BLE001
            raise _MarketFailure(stage, exc) from exc
        return {"inserted": code_inserted, "skipped_rows": len(outcome.skipped), "no_release_rows": no_release}


class _MarketFailure(Exception):
    """One market's pull failed at ``stage``; the caller records it and moves on."""

    def __init__(self, stage: str, cause: BaseException) -> None:
        super().__init__(f"{stage}: {cause}")
        self.stage = stage
        self.cause = cause
