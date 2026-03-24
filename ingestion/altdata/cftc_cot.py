"""
GRID CFTC Commitments of Traders (COT) data ingestion module.

Pulls weekly COT reports from the CFTC Socrata API (futures-only) and stores
positioning data (commercial, noncommercial, open interest, net speculative)
as separate series in ``raw_series``.

Data source: https://publicreporting.cftc.gov/resource/jun7-fc8e.json
No API key required (public dataset).
"""

from __future__ import annotations

import json
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# CFTC Socrata API endpoint — Futures-Only COT reports
_API_BASE: str = "https://publicreporting.cftc.gov/resource/jun7-fc8e.json"

# Minimum delay between CFTC API calls (seconds)
_RATE_LIMIT_DELAY: float = 1.0

# HTTP request timeout (seconds)
_REQUEST_TIMEOUT: int = 30

# Socrata API page size limit
_PAGE_LIMIT: int = 5000

# Contract name mappings: short key -> CFTC market_and_exchange_names substring
# The CFTC uses long descriptive names; we match on substrings.
CONTRACT_MAP: dict[str, dict[str, str]] = {
    "SP500": {
        "match": "S&P 500",
        "description": "S&P 500 futures positioning",
    },
    "NASDAQ": {
        "match": "NASDAQ",
        "description": "NASDAQ-100 futures positioning",
    },
    "DJIA": {
        "match": "DOW JONES",
        "description": "DJIA futures positioning",
    },
    "USBOND": {
        "match": "U.S. TREASURY BONDS",
        "description": "US Treasury Bond futures positioning",
    },
    "NOTE10Y": {
        "match": "10-YEAR",
        "description": "10-Year Treasury Note futures positioning",
    },
    "NOTE5Y": {
        "match": "5-YEAR",
        "description": "5-Year Treasury Note futures positioning",
    },
    "NOTE2Y": {
        "match": "2-YEAR",
        "description": "2-Year Treasury Note futures positioning",
    },
    "EURODOLLAR": {
        "match": "EURODOLLAR",
        "description": "Eurodollar futures positioning",
    },
    "GOLD": {
        "match": "GOLD",
        "description": "Gold futures positioning",
    },
    "SILVER": {
        "match": "SILVER",
        "description": "Silver futures positioning",
    },
    "CRUDE_OIL": {
        "match": "CRUDE OIL, LIGHT SWEET",
        "description": "Crude Oil WTI futures positioning",
    },
    "NATGAS": {
        "match": "NATURAL GAS",
        "description": "Natural Gas futures positioning",
    },
    "COPPER": {
        "match": "COPPER",
        "description": "Copper futures positioning",
    },
    "CORN": {
        "match": "CORN",
        "description": "Corn futures positioning",
    },
    "SOYBEANS": {
        "match": "SOYBEANS",
        "description": "Soybeans futures positioning",
    },
    "WHEAT": {
        "match": "WHEAT",
        "description": "Wheat futures positioning",
    },
    "VIX": {
        "match": "VIX",
        "description": "VIX futures positioning",
    },
}

# Metrics extracted from each COT report row
COT_METRICS: list[str] = [
    "commercial_long",
    "commercial_short",
    "noncommercial_long",
    "noncommercial_short",
    "total_open_interest",
    "net_speculative",
]

# Mapping from metric name to CFTC API field name
_FIELD_MAP: dict[str, str] = {
    "commercial_long": "comm_positions_long_all",
    "commercial_short": "comm_positions_short_all",
    "noncommercial_long": "noncomm_positions_long_all",
    "noncommercial_short": "noncomm_positions_short_all",
    "total_open_interest": "open_interest_all",
}


def _build_series_id(contract_key: str, metric: str) -> str:
    """Build a series_id in the form ``cftc.{CONTRACT}.{metric}``.

    Parameters:
        contract_key: Short contract key (e.g. 'SP500', 'GOLD').
        metric: Metric name (e.g. 'net_speculative').

    Returns:
        Formatted series_id string.
    """
    return f"cftc.{contract_key}.{metric}"


class CFTCCOTPuller(BasePuller):
    """Pulls CFTC Commitments of Traders (futures-only) data.

    Data source: CFTC Socrata open data portal (no API key required).

    Extracts positioning metrics for 17 major futures contracts across
    equity indices, treasuries, metals, energy, and agriculture.

    Each metric is stored as a separate series_id:
        ``cftc.SP500.net_speculative``, ``cftc.GOLD.commercial_long``, etc.

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for CFTC_COT.
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
        """Initialise the CFTC COT puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        super().__init__(db_engine)
        log.info("CFTCCOTPuller initialised -- source_id={sid}", sid=self.source_id)

    @retry_on_failure(
        max_attempts=3,
        backoff=3.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError, requests.RequestException),
    )
    def _fetch_cot_data(
        self,
        contract_match: str,
        start_date: date | None = None,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """Fetch COT records from the CFTC Socrata API for a contract.

        Parameters:
            contract_match: Substring to match in market_and_exchange_names.
            start_date: Only fetch reports on or after this date.
            offset: Pagination offset for Socrata API.

        Returns:
            List of JSON records from the API.

        Raises:
            requests.RequestException: On HTTP errors.
        """
        params: dict[str, Any] = {
            "$limit": _PAGE_LIMIT,
            "$offset": offset,
            "$order": "report_date_as_yyyy_mm_dd DESC",
        }

        # Build SoQL where clause
        where_parts: list[str] = [
            f"upper(market_and_exchange_names) like upper('%{contract_match}%')",
        ]
        if start_date is not None:
            where_parts.append(
                f"report_date_as_yyyy_mm_dd >= '{start_date.isoformat()}'"
            )
        params["$where"] = " AND ".join(where_parts)

        headers = {
            "User-Agent": "GRID-DataPuller/1.0",
            "Accept": "application/json",
        }

        resp = requests.get(
            _API_BASE,
            params=params,
            headers=headers,
            timeout=_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        records: list[dict[str, Any]] = resp.json()
        return records

    def _extract_metrics(self, record: dict[str, Any]) -> dict[str, float | None]:
        """Extract positioning metrics from a single COT API record.

        Computes net_speculative as noncommercial_long - noncommercial_short.

        Parameters:
            record: Single JSON record from the CFTC API.

        Returns:
            dict mapping metric name to float value (or None if missing).
        """
        metrics: dict[str, float | None] = {}

        for metric_name, field_name in _FIELD_MAP.items():
            raw_val = record.get(field_name)
            if raw_val is not None:
                try:
                    metrics[metric_name] = float(raw_val)
                except (ValueError, TypeError):
                    log.warning(
                        "CFTC COT: could not parse {f}={v} as float",
                        f=field_name,
                        v=raw_val,
                    )
                    metrics[metric_name] = None
            else:
                metrics[metric_name] = None

        # Compute net speculative position
        nc_long = metrics.get("noncommercial_long")
        nc_short = metrics.get("noncommercial_short")
        if nc_long is not None and nc_short is not None:
            metrics["net_speculative"] = nc_long - nc_short
        else:
            metrics["net_speculative"] = None

        return metrics

    def _parse_report_date(self, record: dict[str, Any]) -> date | None:
        """Parse the report date from a CFTC record.

        The API returns ``report_date_as_yyyy_mm_dd`` as a string or
        ISO timestamp.

        Parameters:
            record: Single JSON record from the CFTC API.

        Returns:
            Parsed date, or None if unparseable.
        """
        raw = record.get("report_date_as_yyyy_mm_dd")
        if raw is None:
            return None
        try:
            # API may return ISO datetime string like '2024-01-02T00:00:00.000'
            dt = pd.Timestamp(raw)
            return dt.date()
        except Exception:
            log.warning("CFTC COT: could not parse report date: {v}", v=raw)
            return None

    def pull_contract(
        self,
        contract_key: str,
        start_date: str | date = "2006-01-01",
    ) -> dict[str, Any]:
        """Pull COT data for a single contract and store in raw_series.

        Parameters:
            contract_key: Short key from CONTRACT_MAP (e.g. 'SP500', 'GOLD').
            start_date: Earliest report date to fetch.

        Returns:
            dict with status, rows_inserted, contract_key, errors.
        """
        if contract_key not in CONTRACT_MAP:
            return {
                "status": "FAILED",
                "rows_inserted": 0,
                "contract_key": contract_key,
                "errors": [f"Unknown contract key: {contract_key}"],
            }

        config = CONTRACT_MAP[contract_key]
        log.info(
            "Pulling CFTC COT for {key} (match={m})",
            key=contract_key,
            m=config["match"],
        )

        result: dict[str, Any] = {
            "contract_key": contract_key,
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        if isinstance(start_date, str):
            start_date = date.fromisoformat(start_date)

        # Use incremental start: check the earliest latest_date across all
        # metrics for this contract, so we only fetch new data
        incremental_start = start_date
        for metric in COT_METRICS:
            sid = _build_series_id(contract_key, metric)
            latest = self._get_latest_date(sid)
            if latest is not None:
                # Overlap by 7 days to catch revisions in weekly data
                candidate = latest - timedelta(days=7)
                if candidate > incremental_start:
                    incremental_start = candidate

        if incremental_start > start_date:
            log.info(
                "CFTC {key}: incremental from {d}",
                key=contract_key,
                d=incremental_start,
            )

        try:
            # Fetch all pages of data
            all_records: list[dict[str, Any]] = []
            offset = 0
            while True:
                records = self._fetch_cot_data(
                    contract_match=config["match"],
                    start_date=incremental_start,
                    offset=offset,
                )
                if not records:
                    break
                all_records.extend(records)
                if len(records) < _PAGE_LIMIT:
                    break
                offset += _PAGE_LIMIT
                time.sleep(_RATE_LIMIT_DELAY)

            if not all_records:
                log.warning(
                    "CFTC COT: no data returned for {key}", key=contract_key
                )
                result["status"] = "PARTIAL"
                result["errors"].append("No data returned")
                return result

            log.info(
                "CFTC {key}: fetched {n} records from API",
                key=contract_key,
                n=len(all_records),
            )

            inserted = 0

            with self.engine.begin() as conn:
                # Pre-fetch existing dates for all metrics in this contract
                existing_dates_map: dict[str, set[date]] = {}
                for metric in COT_METRICS:
                    sid = _build_series_id(contract_key, metric)
                    existing_dates_map[metric] = self._get_existing_dates(sid, conn)

                for record in all_records:
                    report_date = self._parse_report_date(record)
                    if report_date is None:
                        continue
                    if report_date < start_date:
                        continue

                    metrics = self._extract_metrics(record)

                    for metric_name, value in metrics.items():
                        if value is None:
                            continue

                        sid = _build_series_id(contract_key, metric_name)

                        # Batch dedup check
                        if report_date in existing_dates_map.get(metric_name, set()):
                            continue

                        self._insert_raw(
                            conn=conn,
                            series_id=sid,
                            obs_date=report_date,
                            value=value,
                            raw_payload={
                                "contract": contract_key,
                                "market_name": record.get(
                                    "market_and_exchange_names", ""
                                ),
                                "metric": metric_name,
                            },
                        )
                        inserted += 1

            result["rows_inserted"] = inserted
            log.info(
                "CFTC {key}: inserted {n} rows",
                key=contract_key,
                n=inserted,
            )

        except Exception as exc:
            log.error(
                "CFTC COT pull failed for {key}: {err}",
                key=contract_key,
                err=str(exc),
            )
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

            # Record the failure row
            try:
                with self.engine.begin() as conn:
                    conn.execute(
                        text(
                            "INSERT INTO raw_series "
                            "(series_id, source_id, obs_date, value, "
                            "raw_payload, pull_status) "
                            "VALUES (:sid, :src, :od, 0, :payload, 'FAILED')"
                        ),
                        {
                            "sid": _build_series_id(contract_key, "net_speculative"),
                            "src": self.source_id,
                            "od": date.today(),
                            "payload": json.dumps({"error": str(exc)}),
                        },
                    )
            except Exception as insert_exc:
                log.error(
                    "Failed to record error row for CFTC {key}: {err}",
                    key=contract_key,
                    err=str(insert_exc),
                )

        return result

    def pull_all(
        self,
        contract_keys: list[str] | None = None,
        start_date: str | date = "2006-01-01",
    ) -> list[dict[str, Any]]:
        """Pull COT data for all configured contracts.

        Never stops on a single-contract failure -- logs and continues.

        Parameters:
            contract_keys: List of contract keys to pull. Defaults to all
                contracts in CONTRACT_MAP.
            start_date: Earliest report date to fetch.

        Returns:
            List of result dicts, one per contract.
        """
        if contract_keys is None:
            contract_keys = list(CONTRACT_MAP.keys())

        log.info(
            "Starting CFTC COT bulk pull -- {n} contracts from {sd}",
            n=len(contract_keys),
            sd=start_date,
        )

        results: list[dict[str, Any]] = []
        for key in contract_keys:
            res = self.pull_contract(key, start_date)
            results.append(res)
            time.sleep(_RATE_LIMIT_DELAY)

        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        total_rows = sum(r["rows_inserted"] for r in results)
        log.info(
            "CFTC COT bulk pull complete -- {ok}/{total} contracts, {rows} rows",
            ok=succeeded,
            total=len(results),
            rows=total_rows,
        )
        return results


if __name__ == "__main__":
    from db import get_engine

    puller = CFTCCOTPuller(db_engine=get_engine())
    results = puller.pull_all(start_date="2020-01-01")
    for r in results:
        status = r["status"]
        rows = r["rows_inserted"]
        key = r["contract_key"]
        print(f"  {key}: {status} ({rows} rows)")
