"""
GRID Australian Bureau of Statistics (ABS) ingestion module.

Pulls Australian CPI, unemployment, iron-ore exports and GDP from the ABS
Data API (SDMX REST).

2026-11-06 endpoint migration
=============================
The old host ``api.data.abs.gov.au/data`` no longer resolves. The ABS Data
API lives at ``https://data.api.abs.gov.au/rest/data/<AGENCY>,<FLOW>,<VER>/<key>``
and the dataflows were re-versioned with new keys, so every legacy key is
mapped to a verified current dataflow/key below (probed live 2026-11-06).
Responses are requested as ``format=csvfile`` (flat CSV with dimension codes).
GRID series ids are unchanged so entity_map / resolver keep working; the
mapping follows the series ids' meaning (the legacy keys could not be
verified -- the old host was dead -- and ``australia_cpi_qoq`` /
``australia_gdp_qoq`` name quarter-on-quarter % changes).
"""

from __future__ import annotations

import csv
import io
import math
import time
from datetime import date, datetime
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from ingestion.base import BasePuller
from ingestion.international.sdmx_json import lookback_start_year
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

# ABS series: legacy SDMX dataset/key -> feature name (series id)
ABS_SERIES: dict[str, str] = {
    "CPI/1.50.10001.10.Q": "australia_cpi_qoq",
    "LABOUR_FORCE/1.1.1599.20.M": "australia_unemployment",
    "MERCH_EXP/1.2601.10..M": "australia_iron_exports",
    "GDP_EXP/1..1..Q": "australia_gdp_qoq",
}

#: Legacy key -> (dataflow ref, current key). Verified live 2026-11-06.
ABS_DATAFLOW_KEYS: dict[str, tuple[str, str]] = {
    # CPI all groups, weighted average of 8 capitals, original,
    # MEASURE 2 = % change from previous period (quarterly).
    "CPI/1.50.10001.10.Q": ("ABS,CPI,2.0.0", "2.10001.10.50.Q"),
    # Unemployment rate (M13), persons, 15+, seasonally adjusted, Australia.
    "LABOUR_FORCE/1.1.1599.20.M": ("ABS,LF,1.0.0", "M13.3.1599.20.AUS.M"),
    # Merchandise exports, SITC 281 iron ore & concentrates, all
    # destinations, all states; published in AUD thousands (UNIT_MULT 3).
    "MERCH_EXP/1.2601.10..M": ("ABS,MERCH_EXP,1.0.0", "281.TOT.TOT.M"),
    # GDP, chain volume measures, % change q/q (M2), seasonally adjusted.
    "GDP_EXP/1..1..Q": ("ABS,ANA_AGG,1.0.0", "M2.GPM.20.AUS.Q"),
}

_ABS_BASE_URL = "https://data.api.abs.gov.au/rest/data"
_RATE_LIMIT_DELAY: float = 2.0
_REQUEST_TIMEOUT: tuple[float, float] = (10.0, 60.0)
_USER_AGENT = "GRID/1.0 (+https://grid.stepdad.finance) abs-puller"


def _is_retryable(exc: BaseException) -> bool:
    """Retry transport errors and 429/5xx only (never 4xx key errors)."""
    if isinstance(exc, requests.HTTPError):
        status = exc.response.status_code if exc.response is not None else 0
        return status == 429 or status >= 500
    return isinstance(exc, (requests.ConnectionError, requests.Timeout))


def abs_series_url(dataset_id: str) -> str:
    """Current ABS Data API URL for a legacy ``ABS_SERIES`` key."""
    flow, key = ABS_DATAFLOW_KEYS[dataset_id]
    return f"{_ABS_BASE_URL}/{flow}/{key}"


def parse_abs_csv(csv_text: str) -> list[tuple[date, float]]:
    """Parse an ABS ``format=csvfile`` payload into ``[(period_start, value)]``.

    Blank / non-numeric / non-finite values are dropped, never coerced to 0.
    Raises ``ValueError`` when the payload is not an ABS data CSV.
    """
    text_in = (csv_text or "").lstrip("﻿")
    if not text_in.strip():
        return []
    reader = csv.DictReader(io.StringIO(text_in))
    fields = set(reader.fieldnames or [])
    if not {"TIME_PERIOD", "OBS_VALUE"} <= fields:
        raise ValueError(f"ABS CSV missing TIME_PERIOD/OBS_VALUE; got {sorted(fields)[:8]}")
    out: list[tuple[date, float]] = []
    for row in reader:
        raw = (row.get("OBS_VALUE") or "").strip()
        if not raw:
            continue
        try:
            value = float(raw)
        except ValueError:
            continue
        if math.isnan(value) or math.isinf(value):
            continue
        obs_dt = ABSPuller._parse_period(row.get("TIME_PERIOD") or "")
        if obs_dt is not None:
            out.append((obs_dt, value))
    out.sort()
    return out


class ABSPuller(BasePuller):
    """Pulls Australian statistics from the ABS Data API."""

    SOURCE_NAME = "ABS_AU"
    SOURCE_CONFIG = {"base_url": "https://data.api.abs.gov.au", "cost_tier": "FREE", "latency_class": "MONTHLY", "pit_available": True, "revision_behavior": "RARE", "trust_score": "HIGH", "priority_rank": 27}

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info("ABSPuller initialised — source_id={sid}", sid=self.source_id)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=10),
        retry=retry_if_exception(_is_retryable),
        reraise=True,
    )
    def _fetch_csv(self, dataset_id: str, start_period: str | None, end_period: str | None = None) -> str:
        params: dict[str, str] = {"format": "csvfile"}
        if start_period:
            params["startPeriod"] = start_period
        if end_period:
            params["endPeriod"] = end_period
        resp = requests.get(
            abs_series_url(dataset_id),
            params=params,
            headers={"Accept": "application/vnd.sdmx.data+csv, text/csv", "User-Agent": _USER_AGENT},
            timeout=_REQUEST_TIMEOUT,
        )
        # A valid key with no rows in the window answers 404 NoRecordsFound.
        if resp.status_code == 404 and "NoRecordsFound" in (resp.text or "")[:500]:
            return ""
        resp.raise_for_status()
        return resp.text

    @staticmethod
    def _parse_period(period_str: str) -> date | None:
        try:
            if "-Q" in period_str:
                year, q = period_str.split("-Q")
                return date(int(year), (int(q) - 1) * 3 + 1, 1)
            elif len(period_str) == 7:
                return datetime.strptime(period_str, "%Y-%m").date()
            elif len(period_str) == 4:
                return date(int(period_str), 1, 1)
        except (ValueError, TypeError):
            pass
        return None

    def pull_series(
        self,
        dataset_id: str,
        start_period: str | None = "1990",
        end_period: str | None = None,
    ) -> dict[str, Any]:
        """Pull a single ABS series (keyed by its legacy ``ABS_SERIES`` key)."""
        feature_name = ABS_SERIES.get(dataset_id, dataset_id)
        log.info("Pulling ABS {fn}", fn=feature_name)

        result: dict[str, Any] = {
            "series_id": feature_name,
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        try:
            payload = self._fetch_csv(dataset_id, start_period, end_period)
            if not payload.strip():
                result["status"] = "UNCHANGED"
                time.sleep(_RATE_LIMIT_DELAY)
                return result
            observations = parse_abs_csv(payload)

            if not observations:
                result["status"] = "PARTIAL"
                result["errors"].append("No data rows in response")
                return result

            inserted = 0
            with self.engine.begin() as conn:
                for obs_dt, value in observations:
                    if self._row_exists(feature_name, obs_dt, conn):
                        continue
                    conn.execute(
                        text(
                            "INSERT INTO raw_series "
                            "(series_id, source_id, obs_date, value, pull_status) "
                            "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                        ),
                        {"sid": feature_name, "src": self.source_id, "od": obs_dt, "val": value},
                    )
                    inserted += 1

            result["rows_inserted"] = inserted
            log.info("ABS {fn}: inserted {n} rows", fn=feature_name, n=inserted)

        except Exception as exc:
            # Upstream HTTP/transport trouble is operational, not a GRID bug.
            log.warning("ABS pull failed for {fn}: {err}", fn=feature_name, err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        time.sleep(_RATE_LIMIT_DELAY)
        return result

    def pull_all(self, start_date: str | date = "1990-01-01") -> dict[str, Any]:
        """Pull all ABS series from ``start_date`` (backed off two years)."""
        log.info("Starting ABS bulk pull from {sd}", sd=start_date)
        # Quarterly series publish ~1 quarter late; an incremental start of
        # "last month" must not filter out the latest published period.
        start_period = lookback_start_year(start_date) or "1990"
        results = [self.pull_series(did, start_period) for did in ABS_SERIES]

        total_rows = sum(r["rows_inserted"] for r in results)
        succeeded = sum(1 for r in results if r["status"] in ("SUCCESS", "UNCHANGED"))
        log.info(
            "ABS bulk pull complete — {ok}/{total} succeeded, {rows} rows",
            ok=succeeded, total=len(results), rows=total_rows,
        )
        return {
            "source": "ABS_AU",
            "total_rows": total_rows,
            "succeeded": succeeded,
            "total": len(results),
            "results": results,
        }
