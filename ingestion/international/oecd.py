"""
GRID OECD SDMX API ingestion module.

Pulls Composite Leading Indicators (CLI) and Main Economic Indicators (MEI)
from the OECD Data Explorer SDMX REST API. Includes 3-month CLI slope
derivation.

2026-11-06 endpoint migration
=============================
``stats.oecd.org/SDMX-JSON`` (the OECD.Stat MEI / MEI_CLI datasets) is
retired. The data now lives on ``sdmx.oecd.org/public/rest`` under new
dataflows with new dimension keys:

* CLI -> ``OECD.SDD.STES,DSD_STES@DF_CLI`` key
  ``<AREA>.M.LI...AA...H`` (composite leading indicator, amplitude adjusted,
  OECD harmonised methodology; long-term average = 100 like the old
  ``LOLITONOSM``). Legacy ``G-7`` is ``G7``. The OECD-total CLI has no
  successor (the new dataflow publishes G7/G20/G4E/A5M/NAFTA aggregates only),
  so ``oecd_cli_oecd`` is no longer pulled.
* MEI -> ``OECD.SDD.STES,DSD_KEI@DF_KEI`` (Key Economic Indicators), key
  ``<AREA>.M.<MEASURE>.<UNIT>.<ACTIVITY>.<ADJUSTMENT>.<TRANSFORMATION>``.
  Index series keep the 2015=100 base of the old MEI series. China has no
  total-industry production series in KEI (only construction), so
  ``china_indpro_oecd`` is no longer pulled rather than being filled with a
  different concept.

GRID series ids are unchanged so entity_map / resolver keep working.

The OECD API is rate limited (~20 requests/minute per IP). Each run makes
two requests: one CLI call with every country joined by ``+`` and one KEI
call with every area/measure joined by ``+``; rows are matched client side.
Responses are requested as ``format=csvfile`` (flat CSV with dimension
codes): it was more complete than ``format=jsondata`` in a 2026-11-06 probe.
"""

from __future__ import annotations

import csv
import io
import math
import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from ingestion.base import BasePuller, log_pull_failure
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

# CLI country codes (legacy codes; series id = oecd_cli_<code lower, no '-'>)
OECD_CLI_COUNTRIES: list[str] = [
    "USA", "DEU", "JPN", "GBR", "FRA", "ITA", "CAN", "CHN", "KOR", "G-7",
]

#: Legacy CLI codes whose REF_AREA changed in the new dataflow.
_CLI_AREA_ALIASES: dict[str, str] = {"G-7": "G7"}

#: Legacy CLI codes with no successor in DF_CLI (documented, not pulled).
OECD_CLI_RETIRED: dict[str, str] = {
    "OECD": "OECD-total CLI not published by DSD_STES@DF_CLI (only G7/G20/G4E/A5M/NAFTA aggregates)",
}

# MEI series: legacy SDMX key -> GRID feature name (series id).
OECD_MEI_SERIES: dict[str, str] = {
    "MEI.PRINTO01.USA.ST.M": "us_indpro_oecd",
    "MEI.LCEAMN01.USA.ST.M": "us_unit_labor_cost",
    "MEI.CPALTT01.USA.GP.M": "us_cpi_oecd",
    "MEI.LRHUTTTT.USA.ST.M": "us_unemployment_oecd",
    "MEI.PRINTO01.DEU.ST.M": "germany_indpro_oecd",
}

#: Legacy MEI keys with no faithful successor (documented, not pulled).
OECD_MEI_RETIRED: dict[str, str] = {
    "MEI.PRINTO01.CHN.ST.M": (
        "china_indpro_oecd: DF_KEI has no China total-industry production "
        "(only PRVM.IX.F = construction)"
    ),
}

#: Legacy MEI key -> DF_KEI series key (REF_AREA.FREQ.MEASURE.UNIT_MEASURE.
#: ACTIVITY.ADJUSTMENT.TRANSFORMATION). Verified live 2026-11-06.
OECD_KEI_KEYS: dict[str, str] = {
    # Industrial production, total industry excl. construction, SA, 2015=100
    "MEI.PRINTO01.USA.ST.M": "USA.M.PRVM.IX.BTE.Y._Z",
    "MEI.PRINTO01.DEU.ST.M": "DEU.M.PRVM.IX.BTE.Y._Z",
    # Hourly earnings, manufacturing, SA, 2015=100 (old LCEAMN01)
    "MEI.LCEAMN01.USA.ST.M": "USA.M.H_EARN.IX.C.Y._Z",
    # CPI all items, % change on previous period (old CPALTT01 GP)
    "MEI.CPALTT01.USA.GP.M": "USA.M.CP.GR._Z._Z.G1",
    # Harmonised unemployment rate, % of labour force, SA
    "MEI.LRHUTTTT.USA.ST.M": "USA.M.UNEMP.PT_LF._T.Y._Z",
}

_OECD_BASE_URL = "https://sdmx.oecd.org/public/rest/data"
_CLI_FLOW = "OECD.SDD.STES,DSD_STES@DF_CLI,"
_KEI_FLOW = "OECD.SDD.STES,DSD_KEI@DF_KEI,"
#: Seconds between the two calls of a run (OECD allows ~20 req/min).
_RATE_LIMIT_DELAY: float = 5.0
_REQUEST_TIMEOUT: tuple[float, float] = (10.0, 90.0)
_USER_AGENT = "GRID/1.0 (+https://grid.stepdad.finance) oecd-puller"
#: Months fetched before the requested start so the 3-month CLI slope of the
#: first new month can be computed on incremental runs.
_SLOPE_LOOKBACK_MONTHS = 3


@dataclass(frozen=True)
class OECDRow:
    """One observation row from an OECD ``csvfile`` response."""

    key: str          # dotted dimension codes in dataflow order
    obs_date: date
    value: float


def _is_retryable(exc: BaseException) -> bool:
    """Retry transport errors and 429/5xx, never 4xx key errors."""
    if isinstance(exc, requests.HTTPError):
        status = exc.response.status_code if exc.response is not None else 0
        return status == 429 or status >= 500
    return isinstance(exc, (requests.ConnectionError, requests.Timeout))


def _parse_period(period_str: str) -> date | None:
    try:
        s = (period_str or "").strip()
        if len(s) == 7 and "-Q" in s:
            year, q = s.split("-Q")
            return date(int(year), (int(q) - 1) * 3 + 1, 1)
        if len(s) == 7:
            return datetime.strptime(s, "%Y-%m").date()
        if len(s) == 4:
            return date(int(s), 1, 1)
        if len(s) == 10:
            return datetime.strptime(s, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        pass
    return None


def parse_oecd_csv(csv_text: str, key_dims: list[str]) -> list[OECDRow]:
    """Parse an OECD ``format=csvfile`` (or ``csvfilewithlabels``) payload.

    ``key_dims`` lists the dimension columns, in dataflow order, used to build
    the dotted series key. Rows with an empty / non-numeric / non-finite
    ``OBS_VALUE`` are dropped, never coerced to 0. Raises ``ValueError`` when
    the payload is not an OECD data CSV (HTML error page, "NoRecordsFound"
    text) so the caller can report a truthful failure.
    """
    text_in = (csv_text or "").lstrip("﻿")
    if not text_in.strip():
        return []
    reader = csv.DictReader(io.StringIO(text_in))
    fields = set(reader.fieldnames or [])
    missing = [c for c in (*key_dims, "TIME_PERIOD", "OBS_VALUE") if c not in fields]
    if missing:
        raise ValueError(f"OECD CSV missing columns {missing}; got {sorted(fields)[:8]}")
    out: list[OECDRow] = []
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
        obs_dt = _parse_period(row.get("TIME_PERIOD") or "")
        if obs_dt is None:
            continue
        key = ".".join((row.get(d) or "").strip() for d in key_dims)
        out.append(OECDRow(key=key, obs_date=obs_dt, value=value))
    return out


_CLI_KEY_DIMS = [
    "REF_AREA", "FREQ", "MEASURE", "UNIT_MEASURE", "ACTIVITY", "ADJUSTMENT",
    "TRANSFORMATION", "TIME_HORIZ", "METHODOLOGY",
]
_KEI_KEY_DIMS = [
    "REF_AREA", "FREQ", "MEASURE", "UNIT_MEASURE", "ACTIVITY", "ADJUSTMENT",
    "TRANSFORMATION",
]


def _cli_area(country_code: str) -> str:
    return _CLI_AREA_ALIASES.get(country_code, country_code)


def _cli_feature(country_code: str) -> str:
    return f"oecd_cli_{country_code.lower().replace('-', '')}"


def _start_period(start_date: str | date | None, lookback_months: int = 0) -> str | None:
    """``YYYY-MM`` start period, optionally backed off by ``lookback_months``."""
    if not start_date:
        return None
    s = str(start_date)
    try:
        year, month = int(s[:4]), int(s[5:7]) if len(s) >= 7 else 1
    except ValueError:
        return None
    month -= lookback_months
    while month < 1:
        month += 12
        year -= 1
    return f"{year:04d}-{month:02d}"


def cli_url() -> str:
    """One DF_CLI URL covering every tracked country (joined with ``+``)."""
    areas = "+".join(_cli_area(cc) for cc in OECD_CLI_COUNTRIES)
    return f"{_OECD_BASE_URL}/{_CLI_FLOW}/{areas}.M.LI...AA...H"


def kei_url() -> str:
    """One DF_KEI URL covering every mapped MEI series (joined with ``+``)."""
    parts = [k.split(".") for k in OECD_KEI_KEYS.values()]
    areas = "+".join(sorted({p[0] for p in parts}))
    measures = "+".join(sorted({p[2] for p in parts}))
    return f"{_OECD_BASE_URL}/{_KEI_FLOW}/{areas}.M.{measures}...."


class OECDPuller(BasePuller):
    """Pulls OECD CLI and MEI-equivalent (KEI) series from sdmx.oecd.org."""

    SOURCE_NAME = "OECD_SDMX"
    SOURCE_CONFIG = {"base_url": "https://sdmx.oecd.org/public/rest", "cost_tier": "FREE", "latency_class": "MONTHLY", "pit_available": True, "revision_behavior": "RARE", "trust_score": "HIGH", "priority_rank": 13}

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info("OECDPuller initialised — source_id={sid}", sid=self.source_id)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=4, min=4, max=30),
        retry=retry_if_exception(_is_retryable),
        reraise=True,
    )
    def _fetch_csv(self, url: str, start_period: str | None, end_period: str | None) -> str:
        params: dict[str, str] = {"format": "csvfile"}
        if start_period:
            params["startPeriod"] = start_period
        if end_period:
            params["endPeriod"] = end_period
        resp = requests.get(
            url,
            params=params,
            headers={"Accept": "application/vnd.sdmx.data+csv, text/csv", "User-Agent": _USER_AGENT},
            timeout=_REQUEST_TIMEOUT,
        )
        # The API answers 404 "NoRecordsFound" for a valid key with no rows
        # in the window: that is "no new data", not a broken endpoint.
        if resp.status_code == 404 and "NoRecordsFound" in (resp.text or "")[:500]:
            return ""
        resp.raise_for_status()
        return resp.text

    def _insert_rows(self, feature_name: str, observations: list[tuple[date, float]]) -> int:
        inserted = 0
        with self.engine.begin() as conn:
            for obs_date_val, value in observations:
                if self._row_exists(feature_name, obs_date_val, conn):
                    continue
                conn.execute(
                    text(
                        "INSERT INTO raw_series "
                        "(series_id, source_id, obs_date, value, pull_status) "
                        "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                    ),
                    {"sid": feature_name, "src": self.source_id, "od": obs_date_val, "val": value},
                )
                inserted += 1
        return inserted

    def pull_cli_all(
        self,
        start_date: str | date = "1970-01-01",
        end_date: str | date | None = None,
    ) -> list[dict[str, Any]]:
        """Pull the CLI for every tracked country in one request (+ slopes)."""
        results: list[dict[str, Any]] = []
        try:
            payload = self._fetch_csv(
                cli_url(),
                _start_period(start_date, _SLOPE_LOOKBACK_MONTHS),
                _start_period(end_date),
            )
            rows = parse_oecd_csv(payload, _CLI_KEY_DIMS)
        except Exception as exc:
            log_pull_failure("OECD CLI", "batch", exc)
            return [
                {"series_id": _cli_feature(cc), "rows_inserted": 0, "status": "FAILED", "errors": [str(exc)]}
                for cc in OECD_CLI_COUNTRIES
            ]

        by_area: dict[str, list[tuple[date, float]]] = {}
        for r in rows:
            by_area.setdefault(r.key.split(".", 1)[0], []).append((r.obs_date, r.value))

        for cc in OECD_CLI_COUNTRIES:
            feature = _cli_feature(cc)
            res: dict[str, Any] = {"series_id": feature, "rows_inserted": 0, "status": "SUCCESS", "errors": []}
            obs = sorted(by_area.get(_cli_area(cc), []))
            if not obs:
                res["status"] = "PARTIAL"
                res["errors"].append("No data returned")
                results.append(res)
                continue
            try:
                res["rows_inserted"] = self._insert_rows(feature, obs)
                if len(obs) >= 3:
                    res["rows_inserted"] += self._compute_cli_slope(feature, obs)
                log.info("OECD CLI {cc}: inserted {n} rows", cc=cc, n=res["rows_inserted"])
            except Exception as exc:
                log_pull_failure("OECD CLI", cc, exc)
                res["status"] = "FAILED"
                res["errors"].append(str(exc))
            results.append(res)
        return results

    def pull_cli(
        self,
        country_code: str,
        start_date: str | date = "1970-01-01",
        end_date: str | date | None = None,
    ) -> dict[str, Any]:
        """Pull the CLI for one country (uses the batched request)."""
        feature = _cli_feature(country_code)
        if country_code not in OECD_CLI_COUNTRIES:
            reason = OECD_CLI_RETIRED.get(country_code, "not a tracked CLI country")
            return {"series_id": feature, "rows_inserted": 0, "status": "SKIPPED", "errors": [reason]}
        for res in self.pull_cli_all(start_date, end_date):
            if res["series_id"] == feature:
                return res
        return {"series_id": feature, "rows_inserted": 0, "status": "FAILED", "errors": ["missing result"]}

    def _compute_cli_slope(self, base_feature: str, observations: list[tuple[date, float]]) -> int:
        """Compute 3-month slope of CLI and insert as derived series."""
        slope_feature = f"{base_feature}_slope"
        sorted_obs = sorted(observations, key=lambda x: x[0])
        values = [v for _, v in sorted_obs]
        dates = [d for d, _ in sorted_obs]

        inserted = 0
        with self.engine.begin() as conn:
            for i in range(2, len(values)):
                slope = (values[i] - values[i - 2]) / 2.0
                obs_dt = dates[i]
                if self._row_exists(slope_feature, obs_dt, conn):
                    continue
                conn.execute(
                    text(
                        "INSERT INTO raw_series "
                        "(series_id, source_id, obs_date, value, pull_status) "
                        "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                    ),
                    {"sid": slope_feature, "src": self.source_id, "od": obs_dt, "val": slope},
                )
                inserted += 1
        return inserted

    def pull_mei_all(
        self,
        start_date: str | date = "1970-01-01",
        end_date: str | date | None = None,
    ) -> list[dict[str, Any]]:
        """Pull every mapped MEI series from DF_KEI in one request."""
        try:
            payload = self._fetch_csv(kei_url(), _start_period(start_date), _start_period(end_date))
            rows = parse_oecd_csv(payload, _KEI_KEY_DIMS)
        except Exception as exc:
            log_pull_failure("OECD MEI", "batch", exc)
            return [
                {"series_id": OECD_MEI_SERIES[k], "rows_inserted": 0, "status": "FAILED", "errors": [str(exc)]}
                for k in OECD_MEI_SERIES
            ]

        by_key: dict[str, list[tuple[date, float]]] = {}
        for r in rows:
            by_key.setdefault(r.key, []).append((r.obs_date, r.value))

        results: list[dict[str, Any]] = []
        for legacy_key, feature in OECD_MEI_SERIES.items():
            res: dict[str, Any] = {"series_id": feature, "rows_inserted": 0, "status": "SUCCESS", "errors": []}
            obs = sorted(by_key.get(OECD_KEI_KEYS[legacy_key], []))
            if not obs:
                res["status"] = "PARTIAL"
                res["errors"].append("No data returned")
                results.append(res)
                continue
            try:
                res["rows_inserted"] = self._insert_rows(feature, obs)
                log.info("OECD MEI {fn}: inserted {n} rows", fn=feature, n=res["rows_inserted"])
            except Exception as exc:
                log_pull_failure("OECD MEI", legacy_key, exc)
                res["status"] = "FAILED"
                res["errors"].append(str(exc))
            results.append(res)
        return results

    def pull_mei(
        self,
        series_key: str,
        start_date: str | date = "1970-01-01",
        end_date: str | date | None = None,
    ) -> dict[str, Any]:
        """Pull a single legacy-keyed MEI series (uses the batched request)."""
        feature = OECD_MEI_SERIES.get(series_key, series_key)
        if series_key not in OECD_KEI_KEYS:
            reason = OECD_MEI_RETIRED.get(series_key, "no DF_KEI mapping")
            return {"series_id": feature, "rows_inserted": 0, "status": "SKIPPED", "errors": [reason]}
        for res in self.pull_mei_all(start_date, end_date):
            if res["series_id"] == feature:
                return res
        return {"series_id": feature, "rows_inserted": 0, "status": "FAILED", "errors": ["missing result"]}

    def pull_all(self, start_date: str | date = "1970-01-01") -> dict[str, Any]:
        """Pull all OECD CLI countries and MEI series (two API requests)."""
        log.info("Starting OECD bulk pull from {sd}", sd=start_date)
        results: list[dict[str, Any]] = list(self.pull_cli_all(start_date))
        time.sleep(_RATE_LIMIT_DELAY)
        results.extend(self.pull_mei_all(start_date))

        total_rows = sum(r["rows_inserted"] for r in results)
        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        log.info(
            "OECD bulk pull complete — {ok}/{total} succeeded, {rows} rows",
            ok=succeeded, total=len(results), rows=total_rows,
        )
        return {
            "source": "OECD_SDMX",
            "total_rows": total_rows,
            "succeeded": succeeded,
            "total": len(results),
            "results": results,
        }
