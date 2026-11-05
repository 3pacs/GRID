"""
GRID IMF IFS and WEO ingestion module.

WEO indicators come from the IMF DataMapper JSON API (annual actuals and
projections; future years are not stored). The former IFS series come from
the IMF SDMX 3.0 data API (api.imf.org), which replaced IFS with topical
dataflows: quarterly national accounts (QNEA) and balance of payments (BOP).

2026-11-04: the imfdatapy client this module used targeted the retired
dataservices.imf.org host (NXDOMAIN), so every IFS pull was SKIPPED with a
warning (10 lines/day), and the library also wrote a log file into the
working directory on every call. imfdatapy is no longer imported.
"""

from __future__ import annotations

import time
from datetime import date
from typing import Any

import pandas as pd
import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from ingestion.base import BasePuller
from ingestion.international import sdmx_json

# Former IFS targets -> IMF SDMX 3.0 (flow, key). Keys verified by probe
# from grid-svr 2026-11-04:
#   QNEA COUNTRY.INDICATOR.PRICE_TYPE.S_ADJUSTMENT.TYPE_OF_TRANSFORMATION.FREQUENCY
#        B1GQ = GDP, Q = chain-linked volume (real), SA/NSA, XDC = national currency
#   BOP  COUNTRY.BOP_ACCOUNTING_ENTRY.INDICATOR.UNIT.FREQUENCY
#        NETCD_T = net (credit - debit), CAB = current account balance, USD
# China publishes no seasonally adjusted real GDP level to the IMF, so its
# series is NSA (compare it year over year, not quarter over quarter).
IMF_IFS_SERIES: dict[str, tuple[str, str]] = {
    "us_gdp_real_imf": ("QNEA", "USA.B1GQ.Q.SA.XDC.Q"),
    "china_gdp_real_imf": ("QNEA", "CHN.B1GQ.Q.NSA.XDC.Q"),
    "germany_gdp_real_imf": ("QNEA", "DEU.B1GQ.Q.SA.XDC.Q"),
    "us_current_account_imf": ("BOP", "USA.NETCD_T.CAB.USD.Q"),
    "china_current_account_imf": ("BOP", "CHN.NETCD_T.CAB.USD.Q"),
}

# WEO extraction targets: (subject_code, country) -> feature name
IMF_WEO_TARGETS: dict[tuple[str, str], str] = {
    ("NGDP_RPCH", "US"): "weo_gdp_growth_us",
    ("NGDP_RPCH", "CN"): "weo_gdp_growth_cn",
    ("NGDP_RPCH", "DE"): "weo_gdp_growth_de",
    ("NGDP_RPCH", "JP"): "weo_gdp_growth_jp",
    ("NGDP_RPCH", "GB"): "weo_gdp_growth_gb",
    ("PCPIPCH", "US"): "weo_inflation_us",
    ("PCPIPCH", "CN"): "weo_inflation_cn",
    ("BCA_NGDPD", "US"): "weo_current_account_us",
    ("GGXCNL_NGDP", "US"): "weo_fiscal_balance_us",
}

_RATE_LIMIT_DELAY: float = 3.0

# Transport-level failures of an IMF host are an upstream outage (warning +
# SKIPPED), not an application error.
_UPSTREAM_OUTAGE_MARKERS: tuple[str, ...] = (
    "dataservices.imf.org",
    "Read timed out",
    "ConnectTimeout",
    "NameResolutionError",
    "Failed to resolve",
    "Max retries exceeded",
    "Connection refused",
)


def is_upstream_outage(exc: BaseException) -> bool:
    """True when *exc* describes the IMF host being unreachable or retired."""
    msg = str(exc)
    return any(marker in msg for marker in _UPSTREAM_OUTAGE_MARKERS)


# IMF DataMapper — the public JSON API behind imf.org/external/datamapper.
# It serves the WEO indicators directly (annual, actuals + projections) and
# replaces the imfdatapy WEO class that disappeared before 2026-05.
DATAMAPPER_BASE_URL: str = "https://www.imf.org/external/datamapper/api/v1"
_DATAMAPPER_TIMEOUT: int = 30

# WEO targets are keyed by ISO-2; DataMapper speaks ISO-3.
ISO2_TO_ISO3: dict[str, str] = {
    "US": "USA", "CN": "CHN", "DE": "DEU", "JP": "JPN", "GB": "GBR",
    "FR": "FRA", "IT": "ITA", "CA": "CAN", "IN": "IND", "BR": "BRA",
    "KR": "KOR", "MX": "MEX", "AU": "AUS", "ES": "ESP", "RU": "RUS",
}


def fetch_datamapper(
    indicator: str,
    iso3_codes: list[str],
    timeout: int = _DATAMAPPER_TIMEOUT,
) -> dict[str, dict[int, float]]:
    """Return ``{iso3: {year: value}}`` for *indicator* from IMF DataMapper.

    Raises ``requests.HTTPError`` on a non-2xx response so callers can decide
    between skip and fail.
    """
    if not iso3_codes:
        return {}
    url = f"{DATAMAPPER_BASE_URL}/{indicator}/{'/'.join(iso3_codes)}"
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    payload = resp.json() if resp.content else {}
    values = ((payload or {}).get("values") or {}).get(indicator) or {}
    out: dict[str, dict[int, float]] = {}
    for code in iso3_codes:
        series: dict[int, float] = {}
        for year_str, raw in (values.get(code) or {}).items():
            try:
                year = int(year_str)
                value = float(raw)
            except (TypeError, ValueError):
                continue
            if pd.isna(value):
                continue
            series[year] = value
        out[code] = series
    return out


class IMFPuller(BasePuller):
    """Pulls macroeconomic data from IMF IFS and WEO datasets."""

    SOURCE_NAME = "IMF_IFS"
    SOURCE_CONFIG = {"base_url": "https://www.imf.org/external/datamapper/api/v1", "cost_tier": "FREE", "latency_class": "MONTHLY", "pit_available": True, "revision_behavior": "RARE", "trust_score": "HIGH", "priority_rank": 11}

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info("IMFPuller initialised — source_id={sid}", sid=self.source_id)

    def pull_ifs(
        self,
        feature_name: str,
        start: str = "2000",
        end: str | None = None,
    ) -> dict[str, Any]:
        """Pull one former-IFS series from the IMF SDMX 3.0 API."""
        flow, key = IMF_IFS_SERIES[feature_name]
        log.info("Pulling IMF {fl} {k} ({fn})", fl=flow, k=key, fn=feature_name)

        result: dict[str, Any] = {
            "series_id": feature_name,
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        try:
            observations = sdmx_json.fetch_imf_series(flow, key, start)
            if end:
                end_dt = self._parse_period(str(end)[:7]) or date.max
                observations = [(d, v) for d, v in observations if d <= end_dt]

            if not observations:
                result["status"] = "PARTIAL"
                result["errors"].append(f"IMF {flow}/{key} returned no observations")
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
            log.info("IMF {fn}: inserted {n} rows", fn=feature_name, n=inserted)

        except Exception as exc:
            if is_upstream_outage(exc) or isinstance(exc, (requests.RequestException, ValueError)):
                log.warning(
                    "IMF {fn} ({fl}/{k}) unavailable: {err}",
                    fn=feature_name, fl=flow, k=key, err=str(exc)[:200],
                )
                result["status"] = "SKIPPED" if is_upstream_outage(exc) else "FAILED"
            else:
                log.error("IMF pull failed for {fn}: {err}", fn=feature_name, err=str(exc))
                result["status"] = "FAILED"
            result["errors"].append(str(exc))

        time.sleep(_RATE_LIMIT_DELAY)
        return result

    @staticmethod
    def _parse_period(period_str: str) -> date | None:
        return sdmx_json.parse_period(period_str)

    def pull_weo(self, max_year: int | None = None) -> dict[str, Any]:
        """Pull the WEO targets from the IMF DataMapper API.

        DataMapper returns actuals and projections in one series. Only years
        up to *max_year* (default: the current year) are stored — a
        projection for a future year would carry a future ``obs_date`` and
        break point-in-time reads.
        """
        log.info("Pulling IMF WEO data (DataMapper)")
        result: dict[str, Any] = {
            "series_id": "weo_all",
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
            "skipped_future": 0,
        }
        cutoff_year = max_year if max_year is not None else date.today().year

        # Group targets by indicator so each indicator is one HTTP call.
        by_indicator: dict[str, list[tuple[str, str]]] = {}
        for (subject, country), feature_name in IMF_WEO_TARGETS.items():
            by_indicator.setdefault(subject, []).append((country, feature_name))

        inserted = 0
        try:
            with self.engine.begin() as conn:
                for indicator, targets in by_indicator.items():
                    codes = [ISO2_TO_ISO3.get(c, c) for c, _ in targets]
                    try:
                        data = fetch_datamapper(indicator, codes)
                    except Exception as exc:  # noqa: BLE001 — one indicator must not sink the rest
                        level = log.warning if is_upstream_outage(exc) else log.error
                        level("IMF WEO {ind} fetch failed: {err}", ind=indicator, err=str(exc)[:200])
                        result["errors"].append(f"{indicator}: {exc}")
                        result["status"] = "PARTIAL"
                        continue

                    for country, feature_name in targets:
                        series = data.get(ISO2_TO_ISO3.get(country, country)) or {}
                        if not series:
                            log.debug("IMF WEO {fn}: no values returned", fn=feature_name)
                            continue
                        existing = self._get_existing_dates(feature_name, conn)
                        for year, value in sorted(series.items()):
                            if year > cutoff_year:
                                result["skipped_future"] += 1
                                continue
                            obs_dt = date(year, 1, 1)
                            if obs_dt in existing:
                                continue
                            conn.execute(
                                text(
                                    "INSERT INTO raw_series "
                                    "(series_id, source_id, obs_date, value, pull_status) "
                                    "VALUES (:sid, :src, :od, :val, 'SUCCESS') "
                                    "ON CONFLICT DO NOTHING"
                                ),
                                {"sid": feature_name, "src": self.source_id, "od": obs_dt, "val": value},
                            )
                            existing.add(obs_dt)
                            inserted += 1

            result["rows_inserted"] = inserted
            log.info(
                "IMF WEO: inserted {n} rows ({f} future-year projections skipped)",
                n=inserted, f=result["skipped_future"],
            )
        except Exception as exc:
            log.error("IMF WEO pull failed: {err}", err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        return result

    def pull_all(self, start_date: str | date = "2000-01-01") -> dict[str, Any]:
        """Pull all IMF IFS and WEO data."""
        log.info("Starting IMF bulk pull from {sd}", sd=start_date)
        results: list[dict[str, Any]] = []

        start_year = sdmx_json.lookback_start_year(start_date) or "2000"
        for feature_name in IMF_IFS_SERIES:
            results.append(self.pull_ifs(feature_name, start=start_year))

        weo_result = self.pull_weo()
        results.append(weo_result)

        total_rows = sum(r["rows_inserted"] for r in results)
        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        log.info(
            "IMF bulk pull complete — {ok}/{total} succeeded, {rows} rows",
            ok=succeeded, total=len(results), rows=total_rows,
        )
        return {
            "source": "IMF_IFS",
            "total_rows": total_rows,
            "succeeded": succeeded,
            "total": len(results),
        }
