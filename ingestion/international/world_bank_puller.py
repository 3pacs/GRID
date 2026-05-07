"""
GRID World Bank Open Data ingestion module.

Pulls macroeconomic indicators for 30 key economies from the World Bank API
(https://api.worldbank.org/v2/). No API key required.

Indicators cover GDP, inflation, unemployment, trade, reserves, debt, FDI,
and exchange rates — the core macro picture for every major market.
"""

from __future__ import annotations

import json
import time
from datetime import date
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_WB_BASE_URL = "https://api.worldbank.org/v2"
_RATE_LIMIT_DELAY: float = 0.5  # Be nice to the free API
_PER_PAGE: int = 500
_DEFAULT_YEARS: int = 10

# World Bank indicator codes -> human-readable labels
INDICATORS: dict[str, str] = {
    "NY.GDP.MKTP.CD": "gdp_current_usd",
    "NY.GDP.MKTP.KD.ZG": "gdp_growth_pct",
    "FP.CPI.TOTL.ZG": "inflation_cpi_pct",
    "SL.UEM.TOTL.ZS": "unemployment_pct",
    "BN.CAB.XOKA.CD": "current_account_bal",
    "FI.RES.TOTL.CD": "total_reserves_incl_gold",
    "GC.DOD.TOTL.GD.ZS": "govt_debt_pct_gdp",
    "BX.KLT.DINV.CD.WD": "fdi_net_inflows",
    "NE.EXP.GNFS.ZS": "exports_pct_gdp",
    "NE.IMP.GNFS.ZS": "imports_pct_gdp",
    "PA.NUS.FCRF": "exchange_rate_lcu_per_usd",
}

# ISO2 country codes for the 30 most market-relevant economies
COUNTRIES: dict[str, str] = {
    "US": "United States",
    "CN": "China",
    "JP": "Japan",
    "DE": "Germany",
    "GB": "United Kingdom",
    "FR": "France",
    "IN": "India",
    "IT": "Italy",
    "BR": "Brazil",
    "CA": "Canada",
    "KR": "South Korea",
    "AU": "Australia",
    "ES": "Spain",
    "MX": "Mexico",
    "ID": "Indonesia",
    "NL": "Netherlands",
    "SA": "Saudi Arabia",
    "TR": "Turkey",
    "CH": "Switzerland",
    "TW": "Taiwan",
    "PL": "Poland",
    "TH": "Thailand",
    "SE": "Sweden",
    "BE": "Belgium",
    "AR": "Argentina",
    "NG": "Nigeria",
    "ZA": "South Africa",
    "EG": "Egypt",
    "VN": "Vietnam",
    "PH": "Philippines",
}


class WorldBankPuller(BasePuller):
    """Pulls macroeconomic indicators from the World Bank Open Data API.

    Series IDs follow the pattern ``wb:{country}:{indicator_code}``.

    Attributes:
        engine: SQLAlchemy engine for database writes.
        source_id: Resolved source_catalog.id for world_bank.
    """

    SOURCE_NAME = "world_bank"
    SOURCE_CONFIG = {
        "base_url": _WB_BASE_URL,
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": False,
        "revision_behavior": "FREQUENT",
        "trust_score": "HIGH",
        "priority_rank": 15,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info("WorldBankPuller initialised — source_id={sid}", sid=self.source_id)

    # ------------------------------------------------------------------
    # API fetch
    # ------------------------------------------------------------------

    @retry_on_failure(
        max_attempts=3,
        backoff=2.0,
        retryable_exceptions=(
            ConnectionError,
            TimeoutError,
            OSError,
            requests.exceptions.RequestException,
        ),
    )
    def _fetch_indicator(
        self,
        country: str,
        indicator: str,
        start_year: int,
        end_year: int,
    ) -> list[dict[str, Any]]:
        """Fetch paginated indicator data from the World Bank API.

        Parameters:
            country: ISO2 country code.
            indicator: World Bank indicator code.
            start_year: First year to request.
            end_year: Last year to request.

        Returns:
            List of observation dicts with keys: date, value, country_id, indicator_id.
        """
        all_records: list[dict[str, Any]] = []
        page = 1

        while True:
            url = f"{_WB_BASE_URL}/country/{country}/indicator/{indicator}"
            params = {
                "format": "json",
                "per_page": _PER_PAGE,
                "page": page,
                "date": f"{start_year}:{end_year}",
            }

            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            payload = resp.json()

            # World Bank returns [metadata, data] or [metadata, None]
            if not isinstance(payload, list) or len(payload) < 2:
                log.warning(
                    "Unexpected WB response for {c}/{i}: {t}",
                    c=country, i=indicator, t=type(payload).__name__,
                )
                break

            meta, data = payload[0], payload[1]
            if data is None:
                break

            all_records.extend(data)

            # Pagination: check if there are more pages
            total_pages = int(meta.get("pages", 1))
            if page >= total_pages:
                break
            page += 1
            time.sleep(_RATE_LIMIT_DELAY)

        return all_records

    # ------------------------------------------------------------------
    # Single indicator pull
    # ------------------------------------------------------------------

    def pull_indicator(
        self,
        country: str,
        indicator: str,
        years: int = _DEFAULT_YEARS,
    ) -> dict[str, Any]:
        """Pull one indicator for one country and insert into raw_series.

        Parameters:
            country: ISO2 country code.
            indicator: World Bank indicator code.
            years: Number of years to look back.

        Returns:
            Result dict with series_id, rows_inserted, status, errors.
        """
        series_id = f"wb:{country.lower()}:{indicator}"
        label = INDICATORS.get(indicator, indicator)
        log.info(
            "Pulling WB {label} for {c} (series={sid})",
            label=label, c=country, sid=series_id,
        )

        result: dict[str, Any] = {
            "series_id": series_id,
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        end_year = date.today().year
        start_year = end_year - years

        try:
            records = self._fetch_indicator(country, indicator, start_year, end_year)

            if not records:
                result["status"] = "PARTIAL"
                result["errors"].append("No data returned")
                return result

            inserted = 0
            with self.engine.begin() as conn:
                existing = self._get_existing_dates(series_id, conn)

                for rec in records:
                    value = rec.get("value")
                    if value is None:
                        continue

                    try:
                        obs_year = int(rec.get("date", "0"))
                        obs_dt = date(obs_year, 1, 1)
                    except (ValueError, TypeError):
                        continue

                    if obs_dt in existing:
                        continue

                    try:
                        float_val = float(value)
                    except (ValueError, TypeError):
                        log.warning(
                            "WB non-numeric value for {sid} @ {d}: {v}",
                            sid=series_id, d=obs_dt, v=value,
                        )
                        continue

                    raw_payload = {
                        "country_id": rec.get("country", {}).get("id"),
                        "country_name": rec.get("country", {}).get("value"),
                        "indicator_id": rec.get("indicator", {}).get("id"),
                        "indicator_name": rec.get("indicator", {}).get("value"),
                    }

                    self._insert_raw(
                        conn,
                        series_id=series_id,
                        obs_date=obs_dt,
                        value=float_val,
                        raw_payload=raw_payload,
                    )
                    existing.add(obs_dt)
                    inserted += 1

            result["rows_inserted"] = inserted
            log.info("WB {sid}: inserted {n} rows", sid=series_id, n=inserted)

        except Exception as exc:
            log.error("WB pull failed for {sid}: {err}", sid=series_id, err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))
            self._record_failure(series_id, exc)

        time.sleep(_RATE_LIMIT_DELAY)
        return result

    # ------------------------------------------------------------------
    # Anomaly detection
    # ------------------------------------------------------------------

    def detect_gdp_anomalies(self) -> list[dict[str, Any]]:
        """Flag countries where GDP growth dropped >5 percentage points YoY.

        Reads stored ``wb:{country}:NY.GDP.MKTP.KD.ZG`` series and compares
        consecutive years.

        Returns:
            List of anomaly dicts: country, year, prev_growth, curr_growth, delta.
        """
        anomalies: list[dict[str, Any]] = []

        for code in COUNTRIES:
            series_id = f"wb:{code.lower()}:NY.GDP.MKTP.KD.ZG"
            try:
                with self.engine.connect() as conn:
                    rows = conn.execute(
                        text(
                            "SELECT obs_date, value FROM raw_series "
                            "WHERE series_id = :sid AND source_id = :src "
                            "AND pull_status = 'SUCCESS' "
                            "ORDER BY obs_date"
                        ),
                        {"sid": series_id, "src": self.source_id},
                    ).fetchall()

                if len(rows) < 2:
                    continue

                for i in range(1, len(rows)):
                    prev_val = float(rows[i - 1][1])
                    curr_val = float(rows[i][1])
                    delta = curr_val - prev_val

                    if delta < -5.0:
                        anomaly = {
                            "country": code,
                            "country_name": COUNTRIES[code],
                            "year": rows[i][0].year if hasattr(rows[i][0], "year") else rows[i][0],
                            "prev_growth": round(prev_val, 2),
                            "curr_growth": round(curr_val, 2),
                            "delta_pp": round(delta, 2),
                            "series_id": series_id,
                        }
                        anomalies.append(anomaly)
                        log.warning(
                            "GDP ANOMALY: {c} ({cn}) {y} — growth dropped {d:.1f}pp "
                            "({pv:.1f}% -> {cv:.1f}%)",
                            c=code,
                            cn=COUNTRIES[code],
                            y=anomaly["year"],
                            d=delta,
                            pv=prev_val,
                            cv=curr_val,
                        )

            except Exception as exc:
                log.debug("Anomaly check skipped for {c}: {e}", c=code, e=str(exc))

        log.info("GDP anomaly scan: {n} anomalies found", n=len(anomalies))
        return anomalies

    # ------------------------------------------------------------------
    # Actor ingestion
    # ------------------------------------------------------------------

    def _ingest_country_actors(self) -> int:
        """Ingest all tracked countries as government actors.

        Returns:
            Number of new actors added.
        """
        try:
            from intelligence.actor_ingest import ingest_actor
        except ImportError:
            log.debug("actor_ingest not available — skipping actor ingestion")
            return 0

        added = 0
        for code, name in COUNTRIES.items():
            if ingest_actor(
                self.engine,
                name=name,
                actor_type="government",
                source="world_bank",
                country=code,
                confidence="confirmed",
                metadata={
                    "iso2": code,
                    "data_source": "World Bank Open Data",
                },
            ):
                added += 1

        if added > 0:
            log.info("WorldBank: ingested {n} new country actors", n=added)
        return added

    # ------------------------------------------------------------------
    # Bulk pull
    # ------------------------------------------------------------------

    def pull_all(
        self,
        start_date: str | date = "2016-01-01",
        years: int = _DEFAULT_YEARS,
    ) -> dict[str, Any]:
        """Pull all indicators for all countries.

        Parameters:
            start_date: Not used directly (years param controls range), kept for API compat.
            years: Number of years to look back from today.

        Returns:
            Summary dict with total rows and per-series status.
        """
        log.info(
            "Starting World Bank bulk pull — {ni} indicators x {nc} countries ({y} years)",
            ni=len(INDICATORS), nc=len(COUNTRIES), y=years,
        )

        # Ingest country actors first
        self._ingest_country_actors()

        results: list[dict[str, Any]] = []
        for indicator in INDICATORS:
            for country in COUNTRIES:
                res = self.pull_indicator(country, indicator, years=years)
                results.append(res)

        # Run anomaly detection after pulling GDP growth data
        anomalies = self.detect_gdp_anomalies()

        total_rows = sum(r["rows_inserted"] for r in results)
        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        failed = sum(1 for r in results if r["status"] == "FAILED")

        log.info(
            "World Bank bulk pull complete — {ok}/{total} succeeded, "
            "{fail} failed, {rows} rows inserted, {anom} GDP anomalies",
            ok=succeeded,
            total=len(results),
            fail=failed,
            rows=total_rows,
            anom=len(anomalies),
        )

        return {
            "source": "world_bank",
            "total_rows": total_rows,
            "succeeded": succeeded,
            "failed": failed,
            "total": len(results),
            "anomalies": anomalies,
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _record_failure(self, series_id: str, exc: Exception) -> None:
        """Record a failed pull attempt in raw_series."""
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        "INSERT INTO raw_series "
                        "(series_id, source_id, obs_date, value, raw_payload, pull_status) "
                        "VALUES (:sid, :src, :od, 0, :payload, 'FAILED')"
                    ),
                    {
                        "sid": series_id,
                        "src": self.source_id,
                        "od": date.today(),
                        "payload": json.dumps({"error": str(exc)}),
                    },
                )
        except Exception as insert_exc:
            log.error("Failed to record error row: {err}", err=str(insert_exc))
