"""
GRID IMF IFS and WEO ingestion module.

Pulls macroeconomic data from the IMF International Financial Statistics (IFS)
and World Economic Outlook (WEO) datasets via the imfdatapy library.
"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# IMF IFS series: (search_terms, period, country) -> feature name
IMF_IFS_SERIES: dict[tuple[str, str, str], str] = {
    ("gross domestic product, real", "Q", "US"): "us_gdp_real_imf",
    ("gross domestic product, real", "Q", "CN"): "china_gdp_real_imf",
    ("gross domestic product, real", "Q", "DE"): "germany_gdp_real_imf",
    ("current account, total", "Q", "US"): "us_current_account_imf",
    ("current account, total", "Q", "CN"): "china_current_account_imf",
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


class IMFPuller:
    """Pulls macroeconomic data from IMF IFS and WEO datasets."""

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine
        self.source_id = self._resolve_source_id()
        log.info("IMFPuller initialised — source_id={sid}", sid=self.source_id)

    def _resolve_source_id(self) -> int:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM source_catalog WHERE name = :name"),
                {"name": "IMF_IFS"},
            ).fetchone()
        if row is None:
            with self.engine.begin() as conn:
                result = conn.execute(
                    text(
                        "INSERT INTO source_catalog "
                        "(name, base_url, license_type, update_frequency, "
                        "has_vintage_data, revision_policy, data_quality, priority, model_eligible) "
                        "VALUES (:name, :url, 'FREE', 'MONTHLY', FALSE, 'RARE', 'HIGH', 14, TRUE) "
                        "RETURNING id"
                    ),
                    {"name": "IMF_IFS", "url": "https://www.imf.org/external/datamapper"},
                )
                return result.fetchone()[0]
        return row[0]

    def _row_exists(self, series_id: str, obs_date: date, conn: Any) -> bool:
        one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
        result = conn.execute(
            text(
                "SELECT 1 FROM raw_series "
                "WHERE series_id = :sid AND source_id = :src "
                "AND obs_date = :od AND pull_timestamp >= :ts LIMIT 1"
            ),
            {"sid": series_id, "src": self.source_id, "od": obs_date, "ts": one_hour_ago},
        ).fetchone()
        return result is not None

    def pull_ifs(
        self,
        search_terms: str,
        period: str,
        country: str,
        start: str = "2000",
        end: str | None = None,
    ) -> dict[str, Any]:
        """Pull a single IFS series using imfdatapy."""
        key = (search_terms, period, country)
        feature_name = IMF_IFS_SERIES.get(key, f"imf_ifs_{country.lower()}")
        log.info("Pulling IMF IFS: {fn} ({ct})", fn=feature_name, ct=country)

        result: dict[str, Any] = {
            "series_id": feature_name,
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        try:
            from imfdatapy.imf import IFS

            ifs = IFS(search_terms=search_terms, period=period, countries=[country])
            df = ifs.download_data()

            if df is None or df.empty:
                result["status"] = "PARTIAL"
                result["errors"].append("No data returned from IFS")
                return result

            inserted = 0
            with self.engine.begin() as conn:
                for idx, row in df.iterrows():
                    try:
                        # imfdatapy returns period as index or column
                        if isinstance(idx, str):
                            obs_dt = self._parse_period(idx)
                        elif hasattr(idx, "date"):
                            obs_dt = idx.date() if callable(idx.date) else idx.date
                        else:
                            obs_dt = pd.Timestamp(str(idx)).date()

                        if obs_dt is None:
                            continue

                        # Get the first numeric column
                        value = None
                        for col in df.columns:
                            try:
                                value = float(row[col])
                                break
                            except (ValueError, TypeError):
                                continue

                        if value is None or pd.isna(value):
                            continue

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
                    except Exception as row_exc:
                        log.debug("Skipping IFS row: {err}", err=str(row_exc))
                        continue

            result["rows_inserted"] = inserted
            log.info("IMF IFS {fn}: inserted {n} rows", fn=feature_name, n=inserted)

        except Exception as exc:
            log.error("IMF IFS pull failed: {err}", err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        time.sleep(_RATE_LIMIT_DELAY)
        return result

    @staticmethod
    def _parse_period(period_str: str) -> date | None:
        try:
            if "Q" in period_str:
                year, q = period_str.split("Q")
                return date(int(year), (int(q) - 1) * 3 + 1, 1)
            elif len(period_str) == 7:
                return datetime.strptime(period_str, "%Y-%m").date()
            elif len(period_str) == 4:
                return date(int(period_str), 1, 1)
        except (ValueError, TypeError):
            pass
        return None

    def pull_weo(self) -> dict[str, Any]:
        """Download and parse latest WEO dataset."""
        log.info("Pulling IMF WEO data")
        result: dict[str, Any] = {
            "series_id": "weo_all",
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        try:
            from imfdatapy.imf import WEO

            weo = WEO()
            df = weo.download_data()

            if df is None or df.empty:
                result["status"] = "PARTIAL"
                return result

            inserted = 0
            with self.engine.begin() as conn:
                for (subject, country), feature_name in IMF_WEO_TARGETS.items():
                    try:
                        # Filter WEO data for this subject and country
                        mask = df.index.str.contains(country, case=False)
                        subset = df[mask]
                        if subset.empty:
                            continue

                        for col in subset.columns:
                            try:
                                year = int(col)
                                obs_dt = date(year, 1, 1)
                                value = float(subset.iloc[0][col])
                                if pd.isna(value):
                                    continue
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
                            except (ValueError, TypeError):
                                continue
                    except Exception as series_exc:
                        log.warning("WEO series {fn} failed: {err}", fn=feature_name, err=str(series_exc))

            result["rows_inserted"] = inserted
            log.info("IMF WEO: inserted {n} rows", n=inserted)

        except Exception as exc:
            log.error("IMF WEO pull failed: {err}", err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        return result

    def pull_all(self, start_date: str | date = "2000-01-01") -> dict[str, Any]:
        """Pull all IMF IFS and WEO data."""
        log.info("Starting IMF bulk pull from {sd}", sd=start_date)
        results: list[dict[str, Any]] = []

        start_year = str(start_date)[:4]
        for (terms, period, country), _fn in IMF_IFS_SERIES.items():
            res = self.pull_ifs(terms, period, country, start=start_year)
            results.append(res)

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
