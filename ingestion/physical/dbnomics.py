"""
GRID DBnomics aggregated central bank data ingestion module.

Pulls time series from 100+ providers via the unified DBnomics API.
Acts as a fallback and supplementary source for international data.
"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# DBnomics series: provider/dataset/series -> feature name
DBNOMICS_SERIES: dict[str, str] = {
    "ECB/IRS/M.IT.L.L40.CI.0000.EUR.N.Z": "italy_10y_ecb_dbnomics",
    "BDF/FM/M.FR.EUR.FR.BB.GVT.YLD.10Y": "france_10y_yield",
    "OECD/KEI/LORSGPNO.DEU.ST.M": "germany_cli_dbnomics",
    "IMF/IFS/M.US.FIMM_PA.USD": "us_mmkt_rate_imf",
    "BIS/total_credit/Q.5J.P.N.I.B.770": "us_credit_gdp_bis",
    "Eurostat/prc_hicp_manr/M.RCH_A.CP00.EA": "eurozone_hicp_yoy",
    "Eurostat/une_rt_m/M.T.NSA.PC_ACT.T.EA19": "eurozone_unemployment",
    "Eurostat/sts_inpr_m/M.I15.NS0.B.EU27_2020": "eu_industrial_output",
    "BDF/BP/M.FR.1.B.S121.A21.T.Z.EUR.Z": "france_current_account",
    "Bundesbank/BBDP1/M.DE.N.VPI.C.A00000.I15.R": "germany_cpi_dbnomics",
}

_RATE_LIMIT_DELAY: float = 2.0


class DBnomicsPuller:
    """Pulls time series data from DBnomics unified API."""

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine
        self.source_id = self._resolve_source_id()
        log.info("DBnomicsPuller initialised — source_id={sid}", sid=self.source_id)

    def _resolve_source_id(self) -> int:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM source_catalog WHERE name = :name"),
                {"name": "DBnomics"},
            ).fetchone()
        if row is None:
            with self.engine.begin() as conn:
                result = conn.execute(
                    text(
                        "INSERT INTO source_catalog "
                        "(name, base_url, license_type, update_frequency, "
                        "has_vintage_data, revision_policy, data_quality, priority, model_eligible) "
                        "VALUES (:name, :url, 'FREE', 'VARIES', FALSE, 'RARE', 'MED', 30, TRUE) "
                        "RETURNING id"
                    ),
                    {"name": "DBnomics", "url": "https://db.nomics.world/api/v22"},
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

    def pull_series(
        self,
        dbnomics_id: str,
        feature_name: str,
        start_date: str | date = "1990-01-01",
        end_date: str | date | None = None,
    ) -> dict[str, Any]:
        """Pull a single DBnomics series.

        Parameters:
            dbnomics_id: DBnomics series ID (provider/dataset/series).
            feature_name: Canonical feature name for storage.
            start_date: Earliest observation date.
            end_date: Latest observation date.
        """
        log.info("Pulling DBnomics {fn} ({did})", fn=feature_name, did=dbnomics_id)
        result: dict[str, Any] = {
            "series_id": feature_name,
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        try:
            import dbnomics as db

            # Parse the dbnomics_id into components
            parts = dbnomics_id.split("/")
            if len(parts) >= 3:
                provider_code = parts[0]
                dataset_code = parts[1]
                series_code = "/".join(parts[2:])
            else:
                result["status"] = "FAILED"
                result["errors"].append(f"Invalid dbnomics_id format: {dbnomics_id}")
                return result

            # Fetch the series
            df = db.fetch_series(provider_code, dataset_code, series_code)

            if df is None or df.empty:
                result["status"] = "PARTIAL"
                result["errors"].append("No data returned")
                return result

            inserted = 0
            with self.engine.begin() as conn:
                for _, row in df.iterrows():
                    try:
                        # DBnomics returns 'period' and 'value' columns
                        period = row.get("period", row.get("original_period", ""))
                        value = row.get("value")

                        if value is None or pd.isna(value):
                            continue

                        obs_dt = pd.Timestamp(str(period)).date()
                        value = float(value)

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
                    except (ValueError, TypeError) as row_exc:
                        log.debug("Skipping DBnomics row: {err}", err=str(row_exc))

            result["rows_inserted"] = inserted
            log.info("DBnomics {fn}: inserted {n} rows", fn=feature_name, n=inserted)

        except Exception as exc:
            log.error("DBnomics pull failed for {fn}: {err}", fn=feature_name, err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        return result

    def pull_all(self, start_date: str | date = "1990-01-01") -> dict[str, Any]:
        """Pull all DBnomics series."""
        log.info("Starting DBnomics bulk pull")
        results: list[dict[str, Any]] = []

        for dbnomics_id, feature_name in DBNOMICS_SERIES.items():
            res = self.pull_series(dbnomics_id, feature_name, start_date)
            results.append(res)
            time.sleep(_RATE_LIMIT_DELAY)

        total_rows = sum(r["rows_inserted"] for r in results)
        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        log.info(
            "DBnomics bulk pull complete — {ok}/{total} succeeded, {rows} rows",
            ok=succeeded, total=len(results), rows=total_rows,
        )
        return {
            "source": "DBnomics",
            "total_rows": total_rows,
            "succeeded": succeeded,
            "total": len(results),
        }
