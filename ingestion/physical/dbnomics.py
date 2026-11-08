"""
GRID DBnomics aggregated central bank data ingestion module.

Pulls time series from 100+ providers via the unified DBnomics API.
Acts as a fallback and supplementary source for international data.
"""

from __future__ import annotations

import time
from datetime import date
from typing import Any

import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from ingestion.base import BasePuller

# DBnomics series: provider/dataset/series -> feature name
#
# Checked against api.db.nomics.world 2026-11-07:
# * The three Eurostat mirrors (prc_hicp_manr, une_rt_m, sts_inpr_m) are gone:
#   ingestion/international/eurostat.py pulls them direct under the same
#   feature names, and DBnomics still mirrors the retired prc_hicp_manr
#   (frozen at 2025-12) while the other two 404. Writing the same series_id
#   from two sources also makes store.observations raise MixedSourceError.
# * france_10y_yield moved to ECB/IRS (same dataset as Italy); BDF/FM 404s.
# * us_credit_gdp_bis moved to BIS/WS_TC (private non-financial sector,
#   all lenders, % of GDP); BIS/total_credit 404s.
# * OECD/KEI, BDF/BP and Bundesbank/BBDP1 404 with no DBnomics replacement,
#   so germany_cli_dbnomics, france_current_account and germany_cpi_dbnomics
#   are dropped. None of the five had ever stored a row.
DBNOMICS_SERIES: dict[str, str] = {
    "ECB/IRS/M.IT.L.L40.CI.0000.EUR.N.Z": "italy_10y_ecb_dbnomics",
    "ECB/IRS/M.FR.L.L40.CI.0000.EUR.N.Z": "france_10y_yield",
    "IMF/IFS/M.US.FIMM_PA.USD": "us_mmkt_rate_imf",
    "BIS/WS_TC/Q.US.P.A.M.770.A": "us_credit_gdp_bis",
}

_RATE_LIMIT_DELAY: float = 2.0


class DBnomicsPuller(BasePuller):
    """Pulls time series data from DBnomics unified API."""

    SOURCE_NAME = "DBnomics"
    SOURCE_CONFIG = {"base_url": "https://api.db.nomics.world/v22", "cost_tier": "FREE", "latency_class": "EOD", "pit_available": True, "revision_behavior": "RARE", "trust_score": "HIGH", "priority_rank": 22}

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info("DBnomicsPuller initialised — source_id={sid}", sid=self.source_id)

    def _latest_stored_values(self, series_id: str, conn: Any) -> dict[date, float]:
        """Latest SUCCESS value per obs_date this source stored for ``series_id``."""
        rows = conn.execute(
            text(
                "SELECT DISTINCT ON (obs_date) obs_date, value FROM raw_series "
                "WHERE series_id = :sid AND source_id = :src AND pull_status = 'SUCCESS' "
                "ORDER BY obs_date, pull_timestamp DESC"
            ),
            {"sid": series_id, "src": self.source_id},
        ).fetchall()
        return {r[0]: float(r[1]) for r in rows if r[1] is not None}

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
                # Compare against the latest stored vintage, not the 1-hour
                # _row_exists window: the old check re-inserted the whole
                # history on every pull (eurozone_hicp_yoy held 36,888 rows
                # for 348 dates on 2026-11-07).
                stored = self._latest_stored_values(feature_name, conn)
                for _, row in df.iterrows():
                    try:
                        # DBnomics returns 'period' and 'value' columns
                        period = row.get("period", row.get("original_period", ""))
                        value = row.get("value")

                        if value is None or pd.isna(value):
                            continue

                        obs_dt = pd.Timestamp(str(period)).date()
                        value = float(value)

                        if obs_dt in stored and stored[obs_dt] == value:
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
            # Provider/network failure, reported in the result: not a GRID bug.
            log.warning("DBnomics pull failed for {fn}: {err}", fn=feature_name, err=str(exc))
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
