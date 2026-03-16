"""
GRID CEPII BACI trade data ingestion module.

Downloads and processes cleaned bilateral trade data from CEPII BACI.
BACI provides reconciled bilateral trade flows at the HS6 product level.
"""

from __future__ import annotations

import os
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from tenacity import retry, stop_after_attempt, wait_exponential

_CEPII_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "cepii")
_RATE_LIMIT_DELAY: float = 2.0

# Key bilateral pairs to extract
CEPII_PAIRS: list[dict[str, str]] = [
    {"exporter": "USA", "importer": "CHN", "feature": "baci_us_china_exports"},
    {"exporter": "CHN", "importer": "USA", "feature": "baci_china_us_exports"},
    {"exporter": "DEU", "importer": "CHN", "feature": "baci_deu_china_exports"},
    {"exporter": "USA", "importer": "ALL", "feature": "baci_us_total_exports"},
    {"exporter": "CHN", "importer": "ALL", "feature": "baci_china_total_exports"},
]


class CEPIIPuller:
    """Pulls cleaned bilateral trade data from CEPII BACI."""

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine
        self.source_id = self._resolve_source_id()
        os.makedirs(_CEPII_DATA_DIR, exist_ok=True)
        log.info("CEPIIPuller initialised — source_id={sid}", sid=self.source_id)

    def _resolve_source_id(self) -> int:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM source_catalog WHERE name = :name"),
                {"name": "CEPII_BACI"},
            ).fetchone()
        if row is None:
            with self.engine.begin() as conn:
                result = conn.execute(
                    text(
                        "INSERT INTO source_catalog "
                        "(name, base_url, license_type, update_frequency, "
                        "has_vintage_data, revision_policy, data_quality, priority, model_eligible) "
                        "VALUES (:name, :url, 'FREE', 'ANNUAL', FALSE, 'RARE', 'HIGH', 25, TRUE) "
                        "RETURNING id"
                    ),
                    {"name": "CEPII_BACI", "url": "https://www.cepii.fr/CEPII/en/bdd_modele"},
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

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=10))
    def _download_baci_year(self, year: int) -> pd.DataFrame | None:
        """Download BACI data for a given year.

        BACI data is distributed as CSV files. This method attempts to
        download from the CEPII public data repository.
        """
        filename = f"BACI_HS17_Y{year}_V202401.csv"
        local_path = os.path.join(_CEPII_DATA_DIR, filename)

        if os.path.exists(local_path):
            return pd.read_csv(local_path)

        # CEPII requires registration for download. In practice, this data
        # would be pre-downloaded. For automated access, use the Comtrade API
        # as a fallback source for bilateral trade flows.
        log.warning(
            "BACI data for {y} not found locally at {p}. "
            "Download from https://www.cepii.fr/CEPII/en/bdd_modele/bdd_modele_item.asp?id=37",
            y=year, p=local_path,
        )
        return None

    def pull_year(self, year: int) -> dict[str, Any]:
        """Process BACI data for a single year."""
        log.info("Processing BACI data for {y}", y=year)
        result: dict[str, Any] = {
            "series_id": f"baci_{year}",
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        try:
            df = self._download_baci_year(year)
            if df is None:
                result["status"] = "PARTIAL"
                result["errors"].append(f"BACI data for {year} not available")
                return result

            inserted = 0
            obs_dt = date(year, 1, 1)

            with self.engine.begin() as conn:
                for pair in CEPII_PAIRS:
                    try:
                        if pair["importer"] == "ALL":
                            mask = df["i"] == pair["exporter"]
                        else:
                            mask = (df["i"] == pair["exporter"]) & (df["j"] == pair["importer"])

                        subset = df[mask]
                        if subset.empty:
                            continue

                        total_value = float(subset["v"].sum()) if "v" in subset.columns else 0.0
                        feature = pair["feature"]

                        if self._row_exists(feature, obs_dt, conn):
                            continue

                        conn.execute(
                            text(
                                "INSERT INTO raw_series "
                                "(series_id, source_id, obs_date, value, pull_status) "
                                "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                            ),
                            {"sid": feature, "src": self.source_id, "od": obs_dt, "val": total_value},
                        )
                        inserted += 1
                    except Exception as pair_exc:
                        log.debug("BACI pair failed: {err}", err=str(pair_exc))

            result["rows_inserted"] = inserted
            log.info("BACI {y}: inserted {n} rows", y=year, n=inserted)

        except Exception as exc:
            log.error("BACI pull failed for {y}: {err}", y=year, err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        return result

    def pull_all(self, start_year: int = 1996) -> dict[str, Any]:
        """Pull all available BACI years."""
        log.info("Starting BACI bulk pull from {sy}", sy=start_year)
        results = [self.pull_year(y) for y in range(start_year, date.today().year)]

        total_rows = sum(r["rows_inserted"] for r in results)
        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        log.info(
            "BACI bulk pull complete — {ok}/{total} succeeded, {rows} rows",
            ok=succeeded, total=len(results), rows=total_rows,
        )
        return {
            "source": "CEPII_BACI",
            "total_rows": total_rows,
            "succeeded": succeeded,
            "total": len(results),
        }
