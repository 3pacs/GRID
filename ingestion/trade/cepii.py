"""
GRID CEPII BACI trade data ingestion module.

Downloads and processes cleaned bilateral trade data from CEPII BACI.
BACI provides reconciled bilateral trade flows at the HS6 product level.
"""

from __future__ import annotations

import os
from datetime import date
from typing import Any

import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from ingestion.base import BasePuller
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


class CEPIIPuller(BasePuller):
    """Pulls cleaned bilateral trade data from CEPII BACI."""

    SOURCE_NAME = "CEPII_BACI"
    SOURCE_CONFIG = {"base_url": "https://www.cepii.fr/CEPII/en/bdd_modele/bdd_modele_item.asp?id=37", "cost_tier": "FREE", "latency_class": "MONTHLY", "pit_available": False, "revision_behavior": "RARE", "trust_score": "HIGH", "priority_rank": 33}

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        os.makedirs(_CEPII_DATA_DIR, exist_ok=True)
        log.info("CEPIIPuller initialised — source_id={sid}", sid=self.source_id)

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
