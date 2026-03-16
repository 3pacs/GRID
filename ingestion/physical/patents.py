"""
GRID USPTO PatentsView ingestion module.

Pulls patent application velocity data by CPC technology class from the
PatentsView API. Derives innovation cycle metrics and composite indices.
"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from tenacity import retry, stop_after_attempt, wait_exponential

# CPC subclass -> feature name mapping
CPC_GROUPS: dict[str, str] = {
    "G06": "patent_velocity_software",
    "H01": "patent_velocity_electrical",
    "A61K": "patent_velocity_pharma",
    "A61P": "patent_velocity_therapeutic",
    "Y02": "patent_velocity_cleanenergy",
    "F03": "patent_velocity_mechanical_energy",
    "C12": "patent_velocity_biotech",
    "H04": "patent_velocity_telecom",
    "B60": "patent_velocity_auto",
}

_PATENTSVIEW_BASE_URL = "https://api.patentsview.org/patents/query"
_RATE_LIMIT_DELAY: float = 3.0


class PatentsPuller:
    """Pulls patent velocity data from the USPTO PatentsView API."""

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine
        self.source_id = self._resolve_source_id()
        log.info("PatentsPuller initialised — source_id={sid}", sid=self.source_id)

    def _resolve_source_id(self) -> int:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM source_catalog WHERE name = :name"),
                {"name": "USPTO_PV"},
            ).fetchone()
        if row is None:
            with self.engine.begin() as conn:
                result = conn.execute(
                    text(
                        "INSERT INTO source_catalog "
                        "(name, base_url, license_type, update_frequency, "
                        "has_vintage_data, revision_policy, data_quality, priority, model_eligible) "
                        "VALUES (:name, :url, 'FREE', 'ANNUAL', FALSE, 'NEVER', 'HIGH', 31, TRUE) "
                        "RETURNING id"
                    ),
                    {"name": "USPTO_PV", "url": _PATENTSVIEW_BASE_URL},
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
    def _query_patentsview(self, cpc_subclass: str, start_year: int, end_year: int) -> list[dict]:
        """Query PatentsView API for patent counts by CPC class."""
        query = {
            "q": {"_and": [
                {"_begins": {"cpc_subclass_id": cpc_subclass}},
                {"_gte": {"app_date": f"{start_year}-01-01"}},
                {"_lte": {"app_date": f"{end_year}-12-31"}},
            ]},
            "f": ["app_date", "patent_date", "cpc_subclass_id"],
            "o": {"per_page": 1000},
        }
        resp = requests.post(_PATENTSVIEW_BASE_URL, json=query, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        return data.get("patents", [])

    def pull_cpc_velocity(
        self,
        cpc_subclass: str,
        start_year: int = 1976,
        end_year: int | None = None,
    ) -> dict[str, Any]:
        """Pull patent application velocity for a CPC subclass.

        Counts applications per year and computes YoY change.
        """
        if end_year is None:
            end_year = date.today().year

        feature_name = CPC_GROUPS.get(cpc_subclass, f"patent_velocity_{cpc_subclass.lower()}")
        log.info("Pulling patent velocity for CPC {cpc}", cpc=cpc_subclass)

        result: dict[str, Any] = {
            "series_id": feature_name,
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        try:
            # Query in year batches to avoid timeout
            annual_counts: dict[int, int] = {}
            for year in range(start_year, end_year + 1, 10):
                batch_end = min(year + 9, end_year)
                try:
                    patents = self._query_patentsview(cpc_subclass, year, batch_end)
                    for patent in patents:
                        app_date = patent.get("app_date", "")
                        if app_date and len(app_date) >= 4:
                            app_year = int(app_date[:4])
                            annual_counts[app_year] = annual_counts.get(app_year, 0) + 1
                except Exception as batch_exc:
                    log.warning(
                        "PatentsView batch {y}-{e} failed: {err}",
                        y=year, e=batch_end, err=str(batch_exc),
                    )
                time.sleep(_RATE_LIMIT_DELAY)

            # Insert annual counts and YoY changes
            inserted = 0
            sorted_years = sorted(annual_counts.keys())

            with self.engine.begin() as conn:
                for year in sorted_years:
                    count = annual_counts[year]
                    obs_dt = date(year, 1, 1)

                    if not self._row_exists(feature_name, obs_dt, conn):
                        conn.execute(
                            text(
                                "INSERT INTO raw_series "
                                "(series_id, source_id, obs_date, value, pull_status) "
                                "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                            ),
                            {"sid": feature_name, "src": self.source_id, "od": obs_dt, "val": float(count)},
                        )
                        inserted += 1

                # Compute YoY velocity change
                for i in range(1, len(sorted_years)):
                    prev_count = annual_counts[sorted_years[i - 1]]
                    curr_count = annual_counts[sorted_years[i]]
                    if prev_count > 0:
                        yoy = (curr_count - prev_count) / prev_count * 100
                        yoy_feature = f"{feature_name}_yoy"
                        obs_dt = date(sorted_years[i], 1, 1)
                        if not self._row_exists(yoy_feature, obs_dt, conn):
                            conn.execute(
                                text(
                                    "INSERT INTO raw_series "
                                    "(series_id, source_id, obs_date, value, pull_status) "
                                    "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                                ),
                                {"sid": yoy_feature, "src": self.source_id, "od": obs_dt, "val": yoy},
                            )
                            inserted += 1

            result["rows_inserted"] = inserted
            log.info("PatentsView CPC {cpc}: inserted {n} rows", cpc=cpc_subclass, n=inserted)

        except Exception as exc:
            log.error("PatentsView pull failed for {cpc}: {err}", cpc=cpc_subclass, err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        return result

    def compute_innovation_cycle(self, start_year: int = 1976) -> dict[str, Any]:
        """Compute innovation composite from tech + biotech + energy velocities."""
        result: dict[str, Any] = {"rows_inserted": 0, "status": "SUCCESS", "errors": []}

        try:
            with self.engine.begin() as conn:
                # Get all velocity series
                tech_rows = conn.execute(
                    text(
                        "SELECT obs_date, value FROM raw_series "
                        "WHERE series_id = 'patent_velocity_software' AND source_id = :src "
                        "AND pull_status = 'SUCCESS' ORDER BY obs_date"
                    ),
                    {"src": self.source_id},
                ).fetchall()

                bio_rows = conn.execute(
                    text(
                        "SELECT obs_date, value FROM raw_series "
                        "WHERE series_id = 'patent_velocity_biotech' AND source_id = :src "
                        "AND pull_status = 'SUCCESS' ORDER BY obs_date"
                    ),
                    {"src": self.source_id},
                ).fetchall()

                energy_rows = conn.execute(
                    text(
                        "SELECT obs_date, value FROM raw_series "
                        "WHERE series_id = 'patent_velocity_cleanenergy' AND source_id = :src "
                        "AND pull_status = 'SUCCESS' ORDER BY obs_date"
                    ),
                    {"src": self.source_id},
                ).fetchall()

                # Build date-aligned composite (weighted: tech 0.5, bio 0.3, energy 0.2)
                tech_dict = {r[0]: r[1] for r in tech_rows}
                bio_dict = {r[0]: r[1] for r in bio_rows}
                energy_dict = {r[0]: r[1] for r in energy_rows}

                all_dates = set(tech_dict.keys()) & set(bio_dict.keys()) & set(energy_dict.keys())
                inserted = 0

                for obs_dt in sorted(all_dates):
                    composite = (
                        0.5 * tech_dict[obs_dt]
                        + 0.3 * bio_dict[obs_dt]
                        + 0.2 * energy_dict[obs_dt]
                    )
                    if not self._row_exists("innovation_composite", obs_dt, conn):
                        conn.execute(
                            text(
                                "INSERT INTO raw_series "
                                "(series_id, source_id, obs_date, value, pull_status) "
                                "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                            ),
                            {"sid": "innovation_composite", "src": self.source_id, "od": obs_dt, "val": composite},
                        )
                        inserted += 1

                result["rows_inserted"] = inserted

        except Exception as exc:
            log.error("Innovation cycle computation failed: {err}", err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        return result

    def pull_all(self, start_year: int = 1976) -> dict[str, Any]:
        """Pull all CPC groups and compute innovation composite."""
        log.info("Starting PatentsView bulk pull from {sy}", sy=start_year)
        results: list[dict[str, Any]] = []

        for cpc in CPC_GROUPS:
            res = self.pull_cpc_velocity(cpc, start_year)
            results.append(res)

        # Compute composite after pulling all velocity series
        composite_result = self.compute_innovation_cycle(start_year)
        results.append(composite_result)

        total_rows = sum(r["rows_inserted"] for r in results)
        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        log.info(
            "PatentsView bulk pull complete — {ok}/{total} succeeded, {rows} rows",
            ok=succeeded, total=len(results), rows=total_rows,
        )
        return {
            "source": "USPTO_PV",
            "total_rows": total_rows,
            "succeeded": succeeded,
            "total": len(results),
        }
