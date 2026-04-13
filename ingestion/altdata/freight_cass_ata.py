"""CAT-81 — Cass Freight Index + ATA Truck Tonnage puller.

Two monthly indices that lead US industrial activity + CPI goods by
2-3 weeks:

  Cass Freight Index:
    - Shipments (volume of domestic freight)
    - Expenditures (total dollars spent on freight)
    - Expenditures/Shipments ratio = implied freight rate per shipment

  ATA Truck Tonnage Index:
    - Seasonally adjusted (for trend)
    - Not seasonally adjusted (for raw volume)

Both are published as FRED series — Cass via FRGTCASSSHP / FRGTCASSEXP
and ATA via TRUCKD11 / TRUCKNS. Monthly release cadence; data typically
lands around the 20th for the prior month.

Why this matters (Tier A catalog #81): Cass + ATA lead US ISM
manufacturing by 2-3 weeks with ~0.7 correlation. When both drop
2+ months in a row while ISM is still positive, it's a strong
leading-indicator divergence worth trading (short transports +
industrials, long defensives).

Stored under raw_series:
  freight:cass_shipments
  freight:cass_expenditures
  freight:ata_tonnage_sa
  freight:ata_tonnage_nsa

All 4 series come from FRED so the fetch path is identical to CAT-27
H.8 puller. Monthly cadence, 5-year lookback for revision robustness.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text

from ingestion.base import BasePuller


FREIGHT_SERIES: dict[str, str] = {
    "FRGTCASSSHP": "cass_shipments",
    "FRGTCASSEXP": "cass_expenditures",
    "TRUCKD11":   "ata_tonnage_sa",
    "TRUCKNS":    "ata_tonnage_nsa",
}

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
_LOOKBACK_YEARS = 5


@dataclass
class FreightRow:
    series_id: str
    obs_date: date
    value: float


class FreightPuller(BasePuller):
    """Monthly Cass + ATA freight indices puller from FRED."""

    SOURCE_NAME = "freight_cass_ata"
    SOURCE_CONFIG = {
        "base_url": FRED_BASE,
        "cost_tier": "FREE",
        "latency_class": "MONTHLY",
        "pit_available": True,
        "revision_behavior": "SMALL",
        "trust_score": "HIGH",
        "priority_rank": 28,
    }

    def __init__(self, api_key: str, db_engine) -> None:
        super().__init__(db_engine)
        self.api_key = api_key

    def _fetch_series(
        self, fred_code: str, *, timeout: float = 10.0,
    ) -> list[FreightRow]:
        if not self.api_key:
            log.warning("freight_cass_ata: FRED_API_KEY not set")
            return []
        params = {
            "series_id": fred_code,
            "api_key": self.api_key,
            "file_type": "json",
            "observation_start": (
                date.today().replace(year=date.today().year - _LOOKBACK_YEARS)
            ).isoformat(),
        }
        try:
            resp = requests.get(FRED_BASE, params=params, timeout=timeout)
            resp.raise_for_status()
            payload = resp.json()
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "freight_cass_ata fetch failed {c}: {e}",
                c=fred_code, e=str(exc),
            )
            return []

        label = FREIGHT_SERIES.get(fred_code, fred_code)
        rows: list[FreightRow] = []
        for obs in payload.get("observations", []):
            val_str = obs.get("value", ".")
            if val_str == "." or not val_str:
                continue
            try:
                rows.append(FreightRow(
                    series_id=f"freight:{label}",
                    obs_date=date.fromisoformat(obs["date"]),
                    value=float(val_str),
                ))
            except (ValueError, KeyError):
                continue
        return rows

    def _upsert_rows(self, rows: list[FreightRow]) -> int:
        if not rows:
            return 0
        inserted = 0
        with self.engine.begin() as conn:
            by_series: dict[str, list[FreightRow]] = {}
            for r in rows:
                by_series.setdefault(r.series_id, []).append(r)
            for series_id, batch in by_series.items():
                existing = self._get_existing_dates(series_id, conn)
                for r in batch:
                    if r.obs_date in existing:
                        continue
                    try:
                        conn.execute(
                            text(
                                "INSERT INTO raw_series "
                                "(series_id, source_id, obs_date, value, "
                                " pull_status, pull_timestamp) "
                                "VALUES (:sid, :src, :od, :val, 'SUCCESS', :ts)"
                            ),
                            {
                                "sid": r.series_id,
                                "src": self.source_id,
                                "od": r.obs_date,
                                "val": r.value,
                                "ts": datetime.now(timezone.utc),
                            },
                        )
                        inserted += 1
                    except Exception as exc:  # noqa: BLE001
                        log.debug(
                            "freight insert failed {s} {d}: {e}",
                            s=r.series_id, d=r.obs_date, e=str(exc),
                        )
        return inserted

    def pull_all(self) -> dict[str, Any]:
        total_fetched = 0
        total_inserted = 0
        per_series: dict[str, int] = {}
        for fred_code, label in FREIGHT_SERIES.items():
            rows = self._fetch_series(fred_code)
            total_fetched += len(rows)
            inserted = self._upsert_rows(rows)
            total_inserted += inserted
            per_series[label] = inserted
        log.info(
            "freight_cass_ata: {f} rows fetched, {i} new ({s} series)",
            f=total_fetched, i=total_inserted, s=len(FREIGHT_SERIES),
        )
        return {
            "fetched": total_fetched,
            "inserted": total_inserted,
            "series": per_series,
        }


def run_freight_puller(engine) -> dict[str, Any]:
    from config import settings
    puller = FreightPuller(
        api_key=getattr(settings, "FRED_API_KEY", "") or "",
        db_engine=engine,
    )
    return puller.pull_all()
