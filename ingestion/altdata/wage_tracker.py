"""CAT-49 — Real-time wage tracker puller.

Pulls the Atlanta Fed Wage Growth Tracker — the best-available monthly
measure of wage inflation at the individual-worker level. The tracker
follows the same person month-over-month so it's immune to the
composition effects that bias aggregate wage measures (CES, ECI).

Series pulled from FRED:

  FRBATLWGT3MMAUMHWGO  Median wage growth, 3-month moving average
  FRBATLWGT3MMAUMHWGJS Job stayer wage growth
  FRBATLWGT3MMAUMHWGJSW Job switcher wage growth
  FRBATLWGTUHWGO       Overall unweighted median

Why this matters (Tier A catalog #49): wage growth persistence is
THE core Fed mandate question right now. Atlanta Fed tracker leads
official CES wages by 2-3 months and leads CPI services by 4-6 months.
When job switchers' wage growth drops below job stayers', the labor
market is weakening — a historically reliable pre-recession signal.

Same FRED pattern as CAT-27/30/81 pullers. Monthly cadence, 5-year
lookback for revisions. Stored under raw_series 'wage_tracker:<label>'.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text

from ingestion.base import BasePuller


WAGE_SERIES: dict[str, str] = {
    "FRBATLWGT3MMAUMHWGO":   "median_3mma_overall",
    "FRBATLWGT3MMAUMHWGJS":  "median_3mma_stayers",
    "FRBATLWGT3MMAUMHWGJSW": "median_3mma_switchers",
    "FRBATLWGTUHWGO":        "median_unweighted_overall",
}

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
_LOOKBACK_YEARS = 5


@dataclass
class WageRow:
    series_id: str
    obs_date: date
    value: float


class WageTrackerPuller(BasePuller):
    """Monthly Atlanta Fed Wage Growth Tracker puller from FRED."""

    SOURCE_NAME = "atlanta_fed_wage_tracker"
    SOURCE_CONFIG = {
        "base_url": FRED_BASE,
        "cost_tier": "FREE",
        "latency_class": "MONTHLY",
        "pit_available": True,
        "revision_behavior": "SMALL",
        "trust_score": "HIGH",
        "priority_rank": 24,
    }

    def __init__(self, api_key: str, db_engine) -> None:
        super().__init__(db_engine)
        self.api_key = api_key

    def _fetch_series(
        self, fred_code: str, *, timeout: float = 10.0,
    ) -> list[WageRow]:
        if not self.api_key:
            log.warning("wage_tracker: FRED_API_KEY not set")
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
            log.warning("wage_tracker fetch failed {c}: {e}",
                        c=fred_code, e=str(exc))
            return []

        label = WAGE_SERIES.get(fred_code, fred_code)
        rows: list[WageRow] = []
        for obs in payload.get("observations", []):
            val_str = obs.get("value", ".")
            if val_str == "." or not val_str:
                continue
            try:
                rows.append(WageRow(
                    series_id=f"wage_tracker:{label}",
                    obs_date=date.fromisoformat(obs["date"]),
                    value=float(val_str),
                ))
            except (ValueError, KeyError):
                continue
        return rows

    def _upsert_rows(self, rows: list[WageRow]) -> int:
        if not rows:
            return 0
        inserted = 0
        with self.engine.begin() as conn:
            by_series: dict[str, list[WageRow]] = {}
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
                            "wage insert failed {s} {d}: {e}",
                            s=r.series_id, d=r.obs_date, e=str(exc),
                        )
        return inserted

    def pull_all(self) -> dict[str, Any]:
        total_fetched = 0
        total_inserted = 0
        per_series: dict[str, int] = {}
        for fred_code, label in WAGE_SERIES.items():
            rows = self._fetch_series(fred_code)
            total_fetched += len(rows)
            inserted = self._upsert_rows(rows)
            total_inserted += inserted
            per_series[label] = inserted
        log.info(
            "wage_tracker: {f} rows fetched, {i} new ({s} series)",
            f=total_fetched, i=total_inserted, s=len(WAGE_SERIES),
        )
        return {
            "fetched": total_fetched,
            "inserted": total_inserted,
            "series": per_series,
        }


def run_wage_tracker_puller(engine) -> dict[str, Any]:
    from config import settings
    puller = WageTrackerPuller(
        api_key=getattr(settings, "FRED_API_KEY", "") or "",
        db_engine=engine,
    )
    return puller.pull_all()
