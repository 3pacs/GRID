"""CAT-30 — Money market fund composition (PUL, Tier A).

Pulls the Fed's weekly money market fund portfolio composition from
FRED. MMF assets split across T-bills, RRP, repo, and agency paper is
the clearest single view of where short-end dollar cash is parked.

Why this matters (Tier A catalog #30): the RRP drainage cycle is one
of the most directly tradable macro signals. When MMFs pull out of
RRP and into T-bills, net reserves in the banking system rise, which
feeds directly into the ALPHA-5 liquidity regime classifier and the
CAT-124 FCI composite. Conversely, flight TO RRP signals short-end
risk-off.

Series pulled (from FRED / OFR MMF Monitor via FRED proxies):

  MMMFFAQ027S   Total financial assets of money market funds (quarterly)
  WRBWFRBL      Reverse repo operations, weekly
  RRPONTSYD     Overnight reverse repurchase agreements (NY Fed facility)
  WGS3MO        3-Month Treasury constant maturity (for the T-bill rate
                MMFs are rotating into)

Stored under raw_series 'fed_mmf:<fred_code>'. Weekly cadence, 2-year
lookback for dedup-friendly re-pulls.

Builds on the BasePuller scaffold (see ingestion/base.py). Reuses the
same FRED fetch pattern as ingestion/altdata/h8_bank_balance.py from
CAT-27.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text

from ingestion.base import BasePuller


# ── Core MMF series ───────────────────────────────────────────────────────

MMF_SERIES: dict[str, str] = {
    "MMMFFAQ027S": "total_mmf_assets",
    "WRBWFRBL": "reverse_repo_weekly",
    "RRPONTSYD": "overnight_rrp",
    "WGS3MO": "three_month_treasury",
}

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
_LOOKBACK_YEARS = 2


@dataclass
class MMFRow:
    series_id: str
    obs_date: date
    value: float


class MMFCompositionPuller(BasePuller):
    """Weekly money market fund composition puller from FRED."""

    SOURCE_NAME = "fed_mmf_composition"
    SOURCE_CONFIG = {
        "base_url": FRED_BASE,
        "cost_tier": "FREE",
        "latency_class": "WEEKLY",
        "pit_available": True,
        "revision_behavior": "SMALL",
        "trust_score": "HIGH",
        "priority_rank": 25,
    }

    def __init__(self, api_key: str, db_engine) -> None:
        super().__init__(db_engine)
        self.api_key = api_key

    def _fetch_series(
        self, fred_code: str, *, timeout: float = 10.0,
    ) -> list[MMFRow]:
        if not self.api_key:
            log.warning("mmf_composition: FRED_API_KEY not set")
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
            log.warning("mmf_composition fetch failed {c}: {e}",
                        c=fred_code, e=str(exc))
            return []
        rows: list[MMFRow] = []
        for obs in payload.get("observations", []):
            val_str = obs.get("value", ".")
            if val_str == "." or not val_str:
                continue
            try:
                rows.append(MMFRow(
                    series_id=f"fed_mmf:{fred_code}",
                    obs_date=date.fromisoformat(obs["date"]),
                    value=float(val_str),
                ))
            except (ValueError, KeyError):
                continue
        return rows

    def _upsert_rows(self, rows: list[MMFRow]) -> int:
        if not rows:
            return 0
        inserted = 0
        with self.engine.begin() as conn:
            by_series: dict[str, list[MMFRow]] = {}
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
                        log.debug("mmf insert failed {s} {d}: {e}",
                                  s=r.series_id, d=r.obs_date, e=str(exc))
        return inserted

    def pull_all(self) -> dict[str, Any]:
        total_fetched = 0
        total_inserted = 0
        per_series: dict[str, int] = {}
        for fred_code, label in MMF_SERIES.items():
            rows = self._fetch_series(fred_code)
            total_fetched += len(rows)
            inserted = self._upsert_rows(rows)
            total_inserted += inserted
            per_series[label] = inserted
        log.info(
            "mmf_composition: fetched {f} rows, {i} new ({s} series)",
            f=total_fetched, i=total_inserted, s=len(MMF_SERIES),
        )
        return {
            "fetched": total_fetched,
            "inserted": total_inserted,
            "series": per_series,
        }


def run_mmf_puller(engine) -> dict[str, Any]:
    from config import settings
    puller = MMFCompositionPuller(
        api_key=getattr(settings, "FRED_API_KEY", "") or "",
        db_engine=engine,
    )
    return puller.pull_all()
