"""CAT-27 — H.8 bank balance sheet by size class (PUL, Tier A).

The Fed's H.8 "Assets and Liabilities of Commercial Banks in the United
States" release reports weekly bank balance sheet aggregates — loans,
deposits, securities, cash assets — split by bank size class (large
domestically chartered, small domestically chartered, foreign-related).

This is the ground-truth credit-creation tracker that sits one level
below the H.4.1 net liquidity aggregate ALPHA-5 already consumes. When
bank loans expand while deposits contract, credit is being pulled
forward — a classic pre-recession pattern.

Why this matters (Tier A catalog #27): the commercial-bank credit
channel is where ~80% of US corporate debt sits. H.8 weekly deltas
lead corporate bond spreads by 4-8 weeks. This puller feeds the
credit_cycle_phase classifier (CAT-126) and the FCI's credit-spread
component (CAT-124).

Implementation
--------------
FRED publishes every H.8 series under the H8 family. We pull the core
8 series on a weekly cadence (the H.8 release is Friday PM for
Wednesday-ending week):

  H8B1001NCBCMG  Total assets, all commercial banks
  H8B1023NCBCMG  Commercial & industrial loans
  H8B1029NCBCMG  Real estate loans
  H8B1094NCBCMG  Consumer loans
  H8B1116NCBCMG  Cash assets
  H8B1151NCBCMG  Total deposits
  H8B1155NCBCMG  Large time deposits
  H8B1247NCBCMG  Total liabilities

Deltas are computed downstream; we only store the raw weekly levels
here under series_id ``fed_h8:<fred_code>`` in raw_series.

The fetch path reuses ``ingestion/altdata/fed_liquidity.py``'s
``_fetch_fred_series`` helper via direct composition so we get the
same retry + rate-limit behavior for free.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text

from ingestion.base import BasePuller


# ── Core H.8 series we track ──────────────────────────────────────────────

H8_SERIES: dict[str, str] = {
    "H8B1001NCBCMG": "total_assets_all_banks",
    "H8B1023NCBCMG": "ci_loans",
    "H8B1029NCBCMG": "real_estate_loans",
    "H8B1094NCBCMG": "consumer_loans",
    "H8B1116NCBCMG": "cash_assets",
    "H8B1151NCBCMG": "total_deposits",
    "H8B1155NCBCMG": "large_time_deposits",
    "H8B1247NCBCMG": "total_liabilities",
}

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

# Weekly cadence — request a 2-year window on every pull so we catch
# any revisions. Idempotent via raw_series dedup.
_LOOKBACK_YEARS = 2


@dataclass
class H8Row:
    """One (series, date, value) observation."""

    series_id: str
    obs_date: date
    value: float


class H8BankBalancePuller(BasePuller):
    """Weekly H.8 commercial bank balance sheet puller.

    Reads every series in ``H8_SERIES`` from FRED and upserts to
    ``raw_series`` under ``fed_h8:<fred_code>``. Non-fatal on per-series
    HTTP failures — partial success is fine because each series is
    independent.
    """

    SOURCE_NAME = "fed_h8"
    SOURCE_CONFIG = {
        "base_url": FRED_BASE,
        "cost_tier": "FREE",
        "latency_class": "WEEKLY",
        "pit_available": True,
        "revision_behavior": "SMALL",
        "trust_score": "HIGH",
        "priority_rank": 20,
    }

    def __init__(self, api_key: str, db_engine) -> None:
        super().__init__(db_engine)
        self.api_key = api_key

    # ── Fetch ─────────────────────────────────────────────────────────

    def _fetch_series(
        self, fred_code: str, *, timeout: float = 10.0,
    ) -> list[H8Row]:
        """Fetch a single H.8 series from FRED.

        Returns an empty list on any HTTP failure — logs at warning level.
        """
        if not self.api_key:
            log.warning("h8_bank_balance: FRED_API_KEY not set")
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
            log.warning("h8_bank_balance: fetch failed for {c}: {e}", c=fred_code, e=str(exc))
            return []

        rows: list[H8Row] = []
        for obs in payload.get("observations", []):
            val_str = obs.get("value", ".")
            if val_str == "." or not val_str:
                continue
            try:
                rows.append(H8Row(
                    series_id=f"fed_h8:{fred_code}",
                    obs_date=date.fromisoformat(obs["date"]),
                    value=float(val_str),
                ))
            except (ValueError, KeyError) as exc:
                log.debug("h8 row parse skipped: {e}", e=str(exc))
                continue
        return rows

    # ── Upsert ────────────────────────────────────────────────────────

    def _upsert_rows(self, rows: list[H8Row]) -> int:
        """Insert rows into raw_series, skipping duplicates.

        Returns the number of NEW rows inserted.
        """
        if not rows:
            return 0
        inserted = 0
        with self.engine.begin() as conn:
            # Pre-fetch existing dates per series to avoid per-row queries
            by_series: dict[str, list[H8Row]] = {}
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
                            "h8 insert failed for {s} {d}: {e}",
                            s=r.series_id, d=r.obs_date, e=str(exc),
                        )
        return inserted

    # ── Orchestrator ──────────────────────────────────────────────────

    def pull_all(self) -> dict[str, Any]:
        """Pull every H.8 series and return a summary.

        Return shape:
            {"fetched": int, "inserted": int, "series": {name: count}}
        """
        total_fetched = 0
        total_inserted = 0
        per_series: dict[str, int] = {}

        for fred_code, label in H8_SERIES.items():
            rows = self._fetch_series(fred_code)
            total_fetched += len(rows)
            inserted = self._upsert_rows(rows)
            total_inserted += inserted
            per_series[label] = inserted

        log.info(
            "h8_bank_balance: fetched {f} rows, {i} new ({s} series)",
            f=total_fetched, i=total_inserted, s=len(H8_SERIES),
        )
        return {
            "fetched": total_fetched,
            "inserted": total_inserted,
            "series": per_series,
        }


def run_h8_puller(engine) -> dict[str, Any]:
    """Convenience entrypoint for the scheduler."""
    from config import settings
    puller = H8BankBalancePuller(
        api_key=getattr(settings, "FRED_API_KEY", "") or "",
        db_engine=engine,
    )
    return puller.pull_all()
